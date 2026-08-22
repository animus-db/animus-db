//! `UpdateTable`'s `GlobalSecondaryIndexUpdates` `Delete` path (ADR 0045 §5):
//! the four-step convergent drop cascade — `SetIndexStatus{Deleting}` (plus
//! its cursor-cleanup side-step), `DropTableTablets` on the index's own
//! hidden table, `DropTableIndex`, and the belt-and-suspenders re-scan —
//! reached via the real DynamoDB `UpdateTable` wire request, not a
//! hand-driven `MetaCommand` (unlike `tests/backfill_seeder.rs`, which still
//! hand-drives `CreateTableIndex` — `UpdateTable`'s own index-*creating*
//! half now exists too, ADR 0045 §2/§6, see `tests/update_table_create_index.rs`).
//!
//! Three scenarios: dropping a fully `Active` index on a populated table
//! (the common case), dropping an index that is still mid-backfill
//! (`Creating`) — the in-flight-cancellation regression — and a
//! create-drop-recreate cycle of the exact same index name, which is the
//! sharp edge the cascade's own backfill-cursor cleanup exists to close
//! (see `crates/animusd/src/index_drain.rs::clear_backfill_cursor`'s doc).

mod support;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use animus_control::{IndexDef, IndexKind, IndexProjection, IndexStatus};
use animusd::{ClientRequest, ClientResponse, MetaCommand, Node, read_frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// Bring up an `n`-node per-process combined cluster — duplicated from
/// `tests/backfill_seeder.rs` rather than shared, per this codebase's own
/// "sibling test modules keep their own fixtures independent" convention.
async fn bring_up(n: usize, dir: &Path) -> (Vec<Node>, animusd::ClusterConfig) {
    let mut brought_up = None;
    'attempts: for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
            })
            .collect();
        let config = animusd::ClusterConfig { nodes: nodes_cfg };
        let mut nodes = Vec::new();
        for i in 0..n {
            match animusd::run_node(&config, i, dir.join(format!("node-{attempt}-{i}"))).await {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    for node in &nodes {
                        node.shutdown_graceful().await;
                    }
                    sleep(Duration::from_millis(50)).await;
                    continue 'attempts;
                }
            }
        }
        brought_up = Some((nodes, config));
        break;
    }
    let (nodes, config) =
        brought_up.expect("could not bring up cluster after retries (ports kept getting stolen)");
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap in 20s");
    (nodes, config)
}

/// One DynamoDB JSON request over the real HTTP wire (duplicated per this
/// module's own doc — every sibling test file that needs the DynamoDB wire
/// keeps its own copy of this helper).
async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    let req = format!(
        "POST / HTTP/1.1\r\nHost: x\r\nX-Amz-Target: {target}\r\n\
         Connection: close\r\n\
         Content-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.expect("read");
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
    (status, body.to_owned())
}

async fn create_table_no_index(addr: SocketAddr, table: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
}

/// `CreateTable` with a GSI declared up front — created `Active` immediately
/// (ADR 0045 §1: a just-created table is empty by construction), unlike
/// `tests/backfill_seeder.rs`'s hand-driven `Creating` fixtures.
async fn create_table_with_gsi(addr: SocketAddr, table: &str, index: &str, hash_attr: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "GlobalSecondaryIndexes":[
                    {{"IndexName":"{index}",
                     "KeySchema":[{{"AttributeName":"{hash_attr}","KeyType":"HASH"}}],
                     "Projection":{{"ProjectionType":"ALL"}}}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
}

async fn put_item(addr: SocketAddr, table: &str, id: &str, attr: &str, value: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        &format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"{attr}":{{"S":"{value}"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "PutItem({id}) failed: {body}");
}

/// Populate `table` with `attr = "{id}@x"` for every id in `ids`, via
/// `BatchWriteItem` in chunks (one Raft entry per chunk) — duplicated from
/// `tests/backfill_seeder.rs::split_during_backfill_converges_with_correct_
/// final_gsi`'s own technique, gentler on WAL fsync throughput than one
/// `PutItem` round trip per item under concurrent multi-replica load.
async fn batch_put_items(addr: SocketAddr, table: &str, attr: &str, ids: &[String]) {
    for chunk in ids.chunks(100) {
        let puts: Vec<String> = chunk
            .iter()
            .map(|id| {
                format!(
                    r#"{{"PutRequest":{{"Item":{{"id":{{"S":"{id}"}},"{attr}":{{"S":"{id}@x"}}}}}}}}"#
                )
            })
            .collect();
        let body = format!(r#"{{"RequestItems":{{"{table}":[{}]}}}}"#, puts.join(","));
        let (status, resp) = dynamo(addr, "DynamoDB_20120810.BatchWriteItem", &body).await;
        assert_eq!(status, 200, "BatchWriteItem failed: {resp}");
    }
}

/// A `Creating` GSI definition hashing on `hash_attribute` (duplicated from
/// `tests/backfill_seeder.rs`).
fn creating_index(name: &str, hash_attribute: &str) -> IndexDef {
    IndexDef {
        name: name.to_owned(),
        kind: IndexKind::Global,
        hash_attribute: hash_attribute.to_owned(),
        sort_attribute: None,
        projection: IndexProjection::All,
        status: IndexStatus::Creating,
    }
}

/// `UpdateTable` with a single `GlobalSecondaryIndexUpdates` `Delete`
/// element (ADR 0045 §6) — the real wire path this whole file tests.
async fn delete_index_via_wire(addr: SocketAddr, table: &str, index: &str) -> (u16, String) {
    dynamo(
        addr,
        "DynamoDB_20120810.UpdateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "GlobalSecondaryIndexUpdates":[{{"Delete":{{"IndexName":"{index}"}}}}]}}"#
        ),
    )
    .await
}

fn has_table_tablet(node: &Node, table: &str) -> bool {
    node.metadata()
        .tablets
        .values()
        .any(|t| t.table.as_deref() == Some(table))
}

fn tablet_ids_for(node: &Node, table: &str) -> Vec<u64> {
    node.metadata()
        .tablets
        .iter()
        .filter(|(_, t)| t.table.as_deref() == Some(table))
        .map(|(id, _)| id.0)
        .collect()
}

fn has_index(node: &Node, table: &str, index: &str) -> bool {
    node.metadata()
        .table_indexes(table)
        .iter()
        .any(|i| i.name == index)
}

/// The file names directly inside `dir` (duplicated from
/// `tests/drop_table_index_cascade.rs`).
fn files_in(dir: &Path) -> Vec<String> {
    match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
            .filter_map(|e| e.file_name().into_string().ok())
            .collect(),
        Err(_) => Vec::new(),
    }
}

fn tablet_wal_present(dir: &Path, tablet: u64) -> bool {
    files_in(dir).contains(&animus_cp_data::wal_file(tablet))
}

async fn await_true<F: Fn() -> bool>(secs: u64, what: &str, cond: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !cond() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        sleep(Duration::from_millis(100)).await;
    }
}

/// How many live rows a table holds, via a whole-table client-protocol scan
/// (duplicated from `tests/backfill_seeder.rs`'s own helper of the same
/// shape). Counts decoded live items, not raw pairs, so a `DeleteItem`
/// tombstone is never counted.
async fn row_count(addr: SocketAddr, table: &str) -> Option<usize> {
    let once = async {
        let mut s = TcpStream::connect(addr).await.ok()?;
        let req = ClientRequest::Scan {
            start: Vec::new(),
            end: None,
            limit: None,
            reverse: false,
            table: table.to_owned(),
        };
        animusd::write_frame(&mut s, &req).await.ok()?;
        match read_frame(&mut s).await.ok()?? {
            ClientResponse::Pairs(rows) => Some(
                rows.iter()
                    .filter(|(_, v)| {
                        matches!(animus_dynamo::wire::decode_stored_item(v), Ok(Some(_)))
                    })
                    .count(),
            ),
            _ => Some(0),
        }
    };
    timeout(Duration::from_secs(5), once).await.ok().flatten()
}

/// A DynamoDB JSON request whose response is never read — used only to
/// start a request and abandon it, for the crash-mid-cascade test below
/// (the request's own eventual reply, if any, races the node's shutdown and
/// is irrelevant either way).
fn fire_and_forget_dynamo(addr: SocketAddr, target: &'static str, body: String) {
    tokio::spawn(async move {
        let _ = dynamo(addr, target, &body).await;
    });
}

async fn await_row_count(addr: SocketAddr, table: &str, want: usize, what: &str) {
    let last = std::sync::Arc::new(std::sync::Mutex::new(None::<usize>));
    let seen = std::sync::Arc::clone(&last);
    let converged = async move {
        loop {
            let got = row_count(addr, table).await;
            *seen.lock().unwrap() = got;
            if got == Some(want) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    if timeout(Duration::from_secs(60), converged).await.is_err() {
        let got = *last.lock().unwrap();
        panic!("{what}: `{table}` never reached {want} rows (last saw {got:?})");
    }
}

/// Dropping a fully `Active` GSI on a populated table via the real
/// `UpdateTable` wire path: the hidden table's tablets leave the tablet map
/// and their data is genuinely reclaimed (not merely orphaned), the catalog
/// entry disappears, the base table is completely unaffected, and a
/// subsequent `Query` naming the now-gone index errors cleanly (the same
/// `NoSuchIndex` `ValidationException` an always-unknown index gets — this
/// adapter never distinguishes "never existed" from "existed and was
/// dropped" with a different error code, matching the fact that real
/// DynamoDB doesn't either; this test asserts that today's behavior, not a
/// promise of a future distinction).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn drop_of_an_active_index_on_a_populated_table_reclaims_everything() {
    timeout(Duration::from_secs(120), async {
        let tmp = tempfile::tempdir().unwrap();
        let (node, _config) =
            support::start_single_node(tmp.path(), animusd::StorageBackend::default()).await;
        let dynamo_addr = node.dynamo_addr();
        let client_addr = node.client_addr();
        let raftkv_dir = tmp.path().join("internal");
        let table = "drop_active";
        let index = "by-email";
        let index_table = "drop_active$by-email";

        create_table_with_gsi(dynamo_addr, table, index, "email").await;
        for (id, email) in [("u1", "a@x"), ("u2", "b@x"), ("u3", "c@x")] {
            put_item(dynamo_addr, table, id, "email", email).await;
        }

        await_true(10, "base tablet provisioned", || {
            has_table_tablet(&node, table)
        })
        .await;
        await_row_count(client_addr, index_table, 3, "GSI converges before drop").await;
        await_true(10, "hidden index table's tablet exists", || {
            has_table_tablet(&node, index_table)
        })
        .await;
        let index_tablets = tablet_ids_for(&node, index_table);
        assert!(!index_tablets.is_empty());

        let (status, body) = delete_index_via_wire(dynamo_addr, table, index).await;
        assert_eq!(status, 200, "UpdateTable Delete failed: {body}");

        // The catalog definition and the hidden table's tablets are gone —
        // and, per `drop_index`'s own commit-wait discipline, already gone
        // on THIS node by the time the 200 response returned (single-node
        // cluster: no replication lag to poll through), but polled anyway
        // per this codebase's own "no fixed-sleep-then-assert" discipline.
        await_true(10, "index definition removed from catalog", || {
            !has_index(&node, table, index)
        })
        .await;
        await_true(10, "hidden index table's tablet dropped", || {
            !has_table_tablet(&node, index_table)
        })
        .await;
        for tablet in index_tablets {
            await_true(30, "hidden table's WAL file reclaimed", || {
                !tablet_wal_present(&raftkv_dir, tablet)
            })
            .await;
        }

        // The base table itself is completely unaffected.
        await_row_count(client_addr, table, 3, "base table unaffected by index drop").await;
        assert!(has_table_tablet(&node, table));

        // A Query against the now-gone index errors cleanly.
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.Query",
            &format!(
                r#"{{"TableName":"{table}","IndexName":"{index}",
                    "KeyConditionExpression":"email = :v",
                    "ExpressionAttributeValues":{{":v":{{"S":"a@x"}}}}}}"#
            ),
        )
        .await;
        assert_ne!(
            status, 200,
            "Query against a dropped index should fail: {body}"
        );
        assert!(
            body.contains("ValidationException"),
            "expected ValidationException, got: {body}"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// The in-flight-cancellation regression: start a backfill on a populated
/// table (300 rows — `BACKFILL_SEED_BATCH == 256`, so the very first seeder
/// tick provably cannot finish sweeping it in one pass, the identical
/// margin `tests/backfill_seeder.rs`'s own split-during-backfill test
/// relies on), then issue the `UpdateTable` `Delete` essentially
/// immediately — concurrently with a background poll proving the index
/// never once reaches `Active` before it converges to fully removed: no
/// orphan hidden tablets, no `index_backfill` rows, no `Active` flip ever
/// observed.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn in_flight_backfill_is_cancelled_by_a_concurrent_drop() {
    timeout(Duration::from_secs(120), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, config) = bring_up(3, dir.path()).await;
        let leader = nodes.iter().position(Node::is_control_leader).unwrap();
        // ADR 0047: `ProposeSchema` is intra-only.
        let client = config.nodes[leader].intra;
        let dynamo_addr = nodes[0].dynamo_addr();
        let client_addr = nodes[0].client_addr();
        let table = "cancel_bf";
        let index = "by-cancel";
        let index_table = "cancel_bf$by-cancel";

        create_table_no_index(dynamo_addr, table).await;
        let ids: Vec<String> = (0..300).map(|i| format!("c{i:04}")).collect();
        batch_put_items(dynamo_addr, table, "g", &ids).await;

        call(
            client,
            ClientRequest::ProposeSchema(MetaCommand::CreateTableIndex {
                table: table.into(),
                index: creating_index(index, "g"),
            }),
        )
        .await;
        // The index must actually be visible on the node the race below
        // issues the `Delete` against before racing it — 300 >
        // `BACKFILL_SEED_BATCH` still guarantees at least one full tick
        // (200ms) before any *chance* of `Active`, so this wait doesn't
        // undermine the "still genuinely mid-backfill" property.
        await_true(10, "index visible before racing the drop", || {
            has_index(&nodes[0], table, index)
        })
        .await;

        let saw_active = Arc::new(AtomicBool::new(false));
        let watch_flag = Arc::clone(&saw_active);
        let watcher = async {
            loop {
                if nodes.iter().any(|n| {
                    n.metadata()
                        .table_indexes(table)
                        .iter()
                        .any(|i| i.name == index && i.status == IndexStatus::Active)
                }) {
                    watch_flag.store(true, Ordering::SeqCst);
                    return;
                }
                if nodes.iter().all(|n| !has_index(n, table, index)) {
                    return; // fully dropped everywhere — nothing left to watch for
                }
                sleep(Duration::from_millis(5)).await;
            }
        };
        let delete = async {
            let (status, body) = delete_index_via_wire(dynamo_addr, table, index).await;
            assert_eq!(status, 200, "UpdateTable Delete failed: {body}");
        };
        tokio::join!(watcher, delete);
        assert!(
            !saw_active.load(Ordering::SeqCst),
            "index reached Active before the concurrent drop cancelled its backfill"
        );

        for n in &nodes {
            await_true(30, "index definition removed from catalog", || {
                !has_index(n, table, index)
            })
            .await;
            await_true(30, "hidden index table's tablet dropped", || {
                !has_table_tablet(n, index_table)
            })
            .await;
            await_true(
                30,
                "no index_backfill rows remain for the cancelled index",
                || {
                    !n.metadata()
                        .index_backfill
                        .keys()
                        .any(|(_, name)| name == index)
                },
            )
            .await;
        }
        // The base table's own data is untouched by the cancelled backfill.
        await_row_count(
            client_addr,
            table,
            ids.len(),
            "base table survives cancellation",
        )
        .await;

        for n in &nodes {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// The sharp edge of accepting the backfill cursor as bounded garbage
/// instead of actively clearing it (see this PR's own report and
/// `index_drain::clear_backfill_cursor`'s doc): drop a fully backfilled
/// index, then recreate an index of the **exact same name**. If the
/// recreated index's own fresh seeder silently resumed from the deleted
/// index's old cursor position instead of starting over, it would flip
/// `Active` having seeded nothing at all (every pre-existing partition
/// looks "already scanned"), leaving its hidden table permanently empty.
/// The correct, cursor-cleaned behavior converges to `Active` with every
/// pre-existing row present, identical to the first index's own backfill.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn create_drop_recreate_same_index_name_backfills_from_scratch() {
    timeout(Duration::from_secs(120), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, config) = bring_up(3, dir.path()).await;
        let leader = nodes.iter().position(Node::is_control_leader).unwrap();
        // ADR 0047: `ProposeSchema` is intra-only.
        let client = config.nodes[leader].intra;
        let dynamo_addr = nodes[0].dynamo_addr();
        let client_addr = nodes[0].client_addr();
        let table = "recreate_bf";
        let index = "by-recreate";
        let index_table = "recreate_bf$by-recreate";

        create_table_no_index(dynamo_addr, table).await;
        let ids: Vec<String> = (0..15).map(|i| format!("r{i}")).collect();
        batch_put_items(dynamo_addr, table, "g", &ids).await;

        // First backfill: create, converge to Active, confirm full
        // materialization.
        call(
            client,
            ClientRequest::ProposeSchema(MetaCommand::CreateTableIndex {
                table: table.into(),
                index: creating_index(index, "g"),
            }),
        )
        .await;
        timeout(Duration::from_secs(60), async {
            loop {
                if nodes.iter().all(|n| {
                    n.metadata()
                        .table_indexes(table)
                        .iter()
                        .any(|i| i.name == index && i.status == IndexStatus::Active)
                }) {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("first backfill did not reach Active within 60s");
        await_row_count(
            client_addr,
            index_table,
            ids.len(),
            "first backfill converges",
        )
        .await;

        // Drop it via the real wire path.
        let (status, body) = delete_index_via_wire(dynamo_addr, table, index).await;
        assert_eq!(status, 200, "UpdateTable Delete failed: {body}");
        for n in &nodes {
            await_true(30, "first index fully dropped", || {
                !has_index(n, table, index)
            })
            .await;
            await_true(30, "hidden table dropped", || {
                !has_table_tablet(n, index_table)
            })
            .await;
        }

        // Recreate the SAME name. If the cursor were stale-poisoned, this
        // would flip Active having seeded zero rows.
        call(
            client,
            ClientRequest::ProposeSchema(MetaCommand::CreateTableIndex {
                table: table.into(),
                index: creating_index(index, "g"),
            }),
        )
        .await;
        timeout(Duration::from_secs(60), async {
            loop {
                if nodes.iter().all(|n| {
                    n.metadata()
                        .table_indexes(table)
                        .iter()
                        .any(|i| i.name == index && i.status == IndexStatus::Active)
                }) {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("recreated index did not reach Active within 60s");
        await_row_count(
            client_addr,
            index_table,
            ids.len(),
            "recreated index backfills every pre-existing row from scratch (not 0)",
        )
        .await;
        for id in &ids {
            let (status, body) = dynamo(
                dynamo_addr,
                "DynamoDB_20120810.Query",
                &format!(
                    r#"{{"TableName":"{table}","IndexName":"{index}",
                        "KeyConditionExpression":"g = :v",
                        "ExpressionAttributeValues":{{":v":{{"S":"{id}@x"}}}}}}"#
                ),
            )
            .await;
            assert_eq!(status, 200, "recreated-index Query failed: {body}");
            assert!(
                body.contains("\"Count\":1") && body.contains(&format!(r#""id":{{"S":"{id}"}}"#)),
                "recreated index missing pre-existing row {id}: {body}"
            );
        }

        for n in &nodes {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// Crash-resume, mirroring `tests/backfill_seeder.rs::
/// a_crash_and_restart_mid_backfill_still_converges`'s own established
/// pattern for this codebase (a real process stop shortly after starting an
/// operation, not a hand-timed pause between named steps — there is no
/// cheap hook into `drop_index`'s own internals from a `tests/` binary):
/// fire the `UpdateTable` `Delete` and abandon it, sleep briefly (long
/// enough that step 1 has very likely already committed, short enough that
/// the whole cascade very likely has not), then `shutdown_graceful` the
/// node (ADR 0024's own restart discipline: `Node::shutdown_graceful`
/// aborts in-flight tasks, including whichever step `drop_index` was
/// awaiting), restart on the same address + dir, and **retry the identical
/// `Delete` call**. Either outcome is correct and both are asserted for:
/// the retry itself succeeds (the cascade had not fully finished before
/// the abort), or it reports the index already gone (`ValidationException`
/// — the cascade had, in fact, already fully committed everything before
/// the abort landed, which the retry can't tell apart from "never
/// existed"). Either way, the **converged end state** — no catalog entry,
/// no hidden tablets, no `index_backfill` rows — must hold, proving each
/// step really is independently idempotent regardless of which subset had
/// committed before the process stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_and_retry_mid_cascade_still_converges() {
    timeout(Duration::from_secs(120), async {
        let tmp = tempfile::tempdir().unwrap();
        let (node, config) =
            support::start_single_node(tmp.path(), animusd::StorageBackend::default()).await;
        let table = "crash_drop";
        let index = "by-crash";
        let index_table = "crash_drop$by-crash";

        create_table_with_gsi(node.dynamo_addr(), table, index, "email").await;
        let ids: Vec<String> = (0..10).map(|i| format!("k{i}")).collect();
        for id in &ids {
            put_item(node.dynamo_addr(), table, id, "email", &format!("{id}@x")).await;
        }
        await_row_count(
            node.client_addr(),
            index_table,
            ids.len(),
            "GSI converges before drop",
        )
        .await;

        fire_and_forget_dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.UpdateTable",
            format!(
                r#"{{"TableName":"{table}",
                    "GlobalSecondaryIndexUpdates":[{{"Delete":{{"IndexName":"{index}"}}}}]}}"#
            ),
        );
        sleep(Duration::from_millis(15)).await;
        node.shutdown_graceful().await;

        let node2 =
            support::restart_same_addrs(&config, 0, tmp.path(), animusd::StorageBackend::default())
                .await;
        timeout(Duration::from_secs(10), async {
            loop {
                if node2.is_control_leader() {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("did not become control leader in time after restart");

        let (status, body) = delete_index_via_wire(node2.dynamo_addr(), table, index).await;
        assert!(
            status == 200 || body.contains("ValidationException"),
            "retry of the delete after crash got an unexpected reply: {status} {body}"
        );

        await_true(30, "index definition removed from catalog", || {
            !has_index(&node2, table, index)
        })
        .await;
        await_true(30, "hidden index table's tablet dropped", || {
            !has_table_tablet(&node2, index_table)
        })
        .await;
        await_true(30, "no index_backfill rows remain", || {
            !node2
                .metadata()
                .index_backfill
                .keys()
                .any(|(_, name)| name == index)
        })
        .await;
        await_row_count(
            node2.client_addr(),
            table,
            ids.len(),
            "base table survives the crash",
        )
        .await;

        node2.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}
