//! The secondary-index **backfill seeder** end to end (ADR 0045 §2):
//! `change_consumer_loop`'s backfill arm sweeps a pre-existing table's
//! `KIND_BASE` rows and seeds a dirty marker for each partition, so the
//! ordinary GSI drain materializes rows that predate the index's own
//! declaration — and the ADR 0045 §4 aggregator (`tests/index_backfill.rs`)
//! flips the index `Creating` → `Active` once every tablet has swept to its
//! own end.
//!
//! `UpdateTable`'s wire path for adding an index to a populated table doesn't
//! exist yet (a later PR) — every test here creates an **unindexed** table,
//! populates it over the real DynamoDB wire, then hand-drives
//! `MetaCommand::CreateTableIndex{status: Creating}` via
//! `ClientRequest::ProposeSchema`, exactly like `tests/index_backfill.rs`
//! does for its own aggregator-only scenarios. Every eventual property here
//! is a converged-or-timeout poll, never a fixed sleep followed by one
//! assertion (a GSI is eventually consistent by contract even without a
//! backfill in the picture).

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_control::{IndexDef, IndexKind, IndexProjection, IndexStatus};
use animus_dynamo::AttributeValue;
use animusd::{ClientRequest, ClientResponse, MetaCommand, Node, read_frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// Bring up an `n`-node per-process combined cluster — duplicated from
/// `tests/index_backfill.rs` rather than shared, per this codebase's own
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

async fn delete_item(addr: SocketAddr, table: &str, id: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DeleteItem",
        &format!(r#"{{"TableName":"{table}","Key":{{"id":{{"S":"{id}"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200, "DeleteItem({id}) failed: {body}");
}

/// A `Creating` GSI definition hashing on `hash_attribute`.
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

fn index_status(nodes: &[Node], table: &str, index: &str) -> Option<IndexStatus> {
    nodes[0]
        .metadata()
        .table_indexes(table)
        .iter()
        .find(|i| i.name == index)
        .map(|i| i.status)
}

async fn await_index_status(
    nodes: &[Node],
    table: &str,
    index: &str,
    want: IndexStatus,
    secs: u64,
) {
    timeout(Duration::from_secs(secs), async {
        loop {
            if nodes.iter().all(|n| {
                n.metadata()
                    .table_indexes(table)
                    .iter()
                    .any(|i| i.name == index && i.status == want)
            }) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "index {table}/{index} did not reach {want:?} within {secs}s (last seen: {:?})",
            index_status(nodes, table, index)
        )
    });
}

/// How many live rows a table holds, via a whole-table client-protocol scan
/// (duplicated from `tests/dynamo_gsi_drain.rs`'s own helper of the same
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
    if timeout(CONVERGE_BUDGET, converged).await.is_err() {
        let got = *last.lock().unwrap();
        panic!("{what}: `{table}` never reached {want} rows (last saw {got:?})");
    }
}

/// Budget for this file's converged-or-timeout polls (`await_row_count` /
/// `await_gsi_query`). Sized runner-aware, like `split_cluster.rs`'s split
/// budgets: `split_during_backfill_converges_with_correct_final_gsi` takes
/// ~25s healthy on idle cores, so its old 60s budget had barely 2x headroom —
/// and on the oversubscribed 2-core CI runners, election churn ("CP group
/// leader moved; retry") repeatedly ate all of it (four gates trips on
/// 2026-08-18 alone, across unrelated PRs). Passing runs exit the poll on
/// convergence, so raising this costs nothing when healthy.
const CONVERGE_BUDGET: Duration = Duration::from_secs(180);

/// Poll a GSI `Query` until `accept` is satisfied (a GSI is eventually
/// consistent by contract).
async fn await_gsi_query(addr: SocketAddr, body: &str, accept: impl Fn(&str) -> bool) {
    let last = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let seen = std::sync::Arc::clone(&last);
    let converged = async move {
        loop {
            let (status, got) = dynamo(addr, "DynamoDB_20120810.Query", body).await;
            if status == 200 && accept(&got) {
                return;
            }
            *seen.lock().unwrap() = got;
            sleep(Duration::from_millis(100)).await;
        }
    };
    if timeout(CONVERGE_BUDGET, converged).await.is_err() {
        panic!(
            "GSI query never converged within {CONVERGE_BUDGET:?} (last saw: {})",
            last.lock().unwrap()
        );
    }
}

async fn await_gsi_hit(
    addr: SocketAddr,
    table: &str,
    index: &str,
    hash: &str,
    value: &str,
    id: &str,
) {
    await_gsi_query(
        addr,
        &format!(
            r#"{{"TableName":"{table}","IndexName":"{index}",
                "KeyConditionExpression":"{hash} = :v",
                "ExpressionAttributeValues":{{":v":{{"S":"{value}"}}}}}}"#
        ),
        |b| b.contains("\"Count\":1") && b.contains(&format!(r#""id":{{"S":"{id}"}}"#)),
    )
    .await;
}

async fn await_gsi_miss(addr: SocketAddr, table: &str, index: &str, hash: &str, value: &str) {
    await_gsi_query(
        addr,
        &format!(
            r#"{{"TableName":"{table}","IndexName":"{index}",
                "KeyConditionExpression":"{hash} = :v",
                "ExpressionAttributeValues":{{":v":{{"S":"{value}"}}}}}}"#
        ),
        |b| b.contains("\"Count\":0"),
    )
    .await;
}

/// The plan's own PR3 acceptance test: pre-populate a table with items across
/// several partitions, hand-drive `CreateTableIndex{status: Creating}`, and
/// assert converged-or-timeout that (a) the GSI's hidden table materializes
/// every pre-existing row and (b) the index flips `Active` via the PR2
/// aggregator — proving the seeder, not just the aggregator in isolation,
/// since nothing in this test ever proposes `MarkIndexBackfilled` by hand.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn backfill_seeder_materializes_every_pre_existing_row_then_flips_active() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(3, dir.path()).await;
    let leader = nodes.iter().position(Node::is_control_leader).unwrap();
    // ADR 0047: `ProposeSchema` is intra-only (intra also serves the
    // occasional `SplitTablet` call in this file — a superset, not a
    // conflict).
    let client = config.nodes[leader].intra;
    let dynamo_addr = nodes[0].dynamo_addr();
    let client_addr = nodes[0].client_addr();
    let table = "bf_seed";
    let index_table = "bf_seed$by-email";

    create_table_no_index(dynamo_addr, table).await;
    let ids: Vec<String> = (0..12).map(|i| format!("p{i}")).collect();
    for id in &ids {
        put_item(dynamo_addr, table, id, "email", &format!("{id}@x")).await;
    }

    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTableIndex {
            table: table.into(),
            index: creating_index("by-email", "email"),
        }),
    )
    .await;

    await_index_status(&nodes, table, "by-email", IndexStatus::Active, 60).await;
    await_row_count(
        client_addr,
        index_table,
        ids.len(),
        "after backfill converges",
    )
    .await;
    for id in &ids {
        await_gsi_hit(
            dynamo_addr,
            table,
            "by-email",
            "email",
            &format!("{id}@x"),
            id,
        )
        .await;
    }

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

/// Live writes racing the backfill sweep: new inserts, an update that moves
/// an existing item's indexed attribute, and a delete, all issued while the
/// index is still `Creating`. The load-bearing property (ADR 0045 §2's own
/// "no record is lost or double-applied" argument) is that the *final*
/// materialized GSI matches the *final* base-table state regardless of how
/// the seeder's sweep and these writes actually interleaved — every live
/// write already leaves a genuine change-log record unconditional on the
/// index's status, so there is nothing for the seeder to race incorrectly
/// against.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn live_writes_during_backfill_converge_to_the_correct_final_gsi() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(3, dir.path()).await;
    let leader = nodes.iter().position(Node::is_control_leader).unwrap();
    // ADR 0047: `ProposeSchema` is intra-only (intra also serves the
    // occasional `SplitTablet` call in this file — a superset, not a
    // conflict).
    let client = config.nodes[leader].intra;
    let dynamo_addr = nodes[0].dynamo_addr();
    let client_addr = nodes[0].client_addr();
    let table = "bf_live";
    let index_table = "bf_live$by-g";

    create_table_no_index(dynamo_addr, table).await;
    let pre_existing: Vec<String> = (0..20).map(|i| format!("p{i}")).collect();
    for id in &pre_existing {
        put_item(dynamo_addr, table, id, "g", &format!("g-{id}")).await;
    }

    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTableIndex {
            table: table.into(),
            index: creating_index("by-g", "g"),
        }),
    )
    .await;

    // Concurrent with the backfill sweep, not sequenced against it: five new
    // items, one moved attribute (p0), one deletion (p1).
    let race_addr = dynamo_addr;
    let race = tokio::spawn(async move {
        for i in 0..5 {
            put_item(race_addr, table, &format!("n{i}"), "g", &format!("g-n{i}")).await;
        }
        put_item(race_addr, table, "p0", "g", "g-p0-moved").await;
        delete_item(race_addr, table, "p1").await;
    });
    race.await.expect("concurrent writer task panicked");

    await_index_status(&nodes, table, "by-g", IndexStatus::Active, 60).await;
    // 20 pre-existing - 1 deleted (p1) + 5 new = 24; p0 still counts once, at
    // its moved key.
    await_row_count(
        client_addr,
        index_table,
        24,
        "after backfill + concurrent writes",
    )
    .await;

    for i in 0..5 {
        await_gsi_hit(
            dynamo_addr,
            table,
            "by-g",
            "g",
            &format!("g-n{i}"),
            &format!("n{i}"),
        )
        .await;
    }
    await_gsi_hit(dynamo_addr, table, "by-g", "g", "g-p0-moved", "p0").await;
    await_gsi_miss(dynamo_addr, table, "by-g", "g", "g-p0").await; // the old key is gone
    await_gsi_miss(dynamo_addr, table, "by-g", "g", "g-p1").await; // deleted
    // An untouched pre-existing item is unaffected.
    await_gsi_hit(dynamo_addr, table, "by-g", "g", "g-p10", "p10").await;

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

/// Two indexes `Creating` on the same table at once: each runs its own
/// independent backfill cursor (ADR 0045 §2's "per-index cursor" choice —
/// see `index_drain.rs`'s module doc) and converges to `Active` with
/// correct, independent content.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn two_indexes_creating_simultaneously_converge_independently() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(3, dir.path()).await;
    let leader = nodes.iter().position(Node::is_control_leader).unwrap();
    // ADR 0047: `ProposeSchema` is intra-only (intra also serves the
    // occasional `SplitTablet` call in this file — a superset, not a
    // conflict).
    let client = config.nodes[leader].intra;
    let dynamo_addr = nodes[0].dynamo_addr();
    let client_addr = nodes[0].client_addr();
    let table = "bf_multi";
    let idx1_table = "bf_multi$by-g1";
    let idx2_table = "bf_multi$by-g2";

    create_table_no_index(dynamo_addr, table).await;
    let ids: Vec<String> = (0..10).map(|i| format!("m{i}")).collect();
    for id in &ids {
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},
                    "g1":{{"S":"g1-{id}"}},"g2":{{"S":"g2-{id}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem({id}) failed: {body}");
    }

    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTableIndex {
            table: table.into(),
            index: creating_index("by-g1", "g1"),
        }),
    )
    .await;
    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTableIndex {
            table: table.into(),
            index: creating_index("by-g2", "g2"),
        }),
    )
    .await;

    await_index_status(&nodes, table, "by-g1", IndexStatus::Active, 60).await;
    await_index_status(&nodes, table, "by-g2", IndexStatus::Active, 60).await;
    await_row_count(
        client_addr,
        idx1_table,
        ids.len(),
        "by-g1 after convergence",
    )
    .await;
    await_row_count(
        client_addr,
        idx2_table,
        ids.len(),
        "by-g2 after convergence",
    )
    .await;
    for id in &ids {
        await_gsi_hit(dynamo_addr, table, "by-g1", "g1", &format!("g1-{id}"), id).await;
        await_gsi_hit(dynamo_addr, table, "by-g2", "g2", &format!("g2-{id}"), id).await;
    }

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

/// Cursor resumability, the cheapest available restart pattern in this
/// codebase's own test style (mirroring `index_drain.rs`'s in-crate
/// `crash_mid_reconcile_recovers_without_skipping_or_corrupting_the_gsi`):
/// a real process crash + restart shortly after the backfill starts must
/// still converge to the complete, correct GSI, because the backfill
/// cursor is a durable `KIND_CURSOR` row a fresh leader (here: a fresh
/// process on the same tablet) resumes from rather than any in-memory
/// state. A genuinely *fault-injected* version of this (interleaved with a
/// concurrent split, seed-reproducible at depth) is named as PR4's own
/// corpus scope, not duplicated here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_and_restart_mid_backfill_still_converges() {
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("node-0");
    let config = animusd::ClusterConfig {
        nodes: vec![{
            let addrs = support::free_addrs(6);
            animusd::RoleAddrs {
                id: animusd::config::node_id(0),
                role: animusd::config::NodeRole::Both,
                internal: addrs[0],
                client: addrs[1],
                dynamo: addrs[2],
                admin: addrs[3],
                intra: addrs[4],
                console: addrs[5],
            }
        }],
    };
    let node = animusd::run_node(&config, 0, &node_dir)
        .await
        .expect("bring up");
    timeout(Duration::from_secs(10), async {
        loop {
            if node.is_control_leader() {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("did not become control leader in time");

    let table = "bf_restart";
    let index_table = "bf_restart$by-email";
    create_table_no_index(node.dynamo_addr(), table).await;
    let ids: Vec<String> = (0..15).map(|i| format!("r{i}")).collect();
    for id in &ids {
        put_item(node.dynamo_addr(), table, id, "email", &format!("{id}@x")).await;
    }

    call(
        // ADR 0047: `ProposeSchema` is intra-only.
        node.intra_addr(),
        ClientRequest::ProposeSchema(MetaCommand::CreateTableIndex {
            table: table.into(),
            index: creating_index("by-email", "email"),
        }),
    )
    .await;
    sleep(Duration::from_millis(20)).await;
    node.shutdown_graceful().await;

    let node2 = animusd::run_node(&config, 0, &node_dir)
        .await
        .expect("restart on the same dir");
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

    timeout(Duration::from_secs(60), async {
        loop {
            if node2
                .metadata()
                .table_indexes(table)
                .iter()
                .any(|i| i.name == "by-email" && i.status == IndexStatus::Active)
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("index did not reach Active within 60s after restart");
    await_row_count(
        node2.client_addr(),
        index_table,
        ids.len(),
        "after restart recovery",
    )
    .await;
    for id in &ids {
        await_gsi_hit(
            node2.dynamo_addr(),
            table,
            "by-email",
            "email",
            &format!("{id}@x"),
            id,
        )
        .await;
    }

    node2.shutdown_graceful().await;
}

/// The data-plane key `dynamo.rs::item_key` computes for a simple
/// (partition-key-only) item — duplicated per this file's own "every
/// sibling test keeps its own copy" convention (mirrors `dynamo_txn.rs`'s
/// identical helper), needed here to predict which side of a chosen split
/// point a given item id lands on *before* creating it — there is no other
/// way to predict a DynamoDB item's tablet placement from outside the edge.
fn item_key(pk: &str) -> Vec<u8> {
    let av = AttributeValue::S(pk.to_owned());
    let escaped = animus_dynamo::storage_key(&av, None);
    let token = animus_tablet::partition_token(&escaped);
    let mut key = token.to_vec();
    key.extend_from_slice(&escaped);
    key
}

/// The split-during-backfill scenario named as PR4's own deterministic
/// acceptance test (ADR 0045 §3 Fork A): pre-populate a table with enough
/// distinct partitions that a single backfill-seeder tick provably *cannot*
/// finish sweeping it (`BACKFILL_SEED_BATCH == 256`, a production constant —
/// 300 single-partition rows guarantee at least two ticks), hand-drive
/// `CreateTableIndex{status: Creating}`, then split the table's *only*
/// tablet — via the real `ClientRequest::SplitTablet` admin path, not a
/// hand-driven `MetaCommand` — into a left and a right child straddling a
/// known, predicted set of pre-existing rows.
///
/// This proves Fork A's claim (a post-split right child restarts its own
/// narrower sweep from scratch, unconditionally correct by the drain's own
/// idempotence — see `index_drain.rs`'s module doc) against the **real**
/// production seeder + drain, not a reimplementation: the final materialized
/// GSI must be exactly correct across both halves regardless of how much of
/// the parent's sweep had already landed before the split committed.
///
/// **On "flips Active only after both children report"**: proving that
/// precise timing property against real wall-clock ticks here would be
/// inherently racy (this table is deliberately small enough to converge in
/// well under a `INDEX_DRAIN_INTERVAL` tick on either child alone). That
/// exact property is already proven, non-flakily, by
/// `tests/index_backfill.rs::
/// a_tablet_that_appears_before_the_flip_blocks_it_until_it_also_reports`
/// (hand-driven `MarkIndexBackfilled`, no real seeder). Combined with this
/// test's proof that the real seeder actually *does* independently drive
/// each child to report — the only way this scenario converges to `Active`
/// at all — the two tests together are a full proof: the aggregator can't
/// flip early without both reporting, and both genuinely do report.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn split_during_backfill_converges_with_correct_final_gsi() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(3, dir.path()).await;
    let leader = nodes.iter().position(Node::is_control_leader).unwrap();
    // ADR 0047: `ProposeSchema` is intra-only (intra also serves the
    // occasional `SplitTablet` call in this file — a superset, not a
    // conflict).
    let client = config.nodes[leader].intra;
    let dynamo_addr = nodes[0].dynamo_addr();
    let client_addr = nodes[0].client_addr();
    let table = "bf_split";
    let index_table = "bf_split$by-email";

    create_table_no_index(dynamo_addr, table).await;

    // 300 candidates, sorted by their actual data-plane key (never by id
    // string), so a split point chosen between two adjacent candidates is
    // known to divide them cleanly — same technique as
    // `dynamo_txn.rs::create_table_pre_split`. 300 > `BACKFILL_SEED_BATCH`
    // (256), so the parent's very first backfill tick provably cannot
    // finish sweeping this table in one pass.
    let mut candidates: Vec<(String, Vec<u8>)> = (0..300)
        .map(|i| {
            let id = format!("s{i:04}");
            let key = item_key(&id);
            (id, key)
        })
        .collect();
    candidates.sort_by(|a, b| a.1.cmp(&b.1));
    let mid = candidates.len() / 2;
    let split_key = candidates[mid].1.clone();
    let ids: Vec<String> = candidates.iter().map(|(id, _)| id.clone()).collect();

    // Populated via `BatchWriteItem` in chunks (one Raft entry per chunk),
    // not 300 individual `PutItem` round trips: this table is still
    // unindexed at population time, so it rides the fast `cp_batch_write`
    // path (`animusd`'s own CLAUDE.md) — far gentler on WAL fsync
    // throughput than 300 independent commits, which was found to starve
    // this environment's disk I/O under concurrent load (three replicas'
    // WAL group-commits) and produce spurious `Backend(..)` panics
    // unrelated to backfill/split logic.
    for chunk in ids.chunks(100) {
        let puts: Vec<String> = chunk
            .iter()
            .map(|id| {
                format!(r#"{{"PutRequest":{{"Item":{{"id":{{"S":"{id}"}},"email":{{"S":"{id}@x"}}}}}}}}"#)
            })
            .collect();
        let body = format!(r#"{{"RequestItems":{{"{table}":[{}]}}}}"#, puts.join(","));
        let (status, resp) = dynamo(dynamo_addr, "DynamoDB_20120810.BatchWriteItem", &body).await;
        assert_eq!(status, 200, "BatchWriteItem failed: {resp}");
    }

    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTableIndex {
            table: table.into(),
            index: creating_index("by-email", "email"),
        }),
    )
    .await;

    // Split immediately — the bootstrap tablet is always id 1.
    let resp = call(
        client,
        ClientRequest::SplitTablet {
            tablet: 1,
            split_key,
        },
    )
    .await;
    assert!(
        matches!(resp, ClientResponse::PutOk),
        "split trigger rejected: {resp:?}"
    );
    // The copy-based workflow (ADR 0050 rung 5) runs build → freeze →
    // backfill-veto → cutover on its own; with an index still `Creating`
    // the cutover deliberately WAITS for the parent's seeder to finish
    // (the rung-5 backfill veto this test now exercises end to end), so
    // the budget is generous. Done = the parent (1) has left the map and
    // two Active children of the base table cover it (the GSI's hidden
    // table may add its own tablet at any point — count only the base
    // table's).
    timeout(Duration::from_secs(90), async {
        loop {
            let done = nodes.iter().all(|n| {
                let meta = n.metadata();
                !meta.tablets.contains_key(&animus_tablet::TabletId(1))
                    && meta
                        .tablets
                        .values()
                        .filter(|t| t.table.as_deref() == Some(table) && t.is_routable())
                        .count()
                        == 2
            });
            if done {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the split workflow did not cut over (backfill veto never released?)");

    await_index_status(&nodes, table, "by-email", IndexStatus::Active, 60).await;
    await_row_count(
        client_addr,
        index_table,
        ids.len(),
        "after split-during-backfill converges",
    )
    .await;
    for id in &ids {
        await_gsi_hit(
            dynamo_addr,
            table,
            "by-email",
            "email",
            &format!("{id}@x"),
            id,
        )
        .await;
    }

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}
