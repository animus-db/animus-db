//! The secondary-index **backfill seeder** end to end (ADR 0045 §2):
//! `change_consumer_loop`'s backfill arm sweeps a pre-existing table's
//! `KIND_BASE` rows and seeds a dirty marker for each partition, so the
//! ordinary GSI drain materializes rows that predate the index's own
//! declaration — and the ADR 0045 §4 aggregator (`tests/index_backfill.rs`)
//! flips the index `Creating` → `Active` once every tablet has swept to its
//! own end.
//!
//! **ADR 0061 rung J, C-10 PR 4**: 4 of this file's original 5 scenarios
//! converted to deterministic, `SimCluster`-driven siblings in
//! `crates/animusd/src/sim_cluster_backfill_seeder.rs` (see that module's
//! own doc for the full mapping). This file is trimmed to the one scenario
//! that stays on `ProdEnv`:
//!
//! **`split_during_backfill_converges_with_correct_final_gsi` is kept here,
//! unconverted, on purpose** — `SimCluster` spawns no
//! `index_drain::change_consumer_loop` at all, so proving this scenario
//! there would mean hand-interleaving three separately-timed on-demand
//! primitives every round (the backfill seeder, the GSI drain, and the
//! in-place split cutover driver) with no way, in the session that
//! attempted this conversion, to verify offline (no `cargo` access) that
//! the resulting sequencing doesn't race the always-on completion
//! aggregator against the cutover propose, or that the post-cutover Fork-A
//! per-child resweep converges within any round budget that was never
//! actually run. See `sim_cluster_backfill_seeder.rs`'s own module doc for
//! the full reasoning — this is the ADR 0061 rung J opener's own licensed
//! fallback for exactly this case.
//!
//! `UpdateTable`'s wire path for adding an index to a populated table
//! didn't exist yet when this scenario was first written — it still
//! hand-drives `MetaCommand::CreateTableIndex{status: Creating}` via
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
use animus_dynamo::wire::BATCH_WRITE_MAX_ITEMS;
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
                advertise_host: None,
                tls: None,
                encryption_key_path: None,
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };
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
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
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
        hash_attribute_type: None,
        sort_attribute_type: None,
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
            stale: false,
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
    let dir = support::panic_safe_tempdir();
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

    // Populated via `BatchWriteItem` in BATCH_WRITE_MAX_ITEMS-sized chunks
    // (one Raft entry per chunk, capped at AWS's own 25-item-per-call
    // limit), not 300 individual `PutItem` round trips: this table is still
    // unindexed at population time, so it rides the fast `cp_batch_write`
    // path (`animusd`'s own CLAUDE.md) — far gentler on WAL fsync
    // throughput than 300 independent commits, which was found to starve
    // this environment's disk I/O under concurrent load (three replicas'
    // WAL group-commits) and produce spurious `Backend(..)` panics
    // unrelated to backfill/split logic.
    for chunk in ids.chunks(BATCH_WRITE_MAX_ITEMS) {
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
    // The in-place split workflow (the only one since the copy-based
    // build → freeze → backfill-veto → cutover workflow was deleted in
    // Layer B1) drives the split on its own; with an index still `Creating`
    // the cutover deliberately WAITS for the parent's seeder to finish (the
    // backfill veto this test exercises end to end, see
    // `animusd/CLAUDE.md`'s in-place cutover driver entry), so the budget
    // is generous. Done = the parent (1) has left the
    // map and two Active children of the base table cover it (the GSI's
    // hidden table may add its own tablet at any point — count only the
    // base table's).
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
