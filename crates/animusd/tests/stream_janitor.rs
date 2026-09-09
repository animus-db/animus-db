//! The DynamoDB Streams **segment janitor** end to end (ADR 0043 §A9, round-3
//! PR7): retention two-phase reclaim and replica repair. Real `ProdEnv`
//! time/sockets throughout — every eventual property is a converged-or-
//! timeout poll, never a fixed sleep.
//!
//! **Trimmed (ADR 0061 rung G, C-07 PR 5; trimmed further by rung L, C-12
//! PR 4d)**: every scenario this fixture can express without a genuinely
//! dead replica (a fault this deterministic fixture's own shared
//! `S3`-backed segment store cannot model — see
//! `crates/animusd/src/sim_cluster_stream_janitor.rs`'s own module doc)
//! moved to `crates/animusd/src/sim_cluster_stream_janitor.rs`
//! (`cargo test -p animusd --lib sim_cluster_stream_janitor`) —
//! deterministic, seed-replayable, no real sockets/disk/clock. C-07 PR 5
//! moved nine of the original eleven scenarios; rung L's own C-12 PR 4d
//! moved the tenth — `segment_janitor_reclaims_objects_from_a_genuinely_
//! control_only_leader` — once `SimCluster` gained per-node `NodeRole`
//! (C-12 PR 2/3): a genuine pure control-only/data-only split deployment
//! (no combined-mode node anywhere) is drivable there too now, so the
//! control-only-leader reclaim property no longer needs a real process
//! split to prove. See that module's own doc and `crates/animusd/
//! CLAUDE.md`'s matching C-07 PR 5 and C-12 PR 4d appendices for the full
//! before/after gate accounting and the test-by-test mapping. The **one**
//! test kept here needs a genuinely dead replica node — a fault no
//! `SimCluster` fixture (this one included) can express, since every
//! shared segment store it wraps has no per-node replica concept at all —
//! and carries its own doc below.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use animus_control::Metadata;
use animus_tablet::TabletId;
use animusd::{Node, SegmentStoreConfig, StorageBackend, StreamSealKnobs, bind_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Seals almost immediately on any pending byte — mirrors
/// `dynamo_streams.rs::tiny_seal_knobs`.
fn tiny_seal_knobs() -> StreamSealKnobs {
    StreamSealKnobs {
        seal_bytes: 1,
        seal_age: Duration::from_secs(3600),
    }
}

async fn start_streamed_cluster(n: usize, dir: &Path, retention: Duration) -> Vec<Node> {
    start_streamed_cluster_with_store(n, dir, retention, SegmentStoreConfig::default()).await
}

async fn start_streamed_cluster_with_store(
    n: usize,
    dir: &Path,
    retention: Duration,
    store: SegmentStoreConfig,
) -> Vec<Node> {
    let bound = bind_cluster(n, "127.0.0.1".parse().unwrap(), dir)
        .await
        .unwrap();
    animusd::start_cluster_with_streams(
        bound,
        StorageBackend::default(),
        None,
        Duration::from_secs(600),
        tiny_seal_knobs(),
        store,
        retention,
    )
    .await
    .unwrap()
}

async fn await_bootstrap(nodes: &[Node]) {
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
    .expect("cluster did not bootstrap within 20s");
}

/// Poll a **synchronous** `check` (a plain in-memory comparison — every
/// `Metadata` this file polls is already fetched into an owned value before
/// the check runs) until it holds, or panic after `secs`.
async fn await_true(secs: u64, msg: &str, mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if check() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("{msg} (timed out after {secs}s)");
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// One DynamoDB JSON request over a fresh HTTP/1.1 connection.
async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr)
        .await
        .unwrap_or_else(|e| panic!("connect to dynamo at {addr} failed: {e}"));
    let request = format!(
        "POST / HTTP/1.1\r\n\
         Host: animus\r\n\
         X-Amz-Target: {target}\r\n\
         Content-Type: application/x-amz-json-1.0\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .expect("read full response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    (status, payload.to_string())
}

fn tablet_for(meta: &Metadata, table: &str) -> TabletId {
    meta.tablets_for_table(table)
        .next()
        .map(|(&t, _)| t)
        .unwrap_or_else(|| panic!("table `{table}` has no tablet yet"))
}

fn chain_len(meta: &Metadata, tablet: TabletId) -> usize {
    meta.stream_shards
        .range((tablet, 0)..=(tablet, u64::MAX))
        .count()
}

async fn await_chain_len(nodes: &[Node], table: &str, at_least: usize) {
    await_true(
        20,
        &format!("chain length {at_least} for `{table}` never reached on every node"),
        || {
            nodes.iter().all(|n| {
                let meta = n.metadata();
                meta.has_table_schema(table)
                    && chain_len(&meta, tablet_for(&meta, table)) >= at_least
            })
        },
    )
    .await;
}

/// The first sealed `(tablet, epoch)` row's own key, for `table`'s tablet —
/// panics if none has sealed yet (call after [`await_chain_len`]).
fn first_sealed(meta: &Metadata, table: &str) -> (TabletId, u64) {
    let tablet = tablet_for(meta, table);
    meta.stream_shards
        .range((tablet, 0)..=(tablet, u64::MAX))
        .next()
        .map(|(&k, _)| k)
        .unwrap_or_else(|| panic!("no sealed shard yet for `{table}`"))
}

/// Where `node_dir`'s own local `FsSegmentStore` building block (the default
/// `ClusterSegmentStore`'s per-node store, rooted at `<node dir>/segments`)
/// would keep an object at `object_id` — the ledger-named-object amendment
/// means that's always a catalog row's own `StreamShardRow::object_id`, a
/// unique per-attempt id, never the bare deterministic `segment::segment_id`
/// (which is now only a shared directory prefix several attempts' ids could
/// nest under, not a file path itself).
fn segment_path(node_dir: &Path, object_id: &str) -> PathBuf {
    node_dir.join("segments").join(object_id)
}

// ---------------------------------------------------------------------------
// Replica repair
// ---------------------------------------------------------------------------

/// **Stays `ProdEnv`**: a genuinely dead replica (a node whose own local copy
/// of a segment object is gone for good, repaired onto a fresh target) is a
/// fault `SimCluster`'s shared `S3`-backed segment store cannot express — that
/// store has no per-node replica concept at all (`row.replicas` is always
/// empty for it), unlike the real `Cluster` store this test exercises. See
/// `crates/animusd/src/sim_cluster_stream_janitor.rs`'s own module doc.
///
/// Killing one of a live shard's own recorded replica nodes triggers the
/// janitor's repair sweep: the catalog row's `replicas` converges to a fresh
/// set (the dead node replaced by the cluster's one spare candidate), and
/// `get_from` against the new set genuinely serves the object — never
/// resurrecting an expired row's object (the row here stays unexpired
/// throughout, since retention never elapses in this test).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repair_re_replicates_to_a_fresh_target_after_a_replica_node_dies() {
    let dir = support::panic_safe_tempdir();
    // 4 nodes, K=3 (the default): exactly one spare candidate beyond
    // whichever 3 the placement view chose for this shard.
    let nodes = start_streamed_cluster(4, dir.path(), Duration::from_secs(600)).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"KEYS_ONLY"}}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_chain_len(&nodes, "t", 1).await;

    let meta = nodes[0].metadata();
    let (tablet, epoch) = first_sealed(&meta, "t");
    let original_replicas = meta.stream_shards[&(tablet, epoch)].replicas.clone();
    assert_eq!(original_replicas.len(), 3);

    // Kill one of the shard's own three replicas (by node id, matched
    // against every bound node's own id — never assume index == replica).
    // `bind_cluster` assigns each node `i`'s id as `config::node_id(i)`.
    let all_ids: Vec<_> = (0..nodes.len()).map(animusd::config::node_id).collect();
    let victim_idx = all_ids
        .iter()
        .position(|id| original_replicas.contains(id))
        .expect("one of the replicas is a known node id");
    let victim_id = all_ids[victim_idx].clone();
    nodes[victim_idx].shutdown_graceful().await;

    let survivors: Vec<&Node> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != victim_idx)
        .map(|(_, n)| n)
        .collect();

    // Wait for the failure detector to mark the victim `Down`, and the
    // repair sweep to converge on a full, healthy 3-replica set that no
    // longer names the victim.
    await_true(
        30,
        "the shard's replica set never repaired away from the dead node",
        || {
            survivors.iter().all(|n| {
                n.metadata()
                    .stream_shards
                    .get(&(tablet, epoch))
                    .is_some_and(|r| r.replicas.len() == 3 && !r.replicas.contains(&victim_id))
            })
        },
    )
    .await;

    // The repaired replica set must still be servable end to end.
    let new_replicas = survivors[0]
        .metadata()
        .stream_shards
        .get(&(tablet, epoch))
        .unwrap()
        .replicas
        .clone();
    let object_id = survivors[0].metadata().stream_shards[&(tablet, epoch)]
        .object_id
        .clone();
    for r in &new_replicas {
        let idx = all_ids.iter().position(|id| id == r).unwrap();
        let path = segment_path(&dir.path().join(format!("node-{idx}")), &object_id);
        assert!(
            tokio::fs::metadata(&path).await.is_ok(),
            "repaired replica {r} must actually hold the object locally: {path:?}"
        );
    }
}
