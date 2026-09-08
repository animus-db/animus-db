//! The DynamoDB Streams **segment janitor** end to end (ADR 0043 §A9, round-3
//! PR7): retention two-phase reclaim and replica repair. Real `ProdEnv`
//! time/sockets throughout — every eventual property is a converged-or-
//! timeout poll, never a fixed sleep.
//!
//! **Trimmed (ADR 0061 rung G, C-07 PR 5)**: every scenario this fixture
//! can express without a genuinely dead replica (a fault this deterministic
//! fixture's own shared `S3`-backed segment store cannot model — see
//! `crates/animusd/src/sim_cluster_stream_janitor.rs`'s own module doc) or a
//! genuine control-only/data-only process split moved to
//! `crates/animusd/src/sim_cluster_stream_janitor.rs`
//! (`cargo test -p animusd --lib sim_cluster_stream_janitor`) — deterministic,
//! seed-replayable, no real sockets/disk/clock. See that module's own doc
//! and `crates/animusd/CLAUDE.md`'s matching C-07 PR 5 appendix for the
//! full before/after gate accounting and the test-by-test mapping. The two
//! tests kept here, and why, each carry their own one-line reason below.

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

/// A tiny retention window — never the 24h production default (this
/// codebase's own testing discipline, see `StreamSealKnobs::default`'s
/// precedent). 2s, not smaller: this file's remaining test needs to seal
/// **two** epochs in sequence (write → await seal → write → await seal)
/// before retention may even begin reclaiming the first one (a tablet's own
/// current *last* epoch is never physically removed while it still exists
/// — see `segment_janitor.rs`'s own doc) — the window must comfortably
/// outlast that whole setup sequence's own real (if small) latency.
const TINY_RETENTION: Duration = Duration::from_secs(2);

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

/// Like [`await_true`], but `check` is itself async (a real I/O poll — a
/// filesystem existence check or an HTTP `/metrics` fetch) rather than an
/// in-memory comparison.
async fn await_true_async<F, Fut>(secs: u64, msg: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if check().await {
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

// ---------------------------------------------------------------------------
// Control-only leadership (W-10)
// ---------------------------------------------------------------------------

/// A real 3-control-only + 2-data-only split deployment, no combined-mode
/// node anywhere — with tiny stream-seal/retention knobs (this codebase's
/// own testing discipline). Mirrors `tests/support::bring_up_split`'s
/// bring-up shape (including its port-TOCTOU retry discipline) plus this
/// file's own `start_streamed_cluster_with_store`'s tiny-knobs threading,
/// neither of which alone covers this combination — `bring_up_split`
/// always uses production stream knobs (`run_node_control`/
/// `run_node_data`'s own defaults), and `start_streamed_cluster_with_store`
/// only ever brings up combined-mode nodes (`bind_cluster`). Returns the
/// data nodes' own directories alongside the nodes (`segment_path` needs
/// them, and — unlike `bind_cluster`'s deterministic `node-{i}` naming —
/// this bring-up's own retry loop makes the directory name depend on which
/// attempt finally succeeded).
async fn start_split_streamed_cluster(
    control_n: usize,
    data_n: usize,
    dir: &Path,
    retention: Duration,
    seal_knobs: StreamSealKnobs,
) -> (Vec<Node>, Vec<Node>, Vec<PathBuf>, animusd::ClusterConfig) {
    let total = control_n + data_n;
    for attempt in 0..16 {
        let addrs = support::free_addrs(total * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..total)
            .map(|i| {
                let role = if i < control_n {
                    animusd::config::NodeRole::Control
                } else {
                    animusd::config::NodeRole::Data
                };
                animusd::RoleAddrs {
                    id: animusd::config::node_id(i),
                    role,
                    internal: addrs[6 * i],
                    client: addrs[6 * i + 1],
                    dynamo: addrs[6 * i + 2],
                    admin: addrs[6 * i + 3],
                    intra: addrs[6 * i + 4],
                    console: addrs[6 * i + 5],
                    advertise_host: None,
                    tls: None,
                    encryption_key_path: None,
                }
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };

        let mut control_nodes = Vec::new();
        let mut data_nodes = Vec::new();
        let mut data_dirs = Vec::new();
        let mut failed = false;
        for i in 0..control_n {
            match animusd::run_node_control_with_stores(
                &config,
                i,
                dir.join(format!("a{attempt}-c{i}")),
                StorageBackend::default(),
                animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
                SegmentStoreConfig::default(),
                animusd::BackupStoreConfig::default(),
                retention,
            )
            .await
            {
                Ok(n) => control_nodes.push(n),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            for i in control_n..total {
                let node_dir = dir.join(format!("a{attempt}-d{i}"));
                match animusd::run_node_data_with_streams(
                    &config,
                    i,
                    node_dir.clone(),
                    StorageBackend::Memory,
                    seal_knobs,
                    SegmentStoreConfig::default(),
                )
                .await
                {
                    Ok(n) => {
                        data_nodes.push(n);
                        data_dirs.push(node_dir);
                    }
                    Err(_) => {
                        failed = true;
                        break;
                    }
                }
            }
        }
        if !failed {
            return (control_nodes, data_nodes, data_dirs, config);
        }
        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up split streamed cluster after retries (ports kept getting stolen)");
}

/// **Stays `ProdEnv`**: needs a genuine control-only/data-only process split
/// (`SimCluster` has no notion of node role at all — every node is the same
/// shape) — see `crates/animusd/src/sim_cluster_stream_janitor.rs`'s own
/// module doc.
///
/// **The whole point of W-10**: a control-only leader (necessarily one of
/// this test's control-only trio — a data-only node never registers a local
/// control `RaftNode`, so it can never lead) reclaims a sealed stream's
/// segment objects on its own, with no data-role node ever needing to take
/// the lead. Before this fix, `segment_janitor_tick`'s phases 2/3 (object
/// deletion, replica repair) skipped unconditionally on every control-only
/// leader (`ctx.data_opt() == None`, `crate::segment_janitor`'s own
/// pre-fix doc) — a row here would be marked `expired` forever (still
/// correctly invisible to `DescribeStream`, per that module's own
/// documented residual, but never physically reclaimed) rather than
/// converging to fully removed the way this test asserts.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn segment_janitor_reclaims_objects_from_a_genuinely_control_only_leader() {
    let dir = support::panic_safe_tempdir();
    let (control_nodes, data_nodes, data_dirs, _config) =
        start_split_streamed_cluster(3, 2, dir.path(), TINY_RETENTION, tiny_seal_knobs()).await;

    support::await_leader(&control_nodes).await;
    let data_ids: Vec<animus_env::NodeId> = (0..data_nodes.len())
        .map(|i| animusd::config::node_id(3 + i))
        .collect();
    support::await_data_nodes_active(&control_nodes, &data_ids).await;

    let addr = data_nodes[0].dynamo_addr();
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t",
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    // Two writes, each sealed before the next — see the identical happy-path
    // test's own doc for why: the janitor's "never remove a tablet's own
    // current max epoch" guard means epoch 0 only becomes reclaimable once a
    // LATER epoch exists.
    let all_nodes: Vec<&Node> = control_nodes.iter().chain(data_nodes.iter()).collect();
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_true(
        20,
        "chain length 1 for `t` never reached on every node",
        || {
            all_nodes.iter().all(|n| {
                let meta = n.metadata();
                meta.has_table_schema("t") && chain_len(&meta, tablet_for(&meta, "t")) >= 1
            })
        },
    )
    .await;
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p2"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_true(
        20,
        "chain length 2 for `t` never reached on every node",
        || {
            all_nodes.iter().all(|n| {
                let meta = n.metadata();
                meta.has_table_schema("t") && chain_len(&meta, tablet_for(&meta, "t")) >= 2
            })
        },
    )
    .await;

    let meta0 = control_nodes[0].metadata();
    let (tablet, epoch) = first_sealed(&meta0, "t");
    assert_eq!(epoch, 0, "the first-sealed shard must be epoch 0");
    let row = meta0.stream_shards[&(tablet, epoch)].clone();
    assert_eq!(
        row.replicas.len(),
        data_nodes.len(),
        "K = min(DEFAULT_K, candidates) — only the 2 data-only nodes are \
         placement candidates (a control-only node never claims `Metadata::\
         members`, so it's never chosen as a replica target itself): {row:?}"
    );
    for id in &row.replicas {
        assert!(
            data_ids.contains(id),
            "every recorded replica must be a data-only node, never a \
             control-only one: {row:?}"
        );
    }

    // Confirm the object genuinely landed on every recorded replica's own
    // disk before asserting it's later gone.
    for (i, node_dir) in data_dirs.iter().enumerate() {
        let path = segment_path(node_dir, &row.object_id);
        assert!(
            tokio::fs::metadata(&path).await.is_ok(),
            "data node {i}'s own segment file must exist before expiry: {path:?}"
        );
    }

    // Past retention: the row is removed from every node's own catalog —
    // control AND data alike — driven entirely by whichever control-only
    // node currently leads.
    await_true(
        20,
        "row was never removed from every node's catalog",
        || {
            all_nodes
                .iter()
                .all(|n| !n.metadata().stream_shards.contains_key(&(tablet, epoch)))
        },
    )
    .await;

    // ...and its object is genuinely gone from every replica's own disk —
    // the physical reclaim step (phase 1b) that a control-only leader used
    // to skip entirely.
    for (i, node_dir) in data_dirs.iter().enumerate() {
        let path = segment_path(node_dir, &row.object_id);
        await_true_async(
            10,
            &format!("data node {i}'s segment file was never reclaimed"),
            || async { !tokio::fs::try_exists(&path).await.unwrap_or(true) },
        )
        .await;
    }

    for n in &control_nodes {
        n.shutdown_graceful().await;
    }
    for n in &data_nodes {
        n.shutdown_graceful().await;
    }
}
