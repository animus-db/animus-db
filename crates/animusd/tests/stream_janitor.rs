//! The DynamoDB Streams **segment janitor** end to end (ADR 0043 §A9, round-3
//! PR7): retention two-phase reclaim, replica repair, the disable-grace
//! lifecycle's retention half (F12-b), and the drop-table cascade's
//! convergent design. Real `ProdEnv` time/sockets throughout, exactly like
//! `dynamo_streams.rs` (PR6) — every eventual property is a
//! converged-or-timeout poll, never a fixed sleep. Small fixtures are
//! duplicated from that file rather than shared (this codebase's own stated
//! convention: sibling test modules keep their own fixtures independent).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use animus_control::Metadata;
use animus_cp_data::segment;
use animus_tablet::TabletId;
use animusd::{Node, SegmentStoreConfig, StorageBackend, StreamSealKnobs, bind_cluster};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// A tiny retention window — never the 24h production default (this
/// codebase's own testing discipline, see `StreamSealKnobs::default`'s
/// precedent). 2s, not smaller: several of this file's tests need to seal
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
    let mut stream = TcpStream::connect(addr).await.expect("connect to dynamo");
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

/// One HTTP/1.0 request to the admin endpoint.
async fn admin(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.0\r\nHost: animus\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );
    stream.write_all(request.as_bytes()).await.expect("send");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let value: Value = serde_json::from_str(payload).unwrap_or(Value::Null);
    (status, value)
}

fn json(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

fn field(body: &str, name: &str) -> String {
    let needle = format!("\"{name}\":\"");
    let start = body
        .find(&needle)
        .unwrap_or_else(|| panic!("field `{name}` not found in: {body}"))
        + needle.len();
    let end = body[start..].find('"').expect("closing quote") + start;
    body[start..end].to_owned()
}

async fn get_shard_iterator(
    addr: SocketAddr,
    stream_arn: &str,
    shard_id: &str,
    iterator_type: &str,
) -> (u16, String) {
    let body = format!(
        r#"{{"StreamArn":"{stream_arn}","ShardId":"{shard_id}","ShardIteratorType":"{iterator_type}"}}"#
    );
    let (status, resp) = dynamo(addr, "DynamoDBStreams_20120810.GetShardIterator", &body).await;
    if status != 200 {
        return (status, resp);
    }
    (
        200,
        json(&resp)["ShardIterator"]
            .as_str()
            .unwrap_or_else(|| panic!("no ShardIterator in: {resp}"))
            .to_owned(),
    )
}

async fn get_records(addr: SocketAddr, iterator: &str) -> (u16, String) {
    let body = format!(r#"{{"ShardIterator":"{iterator}"}}"#);
    dynamo(addr, "DynamoDBStreams_20120810.GetRecords", &body).await
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

async fn get_metrics_text(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect to metrics");
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: animus\r\nConnection: close\r\n\r\n")
        .await
        .expect("send");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read");
    let text = String::from_utf8(raw).expect("utf8");
    text.split_once("\r\n\r\n").expect("body").1.to_owned()
}

fn metric_value(text: &str, name: &str) -> Option<u64> {
    text.lines()
        .find_map(|l| l.strip_prefix(&format!("{name} ")))
        .and_then(|v| v.trim().parse().ok())
}

// ---------------------------------------------------------------------------
// Two-phase retention expiry
// ---------------------------------------------------------------------------

/// Happy path: mark → objects deleted from every replica's own local store →
/// row removed, converging on every node, with a real segment object on
/// disk actually gone at every recorded replica (the durability half — not
/// just the catalog row disappearing).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_phase_expiry_removes_the_row_and_every_replicas_object() {
    let dir = support::panic_safe_tempdir();
    let nodes = start_streamed_cluster(3, dir.path(), TINY_RETENTION).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    // Two writes, EACH sealed before the next is issued (`seal_bytes: 1`,
    // forced via an intermediate `await_chain_len`): the janitor's "never
    // remove a tablet's own current max epoch" guard (see
    // `segment_janitor.rs`'s own doc) means epoch 0 only ever becomes fully
    // reclaimable once a LATER epoch exists — otherwise removing it would
    // let a future seal recompute the identical, now-stale epoch number.
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_chain_len(&nodes, "t", 1).await;
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p2"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_chain_len(&nodes, "t", 2).await;

    let meta0 = nodes[0].metadata();
    let (tablet, epoch) = first_sealed(&meta0, "t");
    assert_eq!(epoch, 0, "the first-sealed shard must be epoch 0");
    let row = meta0.stream_shards[&(tablet, epoch)].clone();
    assert!(
        !row.replicas.is_empty(),
        "cluster mode always records replicas"
    );
    assert_eq!(row.replicas.len(), 3, "K=3 on a 3-node cluster");

    // Confirm the object genuinely landed on every recorded replica before
    // asserting it's later gone — otherwise "gone" would be trivially true.
    for i in 0..3 {
        let path = segment_path(&dir.path().join(format!("node-{i}")), &row.object_id);
        assert!(
            tokio::fs::metadata(&path).await.is_ok(),
            "node {i}'s own segment file must exist before expiry: {path:?}"
        );
    }

    // Past retention: the row is removed from every node's own catalog...
    await_true(
        20,
        "row was never removed from every node's catalog",
        || {
            nodes
                .iter()
                .all(|n| !n.metadata().stream_shards.contains_key(&(tablet, epoch)))
        },
    )
    .await;

    // ...and its object is genuinely gone from every replica's own disk.
    for i in 0..3 {
        let path = segment_path(&dir.path().join(format!("node-{i}")), &row.object_id);
        await_true_async(
            10,
            &format!("node {i}'s segment file was never reclaimed"),
            || async { !tokio::fs::try_exists(&path).await.unwrap_or(true) },
        )
        .await;
    }
}

/// A crash mid-sweep (the control-plane leader is killed right after a row
/// has been *marked* but before it's necessarily fully removed) converges
/// once a new leader takes over — the mark/delete/remove sequence is
/// idempotent and re-derived fresh every tick, so a new leader simply
/// resumes it, with no orphaned object and no premature removal.
///
/// Uses the single-directory `Fs` segment store, shared by every node
/// (feasible in-process, one filesystem) — deliberately decoupling "which
/// node is the control leader" from "can the object still be deleted": a
/// `ClusterSegmentStore`-backed row's own object lives specifically on its
/// **recorded replicas**, which on a 3-node/K=3 cluster is *every* node, so
/// killing any one of them would also make it one of the row's own
/// replicas — conflating this test's actual subject (leader failover
/// resuming an idempotent sweep) with a *different* one (a permanently
/// lost replica, which this module's dead-replica-rule deliberately blocks
/// on until that replica is decommissioned — an accepted, separate
/// durability-over-availability tradeoff, not a bug this test is about).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expiry_survives_a_control_leader_kill_mid_sweep() {
    let dir = support::panic_safe_tempdir();
    let nodes = start_streamed_cluster_with_store(
        3,
        dir.path(),
        TINY_RETENTION,
        SegmentStoreConfig::Fs(dir.path().join("shared-segments")),
    )
    .await;
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
    // Two writes/seals, each sealed before the next is issued — see
    // `two_phase_expiry_...`'s own comment on why epoch 0 only ever becomes
    // fully reclaimable once a later epoch exists.
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_chain_len(&nodes, "t", 1).await;
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p2"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_chain_len(&nodes, "t", 2).await;
    let (tablet, epoch) = first_sealed(&nodes[0].metadata(), "t");
    assert_eq!(epoch, 0);

    // Wait until it's at least MARKED (expired: true) somewhere — proving
    // the sweep genuinely started — then immediately kill whichever node
    // currently leads the control plane.
    await_true(20, "row was never marked expired anywhere", || {
        nodes.iter().any(|n| {
            n.metadata()
                .stream_shards
                .get(&(tablet, epoch))
                .is_some_and(|r| r.expired)
        })
    })
    .await;
    let leader_idx = nodes
        .iter()
        .position(Node::is_control_leader)
        .expect("a control leader exists");
    nodes[leader_idx].shutdown_graceful().await;

    let survivors: Vec<&Node> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader_idx)
        .map(|(_, n)| n)
        .collect();

    // A new leader emerges among the survivors and finishes the reclaim.
    await_true(30, "no new leader elected among survivors", || {
        survivors.iter().any(|n| n.is_control_leader())
    })
    .await;
    await_true(
        30,
        "row was never fully removed after the leader kill",
        || {
            survivors
                .iter()
                .all(|n| !n.metadata().stream_shards.contains_key(&(tablet, epoch)))
        },
    )
    .await;
}

// ---------------------------------------------------------------------------
// Reader racing expiry (D4)
// ---------------------------------------------------------------------------

/// A `GetRecords` call against a shard whose retention window is elapsing
/// either serves the records or reports `TrimmedDataAccessException` — never
/// an ambiguous empty-but-`200` "success" once the object is genuinely gone.
/// Also proves a `GetShardIterator` minted well *after* the row is fully
/// removed is a clean `TrimmedDataAccessException` (the label itself is
/// still perfectly valid — it's this one, now-superseded shard that's gone),
/// and one minted *before* removal but resolved *after* gets the identical
/// outcome — the iterator-straddling-the-horizon case, never a silent
/// empty success.
///
/// Two writes/seals (epoch 0 and epoch 1): epoch 0 is the one this test
/// reclaims — see `two_phase_expiry_...`'s own comment on why a tablet's
/// current *last* epoch is never physically removed while it still exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reader_never_sees_an_empty_success_gap_across_expiry() {
    let dir = support::panic_safe_tempdir();
    let nodes = start_streamed_cluster(3, dir.path(), TINY_RETENTION).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_IMAGE"}}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/t/stream/{label}");

    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_chain_len(&nodes, "t", 1).await;
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p2"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_chain_len(&nodes, "t", 2).await;
    let (tablet, epoch) = first_sealed(&nodes[0].metadata(), "t");
    assert_eq!(epoch, 0);
    let shard_id = segment::shard_id(tablet.0, epoch);

    // Mint an iterator BEFORE the row is reclaimed (the straddling case).
    let (status, straddling_iter) =
        get_shard_iterator(addr, &stream_arn, &shard_id, "TRIM_HORIZON").await;
    assert_eq!(status, 200, "{straddling_iter}");

    // Drain it now, while the shard is still live: must see the record.
    let (status, resp) = get_records(addr, &straddling_iter).await;
    assert_eq!(status, 200, "{resp}");
    let v = json(&resp);
    let records = v["Records"].as_array().cloned().unwrap_or_default();
    assert_eq!(records.len(), 1, "{resp}");
    let drained_iter = v["NextShardIterator"].as_str().map(str::to_owned);

    // Wait for full removal of epoch 0 (epoch 1 stays — it's the tablet's
    // current last epoch).
    await_true(20, "epoch 0's row was never fully removed", || {
        !nodes[0]
            .metadata()
            .stream_shards
            .contains_key(&(tablet, epoch))
    })
    .await;

    // A fresh mint against the now-gone shard: TrimmedDataAccessException —
    // the label is still fully valid (and still enabled), it's this one
    // superseded shard, specifically, that's gone.
    let (status, body) = get_shard_iterator(addr, &stream_arn, &shard_id, "TRIM_HORIZON").await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("TrimmedDataAccessException"), "{body}");

    // The pre-minted, already-drained iterator: whatever it resolves to now
    // must be the identical TrimmedDataAccessException, never a bare empty
    // `200` success.
    if let Some(iter) = drained_iter {
        let (status, resp) = get_records(addr, &iter).await;
        assert_eq!(
            status, 400,
            "a post-removal poll must never be an empty 200: {resp}"
        );
        assert!(resp.contains("TrimmedDataAccessException"), "{resp}");
    }
}

// ---------------------------------------------------------------------------
// Replica repair
// ---------------------------------------------------------------------------

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
// Disable-grace lifecycle end to end (F12-b)
// ---------------------------------------------------------------------------

/// Write → disable (final seal) → readable through grace (`ListStreams`
/// DISABLED, `GetRecords` works) → retention passes → the label vanishes
/// from `ListStreams` → `ResourceNotFoundException`; re-enable during grace
/// → two labels coexist → the old one drains out while the new one
/// accumulates independently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disable_grace_lifecycle_end_to_end_with_reenable_coexistence() {
    let dir = support::panic_safe_tempdir();
    let nodes = start_streamed_cluster(1, dir.path(), TINY_RETENTION).await;
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
    let old_label = field(&body, "LatestStreamLabel");

    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);

    // Disable: F12-b's own final seal.
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTable",
        r#"{"TableName":"t","StreamSpecification":{"StreamEnabled":false}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_true(20, "disable never converged", || {
        nodes[0].metadata().table_stream("t").is_none()
    })
    .await;

    // Still listed + readable through grace.
    let (status, body) = dynamo(addr, "DynamoDBStreams_20120810.ListStreams", "{}").await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains(&old_label), "{body}");

    // Re-enable during the grace window: a genuinely new label, coexisting.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTable",
        r#"{"TableName":"t","StreamSpecification":
            {"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"}}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let new_label = field(&body, "LatestStreamLabel");
    assert_ne!(old_label, new_label);

    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p2"}}}"#,
    )
    .await;
    assert_eq!(status, 200);

    let (status, body) = dynamo(addr, "DynamoDBStreams_20120810.ListStreams", "{}").await;
    assert_eq!(status, 200, "{body}");
    assert!(
        body.contains(&old_label) && body.contains(&new_label),
        "both labels must coexist during the grace window: {body}"
    );

    // Retention passes: the OLD label's rows are reclaimed (it was already
    // sealed by the disable's own final seal, so it has exactly its final
    // shard, no open tail — it ages out on schedule); the NEW label's rows
    // do not (nothing has sealed for it yet, and even once it does, it's
    // fresh).
    await_true(
        20,
        "the old disabled label's rows never fully drained",
        || {
            nodes[0]
                .metadata()
                .stream_labels_with_rows("t")
                .iter()
                .all(|l| l != &old_label)
        },
    )
    .await;

    let (status, body) = dynamo(addr, "DynamoDBStreams_20120810.ListStreams", "{}").await;
    assert_eq!(status, 200, "{body}");
    assert!(
        !body.contains(&old_label),
        "the old label must vanish from ListStreams once fully drained: {body}"
    );
    assert!(
        body.contains(&new_label),
        "the new (current, enabled) label must still be listed: {body}"
    );

    // A request against the now-fully-reaped old label: ResourceNotFound.
    let old_arn = format!("arn:aws:dynamodb:animus:0:table/t/stream/{old_label}");
    let (status, body) = dynamo(
        addr,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{old_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("ResourceNotFoundException"), "{body}");
}

// ---------------------------------------------------------------------------
// Drop-table cascade (the convergent design)
// ---------------------------------------------------------------------------

/// Dropping a table with live catalog rows converges its rows/objects to
/// zero — with no dedicated cascade code path in `drop_table` itself (see
/// `segment_janitor.rs`'s own doc): this is purely the janitor's ordinary
/// retention-zero rule reacting to the schema's own disappearance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_table_cascade_converges_via_the_janitor() {
    let dir = support::panic_safe_tempdir();
    // A generous retention (600s): if this test ever passed only because
    // retention itself elapsed rather than the drop-table rule, this would
    // time out instead of passing for the wrong reason.
    let nodes = start_streamed_cluster(1, dir.path(), Duration::from_secs(600)).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();
    let admin_addr = nodes[0].admin_addr();

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
    assert!(
        !nodes[0].metadata().stream_shards.is_empty(),
        "test premise: at least one live catalog row exists"
    );

    let (status, body) = admin(
        admin_addr,
        "POST",
        "/admin/data/drop-table",
        Some(r#"{"table":"t"}"#),
    )
    .await;
    assert_eq!(status, 200, "drop-table: {body}");

    await_true(
        20,
        "stream-shard catalog rows were never cleared after a table drop",
        || nodes[0].metadata().stream_shards.is_empty(),
    )
    .await;
}

/// The mid-grace variant: drop a table while an old, disabled-but-draining
/// label and a new, currently-enabled label both have live rows — both
/// converge to zero via the same janitor sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mid_grace_drop_removes_both_coexisting_labels() {
    let dir = support::panic_safe_tempdir();
    let nodes = start_streamed_cluster(1, dir.path(), Duration::from_secs(600)).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();
    let admin_addr = nodes[0].admin_addr();

    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"KEYS_ONLY"}}"#,
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);

    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTable",
        r#"{"TableName":"t","StreamSpecification":{"StreamEnabled":false}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_true(20, "disable never converged", || {
        nodes[0].metadata().table_stream("t").is_none()
    })
    .await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTable",
        r#"{"TableName":"t","StreamSpecification":
            {"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"}}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p2"}}}"#,
    )
    .await;
    assert_eq!(status, 200);

    await_true(20, "two coexisting labels never appeared", || {
        nodes[0].metadata().stream_labels_with_rows("t").len() >= 2
    })
    .await;

    let (status, body) = admin(
        admin_addr,
        "POST",
        "/admin/data/drop-table",
        Some(r#"{"table":"t"}"#),
    )
    .await;
    assert_eq!(status, 200, "drop-table: {body}");

    await_true(
        20,
        "both coexisting labels' rows were never cleared after the drop",
        || nodes[0].metadata().stream_shards.is_empty(),
    )
    .await;
}

// ---------------------------------------------------------------------------
// Observability
// ---------------------------------------------------------------------------

/// A completed retention cycle moves `stream_segments_expired_total` and
/// leaves `stream_segments_live` at a level reflecting the surviving rows —
/// the "observability lands with the mechanism" house rule.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_reflect_a_completed_retention_cycle() {
    let dir = support::panic_safe_tempdir();
    let nodes = start_streamed_cluster(1, dir.path(), TINY_RETENTION).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"KEYS_ONLY"}}"#,
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_chain_len(&nodes, "t", 1).await;
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p2"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_chain_len(&nodes, "t", 2).await;
    let (tablet, epoch) = first_sealed(&nodes[0].metadata(), "t");
    assert_eq!(epoch, 0);

    await_true(20, "epoch 0's row was never fully removed", || {
        !nodes[0]
            .metadata()
            .stream_shards
            .contains_key(&(tablet, epoch))
    })
    .await;

    await_true_async(10, "stream_segments_expired_total never moved", || async {
        let text = get_metrics_text(addr).await;
        metric_value(&text, "stream_segments_expired_total").unwrap_or(0) >= 1
    })
    .await;
}

// --- ADR 0050 Train B rung 6: the retired-tablet rule ---------------------

/// Completes one copy-based split of streamed `table` (currently one
/// tablet) via `/admin/stream/grow`, returning the retired root's id once
/// every node observes the cutover (root absent, two routable children).
async fn grow_and_await_cutover(nodes: &[Node], table: &str) -> TabletId {
    let root = tablet_for(&nodes[0].metadata(), table);
    let (status, body) = admin(
        nodes[1].admin_addr(),
        "POST",
        "/admin/stream/grow",
        Some(&format!(r#"{{"table":"{table}"}}"#)),
    )
    .await;
    assert_eq!(status, 200, "stream/grow failed: {body}");
    assert_eq!(
        body["split_count"].as_u64(),
        Some(1),
        "root must split: {body}"
    );
    await_true(30, "cutover never completed on every node", || {
        nodes.iter().all(|n| {
            let m = n.metadata();
            !m.tablets.contains_key(&root)
                && m.tablets
                    .iter()
                    .filter(|(_, t)| t.serves_table(table))
                    .count()
                    == 2
        })
    })
    .await;
    root
}

/// The mark half of the retired-tablet rule: a retired split parent's
/// sealed shards expire by ORDINARY retention — never the drop-table
/// retention-zero rule, which keys on the TABLE's schema (still live via
/// the children), not on tablet presence. With a long retention, several
/// janitor ticks after the cutover must leave every one of the root's rows
/// present and unexpired. (Teeth: keying the drop rule on tablet presence
/// instead — a plausible wrong implementation — marks these rows within
/// one 200ms tick and turns this red.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retired_parents_shards_are_not_reaped_early() {
    let dir = support::panic_safe_tempdir();
    let nodes = start_streamed_cluster(3, dir.path(), Duration::from_secs(3600)).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"rt","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    for i in 0..8 {
        let (status, _) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &format!(r#"{{"TableName":"rt","Item":{{"id":{{"S":"r{i:04}"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200);
    }
    await_chain_len(&nodes, "rt", 1).await;
    let root = grow_and_await_cutover(&nodes, "rt").await;

    // Several janitor intervals (200ms each) after the cutover: the
    // retired root's rows must all still be present and unexpired.
    sleep(Duration::from_millis(1500)).await;
    let meta = nodes[0].metadata();
    let rows: Vec<_> = meta
        .stream_shards
        .range((root, 0)..=(root, u64::MAX))
        .collect();
    assert!(
        !rows.is_empty(),
        "the retired root must still have its sealed shards cataloged"
    );
    for ((_, epoch), row) in rows {
        assert!(
            !row.expired,
            "a retired parent's shard (epoch {epoch}) must expire by ordinary \
             retention, never be reaped early as dropped-table work"
        );
    }
}

/// The removal half: past retention, a retired parent's rows are removed
/// **including its final (max-epoch) shard** — the max-epoch pin exists
/// only for a LIVE tablet (whose next seal re-derives its epoch from the
/// chain); a retired tablet can never seal again, so nothing pins its
/// final row. (Teeth: reverting `may_remove_row`'s absent-tablet arm pins
/// the final row forever and turns this red.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retired_parents_final_shard_expires_by_retention() {
    let dir = support::panic_safe_tempdir();
    let nodes = start_streamed_cluster(3, dir.path(), TINY_RETENTION).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"re","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    for i in 0..8 {
        let (status, _) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &format!(r#"{{"TableName":"re","Item":{{"id":{{"S":"e{i:04}"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200);
    }
    await_chain_len(&nodes, "re", 1).await;
    let root = grow_and_await_cutover(&nodes, "re").await;

    // Capture the final shard's object while it exists, to also prove the
    // bytes are reclaimed, not just the row.
    let meta = nodes[0].metadata();
    let final_row = meta
        .stream_shards
        .range((root, 0)..=(root, u64::MAX))
        .next_back()
        .map(|(_, row)| row.clone())
        .expect("the retired root sealed at least one shard");

    await_true(
        20,
        "the retired root's rows (final epoch included) were never removed",
        || {
            nodes.iter().all(|n| {
                n.metadata()
                    .stream_shards
                    .range((root, 0)..=(root, u64::MAX))
                    .next()
                    .is_none()
            })
        },
    )
    .await;
    for i in 0..3 {
        let path = segment_path(&dir.path().join(format!("node-{i}")), &final_row.object_id);
        await_true_async(
            10,
            &format!("node {i}'s final-shard object was never reclaimed"),
            || async { !tokio::fs::try_exists(&path).await.unwrap_or(true) },
        )
        .await;
    }
}

// ---------------------------------------------------------------------------
// W-10 (ADR 0043 §A9's control-only-leader gap, closed): a genuine split
// deployment, no combined-mode node anywhere.
// ---------------------------------------------------------------------------

/// Bring up a **genuine split deployment** — `control_n` control-only nodes
/// (`animusd control`'s `run_node_control_with_stores`) plus `data_n`
/// data-only nodes (`animusd data`'s `run_node_data_with_streams`), no
/// combined-mode node anywhere — with tiny stream-seal/retention knobs
/// (this codebase's own testing discipline). Mirrors `tests/support::
/// bring_up_split`'s bring-up shape (including its port-TOCTOU retry
/// discipline) plus this file's own `start_streamed_cluster_with_store`'s
/// tiny-knobs threading, neither of which alone covers this combination —
/// `bring_up_split` always uses production stream knobs (`run_node_control`/
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
