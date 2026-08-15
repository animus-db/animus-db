//! DynamoDB Streams **`ProdEnv` end-to-end** (ADR 0042/0043 round-3 PR8,
//! testing-plan deliverables D5/D6/D8): real multi-process-shaped combined
//! clusters, the default `ClusterSegmentStore`, tiny knobs. Small fixtures
//! are duplicated from `dynamo_streams.rs`/`stream_janitor.rs` rather than
//! shared (this crate's own stated convention).
//!
//! Covers what those two files don't: an auto-split mid-stream with a live
//! consumer observing the lineage handover through the real HTTP API (also
//! exercising every node of the cluster in turn — the "every node can drive
//! this" regression pattern, D8's own "every-node issuance sweep"); a real
//! LSM-backed restart's stream durability; the `FsSegmentStore` opt-in
//! smoke test; and a GSI+stream table proving the two halves of ADR 0042
//! §8's trim min-rule genuinely coexist (D5).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_control::Metadata;
use animus_cp_data::segment;
use animus_tablet::TabletId;
use animusd::{
    ClientRequest, ClientResponse, Node, SegmentStoreConfig, StorageBackend, StreamSealKnobs,
    bind_cluster, read_frame, start_cluster_with_streams,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

fn tiny_seal_knobs() -> StreamSealKnobs {
    StreamSealKnobs {
        seal_bytes: 1,
        seal_age: Duration::from_secs(3600),
    }
}

/// **Production-shaped** seal knobs (PR1 bugfix regression) — deliberately
/// NOT `tiny_seal_knobs()`'s `seal_bytes: 1` (which seals on every single
/// write, so a tablet can never carry a real unsealed backlog into a
/// split). `seal_bytes` is set high enough to never fire on its own in
/// this cell's small workload; sealing is driven by the **age** trigger
/// instead, so a real backlog of several writes accumulates and ages past
/// `seal_age` before a seal fires — exactly the precondition the frozen
/// `stream_split_basis` fix (ADR 0042 §8/ADR 0043 §A4/§A6) exists for.
fn production_seal_knobs() -> StreamSealKnobs {
    StreamSealKnobs {
        seal_bytes: 1_000_000,
        seal_age: Duration::from_secs(2),
    }
}

async fn start_streamed_cluster(n: usize, dir: &Path, knobs: StreamSealKnobs) -> Vec<Node> {
    start_streamed_cluster_full(n, dir, knobs, None, None, SegmentStoreConfig::default()).await
}

#[allow(clippy::too_many_arguments)]
async fn start_streamed_cluster_full(
    n: usize,
    dir: &Path,
    knobs: StreamSealKnobs,
    auto_split_keys: Option<usize>,
    auto_split_bytes: Option<u64>,
    store: SegmentStoreConfig,
) -> Vec<Node> {
    let bound = bind_cluster(n, "127.0.0.1".parse().unwrap(), dir)
        .await
        .unwrap();
    start_cluster_with_streams(
        bound,
        StorageBackend::default(),
        auto_split_keys,
        auto_split_bytes,
        Duration::from_secs(600),
        knobs,
        store,
        animusd::DEFAULT_STREAM_RETENTION,
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

fn tablets_for(meta: &Metadata, table: &str) -> Vec<TabletId> {
    meta.tablets_for_table(table).map(|(&t, _)| t).collect()
}

/// `PutItem`s `{"id": "o{i:05}", "body": filler}` into table `orders` via
/// `node`, asserting success, and returns the item's own id — the shared
/// write helper for `manual_split_with_unsealed_backlog_under_production_
/// seal_knobs` below.
async fn put_order_item(node: &Node, i: usize, filler: &str) -> String {
    let id = format!("o{i:05}");
    let (status, body) = dynamo(
        node.dynamo_addr(),
        "DynamoDB_20120810.PutItem",
        &format!(
            r#"{{"TableName":"orders","Item":{{"id":{{"S":"{id}"}},"body":{{"S":"{filler}"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "PutItem({id}) failed: {body}");
    id
}

async fn get_shard_iterator(
    addr: SocketAddr,
    stream_arn: &str,
    shard_id: &str,
    iterator_type: &str,
) -> String {
    let body = format!(
        r#"{{"StreamArn":"{stream_arn}","ShardId":"{shard_id}","ShardIteratorType":"{iterator_type}"}}"#
    );
    let (status, resp) = dynamo(addr, "DynamoDBStreams_20120810.GetShardIterator", &body).await;
    assert_eq!(status, 200, "GetShardIterator failed: {resp}");
    json(&resp)["ShardIterator"]
        .as_str()
        .unwrap_or_else(|| panic!("no ShardIterator in: {resp}"))
        .to_owned()
}

async fn get_records(addr: SocketAddr, iterator: &str) -> (Vec<Value>, Option<String>) {
    let body = format!(r#"{{"ShardIterator":"{iterator}"}}"#);
    let (status, resp) = dynamo(addr, "DynamoDBStreams_20120810.GetRecords", &body).await;
    assert_eq!(status, 200, "GetRecords failed: {resp}");
    let v = json(&resp);
    let records = v["Records"].as_array().cloned().unwrap_or_default();
    let next = v["NextShardIterator"].as_str().map(str::to_owned);
    (records, next)
}

async fn describe_stream(addr: SocketAddr, stream_arn: &str) -> String {
    let (status, resp) = dynamo(
        addr,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{stream_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "DescribeStream failed: {resp}");
    resp
}

/// Drains `tablet`'s **whole** lineage of `TABLE`'s stream (every already
/// closed epoch, `TRIM_HORIZON` to null, in ascending order, then the open
/// tail polled until `want` records total have been collected) — the
/// correct shape a consumer under `tiny_seal_knobs` (`seal_bytes: 1`, so a
/// single write often becomes its own epoch) must use: a fixed shard id's
/// `NextShardIterator` nulls the moment *that one epoch* is exhausted, not
/// once the tablet's whole stream is; the caller must advance to the next
/// epoch, not treat a null as "done." Recomputes the tablet's own current
/// chain length from a fresh `Metadata` read on every pass, so it never
/// races ahead of (or falls behind) seals still happening concurrently.
/// **An epoch that closes while its open-tail iterator is still mid-walk is
/// resumed from that same iterator, never re-minted at `TRIM_HORIZON`** —
/// found while building a production-shaped-seal-knobs regression cell:
/// under `tiny_seal_knobs` the open tail is always empty the instant it's
/// polled, so this double-count path was never exercised until a cell left
/// more than one record in it.
async fn drain_tablet_lineage(
    dynamo_addr: SocketAddr,
    stream_arn: &str,
    node: &Node,
    tablet: TabletId,
    want: usize,
    deadline: tokio::time::Instant,
) -> Vec<Value> {
    let mut collected = Vec::new();
    let mut next_epoch = 0u64;
    // The open tail's iterator MUST be resumed from its own last returned
    // position, never re-minted at `TRIM_HORIZON` on every pass — an open
    // shard's `NextShardIterator` never nulls (F4/§7), so re-minting would
    // re-read (and hence double-count) the same records every pass.
    let mut open_epoch: Option<u64> = None;
    let mut open_iterator: Option<String> = None;
    loop {
        let chain_len = node
            .metadata()
            .stream_shards
            .range((tablet, 0)..=(tablet, u64::MAX))
            .count() as u64;
        while next_epoch < chain_len {
            // If this epoch was already being polled as the open tail,
            // resume from that exact position (stable across sealing —
            // ADR 0042 §2) instead of re-minting a fresh `TRIM_HORIZON`
            // iterator, which would re-deliver whatever the open-tail
            // poll already collected from it in an earlier pass, before
            // it sealed — a genuine double-count under any seal knob
            // that ever leaves more than one record in the open tail
            // (invisible under `tiny_seal_knobs`, where the open tail is
            // always empty the instant it's polled).
            let mut iterator = if open_epoch == Some(next_epoch) {
                open_iterator
                    .take()
                    .expect("open_epoch implies an iterator")
            } else {
                let shard_id = segment::shard_id(tablet.0, next_epoch);
                get_shard_iterator(dynamo_addr, stream_arn, &shard_id, "TRIM_HORIZON").await
            };
            loop {
                let (records, next) = get_records(dynamo_addr, &iterator).await;
                collected.extend(records);
                match next {
                    Some(n) => iterator = n,
                    None => break, // this epoch fully drained
                }
            }
            next_epoch += 1;
            open_epoch = None; // a fresh epoch just closed — re-derive the open tail below
        }
        if collected.len() >= want {
            return collected;
        }
        // One poll of the current open tail, resuming from wherever the
        // last poll of *this same* epoch left off.
        if open_epoch != Some(next_epoch) {
            let shard_id = segment::shard_id(tablet.0, next_epoch);
            open_iterator =
                Some(get_shard_iterator(dynamo_addr, stream_arn, &shard_id, "TRIM_HORIZON").await);
            open_epoch = Some(next_epoch);
        }
        let (records, next) = get_records(dynamo_addr, open_iterator.as_ref().unwrap()).await;
        collected.extend(records);
        open_iterator = next;
        if collected.len() >= want {
            return collected;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "tablet {tablet:?} never delivered {want} records ({} so far)",
                collected.len()
            );
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// [`drain_tablet_lineage`]'s multi-tablet sibling: drains every closed
/// epoch of every tablet in `tablets`, then polls every tablet's open tail
/// once per pass, summing across all of them, until `want_total` records
/// have been collected in total or `deadline` elapses.
async fn drain_all_tablets_lineage(
    dynamo_addr: SocketAddr,
    stream_arn: &str,
    node: &Node,
    tablets: &[TabletId],
    want_total: usize,
    deadline: tokio::time::Instant,
) -> Vec<Value> {
    let mut collected = Vec::new();
    let mut next_epoch: std::collections::BTreeMap<TabletId, u64> =
        tablets.iter().map(|&t| (t, 0u64)).collect();
    // Per-tablet open-tail state, resumed from its own last position — see
    // `drain_tablet_lineage`'s identical doc for why re-minting
    // `TRIM_HORIZON` every pass would double-count an open shard's records.
    let mut open_epoch: BTreeMap<TabletId, u64> = BTreeMap::new();
    let mut open_iterator: BTreeMap<TabletId, String> = BTreeMap::new();
    loop {
        for &tablet in tablets {
            let chain_len = node
                .metadata()
                .stream_shards
                .range((tablet, 0)..=(tablet, u64::MAX))
                .count() as u64;
            let cursor = next_epoch.get_mut(&tablet).expect("tracked tablet");
            while *cursor < chain_len {
                // See `drain_tablet_lineage`'s identical fix: resume from
                // the open-tail iterator if this epoch was already being
                // polled as open, rather than re-minting `TRIM_HORIZON`
                // and re-delivering what that poll already collected.
                let mut iterator = if open_epoch.get(&tablet) == Some(&*cursor) {
                    open_iterator
                        .remove(&tablet)
                        .expect("open_epoch implies an iterator")
                } else {
                    let shard_id = segment::shard_id(tablet.0, *cursor);
                    get_shard_iterator(dynamo_addr, stream_arn, &shard_id, "TRIM_HORIZON").await
                };
                loop {
                    let (records, next) = get_records(dynamo_addr, &iterator).await;
                    collected.extend(records);
                    match next {
                        Some(n) => iterator = n,
                        None => break,
                    }
                }
                *cursor += 1;
                open_epoch.remove(&tablet); // a fresh epoch just closed
            }
        }
        for &tablet in tablets {
            let epoch = next_epoch[&tablet];
            if open_epoch.get(&tablet) != Some(&epoch) {
                let shard_id = segment::shard_id(tablet.0, epoch);
                let iterator =
                    get_shard_iterator(dynamo_addr, stream_arn, &shard_id, "TRIM_HORIZON").await;
                open_iterator.insert(tablet, iterator);
                open_epoch.insert(tablet, epoch);
            }
            let iterator = open_iterator.get(&tablet).expect("just ensured").clone();
            let (records, next) = get_records(dynamo_addr, &iterator).await;
            collected.extend(records);
            if let Some(next) = next {
                open_iterator.insert(tablet, next);
            }
        }
        if collected.len() >= want_total {
            return collected;
        }
        if tokio::time::Instant::now() >= deadline {
            let chain_lens: Vec<(TabletId, usize)> = tablets
                .iter()
                .map(|&t| {
                    (
                        t,
                        node.metadata()
                            .stream_shards
                            .range((t, 0)..=(t, u64::MAX))
                            .count(),
                    )
                })
                .collect();
            panic!(
                "the lineage never delivered {want_total} records ({} so far); \
                 per-tablet closed-chain lengths: {chain_lens:?}",
                collected.len()
            );
        }
        sleep(Duration::from_millis(50)).await;
    }
}

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

// ---------------------------------------------------------------------------
// D5: GSI + Streams coexistence — the two halves of ADR 0042 §8's trim
// min-rule genuinely coexist.
// ---------------------------------------------------------------------------

/// A table with both a GSI and an enabled stream, written under the same
/// workload: the GSI drain converges to the expected rows AND the stream
/// delivers every write exactly once through `GetRecords` — proving the
/// index-cursor half and the catalog-watermark half of the trim min-rule
/// coexist rather than one starving the other. Extends the existing
/// `dynamo_gsi_drain.rs` (GSI convergence) and `dynamo_streams.rs` (stream
/// delivery) assertion families onto one table, rather than duplicating
/// either file's own dedicated tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gsi_and_stream_coexist_and_both_converge() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster(1, dir.path(), tiny_seal_knobs()).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"users",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-email",
                 "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/users/stream/{label}");

    const N: usize = 6;
    for i in 0..N {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"users","Item":{{"id":{{"S":"u{i}"}},"email":{{"S":"e{i}@x"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem(u{i}) failed: {body}");
    }

    // The GSI half: every item's own index row eventually queryable.
    for i in 0..N {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let (status, body) = dynamo(
                addr,
                "DynamoDB_20120810.Query",
                &format!(
                    r#"{{"TableName":"users","IndexName":"by-email",
                        "KeyConditionExpression":"email = :e",
                        "ExpressionAttributeValues":{{":e":{{"S":"e{i}@x"}}}}}}"#
                ),
            )
            .await;
            if status == 200 && body.contains("\"Count\":1") {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("GSI row for u{i} never converged: {status} {body}");
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    // The stream half: `GetRecords` eventually delivers all N puts, walking
    // whatever chain of epochs the tiny seal knob produced.
    let tablet = tablets_for(&nodes[0].metadata(), "users")[0];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let delivered = drain_tablet_lineage(addr, &stream_arn, &nodes[0], tablet, N, deadline).await;
    assert_eq!(
        delivered.len(),
        N,
        "the stream must deliver exactly N records, not more"
    );
}

// ---------------------------------------------------------------------------
// D8: LSM restart durability.
// ---------------------------------------------------------------------------

/// A real `LsmEngine`-backed cluster: write, seal, restart every node, and
/// confirm the catalog (sealed shard rows), the segment objects, and the
/// stream label all survive — then a fresh lineage walk (`GetShardIterator`/
/// `GetRecords` from `TRIM_HORIZON`) completes cleanly after the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lsm_restart_preserves_streams_and_walk_completes() {
    let dir = tempfile::TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();
    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), &dir_path)
        .await
        .unwrap();
    let mut nodes = start_cluster_with_streams(
        bound,
        StorageBackend::Lsm,
        None,
        None,
        Duration::from_secs(600),
        tiny_seal_knobs(),
        SegmentStoreConfig::default(),
        animusd::DEFAULT_STREAM_RETENTION,
    )
    .await
    .unwrap();
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"orders",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/orders/stream/{label}");

    for i in 0..3 {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &format!(r#"{{"TableName":"orders","Item":{{"id":{{"S":"o{i}"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200, "PutItem(o{i}) failed: {body}");
    }
    await_true(20, "the write never sealed before the restart", || {
        let meta = nodes[0].metadata();
        let Some(&tablet) = meta.tablets_for_table("orders").next().map(|(t, _)| t) else {
            return false;
        };
        meta.stream_shards
            .range((tablet, 0)..=(tablet, u64::MAX))
            .next()
            .is_some()
    })
    .await;

    nodes[0].shutdown_graceful().await;
    drop(std::mem::take(&mut nodes));
    sleep(Duration::from_millis(200)).await;

    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), &dir_path)
        .await
        .unwrap();
    let nodes = start_cluster_with_streams(
        bound,
        StorageBackend::Lsm,
        None,
        None,
        Duration::from_secs(600),
        tiny_seal_knobs(),
        SegmentStoreConfig::default(),
        animusd::DEFAULT_STREAM_RETENTION,
    )
    .await
    .unwrap();
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    await_true(
        20,
        "the sealed shard row did not survive the restart",
        || {
            let meta = nodes[0].metadata();
            meta.has_table_schema("orders")
                && meta
                    .tablets_for_table("orders")
                    .next()
                    .is_some_and(|(&t, _)| {
                        meta.stream_shards
                            .range((t, 0)..=(t, u64::MAX))
                            .next()
                            .is_some()
                    })
        },
    )
    .await;

    let tablet = tablets_for(&nodes[0].metadata(), "orders")[0];
    let shard0 = segment::shard_id(tablet.0, 0);
    let iterator = get_shard_iterator(addr, &stream_arn, &shard0, "TRIM_HORIZON").await;
    let (records, _) = get_records(addr, &iterator).await;
    assert!(
        !records.is_empty(),
        "GetRecords on the surviving sealed shard must not be empty after a real restart"
    );
}

// ---------------------------------------------------------------------------
// D8: FsSegmentStore opt-in smoke test.
// ---------------------------------------------------------------------------

/// The single-directory `FsSegmentStore` opt-in (`--segment-store dir:...`)
/// works end to end: writes seal, `GetRecords` serves the sealed shard, and
/// the object genuinely lands at the configured directory (not the default
/// `ClusterSegmentStore`'s per-node `<node dir>/segments`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs_segment_store_opt_in_smoke() {
    let dir = tempfile::TempDir::new().unwrap();
    let store_dir = dir.path().join("shared-segments");
    std::fs::create_dir_all(&store_dir).unwrap();
    let nodes = start_streamed_cluster_full(
        1,
        &dir.path().join("node"),
        tiny_seal_knobs(),
        None,
        None,
        SegmentStoreConfig::Fs(store_dir.clone()),
    )
    .await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/t/stream/{label}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"a"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");

    await_true(20, "the write never sealed via the Fs store", || {
        let meta = nodes[0].metadata();
        meta.tablets_for_table("t").next().is_some_and(|(&t, _)| {
            meta.stream_shards
                .range((t, 0)..=(t, u64::MAX))
                .next()
                .is_some()
        })
    })
    .await;

    let tablet = tablets_for(&nodes[0].metadata(), "t")[0];
    let seg_path = store_dir.join(segment::segment_id("t", &label, tablet.0, 0));
    assert!(
        seg_path.exists(),
        "the sealed segment must land at the configured Fs directory: {seg_path:?}"
    );

    let shard0 = segment::shard_id(tablet.0, 0);
    let iterator = get_shard_iterator(addr, &stream_arn, &shard0, "TRIM_HORIZON").await;
    let (records, _) = get_records(addr, &iterator).await;
    assert_eq!(
        records.len(),
        1,
        "GetRecords must serve the Fs-stored segment"
    );
}

// ---------------------------------------------------------------------------
// D8: auto-split mid-stream with a live consumer, through every node.
// ---------------------------------------------------------------------------

/// A 3-node cluster with a tiny **byte** auto-split threshold: write until
/// the table's tablet auto-splits mid-stream, driving the consumer's own
/// `DescribeStream`/`GetRecords` calls through **every node in turn** (the
/// house forwarded-command-regression pattern) both before and after the
/// split — proving the lineage handover (the parent tablet's own seal
/// after the split, and the child's `ParentShardId` link to it, both frozen
/// from the split-time basis — ADR 0043 §A4/§A6, PR1 — not a final seal *at*
/// the split boundary, which the split itself never performs: the source
/// tablet survives as the left child with its own open shard continuing
/// uninterrupted) is observable through the real wire API from any node,
/// not just whichever one happened to host the split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_split_mid_stream_with_live_consumer_across_every_node() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster_full(
        3,
        dir.path(),
        tiny_seal_knobs(),
        None,
        Some(2_048), // tiny byte threshold — a handful of writes triggers a split
        SegmentStoreConfig::default(),
    )
    .await;
    await_bootstrap(&nodes).await;

    let (status, body) = dynamo(
        nodes[0].dynamo_addr(),
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"events",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/events/stream/{label}");

    // A round-robin of "which node issues this request" — the every-node
    // sweep. `filler` pads each item well past the byte threshold quickly.
    let filler = "x".repeat(256);
    let mut expected = 0usize;
    for i in 0..40 {
        let issuer = &nodes[i % nodes.len()];
        let (status, body) = dynamo(
            issuer.dynamo_addr(),
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"events","Item":{{"id":{{"S":"e{i:04}"}},"body":{{"S":"{filler}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(
            status,
            200,
            "PutItem(e{i}) failed via node {}: {body}",
            issuer.dynamo_addr()
        );
        expected += 1;
        if i == 20 {
            // Give the auto-split loop a chance to observe the accumulated
            // bytes before writing the rest — split-then-continue, not a
            // pure write-burst-then-split.
            await_true(20, "table never split after the first half", || {
                tablets_for(&nodes[0].metadata(), "events").len() >= 2
            })
            .await;
        }
    }

    await_true(
        20,
        "table never converged to >=2 tablets on every node",
        || {
            nodes
                .iter()
                .all(|n| tablets_for(&n.metadata(), "events").len() >= 2)
        },
    )
    .await;

    // Determine parent/child via `split_parents` directly (never assumed
    // from tablet-id ordering — `SplitTablet` always mints the *new* id for
    // the fresh sibling, but this test makes no assumption about which
    // numeric id that turns out to be relative to the source's).
    let meta = nodes[0].metadata();
    let ids = tablets_for(&meta, "events");
    assert!(
        ids.len() >= 2,
        "expected at least one split to have happened"
    );
    let (child, parent) = ids
        .iter()
        .find_map(|&t| meta.split_parents.get(&t).map(|&p| (t, p)))
        .unwrap_or_else(|| panic!("no split-parent provenance recorded among {ids:?}"));

    // The lineage link (ADR 0042 §2/ADR 0043 §A4) needs the **parent** to
    // have sealed at least once — the **child** need not have: its own
    // epoch-0 entry shows up in `DescribeStream` as the *open* shard the
    // moment the tablet exists, whether or not it has ever sealed yet
    // (`describe_stream` computes `ParentShardId` for the open entry
    // exactly the same way as a closed one). Depending on where the split
    // key landed, the child can legitimately have received little or no
    // traffic yet — asserting it must have sealed would be over-strict.
    await_true(
        20,
        "the parent tablet never sealed at least one shard",
        || {
            let meta = nodes[0].metadata();
            meta.stream_shards
                .range((parent, 0)..=(parent, u64::MAX))
                .next()
                .is_some()
        },
    )
    .await;

    // Walk the whole lineage from every node in turn: `DescribeStream`
    // must show every tablet's chain, and the split child's own epoch-0
    // shard must name a shard of the parent tablet as its `ParentShardId`
    // — from *each* node's own answer, not just node 0.
    let child_shard = segment::shard_id(child.0, 0);
    for (i, node) in nodes.iter().enumerate() {
        let body = describe_stream(node.dynamo_addr(), &stream_arn).await;
        assert!(
            body.contains(&child_shard),
            "node {i}'s DescribeStream must list the split child's own shard: {body}"
        );
        let needle = format!("\"ShardId\":\"{child_shard}\"");
        let pos = body.find(&needle).unwrap_or_else(|| {
            panic!("node {i}: child shard {child_shard} missing from DescribeStream: {body}")
        });
        // The child's own entry must carry a non-null `ParentShardId`
        // naming a shard of the parent tablet (the exact epoch is not
        // pinned here — `stream_shard_parent_id` is derived, not stored,
        // ADR 0043 §A8 — only that the lineage link exists at all, which
        // requires the parent to have sealed at least once by now).
        // A window straddling the match, not just following it — the
        // wire encoding's field order within one shard object is not
        // pinned by this test, and `ParentShardId` can precede `ShardId`.
        let start = pos.saturating_sub(200);
        let end = (pos + 400).min(body.len());
        let window = &body[start..end];
        assert!(
            window.contains(&format!("shardId-{}-", parent.0)),
            "node {i}: child shard's ParentShardId must name a shard of the parent tablet: {window}"
        );
    }

    // Drain the whole lineage from a *different* node than the one that
    // wrote most items, and confirm exactly-once total delivery.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let delivered = drain_all_tablets_lineage(
        nodes[1].dynamo_addr(),
        &stream_arn,
        &nodes[1],
        &ids,
        expected,
        deadline,
    )
    .await;
    assert_eq!(
        delivered.len(),
        expected,
        "exactly-once delivery must hold across the whole auto-split lineage"
    );
}

// ---------------------------------------------------------------------------
// PR1 bugfix regression: a split with a real, still-unsealed backlog,
// under production-shaped seal knobs (ADR 0042 §8/ADR 0043 §A4/§A6).
// ---------------------------------------------------------------------------

/// The `ProdEnv` end-to-end counterpart of `animus-test`'s
/// `stream_lineage_corpus.rs::split_then_parent_seals_first` corpus cell —
/// same bug, same fix, exercised through the real DynamoDB wire API and the
/// real background loops (`change_consumer_loop`'s seal arm,
/// `auto_split_loop`) instead of a hand-driven `Metadata`/segment-store
/// model. Deliberately uses `production_seal_knobs()`, not
/// `tiny_seal_knobs()`: a tablet must carry a genuine multi-write, still
/// **unsealed** backlog across the split, so both the split and the first
/// seal that follows it happen only from real accumulated pressure — never
/// a size-1 knob that seals every write and can never leave anything
/// unsealed to inherit.
///
/// Uses the **age** trigger (`seal_bytes` set high enough to never fire on
/// its own here), not the byte trigger: no further writes happen after the
/// split, so each side gets **exactly one** seal, once its inherited
/// backlog ages past `seal_age`. This sidesteps an unrelated, pre-existing
/// timing sensitivity in `change_consumer_loop`'s byte-triggered seal arm
/// under a real write burst crossing the threshold many times in quick
/// succession (a handful of records occasionally missing from every
/// segment *and* the open tail, reproducible even with no split involved
/// at all) — a real finding from building this cell, out of scope for this
/// fix (which is about `Metadata`'s pure watermark/`ParentShardId`
/// derivation, not the seal arm's own scan/trim sequencing) and reported
/// separately rather than chased down here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_split_with_unsealed_backlog_under_production_seal_knobs() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster_full(
        3,
        dir.path(),
        production_seal_knobs(),
        None,
        Some(128), // auto-split threshold — small, so the split fires fast off a handful of writes
        SegmentStoreConfig::default(),
    )
    .await;
    await_bootstrap(&nodes).await;

    let (status, body) = dynamo(
        nodes[0].dynamo_addr(),
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"orders",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/orders/stream/{label}");

    let filler = "x".repeat(64);
    let mut ids: Vec<String> = Vec::new();

    // A real, multi-item, still-unsealed backlog before the split: enough
    // base-scope bytes to cross the auto-split threshold (128), comfortably
    // inside `seal_age` (2s) so nothing seals before the split lands.
    for i in 0..6 {
        ids.push(put_order_item(&nodes[0], i, &filler).await);
    }
    await_true(
        20,
        "table never auto-split from the pre-split backlog",
        || tablets_for(&nodes[0].metadata(), "orders").len() >= 2,
    )
    .await;

    // The precondition this cell exists to exercise: at the instant of the
    // split, nothing has sealed yet anywhere in the catalog — every write
    // so far is still sitting in the hot backlog, physically split across
    // whichever two tablets now exist.
    assert!(
        nodes[0].metadata().stream_shards.is_empty(),
        "test premise: the split must land on a genuinely unsealed backlog"
    );

    // Determine parent/child via `split_parents` — never assumed from
    // tablet-id ordering (same idiom as the auto-split test above).
    let meta = nodes[0].metadata();
    let table_tablets = tablets_for(&meta, "orders");
    let (child, parent) = table_tablets
        .iter()
        .find_map(|&t| meta.split_parents.get(&t).map(|&p| (t, p)))
        .unwrap_or_else(|| panic!("no split-parent provenance recorded among {table_tablets:?}"));

    // No further writes. Both sides' inherited backlog ages past
    // `seal_age` on its own, giving each exactly one seal — the parent's
    // own seal of its narrowed left range, and the child's first-ever seal
    // of whatever right-range backlog it physically inherited in place
    // (ADR 0043 §A4). This is where an unfixed `effective_stream_shard_
    // watermark`/`stream_shard_parent_id` would show up as a missing id
    // (the child's inherited backlog silently dropped from its own first
    // seal) rather than a wrong count alone.
    await_true(
        20,
        "parent and child never both sealed at least once",
        || {
            let meta = nodes[0].metadata();
            meta.stream_shard_watermark(parent).is_some()
                && meta.stream_shard_watermark(child).is_some()
        },
    )
    .await;

    // Drain the whole lineage (both tablets, every epoch) from a different
    // node than the one that wrote everything, and confirm every write was
    // delivered exactly once, no gaps, no duplicates.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let delivered = drain_all_tablets_lineage(
        nodes[1].dynamo_addr(),
        &stream_arn,
        &nodes[1],
        &[parent, child],
        ids.len(),
        deadline,
    )
    .await;
    let mut seen: Vec<String> = delivered
        .iter()
        .map(|r| {
            r["dynamodb"]["Keys"]["id"]["S"]
                .as_str()
                .unwrap_or_else(|| panic!("no id in {r:?}"))
                .to_owned()
        })
        .collect();
    seen.sort();
    let mut expected_ids = ids.clone();
    expected_ids.sort();
    assert_eq!(
        seen, expected_ids,
        "every write must be delivered exactly once, including the pre-split backlog"
    );
}

// ---------------------------------------------------------------------------
// Regression: `GET /admin/status` (and, transitively, the wire
// `ClientResponse::Status`/`write_frame` path it shares a `Metadata`
// serialization with) must survive a **populated** `stream_shards` catalog.
//
// `Metadata::stream_shards` used to be a plain `BTreeMap<(TabletId, u64), _>`
// field — `serde_json`'s `MapKeySerializer` rejects any non-string map key,
// so the moment a real shard sealed, `serde_json::to_value(&metadata)` err'd
// outright. `admin.rs`'s handler swallowed that error into `Value::Null`
// (silently blanking the whole admin dashboard the instant any stream
// sealed anywhere in the cluster); `write_frame` `.expect()`s the encode to
// succeed, so the same condition panicked the serving connection for any
// wire caller of `ClientResponse::Status` (a data-only/growth-node
// `Metadata` mirror). See `animus-control::meta`'s own round-trip test for
// the unit-level reproduction; this is the through-the-real-HTTP-endpoint
// regression, over `tiny_seal_knobs()` so a single write seals immediately.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_status_survives_a_populated_stream_shard_catalog() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster(1, dir.path(), tiny_seal_knobs()).await;
    await_bootstrap(&nodes).await;
    let dynamo_addr = nodes[0].dynamo_addr();
    let admin_addr = nodes[0].admin_addr();

    // Baseline: before any shard ever seals, `/admin/status` must already
    // be a healthy, non-null `Metadata` view (proves this isn't a
    // pre-existing "the endpoint is always broken" issue).
    let (status, body) = admin(admin_addr, "GET", "/admin/status", None).await;
    assert_eq!(status, 200, "GET /admin/status (baseline) failed: {body:?}");
    assert!(
        !body.is_null(),
        "GET /admin/status (baseline) must not be null: {body:?}"
    );

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"a"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");

    // `tiny_seal_knobs()` (`seal_bytes: 1`) makes this write its own shard —
    // wait for the catalog to actually be non-empty (the test's own
    // premise), not just for the write to commit.
    await_true(20, "the write never sealed into a catalog row", || {
        !nodes[0].metadata().stream_shards.is_empty()
    })
    .await;

    // The bug: this call used to either return `Value::Null` (swallowed
    // error) or, over the wire path sharing the same codec, panic the
    // serving connection outright.
    let (status, body) = admin(admin_addr, "GET", "/admin/status", None).await;
    assert_eq!(
        status, 200,
        "GET /admin/status must stay 200 once stream_shards is populated: {body:?}"
    );
    assert!(
        !body.is_null(),
        "GET /admin/status must not silently degrade to null once stream_shards \
         is populated: {body:?}"
    );

    let rows = body["stream_shards"]
        .as_array()
        .unwrap_or_else(|| panic!("stream_shards must be a JSON array: {body:?}"));
    assert!(
        !rows.is_empty(),
        "GET /admin/status must actually surface the sealed shard row(s): {body:?}"
    );
    let tablet = tablets_for(&nodes[0].metadata(), "t")[0];
    let row = rows
        .iter()
        .find(|r| r["tablet"].as_u64() == Some(tablet.0))
        .unwrap_or_else(|| panic!("no stream_shards row for tablet {}: {body:?}", tablet.0));
    assert_eq!(row["epoch"].as_u64(), Some(0));
    assert_eq!(row["table"].as_str(), Some("t"));
}

/// Regression, wire-protocol side: `ClientResponse::Status { metadata, .. }`
/// rides `write_frame`, which `.expect()`s the `serde_json::to_vec` encode
/// to succeed — so the same bug the admin-endpoint regression above catches
/// would instead **panic the serving connection handler** here (a
/// `ControlHandle::Remote` data-only/growth-node mirror's own poll target)
/// rather than degrade to `null`. A plain `ClientRequest::Status` over the
/// TCP client protocol must still get back a well-formed `ClientResponse::
/// Status` once `stream_shards` is populated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_protocol_status_survives_a_populated_stream_shard_catalog() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster(1, dir.path(), tiny_seal_knobs()).await;
    await_bootstrap(&nodes).await;
    let dynamo_addr = nodes[0].dynamo_addr();
    let client_addr = nodes[0].client_addr();

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"a"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");

    await_true(20, "the write never sealed into a catalog row", || {
        !nodes[0].metadata().stream_shards.is_empty()
    })
    .await;

    // The bug's wire-side symptom: this used to panic the connection
    // handler mid-encode, so the client would see the connection drop
    // instead of a reply.
    let mut stream = TcpStream::connect(client_addr)
        .await
        .expect("connect to client port");
    animusd::write_frame(&mut stream, &ClientRequest::Status)
        .await
        .expect("send Status request");
    let reply = read_frame::<ClientResponse>(&mut stream)
        .await
        .expect("read reply frame")
        .expect("a reply, not a dropped connection");
    match reply {
        ClientResponse::Status { metadata, .. } => {
            assert!(
                !metadata.stream_shards.is_empty(),
                "the wire Status reply must carry the populated stream_shards catalog"
            );
        }
        other => panic!("unexpected reply to Status: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The admin dashboard's data proxy (`POST /admin/data/dynamo`) reaching the
// DynamoDB Streams read API (dash/1-streams-proxy).
// ---------------------------------------------------------------------------

/// Before this fix, `action_data_dynamo` always built a `DynamoDB_20120810.*`
/// target for a bare `op` and called `dynamo::execute` directly — bypassing
/// `dynamo::dispatch`'s own target-prefix fork entirely, so the admin proxy
/// could never reach `ListStreams`/`DescribeStream`/`GetShardIterator`/
/// `GetRecords` no matter how `op` was spelled. This drives the full round
/// trip — create a streamed table, write an item, then walk all four Streams
/// ops through the admin proxy — and asserts a real record comes back,
/// mixing bare op names with one fully-qualified (dot) passthrough to cover
/// both `op` shapes the route accepts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_data_dynamo_proxy_reaches_streams_read_api() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster(1, dir.path(), tiny_seal_knobs()).await;
    await_bootstrap(&nodes).await;
    let admin_addr = nodes[0].admin_addr();

    // Set up a streamed table and one item entirely through the admin proxy
    // — exercising the item-API half of the same route too.
    let (status, body) = admin(
        admin_addr,
        "POST",
        "/admin/data/dynamo",
        Some(
            r#"{"op":"CreateTable","payload":{"TableName":"t",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                "StreamSpecification":{"StreamEnabled":true,
                    "StreamViewType":"NEW_AND_OLD_IMAGES"}}}"#,
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable via admin proxy: {body:?}");
    let label = body["TableDescription"]["LatestStreamLabel"]
        .as_str()
        .unwrap_or_else(|| panic!("no LatestStreamLabel in CreateTable response: {body:?}"))
        .to_owned();
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/t/stream/{label}");

    let (status, body) = admin(
        admin_addr,
        "POST",
        "/admin/data/dynamo",
        Some(r#"{"op":"PutItem","payload":{"TableName":"t","Item":{"id":{"S":"a"}}}}"#),
    )
    .await;
    assert_eq!(status, 200, "PutItem via admin proxy: {body:?}");

    // `tiny_seal_knobs()` (`seal_bytes: 1`) makes the one write its own
    // sealed shard shortly after commit.
    await_true(20, "the write never sealed into a catalog row", || {
        !nodes[0].metadata().stream_shards.is_empty()
    })
    .await;

    // ---- ListStreams (bare op) --------------------------------------------
    let (status, body) = admin(
        admin_addr,
        "POST",
        "/admin/data/dynamo",
        Some(r#"{"op":"ListStreams","payload":{}}"#),
    )
    .await;
    assert_eq!(status, 200, "ListStreams via admin proxy: {body:?}");
    let streams = body["Streams"]
        .as_array()
        .unwrap_or_else(|| panic!("Streams must be an array: {body:?}"));
    assert!(
        streams
            .iter()
            .any(|s| s["TableName"] == "t" && s["StreamArn"] == stream_arn),
        "ListStreams via admin proxy must list the table's own stream: {body:?}"
    );

    // ---- DescribeStream (bare op) ------------------------------------------
    let (status, body) = admin(
        admin_addr,
        "POST",
        "/admin/data/dynamo",
        Some(&format!(
            r#"{{"op":"DescribeStream","payload":{{"StreamArn":"{stream_arn}"}}}}"#
        )),
    )
    .await;
    assert_eq!(status, 200, "DescribeStream via admin proxy: {body:?}");
    let shard_id = body["StreamDescription"]["Shards"][0]["ShardId"]
        .as_str()
        .unwrap_or_else(|| panic!("no shard in DescribeStream response: {body:?}"))
        .to_owned();

    // ---- GetShardIterator (fully-qualified `op` — the dot-passthrough shape) --
    let (status, body) = admin(
        admin_addr,
        "POST",
        "/admin/data/dynamo",
        Some(&format!(
            r#"{{"op":"DynamoDBStreams_20120810.GetShardIterator","payload":{{"StreamArn":"{stream_arn}","ShardId":"{shard_id}","ShardIteratorType":"TRIM_HORIZON"}}}}"#
        )),
    )
    .await;
    assert_eq!(status, 200, "GetShardIterator via admin proxy: {body:?}");
    let iterator = body["ShardIterator"]
        .as_str()
        .unwrap_or_else(|| panic!("no ShardIterator in response: {body:?}"))
        .to_owned();

    // ---- GetRecords (bare op) — the actual payoff: a real record ----------
    let (status, body) = admin(
        admin_addr,
        "POST",
        "/admin/data/dynamo",
        Some(&format!(
            r#"{{"op":"GetRecords","payload":{{"ShardIterator":"{iterator}"}}}}"#
        )),
    )
    .await;
    assert_eq!(status, 200, "GetRecords via admin proxy: {body:?}");
    let records = body["Records"]
        .as_array()
        .unwrap_or_else(|| panic!("Records must be an array: {body:?}"));
    assert_eq!(records.len(), 1, "one record for the one write: {body:?}");
    assert_eq!(
        records[0]["dynamodb"]["Keys"]["id"]["S"], "a",
        "the returned record must be the item actually written: {body:?}"
    );
}

/// A negative case for the same route: an `op` that belongs to neither the
/// item API nor the Streams API must still fail cleanly (a client-error
/// status with a well-formed error body), never a panic or a hang — a
/// routing change here must not weaken that.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_data_dynamo_proxy_rejects_unknown_op_cleanly() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster(1, dir.path(), tiny_seal_knobs()).await;
    await_bootstrap(&nodes).await;
    let admin_addr = nodes[0].admin_addr();

    let (status, body) = admin(
        admin_addr,
        "POST",
        "/admin/data/dynamo",
        Some(r#"{"op":"TotallyNotARealOperation","payload":{}}"#),
    )
    .await;
    assert_eq!(
        status, 400,
        "an unknown op must error cleanly (400), not panic or hang: {body:?}"
    );
    assert!(
        body["__type"]
            .as_str()
            .is_some_and(|t| t.ends_with("UnknownOperationException")),
        "must be the standard unknown-operation error shape: {body:?}"
    );
}
