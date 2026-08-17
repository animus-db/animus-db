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

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_control::Metadata;
use animus_cp_data::segment;
use animus_tablet::{TabletId, partition_token};
use animusd::{
    ClientRequest, ClientResponse, Node, SegmentStoreConfig, StorageBackend, StreamSealKnobs,
    bind_cluster, read_frame, start_cluster_with_streams, write_frame,
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

/// Growth PR3 Fork F (ADR 0042 §14): [`start_streamed_cluster`], but with
/// the opt-in `--auto-split-change-rate` trigger enabled — a test-local
/// helper (this file's own "sibling test modules keep their own fixtures
/// independent" convention) rather than widening `start_streamed_cluster_full`'s
/// already-long signature for a knob only this one cell needs.
async fn start_streamed_cluster_with_change_rate(
    n: usize,
    dir: &Path,
    knobs: StreamSealKnobs,
    change_rate_bytes_per_sec: u64,
) -> Vec<Node> {
    let bound = bind_cluster(n, "127.0.0.1".parse().unwrap(), dir)
        .await
        .unwrap();
    animusd::start_cluster_with_growth(
        bound,
        StorageBackend::default(),
        None,
        None,
        Duration::from_secs(600),
        knobs,
        SegmentStoreConfig::default(),
        animusd::DEFAULT_STREAM_RETENTION,
        Some(change_rate_bytes_per_sec),
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

/// The full, *current* set of tablet ids the stream's shard list spans —
/// paginating via `ExclusiveStartShardId`/`LastEvaluatedShardId` until a
/// page comes back with no more to page through. This is the wire-visible
/// source of truth [`drain_all_tablets_lineage`] re-consults every pass so a
/// cascading split landing mid-drain (a child tablet's own later split,
/// minting a brand-new grandchild tablet this walk was never told about up
/// front) is discovered the same way any real consumer would: by asking
/// `DescribeStream` again, never by peeking at `Metadata`'s tablet map
/// directly (that stays reserved for the already-tracked-tablet chain-length
/// read below, which — unlike *discovering a tablet exists at all* — has no
/// wire equivalent short of re-deriving it from a full shard list per
/// tablet).
async fn stream_tablet_ids(addr: SocketAddr, stream_arn: &str) -> BTreeSet<u64> {
    let mut ids = BTreeSet::new();
    let mut start: Option<String> = None;
    loop {
        let start_clause = start
            .as_ref()
            .map(|s| format!(r#","ExclusiveStartShardId":"{s}""#))
            .unwrap_or_default();
        let (status, resp) = dynamo(
            addr,
            "DynamoDBStreams_20120810.DescribeStream",
            &format!(r#"{{"StreamArn":"{stream_arn}"{start_clause}}}"#),
        )
        .await;
        assert_eq!(status, 200, "DescribeStream (discovery) failed: {resp}");
        let v = json(&resp);
        let shards = v["StreamDescription"]["Shards"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        for shard in &shards {
            if let Some(tablet) = shard["ShardId"]
                .as_str()
                .and_then(|id| id.strip_prefix("shardId-"))
                .and_then(|rest| rest.split_once('-'))
                .and_then(|(tablet, _epoch)| tablet.parse::<u64>().ok())
            {
                ids.insert(tablet);
            }
        }
        match v["StreamDescription"]["LastEvaluatedShardId"].as_str() {
            Some(next) => start = Some(next.to_owned()),
            None => break,
        }
    }
    ids
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
/// more than one record in it. **A second, distinct interleaving of the same
/// race**: when the open-tail poll's *own* call is the one that witnesses
/// the seal (the fresh open-vs-sealed check inside that single call flips to
/// the sealed path and returns the epoch's last records with a null
/// `NextShardIterator` in one response, rather than the epoch being
/// discovered already-closed on a *later* pass's `chain_len` read first),
/// the poll must advance past that epoch immediately instead of leaving
/// `open_epoch` pointed at the now-exhausted iterator — otherwise the next
/// pass's "resume" branch re-issues that same spent iterator and
/// re-delivers the records it already returned. Only exercised at any real
/// rate by a cascading multi-tablet split under sustained write pressure
/// (`auto_split_mid_stream_with_live_consumer_across_every_node`, D8) — a
/// single controlled split leaves little chance of a poll racing the seal
/// this precisely.
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
        match next {
            Some(n) => open_iterator = Some(n),
            None => {
                // The shard sealed between this outstanding iterator's mint
                // (or its last poll) and *this* call — ADR 0042 §2's fresh
                // open-vs-sealed check means this one call transparently
                // switched to serving the sealed path and returned that
                // epoch's final, fully-exhausted read in the same response
                // (`records` already holds everything up to the seal).
                // Advance past this epoch now, without re-reading: leaving
                // `open_epoch` pointed at an iterator with nothing left to
                // give would make the closed-epoch loop above "resume" it
                // next pass and re-deliver exactly what was just collected
                // — a genuine double-count under any interleaving where the
                // open-tail poll itself is the one that witnesses the seal,
                // as opposed to discovering an already-sealed epoch via a
                // fresh `chain_len` read first (the case the loop above
                // already handles by resuming a *non-exhausted* iterator).
                next_epoch += 1;
                open_epoch = None;
                open_iterator = None;
            }
        }
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
/// epoch of every currently-known tablet, then polls every known tablet's
/// open tail once per pass, summing across all of them, until `want_total`
/// records have been collected in total or `deadline` elapses. Carries the
/// identical fix documented on [`drain_tablet_lineage`] for the
/// poll-witnesses-its-own-seal race, applied **per tablet independently** —
/// each tablet in a cascading multi-generation split (D8) hits this race on
/// its own schedule, so the fix must self-correct one tablet at a time
/// rather than assume the whole set transitions in lockstep.
///
/// `tablets` seeds the walk but is **not** a fixed set for its whole
/// duration: a real cascading split — a child of the caller's own split
/// child splitting again while this walk is still mid-drain — mints a
/// brand-new tablet id nobody handed this function up front. Each outer
/// pass re-resolves the *current* shard chain via a fresh [`DescribeStream`]
/// call ([`stream_tablet_ids`]) before touching any tablet's records, and
/// any newly discovered id is folded into the tracked set with its own
/// fresh `next_epoch = 0` cursor — never disturbing an already-tracked
/// tablet's in-flight cursor, which is exactly the resume-not-remint
/// invariant [`drain_tablet_lineage`]'s own doc protects. Previously this
/// function only ever saw the tablet set the caller captured once, before
/// the drain started; a third-generation split landing mid-drain (a child's
/// own child) would mint a tablet this walk could never learn about, and its
/// records would never be read — a spurious deficit under sustained write
/// pressure (`auto_split_mid_stream_with_live_consumer_across_every_node`,
/// D8, ~1/20 iterations before this fix), now closed structurally rather
/// than adjudicated as a known harness limitation.
async fn drain_all_tablets_lineage(
    dynamo_addr: SocketAddr,
    stream_arn: &str,
    node: &Node,
    tablets: &[TabletId],
    want_total: usize,
    deadline: tokio::time::Instant,
) -> Vec<Value> {
    let mut collected = Vec::new();
    let mut tracked: BTreeSet<TabletId> = tablets.iter().copied().collect();
    let mut next_epoch: std::collections::BTreeMap<TabletId, u64> =
        tracked.iter().map(|&t| (t, 0u64)).collect();
    // Per-tablet open-tail state, resumed from its own last position — see
    // `drain_tablet_lineage`'s identical doc for why re-minting
    // `TRIM_HORIZON` every pass would double-count an open shard's records.
    let mut open_epoch: BTreeMap<TabletId, u64> = BTreeMap::new();
    let mut open_iterator: BTreeMap<TabletId, String> = BTreeMap::new();
    loop {
        // Re-resolve the shard chain before touching any tablet's records
        // this pass — see this function's own doc for why a static snapshot
        // of `tablets` misses a cascading split's newest generation. A
        // freshly discovered tablet starts at epoch 0 and has no open-tail
        // state yet, so it falls straight into the ordinary per-tablet loops
        // below exactly like one of the originally-seeded tablets would.
        for tablet_id in stream_tablet_ids(dynamo_addr, stream_arn).await {
            let tablet = TabletId(tablet_id);
            if tracked.insert(tablet) {
                next_epoch.insert(tablet, 0);
            }
        }
        let current_tablets: Vec<TabletId> = tracked.iter().copied().collect();
        for &tablet in &current_tablets {
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
        for &tablet in &current_tablets {
            // ADR 0050: a RETIRED split parent has no open shard — its
            // chain ENDS at its final sealed epoch (all consumed by the
            // closed-epoch loop above); polling a minted "open" successor
            // would be answered `TrimmedDataAccess`. The children carry on
            // via their own tracked entries.
            if !node.metadata().tablets.contains_key(&tablet) {
                continue;
            }
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
            match next {
                Some(next) => {
                    open_iterator.insert(tablet, next);
                }
                None => {
                    // Identical race to `drain_tablet_lineage`'s fix above:
                    // this tablet's epoch sealed between mint/last-poll and
                    // this call, so this response is that epoch's final,
                    // fully-exhausted read, already folded into `records`.
                    // Advance this tablet's own cursor past it now and drop
                    // the now-spent iterator, rather than leaving
                    // `open_epoch`/`open_iterator` pointed at it — the next
                    // pass's closed-epoch loop would otherwise "resume" an
                    // iterator with nothing left to give and re-deliver
                    // exactly what was just collected. Each tablet's own
                    // cascade of splits/seals hits this independently, so
                    // this must self-correct per tablet, not just once.
                    *next_epoch.get_mut(&tablet).expect("tracked tablet") += 1;
                    open_epoch.remove(&tablet);
                    open_iterator.remove(&tablet);
                }
            }
        }
        if collected.len() >= want_total {
            return collected;
        }
        if tokio::time::Instant::now() >= deadline {
            let chain_lens: Vec<(TabletId, usize)> = current_tablets
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
                 tracked tablets: {current_tablets:?}; per-tablet closed-chain lengths: \
                 {chain_lens:?}",
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
    let meta = nodes[0].metadata();
    let row = &meta.stream_shards[&(tablet, 0)];
    // Ledger-named-object amendment: the object lands at the row's own
    // unique `object_id`, never the bare deterministic `segment_id` (which
    // is now only a directory prefix several attempts could share).
    let seg_path = store_dir.join(&row.object_id);
    assert!(
        seg_path.is_file(),
        "the sealed segment must land at the configured Fs directory, at the row's own \
         object_id: {seg_path:?}"
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
///
/// **Known open failure modes — adjudicate against these, don't
/// re-investigate from scratch.** (1) An **over-count** (`delivered >
/// expected`) is an open, *production* cross-tablet duplication at a split
/// boundary: the same write's change-log record gets independently sealed
/// into both the parent tablet's own stream and the freshly-split child's
/// epoch 0. The final `assert_eq!` below prints a diagnostic on any
/// mismatch (grouping by `eventID` vs. by item id) that confirms this is
/// the shape every time — distinct `eventID`s, same trailing packed-HLC
/// digits, different `shardId-<tablet>-<epoch>` prefixes — as opposed to a
/// harness double-read (which would show a *repeated* `eventID`). Tracked
/// in `docs/engineering-lessons.md`, not fixed here (this file is test-only
/// by convention/scope).
///
/// (2) A **deficit** (a timeout short of `expected`, ~1/20 iterations
/// before the fix below) used to be this test's own dedicated harness bug,
/// now fixed: `drain_all_tablets_lineage` took a **static** snapshot of the
/// tablet/shard set (`ids`, captured once, above), so a **cascading**
/// third-generation split — a child of this test's own split child
/// splitting again while the drain was already mid-walk — minted a
/// grandchild tablet the walk was never told about and could never read,
/// silently short-counting. Production was never wrong here: only the
/// harness's snapshot was stale. `drain_all_tablets_lineage` now
/// re-resolves the live shard chain via `DescribeStream` every pass
/// (`stream_tablet_ids`) and folds any newly discovered tablet in without
/// disturbing an already-tracked one's in-flight iterator (preserving
/// PR #219's resume-not-remint fix), so this no longer needs adjudicating
/// as a known limitation. A genuinely unrelated deficit mode remains
/// possible in principle (`manual_split_with_unsealed_backlog_under_
/// production_seal_knobs`'s own doc comment below documents a separate,
/// pre-existing timing sensitivity in the byte-triggered seal arm under a
/// real write burst) — if a deficit ever recurs here, check the diagnostic
/// panic's `tracked tablets`/chain-length dump first: a set that still
/// matches `ids` with no cascading grandchild in it points at that
/// different, seal-arm-timing cause instead.
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

    // Determine parent/child via `split_lineage` (ADR 0050 fork F9) —
    // written at CUTOVER, so poll the workflow to completion rather than
    // sampling once mid-build (a `Splitting` parent + `Building` children
    // is a legitimate transient this loop simply waits out).
    let (child, parent) = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
        loop {
            let meta = nodes[0].metadata();
            let ids = tablets_for(&meta, "events");
            if let Some((c, p)) = ids
                .iter()
                .find_map(|&t| meta.split_lineage.get(&t).map(|l| (t, l.parent)))
            {
                break (c, p);
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no cutover lineage ever recorded among {ids:?}"
            );
            sleep(Duration::from_millis(100)).await;
        }
    };

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
    let seed_tablets: Vec<TabletId> = {
        let meta = nodes[1].metadata();
        let mut ts = tablets_for(&meta, "events");
        // The retired parent still carries the pre-split history — walk it
        // too (its sealed rows persist in the catalog; the helper skips a
        // retired tablet's nonexistent open tail).
        ts.push(parent);
        ts
    };
    let delivered = drain_all_tablets_lineage(
        nodes[1].dynamo_addr(),
        &stream_arn,
        &nodes[1],
        &seed_tablets,
        expected,
        deadline,
    )
    .await;
    if delivered.len() != expected {
        // Self-adjudicating failure diagnostic: distinguish "same shard+HLC
        // re-read twice" (a harness double-poll — duplicate `eventID`s)
        // from "two different shards produced a record for the same item"
        // (a genuine cross-tablet production duplication — duplicate item
        // ids under *distinct* `eventID`s, same trailing packed-HLC digits
        // but a different `shardId-<tablet>-<epoch>` prefix). A run against
        // `c37995d` found the latter: the *same* write shows up sealed into
        // both the parent tablet's own epoch and the freshly-split child's
        // epoch 0 — an open production bug in the split-time change-log
        // drain (not this file's own iterator bookkeeping), tracked
        // separately (see this function's own doc comment and
        // `docs/engineering-lessons.md`) rather than re-investigated here
        // every time this test goes red.
        let mut by_event_id: BTreeMap<String, usize> = BTreeMap::new();
        let mut by_item_id: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for r in &delivered {
            let event_id = r["eventID"].as_str().unwrap_or("?").to_owned();
            *by_event_id.entry(event_id.clone()).or_insert(0) += 1;
            let item_id = r["dynamodb"]["Keys"]["id"]["S"]
                .as_str()
                .unwrap_or("?")
                .to_owned();
            by_item_id.entry(item_id).or_default().push(event_id);
        }
        let dup_events: Vec<_> = by_event_id.iter().filter(|&(_, &c)| c > 1).collect();
        let dup_items: Vec<_> = by_item_id.iter().filter(|(_, v)| v.len() > 1).collect();
        eprintln!(
            "DIAGNOSTIC delivered={} expected={expected} dup_event_ids={dup_events:?} dup_item_ids={dup_items:?}",
            delivered.len(),
        );
    }
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

    // The precondition this cell exists to exercise: the workflow lands on
    // a genuinely unsealed backlog (seal_age = 2s, the auto-split fires
    // within milliseconds of the byte threshold) — under ADR 0050 that
    // backlog's consumer-visibility story is now entirely the PARENT's:
    // the freeze seals it whole into the parent's final shard(s), and the
    // children are born with EMPTY change logs (no inherited backlog, no
    // frozen-basis watermark — principle 3's stronger form), so the child
    // seals nothing at all here (no further writes ever land on it).
    //
    // Wait for the workflow to CUT OVER — lineage present = children
    // Active + parent retired (fork F9's cutover-time map).
    let (child, parent) = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
        loop {
            let meta = nodes[0].metadata();
            let table_tablets = tablets_for(&meta, "orders");
            if let Some((c, p)) = table_tablets
                .iter()
                .find_map(|&t| meta.split_lineage.get(&t).map(|l| (t, l.parent)))
            {
                break (c, p);
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the split workflow never cut over (no lineage among {table_tablets:?})"
            );
            sleep(Duration::from_millis(100)).await;
        }
    };

    // The parent's final seal (the freeze's own step) covers the whole
    // pre-split backlog — this is where a lost final seal would show up as
    // missing ids in the walk below.
    await_true(20, "the retired parent never sealed its backlog", || {
        nodes[0].metadata().stream_shard_watermark(parent).is_some()
    })
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

// ---------------------------------------------------------------------------
// Growth PR3 (ADR 0042 §14): `POST /admin/stream/grow`.
// ---------------------------------------------------------------------------

/// A plain-client-protocol `SplitTablet` call — an **arbitrary binary**
/// `split_key`, unlike the admin HTTP surface's UTF8-string one (see
/// `crates/animusd/CLAUDE.md`'s Tests section) — needed here to bisect a
/// REAL DynamoDB-written tablet by an actual computed partition token
/// (murmur hash bytes are not, in general, valid UTF-8, so `POST
/// /admin/tablet/split`'s JSON string field cannot carry one).
async fn plain_split(client_addr: SocketAddr, tablet: TabletId, split_key: Vec<u8>, new_id: u64) {
    let mut stream = TcpStream::connect(client_addr)
        .await
        .expect("connect to client port");
    write_frame(
        &mut stream,
        &ClientRequest::SplitTablet {
            tablet: tablet.0,
            split_key,
        },
    )
    .await
    .expect("send SplitTablet");
    let resp: ClientResponse = read_frame(&mut stream)
        .await
        .expect("read reply")
        .expect("a reply");
    assert!(
        matches!(resp, ClientResponse::PutOk),
        "plain-protocol split of tablet {} into {new_id} failed: {resp:?}",
        tablet.0
    );
}

/// `POST /admin/stream/grow` (ADR 0042 §14, growth PR3): split EVERY
/// tablet of a streamed table at its own byte-weighted median in one
/// action. Starts from a genuinely multi-tablet table (a real, data-driven
/// bootstrap split — the precondition growth's own "every tablet, not just
/// one" behavior needs to prove anything beyond a single ordinary split),
/// writes further real items across both halves, then grows through a
/// THIRD node (neither the one that wrote most items nor necessarily the
/// leader of either tablet) — doubling 2 tablets to 4 — and walks the
/// resulting lineage from EVERY node in turn (the house forwarded-command
/// pattern: `ClientRequest::TriggerAutoSplit` is an internal, relayable-
/// via-forwarding RPC, since a table's two pre-existing tablets can be led
/// by two different nodes, neither necessarily the one serving the admin
/// request), asserting exactly-once delivery across every cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_stream_grow_doubles_a_multi_tablet_table_with_exactly_once_delivery() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster(3, dir.path(), tiny_seal_knobs()).await;
    await_bootstrap(&nodes).await;

    let (status, body) = dynamo(
        nodes[0].dynamo_addr(),
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"widgets",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/widgets/stream/{label}");

    // 40 items round-robin across every node, into the single bootstrap
    // tablet.
    let mut expected = 0usize;
    let mut ids_written: Vec<String> = Vec::new();
    for i in 0..40 {
        let id = format!("w{i:04}");
        let issuer = &nodes[i % nodes.len()];
        let (status, body) = dynamo(
            issuer.dynamo_addr(),
            "DynamoDB_20120810.PutItem",
            &format!(r#"{{"TableName":"widgets","Item":{{"id":{{"S":"{id}"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200, "PutItem({id}) failed: {body}");
        ids_written.push(id);
        expected += 1;
    }

    // A genuine, data-driven bootstrap split: the median of the 40 items'
    // OWN real partition tokens (ADR 0022) — not a guessed/arbitrary key —
    // so both resulting tablets provably hold real rows for growth's own
    // byte-weighted median to later bisect again.
    let bootstrap_tablet = tablets_for(&nodes[0].metadata(), "widgets")
        .into_iter()
        .next()
        .expect("bootstrap tablet exists");
    let mut tokens: Vec<[u8; 8]> = ids_written
        .iter()
        .map(|id| partition_token(id.as_bytes()))
        .collect();
    tokens.sort_unstable();
    let median_token = tokens[tokens.len() / 2].to_vec();
    plain_split(
        nodes[0].client_addr(),
        bootstrap_tablet,
        median_token,
        // The allocator's next id: only one tablet exists yet.
        2,
    )
    .await;

    await_true(
        20,
        "table never converged to exactly 2 tablets on every node",
        || {
            nodes
                .iter()
                .all(|n| tablets_for(&n.metadata(), "widgets").len() == 2)
        },
    )
    .await;

    // 40 more items, round-robin across every node — real writes into
    // BOTH halves via ordinary hashing (no placement control needed:
    // with 80 total near-uniformly-hashed items across 2 tablets, the
    // chance either one ends up with fewer than 2 distinct keys, and
    // hence no legal split point for growth to find, is astronomically
    // small).
    for i in 40..80 {
        let id = format!("w{i:04}");
        let issuer = &nodes[i % nodes.len()];
        let (status, body) = dynamo(
            issuer.dynamo_addr(),
            "DynamoDB_20120810.PutItem",
            &format!(r#"{{"TableName":"widgets","Item":{{"id":{{"S":"{id}"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200, "PutItem({id}) failed: {body}");
        expected += 1;
    }

    let pre_grow_ids = tablets_for(&nodes[0].metadata(), "widgets");
    assert_eq!(
        pre_grow_ids.len(),
        2,
        "test setup: exactly 2 tablets before growing"
    );

    // Grow through node 2 — not node 0 (which wrote/hosts the original
    // bootstrap tablet's early traffic) and not necessarily the leader of
    // either tablet — exercising `TriggerAutoSplit`'s one-hop forward to
    // whichever node actually leads each of the 2 tablets.
    let (status, body) = admin(
        nodes[2].admin_addr(),
        "POST",
        "/admin/stream/grow",
        Some(r#"{"table":"widgets"}"#),
    )
    .await;
    assert_eq!(status, 200, "stream/grow failed: {body}");
    assert_eq!(
        body["split_count"].as_u64(),
        Some(2),
        "both pre-existing tablets must split: {body}"
    );
    assert_eq!(
        body["error_count"].as_u64(),
        Some(0),
        "no tablet should error: {body}"
    );

    // Self-adjudicating failure diagnostic (matching this file's own
    // `auto_split_mid_stream_with_live_consumer_across_every_node`
    // convention above): dump each node's own tablet view on a timeout,
    // rather than a bare "timed out" with nothing to debug from.
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if nodes
                .iter()
                .all(|n| tablets_for(&n.metadata(), "widgets").len() == 4)
            {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                for (i, n) in nodes.iter().enumerate() {
                    eprintln!(
                        "DIAGNOSTIC node {i} tablets_for(widgets) = {:?} is_control_leader={}",
                        tablets_for(&n.metadata(), "widgets"),
                        n.is_control_leader(),
                    );
                }
                panic!(
                    "table never converged to exactly 4 tablets on every node (timed out after 20s)"
                );
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    let ids = tablets_for(&nodes[0].metadata(), "widgets");
    assert_eq!(ids.len(), 4, "growth must double 2 tablets to 4");

    // Walk the whole doubled lineage from EVERY node in turn:
    // `DescribeStream` must show all 4 children's chains PLUS each retired
    // parent's closed final shard (ADR 0050 — a copy-based grow retires
    // the two pre-existing tablets; their sealed history stays listed,
    // the AWS closed-ancestor-shard shape) — from each node's own answer,
    // not just the one that happened to serve the grow request.
    let mut want_tablet_ids: BTreeSet<u64> = ids.iter().map(|t| t.0).collect();
    {
        // Every ancestor that ever sealed stays visible via its catalog
        // rows — transitively (the table's own setup split retired a
        // grandparent too), so derive the set from the catalog itself.
        let meta = nodes[0].metadata();
        for (tablet, _) in meta.stream_shards.keys() {
            want_tablet_ids.insert(tablet.0);
        }
    }
    for (i, node) in nodes.iter().enumerate() {
        let found = stream_tablet_ids(node.dynamo_addr(), &stream_arn).await;
        assert_eq!(
            found, want_tablet_ids,
            "node {i}'s DescribeStream must list all 4 children + the retired parents' closed shards"
        );
    }

    // Drain the whole doubled lineage from yet another node and confirm
    // exactly-once total delivery across every cut this growth action made.
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
        "exactly-once delivery must hold across the whole doubled lineage"
    );
}

/// Growth PR3 Fork F (ADR 0042 §14): the opt-in `--auto-split-change-rate`
/// trigger. Aggressive knobs (a low `RATE`, no other threshold configured)
/// so a short, sizable write burst against a **streamed** table's single
/// tablet drives its own smoothed change-append rate well above `RATE`
/// within a couple of `INDEX_DRAIN_INTERVAL` ticks — proving a high-churn
/// streamed table splits on rate alone. The SAME burst against a **plain,
/// unstreamed** table must never split at all: no byte/key threshold is
/// configured, and the change-rate tracker is never even populated for an
/// unstreamed tablet (`index_drain::seal_tick`'s `stream_enabled` gate),
/// so `--auto-split-change-rate` must have zero effect on it regardless of
/// write volume — the "opt-in, streamed tables only, no surprise splits on
/// an existing plain table" guarantee.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_split_change_rate_splits_a_high_churn_streamed_table_never_a_plain_one() {
    let dir = tempfile::TempDir::new().unwrap();
    // Large seal knobs: the burst below must accumulate in `KIND_CHANGE`
    // rather than sealing (and hence trimming) mid-burst, so the tracker
    // sees a clean, strongly-rising byte level rather than seal-driven
    // sawtooth noise. An aggressive 10 KB/sec threshold — the burst below
    // produces roughly two orders of magnitude more than that.
    let nodes = start_streamed_cluster_with_change_rate(
        1,
        dir.path(),
        StreamSealKnobs {
            seal_bytes: 10_000_000,
            seal_age: Duration::from_secs(3600),
        },
        10_000,
    )
    .await;
    await_bootstrap(&nodes).await;

    // The streamed table.
    let (status, body) = dynamo(
        nodes[0].dynamo_addr(),
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"hot_stream",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable(hot_stream) failed: {body}");

    // A plain, unstreamed table — otherwise identical treatment.
    let (status, body) = dynamo(
        nodes[0].dynamo_addr(),
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"plain_table",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable(plain_table) failed: {body}");

    // The SAME sizable burst against both tables — 60 items of ~2 KB each
    // (~120 KB total), written as fast as the test can issue them (well
    // under the `INDEX_DRAIN_INTERVAL` scale this needs to look "bursty"
    // against).
    let filler = "x".repeat(2_000);
    for i in 0..60u32 {
        for table in ["hot_stream", "plain_table"] {
            let (status, body) = dynamo(
                nodes[0].dynamo_addr(),
                "DynamoDB_20120810.PutItem",
                &format!(
                    r#"{{"TableName":"{table}","Item":{{"id":{{"S":"i{i:04}"}},"body":{{"S":"{filler}"}}}}}}"#
                ),
            )
            .await;
            assert_eq!(status, 200, "PutItem({table}, i{i}) failed: {body}");
        }
    }

    // The streamed table's own tablet count must reach 2 — the change-rate
    // trigger fired.
    await_true(
        20,
        "hot_stream never auto-split on its own change-append rate",
        || tablets_for(&nodes[0].metadata(), "hot_stream").len() >= 2,
    )
    .await;

    // Meanwhile — over a comparable window — the plain table must NEVER
    // gain a second tablet: no byte/key threshold is configured, and the
    // change-rate tracker was never populated for an unstreamed tablet in
    // the first place. A converged-or-timeout window that fails the
    // instant a split is observed (never a fixed sleep followed by one
    // assertion), matching this crate's own negative-property discipline.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let plain_tablets = tablets_for(&nodes[0].metadata(), "plain_table").len();
        assert_eq!(
            plain_tablets, 1,
            "an unstreamed table must never be split by --auto-split-change-rate"
        );
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Every shard object `DescribeStream` currently lists for `stream_arn`,
/// across pagination — the raw JSON shard entries, for tests asserting the
/// wire *shape* (`SequenceNumberRange.EndingSequenceNumber`,
/// `ParentShardId`) rather than just the tablet-id set
/// ([`stream_tablet_ids`]'s narrower digest).
async fn all_stream_shards(addr: SocketAddr, stream_arn: &str) -> Vec<Value> {
    let mut shards = Vec::new();
    let mut start: Option<String> = None;
    loop {
        let start_clause = start
            .as_ref()
            .map(|s| format!(r#","ExclusiveStartShardId":"{s}""#))
            .unwrap_or_default();
        let (status, resp) = dynamo(
            addr,
            "DynamoDBStreams_20120810.DescribeStream",
            &format!(r#"{{"StreamArn":"{stream_arn}"{start_clause}}}"#),
        )
        .await;
        assert_eq!(status, 200, "DescribeStream (shard shape) failed: {resp}");
        let v = json(&resp);
        shards.extend(
            v["StreamDescription"]["Shards"]
                .as_array()
                .cloned()
                .unwrap_or_default(),
        );
        match v["StreamDescription"]["LastEvaluatedShardId"].as_str() {
            Some(next) => start = Some(next.to_owned()),
            None => break,
        }
    }
    shards
}

/// The cascade e2e (ADR 0050 Train B rung 6): a streamed table walked by a
/// consumer across (a) a routine seal, (b) one completed copy-based split
/// (the root retired, its final shard closed), and (c) a SECOND generation
/// (both children split again — the grandparent chain), asserting the wire
/// *shape* the other lineage tests don't pin: every retired ancestor's
/// shard closed (`SequenceNumberRange.EndingSequenceNumber` present),
/// exactly one open shard per routable tablet, `ParentShardId` links
/// walking a grandchild transitively back into the root's own chain, a
/// closed shard's `TRIM_HORIZON` drain ending in a null
/// `NextShardIterator`, and exactly-once delivery (distinct `eventID`s)
/// across the full three-generation lineage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cascade_split_walks_the_grandparent_chain_with_closed_shard_shape() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster(3, dir.path(), tiny_seal_knobs()).await;
    await_bootstrap(&nodes).await;

    let (status, body) = dynamo(
        nodes[0].dynamo_addr(),
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"casc",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/casc/stream/{label}");

    let mut expected = 0usize;
    let put = |from: usize, upto: usize| {
        let nodes = &nodes;
        async move {
            let mut n = 0usize;
            for i in from..upto {
                let id = format!("c{i:04}");
                let issuer = &nodes[i % nodes.len()];
                let (status, body) = dynamo(
                    issuer.dynamo_addr(),
                    "DynamoDB_20120810.PutItem",
                    &format!(r#"{{"TableName":"casc","Item":{{"id":{{"S":"{id}"}}}}}}"#),
                )
                .await;
                assert_eq!(status, 200, "PutItem({id}) failed: {body}");
                n += 1;
            }
            n
        }
    };
    expected += put(0, 40).await;

    // (a) a routine seal on the root before anything splits.
    let root = tablets_for(&nodes[0].metadata(), "casc")
        .into_iter()
        .next()
        .expect("bootstrap tablet exists");
    await_true(20, "the root never routine-sealed an epoch", || {
        nodes[0]
            .metadata()
            .stream_shards
            .range((root, 0)..=(root, u64::MAX))
            .next()
            .is_some()
    })
    .await;

    // (b) generation 1: grow retires the root, activates 2 children.
    let (status, body) = admin(
        nodes[1].admin_addr(),
        "POST",
        "/admin/stream/grow",
        Some(r#"{"table":"casc"}"#),
    )
    .await;
    assert_eq!(status, 200, "grow #1 failed: {body}");
    assert_eq!(
        body["split_count"].as_u64(),
        Some(1),
        "root must split: {body}"
    );
    await_true(30, "root never retired / children never activated", || {
        nodes.iter().all(|n| {
            let m = n.metadata();
            !m.tablets.contains_key(&root) && tablets_for(&m, "casc").len() == 2
        })
    })
    .await;
    let children = tablets_for(&nodes[0].metadata(), "casc");
    expected += put(40, 80).await;

    // (c) generation 2: both children split — grandchildren, root = the
    // grandparent. A `grow` issued mid-nothing must also classify cleanly
    // (covered separately below with a mid-split grow).
    let (status, body) = admin(
        nodes[2].admin_addr(),
        "POST",
        "/admin/stream/grow",
        Some(r#"{"table":"casc"}"#),
    )
    .await;
    assert_eq!(status, 200, "grow #2 failed: {body}");
    assert_eq!(
        body["split_count"].as_u64(),
        Some(2),
        "both children must split: {body}"
    );
    await_true(
        30,
        "children never retired / grandchildren never active",
        || {
            nodes.iter().all(|n| {
                let m = n.metadata();
                children.iter().all(|c| !m.tablets.contains_key(c))
                    && tablets_for(&m, "casc").len() == 4
            })
        },
    )
    .await;
    expected += put(80, 120).await;

    // The wire shape, from a node that served neither grow call.
    let meta = nodes[0].metadata();
    let shards = all_stream_shards(nodes[0].dynamo_addr(), &stream_arn).await;
    let mut open_count = 0usize;
    for shard in &shards {
        let shard_id = shard["ShardId"].as_str().expect("ShardId present");
        let tablet = shard_id
            .strip_prefix("shardId-")
            .and_then(|r| r.split_once('-'))
            .and_then(|(t, _)| t.parse::<u64>().ok())
            .expect("parseable shard id");
        let closed = !shard["SequenceNumberRange"]["EndingSequenceNumber"].is_null();
        if meta.tablets.contains_key(&TabletId(tablet)) {
            if !closed {
                open_count += 1;
            }
        } else {
            assert!(
                closed,
                "a retired tablet's every listed shard must be CLOSED \
                 (EndingSequenceNumber present): {shard}"
            );
        }
    }
    assert_eq!(
        open_count, 4,
        "exactly one open shard per routable (grandchild) tablet"
    );

    // ParentShardId transitivity: a grandchild's epoch-0 shard names its
    // own retired parent's FINAL shard; that shard's entry in turn links
    // onward — following the links from any grandchild must reach one of
    // the ROOT tablet's own shards (the grandparent chain, walked purely
    // through the wire listing).
    let by_id: std::collections::BTreeMap<&str, &Value> = shards
        .iter()
        .map(|s| (s["ShardId"].as_str().unwrap(), s))
        .collect();
    let grandchild = tablets_for(&meta, "casc")[0];
    let mut cursor = format!("shardId-{}-0", grandchild.0);
    let mut hops = 0usize;
    let reached_root = loop {
        let Some(parent) = by_id
            .get(cursor.as_str())
            .and_then(|s| s["ParentShardId"].as_str())
        else {
            break false;
        };
        if parent.starts_with(&format!("shardId-{}-", root.0)) {
            break true;
        }
        cursor = parent.to_owned();
        hops += 1;
        assert!(hops < 64, "ParentShardId walk must terminate");
    };
    assert!(
        reached_root,
        "a grandchild's ParentShardId chain must reach the root (grandparent) tablet's own shards"
    );

    // A closed shard drains to a null NextShardIterator: the root's final
    // (highest-epoch) shard, from TRIM_HORIZON, must exhaust.
    let root_final = shards
        .iter()
        .filter_map(|s| {
            let id = s["ShardId"].as_str()?;
            let (t, e) = id.strip_prefix("shardId-")?.split_once('-')?;
            (t.parse::<u64>().ok()? == root.0).then(|| (e.parse::<u64>().ok().unwrap_or(0), id))
        })
        .max_by_key(|(e, _)| *e)
        .expect("the root must have listed shards")
        .1;
    let mut iterator = get_shard_iterator(
        nodes[2].dynamo_addr(),
        &stream_arn,
        root_final,
        "TRIM_HORIZON",
    )
    .await;
    let mut drained = 0usize;
    loop {
        let (records, next) = get_records(nodes[2].dynamo_addr(), &iterator).await;
        drained += records.len();
        match next {
            Some(n) => iterator = n,
            None => break, // the closed-shard contract: it nulls
        }
    }
    assert!(
        drained > 0,
        "the root's final shard must hold its pre-cutover backlog"
    );

    // Exactly-once across all three generations, walked live.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    let delivered = drain_all_tablets_lineage(
        nodes[1].dynamo_addr(),
        &stream_arn,
        &nodes[1],
        &tablets_for(&nodes[1].metadata(), "casc"),
        expected,
        deadline,
    )
    .await;
    assert_eq!(
        delivered.len(),
        expected,
        "exactly-once delivery must hold across the cascade"
    );
    let event_ids: BTreeSet<&str> = delivered
        .iter()
        .map(|r| r["eventID"].as_str().expect("eventID present"))
        .collect();
    assert_eq!(
        event_ids.len(),
        expected,
        "every delivered record must carry a distinct eventID"
    );

    // The mid-split grow classification (rung 6's `grow_stream` refinement):
    // kick a THIRD grow and, while its splits are in flight, issue another —
    // any tablet already `Splitting` must classify as a skip, never as a
    // fresh split and never an error.
    let (status, body) = admin(
        nodes[0].admin_addr(),
        "POST",
        "/admin/stream/grow",
        Some(r#"{"table":"casc"}"#),
    )
    .await;
    assert_eq!(status, 200, "grow #3 failed: {body}");
    let (status, body) = admin(
        nodes[1].admin_addr(),
        "POST",
        "/admin/stream/grow",
        Some(r#"{"table":"casc"}"#),
    )
    .await;
    assert_eq!(status, 200, "grow #4 (mid-split) failed: {body}");
    assert_eq!(
        body["error_count"].as_u64(),
        Some(0),
        "a mid-split tablet must classify as a skip, never an error: {body}"
    );
}

// ---------------------------------------------------------------------------
// Rung 8 acceptance (a): the multi-split soak — one streamed + GSI'd table
// through ≥3 auto-split cutovers under continuous mixed load.
// ---------------------------------------------------------------------------

/// ADR 0050 Train B rung 8's named acceptance soak. A populated streamed +
/// GSI'd table auto-splits repeatedly (tiny byte threshold) while plain
/// writes, `TransactWriteItems`, and GSI queries race the workflows from
/// every node. Asserts, at the end: zero lost writes (every acked item
/// reads back), exactly-once stream delivery via the full lineage walk
/// (retired parents included), GSI convergence to exactly one row per
/// item, and every retired parent's per-tablet engine physically deleted
/// from every node's dir (`db-t{parent}-*` gone — the B1 file-deletion
/// teardown observed end to end). The GSI-convergence + reclaim pair is
/// also the closure check for the `split-child-gsi-cursor-unreadable`
/// memory bug class: children's drains start clean (RestartFromScratch at
/// activation) and no retired parent's watermark row survives to pollute
/// anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_split_soak_streamed_gsi_table_under_mixed_load() {
    tokio::time::timeout(Duration::from_secs(300), async {
        let dir = tempfile::TempDir::new().unwrap();
        let nodes = start_streamed_cluster_full(
            3,
            dir.path(),
            tiny_seal_knobs(),
            None,
            Some(2_048),
            SegmentStoreConfig::default(),
        )
        .await;
        await_bootstrap(&nodes).await;

        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"soak",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                "StreamSpecification":{"StreamEnabled":true,
                    "StreamViewType":"NEW_AND_OLD_IMAGES"},
                "GlobalSecondaryIndexes":[
                    {"IndexName":"by-tag",
                     "KeySchema":[{"AttributeName":"tag","KeyType":"HASH"}],
                     "Projection":{"ProjectionType":"ALL"}}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");
        let label = field(&body, "LatestStreamLabel");
        let stream_arn = format!("arn:aws:dynamodb:animus:0:table/soak/stream/{label}");

        // Mixed load, round-robin across nodes: 120 plain puts, every 10th
        // iteration ALSO a two-item transaction, every 15th a GSI query.
        // Every item carries one of 4 tags + a 256B filler (so the 2KiB
        // auto-split threshold fires early and often — a cascade of
        // cutovers, not one).
        let filler = "x".repeat(256);
        let mut ids: Vec<String> = Vec::new();
        for i in 0..120usize {
            let issuer = &nodes[i % nodes.len()];
            let id = format!("e{i:04}");
            let (status, body) = dynamo(
                issuer.dynamo_addr(),
                "DynamoDB_20120810.PutItem",
                &format!(
                    r#"{{"TableName":"soak","Item":{{"id":{{"S":"{id}"}},"tag":{{"S":"t{}"}},"body":{{"S":"{filler}"}}}}}}"#,
                    i % 4
                ),
            )
            .await;
            assert_eq!(status, 200, "PutItem({id}) failed: {body}");
            ids.push(id);
            if i % 10 == 9 {
                let (a, b) = (format!("x{i:04}a"), format!("x{i:04}b"));
                let (status, body) = dynamo(
                    issuer.dynamo_addr(),
                    "DynamoDB_20120810.TransactWriteItems",
                    &format!(
                        r#"{{"TransactItems":[
                            {{"Put":{{"TableName":"soak","Item":{{"id":{{"S":"{a}"}},"tag":{{"S":"t0"}}}}}}}},
                            {{"Put":{{"TableName":"soak","Item":{{"id":{{"S":"{b}"}},"tag":{{"S":"t1"}}}}}}}}]}}"#
                    ),
                )
                .await;
                assert_eq!(status, 200, "transact({a},{b}) failed: {body}");
                ids.push(a);
                ids.push(b);
            }
            if i % 15 == 14 {
                // A live GSI read mid-workflow — result content converges
                // later; mid-flight it only has to serve.
                let (status, _) = dynamo(
                    issuer.dynamo_addr(),
                    "DynamoDB_20120810.Query",
                    r#"{"TableName":"soak","IndexName":"by-tag",
                        "KeyConditionExpression":"tag = :t",
                        "ExpressionAttributeValues":{":t":{"S":"t0"}}}"#,
                )
                .await;
                assert_eq!(status, 200, "mid-flight GSI query failed");
            }
        }
        let expected = ids.len();

        // ≥3 completed cutovers (retired parents recorded in lineage), and
        // the surviving topology converged on every node.
        await_true(60, "fewer than 3 cutovers ever completed", || {
            let meta = nodes[0].metadata();
            let parents: BTreeSet<TabletId> =
                meta.split_lineage.values().map(|l| l.parent).collect();
            parents.iter().filter(|p| !meta.tablets.contains_key(p)).count() >= 3
        })
        .await;
        let retired: Vec<TabletId> = {
            let meta = nodes[0].metadata();
            meta.split_lineage
                .values()
                .map(|l| l.parent)
                .filter(|p| !meta.tablets.contains_key(p))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        };

        // 1. Zero lost writes: every acked item reads back through a node
        //    that didn't originate most of them.
        for id in &ids {
            let (status, body) = dynamo(
                nodes[2].dynamo_addr(),
                "DynamoDB_20120810.GetItem",
                &format!(r#"{{"TableName":"soak","Key":{{"id":{{"S":"{id}"}}}}}}"#),
            )
            .await;
            assert_eq!(status, 200, "GetItem({id}) failed");
            assert!(
                body.contains(&format!("\"S\":\"{id}\"")),
                "acked write {id} lost across the soak's cutovers: {body}"
            );
        }

        // 2. Exactly-once stream delivery over the full lineage (live
        //    tablets + every retired parent; the walk self-discovers any
        //    generation it wasn't seeded with).
        let seed_tablets: Vec<TabletId> = {
            let meta = nodes[1].metadata();
            let mut ts = tablets_for(&meta, "soak");
            ts.extend(retired.iter().copied());
            ts
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let delivered = drain_all_tablets_lineage(
            nodes[1].dynamo_addr(),
            &stream_arn,
            &nodes[1],
            &seed_tablets,
            expected,
            deadline,
        )
        .await;
        assert_eq!(
            delivered.len(),
            expected,
            "exactly-once delivery must hold across every soak cutover"
        );

        // 3. GSI convergence: the four tags' Counts sum to exactly one row
        //    per item (children's drains restarted clean at activation).
        await_true(60, "GSI never converged to one row per item", || {
            // `await_true` takes a sync closure — sample via a blocking
            // one-shot runtime handle instead: issue the four queries on
            // the current runtime through block_in_place.
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let mut total = 0u64;
                    for t in 0..4 {
                        let (status, body) = dynamo(
                            nodes[1].dynamo_addr(),
                            "DynamoDB_20120810.Query",
                            &format!(
                                r#"{{"TableName":"soak","IndexName":"by-tag",
                                    "KeyConditionExpression":"tag = :t",
                                    "ExpressionAttributeValues":{{":t":{{"S":"t{t}"}}}}}}"#
                            ),
                        )
                        .await;
                        if status != 200 {
                            return false;
                        }
                        let v: serde_json::Value =
                            serde_json::from_str(&body).unwrap_or_default();
                        total += v["Count"].as_u64().unwrap_or(0);
                    }
                    total == expected as u64
                })
            })
        })
        .await;

        // 4. Every retired parent's per-tablet engine physically deleted
        //    from every node dir (B1's file-deletion teardown, end to end).
        await_true(30, "a retired parent's engine files survived", || {
            (0..nodes.len()).all(|n| {
                let node_dir = dir.path().join(format!("node-{n}"));
                let Ok(entries) = std::fs::read_dir(&node_dir) else {
                    return true;
                };
                let names: Vec<String> = entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect();
                retired.iter().all(|p| {
                    let prefix = format!("db-t{}-", p.0);
                    let wal = format!("raftkv.wal.{}", p.0);
                    names.iter().all(|f| !f.starts_with(&prefix) && *f != wal)
                })
            })
        })
        .await;
    })
    .await
    .expect("soak timed out");
}
