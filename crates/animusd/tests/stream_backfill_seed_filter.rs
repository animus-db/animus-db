//! ADR 0045 follow-up "E1" (closed by this PR): a table streamed **while** a
//! GSI backfill runs must never surface the backfill seeder's own synthetic,
//! image-less dirty marker (`animusd::index_drain::backfill_seed_tick`) as a
//! phantom `GetRecords` event. Real DynamoDB emits **no** stream event at
//! all for a GSI backfill's own coverage sweep over pre-existing data — a
//! seeded record decodes fine (it is a legitimate, well-formed
//! `ChangeRecord`) but, before this fix, carried neither image and an empty
//! `Keys` (AWS documents `Keys` as *always* present), so it read as an
//! invalid, fabricated event no real client would ever see.
//!
//! Scenario: a table with several pre-existing rows across distinct
//! partitions, a stream already enabled, then a GSI added via the real
//! `UpdateTable` wire path (`tests/update_table_create_index.rs`'s own
//! `create_index_via_wire` helper) so a backfill runs against those
//! pre-existing rows while the stream is live — concurrent live writes are
//! also issued while the backfill is in flight, mirroring
//! `tests/backfill_seeder.rs`'s "live writes racing the sweep" scenario, to
//! prove the fix filters *only* seed markers, never a real write.
//!
//! **Both `GetRecords` serve paths get their own test** (issue #267's ask —
//! the filter is one shared predicate, `ChangeRecord::consumer_hidden`, but
//! a shared predicate only proves the paths *agree*, not that each is
//! actually reached with a seed record in hand):
//!
//! - [`backfill_seed_markers_never_surface_as_phantom_stream_events`] pins
//!   the **open-tail** path: seal knobs tuned to never fire, so the table's
//!   one open shard serves every record straight from the hot change log.
//! - [`backfill_seed_markers_never_surface_from_sealed_shards_either`] pins
//!   the **sealed** path: aggressive seal knobs so the seal arm sweeps the
//!   whole log — seed markers included, by design ("hiding is a serve-time
//!   decision", `docs/streams-notes.md`) — into sealed segments, and the
//!   walk converges only once every real event is served from a *sealed*
//!   shard's segment decode (`dynamo_streams::get_records_sealed`).
//!
//! Each test drains its shard lineage to convergence and asserts (a) zero
//! delivered records have the phantom shape (empty `Keys`, no images) and
//! (b) every real write — pre-existing and concurrent alike — is delivered
//! exactly once, no more, no less.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_control::IndexStatus;
use animusd::{
    Node, SegmentStoreConfig, StorageBackend, StreamSealKnobs, bind_cluster,
    start_cluster_with_streams,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, timeout};

mod support;

/// Seal knobs tuned to never fire during this test's short lifetime — the
/// table's one tablet keeps exactly one **open** shard throughout, so the
/// test never needs to walk a sealed/open lineage (`tests/streams_e2e.rs`'s
/// `drain_tablet_lineage`); a single `TRIM_HORIZON` iterator, polled to
/// convergence, sees everything.
fn no_seal_knobs() -> StreamSealKnobs {
    StreamSealKnobs {
        seal_bytes: 1_000_000,
        seal_age: Duration::from_secs(3600),
    }
}

/// The sealed-path test's dual: seals almost immediately on any pending
/// byte (`tests/dynamo_streams.rs`'s own `tiny_seal_knobs` convention), so
/// the backfill's seed markers — which the sealer deliberately seals
/// alongside real records — end up inside sealed segments and the phantom
/// question moves to `get_records_sealed`'s segment decode.
fn tiny_seal_knobs() -> StreamSealKnobs {
    StreamSealKnobs {
        seal_bytes: 1,
        seal_age: Duration::from_secs(3600),
    }
}

async fn start_streamed_cluster(n: usize, dir: &Path, knobs: StreamSealKnobs) -> Vec<Node> {
    let bound = bind_cluster(n, "127.0.0.1".parse().unwrap(), dir)
        .await
        .unwrap();
    start_cluster_with_streams(
        bound,
        StorageBackend::default(),
        None,
        Duration::from_secs(600),
        knobs,
        SegmentStoreConfig::default(),
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

/// One DynamoDB JSON request over the real HTTP wire (duplicated per this
/// codebase's own "every sibling test file keeps its own copy" convention).
async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let request = format!(
        "POST / HTTP/1.1\r\nHost: animus\r\nX-Amz-Target: {target}\r\n\
         Connection: close\r\n\
         Content-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.expect("send");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    (status, payload.to_owned())
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

async fn put_item(addr: SocketAddr, table: &str, id: &str, cat: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        &format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"cat":{{"S":"{cat}"}}}}}}"#
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

/// `UpdateTable` with a single `GlobalSecondaryIndexUpdates` `Create`
/// element (duplicated from `tests/update_table_create_index.rs`'s own
/// `create_index_via_wire`) — the real wire path that triggers a backfill on
/// a populated table.
async fn create_index_via_wire(addr: SocketAddr, table: &str, index: &str, hash_attr: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "GlobalSecondaryIndexUpdates":[{{"Create":{{
                    "IndexName":"{index}",
                    "KeySchema":[{{"AttributeName":"{hash_attr}","KeyType":"HASH"}}],
                    "Projection":{{"ProjectionType":"ALL"}}}}}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "UpdateTable(create index) failed: {body}");
}

async fn await_index_active(nodes: &[Node], table: &str, index: &str) {
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
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("index never reached Active within 60s");
}

async fn get_shard_iterator(addr: SocketAddr, stream_arn: &str, shard_id: &str) -> String {
    let body = format!(
        r#"{{"StreamArn":"{stream_arn}","ShardId":"{shard_id}","ShardIteratorType":"TRIM_HORIZON"}}"#
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

/// Poll the table's one (never-sealed) open shard from `iterator` until no
/// new record has arrived for several consecutive polls — the open-shard
/// contract never nulls `NextShardIterator` (F4/§7: "not there yet, poll
/// again"), so convergence must be judged by a stable count, not a null.
async fn drain_open_shard_to_convergence(addr: SocketAddr, iterator: &str) -> Vec<Value> {
    let mut collected: Vec<Value> = Vec::new();
    let mut cur = iterator.to_owned();
    let mut stable_polls = 0;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (records, next) = get_records(addr, &cur).await;
        if let Some(next) = next {
            cur = next;
        }
        if records.is_empty() {
            stable_polls += 1;
        } else {
            stable_polls = 0;
            collected.extend(records);
        }
        if stable_polls >= 10 {
            return collected;
        }
        if Instant::now() >= deadline {
            panic!(
                "open shard never converged within 30s (collected so far: {})",
                collected.len()
            );
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// How many delivered records name partition-key value `id` in their own
/// `dynamodb.Keys` — a real write's `Keys` always carries the base table's
/// partition key (`keys_from_images`), so this both counts real deliveries
/// and (via the caller's own separate phantom-shape assertion) is never
/// satisfied by a seed marker.
fn count_for_id(records: &[Value], id: &str) -> usize {
    records
        .iter()
        .filter(|r| r["dynamodb"]["Keys"]["id"]["S"] == id)
        .count()
}

/// The phantom shape: an empty `Keys` (or, equivalently, neither image
/// present) must never appear in a delivered record — that is exactly and
/// only what an unfiltered backfill seed marker decodes to.
fn assert_no_phantom_shape(records: &[Value]) {
    for r in records {
        let keys = r["dynamodb"]["Keys"]
            .as_object()
            .unwrap_or_else(|| panic!("record has no `Keys` object at all: {r}"));
        assert!(
            !keys.is_empty(),
            "phantom event with an empty `Keys` field surfaced: {r}"
        );
        let has_image =
            r["dynamodb"].get("OldImage").is_some() || r["dynamodb"].get("NewImage").is_some();
        assert!(
            has_image,
            "phantom event with no image at all surfaced: {r}"
        );
    }
}

/// One full `TRIM_HORIZON` walk of the stream's current shard lineage,
/// with the delivered events split by which serve path produced them:
/// events read from shards `DescribeStream` reported **sealed** (an
/// `EndingSequenceNumber` present — `get_records_sealed`'s segment decode)
/// vs from the still-open tail (the hot-read path). A shard that seals
/// between the describe and its own drain is served through the sealed
/// resolution regardless (the iterator-survives-seal property) — it is
/// only *attributed* to the open bucket, which the caller's convergence
/// condition (open bucket empty) makes moot on the walk that decides.
async fn walk_lineage(addr: SocketAddr, stream_arn: &str) -> (Vec<Value>, Vec<Value>) {
    let (status, body) = dynamo(
        addr,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{stream_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "DescribeStream failed: {body}");
    let shards = json(&body)["StreamDescription"]["Shards"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut sealed_events: Vec<Value> = Vec::new();
    let mut open_events: Vec<Value> = Vec::new();
    for shard in &shards {
        let shard_id = shard["ShardId"].as_str().expect("ShardId");
        let is_sealed = shard["SequenceNumberRange"]["EndingSequenceNumber"].is_string();
        let mut it = Some(get_shard_iterator(addr, stream_arn, shard_id).await);
        while let Some(iterator) = it {
            let (records, next) = get_records(addr, &iterator).await;
            let drained = records.is_empty();
            if is_sealed {
                sealed_events.extend(records);
            } else {
                open_events.extend(records);
            }
            // A sealed shard drains to a null iterator; an open shard
            // returns the same position on an empty poll — one empty poll
            // ends it either way.
            it = if drained { None } else { next };
        }
    }
    (sealed_events, open_events)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backfill_seed_markers_never_surface_as_phantom_stream_events() {
    let dir = support::panic_safe_tempdir();
    let nodes = start_streamed_cluster(1, dir.path(), no_seal_knobs()).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();
    let table = "orders";

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "StreamSpecification":{{"StreamEnabled":true,
                    "StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{label}");

    // Five pre-existing partitions, written *before* the GSI (and its
    // backfill) ever exist — exactly what `backfill_seed_tick` sweeps.
    let pre_existing: Vec<String> = (0..5).map(|i| format!("p{i}")).collect();
    for id in &pre_existing {
        put_item(addr, table, id, &format!("cat-{id}")).await;
    }

    // Mint the iterator from the very start of the stream now, before the
    // backfill (and its seed markers) even begins.
    let (status, body) = dynamo(
        addr,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{stream_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "DescribeStream failed: {body}");
    let shards = json(&body)["StreamDescription"]["Shards"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(shards.len(), 1, "expected exactly one (open) shard: {body}");
    let shard_id = shards[0]["ShardId"].as_str().unwrap().to_owned();
    let iterator = get_shard_iterator(addr, &stream_arn, &shard_id).await;

    // Add the GSI over the real `UpdateTable` wire path — this is what
    // starts `backfill_seed_tick` sweeping the five pre-existing partitions
    // above and seeding one image-less marker per partition.
    create_index_via_wire(addr, table, "by-cat", "cat").await;

    // Genuine concurrent writes racing the backfill (mirrors
    // `tests/backfill_seeder.rs`'s "live writes during backfill" scenario):
    // two brand-new partitions, a modify of an existing one, and a delete of
    // another. Every one of these must be delivered — the fix must filter
    // *only* seed markers, never a real write.
    put_item(addr, table, "p5", "cat-p5").await;
    put_item(addr, table, "p6", "cat-p6").await;
    put_item(addr, table, "p0", "cat-p0-updated").await; // MODIFY on p0
    delete_item(addr, table, "p1").await; // REMOVE on p1

    await_index_active(&nodes, table, "by-cat").await;

    let delivered = drain_open_shard_to_convergence(addr, &iterator).await;

    // (a) The phantom shape must never appear — see `assert_no_phantom_shape`.
    assert_no_phantom_shape(&delivered);

    // (b) Every real write, and only real writes, delivered exactly once:
    // 5 pre-existing inserts + 2 new inserts + 1 modify + 1 delete = 9. A
    // stray seed marker would inflate this past 9; a filter bug eating a
    // real write would deflate it below.
    assert_eq!(
        delivered.len(),
        9,
        "expected exactly 9 real events, got {}: {delivered:#?}",
        delivered.len()
    );
    for id in &pre_existing {
        let want = if id == "p0" || id == "p1" { 2 } else { 1 };
        assert_eq!(
            count_for_id(&delivered, id),
            want,
            "wrong delivery count for pre-existing partition {id}: {delivered:#?}"
        );
    }
    assert_eq!(count_for_id(&delivered, "p5"), 1, "{delivered:#?}");
    assert_eq!(count_for_id(&delivered, "p6"), 1, "{delivered:#?}");

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

/// The **sealed** serve path's dual of the test above (issue #267): with
/// `tiny_seal_knobs` the seal arm sweeps the change log — the backfill's
/// seed markers included, deliberately ("hiding is a serve-time decision",
/// `docs/streams-notes.md`) — into sealed segments, so an unfiltered seed
/// marker would surface through `get_records_sealed`'s segment decode
/// rather than the open tail's hot read. Identical scenario (pre-existing
/// rows, stream live, GSI added mid-flight, concurrent writes racing the
/// sweep); the walk converges only once every real event is served from a
/// *sealed* shard and the open tail is empty — proving the sealed branch
/// filtered the seed markers while delivering every real write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backfill_seed_markers_never_surface_from_sealed_shards_either() {
    let dir = support::panic_safe_tempdir();
    let nodes = start_streamed_cluster(1, dir.path(), tiny_seal_knobs()).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();
    let table = "orders_sealed";

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "StreamSpecification":{{"StreamEnabled":true,
                    "StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{label}");

    // Five pre-existing partitions, written *before* the GSI (and its
    // backfill) ever exist — exactly what `backfill_seed_tick` sweeps.
    let pre_existing: Vec<String> = (0..5).map(|i| format!("p{i}")).collect();
    for id in &pre_existing {
        put_item(addr, table, id, &format!("cat-{id}")).await;
    }

    // Add the GSI over the real `UpdateTable` wire path — this starts
    // `backfill_seed_tick` seeding one image-less marker per pre-existing
    // partition, racing the aggressive seal arm.
    create_index_via_wire(addr, table, "by-cat", "cat").await;

    // Genuine concurrent writes racing both the backfill sweep and the seal
    // arm — every one must be delivered; the filter must eat only seed
    // markers.
    put_item(addr, table, "p5", "cat-p5").await;
    put_item(addr, table, "p6", "cat-p6").await;
    put_item(addr, table, "p0", "cat-p0-updated").await; // MODIFY on p0
    delete_item(addr, table, "p1").await; // REMOVE on p1

    await_index_active(&nodes, table, "by-cat").await;

    // Converge: with `seal_bytes: 1` every pending record — real and seeded
    // alike — is eventually sealed, so the walk must end with all 9 real
    // events served from sealed shards and the open tail empty. Judged by
    // repeated full-lineage walks (the house converged-or-timeout idiom):
    // a transiently short count just re-polls; a count *past* 9 can only
    // mean a phantom leaked (or a real event was double-delivered) and
    // fails immediately rather than timing out ambiguously.
    let deadline = Instant::now() + Duration::from_secs(60);
    let delivered = loop {
        let (sealed, open) = walk_lineage(addr, &stream_arn).await;
        assert_no_phantom_shape(&sealed);
        assert_no_phantom_shape(&open);
        assert!(
            sealed.len() + open.len() <= 9,
            "more events than the 9 real writes were delivered — a seed \
             marker leaked through a serve path, or a real event was \
             double-delivered: sealed={sealed:#?} open={open:#?}"
        );
        if sealed.len() == 9 && open.is_empty() {
            break sealed;
        }
        if Instant::now() >= deadline {
            panic!(
                "the lineage never converged to all 9 real events sealed \
                 within 60s (sealed={}, open={})",
                sealed.len(),
                open.len()
            );
        }
        sleep(Duration::from_millis(50)).await;
    };

    // Every real write, and only real writes, delivered exactly once — the
    // same accounting as the open-path test, now entirely off sealed
    // segments that physically contain the seed markers.
    for id in &pre_existing {
        let want = if id == "p0" || id == "p1" { 2 } else { 1 };
        assert_eq!(
            count_for_id(&delivered, id),
            want,
            "wrong delivery count for pre-existing partition {id}: {delivered:#?}"
        );
    }
    assert_eq!(count_for_id(&delivered, "p5"), 1, "{delivered:#?}");
    assert_eq!(count_for_id(&delivered, "p6"), 1, "{delivered:#?}");

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}
