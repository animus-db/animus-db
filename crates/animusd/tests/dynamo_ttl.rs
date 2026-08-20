//! End-to-end tests for DynamoDB-style TTL (ADR 0051) over the real
//! DynamoDB JSON/HTTP wire: `UpdateTimeToLive`/`DescribeTimeToLive`, the
//! reaper's AWS-faithful "visible until reaped" contract, the 5-year
//! safety window, the conditional-delete outcome, and the TTL-deletion
//! stream `userIdentity`. Real time/sockets (the `ProdEnv` edge) — every
//! eventual property is a converged-or-timeout poll, never a fixed sleep
//! (this codebase's own testing discipline).
//!
//! The `UpdateTimeToLive` follower-relay regression (`is_relayable_command`
//! must allow `MetaCommand::SetTableTtl`) lives in
//! `tests/schema_ddl_relay.rs`, mirroring that file's own DDL-relay suite —
//! not duplicated here.

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use animusd::config::NodeRole;
use animusd::{ClusterConfig, Node, RoleAddrs, StorageBackend};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// A fast TTL sweep interval so a test never has to wait out the real
/// production default (`ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL`, on the
/// order of a minute) — this codebase's own testing discipline (never wait
/// out a production-scale interval; see `streams_e2e.rs`'s
/// `tiny_seal_knobs` for the identical idea applied to Streams).
const TEST_TTL_SWEEP_INTERVAL: Duration = Duration::from_millis(300);

/// Bring up a single node with [`TEST_TTL_SWEEP_INTERVAL`] instead of the
/// production default, retrying the port-TOCTOU race exactly like
/// `support::start_single_node` does (that helper always uses the
/// production interval, so this file needs its own copy through
/// `run_node_with_ttl_sweep_interval`).
async fn start_single_node_fast_ttl(dir: &Path) -> (Node, ClusterConfig) {
    let mut last_err = None;
    for attempt in 0..10 {
        let addrs = support::free_addrs(7);
        let config = ClusterConfig {
            nodes: vec![RoleAddrs {
                id: animusd::config::node_id(0),
                role: NodeRole::Both,
                internal: addrs[0],
                client: addrs[1],
                dynamo: addrs[2],
                cql: addrs[3],
                admin: addrs[4],
                intra: addrs[5],
                console: addrs[6],
            }],
        };
        match animusd::run_node_with_ttl_sweep_interval(
            &config,
            0,
            dir,
            StorageBackend::default(),
            TEST_TTL_SWEEP_INTERVAL,
        )
        .await
        {
            Ok(node) => return (node, config),
            Err(e) => {
                last_err = Some(e);
                sleep(Duration::from_millis(50 * (attempt + 1))).await;
            }
        }
    }
    panic!("single node (fast TTL) failed to start after 10 attempts: {last_err:?}");
}

/// One DynamoDB JSON request over a fresh HTTP/1.1 connection → `(status,
/// body)`. Mirrors every other `tests/dynamo_*.rs` file's identical helper.
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

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

/// The current wall-clock epoch second — real `SystemTime`, since these are
/// real `ProdEnv` tests (never `animus_env::Clock` from a test binary; see
/// `ttl_reaper.rs`'s own doc for why `ProdEnv::wall_now()` reads the same
/// clock).
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs()
}

async fn create_table(addr: SocketAddr, table: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}","KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
}

async fn enable_ttl(addr: SocketAddr, table: &str, attribute: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTimeToLive",
        &format!(
            r#"{{"TableName":"{table}","TimeToLiveSpecification":{{"Enabled":true,"AttributeName":"{attribute}"}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "UpdateTimeToLive(enable) failed: {body}");
}

async fn put_item(addr: SocketAddr, table: &str, item_json: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        &format!(r#"{{"TableName":"{table}","Item":{item_json}}}"#),
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");
}

/// `GetItem` by `id`, returning the raw response body — `{}` when absent,
/// `{"Item": {..}}` when present.
async fn get_item(addr: SocketAddr, table: &str, id: &str) -> String {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        &format!(r#"{{"TableName":"{table}","Key":{{"id":{{"S":"{id}"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {body}");
    body
}

async fn item_present(addr: SocketAddr, table: &str, id: &str) -> bool {
    get_item(addr, table, id).await.contains("\"Item\"")
}

/// Poll until `id` is no longer readable, or panic after `deadline`.
async fn await_deleted(addr: SocketAddr, table: &str, id: &str, deadline: Duration) {
    timeout(deadline, async {
        loop {
            if !item_present(addr, table, id).await {
                return;
            }
            sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("`{id}` was never reaped within {deadline:?}"));
}

async fn await_node_bootstrap(node: &Node) {
    timeout(Duration::from_secs(20), async {
        loop {
            if node.is_control_leader() && !node.metadata().members.is_empty() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("node did not bootstrap within 20s");
}

/// `UpdateTimeToLive` enable → `DescribeTimeToLive` reports `ENABLED` plus
/// the attribute name; disable → `DISABLED` with **no** `AttributeName`
/// (ADR 0051 §2, matching AWS's own omission rule).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_time_to_live_enable_and_disable_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let (node, config) =
        support::start_single_node(&dir.path().join("n"), StorageBackend::default()).await;
    await_node_bootstrap(&node).await;
    let addr = config.nodes[0].dynamo;
    create_table(addr, "t").await;

    enable_ttl(addr, "t", "expiresAt").await;
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DescribeTimeToLive",
        r#"{"TableName":"t"}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let desc = json(&body);
    assert_eq!(desc["TimeToLiveDescription"]["TimeToLiveStatus"], "ENABLED");
    assert_eq!(desc["TimeToLiveDescription"]["AttributeName"], "expiresAt");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTimeToLive",
        r#"{"TableName":"t","TimeToLiveSpecification":{"Enabled":false,"AttributeName":"expiresAt"}}"#,
    )
    .await;
    assert_eq!(status, 200, "UpdateTimeToLive(disable) failed: {body}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DescribeTimeToLive",
        r#"{"TableName":"t"}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let desc = json(&body);
    assert_eq!(
        desc["TimeToLiveDescription"]["TimeToLiveStatus"],
        "DISABLED"
    );
    assert!(
        desc["TimeToLiveDescription"].get("AttributeName").is_none(),
        "a disabled table must omit `AttributeName` entirely: {body}"
    );

    node.shutdown_graceful().await;
}

/// Disabling with an `AttributeName` that doesn't match the currently
/// enabled one is rejected client-side (ADR 0051's `UpdateTimeToLive`
/// contract) — never silently accepted or silently disabling the wrong
/// attribute.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disable_with_a_mismatched_attribute_name_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (node, config) =
        support::start_single_node(&dir.path().join("n"), StorageBackend::default()).await;
    await_node_bootstrap(&node).await;
    let addr = config.nodes[0].dynamo;
    create_table(addr, "t").await;
    enable_ttl(addr, "t", "expiresAt").await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTimeToLive",
        r#"{"TableName":"t","TimeToLiveSpecification":{"Enabled":false,"AttributeName":"wrongAttr"}}"#,
    )
    .await;
    assert_eq!(status, 400, "{body}");

    // TTL is still enabled under the original attribute — the rejected
    // call must not have taken effect.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DescribeTimeToLive",
        r#"{"TableName":"t"}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        json(&body)["TimeToLiveDescription"]["TimeToLiveStatus"],
        "ENABLED"
    );

    node.shutdown_graceful().await;
}

/// ADR 0051 §3: an expired item is **AWS-faithfully visible** immediately
/// after its TTL passes — no read path filters it. Uses the *production*
/// sweep interval (a minute) precisely so this assertion cannot race the
/// reaper: `GetItem` runs well within the first interval of node start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_item_is_still_readable_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let (node, config) =
        support::start_single_node(&dir.path().join("n"), StorageBackend::default()).await;
    await_node_bootstrap(&node).await;
    let addr = config.nodes[0].dynamo;
    create_table(addr, "t").await;
    enable_ttl(addr, "t", "expiresAt").await;

    let past = now_secs() - 3600;
    put_item(
        addr,
        "t",
        &format!(r#"{{"id":{{"S":"a"}},"expiresAt":{{"N":"{past}"}}}}"#),
    )
    .await;
    assert!(
        item_present(addr, "t", "a").await,
        "an expired item must stay visible until the reaper actually deletes it"
    );

    node.shutdown_graceful().await;
}

/// The reaper actually deletes an expired item — a converged-or-timeout
/// poll against the fast-sweep node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_item_is_eventually_reaped() {
    let dir = tempfile::tempdir().unwrap();
    let (node, config) = start_single_node_fast_ttl(&dir.path().join("n")).await;
    await_node_bootstrap(&node).await;
    let addr = config.nodes[0].dynamo;
    create_table(addr, "t").await;
    enable_ttl(addr, "t", "expiresAt").await;

    let past = now_secs() - 3600;
    put_item(
        addr,
        "t",
        &format!(r#"{{"id":{{"S":"a"}},"expiresAt":{{"N":"{past}"}}}}"#),
    )
    .await;
    await_deleted(addr, "t", "a", Duration::from_secs(20)).await;

    node.shutdown_graceful().await;
}

/// An item with a future TTL is never deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn future_ttl_item_is_never_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let (node, config) = start_single_node_fast_ttl(&dir.path().join("n")).await;
    await_node_bootstrap(&node).await;
    let addr = config.nodes[0].dynamo;
    create_table(addr, "t").await;
    enable_ttl(addr, "t", "expiresAt").await;

    let future = now_secs() + 3600;
    put_item(
        addr,
        "t",
        &format!(r#"{{"id":{{"S":"a"}},"expiresAt":{{"N":"{future}"}}}}"#),
    )
    .await;
    // Ride out several sweep intervals — a stable negative, not an eventual
    // property to converge on, so a bounded wait-then-assert is the right
    // shape here (mirroring this codebase's other one-shot "must never
    // happen" gates).
    sleep(TEST_TTL_SWEEP_INTERVAL * 6).await;
    assert!(
        item_present(addr, "t", "a").await,
        "a future TTL must never be treated as expired"
    );

    node.shutdown_graceful().await;
}

/// A TTL attribute of the wrong DynamoDB type (`S` instead of `N`) is
/// silently never-expiring, matching AWS.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_type_ttl_attribute_is_never_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let (node, config) = start_single_node_fast_ttl(&dir.path().join("n")).await;
    await_node_bootstrap(&node).await;
    let addr = config.nodes[0].dynamo;
    create_table(addr, "t").await;
    enable_ttl(addr, "t", "expiresAt").await;

    // A `String` far in the "past" by any calendar reading, but the wrong
    // DynamoDB type — `is_expired` must read this as absent, not expired.
    put_item(
        addr,
        "t",
        r#"{"id":{"S":"a"},"expiresAt":{"S":"1999-01-01"}}"#,
    )
    .await;
    sleep(TEST_TTL_SWEEP_INTERVAL * 6).await;
    assert!(
        item_present(addr, "t", "a").await,
        "a wrong-type TTL attribute must never be treated as expired"
    );

    node.shutdown_graceful().await;
}

/// **The most important test in the feature** (ADR 0051 §5): an expiry
/// further than [`animus_dynamo::MAX_PAST_EXPIRY_SECS`] in the past — the
/// signature of a client writing milliseconds where seconds were expected,
/// or otherwise unit-confused — is treated as **not expired**, guarding an
/// entire table against instant mass deletion the moment TTL is enabled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn absurdly_past_ttl_is_never_deleted_the_five_year_safety_window() {
    let dir = tempfile::tempdir().unwrap();
    let (node, config) = start_single_node_fast_ttl(&dir.path().join("n")).await;
    await_node_bootstrap(&node).await;
    let addr = config.nodes[0].dynamo;
    create_table(addr, "t").await;
    enable_ttl(addr, "t", "expiresAt").await;

    // Ten years in the past — well beyond the 5-year window.
    let absurdly_past = now_secs() - 10 * 365 * 24 * 60 * 60;
    put_item(
        addr,
        "t",
        &format!(r#"{{"id":{{"S":"a"}},"expiresAt":{{"N":"{absurdly_past}"}}}}"#),
    )
    .await;
    sleep(TEST_TTL_SWEEP_INTERVAL * 6).await;
    assert!(
        item_present(addr, "t", "a").await,
        "an expiry more than 5 years in the past must never be deleted \
         (the milliseconds-vs-seconds safety guard)"
    );

    node.shutdown_graceful().await;
}

/// The conditional delete's outcome (ADR 0051 §4): an item whose TTL is
/// refreshed to the future before the reaper deletes it survives. Setup
/// (`PutItem` + the refreshing `UpdateItem`) both land well inside the
/// first sweep interval, so the reaper never observes the item at its
/// stale, expired value at all — proving the condition mechanism produces
/// the right *outcome*. (The tighter scan-vs-propose race window itself is
/// a leader-side, sub-millisecond internal race already covered by
/// `animus-cp-data`'s own `KindBatch.conditions` OCC seatbelt tests — not
/// reproducible deterministically from a black-box HTTP client.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refreshed_ttl_survives_the_reaper() {
    let dir = tempfile::tempdir().unwrap();
    let (node, config) = start_single_node_fast_ttl(&dir.path().join("n")).await;
    await_node_bootstrap(&node).await;
    let addr = config.nodes[0].dynamo;
    create_table(addr, "t").await;
    enable_ttl(addr, "t", "expiresAt").await;

    let past = now_secs() - 3600;
    put_item(
        addr,
        "t",
        &format!(r#"{{"id":{{"S":"a"}},"expiresAt":{{"N":"{past}"}}}}"#),
    )
    .await;
    let future = now_secs() + 3600;
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateItem",
        &format!(
            r#"{{"TableName":"t","Key":{{"id":{{"S":"a"}}}},
                "UpdateExpression":"SET expiresAt = :v",
                "ExpressionAttributeValues":{{":v":{{"N":"{future}"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "refreshing UpdateItem failed: {body}");

    sleep(TEST_TTL_SWEEP_INTERVAL * 6).await;
    let body = get_item(addr, "t", "a").await;
    assert!(
        body.contains("\"Item\""),
        "an item refreshed to a future TTL before the reaper reached it must survive: {body}"
    );
    assert!(
        body.contains(&format!("\"expiresAt\":{{\"N\":\"{future}\"}}"))
            || body.contains(&format!("\"N\":\"{future}\"")),
        "the surviving item must carry the refreshed value: {body}"
    );

    node.shutdown_graceful().await;
}

/// ADR 0051 §7: a TTL-reaper delete's stream record carries `userIdentity:
/// {"PrincipalId": "dynamodb.amazonaws.com", "Type": "Service"}`; an
/// ordinary client `DeleteItem` carries none at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_deletion_is_visible_in_the_stream_with_a_service_user_identity() {
    let dir = tempfile::tempdir().unwrap();
    let (node, config) = start_single_node_fast_ttl(&dir.path().join("n")).await;
    await_node_bootstrap(&node).await;
    let addr = config.nodes[0].dynamo;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let stream_arn = json(&body)["TableDescription"]["LatestStreamArn"]
        .as_str()
        .expect("LatestStreamArn")
        .to_owned();
    enable_ttl(addr, "t", "expiresAt").await;

    // `ttl-item`: expired, reaped by the TTL loop.
    let past = now_secs() - 3600;
    put_item(
        addr,
        "t",
        &format!(r#"{{"id":{{"S":"ttl-item"}},"expiresAt":{{"N":"{past}"}}}}"#),
    )
    .await;
    // `client-item`: never expires, deleted by the client itself.
    put_item(addr, "t", r#"{"id":{"S":"client-item"}}"#).await;
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DeleteItem",
        r#"{"TableName":"t","Key":{"id":{"S":"client-item"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "client DeleteItem failed: {body}");

    await_deleted(addr, "t", "ttl-item", Duration::from_secs(20)).await;

    // Walk the open shard's hot tail from TRIM_HORIZON until both REMOVE
    // events are seen (converged-or-timeout: the reaper's delete and this
    // read are two independent asynchronous paths).
    let (status, body) = dynamo(
        addr,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{stream_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let shard_id = json(&body)["StreamDescription"]["Shards"][0]["ShardId"]
        .as_str()
        .expect("at least one shard")
        .to_owned();

    let (status, body) = dynamo(
        addr,
        "DynamoDBStreams_20120810.GetShardIterator",
        &format!(
            r#"{{"StreamArn":"{stream_arn}","ShardId":"{shard_id}","ShardIteratorType":"TRIM_HORIZON"}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let mut iterator = json(&body)["ShardIterator"]
        .as_str()
        .expect("ShardIterator")
        .to_owned();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let (mut ttl_record, mut client_record) = (None, None);
    while (ttl_record.is_none() || client_record.is_none())
        && tokio::time::Instant::now() < deadline
    {
        let (status, body) = dynamo(
            addr,
            "DynamoDBStreams_20120810.GetRecords",
            &format!(r#"{{"ShardIterator":"{iterator}"}}"#),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let v = json(&body);
        for record in v["Records"].as_array().cloned().unwrap_or_default() {
            if record["eventName"] != "REMOVE" {
                continue;
            }
            let pk = record["dynamodb"]["Keys"]["id"]["S"].as_str().unwrap_or("");
            match pk {
                "ttl-item" => ttl_record = Some(record),
                "client-item" => client_record = Some(record),
                _ => {}
            }
        }
        if let Some(next) = v["NextShardIterator"].as_str() {
            iterator = next.to_owned();
        }
        sleep(Duration::from_millis(50)).await;
    }

    let ttl_record = ttl_record.expect("the TTL delete's REMOVE record never appeared");
    let client_record = client_record.expect("the client delete's REMOVE record never appeared");

    assert_eq!(
        ttl_record["userIdentity"]["PrincipalId"], "dynamodb.amazonaws.com",
        "a TTL delete must carry the service userIdentity: {ttl_record}"
    );
    assert_eq!(ttl_record["userIdentity"]["Type"], "Service");
    assert!(
        client_record.get("userIdentity").is_none(),
        "a client delete must carry no userIdentity at all: {client_record}"
    );

    node.shutdown_graceful().await;
}
