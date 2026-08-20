//! End-to-end tests for the AnimusDB Data Console's table page Stream data
//! tab (ADR 0052): a table's stream shards and the records inside them, all
//! through the **console** port, never the admin port — modeled on
//! `tests/console_items.rs`. The property most worth a regression test here
//! specifically (more than for any other tab): a shard id's own
//! `shardId-<tablet>-<epoch>` format does not smuggle any replica/placement
//! shape past `assert_no_cluster_shape` — see `console.rs`'s own module doc
//! for the full reasoning on why the id itself is fine to surface.
//!
//! Tables are created and populated through the real DynamoDB JSON/HTTP
//! wire so the fixtures match what an application would actually declare;
//! only the Stream tab's own reads go through the console port.
//!
//! Real time + sockets, so it brings the cluster up with the documented
//! port-TOCTOU bounded retry (`support::start_single_node`, the same helper
//! `console_items.rs` uses — every node bring-up in this file goes through
//! it or its fast-TTL-sweep sibling below, never a hand-rolled retry loop).

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use animusd::config::NodeRole;
use animusd::{ClusterConfig, Node, RoleAddrs, StorageBackend};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// A fast TTL sweep interval so the TTL-deletion test never has to wait out
/// the real production default — the identical idea `tests/dynamo_ttl.rs`'s
/// own `start_single_node_fast_ttl` already uses, duplicated here rather
/// than shared because it needs a non-default `run_node_with_ttl_sweep_
/// interval` bring-up `support::start_single_node` doesn't offer.
const TEST_TTL_SWEEP_INTERVAL: Duration = Duration::from_millis(300);

/// Bring up a single node with [`TEST_TTL_SWEEP_INTERVAL`] instead of the
/// production default, retrying the port-TOCTOU race exactly like
/// `support::start_single_node` does — same bounded-retry idiom, just
/// through `run_node_with_ttl_sweep_interval` instead of `run_node_with`
/// (mirrors `tests/dynamo_ttl.rs`'s identical helper).
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
/// body)`. Mirrors every other `tests/dynamo_*.rs`/`console_*.rs` file's
/// identical helper.
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

/// One request against the **console** listener with an arbitrary method
/// and (optional) JSON body → `(status, body)`. Identical to
/// `console_items.rs`'s own helper.
async fn console(addr: SocketAddr, method: &str, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to console");
    let request = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: animus\r\n\
         Content-Type: application/json\r\n\
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

/// No node/tablet/replica/raft/leader/quorum/placement/health/epoch-shaped
/// key anywhere in `body` — the same forbidden-substring list
/// `console_items.rs` checks every Items tab response against, reused here
/// for every Stream tab response. This is the assertion that matters most
/// in this file: a shard id's own digits (a tablet id and a seal epoch,
/// ADR 0042 §2) must never surface as a **named field** — the id string
/// itself is fine (see this file's module doc), a `"tablet_id": ...`- or
/// `"epoch": ...`-shaped key would not be.
fn assert_no_cluster_shape(body: &str) {
    let lower = body.to_ascii_lowercase();
    for forbidden in [
        "\"node",
        "\"tablet",
        "\"replica",
        "\"raft",
        "\"leader",
        "\"quorum",
        "\"placement",
        "\"health",
        "\"epoch",
    ] {
        assert!(
            !lower.contains(forbidden),
            "found cluster-shaped key `{forbidden}` in the console's response: {body}"
        );
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs()
}

/// A table with **no** stream: `GET .../stream/shards` is the honest
/// `enabled: false` empty answer, never an error and never a broken-looking
/// empty grid pretending a stream exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn table_with_no_stream_reports_the_honest_disabled_answer() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"plain",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        let (status, body) = console(
            console_addr,
            "GET",
            "/console/api/tables/plain/stream/shards",
            "",
        )
        .await;
        assert_eq!(
            status, 200,
            "stream/shards must be 200 even with no stream: {body}"
        );
        assert_no_cluster_shape(&body);
        let v = json(&body);
        assert_eq!(v["enabled"], false);
        assert!(v["shards"].as_array().unwrap().is_empty());
        assert!(v["stream_arn"].is_null());
        assert!(v["view_type"].is_null());

        // Minting an iterator against a never-enabled table is a clean
        // client error, not a panic/500.
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/plain/stream/iterator",
            r#"{"shard_id":"shardId-1-0","iterator_type":"TRIM_HORIZON"}"#,
        )
        .await;
        assert_ne!(
            status, 200,
            "an iterator on an unstreamed table must fail: {body}"
        );
        assert_no_cluster_shape(&body);

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// A table with a stream enabled: the shard list is non-empty, and a
/// shard's records reflect real writes made through the DynamoDB wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_enabled_lists_shards_and_records_reflect_real_writes() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"orders",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        for id in ["o1", "o2", "o3"] {
            let (status, body) = dynamo(
                dynamo_addr,
                "DynamoDB_20120810.PutItem",
                &format!(r#"{{"TableName":"orders","Item":{{"id":{{"S":"{id}"}}}}}}"#),
            )
            .await;
            assert_eq!(status, 200, "PutItem({id}) failed: {body}");
        }

        // ---- shard list -------------------------------------------------
        let (status, body) = console(
            console_addr,
            "GET",
            "/console/api/tables/orders/stream/shards",
            "",
        )
        .await;
        assert_eq!(status, 200, "stream/shards failed: {body}");
        assert_no_cluster_shape(&body);
        let v = json(&body);
        assert_eq!(v["enabled"], true);
        assert_eq!(v["view_type"], "NEW_AND_OLD_IMAGES");
        assert!(
            v["stream_arn"].as_str().unwrap().starts_with("arn:aws:dynamodb:"),
            "stream_arn must be DynamoDB's own ARN shape: {body}"
        );
        let shards = v["shards"].as_array().unwrap();
        assert_eq!(shards.len(), 1, "a fresh single-tablet table has exactly one open shard: {body}");
        let shard_id = shards[0]["shard_id"].as_str().unwrap().to_string();
        assert!(shard_id.starts_with("shardId-"), "unexpected shard id shape: {shard_id}");
        assert!(
            shards[0]["ending_sequence_number"].is_null(),
            "the table's only shard must still be open: {body}"
        );

        // ---- mint an iterator from TRIM_HORIZON --------------------------
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/orders/stream/iterator",
            &format!(r#"{{"shard_id":"{shard_id}","iterator_type":"TRIM_HORIZON"}}"#),
        )
        .await;
        assert_eq!(status, 200, "stream/iterator failed: {body}");
        assert_no_cluster_shape(&body);
        let mut iterator = json(&body)["shard_iterator"]
            .as_str()
            .expect("shard_iterator")
            .to_string();

        // ---- poll GetRecords until all 3 writes are visible --------------
        // (open-shard reads are leader-local, no barrier — ADR 0042 §7 —
        // so this is a genuine converged-or-timeout read, not a fixed sleep).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut seen_ids = std::collections::BTreeSet::new();
        while seen_ids.len() < 3 && tokio::time::Instant::now() < deadline {
            let (status, body) = console(
                console_addr,
                "POST",
                "/console/api/tables/orders/stream/records",
                &format!(r#"{{"shard_iterator":"{iterator}"}}"#),
            )
            .await;
            assert_eq!(status, 200, "stream/records failed: {body}");
            assert_no_cluster_shape(&body);
            let v = json(&body);
            for record in v["records"].as_array().cloned().unwrap_or_default() {
                assert_eq!(record["eventName"], "INSERT");
                assert!(record["userIdentity"].is_null(), "a client write must carry no userIdentity: {record}");
                let pk = record["dynamodb"]["Keys"]["id"]["S"].as_str().unwrap().to_string();
                seen_ids.insert(pk);
            }
            if let Some(next) = v["next_shard_iterator"].as_str() {
                iterator = next.to_string();
            }
            sleep(Duration::from_millis(30)).await;
        }
        assert_eq!(
            seen_ids,
            ["o1", "o2", "o3"].into_iter().map(String::from).collect(),
            "every written row's INSERT record must eventually appear"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// Walking a shard with `Limit` small enough to force more than one
/// `GetRecords` call, following each page's own `next_shard_iterator`:
/// every record must be seen **exactly once**, not merely "a second call
/// returns something."
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn walking_a_shard_with_next_shard_iterator_visits_every_record_exactly_once() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"widgets",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"}}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        let mut expected = std::collections::BTreeSet::new();
        for i in 0..9 {
            let id = format!("w{i}");
            let (status, body) = dynamo(
                dynamo_addr,
                "DynamoDB_20120810.PutItem",
                &format!(r#"{{"TableName":"widgets","Item":{{"id":{{"S":"{id}"}}}}}}"#),
            )
            .await;
            assert_eq!(status, 200, "PutItem({id}) failed: {body}");
            expected.insert(id);
        }

        let (status, body) = console(
            console_addr,
            "GET",
            "/console/api/tables/widgets/stream/shards",
            "",
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_no_cluster_shape(&body);
        let shard_id = json(&body)["shards"][0]["shard_id"]
            .as_str()
            .expect("shard_id")
            .to_string();

        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/widgets/stream/iterator",
            &format!(r#"{{"shard_id":"{shard_id}","iterator_type":"TRIM_HORIZON"}}"#),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let mut iterator = json(&body)["shard_iterator"]
            .as_str()
            .expect("shard_iterator")
            .to_string();

        // `Limit: 2` forces at least 5 `GetRecords` calls to see all 9
        // records — the pagination this test is actually exercising.
        let mut seen = std::collections::BTreeSet::new();
        let mut calls = 0;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while seen.len() < expected.len() && tokio::time::Instant::now() < deadline {
            calls += 1;
            assert!(
                calls < 200,
                "pagination did not converge: seen so far {seen:?}"
            );
            let (status, body) = console(
                console_addr,
                "POST",
                "/console/api/tables/widgets/stream/records",
                &format!(r#"{{"shard_iterator":"{iterator}","limit":2}}"#),
            )
            .await;
            assert_eq!(status, 200, "stream/records failed: {body}");
            assert_no_cluster_shape(&body);
            let v = json(&body);
            for record in v["records"].as_array().cloned().unwrap_or_default() {
                let pk = record["dynamodb"]["Keys"]["id"]["S"]
                    .as_str()
                    .unwrap()
                    .to_string();
                assert!(
                    seen.insert(pk.clone()),
                    "record for `{pk}` was returned by more than one page"
                );
            }
            if let Some(next) = v["next_shard_iterator"].as_str() {
                iterator = next.to_string();
            }
            sleep(Duration::from_millis(20)).await;
        }
        assert!(
            calls > 1,
            "the walk should have taken more than one GetRecords call at Limit=2 over 9 records"
        );
        assert_eq!(
            seen, expected,
            "every record must be visited exactly once across the whole walk"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// ADR 0051 §7: a TTL-reaper delete's stream record carries `userIdentity`
/// (`{"PrincipalId": "dynamodb.amazonaws.com", "Type": "Service"}`) when
/// read through the console's own `stream/records` endpoint, exactly as it
/// does over the raw DynamoDB Streams wire — the console passes the wire
/// `Record` shape straight through (`console::StreamRecordsPage`'s own
/// doc), so this is the console-side half of the regression
/// `tests/dynamo_ttl.rs::ttl_deletion_is_visible_in_the_stream_with_a_
/// service_user_identity` already proves over the raw wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_deletion_carries_the_service_user_identity_through_the_console() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let (node, _config) = start_single_node_fast_ttl(&dir.path().join("n")).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"sessions",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.UpdateTimeToLive",
            r#"{"TableName":"sessions","TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}"#,
        )
        .await;
        assert_eq!(status, 200, "UpdateTimeToLive failed: {body}");

        let past = now_secs() - 3600;
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.PutItem",
            &format!(r#"{{"TableName":"sessions","Item":{{"id":{{"S":"s1"}},"expiresAt":{{"N":"{past}"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200, "PutItem failed: {body}");

        // Wait for the reaper to actually delete it (its own independent
        // asynchronous path) before looking for the stream record.
        timeout(Duration::from_secs(20), async {
            loop {
                let (status, body) = dynamo(
                    dynamo_addr,
                    "DynamoDB_20120810.GetItem",
                    r#"{"TableName":"sessions","Key":{"id":{"S":"s1"}}}"#,
                )
                .await;
                assert_eq!(status, 200, "{body}");
                if !body.contains("\"Item\"") {
                    return;
                }
                sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("`s1` was never reaped");

        let (status, body) = console(
            console_addr,
            "GET",
            "/console/api/tables/sessions/stream/shards",
            "",
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_no_cluster_shape(&body);
        let shard_id = json(&body)["shards"][0]["shard_id"]
            .as_str()
            .expect("shard_id")
            .to_string();

        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/sessions/stream/iterator",
            &format!(r#"{{"shard_id":"{shard_id}","iterator_type":"TRIM_HORIZON"}}"#),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_no_cluster_shape(&body);
        let mut iterator = json(&body)["shard_iterator"]
            .as_str()
            .expect("shard_iterator")
            .to_string();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut ttl_record = None;
        while ttl_record.is_none() && tokio::time::Instant::now() < deadline {
            let (status, body) = console(
                console_addr,
                "POST",
                "/console/api/tables/sessions/stream/records",
                &format!(r#"{{"shard_iterator":"{iterator}"}}"#),
            )
            .await;
            assert_eq!(status, 200, "{body}");
            assert_no_cluster_shape(&body);
            let v = json(&body);
            for record in v["records"].as_array().cloned().unwrap_or_default() {
                if record["eventName"] == "REMOVE" {
                    ttl_record = Some(record);
                }
            }
            if let Some(next) = v["next_shard_iterator"].as_str() {
                iterator = next.to_string();
            }
            sleep(Duration::from_millis(30)).await;
        }
        let ttl_record = ttl_record.expect("the TTL delete's REMOVE record never appeared through the console");
        assert_eq!(
            ttl_record["userIdentity"]["PrincipalId"], "dynamodb.amazonaws.com",
            "a TTL delete read through the console must carry the service userIdentity: {ttl_record}"
        );
        assert_eq!(ttl_record["userIdentity"]["Type"], "Service");

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}
