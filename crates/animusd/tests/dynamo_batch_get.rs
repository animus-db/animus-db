//! End-to-end tests for `BatchGetItem` over the real DynamoDB JSON/HTTP wire.
//!
//! The operation was previously unsupported — a wire test asserted it returned
//! `UnknownOperationException`.
//!
//! It is deliberately **not** transactional: DynamoDB's `BatchGetItem` gives no
//! cross-item atomicity, so this reuses the ordinary `GetItem` read path per
//! key rather than the quiescent multi-get `TransactGetItems` needs. Misses are
//! reported by *omission* from the table's list, not positionally.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            let leader = nodes.iter().any(Node::is_control_leader);
            let everyone_has_tablet = nodes.iter().all(|n| !n.metadata().members.is_empty());
            if leader && everyone_has_tablet {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not elect a leader and bootstrap within 20s");
}

/// One DynamoDB request over a fresh HTTP/1.1 connection → `(status, body)`.
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

/// `dynamo`, retried on a retryable `500 InternalServerError` for up to 20s —
/// a read is trivially idempotent, so retrying it is always safe. See
/// `dynamo_index_scan.rs`'s identical helper for the full rationale (the CP
/// data plane's transient "not the leader here"/leadership-churn refusal
/// surfaces as a clean `500`, including well after initial cluster
/// formation).
async fn dynamo_retry(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let (status, resp) = dynamo(addr, target, body).await;
        if status != 500 || tokio::time::Instant::now() >= deadline {
            return (status, resp);
        }
        sleep(Duration::from_millis(150)).await;
    }
}

/// Stand up a 3-node cluster with one table (`events`, composite key
/// `pk`/`sk`) carrying a hash-only GSI (`by-cat`, hash `cat`) and an LSI
/// (`by-score`, alt-sort `score`) — six items, all in base partition
/// `pk = "p1"` and all sharing GSI hash `cat = "X"`, so one fixture serves the
/// base, GSI and LSI filter tests alike.
///
/// The filterable attribute is `parity`, a **non-key** attribute on every
/// index involved, set to `even` on the three even `sk`s and `odd` on the
/// three odd ones. Half the partition matching is what makes the
/// fewer-than-`Limit` page observable.
///
/// | sk | cat | score | parity | seq (N) |
/// |----|-----|-------|--------|
/// | a0 | X   | s0    | even   |
/// | a1 | X   | s1    | odd    |
/// | a2 | X   | s2    | even   |
/// | a3 | X   | s3    | odd    |
/// | a4 | X   | s4    | even   |
/// | a5 | X   | s5    | odd    |
async fn setup() -> (support::PanicSafeTempDir, Vec<Node>, Vec<SocketAddr>) {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addrs: Vec<SocketAddr> = nodes.iter().map(Node::dynamo_addr).collect();

    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"events",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-cat",
                 "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-score",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"score","KeyType":"RANGE"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    for i in 0..6 {
        let parity = if i % 2 == 0 { "even" } else { "odd" };
        let (status, body) = dynamo_retry(
            addrs[0],
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"events","Item":{{
                    "pk":{{"S":"p1"}},"sk":{{"S":"a{i}"}},"cat":{{"S":"X"}},
                    "score":{{"S":"s{i}"}},"parity":{{"S":"{parity}"}},
                    "seq":{{"N":"{i}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem(a{i}) failed: {body}");
    }
    (dir, nodes, addrs)
}

/// Reads across two tables in one call, grouped by table in the response.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_get_reads_across_tables() {
    let (_dir, nodes, addrs) = setup().await;

    // A second table so the multi-table grouping is actually exercised.
    let (status, made) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"other","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "BillingMode":"PAY_PER_REQUEST"}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable(other) failed: {made}");

    let (status, put) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"other","Item":{"id":{"S":"o1"},"v":{"S":"vee"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem(other) failed: {put}");

    let (status, body) = dynamo_retry(
        addrs[1],
        "DynamoDB_20120810.BatchGetItem",
        // `ConsistentRead: true` per table request (ADR 0055): this reads
        // back items the test itself just wrote, and the wire default is now
        // a genuinely eventually-consistent read that may not reflect them
        // yet. `BatchGetItem` carries the flag per table, not per batch.
        r#"{"RequestItems":{
            "events":{"ConsistentRead":true,
                      "Keys":[{"pk":{"S":"p1"},"sk":{"S":"a0"}},
                              {"pk":{"S":"p1"},"sk":{"S":"a2"}}]},
            "other":{"ConsistentRead":true,"Keys":[{"id":{"S":"o1"}}]}}}"#,
    )
    .await;
    assert_eq!(status, 200, "BatchGetItem failed: {body}");
    assert!(
        body.contains("\"a0\"") && body.contains("\"a2\""),
        "both event keys: {body}"
    );
    assert!(
        body.contains("\"vee\""),
        "and the other table's item: {body}"
    );
    assert!(body.contains(r#""UnprocessedKeys":{}"#), "{body}");

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// A key that matches nothing is omitted, not reported as an empty slot —
/// `BatchGetItem` reports misses by omission.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_missing_key_is_omitted_from_the_response() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[2],
        "DynamoDB_20120810.BatchGetItem",
        r#"{"RequestItems":{"events":{"Keys":[
            {"pk":{"S":"p1"},"sk":{"S":"a0"}},
            {"pk":{"S":"p1"},"sk":{"S":"nope"}}]}}}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"a0\""), "the hit is present: {body}");
    assert!(!body.contains("nope"), "the miss is simply absent: {body}");

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// The projection is scoped to the table and applies to every key under it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_scoped_projection_applies_to_every_key() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.BatchGetItem",
        r#"{"RequestItems":{"events":{
            "Keys":[{"pk":{"S":"p1"},"sk":{"S":"a0"}},{"pk":{"S":"p1"},"sk":{"S":"a1"}}],
            "ProjectionExpression":"sk"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"a0\"") && body.contains("\"a1\""), "{body}");
    assert!(
        !body.contains("parity"),
        "an unprojected attribute must not appear for any key: {body}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// An unknown table is a `ResourceNotFoundException`, not a silent empty list.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unknown_table_is_reported() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo(
        addrs[1],
        "DynamoDB_20120810.BatchGetItem",
        r#"{"RequestItems":{"ghost":{"Keys":[{"id":{"S":"x"}}]}}}"#,
    )
    .await;
    assert_eq!(status, 400, "unknown table must be reported: {body}");
    assert!(body.contains("ResourceNotFound"), "{body}");

    for n in nodes {
        n.shutdown_graceful().await;
    }
}
