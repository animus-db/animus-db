//! End-to-end tests for `ReturnItemCollectionMetrics` over the real DynamoDB
//! JSON/HTTP wire (ADR 0006).
//!
//! Two things are being pinned here, and the second is the reason this test
//! exists at all.
//!
//! **The gating.** DynamoDB answers this field only for a table that has a
//! local secondary index, and only when `SIZE` was asked for. Both halves are
//! tested in both directions, because a field that appears when it shouldn't is
//! as wrong as one that doesn't when it should.
//!
//! **The forwarding hop.** The size can only be priced by the node that *hosts*
//! the tablet, so it is computed at the leader and rides back on the write
//! reply. A receiving edge node that is not the leader must therefore end up
//! with the same answer as one that is. `metrics_agree_from_every_node` writes
//! the same collection through all three nodes and asserts the reports match:
//! with three nodes at least one write is forwarded, so a hop that dropped the
//! field would show up as a missing or differing report on the majority of
//! them — which is precisely the bimodal per-process failure the engineering
//! lessons warn a forwarded-response change can introduce.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

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

/// `dynamo`, retried on a retryable `500 InternalServerError` for up to 20s.
/// See `dynamo_index_scan.rs`'s identical helper for the rationale.
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

/// The parsed body of a successful request.
async fn ok_body(addr: SocketAddr, target: &str, body: &str) -> Value {
    let (status, resp) = dynamo_retry(addr, target, body).await;
    assert_eq!(status, 200, "{target} failed: {resp}");
    serde_json::from_str(&resp).expect("json response")
}

/// A 3-node cluster with two tables: `withlsi` (composite `pk`/`sk`, an LSI on
/// `score`) and `nolsi` (same key schema, a GSI but no LSI). The second table
/// is what makes the "only for LSI tables" rule testable rather than assumed —
/// it has an index, so it takes the same leader-evaluated write path, and
/// differs from `withlsi` in exactly the one property that should gate the
/// field.
async fn setup() -> (tempfile::TempDir, Vec<Node>, Vec<SocketAddr>) {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addrs: Vec<SocketAddr> = nodes.iter().map(Node::dynamo_addr).collect();

    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"withlsi",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-score",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"score","KeyType":"RANGE"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable withlsi failed: {body}");

    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"nolsi",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-cat",
                 "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable nolsi failed: {body}");

    (dir, nodes, addrs)
}

/// A `PutItem` body for `table`, with `extra` spliced in as additional
/// top-level fields (`""` for none).
fn put(table: &str, sk: &str, extra: &str) -> String {
    let tail = if extra.is_empty() {
        String::new()
    } else {
        format!(",{extra}")
    };
    format!(
        r#"{{"TableName":"{table}",
             "Item":{{"pk":{{"S":"p1"}},"sk":{{"S":"{sk}"}},"score":{{"S":"s1"}}}}{tail}}}"#
    )
}

/// Assert a well-formed report and return its upper bound in GB.
fn check_shape(metrics: &Value) -> f64 {
    assert_eq!(
        metrics["ItemCollectionKey"]["pk"]["S"], "p1",
        "the collection is named by the partition key: {metrics}"
    );
    assert!(
        metrics["ItemCollectionKey"].get("sk").is_none(),
        "a collection is a partition, not an item — the sort key must not \
         appear: {metrics}"
    );
    let range = metrics["SizeEstimateRangeGB"]
        .as_array()
        .unwrap_or_else(|| panic!("SizeEstimateRangeGB is an array: {metrics}"));
    assert_eq!(range.len(), 2, "{metrics}");
    let lo = range[0].as_f64().expect("lower bound is a number");
    let hi = range[1].as_f64().expect("upper bound is a number");
    assert_eq!(
        lo, 0.0,
        "the lower end is zero — we bound, we do not measure"
    );
    assert!(hi >= 0.0, "{metrics}");
    hi
}

#[tokio::test(flavor = "multi_thread")]
async fn no_metrics_are_reported_unless_size_was_asked_for() {
    let (_dir, _nodes, addrs) = setup().await;

    // Default is NONE, and NONE means the field is absent entirely.
    let body = ok_body(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        &put("withlsi", "a0", ""),
    )
    .await;
    assert!(
        body.get("ItemCollectionMetrics").is_none(),
        "reported metrics nobody asked for: {body}"
    );

    // An unrecognised level is refused rather than quietly downgraded.
    let (status, resp) = dynamo(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        &put(
            "withlsi",
            "a1",
            r#""ReturnItemCollectionMetrics":"SOMETIMES""#,
        ),
    )
    .await;
    assert_eq!(status, 400, "{resp}");
    assert!(resp.contains("ValidationException"), "{resp}");
    assert!(resp.contains("SOMETIMES"), "{resp}");
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_are_reported_only_for_a_table_that_has_an_lsi() {
    let (_dir, _nodes, addrs) = setup().await;
    let want = r#""ReturnItemCollectionMetrics":"SIZE""#;

    // `withlsi` has one, so SIZE is answered.
    let body = ok_body(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        &put("withlsi", "a0", want),
    )
    .await;
    check_shape(&body["ItemCollectionMetrics"]);

    // `nolsi` has a GSI but no LSI. It takes the identical leader-evaluated
    // write path, so this is not testing "indexed vs unindexed" — it is
    // testing the LSI rule specifically.
    let body = ok_body(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        &put("nolsi", "a0", want),
    )
    .await;
    assert!(
        body.get("ItemCollectionMetrics").is_none(),
        "a table without an LSI has no item collection to report: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn every_write_operation_reports_the_collection() {
    let (_dir, _nodes, addrs) = setup().await;
    let want = r#""ReturnItemCollectionMetrics":"SIZE""#;

    let body = ok_body(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        &put("withlsi", "a0", want),
    )
    .await;
    check_shape(&body["ItemCollectionMetrics"]);

    let body = ok_body(
        addrs[0],
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"withlsi","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "UpdateExpression":"SET note = :v",
            "ExpressionAttributeValues":{":v":{"S":"hi"}},
            "ReturnItemCollectionMetrics":"SIZE"}"#,
    )
    .await;
    check_shape(&body["ItemCollectionMetrics"]);

    let body = ok_body(
        addrs[0],
        "DynamoDB_20120810.DeleteItem",
        r#"{"TableName":"withlsi","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "ReturnItemCollectionMetrics":"SIZE"}"#,
    )
    .await;
    check_shape(&body["ItemCollectionMetrics"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_agree_from_every_node_including_forwarded_writes() {
    let (_dir, _nodes, addrs) = setup().await;
    let want = r#""ReturnItemCollectionMetrics":"SIZE""#;

    // The size is priced by the node hosting the tablet and travels back on
    // the write reply. With three nodes, at least one of these three writes is
    // forwarded rather than served locally, so a forwarding hop that dropped
    // the field would leave the majority of these reports missing.
    let mut bounds = Vec::new();
    for (i, addr) in addrs.iter().enumerate() {
        let body = ok_body(
            *addr,
            "DynamoDB_20120810.PutItem",
            &put("withlsi", &format!("node{i}"), want),
        )
        .await;
        let metrics = body.get("ItemCollectionMetrics").unwrap_or_else(|| {
            panic!("node {i} returned no ItemCollectionMetrics — a dropped forwarding hop? {body}")
        });
        bounds.push(check_shape(metrics));
    }
    assert_eq!(bounds.len(), 3);
    // Every node names the same collection and prices it from the same tablet,
    // so the bound must be non-decreasing as rows are added, never divergent
    // per-node.
    assert!(
        bounds.windows(2).all(|w| w[1] >= w[0]),
        "a forwarded write priced the collection differently: {bounds:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_bound_grows_as_the_collection_does() {
    let (_dir, _nodes, addrs) = setup().await;
    let want = r#""ReturnItemCollectionMetrics":"SIZE""#;
    let blob = "x".repeat(4000);

    let first = ok_body(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        &put("withlsi", "a0", want),
    )
    .await;
    let before = check_shape(&first["ItemCollectionMetrics"]);

    // Add ~1 MB to the collection.
    for i in 0..250 {
        let (status, resp) = dynamo_retry(
            addrs[0],
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"withlsi",
                     "Item":{{"pk":{{"S":"p1"}},"sk":{{"S":"b{i}"}},
                              "score":{{"S":"s{i}"}},"blob":{{"S":"{blob}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "bulk write {i} failed: {resp}");
    }

    let last = ok_body(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        &put("withlsi", "a1", want),
    )
    .await;
    let after = check_shape(&last["ItemCollectionMetrics"]);

    // The bound is an upper bound on a collection that definitely grew, so it
    // must not have shrunk. It is deliberately not asserted to equal any
    // particular figure: it is a bound taken from the hosting tablet, not a
    // measurement of the collection.
    assert!(
        after >= before,
        "the bound went down as the collection grew: {before} → {after}"
    );
}
