//! End-to-end tests for the three silently-wrong predicate-parser bugs, over
//! the real DynamoDB JSON/HTTP wire.
//!
//! All three shared one root cause — a naive `split_once('=')` and a discarded
//! attribute name — and all three failed *silently*: the caller got a
//! plausible-looking empty (or wrong) result set rather than an error.
//!
//! - `price >= :p` was cut into an equality against an attribute named
//!   `price >`, so the filter matched nothing.
//! - `#p = :v` was never alias-resolved, so it compared against an attribute
//!   literally named `#p` — always false. Aliases are mandatory for
//!   DynamoDB's reserved words, so this hit ordinary schemas.
//! - the key condition's attribute name was dropped entirely, so a `Query`
//!   naming a non-key attribute was served as a partition-key query against
//!   whatever value it named.

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
/// | sk | cat | score | parity |
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
        r#"{"TableName":"events","AttributeDefinitions":[{"AttributeName":"cat","AttributeType":"S"},{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"score","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],
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
                    "score":{{"S":"s{i}"}},"parity":{{"S":"{parity}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem(a{i}) failed: {body}");
    }
    (dir, nodes, addrs)
}

/// The alias fix, end to end: a filter written with `#alias` must actually
/// filter. Before, this returned zero items with a 200.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aliased_filter_actually_filters() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.Query",
        // ConsistentRead: true (ADR 0055, #604): asserts on the test's own
        // just-written rows.
        r##"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"#par = :v",
            "ExpressionAttributeNames":{"#par":"parity"},
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"even"}}}"##,
    )
    .await;
    assert_eq!(status, 200, "aliased filter failed: {body}");
    assert!(
        body.contains("\"a0\""),
        "the aliased filter must match real items, not an attribute named `#par`: {body}"
    );
    assert!(
        !body.contains("\"a1\""),
        "and must still exclude non-matching ones: {body}"
    );
    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// The comparison operators are now served (the expression-surface rung).
/// What must never come back is the old behaviour: a 200 with an empty page
/// because `sk >= :v` had been truncated into an equality on `sk >`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn comparisons_filter_rather_than_matching_nothing() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.Query",
        // ConsistentRead: true (ADR 0055, #604): asserts on the test's own
        // just-written rows.
        r#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"sk >= :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"a3"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "`>=` is served now: {body}");
    assert!(
        body.contains("\"a3\"") && body.contains("\"a5\""),
        "it must actually match — the truncated form matched nothing: {body}"
    );
    assert!(!body.contains("\"a2\""), "and still be bounded: {body}");
    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// A sort-key range comparator is genuinely served (issue #373 — `<`/`<=`/`>`/
/// `>=` used to be rejected here the same way `<>` still is), not silently
/// narrowed to an equality the way the pre-fix `>=` truncation used to behave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sort_key_range_is_served_rather_than_narrowed() {
    let (_dir, nodes, addrs) = setup().await;

    // `<>` is still rejected: it is not in AWS's own KeyConditionExpression
    // grammar (there is no not-equal *range*), unlike the other five.
    let (status, resp) = dynamo(
        addrs[1],
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p AND sk <> :s",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":s":{"S":"a3"}}}"#,
    )
    .await;
    assert_eq!(status, 400, "`<>` must stay rejected: {resp}");
    assert!(resp.contains("ValidationException"), "{resp}");

    // `>=`, which now genuinely narrows the range rather than being rejected.
    let (status, ge) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.Query",
        // ConsistentRead: true (ADR 0055, #604): asserts on the test's own
        // just-written rows.
        r#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p AND sk >= :s",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":s":{"S":"a3"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "`>=` is served now: {ge}");
    assert!(
        ge.contains("\"a3\"") && ge.contains("\"a5\""),
        "it must actually match — the pre-fix truncated form matched nothing: {ge}"
    );
    assert!(!ge.contains("\"a2\""), "and still be bounded: {ge}");

    // BETWEEN, which was already supported, still works over the same data.
    let (status, ok) = dynamo_retry(
        addrs[1],
        "DynamoDB_20120810.Query",
        // ConsistentRead: true (ADR 0055, #604): asserts on the test's own
        // just-written rows.
        r#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p AND sk BETWEEN :lo AND :hi",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":lo":{"S":"a1"},":hi":{"S":"a3"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "BETWEEN still works: {ok}");
    assert!(ok.contains("\"a2\""), "the range really is served: {ok}");
    assert!(!ok.contains("\"a5\""), "and really is bounded: {ok}");
    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// The edge-side half of the fix: a key condition naming an attribute that is
/// not the table's partition key is rejected. Before, the name was discarded
/// and the query was served against whatever value it named.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_condition_naming_a_non_key_attribute_is_rejected() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, resp) = dynamo(
        addrs[2],
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"X"}}}"#,
    )
    .await;
    assert_eq!(
        status, 400,
        "`cat` is a real attribute but not the partition key: {resp}"
    );
    assert!(resp.contains("ValidationException"), "{resp}");

    // Naming a sort key the table does not have is likewise rejected.
    let (status, resp2) = dynamo(
        addrs[2],
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p AND cat = :c",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":c":{"S":"X"}}}"#,
    )
    .await;
    assert_eq!(status, 400, "`cat` is not the sort key: {resp2}");
    assert!(resp2.contains("ValidationException"), "{resp2}");
    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// The valid forms still work — including an aliased key condition, which is
/// how a table whose key collides with a reserved word must be queried.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aliased_and_plain_key_conditions_both_still_serve() {
    let (_dir, nodes, addrs) = setup().await;

    // ConsistentRead: true (ADR 0055, #604): asserts on the test's own
    // just-written rows.
    for body in [
        r#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
        r##"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"#k = :p",
            "ExpressionAttributeNames":{"#k":"pk"},
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"##,
    ] {
        let (status, resp) = dynamo_retry(addrs[0], "DynamoDB_20120810.Query", body).await;
        assert_eq!(status, 200, "valid key condition failed: {resp}");
        assert!(resp.contains("\"a0\""), "and returns the partition: {resp}");
    }
    for n in nodes {
        n.shutdown_graceful().await;
    }
}
