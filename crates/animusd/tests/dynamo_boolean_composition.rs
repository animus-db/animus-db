//! End-to-end tests for `AND`/`OR`/`NOT` composition in
//! `FilterExpression`/`ConditionExpression`, over the real DynamoDB
//! JSON/HTTP wire.
//!
//! Precedence is the whole point: `NOT` binds tightest, then `AND`, then
//! `OR`, and parentheses override. The same leaves under a different tree
//! give a different answer, so getting this wrong returns plausible wrong
//! rows rather than an error.
//!
//! The parser's sharp edge is that `a BETWEEN :lo AND :hi` contains an `AND`
//! belonging to the term, not the combinator.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
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

/// `AND` narrows, `OR` widens, and both reach the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn and_or_compose_over_the_wire() {
    let (_dir, nodes, addrs) = setup().await;

    async fn q(addr: SocketAddr, frag: &str) -> (u16, String) {
        let body = format!(
            r#"{{"TableName":"events","KeyConditionExpression":"pk = :p",
                 "FilterExpression":"{frag}",
                 "ExpressionAttributeValues":{{":p":{{"S":"p1"}},
                    ":even":{{"S":"even"}},":three":{{"N":"3"}},
                    ":one":{{"N":"1"}},":five":{{"N":"5"}}}}}}"#
        );
        dynamo_retry(addr, "DynamoDB_20120810.Query", &body).await
    }
    let at = addrs[0];

    // even AND seq >= 3  -> a4 only (a0,a2 are even but below 3)
    let (status, both) = q(at, "parity = :even AND seq >= :three").await;
    assert_eq!(status, 200, "AND failed: {both}");
    assert!(both.contains("\"a4\""), "{both}");
    assert!(
        !both.contains("\"a0\"") && !both.contains("\"a3\""),
        "{both}"
    );

    // seq = 1 OR seq = 5  -> a1 and a5
    let (_, either) = q(at, "seq = :one OR seq = :five").await;
    assert!(
        either.contains("\"a1\"") && either.contains("\"a5\""),
        "{either}"
    );
    assert!(!either.contains("\"a2\""), "{either}");

    // NOT even  -> the odd ones
    let (_, negated) = q(at, "NOT parity = :even").await;
    assert!(negated.contains("\"a1\""), "{negated}");
    assert!(!negated.contains("\"a0\""), "{negated}");

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// Precedence, demonstrated by the same leaves under two trees returning
/// different rows. `a OR b AND c` must group as `a OR (b AND c)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn precedence_and_parentheses_change_the_answer() {
    let (_dir, nodes, addrs) = setup().await;

    async fn q(addr: SocketAddr, frag: &str) -> String {
        let body = format!(
            r#"{{"TableName":"events","KeyConditionExpression":"pk = :p",
                 "FilterExpression":"{frag}",
                 "ExpressionAttributeValues":{{":p":{{"S":"p1"}},
                    ":zero":{{"N":"0"}},":odd":{{"S":"odd"}},":five":{{"N":"5"}}}}}}"#
        );
        let (status, resp) = dynamo_retry(addr, "DynamoDB_20120810.Query", &body).await;
        assert_eq!(status, 200, "`{frag}` failed: {resp}");
        resp
    }
    let at = addrs[1];

    // seq = 0 OR parity = odd AND seq = 5
    //   default grouping: seq=0 OR (odd AND seq=5)  -> a0 and a5
    let default = q(at, "seq = :zero OR parity = :odd AND seq = :five").await;
    assert!(
        default.contains("\"a0\""),
        "a0 via the left disjunct: {default}"
    );
    assert!(
        default.contains("\"a5\""),
        "a5 via the right conjunction: {default}"
    );
    assert!(
        !default.contains("\"a1\""),
        "a1 is odd but not seq=5: {default}"
    );

    //   parenthesised the other way: (seq=0 OR odd) AND seq=5  -> a5 only
    let grouped = q(at, "(seq = :zero OR parity = :odd) AND seq = :five").await;
    assert!(grouped.contains("\"a5\""), "{grouped}");
    assert!(
        !grouped.contains("\"a0\""),
        "a0 must drop out once the AND applies to the whole disjunction: {grouped}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// The `BETWEEN … AND …` trap, end to end: the first `AND` closes the range,
/// the second joins terms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn between_composes_with_a_following_and() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[2],
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "FilterExpression":"seq BETWEEN :lo AND :hi AND parity = :even",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":lo":{"N":"1"},
                ":hi":{"N":"4"},":even":{"S":"even"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "BETWEEN + AND failed: {body}");
    assert!(body.contains("\"a2\"") && body.contains("\"a4\""), "{body}");
    assert!(!body.contains("\"a3\""), "a3 is in range but odd: {body}");
    assert!(
        !body.contains("\"a0\""),
        "a0 is even but below the range: {body}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// Composition reaches conditional writes too, since one decoder serves both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditional_writes_accept_composed_conditions() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, ok) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"a0"},"seq":{"N":"0"},"parity":{"S":"even"}},
            "ConditionExpression":"attribute_exists(sk) AND seq < :one",
            "ExpressionAttributeValues":{":one":{"N":"1"}}}"#,
    )
    .await;
    assert_eq!(
        status, 200,
        "a satisfied conjunction must let the write through: {ok}"
    );

    let (status, no) = dynamo(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"a0"},"seq":{"N":"0"}},
            "ConditionExpression":"attribute_exists(sk) AND seq > :one",
            "ExpressionAttributeValues":{":one":{"N":"1"}}}"#,
    )
    .await;
    assert_eq!(
        status, 400,
        "one false conjunct must refuse the write: {no}"
    );
    assert!(no.contains("ConditionalCheckFailed"), "{no}");

    for n in nodes {
        n.shutdown_graceful().await;
    }
}
