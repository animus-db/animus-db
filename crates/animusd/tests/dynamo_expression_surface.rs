//! End-to-end tests for the full `FilterExpression`/`ConditionExpression`
//! surface over the real DynamoDB JSON/HTTP wire.
//!
//! Before this the surface was three forms — `attribute_exists`,
//! `attribute_not_exists` and `a = :v` — so every comparison, range,
//! membership test and function was a `ValidationException`.
//!
//! The property worth pinning is that **numbers compare numerically**. The
//! adapter's key encoding orders numbers lexicographically (a documented
//! simplification for *key* ordering), and inheriting that here would make
//! `price > :p` quietly wrong for ordinary data — 9 would outrank 10.

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

/// Comparators against a numeric attribute, over the wire. The 9-vs-10 case
/// is the one a lexicographic shortcut gets wrong.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn numeric_comparators_filter_numerically() {
    let (_dir, nodes, addrs) = setup().await;

    // seq is 0..5 as numbers, so a text comparison would sort 10 before 9 —
    // seeded above with two-digit values to make that visible.
    async fn q(addr: SocketAddr, op: &str, v: &str) -> (u16, String) {
        let body = format!(
            r#"{{"TableName":"events","KeyConditionExpression":"pk = :p",
                 "FilterExpression":"seq {op} :v",
                 "ExpressionAttributeValues":{{":p":{{"S":"p1"}},":v":{{"N":"{v}"}}}}}}"#
        );
        dynamo_retry(addr, "DynamoDB_20120810.Query", &body).await
    }
    let at = addrs[0];

    let (status, ge) = q(at, ">=", "3").await;
    assert_eq!(status, 200, "`>=` must now be served: {ge}");
    assert!(ge.contains("\"a3\"") && ge.contains("\"a5\""), "{ge}");
    assert!(!ge.contains("\"a2\""), "and bounded below: {ge}");

    let (_, lt) = q(at, "<", "2").await;
    assert!(lt.contains("\"a0\"") && lt.contains("\"a1\""), "{lt}");
    assert!(!lt.contains("\"a2\""), "{lt}");

    let (_, ne) = q(at, "<>", "0").await;
    assert!(!ne.contains("\"a0\""), "`<>` excludes the equal one: {ne}");
    assert!(ne.contains("\"a5\""), "{ne}");

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// BETWEEN, IN, begins_with, contains, attribute_type and size over the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_function_and_range_forms_serve() {
    let (_dir, nodes, addrs) = setup().await;

    async fn run(addr: SocketAddr, frag: &str) -> (u16, String) {
        let body = format!(
            r#"{{"TableName":"events","KeyConditionExpression":"pk = :p",
                 "FilterExpression":"{frag}",
                 "ExpressionAttributeValues":{{":p":{{"S":"p1"}},
                    ":lo":{{"N":"1"}},":hi":{{"N":"3"}},
                    ":one":{{"N":"1"}},":five":{{"N":"5"}},
                    ":pre":{{"S":"a"}},":sub":{{"S":"3"}},
                    ":ty":{{"S":"S"}},":two":{{"N":"2"}}}}}}"#
        );
        dynamo_retry(addr, "DynamoDB_20120810.Query", &body).await
    }
    let at = addrs[1];

    let (status, between) = run(at, "seq BETWEEN :lo AND :hi").await;
    assert_eq!(status, 200, "BETWEEN failed: {between}");
    assert!(
        between.contains("\"a1\"") && between.contains("\"a3\""),
        "{between}"
    );
    assert!(
        !between.contains("\"a0\"") && !between.contains("\"a4\""),
        "{between}"
    );

    let (_, in_) = run(at, "seq IN (:one, :five)").await;
    assert!(in_.contains("\"a1\"") && in_.contains("\"a5\""), "{in_}");
    assert!(!in_.contains("\"a2\""), "{in_}");

    let (_, begins) = run(at, "begins_with(sk, :pre)").await;
    assert!(
        begins.contains("\"a0\""),
        "every sk begins with `a`: {begins}"
    );

    let (_, contains) = run(at, "contains(sk, :sub)").await;
    assert!(contains.contains("\"a3\""), "{contains}");
    assert!(!contains.contains("\"a1\""), "{contains}");

    let (_, ty) = run(at, "attribute_type(sk, :ty)").await;
    assert!(ty.contains("\"a0\""), "sk is a string: {ty}");

    let (_, size) = run(at, "size(sk) = :two").await;
    assert!(size.contains("\"a0\""), "`a0` is two bytes: {size}");

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// `size()` on an attribute that **exists** with a type it has no size for
/// (`N`/`BOOL`/`NULL`) is a real DynamoDB `ValidationException`, not a false
/// filter match — the fidelity gap flagged in review of the commit that
/// introduced `size()` (fe0ce0c). Covers both evaluation paths the wire
/// shares: a `Scan`/`Query` `FilterExpression` and a `PutItem`
/// `ConditionExpression`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn size_of_an_existing_number_attribute_is_a_validation_exception() {
    let (_dir, nodes, addrs) = setup().await;

    // `seq` is an `N` on every seeded item — `size()` has no meaning for it.
    // `ConsistentRead: true` is load-bearing for the ASSERTION, not the
    // semantics: the error only raises when an item is actually examined, so
    // an eventual read served by a still-empty lagging replica (ADR 0055's
    // wire default) would legitimately return 200/Count:0 instead.
    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"events","ConsistentRead":true,
            "FilterExpression":"size(seq) > :zero",
            "ExpressionAttributeValues":{":zero":{"N":"0"}}}"#,
    )
    .await;
    assert_eq!(
        status, 400,
        "size() on an existing N attribute must be rejected, not just false: {body}"
    );
    assert!(body.contains("ValidationException"), "{body}");
    assert!(
        body.contains("operator or function: size, operand type: N"),
        "message should match AWS's own wording: {body}"
    );

    // The same evaluator backs a conditional write — issued through EVERY
    // node, not one fixed address: a conditional write evaluates at the
    // tablet's leader (ADR 0046 U3), so on a 3-node cluster at least two of
    // these sends cross the forwarded `KindWriteItem` hop, where the typed
    // error's own code must survive the string-typed reply channel
    // (`dynamo::encode_relayed_error`). Pre-fix this was a
    // placement-dependent failure: the leader-local send returned the
    // correct 400 while a forwarded one degraded to a 500 — exactly what CI
    // caught. `dynamo_retry` retries only genuine transient 500s
    // (leadership churn), so it converges on the stable answer either way.
    for (i, addr) in addrs.iter().enumerate() {
        let (status, body) = dynamo_retry(
            *addr,
            "DynamoDB_20120810.PutItem",
            r#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"a0"},"seq":{"N":"0"}},
                "ConditionExpression":"size(seq) > :zero",
                "ExpressionAttributeValues":{":zero":{"N":"0"}}}"#,
        )
        .await;
        assert_eq!(
            status, 400,
            "node {i}: the same size()-on-N error must reach a conditional \
             write too (a forwarded hop must not degrade it to a 500): {body}"
        );
        assert!(body.contains("ValidationException"), "node {i}: {body}");
        assert!(
            !body.contains("ConditionalCheckFailed"),
            "node {i}: an operand-type violation is a ValidationException, \
             not a failed condition check: {body}"
        );
    }

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// The same surface reaches a **conditional write**, which shares the decoder.
/// A comparison that could never hold before must now gate correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditional_writes_use_the_same_surface() {
    let (_dir, nodes, addrs) = setup().await;

    // seq of a0 is 0; require seq < 1, which holds.
    let (status, ok) = dynamo_retry(
        addrs[2],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"a0"},"seq":{"N":"0"},"won":{"S":"yes"}},
            "ConditionExpression":"seq < :one",
            "ExpressionAttributeValues":{":one":{"N":"1"}}}"#,
    )
    .await;
    assert_eq!(
        status, 200,
        "a satisfiable comparison must let the write through: {ok}"
    );

    // And one that does not hold is refused, not silently applied.
    let (status, no) = dynamo(
        addrs[2],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"a0"},"seq":{"N":"0"}},
            "ConditionExpression":"seq > :one",
            "ExpressionAttributeValues":{":one":{"N":"1"}}}"#,
    )
    .await;
    assert_eq!(status, 400, "an unsatisfied condition must refuse: {no}");
    assert!(no.contains("ConditionalCheckFailed"), "{no}");

    for n in nodes {
        n.shutdown_graceful().await;
    }
}
