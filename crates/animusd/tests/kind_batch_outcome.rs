//! End-to-end tests for the `KindBatch` apply-time outcome channel.
//!
//! A CP kind write used to be confirmed by reading the key back and comparing
//! values, which cannot distinguish **"my entry no-op'd"** from **"my entry
//! applied and a concurrent write then overwrote it"**. The second is a
//! success. Reporting it as a failure made any contended key fail spuriously:
//! measured before this change, ten concurrent `PutItem`s to one key produced
//! **six** `superseded ... retry` errors on writes that had applied.
//!
//! The entry now records what it did, keyed by its Raft log index, exactly as
//! `TxnStage` and `Cas` already did — so the proposer asks the entry rather
//! than guessing from the value.

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

/// The headline property: concurrent writers to one key all succeed.
///
/// Before the outcome channel this reported ~6 failures in 10. Every write
/// here genuinely applies — they are last-writer-wins on the same key — so
/// every request must be acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_to_one_key_all_succeed() {
    let (_dir, nodes, addrs) = setup().await;

    let mut handles = Vec::new();
    for i in 0..10 {
        let addr = addrs[i % addrs.len()];
        handles.push(tokio::spawn(async move {
            // Deliberately NOT the retrying helper: a retry would mask the
            // spurious failure this test exists to catch.
            let body = format!(
                r#"{{"TableName":"events","Item":{{"pk":{{"S":"p1"}},"sk":{{"S":"hot"}},
                     "who":{{"S":"w{i}"}},"cat":{{"S":"X"}}}}}}"#
            );
            dynamo(addr, "DynamoDB_20120810.PutItem", &body).await
        }));
    }

    let mut failures = Vec::new();
    for h in handles {
        let (status, resp) = h.await.expect("task");
        if status != 200 {
            failures.push(format!("{status}: {resp}"));
        }
    }
    assert!(
        failures.is_empty(),
        "every concurrent write applied, so every one must be acknowledged; \
         got {} failure(s): {failures:#?}",
        failures.len()
    );

    // And the row holds exactly one of the ten values — last writer wins.
    let (status, got) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"hot"}},
            "ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{got}");
    let winners = (0..10)
        .filter(|i| got.contains(&format!("\"w{i}\"")))
        .count();
    assert_eq!(winners, 1, "exactly one writer's value survives: {got}");

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// A genuinely failed condition must still be reported — the outcome channel
/// has to keep rejecting, not just start accepting everything.
///
/// A conditional write whose precondition does not hold no-ops at apply time,
/// and that is now recorded as `ConditionFailed` rather than being inferred
/// from a value mismatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_condition_is_still_reported() {
    let (_dir, nodes, addrs) = setup().await;

    // `a0` exists, so attribute_not_exists(sk) must fail.
    let (status, resp) = dynamo(
        addrs[1],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"a0"},"cat":{"S":"X"}},
            "ConditionExpression":"attribute_not_exists(sk)"}"#,
    )
    .await;
    assert_eq!(status, 400, "an unmet condition must be refused: {resp}");
    assert!(resp.contains("ConditionalCheckFailed"), "{resp}");

    // And one that does hold still succeeds.
    let (status, ok) = dynamo_retry(
        addrs[1],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"brand-new"},"cat":{"S":"X"}},
            "ConditionExpression":"attribute_not_exists(sk)"}"#,
    )
    .await;
    assert_eq!(status, 200, "a met condition must apply: {ok}");

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// Contention plus conditions: racing conditional writes on one key must
/// resolve to exactly one winner, with the losers told their condition failed
/// rather than given an ambiguous error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_conditional_writes_yield_exactly_one_winner() {
    let (_dir, nodes, addrs) = setup().await;

    let mut handles = Vec::new();
    for i in 0..6 {
        let addr = addrs[i % addrs.len()];
        handles.push(tokio::spawn(async move {
            let body = format!(
                r#"{{"TableName":"events","Item":{{"pk":{{"S":"p1"}},"sk":{{"S":"once"}},
                     "who":{{"S":"w{i}"}},"cat":{{"S":"X"}}}},
                     "ConditionExpression":"attribute_not_exists(sk)"}}"#
            );
            dynamo(addr, "DynamoDB_20120810.PutItem", &body).await
        }));
    }

    let mut won = 0;
    let mut condition_failed = 0;
    let mut ambiguous = Vec::new();
    for h in handles {
        let (status, resp) = h.await.expect("task");
        if status == 200 {
            won += 1;
        } else if resp.contains("ConditionalCheckFailed") {
            condition_failed += 1;
        } else {
            ambiguous.push(format!("{status}: {resp}"));
        }
    }
    assert_eq!(won, 1, "exactly one create wins");
    assert!(
        ambiguous.is_empty(),
        "the losers must be told their condition failed, not given an \
         ambiguous error: {ambiguous:#?}"
    );
    assert_eq!(
        condition_failed, 5,
        "and the other five lost on the condition"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}
