//! End-to-end tests for `UpdateExpression`'s `ADD` and `DELETE` clauses over
//! the real DynamoDB JSON/HTTP wire.
//!
//! These are the first rung of this series to touch the **write** path
//! (ADR 0049), so the load-bearing question is not just "does the arithmetic
//! work" but "does everything hanging off a write still happen": a GSI whose
//! indexed attribute an `ADD` changed must be re-indexed, exactly as a `SET`
//! would have caused.
//!
//! Numeric `ADD` is the adapter's only **non-idempotent** write, and it took
//! two write-path fixes to make it safe.
//!
//! First, `ClientCtx::cp_kind_write_item` used to retry
//! `kind_write_item_at_leader` on any retryable error, and that re-reads the
//! old image and re-applies — a fresh read-modify-write, not a replay.
//! Measured then: ten concurrent increments with two accepted responses left
//! the counter at **431**. The guarantee to hold is DynamoDB's —
//! **at-most-once per request**, not exactly-once, since a client that retries
//! an `ADD` which applied double-counts there too — so the service simply must
//! not re-apply on its own.
//!
//! Second, confirmation compared the written value back, which cannot tell
//! "my entry no-op'd" from "my entry applied and was immediately overwritten".
//! That reported **8 of 10** concurrent increments as needing a retry although
//! they had applied — and retrying is exactly what double-counts. A
//! `KindBatch` now records what it did at apply time.
//!
//! With both in place: ten concurrent increments are all accepted and leave
//! the counter at exactly ten, pinned below.
//!
//! **Seven of this file's original eight tests moved to
//! `crates/animusd/src/sim_cluster_dynamo_update_add_delete.rs`** (ADR 0061
//! rung D3 PR 1) — base-table-only, reachable through `dynamo::
//! dispatch_item_op`. `an_add_that_changes_an_indexed_attribute_reindexes`
//! stays here: it queries a GSI, which that generic core does not dispatch
//! yet.

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
                    "score":{{"S":"s{i}"}},"parity":{{"S":"{parity}"}},
                    "seq":{{"N":"{i}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem(a{i}) failed: {body}");
    }
    (dir, nodes, addrs)
}

/// Poll a GSI `Query` until `accept` is satisfied. A GSI is materialized
/// **asynchronously** by the drain (ADR 0041 §4/§5) — DynamoDB's own
/// eventually-consistent contract — so every assertion against one must be a
/// converged-or-timeout poll, never a fixed sleep + one-shot check.
async fn await_gsi_query(addr: SocketAddr, body: &str, accept: impl Fn(&str) -> bool) -> String {
    let last = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let seen = std::sync::Arc::clone(&last);
    let converged = async move {
        loop {
            let (status, got) = dynamo(addr, "DynamoDB_20120810.Query", body).await;
            if status == 200 && accept(&got) {
                return got;
            }
            *seen.lock().unwrap() = got;
            sleep(Duration::from_millis(100)).await;
        }
    };
    match timeout(Duration::from_secs(15), converged).await {
        Ok(body) => body,
        Err(_) => panic!(
            "GSI query never converged within 15s (last saw: {})",
            last.lock().unwrap()
        ),
    }
}

/// The risk this rung carries: an `ADD` that changes a **GSI-indexed**
/// attribute must re-index the row, exactly as a `SET` would. Index
/// maintenance is asynchronous, so this converges-or-times-out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_add_that_changes_an_indexed_attribute_reindexes() {
    let (_dir, nodes, addrs) = setup().await;

    // `cat` is the GSI hash attribute. Move a0 out of partition X by setting
    // it to Y, via SET, then confirm the index followed — establishing the
    // baseline the ADD case must match.
    let (status, moved) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "UpdateExpression":"SET cat = :y ADD tags :t",
            "ExpressionAttributeValues":{":y":{"S":"Y"},":t":{"SS":["new"]}},
            "ReturnValues":"ALL_NEW"}"#,
    )
    .await;
    assert_eq!(status, 200, "combined SET+ADD failed: {moved}");
    assert!(moved.contains(r#""new""#), "the ADD applied: {moved}");

    // The GSI must converge to showing a0 under Y, not X.
    let body = await_gsi_query(
        addrs[1],
        r#"{"TableName":"events","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"Y"}}}"#,
        |got| got.contains("\"a0\""),
    )
    .await;
    assert!(
        body.contains("\"a0\""),
        "the GSI followed the update: {body}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}
