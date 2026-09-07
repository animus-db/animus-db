//! End-to-end test of `Query`'s `FilterExpression` over the real DynamoDB
//! JSON/HTTP wire.
//!
//! Before this, `FilterExpression` was decoded only for `Scan`, so a `Query`
//! carrying one **silently returned unfiltered results** — the caller got
//! extra items it believed had been filtered out. This file used to pin the
//! whole contract (deliberately identical to `Scan`'s, `run_base_scan`): the
//! filter runs *after* the key condition has chosen what to evaluate and
//! *after* `Limit` has capped it, so a filtered-out item still counts toward
//! `ScannedCount`, still consumes a `Limit` slot, and can still be the item a
//! `LastEvaluatedKey` points at.
//!
//! **ADR 0061 rung D3 PR 3a moved every base/LSI test here to `SimCluster`**
//! (`crates/animusd/src/sim_cluster_dynamo_query_filter.rs`) — see that
//! module's own doc for the full account. **Only `filter_applies_to_a_gsi_
//! query` stays here**: it reads a materialized GSI row, which `SimCluster`
//! cannot produce (no `index_drain::change_consumer_loop`).
//!
//! Real time/sockets, so this assertion polls with generous timeouts.

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

/// Read `"Count"` / `"ScannedCount"` out of a response body.
fn counts(body: &str) -> (usize, usize) {
    let read = |field: &str| -> usize {
        let marker = format!("\"{field}\":");
        let at = body
            .find(&marker)
            .unwrap_or_else(|| panic!("no {field} in {body}"))
            + marker.len();
        body[at..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>()
            .parse()
            .unwrap_or_else(|_| panic!("unparsable {field} in {body}"))
    };
    (read("Count"), read("ScannedCount"))
}

/// The filter reaches a **GSI** query too. A GSI is materialized
/// asynchronously (ADR 0041 §4/§5), so this converges-or-times-out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_applies_to_a_gsi_query() {
    let (_dir, nodes, addrs) = setup().await;

    let body = await_gsi_query(
        addrs[0],
        r#"{"TableName":"events","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "FilterExpression":"parity = :v",
            "ExpressionAttributeValues":{":c":{"S":"X"},":v":{"S":"even"}}}"#,
        |got| counts(got) == (3, 6),
    )
    .await;
    for kept in ["a0", "a2", "a4"] {
        assert!(body.contains(kept), "{kept} should have matched: {body}");
    }
    for dropped in ["a1", "a3", "a5"] {
        assert!(
            !body.contains(dropped),
            "{dropped} should have been filtered out of the GSI page: {body}"
        );
    }
    for n in nodes {
        n.shutdown_graceful().await;
    }
}
