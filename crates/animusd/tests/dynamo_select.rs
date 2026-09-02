//! End-to-end test of `Query`/`Scan`'s `Select` over the real DynamoDB
//! JSON/HTTP wire.
//!
//! `Select` was previously not decoded at all, so `Select: COUNT` — "how many
//! match, don't send them" — silently returned every item. That is both the
//! wrong response shape and an unbounded payload for a request whose whole
//! point is to avoid one.
//!
//! The load-bearing property is that `COUNT` changes only what is *returned*,
//! never what is *read*: a filter still runs, `Limit` still caps what is
//! examined, and a truncated `COUNT` page still carries a `LastEvaluatedKey`.
//! So `Count` is the matches on this page, not of the whole query — a client
//! wanting a total must still page to exhaustion.
//!
//! Real time/sockets, so every assertion polls with generous timeouts.

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

/// Extract the raw `LastEvaluatedKey` JSON object verbatim from a `Query`
/// response body — brace-matched, since it can be an arbitrary
/// AttributeValue map shape (a base cursor is `{pk,sk}`; a GSI cursor also
/// carries the index's own hash/sort attributes; an LSI cursor the index's
/// alt-sort attribute — see `dynamo_index_scan.rs`'s identical helper).
/// `None` when the page wasn't truncated.
fn extract_last_evaluated_key(body: &str) -> Option<String> {
    let marker = "\"LastEvaluatedKey\":";
    let start = body.find(marker)? + marker.len();
    let bytes = body.as_bytes();
    if bytes.get(start) != Some(&b'{') {
        return None;
    }
    let mut depth = 0usize;
    for (i, &b) in bytes[start..].iter().enumerate() {
        match b {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(body[start..start + i + 1].to_string());
                }
            }
            _ => {}
        }
    }
    None
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
                    "score":{{"S":"s{i}"}},"parity":{{"S":"{parity}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem(a{i}) failed: {body}");
    }
    (dir, nodes, addrs)
}

/// Extract an integer field like `"Count":3` from a response body.
fn field(body: &str, name: &str) -> Option<i64> {
    let needle = format!("\"{name}\":");
    let at = body.find(&needle)? + needle.len();
    let rest = &body[at..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit() && c != '-')
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

/// The headline fix: `Select: COUNT` returns counts and **no** `Items`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn count_select_returns_counts_without_items() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "Select":"COUNT"}"#,
    )
    .await;
    assert_eq!(status, 200, "COUNT query failed: {body}");
    assert!(
        !body.contains("\"Items\""),
        "COUNT must not carry an Items array: {body}"
    );
    assert_eq!(field(&body, "Count"), Some(6), "{body}");
    assert_eq!(field(&body, "ScannedCount"), Some(6), "{body}");
    // The items really are absent, not merely an empty array.
    assert!(!body.contains("\"a0\""), "no item payload leaked: {body}");
    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// `COUNT` still runs the filter — `Count` is matches, `ScannedCount` is
/// what was examined. If the filter were skipped the two would be equal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn count_select_still_applies_the_filter() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[1],
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "FilterExpression":"parity = :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"even"}},
            "Select":"COUNT"}"#,
    )
    .await;
    assert_eq!(status, 200, "filtered COUNT failed: {body}");
    assert!(!body.contains("\"Items\""), "{body}");
    assert_eq!(field(&body, "Count"), Some(3), "three even items: {body}");
    assert_eq!(
        field(&body, "ScannedCount"),
        Some(6),
        "all six were examined: {body}"
    );
    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// A truncated `COUNT` page still paginates: `Limit` caps what is examined
/// and the cursor lets the caller keep counting. This is what makes `Count`
/// a per-page number rather than a total.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn count_select_paginates_and_sums_to_the_whole_partition() {
    let (_dir, nodes, addrs) = setup().await;

    let mut total = 0i64;
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    loop {
        pages += 1;
        assert!(pages < 20, "COUNT pagination did not terminate");
        let esk = match &cursor {
            Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
            None => String::new(),
        };
        let req = format!(
            // `ConsistentRead: true` (ADR 0055): this walk rotates across
            // nodes, and the wire default is now served from whichever replica
            // the receiving node holds — consecutive pages would otherwise
            // sample different, independently-lagging views. The strong read
            // still exercises the forwarded path the rotation exists to cover.
            r#"{{"TableName":"events","ConsistentRead":true,
                 "KeyConditionExpression":"pk = :p",
                 "ExpressionAttributeValues":{{":p":{{"S":"p1"}}}},
                 "Select":"COUNT","Limit":2{esk}}}"#
        );
        let (status, body) =
            dynamo_retry(addrs[pages % addrs.len()], "DynamoDB_20120810.Query", &req).await;
        assert_eq!(status, 200, "COUNT page failed: {body}");
        assert!(!body.contains("\"Items\""), "still no Items: {body}");
        total += field(&body, "Count").unwrap_or_default();
        match extract_last_evaluated_key(&body) {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(total, 6, "the pages sum to the whole partition");
    assert!(pages > 1, "the walk really was paginated ({pages} pages)");
    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// `Select: COUNT` reaches `Scan` through the same decode path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn count_select_applies_to_scan() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[2],
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"events","Select":"COUNT"}"#,
    )
    .await;
    assert_eq!(status, 200, "COUNT scan failed: {body}");
    assert!(!body.contains("\"Items\""), "{body}");
    assert!(
        field(&body, "Count").unwrap_or_default() >= 6,
        "the scan counted the table: {body}"
    );
    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// `SPECIFIC_ATTRIBUTES` alongside a projection is accepted and returns
/// exactly the projected attributes — the value that must keep working.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn specific_attributes_returns_the_projection() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "ProjectionExpression":"sk","Select":"SPECIFIC_ATTRIBUTES","Limit":1}"#,
    )
    .await;
    assert_eq!(status, 200, "SPECIFIC_ATTRIBUTES failed: {body}");
    assert!(body.contains("\"sk\""), "the projected attribute: {body}");
    assert!(
        !body.contains("\"parity\""),
        "an unprojected attribute must not appear: {body}"
    );
    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// The validations DynamoDB performs, which this adapter previously accepted
/// silently. Each must be a 400 `ValidationException`, not a 500 and not a
/// success.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn contradictory_select_requests_are_rejected() {
    let (_dir, nodes, addrs) = setup().await;

    async fn reject(addr: SocketAddr, body: &str) {
        let (status, resp) = dynamo(addr, "DynamoDB_20120810.Query", body).await;
        assert_eq!(status, 400, "expected a validation error, got: {resp}");
        assert!(
            resp.contains("ValidationException"),
            "expected ValidationException: {resp}"
        );
    }
    let at = addrs[0];

    // SPECIFIC_ATTRIBUTES naming nothing.
    reject(
        at,
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "Select":"SPECIFIC_ATTRIBUTES"}"#,
    )
    .await;
    // A projection contradicting COUNT.
    reject(
        at,
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "ProjectionExpression":"sk","Select":"COUNT"}"#,
    )
    .await;
    // ALL_PROJECTED_ATTRIBUTES with no index to project.
    reject(
        at,
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "Select":"ALL_PROJECTED_ATTRIBUTES"}"#,
    )
    .await;
    // An unknown value.
    reject(
        at,
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "Select":"EVERYTHING"}"#,
    )
    .await;
    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// `COUNT` on a GSI query — the index leaf reaches the same builder.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn count_select_applies_to_a_gsi_query() {
    let (_dir, nodes, addrs) = setup().await;

    let body = await_gsi_query(
        addrs[1],
        r#"{"TableName":"events","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"X"}},
            "Select":"COUNT"}"#,
        |got| field(got, "Count") == Some(6),
    )
    .await;
    assert!(
        !body.contains("\"Items\""),
        "GSI COUNT has no Items: {body}"
    );
    assert_eq!(field(&body, "Count"), Some(6), "{body}");
    for n in nodes {
        n.shutdown_graceful().await;
    }
}
