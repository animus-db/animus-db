//! End-to-end test of `Query`'s `FilterExpression` over the real DynamoDB
//! JSON/HTTP wire.
//!
//! Before this, `FilterExpression` was decoded only for `Scan`, so a `Query`
//! carrying one **silently returned unfiltered results** — the caller got
//! extra items it believed had been filtered out. These tests pin the whole
//! contract, which is deliberately identical to `Scan`'s (`run_base_scan`):
//! the filter runs *after* the key condition has chosen what to evaluate and
//! *after* `Limit` has capped it, so a filtered-out item still counts toward
//! `ScannedCount`, still consumes a `Limit` slot, and can still be the item a
//! `LastEvaluatedKey` points at. The visible consequence — and the one
//! applications trip over — is that a page may come back with **fewer items
//! than `Limit`, or none at all, and still carry a cursor**.
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

/// The headline fix: a `FilterExpression` on a base `Query` is **honoured**.
/// Before, it was ignored entirely and all six items came back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_narrows_a_base_query_instead_of_being_ignored() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[1],
        "DynamoDB_20120810.Query",
        // ConsistentRead: true (ADR 0055, #604): this test's subject is
        // filter evaluation, not read consistency, and it asserts on the
        // test's own just-written rows.
        r#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"parity = :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"even"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "filtered query failed: {body}");

    for kept in ["a0", "a2", "a4"] {
        assert!(body.contains(kept), "{kept} should have matched: {body}");
    }
    for dropped in ["a1", "a3", "a5"] {
        assert!(
            !body.contains(dropped),
            "{dropped} should have been filtered out: {body}"
        );
    }
    // Count is post-filter; ScannedCount is what the key condition evaluated.
    let (count, scanned) = counts(&body);
    assert_eq!(count, 3, "Count is the post-filter total: {body}");
    assert_eq!(
        scanned, 6,
        "ScannedCount counts every item the key condition evaluated: {body}"
    );
    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// The behaviour applications trip over: `Limit` caps items **evaluated**, not
/// items returned, so a filtered page can come back short — even empty — and
/// still carry a `LastEvaluatedKey`. A client that stops paging on a short
/// page (or on zero items) silently loses data, which is exactly why this has
/// to match DynamoDB rather than being "helpfully" topped up to `Limit`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filtered_page_returns_fewer_than_limit_and_still_carries_a_cursor() {
    let (_dir, nodes, addrs) = setup().await;

    // Evaluate exactly two items (a0, a1); only a1 is odd.
    let (status, body) = dynamo_retry(
        addrs[2],
        "DynamoDB_20120810.Query",
        // ConsistentRead: true (ADR 0055, #604): this test's subject is
        // filter/Limit interaction, not read consistency, and it asserts on
        // the test's own just-written rows.
        r#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"parity = :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"odd"}},
            "Limit":2}"#,
    )
    .await;
    assert_eq!(status, 200, "filtered limited query failed: {body}");

    let (count, scanned) = counts(&body);
    assert_eq!(scanned, 2, "Limit caps items evaluated: {body}");
    assert_eq!(count, 1, "only a1 survives the filter on this page: {body}");
    assert!(count < 2, "the page is short of Limit: {body}");
    assert!(body.contains("a1"), "a1 should be the kept item: {body}");
    assert!(!body.contains("\"a0\""), "a0 was filtered out: {body}");
    assert!(
        extract_last_evaluated_key(&body).is_some(),
        "a short filtered page must still carry a cursor: {body}"
    );

    // Walking the cursor to exhaustion yields exactly the three odd items and
    // no duplicates — the short pages are a paging artifact, not data loss.
    let mut seen: Vec<String> = Vec::new();
    let mut cursor = extract_last_evaluated_key(&body);
    let mut kept_first_page = 1usize;
    let mut pages = 0usize;
    while let Some(c) = cursor {
        pages += 1;
        assert!(pages < 20, "pagination did not terminate");
        let (status, page) = dynamo_retry(
            addrs[pages % addrs.len()],
            "DynamoDB_20120810.Query",
            &format!(
                // `ConsistentRead: true` (ADR 0055): this walk rotates across
                // nodes, and the wire default is now served from whichever replica
                // the receiving node holds — consecutive pages would otherwise
                // sample different, independently-lagging views. The strong read
                // still exercises the forwarded path the rotation exists to cover.
                r#"{{"TableName":"events","ConsistentRead":true,
                    "KeyConditionExpression":"pk = :p",
                    "FilterExpression":"parity = :v",
                    "ExpressionAttributeValues":{{":p":{{"S":"p1"}},":v":{{"S":"odd"}}}},
                    "Limit":2,"ExclusiveStartKey":{c}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "cursor page failed: {page}");
        for sk in ["a1", "a3", "a5"] {
            if page.contains(&format!("\"{sk}\"")) {
                seen.push(sk.to_string());
            }
        }
        kept_first_page += counts(&page).0;
        cursor = extract_last_evaluated_key(&page);
    }
    seen.sort();
    seen.dedup();
    assert_eq!(
        kept_first_page, 3,
        "the whole walk returns exactly the three odd items"
    );
    assert_eq!(seen, vec!["a3".to_string(), "a5".to_string()]);
    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// A filter that matches nothing returns an empty `Items` with a **non-zero**
/// `ScannedCount` — proving the filter ran rather than the key condition
/// having selected nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filter_matching_nothing_still_reports_what_it_evaluated() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.Query",
        // ConsistentRead: true (ADR 0055, #604): this test's subject is
        // filter evaluation, not read consistency — right after setup()'s
        // puts, a replica-local eventual read may not have applied the
        // sixth row yet, which would silently under-report ScannedCount.
        r#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"parity = :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"neither"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "query failed: {body}");
    let (count, scanned) = counts(&body);
    assert_eq!(count, 0, "nothing matches the filter: {body}");
    assert_eq!(scanned, 6, "but all six were evaluated: {body}");
    for n in nodes {
        n.shutdown_graceful().await;
    }
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

/// ...and an **LSI** query, which reads the base tablet's `KIND_LSI` scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_applies_to_an_lsi_query() {
    let (_dir, nodes, addrs) = setup().await;

    let body = await_gsi_query(
        addrs[1],
        r#"{"TableName":"events","IndexName":"by-score",
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"parity = :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"odd"}}}"#,
        |got| counts(got) == (3, 6),
    )
    .await;
    for kept in ["a1", "a3", "a5"] {
        assert!(body.contains(kept), "{kept} should have matched: {body}");
    }
    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// `attribute_exists`/`attribute_not_exists` work as filters too, not just the
/// equality form — every predicate `Scan` accepts, `Query` now accepts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn attribute_exists_works_as_a_query_filter() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, present) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.Query",
        // ConsistentRead: true (ADR 0055, #604): asserts on the test's own
        // just-written rows.
        r#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"attribute_exists(parity)",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "query failed: {present}");
    assert_eq!(counts(&present), (6, 6), "every item has parity: {present}");

    let (status, absent) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.Query",
        // ConsistentRead: true (ADR 0055, #604): asserts on the test's own
        // just-written rows.
        r#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"attribute_not_exists(parity)",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "query failed: {absent}");
    assert_eq!(counts(&absent), (0, 6), "none lack parity: {absent}");
    for n in nodes {
        n.shutdown_graceful().await;
    }
}
