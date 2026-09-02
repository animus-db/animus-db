//! End-to-end tests for `KeyConditionExpression` sort-key **range**
//! comparators (`<`, `<=`, `>`, `>=`, issue #373) over real DynamoDB
//! JSON/HTTP — base table, GSI, and LSI, plus the type-mismatch validation
//! and the documented `ScanIndexForward` ordering caveat. Mirrors
//! `dynamo_predicate_bugs.rs`/`dynamo_documents.rs`: a 3-node in-process
//! cluster driven by the actual DynamoDB JSON protocol over hand-written
//! HTTP/1.1. Real time/sockets, so it polls with generous timeouts.
//!
//! The fixture's sort keys are deliberately **mixed digit counts and
//! signs** (`-10`, `-2`, `1`, `5`, `9`, `10`, `15`, `20`, `100`) — the exact
//! shape a byte-lexicographic compare gets wrong (`"9" > "15"` as text) and
//! issue #373 was filed over. A range comparator that only worked on
//! same-width positive numbers would pass a lazier fixture and still be
//! broken in production.

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
/// `dynamo_predicate_bugs.rs`'s identical helper for the full rationale.
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

/// Poll a GSI `Query` until `accept` is satisfied, returning the last body
/// observed. A GSI is materialized **asynchronously** by the drain (ADR 0041
/// §4/§5) — DynamoDB's own eventually-consistent contract — so every
/// assertion against one must be a converged-or-timeout poll, never a fixed
/// sleep followed by a one-shot check (`docs/engineering-lessons.md`).
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

/// Stand up a 3-node cluster with one table (`readings`, composite key
/// `pk`/`sk`, `sk` declared `N` via `AttributeDefinitions` so the new
/// operand-type validation has a real declared type to check against),
/// carrying a composite GSI (`by-device-value`, hash `device`, sort `value`)
/// and a composite LSI (`by-alt`, alt-sort `alt`) — both `N`-sorted too, so
/// every one of the three read paths (`run_base_query`/`run_gsi_query`/
/// `run_lsi_query`) gets exercised with the identical mixed-digit-count,
/// mixed-sign fixture.
///
/// Every item in partition `pk = "p1"` carries the same numeric text in
/// `sk`/`value`/`alt`, so one fixture serves all three read paths without
/// three separate value tables to keep straight.
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
        r#"{"TableName":"readings",
            "AttributeDefinitions":[
                {"AttributeName":"pk","AttributeType":"S"},
                {"AttributeName":"sk","AttributeType":"N"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-device-value",
                 "KeySchema":[{"AttributeName":"device","KeyType":"HASH"},
                              {"AttributeName":"value","KeyType":"RANGE"}],
                 "Projection":{"ProjectionType":"ALL"}}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-alt",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"alt","KeyType":"RANGE"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    for sk in ["-10", "-2", "1", "5", "9", "10", "15", "20", "100"] {
        let (status, body) = dynamo_retry(
            addrs[0],
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"readings","Item":{{
                    "pk":{{"S":"p1"}},"sk":{{"N":"{sk}"}},
                    "device":{{"S":"d1"}},"value":{{"N":"{sk}"}},
                    "alt":{{"N":"{sk}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem(sk={sk}) failed: {body}");
    }
    (dir, nodes, addrs)
}

/// The set of `sk` values a body contains, read back off `"sk":{"N":"..."}`
/// — order-independent, so callers assert membership, not position (the
/// `ScanIndexForward` test below asserts position separately).
fn sk_values(body: &str) -> Vec<String> {
    let marker = "\"N\":\"";
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(at) = rest.find("\"sk\":{") {
        let tail = &rest[at..];
        let n_at = tail.find(marker).expect("sk is always N-typed here") + marker.len();
        let end = tail[n_at..].find('"').expect("closing quote");
        out.push(tail[n_at..n_at + end].to_string());
        rest = &tail[n_at + end..];
    }
    out.sort();
    out
}

/// Every range comparator, over the base table, against the mixed-digit-count
/// and mixed-sign fixture — the direct end-to-end regression for issue #373's
/// original filter bug plus its four new operators.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn base_table_range_queries_over_mixed_digit_count_n_sort_keys() {
    let (_dir, nodes, addrs) = setup().await;

    async fn query(addr: SocketAddr, op: &str, v: &str) -> Vec<String> {
        let body = format!(
            r#"{{"TableName":"readings","ConsistentRead":true,
                 "KeyConditionExpression":"pk = :p AND sk {op} :v",
                 "ExpressionAttributeValues":{{":p":{{"S":"p1"}},":v":{{"N":"{v}"}}}}}}"#
        );
        // A read that verifies a preceding write must ask for
        // `ConsistentRead: true` (ADR 0055) — the wire default is
        // eventually consistent, so an unqualified read here would be a
        // race, not a correctness assertion.
        let (status, body) = dynamo_retry(addr, "DynamoDB_20120810.Query", &body).await;
        assert_eq!(status, 200, "sk {op} {v} failed: {body}");
        sk_values(&body)
    }

    // `>`: 9 is NOT `> 9`, but 10/15/20/100 are — a byte compare would wrongly
    // exclude the multi-digit values (or include "9" itself).
    assert_eq!(
        query(addrs[1], ">", "9").await,
        vec!["10", "100", "15", "20"],
        "strictly greater than 9, numerically"
    );
    // `>=`: same set plus 9 itself.
    assert_eq!(
        query(addrs[1], ">=", "9").await,
        vec!["10", "100", "15", "20", "9"]
    );
    // `<`: negative and small positives below the multi-digit 10 boundary —
    // proves negatives aren't treated as "greater" by a stray minus-sign byte.
    assert_eq!(
        query(addrs[1], "<", "10").await,
        vec!["-10", "-2", "1", "5", "9"]
    );
    // `<=` at a negative bound.
    assert_eq!(query(addrs[1], "<=", "-2").await, vec!["-10", "-2"]);

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// A sort-key condition operand whose type disagrees with the table's
/// declared sort-key `AttributeType` is a `ValidationException` — the new
/// `validate_sort_condition_type` check, mirroring how `validate_key_
/// condition_names` already rejects a wrong attribute *name*.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_operand_type_mismatch_is_rejected() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo(
        addrs[0],
        "DynamoDB_20120810.Query",
        r#"{"TableName":"readings",
            "KeyConditionExpression":"pk = :p AND sk > :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"9"}}}"#,
    )
    .await;
    assert_eq!(
        status, 400,
        "an S operand against a declared-N sort key must be rejected: {body}"
    );
    assert!(body.contains("ValidationException"), "{body}");

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// The same range comparators over a **composite GSI**'s own `N` sort
/// attribute (`value`) — a second, independent native range scan
/// (`run_gsi_query`), eventually consistent by DynamoDB's own contract, so
/// every assertion here is a converged-or-timeout poll.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gsi_range_queries_over_mixed_digit_count_n_sort_keys() {
    let (_dir, nodes, addrs) = setup().await;

    let body = await_gsi_query(
        addrs[1],
        r#"{"TableName":"readings","IndexName":"by-device-value",
            "KeyConditionExpression":"device = :d AND value > :v",
            "ExpressionAttributeValues":{":d":{"S":"d1"},":v":{"N":"9"}}}"#,
        |b| b.contains("\"Count\":4"),
    )
    .await;
    // The GSI's projection is `ALL`, so the returned item still carries its
    // own `sk` attribute directly — no need to re-derive it from `value`.
    let values: Vec<String> = sk_values(&body);
    assert_eq!(values, vec!["10", "100", "15", "20"]);

    let body = await_gsi_query(
        addrs[1],
        r#"{"TableName":"readings","IndexName":"by-device-value",
            "KeyConditionExpression":"device = :d AND value <= :v",
            "ExpressionAttributeValues":{":d":{"S":"d1"},":v":{"N":"-2"}}}"#,
        |b| b.contains("\"Count\":2"),
    )
    .await;
    // The GSI's projection is `ALL`, so the returned item still carries its
    // own `sk` attribute directly — no need to re-derive it from `value`.
    let values: Vec<String> = sk_values(&body);
    assert_eq!(values, vec!["-10", "-2"]);

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// The same range comparators over a **composite LSI**'s own `N` alt-sort
/// attribute (`alt`) — an LSI row commits atomically with its base row (ADR
/// 0041 §2), so unlike the GSI case this is strongly consistent and needs no
/// polling; a plain immediate assertion suffices.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lsi_range_queries_over_mixed_digit_count_n_sort_keys() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[1],
        "DynamoDB_20120810.Query",
        r#"{"TableName":"readings","IndexName":"by-alt","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p AND alt >= :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"N":"10"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "LSI range query failed: {body}");
    // The LSI defaults to `ALL` projection too, so `sk` is present directly.
    let values: Vec<String> = sk_values(&body);
    assert_eq!(values, vec!["10", "100", "15", "20"]);

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// **Documents the `ScanIndexForward` ordering caveat** the
/// `animus-dynamo/CLAUDE.md` note now states explicitly: fixing the *filter*
/// for `N` (this issue) did **not** fix result *ordering*. `ScanIndexForward`
/// still walks the raw byte-ordered storage scan, which sorts `N` as decimal
/// **text** — so `"10"` sorts before `"2"` (`'1' < '2'`), even though `10 >
/// 2` numerically. This is a deliberately separate partition (`pk = "ord"`)
/// with just those two values, so the ordering assertion below isn't
/// entangled with the membership assertions the other tests already cover.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_index_forward_is_still_byte_order_not_numeric_order_for_n() {
    let (_dir, nodes, addrs) = setup().await;

    for sk in ["2", "10"] {
        let (status, body) = dynamo_retry(
            addrs[0],
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"readings","Item":{{"pk":{{"S":"ord"}},"sk":{{"N":"{sk}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem(sk={sk}) failed: {body}");
    }

    let (status, body) = dynamo_retry(
        addrs[1],
        "DynamoDB_20120810.Query",
        r#"{"TableName":"readings","ConsistentRead":true,"ScanIndexForward":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"ord"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "query failed: {body}");

    // Both rows are present (the *filter*/scan-range side is unaffected by
    // this caveat — it can only widen a range, never narrow it).
    let pos_2 = body.find(r#""sk":{"N":"2"}"#).expect("sk=2 present");
    let pos_10 = body.find(r#""sk":{"N":"10"}"#).expect("sk=10 present");
    // But the *order* is still lexicographic-by-text, not numeric: with
    // `ScanIndexForward: true` (ascending), "10" comes first because '1' <
    // '2' as a byte, even though 10 > 2 as a number. A truly numeric
    // ascending order would put "2" first — this assertion is intentionally
    // the *unfaithful* order, to pin down the documented gap rather than
    // hide it. (Fixing this needs an order-preserving numeric key encoding,
    // a real wire-format change tracked as future ADR work, not a filter fix.)
    assert!(
        pos_10 < pos_2,
        "ScanIndexForward:true still returns byte order (\"10\" before \"2\"), \
         not numeric order — if this now fails, the ordering caveat has been \
         fixed and the CLAUDE.md note (and this test) should be updated \
         together: {body}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}
