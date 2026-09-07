//! End-to-end test of DynamoDB `Query` pagination (`Limit` /
//! `ExclusiveStartKey` / `LastEvaluatedKey` / `Count` / `ScannedCount`) over
//! the real DynamoDB JSON/HTTP wire.
//!
//! **ADR 0061 rung D3 PR 3a moved five of this file's six tests to
//! `SimCluster`** (`crates/animusd/src/sim_cluster_dynamo_query_pagination
//! .rs`) — see that module's own doc for the full account, including why
//! its own `cross_index_cursor_mismatch_is_rejected` drops one of the four
//! sub-cases below. **Only `gsi_query_paginates_with_the_scan_cursor_shape`
//! stays here**: it pages over a materialized GSI, which `SimCluster`
//! cannot produce (no `index_drain::change_consumer_loop`). Real
//! time/sockets, so this assertion polls with generous timeouts.

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

/// [`await_gsi_query`]'s multi-node form: poll `body` on **every** address
/// until each one satisfies `accept`.
///
/// Load-bearing since ADR 0055, and only since then. A rotating paginated
/// walk (see [`drain_query_pages`]) is stable only if every node it will
/// touch already agrees on the data — and a `ConsistentRead: false` read (the
/// wire default) is now served from whichever replica the *receiving* node
/// holds, so converging on one node says nothing about the node the next page
/// lands on. Before ADR 0055 every node forwarded to the tablet leader, so a
/// single convergence check covered all of them for free.
///
/// A GSI walk has no alternative to this: `ConsistentRead: true` against a
/// global index is a `ValidationException` (ADR 0041 §5), so it cannot simply
/// ask for the strong read the base/LSI walks in this file do.
async fn await_gsi_query_everywhere(
    addrs: &[SocketAddr],
    body: &str,
    accept: impl Fn(&str) -> bool + Copy,
) {
    for &addr in addrs {
        await_gsi_query(addr, body, accept).await;
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

/// How many times `needle` occurs in `haystack` (non-overlapping).
fn count_occurrences(haystack: &str, needle: &str) -> usize {
    let mut count = 0;
    let mut rest = haystack;
    while let Some(at) = rest.find(needle) {
        count += 1;
        rest = &rest[at + needle.len()..];
    }
    count
}

/// Stand up a 3-node cluster with one table (`events`, composite key
/// `pk`/`sk`) carrying a hash-only GSI (`by-cat`, hash `cat`) and an LSI
/// (`by-score`, alt-sort `score`) — six items, **all in the same base
/// partition** (`pk = "p1"`) and **all sharing the same GSI hash value**
/// (`cat = "X"`), so one `setup()` serves the base, GSI, and LSI pagination
/// tests alike (a `Query`, unlike a `Scan`, is always scoped to one
/// partition/hash-value, so this is the natural shared fixture).
///
/// | sk | cat | score |
/// |----|-----|-------|
/// | a0 | X   | s0    |
/// | a1 | X   | s1    |
/// | a2 | X   | s2    |
/// | a3 | X   | s3    |
/// | a4 | X   | s4    |
/// | a5 | X   | s5    |
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
        let (status, body) = dynamo_retry(
            addrs[0],
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"events","Item":{{
                    "pk":{{"S":"p1"}},"sk":{{"S":"a{i}"}},"cat":{{"S":"X"}},
                    "score":{{"S":"s{i}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem(a{i}) failed: {body}");
    }
    (dir, nodes, addrs)
}

/// (c) A **GSI** `Query` paginates with the exact same cursor shape
/// `run_gsi_scan` already uses (`gsi_key_item_of`): the index's own hash
/// attribute (`cat`) *and* the base table's key attributes (`pk`/`sk`), no
/// more and no less. Waits for the drain to materialize all 6 rows first
/// (a converged-or-timeout poll — the drain is asynchronous), then walks
/// the paginated cursor chain over the now-stable data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gsi_query_paginates_with_the_scan_cursor_shape() {
    let (_dir, _nodes, addrs) = setup().await;

    // Every node, not just one (ADR 0055): the walk below rotates across all
    // three, and each now answers from its own replica — so one node having
    // converged says nothing about the next page's node. This is what the CI
    // failure on PR #360 actually was: node 1 had all 6 GSI rows, another
    // node still had 5, and the walk terminated a page early with `sk=a5`
    // never returned.
    await_gsi_query_everywhere(
        &addrs,
        r#"{"TableName":"events","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"X"}}}"#,
        |b| b.contains("\"Count\":6"),
    )
    .await;

    // Drive pagination by hand (rather than `drain_query_pages`) so every
    // page's raw `LastEvaluatedKey` JSON can be checked for its exact
    // attribute set (the index's own hash attribute plus the base table's
    // `pk`/`sk` — exactly `gsi_key_item_of`'s shape) before moving on.
    let mut combined = String::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    let mut saw_cursor = false;
    loop {
        let addr = addrs[pages % addrs.len()];
        pages += 1;
        assert!(pages < 50, "pagination did not terminate: {combined}");
        let esk = match &cursor {
            Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
            None => String::new(),
        };
        let body = format!(
            // Deliberately no `ConsistentRead` here, unlike the base/LSI
            // walks: a GSI rejects it (ADR 0041 §5). The convergence sweep
            // above is what makes this rotating walk stable instead.
            r#"{{"TableName":"events","IndexName":"by-cat",
                "KeyConditionExpression":"cat = :c",
                "ExpressionAttributeValues":{{":c":{{"S":"X"}}}},
                "Limit":2{esk}}}"#
        );
        let (status, resp) = dynamo_retry(addr, "DynamoDB_20120810.Query", &body).await;
        assert_eq!(status, 200, "gsi query page failed: {resp}");
        let items_part = resp
            .find("\"LastEvaluatedKey\":")
            .map_or(resp.as_str(), |at| &resp[..at]);
        combined.push_str(items_part);
        combined.push('\n');
        match extract_last_evaluated_key(&resp) {
            Some(next) => {
                assert!(
                    next.contains("\"cat\"") && next.contains("\"pk\"") && next.contains("\"sk\""),
                    "GSI cursor missing expected attributes: {next}"
                );
                saw_cursor = true;
                cursor = Some(next);
            }
            None => break,
        }
    }
    assert!(saw_cursor, "expected at least one truncated page");
    for i in 0..6 {
        let marker = format!(r#""sk":{{"S":"a{i}"}}"#);
        assert_eq!(
            count_occurrences(&combined, &marker),
            1,
            "expected exactly one page to carry sk=a{i}, got pages:\n{combined}"
        );
    }
}
