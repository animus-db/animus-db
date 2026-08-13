//! `Scan` with `IndexName` (ADR 0041 §5), end to end over the real DynamoDB
//! JSON/HTTP wire — the last functional gap in the secondary-index stack.
//! Mirrors `dynamo_indexes.rs` (base `Scan` pagination) and `kind_scan.rs`
//! (the LSI forwarded-path multi-node pattern): a 3-node in-process cluster,
//! one table with a GSI and two LSIs across two partitions, driven by the
//! actual DynamoDB JSON protocol over hand-written HTTP/1.1.

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

/// Poll an index `Scan` until `accept` is satisfied, returning the last body
/// observed. A GSI is materialized **asynchronously** by the drain (ADR 0041
/// §4/§5) — DynamoDB's own eventually-consistent contract — so every
/// assertion against one must be a converged-or-timeout poll, never a fixed
/// sleep followed by a one-shot check.
async fn await_gsi_scan(addr: SocketAddr, body: &str, accept: impl Fn(&str) -> bool) -> String {
    let last = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let seen = std::sync::Arc::clone(&last);
    let converged = async move {
        loop {
            let (status, got) = dynamo(addr, "DynamoDB_20120810.Scan", body).await;
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
            "GSI scan never converged within 15s (last saw: {})",
            last.lock().unwrap()
        ),
    }
}

/// Extract the raw `LastEvaluatedKey` JSON object verbatim from a scan
/// response body — brace-matched, since it can be an arbitrary
/// AttributeValue map shape (a base cursor is `{pk,sk}`; a GSI cursor also
/// carries the index's own hash/sort attributes; an LSI cursor the index's
/// alt-sort attribute). `None` when the page wasn't truncated. Used as-is for
/// the next page's `ExclusiveStartKey` — response and request share the exact
/// same shape.
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

/// Drive the full `LastEvaluatedKey` pagination loop for a `Scan` whose body
/// (minus the trailing `}`) is `request_prefix`, at `limit` items per page,
/// returning every page's raw response body concatenated (newline-separated)
/// so a caller can assert on item markers across the whole walk.
async fn drain_scan_pages(addr: SocketAddr, request_prefix: &str, limit: usize) -> String {
    let mut combined = String::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages < 50, "pagination did not terminate: {combined}");
        let esk = match &cursor {
            Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
            None => String::new(),
        };
        let body = format!("{request_prefix},\"Limit\":{limit}{esk}}}");
        let (status, resp) = dynamo(addr, "DynamoDB_20120810.Scan", &body).await;
        assert_eq!(status, 200, "scan page failed: {resp}");
        // Only the part before `LastEvaluatedKey` — that field can repeat the
        // boundary item's own attribute values (it's built from that same
        // item), which would otherwise double-count a marker search over the
        // concatenated pages.
        let items_part = resp
            .find("\"LastEvaluatedKey\":")
            .map_or(resp.as_str(), |at| &resp[..at]);
        combined.push_str(items_part);
        combined.push('\n');
        match extract_last_evaluated_key(&resp) {
            Some(next) => cursor = Some(next),
            None => return combined,
        }
    }
}

/// Stand up a 3-node cluster with one table (`events`, composite key
/// `pk`/`sk`) carrying a GSI (`by-cat`, hash `cat`) and two LSIs (`by-score`,
/// alt-sort `score`; `by-rank`, alt-sort `rank`) — two LSIs specifically so a
/// scan against one can be checked for leakage from the other's interleaved
/// rows. Five items across two partitions:
///
/// | pk | sk | cat | score | rank |
/// |----|----|-----|-------|------|
/// | p1 | a0 | A   | 10    | 90   |
/// | p1 | a1 | B   | 11    | 91   |
/// | p1 | a2 | A   | 12    | 92   |
/// | p2 | b0 | B   | 13    | 93   |
/// | p2 | b1 | A   | 14    | 94   |
async fn setup() -> (Vec<Node>, SocketAddr) {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let addr0 = nodes[0].dynamo_addr();
    let (status, body) = dynamo(
        addr0,
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
                              {"AttributeName":"score","KeyType":"RANGE"}]},
                {"IndexName":"by-rank",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"rank","KeyType":"RANGE"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    for (pk, sk, cat, score, rank) in [
        ("p1", "a0", "A", "10", "90"),
        ("p1", "a1", "B", "11", "91"),
        ("p1", "a2", "A", "12", "92"),
        ("p2", "b0", "B", "13", "93"),
        ("p2", "b1", "A", "14", "94"),
    ] {
        let (status, body) = dynamo(
            addr0,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"events","Item":{{
                    "pk":{{"S":"{pk}"}},"sk":{{"S":"{sk}"}},"cat":{{"S":"{cat}"}},
                    "score":{{"S":"{score}"}},"rank":{{"S":"{rank}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem({pk},{sk}) failed: {body}");
    }
    (nodes, addr0)
}

/// (a) A GSI `Scan` returns every index row, walked across pages via
/// `LastEvaluatedKey` — first a converged-or-timeout poll on the unpaginated
/// scan (the drain is asynchronous), then a `Limit`-paginated walk over the
/// now-stable data collecting every distinct item exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gsi_scan_paginates_and_drains_all_rows() {
    let (_nodes, addr) = setup().await;

    // Wait for the drain to materialize all 5 rows before pagination-testing
    // against them (pagination correctness is a separate concern from the
    // drain's own eventual consistency).
    await_gsi_scan(
        addr,
        r#"{"TableName":"events","IndexName":"by-cat"}"#,
        |b| b.contains("\"Count\":5"),
    )
    .await;

    let combined = drain_scan_pages(addr, r#"{"TableName":"events","IndexName":"by-cat""#, 2).await;
    for sk in ["a0", "a1", "a2", "b0", "b1"] {
        let marker = format!(r#""sk":{{"S":"{sk}"}}"#);
        assert_eq!(
            count_occurrences(&combined, &marker),
            1,
            "expected exactly one page to carry sk={sk}, got pages:\n{combined}"
        );
    }
}

/// (b) `ConsistentRead: true` against a GSI `Scan` is rejected exactly like a
/// GSI `Query` (ADR 0041 §5's asymmetric contract) — and, for contrast, the
/// same flag against the base table's own `Scan` and an LSI `Scan` is
/// accepted (both already linearizable here).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gsi_scan_rejects_consistent_read() {
    let (_nodes, addr) = setup().await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"events","IndexName":"by-cat","ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 400, "expected rejection: {body}");
    assert!(body.contains("ValidationException"), "got: {body}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"events","ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "base ConsistentRead Scan rejected: {body}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"events","IndexName":"by-score","ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "LSI ConsistentRead Scan rejected: {body}");
}

/// (c) + (e): an LSI `Scan` returns exactly the requested index's rows — not
/// its sibling LSI's interleaved rows, and not duplicated across the two
/// partitions — with **immediate** consistency (no polling: an LSI row
/// commits atomically with its base row). Issued through **every** node of
/// the 3-node cluster in turn (the `kind_scan.rs` forwarding-regression
/// pattern): one un-split table has exactly one tablet and hence one leader,
/// so at least two of the three nodes below are not it, exercising
/// `cp_scan_kind_table`'s forwarded `KindScan` path per tablet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lsi_scan_returns_only_the_requested_index_through_every_node() {
    let (nodes, _addr) = setup().await;

    for (i, node) in nodes.iter().enumerate() {
        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.Scan",
            r#"{"TableName":"events","IndexName":"by-score"}"#,
        )
        .await;
        assert_eq!(status, 200, "LSI scan via node {i} failed: {body}");
        assert!(
            body.contains("\"Count\":5"),
            "node {i}: expected exactly 5 by-score rows (no by-rank leakage \
             or duplication across partitions): {body}"
        );
        assert!(
            body.contains("\"ScannedCount\":5"),
            "node {i}: expected 5 rows evaluated: {body}"
        );
        for sk in ["a0", "a1", "a2", "b0", "b1"] {
            let marker = format!(r#""sk":{{"S":"{sk}"}}"#);
            assert_eq!(
                count_occurrences(&body, &marker),
                1,
                "node {i}: expected sk={sk} exactly once: {body}"
            );
        }
    }
}

/// (d) `FilterExpression` filters an LSI `Scan`'s already-scanned rows —
/// `ScannedCount` still reflects every row of the *requested* index alone
/// (never the sibling LSI's), `Count` only the matches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lsi_scan_supports_filter_expression() {
    let (_nodes, addr) = setup().await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"events","IndexName":"by-score",
            "FilterExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"A"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "filtered LSI scan failed: {body}");
    // cat=A: a0, a2, b1 (3 of the 5 by-score rows).
    assert!(body.contains("\"Count\":3"), "got: {body}");
    assert!(body.contains("\"ScannedCount\":5"), "got: {body}");
    for sk in ["a0", "a2", "b1"] {
        assert!(
            body.contains(&format!(r#""sk":{{"S":"{sk}"}}"#)),
            "missing sk={sk}: {body}"
        );
    }
    for sk in ["a1", "b0"] {
        assert!(
            !body.contains(&format!(r#""sk":{{"S":"{sk}"}}"#)),
            "unexpected sk={sk} (cat=B): {body}"
        );
    }

    // The filtered scan also paginates correctly: a Limit-walk over the
    // filtered index still recovers every matching item exactly once.
    let combined = drain_scan_pages(
        addr,
        r#"{"TableName":"events","IndexName":"by-score",
            "FilterExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"A"}}"#,
        1,
    )
    .await;
    for sk in ["a0", "a2", "b1"] {
        let marker = format!(r#""sk":{{"S":"{sk}"}}"#);
        assert_eq!(
            count_occurrences(&combined, &marker),
            1,
            "expected exactly one page to carry sk={sk}, got pages:\n{combined}"
        );
    }
}
