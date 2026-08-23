//! `Scan` with `IndexName` (ADR 0041 §5), end to end over the real DynamoDB
//! JSON/HTTP wire — the last functional gap in the secondary-index stack.
//! Mirrors `dynamo_indexes.rs` (base `Scan` pagination) and `kind_scan.rs`
//! (the LSI forwarded-path multi-node pattern): a 3-node in-process cluster,
//! one table with a GSI and two LSIs across two partitions, driven by the
//! actual DynamoDB JSON protocol over hand-written HTTP/1.1.

use std::net::SocketAddr;
use std::time::Duration;

use animus_dynamo::{AttributeValue, storage_key};
use animus_tablet::partition_token;
use animusd::{ClientRequest, ClientResponse, Node, bind_cluster, read_frame, start_cluster};
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

/// `dynamo`, retried on a retryable `500 InternalServerError` for up to 20s:
/// any Dynamo-edge request — read or write — can surface the CP data
/// plane's transient "not the leader here"/leadership-churn refusal as a
/// clean `500` (`dynamo::error_status`), including well after initial
/// cluster formation (a mid-test leadership change under CI-runner
/// contention). A read is trivially idempotent, so retrying it is always
/// safe; a write is idempotent here too (`PutItem`/`CreateTable` are the
/// only writes this file issues, both safe to resend). See
/// `docs/engineering-lessons.md`'s "CP write-forward path has no
/// retry-on-not-the-leader-here" entry and issue #268's fast-futility
/// entry — the same retryable-error convention, just reached over the
/// Dynamo edge instead of the plain client protocol.
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
        let (status, resp) = dynamo_retry(addr, "DynamoDB_20120810.Scan", &body).await;
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
async fn setup() -> (tempfile::TempDir, Vec<Node>, SocketAddr) {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let addr0 = nodes[0].dynamo_addr();
    let (status, body) = dynamo_retry(
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
        let (status, body) = dynamo_retry(
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
    (dir, nodes, addr0)
}

/// (a) A GSI `Scan` returns every index row, walked across pages via
/// `LastEvaluatedKey` — first a converged-or-timeout poll on the unpaginated
/// scan (the drain is asynchronous), then a `Limit`-paginated walk over the
/// now-stable data collecting every distinct item exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gsi_scan_paginates_and_drains_all_rows() {
    let (_dir, _nodes, addr) = setup().await;

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
/// accepted (ADR 0055: on those it selects the linearizable read).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gsi_scan_rejects_consistent_read() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"events","IndexName":"by-cat","ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 400, "expected rejection: {body}");
    assert!(body.contains("ValidationException"), "got: {body}");

    let (status, body) = dynamo_retry(
        addr,
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"events","ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "base ConsistentRead Scan rejected: {body}");

    let (status, body) = dynamo_retry(
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
    let (_dir, nodes, _addr) = setup().await;

    for (i, node) in nodes.iter().enumerate() {
        let (status, body) = dynamo_retry(
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
    let (_dir, _nodes, addr) = setup().await;

    let (status, body) = dynamo_retry(
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

/// One `ClientRequest`/`ClientResponse` round trip over the plain
/// length-prefixed client protocol — used only to drive `SplitTablet` below;
/// no such op exists on the DynamoDB wire.
async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect to client port");
    animusd::write_frame(&mut stream, &req)
        .await
        .expect("send request");
    read_frame(&mut stream)
        .await
        .expect("read reply")
        .expect("a reply")
}

async fn await_true<F: Fn() -> bool>(secs: u64, what: &str, cond: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !cond() {
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for: {what}");
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// (e) The per-tablet `limit` on `KindScan` (ADR 0041 §5 as-built) is a
/// **coordinator-side/wire optimization only** — `StorageEngine::scan` has
/// no limit parameter, so a tablet still reads its whole matching sub-range;
/// the win is a smaller reply and less coordinator-side memory, never
/// reduced engine I/O — and must never change what a caller observes. Split
/// `events`'s one tablet into two (straddling the `p1`/`p2` partitions), then
/// re-run the exact small-`Limit` pagination walk
/// `lsi_scan_supports_filter_expression` already proves on a single tablet —
/// this time fanning `cp_scan_kind_table`'s `KindScan` across **two**
/// tablets and their (possibly two different) group leaders. The walk must
/// still recover every `by-score` row exactly once, in the same
/// Limit-paginated shape as before the split: identical behavior, only a
/// leaner per-page payload underneath.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lsi_scan_with_limit_paginates_identically_across_a_split_table() {
    let (_dir, nodes, addr) = setup().await;

    // A split point strictly between the `p1` and `p2` partitions' own
    // token-prefixed keys (ADR 0022/0023): `partition_token` hashes a
    // partition key's own encoded bytes (`storage_key(pk, None)`, exactly as
    // `animusd::dynamo::item_key` computes it), so the *larger* of the two
    // partitions' 8-byte tokens, used verbatim as the split key, is both (a)
    // strictly greater than every `p1` key — whose own token differs from it
    // at some byte within the first 8, which alone decides the byte-string
    // comparison — and (b) a strict byte-string *prefix* of every `p2` key
    // (which continues past those same 8 bytes), hence strictly less than
    // every one of them. Together that cleanly divides the two partitions
    // across the split regardless of which token happens to be larger.
    let token_p1 = partition_token(&storage_key(&AttributeValue::S("p1".into()), None));
    let token_p2 = partition_token(&storage_key(&AttributeValue::S("p2".into()), None));
    let split_key = token_p1.max(token_p2).to_vec();

    // The bootstrap tablet of the one table created so far is always id 1.
    let resp = call(
        nodes[0].client_addr(),
        ClientRequest::SplitTablet {
            tablet: 1,
            split_key,
        },
    )
    .await;
    assert!(
        matches!(resp, ClientResponse::PutOk),
        "split did not commit: {resp:?}"
    );
    await_true(20, "split child tablet hosted on node 0", || {
        nodes[0].metadata().tablets.len() >= 2
    })
    .await;

    let combined =
        drain_scan_pages(addr, r#"{"TableName":"events","IndexName":"by-score""#, 1).await;
    for sk in ["a0", "a1", "a2", "b0", "b1"] {
        let marker = format!(r#""sk":{{"S":"{sk}"}}"#);
        assert_eq!(
            count_occurrences(&combined, &marker),
            1,
            "expected exactly one page to carry sk={sk} across the split-table \
             walk, got pages:\n{combined}"
        );
    }
}
