//! End-to-end tests for parallel `Scan` (`Segment`/`TotalSegments`) over the
//! real DynamoDB JSON/HTTP wire.
//!
//! The contract is that N workers, each scanning its own segment, see every
//! item **exactly once between them** — no gaps, no duplicates. Here that
//! falls out of the key layout: every data-plane key leads with an 8-byte
//! big-endian partition token (ADR 0022), so the segments are equal slices of
//! the 64-bit token ring.
//!
//! The test that matters is the fleet one: it reassembles the whole table from
//! the segments and compares against an unsegmented scan. A gap or an overlap
//! shows up there and nowhere else.

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
                    "pk":{{"S":"p{i}"}},"sk":{{"S":"a{i}"}},"cat":{{"S":"X"}},
                    "score":{{"S":"s{i}"}},"parity":{{"S":"{parity}"}},
                    "seq":{{"N":"{i}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem(a{i}) failed: {body}");
    }
    (dir, nodes, addrs)
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

/// Collect the `sk` values a scan body returned.
fn sks(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    // Stop at `LastEvaluatedKey` — the cursor is itself an item key, so
    // scanning the whole body would count the page-boundary item twice.
    let mut rest = body
        .find("\"LastEvaluatedKey\":")
        .map_or(body, |at| &body[..at]);
    while let Some(at) = rest.find("\"sk\":{\"S\":\"") {
        let after = &rest[at + "\"sk\":{\"S\":\"".len()..];
        let endq = after.find('"').expect("closing quote");
        out.push(after[..endq].to_string());
        rest = &after[endq..];
    }
    out
}

/// The headline property: a segmented fleet reassembles the table exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_segmented_fleet_sees_every_item_exactly_once() {
    let (_dir, nodes, addrs) = setup().await;

    // The unsegmented truth.
    let (status, whole) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"events"}"#,
    )
    .await;
    assert_eq!(status, 200, "plain scan failed: {whole}");
    let mut expected = sks(&whole);
    expected.sort();
    assert!(!expected.is_empty(), "the fixture seeded rows: {whole}");

    for total in [2u32, 3, 4] {
        let mut seen: Vec<String> = Vec::new();
        for segment in 0..total {
            let body =
                format!(r#"{{"TableName":"events","Segment":{segment},"TotalSegments":{total}}}"#);
            let (status, resp) = dynamo_retry(
                addrs[segment as usize % addrs.len()],
                "DynamoDB_20120810.Scan",
                &body,
            )
            .await;
            assert_eq!(status, 200, "segment {segment}/{total} failed: {resp}");
            seen.extend(sks(&resp));
        }
        seen.sort();
        assert_eq!(
            seen, expected,
            "the {total} segments must reassemble the table with no gap and no duplicate"
        );
    }

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// A segment is a real slice: with more than one segment, at least one comes
/// back smaller than the whole table. Without this, a "parallel" scan that
/// quietly ignored the parameters would still pass the reassembly test by
/// returning everything from every worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segments_actually_partition_rather_than_each_returning_everything() {
    let (_dir, nodes, addrs) = setup().await;

    let (_, whole) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"events"}"#,
    )
    .await;
    let total_rows = sks(&whole).len();
    assert!(total_rows >= 2, "need a few rows to split: {whole}");

    let mut sizes = Vec::new();
    for segment in 0..2 {
        let body = format!(r#"{{"TableName":"events","Segment":{segment},"TotalSegments":2}}"#);
        let (_, resp) = dynamo_retry(addrs[0], "DynamoDB_20120810.Scan", &body).await;
        sizes.push(sks(&resp).len());
    }
    assert_eq!(
        sizes.iter().sum::<usize>(),
        total_rows,
        "the halves sum to the whole"
    );
    assert!(
        sizes.iter().all(|n| *n < total_rows),
        "neither half may be the entire table — that would mean the parameters \
         were ignored: {sizes:?} of {total_rows}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// Pagination composes with segmentation: paging a segment to exhaustion
/// yields that segment's rows, and a cursor never escapes into a neighbour.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_segment_paginates_within_its_own_slice() {
    let (_dir, nodes, addrs) = setup().await;

    let mut all: Vec<String> = Vec::new();
    for segment in 0..3 {
        let mut cursor: Option<String> = None;
        let mut pages = 0;
        loop {
            pages += 1;
            assert!(pages < 20, "segment {segment} did not terminate");
            let esk = match &cursor {
                Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
                None => String::new(),
            };
            let body = format!(
                r#"{{"TableName":"events","Segment":{segment},"TotalSegments":3,"Limit":1{esk}}}"#
            );
            let (status, resp) = dynamo_retry(addrs[1], "DynamoDB_20120810.Scan", &body).await;
            assert_eq!(status, 200, "segment {segment} page failed: {resp}");
            let got = sks(&resp);
            println!(
                "SEG {segment} page {pages}: {got:?} cursor={:?}",
                extract_last_evaluated_key(&resp)
            );
            all.extend(got);
            match extract_last_evaluated_key(&resp) {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
    }

    let (_, whole) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"events"}"#,
    )
    .await;
    let mut expected = sks(&whole);
    expected.sort();
    all.sort();
    assert_eq!(
        all, expected,
        "paging every segment to exhaustion reassembles the table exactly once"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// The validations, over the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_segment_requests_are_rejected() {
    let (_dir, nodes, addrs) = setup().await;

    for body in [
        r#"{"TableName":"events","Segment":0}"#,
        r#"{"TableName":"events","TotalSegments":4}"#,
        r#"{"TableName":"events","Segment":4,"TotalSegments":4}"#,
        r#"{"TableName":"events","Segment":0,"TotalSegments":0}"#,
    ] {
        let (status, resp) = dynamo(addrs[2], "DynamoDB_20120810.Scan", body).await;
        assert_eq!(status, 400, "`{body}` must be rejected: {resp}");
        assert!(resp.contains("ValidationException"), "{resp}");
    }

    for n in nodes {
        n.shutdown_graceful().await;
    }
}
