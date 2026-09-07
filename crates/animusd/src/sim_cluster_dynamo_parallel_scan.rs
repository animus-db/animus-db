//! `SimCluster`-driven end-to-end tests for parallel `Scan`
//! (`Segment`/`TotalSegments`) over the DynamoDB wire (ADR 0061 rung D3
//! PR 1) — replaces the real-socket `ProdEnv` binary
//! `crates/animusd/tests/dynamo_parallel_scan.rs`, whose four tests are all
//! base-table-only. Driven through `SimCluster::dynamo` — see
//! `sim_cluster_dynamo.rs`'s own module doc.
//!
//! The contract is that N workers, each scanning its own segment, see every
//! item **exactly once between them** — no gaps, no duplicates. That falls
//! out of the key layout: every data-plane key leads with an 8-byte
//! big-endian partition token (ADR 0022), so the segments are equal slices
//! of the 64-bit token ring.
//!
//! Seed replay: `ANIMUS_SEED=<seed> cargo test -p animusd --lib <test name>`.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Seed a 3-node RF3 cluster with one base table (`events`) and six items
/// spread across six different partitions (`p0..p5`) — the fixture's own
/// spread is load-bearing for the segment-slicing tests, unlike the shared
/// `pk = "p1"` fixture the other converted `dynamo_*` families use.
fn setup(seed: u64) -> SimCluster {
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("events");
    for i in 0..6u32 {
        let parity = if i % 2 == 0 { "even" } else { "odd" };
        let body = format!(
            r#"{{"TableName":"events","Item":{{
                "pk":{{"S":"p{i}"}},"sk":{{"S":"a{i}"}},"cat":{{"S":"X"}},
                "score":{{"S":"s{i}"}},"parity":{{"S":"{parity}"}},
                "seq":{{"N":"{i}"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(
            status, 200,
            "seed PutItem(a{i}) failed (seed={seed}): {resp}"
        );
    }
    cluster
}

/// Extract the raw `LastEvaluatedKey` JSON object verbatim from a `Scan`
/// response body — brace-matched, since it can be an arbitrary
/// AttributeValue map shape. `None` when the page wasn't truncated.
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
#[test]
fn a_segmented_fleet_sees_every_item_exactly_once() {
    let seed = env_seed(0x9CA1_0001);
    let mut cluster = setup(seed);

    let (status, whole) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Scan",
        br#"{"TableName":"events","ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "plain scan failed (seed={seed}): {whole}");
    let mut expected = sks(&whole);
    expected.sort();
    assert!(
        !expected.is_empty(),
        "the fixture seeded rows (seed={seed}): {whole}"
    );

    for total in [2u32, 3, 4] {
        let mut seen: Vec<String> = Vec::new();
        for segment in 0..total {
            let body = format!(
                r#"{{"TableName":"events","ConsistentRead":true,
                        "Segment":{segment},"TotalSegments":{total}}}"#
            );
            let node = segment as u64 % cluster.node_count() as u64;
            let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.Scan", body.as_bytes());
            assert_eq!(
                status, 200,
                "segment {segment}/{total} failed (seed={seed}): {resp}"
            );
            seen.extend(sks(&resp));
        }
        seen.sort();
        assert_eq!(
            seen, expected,
            "the {total} segments must reassemble the table with no gap and no duplicate (seed={seed})"
        );
    }
}

/// A segment is a real slice: with more than one segment, at least one
/// comes back smaller than the whole table.
#[test]
fn segments_actually_partition_rather_than_each_returning_everything() {
    let seed = env_seed(0x9CA1_0002);
    let mut cluster = setup(seed);

    let (_, whole) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Scan",
        br#"{"TableName":"events","ConsistentRead":true}"#,
    );
    let total_rows = sks(&whole).len();
    assert!(
        total_rows >= 2,
        "need a few rows to split (seed={seed}): {whole}"
    );

    let mut sizes = Vec::new();
    for segment in 0..2 {
        let body = format!(
            r#"{{"TableName":"events","ConsistentRead":true,"Segment":{segment},"TotalSegments":2}}"#
        );
        let (_, resp) = cluster.dynamo(0, "DynamoDB_20120810.Scan", body.as_bytes());
        sizes.push(sks(&resp).len());
    }
    assert_eq!(
        sizes.iter().sum::<usize>(),
        total_rows,
        "the halves sum to the whole (seed={seed})"
    );
    assert!(
        sizes.iter().all(|n| *n < total_rows),
        "neither half may be the entire table — that would mean the parameters \
         were ignored (seed={seed}): {sizes:?} of {total_rows}"
    );
}

/// Pagination composes with segmentation: paging a segment to exhaustion
/// yields that segment's rows, and a cursor never escapes into a neighbour.
#[test]
fn a_segment_paginates_within_its_own_slice() {
    let seed = env_seed(0x9CA1_0003);
    let mut cluster = setup(seed);

    let mut all: Vec<String> = Vec::new();
    for segment in 0..3 {
        let mut cursor: Option<String> = None;
        let mut pages = 0;
        loop {
            pages += 1;
            assert!(
                pages < 20,
                "segment {segment} did not terminate (seed={seed})"
            );
            let esk = match &cursor {
                Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
                None => String::new(),
            };
            let body = format!(
                r#"{{"TableName":"events","ConsistentRead":true,
                        "Segment":{segment},"TotalSegments":3,"Limit":1{esk}}}"#
            );
            let (status, resp) = cluster.dynamo(1, "DynamoDB_20120810.Scan", body.as_bytes());
            assert_eq!(
                status, 200,
                "segment {segment} page failed (seed={seed}): {resp}"
            );
            let got = sks(&resp);
            all.extend(got);
            match extract_last_evaluated_key(&resp) {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
    }

    let (_, whole) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Scan",
        br#"{"TableName":"events","ConsistentRead":true}"#,
    );
    let mut expected = sks(&whole);
    expected.sort();
    all.sort();
    assert_eq!(
        all, expected,
        "paging every segment to exhaustion reassembles the table exactly once (seed={seed})"
    );
}

/// The validations, over the wire.
#[test]
fn malformed_segment_requests_are_rejected() {
    let seed = env_seed(0x9CA1_0004);
    let mut cluster = setup(seed);

    for body in [
        r#"{"TableName":"events","Segment":0}"#,
        r#"{"TableName":"events","TotalSegments":4}"#,
        r#"{"TableName":"events","Segment":4,"TotalSegments":4}"#,
        r#"{"TableName":"events","Segment":0,"TotalSegments":0}"#,
    ] {
        let (status, resp) = cluster.dynamo(2, "DynamoDB_20120810.Scan", body.as_bytes());
        assert_eq!(
            status, 400,
            "`{body}` must be rejected (seed={seed}): {resp}"
        );
        assert!(resp.contains("ValidationException"), "seed={seed}: {resp}");
    }
}
