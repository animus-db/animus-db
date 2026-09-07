//! `SimCluster`-driven end-to-end tests of `Query` pagination (`Limit`/
//! `ExclusiveStartKey`/`LastEvaluatedKey`/`Count`/`ScannedCount`, ADR 0061
//! rung D3 PR 3a, C-04 D3) — driven through the now-generic
//! [`crate::dynamo::run_index_query`] and its GSI/LSI siblings.
//!
//! Replaces five of `crates/animusd/tests/dynamo_query_pagination.rs`'s six
//! tests: `base_query_paginates_a_partition_without_duplicates_or_gaps`,
//! `final_page_carries_no_last_evaluated_key`, `pagination_composes_with_a_
//! sort_key_condition`, `lsi_query_paginates_with_the_scan_cursor_shape`,
//! `cross_index_cursor_mismatch_is_rejected`. **`gsi_query_paginates_with_
//! the_scan_cursor_shape` stays on `ProdEnv`** — it pages over a
//! *materialized* GSI, which `SimCluster` cannot produce (see
//! `sim_cluster_dynamo_query_filter.rs`'s own module doc for the boundary).
//!
//! `cross_index_cursor_mismatch_is_rejected` needs no materialized GSI
//! *row*, despite naming a GSI cursor: `validate_query_cursor_shape` rejects
//! a cursor purely by its `ExclusiveStartKey` **attribute names**, so this
//! version hand-crafts a GSI-shaped (`cat`/`pk`/`sk`) and LSI-shaped
//! (`score`/`pk`/`sk`) cursor literal instead of extracting one from a real
//! truncated GSI page. **It does drop one of the original's four sub-cases**
//! — "a base cursor replayed against the GSI" — found live while
//! converting: `run_gsi_query`'s own empty-page gate
//! (`!meta.has_table_tablet`) runs *before* the cursor check and is
//! unconditionally true here (no hidden GSI tablet is ever created under
//! this fixture), so that one direction always short-circuits to an empty
//! `200` regardless of the cursor's shape — a narrower gap than "GSI rows
//! aren't materialized," since even a row-free shape check can't be reached
//! from that direction. See the test's own doc for the full account.
//!
//! Seed replay (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <test name>`.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// A 3-node cluster with table `events` (composite `pk`/`sk`), a hash-only
/// GSI (`by-cat`) and an LSI (`by-score`) — mirrors `dynamo_query_
/// pagination.rs::setup`: six items, one partition (`p1`), one shared GSI
/// hash value (`cat = "X"`).
fn setup(seed: u64) -> SimCluster {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"events","AttributeDefinitions":[{"AttributeName":"cat","AttributeType":"S"},{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"score","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],
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
    );
    assert_eq!(status, 200, "CreateTable failed: {body} (seed={seed})");

    for i in 0..6 {
        let body = format!(
            r#"{{"TableName":"events","Item":{{
                "pk":{{"S":"p1"}},"sk":{{"S":"a{i}"}},"cat":{{"S":"X"}},
                "score":{{"S":"s{i}"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(status, 200, "PutItem(a{i}) failed: {resp} (seed={seed})");
    }
    cluster
}

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

/// Drive a `Query`'s full `LastEvaluatedKey` pagination loop, round-robining
/// across nodes 0/1/2 so the walk exercises the forwarded-read path too.
/// Mirrors `dynamo_query_pagination.rs::drain_query_pages`.
fn drain_query_pages(cluster: &mut SimCluster, request_prefix: &str, limit: usize) -> String {
    let mut combined = String::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    loop {
        let node = pages as u64 % 3;
        pages += 1;
        assert!(pages < 50, "pagination did not terminate: {combined}");
        let esk = match &cursor {
            Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
            None => String::new(),
        };
        let body = format!("{request_prefix},\"ConsistentRead\":true,\"Limit\":{limit}{esk}}}");
        let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.Query", body.as_bytes());
        assert_eq!(status, 200, "query page failed: {resp}");
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

/// A base `Query` over a partition bigger than `Limit` pages cleanly: every
/// item appears in exactly one page, and only the final page omits
/// `LastEvaluatedKey`. Mirrors `dynamo_query_pagination.rs::base_query_
/// paginates_a_partition_without_duplicates_or_gaps`.
#[test]
fn base_query_paginates_a_partition_without_duplicates_or_gaps() {
    let seed = env_seed(0xE4C2_0001);
    let mut cluster = setup(seed);

    let (status, page1) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","Limit":2,"ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    );
    assert_eq!(status, 200, "{page1} (seed={seed})");
    assert!(page1.contains("\"Count\":2"), "{page1}");
    assert!(page1.contains("\"ScannedCount\":2"), "{page1}");
    assert!(page1.contains("\"LastEvaluatedKey\""), "{page1}");

    let combined = drain_query_pages(
        &mut cluster,
        r#"{"TableName":"events",
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}"#,
        2,
    );
    for i in 0..6 {
        let marker = format!(r#""sk":{{"S":"a{i}"}}"#);
        assert_eq!(
            count_occurrences(&combined, &marker),
            1,
            "expected exactly one page to carry sk=a{i}, got pages:\n{combined} (seed={seed})"
        );
    }
    assert_eq!(count_occurrences(&combined, "\"Count\":2"), 3, "{combined}");
}

/// A `Query`'s final page carries no `LastEvaluatedKey`, even when `Limit`
/// exactly matches the partition's size. Mirrors `dynamo_query_pagination.rs::
/// final_page_carries_no_last_evaluated_key`.
#[test]
fn final_page_carries_no_last_evaluated_key() {
    let seed = env_seed(0xE4C2_0002);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","Limit":6,"ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    );
    assert_eq!(status, 200, "query failed: {body} (seed={seed})");
    assert!(body.contains("\"Count\":6"), "{body}");
    assert!(!body.contains("LastEvaluatedKey"), "{body}");

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","Limit":100,"ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    );
    assert_eq!(status, 200, "query failed: {body} (seed={seed})");
    assert!(body.contains("\"Count\":6"), "{body}");
    assert!(!body.contains("LastEvaluatedKey"), "{body}");
}

/// A `SortKeyCondition` narrows *before* paging: walking the whole cursor
/// chain visits exactly the narrowed items, no more, no fewer. Mirrors
/// `dynamo_query_pagination.rs::pagination_composes_with_a_sort_key_
/// condition`.
#[test]
fn pagination_composes_with_a_sort_key_condition() {
    let seed = env_seed(0xE4C2_0003);
    let mut cluster = setup(seed);

    let combined = drain_query_pages(
        &mut cluster,
        r#"{"TableName":"events",
            "KeyConditionExpression":"pk = :p AND sk BETWEEN :lo AND :hi",
            "ExpressionAttributeValues":
                {":p":{"S":"p1"},":lo":{"S":"a1"},":hi":{"S":"a4"}}"#,
        2,
    );
    for i in 1..=4 {
        let marker = format!(r#""sk":{{"S":"a{i}"}}"#);
        assert_eq!(
            count_occurrences(&combined, &marker),
            1,
            "expected exactly one page to carry sk=a{i}, got pages:\n{combined} (seed={seed})"
        );
    }
    for excluded in ["a0", "a5"] {
        let marker = format!(r#""sk":{{"S":"{excluded}"}}"#);
        assert_eq!(
            count_occurrences(&combined, &marker),
            0,
            "sk={excluded} is outside the sort condition, got pages:\n{combined} (seed={seed})"
        );
    }
}

/// An **LSI** `Query` paginates with the exact same cursor shape
/// [`crate::dynamo::run_lsi_scan`] already uses (the index's own alt-sort
/// attribute plus the base table's key attributes) — strongly consistent, no
/// convergence poll needed. Mirrors `dynamo_query_pagination.rs::lsi_query_
/// paginates_with_the_scan_cursor_shape`.
#[test]
fn lsi_query_paginates_with_the_scan_cursor_shape() {
    let seed = env_seed(0xE4C2_0004);
    let mut cluster = setup(seed);

    let mut combined = String::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    let mut saw_cursor = false;
    loop {
        let node = pages as u64 % 3;
        pages += 1;
        assert!(pages < 50, "pagination did not terminate: {combined}");
        let esk = match &cursor {
            Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
            None => String::new(),
        };
        let body = format!(
            r#"{{"TableName":"events","IndexName":"by-score",
                "ConsistentRead":true,
                "KeyConditionExpression":"pk = :p",
                "ExpressionAttributeValues":{{":p":{{"S":"p1"}}}},
                "Limit":2{esk}}}"#
        );
        let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.Query", body.as_bytes());
        assert_eq!(status, 200, "lsi query page failed: {resp} (seed={seed})");
        let items_part = resp
            .find("\"LastEvaluatedKey\":")
            .map_or(resp.as_str(), |at| &resp[..at]);
        combined.push_str(items_part);
        combined.push('\n');
        match extract_last_evaluated_key(&resp) {
            Some(next) => {
                assert!(
                    next.contains("\"score\"")
                        && next.contains("\"pk\"")
                        && next.contains("\"sk\""),
                    "LSI cursor missing expected attributes: {next} (seed={seed})"
                );
                saw_cursor = true;
                cursor = Some(next);
            }
            None => break,
        }
    }
    assert!(
        saw_cursor,
        "expected at least one truncated page (seed={seed})"
    );
    for i in 0..6 {
        let marker = format!(r#""sk":{{"S":"a{i}"}}"#);
        assert_eq!(
            count_occurrences(&combined, &marker),
            1,
            "expected exactly one page to carry sk=a{i}, got pages:\n{combined} (seed={seed})"
        );
    }
}

/// A cursor built for one target (base/GSI/LSI) is rejected with
/// `ValidationException` when replayed against a different one —
/// `validate_query_cursor_shape` checks only the `ExclusiveStartKey`'s
/// attribute *names*, so this hand-crafts each cursor shape directly rather
/// than reading one back off a real (materialized-under-`ProdEnv`-only) GSI
/// page. Mirrors `dynamo_query_pagination.rs::cross_index_cursor_mismatch_
/// is_rejected`, **minus its "base cursor replayed against the GSI"
/// sub-case** — found live while converting, not anticipated by this rung's
/// design pass: `run_gsi_query`'s own `!meta.has_table_tablet(&index_table)`
/// empty-page gate runs *before* `validate_query_cursor_shape`, and under
/// `SimCluster` that gate is unconditionally true (no drain, hence no hidden
/// GSI tablet, ever gets created — see `sim_cluster_dynamo_table_ops.rs`'s
/// own pinned regression for the identical root cause), so a `Query` against
/// the GSI always short-circuits to an empty `200` before the cursor is ever
/// inspected, regardless of its shape. This is a distinct, narrower gap than
/// "GSI rows aren't materialized" — even a cursor-shape check that reads no
/// row at all is unreachable from this direction. The other three
/// directions (GSI/LSI cursor on the base table; GSI cursor on the LSI) all
/// reach `run_base_query`/`run_lsi_query`, whose own `has_table_tablet`
/// gates are on the *base* table (always hosted here), so they convert
/// cleanly.
#[test]
fn cross_index_cursor_mismatch_is_rejected() {
    let seed = env_seed(0xE4C2_0005);
    let mut cluster = setup(seed);

    let lsi_cursor = r#"{"score":{"S":"s0"},"pk":{"S":"p1"},"sk":{"S":"a0"}}"#;
    let gsi_cursor = r#"{"cat":{"S":"X"},"pk":{"S":"p1"},"sk":{"S":"a0"}}"#;

    // GSI cursor replayed against the base table: rejected (extra `cat`).
    let body = format!(
        r#"{{"TableName":"events","ExclusiveStartKey":{gsi_cursor},
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{{":p":{{"S":"p1"}}}}}}"#
    );
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.Query", body.as_bytes());
    assert_eq!(
        status, 400,
        "GSI cursor accepted on base Query: {resp} (seed={seed})"
    );
    assert!(resp.contains("ValidationException"), "got: {resp}");

    // LSI cursor replayed against the base table: rejected (extra `score`).
    let body = format!(
        r#"{{"TableName":"events","ExclusiveStartKey":{lsi_cursor},
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{{":p":{{"S":"p1"}}}}}}"#
    );
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.Query", body.as_bytes());
    assert_eq!(
        status, 400,
        "LSI cursor accepted on base Query: {resp} (seed={seed})"
    );
    assert!(resp.contains("ValidationException"), "got: {resp}");

    // GSI cursor replayed against the LSI: rejected (`cat` foreign, `score`
    // missing).
    let body = format!(
        r#"{{"TableName":"events","IndexName":"by-score","ExclusiveStartKey":{gsi_cursor},
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{{":p":{{"S":"p1"}}}}}}"#
    );
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.Query", body.as_bytes());
    assert_eq!(
        status, 400,
        "GSI cursor accepted on LSI Query: {resp} (seed={seed})"
    );
    assert!(resp.contains("ValidationException"), "got: {resp}");
}
