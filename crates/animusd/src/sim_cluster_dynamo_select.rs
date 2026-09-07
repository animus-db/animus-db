//! `SimCluster`-driven end-to-end tests of `Query`/`Scan`'s `Select` (ADR
//! 0061 rung D3 PR 3a, C-04 D3) — driven through the now-generic
//! [`crate::dynamo::run_index_query`]/[`crate::dynamo::run_index_scan`]
//! (`Select` is decoded ahead of index dispatch, so a base `Query`/`Scan`
//! test needs neither).
//!
//! Replaces six of `crates/animusd/tests/dynamo_select.rs`'s seven tests:
//! `count_select_returns_counts_without_items`, `count_select_still_
//! applies_the_filter`, `count_select_paginates_and_sums_to_the_whole_
//! partition`, `count_select_applies_to_scan`, `specific_attributes_
//! returns_the_projection`, `contradictory_select_requests_are_rejected`.
//! **`count_select_applies_to_a_gsi_query` stays on `ProdEnv`** — it reads a
//! materialized GSI row, which `SimCluster` cannot produce (see
//! `sim_cluster_dynamo_query_filter.rs`'s own module doc for the boundary).
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
/// GSI (`by-cat`) and an LSI (`by-score`) — mirrors `dynamo_select.rs::
/// setup`: six items, one partition, `parity` alternating `even`/`odd`.
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
        let parity = if i % 2 == 0 { "even" } else { "odd" };
        let body = format!(
            r#"{{"TableName":"events","Item":{{
                "pk":{{"S":"p1"}},"sk":{{"S":"a{i}"}},"cat":{{"S":"X"}},
                "score":{{"S":"s{i}"}},"parity":{{"S":"{parity}"}}}}}}"#
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

/// `Select: COUNT` returns counts and no `Items`. Mirrors `dynamo_select.rs::
/// count_select_returns_counts_without_items`.
#[test]
fn count_select_returns_counts_without_items() {
    let seed = env_seed(0xE4C6_0001);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "Select":"COUNT"}"#,
    );
    assert_eq!(status, 200, "COUNT query failed: {body} (seed={seed})");
    assert!(
        !body.contains("\"Items\""),
        "COUNT must not carry an Items array: {body}"
    );
    assert_eq!(field(&body, "Count"), Some(6), "{body}");
    assert_eq!(field(&body, "ScannedCount"), Some(6), "{body}");
    assert!(!body.contains("\"a0\""), "no item payload leaked: {body}");
}

/// `COUNT` still runs the filter. Mirrors `dynamo_select.rs::count_select_
/// still_applies_the_filter`.
#[test]
fn count_select_still_applies_the_filter() {
    let seed = env_seed(0xE4C6_0002);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"parity = :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"even"}},
            "Select":"COUNT"}"#,
    );
    assert_eq!(status, 200, "filtered COUNT failed: {body} (seed={seed})");
    assert!(!body.contains("\"Items\""), "{body}");
    assert_eq!(field(&body, "Count"), Some(3), "three even items: {body}");
    assert_eq!(
        field(&body, "ScannedCount"),
        Some(6),
        "all six were examined: {body}"
    );
}

/// A truncated `COUNT` page still paginates and sums to the whole partition.
/// Mirrors `dynamo_select.rs::count_select_paginates_and_sums_to_the_whole_
/// partition`.
#[test]
fn count_select_paginates_and_sums_to_the_whole_partition() {
    let seed = env_seed(0xE4C6_0003);
    let mut cluster = setup(seed);

    let mut total = 0i64;
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    loop {
        let node = pages as u64 % 3;
        pages += 1;
        assert!(
            pages < 20,
            "COUNT pagination did not terminate (seed={seed})"
        );
        let esk = match &cursor {
            Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
            None => String::new(),
        };
        let req = format!(
            r#"{{"TableName":"events","ConsistentRead":true,
                 "KeyConditionExpression":"pk = :p",
                 "ExpressionAttributeValues":{{":p":{{"S":"p1"}}}},
                 "Select":"COUNT","Limit":2{esk}}}"#
        );
        let (status, body) = cluster.dynamo(node, "DynamoDB_20120810.Query", req.as_bytes());
        assert_eq!(status, 200, "COUNT page failed: {body} (seed={seed})");
        assert!(!body.contains("\"Items\""), "still no Items: {body}");
        total += field(&body, "Count").unwrap_or_default();
        match extract_last_evaluated_key(&body) {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(
        total, 6,
        "the pages sum to the whole partition (seed={seed})"
    );
    assert!(pages > 1, "the walk really was paginated ({pages} pages)");
}

/// `Select: COUNT` reaches `Scan` through the same decode path. Mirrors
/// `dynamo_select.rs::count_select_applies_to_scan`.
#[test]
fn count_select_applies_to_scan() {
    let seed = env_seed(0xE4C6_0004);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        2,
        "DynamoDB_20120810.Scan",
        br#"{"TableName":"events","ConsistentRead":true,"Select":"COUNT"}"#,
    );
    assert_eq!(status, 200, "COUNT scan failed: {body} (seed={seed})");
    assert!(!body.contains("\"Items\""), "{body}");
    assert!(
        field(&body, "Count").unwrap_or_default() >= 6,
        "the scan counted the table: {body}"
    );
}

/// `SPECIFIC_ATTRIBUTES` alongside a projection returns exactly the
/// projected attributes. Mirrors `dynamo_select.rs::specific_attributes_
/// returns_the_projection`.
#[test]
fn specific_attributes_returns_the_projection() {
    let seed = env_seed(0xE4C6_0005);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "ProjectionExpression":"sk","Select":"SPECIFIC_ATTRIBUTES","Limit":1}"#,
    );
    assert_eq!(
        status, 200,
        "SPECIFIC_ATTRIBUTES failed: {body} (seed={seed})"
    );
    assert!(body.contains("\"sk\""), "the projected attribute: {body}");
    assert!(
        !body.contains("\"parity\""),
        "an unprojected attribute must not appear: {body}"
    );
}

/// Contradictory `Select` requests are `ValidationException`, not a 500 and
/// not a success. Mirrors `dynamo_select.rs::contradictory_select_requests_
/// are_rejected`.
#[test]
fn contradictory_select_requests_are_rejected() {
    let seed = env_seed(0xE4C6_0006);
    let mut cluster = setup(seed);

    let mut reject = |body: &[u8]| {
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.Query", body);
        assert_eq!(
            status, 400,
            "expected a validation error, got: {resp} (seed={seed})"
        );
        assert!(
            resp.contains("ValidationException"),
            "expected ValidationException: {resp}"
        );
    };

    reject(
        br#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "Select":"SPECIFIC_ATTRIBUTES"}"#,
    );
    reject(
        br#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "ProjectionExpression":"sk","Select":"COUNT"}"#,
    );
    reject(
        br#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "Select":"ALL_PROJECTED_ATTRIBUTES"}"#,
    );
    reject(
        br#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "Select":"EVERYTHING"}"#,
    );
}
