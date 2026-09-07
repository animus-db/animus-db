//! `SimCluster`-driven end-to-end tests for `KeyConditionExpression` sort-key
//! range comparators (`<`, `<=`, `>`, `>=`, issue #373) and `ScanIndexForward`
//! numeric ordering (ADR 0063), over the base table and an LSI (ADR 0061 rung
//! D3 PR 3a, C-04 D3) — driven through the now-generic [`crate::dynamo::
//! run_index_query`]/[`crate::dynamo::run_lsi_query`].
//!
//! Replaces four of `crates/animusd/tests/dynamo_query_range.rs`'s five
//! tests: `base_table_range_queries_over_mixed_digit_count_n_sort_keys`,
//! `range_operand_type_mismatch_is_rejected`, `lsi_range_queries_over_mixed_
//! digit_count_n_sort_keys`, `scan_index_forward_orders_n_sort_keys_
//! numerically`. **`gsi_range_queries_over_mixed_digit_count_n_sort_keys`
//! stays on `ProdEnv`** — it reads a materialized GSI row, which `SimCluster`
//! cannot produce (see `sim_cluster_dynamo_query_filter.rs`'s own module doc
//! for the boundary).
//!
//! Fixture: sort keys deliberately mixed digit counts and signs (`-10`,
//! `-2`, `1`, `5`, `9`, `10`, `15`, `20`, `100`) — the exact shape a
//! byte-lexicographic compare gets wrong.
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

/// A 3-node cluster with table `readings` (composite `pk`/`sk`, `sk`
/// declared `N`), a composite GSI (`by-device-value`) and a composite LSI
/// (`by-alt`) — mirrors `dynamo_query_range.rs::setup`: nine items in
/// partition `p1`, `sk`/`value`/`alt` all carrying the identical numeric
/// text.
fn setup(seed: u64) -> SimCluster {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"readings",
            "AttributeDefinitions":[{"AttributeName":"alt","AttributeType":"S"},{"AttributeName":"device","AttributeType":"S"},{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"N"},{"AttributeName":"value","AttributeType":"S"}],
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
    );
    assert_eq!(status, 200, "CreateTable failed: {body} (seed={seed})");

    for sk in ["-10", "-2", "1", "5", "9", "10", "15", "20", "100"] {
        let body = format!(
            r#"{{"TableName":"readings","Item":{{
                "pk":{{"S":"p1"}},"sk":{{"N":"{sk}"}},
                "device":{{"S":"d1"}},"value":{{"N":"{sk}"}},
                "alt":{{"N":"{sk}"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(status, 200, "PutItem(sk={sk}) failed: {resp} (seed={seed})");
    }
    cluster
}

/// The `sk` values a body contains, in first-appearance order.
fn sk_order(body: &str) -> Vec<String> {
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
    out
}

/// [`sk_order`], order-independent (for a membership assertion).
fn sk_values(body: &str) -> Vec<String> {
    let mut out = sk_order(body);
    out.sort();
    out
}

/// Every range comparator, over the base table, against the mixed-digit-count
/// and mixed-sign fixture. Mirrors `dynamo_query_range.rs::base_table_range_
/// queries_over_mixed_digit_count_n_sort_keys`.
#[test]
fn base_table_range_queries_over_mixed_digit_count_n_sort_keys() {
    let seed = env_seed(0xE4C3_0001);
    let mut cluster = setup(seed);

    let mut query = |node: u64, op: &str, v: &str| -> Vec<String> {
        let body = format!(
            r#"{{"TableName":"readings","ConsistentRead":true,
                 "KeyConditionExpression":"pk = :p AND sk {op} :v",
                 "ExpressionAttributeValues":{{":p":{{"S":"p1"}},":v":{{"N":"{v}"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.Query", body.as_bytes());
        assert_eq!(status, 200, "sk {op} {v} failed: {resp} (seed={seed})");
        sk_values(&resp)
    };

    assert_eq!(
        query(1, ">", "9"),
        vec!["10", "100", "15", "20"],
        "strictly greater than 9, numerically"
    );
    assert_eq!(query(1, ">=", "9"), vec!["10", "100", "15", "20", "9"]);
    assert_eq!(query(1, "<", "10"), vec!["-10", "-2", "1", "5", "9"]);
    assert_eq!(query(1, "<=", "-2"), vec!["-10", "-2"]);
}

/// A sort-key condition operand whose type disagrees with the table's
/// declared sort-key `AttributeType` is a `ValidationException`. Mirrors
/// `dynamo_query_range.rs::range_operand_type_mismatch_is_rejected`.
#[test]
fn range_operand_type_mismatch_is_rejected() {
    let seed = env_seed(0xE4C3_0002);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"readings",
            "KeyConditionExpression":"pk = :p AND sk > :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"9"}}}"#,
    );
    assert_eq!(
        status, 400,
        "an S operand against a declared-N sort key must be rejected: {body} (seed={seed})"
    );
    assert!(body.contains("ValidationException"), "{body}");
}

/// The same range comparators over a composite **LSI**'s own `N` alt-sort
/// attribute — strongly consistent, no polling needed. Mirrors
/// `dynamo_query_range.rs::lsi_range_queries_over_mixed_digit_count_n_sort_
/// keys`.
#[test]
fn lsi_range_queries_over_mixed_digit_count_n_sort_keys() {
    let seed = env_seed(0xE4C3_0003);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"readings","IndexName":"by-alt","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p AND alt >= :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"N":"10"}}}"#,
    );
    assert_eq!(status, 200, "LSI range query failed: {body} (seed={seed})");
    let values: Vec<String> = sk_values(&body);
    assert_eq!(values, vec!["10", "100", "15", "20"]);
}

/// `ScanIndexForward` is numeric order for an `N` sort key (ADR 0063), over
/// the base table. Mirrors `dynamo_query_range.rs::scan_index_forward_
/// orders_n_sort_keys_numerically`.
#[test]
fn scan_index_forward_orders_n_sort_keys_numerically() {
    let seed = env_seed(0xE4C3_0004);
    let mut cluster = setup(seed);

    for sk in ["-10", "-5", "0", "2", "10", "100", "2.5"] {
        let body = format!(
            r#"{{"TableName":"readings","Item":{{"pk":{{"S":"ord"}},"sk":{{"N":"{sk}"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(status, 200, "PutItem(sk={sk}) failed: {resp} (seed={seed})");
    }

    let (status, asc) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"readings","ConsistentRead":true,"ScanIndexForward":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"ord"}}}"#,
    );
    assert_eq!(status, 200, "ascending query failed: {asc} (seed={seed})");
    assert_eq!(
        sk_order(&asc),
        vec!["-10", "-5", "0", "2", "2.5", "10", "100"],
        "ScanIndexForward:true returns ascending numeric order: {asc}"
    );

    let (status, desc) = cluster.dynamo(
        2,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"readings","ConsistentRead":true,"ScanIndexForward":false,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"ord"}}}"#,
    );
    assert_eq!(status, 200, "descending query failed: {desc} (seed={seed})");
    assert_eq!(
        sk_order(&desc),
        vec!["100", "10", "2.5", "2", "0", "-5", "-10"],
        "ScanIndexForward:false returns descending numeric order: {desc}"
    );
}
