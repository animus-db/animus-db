//! `SimCluster`-driven end-to-end test of `ConsistentRead` fidelity (ADR
//! 0041 §5) on `Query`, over the real DynamoDB wire (ADR 0061 rung D3 PR 3a,
//! C-04 D3) — driven through the now-generic [`crate::dynamo::
//! run_index_query`].
//!
//! Replaces `crates/animusd/tests/dynamo_consistent_read.rs`'s one test:
//! `consistent_read_rejects_gsi_query_but_accepts_lsi_and_base`. Unlike most
//! of this rung's other GSI-adjacent conversions, this one needs no
//! materialized GSI row at all: `run_index_query`'s `ConsistentRead: true`
//! rejection for a `Global` index fires before any row is ever read (see
//! `run_index_query`'s own doc), so the `ProdEnv` original's convergence poll
//! (waiting for the drain before probing the rejection) is dropped here —
//! it made the test robust to either implementation choice, but the rejection
//! actually being early is exactly what this conversion can now assert
//! directly, since a `SimCluster` GSI `Query` would otherwise just read as
//! empty (see `sim_cluster_dynamo_query_filter.rs`'s own module doc for that
//! boundary) rather than ever converging.
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

#[test]
fn consistent_read_rejects_gsi_query_but_accepts_lsi_and_base() {
    let seed = env_seed(0xE4C5_0001);
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"events","AttributeDefinitions":[{"AttributeName":"kind","AttributeType":"S"},{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"},{"AttributeName":"ts","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-kind",
                 "KeySchema":[{"AttributeName":"kind","KeyType":"HASH"}]}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-ts",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"ts","KeyType":"RANGE"}]}]}"#,
    );
    assert_eq!(status, 200, "CreateTable failed: {body} (seed={seed})");

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"events","Item":{
            "pk":{"S":"p1"},"sk":{"S":"a"},"kind":{"S":"click"},"ts":{"S":"10"}}}"#,
    );
    assert_eq!(status, 200, "PutItem failed: {body} (seed={seed})");

    // `ConsistentRead: true` against the GSI is a `ValidationException` —
    // DynamoDB's own contract, and (unlike the `ProdEnv` original) asserted
    // directly here with no drain to wait on first: the rejection fires
    // before any row of the (empty, under this fixture) hidden table is ever
    // touched.
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","IndexName":"by-kind","ConsistentRead":true,
            "KeyConditionExpression":"kind = :k",
            "ExpressionAttributeValues":{":k":{"S":"click"}}}"#,
    );
    assert_eq!(
        status, 400,
        "GSI ConsistentRead should be rejected: {body} (seed={seed})"
    );
    assert!(
        body.contains("ValidationException"),
        "expected ValidationException: {body}"
    );

    // `ConsistentRead: true` against the LSI is legal (commits atomically
    // with the base row) — no drain to wait on.
    let (status, body) = cluster.dynamo(
        2,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","IndexName":"by-ts","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    );
    assert_eq!(
        status, 200,
        "LSI ConsistentRead should be accepted: {body} (seed={seed})"
    );
    assert!(body.contains("\"Count\":1"), "LSI query result: {body}");

    // `ConsistentRead: true` against the base table (no `IndexName`) is
    // likewise legal.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    );
    assert_eq!(
        status, 200,
        "base ConsistentRead should be accepted: {body} (seed={seed})"
    );
    assert!(body.contains("\"Count\":1"), "base query result: {body}");

    // `ConsistentRead: true` against a plain `GetItem` is likewise accepted.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.GetItem",
        br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a"}},
            "ConsistentRead":true}"#,
    );
    assert_eq!(
        status, 200,
        "GetItem ConsistentRead should be accepted: {body} (seed={seed})"
    );
    assert!(body.contains("\"Item\""), "GetItem result: {body}");
}
