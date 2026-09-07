//! `SimCluster`-driven end-to-end tests of base-table `Scan` (ADR 0061 rung
//! D3 PR 3a, C-04 D3) — driven through [`crate::dynamo::run_base_scan`] via
//! [`crate::dynamo::dispatch_item_op`]'s no-index arm (needs no new
//! generality: `Scan` with no `IndexName` was already routed through
//! `dispatch_item_op` before this rung).
//!
//! Replaces two of `crates/animusd/tests/dynamo_indexes.rs`'s three tests:
//! `scan_paginates_a_whole_table`, `scan_skips_deleted_items_and_paginates`.
//! **`gsi_write_then_query` stays in that file, untouched** — D2 PR 1 names
//! it as the real-socket proof that `run_operation`'s own path works
//! independently of `dispatch_item_op`, and it is never deleted.
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

/// Pull the `id` string out of a `LastEvaluatedKey` of the form
/// `"LastEvaluatedKey":{"id":{"S":"<v>"}}` in a scan response body.
fn extract_cursor_id(body: &str) -> String {
    let marker = "\"LastEvaluatedKey\":{\"id\":{\"S\":\"";
    let start = body.find(marker).expect("LastEvaluatedKey present") + marker.len();
    let end = start + body[start..].find('"').expect("closing quote");
    body[start..end].to_string()
}

/// Mirrors `dynamo_indexes.rs::scan_paginates_a_whole_table`.
#[test]
fn scan_paginates_a_whole_table() {
    let seed = env_seed(0xE4C9_0001);
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"docs","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "CreateTable failed: {body} (seed={seed})");

    for id in 0..5 {
        let kind = if id % 2 == 0 { "even" } else { "odd" };
        let body = format!(
            r#"{{"TableName":"docs","Item":{{"id":{{"S":"{id}"}},"kind":{{"S":"{kind}"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(status, 200, "PutItem({id}) failed: {resp} (seed={seed})");
    }

    let (status, page1) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Scan",
        br#"{"ConsistentRead":true,"TableName":"docs","Limit":2}"#,
    );
    assert_eq!(status, 200, "Scan page1 failed: {page1} (seed={seed})");
    assert!(page1.contains("\"Count\":2"), "page1: {page1}");
    assert!(page1.contains("\"LastEvaluatedKey\""), "page1: {page1}");
    let cursor_id = extract_cursor_id(&page1);

    let (status, page2) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Scan",
        format!(r#"{{"ConsistentRead":true,"TableName":"docs","ExclusiveStartKey":{{"id":{{"S":"{cursor_id}"}}}}}}"#).as_bytes(),
    );
    assert_eq!(status, 200, "Scan page2 failed: {page2} (seed={seed})");
    assert!(page2.contains("\"Count\":3"), "page2: {page2}");
    assert!(!page2.contains("LastEvaluatedKey"), "page2: {page2}");

    let (status, all) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Scan",
        br#"{"ConsistentRead":true,"TableName":"docs"}"#,
    );
    assert_eq!(status, 200, "{seed}");
    assert!(all.contains("\"Count\":5"), "all: {all}");

    let (status, even) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Scan",
        br#"{"ConsistentRead":true,"TableName":"docs","FilterExpression":"kind = :k",
            "ExpressionAttributeValues":{":k":{"S":"even"}}}"#,
    );
    assert_eq!(status, 200, "filtered scan failed: {even} (seed={seed})");
    assert!(even.contains("\"Count\":3"), "even: {even}");
    assert!(even.contains("\"ScannedCount\":5"), "even: {even}");

    let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.Scan", br#"{"TableName":"ghost"}"#);
    assert_eq!(status, 400, "{seed}");
    assert!(body.contains("ResourceNotFoundException"), "got: {body}");
}

/// A `Scan` after a `DeleteItem` omits the deleted item and keeps
/// pagination correct even when the page boundary would have fallen on the
/// deleted item — the `Limit` counts only live, decoded items, so a
/// tombstone value never consumes a slot or strands the cursor. Mirrors
/// `dynamo_indexes.rs::scan_skips_deleted_items_and_paginates`.
#[test]
fn scan_skips_deleted_items_and_paginates() {
    let seed = env_seed(0xE4C9_0002);
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"docs","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "{body} (seed={seed})");

    for id in 0..5 {
        let body = format!(r#"{{"TableName":"docs","Item":{{"id":{{"S":"{id}"}}}}}}"#);
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(status, 200, "{resp} (seed={seed})");
    }
    let (status, resp) = cluster.dynamo(
        0,
        "DynamoDB_20120810.DeleteItem",
        br#"{"TableName":"docs","Key":{"id":{"S":"1"}}}"#,
    );
    assert_eq!(status, 200, "{resp} (seed={seed})");

    let (status, all) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Scan",
        br#"{"ConsistentRead":true,"TableName":"docs"}"#,
    );
    assert_eq!(status, 200, "scan: {all} (seed={seed})");
    assert!(
        all.contains("\"Count\":4"),
        "deleted item not omitted: {all}"
    );
    assert!(
        !all.contains(r#""id":{"S":"1"}"#),
        "deleted item present: {all}"
    );

    let (status, page1) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Scan",
        br#"{"ConsistentRead":true,"TableName":"docs","Limit":2}"#,
    );
    assert_eq!(status, 200, "page1: {page1} (seed={seed})");
    assert!(page1.contains("\"Count\":2"), "page1 count: {page1}");
    assert!(
        page1.contains("\"LastEvaluatedKey\""),
        "page1 cursor: {page1}"
    );
    let cursor_id = extract_cursor_id(&page1);

    let (status, page2) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Scan",
        format!(r#"{{"ConsistentRead":true,"TableName":"docs","ExclusiveStartKey":{{"id":{{"S":"{cursor_id}"}}}}}}"#).as_bytes(),
    );
    assert_eq!(status, 200, "page2: {page2} (seed={seed})");
    assert!(page2.contains("\"Count\":2"), "page2 count: {page2}");
    assert!(
        !page2.contains(r#""id":{"S":"1"}"#),
        "deleted item paged in: {page2}"
    );
}
