//! `SimCluster`-driven end-to-end tests for ADR 0072's wire-decode
//! validation limits (the layer-2 PR of the "unleashed mode" series):
//! partition/sort-key value size caps, table-name shape validated ahead of
//! existence, and item nesting depth. `sim_cluster_dynamo_key_validation.rs`
//! covers the adjacent empty-key-value rule (issue #848); this file covers
//! the newer ADR 0072 caps, at the same layer for the same reason — the
//! checks live at a real key-schema-aware/catalog-aware edge
//! (`animusd::dynamo::resolve_key`/`animus_dynamo::wire`), not purely in
//! the wire crate's own decode-time unit tests.
//!
//! Seed replay: `ANIMUS_SEED=<seed> cargo test -p animusd --lib <test name>`.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Mirrors `animus_dynamo::limits::MAX_PARTITION_KEY_BYTES`.
const MAX_PARTITION_KEY_BYTES: usize = 2048;
/// Mirrors `animus_dynamo::limits::MAX_SORT_KEY_BYTES`.
const MAX_SORT_KEY_BYTES: usize = 1024;
/// Mirrors `animus_item::MAX_NESTING_DEPTH`.
const MAX_NESTING_DEPTH: usize = 32;

fn long_string(n: usize) -> String {
    "x".repeat(n)
}

/// A JSON-encoded `AttributeValue` nested `depth` `M` levels deep (`depth ==
/// 1` ⇒ a bare `S` leaf) — the wire-level mirror of `animus-item`'s own
/// test-only `nested_map` helper (`crates/animus-item/src/size.rs`,
/// `crates/animus-item/src/update.rs`), rebuilt here in JSON text since this
/// module drives the wire, not the pure item model directly.
fn nested_attribute_json(depth: usize) -> String {
    let mut value = r#"{"S":"leaf"}"#.to_string();
    for _ in 1..depth {
        value = format!(r#"{{"M":{{"x":{value}}}}}"#);
    }
    value
}

/// `PutItem` rejects a partition key value over [`MAX_PARTITION_KEY_BYTES`],
/// and accepts one landing exactly on it.
#[test]
fn put_item_rejects_a_partition_key_value_over_the_cap() {
    let seed = env_seed(0x0072_0001);
    let mut cluster = SimCluster::new(seed, 1, 1);
    cluster.create_table("tbl");

    let at_cap = long_string(MAX_PARTITION_KEY_BYTES);
    let body =
        format!(r#"{{"TableName":"tbl","Item":{{"pk":{{"S":"{at_cap}"}},"sk":{{"S":"s"}}}}}}"#);
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
    assert_eq!(
        status, 200,
        "exactly the partition-key cap must be accepted (seed={seed}): {resp}"
    );

    let over_cap = long_string(MAX_PARTITION_KEY_BYTES + 1);
    let body =
        format!(r#"{{"TableName":"tbl","Item":{{"pk":{{"S":"{over_cap}"}},"sk":{{"S":"s"}}}}}}"#);
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
    assert_eq!(
        status, 400,
        "a partition key value one byte over the cap must be rejected (seed={seed}): {resp}"
    );
    assert!(
        resp.contains("ValidationException"),
        "expected ValidationException (seed={seed}), got: {resp}"
    );
    assert!(
        resp.contains("partition key"),
        "expected the partition-key size message (seed={seed}), got: {resp}"
    );
}

/// `PutItem` rejects a sort key value over [`MAX_SORT_KEY_BYTES`].
#[test]
fn put_item_rejects_a_sort_key_value_over_the_cap() {
    let seed = env_seed(0x0072_0002);
    let mut cluster = SimCluster::new(seed, 1, 1);
    cluster.create_table("tbl");

    let over_cap = long_string(MAX_SORT_KEY_BYTES + 1);
    let body =
        format!(r#"{{"TableName":"tbl","Item":{{"pk":{{"S":"p1"}},"sk":{{"S":"{over_cap}"}}}}}}"#);
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
    assert_eq!(
        status, 400,
        "a sort key value one byte over the cap must be rejected (seed={seed}): {resp}"
    );
    assert!(
        resp.contains("ValidationException"),
        "expected ValidationException (seed={seed}), got: {resp}"
    );
    assert!(
        resp.contains("sort key"),
        "expected the sort-key size message (seed={seed}), got: {resp}"
    );
}

/// The same partition-key size cap applies to `GetItem`'s `Key` map, not
/// just `PutItem`'s `Item` — both resolve their key through the same
/// `animusd::dynamo::resolve_key` choke point.
#[test]
fn get_item_rejects_a_partition_key_value_over_the_cap_in_key() {
    let seed = env_seed(0x0072_0003);
    let mut cluster = SimCluster::new(seed, 1, 1);
    cluster.create_table("tbl");

    let over_cap = long_string(MAX_PARTITION_KEY_BYTES + 1);
    let body = format!(
        r#"{{"TableName":"tbl","Key":{{"pk":{{"S":"{over_cap}"}},"sk":{{"S":"s"}}}},"ConsistentRead":true}}"#
    );
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.GetItem", body.as_bytes());
    assert_eq!(
        status, 400,
        "a GetItem Key partition value over the cap must be rejected (seed={seed}): {resp}"
    );
    assert!(
        resp.contains("ValidationException"),
        "expected ValidationException (seed={seed}), got: {resp}"
    );
}

/// A malformed table name is rejected as `ValidationException` even when the
/// table also does not exist — DynamoDB validates a name's *shape* before it
/// ever checks existence (ADR 0072), so this must never surface as
/// `ResourceNotFoundException`. `Query` is used here since (unlike
/// `PutItem`/`GetItem`'s own legacy-table auto-registration) it genuinely
/// checks table existence against the replicated catalog, so this is a real
/// shape-before-existence race, not a vacuous one.
#[test]
fn malformed_table_name_is_rejected_before_the_existence_check() {
    let seed = env_seed(0x0072_0004);
    let mut cluster = SimCluster::new(seed, 1, 1);

    // "ab" is 2 characters — under AWS's 3-character minimum — and this
    // table was never created, so a check that looked at existence first
    // would report `ResourceNotFoundException` instead.
    let body = br#"{"TableName":"ab","KeyConditionExpression":"pk = :p",
        "ExpressionAttributeValues":{":p":{"S":"x"}}}"#;
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.Query", body);
    assert_eq!(
        status, 400,
        "a malformed table name must be rejected (seed={seed}): {resp}"
    );
    assert!(
        resp.contains("ValidationException"),
        "expected ValidationException, not ResourceNotFoundException (seed={seed}): {resp}"
    );
}

/// `PutItem` accepts an item nested exactly to [`MAX_NESTING_DEPTH`] and
/// rejects one level over.
#[test]
fn put_item_rejects_nesting_depth_over_the_cap() {
    let seed = env_seed(0x0072_0005);
    let mut cluster = SimCluster::new(seed, 1, 1);
    cluster.create_table("tbl");

    let at_cap = nested_attribute_json(MAX_NESTING_DEPTH);
    let body = format!(
        r#"{{"TableName":"tbl","Item":{{"pk":{{"S":"p1"}},"sk":{{"S":"s"}},"deep":{at_cap}}}}}"#
    );
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
    assert_eq!(
        status, 200,
        "exactly the nesting-depth cap must be accepted (seed={seed}): {resp}"
    );

    let over_cap = nested_attribute_json(MAX_NESTING_DEPTH + 1);
    let body = format!(
        r#"{{"TableName":"tbl","Item":{{"pk":{{"S":"p2"}},"sk":{{"S":"s"}},"deep":{over_cap}}}}}"#
    );
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
    assert_eq!(
        status, 400,
        "one level over the nesting-depth cap must be rejected (seed={seed}): {resp}"
    );
    assert!(
        resp.contains("ValidationException"),
        "expected ValidationException (seed={seed}), got: {resp}"
    );
    assert!(
        resp.contains("nesting depth"),
        "expected the nesting-depth message (seed={seed}), got: {resp}"
    );
}
