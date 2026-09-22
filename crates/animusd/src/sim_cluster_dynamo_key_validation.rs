//! `SimCluster`-driven end-to-end tests for issue #848's empty-key-value
//! rule: AWS's 2020 empty-value change allows an empty `S`/`B` for a
//! **non-key** attribute, but a partition/sort key value must stay
//! non-empty. The check lives at the `animusd` edge
//! (`dynamo::resolve_key`/`reject_empty_key_value`), not in
//! `animus_dynamo::wire`'s decoder — the wire layer never sees which
//! attribute *is* the key, only `animusd`'s registry mirror of the
//! replicated schema does — so this needs a real key-schema-aware path,
//! not just a decode-time unit test.
//!
//! Seed replay: `ANIMUS_SEED=<seed> cargo test -p animusd --lib <test name>`.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// `SimCluster::create_table` always declares a composite `(pk, sk)`
/// schema, both `S` — an empty partition key value must be rejected.
#[test]
fn put_item_rejects_an_empty_partition_key() {
    let seed = env_seed(0x0848_0001);
    let mut cluster = SimCluster::new(seed, 1, 1);
    cluster.create_table("tbl");

    let body = br#"{"TableName":"tbl","Item":{"pk":{"S":""},"sk":{"S":"s1"}}}"#;
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body);
    assert_eq!(
        status, 400,
        "an empty S partition key must be rejected (seed={seed}): {resp}"
    );
    assert!(
        resp.contains("ValidationException"),
        "expected ValidationException (seed={seed}), got: {resp}"
    );
    assert!(
        resp.contains("cannot contain an empty string value"),
        "expected AWS's empty-key-value message (seed={seed}), got: {resp}"
    );
}

/// The same rule applies to the sort key.
#[test]
fn put_item_rejects_an_empty_sort_key() {
    let seed = env_seed(0x0848_0002);
    let mut cluster = SimCluster::new(seed, 1, 1);
    cluster.create_table("tbl");

    let body = br#"{"TableName":"tbl","Item":{"pk":{"S":"p1"},"sk":{"S":""}}}"#;
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body);
    assert_eq!(
        status, 400,
        "an empty S sort key must be rejected (seed={seed}): {resp}"
    );
    assert!(
        resp.contains("ValidationException"),
        "expected ValidationException (seed={seed}), got: {resp}"
    );
}

/// The 2020 empty-value change is not reverted: an empty `S` on a
/// **non-key** attribute is still accepted and round-trips.
#[test]
fn put_item_accepts_an_empty_non_key_string() {
    let seed = env_seed(0x0848_0003);
    let mut cluster = SimCluster::new(seed, 1, 1);
    cluster.create_table("tbl");

    let body = br#"{"TableName":"tbl","Item":{"pk":{"S":"p1"},"sk":{"S":"s1"},"note":{"S":""}}}"#;
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body);
    assert_eq!(
        status, 200,
        "an empty S non-key attribute must still be accepted (seed={seed}): {resp}"
    );

    let (status, item) = cluster.dynamo(
        0,
        "DynamoDB_20120810.GetItem",
        br#"{"TableName":"tbl","Key":{"pk":{"S":"p1"},"sk":{"S":"s1"}},"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {item}");
    assert!(
        item.contains(r#""note":{"S":""}"#),
        "the empty non-key string must round-trip (seed={seed}): {item}"
    );
}
