//! `SimCluster`-driven end-to-end tests of `ReturnConsumedCapacity` over the
//! real DynamoDB wire (ADR 0006, ADR 0061 rung D3 PR 3a, C-04 D3). Every one
//! of these is a plain item op (`PutItem`/`GetItem`/`UpdateItem`/
//! `DeleteItem`), already reachable through [`crate::dynamo::
//! dispatch_item_op`] since D2 PR 1 — this rung's widening doesn't even
//! touch these call sites; they're converted alongside the `Query`/`Scan`
//! batch because capacity is computed **from the catalog's index
//! definitions plus the written item**, never from a materialized index row
//! (see `dynamo::write_capacity`), so no GSI-drain boundary applies here at
//! all — all seven tests convert with no ProdEnv-only residual.
//!
//! Replaces all seven of `crates/animusd/tests/dynamo_consumed_capacity.rs`'s
//! tests: `no_consumed_capacity_is_reported_unless_it_was_asked_for`,
//! `total_aggregates_the_table_and_its_indexes_into_one_number`, `indexes_
//! charges_each_index_on_its_own_row_not_on_the_base_item`, `an_item_that_
//! is_not_indexed_is_charged_for_no_index`, `an_eventually_consistent_read_
//! is_charged_half_a_unit`, `a_delete_is_charged_on_the_item_it_removed`,
//! `an_update_is_charged_on_the_larger_of_the_two_images`.
//!
//! Seed replay (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <test name>`.

use serde_json::Value;

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// The oversized attribute that pushes the base item past one write unit —
/// mirrors `dynamo_consumed_capacity.rs::BLOB`.
const BLOB: &str = concat!(
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
);

/// A 3-node cluster with table `caps` (composite `pk`/`sk`), a `KEYS_ONLY`
/// GSI on `cat` and an `ALL`-projecting LSI on `score`. Mirrors
/// `dynamo_consumed_capacity.rs::setup`.
fn setup(seed: u64) -> SimCluster {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"caps","AttributeDefinitions":[{"AttributeName":"cat","AttributeType":"S"},{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"score","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-cat",
                 "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"KEYS_ONLY"}}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-score",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"score","KeyType":"RANGE"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    );
    assert_eq!(status, 200, "CreateTable failed: {body} (seed={seed})");
    cluster
}

/// The big item's body, as a `PutItem` `Item` fragment.
fn big_item(sk: &str) -> String {
    format!(
        r#"{{"pk":{{"S":"p1"}},"sk":{{"S":"{sk}"}},
             "cat":{{"S":"X"}},"score":{{"S":"s1"}},
             "blob":{{"S":"{BLOB}"}}}}"#
    )
}

/// The parsed `ConsumedCapacity` of a successful request.
fn capacity_of(cluster: &mut SimCluster, node: u64, target: &str, body: &[u8]) -> Value {
    let (status, resp) = cluster.dynamo(node, target, body);
    assert_eq!(status, 200, "{target} failed: {resp}");
    let parsed: Value = serde_json::from_str(&resp).expect("json response");
    parsed
        .get("ConsumedCapacity")
        .unwrap_or_else(|| panic!("{target} returned no ConsumedCapacity: {resp}"))
        .clone()
}

/// Mirrors `dynamo_consumed_capacity.rs::no_consumed_capacity_is_reported_
/// unless_it_was_asked_for`.
#[test]
fn no_consumed_capacity_is_reported_unless_it_was_asked_for() {
    let seed = env_seed(0xE4C7_0001);
    let mut cluster = setup(seed);

    for (target, body) in [
        (
            "DynamoDB_20120810.PutItem",
            format!(r#"{{"TableName":"caps","Item":{}}}"#, big_item("a0")),
        ),
        (
            "DynamoDB_20120810.GetItem",
            r#"{"TableName":"caps","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}}}"#.to_string(),
        ),
        (
            "DynamoDB_20120810.UpdateItem",
            r#"{"TableName":"caps","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
                "UpdateExpression":"SET note = :v",
                "ExpressionAttributeValues":{":v":{"S":"hi"}}}"#
                .to_string(),
        ),
        (
            "DynamoDB_20120810.DeleteItem",
            r#"{"TableName":"caps","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}}}"#.to_string(),
        ),
    ] {
        let (status, resp) = cluster.dynamo(0, target, body.as_bytes());
        assert_eq!(status, 200, "{target} failed: {resp} (seed={seed})");
        assert!(
            !resp.contains("ConsumedCapacity"),
            "{target} reported capacity nobody asked for: {resp}"
        );
    }

    let (status, resp) = cluster.dynamo(
        0,
        "DynamoDB_20120810.GetItem",
        br#"{"TableName":"caps","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "ReturnConsumedCapacity":"SOMETIMES"}"#,
    );
    assert_eq!(status, 400, "{resp} (seed={seed})");
    assert!(resp.contains("ValidationException"), "{resp}");
}

/// Mirrors `dynamo_consumed_capacity.rs::total_aggregates_the_table_and_
/// its_indexes_into_one_number`.
#[test]
fn total_aggregates_the_table_and_its_indexes_into_one_number() {
    let seed = env_seed(0xE4C7_0002);
    let mut cluster = setup(seed);

    let cc = capacity_of(
        &mut cluster,
        0,
        "DynamoDB_20120810.PutItem",
        format!(
            r#"{{"TableName":"caps","Item":{},"ReturnConsumedCapacity":"TOTAL"}}"#,
            big_item("a0")
        )
        .as_bytes(),
    );

    assert_eq!(cc["TableName"], "caps");
    // base 2 + LSI(ALL) 2 + GSI(KEYS_ONLY) 1.
    assert_eq!(cc["CapacityUnits"], 5.0, "{cc} (seed={seed})");
    assert!(cc.get("Table").is_none(), "{cc}");
    assert!(cc.get("GlobalSecondaryIndexes").is_none(), "{cc}");
    assert!(cc.get("LocalSecondaryIndexes").is_none(), "{cc}");
}

/// Mirrors `dynamo_consumed_capacity.rs::indexes_charges_each_index_on_its_
/// own_row_not_on_the_base_item`.
#[test]
fn indexes_charges_each_index_on_its_own_row_not_on_the_base_item() {
    let seed = env_seed(0xE4C7_0003);
    let mut cluster = setup(seed);

    let cc = capacity_of(
        &mut cluster,
        0,
        "DynamoDB_20120810.PutItem",
        format!(
            r#"{{"TableName":"caps","Item":{},"ReturnConsumedCapacity":"INDEXES"}}"#,
            big_item("a0")
        )
        .as_bytes(),
    );

    assert_eq!(cc["CapacityUnits"], 5.0, "{cc} (seed={seed})");
    assert_eq!(cc["Table"]["CapacityUnits"], 2.0, "{cc}");
    assert_eq!(
        cc["LocalSecondaryIndexes"]["by-score"]["CapacityUnits"],
        2.0
    );
    assert_eq!(cc["GlobalSecondaryIndexes"]["by-cat"]["CapacityUnits"], 1.0);
}

/// Mirrors `dynamo_consumed_capacity.rs::an_item_that_is_not_indexed_is_
/// charged_for_no_index`.
#[test]
fn an_item_that_is_not_indexed_is_charged_for_no_index() {
    let seed = env_seed(0xE4C7_0004);
    let mut cluster = setup(seed);

    let cc = capacity_of(
        &mut cluster,
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"caps",
            "Item":{"pk":{"S":"p1"},"sk":{"S":"bare"},"note":{"S":"x"}},
            "ReturnConsumedCapacity":"INDEXES"}"#,
    );
    assert_eq!(cc["CapacityUnits"], 1.0, "{cc} (seed={seed})");
    assert_eq!(cc["Table"]["CapacityUnits"], 1.0, "{cc}");
    assert!(cc.get("GlobalSecondaryIndexes").is_none(), "{cc}");
    assert!(cc.get("LocalSecondaryIndexes").is_none(), "{cc}");

    let cc = capacity_of(
        &mut cluster,
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"caps",
            "Item":{"pk":{"S":"p1"},"sk":{"S":"half"},"cat":{"S":"X"}},
            "ReturnConsumedCapacity":"INDEXES"}"#,
    );
    assert_eq!(cc["GlobalSecondaryIndexes"]["by-cat"]["CapacityUnits"], 1.0);
    assert!(cc.get("LocalSecondaryIndexes").is_none(), "{cc}");
}

/// Mirrors `dynamo_consumed_capacity.rs::an_eventually_consistent_read_is_
/// charged_half_a_unit`.
#[test]
fn an_eventually_consistent_read_is_charged_half_a_unit() {
    let seed = env_seed(0xE4C7_0005);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        format!(r#"{{"TableName":"caps","Item":{}}}"#, big_item("a0")).as_bytes(),
    );
    assert_eq!(status, 200, "seed failed: {body} (seed={seed})");

    let key = r#""Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}}"#;

    let cc = capacity_of(
        &mut cluster,
        0,
        "DynamoDB_20120810.GetItem",
        format!(r#"{{"TableName":"caps",{key},"ReturnConsumedCapacity":"TOTAL"}}"#).as_bytes(),
    );
    assert_eq!(cc["CapacityUnits"], 0.5, "{cc} (seed={seed})");

    let cc = capacity_of(
        &mut cluster,
        0,
        "DynamoDB_20120810.GetItem",
        format!(
            r#"{{"TableName":"caps",{key},"ConsistentRead":true,
                 "ReturnConsumedCapacity":"TOTAL"}}"#
        )
        .as_bytes(),
    );
    assert_eq!(cc["CapacityUnits"], 1.0, "{cc}");

    let cc = capacity_of(
        &mut cluster,
        0,
        "DynamoDB_20120810.GetItem",
        format!(
            r#"{{"TableName":"caps",{key},"ConsistentRead":true,
                 "ReturnConsumedCapacity":"INDEXES"}}"#
        )
        .as_bytes(),
    );
    assert_eq!(cc["Table"]["CapacityUnits"], 1.0, "{cc}");
    assert!(cc.get("GlobalSecondaryIndexes").is_none(), "{cc}");

    let cc = capacity_of(
        &mut cluster,
        0,
        "DynamoDB_20120810.GetItem",
        format!(
            r#"{{"TableName":"caps",{key},"ConsistentRead":true,
                 "ProjectionExpression":"pk",
                 "ReturnConsumedCapacity":"TOTAL"}}"#
        )
        .as_bytes(),
    );
    assert_eq!(cc["CapacityUnits"], 1.0, "{cc}");
}

/// Mirrors `dynamo_consumed_capacity.rs::a_delete_is_charged_on_the_item_
/// it_removed`.
#[test]
fn a_delete_is_charged_on_the_item_it_removed() {
    let seed = env_seed(0xE4C7_0006);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        format!(r#"{{"TableName":"caps","Item":{}}}"#, big_item("a0")).as_bytes(),
    );
    assert_eq!(status, 200, "seed failed: {body} (seed={seed})");

    let cc = capacity_of(
        &mut cluster,
        0,
        "DynamoDB_20120810.DeleteItem",
        br#"{"TableName":"caps","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "ReturnConsumedCapacity":"INDEXES"}"#,
    );
    assert_eq!(cc["CapacityUnits"], 5.0, "{cc} (seed={seed})");
    assert_eq!(cc["Table"]["CapacityUnits"], 2.0, "{cc}");
    assert_eq!(
        cc["LocalSecondaryIndexes"]["by-score"]["CapacityUnits"],
        2.0
    );
    assert_eq!(cc["GlobalSecondaryIndexes"]["by-cat"]["CapacityUnits"], 1.0);

    let cc = capacity_of(
        &mut cluster,
        0,
        "DynamoDB_20120810.DeleteItem",
        br#"{"TableName":"caps","Key":{"pk":{"S":"p1"},"sk":{"S":"ghost"}},
            "ReturnConsumedCapacity":"INDEXES"}"#,
    );
    assert_eq!(cc["CapacityUnits"], 1.0, "{cc}");
    assert!(cc.get("GlobalSecondaryIndexes").is_none(), "{cc}");
}

/// Mirrors `dynamo_consumed_capacity.rs::an_update_is_charged_on_the_
/// larger_of_the_two_images`.
#[test]
fn an_update_is_charged_on_the_larger_of_the_two_images() {
    let seed = env_seed(0xE4C7_0007);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"caps",
            "Item":{"pk":{"S":"p1"},"sk":{"S":"u"},"cat":{"S":"X"},"score":{"S":"s1"}}}"#,
    );
    assert_eq!(status, 200, "seed failed: {body} (seed={seed})");

    let cc = capacity_of(
        &mut cluster,
        0,
        "DynamoDB_20120810.UpdateItem",
        format!(
            r#"{{"TableName":"caps","Key":{{"pk":{{"S":"p1"}},"sk":{{"S":"u"}}}},
                 "UpdateExpression":"SET blob = :b",
                 "ExpressionAttributeValues":{{":b":{{"S":"{BLOB}"}}}},
                 "ReturnConsumedCapacity":"INDEXES"}}"#
        )
        .as_bytes(),
    );
    assert_eq!(cc["Table"]["CapacityUnits"], 2.0, "{cc} (seed={seed})");
    assert_eq!(
        cc["LocalSecondaryIndexes"]["by-score"]["CapacityUnits"],
        2.0
    );

    let cc = capacity_of(
        &mut cluster,
        0,
        "DynamoDB_20120810.UpdateItem",
        br#"{"TableName":"caps","Key":{"pk":{"S":"p1"},"sk":{"S":"u"}},
            "UpdateExpression":"REMOVE blob",
            "ReturnConsumedCapacity":"INDEXES"}"#,
    );
    assert_eq!(cc["Table"]["CapacityUnits"], 2.0, "{cc}");
}
