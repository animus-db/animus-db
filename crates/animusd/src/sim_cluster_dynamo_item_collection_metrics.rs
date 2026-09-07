//! `SimCluster`-driven end-to-end tests of `ReturnItemCollectionMetrics`
//! over the real DynamoDB wire (ADR 0006, ADR 0061 rung D3 PR 3a, C-04 D3).
//! Every one of these is a plain item op (`PutItem`/`UpdateItem`/
//! `DeleteItem`), already reachable through [`crate::dynamo::
//! dispatch_item_op`] since D2 PR 1 — item-collection sizing is priced at
//! the tablet leader from the base row plus the LSI it hosts, never from a
//! GSI's own hidden table (DynamoDB reports this field only for an LSI, and
//! `nolsi`'s GSI in the fixture below only proves the negative gate), so no
//! GSI-drain boundary applies and all five tests convert with no ProdEnv-
//! only residual.
//!
//! Replaces all five of `crates/animusd/tests/dynamo_item_collection_
//! metrics.rs`'s tests: `no_metrics_are_reported_unless_size_was_asked_for`,
//! `metrics_are_reported_only_for_a_table_that_has_an_lsi`, `every_write_
//! operation_reports_the_collection`, `metrics_agree_from_every_node_
//! including_forwarded_writes`, `the_bound_grows_as_the_collection_does`.
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

/// A 3-node cluster with two tables: `withlsi` (composite `pk`/`sk`, an LSI
/// on `score`) and `nolsi` (same key schema, a GSI but no LSI). Mirrors
/// `dynamo_item_collection_metrics.rs::setup`.
fn setup(seed: u64) -> SimCluster {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"withlsi","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"score","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-score",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"score","KeyType":"RANGE"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    );
    assert_eq!(
        status, 200,
        "CreateTable withlsi failed: {body} (seed={seed})"
    );

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"nolsi","AttributeDefinitions":[{"AttributeName":"cat","AttributeType":"S"},{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-cat",
                 "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    );
    assert_eq!(
        status, 200,
        "CreateTable nolsi failed: {body} (seed={seed})"
    );

    cluster
}

/// A `PutItem` body for `table`, with `extra` spliced in as additional
/// top-level fields (`""` for none).
fn put(table: &str, sk: &str, extra: &str) -> String {
    let tail = if extra.is_empty() {
        String::new()
    } else {
        format!(",{extra}")
    };
    format!(
        r#"{{"TableName":"{table}",
             "Item":{{"pk":{{"S":"p1"}},"sk":{{"S":"{sk}"}},"score":{{"S":"s1"}}}}{tail}}}"#
    )
}

/// Assert a well-formed report and return its upper bound in GB.
fn check_shape(metrics: &Value) -> f64 {
    assert_eq!(
        metrics["ItemCollectionKey"]["pk"]["S"], "p1",
        "the collection is named by the partition key: {metrics}"
    );
    assert!(
        metrics["ItemCollectionKey"].get("sk").is_none(),
        "a collection is a partition, not an item — the sort key must not appear: {metrics}"
    );
    let range = metrics["SizeEstimateRangeGB"]
        .as_array()
        .unwrap_or_else(|| panic!("SizeEstimateRangeGB is an array: {metrics}"));
    assert_eq!(range.len(), 2, "{metrics}");
    let lo = range[0].as_f64().expect("lower bound is a number");
    let hi = range[1].as_f64().expect("upper bound is a number");
    assert_eq!(
        lo, 0.0,
        "the lower end is zero — we bound, we do not measure"
    );
    assert!(hi >= 0.0, "{metrics}");
    hi
}

fn ok_body(cluster: &mut SimCluster, node: u64, target: &str, body: &str) -> Value {
    let (status, resp) = cluster.dynamo(node, target, body.as_bytes());
    assert_eq!(status, 200, "{target} failed: {resp}");
    serde_json::from_str(&resp).expect("json response")
}

/// Mirrors `dynamo_item_collection_metrics.rs::no_metrics_are_reported_
/// unless_size_was_asked_for`.
#[test]
fn no_metrics_are_reported_unless_size_was_asked_for() {
    let seed = env_seed(0xE4C8_0001);
    let mut cluster = setup(seed);

    let body = ok_body(
        &mut cluster,
        0,
        "DynamoDB_20120810.PutItem",
        &put("withlsi", "a0", ""),
    );
    assert!(
        body.get("ItemCollectionMetrics").is_none(),
        "reported metrics nobody asked for: {body} (seed={seed})"
    );

    let (status, resp) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        put(
            "withlsi",
            "a1",
            r#""ReturnItemCollectionMetrics":"SOMETIMES""#,
        )
        .as_bytes(),
    );
    assert_eq!(status, 400, "{resp} (seed={seed})");
    assert!(resp.contains("ValidationException"), "{resp}");
    assert!(resp.contains("SOMETIMES"), "{resp}");
}

/// Mirrors `dynamo_item_collection_metrics.rs::metrics_are_reported_only_
/// for_a_table_that_has_an_lsi`.
#[test]
fn metrics_are_reported_only_for_a_table_that_has_an_lsi() {
    let seed = env_seed(0xE4C8_0002);
    let mut cluster = setup(seed);
    let want = r#""ReturnItemCollectionMetrics":"SIZE""#;

    let body = ok_body(
        &mut cluster,
        0,
        "DynamoDB_20120810.PutItem",
        &put("withlsi", "a0", want),
    );
    check_shape(&body["ItemCollectionMetrics"]);

    let body = ok_body(
        &mut cluster,
        0,
        "DynamoDB_20120810.PutItem",
        &put("nolsi", "a0", want),
    );
    assert!(
        body.get("ItemCollectionMetrics").is_none(),
        "a table without an LSI has no item collection to report: {body} (seed={seed})"
    );
}

/// Mirrors `dynamo_item_collection_metrics.rs::every_write_operation_
/// reports_the_collection`.
#[test]
fn every_write_operation_reports_the_collection() {
    let seed = env_seed(0xE4C8_0003);
    let mut cluster = setup(seed);
    let want = r#""ReturnItemCollectionMetrics":"SIZE""#;

    let body = ok_body(
        &mut cluster,
        0,
        "DynamoDB_20120810.PutItem",
        &put("withlsi", "a0", want),
    );
    check_shape(&body["ItemCollectionMetrics"]);

    let body = ok_body(
        &mut cluster,
        0,
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"withlsi","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "UpdateExpression":"SET note = :v",
            "ExpressionAttributeValues":{":v":{"S":"hi"}},
            "ReturnItemCollectionMetrics":"SIZE"}"#,
    );
    check_shape(&body["ItemCollectionMetrics"]);

    let body = ok_body(
        &mut cluster,
        0,
        "DynamoDB_20120810.DeleteItem",
        r#"{"TableName":"withlsi","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "ReturnItemCollectionMetrics":"SIZE"}"#,
    );
    check_shape(&body["ItemCollectionMetrics"]);
    let _ = seed;
}

/// Mirrors `dynamo_item_collection_metrics.rs::metrics_agree_from_every_
/// node_including_forwarded_writes` — three nodes writing the same
/// collection, at least one of which is a non-leader-hosting node, so a
/// forwarding hop that dropped the field would show up.
#[test]
fn metrics_agree_from_every_node_including_forwarded_writes() {
    let seed = env_seed(0xE4C8_0004);
    let mut cluster = setup(seed);
    let want = r#""ReturnItemCollectionMetrics":"SIZE""#;

    let mut bounds = Vec::new();
    for node in 0..3u64 {
        let body = ok_body(
            &mut cluster,
            node,
            "DynamoDB_20120810.PutItem",
            &put("withlsi", &format!("node{node}"), want),
        );
        let metrics = body.get("ItemCollectionMetrics").unwrap_or_else(|| {
            panic!(
                "node {node} returned no ItemCollectionMetrics — a dropped forwarding hop? \
                 {body} (seed={seed})"
            )
        });
        bounds.push(check_shape(metrics));
    }
    assert_eq!(bounds.len(), 3);
    assert!(
        bounds.windows(2).all(|w| w[1] >= w[0]),
        "a forwarded write priced the collection differently: {bounds:?} (seed={seed})"
    );
}

/// Mirrors `dynamo_item_collection_metrics.rs::the_bound_grows_as_the_
/// collection_does`.
#[test]
fn the_bound_grows_as_the_collection_does() {
    let seed = env_seed(0xE4C8_0005);
    let mut cluster = setup(seed);
    let want = r#""ReturnItemCollectionMetrics":"SIZE""#;
    let blob = "x".repeat(4000);

    let first = ok_body(
        &mut cluster,
        0,
        "DynamoDB_20120810.PutItem",
        &put("withlsi", "a0", want),
    );
    let before = check_shape(&first["ItemCollectionMetrics"]);

    for i in 0..250 {
        let body = format!(
            r#"{{"TableName":"withlsi",
                 "Item":{{"pk":{{"S":"p1"}},"sk":{{"S":"b{i}"}},
                          "score":{{"S":"s{i}"}},"blob":{{"S":"{blob}"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(status, 200, "bulk write {i} failed: {resp} (seed={seed})");
    }

    let last = ok_body(
        &mut cluster,
        0,
        "DynamoDB_20120810.PutItem",
        &put("withlsi", "a1", want),
    );
    let after = check_shape(&last["ItemCollectionMetrics"]);

    assert!(
        after >= before,
        "the bound went down as the collection grew: {before} → {after} (seed={seed})"
    );
}
