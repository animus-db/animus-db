//! Issue #1245 regression: a table dropped and re-created under the same name
//! with a **different key schema** must be usable on every node, including
//! nodes whose `SchemaRegistry` still held the old entry.
//!
//! `SchemaRegistry::sync_indexes` used to replace only the index set of an
//! already-registered table, so the stale key schema stuck and `GetItem` /
//! `PutItem` failed with ``missing key attribute `id` `` forever.
//!
//! Seed replay: `ANIMUS_SEED=<seed> cargo test -p animusd --lib
//! recreated_table_with_new_key_schema`.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn create(cluster: &mut SimCluster, key: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"recreated","KeySchema":[{{"AttributeName":"{key}","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"{key}","AttributeType":"S"}}]}}"#
    );
    cluster.dynamo(0, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

#[test]
fn recreated_table_with_new_key_schema_works_on_every_node() {
    let seed = env_seed(0x1245_0001);
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = create(&mut cluster, "pk");
    assert_eq!(status, 200, "seed={seed}: CreateTable(pk): {body}");

    // Make every node register the old `pk` schema in its own registry.
    for n in 0..3u64 {
        let item = format!(r#"{{"TableName":"recreated","Item":{{"pk":{{"S":"old{n}"}}}}}}"#);
        let (status, body) = cluster.dynamo(n, "DynamoDB_20120810.PutItem", item.as_bytes());
        assert_eq!(status, 200, "seed={seed}: old PutItem on {n}: {body}");
    }

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.DeleteTable",
        br#"{"TableName":"recreated"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: DeleteTable: {body}");

    let (status, body) = create(&mut cluster, "id");
    assert_eq!(status, 200, "seed={seed}: CreateTable(id): {body}");

    for n in 0..3u64 {
        let item = format!(
            r#"{{"TableName":"recreated","Item":{{"id":{{"S":"new{n}"}},"v":{{"N":"1"}}}}}}"#
        );
        let (status, body) = cluster.dynamo(n, "DynamoDB_20120810.PutItem", item.as_bytes());
        assert_eq!(status, 200, "seed={seed}: new PutItem on node {n}: {body}");
    }
    for n in 0..3u64 {
        for w in 0..3u64 {
            let key = format!(r#"{{"TableName":"recreated","Key":{{"id":{{"S":"new{w}"}}}}}}"#);
            let (status, body) = cluster.dynamo(n, "DynamoDB_20120810.GetItem", key.as_bytes());
            assert_eq!(
                status, 200,
                "seed={seed}: GetItem new{w} on node {n}: {body}"
            );
            assert!(
                body.contains(r#""v":{"N":"1"}"#),
                "seed={seed}: node {n} did not read new{w}: {body}"
            );
        }
    }
}
