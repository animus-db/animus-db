//! `SimCluster`-driven end-to-end test proving a `CreateTable`-declared
//! GSI's **definition** replicates cluster-wide through the catalog (ADR
//! 0061 rung D3 PR 3b, C-04 D3), and that a *second* node — one whose own
//! registry never saw the `CreateTable` at all — resolves a `Query` against
//! it once `[SimCluster::drain_gsi]` has materialized the index's hidden
//! table.
//!
//! Replaces one of `crates/animusd/tests/dynamo_schema.rs`'s three tests:
//! `create_table_index_replicates_to_second_node`. **`create_table_index_
//! survives_node_restart` and `extended_surface` stay on `ProdEnv`** — the
//! former needs a genuine process restart/WAL-durability proof `SimCluster`
//! (`MemoryEngine`-backed, restart-as-wipe-and-rejoin) cannot give; the
//! latter drives `TransactWriteItems` and other operations `dynamo::
//! dispatch_item_op` does not reach yet.
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

/// Mirrors `dynamo_schema.rs::create_table_index_replicates_to_second_node`.
#[test]
fn create_table_index_replicates_to_second_node() {
    let seed = env_seed(0xE4D1_0001);
    let mut cluster = SimCluster::new(seed, 3, 3);

    // CreateTable with a GSI on `email`, projecting ALL.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"users","AttributeDefinitions":[{"AttributeName":"email","AttributeType":"S"},{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-email",
                 "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    );
    assert_eq!(status, 200, "CreateTable failed (seed={seed}): {body}");

    // The index DEFINITION must be visible in every node's own `Metadata`
    // (cluster-wide and durable, not process-local).
    for n in 0..3u64 {
        let meta = cluster.metadata(n);
        assert!(
            meta.table_indexes("users")
                .iter()
                .any(|d| d.name == "by-email"),
            "node {n} does not see the by-email GSI definition (seed={seed})"
        );
    }

    // Write an item through node 0.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"users","Item":{"id":{"S":"u1"},"email":{"S":"a@x"},"v":{"N":"7"}}}"#,
    );
    assert_eq!(status, 200, "PutItem failed (seed={seed}): {body}");

    // Drain the GSI (leader-side), then query it from a SECOND node whose own
    // edge never handled the `CreateTable` at all — it resolves the index's
    // *shape* from the replicated definition and reads the index's hidden
    // table natively, exactly like the `ProdEnv` original's own assertion.
    let meta0 = cluster.metadata(0);
    let (tablet, _) = meta0
        .tablets_for_table("users")
        .next()
        .unwrap_or_else(|| panic!("users has no tablet (seed={seed})"));
    let tablet = *tablet;
    drop(meta0);
    let leader = cluster
        .leader_index_of(tablet)
        .expect("users tablet has a leader");
    cluster.drain_gsi(leader, "users");

    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"users","IndexName":"by-email",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#,
    );
    assert_eq!(
        status, 200,
        "GSI query on second node failed (seed={seed}): {body}"
    );
    assert!(
        body.contains(r#""v":{"N":"7"}"#),
        "second node's own query must return the row (seed={seed}): {body}"
    );
    assert!(
        body.contains(r#""id":{"S":"u1"}"#),
        "id missing (seed={seed}): {body}"
    );
}
