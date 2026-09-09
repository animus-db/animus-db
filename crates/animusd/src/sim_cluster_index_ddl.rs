//! `SimCluster`-driven end-to-end tests of index DDL **beyond plain
//! `CreateTable`** (ADR 0061 rung J, C-10 PR 2) — the groundwork
//! `docs/roadmap.md`'s C-04 residual inventory named as unowned: `UpdateTable`
//! adding/dropping a GSI on a populated table via `dispatch_table_op`'s new
//! index-change sub-arm, `crate::dynamo::create_index`/`drop_index`, and the
//! now-generic `crate::index_drain::backfill_seed_tick` (driven on demand via
//! [`SimCluster::drive_backfill_seed`]) alongside the always-on
//! `index_backfill::index_backfill_loop` completion aggregator (spawned
//! unconditionally by [`SimCluster::new`]/[`SimCluster::restart`] since this
//! rung).
//!
//! Two scenarios (`_over_seeds` at 5 seeds each, mirroring every sibling
//! module's own convention — `ANIMUS_SEED=<seed> cargo test -p animusd --lib
//! <test name>` replays any one):
//!
//! (a) `UpdateTable` adding a GSI on a table already holding rows returns the
//!     re-described table with the new index `CREATING`/`Backfilling: true`;
//!     a `Query` against it is rejected while backfilling
//!     (`ValidationException`, mirroring the real `NoSuchIndex` dispatch a
//!     not-yet-`Active` index gets); [`SimCluster::drive_backfill_seed`]
//!     seeds the base table's own tablet, [`SimCluster::drain_gsi`]
//!     materializes the hidden index table's rows, and — since the
//!     always-on completion aggregator needs a further tick to observe the
//!     freshly-reported tablet — a small converged-or-timeout loop of
//!     further `DescribeTable` calls (never a single call assumed to
//!     finish everything: a populated table can need several
//!     `BACKFILL_SEED_BATCH`-sized ticks, though this scenario's own row
//!     count fits in one) confirms the index reaches `ACTIVE`, at which
//!     point the `Query` returns exactly the expected rows.
//! (b) `UpdateTable` deleting that GSI leaves it absent from `DescribeTable`
//!     and a `Query` against the now-gone index name is rejected the
//!     identical `ValidationException` way.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Pull `(IndexStatus, Backfilling)` for `index` out of a `DescribeTable`/
/// `UpdateTable` response body's `GlobalSecondaryIndexes` array — `None` if
/// the index isn't listed at all (dropped, or never created). Mirrors
/// `tests/update_table_create_index.rs::index_status` (duplicated rather
/// than shared, per this crate's own per-file-fixture convention).
fn index_status(body: &str, index: &str) -> Option<(String, bool)> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let table = v.get("Table").or_else(|| v.get("TableDescription"))?;
    let gsis = table.get("GlobalSecondaryIndexes")?.as_array()?;
    let entry = gsis
        .iter()
        .find(|g| g.get("IndexName").and_then(|n| n.as_str()) == Some(index))?;
    let status = entry.get("IndexStatus")?.as_str()?.to_owned();
    let backfilling = entry
        .get("Backfilling")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    Some((status, backfilling))
}

/// Find the tablet leader of `table`'s (sole) tablet — mirrors
/// `sim_cluster_dynamo_indexes.rs::gsi_write_then_query_sim`'s own idiom.
fn leader_of_table(cluster: &SimCluster, table: &str) -> u64 {
    let meta = cluster.metadata(0);
    let (tablet, _) = meta
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("{table} has no tablet"));
    let tablet = *tablet;
    drop(meta);
    cluster
        .leader_index_of(tablet)
        .unwrap_or_else(|| panic!("{table} tablet has no leader"))
}

/// Drive the backfill seeder + GSI drain to exhaustion, then poll
/// `DescribeTable` (a real op call each time, which is what actually gives
/// the always-on `index_backfill_loop` completion aggregator a chance to
/// observe the freshly-reported tablet and flip the index `Active`) until
/// `index` converges to `ACTIVE` — never a single call assumed to finish a
/// populated table's backfill in one pass.
fn converge_gsi_active(cluster: &mut SimCluster, table: &str, index: &str) {
    let leader = leader_of_table(cluster, table);
    for _ in 0..10 {
        cluster.drive_backfill_seed(leader, table);
        cluster.drain_gsi(leader, table);
        let (_, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.DescribeTable",
            format!(r#"{{"TableName":"{table}"}}"#).as_bytes(),
        );
        if index_status(&body, index).map(|(s, _)| s) == Some("ACTIVE".to_owned()) {
            return;
        }
    }
    panic!("index `{index}` on `{table}` did not converge to ACTIVE within 10 rounds");
}

// ---------------------------------------------------------------------------
// Scenario (a): UpdateTable adds a GSI to a populated table, backfills, and
// converges to ACTIVE with the expected rows queryable.
// ---------------------------------------------------------------------------

fn run_update_table_add_index_on_populated_table_backfills_to_active(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"docs","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    // Populate the table BEFORE the GSI is declared — the point of this
    // scenario is a live backfill over pre-existing rows, not just live-write
    // coverage of new ones.
    for (id, cat) in [("d1", "a"), ("d2", "b"), ("d3", "a")] {
        let (status, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.PutItem",
            format!(
                r#"{{"TableName":"docs","Item":{{"id":{{"S":"{id}"}},"cat":{{"S":"{cat}"}}}}}}"#
            )
            .as_bytes(),
        );
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
    }

    // Add the GSI via UpdateTable.
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.UpdateTable",
        br#"{"TableName":"docs",
            "AttributeDefinitions":[{"AttributeName":"cat","AttributeType":"S"}],
            "GlobalSecondaryIndexUpdates":[{"Create":{
                "IndexName":"by-cat",
                "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                "Projection":{"ProjectionType":"ALL"}}}]}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTable(add index) failed: {body}"
    );
    let (idx_status, backfilling) =
        index_status(&body, "by-cat").unwrap_or_else(|| panic!("seed={seed}: no by-cat in {body}"));
    assert_eq!(idx_status, "CREATING", "seed={seed}: got {body}");
    assert!(
        backfilling,
        "seed={seed}: expected Backfilling:true in {body}"
    );

    // A Query against a still-backfilling index is rejected.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"docs","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"a"}}}"#,
    );
    assert_eq!(status, 400, "seed={seed}: got {body}");
    assert!(
        body.contains("ValidationException"),
        "seed={seed}: got {body}"
    );

    // Drive the backfill seeder + drain to exhaustion, converging the index
    // to ACTIVE.
    converge_gsi_active(&mut cluster, "docs", "by-cat");

    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.DescribeTable",
        br#"{"TableName":"docs"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    let (idx_status, backfilling) =
        index_status(&body, "by-cat").unwrap_or_else(|| panic!("seed={seed}: no by-cat in {body}"));
    assert_eq!(idx_status, "ACTIVE", "seed={seed}: got {body}");
    assert!(
        !backfilling,
        "seed={seed}: Backfilling must be omitted/false once ACTIVE: {body}"
    );

    // Every pre-existing row is queryable through the now-ACTIVE index.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"docs","IndexName":"by-cat",
            "ConsistentRead":false,
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"a"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: GSI query failed: {body}");
    assert!(body.contains("\"Count\":2"), "seed={seed}: {body}");
    assert!(body.contains(r#""id":{"S":"d1"}"#), "seed={seed}: {body}");
    assert!(body.contains(r#""id":{"S":"d3"}"#), "seed={seed}: {body}");
    assert!(
        !body.contains(r#""id":{"S":"d2"}"#),
        "seed={seed}: got: {body}"
    );
}

#[test]
fn update_table_add_index_on_populated_table_backfills_to_active() {
    run_update_table_add_index_on_populated_table_backfills_to_active(env_seed(0x1DD1_0001));
}

#[test]
fn update_table_add_index_on_populated_table_backfills_to_active_over_seeds() {
    for i in 0..5 {
        run_update_table_add_index_on_populated_table_backfills_to_active(0x1DD1_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (b): UpdateTable deletes a GSI — gone from DescribeTable, and a
// Query against it is rejected.
// ---------------------------------------------------------------------------

fn run_update_table_delete_index_removes_it_and_rejects_query(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"docs","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    for (id, cat) in [("d1", "a"), ("d2", "b")] {
        let (status, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.PutItem",
            format!(
                r#"{{"TableName":"docs","Item":{{"id":{{"S":"{id}"}},"cat":{{"S":"{cat}"}}}}}}"#
            )
            .as_bytes(),
        );
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
    }

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.UpdateTable",
        br#"{"TableName":"docs",
            "AttributeDefinitions":[{"AttributeName":"cat","AttributeType":"S"}],
            "GlobalSecondaryIndexUpdates":[{"Create":{
                "IndexName":"by-cat",
                "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                "Projection":{"ProjectionType":"ALL"}}}]}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTable(add index) failed: {body}"
    );

    converge_gsi_active(&mut cluster, "docs", "by-cat");

    // Sanity: the index is genuinely queryable before it's dropped.
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"docs","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"a"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    assert!(body.contains("\"Count\":1"), "seed={seed}: {body}");

    // Delete the GSI via UpdateTable.
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.UpdateTable",
        br#"{"TableName":"docs",
            "GlobalSecondaryIndexUpdates":[{"Delete":{"IndexName":"by-cat"}}]}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTable(drop index) failed: {body}"
    );
    assert!(
        index_status(&body, "by-cat").is_none(),
        "seed={seed}: by-cat still present right after drop: {body}"
    );

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.DescribeTable",
        br#"{"TableName":"docs"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    assert!(
        index_status(&body, "by-cat").is_none(),
        "seed={seed}: by-cat still present in DescribeTable: {body}"
    );

    // A Query against the now-gone index is rejected.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"docs","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"a"}}}"#,
    );
    assert_eq!(status, 400, "seed={seed}: got {body}");
    assert!(
        body.contains("ValidationException"),
        "seed={seed}: got {body}"
    );
}

#[test]
fn update_table_delete_index_removes_it_and_rejects_query() {
    run_update_table_delete_index_removes_it_and_rejects_query(env_seed(0x1DD1_0002));
}

#[test]
fn update_table_delete_index_removes_it_and_rejects_query_over_seeds() {
    for i in 0..5 {
        run_update_table_delete_index_removes_it_and_rejects_query(0x1DD1_2000 + i);
    }
}
