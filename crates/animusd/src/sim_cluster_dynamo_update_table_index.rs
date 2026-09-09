//! `SimCluster`-driven conversion of index DDL beyond plain `CreateTable`
//! (ADR 0061 rung J, C-10 PR 3) — the real-socket suites this PR deletes:
//! `tests/update_table_create_index.rs` (4), `tests/update_table_drop_
//! index.rs` (4), and `tests/dynamo_gsi_drain.rs` (1), 9 tests total. Every
//! scenario below asserts the identical observable behaviour the original
//! asserted, through the fixture's own `SimCluster::dynamo` DynamoDB entry
//! point — see `crates/animusd/CLAUDE.md`'s matching C-10 residual-inventory
//! entry and `docs/adr/0061-testability-node-crate-simulator.md`'s Rung J
//! PR 3 amendment for the full account.
//!
//! **Shared helpers below duplicate `sim_cluster_index_ddl.rs`'s own
//! `env_seed`/`index_status`/`leader_of_table`/`converge_gsi_active`**
//! rather than importing them — this crate's own "sibling test modules
//! keep their own fixtures independent" convention (see that module's own
//! doc, and `tests/update_table_drop_index.rs`'s original `bring_up` doc
//! for the real-socket precedent this mirrors).
//!
//! **Two genuine fixture-shape substitutions, both documented inline at
//! their own scenario:**
//!
//! 1. `drop_of_an_active_index_on_a_populated_table_reclaims_everything`'s
//!    original asserted physical reclaim by checking a real WAL file's
//!    absence on disk (`tablet_wal_present`) — `SimCluster` hosts every
//!    tablet on an in-memory `MemoryEngine`, so there is no file to check.
//!    The substitute is strictly stronger: `Metadata`/`hosted_tablets`
//!    absence **and** the hidden table's own tablet id reading back an
//!    empty engine, together in one converged-or-timeout poll — the exact
//!    discipline `sim_cluster_dynamo_drop_table.rs::assert_reclaimed`
//!    already established for the base-table drop case, generalized here
//!    to an index's own hidden table.
//! 2. `in_flight_backfill_is_cancelled_by_a_concurrent_drop` and
//!    `a_crash_and_retry_mid_cascade_still_converges` originally raced a
//!    background poll (or an abandoned in-flight request) against a live
//!    op using real OS threads/sockets. `SimCluster::dynamo` always runs a
//!    request to completion in one synchronous call — there is no window
//!    from a test's own code to interleave a second action mid-flight the
//!    way `tokio::join!`/`fire_and_forget_dynamo` did. Both scenarios keep
//!    below use this fixture's own documented technique instead (see
//!    `SimClusterHandle::dynamo`'s doc: "self-bounded... so callable
//!    directly inside an `env.spawn_task`-ed future with no wrapper
//!    needed"): spawn the request by hand on the target node's own `SimEnv`
//!    via `SimCluster::handle`, drive the simulator only a little (or not
//!    at all) via `SimCluster::run_for`, then interrupt with
//!    `SimCluster::restart` (a true process stop, dropping the still
//!    in-flight task) before it can ever finish — the deterministic,
//!    single-seed-reproducible analogue of "abandon an in-flight op",
//!    already this fixture's own established idiom for "crash during X"
//!    (`sim_cluster_dynamo_drop_table.rs::run_scenario_4_a_node_crashed_
//!    during_the_drop_and_restarted_reclaims_its_engine` crashes BEFORE
//!    issuing the racing op rather than mid-flight, for the identical
//!    reason). Both scenarios' own doc comments below spell out exactly
//!    what is and isn't preserved from the original's real concurrency.
//!
//! Backfill convergence is always a converged-or-timeout loop over several
//! `SimCluster::drive_backfill_seed` + `SimCluster::drain_gsi` rounds (each
//! round costs the 12s `OP_BUDGET` when driven through a `dynamo()` call) —
//! never a single call assumed to finish, per the ADR's own binding rule.
//!
//! Seed replay (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <test name>`.

use std::time::Duration;

use animus_control::{IndexDef, IndexKind, IndexProjection, IndexStatus, MetaCommand};
use animus_dynamo::wire::{BATCH_WRITE_MAX_ITEMS, MAX_GSI_PER_TABLE};
use animus_env::EnvExt;
use animus_storage::StorageEngine;
use animus_tablet::TabletId;

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

// ---------------------------------------------------------------------------
// Shared wire helpers (duplicated from `sim_cluster_index_ddl.rs`/the
// deleted real-socket files per this crate's own per-file-fixture
// convention).
// ---------------------------------------------------------------------------

fn create_table_no_index(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    cluster.dynamo(
        node,
        "DynamoDB_20120810.CreateTable",
        format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        )
        .as_bytes(),
    )
}

/// `CreateTable` with a GSI declared up front — created `Active`
/// immediately (ADR 0045 §1: a just-created table is empty by
/// construction).
fn create_table_with_gsi(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    index: &str,
    hash_attr: &str,
) -> (u16, String) {
    cluster.dynamo(
        node,
        "DynamoDB_20120810.CreateTable",
        format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}},
                                         {{"AttributeName":"{hash_attr}","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "GlobalSecondaryIndexes":[
                    {{"IndexName":"{index}",
                     "KeySchema":[{{"AttributeName":"{hash_attr}","KeyType":"HASH"}}],
                     "Projection":{{"ProjectionType":"ALL"}}}}]}}"#
        )
        .as_bytes(),
    )
}

fn put_item(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    id: &str,
    attr: &str,
    value: &str,
) -> (u16, String) {
    cluster.dynamo(
        node,
        "DynamoDB_20120810.PutItem",
        format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"{attr}":{{"S":"{value}"}}}}}}"#
        )
        .as_bytes(),
    )
}

/// Populate `table` with `attr = "{id}@x"` for every id in `ids`, via
/// `BatchWriteItem` in [`BATCH_WRITE_MAX_ITEMS`]-sized chunks (duplicated
/// from `tests/update_table_drop_index.rs`'s own technique).
fn batch_put_items(cluster: &mut SimCluster, node: u64, table: &str, attr: &str, ids: &[String]) {
    for chunk in ids.chunks(BATCH_WRITE_MAX_ITEMS) {
        let puts: Vec<String> = chunk
            .iter()
            .map(|id| {
                format!(
                    r#"{{"PutRequest":{{"Item":{{"id":{{"S":"{id}"}},"{attr}":{{"S":"{id}@x"}}}}}}}}"#
                )
            })
            .collect();
        let body = format!(r#"{{"RequestItems":{{"{table}":[{}]}}}}"#, puts.join(","));
        let (status, resp) =
            cluster.dynamo(node, "DynamoDB_20120810.BatchWriteItem", body.as_bytes());
        assert_eq!(status, 200, "BatchWriteItem failed: {resp}");
    }
}

fn create_index_via_wire(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    index: &str,
    hash_attr: &str,
) -> (u16, String) {
    cluster.dynamo(
        node,
        "DynamoDB_20120810.UpdateTable",
        format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"{hash_attr}","AttributeType":"S"}}],
                "GlobalSecondaryIndexUpdates":[{{"Create":{{
                    "IndexName":"{index}",
                    "KeySchema":[{{"AttributeName":"{hash_attr}","KeyType":"HASH"}}],
                    "Projection":{{"ProjectionType":"ALL"}}}}}}]}}"#
        )
        .as_bytes(),
    )
}

fn delete_index_via_wire(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    index: &str,
) -> (u16, String) {
    cluster.dynamo(
        node,
        "DynamoDB_20120810.UpdateTable",
        format!(
            r#"{{"TableName":"{table}",
                "GlobalSecondaryIndexUpdates":[{{"Delete":{{"IndexName":"{index}"}}}}]}}"#
        )
        .as_bytes(),
    )
}

fn describe_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    cluster.dynamo(
        node,
        "DynamoDB_20120810.DescribeTable",
        format!(r#"{{"TableName":"{table}"}}"#).as_bytes(),
    )
}

fn query_index(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    index: &str,
    hash_attr: &str,
    value: &str,
) -> (u16, String) {
    cluster.dynamo(
        node,
        "DynamoDB_20120810.Query",
        format!(
            r#"{{"TableName":"{table}","IndexName":"{index}",
                "ConsistentRead":false,
                "KeyConditionExpression":"{hash_attr} = :v",
                "ExpressionAttributeValues":{{":v":{{"S":"{value}"}}}}}}"#
        )
        .as_bytes(),
    )
}

/// Pull `(IndexStatus, Backfilling)` for `index` out of a `DescribeTable`/
/// `UpdateTable` response body's `GlobalSecondaryIndexes` array — `None` if
/// the index isn't listed at all.
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

fn has_index(cluster: &SimCluster, node: u64, table: &str, index: &str) -> bool {
    cluster
        .metadata(node)
        .table_indexes(table)
        .iter()
        .any(|i| i.name == index)
}

fn is_index_active(cluster: &SimCluster, node: u64, table: &str, index: &str) -> bool {
    cluster
        .metadata(node)
        .table_indexes(table)
        .iter()
        .any(|i| i.name == index && i.status == IndexStatus::Active)
}

/// Find the tablet leader of `table`'s (sole) tablet — mirrors
/// `sim_cluster_index_ddl.rs::leader_of_table`.
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

fn tablet_of_table(cluster: &SimCluster, node: u64, table: &str) -> TabletId {
    let meta = cluster.metadata(node);
    *meta
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table {table} has no tablet on node {node}'s own view"))
        .0
}

/// How many live rows `table` holds on `node`, via a whole-table scan —
/// counts decoded live items, not raw pairs, so a tombstone is never
/// counted (mirrors the deleted real-socket files' own `row_count`).
fn row_count(cluster: &mut SimCluster, node: u64, table: &str) -> usize {
    cluster
        .scan(node, table, false)
        .map(|rows| {
            rows.iter()
                .filter(|(_, v)| matches!(animus_item::decode_stored_item(v), Ok(Some(_))))
                .count()
        })
        .unwrap_or(0)
}

/// Drive the backfill seeder + GSI drain to exhaustion, then poll
/// `DescribeTable` until `index` converges to `ACTIVE` on node 0 — never a
/// single call assumed to finish (mirrors `sim_cluster_index_ddl.rs::
/// converge_gsi_active` exactly).
fn converge_gsi_active(cluster: &mut SimCluster, table: &str, index: &str) {
    let leader = leader_of_table(cluster, table);
    for _ in 0..10 {
        cluster.drive_backfill_seed(leader, table);
        cluster.drain_gsi(leader, table);
        let (_, body) = describe_table(cluster, 0, table);
        if index_status(&body, index).map(|(s, _)| s) == Some("ACTIVE".to_owned()) {
            return;
        }
    }
    panic!("index `{index}` on `{table}` did not converge to ACTIVE within 10 rounds");
}

/// [`converge_gsi_active`]'s multi-node form: converges on **every** node's
/// own `Metadata` view, needed by the non-leader-relay scenario below,
/// where the caller cares that a control-plane-non-leader-issued
/// `UpdateTable` really did commit and propagate everywhere, not merely on
/// node 0.
fn converge_gsi_active_all_nodes(cluster: &mut SimCluster, table: &str, index: &str) {
    let leader = leader_of_table(cluster, table);
    for _ in 0..10 {
        cluster.drive_backfill_seed(leader, table);
        cluster.drain_gsi(leader, table);
        // Give the always-on `index_backfill_loop` completion aggregator a
        // further tick to observe the freshly-reported tablet on every
        // node, not just the one `dynamo()` happened to route through.
        cluster.run_for(Duration::from_millis(200));
        let mut all_active = true;
        for n in 0..cluster.node_count() as u64 {
            if !is_index_active(cluster, n, table, index) {
                all_active = false;
                break;
            }
        }
        if all_active {
            return;
        }
    }
    panic!(
        "index `{index}` on `{table}` did not converge to ACTIVE on every node within 10 rounds"
    );
}

/// A `Creating` GSI definition hashing on `hash_attribute` (duplicated from
/// `tests/update_table_drop_index.rs`).
fn creating_index(name: &str, hash_attribute: &str) -> IndexDef {
    IndexDef {
        name: name.to_owned(),
        kind: IndexKind::Global,
        hash_attribute: hash_attribute.to_owned(),
        sort_attribute: None,
        projection: IndexProjection::All,
        status: IndexStatus::Creating,
        hash_attribute_type: None,
        sort_attribute_type: None,
    }
}

/// Poll `run_for(STEP)` — never another op call, so `OP_BUDGET`'s own
/// per-call burn never distorts the wait (mirrors `sim_cluster_stream_
/// janitor.rs::poll_run_for` exactly).
fn poll_run_for(
    cluster: &mut SimCluster,
    budget: Duration,
    msg: &str,
    mut done: impl FnMut(&mut SimCluster) -> bool,
) {
    const STEP: Duration = Duration::from_millis(50);
    let seed = cluster.seed();
    let mut elapsed = Duration::ZERO;
    loop {
        if done(cluster) {
            return;
        }
        assert!(
            elapsed < budget,
            "{msg} did not converge within {budget:?} (seed={seed})"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

/// The three converged-or-timeout observables an index's own hidden
/// table's drop must reach, checked TOGETHER in the SAME poll (mirrors
/// `sim_cluster_dynamo_drop_table.rs::assert_reclaimed`'s own discipline
/// and its doc for why splitting metadata/hosted-set and engine-emptiness
/// into two passes is a real bug class, not merely a style nit): no node's
/// `Metadata` still lists `index` on `table`, no node's `Metadata` still
/// carries a tablet for `index_table`, no node's `hosted_tablets` still
/// names `tablet`, and every node's own private engine for `tablet` reads
/// back empty.
async fn assert_index_reclaimed(
    cluster: &mut SimCluster,
    table: &str,
    index: &str,
    index_table: &str,
    tablet: TabletId,
    budget: Duration,
) {
    let seed = cluster.seed();
    const STEP: Duration = Duration::from_millis(50);
    let mut elapsed = Duration::ZERO;
    loop {
        let mut converged = true;
        for n in 0..cluster.node_count() as u64 {
            if has_index(cluster, n, table, index)
                || cluster.metadata(n).has_table_tablet(index_table)
                || cluster.hosted_tablets(n).contains(&tablet)
            {
                converged = false;
                break;
            }
            let entries = cluster
                .storage(n, tablet)
                .entries()
                .await
                .expect("a MemoryEngine read never fails");
            if !entries.is_empty() {
                converged = false;
                break;
            }
        }
        if converged {
            return;
        }
        assert!(
            elapsed < budget,
            "index `{index}` on `{table}` (hidden table `{index_table}`, tablet {tablet:?}) was \
             not reclaimed within {budget:?} (seed={seed})"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

// ---------------------------------------------------------------------------
// From `tests/update_table_create_index.rs`
// ---------------------------------------------------------------------------

/// The headline scenario (ADR 0045 §2/§6): a GSI created on a table that
/// already has rows immediately reports `CREATING`/`Backfilling: true` and
/// rejects a `Query`, a row written while backfill is still running is
/// covered by the live-write path (not merely the seeder), the index
/// converges to `ACTIVE` reporting exactly the expected rows, and dropping
/// it afterward removes it from `DescribeTable` — proving the two
/// `UpdateTable` halves compose end to end.
fn run_create_index_on_populated_table_backfills_live_and_pre_existing_rows(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "orders";
    let index = "by-cat";

    let (status, body) = create_table_no_index(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    for (id, cat) in [("p1", "a"), ("p2", "a"), ("p3", "b")] {
        let (status, body) = put_item(&mut cluster, 0, table, id, "cat", cat);
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
    }

    let (status, body) = create_index_via_wire(&mut cluster, 0, table, index, "cat");
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTable Create failed: {body}"
    );
    let (idx_status, backfilling) = index_status(&body, index)
        .unwrap_or_else(|| panic!("seed={seed}: index missing from UpdateTable response: {body}"));
    assert_eq!(
        idx_status, "CREATING",
        "seed={seed}: expected CREATING right after Create: {body}"
    );
    assert!(
        backfilling,
        "seed={seed}: expected Backfilling:true right after Create: {body}"
    );

    let (qs, qb) = query_index(&mut cluster, 0, table, index, "cat", "a");
    assert_eq!(
        qs, 400,
        "seed={seed}: Query against a CREATING index should fail: {qb}"
    );
    assert!(
        qb.contains("ValidationException"),
        "seed={seed}: expected ValidationException, got: {qb}"
    );

    // A write racing the backfill — must be covered by the live-write path
    // (index presence, not status, gates it), not merely by the seeder's
    // own forward sweep.
    let (status, body) = put_item(&mut cluster, 0, table, "p4", "cat", "a");
    assert_eq!(status, 200, "seed={seed}: PutItem(p4) failed: {body}");

    converge_gsi_active(&mut cluster, table, index);

    let (status, body) = describe_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: {body}");
    let (idx_status, backfilling) =
        index_status(&body, index).unwrap_or_else(|| panic!("seed={seed}: index still listed"));
    assert_eq!(idx_status, "ACTIVE", "seed={seed}: got {body}");
    assert!(
        !backfilling,
        "seed={seed}: Backfilling must not be reported once ACTIVE: {body}"
    );

    let (qs, qb) = query_index(&mut cluster, 0, table, index, "cat", "a");
    assert_eq!(qs, 200, "seed={seed}: GSI query failed: {qb}");
    assert!(qb.contains("\"Count\":3"), "seed={seed}: {qb}");
    for id in ["p1", "p2", "p4"] {
        assert!(
            qb.contains(&format!(r#""id":{{"S":"{id}"}}"#)),
            "seed={seed}: {qb}"
        );
    }
    assert!(
        !qb.contains("\"S\":\"p3\""),
        "seed={seed}: cat=b row leaked into a cat=a query: {qb}"
    );

    // Drop it — proves the two UpdateTable halves compose.
    let (status, body) = delete_index_via_wire(&mut cluster, 0, table, index);
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTable Delete failed: {body}"
    );
    assert!(
        index_status(&body, index).is_none(),
        "seed={seed}: by-cat still present right after drop: {body}"
    );
}

#[test]
fn create_index_on_populated_table_backfills_live_and_pre_existing_rows() {
    run_create_index_on_populated_table_backfills_live_and_pre_existing_rows(env_seed(0x0C11_0001));
}

#[test]
fn create_index_on_populated_table_backfills_live_and_pre_existing_rows_over_seeds() {
    for i in 0..5 {
        run_create_index_on_populated_table_backfills_live_and_pre_existing_rows(0x0C11_1000 + i);
    }
}

/// Client-side validation, all rejected before ever proposing anything: a
/// duplicate index name, a reserved-namespace name, a name containing the
/// hidden index table's own `$` separator, and `Create` against a table
/// that was never created at all.
fn run_update_table_create_validation_rejects_bad_index_declarations(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    // Duplicate name: the table already has this GSI from CreateTable.
    let table = "dup_name";
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}},
                                         {{"AttributeName":"x","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "GlobalSecondaryIndexes":[
                    {{"IndexName":"by-x","KeySchema":[{{"AttributeName":"x","KeyType":"HASH"}}],
                     "Projection":{{"ProjectionType":"ALL"}}}}]}}"#
        )
        .as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = create_index_via_wire(&mut cluster, 0, table, "by-x", "y");
    assert_ne!(
        status, 200,
        "seed={seed}: duplicate index name should be rejected: {body}"
    );
    assert!(
        body.contains("ValidationException"),
        "seed={seed}: expected ValidationException, got: {body}"
    );

    // Reserved-namespace name.
    let table2 = "reserved_name";
    let (status, body) = create_table_no_index(&mut cluster, 0, table2);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) =
        create_index_via_wire(&mut cluster, 0, table2, "__animus_system_by_x", "x");
    assert_ne!(
        status, 200,
        "seed={seed}: reserved index name should be rejected: {body}"
    );
    assert!(
        body.contains("ValidationException"),
        "seed={seed}: expected ValidationException, got: {body}"
    );

    // `$`-containing name (the hidden index table's own separator).
    let (status, body) = create_index_via_wire(&mut cluster, 0, table2, "by$x", "x");
    assert_ne!(
        status, 200,
        "seed={seed}: `$`-containing index name should be rejected: {body}"
    );
    assert!(
        body.contains("ValidationException"),
        "seed={seed}: expected ValidationException, got: {body}"
    );

    // Create on a table that was never created at all.
    let (status, body) = create_index_via_wire(&mut cluster, 0, "no_such_table", "by-x", "x");
    assert_ne!(
        status, 200,
        "seed={seed}: Create on a nonexistent table should be rejected: {body}"
    );
    assert!(
        body.contains("ResourceNotFoundException"),
        "seed={seed}: expected ResourceNotFoundException, got: {body}"
    );
}

#[test]
fn update_table_create_validation_rejects_bad_index_declarations() {
    run_update_table_create_validation_rejects_bad_index_declarations(env_seed(0x0C11_0002));
}

#[test]
fn update_table_create_validation_rejects_bad_index_declarations_over_seeds() {
    for i in 0..5 {
        run_update_table_create_validation_rejects_bad_index_declarations(0x0C11_2000 + i);
    }
}

/// `create_index` enforces AWS's [`MAX_GSI_PER_TABLE`] (20) cap against the
/// table's *current* replicated GSI count.
fn run_update_table_create_rejects_past_the_gsi_cap(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "at_gsi_cap";
    let gsis: Vec<String> = (0..MAX_GSI_PER_TABLE)
        .map(|i| {
            format!(
                r#"{{"IndexName":"gsi{i}","KeySchema":[{{"AttributeName":"a{i}","KeyType":"HASH"}}],
                    "Projection":{{"ProjectionType":"ALL"}}}}"#
            )
        })
        .collect();
    let gsi_attribute_defs: Vec<String> = (0..MAX_GSI_PER_TABLE)
        .map(|i| format!(r#"{{"AttributeName":"a{i}","AttributeType":"S"}}"#))
        .collect();
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}},{}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "GlobalSecondaryIndexes":[{}]}}"#,
            gsi_attribute_defs.join(","),
            gsis.join(",")
        )
        .as_bytes(),
    );
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable with exactly {MAX_GSI_PER_TABLE} GSIs failed: {body}"
    );

    let (status, body) = create_index_via_wire(&mut cluster, 0, table, "one_too_many", "over");
    assert_ne!(
        status, 200,
        "seed={seed}: a 21st GSI should be rejected: {body}"
    );
    assert!(
        body.contains("ValidationException"),
        "seed={seed}: expected ValidationException, got: {body}"
    );
}

#[test]
fn update_table_create_rejects_past_the_gsi_cap() {
    run_update_table_create_rejects_past_the_gsi_cap(env_seed(0x0C11_0003));
}

#[test]
fn update_table_create_rejects_past_the_gsi_cap_over_seeds() {
    for i in 0..5 {
        run_update_table_create_rejects_past_the_gsi_cap(0x0C11_3000 + i);
    }
}

/// `CreateTableIndex` is on `is_relayable_command`'s allowlist: issued
/// against a node that is **not** the control-plane leader, on a 3-node
/// cluster, `UpdateTable`'s `Create` path must still commit, backfill, and
/// converge to `ACTIVE` on every node — not just the one the request
/// landed on.
fn run_update_table_create_via_a_non_leader_node_converges_on_every_node(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "relay_create";
    let index = "by-cat";

    let leader = cluster.control_leader_index() as u64;
    let follower = (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a 3-node cluster has a non-leader");

    let (status, body) = create_table_no_index(&mut cluster, follower, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    for (id, cat) in [("r1", "a"), ("r2", "a")] {
        let (status, body) = put_item(&mut cluster, follower, table, id, "cat", cat);
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
    }

    let (status, body) = create_index_via_wire(&mut cluster, follower, table, index, "cat");
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTable Create via a follower failed: {body}"
    );

    converge_gsi_active_all_nodes(&mut cluster, table, index);

    let (status, body) = query_index(&mut cluster, follower, table, index, "cat", "a");
    assert_eq!(status, 200, "seed={seed}: relayed GSI query failed: {body}");
    assert!(body.contains("\"Count\":2"), "seed={seed}: {body}");
}

#[test]
fn update_table_create_via_a_non_leader_node_converges_on_every_node() {
    run_update_table_create_via_a_non_leader_node_converges_on_every_node(env_seed(0x0C11_0004));
}

#[test]
fn update_table_create_via_a_non_leader_node_converges_on_every_node_over_seeds() {
    for i in 0..5 {
        run_update_table_create_via_a_non_leader_node_converges_on_every_node(0x0C11_4000 + i);
    }
}

// ---------------------------------------------------------------------------
// From `tests/update_table_drop_index.rs`
// ---------------------------------------------------------------------------

/// Dropping a fully `Active` GSI on a populated table via the real
/// `UpdateTable` wire path: the hidden table's tablet leaves the tablet
/// map, its engine reads back empty on every node (this fixture's own
/// physical-reclaim proof — see this module's own top-of-file doc for why
/// this replaces the original's on-disk WAL-file check), the catalog entry
/// disappears, the base table is completely unaffected, and a subsequent
/// `Query` naming the now-gone index errors cleanly.
fn run_drop_of_an_active_index_on_a_populated_table_reclaims_everything(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "drop_active";
    let index = "by-email";
    let index_table = "drop_active$by-email";

    let (status, body) = create_table_with_gsi(&mut cluster, 0, table, index, "email");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    for (id, email) in [("u1", "a@x"), ("u2", "b@x"), ("u3", "c@x")] {
        let (status, body) = put_item(&mut cluster, 0, table, id, "email", email);
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
    }

    let leader = leader_of_table(&cluster, table);
    cluster.drain_gsi(leader, table);
    assert_eq!(
        row_count(&mut cluster, 0, index_table),
        3,
        "seed={seed}: GSI converges before drop"
    );
    let index_tablet = tablet_of_table(&cluster, 0, index_table);

    let (status, body) = delete_index_via_wire(&mut cluster, 0, table, index);
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTable Delete failed: {body}"
    );

    futures::executor::block_on(assert_index_reclaimed(
        &mut cluster,
        table,
        index,
        index_table,
        index_tablet,
        Duration::from_secs(10),
    ));

    // The base table itself is completely unaffected.
    assert_eq!(
        row_count(&mut cluster, 0, table),
        3,
        "seed={seed}: base table unaffected by index drop"
    );
    assert!(cluster.metadata(0).has_table_tablet(table));

    // A Query against the now-gone index errors cleanly.
    let (status, body) = query_index(&mut cluster, 0, table, index, "email", "a@x");
    assert_eq!(
        status, 400,
        "seed={seed}: Query against a dropped index should fail: {body}"
    );
    assert!(
        body.contains("ValidationException"),
        "seed={seed}: expected ValidationException, got: {body}"
    );
}

#[test]
fn drop_of_an_active_index_on_a_populated_table_reclaims_everything() {
    run_drop_of_an_active_index_on_a_populated_table_reclaims_everything(env_seed(0x0D20_0001));
}

#[test]
fn drop_of_an_active_index_on_a_populated_table_reclaims_everything_over_seeds() {
    for i in 0..5 {
        run_drop_of_an_active_index_on_a_populated_table_reclaims_everything(0x0D20_1000 + i);
    }
}

/// The in-flight-cancellation regression: start a backfill on a populated
/// table (300 rows — well past the seeder's own per-tick partition-
/// discovery cap, so the very first `drive_backfill_seed` tick provably
/// cannot finish sweeping it), then drop the index before driving any
/// further ticks — the index must converge to fully removed, never
/// `Active`, with no orphan hidden tablets and no `index_backfill` rows.
///
/// **Deviates from the original's own true concurrency** (see this
/// module's own top-of-file doc, substitution 2): the real-socket version
/// raced a background poll against a live `UpdateTable Delete` over real
/// threads/sockets. `SimCluster::dynamo` always runs a request to
/// completion in one synchronous call, so there is no window to interleave
/// a second action mid-flight the way `tokio::join!` did. This scenario's
/// own deterministic analogue — issue the drop immediately after exactly
/// one partial `drive_backfill_seed` tick, before the index could possibly
/// have reached `Active` — still proves the cancellation-not-a-race
/// property (an in-progress, not-yet-finished backfill is genuinely
/// cancelled, not raced to completion) without literally reproducing the
/// original's own thread interleaving.
fn run_in_flight_backfill_is_cancelled_by_a_concurrent_drop(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "cancel_bf";
    let index = "by-cancel";
    let index_table = "cancel_bf$by-cancel";

    let (status, body) = create_table_no_index(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let ids: Vec<String> = (0..300).map(|i| format!("c{i:04}")).collect();
    batch_put_items(&mut cluster, 0, table, "g", &ids);

    let _ = cluster.propose_meta(MetaCommand::CreateTableIndex {
        table: table.into(),
        index: creating_index(index, "g"),
    });
    poll_run_for(
        &mut cluster,
        Duration::from_secs(10),
        "index visible before racing the drop",
        |c| has_index(c, 0, table, index),
    );

    // ONE partial backfill-seed tick — 300 rows is well past the seeder's
    // own per-tick cap, so this alone cannot finish sweeping the table:
    // the index is genuinely still `Creating` afterward, the identical
    // margin the original real-socket test relied on.
    let leader = leader_of_table(&cluster, table);
    cluster.drive_backfill_seed(leader, table);
    assert!(
        !is_index_active(&cluster, 0, table, index),
        "seed={seed}: backfill must not have finished in a single partial tick"
    );

    let (status, body) = delete_index_via_wire(&mut cluster, 0, table, index);
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTable Delete failed: {body}"
    );

    for n in 0..cluster.node_count() as u64 {
        poll_run_for(
            &mut cluster,
            Duration::from_secs(30),
            "index definition removed from catalog",
            |c| !has_index(c, n, table, index),
        );
        poll_run_for(
            &mut cluster,
            Duration::from_secs(30),
            "hidden index table's tablet dropped",
            |c| !c.metadata(n).has_table_tablet(index_table),
        );
        poll_run_for(
            &mut cluster,
            Duration::from_secs(30),
            "no index_backfill rows remain for the cancelled index",
            |c| {
                !c.metadata(n)
                    .index_backfill
                    .keys()
                    .any(|(_, name)| name == index)
            },
        );
    }

    // The base table's own data is untouched by the cancelled backfill.
    assert_eq!(
        row_count(&mut cluster, 0, table),
        ids.len(),
        "seed={seed}: base table survives cancellation"
    );
}

#[test]
fn in_flight_backfill_is_cancelled_by_a_concurrent_drop() {
    run_in_flight_backfill_is_cancelled_by_a_concurrent_drop(env_seed(0x0D20_0002));
}

#[test]
fn in_flight_backfill_is_cancelled_by_a_concurrent_drop_over_seeds() {
    for i in 0..5 {
        run_in_flight_backfill_is_cancelled_by_a_concurrent_drop(0x0D20_2000 + i);
    }
}

/// The sharp edge of accepting the backfill cursor as bounded garbage
/// instead of actively clearing it: drop a fully backfilled index, then
/// recreate an index of the **exact same name**. The correct,
/// cursor-cleaned behavior converges to `Active` with every pre-existing
/// row present, identical to the first index's own backfill — not silently
/// resumed from the deleted index's own stale cursor (which would flip
/// `Active` having seeded nothing).
fn run_create_drop_recreate_same_index_name_backfills_from_scratch(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "recreate_bf";
    let index = "by-recreate";
    let index_table = "recreate_bf$by-recreate";

    let (status, body) = create_table_no_index(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let ids: Vec<String> = (0..15).map(|i| format!("r{i}")).collect();
    batch_put_items(&mut cluster, 0, table, "g", &ids);

    // First backfill: create (the direct `MetaCommand` bypass, mirroring
    // the original — `UpdateTable`'s own `Create` path is exercised
    // separately above), converge to Active, confirm full materialization.
    let _ = cluster.propose_meta(MetaCommand::CreateTableIndex {
        table: table.into(),
        index: creating_index(index, "g"),
    });
    converge_gsi_active(&mut cluster, table, index);
    assert_eq!(
        row_count(&mut cluster, 0, index_table),
        ids.len(),
        "seed={seed}: first backfill converges"
    );

    // Drop it via the real wire path.
    let (status, body) = delete_index_via_wire(&mut cluster, 0, table, index);
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTable Delete failed: {body}"
    );
    for n in 0..cluster.node_count() as u64 {
        poll_run_for(
            &mut cluster,
            Duration::from_secs(30),
            "first index fully dropped",
            |c| !has_index(c, n, table, index),
        );
        poll_run_for(
            &mut cluster,
            Duration::from_secs(30),
            "hidden table dropped",
            |c| !c.metadata(n).has_table_tablet(index_table),
        );
    }

    // Recreate the SAME name. If the cursor were stale-poisoned, this
    // would flip Active having seeded zero rows.
    let _ = cluster.propose_meta(MetaCommand::CreateTableIndex {
        table: table.into(),
        index: creating_index(index, "g"),
    });
    converge_gsi_active(&mut cluster, table, index);
    assert_eq!(
        row_count(&mut cluster, 0, index_table),
        ids.len(),
        "seed={seed}: recreated index backfills every pre-existing row from scratch (not 0)"
    );

    // Every id is genuinely queryable through the recreated GSI, not just
    // present by raw row count.
    for id in &ids {
        let (status, body) = query_index(&mut cluster, 0, table, index, "g", &format!("{id}@x"));
        assert_eq!(status, 200, "seed={seed}: {body}");
        assert!(
            body.contains("\"Count\":1") && body.contains(&format!(r#""id":{{"S":"{id}"}}"#)),
            "seed={seed}: {body}"
        );
    }
}

#[test]
fn create_drop_recreate_same_index_name_backfills_from_scratch() {
    run_create_drop_recreate_same_index_name_backfills_from_scratch(env_seed(0x0D20_0003));
}

#[test]
fn create_drop_recreate_same_index_name_backfills_from_scratch_over_seeds() {
    for i in 0..5 {
        run_create_drop_recreate_same_index_name_backfills_from_scratch(0x0D20_3000 + i);
    }
}

/// Crash-resume: fire the `UpdateTable` `Delete` and abandon it, interrupt
/// the issuing node with a true process restart before it can finish, then
/// **retry the identical `Delete` call** through the recovered node.
/// Either outcome is correct and both are asserted for: the retry itself
/// succeeds (the cascade had not fully finished before the abort), or it
/// reports the index already gone (`ValidationException` — the cascade
/// had, in fact, already fully committed before the restart). Either way,
/// the converged end state — no catalog entry, no hidden tablets, no
/// `index_backfill` rows — must hold.
///
/// **Deviates from the original's own mechanism** (see this module's own
/// top-of-file doc, substitution 2): the real-socket version used a real
/// process (`fire_and_forget_dynamo` + a 15ms sleep + `shutdown_graceful`)
/// on a single-node cluster. `SimCluster::dynamo` always runs to
/// completion in one call, so this scenario spawns the request by hand
/// on the target node's own `SimEnv` (`SimCluster::handle`), drives the
/// simulator only a few milliseconds (giving the cascade a chance to
/// partly commit, mirroring the original's own brief sleep), then
/// interrupts with `SimCluster::restart` — a true process stop that drops
/// the still in-flight task, exactly like the original's abandoned
/// request. **Runs on a 3-node cluster, not 1** — `SimCluster::restart`
/// rebuilds the restarted node's own control-plane log from scratch and
/// relies on ordinary peer catch-up to repopulate it (see that method's
/// own doc); a 1-node cluster has no peer to catch up from, so it would
/// lose all replicated `Metadata` (including the table itself) on
/// restart, unlike the original's real single-process node, which
/// recovered from its own on-disk WAL.
fn run_a_crash_and_retry_mid_cascade_still_converges(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "crash_drop";
    let index = "by-crash";
    let index_table = "crash_drop$by-crash";

    let (status, body) = create_table_with_gsi(&mut cluster, 0, table, index, "email");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let ids: Vec<String> = (0..10).map(|i| format!("k{i}")).collect();
    for id in &ids {
        let (status, body) = put_item(&mut cluster, 0, table, id, "email", &format!("{id}@x"));
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
    }
    let leader = leader_of_table(&cluster, table);
    cluster.drain_gsi(leader, table);
    assert_eq!(
        row_count(&mut cluster, 0, index_table),
        ids.len(),
        "seed={seed}: GSI converges before drop"
    );

    // Fire the Delete on node 0 and abandon it: spawn the request by hand
    // on node 0's own `SimEnv`, let it make a little partial progress, then
    // restart node 0 (a true process stop, dropping the still-pending
    // task) before it can ever complete.
    let handle = cluster.handle();
    let env = handle.env(0);
    let h = handle.clone();
    let del_body = format!(
        r#"{{"TableName":"{table}","GlobalSecondaryIndexUpdates":[{{"Delete":{{"IndexName":"{index}"}}}}]}}"#
    );
    env.spawn_task(async move {
        let _ = h
            .dynamo(0, "DynamoDB_20120810.UpdateTable", del_body.as_bytes())
            .await;
    });
    cluster.run_for(Duration::from_millis(5));
    cluster.restart(0);

    // Node 0 recovers its own view of `Metadata` via ordinary peer
    // catch-up before the retry is issued through it.
    poll_run_for(
        &mut cluster,
        Duration::from_secs(10),
        "node 0 recovers table metadata after restart",
        |c| c.metadata(0).has_table_tablet(table),
    );

    let (status, body) = delete_index_via_wire(&mut cluster, 0, table, index);
    assert!(
        status == 200 || body.contains("ValidationException"),
        "seed={seed}: retry of the delete after crash got an unexpected reply: {status} {body}"
    );

    for n in 0..cluster.node_count() as u64 {
        poll_run_for(
            &mut cluster,
            Duration::from_secs(30),
            "index definition removed from catalog",
            |c| !has_index(c, n, table, index),
        );
        poll_run_for(
            &mut cluster,
            Duration::from_secs(30),
            "hidden index table's tablet dropped",
            |c| !c.metadata(n).has_table_tablet(index_table),
        );
        poll_run_for(
            &mut cluster,
            Duration::from_secs(30),
            "no index_backfill rows remain",
            |c| {
                !c.metadata(n)
                    .index_backfill
                    .keys()
                    .any(|(_, name)| name == index)
            },
        );
    }

    assert_eq!(
        row_count(&mut cluster, 0, table),
        ids.len(),
        "seed={seed}: base table survives the crash"
    );
}

#[test]
fn a_crash_and_retry_mid_cascade_still_converges() {
    run_a_crash_and_retry_mid_cascade_still_converges(env_seed(0x0D20_0004));
}

#[test]
fn a_crash_and_retry_mid_cascade_still_converges_over_seeds() {
    for i in 0..5 {
        run_a_crash_and_retry_mid_cascade_still_converges(0x0D20_4000 + i);
    }
}

// ---------------------------------------------------------------------------
// From `tests/dynamo_gsi_drain.rs`
// ---------------------------------------------------------------------------

/// The GSI drain end to end (ADR 0041 §4): an indexed write leaves a
/// change-log record, and [`SimCluster::drain_gsi`] (this fixture's own
/// hand-driven stand-in for the production `index_drain::change_consumer_
/// loop`'s GSI-drain arm, which this fixture never spawns) materializes the
/// table's GSI rows into the index's own hidden table — including moving a
/// row on an indexed-attribute overwrite and pruning it on delete.
fn run_the_drain_materializes_and_prunes_a_gsis_rows(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "users";
    let index_table = "users$by-email";

    let (status, body) = create_table_with_gsi(&mut cluster, 0, table, "by-email", "email");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    for (id, email) in [("u1", "a@x"), ("u2", "b@x"), ("u3", "a@x")] {
        let (status, body) = put_item(&mut cluster, 0, table, id, "email", email);
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
    }
    let leader = leader_of_table(&cluster, table);
    cluster.drain_gsi(leader, table);
    assert_eq!(
        row_count(&mut cluster, 0, index_table),
        3,
        "seed={seed}: after three puts"
    );

    // Overwriting an item's indexed attribute must MOVE its row, not add
    // one.
    let (status, body) = put_item(&mut cluster, 0, table, "u3", "email", "c@x");
    assert_eq!(status, 200, "seed={seed}: overwrite failed: {body}");
    cluster.drain_gsi(leader, table);
    assert_eq!(
        row_count(&mut cluster, 0, index_table),
        3,
        "seed={seed}: after re-indexing u3"
    );

    // Deleting an item removes its index row.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.DeleteItem",
        format!(r#"{{"TableName":"{table}","Key":{{"id":{{"S":"u3"}}}}}}"#).as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: DeleteItem failed: {body}");
    cluster.drain_gsi(leader, table);
    assert_eq!(
        row_count(&mut cluster, 0, index_table),
        2,
        "seed={seed}: after deleting u3"
    );

    assert_eq!(
        row_count(&mut cluster, 0, table),
        2,
        "seed={seed}: base table after the delete"
    );

    // The acceptance the whole mechanism exists for: a real DynamoDB
    // `Query` against the GSI returns the drain's materialized rows.
    let (status, body) = query_index(&mut cluster, 0, table, "by-email", "email", "a@x");
    assert_eq!(status, 200, "seed={seed}: {body}");
    assert!(
        body.contains("\"Count\":1") && body.contains(r#""id":{"S":"u1"}"#),
        "seed={seed}: {body}"
    );

    // c@x was u3's overwritten email, and u3 was then deleted — the GSI
    // must show it gone, not merely absent-because-never-written.
    let (status, body) = query_index(&mut cluster, 0, table, "by-email", "email", "c@x");
    assert_eq!(status, 200, "seed={seed}: {body}");
    assert!(body.contains("\"Count\":0"), "seed={seed}: {body}");
}

#[test]
fn the_drain_materializes_and_prunes_a_gsis_rows() {
    run_the_drain_materializes_and_prunes_a_gsis_rows(env_seed(0x0D8A_0001));
}

#[test]
fn the_drain_materializes_and_prunes_a_gsis_rows_over_seeds() {
    for i in 0..5 {
        run_the_drain_materializes_and_prunes_a_gsis_rows(0x0D8A_1000 + i);
    }
}
