//! `SimCluster`-driven deterministic coverage for [`index_drain::
//! pitr_seal_now`] (issue #993) — the PITR seal arm's structural twin of
//! [`index_drain::seal_now`] (proven under `SimEnv` since ADR 0061 rung G,
//! C-07 PR 2, `sim_cluster_dynamo_streams.rs`).
//!
//! **The defect this module regresses against**: `pitr_seal_now`'s own
//! commit-wait loop had a fully generic `<E: Env, R: RelayClient>`
//! signature (since ADR 0061 rung C5 step 3a) but its BODY still called
//! bare `tokio::time::Instant::now()`/`tokio::time::sleep` directly,
//! despite its structural twin `seal_now` already having been converted to
//! the `Env` seam — a generic *signature* does not imply a seam-clean
//! *body*, this crate's own recurring lesson (see `crates/animusd/
//! CLAUDE.md`'s "ADR 0061 rung G (C-07 PR 2)" `seal_now` entry, and every
//! later recurrence it names). Under `SimEnv` there is no real Tokio
//! reactor, so `tokio::time::sleep` panics ("there is no reactor running")
//! the instant a `SimEnv`-driven caller's first poll iteration doesn't
//! resolve immediately — which [`SimCluster::drive_pitr_seal`] (this
//! module's own driver, mirroring `drive_stream_seal`'s shape exactly) hits
//! on its very first call, since the commit-wait loop always takes at least
//! one non-resolving iteration before the propose it issues commits.
//! `pitr_seal_now`'s loop is now converted to `ctx.env.now()`/
//! `ctx.env.sleep(..)`, byte-identical under `ProdEnv` to before the fix.
//!
//! **How PITR is enabled here**: the wire's own `UpdateContinuousBackups`
//! (`dynamo::update_continuous_backups`) has no `dispatch_item_op`/
//! `dispatch_table_op` arm yet (`Operation::UpdateContinuousBackups` is
//! still `ProdEnv`-only — a documented residual named by this crate's own
//! C-08 PR 4 and C-10 close-out appendices), so this module uses
//! [`SimCluster::enable_pitr`] instead — a sim-native bypass proposing the
//! identical `MetaCommand::UpdateContinuousBackups{enabled: true}` directly
//! on the control leader, the same "DDL is a control-plane-Raft bypass"
//! shape [`SimCluster::set_table_throughput`]/[`SimCluster::
//! create_table_with_replication`] already use for their own commands (see
//! this crate's `CLAUDE.md` SimCluster "Design decisions" section).
//!
//! # Scenario
//!
//! [`run_create_enable_write_seal`] — create a table over the wire, enable
//! PITR via [`SimCluster::enable_pitr`], write a few items from a
//! **non-leader** node (verified with `ConsistentRead: true`, ADR 0055),
//! call [`SimCluster::drive_pitr_seal`] on the tablet's leader, and assert
//! via `Metadata::pitr_segments` (through [`SimCluster::metadata`]) that a
//! sealed PITR segment row exists for the tablet, covers every written
//! record, and that its segment object is durably present in the shared
//! backup store ([`SimCluster::backup_store`]'s `stored_ids()`).
//!
//! Replays at a pinned seed plus a `_over_seeds` sibling at five seeds, per
//! every sibling module's own convention: `ANIMUS_SEED=<seed> cargo test -p
//! animusd --lib create_enable_write_seal`.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// One DynamoDB wire `CreateTable` for a plain single-key (`pk`, string)
/// table named `table`, issued from `node` — mirrors every other
/// `sim_cluster_*` module's identically-named helper.
fn create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

fn put_item(cluster: &mut SimCluster, node: u64, table: &str, pk: &str, v: &str) -> (u16, String) {
    let body =
        format!(r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"{pk}"}},"v":{{"S":"{v}"}}}}}}"#);
    cluster.dynamo(node, "DynamoDB_20120810.PutItem", body.as_bytes())
}

fn get_item(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    pk: &str,
    consistent: bool,
) -> (u16, String) {
    let body = format!(
        r#"{{"ConsistentRead":{consistent},"TableName":"{table}","Key":{{"pk":{{"S":"{pk}"}}}}}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.GetItem", body.as_bytes())
}

/// `table`'s own tablet id, resolved from the replicated catalog
/// (`Metadata::tablets_for_table`) — this table is always created over the
/// real wire, never via `SimCluster::create_table`, so `SimCluster::
/// tablet_of`'s own hand-hosted-only bookkeeping never covers it (the same
/// lookup every other `sim_cluster_dynamo_*` sibling uses for a
/// wire-created table).
fn tablet_of_table(cluster: &SimCluster, table: &str) -> animus_tablet::TabletId {
    *cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table `{table}` has no tablet"))
        .0
}

/// The node id currently leading `table`'s own tablet.
fn leader_of_table(cluster: &SimCluster, table: &str) -> u64 {
    let tablet = tablet_of_table(cluster, table);
    (0..cluster.node_count() as u64)
        .find(|&n| cluster.is_leader_local(n, tablet))
        .unwrap_or_else(|| panic!("tablet {} has no leader", tablet.0))
}

/// A node id that does **not** lead `table`'s own tablet — mirrors every
/// sibling module's own `non_leader_of_table`/`non_leader` helper.
fn non_leader_of_table(cluster: &SimCluster, table: &str) -> u64 {
    let leader = leader_of_table(cluster, table);
    (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a 3-node cluster always has a non-leader node")
}

fn run_create_enable_write_seal(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "orders";

    let (status, body) = create_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    // Enable PITR via the sim-native bypass (the wire's own
    // `UpdateContinuousBackups` has no generic-dispatch arm yet).
    cluster.enable_pitr(table);
    assert!(
        cluster.metadata(0).table_pitr(table).is_some(),
        "seed={seed}: table_pitr({table}) still None right after enable_pitr"
    );
    // Every node's own view must agree PITR is enabled — a plain
    // catalog-convergence check, no wire round trip needed (mirrors
    // `create_enable_write_seal_disable`'s own stream-label convergence
    // check).
    for n in 0..cluster.node_count() as u64 {
        assert!(
            cluster.metadata(n).table_pitr(table).is_some(),
            "seed={seed}: node {n} disagrees that PITR is enabled for {table}"
        );
    }

    let tablet = tablet_of_table(&cluster, table);
    let leader = leader_of_table(&cluster, table);

    // Write a few items from a NON-leader node — proving the writes reach
    // the tablet's real leader (forwarded, since the writer isn't it)
    // before this test ever seals anything.
    let writer = non_leader_of_table(&cluster, table);
    for (pk, v) in [("a", "1"), ("b", "2"), ("c", "3")] {
        let (status, body) = put_item(&mut cluster, writer, table, pk, v);
        assert_eq!(
            status, 200,
            "seed={seed}: PutItem({pk}) from non-leader node {writer} failed: {body}"
        );
    }
    // Read one back with `ConsistentRead: true` (ADR 0055) before trusting
    // the writes landed — a read that verifies a write must ask for the
    // strong path, or this assertion races the eventually-consistent
    // default and proves nothing.
    let (status, body) = get_item(&mut cluster, leader, table, "a", true);
    assert_eq!(status, 200, "seed={seed}: GetItem(a) failed: {body}");
    assert!(
        body.contains(r#""v":{"S":"1"}"#),
        "seed={seed}: item \"a\" missing/wrong after write: {body}"
    );

    // Seal on the leader — `drive_pitr_seal` loops `index_drain::
    // pitr_seal_now` to exhaustion for every led, PITR-enabled tablet. This
    // is the call that panicked with "there is no reactor running" before
    // `pitr_seal_now`'s commit-wait loop was converted to the `Env` seam
    // (issue #993).
    cluster.drive_pitr_seal(leader);

    let meta = cluster.metadata(leader);
    let sealed: Vec<_> = meta
        .pitr_segments
        .iter()
        .filter(|((t, _epoch), _row)| *t == tablet)
        .collect();
    assert!(
        !sealed.is_empty(),
        "seed={seed}: drive_pitr_seal(leader={leader}) produced no pitr_segments row \
         for tablet {}",
        tablet.0
    );
    assert_eq!(
        sealed.len(),
        1,
        "seed={seed}: expected exactly one sealed epoch for a single seal pass: {sealed:?}"
    );
    let (_, row) = sealed[0];
    assert_eq!(
        row.count, 3,
        "seed={seed}: sealed segment row's own record count doesn't match the 3 writes: {row:?}"
    );
    assert_eq!(
        row.table, table,
        "seed={seed}: sealed row names the wrong table: {row:?}"
    );
    assert!(
        !row.expired,
        "seed={seed}: a freshly sealed segment must not already be marked expired: {row:?}"
    );

    // The segment object the seal wrote is durably present in the shared
    // `SimSegmentStore` every node's own `backup_store` wraps a clone of.
    let store = cluster.backup_store();
    assert!(
        !store.stored_ids().is_empty(),
        "seed={seed}: backup_store().stored_ids() is empty after a real PITR seal"
    );
}

/// `ANIMUS_SEED=<seed> cargo test -p animusd --lib create_enable_write_seal`
/// replays this scenario at a specific seed (repo convention).
#[test]
fn create_enable_write_seal() {
    run_create_enable_write_seal(env_seed(0x993E_0001));
}

#[test]
fn create_enable_write_seal_over_seeds() {
    for i in 0..5 {
        run_create_enable_write_seal(0x993E_1000 + i);
    }
}
