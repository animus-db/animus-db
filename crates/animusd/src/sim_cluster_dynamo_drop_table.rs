//! `SimCluster`-driven deterministic coverage for the dropped-table GC
//! reclaim (ADR 0024) — C-04 / ADR 0061 rung D4 PR 3.
//!
//! Since [`super::sim_cluster`]'s own D4 PR 1 (closing issue #715), every
//! node runs a real `animus_cp_data::host::Reconciler` (see that module's
//! own doc), so a dropped table's tablets are now genuinely released and
//! their engines reclaimed under this fixture — the identical `HostAction::
//! Reclaim` mechanism `animus-cp-data/tests/reconciler_corpus.rs` already
//! exercises at the reconciler-unit level. This module is a **driver plus
//! assertions**, not new mechanism: no code in `animus-cp-data`/`host.rs`
//! changed for this PR.
//!
//! **Driver: the real wire, `DeleteTable`.** `dynamo::dispatch_table_op`'s
//! `DeleteTable` arm already calls `delete_table` → `ClientCtx::drop_table`
//! (`schema.rs`), which is `<E: Env, R: RelayClient>`-generic and therefore
//! reachable under `SimEnv` with **zero** widening needed — unlike several
//! earlier D3 rungs, this PR needed no new generic surface at all. Every
//! scenario below issues a real `DynamoDB_20120810.DeleteTable` request via
//! [`SimCluster::dynamo`], exactly as `sim_cluster_dynamo_table_ops.rs`'s own
//! `delete_table_removes_it_and_a_repeat_delete_is_not_found` already does —
//! this module goes one step further and proves the **physical** reclaim
//! (the tablet's own private engine reads back empty) that file's own
//! `DeleteTable` tests stop short of.
//!
//! **Observables, converged-or-timeout polled** (never a one-shot assert —
//! `host::Reconciler`'s own teardown is itself async,
//! `RECLAIM_STOP_TIMEOUT`-bounded on the production side, so a snapshot
//! taken mid-teardown can legitimately still show a stale entry for one more
//! tick — mirroring `sim_cluster_dynamo_table_ops.rs::
//! assert_no_zombie_groups`'s own discipline):
//!
//! 1. every node's own [`SimCluster::metadata`]`.tablets` no longer names
//!    the table's tablet(s) (`Metadata::has_table_tablet`);
//! 2. every node's own [`SimCluster::hosted_tablets`] no longer names them
//!    (no zombie `RaftKvNode` handle in `ClusterEdgeState`);
//! 3. every node's own [`SimCluster::storage`] for that tablet id reads back
//!    **empty** (`MemoryTabletEngines::engine` get-or-creates, so a
//!    reclaimed tablet's engine is a fresh, empty one — the actual GC proof,
//!    mirroring `animus-cp-data/tests/reconciler_corpus.rs::Cluster::
//!    storage`'s own "reads back empty" convention exactly);
//! 4. a fresh `CreateTable` with the SAME name afterward gets a NEW tablet
//!    id (ids are never reused, ADR 0024) and serves reads/writes.
//!
//! **Scenarios** (seed-parameterized, replayed at 5+ seeds each except
//! scenario 4 — see below): (1) drop after writes, base table, 3 nodes; (2)
//! drop with a declared GSI — the hidden index table's tablet is reclaimed
//! too, cascading in the ADR 0041 §5 order `ClientCtx::drop_table`'s own
//! doc states; (3) drop issued immediately after create, with no
//! intervening `run_for` — the Host-vs-Reclaim race `assert_idempotent`'s
//! own discipline calls for (assert *state*, never action counts — a node
//! whose reconciler hasn't ticked even once yet simply never hosts the
//! tablet at all, which is exactly as valid a path to "reclaimed" as
//! hosting-then-tearing-down); (4) a node crashed while hosting the table
//! then restarted after the drop commits — **not** a converging scenario,
//! see below; (5) a 4-node cluster (the `node_count > MAX_REPLICATION_
//! FACTOR` shape D4 PR 1 made safe, `sim_cluster_dynamo_table_ops.rs::
//! every_node_hosts_exactly_its_replica_set_after_rebalance`'s own fixture)
//! where the dropped table's tablet was rebalanced onto a *different* node
//! set than the one `CreateTable` originally picked — proving reclaim keys
//! off `Metadata`'s own **current** replica set, never a stale
//! creation-time snapshot.
//!
//! **A real, previously-uncharacterized reclaim gap was found investigating
//! scenario 4 — reported here, not fixed (out of this PR's own "driver plus
//! assertions, not new mechanism" scope).** See
//! [`scenario_4_a_node_crashed_during_the_drop_and_restarted_leaks_its_
//! engine`]'s own doc for the full account: `host::Reconciler::
//! gather_facts` derives every fact **exclusively** from the tablets
//! currently named in `Metadata` (`view.tablets.iter()`, both for the
//! already-hosted branch and the join-candidate branch) plus this
//! reconciler's own in-process `LocalState`. A table dropped from
//! `Metadata` is removed from `view.tablets` **synchronously**, so a node
//! whose whole process is down across the drop-and-Metadata-converges
//! window comes back with (a) a brand-new, empty `LocalState` (nothing ever
//! persists it — see `crates/animusd/CLAUDE.md`'s drop-table-GC entry:
//! "there is no more durable `cp-hosted` marker... a restart just
//! re-discovers every tablet to host from replicated `Metadata`") and (b) a
//! `Metadata` that already, synchronously, never names the dropped tablet
//! at all by the time this node's reconciler first ticks — so
//! `gather_facts` produces **no fact whatsoever** for that tablet id,
//! `plan()` never places it in `next.hosted`, and `HostAction::Reclaim`
//! (which only ever fires for a tablet this reconciler's own `LocalState`
//! currently claims — see `plan`'s own Phase 3 doc) can never target it.
//! The tablet's own private engine — genuinely populated with data written
//! before the crash — is a permanent, silent leak: reachable in production
//! too, not a `SimCluster`-fixture artifact (`gather_facts`'s scoping is
//! unconditional, real `LsmEngine`'s `LsmTabletFactory::probe`/`destroy`
//! included — see that impl's own doc in `lib.rs`, which scans by filename
//! prefix, called only for tablet ids `gather_facts` already decided to ask
//! about). `docs/adr/0024-drop-table-data-gc.md`'s own text (lines 94-99)
//! describes the *pre-`host::Reconciler`* per-tablet-marker design's
//! guarantee here — "a replica that was down during the drop restarts,
//! re-hosts the tablet from its marker/engine, then its GC loop reclaims it
//! once its control replica catches up" — which depended on a durable
//! per-node marker surviving the restart to force a re-host attempt first;
//! that marker is gone (ADR 0050, see `crates/animusd/CLAUDE.md`'s matching
//! note), and nothing replaced its restart-time "ask about what I used to
//! host, not just what `Metadata` currently says" role.
//! `every_node_hosts_exactly_its_replica_set_after_rebalance`-style "no
//! zombie groups" convergence (metadata + hosted-set) is **unaffected** —
//! `ClusterEdgeState`'s own registrations are purged directly by
//! `SimCluster::restart`'s fixture code (mirroring a real process's driver
//! tasks simply not existing after a restart), independent of
//! `host::Reconciler` entirely — only the **physical engine data** leaks.
//! Confirmed empirically at 6 seeds (see that test's own doc), not just by
//! static analysis, and kept as one `#[ignore]`d regression rather than 5
//! passing ones — a green test pinning the current, buggy behavior would
//! misleadingly read as an accepted contract, which this is not; scenario
//! 4 does not get the same 5-seed replay every other scenario does, since a
//! single reproducible ignored regression is what a future fix needs to run
//! against, not a soak.
//!
//! Seed replay (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <test name>`.

use std::time::Duration;

use animus_env::{NodeId, nid};
use animus_storage::StorageEngine;
use animus_tablet::TabletId;

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// One DynamoDB wire `CreateTable` for a plain single-key (`pk`, string)
/// table named `table`, issued from `node` — mirrors `sim_cluster_dynamo_
/// table_ops.rs`'s own identically-named helper.
fn create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

fn delete_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(r#"{{"TableName":"{table}"}}"#);
    cluster.dynamo(node, "DynamoDB_20120810.DeleteTable", body.as_bytes())
}

fn put_item(cluster: &mut SimCluster, node: u64, table: &str, pk: &str) -> (u16, String) {
    let body = format!(r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"{pk}"}}}}}}"#);
    cluster.dynamo(node, "DynamoDB_20120810.PutItem", body.as_bytes())
}

/// `table`'s own tablet id, per `node`'s own view of `Metadata` — panics if
/// `node` sees no tablet for `table` (every caller checks this immediately
/// after a `CreateTable` that already returned 200, so absence would mean a
/// real bug, not a legitimate empty state to tolerate).
fn tablet_of(cluster: &SimCluster, node: u64, table: &str) -> TabletId {
    let meta = cluster.metadata(node);
    *meta
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table {table} has no tablet on node {node}'s own view"))
        .0
}

/// Drive `cluster` in `step`-sized increments, calling `done` after each,
/// until it returns `true` or `budget` is exhausted — the converged-or-
/// timeout idiom every eventual property in this repo needs (root
/// `CLAUDE.md`'s Testing rule); a private, file-local twin of `SimCluster`'s
/// own (private) `poll_until`, matching every other sibling `sim_cluster_
/// dynamo_*.rs` module's own convention of rolling its own rather than
/// widening that method's visibility for no other reason.
fn poll_until(
    cluster: &mut SimCluster,
    budget: Duration,
    mut done: impl FnMut(&SimCluster) -> bool,
) {
    const STEP: Duration = Duration::from_millis(50);
    let mut elapsed = Duration::ZERO;
    loop {
        if done(cluster) {
            return;
        }
        assert!(
            elapsed < budget,
            "condition did not converge within {budget:?} (seed={})",
            cluster.seed()
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

/// The three converged-or-timeout observables this whole module is about,
/// checked together against every id in `nodes`: `table` no longer has a
/// tablet anywhere, no node's `ClusterEdgeState` still names `tablet`, and —
/// once that much has converged — every one of `nodes`'s own private engine
/// for `tablet` reads back empty (the actual physical-reclaim proof, ADR
/// 0050 rung 1's "the engine is private, so whole-engine deletion is the
/// erase" contract).
async fn assert_reclaimed(
    cluster: &mut SimCluster,
    table: &str,
    tablet: TabletId,
    nodes: &[u64],
    budget: Duration,
) {
    let seed = cluster.seed();
    poll_until(cluster, budget, |c| {
        nodes.iter().all(|&n| {
            !c.metadata(n).has_table_tablet(table) && !c.hosted_tablets(n).contains(&tablet)
        })
    });
    for &n in nodes {
        let engine = cluster.storage(n, tablet);
        let entries = engine
            .entries()
            .await
            .expect("a MemoryEngine read never fails");
        assert!(
            entries.is_empty(),
            "node {n}'s own private engine for reclaimed tablet {tablet:?} of table \
             `{table}` is not empty (seed={seed}): {entries:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Scenario 1: drop after writes, base table, 3 nodes.
// ---------------------------------------------------------------------------

fn run_drop_after_writes_reclaims_base_table(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "orders");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let tablet = tablet_of(&cluster, 0, "orders");

    for pk in ["a", "g", "m", "s", "z"] {
        let (status, body) = put_item(&mut cluster, 0, "orders", pk);
        assert_eq!(status, 200, "seed={seed}: PutItem({pk}) failed: {body}");
    }

    let (status, body) = delete_table(&mut cluster, 0, "orders");
    assert_eq!(status, 200, "seed={seed}: DeleteTable failed: {body}");

    let nodes: Vec<u64> = (0..cluster.node_count() as u64).collect();
    let cluster_seed = cluster.seed();
    futures::executor::block_on(assert_reclaimed(
        &mut cluster,
        "orders",
        tablet,
        &nodes,
        Duration::from_secs(10),
    ));

    // Ids are never reused: a fresh `CreateTable` with the identical name
    // mints a NEW tablet id and serves reads/writes normally.
    let (status, body) = create_table(&mut cluster, 0, "orders");
    assert_eq!(
        status, 200,
        "seed={cluster_seed}: re-CreateTable failed: {body}"
    );
    let new_tablet = tablet_of(&cluster, 0, "orders");
    assert_ne!(
        new_tablet, tablet,
        "seed={cluster_seed}: a recreated table must get a NEW tablet id, never the \
         reclaimed one back"
    );
    let (status, body) = put_item(&mut cluster, 0, "orders", "fresh");
    assert_eq!(
        status, 200,
        "seed={cluster_seed}: PutItem on the recreated table failed: {body}"
    );
    let get_body = r#"{"ConsistentRead":true,"TableName":"orders","Key":{"pk":{"S":"fresh"}}}"#;
    let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.GetItem", get_body.as_bytes());
    assert_eq!(
        status, 200,
        "seed={cluster_seed}: GetItem on the recreated table failed: {body}"
    );
    assert!(
        body.contains(r#""pk":{"S":"fresh"}"#),
        "seed={cluster_seed}: the recreated table must actually serve the write back: {body}"
    );
}

/// `ANIMUS_SEED=<seed> cargo test -p animusd --lib
/// drop_after_writes_reclaims_base_table` replays this scenario at a
/// specific seed (repo convention).
#[test]
fn drop_after_writes_reclaims_base_table() {
    run_drop_after_writes_reclaims_base_table(env_seed(0xE4AF_0001));
}

#[test]
fn drop_after_writes_reclaims_base_table_over_seeds() {
    for i in 0..5 {
        run_drop_after_writes_reclaims_base_table(0xE4AF_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario 2: drop with a declared GSI — the hidden index table is
// reclaimed too.
// ---------------------------------------------------------------------------

/// `CreateTable` with one declared GSI, a write, `SimCluster::drain_gsi` to
/// materialize the hidden `<base>$<index>` table's own tablet (mirroring
/// `sim_cluster_dynamo_table_ops.rs::gsi_query_materializes_rows_after_a_
/// drain`'s own setup), then `DeleteTable` on the BASE table — proving
/// `ClientCtx::drop_table`'s ADR 0041 §5 cascade reaches the hidden table
/// too: both the base and the hidden index table's own tablets, hosted
/// sets, and physical engines are reclaimed together.
fn run_drop_with_gsi_reclaims_the_hidden_table(seed: u64) {
    let mut cluster = SimCluster::new(seed, 1, 1);

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
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable with a GSI failed: {body}"
    );
    let base_tablet = tablet_of(&cluster, 0, "users");

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"users","Item":{"id":{"S":"u1"},"email":{"S":"a@x"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: PutItem failed: {body}");

    cluster.drain_gsi(0, "users");
    let hidden_table = animus_dynamo::index_table_name("users", "by-email");
    assert!(
        cluster.metadata(0).has_table_tablet(&hidden_table),
        "seed={seed}: the hidden index table must have a tablet after the drain"
    );
    let hidden_tablet = tablet_of(&cluster, 0, &hidden_table);
    assert_ne!(
        base_tablet, hidden_tablet,
        "seed={seed}: the base and hidden index tables must be distinct tablets"
    );

    let (status, body) = delete_table(&mut cluster, 0, "users");
    assert_eq!(status, 200, "seed={seed}: DeleteTable failed: {body}");

    let nodes = [0u64];
    let cluster_seed = cluster.seed();
    futures::executor::block_on(async {
        assert_reclaimed(
            &mut cluster,
            "users",
            base_tablet,
            &nodes,
            Duration::from_secs(10),
        )
        .await;
        assert_reclaimed(
            &mut cluster,
            &hidden_table,
            hidden_tablet,
            &nodes,
            Duration::from_secs(10),
        )
        .await;
    });
    assert!(
        !cluster.metadata(0).has_table_schema("users"),
        "seed={cluster_seed}: the base table's own schema must be gone too"
    );
}

#[test]
fn drop_with_gsi_reclaims_the_hidden_table() {
    run_drop_with_gsi_reclaims_the_hidden_table(env_seed(0xE4AF_0002));
}

#[test]
fn drop_with_gsi_reclaims_the_hidden_table_over_seeds() {
    for i in 0..5 {
        run_drop_with_gsi_reclaims_the_hidden_table(0xE4AF_2000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario 3: drop issued immediately after create — the Host-vs-Reclaim
// race.
// ---------------------------------------------------------------------------

/// `CreateTable` (which already commit-waits the schema AND
/// `await_table_serveable`s at least one replica, ADR 0023's 2026-08-17
/// amendment — so the tablet's LEADER replica is guaranteed hosted the
/// instant this call returns 200) immediately followed by `DeleteTable`,
/// with **no** intervening `run_for` at all — the other two replicas'
/// reconcilers may not have ticked even once yet. Mirrors `reconciler_
/// corpus.rs::assert_idempotent`'s own discipline: this asserts
/// converged-`state` (every observable in [`assert_reclaimed`]), never a
/// count of which `HostAction` each node happened to take — a replica that
/// never got around to hosting the tablet before it vanished from
/// `Metadata` reaches the identical reclaimed end state as one that hosted
/// then tore down, and both are equally valid.
fn run_drop_immediately_after_create_races_host_and_reclaim(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "hot");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let tablet = tablet_of(&cluster, 0, "hot");

    // No `run_for` between create and delete: the race is the point.
    let (status, body) = delete_table(&mut cluster, 0, "hot");
    assert_eq!(status, 200, "seed={seed}: DeleteTable failed: {body}");

    let nodes: Vec<u64> = (0..cluster.node_count() as u64).collect();
    futures::executor::block_on(assert_reclaimed(
        &mut cluster,
        "hot",
        tablet,
        &nodes,
        Duration::from_secs(10),
    ));
}

#[test]
fn drop_immediately_after_create_races_host_and_reclaim() {
    run_drop_immediately_after_create_races_host_and_reclaim(env_seed(0xE4AF_0003));
}

#[test]
fn drop_immediately_after_create_races_host_and_reclaim_over_seeds() {
    for i in 0..5 {
        run_drop_immediately_after_create_races_host_and_reclaim(0xE4AF_3000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario 4: a node crashed while hosting the table, then restarted after
// the drop commits — a REAL reclaim gap, not a converging scenario.
// ---------------------------------------------------------------------------

/// **A real, previously-uncharacterized reclaim gap**, found investigating
/// this exact scenario (see this module's own top-of-file doc for the full
/// mechanism) — NOT the "converges to the same state" positive proof the
/// scenario was meant to be. Crash one non-leader replica that is actively
/// hosting the table's tablet (network-muted, its tasks — including its own
/// reconciler loop — stay alive, per `SimCluster::crash`'s own doc), issue
/// `DeleteTable` from a still-live node, then `SimCluster::restart` the
/// crashed node (a true process restart: every task dropped, a fresh
/// `RaftNode`/`Reconciler` built on the same id, reusing the same
/// `MemoryTabletEngines` handle — see `sim_cluster.rs`'s own doc).
///
/// **The three `assert_reclaimed` observables never converge for the
/// restarted node's own engine.** `host::Reconciler::gather_facts` derives
/// every fact exclusively from tablets `Metadata` *currently* names
/// (`view.tablets.iter()`, both for the already-hosted branch and the
/// join-candidate branch) plus this reconciler's own in-process
/// `LocalState` — nothing persists `LocalState` across a real process
/// restart (`crates/animusd/CLAUDE.md`'s drop-table-GC entry: "there is no
/// more durable `cp-hosted` marker... a restart just re-discovers every
/// tablet to host from replicated `Metadata`"). Under `SimEnv`'s
/// effectively-instant replication, the restarted node's own control-plane
/// `RaftNode` catches up (via ordinary peer replication) to the
/// already-converged "table absent" state well within one
/// `RECONCILER_FALLBACK` tick — so `gather_facts` produces **no fact at
/// all** for the dropped tablet id on the very first tick the fresh
/// reconciler ever runs, `plan()` never places it in `next.hosted`, and
/// `HostAction::Reclaim` (which only ever fires for a tablet this
/// reconciler's own `LocalState` currently claims) can never target it. The
/// tablet's own private engine — genuinely populated with real data written
/// before the crash — is a permanent, silent leak.
///
/// **This is not a `SimCluster`-fixture artifact.** `gather_facts`'s
/// scoping is unconditional, and `LsmTabletFactory::probe`/`destroy`
/// (`lib.rs`, the real `LsmEngine` backend) is only ever called for tablet
/// ids `gather_facts` already decided to ask about — the identical
/// mechanism, real disk included. `docs/adr/0024-drop-table-data-gc.md`'s
/// own text (lines 94-99) describes the *pre-`host::Reconciler`*
/// per-tablet-marker design's guarantee here — "a replica that was down
/// during the drop restarts, re-hosts the tablet from its marker/engine,
/// then its GC loop reclaims it once its control replica catches up" —
/// which depended on a durable per-node marker surviving the restart to
/// force a re-host attempt first; that marker is gone (ADR 0050), and
/// nothing replaced its restart-time "ask about what I used to host, not
/// just what `Metadata` currently says" role.
///
/// **Confirmed empirically, not just by static analysis**: this exact
/// scenario (crash-then-drop-then-restart, in that order, so the
/// straightforwardly "realistic" timing) was first written as a POSITIVE
/// convergence assertion and reliably failed — every one of 6 seeds tried
/// (`0xE4AF_0004`, `0xE4AF_4000..=0xE4AF_4004`) reproduced the identical
/// non-empty leftover engine on the restarted node, never once converging
/// within a 15s virtual-time budget. `assert_reclaimed`'s metadata/hosted-
/// set observables converge fine regardless (they're purged/re-derived by
/// `SimCluster::restart`'s own fixture code and by `plan()`'s ordinary
/// `Release` path respectively, independent of `gather_facts`'s scoping
/// gap) — only the physical engine leaks, silently.
///
/// **`#[ignore]`d rather than asserted-passing or deleted**: this pins the
/// CURRENT (buggy) behavior as a reproducible regression a future fix can
/// run against, without landing a green test that would misleadingly read
/// as "this converges" — root `CLAUDE.md`'s green-is-an-invariant rule is
/// exactly why a genuine gap can't be asserted around here, and this PR's
/// own scope ("a driver plus assertions, not new mechanism") is why the
/// gap is reported rather than fixed in `animus-cp-data::host` here. Run
/// explicitly to reproduce: `cargo test -p animusd --lib
/// scenario_4_a_node_crashed_during_the_drop_and_restarted_leaks_its_engine
/// -- --ignored`.
#[test]
#[ignore = "known gap (ADR 0061 rung D4 PR 3 finding): a node crashed while \
            hosting a table, restarted after the table's drop has already \
            converged elsewhere, never reclaims its own stale tablet engine \
            — see this test's own doc for the full mechanism (host::\
            Reconciler::gather_facts scopes every fact to Metadata's \
            CURRENT tablet map, which a dropped tablet is synchronously \
            absent from); not fixed by this PR (driver+assertions scope \
            only)"]
fn scenario_4_a_node_crashed_during_the_drop_and_restarted_leaks_its_engine() {
    let seed = 0xE4AF_0004;
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "ledger");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let tablet = tablet_of(&cluster, 0, "ledger");
    let (status, body) = put_item(&mut cluster, 0, "ledger", "k1");
    assert_eq!(status, 200, "seed={seed}: PutItem failed: {body}");

    let leader = cluster
        .leader_index_of(tablet)
        .expect("seed={seed}: the fresh group elected a leader");
    let victim = (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a 3-node cluster has a non-leader node");

    // The victim genuinely holds this tablet's own data before it goes
    // offline — the leak below is of real content, not an empty engine
    // that would trivially "converge" either way.
    futures::executor::block_on(async {
        let entries = cluster
            .storage(victim, tablet)
            .entries()
            .await
            .expect("engine read ok");
        assert!(
            !entries.is_empty(),
            "seed={seed}: the victim must actually host real data before crashing"
        );
    });

    cluster.crash(victim);

    // The drop is issued from — and commits via — the two still-live nodes
    // (a 3-node control/data quorum tolerates one crashed member).
    let issuer = (0..cluster.node_count() as u64)
        .find(|&n| n != victim)
        .expect("a 3-node cluster has a live node other than the victim");
    let (status, body) = delete_table(&mut cluster, issuer, "ledger");
    assert_eq!(status, 200, "seed={seed}: DeleteTable failed: {body}");

    // Let the drop fully converge on the two live nodes before the victim
    // ever comes back — the realistic "restart once the dust has settled"
    // shape, not an artificially narrow race window.
    let live_nodes: Vec<u64> = (0..cluster.node_count() as u64)
        .filter(|&n| n != victim)
        .collect();
    futures::executor::block_on(assert_reclaimed(
        &mut cluster,
        "ledger",
        tablet,
        &live_nodes,
        Duration::from_secs(10),
    ));

    cluster.restart(victim);

    // Expected to time out and fail on the CURRENT (buggy) behavior — see
    // this test's own doc. `budget` is generous (well past the point where
    // every other observable in this module converges in well under 1s)
    // specifically so a future fix's own success is unambiguous, not an
    // artifact of a too-short poll.
    futures::executor::block_on(assert_reclaimed(
        &mut cluster,
        "ledger",
        tablet,
        &[victim],
        Duration::from_secs(15),
    ));
}

// ---------------------------------------------------------------------------
// Scenario 5: a 4-node cluster, dropping a table whose tablet a rebalance
// already moved onto a DIFFERENT replica set than `CreateTable` first
// picked.
// ---------------------------------------------------------------------------

/// Mirrors `sim_cluster_dynamo_table_ops.rs::
/// every_node_hosts_exactly_its_replica_set_after_rebalance`'s own setup —
/// 4 nodes, `MAX_REPLICATION_FACTOR = 3` — but adds a `DeleteTable` at the
/// end: after the control leader's own live `rebalance_placement` pass has
/// already moved `soak0`'s tablet from its creation-time replicas
/// (`[n0,n1,n2]`) onto `[n1,n2,n3]`, dropping `soak0` must reclaim off the
/// tablet's **current** replica set (`[n1,n2,n3]`) — node 0, which no
/// longer hosts it at all by the time the drop is issued, needs nothing
/// reclaimed from it (already converged empty by the pre-existing ADR 0029
/// removed-replica GC, proven by `every_node_hosts_exactly_its_replica_set_
/// after_rebalance` itself) — while nodes 1-3 genuinely reclaim real,
/// currently-hosted data.
fn run_drop_reclaims_off_the_rebalanced_replica_set(seed: u64) {
    let mut cluster = SimCluster::new(seed, 4, 3);
    let leader = cluster.control_leader_index() as u64;

    let mut tablets = Vec::new();
    for i in 0..3 {
        let table = format!("soak{i}");
        let (status, body) = create_table(&mut cluster, leader, &table);
        assert_eq!(
            status, 200,
            "seed={seed}: CreateTable {table} failed: {body}"
        );
        tablets.push((table, tablet_of(&cluster, leader, &format!("soak{i}"))));
    }

    // Give the control leader's own rebalance pass, and then every node's
    // own reconciler, time to converge — mirroring the sibling test's own
    // wait exactly.
    cluster.run_for(Duration::from_secs(10));

    let (soak0, soak0_tablet) = &tablets[0];
    let replicas: Vec<NodeId> = {
        let meta = cluster.metadata(leader);
        let (_, t) = meta
            .tablets_for_table(soak0)
            .next()
            .unwrap_or_else(|| panic!("seed={seed}: {soak0} has no tablet"));
        let mut ids = t.replicas.clone();
        ids.sort();
        ids
    };
    assert_eq!(
        replicas,
        vec![nid(1), nid(2), nid(3)],
        "seed={seed}: soak0's tablet should have been rebalanced onto the idle \
         fourth node, same as the sibling reconciler-hazard test — if this \
         assertion itself starts failing, `rebalance_placement`'s own decision \
         changed and this scenario needs re-deriving from whatever it now does"
    );

    // Data written to soak0 (through its CURRENT leader, wherever that is
    // post-rebalance) must genuinely be reclaimed off nodes 1-3, not just
    // trivially absent because nothing was ever written.
    let put_target = cluster
        .leader_index_of(*soak0_tablet)
        .unwrap_or_else(|| panic!("seed={seed}: {soak0}'s tablet has a leader"));
    let (status, body) = put_item(&mut cluster, put_target, soak0, "k1");
    assert_eq!(status, 200, "seed={seed}: PutItem({soak0}) failed: {body}");

    let (status, body) = delete_table(&mut cluster, leader, soak0);
    assert_eq!(
        status, 200,
        "seed={seed}: DeleteTable({soak0}) failed: {body}"
    );

    // Node 0 already converged to "not hosting soak0 at all" via the
    // pre-existing rebalance/release path — assert that plainly too, so a
    // regression there wouldn't be silently masked by only checking 1-3.
    assert!(
        !cluster.hosted_tablets(0).contains(soak0_tablet),
        "seed={seed}: node 0 should already not host soak0's tablet, independent \
         of the drop"
    );

    futures::executor::block_on(assert_reclaimed(
        &mut cluster,
        soak0,
        *soak0_tablet,
        &[1, 2, 3],
        Duration::from_secs(10),
    ));

    // The two tables never dropped stay completely unaffected.
    for (table, tablet) in &tablets[1..] {
        assert!(
            cluster.metadata(leader).has_table_tablet(table),
            "seed={seed}: {table} must be unaffected by soak0's drop"
        );
        assert!(
            !cluster.hosted_tablets(0).is_empty() || !cluster.hosted_tablets(1).is_empty(),
            "seed={seed}: at least one surviving table should still be hosted \
             somewhere after soak0's drop (tablet {tablet:?})"
        );
    }
}

#[test]
fn drop_reclaims_off_the_rebalanced_replica_set() {
    run_drop_reclaims_off_the_rebalanced_replica_set(env_seed(0xE4AF_0006));
}

#[test]
fn drop_reclaims_off_the_rebalanced_replica_set_over_seeds() {
    for i in 0..5 {
        run_drop_reclaims_off_the_rebalanced_replica_set(0xE4AF_6000 + i);
    }
}
