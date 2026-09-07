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
//! **All three of (1)-(3) are checked TOGETHER, in the SAME poll — never a
//! metadata/hosted-set check first, then a separate unwaited engine read**
//! (`assert_reclaimed`'s own doc has the full account): a just-restarted
//! node's `Metadata`/hosted-set facts can read as "already converged" from
//! the very first poll, well before its own `Reconciler` has ticked even
//! once — this was a real bug in this module's own harness, found and fixed
//! delivering scenario 4's own positive assertion (issue #722), not merely
//! a theoretical concern.
//!
//! **Scenarios** (seed-parameterized, replayed at 5+ seeds each): (1) drop
//! after writes, base table, 3 nodes; (2) drop with a declared GSI — the
//! hidden index table's tablet is reclaimed too, cascading in the ADR 0041
//! §5 order `ClientCtx::drop_table`'s own doc states; (3) drop issued
//! immediately after create, with no intervening `run_for` — the
//! Host-vs-Reclaim race `assert_idempotent`'s own discipline calls for
//! (assert *state*, never action counts — a node whose reconciler hasn't
//! ticked even once yet simply never hosts the tablet at all, which is
//! exactly as valid a path to "reclaimed" as hosting-then-tearing-down);
//! (4) a node crashed while hosting the table then restarted only after the
//! drop has already converged everywhere else — see below, this is the
//! issue #722 regression, now converging; (5) a 4-node cluster (the
//! `node_count > MAX_REPLICATION_FACTOR` shape D4 PR 1 made safe,
//! `sim_cluster_dynamo_table_ops.rs::every_node_hosts_exactly_its_replica_
//! set_after_rebalance`'s own fixture) where the dropped table's tablet was
//! rebalanced onto a *different* node set than the one `CreateTable`
//! originally picked — proving reclaim keys off `Metadata`'s own
//! **current** replica set, never a stale creation-time snapshot.
//!
//! **Scenario 4 pins issue #722's own fix, closed by a sibling PR to this
//! one** (`animus_cp_data::host`'s `EngineFactory::local_tablets` — the
//! reconciler's second fact source, alongside replicated `Metadata`). The
//! gap this scenario originally found and reported (not fixed, per this
//! PR's own "driver plus assertions, not new mechanism" scope) was real:
//! `host::Reconciler::gather_facts` used to derive every fact
//! **exclusively** from the tablets currently named in `Metadata`
//! (`view.tablets.iter()`) plus this reconciler's own in-process
//! `LocalState` — never persisted, so a node whose whole process was down
//! across the drop-and-`Metadata`-converges window came back with a
//! brand-new, empty `LocalState` and a `Metadata` view that, by the time
//! its first tick ever ran, already never named the dropped tablet at all;
//! `gather_facts` then produced no fact whatsoever for that tablet id, and
//! `HostAction::Reclaim` could never target it — a permanent, silent leak
//! of real data, reachable in production too (`LsmTabletFactory`'s own
//! `list()`-based enumeration is the identical mechanism, real disk
//! included). The fix gives the reconciler a second, restart-surviving
//! fact source: `EngineFactory::local_tablets()`, consulted exactly ONCE
//! per reconciler lifetime (its very first tick — see that method's and
//! `Reconciler::tick`'s own docs for why once is enough and for the
//! per-file safety argument, including why a pre-cutover in-place split
//! child's own already-materialized engine is never mistaken for an
//! orphan). See `docs/adr/0024-drop-table-data-gc.md`'s dated amendment,
//! `crates/animus-cp-data/CLAUDE.md`'s host-module entry, and
//! `docs/engineering-lessons.md` for the full account.
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

/// The three converged-or-timeout observables this whole module is about,
/// checked TOGETHER, every poll, against every id in `nodes`: `table` no
/// longer has a tablet anywhere, no node's `ClusterEdgeState` still names
/// `tablet`, and every one of `nodes`'s own private engine for `tablet`
/// reads back empty (the actual physical-reclaim proof, ADR 0050 rung 1's
/// "the engine is private, so whole-engine deletion is the erase"
/// contract).
///
/// **All three conditions must be folded into the SAME poll loop, never
/// checked in two passes** (a metadata/hosted-set check first, then a
/// separate post-loop engine read) — a restarted node's metadata/hosted-set
/// facts can read as "already converged" from the very first poll (a
/// freshly restarted control `RaftNode` starts with a genuinely blank
/// `Metadata`, indistinguishable at that instant from "caught up to the
/// table being dropped"), well before its own `Reconciler` has ticked even
/// once — a separate, unwaited post-loop engine read would then observe
/// stale, pre-crash content and fail even on fully correct behavior. This
/// is exactly the shape of bug root `CLAUDE.md`'s "eventual properties get
/// a converged-or-timeout poll, never a fixed-deadline one-shot assert"
/// rule warns about, just with the "one-shot assert" split across two
/// otherwise-correct-looking checks instead of one.
async fn assert_reclaimed(
    cluster: &mut SimCluster,
    table: &str,
    tablet: TabletId,
    nodes: &[u64],
    budget: Duration,
) {
    let seed = cluster.seed();
    // A hand-rolled async poll loop (not a sync `FnMut(&SimCluster) -> bool`
    // closure driven by a generic `poll_until` helper, this module's
    // earlier shape) — checking the engine's own content needs a real
    // `.await`, and a nested `futures::executor::block_on` from inside a
    // sync poll closure panics: "cannot execute `LocalPool` executor from
    // within another executor", confirmed empirically, not merely
    // suspected, while building this exact fix.
    const STEP: Duration = Duration::from_millis(50);
    let mut elapsed = Duration::ZERO;
    loop {
        let mut converged = true;
        for &n in nodes {
            if cluster.metadata(n).has_table_tablet(table)
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
            "condition did not converge within {budget:?} (seed={seed})"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
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
// the drop commits — issue #722, now a converging scenario (fixed).
// ---------------------------------------------------------------------------

/// **Issue #722, fixed.** Crash one non-leader replica that is actively
/// hosting the table's tablet (network-muted, its tasks — including its own
/// reconciler loop — stay alive, per `SimCluster::crash`'s own doc), issue
/// `DeleteTable` from a still-live node, let the drop fully converge on the
/// two live nodes, and only THEN `SimCluster::restart` the crashed node (a
/// true process restart: every task dropped, a fresh `RaftNode`/
/// `Reconciler` built on the same id, reusing the same `MemoryTabletEngines`
/// handle — see `sim_cluster.rs`'s own doc). This is the exact "realistic"
/// timing that used to leak: the restarted node's `Reconciler` starts from a
/// brand-new, empty `LocalState`, and its very first tick already sees the
/// fully-converged, table-absent `Metadata` — so before the fix, nothing
/// left this node any fact at all to reclaim tablet 1 by (see this module's
/// own top-of-file doc for the full pre-fix mechanism).
///
/// **The fix**: `host::Reconciler` now consults
/// `EngineFactory::local_tablets()` — the second fact source, alongside
/// `Metadata` — exactly once, on this exact first tick, and folds any
/// locally-present-but-`Metadata`-absent tablet id into `plan`'s ordinary
/// reclaim path. The victim genuinely holds real data (asserted below,
/// before it ever crashes) — the fix reclaims that real content, not an
/// already-empty engine that would trivially "converge" either way — and
/// `assert_reclaimed` (this module's own physical-reclaim proof: a fresh,
/// empty engine read back through the SAME registry the reconciler opens
/// from) now converges for the restarted node too, well inside the
/// generous 15s budget this test always used (the mechanism resolves in a
/// single tick in practice).
///
/// **Seed replay (repo convention)**: `ANIMUS_SEED=<seed> cargo test -p
/// animusd --lib scenario_4_a_node_crashed_during_the_drop_and_restarted_
/// reclaims_its_engine`.
fn run_scenario_4_a_node_crashed_during_the_drop_and_restarted_reclaims_its_engine(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "ledger");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let tablet = tablet_of(&cluster, 0, "ledger");
    let (status, body) = put_item(&mut cluster, 0, "ledger", "k1");
    assert_eq!(status, 200, "seed={seed}: PutItem failed: {body}");

    let leader = cluster
        .leader_index_of(tablet)
        .unwrap_or_else(|| panic!("seed={seed}: the fresh group elected a leader"));
    // The victim must be neither the tablet's own data-plane leader (the
    // scenario is about a REPLICA crashing) NOR the control-plane's own
    // leader — crashing the latter would force a control-plane election
    // before `DeleteTable`'s own commit-wait could ever succeed, an
    // entirely different (and, for a 3-node cluster, not always
    // fast-under-SimEnv) scenario this test isn't about. A 3-node cluster
    // always has at least one node that is neither.
    let control_leader = cluster.control_leader_index() as u64;
    let victim = (0..cluster.node_count() as u64)
        .find(|&n| n != leader && n != control_leader)
        .unwrap_or_else(|| {
            panic!(
                "seed={seed}: a 3-node cluster must have a node that is neither the \
                 tablet leader ({leader}) nor the control leader ({control_leader})"
            )
        });

    // The victim genuinely holds this tablet's own data before it goes
    // offline — the reclaim below is of real content, not an empty engine
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

    // `budget` stays generous (well past the point where every other
    // observable in this module converges in well under 1s) so a future
    // regression's own timeout is unambiguous, not an artifact of a
    // too-short poll.
    futures::executor::block_on(assert_reclaimed(
        &mut cluster,
        "ledger",
        tablet,
        &[victim],
        Duration::from_secs(15),
    ));
}

#[test]
fn scenario_4_a_node_crashed_during_the_drop_and_restarted_reclaims_its_engine() {
    run_scenario_4_a_node_crashed_during_the_drop_and_restarted_reclaims_its_engine(env_seed(
        0xE4AF_0004,
    ));
}

/// The six seeds issue #722's own investigation confirmed reliably
/// reproduced the pre-fix leak (`0xE4AF_0004`, `0xE4AF_4000..=0xE4AF_4004`),
/// plus ten more for soak — this scenario now gets the same seed-replay
/// discipline every other scenario in this module has.
#[test]
fn scenario_4_a_node_crashed_during_the_drop_and_restarted_reclaims_its_engine_over_seeds() {
    for seed in [
        0xE4AF_0004,
        0xE4AF_4000,
        0xE4AF_4001,
        0xE4AF_4002,
        0xE4AF_4003,
        0xE4AF_4004,
    ] {
        run_scenario_4_a_node_crashed_during_the_drop_and_restarted_reclaims_its_engine(seed);
    }
    for i in 0..10 {
        run_scenario_4_a_node_crashed_during_the_drop_and_restarted_reclaims_its_engine(
            0xE4AF_7000 + i,
        );
    }
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
