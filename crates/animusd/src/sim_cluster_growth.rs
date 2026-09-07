//! `SimCluster`-driven deterministic coverage for ADR 0030 online cluster
//! growth and ADR 0032 seed-join decommission — ADR 0061 rung D4 PR 4 (C-04
//! D4), the last of the four D4 roadmap rungs (#715/#722, auto-split,
//! backup janitor, and this one).
//!
//! **The fixture surface this rung adds** (`sim_cluster.rs`'s own
//! `SimCluster::grow`/`drain`/`remove`, plus the private `SimClusterHandle::
//! push_ctx` and free function `spawn_remote_mirror_sync_loop` they build
//! on — five new signatures total, comfortably under this rung's own ≤ 25
//! budget) is the one piece of D4 machinery `SimCluster::new` structurally
//! cannot exercise: that constructor fixes the whole node set up front (its
//! own module doc's "the whole node set is known at construction" note),
//! and production's real growth/data-join constructors
//! (`animusd::run_node_join`/`BoundDataNode::start_data_with_growth`) are
//! `ProdEnv`-only (real sockets, real `tokio::time::sleep`) and cannot run
//! under `SimEnv` at all. See `sim_cluster.rs`'s own doc comments on
//! `SimCluster::grow`/`drain`/`remove`/`spawn_remote_mirror_sync_loop` for
//! the full per-method design (this file intentionally doesn't restate it).
//!
//! **`grow` supports `role = "data"` only** — a `"combined"` growth node (a
//! new control-plane voter, joining the *live* Raft quorum via
//! `change_membership`) was scoped for this rung and deferred: it needs
//! `self.controls` itself to grow, a materially different and separately
//! budgeted mechanism from a data-only node's `ControlHandle::Remote`
//! mirror. Every scenario below is therefore a data-only growth/removal.
//!
//! **The one genuinely new mechanism**: `ControlHandle::Remote`'s real
//! mirror-sync logic (`RemoteControlClient::observe`/`observe_delta`, the
//! leader-hint lifecycle) had never run under `SimEnv` before this rung —
//! every prior `SimCluster` node was a genuine control-group voter
//! (`ControlHandle::Local`). `spawn_remote_mirror_sync_loop` is a
//! `SimEnv`-native reimplementation of `animusd`'s own
//! `remote_metadata_watch_loop` (that production function is structurally
//! unreachable here — see its own doc for why) driving the identical wire
//! protocol, so what's actually under test is the real mirror logic, not a
//! stand-in for it.
//!
//! **Scenarios** (seed-parameterized, replayed at 5 seeds each via a
//! `_over_seeds` sibling — `ANIMUS_SEED=<seed> cargo test -p animusd --lib
//! <test name>` replays any one, per the repo convention; primary seeds:
//! (a) `0x6706_0001`, (c) `0x6706_0003`):
//!
//! (a) grow a 3-node cluster to 4 (data-only): the new node self-registers
//!     `Active` on every node's own view (including its own `Remote`
//!     mirror, converged-or-timeout polled on `effective_metadata()` via
//!     [`SimCluster::metadata`]), and serves a genuinely forwarded write +
//!     read (it hosts no replica of the table at all, so both round-trip
//!     over the real `SimRelayClient` wire to whichever node actually
//!     leads the tablet);
//! (b) growth, THEN three `CreateTable`s at the wire's own fixed RF (3,
//!     over 4 now-`Active` members) — three, not one: `rebalance_step`'s
//!     own convergence bound (max−min ≤ 1) is already satisfied by a single
//!     tablet's "three nodes hold it, one holds none," so nothing would
//!     ever move (see [`provision_soak_tables_and_wait_for_replica`]'s own
//!     doc, and its matching `docs/engineering-lessons.md` entry, for this
//!     rung's own first-draft mistake). Three tables reproduce
//!     `sim_cluster_dynamo_table_ops.rs`'s own #715 regression imbalance,
//!     and the ordinary balance-driven rebalance (already running
//!     unconditionally — no extra driving needed) converges to the new node
//!     holding a replica of at least one of them — [`assert_no_zombie_
//!     groups`] then proves the replica it displaced was actually torn
//!     down, not merely that a new one was added;
//! (c) grow, wait for a rebalance-driven replica to land on the new node
//!     (reusing (b)'s own three-table setup), then [`SimCluster::drain`] +
//!     [`SimCluster::remove`] it: its replica re-homes onto the three
//!     original survivors, no node is left holding a zombie group, no
//!     node's own `Metadata::members` names it any more, and — the
//!     "ids are never reused" property — a subsequent `grow` mints a
//!     strictly higher index, never the removed one;
//! (d) the control-plane leader crashes between the two `MetaCommand`s
//!     `SimCluster::grow` itself proposes (`RegisterNode` then
//!     `UpsertMember{Active}`) — reproduced by hand via
//!     [`SimCluster::propose_meta`] (which re-resolves the CURRENT leader
//!     on every call) rather than instrumenting `grow` itself for fault
//!     injection, since `grow` has no internal hook to interrupt and one
//!     isn't worth adding for a single scenario. Virtual time is advanced
//!     between the first propose and the crash (the propose-then-crash
//!     lesson, `docs/engineering-lessons.md`: crashing in the same instant
//!     as a propose risks losing an entry that was accepted locally but
//!     never left the leader) so the `RegisterNode` has already replicated
//!     to the surviving majority before the crash. The follow-up
//!     `UpsertMember` — proposed again via `propose_meta`, which finds
//!     whichever NEW leader the two survivors just elected — still commits,
//!     and the registration converges once healed;
//! (e) mirror sync survives a partition: after growing, the new node is
//!     partitioned from **every** control voter (not merely the leader —
//!     see [`run_e_mirror_sync_recovers_from_partition`]'s own doc for why
//!     partitioning only the leader would not actually block anything), a
//!     schema change commits while it's cut off, the partitioned node's own
//!     mirror provably does NOT see it yet, and after [`SimCluster::
//!     heal_all`] the mirror catches up — the long-poll path's own recovery
//!     under fault, not just its happy path.
//!
//! **What stays `ProdEnv`-only** (see `crates/animusd/CLAUDE.md`'s matching
//! entry for the full account): every real-socket bring-up
//! (`--seed ADDR[,ADDR...]` discovery/parsing, `JoinInfo` polling, TCP
//! listener binding), config-file resolution, DNS-based seed addressing
//! (`seed_join_hostname.rs`/`advertise_host.rs`, untouched by this rung),
//! and any admin-HTTP-surface assertion — none of those are reachable from
//! a `SimCluster`-based fixture at all, growth or not.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{MetaCommand, NodeAddrs, NodeStatus, ProposeResult};
use animus_env::{NodeId, nid};

use super::sim_cluster::SimCluster;
use super::sim_cluster_dynamo_table_ops::assert_no_zombie_groups;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn accepted(r: &ProposeResult) -> bool {
    matches!(r, ProposeResult::Accepted { .. })
}

/// A single-hash-key (`pk`, string) `CreateTable`, issued from `node` —
/// mirrors every other `sim_cluster_*` module's identically-named helper.
fn create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

/// `table`'s own tablet id, per `node`'s own view of `Metadata` — mirrors
/// every other `sim_cluster_*` module's identically-named helper.
fn tablet_of(cluster: &SimCluster, node: u64, table: &str) -> animus_tablet::TabletId {
    let meta = cluster.metadata(node);
    *meta
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table {table} has no tablet on node {node}'s own view"))
        .0
}

/// Poll until every node in `0..cluster.node_count()` shows `target`
/// `Active` in its own view of `Metadata::members` — [`SimCluster::grow`]'s
/// own convergence check, exposed here for scenario (d), which drives the
/// same two `MetaCommand`s by hand (with a crash injected between them)
/// rather than through `grow` itself.
fn poll_until_member_active(cluster: &mut SimCluster, target: &NodeId, seed: u64) {
    const BUDGET: Duration = Duration::from_secs(15);
    const STEP: Duration = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    loop {
        if (0..cluster.node_count() as u64).all(|n| {
            cluster
                .metadata(n)
                .members
                .get(target)
                .is_some_and(|m| m.status == NodeStatus::Active)
        }) {
            return;
        }
        assert!(
            elapsed < BUDGET,
            "seed={seed}: {target} never converged Active on every node within {BUDGET:?}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

/// Provision **three** tables (mirroring `sim_cluster_dynamo_table_ops.rs`'s
/// own #715 regression setup, `run_every_node_hosts_exactly_its_replica_
/// set_after_rebalance`) and poll until at least one of their tablets'
/// CURRENT `Metadata` replica set (per node 0's view) names `node`, then
/// return every `(table, tablet)` pair provisioned.
///
/// **Three tables, deliberately not one**: `rebalance_step`'s own
/// convergence bound is max−min ≤ 1 (`animus-placement/CLAUDE.md`) — with
/// only ONE tablet in the whole cluster, "three nodes hold it, one holds
/// none" (loads 1,1,1,0) is ALREADY within that bound, so nothing would
/// ever move; a single-table version of this helper hangs forever waiting
/// for a rebalance the placement engine correctly judges unnecessary (found
/// live building this scenario — see `docs/engineering-lessons.md`'s
/// matching entry). Three tables reproduce the reference regression's own
/// imbalance (loads 3,3,3,0 after the wire's own fixed-RF initial pick,
/// which — deterministic candidate order — lands every one of them on the
/// first three `Active` members) — enough for the balance-driven rebalance
/// to move at least one tablet onto the otherwise-idle fourth (grown) node.
fn provision_soak_tables_and_wait_for_replica(
    cluster: &mut SimCluster,
    node: u64,
    seed: u64,
) -> Vec<(String, animus_tablet::TabletId)> {
    let leader = cluster.control_leader_index() as u64;
    let mut tables = Vec::new();
    for i in 0..3 {
        let table = format!("soak{i}");
        let (status, body) = create_table(cluster, leader, &table);
        assert_eq!(
            status, 200,
            "seed={seed}: CreateTable {table} failed: {body}"
        );
        let tablet = tablet_of(cluster, leader, &table);
        tables.push((table, tablet));
    }

    const BUDGET: Duration = Duration::from_secs(20);
    const STEP: Duration = Duration::from_millis(100);
    let target = nid(node);
    let mut elapsed = Duration::ZERO;
    loop {
        let meta = cluster.metadata(0);
        if tables.iter().any(|(_, t)| {
            meta.tablets
                .get(t)
                .is_some_and(|tab| tab.replicas.contains(&target))
        }) {
            return tables;
        }
        assert!(
            elapsed < BUDGET,
            "seed={seed}: rebalance never placed a replica on node {node} within {BUDGET:?}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

// ---------------------------------------------------------------------------
// Scenario (a): grow converges and serves forwarded ops.
// ---------------------------------------------------------------------------

fn run_a_grow_converges_and_serves_forwarded_ops(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let new_node = cluster.grow("data");
    assert_eq!(
        new_node, 3,
        "seed={seed}: grow's first call must mint node 3"
    );
    assert_eq!(cluster.node_count(), 4, "seed={seed}");

    // The new node's own remote mirror must know about every ORIGINAL
    // member too, not just itself — `SimCluster::grow`'s own convergence
    // poll already waits for this, but assert it explicitly here as the
    // scenario's own documented property.
    let meta = cluster.metadata(new_node);
    for n in 0..3u64 {
        assert!(
            meta.members
                .get(&nid(n))
                .is_some_and(|m| m.status == NodeStatus::Active),
            "seed={seed}: new node's own mirror must know node {n} is Active: {meta:?}"
        );
    }

    // A write/read issued FROM the new node: it hosts no replica of `t` at
    // all (RF 3 over the original 3 nodes), so both genuinely forward over
    // the real `SimRelayClient` wire to whichever node leads the tablet.
    cluster.create_table("t");
    cluster
        .put(new_node, "t", "pk1", "sk1", b"v1")
        .unwrap_or_else(|e| panic!("seed={seed}: put from the new node failed: {e}"));
    let got = cluster
        .get(new_node, "t", "pk1", "sk1", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get from the new node failed: {e}"));
    assert_eq!(got.as_deref(), Some(&b"v1"[..]), "seed={seed}");

    assert_no_zombie_groups(&mut cluster, seed);
}

#[test]
fn a_grow_converges_and_serves_forwarded_ops() {
    run_a_grow_converges_and_serves_forwarded_ops(env_seed(0x6706_0001));
}

#[test]
fn a_grow_converges_and_serves_forwarded_ops_over_seeds() {
    for i in 0..5 {
        run_a_grow_converges_and_serves_forwarded_ops(0x6706_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (b): growth, then CreateTable places and rebalances onto the
// new node.
// ---------------------------------------------------------------------------

fn run_b_growth_then_create_table_places_and_rebalances(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let new_node = cluster.grow("data");

    // See `provision_soak_tables_and_wait_for_replica`'s own doc for why
    // three tables, not one: `node_count (4) > RF (3)` alone isn't enough
    // imbalance to trigger a move under a single tablet.
    provision_soak_tables_and_wait_for_replica(&mut cluster, new_node, seed);

    // The moved-away replica was actually TORN DOWN, not merely duplicated
    // — every node's own hosted set matches Metadata's current replica set
    // exactly.
    assert_no_zombie_groups(&mut cluster, seed);
}

#[test]
fn b_growth_then_create_table_places_and_rebalances() {
    run_b_growth_then_create_table_places_and_rebalances(env_seed(0x6706_0002));
}

#[test]
fn b_growth_then_create_table_places_and_rebalances_over_seeds() {
    for i in 0..5 {
        run_b_growth_then_create_table_places_and_rebalances(0x6706_2000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (c): grow, then drain + remove the same node.
// ---------------------------------------------------------------------------

fn run_c_grow_then_drain_and_remove(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let new_node = cluster.grow("data");

    let tables = provision_soak_tables_and_wait_for_replica(&mut cluster, new_node, seed);

    cluster.drain(new_node);
    cluster.remove(new_node);

    // No node's own `Metadata::members` names it any more.
    for n in 0..cluster.node_count() as u64 {
        assert!(
            !cluster.metadata(n).members.contains_key(&nid(new_node)),
            "seed={seed}: node {n} still lists the removed node {new_node}"
        );
    }

    // Every one of its replicas re-homed onto the three original survivors
    // — no tablet names it at all any more, and every tablet stays at RF 3.
    let meta = cluster.metadata(0);
    for (table, tablet) in &tables {
        let t = meta
            .tablets
            .get(tablet)
            .unwrap_or_else(|| panic!("table {table} tablet must still exist"));
        assert!(
            !t.replicas.contains(&nid(new_node)),
            "seed={seed}: removed node {new_node} still a replica of {table}: {t:?}"
        );
        assert_eq!(
            t.replicas.len(),
            3,
            "seed={seed}: {table}'s replica count must stay at RF 3 post-removal: {t:?}"
        );
    }

    // No zombie group anywhere, on any node — including the removed one's
    // own (still-running, still-heartbeating, but now inert) tasks.
    assert_no_zombie_groups(&mut cluster, seed);

    // Ids are never reused: a later grow mints a strictly higher index.
    let next = cluster.grow("data");
    assert!(
        next > new_node,
        "seed={seed}: a removed node's id must never be reused (removed={new_node} next={next})"
    );
}

#[test]
fn c_grow_then_drain_and_remove() {
    run_c_grow_then_drain_and_remove(env_seed(0x6706_0003));
}

#[test]
fn c_grow_then_drain_and_remove_over_seeds() {
    for i in 0..5 {
        run_c_grow_then_drain_and_remove(0x6706_3000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (d): the control leader crashes between grow's own two
// registration proposes.
// ---------------------------------------------------------------------------

fn run_d_leader_crash_mid_registration_converges(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let leader = cluster.control_leader_index() as u64;

    // The exact two `MetaCommand`s `SimCluster::grow` itself proposes,
    // driven by hand via `propose_meta` (which re-resolves the CURRENT
    // leader fresh on every call) so a crash can be injected between them.
    let new_id = nid(3);
    let addr = new_id.to_string();
    let addrs = NodeAddrs {
        internal: addr.clone(),
        client: addr.clone(),
        intra: addr.clone(),
        admin: addr,
        role: "data".to_owned(),
    };
    let outcome = cluster.propose_meta(MetaCommand::RegisterNode {
        node: new_id.clone(),
        addrs,
        labels: BTreeMap::new(),
    });
    assert!(
        accepted(&outcome),
        "seed={seed}: RegisterNode rejected: {outcome:?}"
    );

    // Advance virtual time BEFORE crashing (the propose-then-crash lesson,
    // docs/engineering-lessons.md): crashing in the same instant as the
    // propose risks losing an entry that was accepted on the leader's own
    // log but never replicated anywhere else. This gives it a real window
    // to reach the surviving majority first.
    cluster.run_for(Duration::from_millis(300));
    cluster.crash(leader);

    // The follow-up propose re-resolves the leader fresh — with the old
    // leader crashed, this must find whichever NEW leader the two
    // survivors just elected, and still commit.
    let outcome2 = cluster.propose_meta(MetaCommand::UpsertMember {
        node: new_id.clone(),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    });
    assert!(
        accepted(&outcome2),
        "seed={seed}: UpsertMember rejected (no new leader took over?): {outcome2:?}"
    );

    cluster.heal_all();
    poll_until_member_active(&mut cluster, &new_id, seed);
}

#[test]
fn d_leader_crash_mid_registration_converges() {
    run_d_leader_crash_mid_registration_converges(env_seed(0x6706_0004));
}

#[test]
fn d_leader_crash_mid_registration_converges_over_seeds() {
    for i in 0..5 {
        run_d_leader_crash_mid_registration_converges(0x6706_4000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (e): mirror sync survives a partition.
// ---------------------------------------------------------------------------

/// Partitions the new node from **every** control voter, not merely the
/// leader — `RemoteControlClient`'s own `seeds` list is the WHOLE pre-growth
/// control voter set (`SimCluster::grow`'s own doc), and its long-poll loop
/// falls through every seed in turn on a failed hop, not just the current
/// leader hint. Partitioning only the leader would leave two other
/// reachable seeds to sync from — a real property of the production
/// fallback logic, not a gap this scenario should paper over by picking an
/// easier target.
fn run_e_mirror_sync_recovers_from_partition(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let new_node = cluster.grow("data");

    for voter in 0..3u64 {
        cluster.partition(new_node, voter);
    }

    let (status, body) = create_table(&mut cluster, 0, "orders");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    // While partitioned, the new node's own mirror must not observe a
    // schema change it was cut off from — a generous window to (fail to)
    // sync before asserting the negative.
    cluster.run_for(Duration::from_secs(5));
    assert!(
        !cluster.metadata(new_node).has_table_tablet("orders"),
        "seed={seed}: the partitioned node's mirror must not see a schema \
         change it was cut off from"
    );

    cluster.heal_all();

    const BUDGET: Duration = Duration::from_secs(15);
    const STEP: Duration = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    loop {
        if cluster.metadata(new_node).has_table_tablet("orders") {
            break;
        }
        assert!(
            elapsed < BUDGET,
            "seed={seed}: the partitioned node's mirror never caught up within \
             {BUDGET:?} of healing"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

#[test]
fn e_mirror_sync_recovers_from_partition() {
    run_e_mirror_sync_recovers_from_partition(env_seed(0x6706_0005));
}

#[test]
fn e_mirror_sync_recovers_from_partition_over_seeds() {
    for i in 0..5 {
        run_e_mirror_sync_recovers_from_partition(0x6706_5000 + i);
    }
}
