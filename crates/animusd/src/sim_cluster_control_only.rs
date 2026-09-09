//! `SimCluster`-driven deterministic coverage for **control-only nodes**
//! (ADR 0061 rung L, C-12 PR 2 — the mechanism PR: per-node roles under
//! `SimCluster` and role-aware `restart`). Production has carried the whole
//! control/data role split since ADR 0035 (`NodeRole::{Control,Data,Both}`,
//! `crates/animusd/src/config.rs:74`) — this rung only extends `SimCluster`,
//! this fixture's own multi-node `SimEnv` harness, to build and exercise a
//! **control-only** node (`data: None`, no reconciler, no per-tablet
//! always-on loop) the way it has always been able to build a combined one.
//!
//! **What's new in `sim_cluster.rs`, for this PR**: [`SimCluster::
//! new_with_roles`]/[`SimCluster::new_with_roles_and_segment_janitor_
//! retention`] (a `NodeRole`-per-index sibling of [`SimCluster::new`],
//! which becomes a thin `vec![NodeRole::Both; nodes]` wrapper over it — no
//! behavior change for any existing all-combined caller); a `roles:
//! Vec<NodeRole>` field tracking each node's own shape (populated by both
//! constructors and by [`SimCluster::grow`]); [`SimCluster::role_of`] (the
//! test-reachable accessor); and role-aware [`SimCluster::restart`] — a
//! `NodeRole::Control` node comes back control-only, mirroring
//! `BoundControlNode::start_control_with`'s production shape exactly rather
//! than [`SimCluster::restart`]'s old unconditional "always rebuild
//! combined" behavior. `SimCluster::crash` needed no change at all (already
//! role-agnostic — see its own doc).
//!
//! **A control-only node here mirrors `BoundControlNode::
//! start_control_with` (`crates/animusd/src/lib.rs:6421`) as closely as
//! this fixture's own existing per-node loop set allows**: `data: None`,
//! and — among the loops [`SimCluster::new`]/`restart` already spawn for
//! every node — no `host::Reconciler` and no `ttl_reaper_loop` (both
//! genuinely data-role-dependent: nothing to host, no engine to scan),
//! while `backup_janitor_loop`/`segment_janitor_loop`/`index_backfill_loop`
//! (control-plane-leader-gated, not data-role-gated) and the control
//! `RaftNode` itself still run exactly as they do on a combined node —
//! production spawns all three of those on a control-only node too (W-10,
//! ADR 0043 §A9's control-only-leader gap, closed — see `start_control_
//! with`'s own doc). The fixture's own `heartbeat_loop` (a `SimCluster`-only
//! mechanism keeping a node's own `Metadata::members` row `Active` — see
//! `SimCluster::seed_members`'s own doc) is skipped for the identical
//! reason production's doc gives for skipping it: "this node has no raftkv
//! env to sync or heartbeat from." A control-only node's own id is never
//! registered into `Metadata::members` at all (`seed_members`'s
//! `RegisterNode` uses `role: "control"`, whose `claims_membership` gate
//! never inserts — `animus-control/src/meta.rs`), so it can never be
//! selected as a tablet replica candidate — confirmed directly by scenario
//! (a) below, not just asserted.
//!
//! **Routing: what production does for a client op "at" a control-only
//! node, and why this fixture needed no new code for it.**
//! `ClientCtx::resolve_cp_route` (`crates/animusd/src/forwarding.rs:162`)
//! already treats `self.data == None` as the limit case of "hosts no local
//! replica of the tablet" — its own comment (`forwarding.rs:203-209`)
//! states this in as many words: "A control-only node ... never hosts a
//! local handle at all ... this is the 'zero new rejection code' degrade
//! path: a control node is just the limit case of 'hosts nothing,' handled
//! by the same logic every other non-replica node already goes through."
//! Neither `write_path.rs`'s `cp_kind_write_raw`/`cp_kind_write_item` nor
//! `read_path.rs`'s `cp_get`/`cp_scan` call `ClientCtx::data()` at all — the
//! `ctx.data()` panic (`ADR 0035 PR3`) only guards genuinely data-role-only
//! bookkeeping (`raftkv_metrics`/`request_rates`, `dynamo::
//! dispatch_item_op`'s own hot path — this fixture's own plain `put`/`get`/
//! `delete`/`scan` driver methods never reach that dispatcher, going
//! through `cp_kind_write_raw`/`cp_get` directly instead). So a
//! `SimCluster` op issued "at" a control-only node index just **forwards**
//! — over the real `SimRelayClient` wire, exactly like any other
//! non-replica-hosting node's op already does in every other `sim_cluster_
//! *` module — with no fixture change needed to make that work; scenario
//! (a) below proves it concretely (a put/get issued from a control-only
//! node index round-trips).
//!
//! **The DynamoDB wire item-op path is a different story, and this is
//! not a gap — it mirrors production exactly.** `dynamo::dispatch_item_op`
//! (`PutItem`/`GetItem`/etc., reached via `SimCluster::dynamo`) calls
//! `ctx.data()` unconditionally on the RECEIVING node's own `ClientCtx`,
//! before ever deciding whether to serve locally or forward — a
//! control-only node's own `data()` panics there (ADR 0035 PR3's guard).
//! This is exactly why production never binds a dynamo listener on a
//! control-only node at all (ADR 0057, `BoundControlNode::
//! start_control_with`'s own `dynamo_addr: None`): there is no listener to
//! even dial, so the question "does it forward or reject a DynamoDB
//! request" never arises in production — the connection itself cannot be
//! made. `SimCluster::dynamo` has no per-node "is the dynamo listener even
//! bound" concept (it drives a node's `ClientCtx` directly, mirroring
//! `admin.rs::action_data_dynamo`'s own unauthenticated proxy), so this
//! fixture can still issue a `SimCluster::dynamo` call at a control-only
//! node's index — and it panics, faithfully reproducing what would happen
//! if that unreachable-in-production connection were ever forced. Every
//! scenario below that needs a DynamoDB item op issues it from a
//! `NodeRole::Both` node's index for exactly this reason (DDL — `dynamo::
//! dispatch_table_op` — never touches `ctx.data()` at all, so a `CreateTable`
//! issued through a control-only node's own control-plane-leader seat is
//! fine and used deliberately in scenarios (c)/(d) below).
//!
//! ## Scenarios (seed-parameterized, `_over_seeds` at 5 seeds each)
//!
//! (a) [`run_a_mixed_cluster_boots_elects_and_serves_forwarded_and_local_ops`]
//!     — a 3-control-only + 2-combined (5-node) cluster boots, elects a
//!     control leader, creates a table over the real wire (every replica
//!     the placement path picked is confirmed data-capable — a control-only
//!     node is never a placement candidate), and a write/read round-trips
//!     both through a combined (hosting) node and through a control-only
//!     node (forwarded).
//! (b) [`run_b_restart_of_a_control_only_node_preserves_metadata`] — a
//!     control-only node's own `restart` rebuilds a fresh, empty-log
//!     control `RaftNode` (this fixture's `MemoryEngine`-only design, no
//!     local WAL to replay) that still catches up to the surviving
//!     control quorum and shows a pre-existing table; a fresh `CreateTable`
//!     issued **through** the just-restarted node also succeeds, proving
//!     its own propose/relay path is live again, not just its read side.
//! (c) [`run_c_crash_of_a_control_only_control_leader_still_elects_and_serves_ddl`]
//!     — forces (deterministically, via a bounded crash/heal loop) a
//!     control-only node into the control-leader seat, crashes it there,
//!     and confirms a genuinely different node is elected and a subsequent
//!     `CreateTable` through the new leader succeeds.
//! (d) [`run_d_control_only_node_serves_no_data_plane_loops`] — every
//!     control-only node's own `hosted_tablets` stays empty for the whole
//!     scenario, before and after a table is created and written to
//!     (unlike every combined node, at least one of which does host it) —
//!     the observable proof that no reconciler ever ran on it.
//! (e) [`run_e_restart_of_a_combined_node_in_a_mixed_cluster_still_respawns_its_ttl_loop`]
//!     — a single-combined-node (+3-control-only) cluster's one data-
//!     capable node is restarted; an item with an already-expired TTL
//!     attribute, written only *after* the restart, is still reaped by the
//!     always-on loop — proving `SimCluster::restart` actually respawns
//!     `ttl_reaper_loop` for a combined node in a mixed cluster, not merely
//!     that pre-restart activity happened to already reap it (the
//!     `sim_cluster_ttl.rs` module doc's own "every `SimCluster::dynamo`
//!     call burns a whole `OP_BUDGET`, dozens of sweep opportunities"
//!     caveat, deliberately avoided here by writing the expired item only
//!     once the fresh, post-restart loop is the only one that could have
//!     ever seen it).

use std::time::Duration;

use animus_env::nid;

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{
    create_table_via_wire, env_seed, get_item_via_wire, put_item_via_wire, tablet_of_table,
};
use crate::config::NodeRole;

/// A 5-node cluster: 2 combined (`NodeRole::Both`, indices 0-1) + 3
/// control-only (`NodeRole::Control`, indices 2-4). Replication factor 2 —
/// small enough that the wire path's own placement always lands both
/// replicas on the two data-capable nodes (never more members exist than
/// that to pick from).
fn new_mixed_cluster(seed: u64) -> SimCluster {
    let roles = [
        NodeRole::Both,
        NodeRole::Both,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
    ];
    SimCluster::new_with_roles(seed, &roles, 2)
}

/// A single-hash-key (`pk`, string) `CreateTable`, issued from `node` —
/// mirrors every other `sim_cluster_*` module's identically-named helper.
fn create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    create_table_via_wire(cluster, node, &body)
}

/// Converged-or-timeout poll on `cond(cluster)` — the shared shape every
/// `sim_cluster_*` module's own scenario-local convergence checks use
/// (`sim_cluster_growth.rs::poll_until_member_active`'s identical idiom),
/// duplicated here rather than reached into a sibling module's private
/// helper (this crate's own "small fixtures duplicated per test module"
/// convention).
fn poll_until(
    cluster: &mut SimCluster,
    budget: Duration,
    seed: u64,
    what: &str,
    mut cond: impl FnMut(&mut SimCluster) -> bool,
) {
    const STEP: Duration = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    loop {
        if cond(cluster) {
            return;
        }
        assert!(
            elapsed < budget,
            "seed={seed}: {what} never converged within {budget:?}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

/// Bounded converged-or-timeout poll for the always-on TTL reaper — mirrors
/// `sim_cluster_ttl.rs::poll_until_reaped`'s own shape (duplicated per this
/// crate's own "small fixtures duplicated per test module" convention,
/// since that one is a private free function, not `pub(crate)`).
fn poll_until_reaped(cluster: &mut SimCluster, node: u64, get_body: &str, seed: u64) {
    const ATTEMPTS: usize = 20;
    const STEP: Duration = Duration::from_millis(400);
    let mut last = String::new();
    for _ in 0..ATTEMPTS {
        let (status, body) = get_item_via_wire(cluster, node, get_body);
        if status == 200 && !body.contains("\"Item\"") {
            return;
        }
        last = format!("status={status} body={body}");
        cluster.run_for(STEP);
    }
    panic!(
        "seed={seed}: item was never reaped by the always-on TTL loop within {ATTEMPTS} \
         attempts (last={last})"
    );
}

/// Force a control-only node into the control-leader seat, deterministically
/// for a given seed: while the current leader is data-capable, crash it,
/// give the survivors enough virtual time to notice the missing heartbeats
/// and elect someone else, then heal it back in (it rejoins as an ordinary,
/// non-leading voter — a healed-but-stale-term node never reclaims
/// leadership for free, Raft's own higher-term step-down rule) and give it
/// a further window to actually step down and re-sync before the next
/// round's own leader check. Bounded by the cluster's own node count, since
/// each crash/heal round changes who currently leads.
///
/// **Both `run_for` windows are load-bearing, not padding.**
/// `RaftNode::is_leader()` is a purely local belief a crashed (muted) node
/// never updates on its own (`crash`/`Simulator::crash` only mutes the
/// network, it never touches Raft role state) — `control_leader_index()`'s
/// own `position`-based scan can find a just-crashed node's still-stale
/// `is_leader() == true` again immediately, with no time yet having passed
/// for the survivors to even notice the missing heartbeats: without the
/// FIRST `run_for` (before `heal_all`), a genuinely new election never even
/// starts. Once healed, the old leader's own stale belief only clears once
/// it actually RECEIVES the new leader's higher-term traffic — without the
/// SECOND `run_for` (after `heal_all`), the next round's `control_leader_
/// index()` call can still find the just-healed former leader's own
/// pre-crash belief, since nothing forced it to process that traffic yet.
fn ensure_control_only_leads(cluster: &mut SimCluster, seed: u64) -> u64 {
    const MAX_ROUNDS: usize = 10;
    const SETTLE: Duration = Duration::from_secs(2);
    for _ in 0..MAX_ROUNDS {
        let leader = cluster.control_leader_index() as u64;
        if cluster.role_of(leader) == NodeRole::Control {
            return leader;
        }
        cluster.crash(leader);
        cluster.run_for(SETTLE);
        cluster.heal_all();
        cluster.run_for(SETTLE);
    }
    panic!(
        "seed={seed}: a control-only node never became the control leader within \
         {MAX_ROUNDS} forced re-elections"
    );
}

// ---------------------------------------------------------------------------
// (a) a mixed cluster boots, elects, and serves both local and forwarded ops
// ---------------------------------------------------------------------------

fn run_a_mixed_cluster_boots_elects_and_serves_forwarded_and_local_ops(seed: u64) {
    let mut cluster = new_mixed_cluster(seed);

    for n in 0..2u64 {
        assert_eq!(
            cluster.role_of(n),
            NodeRole::Both,
            "seed={seed}: node {n} must be combined"
        );
    }
    for n in 2..5u64 {
        assert_eq!(
            cluster.role_of(n),
            NodeRole::Control,
            "seed={seed}: node {n} must be control-only"
        );
    }

    // The control group itself elects across all 5 voters (control-only
    // nodes included) before anything else — `control_leader_index` is
    // already a converged-or-timeout wait.
    let leader = cluster.control_leader_index() as u64;

    let (status, body) = create_table(&mut cluster, leader, "mixed");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    // Every replica the real wire placement path picked must be
    // data-capable — a control-only node is never `Active` in
    // `Metadata::members` (`SimCluster::seed_members`'s `RegisterNode`
    // uses `role: "control"`, whose `claims_membership` gate never
    // inserts), so it can never be a placement candidate at all.
    let tablet = tablet_of_table(&cluster, "mixed");
    let meta = cluster.metadata(0);
    let replicas = meta
        .tablets
        .get(&tablet)
        .unwrap_or_else(|| panic!("seed={seed}: tablet {} missing from Metadata", tablet.0))
        .replicas
        .clone();
    assert!(!replicas.is_empty(), "seed={seed}: table has no replicas");
    for n in 0..cluster.node_count() as u64 {
        if replicas.contains(&nid(n)) {
            assert_eq!(
                cluster.role_of(n),
                NodeRole::Both,
                "seed={seed}: replica node {n} must be data-capable"
            );
        }
    }

    // A write/read through a combined (hosting) node round-trips.
    cluster
        .put(0, "mixed", "pk1", "sk1", b"v1")
        .unwrap_or_else(|e| panic!("seed={seed}: put via the combined node failed: {e}"));
    let got = cluster
        .get(0, "mixed", "pk1", "sk1", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get via the combined node failed: {e}"));
    assert_eq!(got.as_deref(), Some(&b"v1"[..]), "seed={seed}");

    // A write/read issued FROM a control-only node: it hosts no replica of
    // anything at all, so both genuinely forward over the real
    // `SimRelayClient` wire to whichever combined node leads the tablet
    // (`ClientCtx::resolve_cp_route`'s "hosts nothing" degrade path).
    cluster
        .put(2, "mixed", "pk2", "sk2", b"v2")
        .unwrap_or_else(|e| panic!("seed={seed}: put via a control-only node failed: {e}"));
    let got2 = cluster
        .get(2, "mixed", "pk2", "sk2", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get via a control-only node failed: {e}"));
    assert_eq!(got2.as_deref(), Some(&b"v2"[..]), "seed={seed}");
}

#[test]
fn a_mixed_cluster_boots_elects_and_serves_forwarded_and_local_ops() {
    run_a_mixed_cluster_boots_elects_and_serves_forwarded_and_local_ops(env_seed(0x6C12_0001));
}

#[test]
fn a_mixed_cluster_boots_elects_and_serves_forwarded_and_local_ops_over_seeds() {
    for i in 0..5 {
        run_a_mixed_cluster_boots_elects_and_serves_forwarded_and_local_ops(0x6C12_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// (b) restart of a control-only node preserves metadata and re-forms the
//     control quorum
// ---------------------------------------------------------------------------

fn run_b_restart_of_a_control_only_node_preserves_metadata(seed: u64) {
    let mut cluster = new_mixed_cluster(seed);
    let leader = cluster.control_leader_index() as u64;

    let (status, body) = create_table(&mut cluster, leader, "b");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let target = 3u64;
    assert_eq!(
        cluster.role_of(target),
        NodeRole::Control,
        "seed={seed}: node {target} must be control-only"
    );

    cluster.restart(target);

    // The restarted node's own control `RaftNode` is genuinely fresh
    // (`MemoryEngine::new()`, no local durable log to replay) — its own
    // view of `Metadata` catching up to show table `b` proves the control
    // quorum actually re-formed around it via ordinary peer replication /
    // `InstallSnapshot`, not local recovery.
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        &format!("node {target}'s own Metadata catching up on table `b`"),
        |c| c.metadata(target).has_table_tablet("b"),
    );

    // A fresh `CreateTable` issued THROUGH the just-restarted node must
    // still succeed — its own propose/relay path is live again, not just
    // its read side.
    let (status2, body2) = create_table(&mut cluster, target, "b2");
    assert_eq!(
        status2, 200,
        "seed={seed}: CreateTable via the restarted control-only node failed: {body2}"
    );
}

#[test]
fn b_restart_of_a_control_only_node_preserves_metadata() {
    run_b_restart_of_a_control_only_node_preserves_metadata(env_seed(0x6C12_0002));
}

#[test]
fn b_restart_of_a_control_only_node_preserves_metadata_over_seeds() {
    for i in 0..5 {
        run_b_restart_of_a_control_only_node_preserves_metadata(0x6C12_2000 + i);
    }
}

// ---------------------------------------------------------------------------
// (c) crash of a control-only control leader still elects and serves DDL
// ---------------------------------------------------------------------------

fn run_c_crash_of_a_control_only_control_leader_still_elects_and_serves_ddl(seed: u64) {
    let mut cluster = new_mixed_cluster(seed);
    let leader = ensure_control_only_leads(&mut cluster, seed);
    assert_eq!(
        cluster.role_of(leader),
        NodeRole::Control,
        "seed={seed}: forced leader must be control-only"
    );

    cluster.crash(leader);
    cluster.run_for(Duration::from_secs(2));

    // Do NOT resolve the new leader via `control_leader_index()` here: a
    // `crash`ed node's own `is_leader()` belief is a purely local flag it
    // never updates while muted (`ensure_control_only_leads`'s own doc) —
    // since `leader` is never healed for the rest of this test, that stale
    // belief persists forever, and `control_leader_index()`'s `position`
    // scan would find it again ahead of the genuine new leader whenever
    // `leader`'s own index happens to sort first. Instead, issue the
    // follow-up `CreateTable` from a guaranteed SURVIVOR node — DDL relays
    // to whichever node genuinely leads, so this proves both "a new leader
    // was elected somewhere" and "DDL still works" without ever needing to
    // name the new leader's own index.
    let survivor = (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a multi-node cluster always has a survivor");
    let (status, body) = create_table(&mut cluster, survivor, "c");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable after the control-only leader's crash failed: {body}"
    );
}

#[test]
fn c_crash_of_a_control_only_control_leader_still_elects_and_serves_ddl() {
    run_c_crash_of_a_control_only_control_leader_still_elects_and_serves_ddl(env_seed(0x6C12_0003));
}

#[test]
fn c_crash_of_a_control_only_control_leader_still_elects_and_serves_ddl_over_seeds() {
    for i in 0..5 {
        run_c_crash_of_a_control_only_control_leader_still_elects_and_serves_ddl(0x6C12_3000 + i);
    }
}

// ---------------------------------------------------------------------------
// (d) a control-only node serves no data-plane loops
// ---------------------------------------------------------------------------

fn run_d_control_only_node_serves_no_data_plane_loops(seed: u64) {
    let mut cluster = new_mixed_cluster(seed);

    // Before any table exists at all: no CP group is registered on any
    // node yet, but a control-only node's own `data: None` is already
    // pinned directly, the second observable this scenario checks.
    for n in 2..5u64 {
        assert!(
            cluster.hosted_tablets(n).is_empty(),
            "seed={seed}: control-only node {n} hosts {:?} before any table exists",
            cluster.hosted_tablets(n)
        );
    }

    // DDL (`CreateTable`) is fine to issue through whichever node currently
    // leads the control group, control-only included — `dispatch_table_op`
    // never touches `ctx.data()` (confirmed directly: it reaches only
    // `Metadata`-level schema/tablet proposes). A DynamoDB item op
    // (`PutItem`) is a different story: `dynamo::dispatch_item_op` calls
    // `ctx.data()` unconditionally, on its own node's `ClientCtx`, before
    // ever deciding whether to serve locally or forward — a control-only
    // node's own `ClientCtx::data()` panics (ADR 0035 PR3's guard), the
    // exact reason production never binds a dynamo listener on this role
    // at all (see this module's own doc). So the item op below is issued
    // from a fixed COMBINED node (0), never from `leader` (which may be
    // control-only) — this is the fixture-level mirror of "you cannot even
    // dial a control-only node's dynamo port in production."
    let leader = cluster.control_leader_index() as u64;
    let (status, body) = create_table(&mut cluster, leader, "d");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let (status2, body2) = put_item_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"d","Item":{"pk":{"S":"a"}}}"#,
    );
    assert_eq!(status2, 200, "seed={seed}: PutItem failed: {body2}");

    // Wait for real hosting to converge on whichever combined node(s) the
    // wire path actually picked (this fixture's real reconciler, not a
    // hand-hosted bookkeeping shortcut).
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        "some combined node hosting table `d`'s tablet",
        |c| (0..2u64).any(|n| !c.hosted_tablets(n).is_empty()),
    );

    for n in 2..5u64 {
        assert!(
            cluster.hosted_tablets(n).is_empty(),
            "seed={seed}: control-only node {n} must never host a tablet, hosts {:?}",
            cluster.hosted_tablets(n)
        );
    }
}

#[test]
fn d_control_only_node_serves_no_data_plane_loops() {
    run_d_control_only_node_serves_no_data_plane_loops(env_seed(0x6C12_0004));
}

#[test]
fn d_control_only_node_serves_no_data_plane_loops_over_seeds() {
    for i in 0..5 {
        run_d_control_only_node_serves_no_data_plane_loops(0x6C12_4000 + i);
    }
}

// ---------------------------------------------------------------------------
// (e) restart of a combined node in a mixed cluster still respawns its
//     data loops (TTL reaper)
// ---------------------------------------------------------------------------

fn run_e_restart_of_a_combined_node_in_a_mixed_cluster_still_respawns_its_ttl_loop(seed: u64) {
    // A single combined node (index 0) + 3 control-only, RF 1 — the
    // combined node is unambiguously both the table's sole replica and
    // this scenario's own restart target.
    let roles = [
        NodeRole::Both,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 1);

    let leader = cluster.control_leader_index() as u64;
    let (status, body) = create_table_via_wire(
        &mut cluster,
        leader,
        r#"{"TableName":"e",
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let (status2, body2) = cluster.dynamo(
        0,
        "DynamoDB_20120810.UpdateTimeToLive",
        br#"{"TableName":"e",
             "TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}"#,
    );
    assert_eq!(
        status2, 200,
        "seed={seed}: UpdateTimeToLive(enable) failed: {body2}"
    );

    // Restart the ONLY combined node — `Simulator::stop` drops its own
    // `ttl_reaper_loop` task along with everything else it owned;
    // `SimCluster::restart` must respawn it (this rung's own "every
    // always-on loop a role owns must be respawned" requirement) for the
    // item written below to ever be reaped at all.
    cluster.restart(0);

    // Wait for the single-voter tablet group to re-host/re-elect on the
    // just-restarted node before writing through it.
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        "node 0 re-hosting table `e`'s tablet after restart",
        |c| !c.hosted_tablets(0).is_empty(),
    );

    // Written only AFTER the restart: if this ever gets reaped, it can
    // only be the fresh, post-restart `ttl_reaper_loop` that did it — the
    // `sim_cluster_ttl.rs` module doc's own "a single `SimCluster::dynamo`
    // call already burns a whole `OP_BUDGET`, dozens of sweep
    // opportunities" caveat is exactly why writing it any earlier would
    // not prove this.
    let past = cluster.wall_now_secs(0).saturating_sub(5);
    let (status3, body3) = put_item_via_wire(
        &mut cluster,
        0,
        &format!(r#"{{"TableName":"e","Item":{{"id":{{"S":"a"}},"expiresAt":{{"N":"{past}"}}}}}}"#),
    );
    assert_eq!(status3, 200, "seed={seed}: PutItem failed: {body3}");

    let get_body = r#"{"ConsistentRead":true,"TableName":"e","Key":{"id":{"S":"a"}}}"#;
    poll_until_reaped(&mut cluster, 0, get_body, seed);
}

#[test]
fn e_restart_of_a_combined_node_in_a_mixed_cluster_still_respawns_its_ttl_loop() {
    run_e_restart_of_a_combined_node_in_a_mixed_cluster_still_respawns_its_ttl_loop(env_seed(
        0x6C12_0005,
    ));
}

#[test]
fn e_restart_of_a_combined_node_in_a_mixed_cluster_still_respawns_its_ttl_loop_over_seeds() {
    for i in 0..5 {
        run_e_restart_of_a_combined_node_in_a_mixed_cluster_still_respawns_its_ttl_loop(
            0x6C12_5000 + i,
        );
    }
}
