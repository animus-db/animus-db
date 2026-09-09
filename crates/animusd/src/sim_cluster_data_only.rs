//! `SimCluster`-driven deterministic coverage for **data-only nodes** (ADR
//! 0061 rung L, C-12 PR 3 — the mechanism PR: `NodeRole::Data` first-class
//! at construction, and role-aware `crash`/`restart` for a data-only node
//! whether constructed or grown). `sim_cluster_control_only.rs` (C-12 PR 2)
//! did the identical thing for control-only nodes; this module is its
//! data-only sibling, sharing that module's own conventions
//! (`poll_until`'s converged-or-timeout shape, `_over_seeds` at 5 seeds).
//!
//! **What's new in `sim_cluster.rs`, for this PR.** Before this PR,
//! [`SimCluster::new_with_roles`] rejected any `NodeRole::Data` entry
//! outright — [`SimCluster::grow`]`("data")` was the only way to add a
//! data-only node, and only ever *after* construction. This PR:
//!
//! - Lets `roles` carry `NodeRole::Data` entries, **control-prefixed**:
//!   every `Control`/`Both` entry before every `Data` one, so
//!   `self.controls` (one local `RaftNode<SimEnv>` per control-bearing
//!   node) stays a contiguous `0..control_count` prefix — the same shape
//!   `SimCluster::grow`'s own append-only `NodeRole::Data` node already
//!   established, generalized to construction time. At least one
//!   control-bearing node is still required.
//! - Builds each `NodeRole::Data` node exactly like `SimCluster::grow`
//!   already builds its own grown one: `ControlHandle::Remote(
//!   RemoteControlClient::new(control_seeds, ..))` dialing the cluster's
//!   own control-bearing node ids, a real per-node `host::Reconciler`, a
//!   `heartbeat_loop` targeting the control-bearing prefix (keeping its own
//!   `Metadata::members` row `Active`), a `ttl_reaper_loop`, and the one
//!   genuinely new-to-this-fixture mechanism `SimCluster::grow` introduced —
//!   `spawn_remote_mirror_sync_loop`, a `SimEnv`-native long-poll mirror
//!   driving `ClientRequest::WatchMetadata`/`ClientResponse::Status|
//!   MetadataDelta` over `SimRelayClient`. Deliberately **not** spawned:
//!   `backup_janitor_loop`/`segment_janitor_loop`/`index_backfill_loop` —
//!   all three are control-plane-leader-only (never data-role-gated, W-10/
//!   ADR 0043 §A9), and a `NodeRole::Data` node's `ControlHandle::Remote`
//!   can structurally never become control-plane leader; verified against
//!   production directly (`BoundDataNode::start_data_with_growth`,
//!   `crates/animusd/src/lib.rs`, spawns none of the three either) —
//!   **not** the plan's own original phrasing ("TTL reaper, index
//!   backfill, janitors" all respawn on restart), which turned out not to
//!   match ground truth; see this module's own restart/crash entry below
//!   for the correction.
//! - **Fixed the identical gap in `SimCluster::grow` itself while factoring
//!   the shared construction logic** (this PR's own required "reuse
//!   `grow`'s path, factor the shared code out" instruction, applied in
//!   both directions): `grow` used to spawn `backup_janitor_loop`
//!   unconditionally on its own grown node (a harmless, but production-
//!   inaccurate, permanent no-op — `control_leader()` on a `Remote` handle
//!   always answers `None`) and never spawned `ttl_reaper_loop` at all (a
//!   real, previously-latent gap — production spawns one on every
//!   data-only node). Both are now fixed: `grow` no longer spawns the
//!   janitor, and does spawn the TTL reaper, matching `new_with_roles`'s
//!   own `NodeRole::Data` construction exactly. `grow`'s own `segment_
//!   store` field was also switched from an inert per-node `Fs` placeholder
//!   to the SAME shared `SimSegmentStore` every other node's own `ClientCtx
//!   ::segment_store` wraps (ADR 0061 rung G, C-07 PR 2) — a grown node's
//!   own stream-segment reads/writes now see every other node's writes too.
//! - Role-aware [`SimCluster::restart`] now dispatches purely on `node <
//!   self.controls.len()` (the control-bearing prefix's own length) instead
//!   of panicking above that boundary: a data-only node — constructed or
//!   grown, the two are indistinguishable to this method — is rebuilt with
//!   a fresh `ControlHandle::Remote`, a fresh `spawn_remote_mirror_sync_
//!   loop`, and every data-role loop it actually owns (reconciler,
//!   `heartbeat_loop`, `ttl_reaper_loop`) — never the three control-plane-
//!   leader-only janitors, for the identical reason construction skips
//!   them. `SimCluster::restart`'s own control-bearing branch ALSO gained a
//!   real, previously-latent bug fix along the way: its fresh `RaftNode`'s
//!   own membership is now `control_ids` (the real control voter set), not
//!   `0..self.nodes` — the old `all_ids` binding, harmless only as long as
//!   every node ran the control plane, which is no longer guaranteed the
//!   instant any `NodeRole::Data` node exists (at construction, since this
//!   PR, or via a pre-existing `grow` call).
//! - `SimCluster::crash` needed **no** change at all — it was already fully
//!   role-agnostic (`self.sim.crash(nid(node))` mutes the network for that
//!   node id regardless of what tasks run on it, or whether it's even
//!   within `self.controls`' own bounds), confirmed by scenario (c) below
//!   using it directly on a data-only node's index with no fixture change.
//!   A crashed data-only node's own mirror-sync loop keeps running, just
//!   muted (every outbound `relay()` call times out and the loop simply
//!   retries) — the identical "tasks stay alive but can't communicate"
//!   contract every other always-on loop already has under `crash`, and
//!   `Drop for SimCluster`/`Simulator::shutdown` already tears down every
//!   node's own tasks uniformly regardless of role (confirmed by direct
//!   inspection — no new `Weak`-handle/Drop-coverage extension was needed,
//!   the identical finding this fixture's own C-07 PR 2 appendix already
//!   recorded for this same loop when `SimCluster::grow` first introduced
//!   it).
//!
//! **Routing at a data-only node needed no new code — the identical finding
//! `sim_cluster_control_only.rs`'s own doc already recorded for a
//! control-only node, extended here.** `ClientCtx::resolve_cp_route`
//! (`crates/animusd/src/forwarding.rs:162`) reads only `self.edge` (this
//! node's own locally-registered replica handles) and, on the fallback
//! path, `Metadata` via `self.effective_metadata()` — neither depends on
//! whether `self.control` is `Local` or `Remote`. A data-only node hosting
//! a replica serves locally; one hosting none forwards over the real
//! `SimRelayClient` wire to whichever node currently leads, exactly like
//! any other non-replica-hosting node's op in every other `sim_cluster_*`
//! module. Scenario (a) below proves both directions concretely (a write/
//! read issued from a data-only node that DOES host the tablet, and this
//! module's crash scenario forwards past one that no longer does).
//!
//! **DDL at a data-only node also needed no new code, verified by tracing
//! both functions it depends on, not merely by inspection.**
//! `ClientCtx::propose_schema` (`crates/animusd/src/schema.rs:133`) is
//! already fully `<E: Env, R: RelayClient>`-generic: `self.edge.
//! leader_handle()` answers `None` for a data-only node (its edge never had
//! `register_control` called on it — only a control-bearing node's own
//! `Local` handle is ever registered there), so it falls through to
//! `self.control.intra_leader_addr_hint()` (populated by the mirror-sync
//! loop's own `observe`/`observe_delta` calls once one round trip has
//! landed) and, failing that, a bounded broadcast over every node this
//! node's own `intra_route_snapshot()` knows — which includes every
//! control-bearing node's own address, so one of them accepts the relay
//! and resolves the real leader itself. `dynamo::create_table`
//! (`crates/animusd/src/dynamo.rs:4099`) never calls `ctx.data()` at all —
//! only `ctx.propose_schema`/`metadata_fresh(ctx)`, both already generic —
//! so it needed no widening either; `create_table`'s own
//! `provision_tablet`/`await_table_serveable` tail (`schema.rs`) is the
//! identical generic path a wire-created table already goes through
//! regardless of which node issued the request. Scenario (e) below proves
//! this concretely: a `CreateTable` issued from a data-only node's own
//! index succeeds and is visible on every node, control-bearing and
//! data-only alike.
//!
//! ## Scenarios (seed-parameterized, `_over_seeds` at 5 seeds each)
//!
//! (a) [`run_a_a_mixed_control_only_data_only_cluster_boots_and_serves`] — a
//!     3-control-only + 3-data-only (6-node) cluster boots, elects a control
//!     leader among the control-only voters, and a table created through a
//!     control-only node becomes visible in every data-only node's own
//!     mirror (converged-or-timeout); a write/read issued from a data-only
//!     node round-trips; every tablet this cluster hosts is hosted only on
//!     a data-only node, never a control-only one.
//! (b) [`run_b_restart_of_a_data_only_node_catches_up_and_re_serves`] — a
//!     data-only node is `crash`ed, a table is created (through a
//!     control-only node) and written to (through a *different* data-only
//!     node) while it stays down, then `restart`ed: its own mirror catches
//!     up to show the table created while it was down, it re-hosts its own
//!     share of the tablet, and a `ConsistentRead: true` read issued
//!     through it afterward serves the correct value.
//! (c) [`run_c_crash_of_a_data_only_replica_holder_the_rest_keep_serving_then_it_catches_up`]
//!     — with a table replicated across every data-only node (RF 3 of 3),
//!     one data-only replica holder is `crash`ed; the surviving two keep
//!     serving linearizable reads/writes throughout (a majority of 3); once
//!     `restart`ed, the crashed one catches up to the writes it missed
//!     (converged-or-timeout).
//! (d) [`run_d_a_mixed_combined_plus_data_only_cluster_round_trips_and_restarts`]
//!     — 1 combined + 2 data-only (3-node), RF 3: a table created through
//!     the combined node round-trips a write/read across both data-only
//!     nodes, then one data-only node is `crash`ed and `restart`ed and
//!     catches up — the identical shape (b)/(c) prove, in the mixed-role
//!     cluster this PR's own scope names explicitly.
//! (e) [`run_e_ddl_issued_at_a_data_only_node_succeeds_and_replicates_cluster_wide`]
//!     — a `CreateTable` issued from a data-only node's own index (its
//!     `ControlHandle::Remote`'s propose-schema relay path) succeeds and is
//!     visible on every node in the cluster, control-only and data-only
//!     alike; a write/read round-trip through the SAME data-only node
//!     confirms the newly created table is genuinely servable, not merely
//!     catalog-visible.

use std::time::Duration;

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{create_table_via_wire, env_seed, tablet_of_table};
use crate::config::NodeRole;

/// A 6-node cluster: 3 control-only (`NodeRole::Control`, indices 0-2) + 3
/// data-only (`NodeRole::Data`, indices 3-5). Replication factor 3 — every
/// data-only node hosts every table this cluster creates, which is exactly
/// what scenario (c)'s "crash one replica holder, the other two keep
/// serving" needs, and keeps every other scenario's own replica-set
/// reasoning trivial (no need to poll for "which nodes did placement
/// pick").
fn new_mixed_cluster(seed: u64) -> SimCluster {
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
        NodeRole::Data,
    ];
    SimCluster::new_with_roles(seed, &roles, 3)
}

/// A single-hash-key (`pk`, string) `CreateTable`, issued from `node` —
/// mirrors `sim_cluster_control_only.rs`'s identically-named helper.
fn create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    create_table_via_wire(cluster, node, &body)
}

/// Converged-or-timeout poll on `cond(cluster)` — the shared shape every
/// `sim_cluster_*` module's own scenario-local convergence checks use,
/// mirrored (not reached into) from `sim_cluster_control_only.rs`'s
/// identically-named helper per this crate's own "small fixtures
/// duplicated per test module" convention.
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

// ---------------------------------------------------------------------------
// (a) a mixed control-only/data-only cluster boots and serves
// ---------------------------------------------------------------------------

fn run_a_a_mixed_control_only_data_only_cluster_boots_and_serves(seed: u64) {
    let mut cluster = new_mixed_cluster(seed);

    for n in 0..3u64 {
        assert_eq!(
            cluster.role_of(n),
            NodeRole::Control,
            "seed={seed}: node {n} must be control-only"
        );
    }
    for n in 3..6u64 {
        assert_eq!(
            cluster.role_of(n),
            NodeRole::Data,
            "seed={seed}: node {n} must be data-only"
        );
    }

    // The control group elects across the 3 control-only voters before
    // anything else — `control_leader_index` is already a
    // converged-or-timeout wait.
    let leader = cluster.control_leader_index() as u64;
    assert!(
        leader < 3,
        "seed={seed}: control leader must be control-only, got {leader}"
    );

    // Issued through a control-only node — proves DDL from that role still
    // works in a cluster that also has data-only nodes present (this
    // fixture's own `sim_cluster_control_only.rs` already proves this in
    // isolation; this scenario is the composition proof).
    let (status, body) = create_table(&mut cluster, leader, "a");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    // Every data-only node's own mirror must eventually show the table —
    // this is the whole point of `spawn_remote_mirror_sync_loop`.
    for n in 3..6u64 {
        poll_until(
            &mut cluster,
            Duration::from_secs(10),
            seed,
            &format!("data-only node {n}'s own mirror catching up on table `a`"),
            |c| c.metadata(n).has_table_tablet("a"),
        );
    }

    // A write/read issued FROM a data-only node round-trips — with RF 3
    // across exactly 3 data-only candidates, node 3 hosts (or will host,
    // once the reconciler catches up) a replica of every tablet this
    // cluster creates.
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        "node 3 hosting table `a`'s tablet",
        |c| !c.hosted_tablets(3).is_empty(),
    );
    cluster
        .put(3, "a", "pk1", "sk1", b"v1")
        .unwrap_or_else(|e| panic!("seed={seed}: put via a data-only node failed: {e}"));
    let got = cluster
        .get(3, "a", "pk1", "sk1", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get via a data-only node failed: {e}"));
    assert_eq!(got.as_deref(), Some(&b"v1"[..]), "seed={seed}");

    // Every replica this cluster's real placement path picked must be
    // data-capable — a control-only node is never `Active` in
    // `Metadata::members` (`SimCluster::seed_members`'s `RegisterNode`
    // uses `role: "control"`, whose `claims_membership` gate never
    // inserts), so it can never be a placement candidate at all — and no
    // control-only node ever hosts a tablet as a result.
    let tablet = tablet_of_table(&cluster, "a");
    let meta = cluster.metadata(0);
    let replicas = meta
        .tablets
        .get(&tablet)
        .unwrap_or_else(|| panic!("seed={seed}: tablet {} missing from Metadata", tablet.0))
        .replicas
        .clone();
    assert!(!replicas.is_empty(), "seed={seed}: table has no replicas");
    for n in 0..cluster.node_count() as u64 {
        if replicas.contains(&animus_env::nid(n)) {
            assert_eq!(
                cluster.role_of(n),
                NodeRole::Data,
                "seed={seed}: replica node {n} must be data-only"
            );
        }
    }
    for n in 0..3u64 {
        assert!(
            cluster.hosted_tablets(n).is_empty(),
            "seed={seed}: control-only node {n} must never host a tablet, hosts {:?}",
            cluster.hosted_tablets(n)
        );
    }
}

#[test]
fn a_a_mixed_control_only_data_only_cluster_boots_and_serves() {
    run_a_a_mixed_control_only_data_only_cluster_boots_and_serves(env_seed(0xDA7A_0001));
}

#[test]
fn a_a_mixed_control_only_data_only_cluster_boots_and_serves_over_seeds() {
    for i in 0..5 {
        run_a_a_mixed_control_only_data_only_cluster_boots_and_serves(0xDA7A_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// (b) restart of a data-only node catches up and re-serves
// ---------------------------------------------------------------------------

fn run_b_restart_of_a_data_only_node_catches_up_and_re_serves(seed: u64) {
    let mut cluster = new_mixed_cluster(seed);
    let leader = cluster.control_leader_index() as u64;

    let target = 3u64;
    assert_eq!(
        cluster.role_of(target),
        NodeRole::Data,
        "seed={seed}: node {target} must be data-only"
    );

    // `target` goes down BEFORE the table it must later catch up on even
    // exists. Advance past `DETECT_TIMEOUT` (500ms, `animus_control::node`)
    // first so the control plane's own failure detector marks `target`
    // `Down` before `provision_tablet` picks the new table's initial
    // replica set — otherwise `target` (still `Active` in `Metadata` for
    // the first ~500ms after a `crash`, which only mutes the network, not
    // this node's own liveness row) would still be picked as one of the
    // tablet's `MAX_REPLICATION_FACTOR` (3) initial voters, and a 3-voter
    // Raft group formed with one voter unreachable from birth never
    // completes its own initial catch-up round within this fixture's
    // budgets — a real formation-liveness edge case, not the "majority
    // still alive" property this scenario means to exercise. With `target`
    // `Down`, `provision_tablet` mints a smaller (2-of-3-candidate) initial
    // set instead — `docs/engineering-lessons.md` and `schema.rs::
    // provision_tablet`'s own doc call this the deliberate self-heal
    // contract: the recorded RF policy target stays `MAX_REPLICATION_
    // FACTOR` regardless, so `reconcile_placement` grows the set back to 3
    // (adding `target` back in) the moment it rejoins `Active` after
    // `restart` below — which is exactly the "re-hosts its replicas" this
    // scenario asserts.
    cluster.crash(target);
    cluster.run_for(Duration::from_millis(600));

    let (status, body) = create_table(&mut cluster, leader, "b");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable (while node {target} is crashed) failed: {body}"
    );

    // Written through a DIFFERENT data-only node — `target` is crashed and
    // cannot serve or forward anything right now.
    cluster
        .put(4, "b", "pk1", "sk1", b"v1")
        .unwrap_or_else(|e| panic!("seed={seed}: put via node 4 failed: {e}"));

    cluster.restart(target);

    // The restarted node's own `ControlHandle::Remote` mirror is genuinely
    // fresh (a new `spawn_remote_mirror_sync_loop`, starting from no
    // observed watermark at all) — its own view of `Metadata` catching up
    // to show table `b` proves the mirror-sync loop actually re-establishes
    // itself post-restart, not that some pre-crash state lingered.
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        &format!("node {target}'s own mirror catching up on table `b`"),
        |c| c.metadata(target).has_table_tablet("b"),
    );

    // It re-hosts its own share of the tablet — the restarted reconciler's
    // own doing, not leftover state (a restarted node's `Simulator::stop`
    // dropped its previous reconciler task along with everything else it
    // owned).
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        &format!("node {target} re-hosting table `b`'s tablet after restart"),
        |c| !c.hosted_tablets(target).is_empty(),
    );

    // And serves a `ConsistentRead: true` read of the value written while
    // it was down.
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        &format!("node {target} serving a consistent read of the value written while it was down"),
        |c| {
            matches!(
                c.get(target, "b", "pk1", "sk1", true),
                Ok(Some(ref v)) if v.as_slice() == b"v1"
            )
        },
    );
}

#[test]
fn b_restart_of_a_data_only_node_catches_up_and_re_serves() {
    run_b_restart_of_a_data_only_node_catches_up_and_re_serves(env_seed(0xDA7A_0002));
}

#[test]
fn b_restart_of_a_data_only_node_catches_up_and_re_serves_over_seeds() {
    for i in 0..5 {
        run_b_restart_of_a_data_only_node_catches_up_and_re_serves(0xDA7A_2000 + i);
    }
}

// ---------------------------------------------------------------------------
// (c) crash of a data-only replica holder: the rest keep serving, then it
//     catches up
// ---------------------------------------------------------------------------

fn run_c_crash_of_a_data_only_replica_holder_the_rest_keep_serving_then_it_catches_up(seed: u64) {
    let mut cluster = new_mixed_cluster(seed);
    let leader = cluster.control_leader_index() as u64;

    let (status, body) = create_table(&mut cluster, leader, "c");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    // RF 3 across exactly 3 data-only candidates means every data-only
    // node hosts this table's tablet, so any of them is a legitimate
    // "the crashed one held a replica" target — confirmed directly rather
    // than assumed.
    for n in 3..6u64 {
        poll_until(
            &mut cluster,
            Duration::from_secs(10),
            seed,
            &format!("node {n} hosting table `c`'s tablet"),
            |c| !c.hosted_tablets(n).is_empty(),
        );
    }

    cluster
        .put(3, "c", "pk1", "sk1", b"v1")
        .unwrap_or_else(|e| panic!("seed={seed}: initial put failed: {e}"));

    let victim = 3u64;
    cluster.crash(victim);

    // The surviving two data-only replicas (a majority of 3) keep serving
    // linearizable reads AND writes throughout — issued from a survivor
    // node, never `victim`.
    cluster
        .put(4, "c", "pk2", "sk2", b"v2")
        .unwrap_or_else(|e| panic!("seed={seed}: put via a survivor failed: {e}"));
    let got = cluster
        .get(5, "c", "pk1", "sk1", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get via a survivor failed: {e}"));
    assert_eq!(got.as_deref(), Some(&b"v1"[..]), "seed={seed}");
    let got2 = cluster
        .get(4, "c", "pk2", "sk2", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get via a survivor failed: {e}"));
    assert_eq!(got2.as_deref(), Some(&b"v2"[..]), "seed={seed}");

    cluster.restart(victim);

    // Once restarted, it catches up to BOTH writes it missed while it was
    // crashed (converged-or-timeout — a fresh replica join/catch-up, not
    // an instantaneous fact).
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        &format!("node {victim} catching up to the write it missed (pk1)"),
        |c| {
            matches!(
                c.get(victim, "c", "pk1", "sk1", true),
                Ok(Some(ref v)) if v.as_slice() == b"v1"
            )
        },
    );
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        &format!("node {victim} catching up to the write it missed (pk2)"),
        |c| {
            matches!(
                c.get(victim, "c", "pk2", "sk2", true),
                Ok(Some(ref v)) if v.as_slice() == b"v2"
            )
        },
    );
}

#[test]
fn c_crash_of_a_data_only_replica_holder_the_rest_keep_serving_then_it_catches_up() {
    run_c_crash_of_a_data_only_replica_holder_the_rest_keep_serving_then_it_catches_up(env_seed(
        0xDA7A_0003,
    ));
}

#[test]
fn c_crash_of_a_data_only_replica_holder_the_rest_keep_serving_then_it_catches_up_over_seeds() {
    for i in 0..5 {
        run_c_crash_of_a_data_only_replica_holder_the_rest_keep_serving_then_it_catches_up(
            0xDA7A_3000 + i,
        );
    }
}

// ---------------------------------------------------------------------------
// (d) a mixed combined + data-only cluster round-trips and restarts
// ---------------------------------------------------------------------------

fn run_d_a_mixed_combined_plus_data_only_cluster_round_trips_and_restarts(seed: u64) {
    // 1 combined (`NodeRole::Both`, index 0) + 2 data-only (`NodeRole::
    // Data`, indices 1-2) — RF 3, so all three (the combined node
    // included) host every tablet this cluster creates.
    let roles = [NodeRole::Both, NodeRole::Data, NodeRole::Data];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 3);

    assert_eq!(cluster.role_of(0), NodeRole::Both, "seed={seed}");
    assert_eq!(cluster.role_of(1), NodeRole::Data, "seed={seed}");
    assert_eq!(cluster.role_of(2), NodeRole::Data, "seed={seed}");

    // Issued through the combined node — the only control-bearing node in
    // this 1-voter control group.
    let (status, body) = create_table(&mut cluster, 0, "d");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    for n in 1..3u64 {
        poll_until(
            &mut cluster,
            Duration::from_secs(10),
            seed,
            &format!("data-only node {n} hosting table `d`'s tablet"),
            |c| !c.hosted_tablets(n).is_empty(),
        );
    }

    // Write via one data-only node, read via the other — proves both are
    // genuinely serving the same replicated tablet, not merely each
    // independently reachable.
    cluster
        .put(1, "d", "pk1", "sk1", b"v1")
        .unwrap_or_else(|e| panic!("seed={seed}: put via node 1 failed: {e}"));
    let got = cluster
        .get(2, "d", "pk1", "sk1", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get via node 2 failed: {e}"));
    assert_eq!(got.as_deref(), Some(&b"v1"[..]), "seed={seed}");

    // Crash + restart one of the two data-only nodes — the identical (b)/
    // (c) shape, in the mixed combined+data-only cluster this scenario's
    // own scope names explicitly.
    let target = 1u64;
    cluster.crash(target);
    cluster
        .put(2, "d", "pk2", "sk2", b"v2")
        .unwrap_or_else(|e| panic!("seed={seed}: put via node 2 (while node 1 down) failed: {e}"));
    cluster.restart(target);

    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        &format!("node {target} catching up on table `d`"),
        |c| c.metadata(target).has_table_tablet("d"),
    );
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        &format!("node {target} re-hosting table `d`'s tablet after restart"),
        |c| !c.hosted_tablets(target).is_empty(),
    );
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        &format!("node {target} catching up to the write it missed"),
        |c| {
            matches!(
                c.get(target, "d", "pk2", "sk2", true),
                Ok(Some(ref v)) if v.as_slice() == b"v2"
            )
        },
    );
}

#[test]
fn d_a_mixed_combined_plus_data_only_cluster_round_trips_and_restarts() {
    run_d_a_mixed_combined_plus_data_only_cluster_round_trips_and_restarts(env_seed(0xDA7A_0004));
}

#[test]
fn d_a_mixed_combined_plus_data_only_cluster_round_trips_and_restarts_over_seeds() {
    for i in 0..5 {
        run_d_a_mixed_combined_plus_data_only_cluster_round_trips_and_restarts(0xDA7A_4000 + i);
    }
}

// ---------------------------------------------------------------------------
// (e) DDL issued at a data-only node succeeds and replicates cluster-wide
// ---------------------------------------------------------------------------

fn run_e_ddl_issued_at_a_data_only_node_succeeds_and_replicates_cluster_wide(seed: u64) {
    let mut cluster = new_mixed_cluster(seed);

    // Issued from a DATA-ONLY node's own index — its `ControlHandle::
    // Remote`'s propose-schema relay path (`ClientCtx::propose_schema`,
    // `schema.rs:133`), never a local control `RaftNode`.
    let (status, body) = create_table(&mut cluster, 3, "e");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable issued at a data-only node failed: {body}"
    );

    // Visible on EVERY node — control-only and data-only alike.
    for n in 0..cluster.node_count() as u64 {
        poll_until(
            &mut cluster,
            Duration::from_secs(10),
            seed,
            &format!("node {n}'s own view catching up on table `e`"),
            |c| c.metadata(n).has_table_tablet("e"),
        );
    }

    // And genuinely servable, not merely catalog-visible — a write/read
    // round trip through the SAME data-only node that issued the DDL.
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        "node 3 hosting table `e`'s tablet",
        |c| !c.hosted_tablets(3).is_empty(),
    );
    cluster
        .put(3, "e", "pk1", "sk1", b"v1")
        .unwrap_or_else(|e| panic!("seed={seed}: put via node 3 failed: {e}"));
    let got = cluster
        .get(3, "e", "pk1", "sk1", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get via node 3 failed: {e}"));
    assert_eq!(got.as_deref(), Some(&b"v1"[..]), "seed={seed}");
}

#[test]
fn e_ddl_issued_at_a_data_only_node_succeeds_and_replicates_cluster_wide() {
    run_e_ddl_issued_at_a_data_only_node_succeeds_and_replicates_cluster_wide(env_seed(
        0xDA7A_0005,
    ));
}

#[test]
fn e_ddl_issued_at_a_data_only_node_succeeds_and_replicates_cluster_wide_over_seeds() {
    for i in 0..5 {
        run_e_ddl_issued_at_a_data_only_node_succeeds_and_replicates_cluster_wide(0xDA7A_5000 + i);
    }
}
