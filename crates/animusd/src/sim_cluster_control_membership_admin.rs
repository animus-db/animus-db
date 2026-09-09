//! `SimCluster`-driven conversion of `tests/control_membership_admin.rs`
//! (12 tests) — ADR 0061 rung L, C-12 PR 4e. Builds on `sim_cluster_
//! control_data_split.rs`'s own PR 2/3 mechanism (`NodeRole`-aware
//! `SimCluster::new_with_roles`, role-aware `restart`/`crash`) and
//! `sim_cluster_admin_actions.rs`'s own PR 6 precedent (`SimCluster::admin`
//! through `GenericAdminHost`, already reaching `GET /admin/control/
//! members`, `POST /admin/control/member/{add,remove}`, and `GET /admin/
//! config` — every route this module drives). **Pure test authorship: no
//! `admin.rs`/`sim_cluster.rs` production-shaped change was needed at
//! all** — every route `GenericAdminHost` needs was already a trait method
//! on both the concrete and generic `AdminHost` impls (`control_members_
//! view`, `action_add_control_member`, `action_remove_control_member`,
//! `config_view`, `action_add_member`), confirmed by reading `admin.rs`'s
//! two impl blocks side by side before writing a single scenario.
//!
//! ## What `SimCluster` can and cannot host for this rung (read this first)
//!
//! [`SimCluster::grow`] supports `role = "data"` (data-only growth) only —
//! its own doc says a **"combined" (new control-plane voter) growth node
//! is deferred**. There is no `SimCluster` primitive that brings up a
//! genuinely fresh process, gives it a real `RaftNode<SimEnv>`, and starts
//! it life as a quiet **non-voter** the way `tests/control_membership_
//! admin.rs::join_control_nonvoter` does for the real suite — every
//! control-bearing node in a `SimCluster::new_with_roles` roster is a
//! voter from construction (`new_with_roles`'s own `control_ids = ids[..
//! control_count]`). This one gap governs two of this module's own
//! dispositions below (see (1) and (4)).
//!
//! Separately, [`animus_env::Env::merge_peer`] — the mechanism `admin_add_
//! control_member` uses to teach the ADDING leader's own env how to dial a
//! freshly-added voter — has a **no-op default on the `Env` trait itself**,
//! and `SimEnv` never overrides it (only `ProdEnv` does). `SimCluster`
//! also seeds every node's full route table up front, at construction
//! (`new_with_roles`'s own `route` map, `ids.iter().map(|id| (id.clone(),
//! id.to_string()))`), so under `SimEnv` **every node already knows how to
//! reach every other node id from the moment both exist** — regardless of
//! which node happened to call `merge_peer` last, or whether it was called
//! at all. This makes `merge_peer`'s own "known scope limit" (only the
//! calling leader's own env learns a runtime-added voter's dial address,
//! until a later `Metadata.node_addrs`-driven `control_peer_sync_loop`
//! catches every other node up) a fact `SimEnv` cannot model: there is no
//! "leader that knows a peer" vs. "leader that doesn't" distinction to
//! race a leadership change against in the first place. See (4)'s own
//! disposition for the test this makes structurally unreachable, not
//! merely hard to trigger.
//!
//! ## Classification table (D3 discipline, per original test)
//!
//! | Original test | A/B | Sim sibling |
//! |---|---|---|
//! | `grow_control_group_converges_everywhere` | A (weakened) | [`run_grow_control_group_converges_everywhere`] — converts the *shape* (`POST /admin/control/member/add` on the leader grows the live voter set, converging everywhere including a `NodeRole::Data` node's own `ControlHandle::Remote` mirror) but not the real test's own "genuinely fresh, previously-nonexistent process starts life as a quiet non-voter" premise — `SimCluster::grow`'s own doc says combined (control-voter) growth is deferred (see the module doc above). Substitutes removing, then re-adding, one of 4 already-running control-bearing nodes: the ADD mechanism under test (mint/register/`change_membership` + converge-everywhere) is exercised identically either way; only the growth-node process bring-up specifics (address resolution, self-registration-landed wait) go unreproduced, and those are inherently `ProdEnv`/real-socket concerns, not part of the admin logic this rung converts |
//! | `add_control_member_collision_shapes` | A | [`run_add_control_member_collision_shapes`] — full convert |
//! | `remove_control_voter_refusals_transfer_and_quorum_warnings` | A | [`run_remove_control_voter_refusals_transfer_and_quorum_warnings`] — full convert: idempotent unknown-node removal, the not-relayable follower refusal for both mutating actions, a clean non-leader removal, the leader-self-removal transfer-then-retry dance (bounded retry against whichever control node currently reports itself leader, mirroring `sim_cluster_split_cluster.rs`'s own `put_retry`/`put_raw_retry` bounded-budget shape, ADR 0061 rung L PR 4b), the down-to-1 quorum warning, and the refused last-voter removal |
//! | `runtime_added_voter_survives_leadership_change_to_a_different_original_voter` | B | **KEPT** whole — the mechanism under test (`ProdEnv::merge_peer`'s per-env peer-book scope limit, and its fix via the replicated `NodeAddrs.control` field + `control_peer_sync_loop`) is structurally invisible under `SimEnv`: `Env::merge_peer` is a documented no-op there, and `SimCluster`'s own route table is fully seeded for every node id at construction, so every `SimEnv` node already knows how to dial every other one regardless of which node added it or when — see the module doc above. This is not a scenario-design difficulty; there is nothing for a `SimCluster` scenario to observe going wrong before the fix, or right after it |
//! | `removing_a_live_voter_while_another_is_already_dead_is_refused_without_force` | A | [`run_removing_a_live_voter_while_another_is_already_dead_is_refused_without_force`] — full convert via [`SimCluster::crash`] of a non-leader follower plus a deterministic `run_for(CONTROL_PEER_LIVENESS_TIMEOUT * 3)` advance (virtual time, so no real-scheduling-jitter margin is actually needed — the multiplier is kept only to mirror the original's own margin) |
//! | `removing_a_live_voter_while_another_is_already_dead_succeeds_with_force` | A | [`run_removing_a_live_voter_while_another_is_already_dead_succeeds_with_force`] — full convert, including the bounded "stays wedged" probe window |
//! | `removing_the_actually_dead_voter_itself_needs_no_force` | A | [`run_removing_the_actually_dead_voter_itself_needs_no_force`] — full convert |
//! | `removing_a_voter_when_every_remaining_voter_is_alive_is_never_refused` | A | [`run_removing_a_voter_when_every_remaining_voter_is_alive_is_never_refused`] — full convert |
//! | `concurrent_control_add_surfaces_in_flight_as_a_clean_retryable_error` | A | [`run_concurrent_control_add_surfaces_in_flight_as_a_clean_retryable_error`] — full convert via [`admin_join2`] (two `SimClusterHandle::admin` futures spawned onto the SAME leader env in the same virtual instant, with no intervening `run_for` between the two spawns, then drained together — the identical "both land at the same discrete-event instant" idiom `sim_cluster_split_cluster.rs`'s own dual-[`SimCluster::crash`] scenario already establishes for two faults, generalized here to two mutating admin calls racing the leader's shared `Mutex<RaftCore>`) |
//! | `omitted_node_add_mints_an_id_and_converges_to_a_live_voter` | A | [`run_omitted_node_add_mints_an_id_and_converges_to_a_live_voter`] — full convert; like the real test, `addr` is a fake, never-dialed placeholder — this proves the admin-plane mint + register + `change_membership` mechanics, not real Raft catch-up (already covered by (1)'s own remove/re-add substitute, which DOES exercise a live, running voter) |
//! | `concurrent_omitted_node_adds_mint_distinct_ids_and_both_become_voters` | A | [`run_concurrent_omitted_node_adds_mint_distinct_ids_and_both_become_voters`] — full convert, via [`admin_join2`] |
//! | `admin_config_reports_the_internal_addr_the_cli_resolves_control_add_through` | A | [`run_admin_config_reports_the_internal_addr_the_cli_resolves_control_add_through`] — full convert. The real test never actually parses a `--config FILE` — it builds its one-node cluster in-process via `bring_up_combined` (itself `animusd::run_node` over a hand-built `ClusterConfig`, no file on disk) and asserts a pure `GET /admin/config` JSON-shape fact `SimCluster::admin`'s own dispatch already reaches; this was mislabeled a permanent real-socket residual by this rung's own opener plan, corrected here per this PR's own brief |
//!
//! Real-socket count before/after this PR: 12 → 1 (only (4) stays, for the
//! structural `SimEnv`/`merge_peer` reason above — not a scenario-design
//! difficulty this rung could design around).
//!
//! Every scenario issues its mutating admin call from the current control
//! leader (`SimCluster::control_leader_index`) or a deliberately-chosen
//! follower where the scenario's own subject is the follower-refusal path,
//! mirroring the real suite's own discipline. `_over_seeds` siblings run 5
//! fixed seeds each, mirroring every other `sim_cluster_*` module's own
//! convention. Seed replay (repo convention): `ANIMUS_SEED=<seed> cargo
//! test -p animusd --lib <scenario name>`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::node::CONTROL_PEER_LIVENESS_TIMEOUT;
use animus_env::{EnvExt, NodeId, nid};

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{env_seed, json};
use crate::config::NodeRole;

/// A fake, never-dialed placeholder control-Raft address — the identical
/// convention the real suite's own `add_control_member`/`concurrent_
/// control_add_surfaces_in_flight_as_a_clean_retryable_error` use: this
/// module proves the admin-plane mint/register/`change_membership`
/// mechanics, not real address dialing (`Env::merge_peer` is a `SimEnv`
/// no-op regardless — see the module doc above).
const PLACEHOLDER_ADDR: &str = "127.0.0.1:1";

/// Converged-or-timeout poll on `cond(cluster)` — the shared shape every
/// `sim_cluster_*` module's own scenario-local convergence checks use
/// (duplicated, not reached into, per this crate's own convention).
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

fn admin_get(cluster: &mut SimCluster, node: u64, path: &str) -> (u16, serde_json::Value) {
    let (status, body) = cluster.admin(node, "GET", path, "", &[]);
    (status, json(&body))
}

fn admin_post(
    cluster: &mut SimCluster,
    node: u64,
    path: &str,
    body: &str,
) -> (u16, serde_json::Value) {
    let (status, resp) = cluster.admin(node, "POST", path, "", body.as_bytes());
    (status, json(&resp))
}

fn control_members(cluster: &mut SimCluster, node: u64) -> (u16, serde_json::Value) {
    admin_get(cluster, node, "/admin/control/members")
}

fn add_control_member(
    cluster: &mut SimCluster,
    node: u64,
    target: u64,
    addr: &str,
) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"node": nid(target).to_string(), "addr": addr}).to_string();
    admin_post(cluster, node, "/admin/control/member/add", &body)
}

/// The allocator-minted-id form: `node` omitted entirely — exercises
/// `AddControlMemberReq`'s `#[serde(default)]` the same way the real
/// suite's own `add_control_member_allocated` does.
fn add_control_member_omitted(
    cluster: &mut SimCluster,
    node: u64,
    addr: &str,
) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"addr": addr}).to_string();
    admin_post(cluster, node, "/admin/control/member/add", &body)
}

/// `POST /admin/member/add` (ADR 0030 online growth) — registers a plain
/// data-plane member with no control role at all, the collision target
/// `add_control_member_collision_shapes` needs.
fn add_member(cluster: &mut SimCluster, node: u64, target: u64) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"node": nid(target).to_string()}).to_string();
    admin_post(cluster, node, "/admin/member/add", &body)
}

fn remove_control_member(
    cluster: &mut SimCluster,
    node: u64,
    target: u64,
    force: bool,
) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"node": nid(target).to_string(), "force": force}).to_string();
    admin_post(cluster, node, "/admin/control/member/remove", &body)
}

fn voters_of(body: &serde_json::Value) -> Option<Vec<NodeId>> {
    body["voters"].as_array().map(|a| {
        a.iter()
            .filter_map(|v| v.as_str()?.parse::<NodeId>().ok())
            .collect()
    })
}

/// Whether `id` looks like a `NodeId::mint` output — exactly 22 chars (128
/// bits of base64url, unpadded) — mirrors the real suite's identically-
/// named helper.
fn looks_minted(id: &NodeId) -> bool {
    id.as_str().chars().count() == 22
}

/// Spawn two admin requests onto `node`'s own env in the same virtual
/// instant — no intervening `run_for` between the two `spawn_task` calls —
/// then drain both with one `run_for(OP_BUDGET)`. Mirrors `sim_cluster_
/// split_cluster.rs`'s own "both faults land at the same discrete-event
/// instant" idiom (two `SimCluster::crash` calls back to back, ADR 0061
/// rung L PR 4b), generalized here to two mutating admin calls racing the
/// leader's shared `Mutex<RaftCore>` the way the real suite's own
/// `tokio::join!` does over two real connections. Never panics on a
/// timeout (mirrors every other `SimCluster` driver primitive): returns a
/// synthetic `500` body for whichever request didn't resolve in the
/// budget.
const OP_BUDGET: Duration = Duration::from_secs(12);

fn admin_join2(
    cluster: &mut SimCluster,
    node: u64,
    req1: (&str, &str, &[u8]),
    req2: (&str, &str, &[u8]),
) -> ((u16, String), (u16, String)) {
    let handle = cluster.handle();
    let env = handle.env(node);

    let slot1: Arc<Mutex<Option<(u16, String)>>> = Arc::new(Mutex::new(None));
    let slot2: Arc<Mutex<Option<(u16, String)>>> = Arc::new(Mutex::new(None));

    let h1 = handle.clone();
    let (m1, p1, b1) = (req1.0.to_owned(), req1.1.to_owned(), req1.2.to_vec());
    let out1 = slot1.clone();
    env.spawn_task(async move {
        let r = h1.admin(node, &m1, &p1, "", &b1).await;
        *out1.lock().expect("admin_join2 slot poisoned") = Some(r);
    });

    let h2 = handle.clone();
    let (m2, p2, b2) = (req2.0.to_owned(), req2.1.to_owned(), req2.2.to_vec());
    let out2 = slot2.clone();
    env.spawn_task(async move {
        let r = h2.admin(node, &m2, &p2, "", &b2).await;
        *out2.lock().expect("admin_join2 slot poisoned") = Some(r);
    });

    cluster.run_for(OP_BUDGET);

    let r1 = slot1
        .lock()
        .expect("admin_join2 slot poisoned")
        .take()
        .unwrap_or_else(|| {
            (
                500,
                format!(
                    "admin_join2: request 1 on node {node} did not complete within {OP_BUDGET:?}"
                ),
            )
        });
    let r2 = slot2
        .lock()
        .expect("admin_join2 slot poisoned")
        .take()
        .unwrap_or_else(|| {
            (
                500,
                format!(
                    "admin_join2: request 2 on node {node} did not complete within {OP_BUDGET:?}"
                ),
            )
        });
    (r1, r2)
}

/// A 3-node combined-role core with one non-leader follower genuinely
/// [`SimCluster::crash`]ed and virtual time advanced deterministically past
/// [`CONTROL_PEER_LIVENESS_TIMEOUT`] — the shared setup for (5)/(6)/(7).
/// Returns `(cluster, leader, dead_id, live_target_id)`.
fn cluster_with_one_dead_follower(seed: u64) -> (SimCluster, u64, u64, u64) {
    let mut cluster = SimCluster::new_with_roles(seed, &[NodeRole::Both; 3], 1);
    let leader = cluster.control_leader_index() as u64;
    let followers: Vec<u64> = (0..3u64).filter(|&i| i != leader).collect();
    let dead_id = followers[0];
    let live_target_id = followers[1];

    cluster.crash(dead_id);
    // Deterministic virtual-time advance — no real-scheduling-jitter
    // margin is actually needed under `SimEnv`, but the 3x multiplier is
    // kept to mirror the real suite's own margin exactly.
    cluster.run_for(CONTROL_PEER_LIVENESS_TIMEOUT * 3);

    (cluster, leader, dead_id, live_target_id)
}

// ---------------------------------------------------------------------------
// (1) grow_control_group_converges_everywhere
// ---------------------------------------------------------------------------

fn run_grow_control_group_converges_everywhere(seed: u64) {
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 1);
    let leader = cluster.control_leader_index() as u64;

    let full: Vec<NodeId> = vec![nid(0), nid(1), nid(2), nid(3)];

    for n in 0..5u64 {
        let full = full.clone();
        poll_until(
            &mut cluster,
            Duration::from_secs(15),
            seed,
            &format!("node {n} observing the initial voter set"),
            move |c| {
                let (status, body) = control_members(c, n);
                status == 200 && voters_of(&body) == Some(full.clone())
            },
        );
    }

    // `SimCluster::grow` supports `role = "data"` growth only today — see
    // the module doc above. Substitute: remove one of the 4 already-running
    // control-bearing nodes, then re-add it — the ADD mechanism itself
    // (mint/register/`change_membership`, converging everywhere including
    // the data-only node's own `ControlHandle::Remote` mirror) is exercised
    // identically either way.
    let target = (0..4u64)
        .find(|&i| i != leader)
        .expect("a follower voter exists");

    let (status, body) = remove_control_member(&mut cluster, leader, target, false);
    assert_eq!(
        status, 200,
        "seed={seed}: removing node {target} (to re-add it) failed: {body}"
    );

    let shrunk: Vec<NodeId> = full
        .iter()
        .filter(|&id| *id != nid(target))
        .cloned()
        .collect();
    for n in (0..5u64).filter(|&n| n != target) {
        let shrunk = shrunk.clone();
        poll_until(
            &mut cluster,
            Duration::from_secs(15),
            seed,
            &format!("node {n} observing the shrunk voter set"),
            move |c| {
                let (status, body) = control_members(c, n);
                status == 200 && voters_of(&body) == Some(shrunk.clone())
            },
        );
    }

    let (status, body) = add_control_member(&mut cluster, leader, target, PLACEHOLDER_ADDR);
    assert_eq!(
        status, 200,
        "seed={seed}: control/member/add failed: {body}"
    );

    for n in 0..5u64 {
        let full = full.clone();
        poll_until(
            &mut cluster,
            Duration::from_secs(30),
            seed,
            &format!("node {n} observing convergence to the regrown voter set"),
            move |c| {
                let (status, body) = control_members(c, n);
                status == 200 && voters_of(&body) == Some(full.clone())
            },
        );
    }
}

#[test]
fn grow_control_group_converges_everywhere() {
    run_grow_control_group_converges_everywhere(env_seed(0xC12E_0001));
}

#[test]
fn grow_control_group_converges_everywhere_over_seeds() {
    for i in 0..5 {
        run_grow_control_group_converges_everywhere(0xC12E_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// (2) add_control_member_collision_shapes
// ---------------------------------------------------------------------------

fn run_add_control_member_collision_shapes(seed: u64) {
    let mut cluster = SimCluster::new_with_roles(seed, &[NodeRole::Both; 3], 1);
    let leader = cluster.control_leader_index() as u64;

    // Already a live voter: idempotent success.
    let (status, body) = add_control_member(&mut cluster, leader, 1, PLACEHOLDER_ADDR);
    assert_eq!(
        status, 200,
        "seed={seed}: re-adding an existing voter should be a no-op: {body}"
    );

    // An existing data-plane member (not yet a control voter) — promoting
    // it now succeeds (ADR 0040: one identity per node, no reserved
    // control-id range to collide with).
    let (status, body) = add_member(&mut cluster, leader, 50);
    assert_eq!(
        status, 200,
        "seed={seed}: registering the collision member failed: {body}"
    );
    let (status, body) = add_control_member(&mut cluster, leader, 50, PLACEHOLDER_ADDR);
    assert_eq!(
        status, 200,
        "seed={seed}: promoting an existing data-plane member to a control voter should succeed: {body}"
    );
}

#[test]
fn add_control_member_collision_shapes() {
    run_add_control_member_collision_shapes(env_seed(0xC12E_0002));
}

#[test]
fn add_control_member_collision_shapes_over_seeds() {
    for i in 0..5 {
        run_add_control_member_collision_shapes(0xC12E_2000 + i);
    }
}

// ---------------------------------------------------------------------------
// (3) remove_control_voter_refusals_transfer_and_quorum_warnings
// ---------------------------------------------------------------------------

fn run_remove_control_voter_refusals_transfer_and_quorum_warnings(seed: u64) {
    let mut cluster = SimCluster::new_with_roles(seed, &[NodeRole::Both; 3], 1);

    // Idempotent: an id that was never a control voter at all.
    {
        let leader = cluster.control_leader_index() as u64;
        let (status, body) = remove_control_member(&mut cluster, leader, 999, false);
        assert_eq!(
            status, 200,
            "seed={seed}: removing an unknown node should be a no-op: {body}"
        );
        assert!(
            body["warning"].is_null(),
            "seed={seed}: an idempotent no-op removal should carry no warning: {body}"
        );
    }

    // Not relayable: both mutating actions refuse cleanly on a follower's
    // admin port.
    {
        let leader = cluster.control_leader_index() as u64;
        let follower = (0..3u64).find(|&i| i != leader).expect("a follower exists");

        let (status, body) = remove_control_member(&mut cluster, follower, 999, false);
        assert_eq!(
            status, 409,
            "seed={seed}: control/member/remove on a follower should be refused: {body}"
        );
        let msg = body["error"]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase();
        assert!(
            msg.contains("leader"),
            "seed={seed}: expected a leader-routing refusal, got: {msg}"
        );

        let (status, body) = add_control_member(&mut cluster, follower, 999, PLACEHOLDER_ADDR);
        assert_eq!(
            status, 409,
            "seed={seed}: control/member/add on a follower should be refused: {body}"
        );
        let msg = body["error"]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase();
        assert!(
            msg.contains("leader"),
            "seed={seed}: expected a leader-routing refusal, got: {msg}"
        );
    }

    // Remove a non-leader voter: succeeds, no warning (2 of 3 remain, both
    // alive).
    let leader = cluster.control_leader_index() as u64;
    let non_leader_voter = (0..3u64)
        .find(|&i| i != leader)
        .expect("a follower id exists");
    {
        let (status, body) = remove_control_member(&mut cluster, leader, non_leader_voter, false);
        assert_eq!(
            status, 200,
            "seed={seed}: removing a non-leader voter failed: {body}"
        );
        assert!(
            body["warning"].is_null(),
            "seed={seed}: removing down to 2 healthy voters should carry no warning: {body}"
        );
    }
    poll_until(
        &mut cluster,
        Duration::from_secs(15),
        seed,
        "removal of the non-leader voter",
        |c| {
            let (status, body) = control_members(c, leader);
            status == 200
                && voters_of(&body)
                    .is_some_and(|v| v.len() == 2 && !v.contains(&nid(non_leader_voter)))
        },
    );

    // Remove the current leader's own slot: arms a transfer and returns the
    // familiar "retry on the leader" refusal — never a silent success.
    // Leadership may bounce between the two remaining voters more than
    // once while the transfer settles, so this retries the removal against
    // whichever currently reports itself leader until it succeeds, bounded
    // overall — mirrors `sim_cluster_split_cluster.rs`'s own `put_retry`/
    // `put_raw_retry` bounded-budget shape (ADR 0061 rung L PR 4b).
    let leader = cluster.control_leader_index() as u64;
    let leader_id = leader;
    let other_id: u64 = (0..3u64)
        .find(|&i| i != leader_id && i != non_leader_voter)
        .expect("exactly one other voter remains");
    let mut saw_leader_refusal = false;
    const STEP: Duration = Duration::from_millis(100);
    let budget = Duration::from_secs(30);
    let mut elapsed = Duration::ZERO;
    let new_leader_id: u64 = loop {
        let current_leader = cluster.control_leader_index() as u64;
        let (status, body) = remove_control_member(&mut cluster, current_leader, leader_id, false);
        if status == 200 {
            assert!(
                body["warning"].as_str().is_some(),
                "seed={seed}: removing down to 1 voter should carry a quorum-loss warning: {body}"
            );
            break current_leader;
        }
        let msg = body["error"]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase();
        assert!(
            msg.contains("leader"),
            "seed={seed}: expected an idempotent no-op or a leader-routing refusal, got: {msg}"
        );
        saw_leader_refusal = true;
        assert!(
            elapsed < budget,
            "seed={seed}: removing the old leader's own slot never succeeded within {budget:?}: {body}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    };
    assert!(
        saw_leader_refusal,
        "seed={seed}: expected at least one leader-self-removal refusal before the eventual success"
    );
    let _ = other_id;

    poll_until(
        &mut cluster,
        Duration::from_secs(15),
        seed,
        "removal down to 1 voter",
        |c| {
            let (status, body) = control_members(c, new_leader_id);
            status == 200 && voters_of(&body).map(|v| v.len()) == Some(1)
        },
    );

    // Removing the last remaining voter is refused outright.
    let (status, body) = remove_control_member(&mut cluster, new_leader_id, other_id, false);
    assert_eq!(
        status, 409,
        "seed={seed}: removing the last remaining voter should be refused: {body}"
    );
}

#[test]
fn remove_control_voter_refusals_transfer_and_quorum_warnings() {
    run_remove_control_voter_refusals_transfer_and_quorum_warnings(env_seed(0xC12E_0003));
}

#[test]
fn remove_control_voter_refusals_transfer_and_quorum_warnings_over_seeds() {
    for i in 0..5 {
        run_remove_control_voter_refusals_transfer_and_quorum_warnings(0xC12E_3000 + i);
    }
}

// ---------------------------------------------------------------------------
// (5) removing_a_live_voter_while_another_is_already_dead_is_refused_without_force
// ---------------------------------------------------------------------------

fn run_removing_a_live_voter_while_another_is_already_dead_is_refused_without_force(seed: u64) {
    let (mut cluster, leader, dead_id, live_target_id) = cluster_with_one_dead_follower(seed);

    let (status, body) = remove_control_member(&mut cluster, leader, live_target_id, false);
    assert_eq!(
        status, 409,
        "seed={seed}: removing a live voter while a different survivor is already dead \
         should be refused by the liveness-aware guard: {body}"
    );
    let msg = body["error"]
        .as_str()
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert!(
        msg.contains(&dead_id.to_string()),
        "seed={seed}: the refusal should name the apparently-dead voter ({dead_id}): {msg}"
    );
    assert!(
        msg.contains("force"),
        "seed={seed}: the refusal should point the operator at --force: {msg}"
    );

    let (status, body) = control_members(&mut cluster, leader);
    assert_eq!(status, 200, "seed={seed}: control/members failed: {body}");
    let mut voters = voters_of(&body).expect("seed={seed}: voters present");
    voters.sort_unstable();
    assert_eq!(
        voters,
        vec![nid(0), nid(1), nid(2)],
        "seed={seed}: a refused removal must not change the live voter set: {body}"
    );
}

#[test]
fn removing_a_live_voter_while_another_is_already_dead_is_refused_without_force() {
    run_removing_a_live_voter_while_another_is_already_dead_is_refused_without_force(env_seed(
        0xC12E_0005,
    ));
}

#[test]
fn removing_a_live_voter_while_another_is_already_dead_is_refused_without_force_over_seeds() {
    for i in 0..5 {
        run_removing_a_live_voter_while_another_is_already_dead_is_refused_without_force(
            0xC12E_5000 + i,
        );
    }
}

// ---------------------------------------------------------------------------
// (6) removing_a_live_voter_while_another_is_already_dead_succeeds_with_force
// ---------------------------------------------------------------------------

fn run_removing_a_live_voter_while_another_is_already_dead_succeeds_with_force(seed: u64) {
    let (mut cluster, leader, _dead_id, live_target_id) = cluster_with_one_dead_follower(seed);

    let (status, body) = remove_control_member(&mut cluster, leader, live_target_id, true);
    assert_eq!(
        status, 200,
        "seed={seed}: removing a live voter while another is already dead should succeed \
         with --force: {body}"
    );
    assert!(
        body["warning"].is_null(),
        "seed={seed}: a 3 -> 2 removal (not down to 1) carries no warning even with \
         --force: {body}"
    );

    // The real consequence: wedged for any FURTHER membership change
    // (`config_change_in_flight` can never clear — the dead voter can
    // never ack). Bounded probe, must never succeed.
    const STEP: Duration = Duration::from_millis(150);
    let probe_budget = Duration::from_secs(5);
    let mut elapsed = Duration::ZERO;
    let mut ever_succeeded = false;
    let mut last_body = serde_json::Value::Null;
    while elapsed < probe_budget {
        let (status, body) = add_control_member(&mut cluster, leader, 90, PLACEHOLDER_ADDR);
        if status == 200 {
            ever_succeeded = true;
            last_body = body;
            break;
        }
        last_body = body;
        cluster.run_for(STEP);
        elapsed += STEP;
    }
    assert!(
        !ever_succeeded,
        "seed={seed}: a further control-membership change must never succeed once the group is \
         stranded (one dead survivor out of 2 voters) — but it did: {last_body}"
    );
}

#[test]
fn removing_a_live_voter_while_another_is_already_dead_succeeds_with_force() {
    run_removing_a_live_voter_while_another_is_already_dead_succeeds_with_force(env_seed(
        0xC12E_0006,
    ));
}

#[test]
fn removing_a_live_voter_while_another_is_already_dead_succeeds_with_force_over_seeds() {
    for i in 0..5 {
        run_removing_a_live_voter_while_another_is_already_dead_succeeds_with_force(
            0xC12E_6000 + i,
        );
    }
}

// ---------------------------------------------------------------------------
// (7) removing_the_actually_dead_voter_itself_needs_no_force
// ---------------------------------------------------------------------------

fn run_removing_the_actually_dead_voter_itself_needs_no_force(seed: u64) {
    let (mut cluster, leader, dead_id, _live_target_id) = cluster_with_one_dead_follower(seed);

    let (status, body) = remove_control_member(&mut cluster, leader, dead_id, false);
    assert_eq!(
        status, 200,
        "seed={seed}: removing the actually-dead voter itself should succeed with no \
         --force needed: {body}"
    );
    assert!(
        body["warning"].is_null(),
        "seed={seed}: removing down to 2 voters, both alive, should carry no warning: {body}"
    );
}

#[test]
fn removing_the_actually_dead_voter_itself_needs_no_force() {
    run_removing_the_actually_dead_voter_itself_needs_no_force(env_seed(0xC12E_0007));
}

#[test]
fn removing_the_actually_dead_voter_itself_needs_no_force_over_seeds() {
    for i in 0..5 {
        run_removing_the_actually_dead_voter_itself_needs_no_force(0xC12E_7000 + i);
    }
}

// ---------------------------------------------------------------------------
// (8) removing_a_voter_when_every_remaining_voter_is_alive_is_never_refused
// ---------------------------------------------------------------------------

fn run_removing_a_voter_when_every_remaining_voter_is_alive_is_never_refused(seed: u64) {
    let mut cluster = SimCluster::new_with_roles(seed, &[NodeRole::Both; 3], 1);
    let leader = cluster.control_leader_index() as u64;
    let non_leader_voter = (0..3u64)
        .find(|&i| i != leader)
        .expect("a follower id exists");

    let (status, body) = remove_control_member(&mut cluster, leader, non_leader_voter, false);
    assert_eq!(
        status, 200,
        "seed={seed}: removing a voter when every remaining voter is alive should never \
         be refused by the liveness guard: {body}"
    );
    assert!(
        body["warning"].is_null(),
        "seed={seed}: removing down to 2 healthy voters should carry no warning: {body}"
    );
}

#[test]
fn removing_a_voter_when_every_remaining_voter_is_alive_is_never_refused() {
    run_removing_a_voter_when_every_remaining_voter_is_alive_is_never_refused(env_seed(
        0xC12E_0008,
    ));
}

#[test]
fn removing_a_voter_when_every_remaining_voter_is_alive_is_never_refused_over_seeds() {
    for i in 0..5 {
        run_removing_a_voter_when_every_remaining_voter_is_alive_is_never_refused(0xC12E_8000 + i);
    }
}

// ---------------------------------------------------------------------------
// (9) concurrent_control_add_surfaces_in_flight_as_a_clean_retryable_error
// ---------------------------------------------------------------------------

fn run_concurrent_control_add_surfaces_in_flight_as_a_clean_retryable_error(seed: u64) {
    let mut cluster = SimCluster::new_with_roles(seed, &[NodeRole::Both; 3], 1);
    let leader = cluster.control_leader_index() as u64;

    let body1 =
        serde_json::json!({"node": nid(10).to_string(), "addr": PLACEHOLDER_ADDR}).to_string();
    let body2 =
        serde_json::json!({"node": nid(11).to_string(), "addr": PLACEHOLDER_ADDR}).to_string();
    let (raw1, raw2) = admin_join2(
        &mut cluster,
        leader,
        ("POST", "/admin/control/member/add", body1.as_bytes()),
        ("POST", "/admin/control/member/add", body2.as_bytes()),
    );
    let r1 = (raw1.0, json(&raw1.1));
    let r2 = (raw2.0, json(&raw2.1));

    let successes = [&r1, &r2]
        .iter()
        .filter(|(status, _)| *status == 200)
        .count();
    assert_eq!(
        successes, 1,
        "seed={seed}: exactly one of two concurrent control-add calls should win: r1={r1:?}, r2={r2:?}"
    );
    let (loser_status, loser_body) = [&r1, &r2]
        .into_iter()
        .find(|(status, _)| *status != 200)
        .expect("a loser exists");
    assert_eq!(
        *loser_status, 409,
        "seed={seed}: the loser must fail cleanly, not hang/crash: {loser_body}"
    );
    let msg = loser_body["error"]
        .as_str()
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert!(
        msg.contains("flight") || msg.contains("leader") || msg.contains("retry"),
        "seed={seed}: expected a clear, retryable-sounding error for the loser, got: {msg}"
    );

    let winner_id: u64 = if r1.0 == 200 { 10 } else { 11 };
    let loser_id: u64 = if winner_id == 10 { 11 } else { 10 };

    poll_until(
        &mut cluster,
        Duration::from_secs(15),
        seed,
        "the winning control-add converging",
        |c| {
            let (status, body) = control_members(c, leader);
            status == 200 && voters_of(&body).is_some_and(|v| v.contains(&nid(winner_id)))
        },
    );

    const STEP: Duration = Duration::from_millis(150);
    let budget = Duration::from_secs(10);
    let mut elapsed = Duration::ZERO;
    loop {
        let (status, body) = add_control_member(&mut cluster, leader, loser_id, PLACEHOLDER_ADDR);
        if status == 200 {
            break;
        }
        assert!(
            elapsed < budget,
            "seed={seed}: the loser's retry should eventually succeed once the winner \
             committed, never did within {budget:?}: {body}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

#[test]
fn concurrent_control_add_surfaces_in_flight_as_a_clean_retryable_error() {
    run_concurrent_control_add_surfaces_in_flight_as_a_clean_retryable_error(env_seed(0xC12E_0009));
}

#[test]
fn concurrent_control_add_surfaces_in_flight_as_a_clean_retryable_error_over_seeds() {
    for i in 0..5 {
        run_concurrent_control_add_surfaces_in_flight_as_a_clean_retryable_error(0xC12E_9000 + i);
    }
}

// ---------------------------------------------------------------------------
// (10) omitted_node_add_mints_an_id_and_converges_to_a_live_voter
// ---------------------------------------------------------------------------

fn run_omitted_node_add_mints_an_id_and_converges_to_a_live_voter(seed: u64) {
    let mut cluster = SimCluster::new_with_roles(seed, &[NodeRole::Both; 3], 1);
    let leader = cluster.control_leader_index() as u64;

    let (status, body) = add_control_member_omitted(&mut cluster, leader, PLACEHOLDER_ADDR);
    assert_eq!(
        status, 200,
        "seed={seed}: omitted-node control/member/add failed: {body}"
    );
    let minted: NodeId = body["node"]
        .as_str()
        .expect("the response carries the minted `node`")
        .parse()
        .expect("minted node id parses");
    assert!(
        looks_minted(&minted),
        "seed={seed}: minted id {minted} should look like a NodeId::mint output"
    );

    poll_until(
        &mut cluster,
        Duration::from_secs(15),
        seed,
        "the minted voter converging",
        |c| {
            let (status, body) = control_members(c, leader);
            status == 200 && voters_of(&body).is_some_and(|v| v.contains(&minted))
        },
    );
}

#[test]
fn omitted_node_add_mints_an_id_and_converges_to_a_live_voter() {
    run_omitted_node_add_mints_an_id_and_converges_to_a_live_voter(env_seed(0xC12E_0010));
}

#[test]
fn omitted_node_add_mints_an_id_and_converges_to_a_live_voter_over_seeds() {
    for i in 0..5 {
        run_omitted_node_add_mints_an_id_and_converges_to_a_live_voter(0xC12E_A000 + i);
    }
}

// ---------------------------------------------------------------------------
// (11) concurrent_omitted_node_adds_mint_distinct_ids_and_both_become_voters
// ---------------------------------------------------------------------------

fn run_concurrent_omitted_node_adds_mint_distinct_ids_and_both_become_voters(seed: u64) {
    let mut cluster = SimCluster::new_with_roles(seed, &[NodeRole::Both; 3], 1);
    let leader = cluster.control_leader_index() as u64;

    let body = serde_json::json!({"addr": PLACEHOLDER_ADDR}).to_string();
    let (raw1, raw2) = admin_join2(
        &mut cluster,
        leader,
        ("POST", "/admin/control/member/add", body.as_bytes()),
        ("POST", "/admin/control/member/add", body.as_bytes()),
    );
    let r1 = (raw1.0, json(&raw1.1));
    let r2 = (raw2.0, json(&raw2.1));
    for (status, body) in [&r1, &r2] {
        assert!(
            *status == 200 || *status == 409,
            "seed={seed}: unexpected status for a concurrent omitted-node add: {body}"
        );
    }
    let successes: Vec<NodeId> = [&r1, &r2]
        .iter()
        .filter(|(status, _)| *status == 200)
        .map(|(_, body)| {
            body["node"]
                .as_str()
                .expect("a successful omitted-node add carries the minted `node`")
                .parse()
                .expect("minted node id parses")
        })
        .collect();
    assert_eq!(
        successes.len(),
        1,
        "seed={seed}: exactly one concurrent omitted-node add should win outright: r1={r1:?}, r2={r2:?}"
    );
    let winner_id = successes[0].clone();
    assert!(
        looks_minted(&winner_id),
        "seed={seed}: minted id {winner_id} should look like a NodeId::mint output"
    );

    poll_until(
        &mut cluster,
        Duration::from_secs(15),
        seed,
        "the winner converging",
        |c| {
            let (status, body) = control_members(c, leader);
            status == 200 && voters_of(&body).is_some_and(|v| v.contains(&winner_id))
        },
    );

    // Retry the loser: a fresh omitted-node call mints a *second*,
    // necessarily distinct, id and adds it once the winner's change has
    // cleared `config_change_in_flight`.
    const STEP: Duration = Duration::from_millis(150);
    let budget = Duration::from_secs(10);
    let mut elapsed = Duration::ZERO;
    let second_id: NodeId = loop {
        let (status, body) = add_control_member_omitted(&mut cluster, leader, PLACEHOLDER_ADDR);
        if status == 200 {
            break body["node"]
                .as_str()
                .expect("the retried add carries the minted `node`")
                .parse()
                .expect("minted node id parses");
        }
        assert!(
            elapsed < budget,
            "seed={seed}: the retried omitted-node add never succeeded within {budget:?}: {body}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    };
    assert_ne!(
        second_id, winner_id,
        "seed={seed}: the retry must mint an id distinct from the winner's"
    );
    assert!(
        looks_minted(&second_id),
        "seed={seed}: minted id {second_id} should look like a NodeId::mint output"
    );

    poll_until(
        &mut cluster,
        Duration::from_secs(15),
        seed,
        "the second minted voter converging",
        |c| {
            let (status, body) = control_members(c, leader);
            status == 200 && voters_of(&body).is_some_and(|v| v.contains(&second_id))
        },
    );
}

#[test]
fn concurrent_omitted_node_adds_mint_distinct_ids_and_both_become_voters() {
    run_concurrent_omitted_node_adds_mint_distinct_ids_and_both_become_voters(env_seed(
        0xC12E_0011,
    ));
}

#[test]
fn concurrent_omitted_node_adds_mint_distinct_ids_and_both_become_voters_over_seeds() {
    for i in 0..5 {
        run_concurrent_omitted_node_adds_mint_distinct_ids_and_both_become_voters(0xC12E_B000 + i);
    }
}

// ---------------------------------------------------------------------------
// (12) admin_config_reports_the_internal_addr_the_cli_resolves_control_add_through
// ---------------------------------------------------------------------------

fn run_admin_config_reports_the_internal_addr_the_cli_resolves_control_add_through(seed: u64) {
    let mut cluster = SimCluster::new_with_roles(seed, &[NodeRole::Both; 1], 1);
    let _ = cluster.control_leader_index();

    let (status, body) = admin_get(&mut cluster, 0, "/admin/config");
    assert_eq!(status, 200, "seed={seed}: GET /admin/config failed: {body}");

    // Mirrors `animus_cli::internal_addr_from_admin_config`'s own key path.
    let internal = body["addrs"]["internal"]
        .as_str()
        .expect("addrs.internal should be a non-empty host:port string");
    assert!(
        !internal.is_empty(),
        "seed={seed}: addrs.internal should be a real dial address, got {internal:?}"
    );
    internal
        .parse::<std::net::SocketAddr>()
        .unwrap_or_else(|e| {
            panic!("seed={seed}: addrs.internal {internal:?} should parse as host:port: {e}")
        });

    // The removed legacy field must actually be gone, not merely
    // unnecessary.
    assert!(
        body.get("control").is_none(),
        "seed={seed}: the legacy top-level `control` field should not exist any more, found: {body}"
    );
}

#[test]
fn admin_config_reports_the_internal_addr_the_cli_resolves_control_add_through() {
    run_admin_config_reports_the_internal_addr_the_cli_resolves_control_add_through(env_seed(
        0xC12E_0012,
    ));
}

#[test]
fn admin_config_reports_the_internal_addr_the_cli_resolves_control_add_through_over_seeds() {
    for i in 0..5 {
        run_admin_config_reports_the_internal_addr_the_cli_resolves_control_add_through(
            0xC12E_C000 + i,
        );
    }
}
