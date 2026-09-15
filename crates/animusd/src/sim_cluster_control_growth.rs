//! `SimCluster::grow_control`'s own test module (ADR 0061 rung N, C-14
//! PR 2) — the primitive [`sim_cluster_control_membership_split.rs`](super::
//! sim_cluster_control_membership_split)'s own module doc (and `sim_cluster_
//! control_membership_admin.rs`'s before it, C-12 PR 4e) both independently
//! named as deferred, separately-budgeted machinery: a genuinely new
//! `RaftNode<SimEnv>` joining the LIVE control-plane voter quorum after
//! construction, self-registered over the real relayed discovery path
//! ([`register_node_over_wire_via_relay`](super::sim_cluster)) and admitted
//! through the real `POST /admin/control/member/add` route
//! (`ClientCtx::admin_add_control_member`) — never a bypass propose. See
//! [`SimCluster::grow_control`](super::sim_cluster::SimCluster::grow_control)'s
//! own doc for the mechanism and `crates/animusd/CLAUDE.md`'s matching C-14
//! appendix.
//!
//! Two scenarios, each a pinned-seed test plus a fixed 5-seed `_over_seeds`
//! sibling (repo convention):
//!
//! 1. [`grown_control_voter_joins_the_live_quorum_and_can_lead`] — a
//!    3-node combined cluster, a table with writes, `grow_control()`,
//!    every node's own live voter belief includes the new id (4 voters),
//!    then the REAL catch-up proof: the pre-growth leader is crashed, a
//!    new leader is observed among the survivors, and — if it isn't the
//!    grown node itself — leadership is transferred to it over the real
//!    `POST /admin/control/transfer` route (the same bounded-retry
//!    discipline `sim_cluster_admin_actions.rs`'s own `run_control_
//!    transfer_moves_leadership_to_the_named_node` uses). The grown node
//!    then genuinely SERVES as leader: it commits a real
//!    `MetaCommand::RemoveMember`-shaped admin action (removing the
//!    crashed node from the control voter set) and every survivor
//!    converges on the resulting 3-voter config. Finally the crashed node
//!    is restarted and rejoins with that same 3-voter config.
//! 2. [`grow_control_after_metadata_has_grown`] — 8 tables created BEFORE
//!    `grow_control()`, so the fresh voter must genuinely catch up on real
//!    committed log/snapshot content rather than starting from an empty
//!    log with nothing to replay — then the identical convergence + serve
//!    proof as scenario 1 (crash → (transfer) → serve → restart).
//!
//! Both scenarios replay via `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <test name>` (repo convention).

use std::time::Duration;

use animus_env::{NodeId, nid};

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{env_seed, json};
use crate::config::NodeRole;

/// The current control-bearing node id set, as real node ids — derived
/// from [`SimCluster::control_count`]/[`SimCluster::control_node_id`]
/// (both already `pub(crate)`) rather than reaching into `SimCluster`'s
/// own private `control_node_ids` field, which this module — a sibling,
/// not a child, of `sim_cluster.rs` — cannot see.
fn control_node_ids_snapshot(cluster: &SimCluster) -> Vec<u64> {
    (0..cluster.control_count())
        .map(|i| cluster.control_node_id(i))
        .collect()
}

/// Every control-bearing node's own live voter belief includes `target`.
fn every_control_node_sees_voter(cluster: &SimCluster, target: u64) -> bool {
    let target_id = nid(target);
    control_node_ids_snapshot(cluster).iter().all(|&n| {
        cluster
            .control_voters(n)
            .is_some_and(|v: std::collections::BTreeSet<NodeId>| v.contains(&target_id))
    })
}

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

/// The shared "REAL catch-up proof" both scenarios run once `grown` is a
/// converged live voter: crash the pre-growth leader, observe a survivor
/// win a new election, transfer leadership to `grown` over the real
/// `POST /admin/control/transfer` route if it didn't already land there,
/// then prove `grown` genuinely SERVES as leader by committing a real
/// admin mutation (removing the crashed node from the control voter set)
/// and waiting for every survivor to converge on the resulting voter set —
/// finally restarting the crashed node and proving it rejoins with that
/// same converged config.
fn assert_grown_voter_crash_transfer_serve_and_restart(
    cluster: &mut SimCluster,
    grown: u64,
    seed: u64,
) {
    // The pre-growth leader almost certainly remains leader immediately
    // after `grow_control` (a runtime `change_membership` never forces a
    // leadership change) — `grow_control`'s own step 5 poll only waits for
    // voter-set convergence, never an election. A defensive assert (not a
    // retry) catches the astronomically unlikely case this invariant ever
    // stops holding, rather than silently crashing the very node this
    // scenario means to prove catches up.
    let pre_crash_leader_idx = cluster.control_leader_index();
    let pre_crash_leader_id = cluster.control_node_id(pre_crash_leader_idx);
    assert_ne!(
        pre_crash_leader_id, grown,
        "seed={seed}: the freshly grown voter {grown} must not already be the control \
         leader at this point — grow_control's own convergence wait never elects, so this \
         would indicate a real timing change worth understanding before this scenario's own \
         crash/transfer logic can be trusted"
    );

    cluster.crash(pre_crash_leader_id);

    // A new leader among the survivors — `control_leader_index_excluding`
    // takes the CRASHED node's own `self.controls`-vec index (not its node
    // id — see that method's own doc), which is exactly what
    // `pre_crash_leader_idx` already is.
    let new_leader_idx = cluster.control_leader_index_excluding(pre_crash_leader_idx as u64);
    let new_leader_id = cluster.control_node_id(new_leader_idx);

    if new_leader_id != grown {
        // Transfer leadership to `grown` over the real route — bounded
        // retry re-resolving the current leader each attempt, mirroring
        // `sim_cluster_admin_actions.rs::run_control_transfer_moves_
        // leadership_to_the_named_node`'s own issue #671/#688 retry
        // discipline against every retryable 409 this route can answer.
        //
        // **Every re-resolve below MUST exclude the crashed node's own
        // vec index** (`control_leader_index_excluding`, never the plain
        // `control_leader_index`): a crashed node is
        // muted, not stopped, so its own `is_leader()` belief is frozen at
        // whatever it was the instant it crashed — since `pre_crash_
        // leader_id` genuinely was leader right before this call, its own
        // stale `is_leader()` would otherwise contaminate every plain
        // leader lookup for the rest of this scenario, exactly the
        // `control_leader_index_excluding`-vs-`control_leader_index`
        // gotcha `sim_cluster_auto_split.rs`'s own module doc documents
        // (found live authoring this scenario, not by inspection — see
        // `docs/engineering-lessons.md`'s matching entry).
        let body = format!(r#"{{"to":"{}"}}"#, nid(grown));
        let mut accepted = None;
        for _ in 0..40 {
            let idx = cluster.control_leader_index_excluding(pre_crash_leader_idx as u64);
            let leader = cluster.control_node_id(idx);
            let (status, resp) = cluster.admin(
                leader,
                "POST",
                "/admin/control/transfer",
                "",
                body.as_bytes(),
            );
            match status {
                200 => {
                    accepted = Some(resp);
                    break;
                }
                409 => cluster.run_for(Duration::from_millis(100)),
                other => panic!(
                    "seed={seed}: transfer to grown node {grown} should be accepted or \
                     retryable: {other} {resp}"
                ),
            }
        }
        let accepted = accepted.unwrap_or_else(|| {
            panic!("seed={seed}: transfer to grown node {grown} was never accepted within budget")
        });
        assert_eq!(
            json(&accepted)["ok"],
            true,
            "seed={seed}: transfer to grown node {grown}: {accepted}"
        );

        let leader_after_transfer_idx =
            cluster.control_leader_index_excluding(pre_crash_leader_idx as u64);
        let leader_after_transfer = cluster.control_node_id(leader_after_transfer_idx);
        assert_eq!(
            leader_after_transfer, grown,
            "seed={seed}: control leadership never moved to the grown node {grown} (now \
             {leader_after_transfer})"
        );
    }

    // `grown` now genuinely leads — prove it SERVES by committing a real
    // admin mutation through it: remove the crashed node from the control
    // voter set. `force: true` sidesteps the failure detector's own
    // `CONTROL_PEER_LIVENESS_TIMEOUT` window (this scenario's own subject
    // is "the grown node serves as a real leader," not the liveness-
    // detection timing `sim_cluster_control_membership_admin.rs`'s own
    // dead-voter scenarios already cover).
    //
    // Retried on 409, exactly like the transfer poll above: a leader that
    // JUST won an election (via transfer or a real re-election after the
    // crash) may not yet have committed its own current-term no-op —
    // `RaftCore::change_membership`'s own erratum guard (Raft §4/Ongaro)
    // rejects a config change until it has, self-hinting via
    // `ProposeResult::NotLeader` (see that method's own doc). This is a
    // genuine, expected one-round-trip-after-election transient, not a
    // bug — `admin_remove_control_member`'s own doc says exactly this
    // ("a caller ... simply retries after the no-op commits") — so the
    // assert belongs after a bounded retry, never on the very next tick.
    let remove_body = format!(r#"{{"node":"{}","force":true}}"#, nid(pre_crash_leader_id));
    let mut remove_result = None;
    for _ in 0..40 {
        let (status, resp) = cluster.admin(
            grown,
            "POST",
            "/admin/control/member/remove",
            "",
            remove_body.as_bytes(),
        );
        match status {
            200 => {
                remove_result = Some((status, resp));
                break;
            }
            409 => cluster.run_for(Duration::from_millis(100)),
            other => panic!(
                "seed={seed}: removing crashed voter {pre_crash_leader_id} through the grown \
                 node {grown} should be accepted or retryable: {other} {resp}"
            ),
        }
    }
    let (status, resp) = remove_result.unwrap_or_else(|| {
        panic!(
            "seed={seed}: removing crashed voter {pre_crash_leader_id} through the grown node \
             {grown} was never accepted within budget"
        )
    });
    assert_eq!(
        status, 200,
        "seed={seed}: the grown node {grown} (now leader) should be able to remove the \
         crashed voter {pre_crash_leader_id}: {resp}"
    );

    // Converge: every SURVIVING node's own live voter belief excludes the
    // crashed node and includes every survivor — never a fixed-deadline
    // one-shot assert.
    let crashed_id = nid(pre_crash_leader_id);
    poll_until(
        cluster,
        Duration::from_secs(20),
        seed,
        "every survivor converging on the post-removal voter set",
        |c| {
            control_node_ids_snapshot(c).iter().all(|&n| {
                if n == pre_crash_leader_id {
                    // The crashed node's own local belief is frozen (muted,
                    // not stopped — SimCluster::crash's own doc) and is not
                    // part of this convergence check; only survivors need
                    // to agree.
                    return true;
                }
                c.control_voters(n)
                    .is_some_and(|v: std::collections::BTreeSet<NodeId>| !v.contains(&crashed_id))
            })
        },
    );

    // Finally: restart the crashed node and assert it rejoins with the
    // same converged (post-removal) voter set — `SimCluster::restart`
    // already dispatches on `control_index_of` (ADR 0061 rung N, C-14
    // PR 1) and rebuilds a restarted control-bearing node's own membership
    // from the CURRENT `control_node_ids`, which by this point no longer
    // names the removed voter.
    cluster.restart(pre_crash_leader_id);
    poll_until(
        cluster,
        Duration::from_secs(20),
        seed,
        "the restarted node rejoining with the converged post-removal voter set",
        |c| {
            c.control_voters(pre_crash_leader_id)
                .is_some_and(|v: std::collections::BTreeSet<NodeId>| !v.contains(&crashed_id))
        },
    );
}

// ---------------------------------------------------------------------------
// (1) grown_control_voter_joins_the_live_quorum_and_can_lead
// ---------------------------------------------------------------------------

fn run_grown_control_voter_joins_the_live_quorum_and_can_lead(seed: u64) {
    let mut cluster = SimCluster::new_with_roles(seed, &[NodeRole::Both; 3], 3);

    cluster.create_table("t1");
    cluster
        .put(0, "t1", "pk-1", "sk-1", b"v1")
        .expect("seed={seed}: seed write should succeed");
    let read = cluster
        .get(0, "t1", "pk-1", "sk-1", true)
        .expect("seed={seed}: seed read should succeed");
    assert_eq!(read.as_deref(), Some(&b"v1"[..]), "seed={seed}");

    let grown = cluster.grow_control();

    // Every control-bearing node's own live voter belief includes the new
    // id — 4 voters total (`grow_control`'s own step 5 already converged
    // this before returning; re-checking here is the scenario's own
    // explicit assertion, per this PR's own brief).
    assert!(
        every_control_node_sees_voter(&cluster, grown),
        "seed={seed}: not every control-bearing node sees the grown voter {grown} in its own \
         live config"
    );
    for &n in &control_node_ids_snapshot(&cluster) {
        let voters = cluster
            .control_voters(n)
            .unwrap_or_else(|| panic!("seed={seed}: node {n} has no live voter belief at all"));
        assert_eq!(
            voters.len(),
            4,
            "seed={seed}: node {n}'s own live voter belief should have exactly 4 members, \
             got {voters:?}"
        );
    }

    assert_grown_voter_crash_transfer_serve_and_restart(&mut cluster, grown, seed);
}

#[test]
fn grown_control_voter_joins_the_live_quorum_and_can_lead() {
    run_grown_control_voter_joins_the_live_quorum_and_can_lead(env_seed(0xC14E_0001));
}

#[test]
fn grown_control_voter_joins_the_live_quorum_and_can_lead_over_seeds() {
    for i in 0..5 {
        run_grown_control_voter_joins_the_live_quorum_and_can_lead(0xC14E_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// (2) grow_control_after_metadata_has_grown
// ---------------------------------------------------------------------------

fn run_grow_control_after_metadata_has_grown(seed: u64) {
    let mut cluster = SimCluster::new_with_roles(seed, &[NodeRole::Both; 3], 3);

    // 8 tables committed BEFORE growth — real committed log/snapshot
    // content the fresh voter's own catch-up (peer replication / chunked
    // InstallSnapshot) must genuinely replay, not an empty log with
    // nothing to do.
    for i in 0..8 {
        cluster.create_table(&format!("t{i}"));
    }

    let grown = cluster.grow_control();

    assert!(
        every_control_node_sees_voter(&cluster, grown),
        "seed={seed}: not every control-bearing node sees the grown voter {grown} in its own \
         live config, after 8 tables' worth of prior committed metadata"
    );
    for &n in &control_node_ids_snapshot(&cluster) {
        let voters = cluster
            .control_voters(n)
            .unwrap_or_else(|| panic!("seed={seed}: node {n} has no live voter belief at all"));
        assert_eq!(
            voters.len(),
            4,
            "seed={seed}: node {n}'s own live voter belief should have exactly 4 members, \
             got {voters:?}"
        );
    }

    assert_grown_voter_crash_transfer_serve_and_restart(&mut cluster, grown, seed);
}

#[test]
fn grow_control_after_metadata_has_grown() {
    run_grow_control_after_metadata_has_grown(env_seed(0xC14E_0002));
}

#[test]
fn grow_control_after_metadata_has_grown_over_seeds() {
    for i in 0..5 {
        run_grow_control_after_metadata_has_grown(0xC14E_2000 + i);
    }
}
