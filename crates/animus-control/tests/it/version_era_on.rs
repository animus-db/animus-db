//! ADR 0073 Phase 2 (P2-A) era-on behavior: the handshake refusal of Phase 1
//! peers (`Network::set_require_peer_ext`, flipped by the control apply task
//! once the replicated era marker is seen) and the startup / install /
//! finalize **cluster-version range check** that halts a `RaftNode` with a
//! named `halt_reason`.
//!
//! Cells (seeded; `ANIMUS_SEED=<seed>` replays one; depth via
//! `ANIMUS_CONTROL_SEEDS`; every assertion prints the seed):
//! - `era_off_clusters_never_refuse_anybody`: regression. Phase 1 and mixed
//!   (half-upgraded) clusters, under faults, never record a single protocol
//!   refusal and keep committing.
//! - `a_phase1_reinstall_after_the_era_is_refused_and_never_wedges_the_rest`:
//!   after the era starts, a node (a control follower or a heartbeat-only
//!   member) whose `ext` is set back to empty is refused by its era-on peers
//!   (`protocol_refusals`), looks partitioned (a control follower stops
//!   advancing; a member goes `Down`), the others keep committing, and it
//!   rejoins when it advertises B2 again.
//! - `a_node_whose_range_excludes_the_cluster_version_halts_with_a_named_reason`:
//!   restart into a cluster at version 2 with own range `[1,1]` (above max) or
//!   `[3,4]` (below min); a stale node halts when a committed
//!   `FinalizeClusterVersion` moves the version out of its range; negative
//!   control: nodes whose range contains the version never halt.
//! - `a_snapshot_install_that_carries_an_out_of_range_version_halts_the_node`:
//!   the same check on the `InstallSnapshot` path (the node only learns the
//!   version through a snapshot), with a negative control.

use std::time::Duration;

use animus_control::version::VersionRange;
use animus_control::{MetaCommand, NodeStatus};
use animus_env::nid;

use super::version_world::{Fault, World, era, seeds};

const VOTERS: [u64; 3] = [0, 1, 2];
const LEARNER: u64 = 3;
const MEMBERS: [u64; 2] = [10, 11];

fn all_ids() -> Vec<u64> {
    VOTERS
        .iter()
        .chain(&[LEARNER])
        .chain(&MEMBERS)
        .copied()
        .collect()
}

fn refusals(w: &World, id: u64) -> u64 {
    w.sim.protocol_refusals(&nid(id))
}

fn upsert(n: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(n),
        labels: Default::default(),
        status: NodeStatus::Down,
    }
}

fn era_everywhere(w: &World) -> bool {
    w.nodes.values().all(|n| era(&n.metadata()))
}

// ---- era off: nobody is ever refused ----

fn run_era_off(seed: u64) {
    let mut w = World::new(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    let mut check = |w: &World| {
        for id in all_ids() {
            assert_eq!(
                w.sim.protocol_refusals(&nid(id)),
                0,
                "seed={}: node {id} refused a peer with the era off",
                w.seed
            );
        }
    };
    // Half upgraded, the rest Phase 1: P cannot hold, the era stays off.
    for id in [0, 10, 3] {
        w.flip(id, false);
    }
    for round in 0..6u64 {
        let f = if round % 2 == 0 {
            Fault::Lossy
        } else {
            Fault::LeaderKill
        };
        if let Some(h) = w.inject(f) {
            w.run(Duration::from_millis(700), &mut check);
            w.heal(h);
        }
        w.run(Duration::from_millis(500), &mut check);
        w.propose_confirmed(
            &upsert(100 + round),
            &|m| m.members.contains_key(&nid(100 + round)),
            "a commit with the era off",
        );
    }
    assert!(
        w.nodes.values().all(|n| !era(&n.metadata())),
        "seed={seed}: era started in a half-upgraded cluster"
    );
}

#[test]
fn era_off_clusters_never_refuse_anybody() {
    for seed in seeds("version_era_off", 8) {
        run_era_off(seed);
    }
}

// ---- Phase 1 reinstall after the era ----

fn run_reinstall(seed: u64, member_victim: bool) {
    let mut w = World::new(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    for id in all_ids() {
        w.flip(id, false);
    }
    let mut check = |_: &World| {};
    w.poll(
        Duration::from_secs(40),
        "era on everywhere",
        &mut check,
        &era_everywhere,
    );
    w.run(Duration::from_secs(2), &mut check);
    for id in all_ids() {
        assert_eq!(
            refusals(&w, id),
            0,
            "seed={seed}: B2 peers refused each other after the era"
        );
    }
    let lossy = seed.is_multiple_of(2);
    if lossy {
        let mut cfg = animus_sim::NetConfig::default();
        cfg.set_drop_prob(0.05);
        w.sim.set_net_config(cfg);
    }

    let leader = w.leader().expect("leader");
    let victim = if member_victim {
        10
    } else {
        *VOTERS.iter().find(|&&v| v != leader).expect("follower")
    };
    // The operator reinstalls a Phase 1 binary: empty `ext`.
    w.sim.set_network_ext_for(nid(victim), Vec::new());
    w.run(Duration::from_secs(3), &mut check);

    // Refused by its era-on peers, in both directions.
    let leader = w.leader().expect("leader");
    assert!(
        refusals(&w, leader) > 0,
        "seed={seed}: the leader never refused the Phase 1 node {victim}"
    );
    if !member_victim {
        assert!(
            refusals(&w, victim) > 0,
            "seed={seed}: the Phase 1 control node {victim} was not refused by peers"
        );
    }

    // The rest keeps committing: no wedge.
    w.propose_confirmed(
        &upsert(900),
        &|m| m.members.contains_key(&nid(900)),
        "a commit while a Phase 1 node is refused",
    );
    w.run(Duration::from_secs(2), &mut check);
    if member_victim {
        // Its heartbeats are dropped: the failure detector marks it Down.
        w.poll(
            Duration::from_secs(20),
            "refused member marked Down",
            &mut check,
            &|w| {
                w.leader_meta()
                    .is_some_and(|m| m.members[&nid(10)].status == NodeStatus::Down)
            },
        );
    } else {
        // It looks partitioned: everyone else has the entry, it does not.
        for (&id, n) in &w.nodes {
            let has = n.metadata().members.contains_key(&nid(900));
            if id == victim {
                assert!(
                    !has,
                    "seed={seed}: the refused node {victim} still replicated"
                );
            } else if id != LEARNER {
                assert!(has, "seed={seed}: node {id} missed a commit");
            }
        }
    }

    // It comes back as B2 and catches up; refusals stop.
    w.sim
        .set_network_ext_for(nid(victim), super::version_world::b2_ext());
    w.poll(
        Duration::from_secs(40),
        "refused node rejoined",
        &mut check,
        &|w| {
            let ok_member = w
                .leader_meta()
                .is_some_and(|m| m.members[&nid(10)].status == NodeStatus::Active);
            let ok_ctl =
                member_victim || w.nodes[&victim].metadata().members.contains_key(&nid(900));
            ok_member && ok_ctl
        },
    );
    let before: u64 = all_ids().iter().map(|&i| refusals(&w, i)).sum();
    w.run(Duration::from_secs(2), &mut check);
    let after: u64 = all_ids().iter().map(|&i| refusals(&w, i)).sum();
    assert_eq!(
        before, after,
        "seed={seed}: refusals continued after B2 was restored"
    );
}

#[test]
fn a_phase1_reinstall_after_the_era_is_refused_and_never_wedges_the_rest() {
    for (i, seed) in seeds("version_era_reinstall", 8).into_iter().enumerate() {
        run_reinstall(seed, i % 4 == 3);
    }
}

// ---- range check ----

const V12: (u32, u32) = (1, 2);

/// A cluster whose every node advertises `[1,2]`, era on and every node
/// recorded, still at cluster version 1.
fn world_at_v1_with_wide_ranges(seed: u64) -> World {
    let mut w = World::new(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    for id in all_ids() {
        w.flip_range(id, V12, false);
    }
    let mut check = |_: &World| {};
    w.poll(
        Duration::from_secs(40),
        "era on with [1,2] records everywhere",
        &mut check,
        &|w| {
            w.nodes.values().all(|n| {
                let m = n.metadata();
                era(&m)
                    && m.node_versions.len() == all_ids().len()
                    && m.node_versions
                        .values()
                        .all(|v| v.range == VersionRange::new(1, 2))
            })
        },
    );
    w
}

fn finalize_to_2(w: &mut World) {
    w.propose_confirmed(
        &MetaCommand::FinalizeClusterVersion {
            expected: 1,
            target: 2,
        },
        &|m| m.cluster_version() == 2,
        "FinalizeClusterVersion 1 -> 2",
    );
}

fn run_range_check(seed: u64) {
    let mut w = world_at_v1_with_wide_ranges(seed);
    let mut check = |_: &World| {};
    let leader = w.leader().expect("leader");
    let followers: Vec<u64> = VOTERS.iter().copied().filter(|&v| v != leader).collect();
    let (stale, healthy) = (followers[0], followers[1]);

    // A stale node: its own range is [1,1] although the cluster still holds
    // its earlier [1,2] record. Version 1 is inside it, so no halt yet.
    w.nodes[&stale].set_own_version_range(Some(VersionRange::new(1, 1)));
    w.run(Duration::from_secs(1), &mut check);
    assert!(
        w.nodes.values().all(|n| !n.is_halted()),
        "seed={seed}: halted below the finalize"
    );
    finalize_to_2(&mut w);
    w.poll(
        Duration::from_secs(20),
        "stale node halts on the committed finalize",
        &mut check,
        &|w| w.nodes[&stale].is_halted(),
    );
    assert_eq!(
        w.nodes[&stale].halt_reason().as_deref(),
        Some("cluster version 2 is above this binary's max 1 (downgrade is not supported)"),
        "seed={seed}"
    );
    // Negative control: every node whose range contains 2 keeps running.
    w.run(Duration::from_secs(2), &mut check);
    for (&id, n) in &w.nodes {
        if id != stale {
            assert!(
                !n.is_halted() && n.halt_reason().is_none(),
                "seed={seed}: node {id} (range [1,2]) halted at version 2"
            );
        }
    }

    // Restart into the version-2 cluster with a range that excludes it.
    w.restart_with_own(healthy, Some(VersionRange::new(1, 1)));
    w.run(Duration::from_millis(500), &mut check);
    assert_eq!(
        w.nodes[&healthy].halt_reason().as_deref(),
        Some("cluster version 2 is above this binary's max 1 (downgrade is not supported)"),
        "seed={seed}: boot-time check (above max)"
    );
    w.restart_with_own(healthy, Some(VersionRange::new(3, 4)));
    w.run(Duration::from_millis(500), &mut check);
    assert_eq!(
        w.nodes[&healthy].halt_reason().as_deref(),
        Some(
            "cluster version 2 is below this binary's min 3 \
             (upgrade through a release whose range contains 2 first)"
        ),
        "seed={seed}: boot-time check (below min)"
    );
    // Negative control: restart with a containing range runs on.
    w.restart_with_own(healthy, Some(VersionRange::new(1, 2)));
    w.run(Duration::from_secs(2), &mut check);
    assert!(
        !w.nodes[&healthy].is_halted() && w.nodes[&healthy].halt_reason().is_none(),
        "seed={seed}: a node whose range contains the version halted"
    );
    // A Phase 1 profile (no own range) never halts on a version check.
    w.restart_with_own(healthy, None);
    w.run(Duration::from_secs(1), &mut check);
    assert!(!w.nodes[&healthy].is_halted(), "seed={seed}");
}

#[test]
fn a_node_whose_range_excludes_the_cluster_version_halts_with_a_named_reason() {
    for seed in seeds("version_range_check", 6) {
        run_range_check(seed);
    }
}

// ---- snapshot install path ----

fn run_install(seed: u64, stale: bool) {
    let mut w = world_at_v1_with_wide_ranges(seed);
    let mut check = |_: &World| {};
    let leader = w.leader().expect("leader");
    let z = *VOTERS.iter().find(|&&v| v != leader).expect("follower");
    if stale {
        w.nodes[&z].set_own_version_range(Some(VersionRange::new(1, 1)));
    }
    // Z is down while the version moves and the log is compacted past it.
    w.sim.crash(nid(z));
    finalize_to_2(&mut w);
    let mut n = 0u64;
    let mut compacted = false;
    for _ in 0..400 {
        let leader = w.leader().expect("leader");
        for _ in 0..20 {
            let _ = w.nodes[&leader].propose(upsert(1000 + n));
            n += 1;
        }
        w.sim.run_for(Duration::from_millis(100));
        if w.nodes[&leader].snapshot_index() > w.nodes[&z].last_log_index() {
            compacted = true;
            break;
        }
    }
    assert!(
        compacted,
        "seed={seed}: the leader never compacted past node {z}"
    );
    assert!(!w.nodes[&z].is_halted(), "seed={seed}: halted while down");
    w.sim.restart(nid(z));
    if stale {
        w.poll(
            Duration::from_secs(60),
            "the stale node halts after installing the snapshot",
            &mut check,
            &|w| w.nodes[&z].is_halted(),
        );
        assert_eq!(
            w.nodes[&z].halt_reason().as_deref(),
            Some("cluster version 2 is above this binary's max 1 (downgrade is not supported)"),
            "seed={seed}"
        );
    } else {
        w.poll(
            Duration::from_secs(60),
            "the node installs the snapshot",
            &mut check,
            &|w| w.nodes[&z].metadata().cluster_version() == 2,
        );
        w.run(Duration::from_secs(2), &mut check);
        assert!(
            !w.nodes[&z].is_halted(),
            "seed={seed}: a node whose range contains 2 halted"
        );
    }
    assert!(
        w.nodes[&z].snapshot_index() > 0,
        "seed={seed}: node {z} caught up without a snapshot install"
    );
}

#[test]
fn a_snapshot_install_that_carries_an_out_of_range_version_halts_the_node() {
    for (i, seed) in seeds("version_install_check", 4).into_iter().enumerate() {
        run_install(seed, i % 2 == 0);
    }
}
