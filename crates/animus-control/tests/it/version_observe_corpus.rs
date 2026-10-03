//! Seed-reproducible fault-injection corpus for the ADR 0073 Phase 2 (P2-A)
//! leader-local version observation, precondition P and era start
//! (`animus_control::version_observe`, `RaftNode`'s version loop).
//!
//! Cells (each runs `ANIMUS_CONTROL_SEEDS` x variants seeds; replay one with
//! `ANIMUS_SEED=<seed>`; every assertion prints the seed):
//!
//! - (a) `rolling_phase1_to_b2`: every node starts as a Phase 1 binary
//!   (empty `ext`), then is upgraded one at a time in a random order (half the
//!   time with a restart) while leader kills, leader partitions, lossy links
//!   and a cut-off member are injected. **Safety, checked every 25 ms on every
//!   control node:** the era is never active while any required node is still
//!   Phase 1. **Liveness (converged-or-timeout poll):** the era starts and
//!   every node of every role is recorded with its range and build.
//! - (b) `unobserved_member_blocks_era`: a registered member that is down /
//!   never observed, or comes back as a Phase 1 binary, blocks the era until
//!   it comes back as B2 or is removed.
//! - (c) `every_role_is_observed`: residual risk #1 at the `RaftNode` level
//!   (the node-assembly level is `animusd`'s `sim_cluster` test): a control-only
//!   voter that never heartbeats (seen via Raft traffic), a learner, and a
//!   heartbeat-only member are all in the leader's table with their range;
//!   Phase 1 peers are in it as `range: None`.
//! - (d) `phase1_profile_leader_never_proposes`: a leader whose own range is
//!   `None` never starts the era even though every peer advertises B2; a B2
//!   successor does.
//! - (e) `leader_change_as_p_holds`: the leader is killed at a swept offset
//!   after the last node upgrades; the era starts exactly once, records are
//!   consistent, and the log stops growing.
//! - (f) `era_on_upkeep`: after the era, a node advertising a new build is
//!   re-recorded with a bounded number of log entries (no proposal storm) and
//!   a node registered later is recorded too.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::version::VersionRange;
use animus_control::{MetaCommand, NodeStatus};
use animus_env::handshake::encode_ext;
use animus_env::{EnvExt, NodeId, nid};

use super::version_world::{Fault, Rng, World, b2_ext, era, records, seeds};

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

fn nid_set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

/// Safety, evaluated on every control node: the era is only ever active when
/// every node of its required set is currently B2; per node, once active it
/// stays active (a restarted node's view is excluded for the run).
fn era_safety(w: &World) {
    let flipped = nid_set(&w.flipped.iter().copied().collect::<Vec<_>>());
    for (id, n) in &w.nodes {
        let m = n.metadata();
        if era(&m) {
            for r in m.required_version_set() {
                assert!(
                    flipped.contains(&r),
                    "seed={}: era active on node {id} while required node {r:?} is still a \
                     Phase 1 binary (flipped={:?})",
                    w.seed,
                    w.flipped
                );
            }
            assert_eq!(m.cluster_version(), 1, "seed={}", w.seed);
        } else {
            assert!(
                m.node_versions.is_empty(),
                "seed={}: records without the era on node {id}",
                w.seed
            );
        }
    }
}

fn shuffled(rng: &mut Rng, mut v: Vec<u64>) -> Vec<u64> {
    for i in (1..v.len()).rev() {
        let j = rng.below(i as u64 + 1) as usize;
        v.swap(i, j);
    }
    v
}

fn pick_fault(rng: &mut Rng) -> Fault {
    match rng.below(4) {
        0 => Fault::LeaderKill,
        1 => Fault::PartitionLeader,
        2 => Fault::Lossy,
        _ => Fault::CutMember,
    }
}

fn ms(rng: &mut Rng, lo: u64, hi: u64) -> Duration {
    Duration::from_millis(lo + rng.below(hi - lo + 1))
}

/// Every id in `ids` is recorded as `(1,1,"b2")` on every control node and the
/// era is on there; all control nodes agree.
fn fully_recorded(w: &World, ids: &[u64]) -> bool {
    let want = nid_set(ids);
    let mut first = None;
    for n in w.nodes.values() {
        let m = n.metadata();
        if !era(&m) {
            return false;
        }
        let r = records(&m);
        if r.keys().cloned().collect::<BTreeSet<_>>() != want {
            return false;
        }
        if !r
            .values()
            .all(|(range, b)| *range == VersionRange::new(1, 1) && b == "b2")
        {
            return false;
        }
        match &first {
            None => first = Some(r),
            Some(f) if *f != r => return false,
            Some(_) => {}
        }
    }
    true
}

fn run_rolling(seed: u64) {
    let mut w = World::new(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    let mut check = |w: &World| era_safety(w);
    w.run(Duration::from_millis(500), &mut check);
    assert!(
        w.nodes.values().all(|n| !era(&n.metadata())),
        "seed={seed}: era on at the start"
    );
    let order = shuffled(&mut w.rng.fork(), all_ids());
    for id in order {
        for _ in 0..w.rng.below(3) {
            let f = pick_fault(&mut w.rng);
            if let Some(h) = w.inject(f) {
                let d = ms(&mut w.rng, 100, 900);
                w.run(d, &mut check);
                w.heal(h);
            }
            let d = ms(&mut w.rng, 50, 400);
            w.run(d, &mut check);
        }
        let restart = w.rng.chance(50);
        w.flip(id, restart);
        let d = ms(&mut w.rng, 50, 600);
        w.run(d, &mut check);
    }
    w.heal_all();
    let ids = all_ids();
    w.poll(
        Duration::from_secs(60),
        "era started and every node recorded",
        &mut check,
        &|w| fully_recorded(w, &ids),
    );
}

#[test]
fn rolling_phase1_to_b2_never_starts_the_era_early_and_eventually_starts_it() {
    for seed in seeds("version_observe_rolling", 12) {
        run_rolling(seed);
    }
}

// ---- (b) ----

fn run_unobserved(seed: u64, variant: u64) {
    let mut w = World::new(seed, &VOTERS, &[LEARNER], &[10, 11, 12]);
    // Member 12 is registered (below) but its process never runs.
    w.sim.stop(nid(12));
    w.bootstrap();
    let mut check = |w: &World| era_safety(w);
    for id in shuffled(&mut w.rng.fork(), vec![0, 1, 2, 3, 10, 11]) {
        let restart = w.rng.chance(50);
        w.flip(id, restart);
        let d = ms(&mut w.rng, 50, 300);
        w.run(d, &mut check);
    }
    // Never observed: the era must not start, however long we wait. (Safety
    // alone would not catch it: 12 is unflipped, so `era_safety` panics.)
    w.run(Duration::from_secs(6), &mut check);
    assert!(
        w.nodes.values().all(|n| !era(&n.metadata())),
        "seed={seed}: era started with an unobserved registered member"
    );
    let mut ids = vec![0, 1, 2, 3, 10, 11];
    match variant {
        0 => {
            // It comes back as a Phase 1 binary: still blocks.
            w.sim.stop(nid(12));
            let env = w.sim.env(nid(12));
            env.spawn_task(animus_control::node::heartbeat_loop(
                env.clone(),
                VOTERS.iter().copied().map(nid).collect(),
            ));
            w.run(Duration::from_secs(4), &mut check);
            assert!(
                w.nodes.values().all(|n| !era(&n.metadata())),
                "seed={seed}: era started with a Phase 1 member heartbeating"
            );
            w.flip(12, true);
            ids.push(12);
        }
        1 => {
            // It comes back as B2.
            w.flip(12, true);
            ids.push(12);
        }
        _ => {
            // It is removed (Down, unreferenced): the required set shrinks.
            w.propose_confirmed(
                &MetaCommand::RemoveMember { node: nid(12) },
                &|m| !m.members.contains_key(&nid(12)),
                "removal of the down member",
            );
        }
    }
    w.poll(
        Duration::from_secs(60),
        "era started after the blocker resolved",
        &mut check,
        &|w| fully_recorded(w, &ids),
    );
}

#[test]
fn an_unobserved_or_phase1_registered_member_blocks_the_era_until_b2_or_removed() {
    for (i, seed) in seeds("version_observe_unobserved", 9)
        .into_iter()
        .enumerate()
    {
        run_unobserved(seed, (i % 3) as u64);
    }
}

// ---- (c) ----

fn run_roles(seed: u64) {
    let mut w = World::new(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    // Phase 1 first: every peer must be in the leader's table as `None`.
    w.sim.run_for(Duration::from_secs(1));
    let l = w.leader().expect("leader");
    let obs = w.nodes[&l].version_observations();
    let peers: Vec<u64> = all_ids().into_iter().filter(|&i| i != l).collect();
    for p in &peers {
        let o = obs
            .get(&nid(*p))
            .unwrap_or_else(|| panic!("seed={seed}: leader {l} never observed node {p}"));
        assert_eq!(o.range, None, "seed={seed}: node {p} read as B2 pre-flip");
    }
    // Then everyone B2 (no restarts), under a lossy network.
    for id in all_ids() {
        w.flip(id, false);
    }
    let mut cfg = animus_sim::NetConfig::default();
    cfg.set_drop_prob(0.1);
    w.sim.set_net_config(cfg);
    let mut check = |_: &World| {};
    w.poll(
        Duration::from_secs(30),
        "leader table holds every role with its B2 range",
        &mut check,
        &|w| {
            let Some(l) = w.leader() else { return false };
            let obs = w.nodes[&l].version_observations();
            all_ids().into_iter().filter(|&i| i != l).all(|p| {
                obs.get(&nid(p)).is_some_and(|o| {
                    o.range == Some(VersionRange::new(1, 1)) && o.build.as_deref() == Some("b2")
                })
            })
        },
    );
}

#[test]
fn every_role_reaches_the_leader_observation_table() {
    for seed in seeds("version_observe_roles", 6) {
        run_roles(seed);
    }
}

// ---- (d) ----

fn run_phase1_profile(seed: u64, variant: u64) {
    let mut w = World::new(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    for id in all_ids() {
        w.set_ext_only(id); // wire says B2, own profile stays Phase 1 (None)
        w.flipped.insert(id);
    }
    let mut check = |_: &World| {};
    w.run(Duration::from_secs(6), &mut check);
    for n in w.nodes.values() {
        assert!(
            !era(&n.metadata()),
            "seed={seed}: a Phase 1 profile leader started the era"
        );
    }
    let l = w.leader().expect("leader");
    if variant == 0 {
        // Everyone gets a B2 profile.
        for n in w.nodes.values() {
            n.set_own_version_range(Some(animus_control::version::own_range()));
            n.set_own_build("b2");
        }
    } else {
        // Only the followers (and the learner) do; the None leader still
        // never proposes ...
        for (&i, n) in &w.nodes {
            if i != l {
                n.set_own_version_range(Some(animus_control::version::own_range()));
                n.set_own_build("b2");
            }
        }
        w.run(Duration::from_secs(4), &mut check);
        assert!(
            w.nodes.values().all(|n| !era(&n.metadata())),
            "seed={seed}: era started under a Phase 1 profile leader"
        );
        // ... until a B2 node leads. The None leader also needs a profile
        // for the final record (it is a registered node), so upgrade it
        // after it is deposed.
        w.sim.crash(nid(l));
        w.poll(
            Duration::from_secs(30),
            "a B2 successor leads",
            &mut check,
            &|w| w.leader().is_some_and(|x| x != l),
        );
        w.sim.restart(nid(l));
        w.nodes[&l].set_own_version_range(Some(animus_control::version::own_range()));
        w.nodes[&l].set_own_build("b2");
    }
    let ids = all_ids();
    w.poll(
        Duration::from_secs(60),
        "era started by a B2 profile leader",
        &mut check,
        &|w| {
            w.nodes
                .values()
                .all(|n| era(&n.metadata()) && n.metadata().node_versions.len() == ids.len())
        },
    );
}

#[test]
fn a_phase1_profile_leader_never_proposes_but_a_b2_one_does() {
    for (i, seed) in seeds("version_observe_phase1_profile", 4)
        .into_iter()
        .enumerate()
    {
        run_phase1_profile(seed, (i % 2) as u64);
    }
}

// ---- (e) ----

fn run_leader_change(seed: u64) {
    let mut w = World::new(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    let mut check = |w: &World| era_safety(w);
    let mut ids = all_ids();
    let last = ids.pop().expect("ids");
    for id in ids {
        w.flip(id, false);
    }
    w.run(Duration::from_millis(500), &mut check);
    w.flip(last, false);
    // Kill the leader at a swept offset around when P can first hold (one
    // observation window after the last upgrade at the earliest).
    let offset = Duration::from_millis(w.rng.below(60) * 20);
    w.run(offset, &mut check);
    let killed = w.leader().filter(|_| w.rng.chance(85));
    if let Some(l) = killed {
        w.sim.crash(nid(l));
        w.run(Duration::from_secs(2), &mut check);
        w.sim.restart(nid(l));
    }
    let ids = all_ids();
    w.poll(
        Duration::from_secs(60),
        "era started once and every node recorded",
        &mut check,
        &|w| fully_recorded(w, &ids),
    );
    // Exactly-once: no further log growth once settled, nothing flips back.
    let settled: Vec<u64> = w.nodes.values().map(|n| n.last_log_index()).collect();
    let before = w.leader_meta().expect("leader");
    w.run(Duration::from_secs(4), &mut check);
    let after = w.leader_meta().expect("leader");
    assert_eq!(before, after, "seed={seed}: metadata moved after settling");
    let leader_idx = w.nodes[&w.leader().expect("leader")].last_log_index();
    let max_settled = settled.into_iter().max().unwrap_or(0);
    assert!(
        leader_idx <= max_settled + 2,
        "seed={seed}: log kept growing after the era settled ({max_settled} -> {leader_idx})"
    );
}

#[test]
fn a_leader_change_as_p_holds_starts_the_era_exactly_once() {
    for seed in seeds("version_observe_leader_change", 16) {
        run_leader_change(seed);
    }
}

// ---- (f) ----

fn run_upkeep(seed: u64, faults: bool) {
    let mut w = World::new(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    let mut check = |w: &World| era_safety(w);
    for id in all_ids() {
        w.flip(id, false);
    }
    let ids = all_ids();
    w.poll(Duration::from_secs(30), "era started", &mut check, &|w| {
        fully_recorded(w, &ids)
    });
    w.run(Duration::from_secs(2), &mut check);
    let base = w
        .nodes
        .values()
        .map(|n| n.last_log_index())
        .max()
        .unwrap_or(0);

    // A node advertises a new build / wider range; the leader re-records it.
    let changed = 10;
    w.sim
        .set_network_ext_for(nid(changed), encode_ext(Some((1, 2)), Some("b3")));
    if faults && let Some(h) = w.inject(Fault::LeaderKill) {
        w.run(Duration::from_secs(1), &mut check);
        w.heal(h);
    }
    w.poll(
        Duration::from_secs(60),
        "new build re-recorded",
        &mut check,
        &|w| {
            w.leader_meta().is_some_and(|m| {
                m.node_versions
                    .get(&nid(changed))
                    .is_some_and(|v| v.range == VersionRange::new(1, 2) && v.build == "b3")
            })
        },
    );

    // A node registered after the era is recorded as well.
    let late = 20;
    {
        let env = w.sim.env(nid(late));
        env.spawn_task(animus_control::node::heartbeat_loop(
            env.clone(),
            VOTERS.iter().copied().map(nid).collect(),
        ));
        w.sim.set_network_ext_for(nid(late), b2_ext());
        w.flipped.insert(late);
    }
    w.propose_confirmed(
        &MetaCommand::UpsertMember {
            node: nid(late),
            labels: Default::default(),
            status: NodeStatus::Active,
        },
        &|m| m.members.contains_key(&nid(late)),
        "late member",
    );
    w.poll(
        Duration::from_secs(60),
        "late member recorded",
        &mut check,
        &|w| {
            w.leader_meta()
                .is_some_and(|m| m.node_versions.contains_key(&nid(late)))
        },
    );
    // Settle, then the log must be quiet: a bounded number of entries for two
    // records (no storm). The status upsert and failure-detector noise are
    // the only other writers.
    w.run(Duration::from_secs(3), &mut check);
    let grown = w
        .nodes
        .values()
        .map(|n| n.last_log_index())
        .max()
        .unwrap_or(0)
        - base;
    let bound = if faults { 40 } else { 12 };
    assert!(
        grown <= bound,
        "seed={seed}: {grown} log entries for two version records (bound {bound}): proposal storm?"
    );
    let a = w
        .nodes
        .values()
        .map(|n| n.last_log_index())
        .max()
        .unwrap_or(0);
    w.run(Duration::from_secs(3), &mut check);
    let b = w
        .nodes
        .values()
        .map(|n| n.last_log_index())
        .max()
        .unwrap_or(0);
    assert_eq!(a, b, "seed={seed}: idle log kept growing");
}

#[test]
fn era_on_upkeep_re_records_changes_without_a_proposal_storm() {
    for (i, seed) in seeds("version_observe_upkeep", 4).into_iter().enumerate() {
        run_upkeep(seed, i % 2 == 1);
    }
}
