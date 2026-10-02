//! Regression for issue #1131: a learner must never be promoted to voter
//! while its own issue #667 boot-time cluster check is still pending.
//!
//! A node with `cluster_check_pending` refuses every vote and never
//! campaigns. A freshly hosted learner's check is seeded from its (stale)
//! bootstrap peers; if the leader promotes it before the check resolves, the
//! first probe reply that does arrive already names the learner as a voter
//! (`heard=false`), which takes the ambiguous wait-for-EVERY-pending-peer
//! path — and if one of those bootstrap peers is dead that path never
//! completes. The promoted voter then sits in the quorum denominator unable
//! to vote: with one original voter killed, a 4-voter group needs all three
//! survivors, so it is leaderless forever (observed live in
//! `animusd::cluster_growth::dashboard_health_recovers_after_grown_cluster_
//! loses_an_original_node`, ~3/46 under heavy CPU load).
//!
//! The fix gates promotion (`RaftCore::learner_caught_up`, which every
//! production promoter consults) on the learner's own reported
//! `check_pending` (an `AppendEntriesResp` field).
//!
//! Two tests:
//! - `learner_caught_up_waits_for_the_check_to_resolve`: a unit-level check
//!   of the gate over a hand-driven `RaftCore` pair.
//! - `grown_group_elects_after_leader_loss_corpus`: the seed-swept SimEnv
//!   scenario (`ANIMUS_LEARNER_PENDING_CHECK_SEEDS`, floor 8). The learner's
//!   bootstrap peers are `{follower, n9}` with `n9` never running; it is
//!   caught up through the leader while every other voter is partitioned
//!   from it, so the leader's promotion can land before its first probe
//!   round does. Then the leader is killed; the survivors plus the promoted
//!   learner must elect within a bounded virtual-time budget.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::{ProposeResult, RaftCore, RaftMsg, RaftNode};
use animus_env::{Nanos, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_test::corpus;

const VOTERS: [u64; 3] = [0, 1, 2];
const LEARNER: u64 = 3;
const DEAD: u64 = 9;
const CATCH_UP_THRESHOLD: u64 = 4;

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

fn converge(
    sim: &mut Simulator,
    attempts: u32,
    slice: Duration,
    mut pred: impl FnMut() -> bool,
) -> bool {
    for _ in 0..attempts {
        if pred() {
            return true;
        }
        sim.run_for(slice);
    }
    pred()
}

/// Route messages between a hand-driven leader (`n0`) and learner (`n1`)
/// over `steps` 50ms ticks; `deliver_probes == false` drops every
/// `ClusterProbe`/`ClusterProbeResp` (the learner's check cannot resolve).
fn pump(
    leader: &mut RaftCore,
    learner: &mut RaftCore,
    t: &mut u64,
    steps: u32,
    deliver_probes: bool,
) {
    let mut in_flight: Vec<(NodeId, NodeId, RaftMsg<_>)> = Vec::new();
    for _ in 0..steps {
        *t += 50_000_000;
        let now = Nanos(*t);
        for (to, m) in leader.tick(now, *t) {
            in_flight.push((nid(0), to, m));
        }
        for (to, m) in learner.tick(now, *t) {
            in_flight.push((nid(1), to, m));
        }
        while let Some((from, to, m)) = in_flight.pop() {
            if !deliver_probes
                && matches!(m, RaftMsg::ClusterProbe | RaftMsg::ClusterProbeResp { .. })
            {
                continue;
            }
            let core = if to == nid(0) {
                &mut *leader
            } else {
                &mut *learner
            };
            for (to2, m2) in core.handle(from.clone(), m, now, *t) {
                in_flight.push((to.clone(), to2, m2));
            }
        }
    }
}

/// The gate itself: a caught-up learner whose boot-time cluster check is
/// still pending is NOT promotable (`learner_caught_up`), however caught up
/// its log is; once the check resolves and the learner reports it, it is.
#[test]
fn learner_caught_up_waits_for_the_check_to_resolve() {
    let mut leader: RaftCore = RaftCore::new(nid(0), &[nid(0)], Nanos(0), 7);
    let _ = leader.tick(Nanos(10_000_000_000), 7);
    assert!(leader.is_leader());
    let mut t = 10_000_000_000u64;
    assert!(matches!(
        leader.add_learner(nid(1)),
        ProposeResult::Accepted { .. }
    ));
    // A fresh learner with stale bootstrap peers {n0, n9}.
    let mut learner: RaftCore = RaftCore::new(nid(1), &[nid(0), nid(9), nid(1)], Nanos(t), 11);
    let _ = learner.begin_cluster_check(Nanos(t), 11);
    assert!(learner.cluster_check_pending());

    assert!(
        !leader.learner_caught_up(&nid(1), 1_000),
        "never reported anything yet: not promotable"
    );
    pump(&mut leader, &mut learner, &mut t, 60, false);
    assert!(learner.cluster_check_pending(), "probes are being dropped");
    assert_eq!(leader.peer_check_pending(&nid(1)), Some(true));
    assert_eq!(
        leader.peer_match(&nid(1)),
        leader.last_log_index(),
        "the learner is fully caught up on the log"
    );
    assert!(
        !leader.learner_caught_up(&nid(1), 1_000),
        "issue #1131: a caught-up learner with a pending cluster check must not be promotable"
    );

    // Let probes flow: the leader's config does not list the learner, so
    // its reply resolves the check at once.
    pump(&mut leader, &mut learner, &mut t, 60, true);
    assert!(!learner.cluster_check_pending());
    assert_eq!(leader.peer_check_pending(&nid(1)), Some(false));
    assert!(
        leader.learner_caught_up(&nid(1), 1_000),
        "promotable once the check has resolved and been reported"
    );
}

/// A learner whose check resolved to REFUSED (a genuinely wiped voter) never
/// votes or campaigns either, so it must never be reported caught up.
#[test]
fn a_refused_learner_is_never_promotable() {
    let mut leader: RaftCore = RaftCore::new(nid(0), &[nid(0)], Nanos(0), 7);
    let _ = leader.tick(Nanos(10_000_000_000), 7);
    assert!(leader.is_leader());
    let mut t = 10_000_000_000u64;
    assert!(matches!(
        leader.add_learner(nid(1)),
        ProposeResult::Accepted { .. }
    ));
    let mut learner: RaftCore = RaftCore::new(nid(1), &[nid(0), nid(1)], Nanos(t), 11);
    let _ = learner.begin_cluster_check(Nanos(t), 11);
    // Its only peer reports real history, names it as an established voter
    // and has heard from it: a wiped-voter verdict.
    let _ = learner.handle(
        nid(0),
        RaftMsg::ClusterProbeResp {
            term: 3,
            committed_index: 5,
            config: set(&[0, 1]),
            ever_heard_from_prober: true,
        },
        Nanos(t),
        11,
    );
    assert!(!learner.cluster_check_pending());
    assert!(learner.refused_as_voter());

    pump(&mut leader, &mut learner, &mut t, 60, true);
    assert_eq!(
        leader.peer_match(&nid(1)),
        leader.last_log_index(),
        "the learner is fully caught up on the log"
    );
    assert_eq!(leader.peer_check_pending(&nid(1)), Some(true));
    assert!(
        !leader.learner_caught_up(&nid(1), 1_000),
        "issue #1131: a refused learner can never vote, so it must not be promotable"
    );
}

fn scenario(seed: u64) {
    let sim_seed = seed;
    let mut sim = Simulator::new(sim_seed);
    let nodes: Vec<RaftNode<SimEnv>> = VOTERS
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                VOTERS.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));
    let leaders: Vec<usize> = (0..3).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(leaders.len(), 1, "seed={seed}: expected one leader");
    let l = leaders[0];
    let others: Vec<usize> = (0..3).filter(|&i| i != l).collect();
    let (f, g) = (others[0], others[1]);

    // The learner is isolated from every voter at boot, so its first probe
    // rounds are dropped and its check is pending. Its bootstrap peers are a
    // live follower and a node that never runs.
    for v in VOTERS {
        sim.partition_pair(nid(v), nid(LEARNER));
    }
    let learner = RaftNode::start(
        sim.env(nid(LEARNER)),
        vec![nid(f as u64), nid(DEAD)],
        MemoryEngine::new(),
    );
    assert!(
        matches!(
            nodes[l].add_learner(nid(LEARNER)),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}"
    );
    sim.run_for(Duration::from_secs(1));
    assert!(
        learner.cluster_check_pending(),
        "seed={seed}: precondition — learner's check is pending while isolated"
    );

    // Open ONLY leader<->learner. Drive the production promoter's shape
    // (promote as soon as `learner_caught_up`) on a fine slice so a
    // promotion can land before the learner's next probe resend.
    sim.heal(nid(l as u64), nid(LEARNER));
    sim.heal(nid(LEARNER), nid(l as u64));
    let mut promoted = false;
    for _ in 0..600 {
        if !promoted && nodes[l].learner_caught_up(&nid(LEARNER), CATCH_UP_THRESHOLD) {
            assert!(
                !learner.cluster_check_pending(),
                "seed={seed}: the leader judged a learner with a pending cluster check \
                 promotable"
            );
            assert!(
                matches!(
                    nodes[l].promote_learner(nid(LEARNER)),
                    ProposeResult::Accepted { .. }
                ),
                "seed={seed}"
            );
            promoted = true;
        }
        if promoted && learner.config().contains(&nid(LEARNER)) {
            break;
        }
        sim.run_for(Duration::from_millis(5));
    }
    assert!(
        promoted && learner.config().contains(&nid(LEARNER)),
        "seed={seed}: the learner must be promoted and learn it (check_pending={}, \
         learner_caught_up={}, config={:?})",
        learner.cluster_check_pending(),
        nodes[l].learner_caught_up(&nid(LEARNER), CATCH_UP_THRESHOLD),
        learner.config()
    );

    // Everyone reachable again; the promotion must commit everywhere.
    for v in [f, g] {
        sim.heal(nid(v as u64), nid(LEARNER));
        sim.heal(nid(LEARNER), nid(v as u64));
    }
    let all4 = set(&[0, 1, 2, LEARNER]);
    let settled = converge(&mut sim, 100, Duration::from_millis(100), || {
        nodes[f].config() == all4
            && nodes[g].config() == all4
            && learner.commit_index() == nodes[l].commit_index()
    });
    assert!(
        settled,
        "seed={seed}: the promotion must commit on all members"
    );

    // Kill the original leader. The 4-voter group now needs all three
    // survivors; the promoted learner must be able to vote.
    sim.crash(nid(l as u64));
    let elected = converge(&mut sim, 150, Duration::from_millis(200), || {
        [
            nodes[f].is_leader(),
            nodes[g].is_leader(),
            learner.is_leader(),
        ]
        .into_iter()
        .filter(|&x| x)
        .count()
            == 1
    });
    assert!(
        elected,
        "seed={seed}: no leader elected after the leader died (issue #1131: a promoted \
         learner with a pending cluster check refuses every vote); \
         f(n{f})={:?} term={} g(n{g})={:?} term={} learner={:?} term={} \
         learner.check_pending={}",
        nodes[f].role(),
        nodes[f].term(),
        nodes[g].role(),
        nodes[g].term(),
        learner.role(),
        learner.term(),
        learner.cluster_check_pending(),
    );
}

#[test]
fn grown_group_elects_after_leader_loss() {
    scenario(0x1131_0001);
}

/// Depth knob: `ANIMUS_LEARNER_PENDING_CHECK_SEEDS=K`, floored at 8.
#[test]
fn grown_group_elects_after_leader_loss_corpus() {
    let k = corpus::seeds_from_env("ANIMUS_LEARNER_PENDING_CHECK_SEEDS").max(8);
    for i in 0..k {
        let seed = if i == 0 {
            0x1131_0001
        } else {
            corpus::name_seed(&format!("learner_pending_check_s{i:03}"))
        };
        scenario(seed);
    }
}
