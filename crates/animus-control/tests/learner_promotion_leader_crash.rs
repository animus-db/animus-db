//! Regression for issue #1019: a permanent Raft election deadlock caused by
//! gating vote-**granting** (the responder side of `handle_pre_vote`/
//! `handle_request_vote`) on the responder's own `is_voter()` view.
//!
//! A membership-change entry takes effect the instant it is **appended** to
//! a node's own log (`log_append` -> `apply_config`), not once it is
//! committed. `promote_learner` only requires the learner to be caught up to
//! within `RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD` entries of the leader's
//! tip before promoting it — so the promotion's own config-change entry can
//! reach a majority of voters (committing it) while the promoted learner
//! itself has not yet received that entry, if the leader happens to die
//! right after replicating to everyone *except* the learner.
//!
//! Scenario: voters {0,1,2}, learner 3 added and caught up. The link between
//! the leader and 3 is cut (not 3's links to the other voters), so when the
//! leader promotes 3, the promotion entry reaches the other two voters (a
//! real majority of the new 4-member config, 3-of-4) and commits, while 3's
//! own log — and therefore its own `is_voter()` view — still says it's only
//! a learner. The leader then dies. The two survivors both believe the
//! voter set is {0,1,2,3}, so neither can reach a majority of 4 (3 votes)
//! without a vote from 3. Node 3 is otherwise healthy and reachable by both
//! survivors (only its link to the now-dead leader was ever cut) — so a
//! correct responder grants their real votes/pre-votes purely on term/log
//! up-to-dateness, the group elects a new leader, and 3 catches up to learn
//! it is a voter. Before the fix, 3 always rejects (its own `is_voter()` is
//! locally false), and the group is stuck forever: this is the exact
//! deadlock observed live in `animusd::cluster_growth::
//! dashboard_health_recovers_after_grown_cluster_loses_an_original_node`.
//!
//! Mirrors `learner_corpus.rs`'s small harness helpers
//! (`cluster`/`unique_leader`/`converge`/`set`/`upsert`).

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::{ProposeResult, RaftNode};
use animus_env::{NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_test::corpus;

const VOTERS: [u64; 3] = [0, 1, 2];
const LEARNER: u64 = 3;
/// Mirrors `learner_corpus.rs`'s own `CATCH_UP_THRESHOLD` (and
/// `animus-cp-data::RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD`): "caught up"
/// means within this many log entries of the leader's own tip.
const CATCH_UP_THRESHOLD: u64 = 4;

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

fn cluster(seed: u64, ids: &[u64]) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = ids
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                ids.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn unique_leader(nodes: &[RaftNode<SimEnv>], live: &[usize], seed: u64) -> usize {
    let leaders: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "expected exactly one leader among {live:?}, found {leaders:?} (seed={seed})"
    );
    leaders[0]
}

/// Poll-to-convergence (never a fixed-deadline one-shot assert, per the
/// repo's testing lessons): keep running short slices of virtual time until
/// `pred` holds or `attempts` are exhausted.
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

/// The scenario itself, run at a caller-chosen seed.
fn scenario(seed: u64) {
    let (mut sim, nodes) = cluster(seed, &VOTERS);
    sim.run_for(Duration::from_secs(2));
    let l = unique_leader(&nodes, &[0, 1, 2], seed);

    // Add the learner and let it fully catch up while everything is healthy.
    let learner = RaftNode::start(
        sim.env(nid(LEARNER)),
        VOTERS.iter().copied().map(nid).collect(),
        MemoryEngine::new(),
    );
    assert!(
        matches!(
            nodes[l].add_learner(nid(LEARNER)),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}"
    );
    // Let real virtual time pass (not just poll a predicate that could be
    // trivially true from the very first check, since `CATCH_UP_THRESHOLD`
    // alone is loose enough — a couple of outstanding entries already count
    // as "caught up" — to pass before anything has actually replicated):
    // this also gives the `add_learner` entry itself time to commit on the
    // leader, which `promote_learner` requires (`config_change_in_flight`).
    sim.run_for(Duration::from_secs(2));
    let caught_up = converge(&mut sim, 40, Duration::from_millis(200), || {
        nodes[l].learner_caught_up(&nid(LEARNER), CATCH_UP_THRESHOLD)
    });
    assert!(
        caught_up,
        "seed={seed}: learner must catch up before the promotion race begins"
    );
    assert_eq!(
        nodes[l].commit_index(),
        nodes[l].last_log_index(),
        "seed={seed}: the add_learner entry itself must be committed before promoting \
         (commit_index={} last_log_index={})",
        nodes[l].commit_index(),
        nodes[l].last_log_index(),
    );
    assert_eq!(
        learner.config(),
        set(&VOTERS),
        "seed={seed}: precondition — still a learner, not yet promoted"
    );

    // Cut only the leader<->learner link (the learner stays fully reachable
    // from the other two voters throughout) and promote. The promotion
    // entry can therefore reach A and B — a real 3-of-4 majority of the new
    // {0,1,2,3} config, which commits it — but never reaches 3.
    sim.partition_pair(nid(l as u64), nid(LEARNER));
    let promote_result = nodes[l].promote_learner(nid(LEARNER));
    assert!(
        matches!(promote_result, ProposeResult::Accepted { .. }),
        "seed={seed}: promotion proposal must be locally accepted by the leader; \
         got {promote_result:?}; role={:?} leader={:?} commit_index={} last_log_index={} \
         log_len={}",
        nodes[l].role(),
        nodes[l].leader(),
        nodes[l].commit_index(),
        nodes[l].last_log_index(),
        nodes[l].log_len(),
    );

    let followers: Vec<usize> = [0usize, 1, 2].into_iter().filter(|&i| i != l).collect();
    let promoted_committed = converge(&mut sim, 60, Duration::from_millis(100), || {
        followers
            .iter()
            .all(|&f| nodes[f].config() == set(&[0, 1, 2, LEARNER]))
    });
    assert!(
        promoted_committed,
        "seed={seed}: both surviving voters must commit the promotion (3-of-4 majority, \
         excluding only the leader-learner link)"
    );

    // Precondition for the deadlock: the learner's own view is still stale
    // — it never received the promotion entry, so it still thinks it's a
    // learner.
    assert_eq!(
        learner.config(),
        set(&VOTERS),
        "seed={seed}: precondition — the learner's own log must NOT yet contain the \
         promotion entry (the leader<->learner link was cut before promoting)"
    );
    assert!(!learner.config().contains(&nid(LEARNER)), "seed={seed}");

    // The leader now dies, permanently. Heal the (now-moot) leader<->learner
    // link — the other two voters were never partitioned from the learner —
    // and give the group a bounded virtual-time budget to elect a new
    // leader and let the learner catch up.
    sim.crash(nid(l as u64));
    sim.heal(nid(l as u64), nid(LEARNER));

    let elected = converge(&mut sim, 100, Duration::from_millis(200), || {
        let leader_count = [
            nodes[followers[0]].is_leader(),
            nodes[followers[1]].is_leader(),
            learner.is_leader(),
        ]
        .into_iter()
        .filter(|&is_leader| is_leader)
        .count();
        leader_count == 1
    });
    assert!(
        elected,
        "seed={seed}: exactly one of the two surviving voters or the learner must become \
         leader within the virtual-time budget — a permanent leaderless deadlock (issue \
         #1019) means this never converges; f0(n{})=role={:?} term={} commit={} last_log={}; \
         f1(n{})=role={:?} term={} commit={} last_log={}; learner(n{LEARNER})=role={:?} \
         term={} commit={} last_log={} config={:?}; f0.leader={:?} f1.leader={:?} \
         learner.leader={:?}",
        followers[0],
        nodes[followers[0]].role(),
        nodes[followers[0]].term(),
        nodes[followers[0]].commit_index(),
        nodes[followers[0]].last_log_index(),
        followers[1],
        nodes[followers[1]].role(),
        nodes[followers[1]].term(),
        nodes[followers[1]].commit_index(),
        nodes[followers[1]].last_log_index(),
        learner.role(),
        learner.term(),
        learner.commit_index(),
        learner.last_log_index(),
        learner.config(),
        nodes[followers[0]].leader(),
        nodes[followers[1]].leader(),
        learner.leader(),
    );

    let caught_up_after_election = converge(&mut sim, 60, Duration::from_millis(100), || {
        learner.config().contains(&nid(LEARNER))
    });
    assert!(
        caught_up_after_election,
        "seed={seed}: the learner must eventually learn (from the new leader) that it is \
         now a voter"
    );
}

#[test]
fn learner_promotion_survives_leader_crash_before_the_learner_hears_it() {
    scenario(0x1019_0001);
}

/// Depth knob: shares `ANIMUS_LEARNER_SEEDS` with `learner_corpus.rs`'s own
/// convention (same corpus family, a different fault shape) via
/// `corpus::seeds_from_env`, but this particular race is narrow enough
/// (needs the promotion entry to land on a majority while missing exactly
/// the promoted node) that this test floors the depth at 8 seeds
/// unconditionally, run at name-derived seeds the same way `learner_corpus
/// .rs::learner_corpus_runs_at_configured_depth` derives its own —
/// `ANIMUS_LEARNER_SEEDS=K` for `K > 8` widens it further, same as there.
#[test]
fn learner_promotion_leader_crash_corpus_runs_at_configured_depth() {
    let k = corpus::seeds_from_env("ANIMUS_LEARNER_SEEDS").max(8);
    for i in 0..k {
        let seed = if i == 0 {
            0x1019_0001
        } else {
            corpus::name_seed(&format!("learner_promotion_leader_crash_s{i:03}"))
        };
        scenario(seed);
    }
}
