//! Core-level tests for the **learner** (non-voting) membership class (ADR
//! 0058 Train 1): `RaftCore::add_learner`/`promote_learner`/`remove_learner`,
//! quorum math excluding learners, and the election/pre-vote gate. Mirrors
//! `control_membership.rs`'s harness idioms (`Simulator::run_for`, never
//! `run()` — perpetual heartbeats; every seed printed in assertion messages).
//!
//! A fault-injected, seed-scalable corpus lives in `learner_corpus.rs`
//! (`ANIMUS_LEARNER_SEEDS`); this file is the deterministic, single-seed
//! mechanism/edge-case suite the ADR's "core-level test" half asks for.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::raft::{MemberRole, ProposeResult, RaftCore};
use animus_control::{MetaCommand, Metadata, NodeStatus};
use animus_env::{Nanos, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

use animus_control::RaftNode;

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: std::collections::BTreeMap::new(),
        status: NodeStatus::Active,
    }
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

// ---- bare `RaftCore<MetaCommand, Metadata>` mechanism (no driver/Env at all) ----

fn core(id: u64, all: &[u64]) -> RaftCore<MetaCommand, Metadata> {
    RaftCore::new(
        nid(id),
        &all.iter().copied().map(nid).collect::<Vec<_>>(),
        Nanos(0),
        0,
    )
}

/// Drive a hand-held `core` to leadership the same way the crate's own
/// hand-driven tests do (`persistence.rs` et al.): a single-node group elects
/// itself on one tick.
fn elect_solo_leader(c: &mut RaftCore<MetaCommand, Metadata>) {
    // A single-voter group wins its own pre-vote round immediately.
    let _ = c.tick(Nanos(10_000_000_000), 0);
    assert!(c.is_leader(), "a lone voter must self-elect");
}

#[test]
fn add_learner_is_rejected_by_a_non_leader() {
    let mut c = core(0, &[0]);
    assert!(!c.is_leader());
    assert!(matches!(
        c.add_learner(nid(1)),
        ProposeResult::NotLeader { .. }
    ));
}

#[test]
fn add_learner_then_promote_moves_the_id_from_learners_to_voters() {
    let mut c = core(0, &[0]);
    elect_solo_leader(&mut c);

    assert!(matches!(
        c.add_learner(nid(1)),
        ProposeResult::Accepted { .. }
    ));
    assert_eq!(c.learners(), set(&[1]));
    assert!(!c.config().contains(&nid(1)));
    assert_eq!(c.member_role(&nid(1)), Some(MemberRole::Learner));

    assert!(matches!(
        c.promote_learner(nid(1)),
        ProposeResult::Accepted { .. }
    ));
    assert!(c.learners().is_empty());
    assert!(c.config().contains(&nid(1)));
    assert_eq!(c.member_role(&nid(1)), Some(MemberRole::Voter));
}

#[test]
fn add_learner_rejects_an_id_already_a_voter_or_learner() {
    let mut c = core(0, &[0]);
    elect_solo_leader(&mut c);
    assert!(
        matches!(c.add_learner(nid(0)), ProposeResult::NotLeader { .. }),
        "the leader's own id is already a voter"
    );

    assert!(matches!(
        c.add_learner(nid(1)),
        ProposeResult::Accepted { .. }
    ));
    assert!(
        matches!(c.add_learner(nid(1)), ProposeResult::NotLeader { .. }),
        "already a learner"
    );
}

#[test]
fn promote_learner_rejects_an_id_that_is_not_currently_a_learner() {
    let mut c = core(0, &[0]);
    elect_solo_leader(&mut c);
    assert!(matches!(
        c.promote_learner(nid(9)),
        ProposeResult::NotLeader { .. }
    ));
}

#[test]
fn remove_learner_drops_it_without_ever_touching_voters() {
    let mut c = core(0, &[0]);
    elect_solo_leader(&mut c);
    assert!(matches!(
        c.add_learner(nid(1)),
        ProposeResult::Accepted { .. }
    ));
    assert!(matches!(
        c.remove_learner(nid(1)),
        ProposeResult::Accepted { .. }
    ));
    assert!(c.learners().is_empty());
    assert!(!c.config().contains(&nid(1)));
}

#[test]
fn change_membership_rejects_a_delta_that_collides_with_a_current_learner() {
    let mut c = core(0, &[0]);
    elect_solo_leader(&mut c);
    assert!(matches!(
        c.add_learner(nid(1)),
        ProposeResult::Accepted { .. }
    ));
    // Adding node 1 straight to the voter set via `change_membership` is
    // rejected: it must go through `promote_learner` instead so `config`/
    // `learners` never both name the same id.
    let mut voters = c.config();
    voters.insert(nid(1));
    assert!(matches!(
        c.change_membership(voters),
        ProposeResult::NotLeader { .. }
    ));
    assert_eq!(c.learners(), set(&[1]), "learners must be untouched");
}

// ---- structural safety: a learner never appears in any majority computation ----

#[test]
fn a_dead_learner_never_blocks_commit_or_election_a_live_learner_never_helps_either() {
    let seed = 0x7EA5_0001;
    let ids = [0u64, 1, 2];
    let (mut sim, nodes) = cluster(seed, &ids);
    sim.run_for(Duration::from_secs(2));
    let l = unique_leader(&nodes, &[0, 1, 2], seed);

    // Add node 3 as a learner (never a voter): the group's quorum stays
    // "2-of-3 voters" throughout this test, never "2-of-4" or "3-of-4".
    let learner = RaftNode::start(
        sim.env(nid(3)),
        ids.iter().copied().map(nid).collect(),
        MemoryEngine::new(),
    );
    assert!(
        matches!(nodes[l].add_learner(nid(3)), ProposeResult::Accepted { .. }),
        "seed={seed}"
    );
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        learner.config(),
        set(&ids),
        "seed={seed}: learner must not be a voter"
    );
    assert_eq!(nodes[l].learners(), set(&[3]), "seed={seed}");

    // The learner alone dying must not affect the voters' ability to commit
    // — if it counted toward quorum this write would stall on 2-of-4.
    sim.crash(nid(3));
    assert!(
        matches!(
            nodes[l].propose(upsert(100)),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}"
    );
    sim.run_for(Duration::from_secs(2));
    for &i in &[0usize, 1, 2] {
        assert!(
            nodes[i].metadata().members.contains_key(&nid(100)),
            "seed={seed}: node {i} must have committed the write purely on 2-of-3 voters"
        );
    }

    // Conversely: with the learner alive and every VOTER but the leader
    // crashed, the leader (1-of-3 voters) must NOT be able to commit —
    // even though a learner-inclusive count (leader + learner) would look
    // like a majority of a naive 4-member count. This is the direct
    // negative-control counterpart of the positive check above: a live
    // learner must never let a proposal succeed than a real voter quorum.
    let followers: Vec<usize> = [0usize, 1, 2].into_iter().filter(|&i| i != l).collect();
    for &f in &followers {
        sim.crash(nid(f as u64));
    }
    sim.restart(nid(3));
    sim.run_for(Duration::from_secs(1));
    let before = nodes[l].last_log_index();
    let result = nodes[l].propose(upsert(101));
    sim.run_for(Duration::from_secs(3));
    // A leader always *accepts* locally (appends to its own log) — the
    // safety property is that it never *commits*: `metadata()` (which
    // reflects only `commit_index`-covered state) must not observe it.
    assert!(
        matches!(result, ProposeResult::Accepted { .. }),
        "seed={seed}: a leader always locally accepts"
    );
    assert!(
        nodes[l].last_log_index() > before,
        "seed={seed}: the entry was appended"
    );
    assert!(
        !nodes[l].metadata().members.contains_key(&nid(101)),
        "seed={seed}: must NOT have committed on 1 voter + 1 learner — \
         the learner must never count toward quorum"
    );
}

#[test]
fn a_learner_never_campaigns_even_when_it_never_hears_from_a_leader() {
    let seed = 0x7EA5_0002;
    let ids = [0u64, 1, 2];
    let (mut sim, nodes) = cluster(seed, &ids);
    sim.run_for(Duration::from_secs(2));
    let l = unique_leader(&nodes, &[0, 1, 2], seed);

    let learner = RaftNode::start(
        sim.env(nid(3)),
        ids.iter().copied().map(nid).collect(),
        MemoryEngine::new(),
    );
    assert!(matches!(
        nodes[l].add_learner(nid(3)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));

    // Isolate the learner completely from every voter (both directions) and
    // let many election-timeout intervals pass. Because `start_election`/
    // `start_pre_vote` gate on `is_voter()`, the learner must never become a
    // `PreCandidate`/`Candidate`/`Leader` no matter how long it goes without
    // hearing from anyone.
    for &v in &ids {
        sim.partition_pair(nid(3), nid(v));
    }
    sim.run_for(Duration::from_secs(10));
    assert!(
        !learner.is_leader(),
        "seed={seed}: an isolated learner must never elect itself"
    );

    // Heal and confirm it resumes ordinary catch-up (it is still a healthy,
    // reachable follower-shaped learner — just never a candidate).
    for &v in &ids {
        sim.heal(nid(3), nid(v));
    }
    assert!(matches!(
        nodes[l].propose(upsert(200)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));
    assert!(
        learner.metadata().members.contains_key(&nid(200)),
        "seed={seed}: a healed learner still catches up normally"
    );
}
