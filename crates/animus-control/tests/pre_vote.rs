//! Pre-vote hardening tests (ADR 0009).
//!
//! Pre-vote is the standard Raft extension that stops a briefly-stalled or
//! partitioned node from repeatedly incrementing the cluster's term and
//! disrupting a healthy leader: a node runs a *pre-vote* round (no term bump)
//! and only starts a real election once a majority would actually vote for it.
//!
//! Two properties, one deterministic at the core level and one end to end:
//!  - a node with a live leader **rejects** a pre-vote, and a pre-vote never
//!    changes a node's term (so an isolated follower can't inflate the term);
//!  - normal elections still succeed once the leader is genuinely gone.

use std::time::Duration;

use animus_control::raft::{RaftCore, RaftMsg, Role};
use animus_control::{MetaCommand, NodeStatus, RaftNode};
use animus_env::{Nanos, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

fn member_ids() -> [NodeId; 3] {
    [nid(0), nid(1), nid(2)]
}

// ---- core-level, fully deterministic --------------------------------------

/// A pure heartbeat (`AppendEntries` with no entries) from `leader` at `term`.
fn heartbeat(leader: NodeId, term: u64) -> RaftMsg {
    RaftMsg::AppendEntries {
        term,
        leader,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: Vec::new(),
        leader_commit: 0,
    }
}

/// A follower that just heard from a live leader **rejects** a pre-vote, and its
/// term is untouched — the leader-lease protection that keeps a partitioned node
/// from winning a pre-vote and forcing an election.
#[test]
fn prevote_rejected_while_leader_lease_valid() {
    let mut core: RaftCore = RaftCore::new(nid(0), &member_ids(), Nanos(0), 7);
    // Hear a heartbeat from leader 1 at term 5: this sets the leader hint and
    // resets the election timer (the lease).
    let hb_at = Nanos(1_000_000);
    core.handle(nid(1), heartbeat(nid(1), 5), hb_at, 7);
    assert_eq!(core.term(), 5);
    assert_eq!(core.role(), Role::Follower);

    // Node 2 solicits a pre-vote for its prospective term 6, still within our
    // lease (only a moment after the heartbeat).
    let outs = core.handle(
        nid(2),
        RaftMsg::PreVote {
            term: 6,
            candidate: nid(2),
            last_log_index: 0,
            last_log_term: 0,
        },
        Nanos(hb_at.0 + 1),
        7,
    );
    assert!(
        matches!(
            outs.as_slice(),
            [(to, RaftMsg::PreVoteResp { granted: false, .. })] if *to == nid(2)
        ),
        "a live-leader lease must reject the pre-vote: {outs:?}"
    );
    // Crucially, the pre-vote left our term (and role) alone.
    assert_eq!(core.term(), 5, "a pre-vote must never change our term");
    assert_eq!(core.role(), Role::Follower);
}

/// Once the election timeout has elapsed with no heartbeat, the lease is gone and
/// the same node **grants** a pre-vote (log up to date) — again without changing
/// its own term.
#[test]
fn prevote_granted_after_lease_expires() {
    let mut core: RaftCore = RaftCore::new(nid(0), &member_ids(), Nanos(0), 7);
    let hb_at = Nanos(1_000_000);
    core.handle(nid(1), heartbeat(nid(1), 5), hb_at, 7);

    // Well past the election timeout (default base 150ms, spread < 300ms): the
    // lease has expired, so the leader is presumed gone.
    let later = Nanos(hb_at.0 + 400_000_000);
    let outs = core.handle(
        nid(2),
        RaftMsg::PreVote {
            term: 6,
            candidate: nid(2),
            last_log_index: 0,
            last_log_term: 0,
        },
        later,
        7,
    );
    assert!(
        matches!(
            outs.as_slice(),
            [(
                to,
                RaftMsg::PreVoteResp {
                    granted: true,
                    term: 6
                }
            )] if *to == nid(2)
        ),
        "an expired lease + up-to-date log must grant the pre-vote: {outs:?}"
    );
    assert_eq!(
        core.term(),
        5,
        "granting a pre-vote must not change our term"
    );
}

/// A follower that times out becomes a **pre-candidate** (not a candidate) and
/// does **not** bump its term — the essence of pre-vote. Only a pre-vote quorum
/// advances it to a real election.
#[test]
fn timeout_makes_pre_candidate_without_bumping_term() {
    let mut core: RaftCore = RaftCore::new(nid(0), &member_ids(), Nanos(0), 7);
    core.handle(nid(1), heartbeat(nid(1), 5), Nanos(1_000_000), 7);
    assert_eq!(core.term(), 5);

    let outs = core.tick(Nanos(1_000_000_000), 7); // long past the timeout
    assert_eq!(
        core.role(),
        Role::PreCandidate,
        "a timeout starts a pre-vote round, not a real election"
    );
    assert_eq!(
        core.term(),
        5,
        "a pre-vote round must not increment the term"
    );
    // It solicited a pre-vote from each peer at the prospective term (6), not a
    // real RequestVote.
    assert_eq!(outs.len(), 2);
    for (_, msg) in &outs {
        assert!(
            matches!(msg, RaftMsg::PreVote { term: 6, .. }),
            "expected PreVote at the prospective term: {msg:?}"
        );
    }
}

/// Issue #930: a voter that just granted a REAL vote has, until this fix, had
/// no live-leader lease of its own — `leader_id` is only ever set by
/// `handle_append_entries`/`InstallSnapshot`/`become_leader`, never by a
/// granted `RequestVote` — so it granted any competing pre-vote in the window
/// before the real winner's first `AppendEntries` arrived. The fix widens the
/// lease to also cover `voted_for.is_some()` within the same
/// `election_deadline` the grant itself reset. See `docs/lessons/code-
/// patterns/2026-09-16-a-voter-that-just-granted-a-real-vote-has-no-pre-vote.md`.
#[test]
fn prevote_rejected_after_granting_a_real_vote_until_deadline() {
    let mut core: RaftCore = RaftCore::new(nid(0), &member_ids(), Nanos(0), 7);
    assert_eq!(core.term(), 0);

    // Node 1 solicits (and wins) a real vote for term 1 — this both bumps our
    // term and resets our own election timer.
    let grant_at = Nanos(1_000_000);
    let outs = core.handle(
        nid(1),
        RaftMsg::RequestVote {
            term: 1,
            candidate: nid(1),
            last_log_index: 0,
            last_log_term: 0,
        },
        grant_at,
        7,
    );
    assert!(
        matches!(
            outs.as_slice(),
            [(to, RaftMsg::RequestVoteResp { term: 1, granted: true })] if *to == nid(1)
        ),
        "expected the real vote to be granted: {outs:?}"
    );
    assert_eq!(core.term(), 1);

    // Node 2 immediately solicits a competing pre-vote for the next
    // prospective term, with an up-to-date (empty) log. Before the fix this
    // was granted, since `leader_id` is still `None` at this point.
    let outs = core.handle(
        nid(2),
        RaftMsg::PreVote {
            term: 2,
            candidate: nid(2),
            last_log_index: 0,
            last_log_term: 0,
        },
        Nanos(grant_at.0 + 1),
        7,
    );
    assert!(
        matches!(
            outs.as_slice(),
            [(to, RaftMsg::PreVoteResp { granted: false, .. })] if *to == nid(2)
        ),
        "a voter that just granted a real vote must reject a competing \
         pre-vote until its election deadline (issue #930): {outs:?}"
    );
    // The rejection itself must not have touched our term.
    assert_eq!(core.term(), 1);

    // Once the granted vote's own election deadline has elapsed, the same
    // pre-vote is now granted — the lease, not the vote itself, is what
    // expires.
    let later = Nanos(grant_at.0 + 400_000_000);
    let outs = core.handle(
        nid(2),
        RaftMsg::PreVote {
            term: 2,
            candidate: nid(2),
            last_log_index: 0,
            last_log_term: 0,
        },
        later,
        7,
    );
    assert!(
        matches!(
            outs.as_slice(),
            [(
                to,
                RaftMsg::PreVoteResp {
                    granted: true,
                    term: 2
                }
            )] if *to == nid(2)
        ),
        "the lease on a granted real vote must expire like any other: {outs:?}"
    );
}

/// Issue #930, consequence 1: a `Candidate` durably self-votes
/// (`voted_for = Some(self)`, in `start_election`) the instant it campaigns,
/// so — now that `handle_pre_vote`'s lease also covers `voted_for` — it
/// rejects a competing pre-vote for the rest of its own election deadline
/// instead of granting it (as it did before this fix, since `leader_id` is
/// `None` for the whole candidacy). This is standard pre-vote behaviour: it
/// reduces dueling candidacies rather than causing any.
#[test]
fn prevote_rejected_by_a_candidate_within_its_own_election_deadline() {
    let mut core: RaftCore = RaftCore::new(nid(0), &member_ids(), Nanos(0), 7);

    // Time out into a pre-vote round.
    core.tick(Nanos(1_000_000_000), 7);
    assert_eq!(core.role(), Role::PreCandidate);
    let prospective = core.term() + 1;

    // A peer's grant reaches pre-vote majority (self + one of two peers, in a
    // 3-node cluster): the real, term-incrementing election starts.
    let elect_at = Nanos(1_000_000_100);
    core.handle(
        nid(1),
        RaftMsg::PreVoteResp {
            term: prospective,
            granted: true,
        },
        elect_at,
        7,
    );
    assert_eq!(
        core.role(),
        Role::Candidate,
        "expected the pre-vote majority to start a real election"
    );
    assert_eq!(core.term(), prospective);

    // A third node's competing pre-vote for a higher prospective term must be
    // rejected: this candidate's own self-vote is a real, term-scoped vote,
    // and `start_election` reset its own election deadline when it cast it.
    let outs = core.handle(
        nid(2),
        RaftMsg::PreVote {
            term: prospective + 1,
            candidate: nid(2),
            last_log_index: 0,
            last_log_term: 0,
        },
        Nanos(elect_at.0 + 1),
        7,
    );
    assert!(
        matches!(
            outs.as_slice(),
            [(to, RaftMsg::PreVoteResp { granted: false, .. })] if *to == nid(2)
        ),
        "a candidate should reject a competing pre-vote within its own \
         election deadline: {outs:?}"
    );

    // Past the candidate's own election deadline, the same pre-vote is
    // granted — the lease is purely a function of `election_deadline`, same
    // as any other voter's.
    let later = Nanos(elect_at.0 + 400_000_000);
    let outs = core.handle(
        nid(2),
        RaftMsg::PreVote {
            term: prospective + 1,
            candidate: nid(2),
            last_log_index: 0,
            last_log_term: 0,
        },
        later,
        7,
    );
    assert!(
        matches!(
            outs.as_slice(),
            [(to, RaftMsg::PreVoteResp { granted: true, term })] if *to == nid(2) && *term == prospective + 1
        ),
        "the lease must expire even for a candidate: {outs:?}"
    );
}

// ---- end to end under SimEnv ----------------------------------------------

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = member_ids()
        .iter()
        .map(|id| {
            RaftNode::start(
                sim.env(id.clone()),
                member_ids().to_vec(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn leader_index(nodes: &[RaftNode<SimEnv>], live: &[usize], seed: u64) -> usize {
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

/// An **isolated follower** running pre-vote rounds does not disturb a stable
/// leader: while partitioned it never obtains a pre-vote quorum, so it never
/// increments its term, and when the partition heals it rejoins under the
/// leader's term with no election. Without pre-vote the isolated node would
/// ratchet its term on every timeout and force a disruptive election on heal.
#[test]
fn isolated_follower_prevote_does_not_disturb_stable_leader() {
    let seed = 0x9E_1501A7ED;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));

    let leader = leader_index(&nodes, &[0, 1, 2], seed);
    let stable_term = nodes[leader].term();
    let follower = (0..3).find(|&i| i != leader).unwrap();
    let other = (0..3).find(|&i| i != leader && i != follower).unwrap();

    // Isolate the follower from both other nodes.
    sim.partition_pair(nid(follower as u64), nid(leader as u64));
    sim.partition_pair(nid(follower as u64), nid(other as u64));

    // Let it sit isolated long enough for many election timeouts to fire.
    sim.run_for(Duration::from_secs(5));

    // The stable leader kept its leadership and, crucially, its term: the
    // isolated node's pre-vote rounds could not inflate the cluster's term.
    assert!(
        nodes[leader].is_leader(),
        "the stable leader was displaced by an isolated node's pre-votes (seed={seed})"
    );
    assert_eq!(
        nodes[leader].term(),
        stable_term,
        "the stable leader's term moved while a follower was isolated (seed={seed})"
    );
    // The isolated follower never incremented its own term either (pre-vote
    // rounds do not bump the term).
    assert_eq!(
        nodes[follower].term(),
        stable_term,
        "an isolated pre-candidate inflated its term (seed={seed})"
    );

    // Heal: the follower rejoins under the same term with no disruption.
    sim.heal(nid(follower as u64), nid(leader as u64));
    sim.heal(nid(follower as u64), nid(other as u64));
    sim.run_for(Duration::from_secs(2));

    assert!(
        nodes[leader].is_leader(),
        "leader lost after heal (seed={seed})"
    );
    assert_eq!(
        nodes[leader].term(),
        stable_term,
        "healing the partition triggered a spurious election (seed={seed})"
    );
    assert_eq!(
        nodes[follower].leader(),
        Some(nid(leader as u64)),
        "the rejoined follower did not recognize the stable leader (seed={seed})"
    );
}

/// Pre-vote does not block a *legitimate* election: once the leader is genuinely
/// gone, the survivors run a pre-vote round (they, too, have lost the leader),
/// win it, and elect a new leader at a higher term.
#[test]
fn election_still_succeeds_when_leader_is_gone() {
    let seed = 0x60_4E1EAD;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));

    let old_leader = leader_index(&nodes, &[0, 1, 2], seed);
    let old_term = nodes[old_leader].term();

    sim.crash(nid(old_leader as u64));
    sim.run_for(Duration::from_secs(4));

    let survivors: Vec<usize> = (0..3).filter(|&i| i != old_leader).collect();
    let new_leader = leader_index(&nodes, &survivors, seed);
    assert!(
        nodes[new_leader].term() > old_term,
        "a genuine failure must still elect a new leader at a higher term (seed={seed})"
    );

    // The new leader can make progress.
    assert!(matches!(
        nodes[new_leader].propose(MetaCommand::UpsertMember {
            node: nid(42),
            labels: Default::default(),
            status: NodeStatus::Active,
        }),
        animus_control::raft::ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));
    for &s in &survivors {
        assert!(
            nodes[s].metadata().members.contains_key(&nid(42)),
            "post-election write did not replicate to survivor {s} (seed={seed})"
        );
    }
}
