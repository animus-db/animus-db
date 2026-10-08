//! Issue #1219 (R-01 (d), ADR 0074 §2): a storage-full node never campaigns,
//! and a storage-full leader hands leadership to its most up-to-date voter.
//!
//! Hand-driven `RaftCore`s only (the idiom of `next_deadline.rs`), so the exact
//! ticks/messages fed to the core are inspectable. The end-to-end behavior over
//! real per-tablet groups is the disk-full corpus in `animus-test`
//! (`raftkv_linearizable.rs`, `LeaderDiskFull`).

use std::collections::BTreeSet;

use animus_control::{RaftCore, RaftMsg, Role};
use animus_env::{Nanos, NodeId, nid};

fn group() -> [NodeId; 3] {
    [nid(0), nid(1), nid(2)]
}
const NOW: Nanos = Nanos(1_000_000_000);
const LATER: Nanos = Nanos(60_000_000_000);

fn elect_leader() -> RaftCore {
    let mut core: RaftCore = RaftCore::new(group()[0].clone(), &group(), Nanos(0), 7);
    let _ = core.tick(NOW, 7);
    let _ = core.handle(
        nid(1),
        RaftMsg::PreVoteResp {
            term: core.term() + 1,
            granted: true,
        },
        NOW,
        7,
    );
    let _ = core.handle(
        nid(1),
        RaftMsg::RequestVoteResp {
            term: core.term(),
            granted: true,
        },
        NOW,
        7,
    );
    assert!(core.is_leader());
    core
}

fn ack(core: &mut RaftCore, from: u64) {
    let term = core.term();
    let match_index = core.last_log_index();
    let _ = core.handle(
        nid(from),
        RaftMsg::AppendEntriesResp {
            term,
            success: true,
            match_index,
            needs_snapshot: false,
            check_pending: false,
        },
        NOW,
        7,
    );
}

#[test]
fn storage_full_follower_never_campaigns_and_ignores_timeout_now() {
    let mut core: RaftCore = RaftCore::new(group()[1].clone(), &group(), Nanos(0), 7);
    core.set_storage_full(true);
    // Far past any election deadline: a healthy follower would pre-vote.
    let outs = core.tick(LATER, 7);
    assert!(outs.is_empty(), "a storage-full node sent {outs:?}");
    assert_eq!(core.role(), Role::Follower);
    assert_eq!(core.term(), 0, "no term bump while storage-full");
    // A `TimeoutNow` for the current term is declined too.
    let outs = core.handle(nid(0), RaftMsg::TimeoutNow { term: 0 }, LATER, 9);
    assert!(outs.is_empty());
    assert_eq!(core.term(), 0);
    assert_eq!(core.role(), Role::Follower);

    // Space returned: the very next deadline campaigns normally.
    core.set_storage_full(false);
    let outs = core.tick(Nanos(LATER.0 * 2), 7);
    assert!(!outs.is_empty(), "a recovered node should pre-vote again");
    assert_eq!(core.role(), Role::PreCandidate);
}

#[test]
fn storage_full_leader_arms_a_transfer_to_the_most_caught_up_voter() {
    let mut core = elect_leader();
    ack(&mut core, 1);
    ack(&mut core, 2);
    // Equal matches: the lowest id wins the tie, deterministically.
    assert_eq!(core.storage_full_step_down(NOW, None), Some(nid(1)));
    assert_eq!(core.transfer_target(), Some(nid(1)));
    // Already armed: nothing further to do.
    assert_eq!(core.storage_full_step_down(NOW, None), None);
}

#[test]
fn storage_full_step_down_rotates_away_from_an_unanswered_target() {
    let mut core = elect_leader();
    ack(&mut core, 1);
    ack(&mut core, 2);
    // The previous attempt named node 1 and it never took over (itself full):
    // the retry goes to node 2.
    assert_eq!(
        core.storage_full_step_down(NOW, Some(&nid(1))),
        Some(nid(2))
    );
    assert_eq!(core.transfer_target(), Some(nid(2)));
}

#[test]
fn storage_full_step_down_skips_a_voter_behind_the_commit_index() {
    // Node 2 holds the committed prefix (match >= commit); node 1 is healthy
    // (it acked, not full) but behind: not a legal target.
    let mut core = elect_leader();
    ack_with(&mut core, 1, 0, false, NOW);
    ack(&mut core, 2);
    assert!(core.commit_index() >= 1);
    assert_eq!(core.peer_match(&nid(1)), 0);
    assert_eq!(core.storage_full_step_down(NOW, None), Some(nid(2)));
}

fn ack_with(core: &mut RaftCore, from: u64, match_index: u64, check_pending: bool, at: Nanos) {
    let term = core.term();
    let _ = core.handle(
        nid(from),
        RaftMsg::AppendEntriesResp {
            term,
            success: true,
            match_index,
            needs_snapshot: false,
            check_pending,
        },
        at,
        7,
    );
}

/// Issue #1228: a follower that reported `check_pending` (which a storage-full
/// follower does on its frozen acks) is never a transfer target, and without a
/// quorum of healthy followers the full leader keeps leading -- stepping down
/// would depose the one node that can still serve reads and refuse writes, with
/// nobody able to win an election or commit.
#[test]
fn storage_full_step_down_needs_a_healthy_quorum_of_followers() {
    // RF3: majority 2, so BOTH followers must be healthy.
    let mut core = elect_leader();
    ack(&mut core, 1);
    let last = core.last_log_index();
    ack_with(&mut core, 2, last, true, NOW); // node 2 is full
    assert_eq!(core.peer_check_pending(&nid(2)), Some(true));
    assert_eq!(
        core.storage_full_step_down(NOW, None),
        None,
        "one healthy follower of two is not a quorum that can elect or commit"
    );
    assert_eq!(core.transfer_target(), None);
    // Node 2's space returns and it reports healthy: the handoff proceeds.
    ack(&mut core, 2);
    assert_eq!(core.storage_full_step_down(NOW, None), Some(nid(1)));
}

#[test]
fn storage_full_step_down_ignores_a_stale_health_report() {
    let mut core = elect_leader();
    ack(&mut core, 1);
    ack(&mut core, 2);
    // Both reported healthy at NOW, but nothing has been heard since: after far
    // longer than an election timeout the reports prove nothing.
    assert_eq!(core.storage_full_step_down(LATER, None), None);
    assert!(core.healthy_followers(LATER).is_empty());
    assert_eq!(core.healthy_followers(NOW).len(), 2);
}

#[test]
fn a_transfer_never_targets_a_voter_that_reported_it_is_full() {
    // The preferred-leader step, rebalance and the step-down all arm through
    // `transfer_leadership`: none may steer leadership onto a full node.
    let mut core = elect_leader();
    ack(&mut core, 1);
    let last = core.last_log_index();
    ack_with(&mut core, 2, last, true, NOW);
    assert!(!core.transfer_leadership(nid(2), NOW));
    assert_eq!(core.transfer_target(), None);
    assert!(core.transfer_leadership(nid(1), NOW));
}

#[test]
fn a_full_leader_without_a_healthy_quorum_refuses_any_transfer() {
    let mut core = elect_leader();
    ack(&mut core, 1);
    let last = core.last_log_index();
    ack_with(&mut core, 2, last, true, NOW);
    core.set_storage_full(true);
    // Node 1 is healthy and caught up, but alone it cannot lead a group whose
    // other two voters (this leader included) cannot persist.
    assert!(!core.transfer_leadership(nid(1), NOW));
    ack(&mut core, 2);
    assert!(core.transfer_leadership(nid(1), NOW));
}

/// The ack a storage-full follower sends is frozen at its durable index, and a
/// leader counting it can never commit an entry the follower did not persist.
#[test]
fn a_full_followers_ack_is_frozen_at_its_durable_index() {
    use animus_control::meta::MetaCommand;
    let mut follower: RaftCore = RaftCore::new(nid(1), &group(), Nanos(0), 7);
    let next = std::cell::Cell::new(0u64);
    let entry = |term: u64| {
        next.set(next.get() + 1);
        animus_control::LogEntry {
            term,
            index: next.get(),
            command: MetaCommand::NoOp,
            config: None,
            learners: None,
        }
    };
    let ae = |prev: u64, entries: Vec<_>| RaftMsg::AppendEntries {
        term: 1,
        leader: nid(0),
        prev_log_index: prev,
        prev_log_term: u64::from(prev > 0),
        entries,
        leader_commit: 0,
    };
    // Healthy: two entries land and are made durable.
    let _ = follower.handle(nid(0), ae(0, vec![entry(1), entry(1)]), NOW, 7);
    follower.mark_durable_through(2);
    // Out of disk: a further entry is taken into memory but cannot be persisted.
    follower.set_storage_full(true);
    let outs = follower.handle(nid(0), ae(2, vec![entry(1)]), NOW, 7);
    assert_eq!(follower.durable_index(), 2);
    assert_eq!(follower.last_log_index(), 3);
    match &outs[0].1 {
        RaftMsg::AppendEntriesResp {
            success,
            match_index,
            check_pending,
            ..
        } => {
            assert!(*success, "a full follower still answers");
            assert_eq!(*match_index, 2, "frozen at the durable index, not 3");
            assert!(*check_pending, "and says it cannot vote");
        }
        other => panic!("expected an AppendEntriesResp, got {other:?}"),
    }
    // A bare heartbeat is acked the same way (this is what keeps a leader's
    // lease and read barriers alive through an every-replica-full outage).
    let outs = follower.handle(nid(0), ae(3, vec![]), NOW, 7);
    assert!(matches!(
        &outs[0].1,
        RaftMsg::AppendEntriesResp {
            success: true,
            match_index: 2,
            ..
        }
    ));

    // Leader side: node 1's frozen ack at 2 and node 2's silence leave entry 3
    // uncommitted (it is held by the leader alone), and entry 2 -- which node 1
    // really persisted -- is the most that can ever commit through it.
    let mut leader = elect_leader();
    let _ = leader.propose(MetaCommand::NoOp);
    let _ = leader.propose(MetaCommand::NoOp);
    let _ = leader.propose(MetaCommand::NoOp);
    let last = leader.last_log_index();
    ack_with(&mut leader, 1, last - 1, true, NOW);
    assert!(
        leader.commit_index() < last,
        "commit advanced past the follower's frozen match"
    );
    assert_eq!(leader.peer_match(&nid(1)), last - 1);
}

#[test]
fn storage_full_step_down_is_leader_only() {
    let mut core: RaftCore = RaftCore::new(group()[1].clone(), &group(), Nanos(0), 7);
    assert_eq!(core.storage_full_step_down(NOW, None), None);
    // Sanity: the set is the voter config.
    assert_eq!(
        core.config(),
        group().into_iter().collect::<BTreeSet<NodeId>>()
    );
}
