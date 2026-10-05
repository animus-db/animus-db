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
    // Only node 2 has acked the leader's log: it alone holds the committed
    // prefix (match >= commit), node 1 (match 0) is not a legal target.
    let mut core = elect_leader();
    ack(&mut core, 2);
    assert!(core.commit_index() >= 1);
    assert_eq!(core.peer_match(&nid(1)), 0);
    assert_eq!(core.storage_full_step_down(NOW, None), Some(nid(2)));
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
