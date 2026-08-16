//! ADR 0044 phase-1 PR2: `RaftCore::next_deadline` changed from `Nanos` to
//! `Option<Nanos>` — the mechanism (both drivers now drop the timer arm from
//! their `select` on `None`) lands here, but the core has no way yet to
//! *produce* `None` (that's phase-1 PR3's `quiesce_after`/quiesced state
//! machine). So the one property this PR must prove at the core level is the
//! converse: **every existing state still returns `Some`**, byte-identical to
//! the pre-`Option` behavior — a fresh follower, a node running a pre-vote
//! round, a candidate mid real-election, a leader, a leader with a
//! leadership transfer armed, and a follower mid an in-progress (not yet
//! complete) snapshot install.
//!
//! Hand-driven `RaftCore`s only (no driver, no `Simulator`) — mirrors
//! `leadership_transfer.rs`'s style, so the exact sequence of ticks/messages
//! fed to the core is deterministic and inspectable.

use std::collections::BTreeSet;

use animus_control::raft::SNAPSHOT_CHUNK_BYTES;
use animus_control::{ProposeResult, RaftCore, RaftMsg, Role};
use animus_env::{Nanos, NodeId, nid};

fn group() -> [NodeId; 3] {
    [nid(0), nid(1), nid(2)]
}
const NOW: Nanos = Nanos(1_000_000_000);

fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
    ids.iter().cloned().collect()
}

/// Elect `group()[0]` leader of the 3-node group (same idiom as
/// `leadership_transfer.rs::elect_leader`).
fn elect_leader() -> RaftCore {
    let mut core: RaftCore = RaftCore::new(group()[0].clone(), &group(), Nanos(0), 7);
    let _ = core.tick(NOW, 7);
    let _ = core.handle(
        group()[1].clone(),
        RaftMsg::PreVoteResp {
            term: core.term() + 1,
            granted: true,
        },
        NOW,
        7,
    );
    let _ = core.handle(
        group()[1].clone(),
        RaftMsg::RequestVoteResp {
            term: core.term(),
            granted: true,
        },
        NOW,
        7,
    );
    assert!(core.is_leader(), "node 0 should have won the election");
    core
}

#[test]
fn next_deadline_is_some_for_a_fresh_follower() {
    let core: RaftCore = RaftCore::new(nid(0), &group(), Nanos(0), 7);
    assert_eq!(core.role(), Role::Follower);
    assert!(
        core.next_deadline().is_some(),
        "a fresh follower must still want an election timer \
         (quiescence, phase-1 PR3, doesn't exist yet)"
    );
}

#[test]
fn next_deadline_is_some_for_a_pre_candidate_mid_election() {
    let mut core: RaftCore = RaftCore::new(nid(0), &group(), Nanos(0), 7);
    let outs = core.tick(NOW, 7); // election timeout -> pre-vote round
    assert_eq!(core.role(), Role::PreCandidate);
    assert!(
        outs.iter()
            .any(|(_, m)| matches!(m, RaftMsg::PreVote { .. })),
        "expected a PreVote round to start: {outs:?}"
    );
    assert!(
        core.next_deadline().is_some(),
        "a pre-candidate must still want a timer to retry the election"
    );
}

#[test]
fn next_deadline_is_some_for_a_candidate_mid_real_election() {
    let mut core: RaftCore = RaftCore::new(nid(0), &group(), Nanos(0), 7);
    let _ = core.tick(NOW, 7); // -> pre-candidate
    let outs = core.handle(
        nid(1),
        RaftMsg::PreVoteResp {
            term: core.term() + 1,
            granted: true,
        },
        NOW,
        7,
    );
    assert_eq!(
        core.role(),
        Role::Candidate,
        "a pre-vote majority (self + node 1 of 3) must start the real election"
    );
    assert!(
        outs.iter()
            .any(|(_, m)| matches!(m, RaftMsg::RequestVote { .. })),
        "expected RequestVote to have been sent: {outs:?}"
    );
    assert!(
        core.next_deadline().is_some(),
        "a candidate awaiting real votes must still want a timer to retry"
    );
}

#[test]
fn next_deadline_is_some_for_a_leader() {
    let core = elect_leader();
    assert_eq!(core.role(), Role::Leader);
    assert!(
        core.next_deadline().is_some(),
        "a leader must still want a heartbeat timer"
    );
}

#[test]
fn next_deadline_is_some_while_a_leadership_transfer_is_armed() {
    let mut core = elect_leader();
    // Catch node 1 up so it's a legally armable target.
    let _ = core.handle(
        nid(1),
        RaftMsg::AppendEntriesResp {
            term: core.term(),
            success: true,
            match_index: core.last_log_index(),
        },
        NOW,
        7,
    );
    assert!(core.transfer_leadership(nid(1), NOW));
    assert!(
        core.next_deadline().is_some(),
        "an armed transfer must still resolve to a concrete deadline \
         (min of the heartbeat and the transfer's own abort deadline)"
    );
}

#[test]
fn next_deadline_is_some_mid_an_in_progress_snapshot_install() {
    // A follower mid a chunked, not-yet-`done` InstallSnapshot transfer — the
    // "snapshot pending" state named in this PR's test list. `next_deadline`
    // doesn't (and, per the design sketch, won't in phase-1 PR3 either) key
    // off `incoming_snapshot`/`pending_install` directly; the leader-side
    // entry predicate excludes it instead. This test pins today's actual
    // behavior: role stays `Follower` throughout, so the deadline is still
    // `Some(election_deadline)`.
    let mut follower: RaftCore = RaftCore::new(nid(1), &group(), Nanos(0), 7);
    let chunk = vec![0u8; SNAPSHOT_CHUNK_BYTES];
    let outs = follower.handle(
        nid(0),
        RaftMsg::InstallSnapshot {
            term: 1,
            leader: nid(0),
            last_index: 10,
            last_term: 1,
            offset: 0,
            data: chunk.clone(),
            total: chunk.len() as u64 * 2, // one more chunk still to come
            done: false,
            config: None,
        },
        NOW,
        7,
    );
    assert!(
        outs.iter()
            .any(|(_, m)| matches!(m, RaftMsg::InstallSnapshotResp { .. })),
        "expected an ack for the partial chunk: {outs:?}"
    );
    assert_eq!(
        follower.role(),
        Role::Follower,
        "receiving a snapshot chunk must not change role"
    );
    assert!(
        follower.next_deadline().is_some(),
        "a follower mid an in-progress snapshot transfer must still want an \
         election timer"
    );

    // Sanity: the transfer really is still in progress (not yet installed).
    assert!(!follower.has_pending_install());
}

#[test]
fn next_deadline_stays_some_across_a_membership_change() {
    let mut core = elect_leader();
    let _ = core.handle(
        nid(1),
        RaftMsg::AppendEntriesResp {
            term: core.term(),
            success: true,
            match_index: core.last_log_index(),
        },
        NOW,
        7,
    );
    assert!(matches!(
        core.change_membership(set(&[nid(0), nid(1)])),
        ProposeResult::Accepted { .. }
    ));
    assert!(
        core.next_deadline().is_some(),
        "an in-flight config change must not turn off the leader's own timer"
    );
}
