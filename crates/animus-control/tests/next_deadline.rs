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
use std::time::Duration;

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
            needs_snapshot: false,
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
            learners: None,
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
            needs_snapshot: false,
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

/// Issue #667 follow-up: `next_deadline()` must wake this node in time to
/// resend its boot-time cluster-check probe (`RaftCore::
/// cluster_check_resend_deadline`) even while `election_deadline` has been
/// pushed far into the future by legitimate contact from an already-elected
/// sibling. `node.rs`'s driver loop sleeps for EXACTLY what `next_deadline()`
/// returns and calls `tick()` only then — before this fix, `next_deadline()`
/// returned only `election_deadline` for a non-leader, so a still-pending
/// founder that started receiving ordinary `AppendEntries` from a sibling
/// that already won its own election would oversleep past its own resend
/// deadline for as long as `election_deadline` kept getting reset, even
/// though `tick()`'s own resend logic was correct in isolation. This is the
/// exact CI regression PR #902 hit under staggered, CPU-starved bring-up
/// ("cluster did not bootstrap in 20s").
#[test]
fn next_deadline_wakes_for_a_cluster_check_resend_even_after_election_deadline_is_pushed_out() {
    let mut core: RaftCore = RaftCore::new(nid(0), &group(), Nanos(0), 7);
    let _ = core.begin_cluster_check(NOW, 7);
    assert!(
        core.cluster_check_pending(),
        "a fresh (empty-WAL) node with peers must start the boot-time check"
    );

    let deadline_before = core
        .next_deadline()
        .expect("a pending cluster check must still want a timer");
    let base_nanos = Duration::from_millis(150).as_nanos() as u64;
    assert!(
        deadline_before.0 <= NOW.0 + 2 * base_nanos,
        "the resend deadline must fall within one randomized election-base \
         window, not some later election_deadline: {deadline_before:?}"
    );

    // A legitimate heartbeat/AppendEntries from an already-elected sibling
    // (n1, having won a real election among a majority formed without this
    // still-checking node) arrives well before `deadline_before` and
    // legitimately pushes `election_deadline` far into the future —
    // `handle_append_entries`'s own contract, unrelated to this fix.
    let contact_at = Nanos(NOW.0 + base_nanos / 4);
    let _ = core.handle(
        nid(1),
        RaftMsg::AppendEntries {
            term: 5,
            leader: nid(1),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        },
        contact_at,
        7,
    );
    assert!(
        core.cluster_check_pending(),
        "ordinary leader contact must not resolve the still-pending check"
    );

    // The load-bearing assertion: `next_deadline()` must still point at (or
    // before) the ORIGINAL resend deadline, never at whatever far-future
    // `election_deadline` the heartbeat above just armed.
    let deadline_after = core
        .next_deadline()
        .expect("the pending cluster check must still want a timer");
    assert!(
        deadline_after.0 <= deadline_before.0,
        "next_deadline() must not oversleep past the cluster-check resend \
         deadline just because election_deadline was pushed out: before \
         heartbeat {deadline_before:?}, after {deadline_after:?}"
    );

    // Driving a `tick()` at that deadline must actually resend the probe —
    // proving the fixed `next_deadline()` and the pre-existing resend logic
    // in `tick()` compose correctly end to end.
    let outs = core.tick(deadline_after, 7);
    assert!(
        outs.iter().any(|(_, m)| matches!(m, RaftMsg::ClusterProbe)),
        "expected a resent ClusterProbe broadcast at the resend deadline: {outs:?}"
    );

    // A genuinely fresh peer reply is unconditionally decisive and resolves
    // the check immediately, regardless of the rest of the (still silent)
    // peer set.
    let _ = core.handle(
        nid(1),
        RaftMsg::ClusterProbeResp {
            term: 0,
            committed_index: 0,
            config: set(&group()),
        },
        deadline_after,
        7,
    );
    assert!(
        !core.cluster_check_pending(),
        "a genuinely fresh peer reply must resolve the check"
    );
    assert!(
        !core.refused_as_voter(),
        "a fresh-peer resolution must never refuse this node as a voter"
    );
}
