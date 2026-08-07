//! Core-level tests for the leadership-transfer primitive (ADR 0029): the
//! mechanism that lets a *healthy* replica move off the current leader of a
//! Raft group, which `change_membership` alone cannot do (it always rejects
//! removing the leader).
//!
//! Three pieces, each driven by hand-built `RaftCore`s (no driver, no sim) so
//! the exact message sequence is deterministic and inspectable, mirroring
//! `membership_commit_gate.rs`'s style:
//!  - [`RaftCore::transfer_leadership`] only arms for a caught-up, current
//!    voter, and `broadcast_append` retries the resulting `TimeoutNow` every
//!    heartbeat until this node steps down;
//!  - receiving [`RaftMsg::TimeoutNow`] makes a voter campaign **immediately**,
//!    bypassing pre-vote (the live-leader lease would otherwise reject it) —
//!    exactly one term bump, straight to `RequestVote`;
//!  - a peer voted out of the configuration keeps receiving the removing
//!    entry (the `departing` mechanism) until it acks past it, instead of
//!    only ever inferring its removal from pre-vote rejection.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::{ProposeResult, RaftCore, RaftMsg, Role};
use animus_env::{Nanos, NodeId};

const GROUP: [NodeId; 3] = [0, 1, 2];
const NOW: Nanos = Nanos(1_000_000_000);

fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
    ids.iter().copied().collect()
}

fn after(base: Nanos, d: Duration) -> Nanos {
    Nanos(base.0 + d.as_nanos() as u64)
}

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

/// Elect node `GROUP[0]` leader of the 3-node group (see
/// `membership_commit_gate.rs`'s identical helper). The election no-op sits
/// **uncommitted** at index 1 until a follower acks it.
fn elect_leader() -> RaftCore {
    let mut core: RaftCore = RaftCore::new(GROUP[0], &GROUP, Nanos(0), 7);
    let _ = core.tick(NOW, 7);
    let _ = core.handle(
        GROUP[1],
        RaftMsg::PreVoteResp {
            term: core.term() + 1,
            granted: true,
        },
        NOW,
        7,
    );
    let _ = core.handle(
        GROUP[1],
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

/// Feed the leader a success ack from `from` matching its whole log, advancing
/// commit and fully catching that peer up (`match_index == last_log_index`).
fn ack_all(core: &mut RaftCore, from: NodeId) {
    let _ = core.handle(
        from,
        RaftMsg::AppendEntriesResp {
            term: core.term(),
            success: true,
            match_index: core.last_log_index(),
        },
        NOW,
        7,
    );
}

// ---- transfer_leadership: preconditions -----------------------------------

#[test]
fn transfer_leadership_rejects_self_a_laggard_and_a_non_member() {
    let mut core = elect_leader();
    assert!(!core.transfer_leadership(0), "cannot transfer to self");
    assert!(
        !core.transfer_leadership(1),
        "node 1 has not acked anything yet (match_index 0 < last_log_index 1)"
    );
    assert!(
        !core.transfer_leadership(99),
        "99 is not in the current configuration"
    );
}

#[test]
fn transfer_leadership_arms_a_caught_up_target_and_is_retried_every_heartbeat() {
    let mut core = elect_leader();
    ack_all(&mut core, 1); // commits the no-op + fully catches node 1 up

    assert!(core.transfer_leadership(1));

    let hb1 = after(NOW, Duration::from_millis(60));
    let outs1 = core.tick(hb1, 7);
    let sent: Vec<_> = outs1
        .iter()
        .filter(|(to, m)| *to == 1 && matches!(m, RaftMsg::TimeoutNow { .. }))
        .collect();
    assert_eq!(
        sent.len(),
        1,
        "expected exactly one TimeoutNow to the caught-up target: {outs1:?}"
    );

    // Retried on the *next* heartbeat too — resilience against one dropped
    // message; still leader, still armed.
    let hb2 = after(hb1, Duration::from_millis(60));
    let outs2 = core.tick(hb2, 7);
    let sent2 = outs2
        .iter()
        .filter(|(to, m)| *to == 1 && matches!(m, RaftMsg::TimeoutNow { .. }))
        .count();
    assert_eq!(
        sent2, 1,
        "a transfer must be retried on every heartbeat until this node steps down"
    );
}

#[test]
fn transfer_leadership_does_not_survive_a_fresh_election_win() {
    let mut core = elect_leader();
    ack_all(&mut core, 1);
    assert!(core.transfer_leadership(1));

    // Deposed by a higher-term leader (node 2). The generic higher-term
    // step-down at the top of `handle` flips role/term before the message is
    // even dispatched to its specific handler.
    let depose_at = after(NOW, Duration::from_secs(1));
    core.handle(2, heartbeat(2, core.term() + 1), depose_at, 7);
    assert_eq!(core.role(), Role::Follower);

    // Re-win a later election the normal way (pre-vote, then the real vote).
    let t = after(depose_at, Duration::from_secs(1));
    let _ = core.tick(t, 7); // election timeout -> pre-candidate
    let _ = core.handle(
        1,
        RaftMsg::PreVoteResp {
            term: core.term() + 1,
            granted: true,
        },
        t,
        7,
    );
    let _ = core.handle(
        1,
        RaftMsg::RequestVoteResp {
            term: core.term(),
            granted: true,
        },
        t,
        7,
    );
    assert!(core.is_leader(), "node 0 should have re-won the election");

    let hb = after(t, Duration::from_millis(60));
    let outs = core.tick(hb, 7);
    assert!(
        outs.iter()
            .all(|(_, m)| !matches!(m, RaftMsg::TimeoutNow { .. })),
        "a stale transfer from a previous leadership stint must not survive a fresh election win: {outs:?}"
    );
}

// ---- TimeoutNow: bypasses pre-vote -----------------------------------------

#[test]
fn timeout_now_triggers_immediate_election_bypassing_pre_vote_and_costs_one_term() {
    let mut core: RaftCore = RaftCore::new(1, &GROUP, Nanos(0), 7);
    // A live leader's heartbeat gives this follower a "lease" that would make a
    // normal election-timeout tick only start a *pre-vote* round (see
    // pre_vote.rs), never campaign directly.
    core.handle(0, heartbeat(0, 5), NOW, 7);
    assert_eq!(core.term(), 5);
    assert_eq!(core.role(), Role::Follower);

    let after_hb = after(NOW, Duration::from_millis(1));
    let outs = core.handle(0, RaftMsg::TimeoutNow { term: 5 }, after_hb, 7);

    assert_eq!(
        core.role(),
        Role::Candidate,
        "TimeoutNow must go straight to a real candidacy, skipping PreCandidate"
    );
    assert_eq!(core.term(), 6, "exactly one term bump for the transfer");
    assert!(!outs.is_empty());
    assert!(
        outs.iter()
            .all(|(_, m)| matches!(m, RaftMsg::RequestVote { .. })),
        "TimeoutNow must solicit real votes directly, never a PreVote: {outs:?}"
    );
}

#[test]
fn timeout_now_is_ignored_when_stale_already_leader_or_not_a_voter() {
    // Stale term: this node already moved on from the term the transfer named.
    let mut stale: RaftCore = RaftCore::new(1, &GROUP, Nanos(0), 7);
    stale.handle(0, heartbeat(0, 5), NOW, 7);
    let outs = stale.handle(
        0,
        RaftMsg::TimeoutNow { term: 4 },
        after(NOW, Duration::from_millis(1)),
        7,
    );
    assert!(outs.is_empty());
    assert_eq!(stale.role(), Role::Follower);
    assert_eq!(
        stale.term(),
        5,
        "a stale transfer must not perturb term or role"
    );

    // Already the leader: nonsensical to timeout-now ourselves.
    let mut leader = elect_leader();
    let leader_term = leader.term();
    let outs2 = leader.handle(1, RaftMsg::TimeoutNow { term: leader_term }, NOW, 7);
    assert!(outs2.is_empty());
    assert!(leader.is_leader());

    // Not a voter: this node's id was never part of the configuration.
    let not_voter_group: [NodeId; 2] = [0, 2];
    let mut not_voter: RaftCore = RaftCore::new(1, &not_voter_group, Nanos(0), 7);
    let outs3 = not_voter.handle(0, RaftMsg::TimeoutNow { term: 0 }, NOW, 7);
    assert!(outs3.is_empty());
    assert_eq!(not_voter.role(), Role::Follower);
}

// ---- departing-peer notification -------------------------------------------

/// A peer voted out of the configuration keeps being sent the removing config
/// entry (via `broadcast_append`, even though `apply_config` has already
/// dropped it from `peers`) until it acks past that entry — the mechanism that
/// makes a removed node adopt a config excluding itself, rather than only ever
/// inferring the removal from a rejected pre-vote (which never touches term or
/// role, so a very-late-partitioned removed node could otherwise sit forever
/// believing it is still a voter).
#[test]
fn departing_peer_keeps_receiving_the_removal_entry_until_it_acks() {
    let mut core = elect_leader();
    ack_all(&mut core, 1);
    ack_all(&mut core, 2);

    let result = core.change_membership(set(&[0, 1]));
    assert!(
        matches!(result, ProposeResult::Accepted { .. }),
        "{result:?}"
    );
    let removal_index = core.last_log_index();

    // Node 2 is out of the active configuration immediately.
    assert!(!core.config().contains(&2));

    // But the next heartbeat must still target it, carrying the entry that
    // removed it.
    let hb1 = after(NOW, Duration::from_millis(60));
    let outs1 = core.tick(hb1, 7);
    let to_two: Vec<_> = outs1.iter().filter(|(to, _)| *to == 2).collect();
    assert_eq!(
        to_two.len(),
        1,
        "the departing peer must still be sent the removing entry: {outs1:?}"
    );
    match &to_two[0].1 {
        RaftMsg::AppendEntries { entries, .. } => {
            assert!(
                entries
                    .iter()
                    .any(|e| e.index == removal_index && e.config.is_some()),
                "expected the removal config entry among {entries:?}"
            );
        }
        other => panic!("expected an AppendEntries carrying the removal config, got {other:?}"),
    }

    // Once node 2 acks past the removal index, it durably has the config
    // excluding itself — stop targeting it.
    core.handle(
        2,
        RaftMsg::AppendEntriesResp {
            term: core.term(),
            success: true,
            match_index: removal_index,
        },
        hb1,
        7,
    );
    let hb2 = after(hb1, Duration::from_millis(60));
    let outs2 = core.tick(hb2, 7);
    assert!(
        outs2.iter().all(|(to, _)| *to != 2),
        "a caught-up departed peer must not be re-targeted: {outs2:?}"
    );
}

/// A peer re-added to the configuration before it fully caught up on its
/// removal is no longer "departing" — it is just an ordinary voter again, kept
/// current the normal way.
#[test]
fn a_peer_re_added_before_catching_up_on_removal_is_no_longer_departing() {
    let mut core = elect_leader();
    ack_all(&mut core, 1);
    ack_all(&mut core, 2);

    assert!(matches!(
        core.change_membership(set(&[0, 1])),
        ProposeResult::Accepted { .. }
    ));
    // Commit the removal so a second single-server change is accepted.
    core.handle(
        1,
        RaftMsg::AppendEntriesResp {
            term: core.term(),
            success: true,
            match_index: core.last_log_index(),
        },
        NOW,
        7,
    );
    assert!(matches!(
        core.change_membership(set(&[0, 1, 2])),
        ProposeResult::Accepted { .. }
    ));
    assert!(core.config().contains(&2));

    // Node 2 is back in `peers`, so it is no longer separately tracked as
    // departing (no duplicate targeting).
    let hb = after(NOW, Duration::from_millis(60));
    let outs = core.tick(hb, 7);
    let to_two = outs.iter().filter(|(to, _)| *to == 2).count();
    assert_eq!(
        to_two, 1,
        "re-added peer should be targeted once, as an ordinary peer: {outs:?}"
    );
}
