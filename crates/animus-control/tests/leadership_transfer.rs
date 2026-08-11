//! Core-level tests for the leadership-transfer primitive (ADR 0029, hardened by
//! the follow-up documented in the root CLAUDE.md engineering-practices
//! section): the mechanism that lets a *healthy* replica move off the current
//! leader of a Raft group, which `change_membership` alone cannot do (it
//! always rejects removing the leader).
//!
//! Four pieces, each driven by hand-built `RaftCore`s (no driver, no sim) so
//! the exact message sequence is deterministic and inspectable, mirroring
//! `membership_commit_gate.rs`'s style:
//!  - [`RaftCore::transfer_leadership`] arms for a *reasonably close*
//!    (`peer_match >= commit_index`, not `== last_log_index` — see below),
//!    current voter, and `broadcast_append` retries the resulting
//!    `TimeoutNow` every heartbeat, but only **once the target has actually
//!    caught up to `last_log_index`**, until this node steps down;
//!  - while a transfer is armed, `propose`/`change_membership` freeze
//!    (`NotLeader`) instead of growing the log — this is what lets a target
//!    that is only "reasonably close" at arm time actually reach
//!    `last_log_index` under sustained writes, instead of the log tip
//!    perpetually running away from it;
//!  - a transfer that is never completed (deadline passes with no step-down —
//!    e.g. the target crashed after arming) aborts, resuming proposals;
//!  - receiving [`RaftMsg::TimeoutNow`] makes a voter campaign **immediately**,
//!    bypassing pre-vote (the live-leader lease would otherwise reject it) —
//!    exactly one term bump, straight to `RequestVote`;
//!  - a peer voted out of the configuration keeps receiving the removing
//!    entry (the `departing` mechanism) until it acks past it, instead of
//!    only ever inferring its removal from pre-vote rejection.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::{MetaCommand, ProposeResult, RaftCore, RaftMsg, Role};
use animus_env::{Nanos, NodeId, nid};
fn group() -> [NodeId; 3] {
    [nid(0), nid(1), nid(2)]
}
const NOW: Nanos = Nanos(1_000_000_000);

fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
    ids.iter().cloned().collect()
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

/// Elect node `group()[0]` leader of the 3-node group (see
/// `membership_commit_gate.rs`'s identical helper). The election no-op sits
/// **uncommitted** at index 1 until a follower acks it.
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
    // Advance `commit_index` past 0 via an ack from node 2, while node 1 stays
    // fully behind (match_index 0) — a genuine laggard under the arm gate
    // (`peer_match(target) >= commit_index`, relaxed from `== last_log_index`
    // so a target reasonably close — not necessarily bang up to date — is
    // eligible; see `transfer_leadership_freezes_proposals_...` below for why
    // that relaxation is safe).
    ack_all(&mut core, nid(2));
    assert!(
        core.commit_index() > 0,
        "sanity: commit should have advanced"
    );

    assert!(
        !core.transfer_leadership(nid(0), NOW),
        "cannot transfer to self"
    );
    assert!(
        !core.transfer_leadership(nid(1), NOW),
        "node 1 has not acked anything yet (match_index 0 < commit_index)"
    );
    assert!(
        !core.transfer_leadership(nid(99), NOW),
        "99 is not in the current configuration"
    );
}

#[test]
fn transfer_leadership_arms_a_caught_up_target_and_is_retried_every_heartbeat() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1)); // commits the no-op + fully catches node 1 up

    assert!(core.transfer_leadership(nid(1), NOW));

    let hb1 = after(NOW, Duration::from_millis(60));
    let outs1 = core.tick(hb1, 7);
    let sent: Vec<_> = outs1
        .iter()
        .filter(|(to, m)| *to == nid(1) && matches!(m, RaftMsg::TimeoutNow { .. }))
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
        .filter(|(to, m)| *to == nid(1) && matches!(m, RaftMsg::TimeoutNow { .. }))
        .count();
    assert_eq!(
        sent2, 1,
        "a transfer must be retried on every heartbeat until this node steps down"
    );
}

/// The core of the fix (defect B): arming only requires `peer_match >=
/// commit_index`, which under sustained writes can be well short of
/// `last_log_index` — so `TimeoutNow` must not fire yet (the target would
/// campaign on a stale log), and any further growth of the log must be
/// refused so replication has a fixed target to catch the armed peer up to.
#[test]
fn transfer_leadership_freezes_proposals_and_waits_for_last_log_index_before_timeout_now() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1)); // node 1 matches last_log_index (1) == commit_index (1)

    // Grow the log past what node 1 has seen — it is now only caught up to
    // `commit_index`, not `last_log_index`.
    assert!(matches!(
        core.propose(MetaCommand::NoOp),
        ProposeResult::Accepted { .. }
    ));
    assert_eq!(core.last_log_index(), 2);
    assert_eq!(core.peer_match(&nid(1)), 1);
    assert_eq!(core.commit_index(), 1, "the new entry isn't acked yet");

    // Still armable: node 1 is caught up to commit_index (relaxed gate).
    assert!(core.transfer_leadership(nid(1), NOW));

    // No TimeoutNow yet — node 1 hasn't reached last_log_index.
    let hb1 = after(NOW, Duration::from_millis(60));
    let outs1 = core.tick(hb1, 7);
    assert!(
        outs1
            .iter()
            .all(|(_, m)| !matches!(m, RaftMsg::TimeoutNow { .. })),
        "must not TimeoutNow a target that hasn't reached last_log_index: {outs1:?}"
    );
    // The frozen leader must still replicate normally so node 1 can catch up.
    let replicated_to_one = outs1.iter().any(|(to, m)| {
        *to == nid(1) && matches!(m, RaftMsg::AppendEntries { entries, .. } if !entries.is_empty())
    });
    assert!(
        replicated_to_one,
        "a frozen leader must keep replicating the existing log tail: {outs1:?}"
    );

    // While armed, new proposals are rejected — the log must stop growing.
    let rejected = core.propose(MetaCommand::NoOp);
    assert!(
        matches!(&rejected, ProposeResult::NotLeader { leader: Some(l) } if *l == nid(1)),
        "propose must freeze (and hint the transfer target) while armed: {rejected:?}"
    );
    let rejected_cm = core.change_membership(set(&[nid(0), nid(1)]));
    assert!(
        matches!(&rejected_cm, ProposeResult::NotLeader { leader: Some(l) } if *l == nid(1)),
        "change_membership must freeze while armed: {rejected_cm:?}"
    );
    assert_eq!(
        core.last_log_index(),
        2,
        "a rejected propose must not have appended anything"
    );

    // Node 1 catches up to last_log_index (e.g. via the replication above).
    ack_all(&mut core, nid(1));
    assert_eq!(core.peer_match(&nid(1)), 2);

    let hb2 = after(hb1, Duration::from_millis(60));
    let outs2 = core.tick(hb2, 7);
    let sent = outs2
        .iter()
        .filter(|(to, m)| *to == nid(1) && matches!(m, RaftMsg::TimeoutNow { .. }))
        .count();
    assert_eq!(
        sent, 1,
        "TimeoutNow must fire once the target reaches last_log_index: {outs2:?}"
    );
}

/// Defect fix: if the target never reaches `last_log_index` (so it never gets
/// (or never acts on) `TimeoutNow` and never wins an election) — e.g. it
/// crashed right after arming — the transfer must abort once its deadline
/// passes, resuming proposals rather than stranding the group frozen forever.
#[test]
fn transfer_leadership_aborts_and_resumes_proposing_if_the_target_never_catches_up() {
    let mut core = elect_leader();
    // Arm before node 1 has acked anything: it is trivially caught up to
    // `commit_index` (0, nothing committed yet) but not to `last_log_index`
    // (1, the election no-op) — the "reasonably close but not there yet" case
    // the relaxed gate is meant to admit, relying on replication (which never
    // arrives here, as if node 1 crashed) to close the gap.
    assert_eq!(core.commit_index(), 0);
    assert!(core.transfer_leadership(nid(1), NOW));

    // Node 1 never acks (as if it crashed / is partitioned) — proposals stay
    // frozen while ticks advance short of the deadline, and TimeoutNow must
    // never fire (node 1 never reaches last_log_index).
    let almost = after(NOW, Duration::from_millis(140));
    let outs_almost = core.tick(almost, 7);
    assert!(
        outs_almost
            .iter()
            .all(|(_, m)| !matches!(m, RaftMsg::TimeoutNow { .. })),
        "must never TimeoutNow a target that never reached last_log_index: {outs_almost:?}"
    );
    assert!(
        matches!(
            core.propose(MetaCommand::NoOp),
            ProposeResult::NotLeader { .. }
        ),
        "still armed and frozen short of the deadline"
    );

    // Past one election timeout (150ms default) with no step-down: abort.
    let past_deadline = after(NOW, Duration::from_millis(200));
    let _ = core.tick(past_deadline, 7);
    assert!(
        core.is_leader(),
        "aborting a transfer must not itself demote this node"
    );
    assert!(
        matches!(
            core.propose(MetaCommand::NoOp),
            ProposeResult::Accepted { .. }
        ),
        "proposing must resume once the stalled transfer aborts"
    );

    // The stale target no longer receives TimeoutNow.
    let hb = after(past_deadline, Duration::from_millis(60));
    let outs = core.tick(hb, 7);
    assert!(
        outs.iter()
            .all(|(_, m)| !matches!(m, RaftMsg::TimeoutNow { .. })),
        "an aborted transfer must not keep sending TimeoutNow: {outs:?}"
    );
}

/// Re-arming the same already-armed target (as a caller retrying every tick,
/// e.g. `RaftKvNode::reconfigure_step`, would do) must not push the deadline
/// out indefinitely — only a fresh arm starts a new deadline.
#[test]
fn re_arming_the_same_target_does_not_extend_the_deadline() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1));
    assert!(core.transfer_leadership(nid(1), NOW));

    // Re-arm to the same target repeatedly, as if a caller polled every tick,
    // right up to (but not past) the original deadline.
    let mut t = NOW;
    for _ in 0..4 {
        t = after(t, Duration::from_millis(30));
        assert!(core.transfer_leadership(nid(1), t), "idempotent re-arm");
    }
    assert!(t.0 < NOW.0 + Duration::from_millis(150).as_nanos() as u64);

    // Past the *original* deadline (one election timeout from the first arm at
    // NOW), the transfer must still abort despite the repeated re-arms.
    let past_original_deadline = after(NOW, Duration::from_millis(200));
    let _ = core.tick(past_original_deadline, 7);
    assert!(
        matches!(
            core.propose(MetaCommand::NoOp),
            ProposeResult::Accepted { .. }
        ),
        "repeated re-arming of the same target must not starve the abort check"
    );
}

#[test]
fn transfer_leadership_does_not_survive_a_fresh_election_win() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1));
    assert!(core.transfer_leadership(nid(1), NOW));

    // Deposed by a higher-term leader (node 2). The generic higher-term
    // step-down at the top of `handle` flips role/term before the message is
    // even dispatched to its specific handler.
    let depose_at = after(NOW, Duration::from_secs(1));
    core.handle(nid(2), heartbeat(nid(2), core.term() + 1), depose_at, 7);
    assert_eq!(core.role(), Role::Follower);

    // Re-win a later election the normal way (pre-vote, then the real vote).
    let t = after(depose_at, Duration::from_secs(1));
    let _ = core.tick(t, 7); // election timeout -> pre-candidate
    let _ = core.handle(
        nid(1),
        RaftMsg::PreVoteResp {
            term: core.term() + 1,
            granted: true,
        },
        t,
        7,
    );
    let _ = core.handle(
        nid(1),
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
    let mut core: RaftCore = RaftCore::new(nid(1), &group(), Nanos(0), 7);
    // A live leader's heartbeat gives this follower a "lease" that would make a
    // normal election-timeout tick only start a *pre-vote* round (see
    // pre_vote.rs), never campaign directly.
    core.handle(nid(0), heartbeat(nid(0), 5), NOW, 7);
    assert_eq!(core.term(), 5);
    assert_eq!(core.role(), Role::Follower);

    let after_hb = after(NOW, Duration::from_millis(1));
    let outs = core.handle(nid(0), RaftMsg::TimeoutNow { term: 5 }, after_hb, 7);

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
    let mut stale: RaftCore = RaftCore::new(nid(1), &group(), Nanos(0), 7);
    stale.handle(nid(0), heartbeat(nid(0), 5), NOW, 7);
    let outs = stale.handle(
        nid(0),
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
    let outs2 = leader.handle(nid(1), RaftMsg::TimeoutNow { term: leader_term }, NOW, 7);
    assert!(outs2.is_empty());
    assert!(leader.is_leader());

    // Not a voter: this node's id was never part of the configuration.
    let not_voter_group: [NodeId; 2] = [nid(0), nid(2)];
    let mut not_voter: RaftCore = RaftCore::new(nid(1), &not_voter_group, Nanos(0), 7);
    let outs3 = not_voter.handle(nid(0), RaftMsg::TimeoutNow { term: 0 }, NOW, 7);
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
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));

    let result = core.change_membership(set(&[nid(0), nid(1)]));
    assert!(
        matches!(result, ProposeResult::Accepted { .. }),
        "{result:?}"
    );
    let removal_index = core.last_log_index();

    // Node 2 is out of the active configuration immediately.
    assert!(!core.config().contains(&nid(2)));

    // But the next heartbeat must still target it, carrying the entry that
    // removed it.
    let hb1 = after(NOW, Duration::from_millis(60));
    let outs1 = core.tick(hb1, 7);
    let to_two: Vec<_> = outs1.iter().filter(|(to, _)| *to == nid(2)).collect();
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
        nid(2),
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
        outs2.iter().all(|(to, _)| *to != nid(2)),
        "a caught-up departed peer must not be re-targeted: {outs2:?}"
    );
}

/// A peer re-added to the configuration before it fully caught up on its
/// removal is no longer "departing" — it is just an ordinary voter again, kept
/// current the normal way.
#[test]
fn a_peer_re_added_before_catching_up_on_removal_is_no_longer_departing() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));

    assert!(matches!(
        core.change_membership(set(&[nid(0), nid(1)])),
        ProposeResult::Accepted { .. }
    ));
    // Commit the removal so a second single-server change is accepted.
    core.handle(
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
        core.change_membership(set(&[nid(0), nid(1), nid(2)])),
        ProposeResult::Accepted { .. }
    ));
    assert!(core.config().contains(&nid(2)));

    // Node 2 is back in `peers`, so it is no longer separately tracked as
    // departing (no duplicate targeting).
    let hb = after(NOW, Duration::from_millis(60));
    let outs = core.tick(hb, 7);
    let to_two = outs.iter().filter(|(to, _)| *to == nid(2)).count();
    assert_eq!(
        to_two, 1,
        "re-added peer should be targeted once, as an ordinary peer: {outs:?}"
    );
}
