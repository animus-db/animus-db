//! ADR 0044 phase-1 PR3: the core quiesce state machine. Hand-driven
//! `RaftCore`s only (no driver, no `Simulator`) — mirrors
//! `leadership_transfer.rs`'s style. The full `SimEnv`/`RaftKvNode`
//! end-to-end corpus (an idle 3-node group actually reaching quiescence,
//! `AppendEntriesSent` staying flat, un-quiescing on a write, a linearizable
//! read served without un-quiescing, and recovery after killing a quiesced
//! leader) lives in `crates/animus-cp-data/tests/quiescence.rs` — this file
//! covers the pure predicate/message-handling mechanics the control plane
//! itself never exercises in production (fork G: nothing here calls
//! `enable_quiescence` outside a test).

use std::time::Duration;

use animus_control::raft::LogEntry;
use animus_control::{MetaCommand, ProposeResult, RaftCore, RaftMsg};
use animus_env::{Nanos, NodeId, nid};

fn group() -> [NodeId; 3] {
    [nid(0), nid(1), nid(2)]
}
const NOW: Nanos = Nanos(1_000_000_000);

fn after(base: Nanos, d: Duration) -> Nanos {
    Nanos(base.0 + d.as_nanos() as u64)
}

/// Elect `group()[0]` leader of the 3-node group (identical idiom to
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

/// Feed the leader a success ack from `from` matching its whole log,
/// advancing commit and fully catching that peer up (`match_index ==
/// last_log_index`) — the same fabricated-ack idiom `leadership_transfer.rs`
/// uses to control a leader's replication state directly, with no real
/// follower `RaftCore` needed.
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

/// Tick the leader one heartbeat interval (60ms — comfortably past the 50ms
/// default) past `from`, the point at which `quiesce_entry_ok` is evaluated.
fn tick_past_heartbeat(core: &mut RaftCore, from: Nanos) -> Vec<(NodeId, RaftMsg)> {
    core.tick(after(from, Duration::from_millis(60)), 7)
}

/// A fresh follower that has directly received (and accepted) leader `0`'s
/// single no-op entry at `term`, with `commit_index` — the minimal state
/// needed to validate a subsequent [`RaftMsg::Quiesce`] against it.
fn caught_up_follower(term: u64, commit_index: u64) -> RaftCore {
    let mut follower: RaftCore = RaftCore::new(nid(1), &group(), Nanos(0), 7);
    follower.handle(
        nid(0),
        RaftMsg::AppendEntries {
            term,
            leader: nid(0),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term,
                index: 1,
                command: MetaCommand::NoOp,
                config: None,
                learners: None,
            }],
            leader_commit: commit_index,
        },
        NOW,
        7,
    );
    follower
}

// ---- entry predicate: the positive case + each clause rejecting it --------

#[test]
fn quiesce_entry_succeeds_when_every_clause_holds() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));
    core.enable_quiescence(Duration::from_millis(1));

    let outs = tick_past_heartbeat(&mut core, NOW);
    assert!(
        core.is_quiesced(),
        "every clause holds; the leader must have quiesced"
    );
    assert_eq!(outs.len(), 2, "one Quiesce per peer: {outs:?}");
    assert!(
        outs.iter()
            .all(|(_, m)| matches!(m, RaftMsg::Quiesce { .. })),
        "must broadcast Quiesce, not an ordinary heartbeat: {outs:?}"
    );
    assert!(
        core.next_deadline().is_none(),
        "a quiesced leader must want no timer at all"
    );
}

#[test]
fn quiescence_never_triggers_without_opting_in() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));
    // No `enable_quiescence` call: `quiesce_after` stays `None`.

    let outs = tick_past_heartbeat(&mut core, NOW);
    assert!(!core.is_quiesced());
    assert!(
        outs.iter()
            .all(|(_, m)| matches!(m, RaftMsg::AppendEntries { .. })),
        "must keep sending ordinary heartbeats: {outs:?}"
    );
}

#[test]
fn quiesce_entry_blocked_by_a_lagging_voter() {
    let mut core = elect_leader();
    // Majority (self + node 1) advances commit to the no-op's index — but
    // node 2 never acks, so its match_index stays 0 while last_log_index is 1.
    ack_all(&mut core, nid(1));
    assert_eq!(core.commit_index(), core.last_log_index());
    assert_eq!(core.peer_match(&nid(2)), 0, "node 2 never acked");
    core.enable_quiescence(Duration::from_millis(1));

    let outs = tick_past_heartbeat(&mut core, NOW);
    assert!(
        !core.is_quiesced(),
        "a lagging voter must block quiescence entry"
    );
    assert!(
        outs.iter()
            .any(|(to, m)| *to == nid(2) && matches!(m, RaftMsg::AppendEntries { .. })),
        "must keep replicating to the lagging voter: {outs:?}"
    );
}

#[test]
fn quiesce_entry_blocked_by_an_armed_transfer() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));
    assert!(core.transfer_leadership(nid(1), NOW));
    core.enable_quiescence(Duration::from_millis(1));

    let outs = tick_past_heartbeat(&mut core, NOW);
    assert!(
        !core.is_quiesced(),
        "an armed leadership transfer must block quiescence entry"
    );
    assert!(!outs.is_empty(), "a frozen leader must still replicate");
}

#[test]
fn quiesce_entry_blocked_by_uncommitted_entries() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));
    // Grow the log past what anyone has acked: commit_index < last_log_index.
    assert!(matches!(
        core.propose(MetaCommand::NoOp),
        ProposeResult::Accepted { .. }
    ));
    assert!(core.commit_index() < core.last_log_index());
    core.enable_quiescence(Duration::from_millis(1));

    let outs = tick_past_heartbeat(&mut core, NOW);
    assert!(
        !core.is_quiesced(),
        "an uncommitted tail must block quiescence entry"
    );
    assert!(!outs.is_empty());
}

/// A freshly elected leader that nobody has acked yet fails two clauses at
/// once by construction: `match_index == last_log_index` for every voter
/// (nobody has acked anything) and `commit_index >= first_term_index`
/// (nothing is committed) — in a fresh group these are coupled (a majority of
/// voters reaching `match_index == last_log_index` is exactly what
/// `maybe_advance_commit` needs to also advance `commit_index` that far, so
/// the "all voters caught up" clause can't hold while "commit lags
/// first_term_index" does). This still exercises both clauses' rejection.
#[test]
fn quiesce_entry_blocked_before_the_no_op_itself_is_committed() {
    let mut core = elect_leader();
    // No `ack_all` calls at all: commit_index is still 0.
    assert_eq!(core.commit_index(), 0);
    assert_eq!(
        core.first_term_index(),
        Some(1),
        "the election no-op is this leader's first-term entry"
    );
    core.enable_quiescence(Duration::from_millis(1));

    let outs = tick_past_heartbeat(&mut core, NOW);
    assert!(!core.is_quiesced());
    assert!(!outs.is_empty());
}

#[test]
fn quiesce_entry_blocked_by_a_held_veto() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));
    core.set_quiesce_veto(true, u64::MAX);
    core.enable_quiescence(Duration::from_millis(1));

    let outs = tick_past_heartbeat(&mut core, NOW);
    assert!(
        !core.is_quiesced(),
        "a held veto (fork D) must block quiescence entry"
    );
    assert!(!outs.is_empty());
}

/// Issue #302 regression: the exact shape of the stale-veto race. An
/// external veto holder (in production, `animusd`'s `change_consumer_loop`)
/// observes this tablet's own obligation state and reports "nothing owed" —
/// but as of an index a write has since moved past. A bare boolean can't
/// tell that observation apart from a genuinely fresh one; this pins the
/// freshness clause that does. Fails (wrongly quiesces) against the
/// pre-fix `quiesce_entry_ok`, which had no freshness clause at all — see
/// this crate's own delivery notes for the stash/restore proof.
#[test]
fn quiesce_entry_blocked_by_a_stale_veto_freshness() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));
    // The external sweeper's own "as of" index right now — the election
    // no-op alone, nothing else committed yet.
    let stale_fresh_through = core.commit_index();

    // A write lands and commits — the very obligation a stale veto would
    // miss (a change-log record, in production; here just any command).
    let ProposeResult::Accepted { .. } = core.propose(MetaCommand::NoOp) else {
        panic!("propose should be accepted");
    };
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));
    assert!(
        core.commit_index() > stale_fresh_through,
        "the new write must have advanced commit past the sweep's own index"
    );

    // The external veto reports "nothing owed", but as of the STALE index —
    // exactly what a sweep that ran before this write committed would
    // report, with no further sweep before quiescence would otherwise fire.
    core.set_quiesce_veto(false, stale_fresh_through);
    core.enable_quiescence(Duration::from_millis(1));

    let outs = tick_past_heartbeat(&mut core, NOW);
    assert!(
        !core.is_quiesced(),
        "a veto observation that predates a since-committed write must \
         block quiescence entry, even though the veto's own boolean reads \
         false"
    );
    assert!(!outs.is_empty());
}

/// The dual of the test above: once a sweep genuinely re-observes this
/// tablet at (or after) the latest committed index, the group quiesces
/// normally. The freshness clause closes a real staleness window — it does
/// not turn the veto into a second permanent block.
#[test]
fn quiesce_entry_succeeds_once_the_veto_freshness_catches_up() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));
    let ProposeResult::Accepted { .. } = core.propose(MetaCommand::NoOp) else {
        panic!("propose should be accepted");
    };
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));

    // A fresh sweep, run AFTER the write committed: reports "nothing owed"
    // as of the current (post-write) commit index.
    core.set_quiesce_veto(false, core.commit_index());
    core.enable_quiescence(Duration::from_millis(1));

    let outs = tick_past_heartbeat(&mut core, NOW);
    assert!(
        core.is_quiesced(),
        "a veto observation as fresh as the current commit index must not \
         block quiescence"
    );
    assert!(!outs.is_empty());
}

#[test]
fn quiesce_entry_blocked_when_the_engine_has_not_caught_up() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));
    core.set_quiesce_engine_caught_up(false);
    core.enable_quiescence(Duration::from_millis(1));

    let outs = tick_past_heartbeat(&mut core, NOW);
    assert!(
        !core.is_quiesced(),
        "the async apply task lagging the engine must block quiescence entry"
    );
    assert!(!outs.is_empty());
}

#[test]
fn quiesce_entry_blocked_until_the_settle_window_elapses() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));
    // A settle window far longer than one heartbeat interval: even though
    // every other clause holds, "no activity for quiesce_after" must not be
    // satisfied by the very first heartbeat tick.
    core.enable_quiescence(Duration::from_secs(10));

    let outs = tick_past_heartbeat(&mut core, NOW);
    assert!(
        !core.is_quiesced(),
        "must not quiesce before the configured settle window elapses"
    );
    assert!(!outs.is_empty());
}

// ---- follower-side Quiesce acceptance --------------------------------------

#[test]
fn follower_accepts_quiesce_when_fully_matching() {
    let mut follower = caught_up_follower(3, 1);
    assert_eq!(follower.last_log_index(), 1);
    assert_eq!(follower.commit_index(), 1);

    follower.handle(
        nid(0),
        RaftMsg::Quiesce {
            term: 3,
            commit_index: 1,
        },
        after(NOW, Duration::from_millis(1)),
        7,
    );
    assert!(follower.is_quiesced());
    assert!(follower.next_deadline().is_none());
}

#[test]
fn follower_ignores_quiesce_at_a_lower_term() {
    let mut follower = caught_up_follower(3, 1);
    follower.handle(
        nid(0),
        RaftMsg::Quiesce {
            term: 2,
            commit_index: 1,
        },
        after(NOW, Duration::from_millis(1)),
        7,
    );
    assert!(!follower.is_quiesced());
    assert!(follower.next_deadline().is_some());
    assert_eq!(
        follower.term(),
        3,
        "a stale-term Quiesce must not perturb term"
    );
}

#[test]
fn follower_ignores_quiesce_from_a_non_leader_sender() {
    let mut follower = caught_up_follower(3, 1);
    follower.handle(
        nid(2),
        RaftMsg::Quiesce {
            term: 3,
            commit_index: 1,
        },
        after(NOW, Duration::from_millis(1)),
        7,
    );
    assert!(
        !follower.is_quiesced(),
        "a Quiesce from anyone but the recorded leader must be ignored"
    );
}

#[test]
fn follower_ignores_quiesce_when_its_own_log_is_behind_the_claimed_commit() {
    let mut follower = caught_up_follower(3, 1);
    follower.handle(
        nid(0),
        RaftMsg::Quiesce {
            term: 3,
            commit_index: 5, // the follower has only seen up to index 1
        },
        after(NOW, Duration::from_millis(1)),
        7,
    );
    assert!(
        !follower.is_quiesced(),
        "a follower not provably caught up to the claimed commit must ignore Quiesce"
    );
}

// ---- WakeRequest (fork B) ---------------------------------------------------

#[test]
fn leader_answers_wake_request_with_an_ordinary_append_regardless_of_its_own_quiesced_state() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));
    core.enable_quiescence(Duration::from_millis(1));
    let _ = tick_past_heartbeat(&mut core, NOW);
    assert!(core.is_quiesced(), "precondition: the leader quiesced");

    let term = core.term();
    let outs = core.handle(
        nid(1),
        RaftMsg::WakeRequest { term },
        after(NOW, Duration::from_secs(1)),
        7,
    );
    assert!(
        !core.is_quiesced(),
        "any inbound message un-quiesces, WakeRequest included"
    );
    assert!(
        outs.iter()
            .any(|(to, m)| *to == nid(1) && matches!(m, RaftMsg::AppendEntries { .. })),
        "must answer with an ordinary replication message: {outs:?}"
    );
}

#[test]
fn wake_request_is_ignored_by_a_non_leader() {
    let mut follower = caught_up_follower(3, 1);
    let outs = follower.handle(
        nid(2),
        RaftMsg::WakeRequest { term: 3 },
        after(NOW, Duration::from_millis(1)),
        7,
    );
    assert!(outs.is_empty(), "a non-leader must not answer WakeRequest");
}

// ---- on_local_wake (fork B) --------------------------------------------------

#[test]
fn on_local_wake_is_inert_on_a_leader() {
    let mut core = elect_leader();
    let outs = core.on_local_wake(NOW, 7);
    assert!(outs.is_empty());
    assert!(!core.is_quiesced());
}

#[test]
fn on_local_wake_re_arms_and_asks_the_known_leader_when_quiesced() {
    let mut follower = caught_up_follower(3, 1);
    follower.handle(
        nid(0),
        RaftMsg::Quiesce {
            term: 3,
            commit_index: 1,
        },
        after(NOW, Duration::from_millis(1)),
        7,
    );
    assert!(follower.is_quiesced());
    assert!(follower.next_deadline().is_none());

    let woke_at = after(NOW, Duration::from_secs(10));
    let term_before = follower.term();
    let outs = follower.on_local_wake(woke_at, 9);
    assert!(!follower.is_quiesced());
    assert!(
        matches!(outs.as_slice(), [(to, RaftMsg::WakeRequest { term })] if *to == nid(0) && *term == term_before),
        "must send exactly one WakeRequest to the recorded leader: {outs:?}"
    );
    let rearmed = follower
        .next_deadline()
        .expect("on_local_wake must re-arm a real deadline");
    assert!(
        rearmed.0 > woke_at.0,
        "the re-armed election timeout must be a fresh interval ahead of now, \
         not a stale deadline: rearmed={rearmed:?} woke_at={woke_at:?}"
    );
}

// ---- hazard 2: the pre-vote lease check under a stale, quiesced deadline ---

/// The regression for finding 4's hazard 2: leaving `election_deadline`
/// stale while quiesced (fork C) is only safe if `handle_pre_vote`'s
/// existing lease check keeps doing the right thing with no edit — granting
/// a pre-vote to a genuinely new candidate once the recorded leader is truly
/// gone, exactly as it already does for any follower whose election timeout
/// has organically expired. This proves it does, without touching
/// `handle_pre_vote` at all.
#[test]
fn a_stale_quiesced_followers_pre_vote_lease_still_grants_to_a_genuinely_new_candidate() {
    let mut follower = caught_up_follower(3, 1);
    follower.handle(
        nid(0),
        RaftMsg::Quiesce {
            term: 3,
            commit_index: 1,
        },
        after(NOW, Duration::from_millis(1)),
        7,
    );
    assert!(follower.is_quiesced());
    assert!(
        follower.next_deadline().is_none(),
        "precondition: genuinely parked, no timer"
    );

    // Time passes well beyond any ordinary election timeout, with no
    // heartbeats at all (quiescence removed that timer) — `election_deadline`
    // is now stale, deep in the past relative to `later`.
    let later = after(NOW, Duration::from_secs(10));

    // A genuinely different candidate (node 2) starts a pre-vote round at a
    // fresh prospective term, with a log at least as up to date.
    let outs = follower.handle(
        nid(2),
        RaftMsg::PreVote {
            term: follower.term() + 1,
            candidate: nid(2),
            last_log_index: follower.last_log_index(),
            last_log_term: follower.term(),
        },
        later,
        7,
    );
    assert!(
        !follower.is_quiesced(),
        "any inbound message un-quiesces, PreVote included"
    );
    assert!(
        matches!(
            outs.as_slice(),
            [(_, RaftMsg::PreVoteResp { granted: true, .. })]
        ),
        "a stale-deadline quiesced follower must still grant a pre-vote to a \
         genuinely new candidate once its recorded leader is (as far as it \
         can tell) unreachable — the mechanism a genuinely dead quiesced \
         leader's group depends on to recover: {outs:?}"
    );
}

/// The safety counterpart: while genuinely still quiesced and *not* woken,
/// this follower must not itself spontaneously start an election — `tick`
/// is simply never called on it in production while `next_deadline` is
/// `None` (no timer arm exists to invoke it), which this pins directly:
/// calling `tick` by hand well past the stale deadline still only starts a
/// pre-vote round if the caller explicitly does so — proving there is
/// nothing *inside* `tick` that would derail this if it were mistakenly
/// invoked, only the driver-level absence of a timer arm doing the real
/// work of never calling it.
#[test]
fn a_quiesced_followers_own_tick_would_start_a_pre_vote_if_ever_called_late() {
    let mut follower = caught_up_follower(3, 1);
    follower.handle(
        nid(0),
        RaftMsg::Quiesce {
            term: 3,
            commit_index: 1,
        },
        after(NOW, Duration::from_millis(1)),
        7,
    );
    assert!(follower.is_quiesced());
    let later = after(NOW, Duration::from_secs(10));
    // `tick` itself doesn't know about `quiesced` at all for a follower (only
    // the leader-side branch consults it) — it just applies the ordinary
    // election-timeout rule. This is exactly why the *driver* must never call
    // `tick` while `next_deadline` is `None`: this call site is not itself
    // guarded.
    let outs = follower.tick(later, 7);
    assert!(
        outs.iter()
            .any(|(_, m)| matches!(m, RaftMsg::PreVote { .. })),
        "pins that `tick` has no internal quiesced-awareness for a follower \
         role — the driver's own timer-arm absence is the only thing \
         preventing this from firing spuriously: {outs:?}"
    );
}
