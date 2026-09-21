//! Regression for issue #1019's `vote_lease_lapsed` mechanism
//! (`crates/animus-control/src/raft.rs`'s `start_pre_vote`/`handle_pre_vote`):
//! a non-voter's own election-timer expiry lapses its **pre-vote** lease
//! (liveness) without ever touching `voted_for` itself (safety) — see the
//! `vote_lease_lapsed` field's own doc for the full mechanism and the
//! two-leaders-in-one-term hazard a naive `voted_for = None` would open.
//!
//! Bare `RaftCore<MetaCommand, Metadata>` unit cell, no driver/Env/sim at
//! all — modeled on `tests/driver_applied_sm.rs`'s direct `RaftCore::new`
//! usage and `tests/learner_membership.rs`'s bare-`core` harness idiom.

use animus_control::{MetaCommand, Metadata, RaftCore, RaftMsg};
use animus_env::{Nanos, nid};

type Core = RaftCore<MetaCommand, Metadata>;

/// Election timeouts are `election_base` (150ms) + `entropy % election_base`
/// (see `reset_election_timer`'s doc), so at most ~300ms from whenever it was
/// last reset. Advancing `now` by comfortably more than that per `tick` call
/// guarantees the timer has genuinely expired each time.
const PAST_ANY_ELECTION_TIMEOUT: u64 = 400_000_000; // 400ms in Nanos

#[test]
fn a_non_voters_pre_vote_lease_lapses_on_timer_expiry_but_its_real_vote_never_double_grants() {
    // X (n3) is deliberately NOT in the voter set it's constructed with —
    // the same "starts as a non-voter in its own view" shape
    // `learner_corpus.rs`/`learner_membership.rs` use for a learner: is_voter()
    // is false for X for the whole test, so it can never itself campaign,
    // exactly the precondition this mechanism exists for.
    let mut x: Core = RaftCore::new(nid(3), &[nid(0), nid(1), nid(2)], Nanos(0), 0);

    let mut now = Nanos(10_000_000); // 10ms

    // (a) A real vote request from n0 at term 1 is granted (X starts with a
    // clean slate: no prior vote, no live-leader belief, an equally-fresh
    // empty log satisfies log-up-to-dateness).
    let resp = x.handle(
        nid(0),
        RaftMsg::RequestVote {
            term: 1,
            candidate: nid(0),
            last_log_index: 0,
            last_log_term: 0,
        },
        now,
        0,
    );
    assert_eq!(resp.len(), 1, "a RequestVote always gets exactly one reply");
    assert!(
        matches!(
            &resp[0],
            (to, RaftMsg::RequestVoteResp { granted: true, .. }) if *to == nid(0)
        ),
        "seed-free: X must grant n0's term-1 request (clean slate) — got {:?}",
        resp[0]
    );

    // (e) Immediately afterward (lease freshly armed, no timer expiry yet), a
    // competing PRE-vote for a later term is rejected — the lease itself
    // still works right after a grant.
    let resp = x.handle(
        nid(1),
        RaftMsg::PreVote {
            term: 2,
            candidate: nid(1),
            last_log_index: 0,
            last_log_term: 0,
        },
        now,
        0,
    );
    assert!(
        matches!(
            &resp[0],
            (to, RaftMsg::PreVoteResp { granted: false, .. }) if *to == nid(1)
        ),
        "the vote lease must still protect n0's just-granted vote before any \
         timer expiry — got {:?}",
        resp[0]
    );

    // (b) Advance past several election timeouts, calling `tick` each time —
    // this is the non-voter timer-expiry path (`start_pre_vote`'s
    // `!is_voter()` early return): X can never itself become a
    // `PreCandidate`, but each expiry is exactly the "haven't heard from
    // anyone in a full election timeout" signal that lapses its lease.
    for i in 0..5u64 {
        now = Nanos(now.0 + PAST_ANY_ELECTION_TIMEOUT);
        let out = x.tick(now, i);
        assert!(
            out.is_empty(),
            "a non-voter's timer-expiry tick must produce no outbound \
             messages (it can never campaign) — got {out:?} on iteration {i}"
        );
    }

    // (c) THE SAFETY HALF: a real vote request from a *different* candidate,
    // still in term 1 (X's own current_term never moved — nothing in this
    // test has sent X a higher-term message), must still be rejected.
    // `voted_for` is never cleared by timer expiry alone — only the PRE-vote
    // lease lapses, never the real, one-vote-per-term commitment.
    let resp = x.handle(
        nid(1),
        RaftMsg::RequestVote {
            term: 1,
            candidate: nid(1),
            last_log_index: 0,
            last_log_term: 0,
        },
        now,
        0,
    );
    assert!(
        matches!(
            &resp[0],
            (to, RaftMsg::RequestVoteResp { granted: false, .. }) if *to == nid(1)
        ),
        "X already voted for n0 in term 1 — n1's competing real-vote request \
         in the SAME term must be rejected even after the pre-vote lease has \
         lapsed, or two candidates could each reach a majority in one term \
         (issue #1019's safety hazard) — got {:?}",
        resp[0]
    );

    // (d) THE LIVENESS HALF: a PRE-vote for a fresh, higher term is now
    // granted (the lease has lapsed, and X's `leader_id` belief was cleared
    // by the very same timer expiry) — this is what actually unblocks a
    // real election once the group X belongs to would otherwise be
    // permanently deadlocked on X's own stale vote lease.
    let resp = x.handle(
        nid(1),
        RaftMsg::PreVote {
            term: 2,
            candidate: nid(1),
            last_log_index: 0,
            last_log_term: 0,
        },
        now,
        0,
    );
    assert!(
        matches!(
            &resp[0],
            (to, RaftMsg::PreVoteResp { granted: true, .. }) if *to == nid(1)
        ),
        "the lapsed lease must let X grant a fresh pre-vote at a higher term \
         — got {:?}",
        resp[0]
    );

    // And the resulting REAL election at that higher term succeeds too —
    // X's own generic higher-term step-down clears `voted_for` the moment a
    // genuinely higher-term message (not a pre-vote) arrives, so the real
    // vote for n1's term-2 candidacy is granted cleanly.
    let resp = x.handle(
        nid(1),
        RaftMsg::RequestVote {
            term: 2,
            candidate: nid(1),
            last_log_index: 0,
            last_log_term: 0,
        },
        now,
        0,
    );
    assert!(
        matches!(
            &resp[0],
            (to, RaftMsg::RequestVoteResp { granted: true, .. }) if *to == nid(1)
        ),
        "a genuinely higher term must let X grant n1's real vote — got {:?}",
        resp[0]
    );
    assert_eq!(x.term(), 2, "X must have adopted the higher real term");
}
