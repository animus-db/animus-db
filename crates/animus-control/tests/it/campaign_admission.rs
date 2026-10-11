//! ADR 0078 step 2: the `RaftCore` campaign-admission and lag-stretch seam
//! (`campaign_due`, `tick_gated`, `campaign_hold` in `next_deadline`,
//! `campaign_active`, `set_lag_stretch`).
//!
//! Hand-driven `RaftCore`s only (no driver, no `Simulator`), the style of
//! `set_timing.rs`, so every tick is inspectable. The properties:
//! - `tick` is `tick_gated(.., true)`: byte-identical for every existing caller;
//! - a deferred campaign changes no role or term, leaves the election timer
//!   LAPSED (so the node still grants pre-votes to admitted campaigners) and
//!   wakes the driver through `next_deadline`, not by spinning;
//! - the permit check is after every early return (learner, single voter,
//!   pending cluster check never need or are refused by a permit);
//! - the stretch widens the follower's patience only, draws no entropy, and
//!   is not applied to the transfer budget, the health grace, the heartbeat
//!   interval or the vote lease.

use std::time::Duration;

use animus_control::{RaftCore, RaftMsg, Role};
use animus_env::{Nanos, NodeId, nid};

fn group() -> [NodeId; 3] {
    [nid(0), nid(1), nid(2)]
}

const MS: u64 = 1_000_000;
const BASE: u64 = 150 * MS;

fn follower() -> RaftCore {
    RaftCore::new(nid(0), &group(), Nanos(0), 0)
}

/// A comparable rendering of a tick's output.
fn render<T: std::fmt::Debug>(outs: &T) -> String {
    format!("{outs:?}")
}

fn pre_vote_from(core: &mut RaftCore, candidate: u64, now: Nanos) -> Vec<String> {
    core.handle(
        nid(candidate),
        RaftMsg::PreVote {
            term: core.term() + 1,
            candidate: nid(candidate),
            last_log_index: 0,
            last_log_term: 0,
        },
        now,
        1,
    )
    .into_iter()
    .map(|(to, m)| format!("{to:?} {m:?}"))
    .collect()
}

fn granted(outs: &[String]) -> bool {
    outs.iter()
        .any(|o| o.contains("PreVoteResp") && o.contains("granted: true"))
}

fn elect_leader() -> RaftCore {
    let now = Nanos(1_000 * MS);
    let mut core = follower();
    let _ = core.tick(now, 7);
    let _ = core.handle(
        nid(1),
        RaftMsg::PreVoteResp {
            term: core.term() + 1,
            granted: true,
        },
        now,
        7,
    );
    let _ = core.handle(
        nid(1),
        RaftMsg::RequestVoteResp {
            term: core.term(),
            granted: true,
        },
        now,
        7,
    );
    assert!(core.is_leader());
    core
}

// ---- admit = true is tick -------------------------------------------------

#[test]
fn tick_and_tick_gated_admit_true_are_identical_over_a_whole_election() {
    // Two cores, same script; one driven with `tick`, one with
    // `tick_gated(.., true)`. Outputs, role, term and deadlines must match at
    // every step, through follower -> pre-candidate -> candidate -> leader and
    // the leader's heartbeats.
    let mut a = follower();
    let mut b = follower();
    for (i, now_ms) in [
        0u64, 100, 149, 150, 200, 301, 302, 400, 500, 650, 800, 1_200,
    ]
    .into_iter()
    .enumerate()
    {
        let now = Nanos(now_ms * MS);
        let entropy = 31 * i as u64 + 5;
        assert_eq!(
            render(&a.tick(now, entropy)),
            render(&b.tick_gated(now, entropy, true)),
            "step {i} at {now_ms}ms"
        );
        assert_eq!((a.role(), a.term()), (b.role(), b.term()), "step {i}");
        assert_eq!(a.next_deadline(), b.next_deadline(), "step {i}");
        if a.role() == Role::PreCandidate {
            // Feed both the same pre-vote and vote grants so they go on to lead.
            for core in [&mut a, &mut b] {
                let _ = core.handle(
                    nid(1),
                    RaftMsg::PreVoteResp {
                        term: core.term() + 1,
                        granted: true,
                    },
                    now,
                    9,
                );
                let _ = core.handle(
                    nid(1),
                    RaftMsg::RequestVoteResp {
                        term: core.term(),
                        granted: true,
                    },
                    now,
                    9,
                );
            }
            assert_eq!(a.role(), b.role());
        }
    }
    assert!(
        a.is_leader() && b.is_leader(),
        "script must reach leadership"
    );
}

#[test]
fn a_denied_permit_on_a_not_due_tick_is_identical_to_tick() {
    // Before the election deadline there is nothing to gate.
    let mut a = follower();
    let mut b = follower();
    let now = Nanos(10 * MS);
    assert!(!a.campaign_due(now));
    assert_eq!(
        render(&a.tick(now, 3)),
        render(&b.tick_gated(now, 3, false))
    );
    assert_eq!(a.next_deadline(), b.next_deadline());
    assert_eq!((a.role(), a.term()), (b.role(), b.term()));
}

// ---- deferral --------------------------------------------------------------

#[test]
fn deferral_changes_no_role_or_term_and_does_not_reset_the_timer() {
    let mut core = follower();
    let before_deadline = core.next_deadline().unwrap();
    let due = Nanos(before_deadline.0 + 1);
    assert!(core.campaign_due(due));

    let out = core.tick_gated(due, 5, false);
    assert!(out.is_empty(), "a deferred campaign sends nothing");
    assert_eq!(core.role(), Role::Follower);
    assert_eq!(core.term(), 0);
    assert!(!core.campaign_active());
    // Still due: the election timer was NOT reset.
    assert!(core.campaign_due(due));
}

#[test]
fn deferral_wakes_the_driver_at_the_retry_instant_and_never_spins() {
    let mut core = follower();
    let deadline = core.next_deadline().unwrap();
    let due = Nanos(deadline.0 + 50 * MS);
    let _ = core.tick_gated(due, 5, false);
    // `next_deadline` must be in the future, or a driver sleeping until it
    // would spin on the lapsed `election_deadline`.
    let next = core.next_deadline().unwrap();
    assert_eq!(next, Nanos(due.0 + BASE), "retry one election_base later");
    // A second denial at the retry instant re-arms the hold again.
    let _ = core.tick_gated(next, 5, false);
    assert_eq!(core.next_deadline().unwrap(), Nanos(next.0 + BASE));
    assert_eq!(core.role(), Role::Follower);
}

#[test]
fn a_deferred_follower_still_grants_pre_votes_to_admitted_campaigners() {
    // The deadlock the ADR describes: deferral must leave the election timer
    // lapsed so this node keeps GRANTING. If it re-armed, it would refuse.
    let mut core = follower();
    let due = Nanos(core.next_deadline().unwrap().0 + 1);
    let _ = core.tick_gated(due, 5, false);
    let resp = pre_vote_from(&mut core, 1, due);
    assert!(granted(&resp), "deferred voter must grant: {resp:?}");

    // Control: a follower that DID hear a leader (timer re-armed) refuses.
    let mut hearing = follower();
    let _ = hearing.handle(
        nid(1),
        RaftMsg::AppendEntries {
            term: 1,
            leader: nid(1),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        },
        Nanos(10 * MS),
        3,
    );
    let resp = pre_vote_from(&mut hearing, 2, Nanos(20 * MS));
    assert!(
        !granted(&resp),
        "a follower with a live leader refuses: {resp:?}"
    );
}

#[test]
fn admission_after_deferral_starts_the_pre_vote_and_clears_the_hold() {
    let mut core = follower();
    let due = Nanos(core.next_deadline().unwrap().0 + 1);
    let _ = core.tick_gated(due, 5, false);
    let retry = core.next_deadline().unwrap();
    let out = core.tick_gated(retry, 5, true);
    assert_eq!(core.role(), Role::PreCandidate);
    assert!(core.campaign_active());
    assert_eq!(out.len(), 2, "PreVote to both peers");
    // The hold is gone: the deadline is the freshly drawn election deadline.
    assert!(core.next_deadline().unwrap().0 >= retry.0 + BASE);
}

#[test]
fn a_heard_leader_cancels_a_deferral() {
    let mut core = follower();
    let due = Nanos(core.next_deadline().unwrap().0 + 1);
    let _ = core.tick_gated(due, 5, false);
    let _ = core.handle(
        nid(1),
        RaftMsg::AppendEntries {
            term: 1,
            leader: nid(1),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        },
        due,
        3,
    );
    assert!(!core.campaign_due(due), "the heartbeat re-armed the timer");
    assert!(core.next_deadline().unwrap().0 >= due.0 + BASE);
}

#[test]
fn a_precandidate_whose_round_lapsed_is_also_gated() {
    // Rule (c): a failed attempt re-enters the queue; the retry needs a permit.
    let mut core = follower();
    let t1 = Nanos(core.next_deadline().unwrap().0);
    let _ = core.tick_gated(t1, 5, true);
    assert_eq!(core.role(), Role::PreCandidate);
    let lapsed = Nanos(core.next_deadline().unwrap().0 + 1);
    assert!(core.campaign_due(lapsed));
    let out = core.tick_gated(lapsed, 5, false);
    assert!(out.is_empty());
    assert_eq!(
        core.role(),
        Role::PreCandidate,
        "role unchanged by a deferral"
    );
    assert!(
        core.campaign_active(),
        "the driver releases the permit itself"
    );
    // A deferred retry is active by role but not campaigning: the driver must
    // not hand it an exempt permit (it would leave the queue and leak a slot).
    assert!(core.campaign_deferred());
    let _ = core.tick_gated(Nanos(core.next_deadline().unwrap().0), 6, true);
    assert!(!core.campaign_deferred(), "starting the round clears it");
}

#[test]
fn a_deferred_precandidate_does_not_finish_its_lapsed_round_on_a_late_grant() {
    // Found by the ProdEnv Tier 3 test under CPU contention: a PreCandidate whose
    // round lapsed is deferred (its driver returned the permit), then a grant
    // from the lapsed round arrives late. Counting it would start a real
    // election with no permit, and the driver would hand the campaign an
    // exempt permit over the cap. A deferred node is not campaigning: the
    // late grant is ignored, the retry runs a fresh round once admitted.
    let mut core = follower();
    let t1 = Nanos(core.next_deadline().unwrap().0);
    let _ = core.tick_gated(t1, 5, true);
    assert_eq!(core.role(), Role::PreCandidate);
    let term = core.term();
    let lapsed = Nanos(core.next_deadline().unwrap().0 + 1);
    let _ = core.tick_gated(lapsed, 5, false);
    assert!(core.campaign_deferred());
    let out = core.handle(
        nid(1),
        RaftMsg::PreVoteResp {
            term: term + 1,
            granted: true,
        },
        Nanos(lapsed.0 + MS),
        7,
    );
    assert!(out.is_empty(), "no RequestVote while deferred: {out:?}");
    assert_eq!(
        core.role(),
        Role::PreCandidate,
        "still waiting for a permit"
    );
    assert_eq!(core.term(), term, "no term bump while deferred");
    assert!(core.campaign_deferred(), "still queued");
    // Admitted: a fresh pre-vote round (the stale grant does not carry over).
    let _ = core.tick_gated(Nanos(core.next_deadline().unwrap().0), 8, true);
    assert_eq!(core.role(), Role::PreCandidate);
    assert_eq!(core.term(), term);
    assert!(!core.campaign_deferred());
}

// ---- the permit check sits after the early returns -------------------------

#[test]
fn a_non_voter_is_never_due_and_never_held() {
    // Not in the configuration: a learner-like node.
    let mut core: RaftCore = RaftCore::new(nid(5), &group(), Nanos(0), 0);
    let due = Nanos(core.next_deadline().unwrap().0 + 1);
    assert!(!core.campaign_due(due));
    let _ = core.tick_gated(due, 5, false);
    // Its timer was re-armed by the ordinary early return, not held.
    assert!(core.next_deadline().unwrap().0 > due.0);
    assert_eq!(core.role(), Role::Follower);
    // Same as plain tick.
    let mut other: RaftCore = RaftCore::new(nid(5), &group(), Nanos(0), 0);
    let _ = other.tick(due, 5);
    assert_eq!(core.next_deadline(), other.next_deadline());
}

#[test]
fn a_single_voter_group_is_not_gated() {
    let mut core: RaftCore = RaftCore::new(nid(0), &[nid(0)], Nanos(0), 0);
    let due = Nanos(core.next_deadline().unwrap().0 + 1);
    assert!(!core.campaign_due(due), "wins without a message: no permit");
    let _ = core.tick_gated(due, 5, false);
    assert!(
        core.is_leader(),
        "a lone voter elects itself even without a permit"
    );
}

#[test]
fn state_machine_behind_and_storage_full_are_not_due() {
    let mut behind = follower();
    behind.set_state_machine_behind(true);
    let due = Nanos(behind.next_deadline().unwrap().0 + 1);
    assert!(!behind.campaign_due(due));

    let mut full = follower();
    full.set_storage_full(true);
    let due = Nanos(full.next_deadline().unwrap().0 + 1);
    assert!(!full.campaign_due(due));
}

#[test]
fn a_leader_is_unaffected_by_the_permit() {
    let mut a = elect_leader();
    let mut b = elect_leader();
    let t = Nanos(a.next_deadline().unwrap().0 + 1);
    assert!(!a.campaign_due(t));
    assert!(!a.campaign_active());
    assert_eq!(render(&a.tick(t, 4)), render(&b.tick_gated(t, 4, false)));
    assert_eq!(a.next_deadline(), b.next_deadline());
}

// ---- lag stretch -----------------------------------------------------------

#[test]
fn set_lag_stretch_is_a_no_op_when_unchanged_and_draws_no_entropy() {
    let mut core = follower();
    assert_eq!(core.lag_stretch(), Duration::ZERO);
    let before = core.next_deadline();
    assert!(!core.set_lag_stretch(Duration::ZERO));
    assert!(core.set_lag_stretch(Duration::from_millis(80)));
    assert!(!core.set_lag_stretch(Duration::from_millis(80)));
    // It re-arms nothing (it has no `now`/entropy to do so with).
    assert_eq!(core.next_deadline(), before);
    assert_eq!(core.lag_stretch(), Duration::from_millis(80));
}

#[test]
fn the_stretch_widens_the_drawn_deadline_within_its_bounds() {
    let now = Nanos(1_000 * MS);
    let s = 80 * MS;
    for entropy in [0u64, 1, 12_345, u64::MAX / 2, u64::MAX] {
        // Reset via a heard leader so the draw goes through reset_election_timer.
        let mut core = follower();
        core.set_lag_stretch(Duration::from_nanos(s));
        let _ = core.handle(
            nid(1),
            RaftMsg::AppendEntries {
                term: 1,
                leader: nid(1),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            },
            now,
            entropy,
        );
        let d = core.next_deadline().unwrap().0 - now.0;
        assert!(
            (BASE + s..2 * (BASE + s)).contains(&d),
            "entropy {entropy}: offset {d} outside [{}, {})",
            BASE + s,
            2 * (BASE + s)
        );
    }
    // entropy 0 is exactly now + base + stretch.
    let mut core = follower();
    core.set_lag_stretch(Duration::from_nanos(s));
    let _ = core.handle(
        nid(1),
        RaftMsg::AppendEntries {
            term: 1,
            leader: nid(1),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        },
        now,
        0,
    );
    assert_eq!(core.next_deadline(), Some(Nanos(now.0 + BASE + s)));
}

#[test]
fn a_zero_stretch_draws_exactly_the_unstretched_deadline() {
    // Same entropy, stretch never set vs set-then-cleared: identical.
    let now = Nanos(500 * MS);
    let hb = |core: &mut RaftCore| {
        let _ = core.handle(
            nid(1),
            RaftMsg::AppendEntries {
                term: 1,
                leader: nid(1),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            },
            now,
            987_654_321,
        );
    };
    let mut plain = follower();
    hb(&mut plain);
    let mut cleared = follower();
    cleared.set_lag_stretch(Duration::from_millis(200));
    cleared.set_lag_stretch(Duration::ZERO);
    hb(&mut cleared);
    assert_eq!(plain.next_deadline(), cleared.next_deadline());
}

#[test]
fn the_stretch_is_not_applied_to_the_configured_base_or_the_heartbeat() {
    let mut core = elect_leader();
    core.set_lag_stretch(Duration::from_millis(400));
    // The configured base (what the health grace, `transfer_leadership` and
    // the vote lease read) is untouched, and so is the heartbeat interval.
    assert_eq!(core.election_timeout(), Duration::from_millis(150));
    assert_eq!(
        core.timing(),
        (Duration::from_millis(150), Duration::from_millis(50))
    );
    let now = Nanos(core.next_deadline().unwrap().0);
    let _ = core.tick(now, 3);
    assert_eq!(
        core.next_deadline(),
        Some(Nanos(now.0 + 50 * MS)),
        "a leader's heartbeat is not slowed"
    );
}

#[test]
fn the_stretch_is_not_applied_to_the_transfer_budget() {
    let mut core = elect_leader();
    core.set_lag_stretch(Duration::from_millis(400));
    let t0 = Nanos(2_000 * MS);
    assert!(core.transfer_leadership(nid(1), t0));
    assert_eq!(core.transfer_target(), Some(nid(1)));
    // Aborts one UNSTRETCHED election timeout after arming.
    let _ = core.tick(Nanos(t0.0 + BASE + MS), 3);
    assert_eq!(core.transfer_target(), None);
}

#[test]
fn the_stretch_is_not_applied_to_the_vote_lease() {
    // A node that granted a real vote protects it for `election_base`, not
    // for `election_base + stretch`: the lease reads the election deadline
    // drawn at grant time, which IS stretched, but only by the draw. What the
    // lease must never do is read `lag_stretch` itself, so clearing the
    // stretch after the grant changes nothing about an already-armed lease.
    let mut a = follower();
    a.set_lag_stretch(Duration::from_millis(300));
    let now = Nanos(1_000 * MS);
    let _ = a.handle(
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
    let armed = a.next_deadline().unwrap();
    a.set_lag_stretch(Duration::ZERO);
    assert_eq!(
        a.next_deadline().unwrap(),
        armed,
        "clearing the stretch re-arms nothing"
    );
}
