//! Issue #1330 (`Gate::FollowerAppliedIndex`): `RaftCore::quorum_applied`, the
//! quorum-th highest engine-applied index among the voters, fed by the
//! `applied_index` on followers' `AppendEntriesResp`s.
//!
//! A single leader core is hand-driven (no follower cores, no `Simulator`): the
//! property is a pure function of the reports the leader has received, so a
//! hand-built `AppendEntriesResp` is the direct input.

use std::time::Duration;

use animus_control::{RaftCore, RaftMsg};
use animus_env::{Nanos, nid};

const WITHIN: Duration = Duration::from_secs(1);

/// Elect `nid(0)` leader of `[0, 1, 2]`.
fn leader_of_three() -> RaftCore {
    let ids = [nid(0), nid(1), nid(2)];
    let mut leader: RaftCore = RaftCore::new(nid(0), &ids, Nanos(0), 7);
    let now = Nanos(60_000_000_000);
    let _ = leader.tick(now, 7);
    for peer in [nid(1), nid(2)] {
        let _ = leader.handle(
            peer.clone(),
            RaftMsg::PreVoteResp {
                term: leader.term() + 1,
                granted: true,
            },
            now,
            7,
        );
        let _ = leader.handle(
            peer,
            RaftMsg::RequestVoteResp {
                term: leader.term(),
                granted: true,
            },
            now,
            7,
        );
    }
    assert!(leader.is_leader());
    leader
}

fn ack(term: u64, applied_index: u64) -> RaftMsg {
    RaftMsg::AppendEntriesResp {
        term,
        success: true,
        match_index: 0,
        needs_snapshot: false,
        check_pending: false,
        applied_index,
    }
}

fn report(l: &mut RaftCore, from: u64, applied: u64, now: Nanos) {
    let term = l.term();
    let _ = l.handle(nid(from), ack(term, applied), now, 7);
}

#[test]
fn quorum_applied_is_the_quorum_th_highest_and_a_lone_slow_follower_does_not_move_it() {
    let mut l = leader_of_three();
    let now = Nanos(61_000_000_000);
    // Nothing reported: unknown counts as the leader's own applied.
    assert_eq!(l.quorum_applied(100, now, WITHIN), 100);

    // One slow follower, one caught up: [100, 100, 40] -> 2nd highest = 100.
    report(&mut l, 1, 40, now);
    report(&mut l, 2, 100, now);
    assert_eq!(l.quorum_applied(100, now, WITHIN), 100);

    // Both followers lag: [100, 60, 40] -> 2nd highest = 60.
    report(&mut l, 2, 60, now);
    assert_eq!(l.quorum_applied(100, now, WITHIN), 60);

    // A follower ahead of the leader never raises it above the leader's own.
    report(&mut l, 1, 500, now);
    report(&mut l, 2, 400, now);
    assert_eq!(l.quorum_applied(100, now, WITHIN), 100);

    // The leader itself is the slow one: [30, 90, 80] -> 2nd highest = 80, capped at 30.
    assert_eq!(l.quorum_applied(30, now, WITHIN), 30);
}

#[test]
fn zero_is_unknown_never_nothing_applied() {
    let mut l = leader_of_three();
    let now = Nanos(61_000_000_000);
    report(&mut l, 1, 40, now);
    report(&mut l, 2, 40, now);
    assert_eq!(l.quorum_applied(100, now, WITHIN), 40);
    // A `0` (an older peer, a closed gate) does not erase the last real value
    // and never reads as "applied nothing".
    report(&mut l, 2, 0, now);
    assert_eq!(l.peer_applied(&nid(2)).map(|(a, _)| a), Some(40));
    let mut fresh = leader_of_three();
    report(&mut fresh, 1, 0, now);
    report(&mut fresh, 2, 0, now);
    assert_eq!(fresh.quorum_applied(100, now, WITHIN), 100);
}

#[test]
fn a_silent_voter_reads_as_caught_up() {
    let mut l = leader_of_three();
    let t0 = Nanos(61_000_000_000);
    report(&mut l, 1, 10, t0);
    report(&mut l, 2, 10, t0);
    assert_eq!(l.quorum_applied(100, t0, WITHIN), 10);
    // Past the freshness window the reports no longer count.
    let later = Nanos(t0.0 + 2_000_000_000);
    assert_eq!(l.quorum_applied(100, later, WITHIN), 100);
    // A fresh report from one voter alone does not make a quorum.
    report(&mut l, 1, 10, later);
    assert_eq!(l.quorum_applied(100, later, WITHIN), 100);
    report(&mut l, 2, 20, later);
    assert_eq!(l.quorum_applied(100, later, WITHIN), 20);
}

#[test]
fn a_stale_term_report_is_ignored() {
    let mut l = leader_of_three();
    let now = Nanos(61_000_000_000);
    let stale_term = l.term() - 1;
    let _ = l.handle(nid(1), ack(stale_term, 5), now, 7);
    let _ = l.handle(nid(2), ack(stale_term, 5), now, 7);
    assert_eq!(l.quorum_applied(100, now, WITHIN), 100);
}

#[test]
fn the_field_is_json_compatible_both_ways() {
    // Absent -> 0 (an older peer's JSON), and 0 is not written (a control-plane
    // message stays byte-identical to before the field existed).
    let old = r#"{"AppendEntriesResp":{"term":3,"success":true,"match_index":9,"needs_snapshot":false,"check_pending":false}}"#;
    let RaftMsg::AppendEntriesResp { applied_index, .. } =
        serde_json::from_str::<RaftMsg>(old).expect("old shape decodes")
    else {
        panic!("wrong variant");
    };
    assert_eq!(applied_index, 0);
    let back = serde_json::to_string(&serde_json::from_str::<RaftMsg>(old).unwrap()).unwrap();
    assert_eq!(back, old, "a zero applied_index adds no JSON field");
    let with = serde_json::to_string(&ack(3, 77)).unwrap();
    assert!(with.contains("\"applied_index\":77"), "{with}");
    // An older reader ignores the unknown field (serde's default): simulated by
    // stripping it back out through a generic value.
    let mut v: serde_json::Value = serde_json::from_str(&with).unwrap();
    v["AppendEntriesResp"]
        .as_object_mut()
        .unwrap()
        .remove("applied_index");
    let RaftMsg::AppendEntriesResp { applied_index, .. } =
        serde_json::from_value::<RaftMsg>(v).unwrap()
    else {
        panic!("wrong variant");
    };
    assert_eq!(applied_index, 0);
}
