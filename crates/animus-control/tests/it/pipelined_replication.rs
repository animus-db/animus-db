//! Issue #1304: a leader used to re-send a peer's whole unacknowledged tail on
//! every propose, every ack and every heartbeat, because `next_index` only
//! moves on an ack. With `w` entries in flight each ack provoked another
//! `w`-entry `AppendEntries`, so the bytes put on the wire (and the encoded
//! copies queued in the transport) grew with the square of the ack lag: a
//! 4 KiB write load measured 30x more entry traffic than the entries
//! written, and 450-700 MB of queued sends on one node.
//!
//! The leader now remembers, per peer, how far it has already sent
//! (`RaftCore::inflight_sent`). Up to `INFLIGHT_OVERLAP_MAX_ENTRIES` (4)
//! entries in flight nothing changes (each send still carries the whole
//! unacknowledged tail, so a lightly loaded group keeps its one-round-trip
//! latency). At or above that depth the propose and ack paths send nothing
//! more, the new entries go out as one batch when the ack lands, and the
//! heartbeat is an empty `AppendEntries` at the acknowledged position until
//! the in-flight entries have gone unacknowledged for
//! `INFLIGHT_RETRANSMIT_HEARTBEATS` (4) heartbeats. A reject forgets the mark.
//!
//! The same change made the follower's commit rule follow Raft section 5.3
//! (commit up to the last entry *the message vouched for*, not the follower's
//! own last index), which the empty heartbeat would otherwise have turned into
//! committing a stale suffix.
//!
//! Hand-drives `RaftCore`s directly, no `Simulator`: every property is a pure
//! function of the messages exchanged.

use animus_control::{LogEntry, MetaCommand, NodeStatus, ProposeResult, RaftCore, RaftMsg};
use animus_env::{Nanos, nid};
use std::collections::BTreeMap;

const HEARTBEAT_NS: u64 = 50_000_000;

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

fn elect_leader_of_pair(now: Nanos) -> RaftCore {
    let mut leader: RaftCore = RaftCore::new(nid(0), &[nid(0), nid(1)], Nanos(0), 7);
    let _ = leader.tick(now, 7);
    let _ = leader.handle(
        nid(1),
        RaftMsg::PreVoteResp {
            term: leader.term() + 1,
            granted: true,
        },
        now,
        7,
    );
    let _ = leader.handle(
        nid(1),
        RaftMsg::RequestVoteResp {
            term: leader.term(),
            granted: true,
        },
        now,
        7,
    );
    assert!(leader.is_leader(), "node 0 should have won the election");
    leader
}

fn ack(term: u64, match_index: u64) -> RaftMsg {
    RaftMsg::AppendEntriesResp {
        term,
        success: true,
        match_index,
        needs_snapshot: false,
        check_pending: false,
    }
}

/// `(prev_log_index, entry count)` of every `AppendEntries` in `outs`.
fn appends(outs: &[(animus_env::NodeId, RaftMsg)]) -> Vec<(u64, usize)> {
    outs.iter()
        .filter_map(|(_, m)| match m {
            RaftMsg::AppendEntries {
                prev_log_index,
                entries,
                ..
            } => Some((*prev_log_index, entries.len())),
            _ => None,
        })
        .collect()
}

/// Propose `n` entries one at a time with no ack in between, returning the
/// `AppendEntries` each `replicate_now` produced.
fn burst(leader: &mut RaftCore, now: Nanos, first: u64, n: u64) -> Vec<Vec<(u64, usize)>> {
    (0..n)
        .map(|i| {
            assert!(matches!(
                leader.propose(upsert(first + i)),
                ProposeResult::Accepted { .. }
            ));
            appends(&leader.replicate_now(now))
        })
        .collect()
}

/// A burst of proposes with no ack in between stops growing the traffic once
/// 4 entries are in flight (the old behaviour re-sent the whole tail every
/// time: 1 + 2 + ... + n entries).
#[test]
fn a_propose_burst_stops_resending_the_tail_once_the_window_is_deep() {
    let now = Nanos(1_000_000_000);
    let mut leader = elect_leader_of_pair(now);
    let n = 40u64;
    let sent: usize = burst(&mut leader, now, 100, n)
        .iter()
        .flatten()
        .map(|(_, c)| c)
        .sum();
    let quadratic = (n * (n + 1) / 2) as usize;
    assert!(
        sent < quadratic / 5,
        "{sent} entries sent for {n} proposes ({quadratic} if every send carried the whole tail)"
    );
}

/// Once the window is deep the proposes send nothing, and the ack that lands
/// is answered with ONE message carrying everything proposed meanwhile,
/// anchored at the acknowledged position.
#[test]
fn entries_proposed_behind_a_deep_window_go_out_as_one_batch_on_the_ack() {
    let now = Nanos(1_000_000_000);
    let mut leader = elect_leader_of_pair(now);
    let term = leader.term();
    let _ = leader.replicate_now(now);
    let per_propose = burst(&mut leader, now, 200, 30);
    // The tail of the burst sent nothing.
    assert!(
        per_propose[20..].iter().all(Vec::is_empty),
        "{per_propose:?}"
    );
    let last = leader.last_log_index();
    // The peer acks what the earlier messages carried.
    let acked = last - 25;
    let out = leader.handle(nid(1), ack(term, acked), now, 7);
    assert_eq!(
        appends(&out),
        vec![(acked, 25)],
        "one batch with everything above the acked index"
    );
}

/// With a shallow window nothing changes: the heartbeat still carries the
/// unacknowledged tail (fast loss recovery for a lightly loaded group).
#[test]
fn a_shallow_window_keeps_the_heartbeat_resending_the_tail() {
    let t0 = 1_000_000_000u64;
    let mut leader = elect_leader_of_pair(Nanos(t0));
    let term = leader.term();
    let _ = leader.replicate_now(Nanos(t0));
    let acked = leader.last_log_index();
    let _ = leader.handle(nid(1), ack(term, acked), Nanos(t0), 7);
    let _ = burst(&mut leader, Nanos(t0), 300, 2);
    let out = leader.tick(Nanos(t0 + HEARTBEAT_NS), 7);
    assert_eq!(appends(&out), vec![(acked, 2)]);
}

/// While a deep window is in flight the heartbeat is empty and anchored at the
/// acknowledged position; once the entries have gone unacknowledged for four
/// heartbeats the tail is offered again from there.
#[test]
fn heartbeats_stay_empty_behind_a_deep_window_until_the_retransmit_window() {
    let t0 = 1_000_000_000u64;
    let mut leader = elect_leader_of_pair(Nanos(t0));
    let term = leader.term();
    let _ = leader.replicate_now(Nanos(t0));
    let acked = leader.last_log_index();
    let _ = leader.handle(nid(1), ack(term, acked), Nanos(t0), 7);
    let _ = burst(&mut leader, Nanos(t0), 400, 12);
    let last = leader.last_log_index();
    assert_eq!(last, acked + 12);

    for k in 1..=3u64 {
        let out = leader.tick(Nanos(t0 + k * HEARTBEAT_NS), 7);
        assert_eq!(appends(&out), vec![(acked, 0)], "heartbeat {k}");
    }
    let out = leader.tick(Nanos(t0 + 4 * HEARTBEAT_NS), 7);
    assert_eq!(appends(&out), vec![(acked, 12)], "retransmit after 4 beats");
    let out = leader.tick(Nanos(t0 + 5 * HEARTBEAT_NS), 7);
    assert_eq!(appends(&out), vec![(acked, 0)], "window restarted");
}

/// A reject means the follower does not have what was sent: the leader backs
/// off and resends from `next_index`, it does not wait out the window.
#[test]
fn a_reject_behind_a_deep_window_resends_at_once() {
    let now = Nanos(1_000_000_000);
    let mut leader = elect_leader_of_pair(now);
    let term = leader.term();
    let _ = leader.replicate_now(now);
    let acked = leader.last_log_index();
    let _ = leader.handle(nid(1), ack(term, acked), now, 7);
    let _ = burst(&mut leader, now, 500, 12);
    let out = leader.handle(
        nid(1),
        RaftMsg::AppendEntriesResp {
            term,
            success: false,
            match_index: 0,
            needs_snapshot: false,
            check_pending: false,
        },
        now,
        7,
    );
    let a = appends(&out);
    assert_eq!(a.len(), 1, "{out:?}");
    assert!(a[0].1 >= 12, "the whole unacknowledged tail again: {a:?}");
}

fn entry(term: u64, index: u64, node: u64) -> LogEntry {
    LogEntry {
        term,
        index,
        command: upsert(node),
        config: None,
        learners: None,
    }
}

/// Raft section 5.3: an `AppendEntries` vouches for the log only up to
/// `prev_log_index + entries.len()`. A follower holding a stale suffix of an
/// earlier term past that point must not commit it because the leader's
/// `leader_commit` is higher.
#[test]
fn a_follower_never_commits_past_the_last_entry_the_message_vouched_for() {
    let now = Nanos(1_000_000_000);
    let mut f: RaftCore = RaftCore::new(nid(1), &[nid(0), nid(1), nid(2)], Nanos(0), 3);
    // Term 1, leader 0: entries 1..=5, none committed.
    let _ = f.handle(
        nid(0),
        RaftMsg::AppendEntries {
            term: 1,
            leader: nid(0),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: (1..=5).map(|i| entry(1, i, 600 + i)).collect(),
            leader_commit: 0,
        },
        now,
        3,
    );
    assert_eq!(f.last_log_index(), 5);
    // Term 2, leader 2, whose log agrees only through index 2. An empty
    // heartbeat at prev=2 with a high leader_commit.
    let _ = f.handle(
        nid(2),
        RaftMsg::AppendEntries {
            term: 2,
            leader: nid(2),
            prev_log_index: 2,
            prev_log_term: 1,
            entries: Vec::new(),
            leader_commit: 5,
        },
        now,
        3,
    );
    assert_eq!(
        f.commit_index(),
        2,
        "indexes 3..=5 were never checked against the term-2 leader's log"
    );
}
