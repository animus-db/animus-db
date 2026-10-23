//! Pure-core regression for the snapshot storm after a leader change:
//! `RaftCore::compaction_floor` must not read a peer the NEW leader has not
//! heard from yet (`match_index == 0`, the `become_leader` seed) as "caught up
//! to 0" and so, on a log past the retention cap, "too far behind to retain
//! for". The peer is estimated at the commit index the leader inherited; the
//! cap still bounds how long a peer that never answers can pin the log.

use animus_control::{MetaCommand, NodeStatus, ProposeResult, RaftCore, RaftMsg};
use animus_env::{Nanos, nid};
use std::collections::BTreeMap;

const CAP: u64 = 100;

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

fn win_election(core: &mut RaftCore, now: Nanos) {
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
    assert!(core.is_leader(), "node 0 should have won the election");
}

fn ack(term: u64, match_index: u64) -> RaftMsg {
    RaftMsg::AppendEntriesResp {
        term,
        success: true,
        match_index,
        needs_snapshot: false,
        check_pending: false,
        applied_index: 0,
    }
}

/// A 3-voter leader that has replicated and committed `n` entries to both
/// peers, then lost and regained leadership (so every `match_index` is the
/// fresh-leader seed `0` again).
fn re_elected_leader_with_long_log(n: u64) -> RaftCore {
    let mut core: RaftCore = RaftCore::new(nid(0), &[nid(0), nid(1), nid(2)], Nanos(0), 7);
    win_election(&mut core, Nanos(1_000_000_000));
    for i in 0..n {
        assert!(matches!(
            core.propose(upsert(100 + i)),
            ProposeResult::Accepted { .. }
        ));
    }
    let term = core.term();
    let last = core.last_log_index();
    for p in [1, 2] {
        let _ = core.handle(nid(p), ack(term, last), Nanos(1_000_000_000), 7);
    }
    assert_eq!(core.commit_index(), last, "both peers acked everything");
    // Depose it with a higher-term ack, then let it win again.
    let _ = core.handle(nid(1), ack(term + 5, 0), Nanos(2_000_000_000), 7);
    assert!(!core.is_leader());
    win_election(&mut core, Nanos(60_000_000_000));
    core
}

#[test]
fn an_unheard_peer_of_a_fresh_leader_is_retained_for_not_excluded() {
    let core = re_elected_leader_with_long_log(CAP * 3);
    assert!(core.last_log_index() > CAP);
    let floor = core.compaction_floor(CAP);
    assert!(
        floor.is_some_and(|f| f > CAP),
        "fresh leader, peers not yet heard from: the floor must sit near the inherited commit \
         index, not exclude both peers (got {floor:?}, last={})",
        core.last_log_index()
    );
}

#[test]
fn a_peer_that_never_answers_is_still_excluded_once_the_cap_passes() {
    let mut core = re_elected_leader_with_long_log(CAP * 3);
    for i in 0..(CAP + 10) {
        let _ = core.propose(upsert(900 + i));
    }
    assert_eq!(
        core.compaction_floor(CAP),
        None,
        "a peer silent for more than the retention cap must not pin the log forever"
    );
}

#[test]
fn a_peer_that_answers_is_floored_at_its_own_match_index() {
    let mut core = re_elected_leader_with_long_log(CAP * 3);
    let term = core.term();
    let last = core.last_log_index();
    let _ = core.handle(nid(1), ack(term, last - 5), Nanos(61_000_000_000), 7);
    let _ = core.handle(nid(2), ack(term, last - 7), Nanos(61_000_000_000), 7);
    assert_eq!(core.compaction_floor(CAP), Some(last - 7));
}
