//! Raft section 5.3: an `AppendEntries` vouches for the follower's log only up
//! to `prev_log_index + entries.len()`, so the follower's commit index may
//! follow `leader_commit` no further than that. `handle_append_entries` used
//! `min(leader_commit, own last_log_index)`, which commits an unchecked stale
//! suffix of an earlier term whenever the follower's log is longer than what
//! the message covers.
//!
//! Hand-drives one follower `RaftCore`: the property is a pure function of the
//! message it receives.

use animus_control::{LogEntry, MetaCommand, NodeStatus, RaftCore, RaftMsg};
use animus_env::{Nanos, nid};
use std::collections::BTreeMap;

fn entry(term: u64, index: u64, node: u64) -> LogEntry {
    LogEntry {
        term,
        index,
        command: MetaCommand::UpsertMember {
            node: nid(node),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        },
        config: None,
        learners: None,
    }
}

fn append(
    term: u64,
    leader: u64,
    prev: (u64, u64),
    entries: Vec<LogEntry>,
    leader_commit: u64,
) -> RaftMsg {
    RaftMsg::AppendEntries {
        term,
        leader: nid(leader),
        prev_log_index: prev.0,
        prev_log_term: prev.1,
        entries,
        leader_commit,
    }
}

/// A follower holding an uncommitted term-1 tail (indexes 3..=5) receives an
/// EMPTY `AppendEntries` from a term-2 leader whose log agrees only through
/// index 2, with `leader_commit` 5. Indexes 3..=5 were never checked against
/// the new leader's log, so the commit index must stop at 2.
#[test]
fn an_empty_append_never_commits_the_followers_stale_suffix() {
    let now = Nanos(1_000_000_000);
    let mut f: RaftCore = RaftCore::new(nid(1), &[nid(0), nid(1), nid(2)], Nanos(0), 3);
    let _ = f.handle(
        nid(0),
        append(
            1,
            0,
            (0, 0),
            (1..=5).map(|i| entry(1, i, 600 + i)).collect(),
            0,
        ),
        now,
        3,
    );
    assert_eq!(f.last_log_index(), 5);
    let _ = f.handle(nid(2), append(2, 2, (2, 1), Vec::new(), 5), now, 3);
    assert_eq!(
        f.commit_index(),
        2,
        "indexes 3..=5 were never checked against the term-2 leader's log"
    );
}

/// The same bound for a message that carries entries: the leader's batch ends
/// at index 3 (matching), `leader_commit` is 5, the follower's own tail goes
/// on to 5. Only 3 is vouched for.
#[test]
fn a_short_matching_batch_commits_only_what_it_carried() {
    let now = Nanos(1_000_000_000);
    let mut f: RaftCore = RaftCore::new(nid(1), &[nid(0), nid(1), nid(2)], Nanos(0), 3);
    let _ = f.handle(
        nid(0),
        append(
            1,
            0,
            (0, 0),
            (1..=5).map(|i| entry(1, i, 600 + i)).collect(),
            0,
        ),
        now,
        3,
    );
    // Term-2 leader sends entry 3 again (same term-1 entry: matching), with a
    // high leader_commit.
    let _ = f.handle(
        nid(2),
        append(2, 2, (2, 1), vec![entry(1, 3, 603)], 5),
        now,
        3,
    );
    assert_eq!(f.commit_index(), 3);
}

/// The ordinary case is unchanged: a batch that covers the commit index
/// commits up to it.
#[test]
fn a_covering_batch_still_commits_up_to_leader_commit() {
    let now = Nanos(1_000_000_000);
    let mut f: RaftCore = RaftCore::new(nid(1), &[nid(0), nid(1), nid(2)], Nanos(0), 3);
    let _ = f.handle(
        nid(0),
        append(
            1,
            0,
            (0, 0),
            (1..=5).map(|i| entry(1, i, 600 + i)).collect(),
            4,
        ),
        now,
        3,
    );
    assert_eq!(f.commit_index(), 4);
}
