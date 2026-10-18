//! ADR 0080 section 3: the transport class every Raft message is sent with.

use animus_control::MetaCommand;
use animus_control::raft::{LogEntry, RaftMsg};
use animus_env::{MsgClass, nid};

fn class(m: RaftMsg) -> (MsgClass, Option<u64>) {
    m.transport_class()
}

#[test]
fn votes_are_priority_heartbeats_supersede_entries_replicate_and_snapshots_are_bulk() {
    assert_eq!(
        class(RaftMsg::PreVote {
            term: 3,
            candidate: nid(1),
            last_log_index: 0,
            last_log_term: 0
        })
        .0,
        MsgClass::Vote
    );
    assert_eq!(
        class(RaftMsg::RequestVoteResp {
            term: 3,
            granted: true
        })
        .0,
        MsgClass::Vote
    );
    assert_eq!(class(RaftMsg::TimeoutNow { term: 3 }).0, MsgClass::Vote);

    let bare = RaftMsg::AppendEntries {
        term: 3,
        leader: nid(0),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: Vec::new(),
        leader_commit: 0,
    };
    assert_eq!(
        class(bare),
        (MsgClass::Heartbeat, Some(0)),
        "a newer bare heartbeat supersedes"
    );

    let with_entries = RaftMsg::<MetaCommand>::AppendEntries {
        term: 3,
        leader: nid(0),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![LogEntry {
            term: 3,
            index: 1,
            command: MetaCommand::NoOp,
            config: None,
            learners: None,
        }],
        leader_commit: 0,
    };
    assert_eq!(class(with_entries), (MsgClass::Replicate, None));

    assert_eq!(
        class(RaftMsg::InstallSnapshot {
            term: 3,
            leader: nid(0),
            last_index: 1,
            last_term: 1,
            offset: 0,
            data: vec![0; 4],
            total: 4,
            done: true,
            config: None,
            learners: None,
        })
        .0,
        MsgClass::Bulk
    );
}
