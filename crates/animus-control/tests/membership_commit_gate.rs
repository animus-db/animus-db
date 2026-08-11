//! Core-level `change_membership` tests: the **current-term-commit gate** (the
//! Raft membership-change erratum guard) plus the single-server rules.
//!
//! A freshly elected leader's `commit_index` may lag entries the previous leader
//! committed (the commit rule never counts old-term entries toward a majority),
//! so appending a *config* entry before the leader has committed an entry of its
//! own term (its election no-op) risks acting on a stale view of the config
//! history. `RaftCore::change_membership` therefore rejects until
//! `commit_index >= first_term_index` — the same watermark the data plane's
//! ReadIndex barrier gates on (Raft §6.4).
//!
//! These drive a hand-built `RaftCore` at message granularity (no driver, no
//! sim), so the pre-/post-no-op-commit window is exact.

use std::collections::BTreeSet;

use animus_control::{ProposeResult, RaftCore, RaftMsg};
use animus_env::{Nanos, NodeId, nid};
fn group() -> [NodeId; 3] {
    [nid(0), nid(1), nid(2)]
}
const NOW: Nanos = Nanos(1_000_000_000);

fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
    ids.iter().cloned().collect()
}

/// Elect node `group()[0]` leader of the 3-node group by timing it out into a
/// pre-vote and feeding it one granted pre-vote + one granted vote (its own +
/// node 1 = a majority of 3). The election no-op sits **uncommitted** at index 1.
fn elect_leader() -> RaftCore {
    let mut core: RaftCore = RaftCore::new(group()[0].clone(), &group(), Nanos(0), 7);
    let _ = core.tick(NOW, 7); // election timeout -> pre-candidate
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
/// commit (leader + one follower = a majority of 3).
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

#[test]
fn change_membership_rejects_until_the_no_op_commits_then_accepts() {
    let mut core = elect_leader();

    // The election no-op is the leader's first current-term entry, index 1 —
    // exposed via the accessor the gates (and the ReadIndex barrier) use.
    assert_eq!(core.first_term_index(), Some(1));
    assert_eq!(core.commit_index(), 0, "the no-op is not yet committed");

    // Gate closed: no config entry until an entry of this term commits.
    assert_eq!(
        core.change_membership(set(&[nid(0), nid(1)])),
        ProposeResult::NotLeader {
            leader: Some(nid(0))
        },
        "a config change before the no-op commits must be rejected"
    );
    assert_eq!(
        core.config(),
        set(&group()),
        "the rejected change must not touch the active config"
    );

    // One follower ack commits the no-op (majority of 3).
    ack_all(&mut core, nid(1));
    assert_eq!(core.commit_index(), 1, "the no-op committed");

    // Gate open: the same change is now accepted and adopted immediately.
    assert!(matches!(
        core.change_membership(set(&[nid(0), nid(1)])),
        ProposeResult::Accepted { index: 2 }
    ));
    assert_eq!(core.config(), set(&[nid(0), nid(1)]));
}

#[test]
fn first_term_index_is_leader_only() {
    // A follower has no current-term appends of its own to gate on.
    let follower: RaftCore = RaftCore::new(group()[1].clone(), &group(), Nanos(0), 7);
    assert_eq!(follower.first_term_index(), None);

    // A leader reports its no-op's index; re-election re-records it.
    let core = elect_leader();
    assert_eq!(core.first_term_index(), Some(1));
}

#[test]
fn single_server_rules_still_enforced_after_the_gate() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1)); // commit the no-op; the term gate is open

    // Multi-server delta: {0,1,2} -> {0} removes two — needs joint consensus.
    assert!(matches!(
        core.change_membership(set(&[nid(0)])),
        ProposeResult::NotLeader { .. }
    ));
    // Leader self-removal: {1,2} drops the leader — transfer leadership first.
    assert!(matches!(
        core.change_membership(set(&[nid(1), nid(2)])),
        ProposeResult::NotLeader { .. }
    ));
    // No-op delta: nothing to change.
    assert!(matches!(
        core.change_membership(set(&group())),
        ProposeResult::NotLeader { .. }
    ));

    // A valid single-server add is accepted...
    assert!(matches!(
        core.change_membership(set(&[nid(0), nid(1), nid(2), nid(3)])),
        ProposeResult::Accepted { .. }
    ));
    // ...and a second change is rejected while the first is in flight
    // (uncommitted config entry).
    assert!(matches!(
        core.change_membership(set(&[nid(0), nid(1), nid(2), nid(3), nid(4)])),
        ProposeResult::NotLeader { .. }
    ));

    // Commit the config entry (majority of the new 4-node config = 3: leader +
    // two follower acks), then the next single-server step is accepted.
    ack_all(&mut core, nid(1));
    ack_all(&mut core, nid(2));
    assert_eq!(core.commit_index(), core.last_log_index());
    assert!(matches!(
        core.change_membership(set(&[nid(0), nid(1), nid(2), nid(3), nid(4)])),
        ProposeResult::Accepted { .. }
    ));
    assert_eq!(
        core.config(),
        set(&[nid(0), nid(1), nid(2), nid(3), nid(4)])
    );
}
