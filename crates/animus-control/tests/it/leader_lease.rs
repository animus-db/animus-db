//! Leader lease / check-quorum on the real `RequestVote` (ADR 0009's
//! 2026-10-29 amendment; Raft thesis 4.2.3 "disruptive servers", etcd `inLease`).
//!
//! Pre-vote only guards the pre-vote phase. A starved follower whose stale
//! pre-vote grant came from a node that has since won its election still sends a
//! *real* `RequestVote` at a higher term, and the generic higher-term step-down
//! deposed the healthy leader (density corpus cell 3, seed 0x1c49137310331dab).
//! A leader holding its lease now ignores it; the lease is only held while a
//! quorum was heard from within an election timeout; and the leader's own
//! transfer target is exempt (recognised from local state, no wire change).

use std::time::Duration;

use animus_control::{RaftCore, RaftMsg, Role};
use animus_env::{Nanos, NodeId, nid};

fn group() -> [NodeId; 3] {
    [nid(0), nid(1), nid(2)]
}
const NOW: Nanos = Nanos(1_000_000_000);

fn after(base: Nanos, d: Duration) -> Nanos {
    Nanos(base.0 + d.as_nanos() as u64)
}

/// Elect node 0 leader of the 3-node group at `NOW` (grants from node 1).
fn elect_leader() -> RaftCore {
    let mut core: RaftCore = RaftCore::new(nid(0), &group(), Nanos(0), 7);
    let _ = core.tick(NOW, 7);
    let _ = core.handle(
        nid(1),
        RaftMsg::PreVoteResp {
            term: core.term() + 1,
            granted: true,
        },
        NOW,
        7,
    );
    let _ = core.handle(
        nid(1),
        RaftMsg::RequestVoteResp {
            term: core.term(),
            granted: true,
        },
        NOW,
        7,
    );
    assert!(core.is_leader());
    core
}

fn ack_all(core: &mut RaftCore, from: NodeId, now: Nanos) {
    let _ = core.handle(
        from,
        RaftMsg::AppendEntriesResp {
            term: core.term(),
            success: true,
            match_index: core.last_log_index(),
            needs_snapshot: false,
            check_pending: false,
            applied_index: 0,
        },
        now,
        7,
    );
}

fn request_vote(core: &RaftCore, candidate: NodeId, term_bump: u64) -> RaftMsg {
    RaftMsg::RequestVote {
        term: core.term() + term_bump,
        candidate,
        last_log_index: core.last_log_index(),
        last_log_term: core.term(),
    }
}

/// The reproduction: a leader with a live lease receives a real `RequestVote`
/// at a higher term from a node whose stale pre-vote was granted. It stays
/// leader, at the same term, and sends nothing back.
#[test]
fn leader_with_live_lease_ignores_higher_term_request_vote() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1), NOW);
    let term = core.term();
    let at = after(NOW, Duration::from_millis(10));
    assert!(core.leader_lease(at));

    let rv = request_vote(&core, nid(2), 1);
    let outs = core.handle(nid(2), rv, at, 7);
    assert!(outs.is_empty(), "ignored, not answered: {outs:?}");
    assert_eq!(core.role(), Role::Leader, "a healthy leader is not deposed");
    assert_eq!(core.term(), term, "the higher term is not adopted");
    assert!(core.campaign_stats().rv_ignored_lease >= 1);

    // And it still refuses the matching pre-vote with the same predicate.
    let outs = core.handle(
        nid(2),
        RaftMsg::PreVote {
            term: term + 2,
            candidate: nid(2),
            last_log_index: core.last_log_index(),
            last_log_term: core.term(),
        },
        at,
        7,
    );
    assert!(
        matches!(
            outs.as_slice(),
            [(_, RaftMsg::PreVoteResp { granted: false, .. })]
        ),
        "{outs:?}"
    );
}

/// A leader cut off from its quorum loses the lease within an election timeout:
/// it then behaves as before (steps down on a higher term and grants it).
#[test]
fn leader_without_quorum_contact_loses_the_lease() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1), NOW);
    let timeout = core.election_timeout();
    assert!(core.leader_lease(after(NOW, timeout / 2)));
    // No further acks: past one election timeout the lease is gone.
    let late = after(NOW, timeout + Duration::from_millis(1));
    assert!(!core.leader_lease(late), "no quorum contact, no lease");
    // A fresh ack from one peer (leader + peer is a majority of 3) restores it.
    ack_all(&mut core, nid(1), late);
    assert!(core.leader_lease(late));

    // Lapse again; now the higher-term vote is honoured like a plain Raft node.
    let lapsed = after(late, timeout * 2);
    assert!(!core.leader_lease(lapsed));
    let term = core.term();
    let rv = request_vote(&core, nid(2), 1);
    let outs = core.handle(nid(2), rv, lapsed, 7);
    assert_eq!(core.role(), Role::Follower);
    assert_eq!(core.term(), term + 1);
    assert!(
        matches!(
            outs.as_slice(),
            [(_, RaftMsg::RequestVoteResp { granted: true, .. })]
        ),
        "{outs:?}"
    );
}

/// A leader refuses a pre-vote whether or not its lease is intact (unchanged
/// rule): only the *real* vote consults the check-quorum lease, because a
/// CPU-starved leader's acks can sit in its own queue past the window and
/// granting pre-votes on that would let followers wave off a healthy leader.
#[test]
fn lapsed_leader_still_refuses_pre_vote() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1), NOW);
    let lapsed = after(NOW, core.election_timeout() * 2);
    let outs = core.handle(
        nid(2),
        RaftMsg::PreVote {
            term: core.term() + 1,
            candidate: nid(2),
            last_log_index: core.last_log_index(),
            last_log_term: core.term(),
        },
        lapsed,
        7,
    );
    assert!(
        matches!(
            outs.as_slice(),
            [(_, RaftMsg::PreVoteResp { granted: false, .. })]
        ),
        "{outs:?}"
    );
}

/// Leadership transfer still works: the leader's own transfer target is exempt
/// from the lease, any other higher-term candidate is not.
#[test]
fn transfer_target_bypasses_the_lease() {
    let mut core = elect_leader();
    ack_all(&mut core, nid(1), NOW);
    assert!(core.transfer_leadership(nid(1), NOW));
    let term = core.term();
    let at = after(NOW, Duration::from_millis(5));
    assert!(core.leader_lease(at));

    // Not the target: still ignored while the transfer is armed.
    let rv = request_vote(&core, nid(2), 1);
    assert!(core.handle(nid(2), rv, at, 7).is_empty());
    assert_eq!(core.role(), Role::Leader);

    // The target: honoured, the leader steps down and grants.
    let rv = request_vote(&core, nid(1), 1);
    let outs = core.handle(nid(1), rv, at, 7);
    assert_eq!(core.role(), Role::Follower);
    assert_eq!(core.term(), term + 1);
    assert!(
        matches!(
            outs.as_slice(),
            [(to, RaftMsg::RequestVoteResp { granted: true, .. })] if *to == nid(1)
        ),
        "{outs:?}"
    );
}

/// End to end over three cores: `TimeoutNow` makes the target campaign and the
/// leader's vote (bypassing its lease) elects it.
#[test]
fn transfer_elects_the_target() {
    let mut leader = elect_leader();
    let mut target: RaftCore = RaftCore::new(nid(1), &group(), Nanos(0), 7);
    // Replicate the election no-op to the target and feed its ack back.
    for (to, m) in leader.replicate_now(NOW) {
        if to == nid(1) {
            for (back, r) in target.handle(nid(0), m, NOW, 7) {
                assert_eq!(back, nid(0));
                let _ = leader.handle(nid(1), r, NOW, 7);
            }
        }
    }
    assert_eq!(target.last_log_index(), leader.last_log_index());
    assert!(leader.transfer_leadership(nid(1), NOW));
    // Fully caught up targets get TimeoutNow on the next broadcast.
    let at = after(NOW, Duration::from_millis(60));
    let outs = leader.tick(at, 7);
    let timeout_now = outs
        .into_iter()
        .find(|(to, m)| *to == nid(1) && matches!(m, RaftMsg::TimeoutNow { .. }))
        .map(|(_, m)| m);
    let Some(timeout_now) = timeout_now else {
        // The target never re-acked past the no-op: nothing to deliver.
        panic!("no TimeoutNow emitted");
    };
    let votes = target.handle(nid(0), timeout_now, at, 7);
    let rv = votes
        .into_iter()
        .find(|(to, _)| *to == nid(0))
        .map(|(_, m)| m)
        .expect("target asks the leader for its vote");
    let resp = leader.handle(nid(1), rv, at, 7);
    assert_eq!(leader.role(), Role::Follower);
    for (to, m) in resp {
        assert_eq!(to, nid(1));
        let _ = target.handle(nid(0), m, at, 7);
    }
    assert!(target.is_leader(), "the transfer target won the election");
}

/// A quiesced leader keeps its lease (it sends and hears nothing by design) and
/// a leader that was just elected holds it before any ack.
#[test]
fn fresh_leader_holds_the_lease_before_any_ack() {
    let core = elect_leader();
    assert!(core.leader_lease(after(NOW, Duration::from_millis(1))));
}

/// Deliver `msgs` (sent by `from`) to the cores named by their destination,
/// returning every message those cores answered with (as `(to, from, msg)`).
fn deliver(
    from: &NodeId,
    msgs: Vec<(NodeId, RaftMsg)>,
    cores: &mut [(NodeId, &mut RaftCore)],
    now: Nanos,
) -> Vec<(NodeId, NodeId, RaftMsg)> {
    let mut answers = Vec::new();
    for (to, msg) in msgs {
        for (id, c) in cores.iter_mut() {
            if *id == to {
                for (back, m) in c.handle(from.clone(), msg.clone(), now, 7) {
                    answers.push((back, to.clone(), m));
                }
            }
        }
    }
    answers
}

/// ADR 0009 (2026-10-29 amendment, pre-vote cluster check): a voter wiped and
/// restarted under the same id has a pending boot-time cluster check. It refuses
/// real votes (issue #667) and must refuse pre-votes the same way; otherwise,
/// until its first AppendEntries, it hands a live follower a pre-vote majority,
/// the follower sends a real higher-term `RequestVote`, and the leader (or the
/// write proposed in the gap) is disturbed.
#[test]
fn wiped_voter_with_pending_check_refuses_pre_vote() {
    let mut leader = elect_leader();
    let mut follower: RaftCore = RaftCore::new(nid(1), &group(), Nanos(0), 7);
    let t0 = after(NOW, Duration::from_millis(5));
    // The follower hears the leader, then the leader learns it is caught up.
    let hb = leader.replicate_now(t0);
    let answers = deliver(&nid(0), hb, &mut [(nid(1), &mut follower)], t0);
    for (_, from, m) in answers {
        let _ = leader.handle(from, m, t0, 7);
    }
    assert_eq!(follower.leader(), Some(nid(0)));
    let term = leader.term();

    // The wiped voter: fresh core, check pending, has not heard the leader.
    let mut wiped: RaftCore = RaftCore::new(nid(2), &group(), Nanos(0), 7);
    let _probes = wiped.begin_cluster_check(t0, 7);
    assert!(wiped.cluster_check_pending());

    // The follower's election timer fires before the wiped node hears anyone.
    let fire = follower.next_deadline().expect("follower has a timer");
    let outs = follower.tick(fire, 7);
    assert!(
        outs.iter()
            .any(|(_, m)| matches!(m, RaftMsg::PreVote { .. })),
        "the follower starts a pre-vote round: {outs:?}"
    );
    let resps = deliver(
        &nid(1),
        outs,
        &mut [(nid(0), &mut leader), (nid(2), &mut wiped)],
        fire,
    );
    assert_eq!(resps.len(), 2, "{resps:?}");
    let mut sent = Vec::new();
    for (_, from, m) in resps {
        assert!(
            matches!(m, RaftMsg::PreVoteResp { granted: false, .. }),
            "node {from} must refuse the pre-vote: {m:?}"
        );
        sent.extend(follower.handle(from, m, fire, 7));
    }
    assert!(
        !sent
            .iter()
            .any(|(_, m)| matches!(m, RaftMsg::RequestVote { .. })),
        "no real higher-term RequestVote may be sent: {sent:?}"
    );
    assert_eq!(wiped.campaign_stats().pv_refused_unchecked, 1);
    assert_eq!(wiped.campaign_stats().pv_granted, 0);

    // The leader is undisturbed and a write proposed to it commits.
    assert_eq!(leader.role(), Role::Leader);
    assert_eq!(leader.term(), term);
    let before = leader.commit_index();
    assert!(matches!(
        leader.propose(animus_control::MetaCommand::UpsertMember {
            node: nid(200),
            labels: Default::default(),
            status: animus_control::NodeStatus::Active,
        }),
        animus_control::ProposeResult::Accepted { .. }
    ));
    let hb = leader.replicate_now(fire);
    let answers = deliver(&nid(0), hb, &mut [(nid(1), &mut follower)], fire);
    for (_, from, m) in answers {
        let _ = leader.handle(from, m, fire, 7);
    }
    assert!(leader.commit_index() > before, "the write commits");
}
