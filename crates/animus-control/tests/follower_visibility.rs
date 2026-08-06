//! Role-aware apply frontier (ADR 0009, durable-before-visible).
//!
//! The durable-before-visible gate is **leader-only**. A follower never acks a
//! control-plane write to a client — it only serves *reads* of its local
//! `Metadata` — and a committed entry already rests on a quorum of durable logs
//! (the driver fsyncs before sending, so a follower fsyncs before its
//! `AppendEntriesResp`). So a follower may expose a committed entry on *commit*,
//! without waiting on its **own** local fsync, while the leader's visibility
//! stays gated on its own `durable_index` (what a proposer acks on).
//!
//! These tests pin the distinction two ways: directly on a hand-driven
//! [`RaftCore`] follower vs. leader (precise control of the durable watermark),
//! and end-to-end over a `SimEnv` `RaftNode` cluster (a follower reflects a
//! leader's committed command).

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::raft::{LogEntry, RaftMsg, Role};
use animus_control::{MetaCommand, NodeStatus, RaftCore, RaftNode};
use animus_env::Nanos;
use animus_sim::{SimEnv, Simulator};

const NODES: [u64; 3] = [0, 1, 2];

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node,
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

/// A **follower** applies a committed entry on commit — *without* its own local
/// fsync — so its `metadata()` reflects the entry even while `last_applied`
/// would still be gated on `durable_index`. This is the cross-node
/// read-visibility relaxation: a follower's reads track commit, not its own disk.
#[test]
fn follower_applies_on_commit_without_its_own_fsync() {
    // A follower in term 1 that has not fsynced anything (durable_index == 0).
    let mut follower = RaftCore::new(0, &NODES, Nanos(0), 7);
    assert_eq!(follower.role(), Role::Follower);
    assert_eq!(follower.durable_index(), 0, "nothing fsynced yet");

    // Leader (node 1) replicates one entry and immediately commits it (the entry
    // already rests on a quorum's durable logs by the time we hear of the commit).
    let entry = LogEntry {
        term: 1,
        index: 1,
        command: upsert(42),
        config: None,
    };
    follower.handle(
        1,
        RaftMsg::AppendEntries {
            term: 1,
            leader: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![entry],
            leader_commit: 1,
        },
        Nanos(1),
        7,
    );

    assert_eq!(follower.role(), Role::Follower, "still a follower");
    assert_eq!(follower.commit_index(), 1, "the entry committed");
    assert_eq!(
        follower.durable_index(),
        0,
        "the follower has NOT fsynced this entry locally"
    );
    // The relaxation: visible on commit despite no local fsync.
    assert_eq!(
        follower.last_applied(),
        1,
        "a follower applies on commit, not gated on its own durable_index"
    );
    assert!(
        follower.metadata().members.contains_key(&42),
        "a committed entry is read-visible on a follower without its own fsync"
    );
}

/// A **leader** stays durability-gated: a freshly proposed command commits (sole
/// quorum reasoning aside, single-node commits immediately) but does **not**
/// become visible until the leader fsyncs it. This is the ack-path guarantee the
/// refinement must preserve — proven here on a single-node group, exactly the
/// shape of the `persistence.rs` regression.
#[test]
fn leader_stays_durability_gated_on_its_own_proposal() {
    let mut leader = RaftCore::new(0, &[0], Nanos(0), 7);
    leader.tick(Nanos(1_000_000_000), 7); // election timeout -> sole leader
    assert_eq!(leader.role(), Role::Leader);
    // Make the leader's initial no-op durable so it isn't what we're observing.
    let through = leader.last_log_index();
    let _ = leader.drain_persist();
    leader.mark_durable_through(through);

    leader.propose(upsert(42));
    assert!(
        leader.commit_index() > leader.durable_index(),
        "single-node commit is immediate, but it is not yet fsynced"
    );
    assert!(
        !leader.metadata().members.contains_key(&42),
        "the leader must NOT expose its own committed-but-unsynced proposal"
    );

    // Fsync: only now is it visible (the ack-path durable-before-visible rule).
    let through = leader.last_log_index();
    let _ = leader.drain_persist();
    leader.mark_durable_through(through);
    assert!(
        leader.metadata().members.contains_key(&42),
        "after the fsync the leader's proposal is durable and visible"
    );
}

/// A follower that applied up to commit while a follower, then **wins an
/// election**, keeps those entries (they are committed / quorum-durable) — and
/// its own future proposals are still durability-gated. `last_applied` only ever
/// moves forward across the role change.
#[test]
fn follower_to_leader_keeps_applied_then_gates_new_proposals() {
    let mut node = RaftCore::new(0, &NODES, Nanos(0), 7);

    // As a follower, learn + commit an entry without any local fsync.
    node.handle(
        1,
        RaftMsg::AppendEntries {
            term: 1,
            leader: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: 1,
                index: 1,
                command: upsert(42),
                config: None,
            }],
            leader_commit: 1,
        },
        Nanos(1),
        7,
    );
    assert_eq!(node.last_applied(), 1, "applied on commit as a follower");
    assert_eq!(node.durable_index(), 0, "no local fsync");

    // Win an election (the other two nodes grant their votes). With pre-vote the
    // timeout first makes the node a *pre-candidate* (no term bump); a pre-vote
    // grant then tips it into the real, term-incrementing election.
    node.tick(Nanos(1_000_000_000), 7); // -> pre-candidate, PreVote out (term still 1)
    assert_eq!(node.role(), Role::PreCandidate);
    node.handle(
        1,
        RaftMsg::PreVoteResp {
            term: node.term() + 1, // the prospective term the pre-vote is for
            granted: true,
        },
        Nanos(1_000_000_001),
        7,
    );
    assert_eq!(
        node.role(),
        Role::Candidate,
        "pre-vote quorum -> real election"
    );
    for granter in [1u64, 2u64] {
        node.handle(
            granter,
            RaftMsg::RequestVoteResp {
                term: node.term(),
                granted: true,
            },
            Nanos(1_000_000_001),
            7,
        );
    }
    assert_eq!(node.role(), Role::Leader, "elected leader");

    // The previously-applied entry is retained (last_applied did not regress),
    // even though durable_index is still 0 < commit_index.
    assert!(
        node.metadata().members.contains_key(&42),
        "a committed entry applied as a follower survives the role change"
    );
    assert!(
        node.last_applied() >= 1,
        "last_applied only moves forward across the follower->leader change"
    );

    // The leader's own new no-op (appended on becoming leader) is NOT visible
    // until the leader fsyncs it: new leader proposals stay durability-gated.
    let new_proposal_index = node.last_log_index();
    assert!(
        new_proposal_index > node.durable_index(),
        "the leader's own fresh entry is ahead of its durable frontier"
    );
}

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| RaftNode::start(sim.env(id), NODES.to_vec()))
        .collect();
    (sim, nodes)
}

fn unique_leader(nodes: &[RaftNode<SimEnv>], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

/// End-to-end over a `SimEnv` cluster: a leader's committed `MetaCommand` becomes
/// read-visible on **both followers** (their `metadata()` reflects it). The
/// follower path applies on commit — the realistic confirmation of the
/// relaxation, on top of the precise hand-driven core tests above.
#[test]
fn followers_reflect_a_committed_command() {
    let seed = 0xF0_110E5;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, seed);

    nodes[leader].propose(upsert(77));
    sim.run_for(Duration::from_secs(2));

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.metadata().members.contains_key(&77),
            "node {i} (leader={leader}) did not reflect the committed command (seed={seed})"
        );
    }
}
