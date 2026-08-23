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
//!
//! ADR 0038 PR3: `Metadata` is `DRIVER_APPLIED`, so a hand-driven `RaftCore` has
//! no `metadata()` to read — "is this entry visible" is instead "does
//! `drain_apply()` yield it", the exact same underlying frontier gate
//! (`RaftCore::apply`'s role-aware `min(commit_index, durable_index)` on a
//! leader / `commit_index` on a follower) just observed at its new drain
//! point. See `persistence.rs`'s `drain_and_apply` idiom.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::raft::{LogEntry, RaftMsg, Role};
use animus_control::{MetaCommand, Metadata, NodeStatus, RaftCore, RaftNode, mirror};
use animus_env::{Nanos, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

fn member_ids() -> [NodeId; 3] {
    [nid(0), nid(1), nid(2)]
}

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

/// Whether `core.drain_apply()` currently yields a command that upserts
/// `node` — the DRIVER_APPLIED-era stand-in for "is this entry visible in
/// `metadata()`". Applies whatever's drained onto a throwaway oracle so the
/// check is a real `Metadata::apply`, not just an index comparison.
fn drained_contains_member(core: &mut RaftCore, node: u64) -> bool {
    let mut oracle = Metadata::default();
    for (_, _, command) in core.drain_apply() {
        let _ = mirror::apply_and_derive_mirror(&mut oracle, &command);
    }
    oracle.members.contains_key(&nid(node))
}

/// A **follower** applies a committed entry on commit — *without* its own local
/// fsync — so it becomes drainable via `drain_apply()` even while
/// `last_applied` would still be gated on `durable_index` for a leader. This is
/// the cross-node read-visibility relaxation: a follower's reads track commit,
/// not its own disk.
#[test]
fn follower_applies_on_commit_without_its_own_fsync() {
    // A follower in term 1 that has not fsynced anything (durable_index == 0).
    let mut follower = RaftCore::new(nid(0), &member_ids(), Nanos(0), 7);
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
        nid(1),
        RaftMsg::AppendEntries {
            term: 1,
            leader: nid(1),
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
        drained_contains_member(&mut follower, 42),
        "a committed entry is applicable on a follower without its own fsync"
    );
}

/// A **leader** stays durability-gated: a freshly proposed command commits (sole
/// quorum reasoning aside, single-node commits immediately) but does **not**
/// become applicable until the leader fsyncs it. This is the ack-path guarantee
/// the refinement must preserve — proven here on a single-node group, exactly
/// the shape of the `persistence.rs` regression.
#[test]
fn leader_stays_durability_gated_on_its_own_proposal() {
    let mut leader = RaftCore::new(nid(0), &[nid(0)], Nanos(0), 7);
    leader.tick(Nanos(1_000_000_000), 7); // election timeout -> sole leader
    assert_eq!(leader.role(), Role::Leader);
    // Make the leader's initial no-op durable so it isn't what we're observing.
    let through = leader.last_log_index();
    let _ = leader.drain_persist();
    leader.mark_durable_through(through);
    let _ = leader.drain_apply(); // drain the no-op away

    leader.propose(upsert(42));
    assert!(
        leader.commit_index() > leader.durable_index(),
        "single-node commit is immediate, but it is not yet fsynced"
    );
    assert!(
        !drained_contains_member(&mut leader, 42),
        "the leader must NOT expose its own committed-but-unsynced proposal"
    );

    // Fsync: only now is it applicable (the ack-path durable-before-visible rule).
    let through = leader.last_log_index();
    let _ = leader.drain_persist();
    leader.mark_durable_through(through);
    assert!(
        drained_contains_member(&mut leader, 42),
        "after the fsync the leader's proposal is durable and applicable"
    );
}

/// A follower that applied up to commit while a follower, then **wins an
/// election**, keeps those entries (they are committed / quorum-durable) — and
/// its own future proposals are still durability-gated. `last_applied` only ever
/// moves forward across the role change.
#[test]
fn follower_to_leader_keeps_applied_then_gates_new_proposals() {
    let mut node = RaftCore::new(nid(0), &member_ids(), Nanos(0), 7);

    // As a follower, learn + commit an entry without any local fsync.
    node.handle(
        nid(1),
        RaftMsg::AppendEntries {
            term: 1,
            leader: nid(1),
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
        nid(1),
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
            nid(granter),
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
    let new_proposal_index = node.last_log_index();
    assert!(
        drained_contains_member(&mut node, 42),
        "a committed entry applied as a follower survives the role change"
    );
    assert!(
        node.last_applied() >= 1,
        "last_applied only moves forward across the follower->leader change"
    );

    // The leader's own new no-op (appended on becoming leader) is NOT visible
    // until the leader fsyncs it: new leader proposals stay durability-gated.
    assert!(
        new_proposal_index > node.durable_index(),
        "the leader's own fresh entry is ahead of its durable frontier"
    );
}

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = member_ids()
        .iter()
        .map(|id| {
            RaftNode::start(
                sim.env(id.clone()),
                member_ids().to_vec(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn unique_leader(nodes: &[RaftNode<SimEnv>], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

/// End-to-end over a `SimEnv` cluster: a leader's committed `MetaCommand` becomes
/// read-visible on **both followers** (their `metadata()` reflects it, once the
/// apply task on each node has caught up). The follower path applies on commit
/// — the realistic confirmation of the relaxation, on top of the precise
/// hand-driven core tests above.
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
            n.metadata().members.contains_key(&nid(77)),
            "node {i} (leader={leader}) did not reflect the committed command (seed={seed})"
        );
    }
}
