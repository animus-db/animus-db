//! Log truncation + `InstallSnapshot`: when the leader compacts its log past a
//! follower that fell behind (e.g. was partitioned), it ships its snapshot to
//! bring the follower back up rather than replaying entries it no longer has.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use custos_control::raft::SNAPSHOT_CHUNK_BYTES;
use custos_control::{MetaCommand, NodeStatus, RaftCore, RaftMsg, RaftNode};
use custos_env::{Nanos, NodeId};
use custos_sim::{SimEnv, Simulator};

const NODES: [u64; 3] = [0, 1, 2];

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node,
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
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

#[test]
fn partitioned_follower_catches_up_via_install_snapshot() {
    let seed = 0x5A95;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, seed);
    let follower = (0..3).find(|&i| i != leader).unwrap() as u64;

    // Isolate the follower from the rest of the cluster.
    for &peer in &NODES {
        if peer != follower {
            sim.partition_pair(follower, peer);
        }
    }

    // Drive enough writes that the leader commits them (with the *other*
    // follower for majority) and compacts its log past the isolated follower.
    for i in 0..100 {
        nodes[leader].propose(upsert(i));
    }
    sim.run_for(Duration::from_secs(4));

    // The leader truncated its log; the isolated follower learned nothing.
    assert!(
        nodes[leader].snapshot_index() >= 60,
        "leader should have snapshotted/truncated, got {}",
        nodes[leader].snapshot_index()
    );
    assert_eq!(
        nodes[follower as usize].snapshot_index(),
        0,
        "isolated follower should be stuck with no snapshot"
    );

    // Heal the partition; the leader can no longer send the missing entries by
    // AppendEntries (they're compacted), so it must InstallSnapshot.
    for &peer in &NODES {
        if peer != follower {
            sim.heal(follower, peer);
        }
    }
    sim.run_for(Duration::from_secs(4));

    // The follower installed the snapshot (a base it never reached by applying)
    // and converged on the leader's state.
    assert!(
        nodes[follower as usize].snapshot_index() > 0,
        "follower never installed a snapshot"
    );
    assert_eq!(
        nodes[follower as usize].metadata(),
        nodes[leader].metadata(),
        "follower did not converge after InstallSnapshot (seed={seed})"
    );
    assert_eq!(nodes[follower as usize].metadata().members.len(), 100);
}

/// A far-behind follower catches up via a **multi-chunk** `InstallSnapshot`:
/// drives a leader and follower `RaftCore` (the deterministic state machine)
/// directly, asserting the transfer spans more than one offset-addressed chunk
/// and the follower converges on the leader's metadata.
///
/// Driving the cores rather than the full sim lets the test observe the wire
/// messages and count chunks unambiguously, while still exercising the real
/// chunk-production (leader) and reassembly (follower) paths.
#[test]
fn follower_catches_up_via_multi_chunk_snapshot() {
    const PAIR: [NodeId; 2] = [0, 1];
    let now = Nanos(1_000_000_000);

    // Elect node 0 leader of a two-node group: time out into a candidacy, then
    // feed it node 1's granted vote.
    let mut leader = RaftCore::new(0, &PAIR, Nanos(0), 7);
    let _ = leader.tick(now, 7); // election timeout -> Candidate, RequestVote
    let _ = leader.handle(
        1,
        RaftMsg::RequestVoteResp {
            term: leader.term(),
            granted: true,
        },
        now,
        7,
    );
    assert!(leader.is_leader(), "node 0 should have won the election");

    // Commit enough members that the serialized snapshot is several chunks long.
    // With node 1 acking, commit advances; then snapshot to compact the prefix.
    let n_members = 300u64;
    for i in 0..n_members {
        if let custos_control::ProposeResult::Accepted { index } = leader.propose(upsert(i)) {
            let _ = leader.handle(
                1,
                RaftMsg::AppendEntriesResp {
                    term: leader.term(),
                    success: true,
                    match_index: index,
                },
                now,
                7,
            );
        }
    }
    leader.snapshot();
    assert!(
        leader.snapshot_index() > 0,
        "leader should have a snapshot to ship"
    );
    let serialized_len = serde_json::to_vec(&leader.metadata()).unwrap().len();
    assert!(
        serialized_len > SNAPSHOT_CHUNK_BYTES,
        "snapshot ({serialized_len} bytes) must exceed one chunk ({SNAPSHOT_CHUNK_BYTES}) to \
         exercise multi-chunk transfer"
    );

    // Fresh follower; drive the chunk exchange to completion, counting the
    // distinct chunk offsets the leader sends.
    let mut follower = RaftCore::new(1, &PAIR, Nanos(0), 7);
    let mut offsets_sent: BTreeSet<u64> = BTreeSet::new();

    // Prime with a heartbeat. The fresh follower rejects the append (its log is
    // far behind), so the leader backtracks `next_index` until it falls below the
    // compacted snapshot base, then switches to shipping snapshot chunks — the
    // real lagging-follower catch-up path.
    let hb = Nanos(now.0 + 1_000_000_000); // past the heartbeat deadline
    let mut pending: Vec<(NodeId, RaftMsg)> = leader.tick(hb, 7);
    assert!(
        !pending.is_empty(),
        "heartbeat should emit a replication message"
    );
    // Pump messages back and forth until the leader stops emitting (transfer
    // done and follower caught up). Each round: leader -> follower -> leader.
    let mut steps = 0;
    while !pending.is_empty() {
        steps += 1;
        assert!(steps < 1000, "chunk exchange did not terminate");
        let mut next: Vec<(NodeId, RaftMsg)> = Vec::new();
        for (to, msg) in pending {
            if let RaftMsg::InstallSnapshot { offset, .. } = &msg {
                offsets_sent.insert(*offset);
            }
            // Deliver to the right core and collect its replies.
            let replies = if to == 1 {
                follower.handle(0, msg, now, 7)
            } else {
                leader.handle(1, msg, now, 7)
            };
            next.extend(replies);
        }
        pending = next;
    }

    assert!(
        offsets_sent.len() > 1,
        "expected a multi-chunk transfer, but only {} chunk offset(s) were sent: {offsets_sent:?}",
        offsets_sent.len()
    );
    assert_eq!(
        follower.metadata(),
        leader.metadata(),
        "follower did not converge on the leader's metadata after reassembly"
    );
    assert_eq!(follower.snapshot_index(), leader.snapshot_index());
    assert_eq!(follower.metadata().members.len() as u64, n_members);
}
