//! Log truncation + `InstallSnapshot`: when the leader compacts its log past a
//! follower that fell behind (e.g. was partitioned), it ships its snapshot to
//! bring the follower back up rather than replaying entries it no longer has.

use std::collections::BTreeMap;
use std::time::Duration;

use custos_control::{MetaCommand, NodeStatus, RaftNode};
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
