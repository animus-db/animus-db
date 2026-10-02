//! End-to-end node restart-and-rejoin: a node's process is stopped (its tasks
//! and volatile state die; its durable WAL survives), a fresh node is started on
//! the same disk, and the cluster converges with no loss of committed writes —
//! including writes made while the node was down.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{MetaCommand, NodeStatus, RaftNode};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

fn leaders(nodes: &[RaftNode<SimEnv>]) -> Vec<usize> {
    (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect()
}

#[test]
fn node_restarts_from_its_disk_and_rejoins() {
    let seed = 0x4E57;
    let sim = Simulator::new(seed);
    let mut sim = sim;
    // One system-keyspace engine per node, created once and kept alive across
    // the restart below — `MemoryEngine` clones share state (like a real
    // node's on-disk engine surviving a process restart), so re-cloning the
    // *same* handle at restart is what actually exercises "the engine
    // durably survives", not a fresh, empty one (ADR 0038 PR3).
    let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
    let mut nodes: Vec<RaftNode<SimEnv>> = NODES
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                engines[id as usize].clone(),
            )
        })
        .collect();

    sim.run_for(Duration::from_secs(2));
    let ls = leaders(&nodes);
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?}");
    let leader = ls[0];

    // Commit writes replicated to all three.
    for id in 0..5 {
        nodes[leader].propose(upsert(id));
    }
    sim.run_for(Duration::from_secs(2));
    let follower = (0..3).find(|&i| i != leader).unwrap();
    assert_eq!(
        nodes[follower].metadata().members.len(),
        5,
        "follower has the pre-stop writes"
    );

    // Stop the follower's process: tasks + volatile state gone, WAL on disk kept.
    sim.stop(nid(follower as u64));

    // The cluster keeps committing with the surviving majority while it is down.
    for id in 5..9 {
        nodes[leader].propose(upsert(id));
    }
    sim.run_for(Duration::from_secs(2));

    // Start a fresh node on the same node id / disk — it recovers from the WAL
    // *and* the same (durable) engine, exactly like a real restart.
    nodes[follower] = RaftNode::start(
        sim.env(nid(follower as u64)),
        NODES.iter().copied().map(nid).collect(),
        engines[follower].clone(),
    );
    sim.run_for(Duration::from_secs(3));

    // It rejoined and converged, including the writes made during its downtime.
    let reference = nodes[leader].metadata();
    assert_eq!(reference.members.len(), 9, "all writes committed");
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            n.metadata(),
            reference,
            "node {i} diverged after restart (seed={seed})"
        );
    }
    assert_eq!(leaders(&nodes).len(), 1, "exactly one leader after rejoin");
}
