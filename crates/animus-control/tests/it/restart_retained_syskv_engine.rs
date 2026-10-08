//! Production-shape regression: a control node restarted on its RETAINED
//! system-keyspace engine over a WAL compacted past a snapshot converges to
//! the leader's full `Metadata`. (A fresh engine over a retained compacted
//! WAL is an unsupported state that silently loses pre-snapshot entries; it
//! was `SimCluster::restart`'s fixture bug, now fixed.)

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

/// `durable_engine`: restart on the retained engine (production shape) vs a
/// fresh one (SimCluster Memory-backend shape). Returns (restarted, leader).
fn run(seed: u64, entries: u64, durable_engine: bool) -> (usize, usize, u64, u64) {
    let mut sim = Simulator::new(seed);
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
    let leader = (0..3).find(|&i| nodes[i].is_leader()).expect("leader");
    let victim = (0..3).find(|&i| i != leader).unwrap();

    // Enough entries to cross SNAPSHOT_THRESHOLD (64) several times.
    for id in 0..entries {
        nodes[leader].propose(upsert(id));
        if id % 20 == 19 {
            sim.run_for(Duration::from_millis(300));
        }
    }
    sim.run_for(Duration::from_secs(3));
    sim.stop(nid(victim as u64));
    sim.run_for(Duration::from_secs(1));

    let engine = if durable_engine {
        engines[victim].clone()
    } else {
        MemoryEngine::new()
    };
    nodes[victim] = RaftNode::start(
        sim.env(nid(victim as u64)),
        NODES.iter().copied().map(nid).collect(),
        engine,
    );
    sim.run_for(Duration::from_secs(5));
    let lead_now = (0..3)
        .find(|&i| i != victim && nodes[i].is_leader())
        .unwrap_or(leader);
    (
        nodes[victim].metadata().members.len(),
        nodes[lead_now].metadata().members.len(),
        nodes[victim].engine_applied_index(),
        nodes[lead_now].engine_applied_index(),
    )
}

#[test]
fn restart_with_retained_syskv_engine_converges_past_compaction() {
    let seed = 0xC17;
    let (v, l, va, la) = run(seed, 300, true);
    assert_eq!(
        v, l,
        "durable-engine restart diverged (seed={seed}, applied {va} vs {la})"
    );
}
