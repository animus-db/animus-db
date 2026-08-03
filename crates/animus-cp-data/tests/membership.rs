//! Stage C (ADR 0017): **single-server Raft membership change** for the per-tablet
//! KV group — the primitive the control plane drives to reconfigure on a node
//! failure (move a replica to a spare) or to grow a group. Membership lives in the
//! Raft log (config entries); a node uses the latest log config for all quorum and
//! election decisions, so single-server changes are safe without joint consensus.
//!
//! Deterministic + seed-reproducible (drive with `run_for`, never `run()`).

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// Bring up a group over node ids `ids` (each its own engine).
fn group(seed: u64, ids: &[u64]) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = ids
        .iter()
        .map(|&id| RaftKvNode::start(sim.env(id), ids.to_vec(), MemoryEngine::new()))
        .collect();
    (sim, nodes)
}

fn leader(nodes: &[KvNode], live: &[usize], seed: u64) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(i, n)| live.contains(i) && n.is_leader())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among {live:?}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

fn put(node: &KvNode, key: &[u8], value: &[u8], seed: u64) {
    assert!(
        matches!(
            node.put(key.to_vec(), value.to_vec()),
            ProposeResult::Accepted { .. }
        ),
        "leader rejected a put (seed={seed})"
    );
}

fn set(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}

#[test]
fn remove_a_follower_shrinks_the_quorum() {
    let seed = 0xC0DE;
    let ids = [0u64, 1, 2, 3];
    let (mut sim, nodes) = group(seed, &ids);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2, 3], seed);
    put(&nodes[l], b"k", b"v", seed);
    sim.run_for(Duration::from_secs(1));

    // Remove a follower: 4 voters -> 3.
    let victim = (0..4).find(|&i| i != l).expect("a follower");
    let remaining: Vec<u64> = ids
        .iter()
        .copied()
        .filter(|&n| n != victim as u64)
        .collect();
    assert!(matches!(
        nodes[l].change_membership(set(&remaining)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    // The config shrank everywhere it matters, and the group still serves writes
    // on the new 3-voter majority.
    assert_eq!(
        nodes[l].config(),
        set(&remaining),
        "leader adopted the new config"
    );
    put(&nodes[l], b"k2", b"v2", seed);
    sim.run_for(Duration::from_secs(2));
    for &i in &remaining {
        let n = &nodes[i as usize];
        assert_eq!(
            block_on(n.local_get(b"k2")),
            Some(b"v2".to_vec()),
            "node {i} missing post-reconfig write"
        );
        assert_eq!(
            n.config(),
            set(&remaining),
            "node {i} adopted the new config"
        );
    }
}

#[test]
fn add_a_node_grows_the_group_and_catches_it_up() {
    let seed = 0xADD;
    // Start the 4th node from the outset (own engine) but reconfigure 3 -> 4.
    let ids = [0u64, 1, 2, 3];
    let (mut sim, nodes) = group(seed, &[0, 1, 2]); // only {0,1,2} are voters initially
    // Bring up node 3 as a (currently non-member) replica that will be added.
    let node3 = RaftKvNode::start(sim.env(3), ids.to_vec(), MemoryEngine::new());
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);
    put(&nodes[l], b"k", b"v", seed);
    sim.run_for(Duration::from_secs(1));

    // Add node 3: 3 voters -> 4.
    assert!(matches!(
        nodes[l].change_membership(set(&ids)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(3));

    // Node 3 joined the config and caught up to the data.
    assert_eq!(
        node3.config(),
        set(&ids),
        "added node adopted the new config"
    );
    assert_eq!(
        block_on(node3.local_get(b"k")),
        Some(b"v".to_vec()),
        "added node caught up"
    );

    // The grown group still serves writes (now needs a 3-of-4 majority).
    let l2 = leader(&nodes, &[0, 1, 2], seed);
    put(&nodes[l2], b"k2", b"v2", seed);
    sim.run_for(Duration::from_secs(2));
    assert_eq!(block_on(node3.local_get(b"k2")), Some(b"v2".to_vec()));
}

#[test]
fn reconfigure_off_a_failed_node() {
    // The control-plane scenario: a follower goes down; the group reconfigures it
    // out so the survivors form a clean, smaller quorum and keep serving.
    let seed = 0xF00D;
    let ids = [0u64, 1, 2, 3];
    let (mut sim, nodes) = group(seed, &ids);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2, 3], seed);
    put(&nodes[l], b"k", b"v", seed);
    sim.run_for(Duration::from_secs(1));

    // A follower dies.
    let dead = (0..4).find(|&i| i != l).expect("a follower");
    sim.crash(dead as u64);

    // The leader reconfigures the dead node out (what the failure detector +
    // placement reconciler would drive): 4 voters -> 3.
    let survivors: Vec<u64> = ids.iter().copied().filter(|&n| n != dead as u64).collect();
    assert!(matches!(
        nodes[l].change_membership(set(&survivors)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    // The surviving 3-voter group commits + applies new writes (the dead node is
    // no longer needed for a quorum).
    put(&nodes[l], b"k2", b"v2", seed);
    sim.run_for(Duration::from_secs(2));
    for &i in &survivors {
        assert_eq!(
            block_on(nodes[i as usize].local_get(b"k2")),
            Some(b"v2".to_vec()),
            "survivor {i} missing write"
        );
    }
}

#[test]
fn rejects_multi_server_and_self_removal() {
    let seed = 0xBAD;
    let ids = [0u64, 1, 2];
    let (mut sim, nodes) = group(seed, &ids);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    // Multi-server delta ({0,1,2} -> {0}) is rejected (would risk disjoint majorities).
    assert!(matches!(
        nodes[l].change_membership(set(&[0])),
        ProposeResult::NotLeader { .. }
    ));
    // Removing the leader itself is rejected (transfer leadership first).
    let others: Vec<u64> = ids.iter().copied().filter(|&n| n != l as u64).collect();
    assert!(matches!(
        nodes[l].change_membership(set(&others)),
        ProposeResult::NotLeader { .. }
    ));
    // Config is unchanged after the rejected attempts.
    assert_eq!(nodes[l].config(), set(&ids));
}

#[test]
fn run_is_deterministic_from_seed() {
    let observe = |seed: u64| {
        let ids = [0u64, 1, 2, 3];
        let (mut sim, nodes) = group(seed, &ids);
        sim.run_for(Duration::from_secs(2));
        let l = leader(&nodes, &[0, 1, 2, 3], seed);
        let victim = (0..4).find(|&i| i != l).unwrap();
        let remaining: Vec<u64> = ids
            .iter()
            .copied()
            .filter(|&n| n != victim as u64)
            .collect();
        let _ = nodes[l].change_membership(set(&remaining));
        sim.run_for(Duration::from_secs(2));
        sim.trace_lines()
    };
    assert_eq!(
        observe(0x33),
        observe(0x33),
        "same seed reproduces the trace"
    );
}
