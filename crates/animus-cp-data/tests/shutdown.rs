//! `RaftKvNode::shutdown` (drop-table GC, ADR 0024): a halted node's driver
//! exits and the node stops participating in its group — no more applies, no
//! more heartbeats — while the surviving replicas re-elect and keep serving.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`, never
//! `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| RaftKvNode::start(sim.env(id), NODES.to_vec(), MemoryEngine::new()))
        .collect();
    (sim, nodes)
}

fn leader(nodes: &[KvNode], live: &[usize], seed: u64) -> usize {
    let ls: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among {live:?}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

fn put(nodes: &[KvNode], live: &[usize], seed: u64, key: &[u8], value: &[u8]) {
    let l = leader(nodes, live, seed);
    match nodes[l].put(key.to_vec(), value.to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected a put: {other:?} (seed={seed})"),
    }
}

/// A shut-down **follower** stops applying: it acknowledges the exit
/// (`is_stopped`), and a write committed after its halt never reaches its
/// engine while the live majority still applies it.
#[test]
fn halted_follower_stops_applying() {
    let seed = 0xD0D0;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect

    put(&nodes, &[0, 1, 2], seed, b"k1", b"v1");
    sim.run_for(Duration::from_secs(2)); // replicate + apply everywhere

    let l = leader(&nodes, &[0, 1, 2], seed);
    let follower = (0..3).find(|&i| i != l).expect("a follower exists");
    assert!(!nodes[follower].is_stopped());
    nodes[follower].shutdown();
    assert!(
        nodes[follower].is_halted(),
        "shutdown must latch (seed={seed})"
    );
    sim.run_for(Duration::from_secs(2)); // driver observes the flag on its next wake
    assert!(
        nodes[follower].is_stopped(),
        "driver must exit after shutdown (seed={seed})"
    );

    put(&nodes, &[l], seed, b"k2", b"v2");
    sim.run_for(Duration::from_secs(2));

    assert_eq!(
        block_on(nodes[follower].local_get(b"k1")),
        Some(b"v1".to_vec()),
        "pre-halt state stays readable locally (seed={seed})"
    );
    assert_eq!(
        block_on(nodes[follower].local_get(b"k2")),
        None,
        "a halted follower must not apply post-halt writes (seed={seed})"
    );
    assert_eq!(
        block_on(nodes[l].local_get(b"k2")),
        Some(b"v2".to_vec()),
        "the live majority still commits + applies (seed={seed})"
    );
}

/// A shut-down **leader** stops heartbeating, so the survivors re-elect and the
/// group keeps accepting writes — a halted node cannot wedge its group.
#[test]
fn survivors_reelect_after_leader_shutdown() {
    let seed = 0x0FF1;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect

    let old = leader(&nodes, &[0, 1, 2], seed);
    nodes[old].shutdown();
    sim.run_for(Duration::from_secs(5)); // halt observed; survivors time out + re-elect

    assert!(
        nodes[old].is_stopped(),
        "old leader's driver exited (seed={seed})"
    );
    let live: Vec<usize> = (0..3).filter(|&i| i != old).collect();
    let new = leader(&nodes, &live, seed);
    put(&nodes, &[new], seed, b"k", b"v");
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        block_on(nodes[new].local_get(b"k")),
        Some(b"v".to_vec()),
        "the re-elected group must keep serving writes (seed={seed})"
    );
}
