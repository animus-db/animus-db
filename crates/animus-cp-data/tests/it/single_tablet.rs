//! Stage B.1 (ADR 0017): a single-tablet Raft KV group over `SimEnv` elects a
//! leader, replicates writes through Raft, and applies them to every replica's
//! engine in the agreed order — including across a leader kill. Reads here are
//! the **local** engine reads (`local_get`); linearizable ReadIndex is B.2.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`, never
//! `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
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

/// Propose a put on whoever is leader, asserting it was accepted.
fn put(nodes: &[KvNode], live: &[usize], seed: u64, key: &[u8], value: &[u8]) {
    let l = leader(nodes, live, seed);
    match nodes[l].put(key.to_vec(), value.to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected a put: {other:?} (seed={seed})"),
    }
}

#[test]
fn writes_replicate_and_apply_on_every_replica() {
    let seed = 0xDA7A;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect

    put(&nodes, &[0, 1, 2], seed, b"k1", b"v1");
    put(&nodes, &[0, 1, 2], seed, b"k2", b"v2");
    sim.run_for(Duration::from_secs(2)); // replicate + apply

    // Every replica's engine reflects both writes (applied in the agreed order).
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"k1")),
            Some(b"v1".to_vec()),
            "node {i} missing k1 (seed={seed})"
        );
        assert_eq!(
            block_on(n.local_get(b"k2")),
            Some(b"v2".to_vec()),
            "node {i} missing k2 (seed={seed})"
        );
    }
}

#[test]
fn writes_survive_a_leader_kill() {
    let seed = 0x515E;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));

    put(&nodes, &[0, 1, 2], seed, b"key", b"before");
    sim.run_for(Duration::from_secs(2));

    // Kill the leader: isolate it from the other two, who must re-elect.
    let old = leader(&nodes, &[0, 1, 2], seed);
    let survivors: Vec<usize> = (0..3).filter(|&i| i != old).collect();
    for &s in &survivors {
        sim.partition_pair(nid(old as u64), nid(s as u64));
    }
    sim.run_for(Duration::from_secs(3)); // survivors re-elect

    // A write on the new leader commits on the surviving majority and applies.
    put(&nodes, &survivors, seed, b"key", b"after");
    sim.run_for(Duration::from_secs(2));
    for &s in &survivors {
        assert_eq!(
            block_on(nodes[s].local_get(b"key")),
            Some(b"after".to_vec()),
            "survivor {s} missing the post-kill write (seed={seed})"
        );
    }

    // Heal the partition; the old leader rejoins and catches up to `after`.
    for &s in &survivors {
        sim.heal(nid(old as u64), nid(s as u64));
    }
    sim.run_for(Duration::from_secs(3));
    assert_eq!(
        block_on(nodes[old].local_get(b"key")),
        Some(b"after".to_vec()),
        "rejoined old leader did not catch up (seed={seed})"
    );
}

#[test]
fn run_is_deterministic_from_seed() {
    let observe = |seed: u64| {
        let (mut sim, nodes) = group(seed);
        sim.run_for(Duration::from_secs(2));
        put(&nodes, &[0, 1, 2], seed, b"k", b"v");
        sim.run_for(Duration::from_secs(2));
        sim.trace_lines()
    };
    assert_eq!(
        observe(0x9),
        observe(0x9),
        "same seed must reproduce the trace"
    );
}

/// The **confirm-by-index** accessor (audit A4): `engine_applied_index()` is the
/// watermark the assembly layer checks against a `ProposeResult::Accepted`
/// index to confirm "my write is committed and merged into the engine", instead
/// of polling value equality (which false-negatives when a concurrent later
/// write to the same key overwrites the proposed value before the poll sees it).
#[test]
fn engine_applied_index_confirms_a_specific_proposal() {
    let seed = 0xA4C0;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    // Before any client write, the watermark sits below any future index.
    let base = nodes[l].engine_applied_index();

    let index = match nodes[l].put(b"confirm".to_vec(), b"v1".to_vec()) {
        animus_control::ProposeResult::Accepted { index, .. } => index,
        other => panic!("put not accepted: {other:?} (seed={seed})"),
    };
    assert!(
        index > base,
        "a fresh proposal's index is above the watermark"
    );
    // Immediately overwrite the same key — the value-equality poll this
    // accessor replaces would now never observe `v1`.
    assert!(matches!(
        nodes[l].put(b"confirm".to_vec(), b"v2".to_vec()),
        animus_control::ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    // The watermark confirms the *first* proposal applied even though its value
    // was long since overwritten.
    assert!(
        nodes[l].engine_applied_index() >= index,
        "engine watermark must cover the accepted index (seed={seed})"
    );
    assert!(nodes[l].is_leader(), "still leader in the proposal's term");
    // And the engine indeed serves the later value — equality-polling for v1
    // would have hung.
    assert_eq!(
        block_on(nodes[l].local_get(b"confirm")),
        Some(b"v2".to_vec())
    );
}
