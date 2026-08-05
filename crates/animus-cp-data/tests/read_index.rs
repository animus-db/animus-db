//! Stage B.2 (ADR 0017): **linearizable ReadIndex reads** for the per-tablet Raft
//! KV group. A read on the leader reflects every write committed before it; a
//! deposed (partitioned) leader cannot confirm a read quorum, so it returns
//! `None` rather than a stale value — no wall clock involved.
//!
//! Linearizable reads are async (a read-barrier probe round + applied wait), so
//! we drive them as spawned tasks and `run_for` to let the sim advance.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::EnvExt;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

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

/// Run a linearizable read on `node` to completion (spawned as a task, since it
/// awaits a quorum probe round), driving the sim up to `budget`.
fn lin_read(sim: &mut Simulator, node: &KvNode, key: &[u8], budget: Duration) -> Option<Vec<u8>> {
    let slot: Arc<Mutex<Option<Option<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let n = node.clone();
    let s = Arc::clone(&slot);
    let k = key.to_vec();
    node.env().clone().spawn_task(async move {
        let v = n.linearizable_get(&k).await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock()
        .unwrap()
        .clone()
        .expect("linearizable read did not complete")
}

fn put(nodes: &[KvNode], live: &[usize], seed: u64, key: &[u8], value: &[u8]) {
    let l = leader(nodes, live, seed);
    assert!(matches!(
        nodes[l].put(key.to_vec(), value.to_vec()),
        ProposeResult::Accepted { .. }
    ));
}

/// Run a linearizable range scan to completion (spawned, since it awaits a read
/// barrier), driving the sim up to `budget`.
#[allow(clippy::type_complexity)]
fn lin_scan(
    sim: &mut Simulator,
    node: &KvNode,
    start: &[u8],
    end: &[u8],
    budget: Duration,
) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    let slot: Arc<Mutex<Option<Option<Vec<(Vec<u8>, Vec<u8>)>>>>> = Arc::new(Mutex::new(None));
    let n = node.clone();
    let s = Arc::clone(&slot);
    let (lo, hi) = (start.to_vec(), end.to_vec());
    node.env().clone().spawn_task(async move {
        *s.lock().unwrap() = Some(n.linearizable_scan(&lo, Some(&hi), None).await);
    });
    sim.run_for(budget);
    slot.lock().unwrap().clone().expect("scan did not complete")
}

#[test]
fn linearizable_scan_returns_sorted_live_range() {
    let seed = 0x5CA4;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    // Write keys out of order; the scan must return them key-sorted.
    for (k, v) in [(b"k3", b"v3"), (b"k1", b"v1"), (b"k2", b"v2")] {
        put(&nodes, &[0, 1, 2], seed, k, v);
    }
    sim.run_for(Duration::from_secs(1));

    // Half-open [k1, k3): k1, k2 — not k3.
    assert_eq!(
        lin_scan(&mut sim, &nodes[l], b"k1", b"k3", Duration::from_secs(2)),
        Some(vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
        ]),
    );
    // Whole range covers all three, sorted.
    assert_eq!(
        lin_scan(&mut sim, &nodes[l], b"k0", b"k9", Duration::from_secs(2)),
        Some(vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
            (b"k3".to_vec(), b"v3".to_vec()),
        ]),
    );

    // A non-leader cannot serve a linearizable scan (no read barrier) → None.
    let follower = (0..3).find(|&i| i != l).unwrap();
    assert_eq!(
        lin_scan(
            &mut sim,
            &nodes[follower],
            b"k0",
            b"k9",
            Duration::from_secs(2)
        ),
        None,
    );
}

#[test]
fn linearizable_read_reflects_committed_writes() {
    let seed = 0x1EAD;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    put(&nodes, &[0, 1, 2], seed, b"x", b"1");
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        lin_read(&mut sim, &nodes[l], b"x", Duration::from_secs(1)),
        Some(b"1".to_vec()),
        "linearizable read must see the committed write (seed={seed})"
    );

    // Read-your-writes across an update.
    put(&nodes, &[0, 1, 2], seed, b"x", b"2");
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        lin_read(&mut sim, &nodes[l], b"x", Duration::from_secs(1)),
        Some(b"2".to_vec()),
    );

    // An absent key reads as None (not a phantom).
    assert_eq!(
        lin_read(&mut sim, &nodes[l], b"absent", Duration::from_secs(1)),
        None
    );
}

#[test]
fn deposed_leader_does_not_serve_a_stale_read() {
    let seed = 0xDEAD;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));

    put(&nodes, &[0, 1, 2], seed, b"x", b"old");
    sim.run_for(Duration::from_secs(1));

    // Partition the leader away; the survivors elect a new leader and accept a
    // newer write the old leader never sees.
    let old = leader(&nodes, &[0, 1, 2], seed);
    let survivors: Vec<usize> = (0..3).filter(|&i| i != old).collect();
    for &s in &survivors {
        sim.partition_pair(old as u64, s as u64);
    }
    sim.run_for(Duration::from_secs(3));
    put(&nodes, &survivors, seed, b"x", b"new");
    sim.run_for(Duration::from_secs(2));

    // The isolated old leader still *believes* it leads its (stale) term, but a
    // linearizable read cannot collect a quorum ack — so it returns None, never
    // the stale "old". (Read timeout is 5s; give it room to expire.)
    let stale = lin_read(&mut sim, &nodes[old], b"x", Duration::from_secs(7));
    assert_eq!(
        stale, None,
        "a deposed leader must not serve a stale linearizable read (seed={seed})"
    );

    // The new leader serves the up-to-date value.
    let new = leader(&nodes, &survivors, seed);
    assert_eq!(
        lin_read(&mut sim, &nodes[new], b"x", Duration::from_secs(1)),
        Some(b"new".to_vec()),
    );
}
