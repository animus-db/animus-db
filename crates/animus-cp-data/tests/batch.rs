//! Batch put (ADR 0017 — bulk-write batching): `KvCommand::Batch` commits many
//! keys as **one** Raft log entry (one propose → one commit round → one apply).
//! This test proves the batch applies all its keys on every replica, survives a
//! leader kill (the committed batch is durable on the surviving majority), and
//! re-applies idempotently on restart (WAL replay re-runs the one entry).
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

/// The synthetic batch: `n` distinct keys `bk{i}` → `bv{i}`.
fn batch(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..n)
        .map(|i| {
            (
                format!("bk{i:03}").into_bytes(),
                format!("bv{i:03}").into_bytes(),
            )
        })
        .collect()
}

/// Every replica's engine reflects every key of a batch committed as one entry.
fn assert_all_present(nodes: &[KvNode], live: &[usize], puts: &[(Vec<u8>, Vec<u8>)], seed: u64) {
    for &i in live {
        for (k, v) in puts {
            assert_eq!(
                block_on(nodes[i].local_get(k)),
                Some(v.clone()),
                "node {i} missing batch key {:?} (seed={seed})",
                String::from_utf8_lossy(k)
            );
        }
    }
}

#[test]
fn batch_commits_and_applies_on_every_replica() {
    let seed = 0xBA7C;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect

    let l = leader(&nodes, &[0, 1, 2], seed);
    let puts = batch(50);
    let index = match nodes[l].put_batch(puts.clone()) {
        ProposeResult::Accepted { index, .. } => index,
        other => panic!("leader rejected the batch: {other:?} (seed={seed})"),
    };
    sim.run_for(Duration::from_secs(2)); // replicate + apply

    // The whole batch is one entry: applying it advances applied to `index`.
    assert!(
        nodes[l].last_applied() >= index,
        "batch entry {index} not applied on leader (seed={seed})"
    );
    assert_all_present(&nodes, &[0, 1, 2], &puts, seed);
}

#[test]
fn batch_survives_a_leader_kill() {
    let seed = 0xB11E;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));

    let l0 = leader(&nodes, &[0, 1, 2], seed);
    let puts = batch(40);
    match nodes[l0].put_batch(puts.clone()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("batch rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2)); // commit on the majority

    // Kill the leader: isolate it from the other two, who must re-elect.
    let survivors: Vec<usize> = (0..3).filter(|&i| i != l0).collect();
    for &s in &survivors {
        sim.partition_pair(nid(l0 as u64), nid(s as u64));
    }
    sim.run_for(Duration::from_secs(3)); // survivors re-elect

    // The batch committed before the kill, so both survivors still hold every key,
    // and a fresh single write on the new leader also lands.
    assert_all_present(&nodes, &survivors, &puts, seed);
    let l1 = leader(&nodes, &survivors, seed);
    match nodes[l1].put(b"after".to_vec(), b"kill".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("post-kill write rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    for &s in &survivors {
        assert_eq!(
            block_on(nodes[s].local_get(b"after")),
            Some(b"kill".to_vec()),
            "survivor {s} missing the post-kill write (seed={seed})"
        );
    }

    // Heal the old leader; it rejoins and catches up to the full batch.
    for &s in &survivors {
        sim.heal(nid(l0 as u64), nid(s as u64));
    }
    sim.run_for(Duration::from_secs(3));
    assert_all_present(&nodes, &[l0], &puts, seed);
}

#[test]
fn batch_reapplies_idempotently_on_restart() {
    let seed = 0xB235;
    let sim = Simulator::new(seed);
    let mut nodes: Vec<KvNode> = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    let mut sim = sim;
    sim.run_for(Duration::from_secs(2));

    let l = leader(&nodes, &[0, 1, 2], seed);
    let puts = batch(30);
    match nodes[l].put_batch(puts.clone()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("batch rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    assert_all_present(&nodes, &[0, 1, 2], &puts, seed);

    // Stop node 0 (volatile engine dies, synced WAL survives), restart it with a
    // fresh engine: the driver replays the WAL, re-applying the one Batch entry —
    // idempotently, at the same MVCC index — so every key is recovered.
    sim.stop(nid(0));
    nodes[0] = RaftKvNode::start(
        sim.env(nid(0)),
        NODES.iter().copied().map(nid).collect(),
        MemoryEngine::new(),
    );
    sim.run_for(Duration::from_secs(4)); // recover + catch up

    assert_all_present(&nodes, &[0, 1, 2], &puts, seed);
}

#[test]
fn run_is_deterministic_from_seed() {
    let observe = |seed: u64| {
        let (mut sim, nodes) = group(seed);
        sim.run_for(Duration::from_secs(2));
        let l = leader(&nodes, &[0, 1, 2], seed);
        let _ = nodes[l].put_batch(batch(20));
        sim.run_for(Duration::from_secs(2));
        sim.trace_lines()
    };
    assert_eq!(
        observe(0x7),
        observe(0x7),
        "same seed must reproduce the trace"
    );
}
