//! ADR 0026 Stage B (PR4 of the single-command-split redesign): `RaftKvNode`
//! sends/recvs on `(peer, stream)` instead of a peer's default inbox, so a
//! tablet group's member identity can be `(base_node_id, tablet_id)` on a
//! node's *existing* env — no `Coresident::sibling`, no derived `NodeId`. This
//! is the mechanism that replaces minting a brand-new node id per co-resident
//! tablet (the old `cp_member_id = base + tablet * STRIDE` scheme).
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`, never
//! `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
const STREAM_A: u64 = 100;
const STREAM_B: u64 = 200;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// A 3-node group hosted on `stream`, over the **same** `NODES` id set every
/// other group in this file also uses — proving isolation comes from the
/// stream axis alone, not from distinct node ids.
fn hosted_group(sim: &Simulator, stream: u64) -> Vec<KvNode> {
    NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start_hosted(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                StorageScope::whole(),
                stream,
            )
        })
        .collect()
}

fn leader(nodes: &[KvNode], seed: u64, which: &str) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader in group {which}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

fn put(nodes: &[KvNode], l: usize, seed: u64, key: &[u8], value: &[u8]) {
    match nodes[l].put(key.to_vec(), value.to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected a put: {other:?} (seed={seed})"),
    }
}

/// Two independent 3-node groups **sharing every node id** (`[0, 1, 2]`) but
/// addressed on distinct streams must each elect their own leader and
/// replicate their own writes with zero cross-talk — the same property
/// `Coresident::sibling` (a distinct `NodeId` per co-resident tablet) used to
/// provide, now delivered by the stream axis instead.
#[test]
fn two_groups_on_identical_node_ids_do_not_cross_talk_via_distinct_streams() {
    let seed = 0x57EA;
    let mut sim = Simulator::new(seed);
    let group_a = hosted_group(&sim, STREAM_A);
    let group_b = hosted_group(&sim, STREAM_B);
    sim.run_for(Duration::from_secs(2));

    let la = leader(&group_a, seed, "A");
    let lb = leader(&group_b, seed, "B");
    put(&group_a, la, seed, b"k", b"A-value");
    put(&group_b, lb, seed, b"k", b"B-value");
    sim.run_for(Duration::from_secs(2));

    for (i, n) in group_a.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"k")),
            Some(b"A-value".to_vec()),
            "group A node {i} (same node id as group B's node {i}) saw a \
             cross-talked write (seed={seed})"
        );
    }
    for (i, n) in group_b.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"k")),
            Some(b"B-value".to_vec()),
            "group B node {i} (same node id as group A's node {i}) saw a \
             cross-talked write (seed={seed})"
        );
    }
}

/// Sustained, interleaved writes into both groups (continuous heartbeats +
/// commits racing on identical node ids) still converge to exactly each
/// group's own values — the kind of workload where any real cross-stream
/// message bleed-through would eventually corrupt one group's state — and the
/// whole run reproduces byte-for-byte from its seed (ADR 0003).
#[test]
fn sustained_interleaved_writes_stay_isolated_and_reproducible() {
    let observe = |seed: u64| {
        let mut sim = Simulator::new(seed);
        let group_a = hosted_group(&sim, STREAM_A);
        let group_b = hosted_group(&sim, STREAM_B);
        sim.run_for(Duration::from_secs(2));
        let la = leader(&group_a, seed, "A");
        let lb = leader(&group_b, seed, "B");
        for i in 0..20u64 {
            put(
                &group_a,
                la,
                seed,
                format!("a{i}").as_bytes(),
                format!("va{i}").as_bytes(),
            );
            put(
                &group_b,
                lb,
                seed,
                format!("b{i}").as_bytes(),
                format!("vb{i}").as_bytes(),
            );
        }
        sim.run_for(Duration::from_secs(3));
        let a_ok = (0..20u64).all(|i| {
            block_on(group_a[0].local_get(format!("a{i}").as_bytes()))
                == Some(format!("va{i}").into_bytes())
        });
        let b_ok = (0..20u64).all(|i| {
            block_on(group_b[0].local_get(format!("b{i}").as_bytes()))
                == Some(format!("vb{i}").into_bytes())
        });
        (a_ok, b_ok, sim.trace_lines())
    };

    let (a_ok, b_ok, _) = observe(0x57EC);
    assert!(
        a_ok,
        "group A must converge to exactly its own writes under interleaved dual-group load"
    );
    assert!(
        b_ok,
        "group B must converge to exactly its own writes under interleaved dual-group load"
    );

    let (_, _, t1) = observe(0x57ED);
    let (_, _, t2) = observe(0x57ED);
    assert_eq!(t1, t2, "same seed must reproduce the trace");
}
