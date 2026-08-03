//! Stage D (ADR 0017): **tablet split**. A running group serving the whole key
//! space splits at a key: the split point is agreed through the Raft log (a
//! committed `Split` command, so every replica splits consistently), each replica
//! tombstones the handed-off upper range, and that range is seeded into a new,
//! independent Raft group. Afterwards the two groups serve disjoint ranges.
//!
//! Full *in-band* new-group creation (every original replica spawning its
//! new-tablet replica on apply) needs an `Env`-seam extension to mint a sibling
//! inbox at runtime; here the harness creates the new group from the leader's
//! handoff snapshot (standing in for the control plane / a tablet host), exactly
//! as the automatic membership trigger is integration plumbing in Stage C.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// Original tablet group ids and the new tablet's group ids (one new inbox per
/// physical node — distinct from the originals, the single-consumer rule).
const ORIG: [u64; 3] = [0, 1, 2];
const NEW: [u64; 3] = [10, 11, 12];

fn leader(nodes: &[KvNode], seed: u64) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(_, n)| n.is_leader())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
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

#[test]
fn split_hands_the_upper_range_to_a_new_group() {
    let seed = 0x5;
    let mut sim = Simulator::new(seed);
    let orig: Vec<KvNode> = ORIG
        .iter()
        .map(|&id| RaftKvNode::start(sim.env(id), ORIG.to_vec(), MemoryEngine::new()))
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l = leader(&orig, seed);

    // Populate the whole key space: k00..k19.
    for i in 0..20u32 {
        put(
            &orig[l],
            format!("k{i:02}").as_bytes(),
            format!("v{i}").as_bytes(),
            seed,
        );
    }
    sim.run_for(Duration::from_secs(2));

    // Split at "k10": keys >= k10 go to the new tablet. Capture the handoff (the
    // leader's committed [k10, ∞) data) BEFORE proposing the split.
    let split_at = b"k10".to_vec();
    let handoff = block_on(orig[l].range_snapshot(&split_at));
    assert_eq!(handoff.len(), 10, "handoff covers k10..k19");

    // Bring up the new group, seeded with the handed-off range.
    let new: Vec<KvNode> = NEW
        .iter()
        .map(|&id| {
            block_on(RaftKvNode::start_seeded(
                sim.env(id),
                NEW.to_vec(),
                MemoryEngine::new(),
                handoff.clone(),
            ))
        })
        .collect();

    // Commit the split on the original group (every replica tombstones [k10, ∞)).
    assert!(matches!(
        orig[l].propose_split(split_at.clone()),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(3)); // commit split on orig + elect new-group leader

    // The original now serves only [k00, k10): it kept the lower range and dropped
    // the upper one on every replica.
    for n in &orig {
        assert_eq!(
            block_on(n.local_get(b"k05")),
            Some(b"v5".to_vec()),
            "orig kept lower range"
        );
        assert_eq!(
            block_on(n.local_get(b"k15")),
            None,
            "orig dropped the handed-off range"
        );
    }

    // The new group serves [k10, ∞): seeded data is present on every replica.
    for n in &new {
        assert_eq!(
            block_on(n.local_get(b"k15")),
            Some(b"v15".to_vec()),
            "new group has the handed-off range"
        );
        assert_eq!(
            block_on(n.local_get(b"k05")),
            None,
            "new group does not hold the lower range"
        );
    }

    // Both groups operate independently afterward: a new write into each range
    // lands on the owning group only.
    let nl = leader(&new, seed);
    put(&new[nl], b"k17", b"v17new", seed);
    put(&orig[l], b"k03", b"v3new", seed);
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        block_on(new[nl].local_get(b"k17")),
        Some(b"v17new".to_vec())
    );
    assert_eq!(block_on(orig[l].local_get(b"k03")), Some(b"v3new".to_vec()));
}

#[test]
fn run_is_deterministic_from_seed() {
    let observe = |seed: u64| {
        let mut sim = Simulator::new(seed);
        let orig: Vec<KvNode> = ORIG
            .iter()
            .map(|&id| RaftKvNode::start(sim.env(id), ORIG.to_vec(), MemoryEngine::new()))
            .collect();
        sim.run_for(Duration::from_secs(2));
        let l = leader(&orig, seed);
        for i in 0..10u32 {
            put(&orig[l], format!("k{i:02}").as_bytes(), b"v", seed);
        }
        sim.run_for(Duration::from_secs(1));
        let _ = orig[l].propose_split(b"k05".to_vec());
        sim.run_for(Duration::from_secs(2));
        sim.trace_lines()
    };
    assert_eq!(observe(0x9), observe(0x9), "same seed reproduces the trace");
}
