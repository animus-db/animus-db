//! ADR 0018 §2 amendment (PR2): the HLC **witnessing chain** is what keeps a
//! group's applied `ts` strictly increasing across the two events that would
//! otherwise let it regress — a leader change (a fresh `Hlc` instance takes
//! over minting) and a process restart (a fresh `Hlc` instance is
//! constructed from scratch). Both are covered structurally by
//! `apply_and_compact`'s hard `assert_ts_monotonic` check (a real panic, not
//! a soft failure, if the chain is ever broken); these tests additionally
//! prove the *positive* property (versions genuinely strictly increase
//! across the event), not just "no panic happened."
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::hlc;
use animus_cp_data::{KIND_BASE, RaftKvNode};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
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

fn put(nodes: &[KvNode], live: &[usize], seed: u64, key: &[u8], value: &[u8]) {
    let l = leader(nodes, live, seed);
    match nodes[l].put(key.to_vec(), value.to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected a put: {other:?} (seed={seed})"),
    }
}

/// The packed MVCC version `storage.get(key)` records for `key` on `node`.
/// Addresses the engine through `physical_key` rather than assuming a layout:
/// since ADR 0041 §3 even a `StorageScope::whole()` group prefixes its rows
/// with a row-kind byte.
fn version_of(node: &KvNode, key: &[u8]) -> u64 {
    block_on(node.storage().get(&node.physical_key(KIND_BASE, key)))
        .expect("engine read ok")
        .unwrap_or_else(|| panic!("key {key:?} missing"))
        .version
}

#[test]
fn leader_change_keeps_applied_ts_strictly_increasing() {
    let seed = 0x517E_5540;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect

    // Pre-kill writes through the original leader.
    put(&nodes, &[0, 1, 2], seed, b"a0", b"v");
    put(&nodes, &[0, 1, 2], seed, b"a1", b"v");
    sim.run_for(Duration::from_secs(2));

    // Kill the leader (partition it away); the survivors re-elect — a fresh
    // `Hlc` instance (the new leader's own) starts minting. Witnessing
    // (every `AppendEntries` this node has received/sent as a follower, plus
    // its own group-start witness off the shared... — here each node has its
    // OWN engine, so group-start witnessing only covers this node's own
    // prior history, and the causal guarantee instead comes from the new
    // leader having witnessed the old leader's own entries as a follower
    // before ever campaigning) must still keep the sequence increasing.
    let old = leader(&nodes, &[0, 1, 2], seed);
    let survivors: Vec<usize> = (0..3).filter(|&i| i != old).collect();
    for &s in &survivors {
        sim.partition_pair(nid(old as u64), nid(s as u64));
    }
    sim.run_for(Duration::from_secs(3)); // survivors re-elect

    // Post-kill writes through the new leader.
    put(&nodes, &survivors, seed, b"b0", b"v");
    put(&nodes, &survivors, seed, b"b1", b"v");
    sim.run_for(Duration::from_secs(2));

    // Every surviving replica's own applied versions strictly increase in
    // the order the keys were proposed — across the leader change. Had the
    // witnessing chain been broken, `assert_ts_monotonic` would have
    // panicked inside the apply task well before this point; this is the
    // positive check that the sequence is genuinely ordered, not merely
    // "didn't crash."
    for &s in &survivors {
        let va0 = version_of(&nodes[s], b"a0");
        let va1 = version_of(&nodes[s], b"a1");
        let vb0 = version_of(&nodes[s], b"b0");
        let vb1 = version_of(&nodes[s], b"b1");
        assert!(
            va0 < va1 && va1 < vb0 && vb0 < vb1,
            "replica {s}: versions must strictly increase across the leader \
             change (a0={va0} a1={va1} b0={vb0} b1={vb1}, seed={seed})"
        );
    }
}

#[test]
fn restart_recovery_rewitnesses_and_the_first_post_recovery_mint_exceeds_everything_recovered() {
    let seed = 0x02E5_7A27;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let id = nid(0);

    let node: KvNode = RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], engine.clone());
    sim.run_for(Duration::from_secs(2)); // elect (single voter)

    match node.put(b"pre".to_vec(), b"v1".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected a put: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    let pre_version = version_of(&node, b"pre");
    let pre_ts = hlc::unpack(pre_version);

    // A genuine process restart (not `crash`/`restart`, which only toggle
    // network reachability without killing tasks — `stop` actually drops
    // this node's tasks, mirroring `reconciler_corpus.rs`'s `crash_restart`
    // helper): its own WAL survives on the same `Env` disk, and the SAME
    // `MemoryEngine` handle stands in for a durable engine surviving a real
    // process crash.
    sim.stop(id.clone());
    let restarted: KvNode =
        RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], engine.clone());
    sim.run_for(Duration::from_secs(2)); // WAL recovery + re-election

    match restarted.put(b"post".to_vec(), b"v2".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("restarted leader rejected a put: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    let post_version = version_of(&restarted, b"post");
    let post_ts = hlc::unpack(post_version);

    assert!(
        post_ts > pre_ts,
        "the first write proposed after restart must be timestamped strictly \
         past everything this node recovered — pre={pre_ts:?} post={post_ts:?} \
         (seed={seed})"
    );
}
