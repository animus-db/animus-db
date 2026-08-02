//! ADR 0011 acceptance tests for **message retry / timeouts**.
//!
//! `Network::send` is fire-and-forget and may drop a message, which would
//! otherwise strand a transaction — a coordinator blocked on a quorum reply that
//! never arrives, or a replica that never learns the `Commit`. The driver's retry
//! tick ([`AccordCore::resend_pending`] on an `Env` timer) re-sends the
//! un-acknowledged protocol messages of any in-flight round until it completes.
//!
//! These tests inject a **lossy** network (an independent per-message drop
//! probability) and assert a transaction still commits and executes everywhere.
//! The whole run is byte-reproducible from its seed.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_consensus::{AccordNode, Key, TxnId};
use animus_sim::{NetConfig, SimEnv, Simulator};

const NODES: [u64; 3] = [0, 1, 2];

fn keys(ks: &[Key]) -> BTreeSet<Key> {
    ks.iter().copied().collect()
}

/// A 3-node cluster on a **lossy** link: every message is independently dropped
/// with probability `drop`, with the simulator's default delay/jitter on top.
fn lossy_cluster(seed: u64, drop: f64) -> (Simulator, Vec<AccordNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let mut cfg = NetConfig::default();
    cfg.set_drop_prob(drop);
    sim.set_net_config(cfg);
    let nodes = NODES
        .iter()
        .map(|&id| AccordNode::start(sim.env(id), NODES.to_vec()))
        .collect();
    (sim, nodes)
}

/// Under a lossy network a single submitted transaction still commits and
/// executes on every replica — the retry tick re-drives the dropped messages.
#[test]
fn transaction_commits_under_message_loss() {
    // A high drop rate so the first burst is very unlikely to all get through;
    // retries must carry the transaction to completion.
    let seed = 0x5E11_0001;
    let (mut sim, nodes) = lossy_cluster(seed, 0.4);

    let a = nodes[0].submit(keys(&[42]));
    // Generously long so several retry ticks (200ms apart) fire through the loss.
    sim.run_for(Duration::from_secs(30));

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(a),
            "node {i} never executed the transaction under message loss (seed={seed})"
        );
        assert_eq!(
            n.committed_execute_at(a),
            nodes[0].committed_execute_at(a),
            "node {i} committed at a different timestamp (seed={seed})"
        );
    }
}

/// Two conflicting transactions, submitted concurrently on a lossy link, both
/// commit and execute in a consistent order on every replica — retry keeps both
/// rounds alive through drops without breaking the safety property.
#[test]
fn conflicting_transactions_commit_under_loss() {
    let seed = 0x5E11_0002;
    let (mut sim, nodes) = lossy_cluster(seed, 0.3);

    let a = nodes[0].submit(keys(&[7]));
    let b = nodes[1].submit(keys(&[7]));
    sim.run_for(Duration::from_secs(40));

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(a) && n.is_applied(b),
            "node {i} did not execute both txns under loss (seed={seed})"
        );
    }

    // Consistent relative execution order on every replica.
    let order = |n: &AccordNode<SimEnv>| -> Vec<TxnId> {
        n.applied_order()
            .into_iter()
            .filter(|t| *t == a || *t == b)
            .collect()
    };
    let reference = order(&nodes[0]);
    assert_eq!(reference.len(), 2, "both must appear (seed={seed})");
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            order(n),
            reference,
            "node {i} diverged on execution order under loss (seed={seed})"
        );
    }
}

/// Retry is robust across many seeds (different drop/delay interleavings): a
/// transaction always reaches a consistent commit on every replica.
#[test]
fn retry_commits_across_seeds() {
    for seed in 0x5E11_1000..0x5E11_1018 {
        let (mut sim, nodes) = lossy_cluster(seed, 0.35);
        let a = nodes[0].submit(keys(&[3]));
        sim.run_for(Duration::from_secs(40));
        for (i, n) in nodes.iter().enumerate() {
            assert!(
                n.is_applied(a),
                "node {i} never executed under loss (seed={seed})"
            );
        }
    }
}

/// The lossy retry run is byte-reproducible from its seed (the retry timer is a
/// deterministic `Env` timer; the drop sampling draws from the seeded RNG).
#[test]
fn retry_run_is_reproducible_from_seed() {
    let seed = 0x5E11_0003;
    let trace = |seed| {
        let (mut sim, nodes) = lossy_cluster(seed, 0.3);
        nodes[0].submit(keys(&[1]));
        sim.run_for(Duration::from_secs(20));
        sim.trace_lines()
    };
    assert_eq!(trace(seed), trace(seed), "retry trace not reproducible");
}
