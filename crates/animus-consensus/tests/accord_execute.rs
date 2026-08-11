//! ADR 0011 acceptance tests for the Accord *execution + durability* milestone.
//!
//! Under `SimEnv`:
//!
//! - Two conflicting transactions **execute** (apply) in the same order on every
//!   replica, and the executed store converges to the same key→last-writer
//!   mapping everywhere (consistent execution order, not just commit order).
//! - A replica whose process is stopped and restarted on the same disk recovers
//!   its committed/executed state from the WAL.
//!
//! The whole run is byte-reproducible from its seed.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_consensus::{AccordNode, Key, TxnId};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];

/// Read a replica's executed-store writer for `key`. The `MemoryEngine` behind
/// it awaits nothing real, so a plain `block_on` resolves it immediately.
fn store_writer(node: &AccordNode<SimEnv>, key: Key) -> Option<TxnId> {
    block_on(node.store_writer(key))
}

fn cluster(seed: u64) -> (Simulator, Vec<AccordNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| AccordNode::start(sim.env(nid(id)), NODES.iter().copied().map(nid).collect()))
        .collect();
    (sim, nodes)
}

fn keys(ks: &[Key]) -> BTreeSet<Key> {
    ks.iter().copied().collect()
}

/// Filter a replica's full applied order down to just `(a, b)`, giving the
/// relative execution order of the two transactions of interest.
fn relative_order(applied: &[TxnId], a: &TxnId, b: &TxnId) -> Vec<TxnId> {
    applied
        .iter()
        .filter(|t| *t == a || *t == b)
        .cloned()
        .collect()
}

/// Two conflicting transactions execute (apply) in the *same* order on every
/// replica, and the shared key ends up written by the same transaction on all
/// replicas — the consistent-execution-order property.
#[test]
fn conflicting_transactions_execute_in_consistent_order() {
    let seed = 0xE0EC_0001;
    let (mut sim, nodes) = cluster(seed);

    let a = nodes[0].submit(keys(&[7]));
    let b = nodes[1].submit(keys(&[7]));
    sim.run_for(Duration::from_secs(3));

    // Both executed on every replica.
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(a.clone()) && n.is_applied(b.clone()),
            "node {i} did not execute both txns (seed={seed})"
        );
    }

    // Every replica executed them in the same relative order.
    let order0 = relative_order(&nodes[0].applied_order(), &a, &b);
    assert_eq!(
        order0.len(),
        2,
        "both must appear in the order (seed={seed})"
    );
    for (i, n) in nodes.iter().enumerate() {
        let order = relative_order(&n.applied_order(), &a, &b);
        assert_eq!(
            order, order0,
            "node {i} executed conflicting txns in a different order (seed={seed})"
        );
    }

    // The shared key's last writer is the transaction executed *second* — and it
    // is identical on every replica (the executed store converged).
    let last = order0.last().unwrap().clone();
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            store_writer(n, 7),
            Some(last.clone()),
            "node {i} store diverged on the shared key (seed={seed})"
        );
    }
}

/// The execution order is consistent across many seeds (different interleavings)
/// and across a slow-path-inducing third coordinator.
#[test]
fn execution_order_consistent_across_seeds() {
    for seed in 0xE0EC_1000..0xE0EC_1030 {
        let (mut sim, nodes) = cluster(seed);
        let a = nodes[0].submit(keys(&[5, 6]));
        let b = nodes[1].submit(keys(&[6]));
        let c = nodes[2].submit(keys(&[6]));
        sim.run_for(Duration::from_secs(5));

        let reference = nodes[0].applied_order();
        let group = [a.clone(), b.clone(), c.clone()];
        for (i, n) in nodes.iter().enumerate() {
            for t in &group {
                assert!(
                    n.is_applied(t.clone()),
                    "node {i} missing an execution (seed={seed})"
                );
            }
            // Restricted to the three conflicting txns, the order is identical.
            let mut want: Vec<TxnId> = reference
                .iter()
                .filter(|t| group.contains(t))
                .cloned()
                .collect();
            let got: Vec<TxnId> = n
                .applied_order()
                .into_iter()
                .filter(|t| group.contains(t))
                .collect();
            want.dedup();
            assert_eq!(got, want, "node {i} execution order diverged (seed={seed})");
        }
        // The single shared key (6) has one final writer everywhere.
        let w = store_writer(&nodes[0], 6);
        assert!(w.is_some(), "shared key never written (seed={seed})");
        for (i, n) in nodes.iter().enumerate() {
            assert_eq!(
                store_writer(n, 6),
                w,
                "node {i} store diverged (seed={seed})"
            );
        }
    }
}

/// A replica's process is stopped (volatile state lost, durable WAL kept) and a
/// fresh node started on the same disk recovers its committed/executed state.
#[test]
fn replica_recovers_executed_state_from_disk() {
    let seed = 0xE0EC_0002;
    let (mut sim, mut nodes) = cluster(seed);

    // Commit + execute two conflicting transactions everywhere.
    let a = nodes[0].submit(keys(&[3]));
    let b = nodes[1].submit(keys(&[3]));
    sim.run_for(Duration::from_secs(3));

    // Capture node 2's executed view before the restart.
    let before_order = relative_order(&nodes[2].applied_order(), &a, &b);
    let before_writer = store_writer(&nodes[2], 3);
    assert_eq!(before_order.len(), 2, "node 2 executed both (seed={seed})");
    assert!(before_writer.is_some());
    assert!(nodes[2].is_applied(a.clone()) && nodes[2].is_applied(b.clone()));

    // Stop node 2's process: tasks + volatile state die; the WAL on disk stays.
    sim.stop(nid(2));

    // Start a fresh node on the same id/disk — it recovers from the WAL.
    nodes[2] = AccordNode::start(sim.env(nid(2)), NODES.iter().copied().map(nid).collect());
    sim.run_for(Duration::from_secs(2));

    // It recovered both transactions, their execution order, and the store.
    assert!(
        nodes[2].is_applied(a.clone()) && nodes[2].is_applied(b.clone()),
        "recovered node lost an executed txn (seed={seed})"
    );
    assert_eq!(
        relative_order(&nodes[2].applied_order(), &a, &b),
        before_order,
        "recovered node lost its execution order (seed={seed})"
    );
    assert_eq!(
        store_writer(&nodes[2], 3),
        before_writer,
        "recovered node lost its executed store (seed={seed})"
    );

    // It still agrees with a live replica.
    assert_eq!(
        store_writer(&nodes[2], 3),
        store_writer(&nodes[0], 3),
        "recovered node diverged from a live replica (seed={seed})"
    );
}

/// Replaying the same seed produces a byte-identical trace, including the
/// execution path.
#[test]
fn execution_run_is_reproducible_from_seed() {
    let seed = 0xE0EC_0003;
    let trace = |seed| {
        let (mut sim, nodes) = cluster(seed);
        nodes[0].submit(keys(&[1]));
        nodes[1].submit(keys(&[1]));
        sim.run_for(Duration::from_secs(3));
        sim.trace_lines()
    };
    assert_eq!(trace(seed), trace(seed), "execution trace not reproducible");
}
