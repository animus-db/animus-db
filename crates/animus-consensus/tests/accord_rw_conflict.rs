//! ADR 0011 acceptance tests for **folding a read set into dependency tracking**
//! (the read-then-write hazard).
//!
//! A read-modify-write transaction (`submit_rw`, the entry point
//! `InteractiveTxn::commit` uses) declares a **conflict set = reads ∪ writes**.
//! So a key the transaction merely *read* participates in ordering exactly like
//! one it writes: a concurrent write to a key this transaction read is ordered
//! relative to it, and the two carry each other as dependencies. That is what
//! lets an interactive read-modify-write be serialized correctly against a
//! conflicting write to a key it depended on.
//!
//! These run a 3-node Accord cluster under `SimEnv`; no data plane is needed to
//! prove the *ordering* property (the core decides order). The whole run is
//! byte-reproducible from its seed.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_consensus::{AccordNode, Key, TxnId};
use animus_sim::{SimEnv, Simulator};

const NODES: [u64; 3] = [0, 1, 2];

fn keys(ks: &[Key]) -> BTreeSet<Key> {
    ks.iter().copied().collect()
}

fn cluster(seed: u64) -> (Simulator, Vec<AccordNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| AccordNode::start(sim.env(id), NODES.to_vec()))
        .collect();
    (sim, nodes)
}

/// The relative execution order of `a` and `b` on a node.
fn order_on(n: &AccordNode<SimEnv>, a: TxnId, b: TxnId) -> Vec<TxnId> {
    n.applied_order()
        .into_iter()
        .filter(|t| *t == a || *t == b)
        .collect()
}

/// A transaction that **reads** key `X` and writes key `Y`, run concurrently
/// with a transaction that **writes** key `X`, must be ordered consistently on
/// every replica: they conflict on `X` (the read participates), so they cannot
/// execute in different relative orders on different replicas, and the
/// second-ordered one carries the first as a dependency.
#[test]
fn read_then_write_hazard_is_ordered_consistently() {
    let seed = 0x5707_0001;
    let (mut sim, nodes) = cluster(seed);

    // rw reads X=1, writes Y=2. w writes X=1. They conflict on the read key X.
    let rw = nodes[0].submit_rw(keys(&[1]), keys(&[2]));
    let w = nodes[1].submit(keys(&[1]));
    sim.run_for(Duration::from_secs(5));

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(rw) && n.is_applied(w),
            "node {i} did not execute both txns (seed={seed})"
        );
    }

    // Consistent relative order on every replica — the read of X made rw conflict
    // with w even though rw never writes X.
    let reference = order_on(&nodes[0], rw, w);
    assert_eq!(reference.len(), 2, "both must execute (seed={seed})");
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            order_on(n, rw, w),
            reference,
            "node {i} diverged on the read/write conflict order (seed={seed})"
        );
    }

    // The conflict was recorded as a dependency: the second-ordered transaction
    // carries the first in its committed deps.
    let first = reference[0];
    let second = reference[1];
    let deps = nodes[0]
        .committed_deps(second)
        .expect("second txn committed");
    assert!(
        deps.contains(&first),
        "the read/write conflict was not recorded as a dependency \
         (second={second:?} deps={deps:?}, first={first:?}, seed={seed})"
    );
}

/// Control: with the read set *dropped* (a plain write transaction over only its
/// write key), the two transactions are **disjoint** and need not be ordered —
/// confirming it is the folded read set, not some incidental conflict, that
/// orders the hazard above. Here rw writes only Y; w writes only X; they share no
/// key, so neither depends on the other.
#[test]
fn without_the_read_the_txns_are_disjoint() {
    let seed = 0x5707_0002;
    let (mut sim, nodes) = cluster(seed);

    // rw writes only Y=2 (no read of X); w writes X=1. Disjoint key sets.
    let rw = nodes[0].submit(keys(&[2]));
    let w = nodes[1].submit(keys(&[1]));
    sim.run_for(Duration::from_secs(5));

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(rw) && n.is_applied(w),
            "node {i} did not execute both txns (seed={seed})"
        );
    }

    // Disjoint: neither carries the other as a dependency.
    let rw_deps = nodes[0].committed_deps(rw).expect("rw committed");
    let w_deps = nodes[0].committed_deps(w).expect("w committed");
    assert!(
        !rw_deps.contains(&w) && !w_deps.contains(&rw),
        "disjoint transactions must not depend on each other (seed={seed})"
    );
}

/// The read/write conflict is consistently ordered across a seed sweep (different
/// delay/arrival interleavings).
#[test]
fn read_then_write_hazard_consistent_across_seeds() {
    for seed in 0x5707_1000..0x5707_1020 {
        let (mut sim, nodes) = cluster(seed);
        let rw = nodes[0].submit_rw(keys(&[10]), keys(&[20]));
        let w = nodes[2].submit(keys(&[10]));
        sim.run_for(Duration::from_secs(6));

        let reference = order_on(&nodes[0], rw, w);
        assert_eq!(reference.len(), 2, "both must execute (seed={seed})");
        for (i, n) in nodes.iter().enumerate() {
            assert!(n.is_applied(rw) && n.is_applied(w));
            assert_eq!(
                order_on(n, rw, w),
                reference,
                "node {i} diverged on read/write order (seed={seed})"
            );
        }
    }
}

/// The read-then-write run is byte-reproducible from its seed.
#[test]
fn rw_conflict_run_is_reproducible_from_seed() {
    let seed = 0x5707_0003;
    let trace = |seed| {
        let (mut sim, nodes) = cluster(seed);
        nodes[0].submit_rw(keys(&[1]), keys(&[2]));
        nodes[1].submit(keys(&[1]));
        sim.run_for(Duration::from_secs(5));
        sim.trace_lines()
    };
    assert_eq!(
        trace(seed),
        trace(seed),
        "rw-conflict trace not reproducible"
    );
}
