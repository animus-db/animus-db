//! ADR 0011 acceptance tests for the minimal Accord slice.
//!
//! Under `SimEnv`, a 3-node replica set agrees on and commits transactions via
//! the leaderless PreAccept → (fast path) Commit protocol, and two conflicting
//! transactions commit in a consistent timestamp order on *every* replica. The
//! whole run is byte-reproducible from its seed.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_consensus::{AccordNode, Key, TxnId};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};

const NODES: [u64; 3] = [0, 1, 2];

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

/// A single transaction submitted to one coordinator is committed on every
/// replica at the same execution timestamp, via the fast path.
#[test]
fn single_transaction_commits_on_all_replicas() {
    let seed = 0xACC0_0001;
    let (mut sim, nodes) = cluster(seed);

    let txn = nodes[0].submit(keys(&[42]));
    sim.run_for(Duration::from_secs(1));

    // The coordinator reached a decision, and it was the fast path (no
    // conflicts, all replicas agree on t0).
    let decisions = nodes[0].decisions();
    assert_eq!(decisions.len(), 1, "one decision expected (seed={seed})");
    assert_eq!(
        decisions[0].txn, txn,
        "decision is for our txn (seed={seed})"
    );
    assert!(
        decisions[0].fast_path,
        "uncontended txn should take the fast path (seed={seed})"
    );

    // Every replica committed it at the same execution timestamp.
    let exec: Vec<Option<_>> = nodes
        .iter()
        .map(|n| n.committed_execute_at(txn.clone()))
        .collect();
    assert!(
        exec.iter().all(|e| *e == exec[0]),
        "execution timestamp diverged across replicas: {exec:?} (seed={seed})"
    );
    assert!(
        exec[0].is_some(),
        "transaction not committed anywhere (seed={seed})"
    );
}

/// Two transactions over an overlapping key are committed on every replica, and
/// the execution-timestamp order between them is the *same* on all replicas
/// (consistent serialization order — the core property the slice proves).
#[test]
fn conflicting_transactions_commit_in_consistent_order() {
    let seed = 0xACC0_0002;
    let (mut sim, nodes) = cluster(seed);

    // Two transactions touching the same key conflict.
    let a = nodes[0].submit(keys(&[7]));
    let b = nodes[1].submit(keys(&[7]));
    sim.run_for(Duration::from_secs(2));

    assert_committed_consistently(&nodes, a, b, seed);
}

/// Same as above but submitted in the opposite coordinator order and with a
/// different seed, to exercise different interleavings.
#[test]
fn conflicting_transactions_consistent_order_reverse() {
    let seed = 0xACC0_0003;
    let (mut sim, nodes) = cluster(seed);

    let b = nodes[2].submit(keys(&[7, 9]));
    let a = nodes[0].submit(keys(&[9]));
    sim.run_for(Duration::from_secs(2));

    assert_committed_consistently(&nodes, a, b, seed);
}

/// Conflicting transactions commit in a consistent order across a range of
/// seeds (different network jitter ⇒ different message interleavings).
#[test]
fn conflicting_order_consistent_across_seeds() {
    for seed in 0xACC0_1000..0xACC0_1040 {
        let (mut sim, nodes) = cluster(seed);
        let a = nodes[0].submit(keys(&[5]));
        let b = nodes[1].submit(keys(&[5]));
        sim.run_for(Duration::from_secs(3));
        assert_committed_consistently(&nodes, a, b, seed);
    }
}

/// Non-conflicting transactions (disjoint keys) both commit; neither depends on
/// the other.
#[test]
fn disjoint_transactions_have_no_dependency() {
    let seed = 0xACC0_0004;
    let (mut sim, nodes) = cluster(seed);

    let a = nodes[0].submit(keys(&[1]));
    let b = nodes[1].submit(keys(&[2]));
    sim.run_for(Duration::from_secs(2));

    for n in &nodes {
        assert!(
            n.committed_execute_at(a.clone()).is_some()
                && n.committed_execute_at(b.clone()).is_some(),
            "both disjoint txns must commit everywhere (seed={seed})"
        );
        let deps_a = n.committed_deps(a.clone()).unwrap_or_default();
        let deps_b = n.committed_deps(b.clone()).unwrap_or_default();
        assert!(
            !deps_a.contains(&b) && !deps_b.contains(&a),
            "disjoint txns must not depend on each other (seed={seed})"
        );
    }
}

/// Replaying the same seed produces a byte-identical trace.
#[test]
fn run_is_reproducible_from_seed() {
    let seed = 0xACC0_0005;
    let trace = |seed| {
        let (mut sim, nodes) = cluster(seed);
        nodes[0].submit(keys(&[3]));
        nodes[1].submit(keys(&[3]));
        sim.run_for(Duration::from_secs(2));
        sim.trace_lines()
    };
    assert_eq!(trace(seed), trace(seed), "trace not reproducible");
}

/// Assert both transactions committed on every replica and that the order
/// between their execution timestamps is identical across replicas.
fn assert_committed_consistently(nodes: &[AccordNode<SimEnv>], a: TxnId, b: TxnId, seed: u64) {
    let mut order: Option<bool> = None;
    for (i, n) in nodes.iter().enumerate() {
        let ea = n.committed_execute_at(a.clone());
        let eb = n.committed_execute_at(b.clone());
        assert!(
            ea.is_some() && eb.is_some(),
            "node {i} missing a commit: a={ea:?} b={eb:?} (seed={seed})"
        );
        let (ea, eb) = (ea.unwrap(), eb.unwrap());
        assert_ne!(
            ea, eb,
            "node {i} gave equal execution timestamps (seed={seed})"
        );
        let a_before_b = ea < eb;
        match order {
            None => order = Some(a_before_b),
            Some(prev) => assert_eq!(
                prev, a_before_b,
                "node {i} disagrees on execution order of a vs b (seed={seed})"
            ),
        }
    }

    // The transaction ordered second must depend on the one ordered first
    // (its commit deps must contain the earlier txn), since they conflict.
    let a_first = order.unwrap();
    let (first, second) = if a_first { (a, b) } else { (b, a) };
    for (i, n) in nodes.iter().enumerate() {
        let deps_second = n.committed_deps(second.clone()).unwrap_or_default();
        assert!(
            deps_second.contains(&first),
            "node {i}: later txn must depend on earlier conflicting txn (seed={seed})"
        );
    }
}
