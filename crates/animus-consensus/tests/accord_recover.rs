//! ADR 0011 acceptance tests for Accord **coordinator failover** (the first
//! recovery slice).
//!
//! Under `SimEnv`, a transaction's original coordinator is isolated mid-flight
//! (after minting `t0` and broadcasting `PreAccept`, before the surviving
//! replicas learn a `Commit`). A *different* replica then takes over as a
//! recovery coordinator, queries the replicas for their recorded state, and
//! drives the transaction to a commit that is **consistent across every
//! surviving replica** — and the transaction then executes against the storage
//! engine. The whole run is byte-reproducible from its seed.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_consensus::{AccordNode, Key, TxnId};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use futures::executor::block_on;

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

fn store_writer(node: &AccordNode<SimEnv>, key: Key) -> Option<TxnId> {
    block_on(node.store_writer(key))
}

/// The original coordinator (node 0) reaches one survivor (node 1) with its
/// `PreAccept` — so node 1 witnesses the transaction's keys — but never hears
/// node 1's reply back and is fully cut off from node 2, so it can never gather a
/// quorum and stalls. (Under the *precise* fast quorum of 2 for N=3, merely
/// partitioning 0↔2 would let 0+1 fast-commit; we additionally block node 1's
/// **reply** to node 0 so the coordinator is genuinely stranded while node 1 still
/// holds the keys.) A *different* survivor (node 2) takes over: it queries the
/// replicas, learns the transaction's keys from node 1's recorded `PreAccept`,
/// re-drives the slow path, and reaches a commit + execution consistent on both
/// survivors.
#[test]
fn recovery_commits_a_stranded_transaction() {
    let seed = 0xFA11_0001;
    let (mut sim, nodes) = cluster(seed);

    // Node 0 cannot reach node 2 at all, and node 1's *reply* to node 0 is dropped
    // (the PreAccept 0→1 still gets through, so node 1 learns the keys). Node 0
    // thus has only its own vote and stalls; node 2 can still reach node 1.
    sim.partition_pair(nid(0), nid(2));
    sim.partition(nid(1), nid(0));

    let txn = nodes[0].submit(keys(&[7]));
    // Settle the (failed) fast-quorum attempt, but stay **inside** the driver's
    // failure-detector bound so this test exercises an *explicit* `recover` — not
    // the auto-recovery that `accord_auto_recover.rs` covers. (The detector fires
    // only after a transaction is stalled for its full bound; ~200ms is well under
    // it.)
    sim.run_for(Duration::from_millis(200));

    // The coordinator is stranded: nobody has committed.
    assert!(
        nodes[0].committed_execute_at(txn).is_none(),
        "stalled coordinator should not have committed (seed={seed})"
    );
    assert!(
        nodes[1].committed_execute_at(txn).is_none()
            && nodes[2].committed_execute_at(txn).is_none(),
        "survivors should not have committed a stranded txn yet (seed={seed})"
    );

    // A surviving replica (node 2) takes over and recovers the transaction. It
    // can reach node 1 (only node 0 is partitioned away).
    nodes[2].recover(txn);
    sim.run_for(Duration::from_secs(2));

    // Both survivors committed it at the same execution timestamp and deps.
    let e1 = nodes[1].committed_execute_at(txn);
    let e2 = nodes[2].committed_execute_at(txn);
    assert!(
        e1.is_some() && e1 == e2,
        "survivors disagree on the recovered commit: {e1:?} vs {e2:?} (seed={seed})"
    );
    assert_eq!(
        nodes[1].committed_deps(txn),
        nodes[2].committed_deps(txn),
        "survivors disagree on recovered deps (seed={seed})"
    );

    // And it executed everywhere it committed, with a converged store.
    assert!(
        nodes[1].is_applied(txn) && nodes[2].is_applied(txn),
        "recovered txn did not execute on the survivors (seed={seed})"
    );
    assert_eq!(
        store_writer(&nodes[1], 7),
        Some(txn),
        "node 1 store missing the recovered write (seed={seed})"
    );
    assert_eq!(
        store_writer(&nodes[1], 7),
        store_writer(&nodes[2], 7),
        "survivors' stores diverged after recovery (seed={seed})"
    );
}

/// Recovery is consistent across a range of seeds (different interleavings),
/// and a recovered transaction's commit + execution agree on every survivor.
#[test]
fn recovery_consistent_across_seeds() {
    for seed in 0xFA11_1000..0xFA11_1020 {
        let (mut sim, nodes) = cluster(seed);
        sim.partition_pair(nid(0), nid(2));

        let txn = nodes[0].submit(keys(&[3, 4]));
        sim.run_for(Duration::from_secs(1));

        nodes[2].recover(txn);
        sim.run_for(Duration::from_secs(2));

        let e1 = nodes[1].committed_execute_at(txn);
        let e2 = nodes[2].committed_execute_at(txn);
        assert!(
            e1.is_some() && e1 == e2,
            "survivors disagree on recovery (seed={seed}): {e1:?} vs {e2:?}"
        );
        assert!(
            nodes[1].is_applied(txn) && nodes[2].is_applied(txn),
            "recovered txn not executed (seed={seed})"
        );
        for &k in &[3u64, 4u64] {
            assert_eq!(
                store_writer(&nodes[1], k),
                Some(txn),
                "node 1 missing recovered write on key {k} (seed={seed})"
            );
            assert_eq!(
                store_writer(&nodes[1], k),
                store_writer(&nodes[2], k),
                "stores diverged on key {k} after recovery (seed={seed})"
            );
        }
    }
}

/// When recovery finds the transaction was *already committed* on a recovery-
/// quorum replica, it must **adopt that exact decision** rather than invent a
/// new one (a committed value is immutable). Here the transaction commits
/// normally on all replicas, then a replica runs recovery anyway (modelling a
/// spurious failover / duplicate recovery): the recovered decision must equal
/// the original commit, and re-execution must be idempotent.
#[test]
fn recovery_adopts_an_existing_commit() {
    let seed = 0xFA11_0002;
    let (mut sim, nodes) = cluster(seed);

    let txn = nodes[0].submit(keys(&[9]));
    sim.run_for(Duration::from_secs(2));

    // The transaction committed and executed everywhere.
    let committed_at = nodes[0].committed_execute_at(txn);
    assert!(
        committed_at.is_some(),
        "txn should have committed (seed={seed})"
    );
    for n in &nodes {
        assert_eq!(
            n.committed_execute_at(txn),
            committed_at,
            "commit diverged before recovery (seed={seed})"
        );
        assert!(n.is_applied(txn), "txn not executed (seed={seed})");
    }
    let writer_before = store_writer(&nodes[2], 9);

    // A replica runs recovery anyway; it must adopt the existing commit verbatim.
    nodes[1].recover(txn);
    sim.run_for(Duration::from_secs(2));

    for n in &nodes {
        assert_eq!(
            n.committed_execute_at(txn),
            committed_at,
            "recovery changed an already-committed decision (seed={seed})"
        );
    }
    assert_eq!(
        store_writer(&nodes[2], 9),
        writer_before,
        "recovery perturbed the executed store (seed={seed})"
    );
}

/// The recovery run is byte-reproducible from its seed.
#[test]
fn recovery_run_is_reproducible_from_seed() {
    let seed = 0xFA11_0003;
    let trace = |seed| {
        let (mut sim, nodes) = cluster(seed);
        sim.partition_pair(nid(0), nid(2));
        let txn = nodes[0].submit(keys(&[7]));
        sim.run_for(Duration::from_secs(1));
        nodes[2].recover(txn);
        sim.run_for(Duration::from_secs(2));
        sim.trace_lines()
    };
    assert_eq!(trace(seed), trace(seed), "recovery trace not reproducible");
}
