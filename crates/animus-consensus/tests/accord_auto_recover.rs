//! ADR 0011 acceptance tests for **failure-detector-triggered recovery**.
//!
//! The earlier recovery slices (`accord_recover.rs`, `accord_recover_ballots.rs`)
//! proved that recovery — invoked **explicitly** by a test — drives a stranded
//! transaction to a consistent commit, and that recovery **ballots** make
//! concurrent recoverers converge. This file closes the loop the others left
//! open: the [`AccordNode`] driver now runs a **failure detector** that
//! *auto-triggers* recovery when a transaction it holds un-committed stays stalled
//! past a time bound — no explicit `recover` call.
//!
//! The properties under test:
//!
//! 1. A coordinator that dies after `PreAccept` but before `Commit` has its
//!    transaction **auto-recovered within the bound** and committed on every
//!    survivor (with the store converged).
//! 2. When several replicas could race to auto-recover, they **converge to one
//!    decision** (the deterministic nominee keeps the common case duel-free; the
//!    recovery **ballots** keep a duel safe when escalation does cause one).
//! 3. A coordinator that is merely **slow but still progressing** is **not**
//!    spuriously recovered, and a healthy cluster never auto-recovers.
//! 4. Auto-recovery **composes with arbitrary write values**: the recovered
//!    transaction's actual value survives onto every replica.
//!
//! Each run is byte-reproducible from its seed. The driver's liveness timer is a
//! **perpetual** `Env` timer, so every test bounds virtual time with `run_for`.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_consensus::{AccordNode, Key, TxnId};
use animus_sim::{SimEnv, Simulator};
use futures::executor::block_on;

const NODES: [u64; 5] = [0, 1, 2, 3, 4];

fn cluster(seed: u64) -> (Simulator, Vec<AccordNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| AccordNode::start(sim.env(id), NODES.to_vec()))
        .collect();
    (sim, nodes)
}

fn keys(ks: &[Key]) -> BTreeSet<Key> {
    ks.iter().copied().collect()
}

fn store_writer(node: &AccordNode<SimEnv>, key: Key) -> Option<TxnId> {
    block_on(node.store_writer(key))
}

fn store_value(node: &AccordNode<SimEnv>, key: Key) -> Option<Vec<u8>> {
    block_on(node.store_value(key))
}

/// Assert every replica that committed `txn` agrees on `(execute_at, deps)`, and
/// at least one replica committed it. Returns the agreed execution timestamp.
fn assert_committed_consistently(
    nodes: &[AccordNode<SimEnv>],
    txn: TxnId,
    seed: u64,
) -> animus_consensus::Timestamp {
    let mut agreed: Option<animus_consensus::Timestamp> = None;
    let mut agreed_deps: Option<BTreeSet<TxnId>> = None;
    let mut committed = 0;
    for (i, n) in nodes.iter().enumerate() {
        if let Some(e) = n.committed_execute_at(txn) {
            committed += 1;
            match agreed {
                None => {
                    agreed = Some(e);
                    agreed_deps = n.committed_deps(txn);
                }
                Some(prev) => assert_eq!(
                    prev, e,
                    "replica {i} committed at a different execute_at (seed={seed})"
                ),
            }
            assert_eq!(
                n.committed_deps(txn),
                agreed_deps,
                "replica {i} committed with different deps (seed={seed})"
            );
        }
    }
    assert!(
        committed > 0,
        "no replica committed the (auto-recovered) txn (seed={seed})"
    );
    agreed.expect("committed > 0")
}

/// **A coordinator that dies after `PreAccept` but before `Commit` is
/// auto-recovered within the bound.** No explicit `recover` call: the original
/// coordinator (node 0) ships its `PreAccept` to the survivors, then is fully
/// partitioned away before it can gather a fast quorum and commit. The
/// survivors' failure detector then notices the transaction is stuck
/// un-committed past the bound and the deterministic nominee (the lowest-id
/// survivor) takes over — driving the transaction to a consistent commit and
/// execution on every survivor.
#[test]
fn dead_coordinator_is_auto_recovered_within_bound() {
    for seed in 0xA070_0000..0xA070_0010 {
        let (mut sim, nodes) = cluster(seed);

        let txn = nodes[0].submit(keys(&[7, 8]));
        // Let the PreAccept reach the survivors (so a recovery quorum learns the
        // keys), then isolate the coordinator before it can commit.
        sim.run_for(Duration::from_millis(30));
        for peer in [1, 2, 3, 4] {
            sim.partition_pair(0, peer);
        }
        // Run well past the failure-detector bound (≈5s). **No explicit
        // recover():** the driver's liveness tick must auto-trigger it, then the
        // recovered transaction must commit + execute on every survivor.
        sim.run_for(Duration::from_secs(10));

        let agreed = assert_committed_consistently(&nodes, txn, seed);
        // Every survivor that applied it carries the recovered write; store converged.
        for &k in &[7u64, 8u64] {
            for n in &nodes {
                if n.is_applied(txn) {
                    assert_eq!(
                        store_writer(n, k),
                        Some(txn),
                        "an applied replica missed the auto-recovered write on key {k} \
                         (seed={seed}); agreed execute_at={agreed:?}"
                    );
                }
            }
        }
        // At least a quorum (the survivors) committed — recovery actually fired.
        let committed = nodes
            .iter()
            .filter(|n| n.committed_execute_at(txn).is_some())
            .count();
        assert!(
            committed >= 3,
            "auto-recovery did not reach a quorum (seed={seed}); committed={committed}"
        );
    }
}

/// **Auto-recoverers converge to one decision even when escalation duels.** The
/// coordinator (node 0) dies after `PreAccept`. The tier-0 nominee (node 1) is
/// *also* partitioned from part of the cluster, so its first recovery cannot
/// reach a quorum and the detector **escalates** to the next-tier nominee while
/// node 1's attempt is still in flight — a genuine duel. Recovery ballots must
/// make them converge to a single committed decision on every replica that
/// commits, and no committed decision is ever reverted.
#[test]
fn escalating_auto_recoverers_converge() {
    for seed in 0xA071_0000..0xA071_0008 {
        let (mut sim, nodes) = cluster(seed);

        let txn = nodes[0].submit(keys(&[15]));
        sim.run_for(Duration::from_millis(30));
        // Kill the coordinator (node 0) and hobble the tier-0 nominee (node 1):
        // node 1 can still see nodes 2 and 3 but not node 4, so depending on
        // timing its recovery may or may not gather a quorum before a higher tier
        // also fires. Either way, ballots must converge the outcome.
        for peer in [1, 2, 3, 4] {
            sim.partition_pair(0, peer);
        }
        sim.partition_pair(1, 4);
        // Past the failure-detector bound (≈5s) so the tier-0 nominee fires (and,
        // if it cannot reach a quorum, a higher tier escalates).
        sim.run_for(Duration::from_secs(9));

        // Heal node 1's link so every survivor can converge, then settle.
        sim.heal(1, 4);
        sim.run_for(Duration::from_secs(3));

        let _ = assert_committed_consistently(&nodes, txn, seed);
        // Among the nodes that committed, the store is converged on the key.
        let mut writers = Vec::new();
        for n in &nodes {
            if n.is_applied(txn) {
                writers.push(store_writer(n, 15));
            }
        }
        assert!(
            !writers.is_empty() && writers.iter().all(|w| *w == Some(txn)),
            "escalating auto-recovery diverged the store (seed={seed})"
        );
    }
}

/// **A slow-but-progressing coordinator is NOT spuriously recovered.** The
/// coordinator (node 0) is briefly partitioned from one peer so its first
/// fast-quorum attempt fails, but the link **heals well within the failure
/// detector's bound**, so the transaction keeps progressing and the coordinator
/// itself commits. The survivors' detector must observe the progress (fingerprint
/// advancing) and **defer** recovery — so no survivor ever becomes a recovery
/// coordinator. We prove that by checking the nominee (node 1) reached **no
/// coordinator decision of its own**: had it spuriously recovered, it would have
/// recorded a (slow-path) decision.
#[test]
fn slow_but_progressing_coordinator_is_not_recovered() {
    for seed in 0xA072_0000..0xA072_0008 {
        let (mut sim, nodes) = cluster(seed);

        // Node 0 cannot reach node 4 at first (no fast quorum on the first try),
        // but the rest of the cluster is healthy, so it still makes progress.
        sim.partition_pair(0, 4);
        let txn = nodes[0].submit(keys(&[9]));
        // Heal quickly — far inside the bound — so the coordinator progresses and
        // commits on its own.
        sim.run_for(Duration::from_millis(50));
        sim.heal(0, 4);
        sim.run_for(Duration::from_secs(2));

        // The original coordinator committed the transaction itself.
        assert!(
            nodes[0].committed_execute_at(txn).is_some(),
            "the slow-but-live coordinator never committed (seed={seed})"
        );
        let agreed = assert_committed_consistently(&nodes, txn, seed);

        // No survivor spuriously recovered it: the tier-0 nominee (node 1, the
        // lowest-id node that is not the coordinator) recorded no decision of its
        // own. (The original coordinator's decision is on node 0, not node 1.)
        assert!(
            nodes[1].decisions().is_empty(),
            "the nominee spuriously recovered a still-progressing txn (seed={seed}); \
             decisions={:?}",
            nodes[1].decisions()
        );
        // And the cluster converged on the coordinator's own decision.
        let _ = agreed;
    }
}

/// **A healthy cluster never auto-recovers anything.** With no faults, every
/// transaction commits promptly via its original coordinator, so the failure
/// detector must never fire — no node other than an original coordinator ever
/// records a decision.
#[test]
fn healthy_cluster_never_auto_recovers() {
    let seed = 0xA072_1000;
    let (mut sim, nodes) = cluster(seed);

    let t1 = nodes[0].submit(keys(&[1, 2]));
    let t2 = nodes[3].submit(keys(&[2, 3]));
    // Run for many failure-detector bounds.
    sim.run_for(Duration::from_secs(5));

    for txn in [t1, t2] {
        let _ = assert_committed_consistently(&nodes, txn, seed);
    }
    // Only the original coordinators (nodes 0 and 3) recorded decisions; every
    // other node's decision list is empty (nothing was recovered).
    for i in [1usize, 2, 4] {
        assert!(
            nodes[i].decisions().is_empty(),
            "node {i} auto-recovered in a healthy cluster (seed={seed}); \
             decisions={:?}",
            nodes[i].decisions()
        );
    }
}

/// **Auto-recovery composes with arbitrary write values.** The coordinator
/// submits a value-carrying write (`submit_writes`) and dies before committing.
/// The auto-recovered transaction must carry the **actual value** — not a
/// fallback register id — onto every applied replica.
#[test]
fn auto_recovery_preserves_write_value() {
    for seed in 0xA073_0000..0xA073_0008 {
        let (mut sim, nodes) = cluster(seed);

        let value = b"appended-element".to_vec();
        let mut writes = std::collections::BTreeMap::new();
        writes.insert(42u64, value.clone());
        let txn = nodes[0].submit_writes(writes);

        sim.run_for(Duration::from_millis(30));
        for peer in [1, 2, 3, 4] {
            sim.partition_pair(0, peer);
        }
        // Past the failure-detector bound (≈5s) so auto-recovery fires.
        sim.run_for(Duration::from_secs(10));

        let _ = assert_committed_consistently(&nodes, txn, seed);
        let mut seen = 0;
        for n in &nodes {
            if n.is_applied(txn) {
                seen += 1;
                assert_eq!(
                    store_value(n, 42),
                    Some(value.clone()),
                    "auto-recovered txn lost its write value (seed={seed})"
                );
            }
        }
        assert!(
            seen > 0,
            "no replica applied the auto-recovered value (seed={seed})"
        );
    }
}

/// The auto-recovery run is byte-reproducible from its seed.
#[test]
fn auto_recovery_is_reproducible_from_seed() {
    let seed = 0xA074_0000;
    let trace = |seed| {
        let (mut sim, nodes) = cluster(seed);
        let txn = nodes[0].submit(keys(&[7]));
        sim.run_for(Duration::from_millis(30));
        for peer in [1, 2, 3, 4] {
            sim.partition_pair(0, peer);
        }
        sim.run_for(Duration::from_secs(10));
        let _ = txn;
        sim.trace_lines()
    };
    assert_eq!(
        trace(seed),
        trace(seed),
        "auto-recovery trace not reproducible (seed={seed})"
    );
}
