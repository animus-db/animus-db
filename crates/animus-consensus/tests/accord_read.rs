//! ADR 0011 acceptance tests for Accord **read-only transactions**.
//!
//! A read transaction is ordered exactly like a write — it mints a timestamp,
//! intersects conflicting keys, and is committed at an agreed execution
//! timestamp — but its execution *effect* is a snapshot read of each key as of
//! that timestamp (from the storage-backed execution store), not a write. The
//! property this proves, under the deterministic `SimEnv`:
//!
//! - a read observes the writes of every transaction ordered **before** it and
//!   none ordered **after** it, and
//! - it observes the **same** values on every replica (consistent snapshot).
//!
//! The whole run is byte-reproducible from its seed.

use std::collections::{BTreeMap, BTreeSet};
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

/// The writer a read observed at `key`, as recorded on `node` for read txn `r`.
/// Panics if the read has not executed here (the caller drives it first).
fn observed(node: &AccordNode<SimEnv>, r: TxnId, key: Key) -> Option<TxnId> {
    let result: BTreeMap<Key, Option<TxnId>> = node
        .read_result(r)
        .expect("read transaction has not executed on this replica");
    *result.get(&key).expect("read covered this key")
}

/// A read ordered after a write observes that write; a read ordered before a
/// later write does not observe it. Sequenced submission makes the order
/// unambiguous: each phase is driven to quiescence before the next, so the read
/// always orders strictly between the writes that bracket it.
#[test]
fn read_observes_writes_before_it_and_not_after() {
    let seed = 0x4EAD_0001;
    let (mut sim, nodes) = cluster(seed);

    // Write A sets key 7.
    let a = nodes[0].submit(keys(&[7]));
    sim.run_for(Duration::from_secs(2));

    // Read R1 over key 7 — submitted after A executed, so it orders after A and
    // must observe A's write.
    let r1 = nodes[1].submit_read(keys(&[7]));
    sim.run_for(Duration::from_secs(2));

    // Write B overwrites key 7 — submitted after R1, so it orders after R1.
    let b = nodes[2].submit(keys(&[7]));
    sim.run_for(Duration::from_secs(2));

    // Read R2 over key 7 — orders after B, must observe B.
    let r2 = nodes[0].submit_read(keys(&[7]));
    sim.run_for(Duration::from_secs(2));

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(r1) && n.is_applied(r2),
            "node {i} did not execute both reads (seed={seed})"
        );
        // R1 ordered after A and before B: it sees A, never B.
        assert_eq!(
            observed(n, r1, 7),
            Some(a),
            "node {i}: read R1 must observe write A (seed={seed})"
        );
        assert_ne!(
            observed(n, r1, 7),
            Some(b),
            "node {i}: read R1 must NOT observe the later write B (seed={seed})"
        );
        // R2 ordered after B: it sees B.
        assert_eq!(
            observed(n, r2, 7),
            Some(b),
            "node {i}: read R2 must observe write B (seed={seed})"
        );
    }
}

/// A read over a key never written observes nothing (`None`), consistently on
/// every replica.
#[test]
fn read_of_unwritten_key_observes_nothing() {
    let seed = 0x4EAD_0002;
    let (mut sim, nodes) = cluster(seed);

    let r = nodes[0].submit_read(keys(&[99]));
    sim.run_for(Duration::from_secs(2));

    for (i, n) in nodes.iter().enumerate() {
        assert!(n.is_applied(r), "node {i} did not execute the read");
        assert_eq!(
            observed(n, r, 99),
            None,
            "node {i}: read of an unwritten key must observe nothing (seed={seed})"
        );
    }
}

/// Every replica records the **same** observation for a read — the read snapshot
/// is consistent across the cluster — across many seeds (different network
/// interleavings).
#[test]
fn read_snapshot_consistent_across_replicas_and_seeds() {
    for seed in 0x4EAD_1000..0x4EAD_1030 {
        let (mut sim, nodes) = cluster(seed);

        // A write, then a read of the same key ordered after it.
        let a = nodes[0].submit(keys(&[3, 4]));
        sim.run_for(Duration::from_secs(2));
        let r = nodes[1].submit_read(keys(&[3, 4]));
        sim.run_for(Duration::from_secs(2));

        // Reference observation taken from node 0.
        assert!(nodes[0].is_applied(r), "read not executed (seed={seed})");
        let reference = nodes[0].read_result(r).unwrap();
        // The read ordered after A, so it sees A on both keys.
        assert_eq!(reference.get(&3), Some(&Some(a)), "seed={seed}");
        assert_eq!(reference.get(&4), Some(&Some(a)), "seed={seed}");

        for (i, n) in nodes.iter().enumerate() {
            assert!(n.is_applied(r), "node {i} read not executed (seed={seed})");
            assert_eq!(
                n.read_result(r),
                Some(reference.clone()),
                "node {i}: read observation diverged from replica 0 (seed={seed})"
            );
        }
    }
}

/// A read transaction's effect is durable through restart: a replica stopped and
/// restarted on the same disk recovers its executed reads (re-runs them in the
/// recovered execution order) and reports the same observations.
#[test]
fn read_result_recovers_from_disk() {
    let seed = 0x4EAD_0003;
    let (mut sim, mut nodes) = cluster(seed);

    let a = nodes[0].submit(keys(&[8]));
    sim.run_for(Duration::from_secs(2));
    let r = nodes[1].submit_read(keys(&[8]));
    sim.run_for(Duration::from_secs(2));

    let before = nodes[2].read_result(r);
    assert_eq!(before, Some(BTreeMap::from([(8u64, Some(a))])));

    // Stop node 2 (volatile state lost) and restart on the same disk.
    sim.stop(nid(2));
    nodes[2] = AccordNode::start(sim.env(nid(2)), NODES.iter().copied().map(nid).collect());
    sim.run_for(Duration::from_secs(2));

    assert!(
        nodes[2].is_applied(r),
        "recovered node lost the executed read (seed={seed})"
    );
    assert_eq!(
        nodes[2].read_result(r),
        before,
        "recovered node lost its read observation (seed={seed})"
    );
}

/// Replaying the same seed produces a byte-identical trace, including the read
/// execution path.
#[test]
fn read_run_is_reproducible_from_seed() {
    let seed = 0x4EAD_0004;
    let trace = |seed| {
        let (mut sim, nodes) = cluster(seed);
        nodes[0].submit(keys(&[1]));
        sim.run_for(Duration::from_secs(2));
        nodes[1].submit_read(keys(&[1]));
        sim.run_for(Duration::from_secs(2));
        sim.trace_lines()
    };
    assert_eq!(trace(seed), trace(seed), "read trace not reproducible");
}
