//! ADR 0011 acceptance tests for **arbitrary caller-supplied write values**.
//!
//! Previously an Accord transaction's execution effect was hard-coded to "write
//! my transaction id" (a register). These tests exercise the value-carrying API
//! (`submit_writes` / `submit_writes_rw` / `InteractiveTxn::write_value`): a
//! committed transaction's *actual* value is readable in agreed order, survives a
//! stop/restart (the WAL replays the value), and an interactive read-modify-write
//! carries real values — all over the per-node consensus store (v1 is CP-only;
//! the AP data-plane frontier was removed with `animus-data`, ADR 0019).
//!
//! The whole run is byte-reproducible from its seed.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_consensus::{AccordNode, Key, TxnId};
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];

fn writes(pairs: &[(Key, &[u8])]) -> BTreeMap<Key, Vec<u8>> {
    pairs.iter().map(|(k, v)| (*k, v.to_vec())).collect()
}

fn keys(ks: &[Key]) -> BTreeSet<Key> {
    ks.iter().copied().collect()
}

fn local_cluster(seed: u64) -> (Simulator, Vec<AccordNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| AccordNode::start(sim.env(nid(id)), NODES.iter().copied().map(nid).collect()))
        .collect();
    (sim, nodes)
}

fn store_value(node: &AccordNode<SimEnv>, key: Key) -> Option<Vec<u8>> {
    block_on(node.store_value(key))
}

/// A write transaction's **actual value** (not its id) lands in every replica's
/// executed store.
#[test]
fn submit_writes_stores_the_actual_value() {
    let seed = 0x7A1E_0001;
    let (mut sim, nodes) = local_cluster(seed);

    let txn = nodes[0].submit_writes(writes(&[(7, b"hello"), (8, b"world")]));
    sim.run_for(Duration::from_secs(3));

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(txn.clone()),
            "node {i} did not execute (seed={seed})"
        );
        assert_eq!(
            store_value(n, 7).as_deref(),
            Some(&b"hello"[..]),
            "node {i} key 7 value (seed={seed})"
        );
        assert_eq!(
            store_value(n, 8).as_deref(),
            Some(&b"world"[..]),
            "node {i} key 8 value (seed={seed})"
        );
    }
}

/// Two conflicting value-carrying writes execute in a consistent order, and the
/// shared key's final value is the *second-ordered* transaction's value on every
/// replica.
#[test]
fn conflicting_values_resolve_in_agreed_order() {
    let seed = 0x7A1E_0002;
    let (mut sim, nodes) = local_cluster(seed);

    let a = nodes[0].submit_writes(writes(&[(5, b"AAA")]));
    let b = nodes[1].submit_writes(writes(&[(5, b"BBB")]));
    sim.run_for(Duration::from_secs(4));

    // Determine the agreed order on node 0.
    let order: Vec<TxnId> = nodes[0]
        .applied_order()
        .into_iter()
        .filter(|t| *t == a || *t == b)
        .collect();
    assert_eq!(order.len(), 2, "both executed (seed={seed})");
    let second = order.last().unwrap().clone();
    let expected: &[u8] = if second == a { b"AAA" } else { b"BBB" };

    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            store_value(n, 5).as_deref(),
            Some(expected),
            "node {i} shared key value diverged (seed={seed})"
        );
    }
}

/// A read transaction observes the **actual stored value** ordered before it.
#[test]
fn read_observes_actual_value() {
    let seed = 0x7A1E_0003;
    let (mut sim, nodes) = local_cluster(seed);

    let w = nodes[0].submit_writes(writes(&[(9, b"payload")]));
    sim.run_for(Duration::from_secs(2));
    assert!(nodes[0].is_applied(w));

    let r = nodes[1].submit_read(keys(&[9]));
    sim.run_for(Duration::from_secs(2));
    assert!(
        nodes[1].is_applied(r.clone()),
        "read executed (seed={seed})"
    );

    let observed = nodes[1].read_value_result(r).expect("read result present");
    assert_eq!(
        observed.get(&9).and_then(|o| o.as_deref()),
        Some(&b"payload"[..]),
        "read saw the actual value (seed={seed})"
    );
}

/// The actual value survives a stop/restart: the WAL replays the bytes into a
/// fresh engine.
#[test]
fn value_recovers_from_disk() {
    let seed = 0x7A1E_0004;
    let (mut sim, mut nodes) = local_cluster(seed);

    let txn = nodes[0].submit_writes(writes(&[(3, b"durable")]));
    sim.run_for(Duration::from_secs(3));
    assert!(nodes[2].is_applied(txn.clone()));
    assert_eq!(store_value(&nodes[2], 3).as_deref(), Some(&b"durable"[..]));

    // Stop node 2, restart it fresh; it recovers from its WAL.
    sim.stop(nid(2));
    nodes[2] = AccordNode::start(sim.env(nid(2)), NODES.iter().copied().map(nid).collect());
    sim.run_for(Duration::from_secs(2));

    assert!(
        nodes[2].is_applied(txn),
        "recovered node lost the txn (seed={seed})"
    );
    assert_eq!(
        store_value(&nodes[2], 3).as_deref(),
        Some(&b"durable"[..]),
        "recovered node lost the actual value (seed={seed})"
    );
}

/// An interactive read-modify-write carries a real value: it reads the current
/// value from the local store, appends to it, and writes the new value back —
/// readable afterward on every replica.
#[test]
fn interactive_read_modify_write_carries_value() {
    let seed = 0x7A1E_1002;
    let (mut sim, nodes) = local_cluster(seed);

    // Seed key 4 with an initial value.
    let seed_txn = nodes[0].submit_writes(writes(&[(4, b"v1")]));
    sim.run_for(Duration::from_secs(3));
    assert!(nodes[0].is_applied(seed_txn));

    // Interactive RMW on node 1: read key 4, decide the next value, write it back.
    let committed: Arc<Mutex<Option<TxnId>>> = Arc::new(Mutex::new(None));
    let node = nodes[1].clone();
    let env = node.env().clone();
    let out = Arc::clone(&committed);
    env.clone().spawn_task(async move {
        let mut tx = node.begin();
        let cur = tx.read_value(4).await;
        // Append a marker to whatever we read.
        let mut next = cur.unwrap_or_default();
        next.extend_from_slice(b"+v2");
        tx.write_value(4, next);
        *out.lock().unwrap() = tx.commit();
    });
    sim.run_for(Duration::from_secs(5));

    let txn = committed
        .lock()
        .unwrap()
        .clone()
        .expect("interactive committed");
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(txn.clone()),
            "node {i} did not apply the RMW (seed={seed})"
        );
        assert_eq!(
            store_value(n, 4).as_deref(),
            Some(&b"v1+v2"[..]),
            "node {i}: interactive RMW wrote the modified value (seed={seed})"
        );
    }
}

/// Replaying the same seed produces a byte-identical trace on the value path.
#[test]
fn value_run_is_reproducible_from_seed() {
    let seed = 0x7A1E_0005;
    let trace = |seed| {
        let (mut sim, nodes) = local_cluster(seed);
        nodes[0].submit_writes(writes(&[(1, b"x")]));
        nodes[1].submit_writes(writes(&[(1, b"y")]));
        sim.run_for(Duration::from_secs(3));
        sim.trace_lines()
    };
    assert_eq!(trace(seed), trace(seed), "value trace not reproducible");
}
