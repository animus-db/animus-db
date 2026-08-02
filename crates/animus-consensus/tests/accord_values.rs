//! ADR 0011 acceptance tests for **arbitrary caller-supplied write values**.
//!
//! Previously an Accord transaction's execution effect was hard-coded to "write
//! my transaction id" (a register). These tests exercise the value-carrying API
//! (`submit_writes` / `submit_writes_rw` / `InteractiveTxn::write_value`): a
//! committed transaction's *actual* value is readable in agreed order, survives a
//! stop/restart (the WAL replays the value), rides the sharded data-plane path,
//! and an interactive read-modify-write carries real values.
//!
//! The whole run is byte-reproducible from its seed.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_consensus::{AccordNode, Key, TxnId};
use animus_data::{DataClient, ReadResult, Router, TabletView, serve_replica};
use animus_env::EnvExt;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{Epoch, KeyRange, Tablet, TabletId};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
const ACCORD: [u64; 3] = [0, 1, 2];
const COORDS: [u64; 3] = [10, 11, 12];
const DATA: [u64; 3] = [3, 4, 5];
const VERIFIER: u64 = 20;
const TIMEOUT: Duration = Duration::from_secs(2);

fn writes(pairs: &[(Key, &[u8])]) -> BTreeMap<Key, Vec<u8>> {
    pairs.iter().map(|(k, v)| (*k, v.to_vec())).collect()
}

fn keys(ks: &[Key]) -> BTreeSet<Key> {
    ks.iter().copied().collect()
}

fn storage_key(key: Key) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// Local-store path (no data plane): the executed value is the actual bytes.
// ---------------------------------------------------------------------------

fn local_cluster(seed: u64) -> (Simulator, Vec<AccordNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| AccordNode::start(sim.env(id), NODES.to_vec()))
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
        assert!(n.is_applied(txn), "node {i} did not execute (seed={seed})");
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
    let second = *order.last().unwrap();
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
    assert!(nodes[1].is_applied(r), "read executed (seed={seed})");

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
    assert!(nodes[2].is_applied(txn));
    assert_eq!(store_value(&nodes[2], 3).as_deref(), Some(&b"durable"[..]));

    // Stop node 2, restart it fresh; it recovers from its WAL.
    sim.stop(2);
    nodes[2] = AccordNode::start(sim.env(2), NODES.to_vec());
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

// ---------------------------------------------------------------------------
// Data-plane frontier path: the actual value is readable via a quorum read.
// ---------------------------------------------------------------------------

fn frontier(seed: u64) -> (Simulator, Vec<AccordNode<SimEnv>>, TabletView) {
    let sim = Simulator::new(seed);
    for &id in &DATA {
        serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL);
    }
    let tablet = Tablet::new(TabletId(1), KeyRange::whole(), DATA.to_vec());
    let view = TabletView::from_tablet(&tablet, 2, 2);
    let nodes = (0..ACCORD.len())
        .map(|i| {
            AccordNode::start_with_data_plane(
                sim.env(ACCORD[i]),
                ACCORD.to_vec(),
                MemoryEngine::new(),
                sim.env(COORDS[i]),
                view.clone(),
            )
        })
        .collect();
    (sim, nodes, view)
}

/// Read a key's raw value bytes through a standalone quorum coordinator.
fn quorum_value(sim: &mut Simulator, view: &TabletView, key: Key) -> Option<Vec<u8>> {
    let result: Arc<Mutex<Option<ReadResult>>> = Arc::new(Mutex::new(None));
    let env = sim.env(VERIFIER);
    let out = Arc::clone(&result);
    let view = view.clone();
    env.clone().spawn_task(async move {
        let client = DataClient::new(env);
        let r = client.read(&view, &storage_key(key), TIMEOUT).await;
        *out.lock().unwrap() = Some(r);
    });
    sim.run_for(TIMEOUT);
    match result.lock().unwrap().clone().expect("read completed") {
        ReadResult::Value(v) => v,
        ReadResult::Failed => panic!("quorum read failed"),
    }
}

/// A value-carrying transaction's actual bytes are readable via the data-plane
/// quorum.
#[test]
fn value_lands_in_data_plane() {
    let seed = 0x7A1E_1001;
    let (mut sim, nodes, view) = frontier(seed);

    let txn = nodes[0].submit_writes(writes(&[(1, b"alpha"), (2, b"beta")]));
    sim.run_for(Duration::from_secs(5));
    assert!(nodes[0].is_applied(txn));

    assert_eq!(
        quorum_value(&mut sim, &view, 1).as_deref(),
        Some(&b"alpha"[..]),
        "key 1 value via data plane (seed={seed})"
    );
    assert_eq!(
        quorum_value(&mut sim, &view, 2).as_deref(),
        Some(&b"beta"[..]),
        "key 2 value via data plane (seed={seed})"
    );
}

/// An interactive read-modify-write carries a real value: it reads the current
/// value, appends to it, and writes the new value back — readable afterward.
#[test]
fn interactive_read_modify_write_carries_value() {
    let seed = 0x7A1E_1002;
    let (mut sim, nodes, view) = frontier(seed);

    // Seed key 4 with an initial value.
    let seed_txn = nodes[0].submit_writes(writes(&[(4, b"v1")]));
    sim.run_for(Duration::from_secs(4));
    assert!(nodes[0].is_applied(seed_txn));

    // Interactive RMW: read key 4, decide the next value, write it back.
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

    let txn = committed.lock().unwrap().expect("interactive committed");
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        quorum_value(&mut sim, &view, 4).as_deref(),
        Some(&b"v1+v2"[..]),
        "interactive RMW wrote the modified value (seed={seed}) txn={txn:?}"
    );
}

// ---------------------------------------------------------------------------
// Sharded path: per-tablet routing carries the actual value.
// ---------------------------------------------------------------------------

/// A cross-tablet value-carrying transaction writes the real value to each key's
/// own tablet quorum.
#[test]
fn sharded_transaction_carries_values() {
    let seed = 0x7A1E_2001;
    let mut sim = Simulator::new(seed);

    // Two tablets split at key 1000: {3,4} and {4,5}.
    for &id in &[3u64, 4, 5] {
        serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL);
    }
    let boundary = storage_key(1000);
    let tablets = vec![
        Tablet::new(
            TabletId(1),
            KeyRange::new(Vec::new(), Some(boundary.clone())),
            vec![3, 4],
        ),
        Tablet::new(TabletId(2), KeyRange::new(boundary, None), vec![4, 5]),
    ];
    let router = Router::new(tablets, 2, 2);

    let nodes: Vec<AccordNode<SimEnv>> = (0..ACCORD.len())
        .map(|i| {
            AccordNode::start_with_router(
                sim.env(ACCORD[i]),
                ACCORD.to_vec(),
                MemoryEngine::new(),
                sim.env(COORDS[i]),
                router.clone(),
            )
        })
        .collect();

    // key 5 → low tablet, key 5000 → high tablet.
    let txn = nodes[0].submit_writes(writes(&[(5, b"low-val"), (5000, b"high-val")]));
    sim.run_for(Duration::from_secs(6));
    assert!(nodes[0].is_applied(txn));

    let view_lo = router.view_for(&storage_key(5)).unwrap();
    let view_hi = router.view_for(&storage_key(5000)).unwrap();
    assert_eq!(
        quorum_value(&mut sim, &view_lo, 5).as_deref(),
        Some(&b"low-val"[..]),
        "low-tablet key value (seed={seed})"
    );
    assert_eq!(
        quorum_value(&mut sim, &view_hi, 5000).as_deref(),
        Some(&b"high-val"[..]),
        "high-tablet key value (seed={seed})"
    );
}
