//! ADR 0011 acceptance tests for **WAL snapshotting / log truncation**.
//!
//! Left unchecked the per-node Accord WAL grows with every phase transition of
//! every transaction. The driver periodically snapshots the applied state and
//! **atomically replaces** the WAL with a single compact `Snapshot` record plus
//! the live tail (mirroring the control-plane Raft). These tests, under `SimEnv`,
//! drive enough transactions to trip the compaction threshold and assert (1) the
//! WAL is **bounded** — it does not grow without limit as transactions
//! accumulate — and (2) a node restarted on the truncated WAL recovers **identical
//! executed state**. The runs are byte-reproducible from their seeds.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_consensus::{AccordNode, Key, PersistedState, TxnId, WalRecord};
use animus_env::{Disk, nid};
use animus_sim::{SimEnv, Simulator};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
/// The per-node WAL filename the driver uses (mirrors `node::WAL`).
const WAL: &str = "accord.wal";

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

fn wal_size(env: &SimEnv) -> u64 {
    block_on(env.size(WAL)).expect("wal size")
}

/// Decode the on-disk WAL into its records.
fn wal_records(env: &SimEnv) -> Vec<WalRecord> {
    let bytes = block_on(env.read(WAL)).expect("wal read");
    PersistedState::decode(&bytes)
}

/// Submit `n` single-key transactions (one fresh key each, so they are all
/// independent and all apply) round-robin across the coordinators, then run to
/// quiescence within the bound.
fn drive_many(sim: &mut Simulator, nodes: &[AccordNode<SimEnv>], n: u64) -> Vec<TxnId> {
    let mut ids = Vec::new();
    for k in 0..n {
        let coord = (k as usize) % nodes.len();
        ids.push(nodes[coord].submit(keys(&[k])));
        // Let each transaction settle so the failure detector never fires.
        sim.run_for(Duration::from_millis(20));
    }
    sim.run_for(Duration::from_secs(2));
    ids
}

/// The WAL is **truncated**: once enough transactions have applied to trip the
/// threshold, the driver atomically rewrites the WAL to a compact image led by a
/// single `Snapshot` record, collapsing every covered transaction's multi-record
/// phase history (`PreAccepted`, `Accepted`/`Promised`, `Committed`, `Applied`)
/// into one entry inside that snapshot. We decode the on-disk WAL and assert the
/// **record count** is a tiny fraction of the raw per-phase history a
/// never-truncated log would hold — the precise, byte-size-independent signal that
/// the prefix was reclaimed.
#[test]
fn wal_is_truncated_under_sustained_load() {
    let seed = 0x57AB_0001;
    let (mut sim, nodes) = cluster(seed);

    // Drive well past the compaction threshold (64 applies).
    let n = 200u64;
    drive_many(&mut sim, &nodes, n);

    let applied = nodes[0].applied_order().len() as u64;
    assert!(
        applied >= n,
        "all transactions should have applied (seed={seed})"
    );

    let records = wal_records(nodes[0].env());
    let snapshots = records
        .iter()
        .filter(|r| matches!(r, WalRecord::Snapshot { .. }))
        .count();
    assert!(
        snapshots >= 1,
        "WAL was never compacted to a Snapshot record (seed={seed})"
    );

    // The raw, un-truncated log would hold *at least* one record per applied
    // transaction for each of its phase transitions (PreAccepted + Committed +
    // Applied ≥ 3 per txn, more under the slow path). After truncation almost all
    // of that prefix is gone, replaced by the single Snapshot record plus only a
    // short live tail. So the total record count must be far below `applied` (the
    // floor a non-truncating log could never get under, since it keeps an `Applied`
    // record per transaction alone). We assert it is under a third of `applied`.
    let total = records.len() as u64;
    assert!(
        total < applied / 3,
        "WAL not truncated: {total} records for {applied} applied transactions — \
         the per-phase prefix was retained instead of collapsed into the snapshot \
         (seed={seed})"
    );
    assert!(
        wal_size(nodes[0].env()) > 0,
        "WAL should be non-empty (seed={seed})"
    );
}

/// A node restarted on a **truncated** WAL recovers identical executed state.
/// After compaction the WAL is a single `Snapshot` record (plus any live tail);
/// replaying it must reconstruct the same execution order and store as before.
#[test]
fn restart_from_truncated_wal_recovers_identical_state() {
    let seed = 0x57AB_0002;
    let (mut sim, mut nodes) = cluster(seed);

    // Cross the compaction threshold so node 2's WAL is truncated to a snapshot.
    let n = 90u64;
    let ids = drive_many(&mut sim, &nodes, n);

    // Everything applied on node 2; capture its full executed view + WAL size.
    for (k, id) in ids.iter().enumerate() {
        assert!(
            nodes[2].is_applied(id.clone()),
            "node 2 did not apply txn for key {k} (seed={seed})"
        );
    }
    let before_order = nodes[2].applied_order();
    let before_store: Vec<Option<TxnId>> = (0..n).map(|k| store_writer(&nodes[2], k)).collect();
    let truncated_size = wal_size(nodes[2].env());
    assert!(
        truncated_size > 0,
        "truncated WAL must be non-empty (seed={seed})"
    );

    // Restart node 2 on the same (truncated) disk — it recovers from snapshot+tail.
    sim.stop(nid(2));
    nodes[2] = AccordNode::start(sim.env(nid(2)), NODES.iter().copied().map(nid).collect());
    sim.run_for(Duration::from_secs(2));

    // Identical execution order and store after recovering from the truncated WAL.
    assert_eq!(
        nodes[2].applied_order(),
        before_order,
        "recovered execution order diverged after truncation (seed={seed})"
    );
    let after_store: Vec<Option<TxnId>> = (0..n).map(|k| store_writer(&nodes[2], k)).collect();
    assert_eq!(
        after_store, before_store,
        "recovered store diverged after truncation (seed={seed})"
    );
    // And it still agrees with a live replica on every key.
    for k in 0..n {
        assert_eq!(
            store_writer(&nodes[2], k),
            store_writer(&nodes[0], k),
            "recovered node diverged from a live replica on key {k} (seed={seed})"
        );
    }
}

/// The snapshot/truncation run is byte-reproducible from its seed.
#[test]
fn snapshot_run_is_reproducible_from_seed() {
    let trace = |seed: u64| {
        let (mut sim, nodes) = cluster(seed);
        drive_many(&mut sim, &nodes, 70);
        sim.trace_lines()
    };
    let seed = 0x57AB_2001;
    assert_eq!(
        trace(seed),
        trace(seed),
        "snapshot run not reproducible (seed={seed})"
    );
}
