//! `RaftKvNode::local_scan_kind_snapshot` (ADR 0059 §4/§5) — the on-demand
//! backup capture driver's own read primitive, at the `animus-cp-data`
//! level (the driver itself is `animusd`, a later PR; this proves the
//! primitive it rests on in isolation).
//!
//! Three properties, each load-bearing for the capture driver built on top:
//!
//! 1. **Snapshot-pinned**: a write committed *after* the caller's own
//!    `version_ceiling` is invisible, exactly as if the read had happened
//!    the instant the ceiling was taken — the property that lets a capture
//!    spanning many ticks (and, across a leader change, many different
//!    replicas) always resolve the identical row set.
//! 2. **Never a raw envelope**: an unresolved (`Pending`) transaction
//!    intent at or below the ceiling is silently omitted, never surfaced as
//!    its tagged envelope bytes — ADR 0059 §5's "a backup holds only
//!    committed values" rule.
//! 3. **Resumable pagination**: a small `limit` still reconstructs the
//!    exact same row set as one large call, via the returned cursor.
//!
//! Deterministic and seed-reproducible (ADR 0003), though these particular
//! scenarios don't depend on Raft timing at all beyond electing a leader.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_cp_data::{KIND_BASE, RaftKvNode, StorageScope, TxnWrite};
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, escape, partition_token};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const ELECT: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_millis(300);

fn logical(pk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out
}

fn group(seed: u64) -> (Simulator, KvNode) {
    let sim = Simulator::new(seed);
    let node: KvNode = RaftKvNode::start_scoped(
        sim.env(nid(0)),
        vec![nid(0)],
        MemoryEngine::new(),
        StorageScope::new(KeyRange::whole()),
    );
    (sim, node)
}

fn drive<T: Send + 'static>(
    sim: &mut Simulator,
    env: &SimEnv,
    budget: Duration,
    fut: impl std::future::Future<Output = T> + Send + 'static,
) -> Option<T> {
    let slot: std::sync::Arc<std::sync::Mutex<Option<T>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let s = std::sync::Arc::clone(&slot);
    env.clone().spawn_task(async move {
        let v = fut.await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock().unwrap().take()
}

fn put(sim: &mut Simulator, node: &KvNode, pk: &[u8], value: &[u8]) {
    let key = logical(pk);
    let result = node.put_kind_batch(vec![(KIND_BASE, key, Some(value.to_vec()))], Vec::new());
    assert!(
        matches!(result, animus_control::ProposeResult::Accepted { .. }),
        "put rejected: {result:?}"
    );
    sim.run_for(SETTLE);
}

#[test]
fn local_scan_kind_snapshot_pins_a_version_and_never_surfaces_a_pending_intent() {
    let seed = 0x0059_5c01;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    // A: committed before the pin.
    put(&mut sim, &node, b"a-before", b"vA");
    // B: staged as a transaction intent, deliberately never resolved — a
    // live `Pending` envelope at (or below) the eventual ceiling.
    let b_key = logical(b"b-pending");
    let write = TxnWrite {
        key: b_key.clone(),
        value: Some(b"vB-staged".to_vec()),
        kind_writes: Vec::new(),
        change_log: None,
        stage_marker: None,
        pending: None,
    };
    let n = node.clone();
    let staged = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor("t", vec![write], Vec::new(), Vec::new())
            .await
    })
    .flatten();
    assert!(staged.is_some(), "seed={seed}: txn_stage_anchor must land");
    // C: committed before the pin, after B.
    put(&mut sim, &node, b"c-before", b"vC");

    let ceiling = node.engine_latest_version();

    // D: committed strictly after the pin — must never appear.
    put(&mut sim, &node, b"d-after", b"vD-late");

    let (rows, cursor) = block_on(node.local_scan_kind_snapshot(KIND_BASE, &[], ceiling, 100));
    assert!(
        cursor.is_none(),
        "seed={seed}: a limit above the row count must exhaust the scope"
    );
    let seen: BTreeSet<(Vec<u8>, Vec<u8>)> = rows
        .iter()
        .map(|(k, v, _)| (k.clone(), v.clone()))
        .collect();
    assert_eq!(
        seen,
        BTreeSet::from([
            (logical(b"a-before"), b"vA".to_vec()),
            (logical(b"c-before"), b"vC".to_vec()),
        ]),
        "seed={seed}: exactly the two committed-and-at-or-below-ceiling rows, \
         never the pending intent's key and never the post-ceiling write — \
         got {rows:?}"
    );
    assert!(
        rows.iter().all(|(k, ..)| k != &b_key),
        "seed={seed}: the pending intent's own key must never surface, resolved or not"
    );
    for (_, value, _) in &rows {
        assert!(
            value != b"vB-staged",
            "seed={seed}: a raw intent's staged value must never leak through as if committed"
        );
    }
}

#[test]
fn local_scan_kind_snapshot_pagination_reconstructs_the_same_set_as_one_large_call() {
    let seed = 0x0059_5c02;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    let pks: Vec<Vec<u8>> = (0..9).map(|i| format!("row{i:03}").into_bytes()).collect();
    for (i, pk) in pks.iter().enumerate() {
        put(&mut sim, &node, pk, format!("v{i}").as_bytes());
    }
    let ceiling = node.engine_latest_version();

    let (whole, whole_cursor) =
        block_on(node.local_scan_kind_snapshot(KIND_BASE, &[], ceiling, pks.len()));
    assert!(
        whole_cursor.is_none(),
        "seed={seed}: one call covers everything"
    );
    assert_eq!(whole.len(), pks.len(), "seed={seed}: {whole:?}");

    // Now the same sweep in small chunks, resuming from each returned cursor.
    let mut chunked = Vec::new();
    let mut start = Vec::new();
    let mut calls = 0usize;
    loop {
        calls += 1;
        assert!(calls <= pks.len() + 2, "seed={seed}: pagination looped");
        let (rows, next) = block_on(node.local_scan_kind_snapshot(KIND_BASE, &start, ceiling, 2));
        chunked.extend(rows);
        match next {
            Some(k) => start = k,
            None => break,
        }
    }
    assert!(
        calls > 1,
        "seed={seed}: a 9-row scope at limit 2 must take more than one call"
    );

    let whole_set: BTreeSet<(Vec<u8>, Vec<u8>, u64)> = whole.into_iter().collect();
    let chunked_set: BTreeSet<(Vec<u8>, Vec<u8>, u64)> = chunked.into_iter().collect();
    assert_eq!(
        whole_set, chunked_set,
        "seed={seed}: chunked pagination must reconstruct byte-identical rows \
         (key, value, and version) to one large call"
    );
}

#[test]
fn local_scan_kind_snapshot_of_an_unknown_kind_is_empty() {
    let seed = 0x0059_5c03;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);
    put(&mut sim, &node, b"x", b"v");
    let ceiling = node.engine_latest_version();
    let (rows, cursor) = block_on(node.local_scan_kind_snapshot(200, &[], ceiling, 10));
    assert!(rows.is_empty());
    assert!(cursor.is_none());
}
