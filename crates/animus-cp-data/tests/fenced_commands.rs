//! The crossover-window range **fence** (PR2 of the single-command-split
//! redesign, ADR 0028): every mutating `KvCommand` variant (`Put`/`Batch`/
//! `Delete`/`Cas`) carries a `fence: KeyRange` stamped by the leader at
//! *propose* time. Every replica's apply checks a command's key(s) against
//! the fence **embedded in the entry itself** — never a locally-polled value
//! — so every replica reaches the identical accept/reject decision for a
//! given log entry regardless of how far each has independently progressed
//! learning the tablet's range has changed. Wired to `animusd`'s real CP
//! write paths (`cp_put_local`/`cp_delete_local`/`cp_batch_propose`, which
//! stamp [`RaftKvNode::scope_range`] as the fence — see
//! `animusd/tests/split_fence.rs`); this suite exercises the mechanism
//! directly via the `*_fenced` proposers and the `scope_range` accessor.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`, never
//! `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::KeyRange;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| RaftKvNode::start(sim.env(id), NODES.to_vec(), MemoryEngine::new()))
        .collect();
    (sim, nodes)
}

/// Like [`group`], but every node starts with a real (non-`whole`) scoped
/// `StorageScope` — the shape `animusd`'s `cp_join_host` constructs a tablet's
/// `RaftKvNode` with — so [`RaftKvNode::narrow_scope`]/[`RaftKvNode::scope_range`]
/// exercise the same live-narrowable range a real split narrows.
fn scoped_group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start_scoped(
                sim.env(id),
                NODES.to_vec(),
                MemoryEngine::new(),
                StorageScope::new(b"T:".to_vec(), KeyRange::whole()),
            )
        })
        .collect();
    (sim, nodes)
}

/// Only consider nodes in `live` — a stopped node's core is frozen and keeps
/// reporting its last state, so a halted former leader would otherwise still
/// show `is_leader() == true` and be double-counted alongside its successor.
fn leader(nodes: &[KvNode], live: &[usize], seed: u64) -> usize {
    let ls: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among {live:?}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

/// A fence excluding every key `>= b"m"` — the shape a post-split parent
/// tablet's fence would take.
fn lower_half() -> KeyRange {
    KeyRange::new(Vec::new(), Some(b"m".to_vec()))
}

/// A `put_fenced` whose key falls **outside** its own fence never lands: every
/// replica's apply is a deterministic no-op, exactly as if the write had never
/// been proposed.
#[test]
fn put_outside_its_own_fence_is_a_noop_on_every_replica() {
    let seed = 0xFE1;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    // b"z" >= b"m", so it falls outside `lower_half()`.
    match nodes[l].put_fenced(b"z".to_vec(), b"v".to_vec(), lower_half()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("fenced put rejected at propose time: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"z")),
            None,
            "node {i} applied a write outside its own fence (seed={seed})"
        );
    }
}

/// A `put_fenced` whose key falls **inside** its own fence applies normally —
/// the fence is a restriction, not a blanket rejection.
#[test]
fn put_inside_its_own_fence_applies_normally() {
    let seed = 0xFE2;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    match nodes[l].put_fenced(b"a".to_vec(), b"v".to_vec(), lower_half()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("fenced put rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"a")),
            Some(b"v".to_vec()),
            "node {i} dropped a write inside its own fence (seed={seed})"
        );
    }
}

/// A `delete_fenced` whose key falls outside its fence never tombstones the
/// key — an existing value survives untouched.
#[test]
fn delete_outside_its_own_fence_is_a_noop() {
    let seed = 0xFE3;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    match nodes[l].put(b"z".to_vec(), b"v".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("seed put rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    match nodes[l].delete_fenced(b"z".to_vec(), lower_half()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("fenced delete rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"z")),
            Some(b"v".to_vec()),
            "node {i} lost a value to a delete outside its own fence (seed={seed})"
        );
    }
}

/// A batch's fence gates the **whole** entry: if any key falls outside it,
/// none of the batch applies — a fenced batch is atomic like an unfenced one.
#[test]
fn batch_with_any_key_outside_the_fence_applies_none_of_it() {
    let seed = 0xFE4;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    let puts = vec![
        (b"a".to_vec(), b"in".to_vec()),  // inside lower_half()
        (b"z".to_vec(), b"out".to_vec()), // outside lower_half()
    ];
    match nodes[l].put_batch_fenced(puts, lower_half()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("fenced batch rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"a")),
            None,
            "node {i} applied part of a batch that had an out-of-fence key (seed={seed})"
        );
        assert_eq!(block_on(n.local_get(b"z")), None, "node {i} (seed={seed})");
    }
}

/// A batch whose every key falls inside the fence applies in full.
#[test]
fn batch_with_every_key_inside_the_fence_applies_in_full() {
    let seed = 0xFE5;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    let puts = vec![
        (b"a".to_vec(), b"1".to_vec()),
        (b"b".to_vec(), b"2".to_vec()),
    ];
    match nodes[l].put_batch_fenced(puts, lower_half()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("fenced batch rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(block_on(n.local_get(b"a")), Some(b"1".to_vec()), "node {i}");
        assert_eq!(block_on(n.local_get(b"b")), Some(b"2".to_vec()), "node {i}");
    }
}

/// A `cas_fenced` whose key falls outside its fence never reads or writes
/// storage, and records outcome `false` — the same shape a proposer already
/// handles for an ordinary `expected` mismatch, so a confirm-poll on the
/// entry's index never hangs.
#[test]
fn cas_outside_its_own_fence_records_false_and_does_not_write() {
    let seed = 0xFE6;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    let index = match nodes[l].cas_fenced(b"z".to_vec(), None, b"v".to_vec(), lower_half()) {
        ProposeResult::Accepted { index } => index,
        other => panic!("fenced cas rejected: {other:?} (seed={seed})"),
    };
    sim.run_for(Duration::from_secs(2));

    assert_eq!(
        nodes[l].cas_result(index),
        Some(false),
        "a fenced-out CAS must record false, not hang forever unresolved (seed={seed})"
    );
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"z")),
            None,
            "node {i} let a fenced-out CAS write anyway (seed={seed})"
        );
    }
}

/// A `cas_fenced` whose key falls inside its fence behaves exactly like an
/// unfenced CAS.
#[test]
fn cas_inside_its_own_fence_behaves_normally() {
    let seed = 0xFE7;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    let index = match nodes[l].cas_fenced(b"a".to_vec(), None, b"v".to_vec(), lower_half()) {
        ProposeResult::Accepted { index } => index,
        other => panic!("fenced cas rejected: {other:?} (seed={seed})"),
    };
    sim.run_for(Duration::from_secs(2));

    assert_eq!(nodes[l].cas_result(index), Some(true), "seed={seed}");
    assert_eq!(block_on(nodes[l].local_get(b"a")), Some(b"v".to_vec()));
}

/// The fence is fixed **at propose time** and rides in the committed entry —
/// it is not "reconsidered" once decided, and a *later* command with a wider
/// fence for the same key does not retroactively change an earlier decision.
/// Propose a fenced-out write for `z` (dropped), then have a **different**
/// node — the leader elected after the first one is killed — propose an
/// unfenced write for the same key (applies), and confirm the timeline is
/// exactly two independent per-entry decisions: the first stays dropped, the
/// second applies, on every surviving replica.
#[test]
fn fence_decision_is_per_entry_not_retroactively_reconsidered() {
    let seed = 0xFE8;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let original_leader = leader(&nodes, &[0, 1, 2], seed);

    match nodes[original_leader].put_fenced(b"z".to_vec(), b"dropped".to_vec(), lower_half()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("fenced put rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2)); // replicate + apply the no-op everywhere

    let survivors: Vec<usize> = (0..NODES.len()).filter(|&i| i != original_leader).collect();
    for &i in &survivors {
        assert_eq!(
            block_on(nodes[i].local_get(b"z")),
            None,
            "node {i} applied an out-of-fence write (seed={seed})"
        );
    }

    // Kill the original leader and elect a new one from the survivors.
    sim.stop(original_leader as u64);
    sim.run_for(Duration::from_secs(3));
    let new_leader = leader(&nodes, &survivors, seed);

    // The new leader proposes an unfenced write for the SAME key: this is a
    // brand-new entry, decided independently — it must apply.
    match nodes[new_leader].put(b"z".to_vec(), b"applied".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("unfenced put rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    for &i in &survivors {
        assert_eq!(
            block_on(nodes[i].local_get(b"z")),
            Some(b"applied".to_vec()),
            "node {i}: the second entry's own (unconstrained) fence must apply, \
             independent of the first entry's fenced-out decision (seed={seed})"
        );
    }
}

/// `RaftKvNode::scope_range` (ADR 0028 write-fence wiring, PR2's additive
/// accessor): a caller reads the group's own **live** `StorageScope` range and
/// stamps it as a proposed command's fence — the exact pattern `animusd`'s
/// `cp_put_local`/`cp_delete_local`/`cp_batch_propose` use before proposing a
/// real CP write. Narrow the scope after start (as a real single-command
/// split would on the source tablet's already-hosted `RaftKvNode`, via
/// `narrow_scope`), confirm `scope_range()` reflects it, then confirm a
/// `put_fenced` stamped from that reading applies as a no-op on every replica
/// for a since-handed-off key while a still-owned key applies normally — the
/// narrowed-scope leader's fenced put for an out-of-range key is a no-op
/// everywhere.
#[test]
fn scope_range_reflects_narrowing_and_a_fence_stamped_from_it_gates_apply() {
    let seed = 0xFE9;
    let (mut sim, nodes) = scoped_group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    // Every replica narrows its own scope, as the join-host loop's
    // `narrow_scope` call does on a real split's source tablet.
    for n in &nodes {
        n.narrow_scope(lower_half());
    }
    assert_eq!(
        nodes[l].scope_range(),
        lower_half(),
        "scope_range() must reflect the narrowed range (seed={seed})"
    );

    // Stamp the group's own live scope_range() as the fence, exactly as
    // animusd's write helpers do — an out-of-range key is a deterministic
    // no-op everywhere.
    let fence = nodes[l].scope_range();
    match nodes[l].put_fenced(b"z".to_vec(), b"v".to_vec(), fence.clone()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("fenced put rejected at propose time: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"z")),
            None,
            "node {i} applied a write outside the narrowed scope_range() (seed={seed})"
        );
    }

    // A key still inside the narrowed scope applies normally.
    match nodes[l].put_fenced(b"a".to_vec(), b"v".to_vec(), fence) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("fenced put rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"a")),
            Some(b"v".to_vec()),
            "node {i} dropped a write inside the narrowed scope_range() (seed={seed})"
        );
    }
}
