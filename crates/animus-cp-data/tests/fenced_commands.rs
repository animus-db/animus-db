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
use animus_cp_data::{KIND_BASE, RaftKvNode, StageOutcome, StorageScope, TxnOutcome};
use animus_env::{EnvExt, nid};
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
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
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
    sim.stop(nid(original_leader as u64));
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
/// accessor): a caller reads the group's own declared `StorageScope` range
/// and stamps it as a proposed command's fence — the exact pattern
/// `animusd`'s `cp_put_local`/`cp_delete_local`/`cp_batch_propose` use before
/// proposing a real CP write. (ADR 0050: the range is immutable from birth —
/// the pre-pivot version of this test narrowed a live scope mid-flight; the
/// fence-gates-apply property it proved is unchanged, now against a group
/// *born* with the narrow declared range.) A `put_fenced` stamped from
/// `scope_range()` applies as a no-op on every replica for a key outside the
/// declared range while an in-range key applies normally.
#[test]
fn a_fence_stamped_from_the_declared_range_gates_apply() {
    let seed = 0xFE9;
    let sim = Simulator::new(seed);
    let nodes: Vec<KvNode> = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start_scoped(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                StorageScope::new(lower_half()),
            )
        })
        .collect();
    let mut sim = sim;
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    assert_eq!(
        nodes[l].scope_range(),
        lower_half(),
        "scope_range() must reflect the declared range (seed={seed})"
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

// ============================================================================
// `KvCommand::TxnResolve`'s own fence (ADR 0018 §2 write-loss amendment —
// Bug 3): the one key-writing command that used to carry no apply-time fence
// check at all. Two tablets of one split table share the same physical
// `StorageEngine` under one `StorageScope` prefix (ADR 0028) — only their
// live `KeyRange` differs — so a resolve misrouted to the wrong tablet's
// `RaftKvNode` would, pre-fix, land directly on the OTHER tablet's own
// physical key. This models exactly that shared-engine shape: two
// single-voter groups, same node id (distinguished only by stream, ADR 0026
// Stage B — the same "several tablets co-resident on one node" shape
// `animusd`'s real hosting path uses), same `StorageScope` prefix, disjoint
// ranges.
// ============================================================================

const TABLE: &str = "t";
/// The boundary between the two modeled tablets: keys `< BOUNDARY` belong to
/// tablet A, `>= BOUNDARY` to tablet B — mirroring `cross_group_lww.rs`'s
/// identical convention.
const BOUNDARY: &[u8] = b"m";
const SETTLE: Duration = Duration::from_millis(300);

/// Two single-voter groups sharing one engine and one `StorageScope` prefix,
/// with disjoint ranges split at [`BOUNDARY`] — the shared-physical-engine
/// shape two tablets of one split table take on a real node. Also returns
/// the shared engine handle directly: `RaftKvNode::local_get` is the wrong
/// probe for this test (it serves a **read-time-resolved** value the moment
/// the anchor's own record is known-committed, regardless of whether the
/// per-key resolve write itself ever physically landed — see
/// `resolve_once_step`'s doc) — only a raw read off the shared engine can
/// distinguish "still a `Pending` intent" from "actually resolved."
fn two_tablets(seed: u64) -> (Simulator, KvNode, KvNode, MemoryEngine, MemoryEngine) {
    let sim = Simulator::new(seed);
    // ADR 0050 rung 1/2: sibling tablets of one table hold their own private
    // engines (the pre-pivot shared-engine construction is gone — under F2b
    // keys, two same-table groups on one engine would collide byte-for-byte).
    let engine_a = MemoryEngine::new();
    let engine_b = MemoryEngine::new();
    let a: KvNode = RaftKvNode::start_hosted(
        sim.env(nid(0)),
        vec![nid(0)],
        engine_a.clone(),
        StorageScope::new(KeyRange::new(Vec::new(), Some(BOUNDARY.to_vec()))),
        1,
    );
    let b: KvNode = RaftKvNode::start_hosted(
        sim.env(nid(0)),
        vec![nid(0)],
        engine_b.clone(),
        StorageScope::new(KeyRange::new(BOUNDARY.to_vec(), None)),
        2,
    );
    (sim, a, b, engine_a, engine_b)
}

/// The envelope tag byte every apply-path write prefixes a value with
/// (`txn.rs`'s doc: `0` = committed, `1` = an intent) — `Envelope`/
/// `decode_envelope` are `pub(crate)`, unreachable from this external
/// `tests/` file, so this reads the tag directly off the raw stored bytes
/// instead (the shape is a stable, documented on-disk contract, not an
/// implementation accident).
#[derive(Debug, PartialEq, Eq)]
enum RawEnvelopeTag {
    Committed,
    Intent,
    Absent,
}

fn raw_envelope_tag(engine: &MemoryEngine, physical_key: &[u8]) -> RawEnvelopeTag {
    use animus_storage::StorageEngine;
    match block_on(engine.get(physical_key)).expect("engine read") {
        None => RawEnvelopeTag::Absent,
        Some(vv) => match vv.value.first() {
            Some(0) => RawEnvelopeTag::Committed,
            Some(1) => RawEnvelopeTag::Intent,
            other => panic!("unexpected envelope tag byte {other:?}"),
        },
    }
}

/// Run `fut` to completion by spawning it on `env` and driving `sim` for
/// `budget` — required for every txn propose-and-wait method (its future
/// polls/sleeps internally, which only advances while the simulator itself
/// is being driven) — mirrors `txn_recovery.rs`/`txn_multi.rs`'s identical
/// helper. **Never `block_on` one of these directly**: with nothing driving
/// `sim`, the future's internal `env.sleep` never resolves and the test
/// hangs forever (caught the hard way — see `docs/engineering-lessons.md`).
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

/// A `TxnResolve` misrouted to the wrong tablet's group — the exact shape
/// `ClientCtx::recovery_resolve`'s pre-fix table-only grouping could produce
/// for a split table — is rejected by its own embedded `fence`, exactly like
/// `Put`/`Batch`/`Delete`/`Cas` above. Under per-tablet engines (ADR 0050)
/// the misroute can no longer corrupt the owning tablet's physical key (the
/// engines are disjoint); the fence's remaining job is keeping the misrouted
/// entry from depositing a spurious row in the *wrong* tablet's own engine —
/// both halves asserted below. The correct tablet's own resolve still
/// succeeds normally.
#[test]
fn txn_resolve_misrouted_to_the_wrong_tablet_is_rejected_by_its_own_fence() {
    let seed = 0xFE10;
    let (mut sim, a, b, engine_a, engine_b) = two_tablets(seed);
    sim.run_for(Duration::from_secs(2)); // elect (single voter, both groups)

    // `b_key` belongs to tablet B's own range (`>= BOUNDARY`) and leads with
    // the 8-byte partition token `txn_stage_anchor` requires (ADR 0022).
    // `physical_key` is a tablet engine's real address for it: `[KIND_BASE]
    // || logical` (F2b — a group's physical key carries the row-kind byte
    // and nothing else, see `RaftKvNode::physical_key`'s doc) — the same
    // bytes in either group's own engine.
    let b_key = {
        let mut k = vec![0xffu8; animus_tablet::TOKEN_BYTES];
        k.extend_from_slice(b"b");
        k
    };
    let physical_key = [[KIND_BASE].as_slice(), b_key.as_slice()].concat();

    let b_env = b.env().clone();
    let n = b.clone();
    let (bk, table) = (b_key.clone(), TABLE);
    let (txn_id, record_key, outcome) = drive(&mut sim, &b_env, SETTLE, async move {
        n.txn_stage_anchor(
            table,
            vec![animus_cp_data::TxnWrite::plain(bk, Some(b"v1".to_vec()))],
            Vec::new(),
            Vec::new(),
        )
        .await
    })
    .flatten()
    .expect("B's anchor stage proposes");
    assert_eq!(
        outcome,
        StageOutcome::Staged,
        "B's own anchor stage must land cleanly (seed={seed})"
    );
    assert_eq!(
        raw_envelope_tag(&engine_b, &physical_key),
        RawEnvelopeTag::Intent,
        "the stage must leave b_key as an unresolved intent in B's own engine (seed={seed})"
    );

    let n = b.clone();
    let (tid, rk) = (txn_id.clone(), record_key.clone());
    let min_ts = txn_id.ts;
    let commit_ts = drive(&mut sim, &b_env, SETTLE, async move {
        n.txn_commit_at_least(tid, rk, min_ts).await
    })
    .flatten()
    .expect("B's own commit applies");
    let committed = TxnOutcome::Committed { commit_ts };
    assert_eq!(
        raw_envelope_tag(&engine_b, &physical_key),
        RawEnvelopeTag::Intent,
        "committing the anchor's own record must not itself touch b_key — only a \
         real per-key resolve does (seed={seed})"
    );

    // The misroute: resolve `b_key` through A's group. `txn_resolve` always
    // proposes successfully (the entry is accepted into A's own log
    // regardless of what it contains) — the fence rejection happens at
    // apply, not propose.
    let a_env = a.env().clone();
    let n = a.clone();
    let (tid, rk, keys, oc) = (
        txn_id.clone(),
        record_key.clone(),
        vec![b_key.clone()],
        committed.clone(),
    );
    let misrouted = drive(&mut sim, &a_env, SETTLE, async move {
        n.txn_resolve(tid, rk, keys, oc).await
    })
    .flatten();
    assert!(
        misrouted.is_some(),
        "a misrouted resolve still proposes/applies as an entry — the fence gates \
         its effect on the key, not whether the entry itself lands (seed={seed})"
    );
    assert_eq!(
        raw_envelope_tag(&engine_b, &physical_key),
        RawEnvelopeTag::Intent,
        "B's intent must be untouched by a resolve applied in a different group \
         (structural under private engines) (seed={seed})"
    );
    assert_eq!(
        raw_envelope_tag(&engine_a, &physical_key),
        RawEnvelopeTag::Absent,
        "A's own fence excludes b_key — the misrouted resolve must not deposit a \
         spurious row in A's own engine either (seed={seed})"
    );

    // The correct resolve, from B's own group (whose fence covers b_key),
    // still succeeds normally.
    let n = b.clone();
    let keys = vec![b_key.clone()];
    let correct = drive(&mut sim, &b_env, SETTLE, async move {
        n.txn_resolve(txn_id, record_key, keys, committed).await
    })
    .flatten();
    assert!(
        correct.is_some(),
        "B's own correct resolve applies (seed={seed})"
    );
    assert_eq!(
        raw_envelope_tag(&engine_b, &physical_key),
        RawEnvelopeTag::Committed,
        "the correctly-routed resolve must still land normally (seed={seed})"
    );
    assert_eq!(
        block_on(b.local_get(&b_key)),
        Some(b"v1".to_vec()),
        "and read back the right value through the normal read path (seed={seed})"
    );
}
