//! `TxnStage` kind-writes (ADR 0046 "materialize-at-resolve", A1): a
//! transactional write against an indexed/streamed table stages its derived
//! kind-scope rows (LSI/footprint) and change-log record *inside its own
//! base-row intent envelope*, and `KvCommand::TxnResolve`'s commit branch
//! materializes them — via the ONE shared `materialize_derived` helper
//! `KvCommand::KindBatch`'s own apply arm also uses — at the resolve's own
//! locally-minted `ts`. Abort discards them entirely: nothing is ever
//! written to a kind scope for an aborted transaction.
//!
//! This is the primitive-level suite for the mechanism itself; the wire
//! edge (participant-leader evaluation, `run_transact`'s rejection removal)
//! is `animusd`'s PR2, and corpus depth is PR3's `txn_serializable.rs`
//! extension. The issue-#266 residual-window interleaving this file used
//! to pin (a conditioned `KindBatch` — the U3 funnel's now-deleted
//! leader-evaluated-write artifact — racing the stage→resolve window of a
//! transactional write of the same item) is retired as a TOMBSTONE below
//! (ADR 0054 step 4b) — the mechanism it exercised no longer exists; its
//! still-live twin for the mechanism that replaced it is
//! `a_kind_eval_racing_a_still_unresolved_pending_stage_gets_condition_
//! failed_then_succeeds_after_resolve`, added in step 4a. The cross-node
//! wire-level suite is `animusd`'s `dynamo_index_writes.rs`.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_cp_data::{
    KIND_BASE, KIND_CHANGE, KIND_LSI, KindBatchOutcome, KindEvalOp, PendingTxnWrite, RaftKvNode,
    StorageScope, TxnOutcome, TxnWrite, hlc,
};
use animus_env::{EnvExt, nid};
use animus_item::{
    AttributeValue, Comparator, ConditionExpression, Item, LsiDef, PathSegment, Projection,
    TableSchema, UpdateAction, WriteSchema, decode_stored_item, encode_stored_item,
};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, escape, partition_token};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const TABLE: &str = "orders";
const SETTLE: Duration = Duration::from_millis(300);
const ELECT: Duration = Duration::from_secs(2);

/// A real ADR 0022-shaped logical key: `partition_token(pk) || escape(pk) ||
/// rk` — mirrors `kind_batch.rs`'s identical helper. Every kind-write key
/// this suite stages leads with the SAME token as its base key, which is
/// what `TxnStage`'s apply-time token validation requires.
fn logical(pk: &[u8], rk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out.extend_from_slice(rk);
    out
}

/// A single-voter group over the whole keyspace, prefixed like a real
/// table's tablet (`escape(b"users")`, mirroring `kind_batch.rs`).
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

/// Run `fut` to completion by spawning it on `env` and driving `sim` for
/// `budget` — required for every txn propose-and-wait method. Mirrors
/// `txn_multi.rs`/`txn_recovery.rs`/`fenced_commands.rs`'s identical helper.
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

/// The change-log record's completed key for a resolve landing at `ts` —
/// `prefix || hlc::pack(ts)`, exactly as `materialize_derived` (and
/// `KindBatch`'s own arm before it) completes it.
fn change_key(prefix: &[u8], ts: animus_cp_data::hlc::HlcTimestamp) -> Vec<u8> {
    let mut k = prefix.to_vec();
    k.extend_from_slice(&hlc::pack(ts).to_be_bytes());
    k
}

/// One participant's write against an indexed+streamed item: a base
/// put alongside one derived LSI row and one change-log record — the
/// shape a real `dynamo::kind_write_item_at_leader`-style evaluator would
/// stage (ADR 0046 U3), simplified to fixed bytes for this primitive-level
/// suite.
fn kind_bearing_write(pk: &[u8], base_value: Vec<u8>, lsi_value: Vec<u8>) -> TxnWrite {
    let base = logical(pk, b"");
    let lsi = logical(pk, b"\x01lsi");
    let change_prefix = logical(pk, b"\x02");
    TxnWrite {
        key: base,
        value: Some(base_value),
        kind_writes: vec![(KIND_LSI, lsi, Some(lsi_value))],
        change_log: Some((change_prefix, b"change-record".to_vec())),
        stage_marker: None,
        pending: None,
    }
}

#[test]
fn commit_materializes_base_lsi_and_change_record_in_one_entry() {
    let seed = 0x4600_0001;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    let pk = b"alice";
    let base = logical(pk, b"");
    let lsi = logical(pk, b"\x01lsi");
    let change_prefix = logical(pk, b"\x02");
    let write = kind_bearing_write(pk, b"v1".to_vec(), b"lsi-row-1".to_vec());

    let n = node.clone();
    let (txn_id, record_key, outcome) = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor(TABLE, vec![write], Vec::new(), Vec::new())
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage_anchor did not complete (seed={seed})"));
    assert_eq!(outcome, animus_cp_data::StageOutcome::Staged);

    // Before resolve: the kind scopes must be untouched (materialize-at-
    // resolve, never at stage) — the whole point of A1 over the rejected
    // A2 intent-staging shape (ADR 0046 Decision 2).
    assert_eq!(
        block_on(node.local_get_kind(KIND_LSI, &lsi)),
        None,
        "a staged (not yet resolved) kind write must not be visible (seed={seed})"
    );

    let n = node.clone();
    let (txn_id_c, record_key_c) = (txn_id.clone(), record_key.clone());
    let commit_ts = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_commit_at_least(txn_id_c, record_key_c, txn_id.ts)
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_commit_at_least did not complete (seed={seed})"));

    let n = node.clone();
    let (txn_id_r, record_key_r, base_r) = (txn_id.clone(), record_key.clone(), base.clone());
    let resolve_ts = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_resolve(
            txn_id_r,
            record_key_r,
            vec![base_r],
            TxnOutcome::Committed { commit_ts },
        )
        .await
    })
    .flatten()
    .map(|(ts, _outcome)| ts)
    .unwrap_or_else(|| panic!("txn_resolve did not complete (seed={seed})"));

    assert_eq!(
        block_on(node.local_get(&base)),
        Some(b"v1".to_vec()),
        "base row must be committed (seed={seed})"
    );
    assert_eq!(
        block_on(node.local_get_kind(KIND_LSI, &lsi)),
        Some(b"lsi-row-1".to_vec()),
        "LSI row must materialize at resolve, in the same entry as the base (seed={seed})"
    );
    let ck = change_key(&change_prefix, resolve_ts);
    assert_eq!(
        block_on(node.local_get_kind(KIND_CHANGE, &ck)),
        Some(b"change-record".to_vec()),
        "the change record must materialize keyed by the RESOLVE's own ts (ADR 0046 B1), \
         not the stage's ts (seed={seed})"
    );
}

#[test]
fn abort_restores_prior_value_and_materializes_nothing() {
    let seed = 0x4600_0002;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    let pk = b"bob";
    let base = logical(pk, b"");
    let lsi = logical(pk, b"\x01lsi");
    let change_prefix = logical(pk, b"\x02");

    // A prior committed value the abort must restore.
    match node.put(base.clone(), b"prior".to_vec()) {
        animus_control::ProposeResult::Accepted { .. } => {}
        other => panic!("prior put rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(SETTLE);
    assert_eq!(block_on(node.local_get(&base)), Some(b"prior".to_vec()));

    let write = kind_bearing_write(pk, b"v2".to_vec(), b"lsi-row-2".to_vec());
    let n = node.clone();
    let (txn_id, record_key, outcome) = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor(TABLE, vec![write], Vec::new(), Vec::new())
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage_anchor did not complete (seed={seed})"));
    assert_eq!(outcome, animus_cp_data::StageOutcome::Staged);

    let n = node.clone();
    let (txn_id_a, record_key_a) = (txn_id.clone(), record_key.clone());
    drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_abort(txn_id_a, record_key_a).await
    });

    let n = node.clone();
    let (txn_id_r, record_key_r, base_r) = (txn_id.clone(), record_key.clone(), base.clone());
    drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_resolve(txn_id_r, record_key_r, vec![base_r], TxnOutcome::Aborted)
            .await
    });

    assert_eq!(
        block_on(node.local_get(&base)),
        Some(b"prior".to_vec()),
        "abort must restore the value that existed before the intent (seed={seed})"
    );
    assert_eq!(
        block_on(node.local_get_kind(KIND_LSI, &lsi)),
        None,
        "abort must discard the staged kind-writes payload entirely — never materialized \
         (seed={seed})"
    );
    let scanned = block_on(node.local_scan_kind(KIND_CHANGE, &change_prefix, None, None));
    let matching: Vec<_> = scanned
        .into_iter()
        .filter(|(k, _)| k.starts_with(&change_prefix))
        .collect();
    assert!(
        matching.is_empty(),
        "abort must not materialize a change record either (seed={seed}): {matching:?}"
    );
}

#[test]
fn double_resolve_is_idempotent_no_duplicate_change_record() {
    let seed = 0x4600_0003;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    let pk = b"carol";
    let base = logical(pk, b"");
    let change_prefix = logical(pk, b"\x02");
    let write = kind_bearing_write(pk, b"v3".to_vec(), b"lsi-row-3".to_vec());

    let n = node.clone();
    let (txn_id, record_key, _outcome) = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor(TABLE, vec![write], Vec::new(), Vec::new())
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage_anchor did not complete (seed={seed})"));

    let n = node.clone();
    let (txn_id_c, record_key_c) = (txn_id.clone(), record_key.clone());
    let commit_ts = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_commit_at_least(txn_id_c, record_key_c, txn_id.ts)
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("commit did not complete (seed={seed})"));
    let outcome = TxnOutcome::Committed { commit_ts };

    // Resolve twice — as `txn_resolver_loop`'s at-least-once retry would.
    for attempt in 0..2 {
        let n = node.clone();
        let (txn_id_r, record_key_r, base_r, outcome_r) = (
            txn_id.clone(),
            record_key.clone(),
            base.clone(),
            outcome.clone(),
        );
        let resolved = drive(&mut sim, node.env(), SETTLE, async move {
            n.txn_resolve(txn_id_r, record_key_r, vec![base_r], outcome_r)
                .await
        })
        .flatten();
        assert!(
            resolved.is_some(),
            "resolve attempt {attempt} did not complete (seed={seed})"
        );
    }

    let scanned = block_on(node.local_scan_kind(KIND_CHANGE, &change_prefix, None, None));
    let matching: Vec<_> = scanned
        .into_iter()
        .filter(|(k, _)| k.starts_with(&change_prefix))
        .collect();
    assert_eq!(
        matching.len(),
        1,
        "a second resolve of an already-resolved key must not re-materialize the change \
         record (seed={seed}): {matching:?}"
    );
}

#[test]
fn leader_kill_between_stage_and_resolve_recovers_from_the_intent_alone() {
    let seed = 0x4600_0004;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let id = nid(0);
    let node: KvNode = RaftKvNode::start_scoped(
        sim.env(id.clone()),
        vec![id.clone()],
        engine.clone(),
        StorageScope::new(KeyRange::whole()),
    );
    sim.run_for(ELECT);

    let pk = b"dave";
    let base = logical(pk, b"");
    let lsi = logical(pk, b"\x01lsi");
    let change_prefix = logical(pk, b"\x02");
    let write = kind_bearing_write(pk, b"v4".to_vec(), b"lsi-row-4".to_vec());

    let n = node.clone();
    let (txn_id, record_key, _outcome) = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor(TABLE, vec![write], Vec::new(), Vec::new())
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage_anchor did not complete (seed={seed})"));

    let n = node.clone();
    let (txn_id_c, record_key_c) = (txn_id.clone(), record_key.clone());
    let commit_ts = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_commit_at_least(txn_id_c, record_key_c, txn_id.ts)
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("commit did not complete (seed={seed})"));

    // A genuine process restart BEFORE resolve — the WAL survives on the
    // same engine; a fresh `RaftKvNode` replays it from scratch (the stage
    // + commit entries, including the kind-writes/change-log payload
    // opaque inside the intent), exactly as `txn_single.rs`'s
    // `crash_restart_reapplies_stage_commit_resolve_idempotently` does for
    // a plain transaction.
    sim.stop(id.clone());
    let restarted: KvNode = RaftKvNode::start_scoped(
        sim.env(id.clone()),
        vec![id.clone()],
        engine.clone(),
        StorageScope::new(KeyRange::whole()),
    );
    sim.run_for(ELECT);

    // The "resolver loop" issues a fresh resolve knowing only
    // `(txn_id, record_key, keys, outcome)` — never the original payload —
    // and materialization still succeeds, proving the payload survived
    // purely inside the durable intent.
    let n = restarted.clone();
    let (txn_id_r, record_key_r, base_r) = (txn_id.clone(), record_key.clone(), base.clone());
    let resolve_ts = drive(&mut sim, restarted.env(), SETTLE, async move {
        n.txn_resolve(
            txn_id_r,
            record_key_r,
            vec![base_r],
            TxnOutcome::Committed { commit_ts },
        )
        .await
    })
    .flatten()
    .map(|(ts, _outcome)| ts)
    .unwrap_or_else(|| panic!("post-restart resolve did not complete (seed={seed})"));

    assert_eq!(block_on(restarted.local_get(&base)), Some(b"v4".to_vec()));
    assert_eq!(
        block_on(restarted.local_get_kind(KIND_LSI, &lsi)),
        Some(b"lsi-row-4".to_vec()),
        "recovered resolve must still materialize the LSI row from the replayed intent alone \
         (seed={seed})"
    );
    let ck = change_key(&change_prefix, resolve_ts);
    assert_eq!(
        block_on(restarted.local_get_kind(KIND_CHANGE, &ck)),
        Some(b"change-record".to_vec()),
        "recovered resolve must still materialize the change record (seed={seed})"
    );
}

// TOMBSTONE (ADR 0054 step 4b): `a_conditioned_kind_batch_racing_the_
// stage_resolve_window_never_orphans_an_lsi_row` (the issue-#266 residual-
// window regression) died here. It modeled a *non-transactional*
// evaluate-at-leader write (ADR 0046 U3's original design) as a
// `put_kind_batch_conditioned` call carrying the stale bytes that write's
// leader-side evaluator had read, proving `KindBatch.conditions`'
// unresolved-intent arm no-op'd the whole stale batch rather than
// orphaning an LSI row racing a transactional write's stage→resolve
// window of the same item. Both halves of that scenario are gone now, not
// just untestable: step 3 (ADR 0054) already moved every non-transactional
// write off leader-side evaluation onto apply-time `KindEval` before this
// PR, so nothing produces a "stale batch carrying a stale seatbelt" for a
// plain write any more; step 4b then deleted `KindBatch.conditions`
// itself, so `put_kind_batch_conditioned` no longer exists to call. The
// underlying hazard (a stale before-image racing a transactional stage) is
// closed structurally rather than by a byte-level seatbelt for this write
// shape too: `KvCommand::KindEval`'s own apply-time read of the current
// committed value already IS a fresh read, so a "stale diff" cannot be
// constructed to propose in the first place. The still-live twin of this
// scenario for a genuinely evaluated write racing a stage is
// `a_kind_eval_racing_a_still_unresolved_pending_stage_gets_condition_
// failed_then_succeeds_after_resolve` in `tests/txn_kind_writes.rs`'s own
// sibling suite (added in step 4a) — it proves the equivalent invariant
// (an intent blocks a racing `KindEval`, and a fresh retry after resolve
// succeeds) for the mechanism that actually exists now. Pre-deletion cell
// retrievable from git history.

// TOMBSTONE (ADR 0050 Train B rung 2): two cells died here with the
// live-narrowable scope — `resolve_of_a_kind_bearing_write_is_fenced_whole_
// or_nothing_and_succeeds_on_the_right_sibling` and `a_kind_write_key_
// outside_fence_blocks_the_whole_resolve_even_though_the_base_key_is_in_
// fence`. Both simulated a zero-copy split by narrowing a group's scope
// between stage and resolve (shared engine, sibling picks up the SAME
// durable intent) — structurally inexpressible under per-tablet engines
// with immutable ranges. The whole-or-nothing fence check itself
// (`resolved.iter().flatten()`) survives, inert, until the Train B deletion
// sweep; the copy-based split's own txn corpus (rung B4+, fork F7: intents
// COPY to children, resolves chase by key) replaces the moved-off-range
// scenario with the real successor mechanism. Pre-pivot cells retrievable
// from git history.

/// ADR 0046's binding decision: `KindBatch`'s apply arm and `TxnResolve`'s
/// commit branch must share ONE materialization helper, never two
/// independently-maintained copies — this proves it at the observable
/// level: an identical `(kind, key, value)`/change-log payload produces
/// byte-identical stored rows whichever path writes it.
#[test]
fn kind_batch_and_txn_resolve_materialize_byte_identical_rows_for_identical_payloads() {
    let seed = 0x4600_0006;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    let lsi_value = b"same-lsi-row".to_vec();
    let change_value = b"same-change-record".to_vec();

    // Path 1: KindBatch, direct.
    let kb_pk = b"frank-kindbatch";
    let kb_lsi_key = logical(kb_pk, b"\x01lsi");
    match node.put_kind_batch(
        vec![(KIND_LSI, kb_lsi_key.clone(), Some(lsi_value.clone()))],
        Vec::new(),
    ) {
        animus_control::ProposeResult::Accepted { .. } => {}
        other => panic!("KindBatch rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(SETTLE);
    let kb_lsi_stored = block_on(node.local_get_kind(KIND_LSI, &kb_lsi_key))
        .unwrap_or_else(|| panic!("KindBatch LSI row missing (seed={seed})"));

    // Path 2: TxnStage + TxnResolve carrying the identical kind-write value.
    let txn_pk = b"frank-txnresolve";
    let base = logical(txn_pk, b"");
    let txn_lsi_key = logical(txn_pk, b"\x01lsi");
    let write = TxnWrite {
        key: base.clone(),
        value: Some(b"base-value".to_vec()),
        kind_writes: vec![(KIND_LSI, txn_lsi_key.clone(), Some(lsi_value.clone()))],
        change_log: None,
        stage_marker: None,
        pending: None,
    };
    let n = node.clone();
    let (txn_id, record_key, _outcome) = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor(TABLE, vec![write], Vec::new(), Vec::new())
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage_anchor did not complete (seed={seed})"));
    let n = node.clone();
    let (txn_id_c, record_key_c) = (txn_id.clone(), record_key.clone());
    let commit_ts = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_commit_at_least(txn_id_c, record_key_c, txn_id.ts)
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("commit did not complete (seed={seed})"));
    let n = node.clone();
    let (txn_id_r, record_key_r, base_r) = (txn_id.clone(), record_key.clone(), base.clone());
    drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_resolve(
            txn_id_r,
            record_key_r,
            vec![base_r],
            TxnOutcome::Committed { commit_ts },
        )
        .await
    });
    let txn_lsi_stored = block_on(node.local_get_kind(KIND_LSI, &txn_lsi_key))
        .unwrap_or_else(|| panic!("TxnResolve LSI row missing (seed={seed})"));

    assert_eq!(
        kb_lsi_stored, txn_lsi_stored,
        "the two materialization paths must produce byte-identical row values for an \
         identical payload — anything else means `materialize_derived` has drifted into two \
         copies (seed={seed})"
    );
    assert_eq!(kb_lsi_stored, lsi_value);
    // Silence "unused" for the change_value constant kept for future
    // extension of this test to the change-log record too.
    let _ = change_value;
}

fn stage_bearing_write(pk: &[u8], base_value: Vec<u8>, lsi_value: Vec<u8>) -> TxnWrite {
    let change_prefix = logical(pk, b"\x02");
    let mut w = kind_bearing_write(pk, base_value, lsi_value);
    w.stage_marker = Some((change_prefix, b"stage-marker".to_vec()));
    w
}

/// The trailing 8-byte packed-HLC suffix of a completed change-log key.
fn key_hlc_suffix(prefix: &[u8], key: &[u8]) -> u64 {
    assert!(
        key.starts_with(prefix) && key.len() == prefix.len() + 8,
        "change key must be prefix || 8-byte packed HLC: {key:?}"
    );
    u64::from_be_bytes(key[prefix.len()..].try_into().unwrap())
}

/// ADR 0049 §3: staging an intent leaves exactly one image-less stage
/// marker in `KIND_CHANGE`, keyed at the stage entry's own apply-completed
/// HLC — the dirty-key signal ADR 0050's split-build tail re-reads a fresh
/// intent envelope through. Red on the pre-rung-3 apply arm (which wrote
/// nothing into any kind scope at stage time, by ADR 0046 Decision 2 —
/// unchanged for the LSI/change payload, which this marker deliberately is
/// not).
#[test]
fn stage_writes_a_stage_marker_at_the_stage_entrys_own_ts() {
    let seed = 0x4900_0301;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    let pk = b"stage-marker-alice";
    let base = logical(pk, b"");
    let lsi = logical(pk, b"\x01lsi");
    let change_prefix = logical(pk, b"\x02");
    let write = stage_bearing_write(pk, b"v1".to_vec(), b"lsi-row-1".to_vec());

    let n = node.clone();
    let (_txn_id, _record_key, outcome) = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor(TABLE, vec![write], Vec::new(), Vec::new())
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage_anchor did not complete (seed={seed})"));
    assert_eq!(outcome, animus_cp_data::StageOutcome::Staged);

    // The derived kind payload stays intent-carried (materialize-at-resolve,
    // unchanged) — only the stage MARKER lands now.
    assert_eq!(
        block_on(node.local_get_kind(KIND_LSI, &lsi)),
        None,
        "the staged LSI payload must still not be visible at stage time (seed={seed})"
    );
    let scanned = block_on(node.local_scan_kind(KIND_CHANGE, &change_prefix, None, None));
    let matching: Vec<_> = scanned
        .into_iter()
        .filter(|(k, _)| k.starts_with(&change_prefix))
        .collect();
    assert_eq!(
        matching.len(),
        1,
        "staging must leave exactly one stage marker in KIND_CHANGE (seed={seed}): {matching:?}"
    );
    let (marker_key, marker_value) = &matching[0];
    assert_eq!(
        marker_value.as_slice(),
        b"stage-marker",
        "the marker's bytes are the edge-built record, opaque to this crate (seed={seed})"
    );
    // Key shape: prefix || 8-byte packed HLC (apply-completed).
    let _ = key_hlc_suffix(&change_prefix, marker_key);
    // The base intent itself staged as usual.
    assert_eq!(
        block_on(node.local_get(&base)),
        None,
        "a still-pending intent must not read as committed (seed={seed})"
    );
}

/// ADR 0049 §3's ordering claim, asserted: the stage marker's key HLC
/// strictly precedes the resolve-materialized record's — stage applies
/// before resolve in the anchor's own log, and each key completes at its
/// own entry's ts.
#[test]
fn stage_marker_hlc_strictly_precedes_the_resolve_records() {
    let seed = 0x4900_0302;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    let pk = b"stage-marker-order";
    let base = logical(pk, b"");
    let change_prefix = logical(pk, b"\x02");
    let write = stage_bearing_write(pk, b"v1".to_vec(), b"lsi-row-1".to_vec());

    let n = node.clone();
    let (txn_id, record_key, outcome) = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor(TABLE, vec![write], Vec::new(), Vec::new())
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage_anchor did not complete (seed={seed})"));
    assert_eq!(outcome, animus_cp_data::StageOutcome::Staged);

    let n = node.clone();
    let (txn_id_c, record_key_c) = (txn_id.clone(), record_key.clone());
    let stage_ts = txn_id.ts;
    let commit_ts = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_commit_at_least(txn_id_c, record_key_c, stage_ts)
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_commit_at_least did not complete (seed={seed})"));

    let n = node.clone();
    let (txn_id_r, record_key_r, base_r) = (txn_id.clone(), record_key.clone(), base.clone());
    let resolve_ts = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_resolve(
            txn_id_r,
            record_key_r,
            vec![base_r],
            TxnOutcome::Committed { commit_ts },
        )
        .await
    })
    .flatten()
    .map(|(ts, _outcome)| ts)
    .unwrap_or_else(|| panic!("txn_resolve did not complete (seed={seed})"));

    let scanned = block_on(node.local_scan_kind(KIND_CHANGE, &change_prefix, None, None));
    let mut suffixes: Vec<u64> = scanned
        .iter()
        .filter(|(k, _)| k.starts_with(&change_prefix))
        .map(|(k, _)| key_hlc_suffix(&change_prefix, k))
        .collect();
    suffixes.sort_unstable();
    assert_eq!(
        suffixes.len(),
        2,
        "one stage marker + one resolve-materialized record (seed={seed}): {scanned:?}"
    );
    assert!(
        suffixes[0] < suffixes[1],
        "the stage marker must strictly precede the resolve record (seed={seed})"
    );
    assert_eq!(
        suffixes[1],
        hlc::pack(resolve_ts),
        "the later record is the resolve's own, keyed at the resolve entry's ts (seed={seed})"
    );
}

/// An aborted transaction's stage marker remains — deliberately, with no
/// special-casing: it is a dirty-key hint pointing at a row whose envelope
/// reverted, and a change-log consumer re-reads whatever is currently
/// there (the restored prior value), so a stale hint is harmless by the
/// same argument the GSI drain's own idempotent reconciliation makes.
#[test]
fn an_aborted_stages_marker_remains_a_harmless_dirty_hint() {
    let seed = 0x4900_0303;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    let pk = b"stage-marker-abort";
    let base = logical(pk, b"");
    let change_prefix = logical(pk, b"\x02");

    match node.put(base.clone(), b"prior".to_vec()) {
        animus_control::ProposeResult::Accepted { .. } => {}
        other => panic!("prior put rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(SETTLE);

    let write = stage_bearing_write(pk, b"v2".to_vec(), b"lsi-row-2".to_vec());
    let n = node.clone();
    let (txn_id, record_key, outcome) = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor(TABLE, vec![write], Vec::new(), Vec::new())
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage_anchor did not complete (seed={seed})"));
    assert_eq!(outcome, animus_cp_data::StageOutcome::Staged);

    let n = node.clone();
    let (txn_id_a, record_key_a) = (txn_id.clone(), record_key.clone());
    drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_abort(txn_id_a, record_key_a).await
    });
    let n = node.clone();
    let (txn_id_r, record_key_r, base_r) = (txn_id.clone(), record_key.clone(), base.clone());
    drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_resolve(txn_id_r, record_key_r, vec![base_r], TxnOutcome::Aborted)
            .await
    });

    assert_eq!(
        block_on(node.local_get(&base)),
        Some(b"prior".to_vec()),
        "abort must restore the prior value (seed={seed})"
    );
    let scanned = block_on(node.local_scan_kind(KIND_CHANGE, &change_prefix, None, None));
    let matching: Vec<_> = scanned
        .into_iter()
        .filter(|(k, _)| k.starts_with(&change_prefix))
        .collect();
    assert_eq!(
        matching.len(),
        1,
        "exactly the stage marker remains — no resolve record for an abort (seed={seed}): \
         {matching:?}"
    );
    assert_eq!(matching[0].1.as_slice(), b"stage-marker");
}

/// A stage marker whose prefix does not lead with its own write's partition
/// token is rejected whole-or-nothing at apply (`Fenced`), exactly like a
/// mis-tokened kind-write key — the marker key must sit at the same
/// tablet-range position the fence-checked base key does (wire-reachable
/// via `ClientRequest::TxnPrepare`, so validated, never assumed).
#[test]
fn a_stage_marker_prefix_off_its_own_token_is_rejected_at_apply() {
    let seed = 0x4900_0304;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    let pk = b"stage-marker-victim";
    let base = logical(pk, b"");
    let mut write = kind_bearing_write(pk, b"v1".to_vec(), b"lsi-row-1".to_vec());
    // A different partition's token — a change-log row that would land at a
    // range position the entry's fence never checked.
    write.stage_marker = Some((logical(b"some-other-pk", b"\x02"), b"evil".to_vec()));

    let n = node.clone();
    let (_txn_id, _record_key, outcome) = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor(TABLE, vec![write], Vec::new(), Vec::new())
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage_anchor did not complete (seed={seed})"));
    assert_eq!(
        outcome,
        animus_cp_data::StageOutcome::Fenced,
        "a mis-tokened stage marker must reject the whole stage (seed={seed})"
    );
    assert_eq!(
        block_on(node.local_get(&base)),
        None,
        "whole-or-nothing: no intent may land either (seed={seed})"
    );
}

/// The `change_log` twin of the stage-marker rejection above (ADR 0049
/// Train A rung 4): a resolve-time change record's prefix rides the same
/// wire-reachable stage payload, and `TxnResolve` would complete-and-write
/// it wherever it points — so a prefix off its own write's partition token
/// must reject the whole stage at apply (`Fenced`), never be admitted and
/// materialized at resolve. Red before the rung: the stage was admitted
/// (`Staged`) with the mis-tokened prefix riding the intent envelope.
#[test]
fn a_change_log_prefix_off_its_own_token_is_rejected_at_apply() {
    let seed = 0x4900_0405;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    let pk = b"change-log-victim";
    let base = logical(pk, b"");
    let mut write = kind_bearing_write(pk, b"v1".to_vec(), b"lsi-row-1".to_vec());
    // A different partition's token — the resolve record would land at a
    // range position no fence ever checked for this entry.
    write.change_log = Some((logical(b"some-other-pk", b"\x02"), b"evil".to_vec()));

    let n = node.clone();
    let (_txn_id, _record_key, outcome) = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor(TABLE, vec![write], Vec::new(), Vec::new())
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage_anchor did not complete (seed={seed})"));
    assert_eq!(
        outcome,
        animus_cp_data::StageOutcome::Fenced,
        "a mis-tokened change_log prefix must reject the whole stage (seed={seed})"
    );
    assert_eq!(
        block_on(node.local_get(&base)),
        None,
        "whole-or-nothing: no intent may land either (seed={seed})"
    );
}

// ---------------------------------------------------------------------------
// ADR 0054 step 4a: `TxnWrite::pending` — apply-time evaluation of a
// transactional write, extending `KvCommand::KindEval`'s own evaluator to
// `KvCommand::TxnStage`. These scenarios use real `AttributeValue`-keyed
// items (mirroring `kind_eval.rs`'s own helpers) rather than this file's
// opaque-byte `logical`/`kind_bearing_write` helpers above, since a pending
// write's `pk`/`sk` must be real `AttributeValue`s the evaluator can read.
// ---------------------------------------------------------------------------

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_owned())
}

fn n(v: &str) -> AttributeValue {
    AttributeValue::N(v.to_owned())
}

fn av_item(pairs: &[(&str, AttributeValue)]) -> Item {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

/// The `KIND_BASE`/`KIND_LSI` logical key an item's own identity resolves
/// to — matches `RaftKvNode::propose_kind_eval`'s private `kind_eval_base_key`
/// exactly (see `kind_eval.rs`'s identical helper).
fn av_base_key(pk: &AttributeValue, sk: Option<&AttributeValue>) -> Vec<u8> {
    let mut key = partition_token(&animus_item::storage_key(pk, None)).to_vec();
    key.extend_from_slice(&animus_item::storage_key(pk, sk));
    key
}

fn schema_with_lsi() -> WriteSchema {
    WriteSchema {
        key: TableSchema::simple("pk"),
        lsis: vec![LsiDef {
            name: "byAge".to_owned(),
            sort_attribute: "age".to_owned(),
            projection: Projection::All,
        }],
        change_records_carry_images: true,
    }
}

/// Stage `pending` as a fresh anchor transaction and return its
/// `(txn_id, record_key, outcome)` — panics if the stage propose itself
/// never even applies (a routing/leadership failure, not a real scenario
/// here: every test in this section runs a single-voter whole-range group).
fn stage_pending(
    sim: &mut Simulator,
    node: &KvNode,
    key: Vec<u8>,
    pending: PendingTxnWrite,
    seed: u64,
) -> (animus_cp_data::TxnId, Vec<u8>, animus_cp_data::StageOutcome) {
    let write = TxnWrite::pending_eval(key, None, pending);
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor(TABLE, vec![write], Vec::new(), Vec::new())
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage_anchor did not complete (seed={seed})"))
}

/// Commit-then-resolve `txn_id`/`record_key` at `key` — the ordinary
/// success path every pending-eval scenario below drives once it wants the
/// staged intent materialized.
fn commit_and_resolve(
    sim: &mut Simulator,
    node: &KvNode,
    txn_id: animus_cp_data::TxnId,
    record_key: Vec<u8>,
    key: Vec<u8>,
    seed: u64,
) {
    let n = node.clone();
    let (txn_id_c, record_key_c) = (txn_id.clone(), record_key.clone());
    let commit_ts = drive(sim, node.env(), SETTLE, async move {
        n.txn_commit_at_least(txn_id_c, record_key_c, txn_id.ts)
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("commit did not complete (seed={seed})"));
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_resolve(
            txn_id,
            record_key,
            vec![key],
            TxnOutcome::Committed { commit_ts },
        )
        .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("resolve did not complete (seed={seed})"));
}

/// A pending write's own evaluation, once resolved, derives exactly the
/// same base/LSI/change-record rows `KvCommand::KindEval`'s identical
/// evaluator (`kind_eval.rs`'s own differential test) derives for the
/// identical operation — the whole point of sharing one evaluator core.
#[test]
fn pending_eval_stage_resolves_to_the_same_rows_kind_eval_would_derive() {
    let seed = 0x0054_0401;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    let schema = schema_with_lsi();
    let pk = s("henry");
    let new = av_item(&[("pk", pk.clone()), ("age", n("40"))]);
    let key = av_base_key(&pk, None);

    let (txn_id, record_key, outcome) = stage_pending(
        &mut sim,
        &node,
        key.clone(),
        PendingTxnWrite {
            schema: schema.clone(),
            pk: pk.clone(),
            sk: None,
            op: KindEvalOp::Put(new.clone()),
            condition: None,
            ttl_expired: false,
        },
        seed,
    );
    assert_eq!(outcome, animus_cp_data::StageOutcome::Staged, "seed={seed}");
    // Before resolve: the intent stands, no LSI row yet (materialize-at-
    // resolve, ADR 0046 Decision 2).
    assert_eq!(
        block_on(node.local_get_kind(KIND_LSI, &av_base_key(&pk, Some(&n("40"))))),
        None,
        "no kind scope may be touched before resolve (seed={seed})"
    );

    commit_and_resolve(&mut sim, &node, txn_id, record_key, key.clone(), seed);

    let expected = animus_item::derive_kind_writes(
        &schema,
        &pk,
        None,
        &partition_token(&animus_item::storage_key(&pk, None)),
        encode_stored_item(&new),
        None,
        Some(&new),
        false,
        KIND_BASE,
        KIND_LSI,
    );
    for (kind, k, v) in &expected.writes {
        assert_eq!(
            block_on(node.local_get_kind(*kind, k)),
            *v,
            "kind {kind} row must match the same evaluator's own direct derivation (seed={seed})"
        );
    }
    assert_eq!(
        block_on(node.local_get(&key)),
        Some(encode_stored_item(&new)),
        "seed={seed}"
    );
}

/// A pending write's own condition evaluating false rejects the WHOLE
/// stage (`StageOutcome::ConditionFailed`) and stages nothing — the
/// transactional mirror of `kind_eval.rs`'s
/// `a_false_condition_leaves_every_row_untouched_on_every_replica`.
#[test]
fn pending_eval_stage_with_a_false_condition_records_condition_failed_and_stages_nothing() {
    let seed = 0x0054_0402;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    let schema = schema_with_lsi();
    let pk = s("iris");
    let key = av_base_key(&pk, None);
    let original = av_item(&[("pk", pk.clone()), ("age", n("21"))]);

    // Seed the committed starting state directly (no evaluation needed —
    // this is a hand-built `KindBatch`, mirroring `kind_eval.rs`'s own
    // seeding step).
    assert!(matches!(
        node.put_kind_batch(
            vec![(KIND_BASE, key.clone(), Some(encode_stored_item(&original)))],
            Vec::new(),
        ),
        animus_control::ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    let false_condition = ConditionExpression::Compare("age".to_owned(), Comparator::Eq, n("999"));
    let (_txn_id, _record_key, outcome) = stage_pending(
        &mut sim,
        &node,
        key.clone(),
        PendingTxnWrite {
            schema,
            pk: pk.clone(),
            sk: None,
            op: KindEvalOp::Update {
                key_item: av_item(&[("pk", pk.clone())]),
                actions: vec![UpdateAction::Set(
                    vec![PathSegment::Field("age".to_owned())],
                    animus_item::UpdateExpr::value(n("999")),
                )],
            },
            condition: Some(false_condition),
            ttl_expired: false,
        },
        seed,
    );
    match outcome {
        animus_cp_data::StageOutcome::ConditionFailed { key: k } => assert_eq!(k, key),
        other => panic!("expected ConditionFailed, got {other:?} (seed={seed})"),
    }
    assert_eq!(
        block_on(node.local_get(&key)),
        Some(encode_stored_item(&original)),
        "the row must be unchanged by the rejected write (seed={seed})"
    );
}

/// A `KvCommand::KindEval` racing a still-unresolved pending stage on the
/// SAME key sees the foreign intent and reports `ConditionFailed`
/// (`KvCommand::KindEval`'s own foreign-intent discipline, `kind_eval.rs`'s
/// module doc) — never guessing at "the current committed value." Once the
/// transaction resolves, an identical `KindEval` on the same key succeeds.
#[test]
fn a_kind_eval_racing_a_still_unresolved_pending_stage_gets_condition_failed_then_succeeds_after_resolve()
 {
    let seed = 0x0054_0403;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    let schema = schema_with_lsi();
    let pk = s("jack");
    let key = av_base_key(&pk, None);
    let staged_item = av_item(&[("pk", pk.clone()), ("age", n("50"))]);

    let (txn_id, record_key, outcome) = stage_pending(
        &mut sim,
        &node,
        key.clone(),
        PendingTxnWrite {
            schema: schema.clone(),
            pk: pk.clone(),
            sk: None,
            op: KindEvalOp::Put(staged_item.clone()),
            condition: None,
            ttl_expired: false,
        },
        seed,
    );
    assert_eq!(outcome, animus_cp_data::StageOutcome::Staged, "seed={seed}");

    // A racing `KindEval` on the same key, while the stage is still
    // unresolved.
    let other_item = av_item(&[("pk", pk.clone()), ("age", n("51"))]);
    match node.propose_kind_eval(
        schema.clone(),
        pk.clone(),
        None,
        KindEvalOp::Put(other_item.clone()),
        None,
        false,
    ) {
        animus_control::ProposeResult::Accepted { index, .. } => {
            sim.run_for(SETTLE);
            match node.kind_batch_outcome(index) {
                Some((_, KindBatchOutcome::ConditionFailed { key: k })) => assert_eq!(k, key),
                other => panic!(
                    "expected ConditionFailed against the unresolved intent, got {other:?} \
                     (seed={seed})"
                ),
            }
        }
        other => panic!("KindEval propose rejected: {other:?} (seed={seed})"),
    }
    // The foreign write never landed.
    assert_ne!(
        block_on(node.local_get(&key)),
        Some(encode_stored_item(&other_item)),
        "seed={seed}"
    );

    commit_and_resolve(&mut sim, &node, txn_id, record_key, key.clone(), seed);
    assert_eq!(
        block_on(node.local_get(&key)),
        Some(encode_stored_item(&staged_item)),
        "the transaction's own value must have landed (seed={seed})"
    );

    // Now a fresh `KindEval` on the same key succeeds cleanly.
    match node.propose_kind_eval(
        schema,
        pk,
        None,
        KindEvalOp::Put(other_item.clone()),
        None,
        false,
    ) {
        animus_control::ProposeResult::Accepted { index, .. } => {
            sim.run_for(SETTLE);
            assert_eq!(
                node.kind_batch_outcome(index).map(|(_, o)| o),
                Some(KindBatchOutcome::Applied),
                "seed={seed}"
            );
        }
        other => panic!("KindEval propose rejected: {other:?} (seed={seed})"),
    }
    assert_eq!(
        block_on(node.local_get(&key)),
        Some(encode_stored_item(&other_item))
    );
}

/// **Same-txn WAL replay must not double-apply a non-idempotent update.**
/// A pending `ADD` stage's own intent, once merged, sits on the base key;
/// if this crate's driver ever replays the `TxnStage` entry a second time
/// against a still-intact engine (the ordinary "no compaction has run yet,
/// so the persisted applied watermark is behind the log" restart shape —
/// see `leader_kill_between_stage_and_resolve_recovers_from_the_intent_alone`'s
/// identical restart discipline just above), re-evaluating `op` against
/// "the current value" would read the intent's own already-computed new
/// value as if it were the pre-stage baseline and double-add. The apply
/// arm's same-txn-replay branch (`TxnWrite::pending`'s own doc) exists
/// precisely to make this a no-op instead: reuse the intent's own already-
/// computed payload verbatim rather than re-evaluating against it.
#[test]
fn pending_add_stage_survives_a_same_engine_restart_without_double_applying() {
    let seed = 0x0054_0404;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let id = nid(0);
    let node: KvNode = RaftKvNode::start_scoped(
        sim.env(id.clone()),
        vec![id.clone()],
        engine.clone(),
        StorageScope::new(KeyRange::whole()),
    );
    sim.run_for(ELECT);

    let schema = schema_with_lsi();
    let pk = s("karl");
    let key = av_base_key(&pk, None);
    let seed_item = av_item(&[("pk", pk.clone()), ("age", n("10"))]);
    assert!(matches!(
        node.put_kind_batch(
            vec![(KIND_BASE, key.clone(), Some(encode_stored_item(&seed_item)))],
            Vec::new(),
        ),
        animus_control::ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    let (txn_id, record_key, outcome) = stage_pending(
        &mut sim,
        &node,
        key.clone(),
        PendingTxnWrite {
            schema,
            pk: pk.clone(),
            sk: None,
            op: KindEvalOp::Update {
                key_item: av_item(&[("pk", pk.clone())]),
                actions: vec![UpdateAction::Add(
                    vec![PathSegment::Field("age".to_owned())],
                    n("5"),
                )],
            },
            condition: None,
            ttl_expired: false,
        },
        seed,
    );
    assert_eq!(outcome, animus_cp_data::StageOutcome::Staged, "seed={seed}");

    // A genuine restart over the SAME (intact) engine — no compaction has
    // run, so this replays the stage entry again from the WAL, exactly as
    // the pre-existing sibling test above documents.
    sim.stop(id.clone());
    let restarted: KvNode = RaftKvNode::start_scoped(
        sim.env(id.clone()),
        vec![id.clone()],
        engine.clone(),
        StorageScope::new(KeyRange::whole()),
    );
    sim.run_for(ELECT);

    commit_and_resolve(&mut sim, &restarted, txn_id, record_key, key.clone(), seed);

    let final_item = block_on(restarted.local_get(&key))
        .and_then(|bytes| decode_stored_item(&bytes).expect("decode"))
        .unwrap_or_else(|| panic!("row must exist (seed={seed})"));
    assert_eq!(
        final_item.get("age"),
        Some(&n("15")),
        "age must be exactly 10+5 once — a replayed re-evaluation against the intent's own \
         already-computed value would double-add to 20 (seed={seed})"
    );
}
