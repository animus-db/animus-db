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
//! extension.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_cp_data::{KIND_CHANGE, KIND_LSI, RaftKvNode, StorageScope, TxnOutcome, TxnWrite, hlc};
use animus_env::{EnvExt, nid};
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
        StorageScope::new(escape(b"users"), KeyRange::whole()),
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
    match node.put_fenced(base.clone(), b"prior".to_vec(), KeyRange::whole()) {
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
        StorageScope::new(escape(b"users"), KeyRange::whole()),
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
        StorageScope::new(escape(b"users"), KeyRange::whole()),
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

/// ADR 0046 A1: resolving a kind-bearing write is fenced whole-or-nothing —
/// if the derived kind keys have (per a split, simulated here by narrowing
/// the group's own scope between stage and resolve) moved off this group's
/// current range, the resolve must reject entirely rather than partially
/// materialize, and the sibling that now owns the range must be able to
/// pick it up from the SAME durable intent (shared engine, ADR 0028).
#[test]
fn resolve_of_a_kind_bearing_write_is_fenced_whole_or_nothing_and_succeeds_on_the_right_sibling() {
    let seed = 0x4600_0005;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let node_id = nid(0);

    let pk = b"erin";
    let token = partition_token(pk);
    let base = logical(pk, b"");
    let lsi = logical(pk, b"\x01lsi");
    let change_prefix = logical(pk, b"\x02");

    // Group A starts owning the WHOLE range (pre-split shape).
    let a: KvNode = RaftKvNode::start_hosted(
        sim.env(node_id.clone()),
        vec![node_id.clone()],
        engine.clone(),
        StorageScope::new(escape(b"users"), KeyRange::whole()),
        1,
    );
    sim.run_for(ELECT);

    let write = kind_bearing_write(pk, b"v5".to_vec(), b"lsi-row-5".to_vec());
    let n = a.clone();
    let (txn_id, record_key, _outcome) = drive(&mut sim, a.env(), SETTLE, async move {
        n.txn_stage_anchor(TABLE, vec![write], Vec::new(), Vec::new())
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage_anchor did not complete (seed={seed})"));

    let n = a.clone();
    let (txn_id_c, record_key_c) = (txn_id.clone(), record_key.clone());
    let commit_ts = drive(&mut sim, a.env(), SETTLE, async move {
        n.txn_commit_at_least(txn_id_c, record_key_c, txn_id.ts)
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("commit did not complete (seed={seed})"));

    // Simulate the split's `NarrowScope`: A now owns only what's strictly
    // below `erin`'s own token — `erin`'s data (base, LSI, change) has
    // moved to a sibling.
    a.narrow_scope(KeyRange::new(Vec::new(), Some(token.to_vec())));

    // A's own resolve is fenced out whole-or-nothing: nothing materializes.
    let n = a.clone();
    let (txn_id_ra, record_key_ra, base_ra) = (txn_id.clone(), record_key.clone(), base.clone());
    let a_resolve = drive(&mut sim, a.env(), SETTLE, async move {
        n.txn_resolve(
            txn_id_ra,
            record_key_ra,
            vec![base_ra],
            TxnOutcome::Committed { commit_ts },
        )
        .await
    })
    .flatten();
    assert!(
        a_resolve.is_some(),
        "the TxnResolve entry itself still applies (whole-or-nothing is an apply-time \
         no-op, not a propose-time rejection) (seed={seed})"
    );
    assert_eq!(
        block_on(a.local_get_kind(KIND_LSI, &lsi)),
        None,
        "A must not materialize a kind write for a key that has moved off its own range \
         (seed={seed})"
    );

    // The sibling now owning `erin`'s token, sharing the SAME engine and
    // prefix (ADR 0028) — including the anchor's own record, which sat
    // inside `erin`'s own token range and moved along with it.
    let b: KvNode = RaftKvNode::start_hosted(
        sim.env(node_id),
        vec![nid(0)],
        engine,
        StorageScope::new(escape(b"users"), KeyRange::new(token.to_vec(), None)),
        2,
    );
    sim.run_for(ELECT);

    let n = b.clone();
    let (txn_id_rb, record_key_rb, base_rb) = (txn_id.clone(), record_key.clone(), base.clone());
    let b_resolve = drive(&mut sim, b.env(), SETTLE, async move {
        n.txn_resolve(
            txn_id_rb,
            record_key_rb,
            vec![base_rb],
            TxnOutcome::Committed { commit_ts },
        )
        .await
    })
    .flatten();
    let resolve_ts =
        b_resolve.unwrap_or_else(|| panic!("B's resolve did not complete (seed={seed})"));

    assert_eq!(
        block_on(b.local_get(&base)),
        Some(b"v5".to_vec()),
        "the correct post-split sibling must resolve the base row (seed={seed})"
    );
    assert_eq!(
        block_on(b.local_get_kind(KIND_LSI, &lsi)),
        Some(b"lsi-row-5".to_vec()),
        "...and materialize the LSI row too, from the SAME durable intent A already staged \
         (seed={seed})"
    );
    let ck = change_key(&change_prefix, resolve_ts);
    assert_eq!(
        block_on(b.local_get_kind(KIND_CHANGE, &ck)),
        Some(b"change-record".to_vec()),
        "...and the change record, keyed at B's own resolve ts (seed={seed})"
    );
}

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

/// A narrower proof than the split scenario above: even when the BASE key
/// alone still falls inside the resolve's own fence, a kind-write key that
/// doesn't must still block the *entire* resolve (whole-or-nothing) —
/// exercising specifically the new `resolved.iter().flatten()` fence
/// coverage this PR adds over `TxnResolve`'s pre-existing base-keys-only
/// check (`txn.rs`'s documented residual: a split's cut point is not
/// token-aligned, so this narrow-but-real edge case must fail SAFE — no
/// partial materialization — rather than silently writing an LSI row this
/// group's own current range doesn't cover).
#[test]
fn a_kind_write_key_outside_fence_blocks_the_whole_resolve_even_though_the_base_key_is_in_fence() {
    let seed = 0x4600_0007;
    let (mut sim, node) = group(seed);
    sim.run_for(ELECT);

    let pk = b"grace";
    let base = logical(pk, b"");
    let lsi = logical(pk, b"\x01lsi");
    let write = kind_bearing_write(pk, b"v7".to_vec(), b"lsi-row-7".to_vec());
    assert!(lsi > base, "test setup: lsi key must sort after base key");

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

    // Narrow so the cut sits strictly BETWEEN the base key and the LSI key
    // — a shape a token-aligned split never produces, but the exact
    // documented residual `txn.rs` flags. `base` is still in fence; `lsi`
    // is not.
    let mut cut = base.clone();
    cut.push(0x01);
    assert!(
        base < cut && cut <= lsi,
        "test setup: cut must separate base from lsi"
    );
    node.narrow_scope(KeyRange::new(Vec::new(), Some(cut)));

    let n = node.clone();
    let (txn_id_r, record_key_r, base_r) = (txn_id.clone(), record_key.clone(), base.clone());
    let resolved = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_resolve(
            txn_id_r,
            record_key_r,
            vec![base_r],
            TxnOutcome::Committed { commit_ts },
        )
        .await
    })
    .flatten();
    assert!(
        resolved.is_some(),
        "the TxnResolve entry itself still applies — the whole-or-nothing rejection is an \
         apply-time no-op (seed={seed})"
    );

    // Note: `local_get` is deliberately NOT the probe for the base key here
    // — it serves a **read-time-resolved** value the moment the record is
    // known-`Committed`, regardless of whether the per-key resolve write
    // itself physically landed (`resolve_once_step`'s doc; see
    // `fenced_commands.rs`'s identical caveat). The LSI scope has no such
    // resolution step (`local_get_kind`'s doc: kind scopes only ever hold
    // committed values, read as-is) — so it is the one probe that can tell
    // "materialized" from "not," and it must show nothing.
    assert_eq!(
        block_on(node.local_get_kind(KIND_LSI, &lsi)),
        None,
        "the LSI row must never materialize once its key is out of this group's fence, even \
         though the base key that carries it is still in fence (seed={seed})"
    );
}

// ---------------------------------------------------------------------------
// ADR 0049 §3 — the TxnStage stage marker (Train A rung 3)
// ---------------------------------------------------------------------------

/// `kind_bearing_write` plus an ADR 0049 §3 stage marker sharing the same
/// change-key prefix, exactly as `animusd::dynamo::eval_kind_txn_write`
/// stages one (the resolve record and the stage marker share one per-item
/// prefix; their apply-completed HLC suffixes keep the keys distinct).
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

    match node.put_fenced(base.clone(), b"prior".to_vec(), KeyRange::whole()) {
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
