//! **In-doubt transaction recovery** (ADR 0018 §2, PR5): the "push"
//! protocol that drives a stale `Pending` record to a decision off a
//! crashed (or merely slow) coordinator, and the decision-semantics fix
//! that makes duelling deciders on the anchor's own record a logged no-op
//! rather than an assert.
//!
//! This crate has no network/wire layer of its own (that lives in
//! `animusd`), so `push`/`recovery_resolve` below mirror
//! `animusd::ClientCtx::txn_recover`'s protocol directly over the raw
//! `RaftKvNode` primitives it composes (`txn_record_view`/
//! `txn_verify_staged`/`txn_commit_at_least`/`txn_abort`/`txn_resolve`) —
//! a table-name lookup closure stands in for `cp_route`, mirroring
//! `txn_multi.rs`'s in-test coordinator style.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()`; never `block_on` a call that waits on `env.sleep`
//! internally (every txn propose-and-wait method here) — see `drive`'s doc.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{RaftKvNode, StorageScope, TxnDecisionStatus, TxnId, TxnOutcome};
use animus_env::{Clock, EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::KeyRange;
use futures::executor::block_on;

const GROUP_A: [u64; 3] = [0, 1, 2];
const GROUP_B: [u64; 3] = [10, 11, 12];
const ELECT: Duration = Duration::from_secs(2);
// **Gotcha (see the root `CLAUDE.md`'s Testing section and `ts_cache.rs`'s
// own history): `drive`'s `sim.run_for(budget)` always advances the full
// `budget` regardless of when the future actually completes.** Kept small
// (not the `txn_multi.rs`-style 2s) because this file's grace-boundary
// tests (`push_declines_before_grace_elapses`) care about precisely how
// much sim time has elapsed relative to `RECOVERY_GRACE` — a 2s-per-call
// budget across several sequential `drive` calls would silently burn
// through the whole grace window before the test's own explicit advance
// ever ran. 300ms is comfortably more than one election-timeout-settled
// propose/apply round trip needs.
const SETTLE: Duration = Duration::from_millis(300);
/// Comfortably past `animus_cp_data::RECOVERY_GRACE` (5s).
const PAST_GRACE: Duration = Duration::from_secs(6);

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn scope(prefix: &[u8]) -> StorageScope {
    StorageScope::new(prefix.to_vec(), KeyRange::whole())
}

fn start_group(
    sim: &Simulator,
    ids: &[u64; 3],
    engine: MemoryEngine,
    prefix: &[u8],
) -> Vec<KvNode> {
    ids.iter()
        .map(|&id| {
            RaftKvNode::start_scoped(
                sim.env(nid(id)),
                ids.iter().copied().map(nid).collect(),
                engine.clone(),
                scope(prefix),
            )
        })
        .collect()
}

fn leader(nodes: &[KvNode], seed: u64, label: &str) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one {label} leader, got {ls:?} (seed={seed})"
    );
    ls[0]
}

/// An 8-byte partition token followed by a distinguishing tail (ADR 0022).
fn key(token: u8, tail: &[u8]) -> Vec<u8> {
    let mut k = vec![token; 8];
    k.extend_from_slice(tail);
    k
}

/// Run `fut` to completion by spawning it on `env` and driving `sim` for
/// `budget` — required for every txn propose-and-wait method here, whose
/// future waits on `env.sleep` internally. Mirrors `txn_multi.rs`'s
/// identical helper.
fn drive<T: Send + 'static>(
    sim: &mut Simulator,
    env: &SimEnv,
    budget: Duration,
    fut: impl Future<Output = T> + Send + 'static,
) -> Option<T> {
    let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let s = Arc::clone(&slot);
    env.clone().spawn_task(async move {
        let v = fut.await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock().unwrap().take()
}

fn stage_anchor(
    sim: &mut Simulator,
    node: &KvNode,
    table: &'static str,
    writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    participant_spans: Vec<(String, KeyRange)>,
) -> Option<(TxnId, Vec<u8>)> {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor(table, writes, participant_spans).await
    })
    .flatten()
}

fn stage_participant(
    sim: &mut Simulator,
    node: &KvNode,
    txn_id: TxnId,
    record_key: Vec<u8>,
    record_table: String,
    writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
) -> Option<HlcTimestamp> {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_stage_participant(txn_id, record_key, record_table, writes)
            .await
    })
    .flatten()
}

fn commit_at_least(
    sim: &mut Simulator,
    node: &KvNode,
    txn_id: TxnId,
    record_key: Vec<u8>,
    min_ts: HlcTimestamp,
) -> Option<HlcTimestamp> {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_commit_at_least(txn_id, record_key, min_ts).await
    })
    .flatten()
}

fn abort_only(
    sim: &mut Simulator,
    node: &KvNode,
    txn_id: TxnId,
    record_key: Vec<u8>,
) -> Option<HlcTimestamp> {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_abort(txn_id, record_key).await
    })
    .flatten()
}

fn abort_orphan(
    sim: &mut Simulator,
    node: &KvNode,
    txn_id: TxnId,
    record_key: Vec<u8>,
    created_ts: HlcTimestamp,
) -> Option<HlcTimestamp> {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_abort_orphan(txn_id, record_key, created_ts).await
    })
    .flatten()
}

fn resolve(
    sim: &mut Simulator,
    node: &KvNode,
    txn_id: TxnId,
    record_key: Vec<u8>,
    keys: Vec<Vec<u8>>,
    outcome: TxnOutcome,
) -> Option<HlcTimestamp> {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_resolve(txn_id, record_key, keys, outcome).await
    })
    .flatten()
}

fn status_local(
    sim: &mut Simulator,
    node: &KvNode,
    record_key: Vec<u8>,
) -> Option<TxnDecisionStatus> {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_status_local(&record_key).await
    })
    .flatten()
}

fn record_view(
    sim: &mut Simulator,
    node: &KvNode,
    record_key: Vec<u8>,
) -> Option<animus_cp_data::TxnRecordView> {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_record_view(&record_key).await
    })
    .flatten()
}

fn verify_staged(
    sim: &mut Simulator,
    node: &KvNode,
    span: KeyRange,
    txn_id: TxnId,
) -> Option<bool> {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_verify_staged(&span, &txn_id).await
    })
    .flatten()
}

/// Resolve every `(table, span)` in `intent_spans` per `status` — the test
/// mirror of `ClientCtx::recovery_resolve`. `lookup(table)` stands in for
/// `cp_route`.
fn recovery_resolve(
    sim: &mut Simulator,
    record_key: Vec<u8>,
    txn_id: TxnId,
    intent_spans: &[(String, KeyRange)],
    status: &TxnDecisionStatus,
    lookup: &dyn Fn(&str) -> KvNode,
) {
    let outcome = match status {
        TxnDecisionStatus::Committed { commit_ts } => TxnOutcome::Committed {
            commit_ts: *commit_ts,
        },
        TxnDecisionStatus::Aborted => TxnOutcome::Aborted,
        TxnDecisionStatus::Pending => return,
    };
    for (table, span) in intent_spans {
        let node = lookup(table);
        resolve(
            sim,
            &node,
            txn_id.clone(),
            record_key.clone(),
            vec![span.start.clone()],
            outcome.clone(),
        );
    }
}

/// The test mirror of `ClientCtx::txn_recover` — the full push protocol,
/// steps (a)-(e) of the ADR's PR5 amendment, driven over raw handles.
/// `lookup(table)` stands in for `cp_route`. `intent_ts_hint` is only
/// consulted when the record is absent entirely (ADR 0018 §2/PR5's
/// orphan-record fix, §2b) — the triggering intent's own applied ts,
/// substituting for the `created_ts` a genuine record would have carried;
/// pass `None` when the caller knows a record exists (every ordinary push).
/// Returns the record's actual, final status.
fn push(
    sim: &mut Simulator,
    anchor: &KvNode,
    record_key: Vec<u8>,
    txn_id: TxnId,
    intent_ts_hint: Option<HlcTimestamp>,
    lookup: &dyn Fn(&str) -> KvNode,
) -> TxnDecisionStatus {
    let view = match record_view(sim, anchor, record_key.clone()) {
        Some(v) => v,
        None => {
            // Record-absent path: there is no `created_ts` to grace-check
            // against, so the intent's own applied ts stands in (mirrors
            // `ClientCtx::txn_recover`'s identical branch).
            let hint = intent_ts_hint.expect(
                "a record-absent push must be given the triggering intent's own ts as a grace hint",
            );
            let now_ms = anchor.env().now().0 / 1_000_000;
            if now_ms < hint.wall_ms + animus_cp_data::RECOVERY_GRACE.as_millis() as u64 {
                return TxnDecisionStatus::Pending;
            }
            abort_orphan(sim, anchor, txn_id.clone(), record_key.clone(), hint)
                .expect("orphan-abort proposal accepted");
            let final_view = record_view(sim, anchor, record_key.clone())
                .expect("txn_abort_orphan must have created the tombstone record");
            // A freshly-synthesized tombstone knows nothing of any
            // participant (an absent record never had a chance to record
            // `intent_spans`) — `recovery_resolve` is a no-op here by
            // construction. Resolving the intent that actually triggered
            // this push is the caller's job (mirroring a real reader:
            // `cp_get_local_resolving` resolves its OWN key directly with
            // the returned status, never via the record's `intent_spans`).
            recovery_resolve(
                sim,
                record_key,
                txn_id,
                &final_view.intent_spans,
                &final_view.status,
                lookup,
            );
            return final_view.status;
        }
    };
    if !matches!(view.status, TxnDecisionStatus::Pending) {
        recovery_resolve(
            sim,
            record_key,
            txn_id,
            &view.intent_spans,
            &view.status,
            lookup,
        );
        return view.status;
    }

    // Grace check (liveness-only, ADR 0018 §2/PR5) — mirrors
    // `animusd::ClientCtx::txn_recover`'s identical check.
    let now_ms = anchor.env().now().0 / 1_000_000;
    if now_ms < view.created_ts.wall_ms + animus_cp_data::RECOVERY_GRACE.as_millis() as u64 {
        return TxnDecisionStatus::Pending;
    }

    let mut all_staged = true;
    for (table, span) in &view.intent_spans {
        let node = lookup(table);
        match verify_staged(sim, &node, span.clone(), txn_id.clone()) {
            Some(true) => {}
            _ => all_staged = false,
        }
    }

    if all_staged {
        commit_at_least(
            sim,
            anchor,
            txn_id.clone(),
            record_key.clone(),
            view.created_ts,
        );
    } else {
        abort_only(sim, anchor, txn_id.clone(), record_key.clone());
    }

    let final_view = record_view(sim, anchor, record_key.clone()).expect("record exists");
    recovery_resolve(
        sim,
        record_key,
        txn_id,
        &final_view.intent_spans,
        &final_view.status,
        lookup,
    );
    final_view.status
}

/// (a) Every participant staged, the record sits `Pending` past grace: a
/// push COMMITS, and both keys become visible atomically on every replica
/// of both groups — the headline "coordinator crash is harmless" property.
#[test]
fn push_commits_when_every_participant_staged_and_grace_elapsed() {
    let seed = 0x9A01;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, engine.clone(), b"accounts:");
    sim.run_for(ELECT);

    let la = leader(&nodes_a, seed, "A");
    let lb = leader(&nodes_b, seed, "B");
    let ka = key(1, b":order");
    let kb = key(2, b":balance");
    let mut end_b = kb.clone();
    end_b.push(0);
    let span_b = KeyRange::new(kb.clone(), Some(end_b));

    let (txn_id, record_key) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"placed".to_vec()))],
        vec![("accounts".to_string(), span_b)],
    )
    .expect("anchor stage");
    stage_participant(
        &mut sim,
        &nodes_b[lb],
        txn_id.clone(),
        record_key.clone(),
        "orders".to_string(),
        vec![(kb.clone(), Some(b"debited".to_vec()))],
    )
    .expect("participant stage");

    // Nobody ever decides — simulate a crashed coordinator by simply never
    // calling commit/abort ourselves. Advance well past grace.
    sim.run_for(PAST_GRACE);

    let lookup = |table: &str| -> KvNode {
        match table {
            "orders" => nodes_a[la].clone(),
            "accounts" => nodes_b[lb].clone(),
            other => panic!("unexpected table {other}"),
        }
    };
    let status = push(&mut sim, &nodes_a[la], record_key, txn_id, None, &lookup);
    assert!(
        matches!(status, TxnDecisionStatus::Committed { .. }),
        "expected a recovery commit (seed={seed}), got {status:?}"
    );
    sim.run_for(SETTLE);

    for (i, n) in nodes_a.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&ka)),
            Some(b"placed".to_vec()),
            "group A replica {i} missing the recovered commit (seed={seed})"
        );
    }
    for (i, n) in nodes_b.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&kb)),
            Some(b"debited".to_vec()),
            "group B replica {i} missing the recovered commit (seed={seed})"
        );
    }
}

/// (b) One participant never staged (its stage command is simply never
/// sent — modeling a coordinator that crashed mid-prepare, or a participant
/// process that never received the request): a push ABORTS, and any value
/// the anchor's own key held before the intent is restored everywhere.
#[test]
fn push_aborts_when_a_participant_never_staged() {
    let seed = 0x9A02;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, engine.clone(), b"accounts:");
    sim.run_for(ELECT);

    let la = leader(&nodes_a, seed, "A");
    let lb = leader(&nodes_b, seed, "B");
    let ka = key(1, b":order");
    let kb = key(2, b":balance");
    let mut end_b = kb.clone();
    end_b.push(0);
    let span_b = KeyRange::new(kb.clone(), Some(end_b));

    // The anchor's own record already names `accounts`'s span — the
    // coordinator committed to the participant set up front — but the
    // participant's own stage never lands (crash mid-prepare).
    let (txn_id, record_key) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"placed".to_vec()))],
        vec![("accounts".to_string(), span_b)],
    )
    .expect("anchor stage");

    sim.run_for(PAST_GRACE);

    let lookup = |table: &str| -> KvNode {
        match table {
            "orders" => nodes_a[la].clone(),
            "accounts" => nodes_b[lb].clone(),
            other => panic!("unexpected table {other}"),
        }
    };
    let status = push(&mut sim, &nodes_a[la], record_key, txn_id, None, &lookup);
    assert_eq!(
        status,
        TxnDecisionStatus::Aborted,
        "expected a recovery abort — the participant never staged (seed={seed})"
    );
    sim.run_for(SETTLE);

    for (i, n) in nodes_a.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&ka)),
            None,
            "group A replica {i} should have reverted to absent after a recovery abort \
             (seed={seed})"
        );
    }
    assert_eq!(block_on(nodes_b[lb].local_get(&kb)), None);
}

/// (c) **Recovery-vs-coordinator race**: a recovery abort applies to the
/// record first; the (still-live) coordinator's own subsequent `TxnCommit`
/// proposal is a logged no-op against the already-`Aborted` record, never
/// an assert — and the caller (driving both proposals explicitly) observes
/// the record's actual status is `Aborted`, not what the coordinator asked
/// for.
#[test]
fn a_recovery_abort_beats_a_late_coordinator_commit_with_no_assert() {
    let seed = 0x9A03;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"orders:");
    sim.run_for(ELECT);
    let la = leader(&nodes_a, seed, "A");
    let ka = key(1, b":order");

    let (txn_id, record_key) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"placed".to_vec()))],
        Vec::new(),
    )
    .expect("anchor stage");

    // Recovery decides first: abort.
    let abort_ts = abort_only(&mut sim, &nodes_a[la], txn_id.clone(), record_key.clone())
        .expect("recovery abort proposal accepted");

    // The "still-live coordinator"'s own commit proposal lands after —
    // this must NOT panic (the decision-semantics fix); it applies as a
    // logged no-op.
    let commit_result = commit_at_least(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        txn_id.ts,
    );
    assert!(
        commit_result.is_some(),
        "the commit PROPOSAL itself still succeeds at the Raft level — it's the \
         apply-time decision that no-ops (seed={seed})"
    );

    // The record's actual status is Aborted — the log position (the abort
    // applied first) is the sole arbiter, never which proposal was made.
    let status = status_local(&mut sim, &nodes_a[la], record_key.clone()).expect("record exists");
    assert_eq!(
        status,
        TxnDecisionStatus::Aborted,
        "the abort, having applied first, must win regardless of the later commit \
         proposal (seed={seed}, abort_ts={abort_ts:?})"
    );

    // No process panicked getting here — the assert-avoidance itself is
    // the property under test; a `#[should_panic]`-free green run over
    // both proposals is the proof.
    resolve(
        &mut sim,
        &nodes_a[la],
        txn_id,
        record_key,
        vec![ka.clone()],
        TxnOutcome::Aborted,
    );
    sim.run_for(SETTLE);
    assert_eq!(block_on(nodes_a[la].local_get(&ka)), None);
}

/// (d) **Duelling recoverers**: two conflicting decisions are proposed
/// back-to-back on the same record (zero intervening sim time — mirroring
/// `cross_group_lww.rs`'s in-flight-race technique) — exactly one wins,
/// both the winning and the losing proposer's own view of "what happened"
/// converge on the SAME actual status once they re-read it, and neither
/// proposal panics.
#[test]
fn duelling_recoverers_converge_on_one_decision_with_no_assert() {
    let seed = 0x9A04;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"orders:");
    sim.run_for(ELECT);
    let la = leader(&nodes_a, seed, "A");
    let ka = key(1, b":order");

    let (txn_id, record_key) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"placed".to_vec()))],
        Vec::new(),
    )
    .expect("anchor stage");

    // Two independent recoverers, both convinced of a different outcome
    // (one saw every span staged and decided commit, the other's verify
    // pass raced a transient failure and decided abort), propose within
    // the same tick — zero intervening sim time.
    let commit_ts = commit_at_least(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        txn_id.ts,
    );
    let abort_result = abort_only(&mut sim, &nodes_a[la], txn_id.clone(), record_key.clone());
    assert!(
        commit_ts.is_some(),
        "commit proposal accepted (seed={seed})"
    );
    assert!(
        abort_result.is_some(),
        "abort proposal accepted (seed={seed})"
    );

    // Whichever applied first (log order, deterministic from the seed) is
    // the actual outcome; both recoverers, re-reading, see the identical
    // status — never two different "truths".
    let status1 = status_local(&mut sim, &nodes_a[la], record_key.clone()).expect("record exists");
    let status2 = status_local(&mut sim, &nodes_a[la], record_key.clone()).expect("record exists");
    assert_eq!(
        status1, status2,
        "every recoverer must converge on the identical actual status (seed={seed})"
    );
    assert!(
        matches!(
            status1,
            TxnDecisionStatus::Committed { .. } | TxnDecisionStatus::Aborted
        ),
        "the record must be decided, not still Pending (seed={seed})"
    );
}

/// (e) Grace has NOT elapsed: a push declines (reports `Pending`) without
/// proposing anything — a still-live coordinator's ordinary in-flight
/// transaction is never disturbed.
#[test]
fn push_declines_before_grace_elapses() {
    let seed = 0x9A05;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, engine.clone(), b"accounts:");
    sim.run_for(ELECT);
    let la = leader(&nodes_a, seed, "A");
    let lb = leader(&nodes_b, seed, "B");
    let ka = key(1, b":order");
    let kb = key(2, b":balance");
    let mut end_b = kb.clone();
    end_b.push(0);
    let span_b = KeyRange::new(kb.clone(), Some(end_b));

    let (txn_id, record_key) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"placed".to_vec()))],
        vec![("accounts".to_string(), span_b)],
    )
    .expect("anchor stage");
    stage_participant(
        &mut sim,
        &nodes_b[lb],
        txn_id.clone(),
        record_key.clone(),
        "orders".to_string(),
        vec![(kb.clone(), Some(b"debited".to_vec()))],
    )
    .expect("participant stage");

    // Deliberately NOT advancing past grace — only a brief settle.
    sim.run_for(Duration::from_millis(200));

    let lookup = |table: &str| -> KvNode {
        match table {
            "orders" => nodes_a[la].clone(),
            "accounts" => nodes_b[lb].clone(),
            other => panic!("unexpected table {other}"),
        }
    };
    let status = push(
        &mut sim,
        &nodes_a[la],
        record_key.clone(),
        txn_id,
        None,
        &lookup,
    );
    assert_eq!(
        status,
        TxnDecisionStatus::Pending,
        "a push before grace elapses must decline, not decide (seed={seed})"
    );
    // Still genuinely Pending on the anchor — nothing was proposed.
    let status = status_local(&mut sim, &nodes_a[la], record_key).expect("record exists");
    assert_eq!(status, TxnDecisionStatus::Pending);
}

/// **Orphan record — no record exists anywhere** (ADR 0018 §2/PR5's
/// follow-up fix, §2b): PR4's prepare phase stages every participant
/// concurrently, so a participant's own stage can genuinely land while the
/// *anchor's* own `TxnStage` — which would create the record — never
/// actually writes it (here: the anchor's whole range is sealed first, so
/// its stage entry applies as a whole-or-nothing no-op; `wait_applied` only
/// confirms the ENTRY applied, never that its content check succeeded, the
/// same gap `txn_multi.rs` already documents for a *participant's* stage,
/// now recognized to apply to the anchor's own stage too). A pusher's
/// `txn_record_view` therefore finds NOTHING. Past grace (measured off the
/// triggering intent's own applied ts, since there's no record `created_ts`
/// to read), the pusher must still decide — abort, by CREATING the record in
/// the `Aborted` state directly (`txn_abort_orphan`) — and the caller then
/// resolves the intent that triggered it away, restoring its prior value.
#[test]
fn push_aborts_an_orphan_intent_with_no_record_anywhere() {
    let seed = 0x9A08;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, engine.clone(), b"accounts:");
    sim.run_for(ELECT);

    let la = leader(&nodes_a, seed, "A");
    let lb = leader(&nodes_b, seed, "B");
    let ka = key(1, b":order");
    let kb = key(2, b":balance");
    let mut end_b = kb.clone();
    end_b.push(0);
    let span_b = KeyRange::new(kb.clone(), Some(end_b));

    // `kb` already holds a committed value before the transaction ever
    // starts, so recovery's restore-to-prior-value behavior has something
    // real to prove.
    let seeded = nodes_b[lb].put(kb.clone(), b"prior".to_vec());
    assert!(matches!(seeded, ProposeResult::Accepted { .. }));
    sim.run_for(ELECT);
    assert_eq!(
        block_on(nodes_b[lb].local_get(&kb)),
        Some(b"prior".to_vec())
    );

    // Seal the anchor's whole range FIRST — its own stage entry (the one
    // that would normally create the record) silently no-ops against it at
    // apply, a whole-or-nothing fence/seal miss (`txn_single.rs`'s
    // already-sealed-range shape).
    let sealed = nodes_a[la].propose_seal(nodes_a[la].scope_range());
    assert!(matches!(sealed, ProposeResult::Accepted { .. }));
    sim.run_for(ELECT);

    let (txn_id, record_key) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"placed".to_vec()))],
        vec![("accounts".to_string(), span_b)],
    )
    .expect("the propose itself still applies — it's the content that no-ops");

    assert!(
        record_view(&mut sim, &nodes_a[la], record_key.clone()).is_none(),
        "the anchor's own stage must have no-op'd against the seal, leaving no record \
         at all (seed={seed})"
    );

    // The participant's own stage is NOT sealed, and lands genuinely — the
    // parallel-prepare gap: an intent now exists with no record anywhere to
    // check it against.
    stage_participant(
        &mut sim,
        &nodes_b[lb],
        txn_id.clone(),
        record_key.clone(),
        "orders".to_string(),
        vec![(kb.clone(), Some(b"debited".to_vec()))],
    )
    .expect("participant stage");
    assert_eq!(
        block_on(nodes_b[lb].local_get(&kb)),
        None,
        "the intent hides the prior value until resolved (seed={seed})"
    );

    sim.run_for(PAST_GRACE);

    let lookup = |table: &str| -> KvNode {
        match table {
            "orders" => nodes_a[la].clone(),
            "accounts" => nodes_b[lb].clone(),
            other => panic!("unexpected table {other}"),
        }
    };
    let status = push(
        &mut sim,
        &nodes_a[la],
        record_key.clone(),
        txn_id.clone(),
        Some(txn_id.ts),
        &lookup,
    );
    assert_eq!(
        status,
        TxnDecisionStatus::Aborted,
        "an orphan intent with no record anywhere can only ever be decided abort \
         (seed={seed})"
    );

    // The tombstone `txn_abort_orphan` created is genuinely Aborted, and
    // still knows nothing of any participant — proving `push`'s own
    // `recovery_resolve` pass over it was a no-op, not a silent skip of a
    // real span.
    let tombstone = record_view(&mut sim, &nodes_a[la], record_key.clone())
        .expect("the orphan-abort tombstone must exist");
    assert_eq!(tombstone.status, TxnDecisionStatus::Aborted);
    assert!(tombstone.intent_spans.is_empty());

    // The reader that actually hit the foreign intent resolves it directly
    // against the decided status — exactly what `cp_get_local_resolving`
    // does with `txn_recover`'s return value, never via the tombstone's own
    // (empty) `intent_spans`.
    resolve(
        &mut sim,
        &nodes_b[lb],
        txn_id,
        record_key,
        vec![kb.clone()],
        TxnOutcome::Aborted,
    );
    sim.run_for(SETTLE);
    assert_eq!(
        block_on(nodes_b[lb].local_get(&kb)),
        Some(b"prior".to_vec()),
        "kb must revert to its pre-transaction committed value, never a tombstone or \
         the staged one (seed={seed})"
    );
    // The anchor's own key never existed to begin with (the seal blocked
    // it) — confirm recovery didn't fabricate it.
    assert_eq!(block_on(nodes_a[la].local_get(&ka)), None);
}

/// (f) **Resolver-tracking survives a restart**: `pending_txns` reflects a
/// staged-but-undecided record both before and after this replica's own
/// process restarts (WAL replay recovers the core; the tracker itself is
/// rebuilt from the engine's own durable record, not log replay — see
/// `rebuild_txn_tracker`'s doc). A single-voter group (mirroring
/// `witnessing.rs`'s own restart-recovery idiom): the restarted node is
/// trivially the sole voter again, sidestepping which-of-three-replicas-
/// becomes-leader-again nondeterminism a multi-voter restart would add,
/// which is irrelevant to what this test is actually proving.
#[test]
fn pending_txns_reflects_applies_across_restart() {
    let seed = 0x9A06;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let id = nid(GROUP_A[0]);
    let ka = key(1, b":order");

    let node: KvNode = RaftKvNode::start_scoped(
        sim.env(id.clone()),
        vec![id.clone()],
        engine.clone(),
        scope(b"orders:"),
    );
    sim.run_for(ELECT);

    let (txn_id, record_key) = stage_anchor(
        &mut sim,
        &node,
        "orders",
        vec![(ka.clone(), Some(b"placed".to_vec()))],
        Vec::new(),
    )
    .expect("anchor stage");
    sim.run_for(SETTLE);

    let pending = node.pending_txns();
    assert_eq!(
        pending.get(&txn_id).map(|(rk, _)| rk.clone()),
        Some(record_key.clone()),
        "the freshly-staged record should be tracked as pending (seed={seed})"
    );

    // A genuine process restart (mirrors `witnessing.rs`'s idiom): stop
    // this replica, start a fresh `RaftKvNode` on the SAME engine.
    sim.stop(id.clone());
    let restarted: KvNode = RaftKvNode::start_scoped(
        sim.env(id),
        vec![nid(GROUP_A[0])],
        engine.clone(),
        scope(b"orders:"),
    );
    sim.run_for(ELECT);

    let pending_after = restarted.pending_txns();
    assert_eq!(
        pending_after.get(&txn_id).map(|(rk, _)| rk.clone()),
        Some(record_key.clone()),
        "the rebuild-at-start scan must re-derive the same pending record from the \
         engine's own durable state (seed={seed})"
    );

    // Deciding it now clears it from `pending` (and moves it into
    // `unresolved_decided` until resolved).
    commit_at_least(
        &mut sim,
        &restarted,
        txn_id.clone(),
        record_key.clone(),
        txn_id.ts,
    )
    .expect("commit after restart");
    assert!(
        !restarted.pending_txns().contains_key(&txn_id),
        "a decided record must no longer be tracked as pending (seed={seed})"
    );
    assert!(
        restarted.unresolved_decided().contains_key(&txn_id),
        "a decided-but-unresolved record must be tracked for the resolver (seed={seed})"
    );
    resolve(
        &mut sim,
        &restarted,
        txn_id.clone(),
        record_key,
        vec![ka],
        TxnOutcome::Committed {
            commit_ts: txn_id.ts,
        },
    );
    assert!(
        !restarted.unresolved_decided().contains_key(&txn_id),
        "a resolved record must stop being tracked (seed={seed})"
    );
}

/// Seed sweep of the headline recovery-commit shape.
#[test]
fn recovery_commit_is_reproducible_across_seeds() {
    for seed in [0x9B01u64, 0x9B02, 0x9B03, 0x9B04, 0x9B05] {
        let mut sim = Simulator::new(seed);
        let engine = MemoryEngine::new();
        let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"orders:");
        let nodes_b = start_group(&sim, &GROUP_B, engine.clone(), b"accounts:");
        sim.run_for(ELECT);

        let la = leader(&nodes_a, seed, "A");
        let lb = leader(&nodes_b, seed, "B");
        let ka = key(1, b":order");
        let kb = key(2, b":balance");
        let mut end_b = kb.clone();
        end_b.push(0);
        let span_b = KeyRange::new(kb.clone(), Some(end_b));

        let (txn_id, record_key) = stage_anchor(
            &mut sim,
            &nodes_a[la],
            "orders",
            vec![(ka.clone(), Some(b"placed".to_vec()))],
            vec![("accounts".to_string(), span_b)],
        )
        .expect("anchor stage");
        stage_participant(
            &mut sim,
            &nodes_b[lb],
            txn_id.clone(),
            record_key.clone(),
            "orders".to_string(),
            vec![(kb.clone(), Some(b"debited".to_vec()))],
        )
        .expect("participant stage");

        sim.run_for(PAST_GRACE);
        let lookup = |table: &str| -> KvNode {
            match table {
                "orders" => nodes_a[la].clone(),
                "accounts" => nodes_b[lb].clone(),
                other => panic!("unexpected table {other}"),
            }
        };
        let status = push(&mut sim, &nodes_a[la], record_key, txn_id, None, &lookup);
        assert!(
            matches!(status, TxnDecisionStatus::Committed { .. }),
            "expected a recovery commit (seed={seed}), got {status:?}"
        );
        sim.run_for(SETTLE);
        assert_eq!(
            block_on(nodes_a[la].local_get(&ka)),
            Some(b"placed".to_vec())
        );
        assert_eq!(
            block_on(nodes_b[lb].local_get(&kb)),
            Some(b"debited".to_vec())
        );
    }
}
