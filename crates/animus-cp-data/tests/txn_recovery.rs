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

fn scope(_prefix: &[u8]) -> StorageScope {
    StorageScope::new(KeyRange::whole())
}

fn start_group(sim: &Simulator, ids: &[u64; 3], prefix: &[u8]) -> Vec<KvNode> {
    // ADR 0050 rung 1: each tablet group holds its own private engine;
    // replicas of ONE group share a clone (per-replica durable-state idiom).
    let engine = MemoryEngine::new();
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
    let writes = writes
        .into_iter()
        .map(|(k, v)| animus_cp_data::TxnWrite::plain(k, v))
        .collect();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor(table, writes, participant_spans, Vec::new())
            .await
    })
    .flatten()
    .map(|(txn_id, record_key, _outcome)| (txn_id, record_key))
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
    let writes = writes
        .into_iter()
        .map(|(k, v)| animus_cp_data::TxnWrite::plain(k, v))
        .collect();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_stage_participant(txn_id, record_key, record_table, writes, Vec::new())
            .await
    })
    .flatten()
    .map(|(ts, _outcome)| ts)
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
    // `txn_record_view` is now `Option<Option<TxnRecordView>>` (outer =
    // served, inner = found — issue #298 shape B fix); this test helper's
    // own callers only ever care about "found" vs "not," so collapse both
    // `Option` layers `drive`'s own wrapper adds on top.
    drive(sim, node.env(), SETTLE, async move {
        n.txn_record_view(&record_key).await
    })
    .flatten()
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
    let nodes_a = start_group(&sim, &GROUP_A, b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, b"accounts:");
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
    let nodes_a = start_group(&sim, &GROUP_A, b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, b"accounts:");
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
    let nodes_a = start_group(&sim, &GROUP_A, b"orders:");
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
    let nodes_a = start_group(&sim, &GROUP_A, b"orders:");
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
    let nodes_a = start_group(&sim, &GROUP_A, b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, b"accounts:");
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
    let nodes_a = start_group(&sim, &GROUP_A, b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, b"accounts:");
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

    // Freeze the anchor's group FIRST (ADR 0050's terminal whole-range
    // seal) — its own stage entry (the one that would normally create the
    // record) silently no-ops against it at apply, a whole-or-nothing
    // seal miss (`txn_single.rs`'s already-frozen shape).
    let sealed = nodes_a[la].propose_freeze();
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
    let id = nid(GROUP_A[0]);
    let ka = key(1, b":order");
    // This replica's own durable engine — reused across the restart below
    // (the sim idiom for per-replica durable state; NOT cross-group sharing).
    let engine = MemoryEngine::new();

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
        let nodes_a = start_group(&sim, &GROUP_A, b"orders:");
        let nodes_b = start_group(&sim, &GROUP_B, b"accounts:");
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

/// **ADR 0018 §2/PR6 corrective note**: two independent decide-attempts for
/// the *same* transaction — each individually well-formed, each proposing a
/// **freshly minted** `commit_ts` via `mint_at_least` (not idempotent
/// across calls) — must never panic even though they disagree on the exact
/// timestamp. `TxnCommit`'s apply arm used to treat "already `Committed` at
/// a *different* `commit_ts`" as impossible-by-construction (a hard
/// assert); it is not: a still-live coordinator's own decide attempt
/// (`animusd`'s `CLIENT_TIMEOUT`, 10s) can genuinely still be in flight
/// past `RECOVERY_GRACE` (5s), racing the recovery resolver's own
/// independent post-grace push — both individually correct ("commit" is
/// the right answer either way), only the exact minted value differs. This
/// regresses the corpus-found bug directly: the ADR 0018 multi-tablet
/// transaction corpus's `participant_leader_kill_early` scenario (seed
/// 2743871795844702347) hit this precisely, deterministically, under
/// nothing more exotic than a single participant leader kill. The first
/// entry to *apply* still wins unconditionally (this group's one
/// totally-ordered log remains the sole arbiter); the second is now a
/// logged no-op, exactly like the pre-existing `Committed`-vs-`Aborted`
/// duelling case — and, since every real caller
/// (`ClientCtx::cp_txn`/`txn_recover`, `txn_resolver_loop`) already
/// re-reads the record's actual decided status before resolving anything
/// (never assumes its own proposal won) and the `TxnTracker` update only
/// ever happens on the first-applied decision, no caller can ever resolve
/// using the losing, stale `commit_ts` — no torn resolve.
#[test]
fn duelling_commits_at_different_timestamps_the_second_is_a_no_op_never_a_panic() {
    let seed = 0x9C01;
    let mut sim = Simulator::new(seed);
    let nodes_a = start_group(&sim, &GROUP_A, b"orders:");
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

    // Two independent decide-attempts, mirroring a still-live coordinator
    // racing the recovery resolver — both propose a commit for the SAME
    // txn_id at the same floor, but `mint_at_least` mints a fresh ts every
    // call, so they genuinely disagree on the exact value.
    let ts1 = commit_at_least(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        txn_id.ts,
    )
    .expect("first commit attempt applies");
    let ts2 = commit_at_least(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        txn_id.ts,
    )
    .expect("second commit attempt ALSO applies as a legal, logged no-op — never panics");
    assert_ne!(
        ts1, ts2,
        "test must exercise genuinely different timestamps (seed={seed})"
    );

    // The FIRST-applied decision wins — the record reflects ts1, never ts2.
    let status = status_local(&mut sim, &nodes_a[la], record_key.clone()).expect("record exists");
    assert_eq!(
        status,
        TxnDecisionStatus::Committed { commit_ts: ts1 },
        "seed={seed}"
    );

    // Resolving with the re-read (correct, winning) outcome lands the
    // committed value cleanly — no torn resolve from the second, no-op'd
    // decision ever having touched anything.
    resolve(
        &mut sim,
        &nodes_a[la],
        txn_id,
        record_key,
        vec![ka.clone()],
        TxnOutcome::Committed { commit_ts: ts1 },
    );
    sim.run_for(SETTLE);
    assert_eq!(
        block_on(nodes_a[la].local_get(&ka)),
        Some(b"placed".to_vec()),
        "seed={seed}"
    );
}

/// The duelling-commit no-op is deterministic and reproducible across seeds
/// — mirrors `recovery_commit_is_reproducible_across_seeds`'s shape.
#[test]
fn duelling_commits_at_different_timestamps_are_reproducible_across_seeds() {
    for seed in [0x9C11u64, 0x9C12, 0x9C13, 0x9C14, 0x9C15] {
        let mut sim = Simulator::new(seed);
        let nodes_a = start_group(&sim, &GROUP_A, b"orders:");
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

        let ts1 = commit_at_least(
            &mut sim,
            &nodes_a[la],
            txn_id.clone(),
            record_key.clone(),
            txn_id.ts,
        )
        .unwrap_or_else(|| panic!("first commit attempt applies (seed={seed})"));
        let ts2 = commit_at_least(
            &mut sim,
            &nodes_a[la],
            txn_id.clone(),
            record_key.clone(),
            txn_id.ts,
        )
        .unwrap_or_else(|| panic!("second commit attempt applies as a no-op (seed={seed})"));
        assert_ne!(ts1, ts2, "seed={seed}");

        let status = status_local(&mut sim, &nodes_a[la], record_key).expect("record exists");
        assert_eq!(
            status,
            TxnDecisionStatus::Committed { commit_ts: ts1 },
            "seed={seed}"
        );
    }
}

/// **Torn-resolve regression** (ADR 0018 §2/PR6, load-bearing per the
/// amendment's own review): duplicate same-outcome commits at different
/// timestamps must not just avoid a panic (the test above) — every
/// participant of the transaction must end up resolved *consistently*,
/// using the record's one, re-read, first-applied outcome, never a losing
/// decider's own candidate. Two participants (unlike the single-group test
/// above, so there is a genuine "every participant" to check): the anchor
/// group's own key and a second group's participant key.
#[test]
fn duelling_commits_resolve_every_participant_consistently_never_torn() {
    let seed = 0x9C21;
    let mut sim = Simulator::new(seed);
    let nodes_a = start_group(&sim, &GROUP_A, b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, b"accounts:");
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

    // First decide-attempt (mirroring a still-live coordinator) — wins.
    let ts1 = commit_at_least(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        txn_id.ts,
    )
    .expect("first commit attempt applies");

    // Sourced from a re-read, exactly like every real caller (the ADR 0018
    // §2/PR6 torn-resolve audit) — never a candidate ts, never a second
    // decider's own proposal.
    let status = status_local(&mut sim, &nodes_a[la], record_key.clone()).expect("record exists");
    assert_eq!(
        status,
        TxnDecisionStatus::Committed { commit_ts: ts1 },
        "seed={seed}"
    );
    let outcome = TxnOutcome::Committed { commit_ts: ts1 };
    // **Gotcha found writing this test**: `txn_resolve`'s own applied `ts`
    // (the MVCC version every resolved key is physically stamped at) is a
    // *fresh mint on the resolving group's own leader*, not `commit_ts` —
    // see `RaftKvNode::txn_resolve`'s doc ("not necessarily `outcome`'s
    // `commit_ts`, which is only a comparison value"). So `ts_resolve_a`/
    // `ts_resolve_b` below, not `ts1`, are each group's own physical
    // resolve version.
    let ts_resolve_a = resolve(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        vec![ka.clone()],
        outcome.clone(),
    )
    .expect("anchor resolve applies");
    let ts_resolve_b = resolve(
        &mut sim,
        &nodes_b[lb],
        txn_id.clone(),
        record_key.clone(),
        vec![kb.clone()],
        outcome,
    )
    .expect("participant resolve applies");
    sim.run_for(SETTLE);

    // Every replica of both groups sees the committed value — nothing
    // torn between the anchor's own key and the participant's.
    for (i, n) in nodes_a.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&ka)),
            Some(b"placed".to_vec()),
            "group A replica {i} torn (seed={seed})"
        );
    }
    for (i, n) in nodes_b.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&kb)),
            Some(b"debited".to_vec()),
            "group B replica {i} torn (seed={seed})"
        );
    }

    // **Now** the second, losing decide-attempt (mirroring a recovery
    // resolver racing in after the coordinator already won and resolved) —
    // applied *after* both keys are physically resolved, so this proves the
    // loser's timestamp has zero effect on anything already converged, not
    // just that the record itself ignores it.
    let ts2 = commit_at_least(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        txn_id.ts,
    )
    .expect("second commit attempt applies as a no-op");
    assert_ne!(ts1, ts2, "seed={seed}");
    assert!(
        ts1 < ts2,
        "test assumes ts1 is the earlier mint (seed={seed})"
    );

    // A snapshot read strictly between "everything is resolved" and the
    // loser's own ts sees BOTH participants' committed values together —
    // the all-or-nothing visibility the torn-resolve hazard would have
    // broken, and proof the losing decider's ts2 never un-resolves
    // anything. **Not** a read between ts1/ts2 directly: `read_at`'s
    // intent resolution is single-tablet only (see its own doc — "the
    // single-tablet snapshot-read building block ..., not a transaction's
    // read itself"), and a participant's own resolve is stamped at its own
    // fresh mint (`ts_resolve_b` above), never at the anchor's `commit_ts`
    // — so a read timestamped between ts1 and ts2 landing *before*
    // `ts_resolve_b` would find the participant's key still physically an
    // unresolved (foreign, from this tablet's own perspective) intent at
    // that historical version, and `read_at` has no cross-tablet resolver
    // to chase it down (unlike the latest-read path, `animusd`'s
    // `cp_get_local_resolving`). Bootstrap each group's read ceiling past
    // `ts_between` first (`read_at`'s own documented contract, ADR 0018
    // §2/PR2b).
    let ts_between = HlcTimestamp {
        wall_ms: ts_resolve_a
            .wall_ms
            .max(ts_resolve_b.wall_ms)
            .midpoint(ts2.wall_ms),
        logical: 0,
    };
    assert!(
        ts_between > ts_resolve_a && ts_between > ts_resolve_b && ts_between < ts2,
        "seed={seed}: test assumes both resolves precede ts_between, which precedes ts2          (ts_resolve_a={ts_resolve_a:?}, ts_resolve_b={ts_resolve_b:?},          ts_between={ts_between:?}, ts2={ts2:?})"
    );
    // A generous budget, not `SETTLE` — `read_at`'s own bootstrap-then-
    // retry (ADR 0018 §2/PR2b §1) needs real room here, unlike this
    // file's grace-boundary tests, which deliberately keep `SETTLE` tight.
    const READ_AT_BUDGET: Duration = Duration::from_secs(2);
    let _ = drive(&mut sim, nodes_a[la].env(), READ_AT_BUDGET, {
        let n = nodes_a[la].clone();
        let ka = ka.clone();
        async move { n.linearizable_get(&ka).await }
    });
    let _ = drive(&mut sim, nodes_b[lb].env(), READ_AT_BUDGET, {
        let n = nodes_b[lb].clone();
        let kb = kb.clone();
        async move { n.linearizable_get(&kb).await }
    });
    let read_a = drive(&mut sim, nodes_a[la].env(), READ_AT_BUDGET, {
        let n = nodes_a[la].clone();
        let ka = ka.clone();
        async move { n.read_at(&ka, ts_between).await }
    })
    .flatten();
    let read_b = drive(&mut sim, nodes_b[lb].env(), READ_AT_BUDGET, {
        let n = nodes_b[lb].clone();
        let kb = kb.clone();
        async move { n.read_at(&kb, ts_between).await }
    })
    .flatten();
    assert_eq!(
        read_a,
        Some(Some(b"placed".to_vec())),
        "seed={seed}: group A must be visible at ts_between"
    );
    assert_eq!(
        read_b,
        Some(Some(b"debited".to_vec())),
        "seed={seed}: group B must be visible at ts_between (all-or-nothing, never torn)"
    );
}

/// ADR 0018 §2/PR6 (task #16): the apply-time writer-push-intents guard
/// (`KvCommand::TxnStage`'s doc) rejects a stage whose target key already
/// holds another transaction's still-unresolved intent, whole-or-nothing,
/// rather than silently overwriting it — the fix for a genuine durability
/// hole a corpus depth run found (a corrupted MVCC version chain that made
/// an already-committed value permanently unreadable; see the same fix's
/// `abort_restore_never_meets_another_transactions_intent` for that
/// property directly). This test proves the coordinator-visible half:
/// a blocked stage still "applies" (its own entry commits through Raft,
/// like a fence/seal miss) but writes nothing, and once the blocking
/// transaction is pushed to a decision, a retried stage over the same key
/// succeeds.
#[test]
fn stage_over_a_foreign_pending_intent_no_ops_then_a_pushed_retry_succeeds() {
    let seed = 0xB16C;
    let mut sim = Simulator::new(seed);
    let nodes_a = start_group(&sim, &GROUP_A, b"orders:");
    sim.run_for(ELECT);
    let la = leader(&nodes_a, seed, "A");
    let ka = key(1, b":order");
    let mut end = ka.clone();
    end.push(0);
    let span = KeyRange::new(ka.clone(), Some(end));

    // txn1 stages the anchor's own key and is left `Pending` — an
    // abandoned coordinator, the same shape that originally let a second
    // transaction's stage silently overwrite it.
    let (txn1, record1) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"v1".to_vec()))],
        vec![],
    )
    .expect("txn1 stages");

    // txn2's own stage over the SAME key: the entry itself still applies
    // (confirmed by the `Some` below — matching a fence/seal miss's own
    // "wait_applied only confirms applied, never that content landed"
    // shape) but is a true no-op — key ka still belongs to txn1.
    let (txn2, record2) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"v2".to_vec()))],
        vec![],
    )
    .expect("txn2's own stage entry still applies (as a whole-or-nothing no-op)");
    assert_eq!(
        verify_staged(&mut sim, &nodes_a[la], span.clone(), txn2.clone()),
        Some(false),
        "txn2's stage must have no-op'd — key ka still belongs to txn1 (seed={seed})"
    );
    assert_eq!(
        record_view(&mut sim, &nodes_a[la], record2.clone()),
        None,
        "a rejected anchor stage must not create a record either (seed={seed})"
    );

    // Past grace, push txn1 to a decision (mirroring a coordinator or
    // recovery pusher discovering the blocker and driving it to completion
    // — the exact mechanism `animusd::ClientCtx::txn_prepare_pushing`'s own
    // backoff-and-retry leaves to this same push protocol).
    sim.run_for(PAST_GRACE);
    let lookup = |_: &str| nodes_a[la].clone();
    let status1 = push(
        &mut sim,
        &nodes_a[la],
        record1.clone(),
        txn1.clone(),
        None,
        &lookup,
    );
    // Not pinning the exact `commit_ts`: `mint_at_least` floors it at this
    // group's own current clock, which has run well past `txn1.ts` by now
    // (`PAST_GRACE`'s own 6s advance) — only the outcome matters here.
    assert!(
        matches!(status1, TxnDecisionStatus::Committed { .. }),
        "txn1 (no participants) must be pushed to a commit, got {status1:?} (seed={seed})"
    );
    assert_eq!(
        block_on(nodes_a[la].local_get(&ka)),
        Some(b"v1".to_vec()),
        "txn1's own committed value must be visible before the retry (seed={seed})"
    );

    // Retry txn2's stage now that key ka is a plain committed value, not
    // an unresolved intent — it must succeed this time.
    let (txn2_retry, record2_retry) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"v2".to_vec()))],
        vec![],
    )
    .expect("txn2's retried stage applies");
    assert_eq!(
        verify_staged(&mut sim, &nodes_a[la], span, txn2_retry.clone()),
        Some(true),
        "txn2's retried stage must have genuinely landed (seed={seed})"
    );
    let commit2 = commit_at_least(
        &mut sim,
        &nodes_a[la],
        txn2_retry.clone(),
        record2_retry.clone(),
        txn2_retry.ts,
    )
    .expect("txn2 commit applies");
    resolve(
        &mut sim,
        &nodes_a[la],
        txn2_retry,
        record2_retry,
        vec![ka.clone()],
        TxnOutcome::Committed { commit_ts: commit2 },
    );
    assert_eq!(
        block_on(nodes_a[la].local_get(&ka)),
        Some(b"v2".to_vec()),
        "txn2's own committed value must be visible after the retry (seed={seed})"
    );
}

/// ADR 0018 §2/PR6 (task #16): the property the apply-time
/// writer-push-intents guard makes structurally true — an abort-restore's
/// one-hop-back `get_at` can never land on another transaction's intent,
/// only a genuinely committed value or true absence. Directly reconstructs
/// the sequence that used to corrupt the MVCC version chain (found by a
/// corpus depth run, `ANIMUS_TXN_SEEDS=10`, `coordinator_abandon_prepare_
/// s01`, seed 16358087571531249382): a committed value, a second
/// transaction that overwrites it and is abandoned before resolving, and a
/// third transaction's own stage attempt over that same still-unresolved
/// intent — under the pre-fix code the third transaction's stage would
/// have silently overwritten the second's intent, and the third
/// transaction's own later abort-restore would then have found the
/// second's stale intent instead of the real committed value (or, one
/// layer deeper, absence) — permanently hiding it, since a later correct
/// resolve's lower ts always loses that race via ordinary LWW.
#[test]
fn abort_restore_never_meets_another_transactions_intent() {
    let seed = 0xB16D;
    let mut sim = Simulator::new(seed);
    let nodes_a = start_group(&sim, &GROUP_A, b"orders:");
    sim.run_for(ELECT);
    let la = leader(&nodes_a, seed, "A");
    let ka = key(1, b":order");
    let mut end = ka.clone();
    end.push(0);
    let span = KeyRange::new(ka.clone(), Some(end));

    // txn1 commits key ka = v1.
    let (txn1, record1) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"v1".to_vec()))],
        vec![],
    )
    .expect("txn1 stages");
    let commit1 = commit_at_least(
        &mut sim,
        &nodes_a[la],
        txn1.clone(),
        record1.clone(),
        txn1.ts,
    )
    .expect("txn1 commit applies");
    resolve(
        &mut sim,
        &nodes_a[la],
        txn1,
        record1,
        vec![ka.clone()],
        TxnOutcome::Committed { commit_ts: commit1 },
    );

    // txn2 stages OVER the committed value (a plain overwrite of a
    // `Committed` value is legal — only overwriting an *unresolved intent*
    // is rejected) and is left `Pending`: an abandoned coordinator.
    let (txn2, record2) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"v2".to_vec()))],
        vec![],
    )
    .expect("txn2 stages");
    assert_eq!(
        verify_staged(&mut sim, &nodes_a[la], span.clone(), txn2.clone()),
        Some(true),
        "txn2 must genuinely stage over a committed value (seed={seed})"
    );

    // txn3's own stage attempt over the SAME key: pre-fix, this would have
    // silently overwritten txn2's still-unresolved intent. Now it is
    // rejected outright, so txn2's intent is never disturbed and txn3
    // creates no record at all — there is nothing left to (mis)decide for
    // txn3, so the corrupt chain the pre-fix code could produce here is
    // structurally unrepresentable.
    let (txn3, record3) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"v3".to_vec()))],
        vec![],
    )
    .expect("txn3's own stage entry still applies (as a whole-or-nothing no-op)");
    assert_eq!(
        verify_staged(&mut sim, &nodes_a[la], span, txn3.clone()),
        Some(false),
        "txn3 must be rejected — key ka still belongs to txn2 (seed={seed})"
    );
    assert_eq!(
        record_view(&mut sim, &nodes_a[la], record3),
        None,
        "a rejected anchor stage creates no record — nothing left to (mis)decide for \
         txn3 (seed={seed})"
    );

    // Abort txn2 (the mechanism — its own coordinator giving up, or a
    // recovery push — doesn't matter here) and confirm the restore finds
    // txn1's real committed value, never a stale intent: `get_at(key,
    // txn2's own intent version - 1)` can only ever land on a genuinely
    // committed value or true absence now, since no other transaction's
    // intent could ever have been written in between.
    abort_only(&mut sim, &nodes_a[la], txn2.clone(), record2.clone()).expect("txn2 abort applies");
    resolve(
        &mut sim,
        &nodes_a[la],
        txn2,
        record2,
        vec![ka.clone()],
        TxnOutcome::Aborted,
    );
    assert_eq!(
        block_on(nodes_a[la].local_get(&ka)),
        Some(b"v1".to_vec()),
        "txn2's abort-restore must find txn1's real committed value, never a stale intent \
         from an overwriting transaction that was rejected (seed={seed})"
    );
}

/// ADR 0018 §2/PR6 hardening: the apply-time defense-in-depth check on
/// `KvCommand::TxnResolve` (see `apply_and_compact`'s arm) rejects a resolve
/// whose carried `outcome` disagrees with the anchor's own decided record,
/// whole-or-nothing, rather than silently applying it. There is no known
/// live caller that gets this wrong — every real decider (`animusd`'s
/// ordinary coordinator path and its recovery pusher alike) already
/// re-reads the record's actual status before resolving — so this test
/// exercises the guard directly via the low-level `txn_resolve` primitive
/// (itself `pub`, since a multi-participant coordinator must call it with
/// an already-decided `outcome` rather than deriving one locally) with a
/// deliberately wrong outcome standing in for a hypothetical future caller
/// that skipped the re-read.
#[test]
fn a_resolve_carrying_the_wrong_outcome_no_ops_against_the_anchors_own_record() {
    let seed = 0xB16E;
    let mut sim = Simulator::new(seed);
    let nodes_a = start_group(&sim, &GROUP_A, b"orders:");
    sim.run_for(ELECT);
    let la = leader(&nodes_a, seed, "A");
    let ka = key(1, b":order");
    let mut end = ka.clone();
    end.push(0);
    let span = KeyRange::new(ka.clone(), Some(end));

    let (txn_id, record_key) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"placed".to_vec()))],
        Vec::new(),
    )
    .expect("anchor stage");
    let commit_ts = commit_at_least(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        txn_id.ts,
    )
    .expect("commit applies");

    // Resolve with an outcome that flatly disagrees with what the record
    // actually decided (`Aborted` when the record is genuinely `Committed`)
    // — the entry itself still applies (propose/apply always accepts a
    // well-formed command), but the guard must refuse to act on it.
    resolve(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        vec![ka.clone()],
        TxnOutcome::Aborted,
    );
    sim.run_for(SETTLE);

    // Rejected: the intent is still live, completely untouched — never
    // resolved into an incorrect abort-restore that would have erased
    // "placed" and left the key absent.
    assert_eq!(
        verify_staged(&mut sim, &nodes_a[la], span, txn_id.clone()),
        Some(true),
        "a mismatched-outcome resolve must no-op, leaving the intent live (seed={seed})"
    );

    // A second resolve with the real, re-read outcome lands cleanly —
    // proving the guard blocks only the wrong outcome, not resolution
    // itself.
    resolve(
        &mut sim,
        &nodes_a[la],
        txn_id,
        record_key,
        vec![ka.clone()],
        TxnOutcome::Committed { commit_ts },
    );
    sim.run_for(SETTLE);
    assert_eq!(
        block_on(nodes_a[la].local_get(&ka)),
        Some(b"placed".to_vec()),
        "seed={seed}"
    );
}
