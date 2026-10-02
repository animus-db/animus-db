//! Regression: `KvCommand::TxnResolve`'s apply-time outcome channel (ADR
//! 0018 §2 write-loss amendment §3/§6). `RaftKvNode::txn_resolve`'s only
//! signal used to be `wait_applied(index).await.then_some(ts)` — "did this
//! entry apply," never "did it actually resolve anything." A whole-or-
//! nothing fence-miss no-op (a concurrent split moves the target key's
//! range out from under the caller's routing decision between `cp_route`
//! and this entry's actual apply) satisfied that signal exactly as well as
//! a genuine resolve, so the proposer had no way to learn its own resolve
//! never took effect — the root cause the amendment traces a real captured
//! `ProdEnv` soak trace to (§1: "resolve reports success but the intent
//! stays live") and names, unfixed, as its own §3/§4.
//!
//! This file reproduces the ambiguity directly and proves the fix, using
//! the identical two-group anchor/participant shape `tests/txn_multi.rs`
//! already uses (a real in-place split only needs to move ONE group's own
//! range, so a genuine multi-tablet transaction is the faithful
//! reproduction — a single-group anchor-only transaction's own resolve, by
//! contrast, can reconstruct its value on a plain read purely from the
//! locally-held decided record + intent even when the physical resolve
//! never lands, which would mask exactly the symptom under test here; see
//! `RaftKvNode::resolve_decided`'s doc for that read-time reconstruction).
//! The **participant**'s own tablet forks via a real in-place split
//! (`KvCommand::SplitTablet`, which reuses `Freeze`'s whole-range seal for
//! its ordering fence — ADR 0058) between the anchor's commit and the
//! participant's own resolve. The resolve must report
//! `ResolveOutcome::Fenced`, distinguishable from `ResolveOutcome::
//! Resolved`, and the participant's own key must stay genuinely
//! unresolved — invisible to a plain local read on that tablet (it holds
//! no copy of the anchor's record to reconstruct the value from) — exactly
//! the "looks lost" shape a caller must not mistake for done.
//!
//! **Scope note**: this crate has no routing/metadata layer of its own (see
//! `tests/txn_recovery.rs`'s identical disclaimer) — the coordinator-side
//! half of the fix (re-route with fresh metadata and retry on `Fenced`,
//! `animusd::ClientCtx::txn_resolve_participant_retrying`) is exercised at
//! the `animusd`-level wire tests instead; this file proves only the
//! primitive that fix depends on: that the outcome channel itself
//! correctly distinguishes the two cases.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{RaftKvNode, ResolveOutcome, StageOutcome, StorageScope, TxnOutcome};
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, SplitChild, TabletId};
use futures::executor::block_on;

const GROUP_A: [u64; 3] = [0, 1, 2];
const GROUP_B: [u64; 3] = [10, 11, 12];
/// Comfortably more than one election-timeout-settled propose/apply round
/// trip needs — mirrors `txn_multi.rs`/`txn_recovery.rs`'s identical const.
const SETTLE: Duration = Duration::from_millis(300);

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn scope(_prefix: &[u8]) -> StorageScope {
    StorageScope::new(KeyRange::whole())
}

fn start_group(sim: &Simulator, ids: &[u64; 3], prefix: &[u8]) -> Vec<KvNode> {
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

/// Two children the fork mints — their replica sets never need to be
/// reachable/hosted for this file's purposes (no host reconciler runs
/// here, and this crate's own apply arm never bootstraps them — see
/// `animus-cp-data/CLAUDE.md`'s lib.rs API entry): proving the fork's own
/// fence/outcome mechanics needs nothing more than the fork committing.
fn test_children() -> [SplitChild; 2] {
    [
        SplitChild {
            id: TabletId(2),
            replicas: vec![nid(100), nid(101), nid(102)],
        },
        SplitChild {
            id: TabletId(3),
            replicas: vec![nid(103), nid(104), nid(105)],
        },
    ]
}

/// The negative control: an ordinary resolve (no split racing it) reports
/// `Resolved`, and the committed value actually lands — proving the new
/// channel doesn't just always say `Fenced`, or always say `Resolved`.
#[test]
fn an_ordinary_resolve_reports_resolved_and_the_value_lands() {
    let seed = 0x5245_5301; // "RES01" loosely
    let mut sim = Simulator::new(seed);
    let a = start_group(&sim, &GROUP_A, b"a");
    sim.run_for(Duration::from_secs(2));
    let la = leader(&a, seed, "anchor");

    let anchor_key = key(1, b"anchor");
    let n = a[la].clone();
    let (txn_id, record_key, outcome) = drive(&mut sim, a[la].env(), SETTLE, {
        let anchor_key = anchor_key.clone();
        async move {
            n.txn_stage("t", vec![(anchor_key, Some(b"v1".to_vec()))])
                .await
        }
    })
    .flatten()
    .unwrap_or_else(|| panic!("stage did not complete (seed={seed})"));
    assert_eq!(outcome, StageOutcome::Staged, "seed={seed}");

    let n = a[la].clone();
    let (txn_id_c, record_key_c) = (txn_id.clone(), record_key.clone());
    let commit_ts = drive(&mut sim, a[la].env(), SETTLE, async move {
        n.txn_commit_at_least(txn_id_c, record_key_c, txn_id.ts)
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("commit did not complete (seed={seed})"));

    let n = a[la].clone();
    let (txn_id_r, record_key_r, key_r) = (txn_id.clone(), record_key.clone(), anchor_key.clone());
    let resolved = drive(&mut sim, a[la].env(), SETTLE, async move {
        n.txn_resolve(
            txn_id_r,
            record_key_r,
            vec![key_r],
            TxnOutcome::Committed { commit_ts },
        )
        .await
    })
    .flatten();

    let (_, resolve_outcome) =
        resolved.unwrap_or_else(|| panic!("resolve entry did not even apply (seed={seed})"));
    assert_eq!(
        resolve_outcome,
        ResolveOutcome::Resolved,
        "an ordinary resolve with no fence race must report Resolved (seed={seed})"
    );

    for (i, n) in a.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&anchor_key)),
            Some(b"v1".to_vec()),
            "replica {i}: a genuine resolve must make the committed value visible (seed={seed})"
        );
    }
}

/// The fix under test: a **participant**'s own resolve, proposed against a
/// tablet that forked (via a real in-place split) between the anchor's
/// commit and this resolve's actual apply, reports `Fenced` — never an
/// indistinguishable `Some(ts)` "success" — and the participant's own key
/// stays genuinely unresolved: invisible to a plain local read on that
/// tablet, exactly the "looks lost" shape a caller must not mistake for
/// done.
#[test]
fn a_participants_resolve_racing_a_split_reports_fenced_not_a_false_success() {
    let seed = 0x5245_5302; // "RES02" loosely
    let mut sim = Simulator::new(seed);
    let a = start_group(&sim, &GROUP_A, b"a");
    let b = start_group(&sim, &GROUP_B, b"b");
    sim.run_for(Duration::from_secs(2));
    let la = leader(&a, seed, "anchor");
    let lb = leader(&b, seed, "participant");

    let anchor_key = key(1, b"anchor");
    let participant_key = key(2, b"participant");

    // Stage the anchor's own key, naming the participant's span so the
    // record's `intent_spans` reflect a genuine multi-participant
    // transaction (mirrors `txn_multi.rs`'s own shape) — not load-bearing
    // for this file's own assertions, but keeps the scenario realistic.
    let mut participant_span_end = participant_key.clone();
    participant_span_end.push(0);
    let n = a[la].clone();
    let (txn_id, record_key, outcome) = drive(&mut sim, a[la].env(), SETTLE, {
        let anchor_key = anchor_key.clone();
        let participant_key = participant_key.clone();
        async move {
            n.txn_stage_anchor(
                "ta",
                vec![animus_cp_data::TxnWrite::plain(
                    anchor_key,
                    Some(b"a1".to_vec()),
                )],
                vec![(
                    "tb".to_string(),
                    KeyRange::new(participant_key, Some(participant_span_end)),
                )],
                Vec::new(),
            )
            .await
        }
    })
    .flatten()
    .unwrap_or_else(|| panic!("anchor stage did not complete (seed={seed})"));
    assert_eq!(outcome, StageOutcome::Staged, "seed={seed}");

    let n = b[lb].clone();
    let (txn_id_p, record_key_p) = (txn_id.clone(), record_key.clone());
    let (participant_ts, p_outcome) = drive(&mut sim, b[lb].env(), SETTLE, {
        let participant_key = participant_key.clone();
        async move {
            n.txn_stage_participant(
                txn_id_p,
                record_key_p,
                "ta".to_string(),
                vec![animus_cp_data::TxnWrite::plain(
                    participant_key,
                    Some(b"b1".to_vec()),
                )],
                Vec::new(),
            )
            .await
        }
    })
    .flatten()
    .unwrap_or_else(|| panic!("participant stage did not complete (seed={seed})"));
    assert_eq!(p_outcome, StageOutcome::Staged, "seed={seed}");

    let n = a[la].clone();
    let (txn_id_c, record_key_c) = (txn_id.clone(), record_key.clone());
    let candidate = txn_id.ts.max(participant_ts);
    let commit_ts = drive(&mut sim, a[la].env(), SETTLE, async move {
        n.txn_commit_at_least(txn_id_c, record_key_c, candidate)
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("anchor commit did not complete (seed={seed})"));

    // The fork lands on the PARTICIPANT's own group BEFORE its resolve is
    // proposed — exactly the race the amendment describes: "the target
    // tablet's range had shifted (a concurrent split) between the
    // coordinator's `cp_route` lookup and the entry's actual apply." A real
    // production coordinator would have captured its routing decision (a
    // `CpGroup` handle to this same group) before the fork too; nothing
    // about calling `txn_resolve` directly on this handle afterward is
    // unrealistic — it's exactly what a stale `animusd::ClientCtx::
    // txn_resolve_participant` call would do.
    match b[lb].propose_split_tablet(b"z".to_vec(), test_children()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("split-tablet not accepted: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
    for (i, n) in b.iter().enumerate() {
        assert!(
            n.is_frozen(),
            "participant replica {i} did not freeze after the fork applied (seed={seed})"
        );
    }

    // The resolve every participant replica now fence-misses on (the
    // fork's whole-range seal covers every key, not just those past the
    // split point — see `KvCommand::Freeze`'s doc, which `SplitTablet`
    // reuses verbatim).
    let n = b[lb].clone();
    let (txn_id_r, record_key_r, key_r) =
        (txn_id.clone(), record_key.clone(), participant_key.clone());
    let resolved = drive(&mut sim, b[lb].env(), SETTLE, async move {
        n.txn_resolve(
            txn_id_r,
            record_key_r,
            vec![key_r],
            TxnOutcome::Committed { commit_ts },
        )
        .await
    })
    .flatten();

    let (_, resolve_outcome) =
        resolved.unwrap_or_else(|| panic!("resolve entry did not even apply (seed={seed})"));
    assert_eq!(
        resolve_outcome,
        ResolveOutcome::Fenced,
        "a participant's resolve racing a split on its own tablet must report Fenced, never an \
         ambiguous success — this is the exact gap ADR 0018's write-loss amendment §3/§6 names \
         (seed={seed})"
    );

    // Not silently marked resolved, and genuinely invisible on this
    // tablet: the participant group holds no local copy of the anchor's
    // record (it lives on GROUP_A), so `local_get` here can only ever
    // report `Foreign`-and-therefore-absent for a still-`Pending`-tagged
    // intent (`RaftKvNode::resolve_once_step`'s doc) — exactly the "looks
    // lost" shape a naive reader would see. This is what makes the outcome
    // channel load-bearing: without it, the coordinator would have no
    // signal that this key needs a fresh, re-routed retry at all.
    for (i, n) in b.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&participant_key)),
            None,
            "participant replica {i}: a Fenced resolve must touch nothing at all, leaving the \
             key exactly as unreachable as before the attempt (seed={seed})"
        );
    }

    // The anchor's own decision, meanwhile, is genuinely and durably
    // `Committed` — this is a live, real gap the fix's caller (`animusd::
    // ClientCtx::txn_resolve_participant_retrying`) exists to close by
    // re-routing and retrying, not a transaction that failed to commit.
    let n = a[la].clone();
    let record_key_s = record_key.clone();
    let status = drive(&mut sim, a[la].env(), SETTLE, async move {
        n.txn_status_local(&record_key_s).await
    })
    .flatten();
    assert_eq!(
        status,
        Some(animus_cp_data::TxnDecisionStatus::Committed { commit_ts }),
        "the anchor's own decision must stay Committed regardless of the participant's fence-miss \
         (seed={seed})"
    );
}
