//! **Multi-participant transactions** (ADR 0018 §2, PR4): two or three
//! independent tablet groups, each scoped on a shared `MemoryEngine`
//! (`shared_engine.rs`'s harness style), driven by a minimal in-test
//! coordinator over the raw group handles — proving the primitives PR4 adds
//! (`txn_stage_participant`/`txn_commit_at_least`/`txn_resolve`/
//! `txn_status_local`/`linearizable_get_served_fast`/
//! `resolve_intent_given_status`) compose into an atomic cross-tablet 2PC
//! before `animusd`'s wire-level coordinator (which uses the identical
//! primitives) is exercised end to end in `animusd/tests/cp_txn.rs`.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()`, and never `block_on` a call whose future waits on
//! `env.sleep` (every txn propose-and-wait method here) — see `drive`'s doc
//! (mirrors `txn_single.rs`'s identical helper).

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{FastRead, RaftKvNode, StorageScope, TxnDecisionStatus, TxnOutcome};
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::KeyRange;
use futures::executor::block_on;

const GROUP_A: [u64; 3] = [0, 1, 2];
const GROUP_B: [u64; 3] = [10, 11, 12];
const GROUP_C: [u64; 3] = [20, 21, 22];
const ELECT: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_secs(2);

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
    leader_among(nodes, &(0..nodes.len()).collect::<Vec<_>>(), seed, label)
}

/// As [`leader`], but only considers `live` indices — a crashed node keeps
/// answering `is_leader() == true` from its last-known (frozen) state since
/// it never learns it lost the term (it's muted from the network, not
/// gracefully shut down), so a post-crash leader check must exclude it
/// explicitly rather than expecting exactly one leader among *every* node.
fn leader_among(nodes: &[KvNode], live: &[usize], seed: u64, label: &str) -> usize {
    let ls: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one {label} leader, got {ls:?} (seed={seed})"
    );
    ls[0]
}

/// An 8-byte partition token followed by a distinguishing tail — every
/// real data-plane key leads with a full token (ADR 0022); the anchor
/// assert in `txn_stage` requires it.
fn key(token: u8, tail: &[u8]) -> Vec<u8> {
    let mut k = vec![token; 8];
    k.extend_from_slice(tail);
    k
}

/// Run `fut` to completion by spawning it on `env` and driving `sim`,
/// returning `None` if it didn't complete within `budget` — required for
/// every txn propose-and-wait method here, whose future waits on
/// `env.sleep` internally; a bare `block_on` would hang forever since
/// nothing else would ever advance `SimEnv`'s virtual clock. Mirrors
/// `txn_single.rs`'s identical helper.
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
) -> Option<(animus_cp_data::TxnId, Vec<u8>)> {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_stage(table, writes).await
    })
    .flatten()
    .map(|(txn_id, record_key, _outcome)| (txn_id, record_key))
}

#[allow(clippy::too_many_arguments)]
fn stage_participant(
    sim: &mut Simulator,
    node: &KvNode,
    txn_id: animus_cp_data::TxnId,
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

/// As [`stage_participant`], but for a write against an indexed/streamed
/// table: carries a derived kind-writes/change-log payload alongside the
/// base write (ADR 0046 A1) — used by
/// `kind_bearing_participant_materializes_at_resolve` below.
#[allow(clippy::too_many_arguments)]
fn stage_participant_kind(
    sim: &mut Simulator,
    node: &KvNode,
    txn_id: animus_cp_data::TxnId,
    record_key: Vec<u8>,
    record_table: String,
    writes: Vec<animus_cp_data::TxnWrite>,
) -> Option<HlcTimestamp> {
    let n = node.clone();
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
    txn_id: animus_cp_data::TxnId,
    record_key: Vec<u8>,
    min_ts: HlcTimestamp,
) -> Option<HlcTimestamp> {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_commit_at_least(txn_id, record_key, min_ts).await
    })
    .flatten()
}

fn resolve(
    sim: &mut Simulator,
    node: &KvNode,
    txn_id: animus_cp_data::TxnId,
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

fn decide(
    sim: &mut Simulator,
    node: &KvNode,
    txn_id: animus_cp_data::TxnId,
    record_key: Vec<u8>,
    keys: Vec<Vec<u8>>,
    commit: bool,
) -> Option<HlcTimestamp> {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_decide(txn_id, record_key, keys, commit).await
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

fn fast_read(sim: &mut Simulator, node: &KvNode, key: Vec<u8>) -> Option<FastRead> {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.linearizable_get_served_fast(&key).await
    })
    .flatten()
}

fn resolve_given_status(
    sim: &mut Simulator,
    node: &KvNode,
    key: Vec<u8>,
    txn_id: animus_cp_data::TxnId,
    status: TxnDecisionStatus,
) -> Option<Option<Vec<u8>>> {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.resolve_intent_given_status(&key, None, &txn_id, status)
            .await
    })
    .flatten()
}

/// The minimal in-test coordinator: stage the anchor (group `a`, table
/// `table_a`) and every other participant, then commit-and-resolve
/// everywhere, exactly mirroring the protocol `animusd::ClientCtx::cp_txn`
/// (PR4's wire-level coordinator) implements over real network forwarding.
/// `participants` is `(group, writes)` for every non-anchor tablet, all
/// sharing the anchor's own table name (matching this test file's fixtures,
/// where every transaction is single-table across tablets — a real
/// coordinator threads each participant's own table name instead). Returns
/// the transaction's canonical commit timestamp, or an `Err` if any phase
/// failed (in which case the anchor and every successfully-staged
/// participant have been aborted, best-effort).
#[allow(clippy::type_complexity)]
fn run_txn(
    sim: &mut Simulator,
    a: &KvNode,
    table_a: &'static str,
    anchor_writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    participants: &[(&KvNode, Vec<(Vec<u8>, Option<Vec<u8>>)>)],
) -> Result<HlcTimestamp, String> {
    let anchor_keys: Vec<Vec<u8>> = anchor_writes.iter().map(|(k, _)| k.clone()).collect();
    let (txn_id, record_key) = stage_anchor(sim, a, table_a, anchor_writes)
        .ok_or_else(|| "anchor stage failed".to_string())?;

    let mut staged_participants: Vec<(&KvNode, Vec<Vec<u8>>)> = Vec::new();
    let mut candidate = txn_id.ts;
    let mut abort_reason: Option<String> = None;
    for (node, writes) in participants {
        let keys: Vec<Vec<u8>> = writes.iter().map(|(k, _)| k.clone()).collect();
        match stage_participant(
            sim,
            node,
            txn_id.clone(),
            record_key.clone(),
            table_a.to_string(),
            writes.clone(),
        ) {
            Some(stage_ts) => {
                candidate = candidate.max(stage_ts);
                staged_participants.push((node, keys));
            }
            None => {
                abort_reason = Some("participant stage failed".to_string());
                break;
            }
        }
    }

    if let Some(reason) = abort_reason {
        // Best-effort abort: the anchor's own decide-and-resolve, plus a
        // resolve-abort on every participant that *did* stage.
        decide(
            sim,
            a,
            txn_id.clone(),
            record_key.clone(),
            anchor_keys,
            false,
        );
        for (node, keys) in &staged_participants {
            resolve(
                sim,
                node,
                txn_id.clone(),
                record_key.clone(),
                keys.clone(),
                TxnOutcome::Aborted,
            );
        }
        return Err(reason);
    }

    let commit_ts = commit_at_least(sim, a, txn_id.clone(), record_key.clone(), candidate)
        .ok_or_else(|| "anchor commit failed".to_string())?;
    let outcome = TxnOutcome::Committed { commit_ts };
    resolve(
        sim,
        a,
        txn_id.clone(),
        record_key.clone(),
        anchor_keys,
        outcome.clone(),
    );
    for (node, keys) in &staged_participants {
        resolve(
            sim,
            node,
            txn_id.clone(),
            record_key.clone(),
            keys.clone(),
            outcome.clone(),
        );
    }
    Ok(commit_ts)
}

/// **Atomicity, the headline property**: a two-tablet transaction's writes
/// become visible on *both* groups together — every replica of both, not
/// just the leaders — never one without the other.
#[test]
fn two_participant_commit_is_atomic_across_both_groups_and_every_replica() {
    let seed = 0x7A01;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, engine.clone(), b"accounts:");
    sim.run_for(ELECT);

    let la = leader(&nodes_a, seed, "A");
    let lb = leader(&nodes_b, seed, "B");
    let ka = key(1, b":order");
    let kb = key(2, b":balance");

    run_txn(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"placed".to_vec()))],
        &[(&nodes_b[lb], vec![(kb.clone(), Some(b"debited".to_vec()))])],
    )
    .expect("two-participant commit should succeed");
    sim.run_for(SETTLE);

    for (i, n) in nodes_a.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&ka)),
            Some(b"placed".to_vec()),
            "group A replica {i} missing the anchor's committed value (seed={seed})"
        );
    }
    for (i, n) in nodes_b.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&kb)),
            Some(b"debited".to_vec()),
            "group B replica {i} missing the participant's committed value (seed={seed})"
        );
    }
}

/// A **three-participant** transaction commits atomically across all three
/// groups — proving the design generalizes past exactly two.
#[test]
fn three_participant_commit_lands_on_all_three_groups() {
    let seed = 0x7A02;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"t1:");
    let nodes_b = start_group(&sim, &GROUP_B, engine.clone(), b"t2:");
    let nodes_c = start_group(&sim, &GROUP_C, engine.clone(), b"t3:");
    sim.run_for(ELECT);

    let la = leader(&nodes_a, seed, "A");
    let lb = leader(&nodes_b, seed, "B");
    let lc = leader(&nodes_c, seed, "C");
    let ka = key(1, b":a");
    let kb = key(2, b":b");
    let kc = key(3, b":c");

    run_txn(
        &mut sim,
        &nodes_a[la],
        "t1",
        vec![(ka.clone(), Some(b"va".to_vec()))],
        &[
            (&nodes_b[lb], vec![(kb.clone(), Some(b"vb".to_vec()))]),
            (&nodes_c[lc], vec![(kc.clone(), Some(b"vc".to_vec()))]),
        ],
    )
    .expect("three-participant commit should succeed");
    sim.run_for(SETTLE);

    assert_eq!(block_on(nodes_a[la].local_get(&ka)), Some(b"va".to_vec()));
    assert_eq!(block_on(nodes_b[lb].local_get(&kb)), Some(b"vb".to_vec()));
    assert_eq!(block_on(nodes_c[lc].local_get(&kc)), Some(b"vc".to_vec()));
}

/// **Abort cleanup**: when the coordinator decides to abort after both
/// participants have staged (the shape a real coordinator takes on a
/// prepare failure at a *third* participant, or a condition-read refresh
/// rejecting the transaction), every staged key is restored to its
/// pre-transaction value — here: absent, since these are fresh keys —
/// never left as a dangling intent.
#[test]
fn abort_restores_every_staged_participants_prior_value() {
    let seed = 0x7A03;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, engine.clone(), b"accounts:");
    sim.run_for(ELECT);

    let la = leader(&nodes_a, seed, "A");
    let lb = leader(&nodes_b, seed, "B");
    let ka = key(1, b":order");
    let kb = key(2, b":balance");

    let (txn_id, record_key) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"placed".to_vec()))],
    )
    .expect("anchor stage");
    let stage_ts_b = stage_participant(
        &mut sim,
        &nodes_b[lb],
        txn_id.clone(),
        record_key.clone(),
        "orders".to_string(),
        vec![(kb.clone(), Some(b"debited".to_vec()))],
    );
    assert!(stage_ts_b.is_some(), "participant stage (seed={seed})");

    decide(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        vec![ka.clone()],
        false,
    );
    resolve(
        &mut sim,
        &nodes_b[lb],
        txn_id.clone(),
        record_key.clone(),
        vec![kb.clone()],
        TxnOutcome::Aborted,
    );
    sim.run_for(SETTLE);

    for (i, n) in nodes_a.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&ka)),
            None,
            "group A replica {i} should have reverted to absent after abort (seed={seed})"
        );
    }
    for (i, n) in nodes_b.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&kb)),
            None,
            "group B replica {i} should have reverted to absent after abort (seed={seed})"
        );
    }
}

/// **Foreign-intent resolution**: a reader on the *participant* tablet
/// encounters its own still-`Pending` intent; `linearizable_get_served_fast`
/// correctly reports it as `Foreign` (the record lives on the anchor's
/// tablet, not here), and `txn_status_local` on the anchor plus
/// `resolve_intent_given_status` on the participant together resolve it
/// correctly — the exact round trip `animusd`'s `cp_get_local` performs
/// over the network.
#[test]
fn foreign_intent_resolves_via_the_anchor_records_status() {
    let seed = 0x7A04;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, engine.clone(), b"accounts:");
    sim.run_for(ELECT);

    let la = leader(&nodes_a, seed, "A");
    let lb = leader(&nodes_b, seed, "B");
    let ka = key(1, b":order");
    let kb = key(2, b":balance");

    let (txn_id, record_key) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"placed".to_vec()))],
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

    // Before any commit decision: the participant's own fast read reports
    // `Foreign`, carrying the anchor's record key/table/txn id.
    let fast = fast_read(&mut sim, &nodes_b[lb], kb.clone())
        .expect("read barrier should succeed on the participant leader");
    let info = match fast {
        FastRead::Foreign(info) => info,
        other => panic!("expected Foreign, got {other:?} (seed={seed})"),
    };
    assert_eq!(info.txn_id, txn_id);
    assert_eq!(info.record_key, record_key);
    assert_eq!(info.record_table, "orders");

    // The anchor hasn't decided yet: its own status query reports Pending.
    let status = status_local(&mut sim, &nodes_a[la], record_key.clone()).expect("record exists");
    assert_eq!(status, TxnDecisionStatus::Pending);

    // Now the coordinator commits...
    let commit_ts = commit_at_least(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        txn_id.ts,
    )
    .expect("anchor commit");
    resolve(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        vec![ka.clone()],
        TxnOutcome::Committed { commit_ts },
    );

    // ...and the participant's own status query (simulating the wire round
    // trip a real `ClientRequest::TxnStatus` performs) now reports Committed.
    let status = status_local(&mut sim, &nodes_a[la], record_key.clone()).expect("record exists");
    assert_eq!(status, TxnDecisionStatus::Committed { commit_ts });

    // The participant resolves its own read using that status, with no
    // local record of its own ever existing on group B.
    let resolved = resolve_given_status(&mut sim, &nodes_b[lb], kb.clone(), txn_id, status)
        .expect("resolves to a value");
    assert_eq!(resolved, Some(b"debited".to_vec()));
}

/// **Fence/seal interplay**: a stage proposed into an already-sealed range
/// is a whole-or-nothing no-op at apply (mirrors `txn_single.rs`'s
/// single-participant case) — the participant's own stage confirms as
/// *proposed* (the propose outcome can't distinguish a fence/seal no-op
/// from a genuine stage — that distinction only exists at apply), but its
/// engine never actually holds the intent, so the coordinator's abort
/// leaves both groups clean.
#[test]
fn participant_stage_into_a_sealed_range_is_a_true_no_op() {
    let seed = 0x7A05;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, engine.clone(), b"accounts:");
    sim.run_for(ELECT);

    let la = leader(&nodes_a, seed, "A");
    let lb = leader(&nodes_b, seed, "B");
    let ka = key(1, b":order");
    let kb = key(2, b":balance");

    // Seal group B's whole range before the transaction ever starts.
    match nodes_b[lb].propose_seal(KeyRange::whole()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("seal proposal rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(1));

    let (txn_id, record_key) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"placed".to_vec()))],
    )
    .expect("anchor stage still succeeds (its own range isn't sealed)");

    let stage_ts = stage_participant(
        &mut sim,
        &nodes_b[lb],
        txn_id.clone(),
        record_key.clone(),
        "orders".to_string(),
        vec![(kb.clone(), Some(b"debited".to_vec()))],
    );
    assert!(
        stage_ts.is_some(),
        "stage command itself commits (seed={seed})"
    );

    // The coordinator can't tell a sealed no-op stage apart from a genuine
    // one via the propose outcome alone — this is exactly why a real
    // coordinator (PR5+) needs a post-stage verification read; for this
    // primitive-level test, directly confirm the no-op: group B's engine
    // never actually holds an intent for `kb`.
    assert_eq!(
        block_on(nodes_b[lb].local_get(&kb)),
        None,
        "a stage into a sealed range must be a true no-op, never a leaked \
         intent (seed={seed})"
    );

    // Abort cleanly regardless (the coordinator's documented fallback when
    // a participant's post-stage state can't be trusted).
    decide(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        vec![ka.clone()],
        false,
    );
    sim.run_for(SETTLE);
    assert_eq!(block_on(nodes_a[la].local_get(&ka)), None);
}

/// **Participant leader-kill during prepare**: the participant's stage
/// never confirms (its leader is killed mid-flight); the coordinator times
/// out that participant, aborts the anchor, and — once a new leader takes
/// over group B — no half-staged intent is ever visible there.
#[test]
fn participant_leader_kill_during_prepare_converges_to_a_clean_abort() {
    let seed = 0x7A06;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, engine.clone(), b"accounts:");
    sim.run_for(ELECT);

    let la = leader(&nodes_a, seed, "A");
    let lb = leader(&nodes_b, seed, "B");
    let ka = key(1, b":order");
    let kb = key(2, b":balance");

    let (txn_id, record_key) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"placed".to_vec()))],
    )
    .expect("anchor stage");

    // Kill group B's leader before it can confirm the participant's stage.
    sim.crash(nid(GROUP_B[lb]));
    let stage_result = stage_participant(
        &mut sim,
        &nodes_b[lb],
        txn_id.clone(),
        record_key.clone(),
        "orders".to_string(),
        vec![(kb.clone(), Some(b"debited".to_vec()))],
    );
    assert!(
        stage_result.is_none(),
        "a killed leader must never confirm a stage as applied (seed={seed})"
    );

    // The coordinator's documented fallback: abort the anchor. No
    // participant intent was ever confirmed, so there is nothing to
    // resolve-abort on group B.
    decide(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        vec![ka.clone()],
        false,
    );

    // Group B re-elects; the new leader never sees a dangling intent for
    // `kb` (the stage that would have written it never committed).
    sim.run_for(Duration::from_secs(3));
    let live_b: Vec<usize> = (0..nodes_b.len()).filter(|&i| i != lb).collect();
    let lb2 = leader_among(&nodes_b, &live_b, seed, "B (post-kill)");
    assert_eq!(
        block_on(nodes_b[lb2].local_get(&kb)),
        None,
        "no half-staged intent should survive a killed leader's unconfirmed \
         propose (seed={seed})"
    );
    assert_eq!(block_on(nodes_a[la].local_get(&ka)), None);
}

/// Seed sweep: the whole two-participant commit shape stays deterministic
/// and correct across an independent run of seeds.
#[test]
fn two_participant_commit_is_reproducible_across_seeds() {
    for seed in [0x7B01u64, 0x7B02, 0x7B03, 0x7B04, 0x7B05] {
        let mut sim = Simulator::new(seed);
        let engine = MemoryEngine::new();
        let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"orders:");
        let nodes_b = start_group(&sim, &GROUP_B, engine.clone(), b"accounts:");
        sim.run_for(ELECT);

        let la = leader(&nodes_a, seed, "A");
        let lb = leader(&nodes_b, seed, "B");
        let ka = key(1, b":order");
        let kb = key(2, b":balance");

        run_txn(
            &mut sim,
            &nodes_a[la],
            "orders",
            vec![(ka.clone(), Some(b"placed".to_vec()))],
            &[(&nodes_b[lb], vec![(kb.clone(), Some(b"debited".to_vec()))])],
        )
        .unwrap_or_else(|e| panic!("commit should succeed (seed={seed}): {e}"));
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

/// ADR 0046 A1 ("materialize-at-resolve"): a **non-anchor participant's**
/// own write can carry a derived kind-writes/change-log payload too, not
/// just the anchor's — a cross-tablet transaction touching an
/// indexed/streamed table on the participant side must materialize its
/// LSI row + change record atomically with its own base commit, same as a
/// single-tablet `KindBatch` would, and same as the anchor's own kind
/// payload (`txn_kind_writes.rs`'s primitive-level suite covers the
/// single-tablet shape in depth; this proves it composes across the 2PC).
#[test]
fn kind_bearing_participant_materializes_its_lsi_row_and_change_record_at_resolve() {
    let seed = 0x7A10;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let nodes_a = start_group(&sim, &GROUP_A, engine.clone(), b"orders:");
    let nodes_b = start_group(&sim, &GROUP_B, engine.clone(), b"accounts:");
    sim.run_for(ELECT);

    let la = leader(&nodes_a, seed, "A");
    let lb = leader(&nodes_b, seed, "B");
    let ka = key(1, b":order");
    // The participant's own base/LSI/change keys must share ONE token (ADR
    // 0022/0046) — `key(2, ..)` always leads with the same 8-byte token 2.
    let kb_base = key(2, b":balance");
    let kb_lsi = key(2, b"\x01:by-amount");
    let kb_change_prefix = key(2, b"\x02");

    let (txn_id, record_key) = stage_anchor(
        &mut sim,
        &nodes_a[la],
        "orders",
        vec![(ka.clone(), Some(b"placed".to_vec()))],
    )
    .expect("anchor stage should succeed");

    let participant_write = animus_cp_data::TxnWrite {
        key: kb_base.clone(),
        value: Some(b"debited".to_vec()),
        kind_writes: vec![(
            animus_cp_data::KIND_LSI,
            kb_lsi.clone(),
            Some(b"by-amount-row".to_vec()),
        )],
        change_log: Some((kb_change_prefix.clone(), b"account-change".to_vec())),
    };
    let stage_ts = stage_participant_kind(
        &mut sim,
        &nodes_b[lb],
        txn_id.clone(),
        record_key.clone(),
        "accounts".to_string(),
        vec![participant_write],
    )
    .expect("kind-bearing participant stage should succeed");

    let candidate = txn_id.ts.max(stage_ts);
    let commit_ts = commit_at_least(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        candidate,
    )
    .expect("anchor commit should succeed");
    let outcome = TxnOutcome::Committed { commit_ts };
    resolve(
        &mut sim,
        &nodes_a[la],
        txn_id.clone(),
        record_key.clone(),
        vec![ka.clone()],
        outcome.clone(),
    );
    let resolve_ts = resolve(
        &mut sim,
        &nodes_b[lb],
        txn_id.clone(),
        record_key.clone(),
        vec![kb_base.clone()],
        outcome,
    )
    .expect("participant resolve should succeed");
    sim.run_for(SETTLE);

    assert_eq!(
        block_on(nodes_a[la].local_get(&ka)),
        Some(b"placed".to_vec())
    );
    assert_eq!(
        block_on(nodes_b[lb].local_get(&kb_base)),
        Some(b"debited".to_vec()),
        "participant's base row must commit (seed={seed})"
    );
    assert_eq!(
        block_on(nodes_b[lb].local_get_kind(animus_cp_data::KIND_LSI, &kb_lsi)),
        Some(b"by-amount-row".to_vec()),
        "participant's LSI row must materialize at ITS OWN resolve (seed={seed})"
    );
    let mut change_key = kb_change_prefix.clone();
    change_key.extend_from_slice(&animus_cp_data::hlc::pack(resolve_ts).to_be_bytes());
    assert_eq!(
        block_on(nodes_b[lb].local_get_kind(animus_cp_data::KIND_CHANGE, &change_key)),
        Some(b"account-change".to_vec()),
        "participant's change record must materialize keyed at its own resolve ts (seed={seed})"
    );
    // And every replica of B agrees, not just the leader.
    for (i, n) in nodes_b.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get_kind(animus_cp_data::KIND_LSI, &kb_lsi)),
            Some(b"by-amount-row".to_vec()),
            "group B replica {i} missing the participant's materialized LSI row (seed={seed})"
        );
    }
}
