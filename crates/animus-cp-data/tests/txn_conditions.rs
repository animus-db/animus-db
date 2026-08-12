//! ADR 0018 §2 apply-time write-key conditions amendment: `KvCommand::
//! TxnStage`'s own-key `conditions` field and the [`StageOutcome`]
//! introspection primitive it feeds — the CAS-style, byte-level OCC
//! primitive `animusd::dynamo`'s `TransactWriteItems` own-key
//! `ConditionExpression` now compiles down to (see `animusd/src/dynamo.rs`'s
//! `run_transact` doc).
//!
//! Mirrors `txn_single.rs`'s harness style exactly (single-participant
//! transactions through `txn_stage`/`txn_stage_anchor`/`txn_decide` over one
//! 3-node group) — conditions are evaluated entirely within one tablet's own
//! apply arm, so no multi-participant coordinator is needed to exercise
//! them.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()`, and never `block_on` a call whose future waits on
//! `env.sleep` — see `drive`'s doc.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::{RaftKvNode, StageOutcome, TxnId};
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{escape, partition_token};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
const ELECT: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_secs(2);

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

fn leader(nodes: &[KvNode], live: &[usize], seed: u64) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(i, n)| live.contains(i) && n.is_leader())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among {live:?}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

/// A real ADR 0022-shaped data-plane key: `partition_token(pk) ||
/// escape(pk) || rk` — matches `txn_single.rs`'s identical helper.
fn key(pk: &[u8], rk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out.extend_from_slice(rk);
    out
}

/// Mirrors `txn_single.rs`'s identical helper: run `fut` to completion by
/// spawning it on `env` and driving `sim`, returning `None` if it didn't
/// complete within `budget`.
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

/// Stage `writes` with `conditions` on `node`, returning
/// `(txn_id, record_key, outcome)` once the entry has committed and
/// applied — panics if it never does (every scenario here has a live
/// leader throughout).
#[allow(clippy::type_complexity)]
fn stage(
    sim: &mut Simulator,
    node: &KvNode,
    writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    seed: u64,
) -> (TxnId, Vec<u8>, StageOutcome) {
    let n = node.clone();
    drive(sim, node.env(), SETTLE, async move {
        n.txn_stage_anchor("t", writes, Vec::new(), conditions)
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage_anchor did not complete (seed={seed})"))
}

fn decide(
    sim: &mut Simulator,
    node: &KvNode,
    txn_id: TxnId,
    record_key: Vec<u8>,
    keys: Vec<Vec<u8>>,
    commit: bool,
    seed: u64,
) {
    let n = node.clone();
    let ts = drive(sim, node.env(), SETTLE, async move {
        n.txn_decide(txn_id, record_key, keys, commit).await
    })
    .flatten();
    assert!(ts.is_some(), "txn_decide did not complete (seed={seed})");
}

/// A condition matching the key's current committed value stages
/// successfully and the transaction goes on to commit normally.
#[test]
fn matching_condition_stages_and_commits() {
    let seed = 0xC0D_0001;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, &[0, 1, 2], seed);

    let k = key(b"acct-1", b"balance");
    assert!(matches!(
        nodes[l].put(k.clone(), b"100".to_vec()),
        animus_control::ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    let (txn_id, record_key, outcome) = stage(
        &mut sim,
        &nodes[l],
        vec![(k.clone(), Some(b"150".to_vec()))],
        vec![(k.clone(), Some(b"100".to_vec()))],
        seed,
    );
    assert_eq!(
        outcome,
        StageOutcome::Staged,
        "a condition matching the current committed value must stage (seed={seed})"
    );
    decide(
        &mut sim,
        &nodes[l],
        txn_id,
        record_key,
        vec![k.clone()],
        true,
        seed,
    );
    sim.run_for(SETTLE);
    assert_eq!(
        block_on(nodes[l].local_get(&k)),
        Some(b"150".to_vec()),
        "seed={seed}"
    );
}

/// A "must be absent" condition (`expected: None`) on a genuinely-absent
/// key stages successfully — the dual of the present-value case above.
#[test]
fn must_be_absent_condition_passes_when_truly_absent() {
    let seed = 0xC0D_0002;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, &[0, 1, 2], seed);

    let k = key(b"acct-2", b"fresh-row");
    let (txn_id, record_key, outcome) = stage(
        &mut sim,
        &nodes[l],
        vec![(k.clone(), Some(b"created".to_vec()))],
        vec![(k.clone(), None)],
        seed,
    );
    assert_eq!(
        outcome,
        StageOutcome::Staged,
        "a must-be-absent condition on a genuinely absent key must stage (seed={seed})"
    );
    decide(
        &mut sim,
        &nodes[l],
        txn_id,
        record_key,
        vec![k.clone()],
        true,
        seed,
    );
    sim.run_for(SETTLE);
    assert_eq!(block_on(nodes[l].local_get(&k)), Some(b"created".to_vec()));
}

/// A "must be absent" condition on a key that already holds a committed
/// value is rejected — the mirror image of the pass case, and (with a
/// second, unconditioned key in the same stage) the multi-key
/// whole-or-nothing proof: neither key is staged as an intent when the
/// condition fails.
#[test]
fn must_be_absent_condition_fails_when_present_no_ops_the_whole_multi_key_stage() {
    let seed = 0xC0D_0003;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, &[0, 1, 2], seed);

    let k1 = key(b"acct-3", b"already-there");
    let k2 = key(b"acct-3", b"other-row");
    assert!(matches!(
        nodes[l].put(k1.clone(), b"existing".to_vec()),
        animus_control::ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    let (_txn_id, _record_key, outcome) = stage(
        &mut sim,
        &nodes[l],
        vec![
            (k1.clone(), Some(b"overwrite".to_vec())),
            (k2.clone(), Some(b"unrelated".to_vec())),
        ],
        vec![(k1.clone(), None)], // must be absent — but it isn't.
        seed,
    );
    assert_eq!(
        outcome,
        StageOutcome::ConditionFailed { key: k1.clone() },
        "a must-be-absent condition on a present key must fail, naming that key (seed={seed})"
    );
    sim.run_for(SETTLE);
    assert_eq!(
        block_on(nodes[l].local_get(&k1)),
        Some(b"existing".to_vec()),
        "the conditioned key's prior value must survive untouched (seed={seed})"
    );
    assert_eq!(
        block_on(nodes[l].local_get(&k2)),
        None,
        "whole-or-nothing: the OTHER key in the same stage must never have been staged \
         either, even though it carried no condition of its own (seed={seed})"
    );
}

/// A condition that does not match the current committed value rejects
/// the whole stage — the single-key counterpart of the multi-key test
/// above, using a present-vs-wrong-value mismatch instead of an
/// absence mismatch.
#[test]
fn mismatched_value_condition_fails_and_leaves_the_key_untouched() {
    let seed = 0xC0D_0004;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, &[0, 1, 2], seed);

    let k = key(b"acct-4", b"balance");
    assert!(matches!(
        nodes[l].put(k.clone(), b"100".to_vec()),
        animus_control::ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    let (_txn_id, _record_key, outcome) = stage(
        &mut sim,
        &nodes[l],
        vec![(k.clone(), Some(b"150".to_vec()))],
        vec![(k.clone(), Some(b"999-wrong".to_vec()))],
        seed,
    );
    assert_eq!(outcome, StageOutcome::ConditionFailed { key: k.clone() });
    sim.run_for(SETTLE);
    assert_eq!(
        block_on(nodes[l].local_get(&k)),
        Some(b"100".to_vec()),
        "a failed condition must never let the write land (seed={seed})"
    );
}

/// A key already holding a *different* transaction's unresolved intent
/// blocks a second transaction's stage as `IntentBlocked`, distinct from
/// `ConditionFailed` — even though the blocked stage also carries an
/// own-key condition on that exact key. Foreign-intent blocking must be
/// checked (and reported) BEFORE a condition is ever evaluated: the
/// "current committed value" is ambiguous while a foreign intent is live,
/// so evaluating the condition at all would be unsound, not just
/// redundant.
#[test]
fn foreign_intent_block_is_reported_distinctly_from_condition_failure() {
    let seed = 0xC0D_0005;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, &[0, 1, 2], seed);

    let k = key(b"acct-5", b"balance");
    assert!(matches!(
        nodes[l].put(k.clone(), b"v0".to_vec()),
        animus_control::ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    // Transaction A stages (but never decides) an intent over `k` — left
    // deliberately `Pending`, simulating an in-flight coordinator.
    let (txn_a, _record_a, outcome_a) = stage(
        &mut sim,
        &nodes[l],
        vec![(k.clone(), Some(b"staged-by-a".to_vec()))],
        Vec::new(),
        seed,
    );
    assert_eq!(outcome_a, StageOutcome::Staged, "seed={seed}");

    // Transaction B's own stage targets the SAME key, carrying a condition
    // — one that would even (irrelevantly) evaluate true against A's own
    // staged value, to prove the block fires regardless of what the
    // condition says.
    let (_txn_b, _record_b, outcome_b) = stage(
        &mut sim,
        &nodes[l],
        vec![(k.clone(), Some(b"staged-by-b".to_vec()))],
        vec![(k.clone(), Some(b"staged-by-a".to_vec()))],
        seed,
    );
    match outcome_b {
        StageOutcome::IntentBlocked {
            key: blocked_key,
            txn_id: blocker,
        } => {
            assert_eq!(blocked_key, k, "seed={seed}");
            assert_eq!(blocker, txn_a, "seed={seed}");
        }
        other => panic!(
            "expected IntentBlocked (foreign-intent priority over condition evaluation), \
             got {other:?} (seed={seed})"
        ),
    }
}

/// Crash/restart WAL-replay idempotency (mirrors `txn_single.rs`'s
/// `crash_restart_reapplies_stage_commit_resolve_idempotently`): a
/// condition-gated stage that committed before a restart re-derives the
/// identical committed value after the node recovers via WAL replay —
/// the condition check itself must be replay-safe (it re-evaluates
/// deterministically against the same committed history), never double
/// stage or diverge.
#[test]
fn condition_gated_commit_survives_crash_restart_idempotently() {
    let seed = 0xC0D_0006;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let id = nid(0);

    let node: KvNode = RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], engine.clone());
    sim.run_for(ELECT);

    let k = key(b"acct-6", b"balance");
    assert!(matches!(
        node.put(k.clone(), b"100".to_vec()),
        animus_control::ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    let (txn_id, record_key, outcome) = stage(
        &mut sim,
        &node,
        vec![(k.clone(), Some(b"150".to_vec()))],
        vec![(k.clone(), Some(b"100".to_vec()))],
        seed,
    );
    assert_eq!(outcome, StageOutcome::Staged, "seed={seed}");
    decide(
        &mut sim,
        &node,
        txn_id,
        record_key,
        vec![k.clone()],
        true,
        seed,
    );
    sim.run_for(SETTLE);
    assert_eq!(block_on(node.local_get(&k)), Some(b"150".to_vec()));

    // A genuine process restart (`stop`, not `crash`/`restart`) — the WAL
    // survives on the same engine; a fresh `RaftKvNode::start` replays it
    // from scratch, re-applying the conditioned `TxnStage`/`TxnCommit`/
    // `TxnResolve` entries exactly as they first applied.
    sim.stop(id.clone());
    let restarted: KvNode =
        RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], engine.clone());
    sim.run_for(ELECT);

    assert_eq!(
        block_on(restarted.local_get(&k)),
        Some(b"150".to_vec()),
        "WAL replay of a condition-gated stage must re-derive the identical committed \
         value (seed={seed})"
    );
}

/// The whole suite above is reproducible from its seed — a light
/// determinism sweep across a handful of fresh seeds re-running the
/// matching-condition-commits scenario, mirroring every other file in
/// this crate's `run_is_deterministic_from_seed` convention.
#[test]
fn matching_condition_scenario_is_reproducible_across_seeds() {
    for seed in [0xC0D_1001, 0xC0D_1002, 0xC0D_1003, 0xC0D_1004, 0xC0D_1005] {
        let (mut sim, nodes) = group(seed);
        sim.run_for(ELECT);
        let l = leader(&nodes, &[0, 1, 2], seed);

        let k = key(b"acct-7", b"balance");
        assert!(matches!(
            nodes[l].put(k.clone(), b"100".to_vec()),
            animus_control::ProposeResult::Accepted { .. }
        ));
        sim.run_for(SETTLE);

        let (txn_id, record_key, outcome) = stage(
            &mut sim,
            &nodes[l],
            vec![(k.clone(), Some(b"150".to_vec()))],
            vec![(k.clone(), Some(b"100".to_vec()))],
            seed,
        );
        assert_eq!(outcome, StageOutcome::Staged, "seed={seed}");
        decide(
            &mut sim,
            &nodes[l],
            txn_id,
            record_key,
            vec![k.clone()],
            true,
            seed,
        );
        sim.run_for(SETTLE);
        assert_eq!(
            block_on(nodes[l].local_get(&k)),
            Some(b"150".to_vec()),
            "seed={seed}"
        );
    }
}
