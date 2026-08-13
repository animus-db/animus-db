//! ADR 0018 §2/PR3: single-participant transactions — the degenerate 2PC
//! (stage/commit-or-abort/resolve) through **one** Raft group. Exercises
//! `RaftKvNode::txn_write`/`txn_stage`/`txn_decide` end to end: the commit
//! path (visible via `read_at`, `local_get`, and a scan), the abort path
//! (the prior committed value is restored, never tombstoned), a staged
//! delete's real tombstone on commit, a point read blocking on a `Pending`
//! intent then serving once committed, intent/record markers never leaking
//! into a scan, crash/restart idempotency (WAL replay re-applies every
//! phase identically), and a stage into an already-sealed range being
//! rejected wholesale (no partial staging).
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers), and
//! never `block_on` a call whose future waits on `env.sleep` (`txn_write`/
//! `txn_stage`/`txn_decide`'s internal `wait_applied` poll, and
//! `linearizable_get`'s read barrier) — see `drive`'s doc.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, escape, partition_token};
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
/// escape(pk) || rk` — the layout `txn_write`'s anchor-token disjointness
/// proof (`txn.rs`) assumes.
fn key(pk: &[u8], rk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out.extend_from_slice(rk);
    out
}

/// Run `fut` to completion by spawning it on `env` and driving `sim`,
/// returning `None` if it didn't complete within `budget`. Required for
/// anything whose future waits on `env.sleep` (every txn propose-and-wait
/// method here, and `linearizable_get`'s read barrier) — `block_on` alone
/// would hang forever, since nothing else would ever drive the simulated
/// clock forward.
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

fn txn_write(
    sim: &mut Simulator,
    node: &KvNode,
    writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    budget: Duration,
) -> Option<HlcTimestamp> {
    let n = node.clone();
    drive(sim, node.env(), budget, async move {
        n.txn_write("t", writes).await
    })
    .flatten()
}

#[test]
fn commit_path_makes_the_value_visible_via_read_at_local_get_and_scan() {
    let seed = 0x7C0_0001;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, &[0, 1, 2], seed);

    let k = key(b"acct-1", b"balance");
    let commit_ts = txn_write(
        &mut sim,
        &nodes[l],
        vec![(k.clone(), Some(b"100".to_vec()))],
        Duration::from_secs(5),
    )
    .unwrap_or_else(|| panic!("txn_write did not complete (seed={seed})"));

    // Replicate to every node.
    sim.run_for(SETTLE);
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&k)),
            Some(b"100".to_vec()),
            "node {i}: committed value must be visible via local_get (seed={seed})"
        );
    }

    // `read_at` at (or after) the commit ts, once the ceiling covers it —
    // an ordinary `linearizable_get` on the leader drives the ceiling
    // forward first (mirrors `snapshot_reads.rs`'s idiom).
    let ln = nodes[l].clone();
    let kk = k.clone();
    drive(&mut sim, nodes[l].env(), SETTLE, async move {
        ln.linearizable_get(&kk).await
    });
    let ln = nodes[l].clone();
    let kk = k.clone();
    let at = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        ln.read_at(&kk, commit_ts).await
    })
    .unwrap_or_else(|| panic!("read_at did not complete (seed={seed})"));
    assert_eq!(
        at,
        Some(Some(b"100".to_vec())),
        "read_at(commit_ts) must see the committed value (seed={seed})"
    );

    // A scan over the covering range sees exactly this one committed row —
    // no internal record/intent bytes leak.
    let ln = nodes[l].clone();
    let scanned = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        ln.linearizable_scan(&partition_token(b"acct-1"), None, None)
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("linearizable_scan did not complete (seed={seed})"));
    assert_eq!(
        scanned,
        vec![(k, b"100".to_vec())],
        "scan must return exactly the committed row (seed={seed})"
    );
}

#[test]
fn abort_path_restores_the_value_that_existed_before_the_intent() {
    let seed = 0xABE_0002;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, &[0, 1, 2], seed);

    let k = key(b"acct-2", b"balance");
    assert!(matches!(
        nodes[l].put(k.clone(), b"old".to_vec()),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);
    assert_eq!(block_on(nodes[l].local_get(&k)), Some(b"old".to_vec()));

    // Stage a would-be overwrite, then decide to ABORT it.
    let n = nodes[l].clone();
    let kk = k.clone();
    let (txn_id, record_key, _outcome) = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        n.txn_stage("t", vec![(kk, Some(b"staged".to_vec()))]).await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage did not complete (seed={seed})"));

    let n = nodes[l].clone();
    let kk = k.clone();
    let decided = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        n.txn_decide(txn_id, record_key, vec![kk], false).await
    })
    .flatten();
    assert!(
        decided.is_some(),
        "abort txn_decide must complete (seed={seed})"
    );

    sim.run_for(SETTLE);
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&k)),
            Some(b"old".to_vec()),
            "node {i}: an aborted intent must restore the prior committed value, \
             never leak the staged one or a tombstone (seed={seed})"
        );
    }
}

#[test]
fn committed_delete_intent_produces_a_real_tombstone() {
    let seed = 0xDE1_0003;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, &[0, 1, 2], seed);

    let k = key(b"acct-3", b"balance");
    assert!(matches!(
        nodes[l].put(k.clone(), b"v".to_vec()),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    let commit_ts = txn_write(&mut sim, &nodes[l], vec![(k.clone(), None)], SETTLE)
        .unwrap_or_else(|| panic!("txn_write (delete) did not complete (seed={seed})"));
    let _ = commit_ts;

    sim.run_for(SETTLE);
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&k)),
            None,
            "node {i}: a committed delete-intent must resolve to a real tombstone (seed={seed})"
        );
    }
}

#[test]
fn a_pending_read_blocks_then_serves_once_committed() {
    let seed = 0xB10_0004;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, &[0, 1, 2], seed);

    let k = key(b"acct-4", b"balance");
    let n = nodes[l].clone();
    let kk = k.clone();
    let (txn_id, record_key, _outcome) = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        n.txn_stage("t", vec![(kk, Some(b"new".to_vec()))]).await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage did not complete (seed={seed})"));

    // A linearizable read of the staged key must NOT immediately report
    // absent — it retries (bounded) while the covering txn is `Pending`.
    // Spawn it, drive only a short budget (well under
    // `INTENT_WAIT_TIMEOUT`), and confirm it has not resolved yet.
    let read_slot: Arc<Mutex<Option<Option<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let n = nodes[l].clone();
    let kk = k.clone();
    let s = Arc::clone(&read_slot);
    nodes[l].env().clone().spawn_task(async move {
        let v = n.linearizable_get(&kk).await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(Duration::from_millis(500));
    assert!(
        read_slot.lock().unwrap().is_none(),
        "a read of a Pending-intent key must not resolve before the txn decides (seed={seed})"
    );

    // Now commit + resolve; the still-in-flight read must go on to serve
    // the committed value.
    let n = nodes[l].clone();
    let kk = k.clone();
    let decided = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        n.txn_decide(txn_id, record_key, vec![kk], true).await
    })
    .flatten();
    assert!(
        decided.is_some(),
        "commit txn_decide must complete (seed={seed})"
    );

    sim.run_for(SETTLE);
    assert_eq!(
        read_slot.lock().unwrap().clone(),
        Some(Some(b"new".to_vec())),
        "the blocked read must eventually serve the committed value (seed={seed})"
    );
}

#[test]
fn intent_and_record_markers_never_leak_into_a_scan() {
    let seed = 0x5CA_0005;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, &[0, 1, 2], seed);

    // Ordinary committed rows, sharing `acct-5`'s token with the txn below.
    let committed: Vec<(Vec<u8>, Vec<u8>)> = (0..3)
        .map(|i| (key(b"acct-5", format!("row{i}").as_bytes()), b"v".to_vec()))
        .collect();
    for (k, v) in &committed {
        assert!(matches!(
            nodes[l].put(k.clone(), v.clone()),
            ProposeResult::Accepted { .. }
        ));
    }
    sim.run_for(SETTLE);

    // Stage (but don't yet decide) a transaction over a fresh key under the
    // SAME token — its record key and this intent both land in the exact
    // range the scan below covers.
    let staged_key = key(b"acct-5", b"row-staged");
    let n = nodes[l].clone();
    let kk = staged_key.clone();
    let staged = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        n.txn_stage("t", vec![(kk, Some(b"pending-value".to_vec()))])
            .await
    })
    .flatten();
    assert!(staged.is_some(), "txn_stage must complete (seed={seed})");

    // A scan over the whole token's range must return exactly the
    // committed rows: no record marker key, no raw intent bytes, and (this
    // PR's documented, non-blocking scan behavior) the still-`Pending`
    // staged key is silently omitted rather than served early or garbled.
    let n = nodes[l].clone();
    let scanned = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        n.linearizable_scan(&partition_token(b"acct-5"), None, None)
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("linearizable_scan did not complete (seed={seed})"));
    let mut expected = committed;
    expected.sort();
    let mut got = scanned;
    got.sort();
    assert_eq!(
        got, expected,
        "scan must return exactly the committed rows — no internal marker/intent \
         bytes, and no early/garbled staged value (seed={seed})"
    );
}

#[test]
fn crash_restart_reapplies_stage_commit_resolve_idempotently() {
    let seed = 0x1DE_0006;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let id = nid(0);

    let node: KvNode = RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], engine.clone());
    sim.run_for(ELECT); // single-voter election

    let k = key(b"acct-6", b"balance");
    let commit_ts = txn_write(
        &mut sim,
        &node,
        vec![(k.clone(), Some(b"v1".to_vec()))],
        SETTLE,
    )
    .unwrap_or_else(|| panic!("txn_write did not complete (seed={seed})"));
    sim.run_for(SETTLE);
    assert_eq!(block_on(node.local_get(&k)), Some(b"v1".to_vec()));

    // A genuine process restart (`stop`, not `crash`/`restart` — see
    // `witnessing.rs`'s identical idiom): the WAL survives on the same
    // engine; a fresh `RaftKvNode::start` replays it from scratch,
    // re-applying every `TxnStage`/`TxnCommit`/`TxnResolve` entry.
    sim.stop(id.clone());
    let restarted: KvNode =
        RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], engine.clone());
    sim.run_for(ELECT);

    assert_eq!(
        block_on(restarted.local_get(&k)),
        Some(b"v1".to_vec()),
        "WAL replay must re-derive the identical committed value (seed={seed})"
    );

    // The recovered node's own clock must still exceed the recovered
    // commit ts (the witnessing chain, ADR 0018 §2 amendment) — a fresh
    // write must land strictly after it.
    let after_restart = txn_write(
        &mut sim,
        &restarted,
        vec![(key(b"acct-6", b"other"), Some(b"v2".to_vec()))],
        SETTLE,
    )
    .unwrap_or_else(|| panic!("post-restart txn_write did not complete (seed={seed})"));
    assert!(
        after_restart > commit_ts,
        "a post-restart commit ts must strictly exceed the recovered one \
         (pre={commit_ts:?}, post={after_restart:?}, seed={seed})"
    );
}

/// The record/intent scheme, and the fence/seal check `TxnStage` shares
/// with `Batch`, wholesale-reject an entire stage if any of its keys (or
/// its own record key) falls in an already-sealed range — never a partial
/// stage.
#[test]
fn stage_into_a_sealed_range_is_rejected_wholesale() {
    let seed = 0x5EA_0007;
    let sim = Simulator::new(seed);
    let node: KvNode = RaftKvNode::start_scoped(
        sim.env(nid(0)),
        vec![nid(0)],
        MemoryEngine::new(),
        StorageScope::new(b"T:".to_vec(), KeyRange::whole()),
    );
    let mut sim = sim;
    sim.run_for(ELECT);

    // Seal the whole (current) scope range before staging anything.
    let sealed_range = node.scope_range();
    let sealed = node.propose_seal(sealed_range);
    assert!(matches!(sealed, ProposeResult::Accepted { .. }));
    sim.run_for(ELECT);

    let k1 = key(b"acct-7", b"row1");
    let k2 = key(b"acct-7", b"row2");
    let n = node.clone();
    let (kk1, kk2) = (k1.clone(), k2.clone());
    // The stage still *proposes* successfully (a seal is checked at apply,
    // not propose — the same fence-miss doctrine `fenced_commands.rs`
    // exercises for `Put`/`Batch`/`Cas`): `txn_stage` reports
    // `Some((txn_id, record_key))` once the entry has committed and
    // applied, even though apply silently no-ops every write in it.
    let staged = drive(&mut sim, node.env(), SETTLE, async move {
        n.txn_stage(
            "t",
            vec![(kk1, Some(b"v1".to_vec())), (kk2, Some(b"v2".to_vec()))],
        )
        .await
    })
    .flatten();
    assert!(
        staged.is_some(),
        "the stage command itself still commits + applies (seed={seed})"
    );

    sim.run_for(SETTLE);
    // Whole-or-nothing: neither key was staged as an intent (both fell in
    // the sealed range), so both read back as genuinely absent.
    assert_eq!(block_on(node.local_get(&k1)), None, "seed={seed}");
    assert_eq!(block_on(node.local_get(&k2)), None, "seed={seed}");
}

#[test]
fn run_is_deterministic_from_seed() {
    let observe = |seed: u64| {
        let (mut sim, nodes) = group(seed);
        sim.run_for(ELECT);
        let l = leader(&nodes, &[0, 1, 2], seed);
        let k = key(b"acct-8", b"balance");
        let _ = txn_write(&mut sim, &nodes[l], vec![(k, Some(b"v".to_vec()))], SETTLE);
        sim.run_for(SETTLE);
        sim.trace_lines()
    };
    assert_eq!(
        observe(0x9A),
        observe(0x9A),
        "the same seed must reproduce the exact same trace"
    );
}
