//! An aborted transaction must restore the value its intent shadowed, even
//! when the engine no longer holds that value's MVCC history (ADR 0018 §2,
//! the 2026-10-04 "intent carries its prior value" amendment).
//!
//! **The finding** (the real-process chaos harness, `docs/chaos.md`):
//! acknowledged list-append writes were lost on keys touched by an *aborted*
//! cross-tablet transaction. `TxnResolve`'s abort branch used to restore the
//! pre-intent value by reading one MVCC version below the intent,
//! `get_at(key, intent_version - 1)`, and wrote a **tombstone** when that read
//! came back empty. Two ordinary mechanisms make that read come back empty
//! while the key still has a committed value:
//!
//! 1. **LSM tombstone/version GC** (`LsmEngine` compaction): versions below the
//!    GC floor (`max_version - tombstone_grace_versions`, default `1 << 20`)
//!    are collapsed to the newest one at or below the floor. Data-plane
//!    versions are packed HLC timestamps (`wall_ms << 20`), so that floor
//!    trails the newest write by about **one millisecond**: once an intent is
//!    a millisecond old and a compaction runs, the intent is the floor anchor
//!    and the committed value under it is gone.
//! 2. **`InstallSnapshot`**: `engine_image` ships each key's *latest* record
//!    only. A follower caught up by snapshot while an intent is live holds the
//!    intent and nothing under it, on `MemoryEngine` too.
//!
//! Every simulation corpus ran on `MemoryEngine` (which keeps every version
//! forever) and the existing snapshot test staged over a key with no prior
//! value and then *committed*, so neither path was ever exercised.
//!
//! Each test here failed before the fix (the abort, or the eventual read
//! during the pending window, observed absence/tombstone instead of the
//! committed value) and passes after it. Deterministic and seed-reproducible
//! (ADR 0003): driven with `run_for`, never `run()`.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{EnvExt, Metric, MetricsHandle, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, MemoryEngine, StorageEngine};
use animus_tablet::{escape, partition_token};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
const ELECT: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_secs(2);

type LsmNode = RaftKvNode<SimEnv, LsmEngine<SimEnv>>;
type MemNode = RaftKvNode<SimEnv, MemoryEngine>;

/// A real ADR 0022-shaped data-plane key (`txn_stage`'s anchor-token
/// disjointness proof assumes the leading 8-byte token).
fn key(pk: &[u8], rk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out.extend_from_slice(rk);
    out
}

/// Spawn `fut` on `env` and drive `sim` for `budget` — see `txn_single.rs`'s
/// identical helper for why `block_on` would hang here.
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

fn leader<N>(nodes: &[N], is_leader: impl Fn(&N) -> bool, live: &[usize], seed: u64) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(i, n)| live.contains(i) && is_leader(n))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among {live:?}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

/// `LsmOptions` that flush and compact after a handful of small writes, with
/// the **production default** `tombstone_grace_versions` (only the
/// size/count thresholds shrink — the GC floor is exactly what `animusd`'s
/// tablet engines run with).
fn compacting_opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 256,
        compaction_trigger: 2,
        target_table_bytes: 1024,
        level_fanout: 2,
        wal_segment_bytes: 512,
        background_maintenance: false,
        ..LsmOptions::default()
    }
}

fn lsm_group(seed: u64) -> (Simulator, Vec<LsmNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            let env = sim.env(nid(id));
            let engine = block_on(LsmEngine::open_with(
                env.clone(),
                "tablet/",
                compacting_opts(),
            ))
            .expect("open LsmEngine");
            RaftKvNode::start(env, NODES.iter().copied().map(nid).collect(), engine)
        })
        .collect();
    (sim, nodes)
}

/// Unrelated single-key writes spread over `n * 5ms` of virtual time: each
/// advances the engine's newest version by several milliseconds of HLC and
/// pushes the memtable past its flush threshold, so compactions run with a GC
/// floor well above an intent staged before the burst.
fn compaction_burst(sim: &mut Simulator, leader: &LsmNode, n: u64, seed: u64) {
    for i in 0..n {
        let k = key(format!("filler-{i:04}").as_bytes(), b"r");
        match leader.put(k, vec![b'x'; 48]) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected filler put {i}: {other:?} (seed={seed})"),
        }
        sim.run_for(Duration::from_millis(5));
    }
    sim.run_for(SETTLE);
}

/// Stage an intent over a committed value, let compaction GC the engine past
/// the intent, then abort: every replica must still hold the committed value.
/// Before the fix every replica wrote a tombstone (the committed value — an
/// acknowledged write — was lost cluster-wide).
#[test]
fn abort_after_lsm_compaction_restores_the_committed_value() {
    let seed = 0x0AB0_0001;
    let (mut sim, nodes) = lsm_group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, LsmNode::is_leader, &[0, 1, 2], seed);

    let k = key(b"list-7", b"v");
    assert!(matches!(
        nodes[l].put(k.clone(), b"acked-committed".to_vec()),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&k)),
            Some(b"acked-committed".to_vec()),
            "node {i}: committed value replicated (seed={seed})"
        );
    }

    let n = nodes[l].clone();
    let kk = k.clone();
    let (txn_id, record_key, _) = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        n.txn_stage("t", vec![(kk, Some(b"staged-never-committed".to_vec()))])
            .await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage did not complete (seed={seed})"));

    compaction_burst(&mut sim, &nodes[l], 80, seed);

    // While the intent is still pending, the ADR 0055 eventual read falls back
    // to the key's last committed value — before the fix it read the GC'd
    // history and reported the key absent.
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.stale_get_served(&k)),
            Some(Some(b"acked-committed".to_vec())),
            "node {i}: an eventual read under a pending intent must serve the last \
             committed value, not absence (seed={seed})"
        );
    }

    let n = nodes[l].clone();
    let kk = k.clone();
    let decided = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        n.txn_decide(txn_id, record_key, vec![kk], false).await
    })
    .flatten();
    assert!(decided.is_some(), "abort must complete (seed={seed})");
    sim.run_for(SETTLE);

    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&k)),
            Some(b"acked-committed".to_vec()),
            "node {i}: an aborted intent must restore the committed value it shadowed \
             even after compaction GC'd the engine's history (seed={seed})"
        );
    }
}

/// A staged **delete** over a committed value, aborted after compaction, must
/// likewise restore the value (the abort branch is the same; a delete intent
/// carries `staged_value: None`, which must not be confused with the prior).
#[test]
fn aborted_delete_intent_after_lsm_compaction_restores_the_committed_value() {
    let seed = 0x0AB0_0002;
    let (mut sim, nodes) = lsm_group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, LsmNode::is_leader, &[0, 1, 2], seed);

    let k = key(b"list-8", b"v");
    assert!(matches!(
        nodes[l].put(k.clone(), b"keep-me".to_vec()),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    let n = nodes[l].clone();
    let kk = k.clone();
    let (txn_id, record_key, _) = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        n.txn_stage("t", vec![(kk, None)]).await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage did not complete (seed={seed})"));

    compaction_burst(&mut sim, &nodes[l], 80, seed);

    let n = nodes[l].clone();
    let kk = k.clone();
    assert!(
        drive(&mut sim, nodes[l].env(), SETTLE, async move {
            n.txn_decide(txn_id, record_key, vec![kk], false).await
        })
        .flatten()
        .is_some(),
        "abort must complete (seed={seed})"
    );
    sim.run_for(SETTLE);
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&k)),
            Some(b"keep-me".to_vec()),
            "node {i}: an aborted delete intent must restore the committed value (seed={seed})"
        );
    }
}

/// A key that had **no** committed value before the intent must still abort
/// to absence (the prior is genuinely `None`), after compaction too.
#[test]
fn abort_of_an_intent_over_an_absent_key_stays_absent_after_compaction() {
    let seed = 0x0AB0_0003;
    let (mut sim, nodes) = lsm_group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, LsmNode::is_leader, &[0, 1, 2], seed);

    let k = key(b"list-9", b"v");
    let n = nodes[l].clone();
    let kk = k.clone();
    let (txn_id, record_key, _) = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        n.txn_stage("t", vec![(kk, Some(b"never".to_vec()))]).await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage did not complete (seed={seed})"));
    compaction_burst(&mut sim, &nodes[l], 40, seed);
    let n = nodes[l].clone();
    let kk = k.clone();
    assert!(
        drive(&mut sim, nodes[l].env(), SETTLE, async move {
            n.txn_decide(txn_id, record_key, vec![kk], false).await
        })
        .flatten()
        .is_some()
    );
    sim.run_for(SETTLE);
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&k)),
            None,
            "node {i}: abort over an absent key must stay absent (seed={seed})"
        );
    }
}

/// A follower caught up by `InstallSnapshot` while an intent is live only
/// receives the key's latest record — the intent. Aborting afterwards must
/// still restore the committed value **on that follower**, not tombstone it
/// (before the fix it diverged from the leader: the leader restored, the
/// snapshot-caught-up follower tombstoned, so a later leadership change onto
/// it lost the acknowledged write). `MemoryEngine`: no GC is involved at all.
///
/// The follower is partitioned from the leader before the committed write and
/// kept out for more than `COMPACT_RETENTION_CAP_ENTRIES` (4096) entries, so
/// follower-aware compaction stops retaining the log for it and it can only
/// catch up by snapshot (asserted via `CpSnapshotInstalls`; shape borrowed
/// from `follower_aware_compaction.rs`).
#[test]
fn abort_restores_the_committed_value_on_a_snapshot_caught_up_follower() {
    let seed = 0x0AB0_0004;
    let mut sim = Simulator::new(seed);
    let handles: Vec<MetricsHandle> = NODES.iter().map(|_| MetricsHandle::recording()).collect();
    let nodes: Vec<MemNode> = NODES
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            RaftKvNode::start_with_metrics(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                handles[i].clone(),
            )
        })
        .collect();
    sim.run_for(ELECT);
    let l = leader(&nodes, MemNode::is_leader, &[0, 1, 2], seed);
    let lagging = (0..3).find(|&i| i != l).expect("a follower exists");

    // The lagging follower never sees the committed write through its log.
    sim.partition_pair(nid(NODES[l]), nid(NODES[lagging]));

    let k = key(b"acct", b"balance");
    assert!(matches!(
        nodes[l].put(k.clone(), b"acked-committed".to_vec()),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    let n = nodes[l].clone();
    let kk = k.clone();
    let (txn_id, record_key, _) = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        n.txn_stage("t", vec![(kk, Some(b"staged".to_vec()))]).await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage did not complete (seed={seed})"));

    // Well past the retention cap, so the partitioned voter falls off the
    // compacted log.
    for round in 0..92u64 {
        for j in 0..50u64 {
            let _ = nodes[l].put(format!("k-{round}-{j}").into_bytes(), b"v".to_vec());
        }
        sim.run_for(Duration::from_millis(5));
    }
    sim.run_for(Duration::from_secs(3));
    sim.heal(nid(NODES[l]), nid(NODES[lagging]));
    sim.heal(nid(NODES[lagging]), nid(NODES[l]));
    let mut installed = false;
    for _ in 0..600 {
        sim.run_for(Duration::from_millis(50));
        if handles[lagging].get(Metric::CpSnapshotInstalls) > 0
            && nodes[lagging].engine_applied_index() >= nodes[l].engine_applied_index()
        {
            installed = true;
            break;
        }
    }
    assert!(
        installed,
        "precondition: follower {lagging} must catch up via InstallSnapshot (seed={seed})"
    );

    // Precondition: the follower holds the intent via its snapshot image.
    let raw = block_on(
        nodes[lagging]
            .storage()
            .get(&nodes[lagging].physical_key(animus_cp_data::KIND_BASE, &k)),
    )
    .expect("engine read ok")
    .unwrap_or_else(|| panic!("follower {lagging} missing the staged intent (seed={seed})"));
    assert_ne!(
        raw.value.first().copied(),
        Some(0u8),
        "precondition: follower {lagging} holds an intent envelope, not a committed value \
         (seed={seed})"
    );
    assert_eq!(
        block_on(nodes[lagging].stale_get_served(&k)),
        Some(Some(b"acked-committed".to_vec())),
        "follower {lagging}: an eventual read under the snapshot-shipped intent must serve \
         the committed value (seed={seed})"
    );

    let n = nodes[l].clone();
    let kk = k.clone();
    assert!(
        drive(&mut sim, nodes[l].env(), SETTLE, async move {
            n.txn_decide(txn_id, record_key, vec![kk], false).await
        })
        .flatten()
        .is_some(),
        "abort must complete (seed={seed})"
    );
    sim.run_for(Duration::from_secs(3));

    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&k)),
            Some(b"acked-committed".to_vec()),
            "node {i}: the abort must restore the committed value on every replica, \
             including the snapshot-caught-up follower (seed={seed})"
        );
    }
}
