//! A `TxnStage` that the live apply REJECTED must also be a no-op when WAL
//! recovery re-applies it over an engine that already holds later entries'
//! effects (issue #1242).
//!
//! A restarted replica replays its log tail from `snapshot_index` over its own
//! durable engine, which is typically far ahead of that start. Every
//! `TxnStage` decision reads engine state, so on replay it saw *future*
//! state: a stale/duplicate stage (rejected live by a resolved marker) or a
//! stage blocked by a since-resolved foreign intent was **accepted**, and its
//! intent merge landed on every key with no later write — an intent
//! resurrected on this replica only. That intent then blocked every later
//! stage touching the key here (whole-or-nothing), so the acknowledged
//! transaction's *other* key silently never applied on this replica: a
//! `ConsistentRead: false` read missing acked appends for the rest of the run
//! (the chaos smoke's `[eventual-prefix]`, with only transaction halves
//! missing).
//!
//! Each test drives one such rejected stage, restarts a follower as a fresh
//! process over its retained engine (replay), and asserts every replica's
//! engine is identical to the leader's. `txn_replay_corpus` additionally
//! replays a seeded random schedule of transactions, stale re-stages,
//! duplicate resolves, crashes, fresh-process restarts and compaction.
//!
//! Deterministic and seed-reproducible (ADR 0003): `ANIMUS_SEED=<seed>`
//! replays one corpus schedule, `ANIMUS_TXN_REPLAY_SEEDS=K` widens it.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::Metadata;
use animus_control::version::ClusterFeatures;
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{
    HostedOptions, RaftKvNode, StageOutcome, StorageScope, TxnId, TxnOutcome, TxnWrite,
};
use animus_env::{EnvExt, PRIMARY_STREAM, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{escape, partition_token};
use futures::executor::block_on;

pub(crate) type RawRows = Vec<(Vec<u8>, Option<Vec<u8>>, u64)>;
pub(crate) type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

pub(crate) const SETTLE: Duration = Duration::from_secs(2);
pub(crate) const BASE_SEED: u64 = 0x1242_0001;
/// Seeds whose schedule diverged on `origin/main` before the fix.
pub(crate) const KNOWN_FAILING_SEEDS: [u64; 1] = [2_882_513_034];
/// Seeds whose schedule diverged at cluster version 1 on `origin/main`
/// before the snapshot-marker fix (issue #1251): a replica caught up by an
/// `InstallSnapshot` lacked the resolved markers the other replicas held.
pub(crate) const KNOWN_FAILING_SEEDS_V1: [u64; 1] = [2_882_520_953];

pub(crate) fn key(pk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out.extend_from_slice(b"rk");
    out
}

pub(crate) fn drive<T: Send + 'static>(
    sim: &mut Simulator,
    env: &SimEnv,
    budget: Duration,
    fut: impl Future<Output = T> + Send + 'static,
) -> Option<T> {
    let slot = Arc::new(Mutex::new(None));
    let s = slot.clone();
    env.spawn_task(async move {
        let v = fut.await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock().unwrap().take()
}

pub(crate) fn voters() -> Vec<animus_env::NodeId> {
    (0..3u64).map(nid).collect()
}

pub(crate) fn start_node(
    sim: &Simulator,
    id: u64,
    engine: MemoryEngine,
    cluster_version: u32,
) -> KvNode {
    let features = ClusterFeatures::new();
    features.update(&Metadata {
        // 1: `Gate::GlobalTables` closed (N-1 replicas may exist, so
        // `engine_image` ships markers through the ignorable wire kind);
        // 2: open (markers ship as base rows). Apply is identical either way.
        cluster_version,
        ..Metadata::default()
    });
    RaftKvNode::start_hosted_with_options(
        sim.env(nid(id)),
        voters(),
        engine,
        StorageScope::whole(),
        PRIMARY_STREAM,
        HostedOptions {
            features,
            ..HostedOptions::default()
        },
    )
}

pub(crate) struct Cluster {
    pub(crate) sim: Simulator,
    pub(crate) engines: Vec<MemoryEngine>,
    pub(crate) nodes: Vec<KvNode>,
    pub(crate) up: [bool; 3],
    pub(crate) seed: u64,
    pub(crate) version: u32,
}

impl Cluster {
    pub(crate) fn new(seed: u64) -> Self {
        Self::new_at(seed, 2)
    }

    pub(crate) fn new_at(seed: u64, version: u32) -> Self {
        let mut sim = Simulator::new(seed);
        let engines: Vec<MemoryEngine> = (0..3).map(|_| MemoryEngine::new()).collect();
        let nodes = (0..3u64)
            .map(|i| start_node(&sim, i, engines[i as usize].clone(), version))
            .collect();
        sim.run_for(Duration::from_secs(2));
        Cluster {
            sim,
            engines,
            nodes,
            up: [true; 3],
            seed,
            version,
        }
    }

    pub(crate) fn leader(&self) -> Option<usize> {
        let ls: Vec<usize> = (0..3)
            .filter(|&i| self.up[i] && self.nodes[i].is_leader())
            .collect();
        (ls.len() == 1).then(|| ls[0])
    }

    pub(crate) fn stage(
        &mut self,
        l: usize,
        id: &TxnId,
        record_key: &[u8],
        keys: &[Vec<u8>],
        value: &str,
    ) -> Option<(HlcTimestamp, StageOutcome)> {
        // An empty `value` stages a delete (tombstone) of every key.
        let v = (!value.is_empty()).then(|| value.as_bytes().to_vec());
        let writes: Vec<TxnWrite> = keys
            .iter()
            .map(|k| TxnWrite::plain(k.clone(), v.clone()))
            .collect();
        let n = self.nodes[l].clone();
        let (id, rk) = (id.clone(), record_key.to_vec());
        drive(&mut self.sim, self.nodes[l].env(), SETTLE, async move {
            n.txn_stage_participant(id, rk, "t".into(), writes, Vec::new())
                .await
        })
        .flatten()
    }

    pub(crate) fn resolve(
        &mut self,
        l: usize,
        id: &TxnId,
        record_key: &[u8],
        keys: &[Vec<u8>],
        outcome: TxnOutcome,
    ) {
        let n = self.nodes[l].clone();
        let (id, rk, ks) = (id.clone(), record_key.to_vec(), keys.to_vec());
        let seed = self.seed;
        drive(&mut self.sim, self.nodes[l].env(), SETTLE, async move {
            n.txn_resolve(id, rk, ks, outcome).await
        })
        .flatten()
        .unwrap_or_else(|| panic!("resolve did not complete (seed={seed})"));
    }

    /// Stage then commit-resolve one transaction on the leader.
    pub(crate) fn commit(&mut self, id: &TxnId, record_key: &[u8], keys: &[Vec<u8>], value: &str) {
        let l = self.leader().expect("leader");
        let (ts, outcome) = self
            .stage(l, id, record_key, keys, value)
            .expect("stage completes");
        assert_eq!(outcome, StageOutcome::Staged, "seed={}", self.seed);
        self.resolve(
            l,
            id,
            record_key,
            keys,
            TxnOutcome::Committed { commit_ts: ts },
        );
    }

    /// A fresh process over the retained engine: WAL replay of the whole log
    /// tail over an engine that already holds all of it.
    pub(crate) fn restart_fresh(&mut self, i: usize) {
        self.sim.stop(nid(i as u64));
        self.nodes[i] = start_node(&self.sim, i as u64, self.engines[i].clone(), self.version);
        self.up[i] = true;
        self.sim.run_for(Duration::from_secs(2));
    }

    /// Raw base-row state of `k` on every replica: `(version, value)` of the
    /// row itself (an intent shows as a non-committed envelope tag).
    pub(crate) fn raw(&self, k: &[u8]) -> Vec<Option<(u64, Vec<u8>)>> {
        self.engines
            .iter()
            .map(|e| {
                block_on(e.entries())
                    .unwrap()
                    .into_iter()
                    .find(|(pk, _)| pk.len() == k.len() + 1 && pk.ends_with(k))
                    .map(|(_, vv)| (vv.version, self.v1_form(vv.value)))
            })
            .collect()
    }

    /// Below `Gate::GlobalTables` a snapshot sender ships every v2 intent as
    /// its v1 form (#1237): an installed replica legitimately holds the same
    /// intent in the v1 encoding, so a version-1 cluster compares in that form
    /// and only a real divergence (a missing/extra/different row) shows.
    fn v1_form(&self, value: Vec<u8>) -> Vec<u8> {
        if self.version < 2 {
            animus_cp_data::downgrade_txn_envelope_to_v1(&value).unwrap_or(value)
        } else {
            value
        }
    }

    /// Every replica's whole raw keyspace INCLUDING tombstones, the txn anchor
    /// records and the resolved markers: `(key, value-or-tombstone, version)`
    /// (minus the per-replica `__animus_system` cursors).
    pub(crate) fn raw_all(&self) -> Vec<RawRows> {
        self.engines
            .iter()
            .map(|e| {
                let mut rows = block_on(e.entries_with_tombstones()).unwrap();
                // Per-replica progress cursors (`cp_applied`, `cp_hlc_hwm`)
                // legitimately differ in timing; everything else must match.
                rows.retain(|(k, _, _)| !k.starts_with(b"__animus_system"));
                for (_, v, _) in &mut rows {
                    *v = v.take().map(|b| self.v1_form(b));
                }
                rows
            })
            .collect()
    }

    pub(crate) fn assert_identical(&self, keys: &[&Vec<u8>], what: &str) {
        let all = self.raw_all();
        for (i, rows) in all.iter().enumerate().skip(1) {
            if rows != &all[0] {
                let only0: Vec<_> = all[0].iter().filter(|r| !rows.contains(r)).collect();
                let onlyi: Vec<_> = rows.iter().filter(|r| !all[0].contains(r)).collect();
                panic!(
                    "replica {i} raw rows (incl. tombstones, anchor records, resolved markers) \
                     diverged from replica 0 ({what}) (seed={}): only on 0: {only0:?}; only on {i}: {onlyi:?}",
                    self.seed
                );
            }
        }
        for k in keys {
            let raw = self.raw(k);
            assert!(
                raw.windows(2).all(|w| w[0] == w[1]),
                "replicas diverged ({what}) (seed={}): {raw:?}",
                self.seed
            );
        }
    }
}

pub(crate) fn txn_id(n: u64) -> TxnId {
    TxnId {
        ts: HlcTimestamp {
            wall_ms: n,
            logical: 0,
        },
        node: nid(9),
    }
}

/// Live: the stale stage of T1 is rejected because key B's resolved marker
/// still names T1. Replay: B's marker has since been overwritten by T5, so no
/// marker names T1 any more; key A (last written before the stale stage, by
/// T4) has no later write, so the stage's merge landed there.
#[test]
pub(crate) fn stale_two_key_restage_is_not_resurrected_by_replay() {
    let mut c = Cluster::new(BASE_SEED);
    let rk = key(b"anchor-elsewhere");
    let (a, b) = (key(b"A"), key(b"B"));
    c.commit(&txn_id(1), &rk, &[a.clone(), b.clone()], "t1");
    c.commit(&txn_id(4), &rk, std::slice::from_ref(&a), "t4");
    // Duplicate stage of the already-resolved T1 (a retried prepare).
    let l = c.leader().unwrap();
    let (_, outcome) = c
        .stage(l, &txn_id(1), &rk, &[a.clone(), b.clone()], "stale")
        .expect("stale stage completes");
    assert_eq!(outcome, StageOutcome::Fenced, "live rejection");
    c.commit(&txn_id(5), &rk, std::slice::from_ref(&b), "t5");
    c.sim.run_for(SETTLE);
    c.assert_identical(&[&a, &b], "before restart");

    let follower = (0..3).find(|&i| i != c.leader().unwrap()).unwrap();
    c.restart_fresh(follower);
    c.sim.run_for(SETTLE);
    c.assert_identical(&[&a, &b], "after follower replay");
    // The committed values survived, no intent anywhere.
    for (k, want) in [(&a, "t4"), (&b, "t5")] {
        for (i, raw) in c.raw(k).into_iter().enumerate() {
            let (_, v) = raw.unwrap_or_else(|| panic!("node {i} lost a row"));
            assert_eq!(v[0], 0, "node {i}: committed envelope, not an intent");
            assert_eq!(&v[1..], want.as_bytes(), "node {i}");
        }
    }
}

/// Live: a stage blocked by X's unresolved intent on A is a no-op (and its
/// transaction's coordinator then dies, so nothing ever resolves it). Replay: X has since
/// been resolved, nothing blocks, and the stage's merge landed an orphan
/// intent on the partner key B.
#[test]
pub(crate) fn blocked_stage_is_not_resurrected_by_replay() {
    let mut c = Cluster::new(BASE_SEED + 1);
    let rk = key(b"anchor-elsewhere");
    let (a, b) = (key(b"A"), key(b"B"));
    let l = c.leader().unwrap();
    let (xts, outcome) = c
        .stage(l, &txn_id(1), &rk, std::slice::from_ref(&a), "x")
        .unwrap();
    assert_eq!(outcome, StageOutcome::Staged);
    let (_, outcome) = c
        .stage(l, &txn_id(2), &rk, &[a.clone(), b.clone()], "t2")
        .unwrap();
    assert!(
        matches!(outcome, StageOutcome::IntentBlocked { .. }),
        "live: blocked by X, got {outcome:?}"
    );
    c.resolve(
        l,
        &txn_id(1),
        &rk,
        std::slice::from_ref(&a),
        TxnOutcome::Committed { commit_ts: xts },
    );
    // T2's coordinator died (kill -9) after the blocked stage: nothing ever
    // resolves T2 on this group, because nothing of T2 ever staged here.
    c.sim.run_for(SETTLE);
    c.assert_identical(&[&a, &b], "before restart");

    let follower = (0..3).find(|&i| i != c.leader().unwrap()).unwrap();
    c.restart_fresh(follower);
    c.sim.run_for(SETTLE);
    c.assert_identical(&[&a, &b], "after follower replay");
    assert!(
        c.raw(&b).iter().all(Option::is_none),
        "B was never staged by anything that committed: no replica holds a row"
    );
}

/// Live: T2's stage {A,B} carries the own-key condition "A must be absent" and
/// is rejected (`ConditionFailed`) because A holds T0's committed value. A
/// plain delete then tombstones A at a version above T2's stage; B has no later
/// write and T2 has no marker. Replay reads A through a plain `get`, where a
/// tombstone is invisible, so "A absent" now holds and an ahead check built on
/// `get` would accept the stage: an orphan intent on B. Only a tombstone-aware
/// version read sees that a later entry (the delete) already ran.
#[test]
pub(crate) fn condition_failed_stage_is_not_resurrected_when_the_key_was_deleted_after() {
    let mut c = Cluster::new(BASE_SEED + 2);
    let rk = key(b"anchor-elsewhere");
    let (a, b) = (key(b"A"), key(b"B"));
    c.commit(&txn_id(1), &rk, std::slice::from_ref(&a), "t0");
    let l = c.leader().unwrap();
    let writes = vec![
        TxnWrite::plain(a.clone(), Some(b"t2".to_vec())),
        TxnWrite::plain(b.clone(), Some(b"t2".to_vec())),
    ];
    let (n, id, rk2, cond) = (
        c.nodes[l].clone(),
        txn_id(2),
        rk.clone(),
        vec![(a.clone(), None)],
    );
    let (_, outcome) = drive(&mut c.sim, c.nodes[l].env(), SETTLE, async move {
        n.txn_stage_participant(id, rk2, "t".into(), writes, cond)
            .await
    })
    .flatten()
    .expect("stage completes");
    assert!(
        !matches!(outcome, StageOutcome::Staged),
        "live: condition 'A absent' must fail, got {outcome:?}"
    );
    // A DeleteItem / TTL reap of A after the rejected stage.
    let _ = c.nodes[l].delete(a.clone());
    c.sim.run_for(SETTLE);
    c.assert_identical(&[&a, &b], "before restart");

    let follower = (0..3).find(|&i| i != c.leader().unwrap()).unwrap();
    c.restart_fresh(follower);
    c.sim.run_for(SETTLE);
    c.assert_identical(&[&a, &b], "after follower replay");
    assert!(
        c.raw(&b).iter().all(Option::is_none),
        "no orphan intent on B"
    );
}

// ---- seeded corpus ---------------------------------------------------------

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn run_schedule(seed: u64, version: u32) {
    let mut c = Cluster::new_at(seed, version);
    let keys: Vec<Vec<u8>> = (0..3).map(|i| key(format!("k{i}").as_bytes())).collect();
    let rk = key(b"anchor-elsewhere");
    let mut rng = Rng(seed | 1);
    let mut done: Vec<(TxnId, Vec<Vec<u8>>, HlcTimestamp)> = Vec::new();
    let (mut counter, mut pad) = (0u64, 0u64);
    for _ in 0..80 {
        let Some(l) = c.leader() else {
            c.sim.run_for(Duration::from_secs(1));
            continue;
        };
        match rng.below(14) {
            0..=5 => {
                counter += 1;
                let id = txn_id(counter);
                let a = rng.below(3) as usize;
                let b = (a + 1 + rng.below(2) as usize) % 3;
                let ks = vec![keys[a].clone(), keys[b].clone()];
                let val = if rng.below(4) == 0 {
                    String::new() // a delete
                } else {
                    format!("v{counter}")
                };
                if let Some((ts, _)) = c.stage(l, &id, &rk, &ks, &val) {
                    let out = if rng.below(6) == 0 {
                        TxnOutcome::Aborted
                    } else {
                        TxnOutcome::Committed { commit_ts: ts }
                    };
                    let (n, id2, rk2, ks2) =
                        (c.nodes[l].clone(), id.clone(), rk.clone(), ks.clone());
                    let _ = drive(&mut c.sim, c.nodes[l].env(), SETTLE, async move {
                        n.txn_resolve(id2, rk2, ks2, out).await
                    });
                    done.push((id, ks, ts));
                }
            }
            6 => {
                // A stale/duplicate stage of an earlier transaction.
                if let Some((id, ks, _)) = done.get(rng.below(done.len().max(1) as u64) as usize) {
                    let (id, ks) = (id.clone(), ks.clone());
                    let _ = c.stage(l, &id, &rk, &ks, "stale");
                }
            }
            7 => {
                // A duplicate resolve of an earlier transaction.
                if let Some((id, ks, ts)) = done
                    .get(rng.below(done.len().max(1) as u64) as usize)
                    .cloned()
                {
                    let (n, rk2) = (c.nodes[l].clone(), rk.clone());
                    let _ = drive(&mut c.sim, c.nodes[l].env(), SETTLE, async move {
                        n.txn_resolve(id, rk2, ks, TxnOutcome::Committed { commit_ts: ts })
                            .await
                    });
                }
            }
            8 => {
                let i = rng.below(3) as usize;
                if c.up[i] {
                    if c.up.iter().filter(|u| **u).count() > 2 {
                        c.sim.crash(nid(i as u64));
                        c.up[i] = false;
                    }
                } else {
                    c.sim.restart(nid(i as u64));
                    c.up[i] = true;
                }
                c.sim.run_for(Duration::from_secs(2));
            }
            9 => {
                let i = rng.below(3) as usize;
                if c.up[i] {
                    c.restart_fresh(i);
                }
            }
            10 => {
                // Pad past the compaction threshold.
                for _ in 0..rng.below(5000) {
                    pad += 1;
                    let _ = c.nodes[l].put(format!("pad{pad:06}").into_bytes(), b"x".to_vec());
                }
                c.sim.run_for(Duration::from_secs(3));
            }
            11 => {
                // A plain delete (DeleteItem / TTL-reap shape) of a txn key.
                let k = keys[rng.below(3) as usize].clone();
                let _ = c.nodes[l].delete(k);
                c.sim.run_for(Duration::from_millis(200));
            }
            _ => c.sim.run_for(Duration::from_millis(500)),
        }
    }
    for i in 0..3 {
        if !c.up[i] {
            c.sim.restart(nid(i as u64));
            c.up[i] = true;
        }
    }
    c.sim.run_for(Duration::from_secs(20));
    let refs: Vec<&Vec<u8>> = keys.iter().collect();
    c.assert_identical(&refs, "end of schedule");
}

#[test]
fn txn_replay_corpus() {
    let seeds: Vec<u64> = if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        vec![s]
    } else {
        let k = std::env::var("ANIMUS_TXN_REPLAY_SEEDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(2)
            .max(1);
        KNOWN_FAILING_SEEDS
            .iter()
            .copied()
            .chain((0..k).map(|i| BASE_SEED + i * 7919))
            .collect()
    };
    for seed in seeds {
        run_schedule(seed, 2);
    }
}

/// The same schedule corpus at cluster version 1 (`Gate::GlobalTables`
/// closed: every unfinalized cluster, fresh ones included). Snapshot images
/// must still carry the resolved markers (issue #1251), so a replica caught
/// up by `InstallSnapshot` decides a stale re-stage exactly as its peers do.
#[test]
fn txn_replay_corpus_at_cluster_version_1() {
    let seeds: Vec<u64> = if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        vec![s]
    } else {
        let k = std::env::var("ANIMUS_TXN_REPLAY_SEEDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(2)
            .max(1);
        KNOWN_FAILING_SEEDS_V1
            .iter()
            .copied()
            .chain((0..k).map(|i| BASE_SEED + i * 7919))
            .collect()
    };
    for seed in seeds {
        run_schedule(seed, 1);
    }
}
