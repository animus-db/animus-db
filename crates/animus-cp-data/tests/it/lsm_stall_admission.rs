//! ADR 0081 composition cell: an LSM write stall under a replicated tablet
//! group surfaces as `ProposeResult::Busy` through the ADR 0080 / #1330 propose
//! bound, never as a panic or an `Err` in the apply task.
//!
//! Three `RaftKvNode`s, each on its own `LsmEngine<SimEnv>` with the L0 stall
//! on (3T slowdown, 5T stop) and a per-node [`Wedge`] gate that withholds every
//! compaction (the stand-in for "compaction cannot keep up", see the storage
//! corpus). The apply task of a wedged node therefore parks in the stall; the
//! leader's propose bound (own backlog, and with `Gate::FollowerAppliedIndex`
//! open the quorum's applied index) must turn that into a definite `Busy`.
//!
//! Oracles:
//!
//! - **no panic**: stepping the sim never panics (an `Err` out of a stalled
//!   write used to take the apply task down);
//! - **bounded backlog**: the leader's `last_log - engine_applied` stays within
//!   the configured bound plus slack;
//! - **refused is absent**: a key whose `put` returned `Busy` is on no replica
//!   after the stall clears (a `Busy` is definite);
//! - **acked survives**: every accepted key reads back on the leader after the
//!   drain;
//! - **liveness**: after the wedge lifts the backlog drains and a fresh write
//!   is accepted;
//! - **quorum shape**: one wedged follower causes no refusal, a wedged leader
//!   or both wedged followers do (#1330);
//! - **resolve admitted**: a `Liveness`-class `txn_resolve` is admitted during
//!   the stall and completes once it clears.
//!
//! Controls: (b) the pre-ADR gate (`StallFault::OldGate`, post-append, ends in
//! `Err` after 10 s) must violate the no-panic oracle.
//!
//! Knobs: `ANIMUS_LSM_STALL_SEEDS=K` (default 1), `ANIMUS_SEED=<seed>` replays.

use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_control::version::ClusterFeatures;
use animus_cp_data::TxnDecisionStatus;
use animus_cp_data::admission::{BacklogLimits, NodeAdmission};
use animus_cp_data::campaign_governor::ElectionBackpressure;
use animus_cp_data::{HostedOptions, RaftKvNode, StageOutcome, StorageScope, TxnOutcome};
use animus_env::{EnvExt, MetricsHandle, PRIMARY_STREAM, QueuedBytes, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{CompactionGate, CompactionPermit, LsmEngine, LsmOptions, StallFault};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, LsmEngine<SimEnv>>;

const NODES: [u64; 3] = [0, 1, 2];
const BOUND_ENTRIES: u64 = 64;
const BOUND_BYTES: u64 = 1 << 20;
const SLACK: u64 = 24;
const CLUSTER_VERSION: u32 = 6;
const PER_TICK: usize = 8;
const TICK: Duration = Duration::from_millis(10);
const VALUE_BYTES: usize = 100;

struct Wedge(Arc<AtomicBool>);
impl CompactionGate for Wedge {
    fn acquire(&self, _bypass: bool) -> Option<CompactionPermit> {
        if self.0.load(Ordering::Acquire) {
            None
        } else {
            Some(Box::new(()))
        }
    }
}

fn seeds() -> Vec<u64> {
    if let Ok(s) = std::env::var("ANIMUS_SEED")
        && let Ok(v) = s.parse::<u64>()
    {
        return vec![v];
    }
    let k = std::env::var("ANIMUS_LSM_STALL_SEEDS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1)
        .max(1);
    (0..k).map(|i| 0x0081_0000 + i).collect()
}

fn cell_enabled(name: &str) -> bool {
    std::env::var("ANIMUS_LSM_STALL_CELL")
        .ok()
        .is_none_or(|f| name.contains(&f))
}

fn engine_opts(fault: StallFault) -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 512,
        compaction_trigger: 4,
        target_table_bytes: 2048,
        level_fanout: 2,
        wal_segment_bytes: 1 << 20,
        background_maintenance: true,
        l0_slowdown_factor: 3,
        l0_stop_factor: 5,
        l0_slowdown_write_rate: 32 << 10,
        l0_min_write_rate: 4 << 10,
        stall_fault: fault,
        ..LsmOptions::default()
    }
}

struct Rig {
    sim: Simulator,
    nodes: Vec<KvNode>,
    engines: Vec<LsmEngine<SimEnv>>,
    wedges: Vec<Arc<AtomicBool>>,
    alive: BTreeSet<usize>,
}

impl Rig {
    fn new(seed: u64, fault: StallFault) -> Self {
        let sim = Simulator::new(seed);
        let voters: Vec<_> = NODES.iter().copied().map(nid).collect();
        let mut nodes = Vec::new();
        let mut engines = Vec::new();
        let mut wedges = Vec::new();
        for &id in &NODES {
            let wedge = Arc::new(AtomicBool::new(false));
            let engine = block_on(LsmEngine::open_with(
                sim.env(nid(id)),
                "kv/",
                engine_opts(fault),
            ))
            .expect("open");
            engine.set_compaction_gate(Arc::new(Wedge(wedge.clone())));
            let features = ClusterFeatures::new();
            features.update(&animus_control::Metadata {
                cluster_version: CLUSTER_VERSION,
                ..animus_control::Metadata::default()
            });
            let adm = NodeAdmission::new(
                BacklogLimits {
                    max_entries: BOUND_ENTRIES,
                    max_bytes: BOUND_BYTES,
                },
                QueuedBytes::new(0),
                MetricsHandle::recording(),
            );
            nodes.push(RaftKvNode::start_hosted_with_options(
                sim.env(nid(id)),
                voters.clone(),
                engine.clone(),
                StorageScope::whole(),
                PRIMARY_STREAM,
                HostedOptions {
                    features,
                    backpressure: ElectionBackpressure {
                        admission: Some(adm),
                        ..ElectionBackpressure::default()
                    },
                    ..HostedOptions::default()
                },
            ));
            engines.push(engine);
            wedges.push(wedge);
        }
        let mut cfg = DiskConfig::default();
        cfg.set_sync_delay(Duration::from_millis(2));
        sim.set_disk_config(cfg);
        Self {
            sim,
            nodes,
            engines,
            wedges,
            alive: (0..NODES.len()).collect(),
        }
    }

    fn leader(&self) -> Option<usize> {
        let ls: Vec<usize> = self
            .alive
            .iter()
            .copied()
            .filter(|&i| self.nodes[i].is_leader())
            .collect();
        (ls.len() == 1).then(|| ls[0])
    }

    /// Step the sim, converting a panic into a violation string.
    fn step(&mut self, d: Duration) -> Result<(), String> {
        let sim = &mut self.sim;
        catch_unwind(AssertUnwindSafe(|| sim.run_for(d))).map_err(|e| {
            let m = e
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| (*s).to_string()))
                .unwrap_or_default();
            format!("no-panic: the sim panicked: {m}")
        })
    }

    fn wait_leader(&mut self) -> Result<usize, String> {
        for _ in 0..200 {
            self.step(Duration::from_millis(50))?;
            if let Some(l) = self.leader() {
                return Ok(l);
            }
        }
        Err("liveness: no leader elected".into())
    }

    fn run_async<T: Clone + Send + 'static>(
        &mut self,
        i: usize,
        budget: Duration,
        fut: impl std::future::Future<Output = T> + Send + 'static,
    ) -> Result<Option<T>, String> {
        let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
        let s = Arc::clone(&slot);
        self.nodes[i].env().clone().spawn_task(async move {
            let v = fut.await;
            *s.lock().unwrap() = Some(v);
        });
        let mut waited = Duration::ZERO;
        while waited < budget {
            self.step(Duration::from_millis(25))?;
            waited += Duration::from_millis(25);
            if let Some(v) = slot.lock().unwrap().clone() {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    fn set_wedge(&self, nodes: &[usize], on: bool) {
        for &i in nodes {
            self.wedges[i].store(on, Ordering::Release);
        }
    }
}

#[derive(Default)]
struct Tally {
    accepted: Vec<Vec<u8>>,
    refused: Vec<Vec<u8>>,
    max_backlog: u64,
    next: u64,
}

fn key_of(seed: u64, n: u64) -> Vec<u8> {
    format!("st{seed:x}-{n:08}").into_bytes()
}

fn flood(rig: &mut Rig, seed: u64, ticks: usize, t: &mut Tally) -> Result<(), String> {
    flood_at(rig, seed, ticks, PER_TICK, t)
}

fn flood_at(
    rig: &mut Rig,
    seed: u64,
    ticks: usize,
    per_tick: usize,
    t: &mut Tally,
) -> Result<(), String> {
    for _ in 0..ticks {
        if let Some(l) = rig.leader() {
            for _ in 0..per_tick {
                let key = key_of(seed, t.next);
                t.next += 1;
                match rig.nodes[l].put(key.clone(), vec![7u8; VALUE_BYTES]) {
                    ProposeResult::Accepted { .. } => t.accepted.push(key),
                    ProposeResult::Busy { .. } => t.refused.push(key),
                    ProposeResult::NotLeader { .. } => {}
                }
            }
            let (entries, _) = rig.nodes[l].propose_backlog();
            t.max_backlog = t.max_backlog.max(entries);
        }
        rig.step(TICK)?;
    }
    Ok(())
}

/// After the wedge lifts: drain, then check the shared oracles.
fn recover_and_check(rig: &mut Rig, seed: u64, t: &Tally) -> Vec<String> {
    let mut v = Vec::new();
    if t.max_backlog > BOUND_ENTRIES + SLACK {
        v.push(format!(
            "backlog: reached {} entries, bound {BOUND_ENTRIES} (+{SLACK})",
            t.max_backlog
        ));
    }
    rig.set_wedge(&[0, 1, 2], false);
    if let Err(e) = rig.step(Duration::from_secs(40)) {
        v.push(e);
        return v;
    }
    let Some(l) = rig.leader() else {
        v.push("liveness: no single leader after the stall cleared".into());
        return v;
    };
    let (entries, _) = rig.nodes[l].propose_backlog();
    if entries > 2 {
        v.push(format!("liveness: backlog {entries} did not drain"));
    }
    // Fresh write is admitted and applies.
    let fresh = key_of(seed, u64::MAX / 2);
    match rig.nodes[l].put(fresh.clone(), b"fresh".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => v.push(format!("liveness: fresh write not accepted: {other:?}")),
    }
    if let Err(e) = rig.step(Duration::from_secs(5)) {
        v.push(e);
        return v;
    }
    for (i, n) in rig.nodes.iter().enumerate() {
        for k in &t.refused {
            let node = n.clone();
            let key = k.clone();
            if block_on(async move { node.local_get(&key).await }).is_some() {
                v.push(format!(
                    "refused-is-absent: refused key present on node {i}"
                ));
                break;
            }
        }
    }
    let node = rig.nodes[l].clone();
    let missing: Vec<Vec<u8>> = t
        .accepted
        .iter()
        .filter(|k| {
            let node = node.clone();
            let key = (*k).clone();
            block_on(async move { node.local_get(&key).await }).is_none()
        })
        .cloned()
        .collect();
    if !missing.is_empty() {
        v.push(format!(
            "acked-survives: {} accepted keys missing on the leader",
            missing.len()
        ));
    }
    v
}

fn assert_clean(name: &str, seed: u64, v: &[String]) {
    assert!(v.is_empty(), "{name} seed {seed}: {v:#?}");
}

/// Cell A: every node wedged. The flood is refused at the bound while the
/// stall holds the apply tasks, nothing panics, and the group recovers.
fn all_wedged(seed: u64, fault: StallFault) -> Vec<String> {
    let mut rig = Rig::new(seed, fault);
    let mut v = Vec::new();
    let l = match rig.wait_leader() {
        Ok(l) => l,
        Err(e) => return vec![e],
    };
    let mut t = Tally::default();
    rig.set_wedge(&[0, 1, 2], true);
    // Long enough for an `OldGate` engine to run out its 10 s of polls.
    if let Err(e) = flood(&mut rig, seed, 1800, &mut t) {
        v.push(e);
        return v;
    }
    if t.refused.is_empty() {
        v.push(format!(
            "refusal: no Busy during the stall (leader {l}, accepted {})",
            t.accepted.len()
        ));
    }
    v.extend(recover_and_check(&mut rig, seed, &t));
    v
}

#[test]
fn composition_cell_a_all_wedged_surfaces_as_busy() {
    if !cell_enabled("cell_a") {
        return;
    }
    for seed in seeds() {
        assert_clean("cell A", seed, &all_wedged(seed, StallFault::None));
    }
}

/// Control (b): the pre-ADR gate ends in `Err` after 10 s and takes the apply
/// task down. The no-panic oracle must fail.
#[test]
fn composition_control_b_old_gate_takes_the_apply_task_down() {
    if !cell_enabled("control_b") {
        return;
    }
    for seed in seeds() {
        let v = all_wedged(seed, StallFault::OldGate);
        assert!(
            v.iter().any(|m| m.starts_with("no-panic")),
            "control (b) seed {seed}: the old gate must violate no-panic, got {v:?}"
        );
    }
}

/// Cell B: one follower wedged. The quorum is intact: no refusal.
#[test]
fn composition_cell_b_one_wedged_follower_causes_no_refusal() {
    if !cell_enabled("cell_b") {
        return;
    }
    for seed in seeds() {
        let mut rig = Rig::new(seed, StallFault::None);
        let l = rig.wait_leader().expect("leader");
        let f = (0..3).find(|&i| i != l).unwrap();
        let mut t = Tally::default();
        rig.set_wedge(&[f], true);
        flood_at(&mut rig, seed, 1000, 1, &mut t).expect("no panic");
        assert!(
            t.refused.is_empty(),
            "cell B seed {seed}: {} refusals with a healthy quorum",
            t.refused.len()
        );
        let v = recover_and_check(&mut rig, seed, &t);
        assert_clean("cell B", seed, &v);
    }
}

/// Cell C: both followers wedged. The quorum cannot apply: refusal (#1330).
#[test]
fn composition_cell_c_both_followers_wedged_refuses() {
    if !cell_enabled("cell_c") {
        return;
    }
    for seed in seeds() {
        let mut rig = Rig::new(seed, StallFault::None);
        let l = rig.wait_leader().expect("leader");
        let fs: Vec<usize> = (0..3).filter(|&i| i != l).collect();
        let mut t = Tally::default();
        rig.set_wedge(&fs, true);
        flood(&mut rig, seed, 1800, &mut t).expect("no panic");
        assert!(
            !t.refused.is_empty(),
            "cell C seed {seed}: no refusal with both followers stalled"
        );
        let v = recover_and_check(&mut rig, seed, &t);
        assert_clean("cell C", seed, &v);
    }
}

/// Cell D: a `Liveness`-class resolve is admitted during the stall (never
/// refused) and completes once it clears.
#[test]
fn composition_cell_d_resolve_admitted_during_stall() {
    if !cell_enabled("cell_d") {
        return;
    }
    for seed in seeds() {
        let mut rig = Rig::new(seed, StallFault::None);
        let l = rig.wait_leader().expect("leader");
        let token = animus_tablet::partition_token(b"txn-a");
        let mut k = token.to_vec();
        k.extend_from_slice(b"txn-a");
        let node = rig.nodes[l].clone();
        let key = k.clone();
        let staged = rig
            .run_async(l, Duration::from_secs(10), async move {
                node.txn_stage("t", vec![(key, Some(b"v".to_vec()))]).await
            })
            .expect("no panic");
        let Some(Some((txn_id, record_key, StageOutcome::Staged))) = staged else {
            panic!("cell D seed {seed}: stage did not land");
        };
        let node = rig.nodes[l].clone();
        let (rk, tid, min_ts) = (record_key.clone(), txn_id.clone(), txn_id.ts);
        let committed = rig
            .run_async(l, Duration::from_secs(10), async move {
                node.txn_commit_at_least(tid, rk, min_ts).await.is_some()
            })
            .expect("no panic");
        assert_eq!(committed, Some(true), "cell D seed {seed}: commit");
        let node = rig.nodes[l].clone();
        let rk = record_key.clone();
        let status = rig
            .run_async(l, Duration::from_secs(10), async move {
                node.txn_status_local(&rk).await
            })
            .expect("no panic");
        let Some(Some(TxnDecisionStatus::Committed { commit_ts })) = status else {
            panic!("cell D seed {seed}: not committed: {status:?}");
        };

        let mut t = Tally::default();
        rig.set_wedge(&[0, 1, 2], true);
        flood(&mut rig, seed, 1200, &mut t).expect("no panic");
        assert!(
            !t.refused.is_empty(),
            "cell D seed {seed}: setup not at the bound"
        );

        // The resolve is proposed during the stall.
        let done = Arc::new(Mutex::new(None::<bool>));
        let d = Arc::clone(&done);
        let node = rig.nodes[l].clone();
        let (rk, kk) = (record_key.clone(), k.clone());
        rig.nodes[l].env().clone().spawn_task(async move {
            let ok = node
                .txn_resolve(txn_id, rk, vec![kk], TxnOutcome::Committed { commit_ts })
                .await
                .is_some();
            *d.lock().unwrap() = Some(ok);
        });
        rig.step(Duration::from_secs(2)).expect("no panic");
        let v = recover_and_check(&mut rig, seed, &t);
        assert_clean("cell D", seed, &v);
        assert_eq!(
            *done.lock().unwrap(),
            Some(true),
            "cell D seed {seed}: the resolve was refused or never completed"
        );
        let _ = rig.engines.len();
    }
}
