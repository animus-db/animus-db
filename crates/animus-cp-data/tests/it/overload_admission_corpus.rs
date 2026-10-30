//! ADR 0080 Tier 1: the leader propose bound under overload (`SimEnv`).
//!
//! RF-3 groups on a disk whose `fsync` is slow, driven by an open-loop proposer
//! well above what the leader can commit. The propose bound
//! (`ProposeResult::Busy`, checked under the core lock in `gated_propose`) must
//! keep the apply backlog at the configured limit, refuse the surplus with a
//! *definite* answer (nothing appended), keep the group led with a bounded term,
//! and let the group drain and accept fresh writes once the load stops.
//!
//! Oracles (each cell returns a list of violations; a real cell asserts the
//! list is empty, a negative control asserts a named oracle FAILS):
//!
//! - **backlog**: the leader's `last_log_index - engine_applied` never exceeds
//!   the limit plus a small slack;
//! - **refused is absent**: a key whose `put` returned `Busy` is in no replica
//!   (a `Busy` is definite, never "maybe applied");
//! - **acked survives** (cells where a quorum survives untouched): every key a
//!   `put` accepted is applied on the leader after the drain;
//! - **liveness**: the group stays led, `max_term` is bounded, the backlog
//!   drains and a fresh write is admitted within a bound after the load stops;
//! - **goodput**: accepted writes stay at a floor derived from the bound (no
//!   collapse under refusal);
//! - **no terminal Busy**: an SDK-style retrying client never sees `Busy` as a
//!   terminal (500-class) error;
//! - **txn deadlock freedom** (cell 8): a stage is refused at the bound while
//!   the resolve of an earlier transaction is still admitted.
//!
//! Knobs: `ANIMUS_OVERLOAD_SEEDS=K` (default 1), `ANIMUS_OVERLOAD_CELL=<substr>`
//! narrows, `ANIMUS_SEED=<seed>` replays one seed.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::admission::{AdmissionClass, BacklogLimits, NodeAdmission};
use animus_cp_data::campaign_governor::ElectionBackpressure;
use animus_cp_data::{HostedOptions, RaftKvNode, StageOutcome, StorageScope};
use animus_cp_data::{TxnDecisionStatus, TxnOutcome, TxnWrite};
use animus_env::{Clock, EnvExt, MetricsHandle, PRIMARY_STREAM, QueuedBytes, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{
    Key, MemoryEngine, MergeOp, Result as StorageResult, StorageEngine, Value, Version,
    VersionedValue, WriteBatch,
};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, ApplyEngine>;

/// The modelled apply capacity: a [`MemoryEngine`] whose merge path costs
/// `per_op` of virtual time per entry applied (the `Env` seam's `sleep`), so a
/// proposer above `1 / per_op` entries per second builds a real apply backlog.
#[derive(Clone)]
struct ApplyEngine {
    inner: MemoryEngine,
    env: SimEnv,
    per_op: Duration,
}

#[async_trait::async_trait]
impl StorageEngine for ApplyEngine {
    type Snapshot = <MemoryEngine as StorageEngine>::Snapshot;

    async fn put(&self, key: &[u8], value: &[u8], version: Version) -> StorageResult<()> {
        self.inner.put(key, value, version).await
    }

    async fn merge(&self, key: &[u8], value: &[u8], version: Version) -> StorageResult<bool> {
        self.env.sleep(self.per_op).await;
        self.inner.merge(key, value, version).await
    }

    async fn merge_tombstone(&self, key: &[u8], version: Version) -> StorageResult<bool> {
        self.env.sleep(self.per_op).await;
        self.inner.merge_tombstone(key, version).await
    }

    async fn merge_batch(&self, ops: Vec<MergeOp>) -> StorageResult<()> {
        self.env.sleep(self.per_op * ops.len().max(1) as u32).await;
        self.inner.merge_batch(ops).await
    }

    async fn delete(&self, key: &[u8], version: Version) -> StorageResult<()> {
        self.inner.delete(key, version).await
    }

    async fn delete_range(&self, start: &[u8], end: &[u8], version: Version) -> StorageResult<()> {
        self.inner.delete_range(start, end, version).await
    }

    async fn write_batch(&self, batch: WriteBatch) -> StorageResult<()> {
        self.inner.write_batch(batch).await
    }

    async fn get(&self, key: &[u8]) -> StorageResult<Option<VersionedValue>> {
        self.inner.get(key).await
    }

    async fn get_at(&self, key: &[u8], version: Version) -> StorageResult<Option<VersionedValue>> {
        self.inner.get_at(key, version).await
    }

    async fn scan(&self, start: &[u8], end: &[u8]) -> StorageResult<Vec<(Key, VersionedValue)>> {
        self.inner.scan(start, end).await
    }

    async fn scan_at(
        &self,
        start: &[u8],
        end: &[u8],
        version: Version,
    ) -> StorageResult<Vec<(Key, VersionedValue)>> {
        self.inner.scan_at(start, end, version).await
    }

    async fn entries(&self) -> StorageResult<Vec<(Key, VersionedValue)>> {
        self.inner.entries().await
    }

    async fn entries_at(&self, version: Version) -> StorageResult<Vec<(Key, VersionedValue)>> {
        self.inner.entries_at(version).await
    }

    async fn entries_with_tombstones(&self) -> StorageResult<Vec<(Key, Option<Value>, Version)>> {
        self.inner.entries_with_tombstones().await
    }

    fn snapshot(&self) -> Self::Snapshot {
        self.inner.snapshot()
    }

    fn latest_version(&self) -> Version {
        self.inner.latest_version()
    }
}

const NODES: [u64; 3] = [0, 1, 2];
/// Apply cost per entry: capacity is 1 / APPLY_PER_OP = 1000 entries/s.
const APPLY_PER_OP: Duration = Duration::from_millis(1);
/// Offered load: `PER_TICK` puts every `TICK`, i.e. 2000 puts/s.
const TICK: Duration = Duration::from_millis(10);
const PER_TICK: usize = 20;
const LOAD_TICKS: usize = 600; // 6 s of virtual time
const VALUE_BYTES: usize = 128;
/// The test bound (the production default is 512 entries / 4 MiB).
const BOUND_ENTRIES: u64 = 64;
const BOUND_BYTES: u64 = 64 * 1024;
/// Backlog slack above the bound: the bound is checked before an append, and a
/// `Client` write is one entry.
const SLACK: u64 = 8;
const MAX_TERM_QUIET: u64 = 6;
const MAX_TERM_FAULT: u64 = 14;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Cell {
    SteadyOverload,
    LeaderKill,
    StalledFollower,
    PartitionHeal,
    TxnStageRefusedResolveAdmitted,
    QuiescedHotWake,
}

impl Cell {
    const ALL: [Cell; 6] = [
        Cell::SteadyOverload,
        Cell::LeaderKill,
        Cell::StalledFollower,
        Cell::PartitionHeal,
        Cell::TxnStageRefusedResolveAdmitted,
        Cell::QuiescedHotWake,
    ];

    fn name(self) -> &'static str {
        match self {
            Cell::SteadyOverload => "c1_steady_overload",
            Cell::LeaderKill => "c2_overload_leader_kill",
            Cell::StalledFollower => "c3_overload_stalled_follower",
            Cell::PartitionHeal => "c4_overload_partition_heal",
            Cell::TxnStageRefusedResolveAdmitted => "c8_txn_stage_refused_resolve_admitted",
            Cell::QuiescedHotWake => "c7_quiesced_hot_wake_under_lag",
        }
    }

    /// Whether a quorum survives the cell's fault untouched, so every accepted
    /// key must still be there at the end.
    fn acked_must_survive(self) -> bool {
        matches!(self, Cell::SteadyOverload | Cell::StalledFollower)
    }

    fn max_term(self) -> u64 {
        match self {
            Cell::SteadyOverload | Cell::StalledFollower => MAX_TERM_QUIET,
            _ => MAX_TERM_FAULT,
        }
    }
}

/// A deliberate defect a negative control installs; `None` is the real system.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Control {
    None,
    /// (a) the propose bound is off.
    BoundsOff,
    /// (b) `Busy` would apply to `Liveness` commands (the class is forced to
    /// `Client` when the harness asks whether a resolve is admitted).
    BusyOnLiveness,
    /// (c) a `Busy` is recorded as `Accepted` by the client.
    BusyAsAccepted,
    /// (h) the cold-wake refusal is off: a write to a quiesced group in the
    /// severe-lag regime wakes it (the old soft cap).
    NoColdWakeRefusal,
    /// (g) a `Busy` surfaces as a terminal (500-class) error: the old `other =>`
    /// arm of the write loops.
    BusyTerminal,
}

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return vec![s];
    }
    let k: u64 = std::env::var("ANIMUS_OVERLOAD_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    animus_test::corpus::shard_retain((0..k).map(|i| 0x0ad0_0000 + i).collect())
}

fn cell_selected(cell: Cell) -> bool {
    std::env::var("ANIMUS_OVERLOAD_CELL")
        .ok()
        .is_none_or(|f| cell.name().contains(&f))
}

struct Rig {
    sim: Simulator,
    nodes: Vec<KvNode>,
    adm: Vec<NodeAdmission>,
    alive: BTreeSet<usize>,
}

impl Rig {
    fn new(seed: u64, limits: BacklogLimits) -> Self {
        let sim = Simulator::new(seed);
        let voters: Vec<_> = NODES.iter().copied().map(nid).collect();
        let mut nodes = Vec::new();
        let mut adm = Vec::new();
        for &id in &NODES {
            let a = NodeAdmission::new(limits, QueuedBytes::new(0), MetricsHandle::recording());
            let n = RaftKvNode::start_hosted_with_options(
                sim.env(nid(id)),
                voters.clone(),
                ApplyEngine {
                    inner: MemoryEngine::new(),
                    env: sim.env(nid(id)),
                    per_op: APPLY_PER_OP,
                },
                StorageScope::whole(),
                PRIMARY_STREAM,
                HostedOptions {
                    backpressure: ElectionBackpressure {
                        admission: Some(a.clone()),
                        ..ElectionBackpressure::default()
                    },
                    ..HostedOptions::default()
                },
            );
            nodes.push(n);
            adm.push(a);
        }
        Self {
            sim,
            nodes,
            adm,
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

    fn max_term(&self) -> u64 {
        self.nodes.iter().map(KvNode::term).max().unwrap_or(0)
    }

    /// Run `fut` as a sim task on node `i` and step the sim until it finishes.
    fn run_async<T: Clone + Send + 'static>(
        &mut self,
        i: usize,
        fut: impl std::future::Future<Output = T> + Send + 'static,
    ) -> Option<T> {
        let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
        let s = Arc::clone(&slot);
        self.nodes[i].env().clone().spawn_task(async move {
            let v = fut.await;
            *s.lock().unwrap() = Some(v);
        });
        for _ in 0..400 {
            self.sim.run_for(Duration::from_millis(25));
            if let Some(v) = slot.lock().unwrap().clone() {
                return Some(v);
            }
        }
        None
    }
}

#[derive(Default)]
struct Tally {
    accepted: Vec<Vec<u8>>,
    refused: Vec<Vec<u8>>,
    /// Ticks with no single leader (no offer made).
    leaderless_ticks: usize,
    max_backlog: u64,
    terminal_errors: usize,
    next_key: u64,
    /// Accepted keys already applied on the leader when the load stopped.
    applied_at_end: usize,
}

fn key_of(seed: u64, n: u64) -> Vec<u8> {
    format!("ov{seed:x}-{n:08}").into_bytes()
}

/// One open-loop tick: offer `PER_TICK` puts to the current leader and
/// classify each answer the way the wire edge does.
fn offer(rig: &Rig, seed: u64, ctl: Control, t: &mut Tally) {
    let Some(l) = rig.leader() else {
        t.leaderless_ticks += 1;
        return;
    };
    for _ in 0..PER_TICK {
        let key = key_of(seed, t.next_key);
        t.next_key += 1;
        match rig.nodes[l].put(key.clone(), vec![7u8; VALUE_BYTES]) {
            ProposeResult::Accepted { .. } => t.accepted.push(key),
            ProposeResult::Busy { .. } => match ctl {
                // (c) the defect: a refusal is booked as an ack.
                Control::BusyAsAccepted => t.accepted.push(key),
                // (g) the defect: a refusal is a terminal 500-class error.
                Control::BusyTerminal => {
                    t.terminal_errors += 1;
                    t.refused.push(key);
                }
                _ => t.refused.push(key),
            },
            ProposeResult::NotLeader { .. } => {}
        }
    }
    let (entries, _) = rig.nodes[l].propose_backlog();
    t.max_backlog = t.max_backlog.max(entries);
}

fn run_load(
    rig: &mut Rig,
    seed: u64,
    ctl: Control,
    t: &mut Tally,
    mut fault: impl FnMut(&mut Rig, usize),
) {
    for tick in 0..LOAD_TICKS {
        fault(rig, tick);
        offer(rig, seed, ctl, t);
        rig.sim.run_for(TICK);
    }
}

/// The oracles shared by the load cells. Returns the violations found.
fn check(cell: Cell, rig: &mut Rig, seed: u64, t: &Tally, capacity: usize) -> Vec<String> {
    let mut v = Vec::new();
    let limit = BOUND_ENTRIES + SLACK;
    if t.max_backlog > limit {
        v.push(format!(
            "backlog: leader backlog reached {} entries, bound is {BOUND_ENTRIES} (+{SLACK} slack)",
            t.max_backlog
        ));
    }
    if t.terminal_errors > 0 {
        v.push(format!(
            "no-terminal-busy: {} Busy answers surfaced as a terminal error",
            t.terminal_errors
        ));
    }
    // Heal everything and let the group drain.
    for a in &NODES {
        for b in &NODES {
            if a != b {
                rig.sim.heal(nid(*a), nid(*b));
            }
        }
    }
    rig.sim.run_for(Duration::from_secs(15));
    let Some(l) = rig.leader() else {
        v.push(format!(
            "liveness: no single leader after the load stopped (alive={:?} leaders={:?} terms={:?})",
            rig.alive,
            rig.nodes.iter().map(KvNode::is_leader).collect::<Vec<_>>(),
            rig.nodes.iter().map(KvNode::term).collect::<Vec<_>>()
        ));
        return v;
    };
    if rig.max_term() > cell.max_term() {
        v.push(format!(
            "liveness: max_term {} exceeds {} (election churn)",
            rig.max_term(),
            cell.max_term()
        ));
    }
    let (entries, _) = rig.nodes[l].propose_backlog();
    if entries > 2 {
        v.push(format!(
            "drain: backlog still {entries} entries 15 s after the load"
        ));
    }
    // A fresh write is admitted and applied within a bound.
    let probe = format!("ov{seed:x}-probe").into_bytes();
    if !matches!(
        rig.nodes[l].put(probe.clone(), vec![1]),
        ProposeResult::Accepted { .. }
    ) {
        v.push("liveness: a fresh write was refused after the drain".into());
    } else {
        rig.sim.run_for(Duration::from_secs(5));
        if block_on(rig.nodes[l].local_get(&probe)).is_none() {
            v.push("liveness: the fresh write was not applied within 5 s".into());
        }
    }
    // Goodput: refusing the surplus must not collapse throughput. The reference
    // is what the same rig applies in the window with the bound off (its
    // modelled apply capacity); the bounded run must apply at least 80 % of it
    // (halved for the fault cells, which lose time to an election).
    let share = if cell.acked_must_survive() { 1.0 } else { 0.4 };
    let floor = (0.8 * share * capacity as f64) as usize;
    if t.applied_at_end < floor && t.leaderless_ticks < LOAD_TICKS / 2 {
        v.push(format!(
            "goodput: {} writes applied in the window, floor {floor} (80 % of capacity {capacity})",
            t.applied_at_end
        ));
    }
    // A refused key is in no replica.
    for k in &t.refused {
        if rig
            .alive
            .iter()
            .any(|&i| block_on(rig.nodes[i].local_get(k)).is_some())
        {
            v.push(format!(
                "refused-absent: a Busy-refused key {} is present",
                String::from_utf8_lossy(k)
            ));
            break;
        }
    }
    if cell.acked_must_survive()
        && let Some(missing) = t
            .accepted
            .iter()
            .find(|k| block_on(rig.nodes[l].local_get(k)).is_none())
    {
        v.push(format!(
            "acked-survives: accepted key {} is missing after the drain",
            String::from_utf8_lossy(missing)
        ));
    }
    v
}

/// The modelled apply capacity: keys the rig applies in the load window with
/// the bound off (a pure function of the seed).
fn capacity_of(seed: u64) -> usize {
    let mut rig = Rig::new(seed, BacklogLimits::off());
    rig.sim.run_for(Duration::from_secs(5));
    let mut t = Tally::default();
    run_load(&mut rig, seed, Control::None, &mut t, |_, _| {});
    applied_now(&rig, &t)
}

fn applied_now(rig: &Rig, t: &Tally) -> usize {
    rig.leader().map_or(0, |l| {
        t.accepted
            .iter()
            .filter(|k| block_on(rig.nodes[l].local_get(k)).is_some())
            .count()
    })
}

fn run_cell(cell: Cell, seed: u64, ctl: Control) -> Vec<String> {
    let limits = if ctl == Control::BoundsOff {
        BacklogLimits::off()
    } else {
        BacklogLimits {
            max_entries: BOUND_ENTRIES,
            max_bytes: BOUND_BYTES,
        }
    };
    let mut rig = Rig::new(seed, limits);
    rig.sim.run_for(Duration::from_secs(5));
    let Some(l0) = rig.leader() else {
        return vec!["setup: no leader elected on a fast disk".into()];
    };
    let mut t = Tally::default();
    match cell {
        Cell::SteadyOverload => run_load(&mut rig, seed, ctl, &mut t, |_, _| {}),
        Cell::LeaderKill => run_load(&mut rig, seed, ctl, &mut t, |r, tick| {
            if tick == LOAD_TICKS / 3
                && let Some(l) = r.leader()
            {
                r.sim.crash(nid(NODES[l]));
                r.alive.remove(&l);
            }
        }),
        Cell::StalledFollower => {
            let f = (l0 + 1) % NODES.len();
            run_load(&mut rig, seed, ctl, &mut t, |r, tick| {
                if tick == LOAD_TICKS / 4 {
                    for (o, &other) in NODES.iter().enumerate() {
                        if o != f {
                            r.sim.partition_pair(nid(NODES[f]), nid(other));
                        }
                    }
                }
            });
        }
        Cell::PartitionHeal => run_load(&mut rig, seed, ctl, &mut t, |r, tick| {
            if tick == LOAD_TICKS / 4
                && let Some(l) = r.leader()
            {
                for (o, &other) in NODES.iter().enumerate() {
                    if o != l {
                        r.sim.partition_pair(nid(NODES[l]), nid(other));
                    }
                }
            }
            if tick == LOAD_TICKS / 2 {
                for a in NODES {
                    for b in NODES {
                        if a != b {
                            r.sim.heal(nid(a), nid(b));
                        }
                    }
                }
            }
        }),
        Cell::TxnStageRefusedResolveAdmitted => return run_txn_cell(&mut rig, seed, ctl, l0),
        Cell::QuiescedHotWake => return run_cold_wake_cell(seed, ctl),
    }
    t.applied_at_end = applied_now(&rig, &t);
    check(cell, &mut rig, seed, &t, capacity_of(seed))
}

/// Cell 8: a transaction is staged and committed, then the leader is flooded to
/// its bound. A *new* stage is refused (`Err(ProposeBusy)`, nothing appended) while
/// the resolve of the earlier transaction is still admitted and applies, so the
/// intents do not sit behind the flood (no stage/resolve deadlock).
fn run_txn_cell(rig: &mut Rig, seed: u64, ctl: Control, l0: usize) -> Vec<String> {
    let mut v = Vec::new();
    let token = animus_tablet::partition_token(b"txn-a");
    let mut k = token.to_vec();
    k.extend_from_slice(b"txn-a");
    let node = rig.nodes[l0].clone();
    let key = k.clone();
    let staged = rig.run_async(l0, async move {
        node.txn_stage("t", vec![(key, Some(b"v".to_vec()))]).await
    });
    let Some(Some((txn_id, record_key, StageOutcome::Staged))) = staged else {
        return vec!["setup: the first stage did not land".into()];
    };
    let node = rig.nodes[l0].clone();
    let rk = record_key.clone();
    let tid = txn_id.clone();
    let min_ts = txn_id.ts;
    let committed = rig.run_async(l0, async move {
        node.txn_commit_at_least(tid, rk, min_ts).await.is_some()
    });
    if committed != Some(true) {
        return vec!["setup: the commit did not apply".into()];
    }

    let mut t = Tally::default();
    // Flood to the bound, sampling until the leader is saturated.
    for _ in 0..40 {
        offer(rig, seed, ctl, &mut t);
        rig.sim.run_for(Duration::from_millis(1));
    }
    if t.refused.is_empty() {
        v.push("setup: the flood never reached the bound (no Busy)".into());
    }
    // A new stage is refused at the bound.
    let node = rig.nodes[l0].clone();
    let token2 = animus_tablet::partition_token(b"txn-b");
    let mut k2 = token2.to_vec();
    k2.extend_from_slice(b"txn-b");
    let (entries, bytes) = rig.nodes[l0].propose_backlog();
    let stage_refused = rig
        .run_async(l0, async move {
            node.txn_stage_anchor_checked(
                "t",
                vec![TxnWrite::plain(k2, Some(vec![1]))],
                Vec::new(),
                Vec::new(),
            )
            .await
            .is_err()
        })
        .unwrap_or(false);
    if !stage_refused {
        v.push(format!(
            "txn: a new stage was admitted at the bound (backlog {entries} entries / {bytes} bytes)"
        ));
    }
    // The resolve of the earlier transaction is admitted. Under control (b) the
    // harness asks the admission layer with the Client class instead, the way a
    // `Busy`-on-Liveness defect would.
    let class = if ctl == Control::BusyOnLiveness {
        AdmissionClass::Client
    } else {
        AdmissionClass::Liveness
    };
    if rig.adm[l0].verdict(class, entries, bytes, 64).is_err() {
        v.push("txn: a resolve-class command was refused at the bound (deadlock)".into());
    }
    let node = rig.nodes[l0].clone();
    let rk = record_key.clone();
    let status = rig.run_async(l0, async move { node.txn_status_local(&rk).await });
    let commit_ts = match status {
        Some(Some(TxnDecisionStatus::Committed { commit_ts })) => commit_ts,
        other => {
            v.push(format!("txn: the record is not committed: {other:?}"));
            return v;
        }
    };
    let node = rig.nodes[l0].clone();
    let rk = record_key.clone();
    let key = k.clone();
    let resolved = rig.run_async(l0, async move {
        node.txn_resolve(txn_id, rk, vec![key], TxnOutcome::Committed { commit_ts })
            .await
            .is_some()
    });
    if resolved != Some(true) {
        v.push("txn: the resolve was not admitted/applied while the flood was at the bound".into());
    }
    v
}

/// Cell 7: three groups per node share one `AwakeAdmission` (`A` = 2) and one
/// `NodeAdmission`; two are written to (awake), one idles into quiescence. In
/// the severe-lag regime (`lag >= 100 ms`) with the awake set at `A_eff`, a
/// write that would wake the cold group is refused with `Busy` while the hot
/// groups are still served; when the lag recovers the cold group wakes and
/// accepts the write. Below the regime nothing is refused (the cap stays soft).
fn run_cold_wake_cell(seed: u64, ctl: Control) -> Vec<String> {
    use animus_cp_data::awake_admission::AwakeAdmission;
    let mut v = Vec::new();
    let sim = Simulator::new(seed);
    let voters: Vec<_> = NODES.iter().copied().map(nid).collect();
    let awake: Vec<AwakeAdmission> = NODES
        .iter()
        .map(|_| AwakeAdmission::new(2, MetricsHandle::recording()))
        .collect();
    let adms: Vec<NodeAdmission> = NODES
        .iter()
        .map(|_| {
            NodeAdmission::new(
                BacklogLimits {
                    max_entries: BOUND_ENTRIES,
                    max_bytes: BOUND_BYTES,
                },
                QueuedBytes::new(0),
                MetricsHandle::recording(),
            )
        })
        .collect();
    // groups[g][n]: group g (stream 1..=3) on node n.
    let groups: Vec<Vec<KvNode>> = (1..=3u64)
        .map(|stream| {
            NODES
                .iter()
                .enumerate()
                .map(|(n, &id)| {
                    RaftKvNode::start_hosted_with_options(
                        sim.env(nid(id)),
                        voters.clone(),
                        ApplyEngine {
                            inner: MemoryEngine::new(),
                            env: sim.env(nid(id)),
                            per_op: Duration::ZERO,
                        },
                        StorageScope::whole(),
                        stream,
                        HostedOptions {
                            backpressure: ElectionBackpressure {
                                admission: Some(adms[n].clone()),
                                awake: (ctl != Control::NoColdWakeRefusal)
                                    .then(|| awake[n].clone()),
                                ..ElectionBackpressure::default()
                            },
                            ..HostedOptions::default()
                        },
                    )
                })
                .collect()
        })
        .collect();
    let mut sim = sim;
    sim.run_for(Duration::from_secs(5));
    let leader_of = |g: &[KvNode]| g.iter().position(KvNode::is_leader);
    let (Some(l0), Some(l1), Some(l2)) = (
        leader_of(&groups[0]),
        leader_of(&groups[1]),
        leader_of(&groups[2]),
    ) else {
        return vec!["setup: a group has no leader".into()];
    };
    // Group 3 idles into quiescence; groups 1 and 2 stay warm.
    for g in &groups {
        for n in g {
            n.enable_quiescence(Duration::from_secs(2));
        }
    }
    for i in 0..60 {
        for (gi, l) in [(0, l0), (1, l1)] {
            let _ = groups[gi][l].put(format!("warm-{i}").into_bytes(), vec![1]);
        }
        sim.run_for(Duration::from_millis(100));
    }
    if !groups[2][l2].is_quiesced() {
        return vec!["setup: the cold group did not quiesce".into()];
    }
    let set_lag = |ms: u64| {
        for n in 0..NODES.len() {
            awake[n].set_lag(ms * 1_000_000);
            adms[n].set_lag(ms * 1_000_000);
        }
    };
    // Soft below the regime: a 30 ms lag never refuses a cold write.
    set_lag(30);
    let soft = groups[2][l2].put(b"cold-soft".to_vec(), vec![1]);
    if !matches!(soft, ProposeResult::Accepted { .. }) {
        v.push(format!(
            "soft: a cold write was refused at 30 ms lag: {soft:?}"
        ));
    }
    // Re-quiesce (the warm groups keep taking writes), then enter the severe
    // regime.
    for i in 0..60 {
        for (gi, l) in [(0, l0), (1, l1)] {
            let _ = groups[gi][l].put(format!("warm2-{i}").into_bytes(), vec![1]);
        }
        sim.run_for(Duration::from_millis(100));
    }
    if !groups[2][l2].is_quiesced() {
        return vec!["setup: the cold group did not re-quiesce".into()];
    }
    set_lag(100);
    let cold = groups[2][l2].put(b"cold-1".to_vec(), vec![1]);
    if !matches!(cold, ProposeResult::Busy { .. }) {
        v.push(format!(
            "cold-wake: a cold write was admitted at 100 ms lag: {cold:?}"
        ));
    }
    let hot = groups[0][l0].put(b"hot-1".to_vec(), vec![1]);
    if !matches!(hot, ProposeResult::Accepted { .. }) {
        v.push(format!(
            "cold-wake: a write to an awake group was refused: {hot:?}"
        ));
    }
    if matches!(cold, ProposeResult::Busy { .. })
        && block_on(groups[2][l2].local_get(b"cold-1")).is_some()
    {
        v.push("refused-absent: the refused cold write is present".into());
    }
    // Recovery.
    set_lag(0);
    sim.run_for(Duration::from_secs(1));
    let after = groups[2][l2].put(b"cold-2".to_vec(), vec![1]);
    if !matches!(after, ProposeResult::Accepted { .. }) {
        v.push(format!(
            "recovery: the cold write is still refused after the lag cleared: {after:?}"
        ));
    }
    v
}

fn real(cell: Cell) {
    if !cell_selected(cell) {
        return;
    }
    for seed in seeds() {
        let v = run_cell(cell, seed, Control::None);
        assert!(
            v.is_empty(),
            "cell {} seed={seed:#x} (replay: ANIMUS_SEED={seed}):\n  {}",
            cell.name(),
            v.join("\n  ")
        );
    }
}

fn control(cell: Cell, ctl: Control, needle: &str) {
    for seed in seeds() {
        let v = run_cell(cell, seed, ctl);
        assert!(
            v.iter().any(|m| m.contains(needle)),
            "negative control {ctl:?} on {} seed={seed:#x}: the oracle did not fail ({needle:?}); got {v:?}",
            cell.name()
        );
    }
}

#[test]
fn overload_c1_steady_overload() {
    real(Cell::SteadyOverload);
}

#[test]
fn overload_c2_leader_kill() {
    real(Cell::LeaderKill);
}

#[test]
fn overload_c3_stalled_follower() {
    real(Cell::StalledFollower);
}

#[test]
fn overload_c4_partition_heal() {
    real(Cell::PartitionHeal);
}

#[test]
fn overload_c8_txn_stage_refused_resolve_admitted() {
    real(Cell::TxnStageRefusedResolveAdmitted);
}

#[test]
fn overload_c7_quiesced_hot_wake_under_lag() {
    real(Cell::QuiescedHotWake);
}

#[test]
fn overload_cells_are_all_listed() {
    // A new cell must be added to ALL and to a #[test] above.
    assert_eq!(Cell::ALL.len(), 6);
}

#[test]
fn overload_control_a_bounds_off_lets_the_backlog_grow() {
    control(Cell::SteadyOverload, Control::BoundsOff, "backlog:");
}

#[test]
fn overload_control_b_busy_on_liveness_deadlocks_the_resolve() {
    control(
        Cell::TxnStageRefusedResolveAdmitted,
        Control::BusyOnLiveness,
        "deadlock",
    );
}

#[test]
fn overload_control_c_busy_as_accepted_loses_acked_writes() {
    control(
        Cell::SteadyOverload,
        Control::BusyAsAccepted,
        "acked-survives:",
    );
}

#[test]
fn overload_control_g_busy_as_terminal_error_is_caught() {
    control(
        Cell::SteadyOverload,
        Control::BusyTerminal,
        "no-terminal-busy:",
    );
}

#[test]
fn overload_control_h_without_the_cold_wake_refusal_a_cold_group_wakes() {
    control(
        Cell::QuiescedHotWake,
        Control::NoColdWakeRefusal,
        "cold-wake:",
    );
}

/// Issue #1331: a `StorageFull` leader must refuse client writes instead of
/// accepting them into memory. With the propose bound itself off (so the
/// apply-backlog bound cannot be what stops the growth), every disk goes full:
/// once the leader's WAL is suspect it can persist and apply nothing, so each
/// further accepted write only grows its in-memory log and everything derived
/// from it (the recovery image, the replication frames). The log must stop
/// growing within a few ticks of the leader noticing, however long the disk
/// stays full, and writes must be admitted again once space returns.
#[test]
fn overload_c9_storage_full_leader_refuses_client_writes() {
    for seed in seeds() {
        let mut rig = Rig::new(seed, BacklogLimits::off());
        rig.sim.run_for(Duration::from_secs(5));
        let l = rig.leader().expect("a leader on a healthy disk");
        let mut t = Tally::default();
        let mut cfg = DiskConfig::default();
        cfg.set_enospc_prob(1.0);
        rig.sim.set_disk_config(cfg);
        const BIG: usize = 4096;
        let mut accepted_while_full = 0usize;
        let mut refused_while_full = 0usize;
        let mut full_seen_at = None;
        for tick in 0..400usize {
            for _ in 0..PER_TICK {
                let key = key_of(seed, t.next_key);
                t.next_key += 1;
                let full = rig.nodes[l].is_storage_full();
                if full && full_seen_at.is_none() {
                    full_seen_at = Some(tick);
                }
                match rig.nodes[l].put(key, vec![9u8; BIG]) {
                    ProposeResult::Accepted { .. } if full => accepted_while_full += 1,
                    ProposeResult::Busy { .. } if full => refused_while_full += 1,
                    _ => {}
                }
            }
            rig.sim.run_for(TICK);
        }
        let full_at = full_seen_at.unwrap_or_else(|| {
            panic!("seed={seed:#x}: the leader never reported StorageFull on a full disk")
        });
        assert!(
            full_at < 50,
            "seed={seed:#x}: the leader took {full_at} ticks to notice a full disk"
        );
        assert_eq!(
            accepted_while_full, 0,
            "seed={seed:#x}: a StorageFull leader accepted {accepted_while_full} client writes \
             into memory (refused {refused_while_full})"
        );
        assert!(
            refused_while_full > 1000,
            "seed={seed:#x}: only {refused_while_full} refusals; the cell is vacuous"
        );
        // Space returns: the group recovers without a restart and admits writes.
        rig.sim.set_disk_config(DiskConfig::default());
        rig.sim.run_for(Duration::from_secs(10));
        assert!(
            !rig.nodes[l].is_storage_full(),
            "seed={seed:#x}: the leader stayed StorageFull after space returned"
        );
        let leader = rig.leader().expect("a leader after recovery");
        let key = key_of(seed, u64::MAX);
        assert!(
            matches!(
                rig.nodes[leader].put(key, vec![1u8; 16]),
                ProposeResult::Accepted { .. }
            ),
            "seed={seed:#x}: a fresh write was not admitted after recovery"
        );
    }
}
