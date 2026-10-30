//! Issue #1330 (ADR 0080 amendment, `Gate::FollowerAppliedIndex`): the per-group
//! propose bound also bounds the gap between the leader and the **quorum-th
//! highest follower-applied index**.
//!
//! Before this, `gated_propose` read only the leader's own `engine_applied`, so
//! a quorum of followers whose apply task lagged (slow engine, stalled merge)
//! was invisible to admission: the leader refused nothing while the followers'
//! unapplied backlog (log retention, replay time on restart, stale eventual
//! reads) grew without bound. The follower's applied index now rides its
//! `AppendEntriesResp` (`raftkv-wire` v2, once the gate is open) and the leader
//! measures its backlog from the quorum-th highest applied index.
//!
//! RF-3 groups, a leader whose apply is fast (so its own backlog is always
//! near zero: the *only* signal is the followers'), an open-loop client far
//! above what the slow followers can apply.
//!
//! Cells (each returns its violations; a real cell asserts none, a negative
//! control asserts a named oracle FAILS):
//!
//! - `f1_quorum_slow`: both followers apply at a fraction of the offered rate.
//!   The quorum-th applied lag stays within the bound (+ slack), the surplus is
//!   refused `Busy`, accepted writes survive on the leader;
//! - `f2_one_slow_outside_quorum`: one follower slow, the other fast. Writes
//!   are never refused (a single slow follower outside the quorum must not stall
//!   the group);
//! - `f3_one_stalled_outside_quorum`: one follower's apply fully stalled, the
//!   other fast: same, no refusal;
//! - `f4_both_stalled_then_resume`: both followers stall, then resume. Busy while
//!   they are stalled with the lag bounded, then the backlog drains and a fresh
//!   write is admitted and applied everywhere;
//! - `f5_leader_change_keeps_the_bound`: the quorum is slow and the (fast)
//!   leader is killed mid-run. The new leader starts with an empty report table
//!   ("unknown = as caught up"), is itself slow so its own backlog bound still
//!   refuses the surplus, and the group drains and serves a fresh write. (With
//!   one voter dead the silent voter reads as caught up, ADR 0080's #1330
//!   amendment, so this cell does not assert the follower lag.)
//!
//! Negative controls: `n1_gate_closed` (cluster version 5: the same load and
//! the same slow quorum, the follower lag oracle MUST fail and no `Busy`
//! appears: exactly the pre-fix behaviour) and `n2_one_slow_blocks` (a defect
//! installed in the harness: the bound is checked against the *slowest*
//! follower, so the one-slow-follower cell's no-refusal oracle MUST fail).
//!
//! Knobs: `ANIMUS_FOLLOWER_LAG_SEEDS=K` (default 2), `ANIMUS_FOLLOWER_LAG_CELL=
//! <substr>` narrows, `ANIMUS_SEED=<seed>` replays one seed.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_control::version::ClusterFeatures;
use animus_cp_data::admission::{BacklogLimits, NodeAdmission};
use animus_cp_data::campaign_governor::ElectionBackpressure;
use animus_cp_data::{HostedOptions, RaftKvNode, StorageScope};
use animus_env::{Clock, MetricsHandle, PRIMARY_STREAM, QueuedBytes, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{
    Key, MemoryEngine, MergeOp, Result as StorageResult, StorageEngine, Value, Version,
    VersionedValue, WriteBatch,
};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, PacedEngine>;

/// Per-node apply pacing, adjustable while the rig runs.
#[derive(Clone)]
struct Pace {
    /// Virtual nanoseconds each applied entry costs.
    per_op_nanos: Arc<AtomicU64>,
    /// While set, a merge parks (polling) instead of applying.
    stalled: Arc<AtomicBool>,
}

impl Pace {
    fn new(per_op: Duration) -> Self {
        Self {
            per_op_nanos: Arc::new(AtomicU64::new(per_op.as_nanos() as u64)),
            stalled: Arc::new(AtomicBool::new(false)),
        }
    }
    fn set(&self, per_op: Duration) {
        self.per_op_nanos
            .store(per_op.as_nanos() as u64, Ordering::SeqCst);
    }
}

/// A [`MemoryEngine`] whose merge path costs the node's current pace of virtual
/// time per entry (the `Env` seam's `sleep`), and parks while stalled.
#[derive(Clone)]
struct PacedEngine {
    inner: MemoryEngine,
    env: SimEnv,
    pace: Pace,
}

impl PacedEngine {
    async fn cost(&self, ops: usize) {
        while self.pace.stalled.load(Ordering::SeqCst) {
            self.env.sleep(Duration::from_millis(5)).await;
        }
        let per = self.pace.per_op_nanos.load(Ordering::SeqCst);
        if per > 0 {
            self.env
                .sleep(Duration::from_nanos(per * ops.max(1) as u64))
                .await;
        }
    }
}

#[async_trait::async_trait]
impl StorageEngine for PacedEngine {
    type Snapshot = <MemoryEngine as StorageEngine>::Snapshot;

    async fn put(&self, key: &[u8], value: &[u8], version: Version) -> StorageResult<()> {
        self.inner.put(key, value, version).await
    }
    async fn merge(&self, key: &[u8], value: &[u8], version: Version) -> StorageResult<bool> {
        self.cost(1).await;
        self.inner.merge(key, value, version).await
    }
    async fn merge_tombstone(&self, key: &[u8], version: Version) -> StorageResult<bool> {
        self.cost(1).await;
        self.inner.merge_tombstone(key, version).await
    }
    async fn merge_batch(&self, ops: Vec<MergeOp>) -> StorageResult<()> {
        self.cost(ops.len()).await;
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
/// A fast engine: the leader's own backlog never builds.
const FAST: Duration = Duration::from_micros(50);
/// A slow follower applies 1 / SLOW = 250 entries/s, far below the offer.
const SLOW: Duration = Duration::from_millis(4);
/// Offered load: `PER_TICK` puts every `TICK`, i.e. 1000 puts/s.
const TICK: Duration = Duration::from_millis(10);
const PER_TICK: usize = 10;
const LOAD_TICKS: usize = 500; // 5 s of virtual time
const VALUE_BYTES: usize = 64;
const BOUND_ENTRIES: u64 = 64;
const BOUND_BYTES: u64 = 1 << 20;
/// The bound is checked before an append and acks lag a tick: a few entries.
const SLACK: u64 = 24;
/// Cluster version that opens `Gate::FollowerAppliedIndex` / one below it.
const GATE_OPEN: u32 = 6;
const GATE_CLOSED: u32 = 5;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Cell {
    QuorumSlow,
    OneSlow,
    OneStalled,
    BothStalledThenResume,
    LeaderChange,
}

impl Cell {
    const ALL: [Cell; 5] = [
        Cell::QuorumSlow,
        Cell::OneSlow,
        Cell::OneStalled,
        Cell::BothStalledThenResume,
        Cell::LeaderChange,
    ];

    fn name(self) -> &'static str {
        match self {
            Cell::QuorumSlow => "f1_quorum_slow",
            Cell::OneSlow => "f2_one_slow_outside_quorum",
            Cell::OneStalled => "f3_one_stalled_outside_quorum",
            Cell::BothStalledThenResume => "f4_both_stalled_then_resume",
            Cell::LeaderChange => "f5_leader_change_keeps_the_bound",
        }
    }

    /// Whether the cell must refuse the surplus (the quorum is the slow side).
    fn expects_busy(self) -> bool {
        matches!(
            self,
            Cell::QuorumSlow | Cell::BothStalledThenResume | Cell::LeaderChange
        )
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Control {
    None,
    /// (n1) the gate is closed: the cluster is at version 5.
    GateClosed,
    /// (n2) the harness enforces the bound against the *slowest* follower
    /// instead of the quorum-th, the defect the quorum semantics exist to avoid.
    SlowestFollowerBound,
}

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return vec![s];
    }
    let k: u64 = std::env::var("ANIMUS_FOLLOWER_LAG_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    animus_test::corpus::shard_retain((0..k).map(|i| 0x1330_0000 + i).collect())
}

fn cell_selected(name: &str) -> bool {
    std::env::var("ANIMUS_FOLLOWER_LAG_CELL")
        .ok()
        .is_none_or(|f| name.contains(&f))
}

struct Rig {
    sim: Simulator,
    nodes: Vec<KvNode>,
    paces: Vec<Pace>,
    alive: BTreeSet<usize>,
}

impl Rig {
    fn new(seed: u64, cluster_version: u32) -> Self {
        let sim = Simulator::new(seed);
        let voters: Vec<_> = NODES.iter().copied().map(nid).collect();
        let mut nodes = Vec::new();
        let mut paces = Vec::new();
        for &id in &NODES {
            let pace = Pace::new(FAST);
            let features = ClusterFeatures::new();
            features.update(&animus_control::Metadata {
                cluster_version,
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
                PacedEngine {
                    inner: MemoryEngine::new(),
                    env: sim.env(nid(id)),
                    pace: pace.clone(),
                },
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
            paces.push(pace);
        }
        Self {
            sim,
            nodes,
            paces,
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

    /// The followers (alive nodes other than `l`).
    fn followers(&self, l: usize) -> Vec<usize> {
        self.alive.iter().copied().filter(|&i| i != l).collect()
    }
}

#[derive(Default)]
struct Tally {
    offered: usize,
    accepted: Vec<Vec<u8>>,
    refused: Vec<Vec<u8>>,
    /// Max over the run of `commit_index - quorum-th highest applied`.
    max_quorum_lag: u64,
    /// Max over the run of `commit_index - highest follower applied`.
    max_best_follower_lag: u64,
    /// Max over the run of `commit_index - lowest follower applied`.
    max_worst_follower_lag: u64,
    next_key: u64,
}

fn key_of(seed: u64, n: u64) -> Vec<u8> {
    format!("fl{seed:x}-{n:08}").into_bytes()
}

/// One open-loop tick: offer `PER_TICK` puts to the current leader, then sample
/// the followers' apply lag against the leader's commit index.
fn offer(rig: &Rig, seed: u64, ctl: Control, t: &mut Tally) {
    let Some(l) = rig.leader() else { return };
    let followers = rig.followers(l);
    for _ in 0..PER_TICK {
        // Defect (n2): the harness refuses once the slowest follower is past
        // the bound, whatever the quorum looks like.
        if ctl == Control::SlowestFollowerBound {
            let commit = rig.nodes[l].commit_index();
            let worst = followers
                .iter()
                .map(|&f| commit.saturating_sub(rig.nodes[f].engine_applied_index()))
                .max()
                .unwrap_or(0);
            if worst > BOUND_ENTRIES {
                t.offered += 1;
                t.refused.push(key_of(seed, t.next_key));
                t.next_key += 1;
                continue;
            }
        }
        let key = key_of(seed, t.next_key);
        t.next_key += 1;
        t.offered += 1;
        match rig.nodes[l].put(key.clone(), vec![7u8; VALUE_BYTES]) {
            ProposeResult::Accepted { .. } => t.accepted.push(key),
            ProposeResult::Busy { .. } => t.refused.push(key),
            ProposeResult::NotLeader { .. } => {}
        }
    }
    let commit = rig.nodes[l].commit_index();
    let mut applied: Vec<u64> = followers
        .iter()
        .map(|&f| rig.nodes[f].engine_applied_index())
        .collect();
    applied.sort_unstable_by(|a, b| b.cmp(a));
    if let (Some(best), Some(worst)) = (applied.first(), applied.last()) {
        t.max_best_follower_lag = t.max_best_follower_lag.max(commit.saturating_sub(*best));
        t.max_worst_follower_lag = t.max_worst_follower_lag.max(commit.saturating_sub(*worst));
        // The leader is fast, so the quorum-th highest of {leader, f1, f2} is
        // the better follower's index.
        t.max_quorum_lag = t.max_quorum_lag.max(commit.saturating_sub(*best));
    }
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

fn set_followers(rig: &Rig, l: usize, per_op: Duration) {
    for f in rig.followers(l) {
        rig.paces[f].set(per_op);
    }
}

fn run_cell(cell: Cell, seed: u64, ctl: Control) -> Vec<String> {
    let version = if ctl == Control::GateClosed {
        GATE_CLOSED
    } else {
        GATE_OPEN
    };
    let mut rig = Rig::new(seed, version);
    rig.sim.run_for(Duration::from_secs(5));
    let Some(l0) = rig.leader() else {
        return vec!["setup: no leader elected".into()];
    };
    let followers = rig.followers(l0);
    let mut t = Tally::default();
    match cell {
        Cell::QuorumSlow => {
            set_followers(&rig, l0, SLOW);
            run_load(&mut rig, seed, ctl, &mut t, |_, _| {});
        }
        Cell::OneSlow => {
            rig.paces[followers[0]].set(SLOW);
            run_load(&mut rig, seed, ctl, &mut t, |_, _| {});
        }
        Cell::OneStalled => {
            rig.paces[followers[0]]
                .stalled
                .store(true, Ordering::SeqCst);
            run_load(&mut rig, seed, ctl, &mut t, |_, _| {});
        }
        Cell::BothStalledThenResume => {
            run_load(&mut rig, seed, ctl, &mut t, |r, tick| {
                if tick == LOAD_TICKS / 5 {
                    for f in r.followers(l0) {
                        r.paces[f].stalled.store(true, Ordering::SeqCst);
                    }
                }
                if tick == LOAD_TICKS * 3 / 5 {
                    for f in r.followers(l0) {
                        r.paces[f].stalled.store(false, Ordering::SeqCst);
                    }
                }
            });
        }
        Cell::LeaderChange => {
            // The slow quorum is the two nodes that stay; the fast old leader
            // dies, so the new leader is itself slow and the bound must come
            // from its own applied index and the surviving follower's acks.
            set_followers(&rig, l0, SLOW);
            run_load(&mut rig, seed, ctl, &mut t, |r, tick| {
                if tick == LOAD_TICKS / 3
                    && let Some(l) = r.leader()
                {
                    r.sim.crash(nid(NODES[l]));
                    r.alive.remove(&l);
                }
            });
        }
    }
    check(cell, ctl, &mut rig, seed, &t, l0)
}

fn check(cell: Cell, ctl: Control, rig: &mut Rig, seed: u64, t: &Tally, l0: usize) -> Vec<String> {
    let mut v = Vec::new();
    let limit = BOUND_ENTRIES + SLACK;
    let leader_changes = cell == Cell::LeaderChange;
    // follower-lag: the quorum-th applied lag stays within the bound. (Not
    // checked for the leader-change cell, whose lag is dominated by the dead
    // leader's absence; its own oracle is the post-change bound below.)
    if !leader_changes
        && t.max_quorum_lag > limit
        && !matches!(cell, Cell::OneSlow | Cell::OneStalled)
    {
        v.push(format!(
            "follower-lag: the quorum-th follower-applied lag reached {} entries, bound is {BOUND_ENTRIES} (+{SLACK} slack)",
            t.max_quorum_lag
        ));
    }
    if cell.expects_busy() && t.refused.is_empty() {
        v.push("busy: the slow quorum never caused a refusal".into());
    }
    if !cell.expects_busy() && !t.refused.is_empty() {
        v.push(format!(
            "no-stall: {} of {} writes were refused although the quorum (leader + one fast follower) was healthy",
            t.refused.len(),
            t.offered
        ));
    }
    if t.accepted.is_empty() {
        v.push("goodput: no write was accepted".into());
    }
    // Heal, resume, drain.
    for f in 0..NODES.len() {
        rig.paces[f].stalled.store(false, Ordering::SeqCst);
        rig.paces[f].set(FAST);
    }
    rig.sim.run_for(Duration::from_secs(20));
    let Some(l) = rig.leader() else {
        v.push("liveness: no single leader after the load stopped".into());
        return v;
    };
    let commit = rig.nodes[l].commit_index();
    for &i in &rig.alive {
        let a = rig.nodes[i].engine_applied_index();
        if a < commit {
            v.push(format!(
                "drain: node {i} applied {a} < commit {commit} 20 s after the load"
            ));
        }
    }
    // A fresh write is admitted and applied on every live replica.
    let probe = format!("fl{seed:x}-probe").into_bytes();
    if !matches!(
        rig.nodes[l].put(probe.clone(), vec![1]),
        ProposeResult::Accepted { .. }
    ) {
        v.push("liveness: a fresh write was refused after the drain".into());
    } else {
        rig.sim.run_for(Duration::from_secs(5));
        for &i in &rig.alive {
            if block_on(rig.nodes[i].local_get(&probe)).is_none() {
                v.push(format!("liveness: the fresh write is missing on node {i}"));
            }
        }
    }
    // A refused key is on no live replica (Busy is definite).
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
    // Every accepted key survives (a quorum was never lost). Not for the
    // leader-change cell: the dead leader may have taken uncommitted tail.
    if !leader_changes
        && ctl != Control::SlowestFollowerBound
        && let Some(missing) = t
            .accepted
            .iter()
            .find(|k| block_on(rig.nodes[l].local_get(k)).is_none())
    {
        v.push(format!(
            "acked-survives: accepted key {} is missing after the drain (leader was node {l0})",
            String::from_utf8_lossy(missing)
        ));
    }
    v
}

fn run_seeds(cell: Cell, ctl: Control) -> Vec<(u64, Vec<String>)> {
    seeds()
        .into_iter()
        .map(|seed| (seed, run_cell(cell, seed, ctl)))
        .collect()
}

#[test]
fn follower_apply_lag_corpus() {
    let mut failures = Vec::new();
    for cell in Cell::ALL {
        if !cell_selected(cell.name()) {
            continue;
        }
        for (seed, v) in run_seeds(cell, Control::None) {
            if !v.is_empty() {
                failures.push(format!("{} seed={seed:#x}: {v:#?}", cell.name()));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// n1: with the gate closed (cluster version 5) the same slow quorum is
/// invisible to the leader: no `Busy`, and the follower lag oracle fires. This
/// is the pre-fix behaviour, and proves the new input is what bites.
#[test]
fn n1_gate_closed_leaves_the_follower_lag_unbounded() {
    if !cell_selected("n1_gate_closed") {
        return;
    }
    for (seed, v) in run_seeds(Cell::QuorumSlow, Control::GateClosed) {
        assert!(
            v.iter().any(|s| s.starts_with("follower-lag:")),
            "seed={seed:#x}: the oracle must fail with the gate closed, got {v:#?}"
        );
        assert!(
            v.iter().any(|s| s.starts_with("busy:")),
            "seed={seed:#x}: nothing may be refused with the gate closed, got {v:#?}"
        );
    }
}

/// n2: a bound against the slowest follower instead of the quorum-th stalls a
/// group whose quorum is healthy; the one-slow-follower cell's no-stall oracle
/// must catch it.
#[test]
fn n2_slowest_follower_bound_stalls_a_healthy_quorum() {
    if !cell_selected("n2_one_slow_blocks") {
        return;
    }
    for (seed, v) in run_seeds(Cell::OneSlow, Control::SlowestFollowerBound) {
        assert!(
            v.iter().any(|s| s.starts_with("no-stall:")),
            "seed={seed:#x}: the oracle must fail, got {v:#?}"
        );
    }
}
