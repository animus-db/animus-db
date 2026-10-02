//! Regression for the reconciler group-driver-stop-timing stall: the apply
//! task must observe `shutdown()` **between entries** of one
//! `apply_and_compact` pass, not only once at the top of `apply_loop`'s own
//! outer loop.
//!
//! Before the fix, a backlog of committed-not-yet-applied entries drained
//! into one pass (`RaftCore::drain_apply`) had to be applied in full —
//! real engine I/O per entry — before `RaftKvNode::is_stopped()` could ever
//! flip, no matter when `shutdown()` was called mid-pass. That is what let
//! one slow-to-apply tablet block `host::Reconciler::teardown`'s inline wait
//! for the full `RECLAIM_STOP_TIMEOUT` and, in turn, starve every other
//! tablet the same node hosts (see `host.rs`'s `teardown`/
//! `RECLAIM_STOP_TIMEOUT` docs).
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Clock, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{
    Key, MemoryEngine, MergeOp, Result as StorageResult, StorageEngine, Value, Version,
    VersionedValue, WriteBatch,
};

const NODES: [u64; 3] = [0, 1, 2];

/// A [`StorageEngine`] wrapper that adds a fixed, `Env`-seam delay to every
/// [`merge`](StorageEngine::merge)/[`merge_batch`](StorageEngine::merge_batch)
/// call and counts how many of each completed — everything else delegates to
/// the inner [`MemoryEngine`] untouched. Models a tablet whose engine's own
/// merge path is genuinely slow (a real disk under load), so a backlog of
/// committed entries takes real simulated time to actually apply — the
/// precondition the bug needs (see this module's own doc).
#[derive(Clone)]
struct SlowEngine {
    inner: MemoryEngine,
    env: SimEnv,
    delay: Duration,
    merges: Arc<AtomicUsize>,
}

impl SlowEngine {
    fn new(env: SimEnv, delay: Duration, merges: Arc<AtomicUsize>) -> Self {
        Self {
            inner: MemoryEngine::new(),
            env,
            delay,
            merges,
        }
    }
}

#[async_trait::async_trait]
impl StorageEngine for SlowEngine {
    type Snapshot = <MemoryEngine as StorageEngine>::Snapshot;

    async fn put(&self, key: &[u8], value: &[u8], version: Version) -> StorageResult<()> {
        self.inner.put(key, value, version).await
    }

    async fn merge(&self, key: &[u8], value: &[u8], version: Version) -> StorageResult<bool> {
        self.env.sleep(self.delay).await;
        let took_effect = self.inner.merge(key, value, version).await;
        self.merges.fetch_add(1, Ordering::SeqCst);
        took_effect
    }

    async fn merge_tombstone(&self, key: &[u8], version: Version) -> StorageResult<bool> {
        self.env.sleep(self.delay).await;
        let took_effect = self.inner.merge_tombstone(key, version).await;
        self.merges.fetch_add(1, Ordering::SeqCst);
        took_effect
    }

    async fn merge_batch(&self, ops: Vec<MergeOp>) -> StorageResult<()> {
        self.env.sleep(self.delay).await;
        let result = self.inner.merge_batch(ops).await;
        self.merges.fetch_add(1, Ordering::SeqCst);
        result
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

type SlowNode = RaftKvNode<SimEnv, SlowEngine>;

fn slow_group(seed: u64, delay: Duration, merges: Arc<AtomicUsize>) -> (Simulator, Vec<SlowNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            let env = sim.env(nid(id));
            let engine = SlowEngine::new(env.clone(), delay, Arc::clone(&merges));
            RaftKvNode::start(env, NODES.iter().copied().map(nid).collect(), engine)
        })
        .collect();
    (sim, nodes)
}

fn leader(nodes: &[SlowNode], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

/// The regression: `shutdown()` called while the leader's apply task is
/// mid-way through a large backlog must let `is_stopped()` flip within a few
/// entries' worth of the injected per-entry delay — never by draining the
/// whole remaining backlog first.
///
/// Every proposal here is a `Cas` (never a plain `Put`/`Delete`): unlike
/// those, which this crate's apply path coalesces into one batched
/// `merge_batch` per pass (see `apply_and_compact`'s own "Coalesce the WAL
/// fsync" comment), a `Cas` calls `StorageEngine::merge` directly, once per
/// entry, so the injected per-call delay bites once per entry — exactly the
/// shape the maintainer's report describes (a backlog of committed entries
/// each needing its own real engine I/O before the next can even be
/// checked for `halted`).
#[test]
fn apply_task_stops_promptly_on_shutdown_mid_backlog() {
    let seed = 0x5106_5701;
    let delay = Duration::from_millis(100);
    const N: usize = 200;
    let merges = Arc::new(AtomicUsize::new(0));

    let (mut sim, nodes) = slow_group(seed, delay, Arc::clone(&merges));
    sim.run_for(Duration::from_secs(2)); // elect

    let l = leader(&nodes, seed);

    // Queue the whole backlog synchronously, no `.await`/`run_for` in
    // between (same "both synchronous, no `.await` between them" pattern
    // `tests/shutdown.rs` uses) — every entry lands in the leader's own
    // Raft log before the sim is ever allowed to run a persist/apply pass.
    for i in 0..N {
        let key = format!("k{i}").into_bytes();
        match nodes[l].cas(key, None, b"v".to_vec()) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected cas #{i}: {other:?} (seed={seed})"),
        }
    }

    // Let the backlog commit + persist, and let the apply task start
    // draining it — each `Cas`'s own `merge` call pays `delay`, so `merges`
    // advances roughly once per `delay` of sim time. Stop as soon as a
    // handful have gone through, confirming the apply task is genuinely
    // mid-backlog (not "just happened to finish everything already").
    let mut waited = Duration::ZERO;
    while merges.load(Ordering::SeqCst) < 3 && waited < Duration::from_secs(5) {
        sim.run_for(Duration::from_millis(10));
        waited += Duration::from_millis(10);
    }
    let observed = merges.load(Ordering::SeqCst);
    assert!(
        observed >= 3,
        "the apply task must have started draining the backlog within 5s (seed={seed})"
    );
    assert!(
        observed < N,
        "sanity: shutdown must land mid-pass with a real backlog remaining, not after \
         the whole thing already applied — only {observed}/{N} done so far (seed={seed})"
    );

    let t_shutdown = sim.env(nid(l as u64)).now();
    nodes[l].shutdown();

    // Poll for `is_stopped()`, recording how long it actually took.
    let budget = Duration::from_secs(30);
    let step = Duration::from_millis(20);
    let mut waited2 = Duration::ZERO;
    let mut stopped_at = None;
    while waited2 < budget {
        sim.run_for(step);
        waited2 += step;
        if nodes[l].is_stopped() {
            stopped_at = Some(sim.env(nid(l as u64)).now());
            break;
        }
    }
    let stopped_at = stopped_at
        .unwrap_or_else(|| panic!("driver must eventually stop within {budget:?} (seed={seed})"));
    let elapsed = Duration::from_nanos(stopped_at.0.saturating_sub(t_shutdown.0));

    // The full remaining backlog (up to ~N entries) would take roughly
    // `(N - observed) * delay` ~= 19.7s to drain one entry at a time; a
    // prompt stop must be bounded by a small, fixed number of entries'
    // worth of delay (the one merge already in flight when `halted` is set,
    // plus poll granularity) — never by how much backlog remains.
    assert!(
        elapsed < delay * 5,
        "shutdown must stop the driver within a few entries' worth of delay, not by \
         draining the whole remaining backlog: elapsed={elapsed:?}, delay={delay:?}, \
         backlog remaining >= {} (seed={seed})",
        N - observed
    );
}
