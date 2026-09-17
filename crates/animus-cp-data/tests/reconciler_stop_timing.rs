//! Regression for the reconciler group-driver-stop-timing fix (`host.rs`'s
//! `Reconciler::teardown`/`sweep_stopping`): a tablet whose driver is slow to
//! stop must not block `Reconciler::tick()` for anywhere near
//! `RECLAIM_STOP_TIMEOUT`, and must not starve every other tablet the same
//! node hosts while it winds down.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()`. Per the documented `SimEnv` gotcha (see `tests/
//! reconciler.rs`'s own module doc), a `tick()` call whose planned action
//! tears a group down internally `env.sleep()`s, so the whole scenario runs
//! inside one spawned task driven by `run_for`, never a bare `block_on`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_cp_data::host::{EngineFactory, MetadataView, Reconciler};
use animus_env::{Clock, Env, EnvExt, Metric, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{
    Key, MemoryEngine, MergeOp, Result as StorageResult, StorageEngine, Value, Version,
    VersionedValue, WriteBatch,
};
use animus_tablet::{KeyRange, Tablet, TabletId};

const BASE: u64 = 320;
const TABLE: &str = "t";

fn tablet(id: u64, replicas: Vec<u64>) -> Tablet {
    Tablet::new_for_table(
        TabletId(id),
        TABLE,
        KeyRange::whole(),
        replicas.into_iter().map(nid).collect(),
    )
}

fn view(tablets: impl IntoIterator<Item = Tablet>) -> MetadataView {
    MetadataView {
        tablets: tablets.into_iter().map(|t| (t.id, t)).collect(),
        ..Default::default()
    }
}

/// A [`StorageEngine`] whose `merge`/`merge_tombstone`/`merge_batch` block
/// (poll a shared flag under `env.sleep`) while their own per-tablet gate is
/// closed — everything else delegates to an inner [`MemoryEngine`]
/// untouched. Models a tablet whose apply task is genuinely stuck mid-pass
/// (a wedged disk, in the real bug's own terms): the test closes the gate,
/// commits a write, and the very next `merge` call inside `apply_and_compact`
/// never returns until the test reopens it — precisely the precondition
/// `Reconciler::teardown`'s inline wait must not block on for more than
/// [`animus_cp_data::host::RECLAIM_STOP_GRACE`].
#[derive(Clone)]
struct GatedEngine {
    inner: MemoryEngine,
    env: SimEnv,
    gate_closed: Arc<AtomicBool>,
}

impl GatedEngine {
    fn new(env: SimEnv, gate_closed: Arc<AtomicBool>) -> Self {
        Self {
            inner: MemoryEngine::new(),
            env,
            gate_closed,
        }
    }

    async fn wait_for_gate(&self) {
        while self.gate_closed.load(Ordering::SeqCst) {
            self.env.sleep(Duration::from_millis(10)).await;
        }
    }
}

#[async_trait::async_trait]
impl StorageEngine for GatedEngine {
    type Snapshot = <MemoryEngine as StorageEngine>::Snapshot;

    async fn put(&self, key: &[u8], value: &[u8], version: Version) -> StorageResult<()> {
        self.inner.put(key, value, version).await
    }

    async fn merge(&self, key: &[u8], value: &[u8], version: Version) -> StorageResult<bool> {
        self.wait_for_gate().await;
        self.inner.merge(key, value, version).await
    }

    async fn merge_tombstone(&self, key: &[u8], version: Version) -> StorageResult<bool> {
        self.wait_for_gate().await;
        self.inner.merge_tombstone(key, version).await
    }

    async fn merge_batch(&self, ops: Vec<MergeOp>) -> StorageResult<()> {
        self.wait_for_gate().await;
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

/// [`EngineFactory`] for [`GatedEngine`]: one engine (and one gate) per
/// tablet id, plus a destroy record the test reads back for assertion (4)
/// ("files erased via the factory fake's destroy record").
#[derive(Clone, Default)]
struct GatedFactory {
    env: Option<SimEnv>,
    engines: Arc<Mutex<BTreeMap<u64, GatedEngine>>>,
    gates: Arc<Mutex<BTreeMap<u64, Arc<AtomicBool>>>>,
    destroyed: Arc<Mutex<BTreeSet<u64>>>,
}

impl GatedFactory {
    fn new(env: SimEnv) -> Self {
        Self {
            env: Some(env),
            ..Default::default()
        }
    }

    /// This tablet's own gate — created open (`false`, i.e. not blocking) on
    /// first access, so a tablet the test never touches behaves like a
    /// plain, fast `MemoryEngine`.
    fn gate(&self, tablet: TabletId) -> Arc<AtomicBool> {
        self.gates
            .lock()
            .expect("gate registry poisoned")
            .entry(tablet.0)
            .or_insert_with(|| Arc::new(AtomicBool::new(false)))
            .clone()
    }

    fn was_destroyed(&self, tablet: TabletId) -> bool {
        self.destroyed
            .lock()
            .expect("destroy record poisoned")
            .contains(&tablet.0)
    }
}

#[async_trait::async_trait]
impl EngineFactory<GatedEngine> for GatedFactory {
    async fn open(&self, tablet: TabletId) -> Result<GatedEngine, String> {
        let gate = self.gate(tablet);
        let env = self.env.clone().expect("GatedFactory::new sets env");
        Ok(self
            .engines
            .lock()
            .expect("engine registry poisoned")
            .entry(tablet.0)
            .or_insert_with(|| GatedEngine::new(env, gate))
            .clone())
    }

    async fn probe(&self, tablet: TabletId) -> bool {
        self.engines
            .lock()
            .expect("engine registry poisoned")
            .contains_key(&tablet.0)
    }

    async fn destroy(&self, tablet: TabletId) {
        self.engines
            .lock()
            .expect("engine registry poisoned")
            .remove(&tablet.0);
        self.destroyed
            .lock()
            .expect("destroy record poisoned")
            .insert(tablet.0);
    }

    async fn clone_engine(
        &self,
        source: &GatedEngine,
        target: TabletId,
        _keep: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<GatedEngine, String> {
        let cloned = GatedEngine {
            inner: source.inner.clone_to(),
            env: source.env.clone(),
            gate_closed: self.gate(target),
        };
        self.engines
            .lock()
            .expect("engine registry poisoned")
            .insert(target.0, cloned.clone());
        Ok(cloned)
    }

    async fn local_tablets(&self) -> BTreeSet<TabletId> {
        self.engines
            .lock()
            .expect("engine registry poisoned")
            .keys()
            .map(|&id| TabletId(id))
            .collect()
    }
}

type GatedNode = RaftKvNode<SimEnv, GatedEngine>;

/// The full regression: A's driver gets stuck mid-teardown (gate closed
/// through a real committed backlog), then:
/// 1. the `tick()` that reclaims A returns promptly (nowhere near
///    `RECLAIM_STOP_TIMEOUT`);
/// 2. a second tablet, B, is hosted and actually serves a write in a later
///    tick while A is still parked stopping;
/// 3. A's live handle is never re-registered with the routing hook
///    (`on_host`) once its teardown began;
/// 4. once the test releases A's gate, a later tick finishes A's teardown
///    (its engine is destroyed, `LocalState` no longer claims it) with
///    `Metric::CpReconcilerStopTimeout` staying at zero (resolved well
///    within the timeout); and
/// 5. a second tablet, C, put through the identical stuck-teardown shape but
///    held past `RECLAIM_STOP_TIMEOUT` before its gate is released, bumps
///    that metric by **exactly one** — never once per tick spent parked,
///    and never again once it is warned.
#[test]
fn a_slow_stopping_tablet_does_not_starve_the_reconciler() {
    let seed = 0x5706_0001_u64;
    let mut sim = Simulator::new(seed);
    let base_env = sim.env(nid(BASE));

    let factory = GatedFactory::new(base_env.clone());
    let hosted_log: Arc<Mutex<Vec<TabletId>>> = Arc::new(Mutex::new(Vec::new()));

    let driver_env = sim.env(nid(900));
    let done = Arc::new(Mutex::new(false));
    let done2 = Arc::clone(&done);

    let factory_task = factory.clone();
    let env_task = base_env.clone();
    let hosted_log_task = Arc::clone(&hosted_log);
    driver_env.spawn_task(async move {
        let mut reconciler: Reconciler<SimEnv, GatedEngine> = Reconciler::new(
            env_task.clone(),
            factory_task.clone(),
            nid(BASE),
            move |t, _n: &GatedNode| hosted_log_task.lock().expect("hosted log poisoned").push(t),
            |_t| {},
        );

        const A: u64 = 1;
        const B: u64 = 2;
        const C: u64 = 3;

        // --- Host A alone and elect it (a single-voter group). ---------
        reconciler.tick(&view([tablet(A, vec![BASE])])).await;
        env_task.sleep(Duration::from_secs(2)).await;
        let a = reconciler
            .hosted_node(TabletId(A))
            .expect("A must be hosted")
            .clone();
        assert!(a.is_leader(), "a single-voter group must self-elect");

        // Close A's gate, then commit a small backlog: the very first
        // `Cas`'s own `merge` call blocks until the test reopens the gate.
        let gate_a = factory_task.gate(TabletId(A));
        gate_a.store(true, Ordering::SeqCst);
        for i in 0..5u32 {
            match a.cas(format!("k{i}").into_bytes(), None, b"v".to_vec()) {
                ProposeResult::Accepted { .. } => {}
                other => panic!("A rejected cas #{i}: {other:?} (seed={seed:#x})"),
            }
        }
        // Let the backlog commit + persist and the apply task get stuck in
        // its first blocked `merge` call.
        env_task.sleep(Duration::from_millis(300)).await;

        // --- Reclaim A (its whole table dropped) -----------------------
        let t0 = env_task.now();
        reconciler.tick(&view([])).await;
        let t1 = env_task.now();
        let tick_elapsed = Duration::from_nanos(t1.0.saturating_sub(t0.0));
        assert!(
            tick_elapsed < Duration::from_secs(2),
            "(1) tick() must return within about RECLAIM_STOP_GRACE even though A's \
             driver is stuck, not anywhere near RECLAIM_STOP_TIMEOUT: {tick_elapsed:?} \
             (seed={seed:#x})"
        );
        assert!(
            reconciler.is_stopping(TabletId(A)),
            "A must be parked stopping, not confirmed torn down (its driver never \
             actually stopped) and not silently dropped (seed={seed:#x})"
        );
        assert!(
            reconciler.hosted_node(TabletId(A)).is_none(),
            "A's handle must have left the live/routable hosted map (seed={seed:#x})"
        );
        assert_eq!(
            hosted_log
                .lock()
                .expect("hosted log poisoned")
                .iter()
                .filter(|&&t| t == TabletId(A))
                .count(),
            1,
            "(3) on_host must never re-register A once its teardown has begun \
             (seed={seed:#x})"
        );

        // --- Host B while A is still parked stopping --------------------
        let t2 = env_task.now();
        reconciler.tick(&view([tablet(B, vec![BASE])])).await;
        let t3 = env_task.now();
        let host_b_elapsed = Duration::from_nanos(t3.0.saturating_sub(t2.0));
        assert!(
            host_b_elapsed < Duration::from_secs(2),
            "(2) hosting B must proceed promptly, not wait on A's still-parked \
             teardown: {host_b_elapsed:?} (seed={seed:#x})"
        );
        env_task.sleep(Duration::from_secs(2)).await; // elect B
        let b = reconciler
            .hosted_node(TabletId(B))
            .expect("B must be hosted")
            .clone();
        assert!(
            b.is_leader(),
            "(2) B must be hosted and elected while A is still stopping (seed={seed:#x})"
        );
        match b.put(b"k".to_vec(), b"v".to_vec()) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("B rejected put: {other:?} (seed={seed:#x})"),
        }
        env_task.sleep(Duration::from_secs(1)).await;
        assert_eq!(
            b.local_get(b"k").await,
            Some(b"v".to_vec()),
            "(2) B must actually serve while A is still stopping (seed={seed:#x})"
        );
        assert!(
            reconciler.is_stopping(TabletId(A)),
            "A must still be parked throughout B's hosting/serving (seed={seed:#x})"
        );

        // --- Case 1: release A's gate well within RECLAIM_STOP_TIMEOUT —
        // no warning, no metric bump. ------------------------------------
        let metric_before = env_task.metrics().get(Metric::CpReconcilerStopTimeout);
        gate_a.store(false, Ordering::SeqCst);
        let mut waited = Duration::ZERO;
        while reconciler.is_stopping(TabletId(A)) && waited < Duration::from_secs(5) {
            reconciler.tick(&view([tablet(B, vec![BASE])])).await;
            env_task.sleep(Duration::from_millis(100)).await;
            waited += Duration::from_millis(100);
        }
        assert!(
            !reconciler.is_stopping(TabletId(A)),
            "A's teardown must complete once its driver actually stops (seed={seed:#x})"
        );
        assert!(
            !reconciler.local_state().hosted.contains(&TabletId(A)),
            "(4) A's claim must be confirmed torn down (seed={seed:#x})"
        );
        assert!(
            factory_task.was_destroyed(TabletId(A)),
            "(4) A's engine files must be erased once its teardown completes \
             (seed={seed:#x})"
        );
        let metric_after = env_task.metrics().get(Metric::CpReconcilerStopTimeout);
        assert_eq!(
            metric_after - metric_before,
            0,
            "no CpReconcilerStopTimeout bump when the driver stops well within \
             RECLAIM_STOP_TIMEOUT (seed={seed:#x})"
        );

        // --- Case 2 (assertion 5): C, held past RECLAIM_STOP_TIMEOUT ----
        let view_bc = view([tablet(B, vec![BASE]), tablet(C, vec![BASE])]);
        reconciler.tick(&view_bc).await;
        env_task.sleep(Duration::from_secs(2)).await; // elect C
        let c = reconciler
            .hosted_node(TabletId(C))
            .expect("C must be hosted")
            .clone();
        assert!(c.is_leader());
        let gate_c = factory_task.gate(TabletId(C));
        gate_c.store(true, Ordering::SeqCst);
        match c.cas(b"k".to_vec(), None, b"v".to_vec()) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("C rejected cas: {other:?} (seed={seed:#x})"),
        }
        env_task.sleep(Duration::from_millis(300)).await;

        let view_b_only = view([tablet(B, vec![BASE])]); // C dropped, B stays
        reconciler.tick(&view_b_only).await; // parks C
        assert!(reconciler.is_stopping(TabletId(C)));

        let metric_before2 = env_task.metrics().get(Metric::CpReconcilerStopTimeout);
        // Keep ticking (each tick's own `sweep_stopping` is what notices the
        // timeout crossing) well past RECLAIM_STOP_TIMEOUT while the gate
        // stays closed.
        let mut elapsed = Duration::ZERO;
        while elapsed < Duration::from_secs(12) {
            reconciler.tick(&view_b_only).await;
            env_task.sleep(Duration::from_millis(500)).await;
            elapsed += Duration::from_millis(500);
        }
        assert!(
            reconciler.is_stopping(TabletId(C)),
            "C must still be parked — its gate was never released in this window \
             (seed={seed:#x})"
        );
        let metric_after2 = env_task.metrics().get(Metric::CpReconcilerStopTimeout);
        assert_eq!(
            metric_after2 - metric_before2,
            1,
            "(5) exactly one CpReconcilerStopTimeout bump once RECLAIM_STOP_TIMEOUT \
             is crossed while still parked — never once per tick spent parked \
             (seed={seed:#x})"
        );

        // Release C's gate: its teardown finishes, and the metric does NOT
        // bump again just because it was already warned once.
        gate_c.store(false, Ordering::SeqCst);
        let mut waited2 = Duration::ZERO;
        while reconciler.is_stopping(TabletId(C)) && waited2 < Duration::from_secs(5) {
            reconciler.tick(&view_b_only).await;
            env_task.sleep(Duration::from_millis(100)).await;
            waited2 += Duration::from_millis(100);
        }
        assert!(!reconciler.is_stopping(TabletId(C)));
        assert!(factory_task.was_destroyed(TabletId(C)));
        let metric_after3 = env_task.metrics().get(Metric::CpReconcilerStopTimeout);
        assert_eq!(
            metric_after3 - metric_before2,
            1,
            "(5) the metric stays at exactly one bump for this stopping episode, \
             even after it goes on to resolve (seed={seed:#x})"
        );

        *done2.lock().expect("done flag poisoned") = true;
    });

    let mut waited = Duration::ZERO;
    let budget = Duration::from_secs(60);
    while !*done.lock().expect("done flag poisoned") && waited < budget {
        sim.run_for(Duration::from_millis(50));
        waited += Duration::from_millis(50);
    }
    assert!(
        *done.lock().expect("done flag poisoned"),
        "scenario task must finish within {budget:?} (seed={seed:#x})"
    );
}
