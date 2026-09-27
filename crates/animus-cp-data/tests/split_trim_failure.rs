//! **Regression: a split child's trim step failing after its clone succeeds
//! must never leave the child permanently untrimmed** (the bug this file was
//! written to catch — see `host.rs::trim_marker`'s module doc for the fix).
//!
//! Before the fix, `Reconciler::materialize_split_child`'s `already_cloned`
//! resume branch (taken whenever `EngineFactory::probe(child.id)` reports the
//! target engine already exists) trusted `probe` alone as "fully materialized
//! and trimmed" and skipped `trim_split_child` outright. `probe` only proves
//! the CLONE half of the G4 contract — if a first attempt's `clone_engine`
//! succeeded but the immediately-following `trim_split_child` then failed (a
//! transient `delete_range` error) or the process crashed between the two,
//! the next tick's resume branch would reopen the untrimmed engine and host
//! the child directly on it: the sibling's own rows and the parent's whole
//! CHANGE/CURSOR scopes would leak into the child **permanently** (ADR 0046
//! principle 3, "no consumer offset ever crosses a split," violated for
//! good — nothing ever re-triggers a trim once the child is hosted).
//!
//! This test injects exactly that: a `delete_range` fault, armed to fail
//! only the RIGHT child's very first trim call — striking after
//! `clone_engine` has already committed but before `trim_split_child`'s own
//! completion marker can be written — then lets the reconciler retry on its
//! next tick (the fault is one-shot, so the retry's own `delete_range` calls
//! succeed). Single-node cluster (mirrors `tests/inplace_split_dead_space.rs`'s
//! own minimal shape): a one-replica Raft group elects and commits
//! trivially, so the test isolates the reconciler's own materialize/trim
//! resume logic from unrelated multi-node election timing.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::host::{EngineFactory, MemoryTabletEngines, MetadataView, Reconciler};
use animus_cp_data::{KIND_BASE, KIND_CHANGE, KIND_CURSOR, RaftKvNode};
use animus_env::{Clock, EnvExt, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, Result as StorageResult, StorageEngine, StorageError};
use animus_storage::{VersionedValue, WriteBatch};
use animus_tablet::{InPlaceSplitIntent, KeyRange, SplitChild, Tablet, TabletId, TabletState};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, FaultyEngine>;
type Recon = Reconciler<SimEnv, FaultyEngine>;

const TABLE: &str = "t";
const PARENT: TabletId = TabletId(1);
const LEFT: TabletId = TabletId(2);
const RIGHT: TabletId = TabletId(3);
const NODE: u64 = 0;

const SCENARIO_BUDGET: Duration = Duration::from_secs(150);
const SCENARIO_STEP: Duration = Duration::from_secs(1);

fn driver_id() -> NodeId {
    nid(900)
}

fn split_key() -> Vec<u8> {
    b"m".to_vec()
}

fn physical(key: &[u8]) -> Vec<u8> {
    let mut out = vec![KIND_BASE];
    out.extend_from_slice(key);
    out
}

// ---------------------------------------------------------------------------
// A `MemoryEngine` wrapper whose `delete_range` can be armed to fail exactly
// once for a chosen tablet id — the fault-injection seam this test needs.
// ---------------------------------------------------------------------------

/// Wraps a real [`MemoryEngine`], delegating every [`StorageEngine`] method
/// to it unchanged EXCEPT `delete_range`, which consults a shared "fail the
/// next call for this tablet" set: present and removed -> this ONE call
/// returns `Err` (never applied); otherwise delegates normally. One-shot by
/// construction (the set entry is consumed on the failing call), mirroring a
/// real transient fault (a disk hiccup, not a permanently broken device).
#[derive(Clone)]
struct FaultyEngine {
    inner: MemoryEngine,
    tablet: u64,
    fail_delete_range_once: Arc<Mutex<BTreeSet<u64>>>,
}

#[async_trait::async_trait]
impl StorageEngine for FaultyEngine {
    type Snapshot = <MemoryEngine as StorageEngine>::Snapshot;

    async fn put(&self, key: &[u8], value: &[u8], version: u64) -> StorageResult<()> {
        self.inner.put(key, value, version).await
    }

    async fn merge(&self, key: &[u8], value: &[u8], version: u64) -> StorageResult<bool> {
        self.inner.merge(key, value, version).await
    }

    async fn merge_tombstone(&self, key: &[u8], version: u64) -> StorageResult<bool> {
        self.inner.merge_tombstone(key, version).await
    }

    async fn delete(&self, key: &[u8], version: u64) -> StorageResult<()> {
        self.inner.delete(key, version).await
    }

    async fn delete_range(&self, start: &[u8], end: &[u8], version: u64) -> StorageResult<()> {
        let should_fail = self
            .fail_delete_range_once
            .lock()
            .expect("fault set poisoned")
            .remove(&self.tablet);
        if should_fail {
            return Err(StorageError::Backend(format!(
                "injected trim failure for tablet {}",
                self.tablet
            )));
        }
        self.inner.delete_range(start, end, version).await
    }

    async fn write_batch(&self, batch: WriteBatch) -> StorageResult<()> {
        self.inner.write_batch(batch).await
    }

    async fn get(&self, key: &[u8]) -> StorageResult<Option<VersionedValue>> {
        self.inner.get(key).await
    }

    async fn get_at(&self, key: &[u8], version: u64) -> StorageResult<Option<VersionedValue>> {
        self.inner.get_at(key, version).await
    }

    async fn scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> StorageResult<Vec<(Vec<u8>, VersionedValue)>> {
        self.inner.scan(start, end).await
    }

    async fn scan_at(
        &self,
        start: &[u8],
        end: &[u8],
        version: u64,
    ) -> StorageResult<Vec<(Vec<u8>, VersionedValue)>> {
        self.inner.scan_at(start, end, version).await
    }

    async fn entries(&self) -> StorageResult<Vec<(Vec<u8>, VersionedValue)>> {
        self.inner.entries().await
    }

    async fn entries_at(&self, version: u64) -> StorageResult<Vec<(Vec<u8>, VersionedValue)>> {
        self.inner.entries_at(version).await
    }

    async fn entries_with_tombstones(&self) -> StorageResult<Vec<(Vec<u8>, Option<Vec<u8>>, u64)>> {
        self.inner.entries_with_tombstones().await
    }

    fn snapshot(&self) -> Self::Snapshot {
        self.inner.snapshot()
    }

    fn latest_version(&self) -> u64 {
        self.inner.latest_version()
    }
}

/// The [`EngineFactory<FaultyEngine>`] this test drives the reconciler with:
/// a thin pass-through over [`MemoryTabletEngines`] that wraps every engine
/// it hands back in a [`FaultyEngine`] sharing one fault registry, so
/// [`arm_delete_range_failure`](Self::arm_delete_range_failure) reaches the
/// SAME underlying engine across `open`/`clone_engine` calls (and across a
/// retry's reopen of an already-cloned target).
#[derive(Clone, Default)]
struct FaultyFactory {
    inner: MemoryTabletEngines,
    fail_delete_range_once: Arc<Mutex<BTreeSet<u64>>>,
}

impl FaultyFactory {
    fn new() -> Self {
        Self::default()
    }

    /// Arm a ONE-SHOT `delete_range` failure for `tablet` — its very next
    /// `delete_range` call (from wherever it comes) fails; every call after
    /// that succeeds normally.
    fn arm_delete_range_failure(&self, tablet: TabletId) {
        self.fail_delete_range_once
            .lock()
            .expect("fault set poisoned")
            .insert(tablet.0);
    }

    fn wrap(&self, tablet: TabletId, inner: MemoryEngine) -> FaultyEngine {
        FaultyEngine {
            inner,
            tablet: tablet.0,
            fail_delete_range_once: self.fail_delete_range_once.clone(),
        }
    }
}

#[async_trait::async_trait]
impl EngineFactory<FaultyEngine> for FaultyFactory {
    async fn open(&self, tablet: TabletId) -> Result<FaultyEngine, String> {
        let inner = self.inner.open(tablet).await?;
        Ok(self.wrap(tablet, inner))
    }

    async fn probe(&self, tablet: TabletId) -> bool {
        self.inner.probe(tablet).await
    }

    async fn destroy(&self, tablet: TabletId) {
        self.inner.destroy(tablet).await;
    }

    async fn clone_engine(
        &self,
        source: &FaultyEngine,
        target: TabletId,
        keep: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<FaultyEngine, String> {
        let cloned = self.inner.clone_engine(&source.inner, target, keep).await?;
        Ok(self.wrap(target, cloned))
    }

    async fn local_tablets(&self) -> BTreeSet<TabletId> {
        self.inner.local_tablets().await
    }
}

// ---------------------------------------------------------------------------
// Harness (single-node shape, mirrors tests/inplace_split_dead_space.rs).
// ---------------------------------------------------------------------------

fn parent_tablet(replicas: Vec<NodeId>, split: Option<InPlaceSplitIntent>) -> Tablet {
    let mut t = Tablet::new_for_table(PARENT, TABLE, KeyRange::whole(), replicas);
    if split.is_some() {
        t.state = TabletState::Splitting;
    }
    t.inplace_split = split;
    t
}

fn view(tablets: impl IntoIterator<Item = Tablet>) -> MetadataView {
    MetadataView {
        tablets: tablets.into_iter().map(|t| (t.id, t)).collect(),
        ..Default::default()
    }
}

fn poll_until(
    sim: &mut Simulator,
    budget: Duration,
    step: Duration,
    msg: &str,
    mut check: impl FnMut() -> bool,
) {
    let mut waited = Duration::ZERO;
    while waited < budget {
        sim.run_for(step);
        waited += step;
        if check() {
            return;
        }
    }
    panic!("{msg} (seed={})", sim.seed());
}

fn run<F, Fut>(seed: u64, body: F)
where
    F: FnOnce(Simulator) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut sim = Simulator::new(seed);
    let driver_env = sim.env(driver_id());
    let done = Arc::new(Mutex::new(false));
    let done2 = Arc::clone(&done);
    let sim_in_task = sim.clone();
    driver_env.spawn_task(async move {
        body(sim_in_task).await;
        *done2.lock().unwrap() = true;
    });
    poll_until(
        &mut sim,
        SCENARIO_BUDGET,
        SCENARIO_STEP,
        "scenario never completed",
        move || *done.lock().unwrap(),
    );
}

async fn converge(
    reconciler: &mut Recon,
    env: &SimEnv,
    view: &MetadataView,
    mut check: impl FnMut(&Recon) -> bool,
) -> bool {
    for _ in 0..300 {
        reconciler.tick(view).await;
        if check(reconciler) {
            return true;
        }
        env.sleep(Duration::from_millis(100)).await;
    }
    check(reconciler)
}

fn intent(replicas: Vec<NodeId>) -> InPlaceSplitIntent {
    InPlaceSplitIntent {
        split_key: split_key(),
        children: [
            SplitChild {
                id: LEFT,
                replicas: replicas.clone(),
            },
            SplitChild {
                id: RIGHT,
                replicas,
            },
        ],
    }
}

#[test]
fn a_trim_failure_after_a_successful_clone_is_retried_not_permanently_skipped() {
    const SEED: u64 = 0x5111_7712_FA17;
    run(SEED, move |sim| async move {
        let seed = SEED;
        let env = sim.env(nid(NODE));
        let factory = FaultyFactory::new();
        let mut recon: Recon = Reconciler::new(
            env.clone(),
            factory.clone(),
            nid(NODE),
            |_: TabletId, _: &KvNode| {},
            |_: TabletId| {},
        );

        let homes = vec![nid(NODE)];
        let base_view = view([parent_tablet(homes.clone(), None)]);
        let elected = converge(&mut recon, &env, &base_view, |r| {
            r.hosted_node(PARENT).is_some_and(|h| h.is_leader())
        })
        .await;
        assert!(elected, "parent never elected (seed={seed})");

        {
            let leader = recon.hosted_node(PARENT).expect("elected above");
            match leader.put(b"left-key".to_vec(), b"lv".to_vec()) {
                ProposeResult::Accepted { .. } => {}
                other => panic!("pre-fork left-key put rejected: {other:?} (seed={seed})"),
            }
            match leader.put(b"z-right-key".to_vec(), b"rv".to_vec()) {
                ProposeResult::Accepted { .. } => {}
                other => panic!("pre-fork right-key put rejected: {other:?} (seed={seed})"),
            }
        }
        env.sleep(Duration::from_millis(200)).await;

        // Arm the fault BEFORE the fork: RIGHT's very first `delete_range`
        // call (issued by `trim_split_child`, right after `clone_engine`
        // commits its target) fails once. LEFT is never armed, so it
        // materializes normally on the very same tick.
        factory.arm_delete_range_failure(RIGHT);

        let pending_view = view([parent_tablet(homes.clone(), Some(intent(homes.clone())))]);

        // First convergence attempt: LEFT must materialize cleanly; RIGHT's
        // clone commits (so `probe(RIGHT)` now reports true) but its trim
        // fails, so RIGHT must NOT appear hosted yet.
        let left_only = converge(&mut recon, &env, &pending_view, |r| {
            r.local_state().hosted.contains(&LEFT)
        })
        .await;
        assert!(left_only, "LEFT never materialized (seed={seed})");
        assert!(
            !recon.local_state().hosted.contains(&RIGHT),
            "test fixture invariant: RIGHT must not be hosted yet — its first \
             trim attempt was supposed to fail (seed={seed})"
        );
        assert!(
            factory.probe(RIGHT).await,
            "test fixture invariant: RIGHT's engine must already be cloned \
             (probe true) even though its trim failed (seed={seed})"
        );

        // Retry: the fault is one-shot, so this tick's `trim_split_child`
        // call succeeds — the fixed `materialize_split_child` re-trims a
        // cloned-but-marker-less engine instead of trusting `probe` alone.
        let recovered = converge(&mut recon, &env, &pending_view, |r| {
            r.local_state().hosted.contains(&RIGHT)
        })
        .await;
        assert!(
            recovered,
            "RIGHT never recovered from its trim failure on retry (seed={seed})"
        );

        // The property this whole test exists to prove: RIGHT holds ONLY its
        // own range's BASE rows, and an EMPTY CHANGE/CURSOR scope — not the
        // sibling's leaked row or the parent's leaked change log.
        let right_engine = factory.inner.engine(RIGHT);
        assert!(
            block_on(right_engine.get(&physical(b"z-right-key")))
                .unwrap()
                .is_some(),
            "RIGHT is missing its own row (seed={seed})"
        );
        assert!(
            block_on(right_engine.get(&physical(b"left-key")))
                .unwrap()
                .is_none(),
            "RIGHT leaked the sibling's row — the trim never actually ran \
             (seed={seed})"
        );
        let leaked_change_or_cursor = block_on(right_engine.entries())
            .unwrap()
            .into_iter()
            .any(|(k, _)| k.first() == Some(&KIND_CHANGE) || k.first() == Some(&KIND_CURSOR));
        assert!(
            !leaked_change_or_cursor,
            "RIGHT was left with a non-empty CHANGE/CURSOR scope — the parent's \
             change log/cursors leaked across the split (ADR 0046 principle 3) \
             (seed={seed})"
        );
    });
}
