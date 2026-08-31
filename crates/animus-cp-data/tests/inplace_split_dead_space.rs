//! **The dead-space win** (ADR 0058 fork closed, range-aware `clone_to_filtered`)
//! — a dedicated, single-node `LsmEngine<SimEnv>` regression proving
//! whole-file assignment actually excludes a wholly-sibling SSTable from a
//! split child's own materialized engine, at the FILE level (not merely the
//! row level — `tests/inplace_split_reconciler.rs`'s own `happy_path`
//! scenario already proves per-row correctness over `MemoryEngine`, which
//! has no files to exclude).
//!
//! Kept as its own small file rather than a `MemoryEngine`-based corpus
//! scenario (`inplace_split_reconciler.rs`'s frozen cells) because it needs
//! a genuinely different engine type — `LsmEngine<SimEnv>`, the one backend
//! `clone_to_filtered`'s file-granularity behavior actually applies to — and
//! its own small `EngineFactory` wiring a tablet id to an `LsmEngine`
//! filename prefix, mirroring `animusd::LsmTabletFactory` but for `SimEnv`.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::host::{EngineFactory, MetadataView, Reconciler};
use animus_cp_data::{KIND_BASE, RaftKvNode};
use animus_env::{Clock, Disk, EnvExt, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use animus_tablet::{InPlaceSplitIntent, KeyRange, SplitChild, Tablet, TabletId, TabletState};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, LsmEngine<SimEnv>>;
type Recon = Reconciler<SimEnv, LsmEngine<SimEnv>>;

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

/// A split key that cleanly separates the two ranges of keys this test
/// seeds (`a*` below it, `z*` above it).
fn split_key() -> Vec<u8> {
    b"m".to_vec()
}

fn physical(key: &[u8]) -> Vec<u8> {
    let mut out = vec![KIND_BASE];
    out.extend_from_slice(key);
    out
}

/// No auto-compaction and a large flush threshold — every SSTable below is
/// produced by an explicit `flush_now()`, so the parent's own table set (and
/// each table's key range) is exactly what this test built, nothing more.
fn no_compact_opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 1 << 20,
        compaction_trigger: 100,
        target_table_bytes: 1 << 20,
        level_fanout: 8,
        wal_segment_bytes: 1 << 20,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

/// The `EngineFactory<LsmEngine<SimEnv>>` this test drives the reconciler
/// with — mirrors `animusd::LsmTabletFactory` exactly (filename-prefix
/// identity, `clone_to_filtered` under the hood), but additionally keeps its
/// own side registry of every engine handle it has ever opened/cloned, so
/// the test can reach in and read a child's own `sstable_views()` after
/// materialization without a second, unsafe re-open of the same prefix.
#[derive(Clone)]
struct LsmSimTabletFactory {
    env: SimEnv,
    registry: Arc<Mutex<BTreeMap<u64, LsmEngine<SimEnv>>>>,
}

impl LsmSimTabletFactory {
    fn new(env: SimEnv) -> Self {
        LsmSimTabletFactory {
            env,
            registry: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn prefix(tablet: TabletId) -> String {
        format!("db-t{}-", tablet.0)
    }

    /// The handle this factory itself opened/cloned for `tablet`, if any —
    /// the test's own read-only window into what the reconciler is hosting.
    fn engine(&self, tablet: TabletId) -> Option<LsmEngine<SimEnv>> {
        self.registry
            .lock()
            .expect("registry poisoned")
            .get(&tablet.0)
            .cloned()
    }
}

#[async_trait::async_trait]
impl EngineFactory<LsmEngine<SimEnv>> for LsmSimTabletFactory {
    async fn open(&self, tablet: TabletId) -> Result<LsmEngine<SimEnv>, String> {
        let engine =
            LsmEngine::open_with(self.env.clone(), &Self::prefix(tablet), no_compact_opts())
                .await
                .map_err(|e| e.to_string())?;
        self.registry
            .lock()
            .expect("registry poisoned")
            .insert(tablet.0, engine.clone());
        Ok(engine)
    }

    async fn probe(&self, tablet: TabletId) -> bool {
        let prefix = Self::prefix(tablet);
        self.env
            .list()
            .await
            .unwrap_or_default()
            .iter()
            .any(|f| f.starts_with(&prefix))
    }

    async fn destroy(&self, tablet: TabletId) {
        let prefix = Self::prefix(tablet);
        for f in self.env.list().await.unwrap_or_default() {
            if f.starts_with(&prefix) {
                let _ = self.env.remove(&f).await;
            }
        }
        self.registry
            .lock()
            .expect("registry poisoned")
            .remove(&tablet.0);
    }

    async fn clone_engine(
        &self,
        source: &LsmEngine<SimEnv>,
        target: TabletId,
        keep: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<LsmEngine<SimEnv>, String> {
        let engine = source
            .clone_to_filtered(Self::prefix(target), keep)
            .await
            .map_err(|e| e.to_string())?;
        self.registry
            .lock()
            .expect("registry poisoned")
            .insert(target.0, engine.clone());
        Ok(engine)
    }
}

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

#[test]
fn split_child_engine_excludes_the_wholly_sibling_sstable() {
    const SEED: u64 = 0xDEAD_5AC0_0001;
    run(SEED, move |sim| async move {
        let seed = SEED;
        let env = sim.env(nid(NODE));
        let factory = LsmSimTabletFactory::new(env.clone());
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

        // Seed two range-disjoint, EXPLICITLY FLUSHED SSTables on the
        // parent's own engine: one wholly left of the split key (`a*`), one
        // wholly right of it (`z*`) — whole-file assignment has a real,
        // independently-linkable file to skip for each child.
        {
            let leader = recon.hosted_node(PARENT).expect("elected above");
            for i in 0..20u64 {
                match leader.put(format!("a{i:04}").into_bytes(), b"lv".to_vec()) {
                    ProposeResult::Accepted { .. } => {}
                    other => panic!("left-range put rejected: {other:?} (seed={seed})"),
                }
            }
        }
        env.sleep(Duration::from_millis(200)).await;
        let parent_engine = factory.engine(PARENT).expect("parent engine opened");
        parent_engine.flush_now().await.expect("flush left table");

        {
            let leader = recon.hosted_node(PARENT).expect("still hosted");
            for i in 0..20u64 {
                match leader.put(format!("z{i:04}").into_bytes(), b"rv".to_vec()) {
                    ProposeResult::Accepted { .. } => {}
                    other => panic!("right-range put rejected: {other:?} (seed={seed})"),
                }
            }
        }
        env.sleep(Duration::from_millis(200)).await;
        parent_engine.flush_now().await.expect("flush right table");

        let before = parent_engine.sstable_views();
        assert!(
            before.len() >= 2,
            "seed={seed}: sanity — expected at least the two explicitly flushed \
             tables, got {before:?}"
        );
        let wholly_left_seq = before
            .iter()
            .find(|v| v.min_key.as_deref() == Some(physical(b"a0000").as_slice()))
            .map(|v| v.seq)
            .expect("the wholly-left table exists");
        let wholly_right_seq = before
            .iter()
            .find(|v| v.min_key.as_deref() == Some(physical(b"z0000").as_slice()))
            .map(|v| v.seq)
            .expect("the wholly-right table exists");
        assert_ne!(wholly_left_seq, wholly_right_seq);

        // Fork.
        let intent = InPlaceSplitIntent {
            split_key: split_key(),
            children: [
                SplitChild {
                    id: LEFT,
                    replicas: homes.clone(),
                },
                SplitChild {
                    id: RIGHT,
                    replicas: homes.clone(),
                },
            ],
        };
        let pending_view = view([parent_tablet(homes.clone(), Some(intent))]);
        let materialized = converge(&mut recon, &env, &pending_view, |r| {
            let hosted = &r.local_state().hosted;
            hosted.contains(&LEFT) && hosted.contains(&RIGHT)
        })
        .await;
        assert!(
            materialized,
            "both children never materialized (seed={seed})"
        );

        // Data correctness (the row-level property, quick sanity — the
        // file-level assertions below are this test's real point).
        let left_engine = factory.engine(LEFT).expect("left engine materialized");
        let right_engine = factory.engine(RIGHT).expect("right engine materialized");
        assert!(
            block_on(left_engine.get(&physical(b"a0000")))
                .unwrap()
                .is_some(),
            "seed={seed}: left-range row missing from LEFT child"
        );
        assert!(
            block_on(right_engine.get(&physical(b"z0000")))
                .unwrap()
                .is_some(),
            "seed={seed}: right-range row missing from RIGHT child"
        );

        // The dead-space win, at the FILE level: the wholly-sibling table is
        // never linked into the wrong child's own namespace at all.
        let left_seqs: Vec<u64> = left_engine.sstable_views().iter().map(|v| v.seq).collect();
        let right_seqs: Vec<u64> = right_engine.sstable_views().iter().map(|v| v.seq).collect();
        assert!(
            !left_seqs.contains(&wholly_right_seq),
            "seed={seed}: LEFT child's engine linked the wholly-RIGHT table \
             (seq {wholly_right_seq}) — whole-file assignment failed to exclude \
             it: LEFT tables = {left_seqs:?}"
        );
        assert!(
            !right_seqs.contains(&wholly_left_seq),
            "seed={seed}: RIGHT child's engine linked the wholly-LEFT table \
             (seq {wholly_left_seq}) — whole-file assignment failed to exclude \
             it: RIGHT tables = {right_seqs:?}"
        );
        assert!(
            left_seqs.contains(&wholly_left_seq),
            "seed={seed}: LEFT child's engine is missing its own wholly-LEFT \
             table (seq {wholly_left_seq}): LEFT tables = {left_seqs:?}"
        );
        assert!(
            right_seqs.contains(&wholly_right_seq),
            "seed={seed}: RIGHT child's engine is missing its own wholly-RIGHT \
             table (seq {wholly_right_seq}): RIGHT tables = {right_seqs:?}"
        );
    });
}
