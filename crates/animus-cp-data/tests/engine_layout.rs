//! The per-tablet **engine layout marker** (ADR 0073 Phase 0, workstream C,
//! layer 4): `Reconciler::ensure_engine` stamps a fresh engine, accepts a
//! stamped one, and *refuses without destroying* an engine whose layout it
//! cannot vouch for; a split child is stamped in its trim-completion batch.
//! All scenarios run under `SimEnv` (seed-reproducible).

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::host::{EngineFactory, MemoryTabletEngines, MetadataView, Reconciler};
use animus_cp_data::layout::{LAYOUT_EPOCH, encode_layout_value, layout_marker_key};
use animus_cp_data::{KIND_BASE, RaftKvNode};
use animus_env::{Clock, Disk, EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, MemoryEngine, StorageEngine};
use animus_tablet::{InPlaceSplitIntent, KeyRange, SplitChild, Tablet, TabletId, TabletState};
use futures::executor::block_on;

const NODE: u64 = 0;
const TABLE: &str = "t";
const T1: TabletId = TabletId(1);
const LEFT: TabletId = TabletId(2);
const RIGHT: TabletId = TabletId(3);

fn view(tablets: impl IntoIterator<Item = Tablet>) -> MetadataView {
    MetadataView {
        tablets: tablets.into_iter().map(|t| (t.id, t)).collect(),
        ..Default::default()
    }
}

fn physical(key: &[u8]) -> Vec<u8> {
    let mut out = vec![KIND_BASE];
    out.extend_from_slice(key);
    out
}

fn run<F, Fut>(seed: u64, body: F)
where
    F: FnOnce(Simulator) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut sim = Simulator::new(seed);
    let driver = sim.env(nid(900));
    let done = Arc::new(Mutex::new(false));
    let done2 = Arc::clone(&done);
    let sim_in_task = sim.clone();
    driver.spawn_task(async move {
        body(sim_in_task).await;
        *done2.lock().unwrap() = true;
    });
    for _ in 0..150 {
        sim.run_for(Duration::from_secs(1));
        if *done.lock().unwrap() {
            return;
        }
    }
    panic!("scenario never completed (seed={seed})");
}

type MemRecon = Reconciler<SimEnv, MemoryEngine>;

fn mem_recon(env: &SimEnv, engines: &MemoryTabletEngines) -> MemRecon {
    Reconciler::new(
        env.clone(),
        engines.clone(),
        nid(NODE),
        |_: TabletId, _: &RaftKvNode<SimEnv, MemoryEngine>| {},
        |_: TabletId| {},
    )
}

fn one_tablet_view(id: TabletId) -> MetadataView {
    view([Tablet::new_for_table(
        id,
        TABLE,
        KeyRange::whole(),
        vec![nid(NODE)],
    )])
}

async fn tick_n(r: &mut MemRecon, env: &SimEnv, v: &MetadataView, n: usize) {
    for _ in 0..n {
        r.tick(v).await;
        env.sleep(Duration::from_millis(200)).await;
    }
}

async fn marker(engine: &MemoryEngine, tablet: TabletId) -> Option<Vec<u8>> {
    engine
        .get(&layout_marker_key(tablet.0))
        .await
        .unwrap()
        .map(|v| v.value)
}

#[test]
fn a_fresh_engine_is_stamped_and_a_reopen_is_accepted() {
    const SEED: u64 = 0x1A70_0001;
    run(SEED, |sim| async move {
        let env = sim.env(nid(NODE));
        let engines = MemoryTabletEngines::new();
        let v = one_tablet_view(T1);

        let mut r = mem_recon(&env, &engines);
        tick_n(&mut r, &env, &v, 20).await;
        assert!(r.hosted_node(T1).is_some(), "hosted (seed={SEED})");
        assert_eq!(
            marker(&engines.engine(T1), T1).await,
            Some(encode_layout_value()),
            "fresh engine stamped (seed={SEED})"
        );
        assert_eq!(
            encode_layout_value(),
            [b'K', b'L', b'Y', b'1', LAYOUT_EPOCH]
        );
        drop(r);

        // "Restart": a brand-new reconciler over the same durable registry.
        let mut r2 = mem_recon(&env, &engines);
        tick_n(&mut r2, &env, &v, 20).await;
        assert!(
            r2.hosted_node(T1).is_some(),
            "a stamped engine reopens fine (seed={SEED})"
        );
    });
}

/// Seed `engine` with one data row (and optionally a marker value for
/// `marker_tablet`), run the reconciler, and assert it refused: not hosted,
/// engine still present with its data (never destroyed/rebuilt).
async fn assert_refused(seed: u64, env: &SimEnv, engines: &MemoryTabletEngines, what: &str) {
    let v = one_tablet_view(T1);
    let mut r = mem_recon(env, engines);
    tick_n(&mut r, env, &v, 20).await;
    assert!(
        r.hosted_node(T1).is_none(),
        "{what}: must not be hosted (seed={seed})"
    );
    assert!(
        engines.probe(T1).await,
        "{what}: engine must not be destroyed (seed={seed})"
    );
    assert!(
        engines
            .engine(T1)
            .get(&physical(b"row"))
            .await
            .unwrap()
            .is_some(),
        "{what}: engine data must survive the refusal (seed={seed})"
    );
}

#[test]
fn an_unknown_epoch_marker_is_refused_and_the_engine_is_not_destroyed() {
    const SEED: u64 = 0x1A70_0002;
    run(SEED, |sim| async move {
        let env = sim.env(nid(NODE));
        let engines = MemoryTabletEngines::new();
        let e = engines.engine(T1);
        e.put(&physical(b"row"), b"v", 1).await.unwrap();
        e.put(&layout_marker_key(T1.0), b"KLY1\x09", 2)
            .await
            .unwrap();
        assert_refused(SEED, &env, &engines, "epoch 9").await;
        // And the marker itself was not rewritten.
        assert_eq!(marker(&e, T1).await, Some(b"KLY1\x09".to_vec()));
    });
}

#[test]
fn a_garbage_marker_is_refused_and_the_engine_is_not_destroyed() {
    const SEED: u64 = 0x1A70_0003;
    run(SEED, |sim| async move {
        let env = sim.env(nid(NODE));
        let engines = MemoryTabletEngines::new();
        let e = engines.engine(T1);
        e.put(&physical(b"row"), b"v", 1).await.unwrap();
        e.put(&layout_marker_key(T1.0), b"junk", 2).await.unwrap();
        assert_refused(SEED, &env, &engines, "garbage marker").await;
    });
}

#[test]
fn a_non_empty_unstamped_engine_is_refused_and_not_destroyed() {
    const SEED: u64 = 0x1A70_0004;
    run(SEED, |sim| async move {
        let env = sim.env(nid(NODE));
        let engines = MemoryTabletEngines::new();
        engines
            .engine(T1)
            .put(&physical(b"row"), b"v", 1)
            .await
            .unwrap();
        assert_refused(SEED, &env, &engines, "pre-baseline engine").await;
        assert_eq!(marker(&engines.engine(T1), T1).await, None, "not stamped");
    });
}

#[test]
fn a_marker_for_a_different_tablet_only_is_refused() {
    const SEED: u64 = 0x1A70_0005;
    run(SEED, |sim| async move {
        let env = sim.env(nid(NODE));
        let engines = MemoryTabletEngines::new();
        let e = engines.engine(T1);
        e.put(&physical(b"row"), b"v", 1).await.unwrap();
        // A well-formed marker, but for tablet 99.
        e.put(&layout_marker_key(99), &encode_layout_value(), 2)
            .await
            .unwrap();
        assert_refused(SEED, &env, &engines, "another tablet's marker").await;
        assert_eq!(marker(&e, T1).await, None, "own marker not stamped");
    });
}

// ---------------------------------------------------------------------------
// Split children over LsmEngine<SimEnv>

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

#[derive(Clone)]
struct LsmSimFactory {
    env: SimEnv,
    registry: Arc<Mutex<BTreeMap<u64, LsmEngine<SimEnv>>>>,
}

impl LsmSimFactory {
    fn new(env: SimEnv) -> Self {
        Self {
            env,
            registry: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
    fn prefix(t: TabletId) -> String {
        format!("db-t{}-", t.0)
    }
    fn engine(&self, t: TabletId) -> Option<LsmEngine<SimEnv>> {
        self.registry.lock().unwrap().get(&t.0).cloned()
    }
}

#[async_trait::async_trait]
impl EngineFactory<LsmEngine<SimEnv>> for LsmSimFactory {
    async fn open(&self, t: TabletId) -> Result<LsmEngine<SimEnv>, String> {
        let e = LsmEngine::open_with(self.env.clone(), &Self::prefix(t), no_compact_opts())
            .await
            .map_err(|e| e.to_string())?;
        self.registry.lock().unwrap().insert(t.0, e.clone());
        Ok(e)
    }
    async fn flush_engine(&self, engine: &LsmEngine<SimEnv>) -> Result<(), String> {
        engine.flush_now().await.map_err(|e| e.to_string())
    }

    async fn probe(&self, t: TabletId) -> bool {
        let p = Self::prefix(t);
        self.env
            .list()
            .await
            .unwrap_or_default()
            .iter()
            .any(|f| f.starts_with(&p))
    }
    async fn destroy(&self, t: TabletId) {
        let p = Self::prefix(t);
        for f in self.env.list().await.unwrap_or_default() {
            if f.starts_with(&p) {
                let _ = self.env.remove(&f).await;
            }
        }
        self.registry.lock().unwrap().remove(&t.0);
    }
    async fn clone_engine(
        &self,
        source: &LsmEngine<SimEnv>,
        target: TabletId,
        keep: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<LsmEngine<SimEnv>, String> {
        let e = source
            .clone_to_filtered(Self::prefix(target), keep)
            .await
            .map_err(|e| e.to_string())?;
        self.registry.lock().unwrap().insert(target.0, e.clone());
        Ok(e)
    }
}

type LsmRecon = Reconciler<SimEnv, LsmEngine<SimEnv>>;

fn pending_split_view() -> MetadataView {
    let mut t = Tablet::new_for_table(T1, TABLE, KeyRange::whole(), vec![nid(NODE)]);
    t.state = TabletState::Splitting;
    t.inplace_split = Some(InPlaceSplitIntent {
        split_key: b"m".to_vec(),
        children: [
            SplitChild {
                id: LEFT,
                replicas: vec![nid(NODE)],
            },
            SplitChild {
                id: RIGHT,
                replicas: vec![nid(NODE)],
            },
        ],
    });
    view([t])
}

async fn lsm_marker(engine: &LsmEngine<SimEnv>, t: TabletId) -> Option<Vec<u8>> {
    engine
        .get(&layout_marker_key(t.0))
        .await
        .unwrap()
        .map(|v| v.value)
}

/// Host the parent, write `a*`/`z*` rows through Raft, flush.
async fn hosted_parent(env: &SimEnv, factory: &LsmSimFactory, seed: u64) -> LsmRecon {
    let mut recon: LsmRecon = Reconciler::new(
        env.clone(),
        factory.clone(),
        nid(NODE),
        |_: TabletId, _: &RaftKvNode<SimEnv, LsmEngine<SimEnv>>| {},
        |_: TabletId| {},
    );
    let base = view([Tablet::new_for_table(
        T1,
        TABLE,
        KeyRange::whole(),
        vec![nid(NODE)],
    )]);
    for _ in 0..300 {
        recon.tick(&base).await;
        if recon.hosted_node(T1).is_some_and(|h| h.is_leader()) {
            break;
        }
        env.sleep(Duration::from_millis(100)).await;
    }
    let leader = recon
        .hosted_node(T1)
        .filter(|h| h.is_leader())
        .unwrap_or_else(|| panic!("parent never elected (seed={seed})"));
    for i in 0..5u64 {
        leader.put(format!("a{i}").into_bytes(), b"l".to_vec());
        leader.put(format!("z{i}").into_bytes(), b"r".to_vec());
    }
    env.sleep(Duration::from_millis(500)).await;
    recon
}

async fn tick_until_children(recon: &mut LsmRecon, env: &SimEnv, seed: u64) {
    let v = pending_split_view();
    for _ in 0..300 {
        recon.tick(&v).await;
        let h = &recon.local_state().hosted;
        if h.contains(&LEFT) && h.contains(&RIGHT) {
            return;
        }
        env.sleep(Duration::from_millis(100)).await;
    }
    panic!("children never materialized (seed={seed})");
}

#[test]
fn split_children_get_their_own_layout_marker() {
    const SEED: u64 = 0x1A70_0006;
    run(SEED, |sim| async move {
        let env = sim.env(nid(NODE));
        let factory = LsmSimFactory::new(env.clone());
        let mut recon = hosted_parent(&env, &factory, SEED).await;
        assert!(
            lsm_marker(&factory.engine(T1).unwrap(), T1).await.is_some(),
            "parent stamped (seed={SEED})"
        );
        tick_until_children(&mut recon, &env, SEED).await;
        for c in [LEFT, RIGHT] {
            assert_eq!(
                lsm_marker(&factory.engine(c).unwrap(), c).await,
                Some(encode_layout_value()),
                "child {c:?} carries its OWN marker (seed={SEED})"
            );
        }
    });
}

/// The engine state a crash between `clone_engine` (the engine commit) and
/// the trim/stamp batch leaves behind: a cloned, non-empty child with no
/// trim marker and no layout marker. The resume branch must re-trim and
/// stamp it (a bare `ensure_engine` would — correctly — refuse such an
/// engine, which is why the child never goes through it mid-materialize).
#[test]
fn a_crash_between_clone_and_stamp_resumes_and_stamps() {
    const SEED: u64 = 0x1A70_0007;
    run(SEED, |sim| async move {
        let env = sim.env(nid(NODE));
        let factory = LsmSimFactory::new(env.clone());
        let mut recon = hosted_parent(&env, &factory, SEED).await;
        let parent = factory.engine(T1).unwrap();
        parent.flush_now().await.expect("flush");

        // The crash state: LEFT cloned (whole-kind keep), never trimmed.
        let keep = vec![(vec![KIND_BASE], Some(vec![KIND_BASE + 1]))];
        let pre = factory
            .clone_engine(&parent, LEFT, &keep)
            .await
            .expect("clone");
        assert_eq!(
            lsm_marker(&pre, LEFT).await,
            None,
            "the crash state has no LEFT marker (seed={SEED})"
        );
        assert!(
            block_on(pre.get(&physical(b"z0"))).unwrap().is_some(),
            "and still carries the sibling's rows (seed={SEED})"
        );
        drop(pre);
        factory.registry.lock().unwrap().remove(&LEFT.0);

        tick_until_children(&mut recon, &env, SEED).await;
        let left = factory.engine(LEFT).unwrap();
        assert_eq!(
            lsm_marker(&left, LEFT).await,
            Some(encode_layout_value()),
            "resumed child stamped (seed={SEED})"
        );
        assert!(
            block_on(left.get(&physical(b"z0"))).unwrap().is_none(),
            "resumed child trimmed (seed={SEED})"
        );
        assert!(
            block_on(left.get(&physical(b"a0"))).unwrap().is_some(),
            "resumed child keeps its own rows (seed={SEED})"
        );
    });
}

/// The stamp must land in its OWN SSTable (`EngineFactory::flush_engine`
/// right after the stamp), so the first table of kind rows never spans the
/// reserved namespace — otherwise `clone_to_filtered`'s whole-file exclusion
/// could never drop it from a split child. Pins the fix for the regression
/// `inplace_split_dead_space` caught.
#[test]
fn the_stamp_lands_in_its_own_sstable_and_kind_rows_do_not_span_it() {
    const SEED: u64 = 0x1A70_0009;
    run(SEED, |sim| async move {
        let env = sim.env(nid(NODE));
        let factory = LsmSimFactory::new(env.clone());
        let recon = hosted_parent(&env, &factory, SEED).await;
        let engine = factory.engine(T1).unwrap();
        let marker_key = layout_marker_key(T1.0);

        // Only the stamp has ever been flushed so far (rows are still in
        // the memtable): exactly one table, holding exactly the marker.
        let before = engine.sstable_views();
        assert_eq!(
            before.len(),
            1,
            "the stamp alone was flushed into one table (seed={SEED}): {before:?}"
        );
        assert_eq!(before[0].min_key.as_deref(), Some(marker_key.as_slice()));
        assert_eq!(before[0].max_key.as_deref(), Some(marker_key.as_slice()));

        // The first table of kind rows: bounded by the rows, strictly below
        // the reserved namespace.
        engine.flush_now().await.expect("flush rows");
        let rows: Vec<_> = engine
            .sstable_views()
            .into_iter()
            .filter(|v| v.min_key.as_deref() != Some(marker_key.as_slice()))
            .collect();
        assert_eq!(rows.len(), 1, "one rows table (seed={SEED}): {rows:?}");
        assert_eq!(rows[0].min_key.as_deref(), Some(physical(b"a0").as_slice()));
        assert!(
            rows[0]
                .max_key
                .as_deref()
                .is_some_and(|m| m < marker_key.as_slice()),
            "the rows table must not span up to the marker (seed={SEED}): {rows:?}"
        );
        drop(recon);
    });
}
