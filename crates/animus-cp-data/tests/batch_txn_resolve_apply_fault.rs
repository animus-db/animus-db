//! Issue #834: `KvCommand::Batch` and `KvCommand::TxnResolve`'s commit /
//! staged-delete branches used to call `storage.merge`/`merge_tombstone`
//! directly, once per key — N un-amortized `fsync`s per Raft entry on a
//! durable engine, and no `halted` gate (a hard `.expect(..)` panic even
//! during a graceful shutdown racing an in-flight write). Both arms now
//! queue onto the shared `pending` run `flush_pending` already coalesces
//! into one `merge_batch` per pass, exactly like `Put`/`Delete`/
//! `SeedBatch`/`materialize_derived`/`KindBatch`.
//!
//! This file covers the two properties a plain `MemoryEngine`/`SimEnv`
//! correctness test (`tests/batch.rs`, `tests/txn_single.rs`) can't see:
//!
//! - **Halted-gate regression** (`FaultyEngine`, below): with `halted`
//!   already latched, a `storage.merge_batch` failure reached via a
//!   `Batch`/`TxnResolve` apply must exit cleanly, never panic — the
//!   `flush_pending` contract `tests/shutdown.rs` already proves for
//!   `persist_wal` (the Raft WAL); `flush_pending`'s own storage-engine
//!   error path had no dedicated regression anywhere in the workspace
//!   before this issue (`MemoryEngine`'s `merge`/`merge_tombstone` never
//!   return `Err`, so nothing could reach it), and — before this fix —
//!   `Batch`/`TxnResolve` didn't route through `flush_pending` at all.
//! - **fsync count** (`CountedEnv`, below): over a real `LsmEngine`, one
//!   `Batch` entry's apply performs the same number of `Disk::sync` calls
//!   regardless of how many keys it carries — O(1), not O(N) — proven
//!   deterministically under `SimEnv` rather than a real-thread bench (see
//!   `benches/wal_fsync_bench.rs`'s identical `CountedEnv` pattern, which
//!   wraps `ProdEnv`; this one wraps `SimEnv` instead).
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{
    BoxFuture, Clock, Disk, Env, EnvExt, Envelope, MetricsHandle, Nanos, Network, NodeId, Rng,
    Spawner, UnixMillis, nid,
};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{
    Key, LsmEngine, LsmOptions, MemoryEngine, MemorySnapshot, MergeOp, Result as StorageResult,
    StorageEngine, StorageError, Value, Version, VersionedValue, WriteBatch,
};
use animus_tablet::{escape, partition_token};
use futures::executor::block_on;

/// A real ADR 0022-shaped data-plane key: `partition_token(pk) ||
/// escape(pk) || rk` — mirrors `tests/txn_single.rs`'s identical helper,
/// the layout `txn_write`'s anchor-token disjointness proof (`txn.rs`)
/// assumes.
fn key(pk: &[u8], rk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out.extend_from_slice(rk);
    out
}

// =======================================================================
// Part 1: the halted-gate regression, over `FaultyEngine`.
// =======================================================================

type FaultKvNode = RaftKvNode<SimEnv, FaultyEngine>;

/// A `MemoryEngine` wrapper whose `merge_batch` can be armed to fail —
/// standing in for a real `LsmEngine`'s `merge_batch` hitting a disk fault,
/// without needing real I/O. `MemoryEngine`'s own `merge`/`merge_tombstone`
/// never return `Err` (the storage crate has no failure mode for an
/// in-memory backend), so this is the only way to reach `flush_pending`'s
/// error-tolerance branch deterministically.
#[derive(Clone)]
struct FaultyEngine {
    inner: MemoryEngine,
    fail_merge_batch: Arc<AtomicBool>,
    // Set once, right after the node it's wired into is constructed (see
    // `single_node_with_faulty_engine`). When the injected failure fires,
    // `merge_batch` calls `shutdown()` on it *before* returning the error —
    // the deterministic, single-threaded-cooperative-scheduler stand-in for
    // a genuine concurrent `shutdown()` racing this in-flight merge (see
    // `flush_pending`'s own doc, `lib.rs`): `MemoryEngine`'s async methods
    // never really suspend (this crate's own `CLAUDE.md`: "awaits nothing
    // real"), so there is no `.await` point for a separately-scheduled task
    // to interleave at, and setting the flag in-line achieves the identical
    // "halted is already true by the time the error is observed" contract.
    node: Arc<OnceLock<FaultKvNode>>,
}

impl FaultyEngine {
    fn new(fail_merge_batch: Arc<AtomicBool>, node: Arc<OnceLock<FaultKvNode>>) -> Self {
        Self {
            inner: MemoryEngine::new(),
            fail_merge_batch,
            node,
        }
    }
}

#[async_trait::async_trait]
impl StorageEngine for FaultyEngine {
    type Snapshot = MemorySnapshot;

    async fn put(&self, key: &[u8], value: &[u8], version: Version) -> StorageResult<()> {
        self.inner.put(key, value, version).await
    }

    async fn merge(&self, key: &[u8], value: &[u8], version: Version) -> StorageResult<bool> {
        self.inner.merge(key, value, version).await
    }

    async fn merge_tombstone(&self, key: &[u8], version: Version) -> StorageResult<bool> {
        self.inner.merge_tombstone(key, version).await
    }

    async fn merge_batch(&self, ops: Vec<MergeOp>) -> StorageResult<()> {
        if self.fail_merge_batch.load(Ordering::SeqCst) {
            if let Some(node) = self.node.get() {
                node.shutdown();
            }
            return Err(StorageError::Backend(
                "issue #834 regression: injected merge_batch failure".to_string(),
            ));
        }
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

    fn snapshot(&self) -> MemorySnapshot {
        self.inner.snapshot()
    }

    fn latest_version(&self) -> Version {
        self.inner.latest_version()
    }
}

fn single_node_with_faulty_engine(seed: u64) -> (Simulator, FaultKvNode, Arc<AtomicBool>) {
    let sim = Simulator::new(seed);
    let fail = Arc::new(AtomicBool::new(false));
    let cell: Arc<OnceLock<FaultKvNode>> = Arc::new(OnceLock::new());
    let engine = FaultyEngine::new(Arc::clone(&fail), Arc::clone(&cell));
    let node = RaftKvNode::start(sim.env(nid(0)), vec![nid(0)], engine);
    cell.set(node.clone())
        .unwrap_or_else(|_| panic!("cell already set"));
    (sim, node, fail)
}

/// Run `fut` to completion by spawning it on `env` and driving `sim` for up
/// to `budget`; returns `None` if it hasn't completed (e.g. because the node
/// halted mid-flight, exactly the scenario these tests induce). Mirrors
/// `tests/txn_single.rs`'s identical `drive` helper.
fn drive<T: Send + 'static>(
    sim: &mut Simulator,
    env: &SimEnv,
    budget: Duration,
    fut: impl std::future::Future<Output = T> + Send + 'static,
) -> Option<T> {
    let slot: Arc<std::sync::Mutex<Option<T>>> = Arc::new(std::sync::Mutex::new(None));
    let s = Arc::clone(&slot);
    env.clone().spawn_task(async move {
        let v = fut.await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock().unwrap().take()
}

/// The `Batch` arm now shares `flush_pending`'s halted-gated tolerance
/// (issue #834, defect 2): a `storage.merge_batch` failure while `halted`
/// exits the apply driver cleanly instead of hard-panicking via the
/// pre-fix `.expect("raftkv apply batch put")`.
#[test]
fn batch_apply_tolerates_a_halted_merge_batch_failure() {
    let seed = 0x834_8A7C;
    let (mut sim, node, fail) = single_node_with_faulty_engine(seed);
    sim.run_for(Duration::from_secs(2)); // single-node self-election
    assert!(
        node.is_leader(),
        "single node must self-elect (seed={seed})"
    );

    fail.store(true, Ordering::SeqCst);
    let puts: Vec<(Vec<u8>, Vec<u8>)> = (0..5)
        .map(|i| (format!("k{i}").into_bytes(), format!("v{i}").into_bytes()))
        .collect();
    match node.put_batch(puts) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("batch rejected: {other:?} (seed={seed})"),
    }

    // No panic here is the assertion: the apply task's own `flush_pending`
    // call hits the injected failure, observes `halted` already latched
    // (set by `FaultyEngine` itself, standing in for a genuine concurrent
    // `shutdown()`), and exits cleanly.
    sim.run_for(Duration::from_secs(2));
    assert!(
        node.is_stopped(),
        "apply driver must exit cleanly after the tolerated merge_batch failure (seed={seed})"
    );
}

/// The identical contract for `TxnResolve`'s commit branch (issue #834,
/// defect 2): a single-participant transaction's resolve, once it reaches
/// apply, now queues its commit write onto the shared `pending` run instead
/// of calling `storage.merge` directly — so a `halted` node tolerates the
/// same injected failure here too.
#[test]
fn txn_resolve_apply_tolerates_a_halted_merge_batch_failure() {
    let seed = 0x834_7E52;
    let (mut sim, node, fail) = single_node_with_faulty_engine(seed);
    sim.run_for(Duration::from_secs(2));
    assert!(
        node.is_leader(),
        "single node must self-elect (seed={seed})"
    );

    // Arm the fault *before* staging: `TxnStage`'s own intent write uses a
    // direct `storage.merge` (untouched by this issue, see `KvCommand::
    // TxnStage`'s doc), so staging still succeeds; only the resolve
    // (commit) branch below routes through `pending`/`merge_batch` and
    // hits the injected failure.
    fail.store(true, Ordering::SeqCst);
    let k = key(b"acct-1", b"balance");
    let n = node.clone();
    let env = node.env().clone();
    let _ = drive(&mut sim, &env, Duration::from_secs(4), async move {
        n.txn_write("t", vec![(k, Some(b"v".to_vec()))]).await
    });
    // The transaction itself may never confirm (the node halts mid-resolve
    // apply) — that's expected and not asserted on; what matters is that
    // the apply driver exits cleanly rather than panicking.
    sim.run_for(Duration::from_secs(2));
    assert!(
        node.is_stopped(),
        "apply driver must exit cleanly after the tolerated merge_batch failure (seed={seed})"
    );
}

// =======================================================================
// Part 2: the fsync-count property, over a real `LsmEngine` wrapped in
// `CountedEnv` (mirrors `benches/wal_fsync_bench.rs`'s own `CountedEnv`,
// which wraps `ProdEnv` for a wall-clock bench; this one wraps `SimEnv` so
// the count is a deterministic `cargo test` assertion instead).
// =======================================================================

/// Wraps a `SimEnv` and counts every `Disk::sync` call that goes through it.
/// Every other `Env` method is a plain pass-through.
#[derive(Clone)]
struct CountedEnv {
    inner: SimEnv,
    syncs: Arc<AtomicU64>,
}

impl CountedEnv {
    fn new(inner: SimEnv) -> Self {
        Self {
            inner,
            syncs: Arc::new(AtomicU64::new(0)),
        }
    }

    fn sync_count(&self) -> u64 {
        self.syncs.load(Ordering::SeqCst)
    }

    fn reset(&self) {
        self.syncs.store(0, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl Clock for CountedEnv {
    fn now(&self) -> Nanos {
        self.inner.now()
    }
    fn wall_now(&self) -> UnixMillis {
        self.inner.wall_now()
    }
    async fn sleep(&self, dur: Duration) {
        self.inner.sleep(dur).await
    }
}

impl Rng for CountedEnv {
    fn next_u64(&self) -> u64 {
        self.inner.next_u64()
    }
    fn fill_bytes(&self, dst: &mut [u8]) {
        self.inner.fill_bytes(dst)
    }
}

#[async_trait::async_trait]
impl Network for CountedEnv {
    async fn send_stream(&self, to: NodeId, stream: u64, payload: Vec<u8>) {
        self.inner.send_stream(to, stream, payload).await
    }
    async fn recv_stream(&self, stream: u64) -> Envelope {
        self.inner.recv_stream(stream).await
    }
}

#[async_trait::async_trait]
impl Disk for CountedEnv {
    async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.append(file, bytes).await
    }
    async fn sync(&self, file: &str) -> std::io::Result<()> {
        self.syncs.fetch_add(1, Ordering::SeqCst);
        self.inner.sync(file).await
    }
    async fn read(&self, file: &str) -> std::io::Result<Vec<u8>> {
        self.inner.read(file).await
    }
    async fn read_at(&self, file: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        self.inner.read_at(file, offset, len).await
    }
    async fn size(&self, file: &str) -> std::io::Result<u64> {
        self.inner.size(file).await
    }
    async fn remove(&self, file: &str) -> std::io::Result<()> {
        self.inner.remove(file).await
    }
    async fn replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.replace(file, bytes).await
    }
    async fn list(&self) -> std::io::Result<Vec<String>> {
        self.inner.list().await
    }
    async fn link(&self, src: &str, dst: &str) -> std::io::Result<()> {
        self.inner.link(src, dst).await
    }
}

impl Spawner for CountedEnv {
    fn spawn(&self, fut: BoxFuture<'static, ()>) {
        self.inner.spawn(fut)
    }
}

impl Env for CountedEnv {
    fn node_id(&self) -> NodeId {
        self.inner.node_id()
    }
    fn metrics(&self) -> MetricsHandle {
        self.inner.metrics()
    }
}

type LsmKvNode = RaftKvNode<CountedEnv, LsmEngine<CountedEnv>>;

/// No auto-compaction and a large flush threshold, mirroring
/// `tests/inplace_split_dead_space.rs`'s `no_compact_opts` — every `sync`
/// this test observes should come from the Raft WAL's own persist plus
/// exactly one storage-engine `merge_batch`, never a flush/compaction
/// triggered mid-test.
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

fn single_node_lsm(seed: u64, prefix: &str) -> (Simulator, LsmKvNode, CountedEnv) {
    let sim = Simulator::new(seed);
    let env = CountedEnv::new(sim.env(nid(0)));
    let engine = block_on(LsmEngine::open_with(env.clone(), prefix, no_compact_opts()))
        .unwrap_or_else(|e| panic!("open LsmEngine at {prefix:?}: {e}"));
    let node = RaftKvNode::start(env.clone(), vec![nid(0)], engine);
    (sim, node, env)
}

/// Issue #834, defect 1: applying one `Batch` entry performs a constant
/// number of `Disk::sync` calls regardless of how many keys it carries —
/// before this fix, the `Batch` arm called `storage.merge` once per key,
/// each a `LsmEngine::log_and_apply` round trip with its own `fsync`, so
/// this ratio would instead track the batch size 1:1.
#[test]
fn batch_apply_sync_count_does_not_scale_with_batch_size() {
    let seed = 0x834_5511;

    let (mut sim_small, node_small, env_small) = single_node_lsm(seed, "issue834-small-");
    sim_small.run_for(Duration::from_secs(2));
    assert!(node_small.is_leader(), "seed={seed}");
    env_small.reset();
    let small: Vec<(Vec<u8>, Vec<u8>)> = (0..5)
        .map(|i| {
            (
                format!("k{i:03}").into_bytes(),
                format!("v{i:03}").into_bytes(),
            )
        })
        .collect();
    match node_small.put_batch(small) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("5-key batch rejected: {other:?} (seed={seed})"),
    }
    sim_small.run_for(Duration::from_secs(2));
    let small_syncs = env_small.sync_count();

    let (mut sim_large, node_large, env_large) =
        single_node_lsm(seed.wrapping_add(1), "issue834-large-");
    sim_large.run_for(Duration::from_secs(2));
    assert!(node_large.is_leader(), "seed={seed}");
    env_large.reset();
    let large: Vec<(Vec<u8>, Vec<u8>)> = (0..50)
        .map(|i| {
            (
                format!("k{i:03}").into_bytes(),
                format!("v{i:03}").into_bytes(),
            )
        })
        .collect();
    match node_large.put_batch(large) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("50-key batch rejected: {other:?} (seed={seed})"),
    }
    sim_large.run_for(Duration::from_secs(2));
    let large_syncs = env_large.sync_count();

    assert_eq!(
        small_syncs, large_syncs,
        "applying one Batch entry's sync count must not scale with its key count \
         (5-key batch: {small_syncs} syncs, 50-key batch: {large_syncs} syncs, seed={seed})"
    );
    assert!(
        small_syncs <= 4,
        "expected O(1) syncs for one coalesced Batch apply (Raft WAL + one merge_batch), \
         got {small_syncs} (seed={seed})"
    );
}
