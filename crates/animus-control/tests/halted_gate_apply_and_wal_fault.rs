//! Issue #939 (control-plane half): the apply task's system-keyspace
//! `merge_batch` write and the consensus loop's own WAL append/sync had no
//! `halted` gate at all — an I/O failure racing this node's own teardown
//! panicked unconditionally, indistinguishable from a genuine durability
//! fault. `RaftNode` now carries the same one-way `halted` latch
//! `animus-cp-data::RaftKvNode` does (ADR 0038's 2026-09-19 amendment):
//! `meta_apply_and_compact`'s system-keyspace write and `persist_wal`'s WAL
//! append/sync tolerate a failure only while it is set — see
//! `crates/animus-control/CLAUDE.md`'s matching entry.
//!
//! Part 1 mirrors `animus-cp-data/tests/batch_txn_resolve_apply_fault.rs`'s
//! `FaultyEngine` idiom exactly: `MemoryEngine`'s own `merge`/`merge_batch`
//! never return `Err`, so a wrapper armed to fail on demand is the only
//! deterministic way to reach the apply task's halted-gated tolerance
//! branch. Part 2 exercises `persist_wal`'s own WAL append/sync gate the
//! same way, via a real (deterministic) `DiskConfig` fault on the node's
//! own `SimEnv` disk — the CLAUDE.md-documented "never point `DiskConfig`
//! at a live node's disk in this crate's tests" warning describes exactly
//! the pre-fix panic these tests prove is now tolerated only while halted.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use animus_control::raft::ProposeResult;
use animus_control::{ColumnType, MetaCommand, RaftNode, TableSchema};
use animus_env::nid;
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{
    Key, MemoryEngine, MemorySnapshot, MergeOp, Result as StorageResult, StorageEngine,
    StorageError, Value, Version, VersionedValue, WriteBatch,
};

type FaultNode = RaftNode<SimEnv>;

fn schema(name: &str) -> MetaCommand {
    MetaCommand::CreateTableSchema {
        table: name.to_string(),
        schema: TableSchema::simple("id", ColumnType::Uuid),
    }
}

// =======================================================================
// Part 1: the apply task's "system-keyspace apply write" halted gate
// (`meta_apply_and_compact`), over a `FaultyEngine` wrapping `MemoryEngine`.
// =======================================================================

/// A `MemoryEngine` wrapper whose `merge_batch` can be armed to fail —
/// standing in for a real `LsmEngine`'s `merge_batch` hitting a disk fault,
/// without needing real I/O. Mirrors `animus-cp-data`'s identically-shaped
/// `FaultyEngine` (`tests/batch_txn_resolve_apply_fault.rs`) exactly.
#[derive(Clone)]
struct FaultyEngine {
    inner: MemoryEngine,
    fail_merge_batch: Arc<AtomicBool>,
    /// Set once, right after the node it's wired into is constructed (see
    /// `single_node_with_faulty_engine`). When the injected failure fires
    /// and `halt_first` is true, `merge_batch` calls `halt()` on it
    /// *before* returning the error — the deterministic,
    /// single-threaded-cooperative-scheduler stand-in for a genuine
    /// concurrent `Node::shutdown()` racing this in-flight apply write
    /// (`MemoryEngine`'s async methods never really suspend, so there is no
    /// `.await` point for a separately-scheduled task to interleave at;
    /// setting the flag in-line achieves the identical "halted is already
    /// true by the time the error is observed" contract).
    node: Arc<OnceLock<FaultNode>>,
    halt_first: bool,
}

impl FaultyEngine {
    fn new(
        fail_merge_batch: Arc<AtomicBool>,
        node: Arc<OnceLock<FaultNode>>,
        halt_first: bool,
    ) -> Self {
        Self {
            inner: MemoryEngine::new(),
            fail_merge_batch,
            node,
            halt_first,
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
            if self.halt_first
                && let Some(node) = self.node.get()
            {
                node.halt();
            }
            return Err(StorageError::Backend(
                "issue #939 regression: injected system-keyspace merge_batch failure".to_string(),
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

fn single_node_with_faulty_engine(
    seed: u64,
    halt_first: bool,
) -> (Simulator, FaultNode, Arc<AtomicBool>) {
    let sim = Simulator::new(seed);
    let fail = Arc::new(AtomicBool::new(false));
    let cell: Arc<OnceLock<FaultNode>> = Arc::new(OnceLock::new());
    let engine = FaultyEngine::new(Arc::clone(&fail), Arc::clone(&cell), halt_first);
    let node = RaftNode::start(sim.env(nid(0)), vec![nid(0)], engine);
    cell.set(node.clone())
        .unwrap_or_else(|_| panic!("cell already set"));
    (sim, node, fail)
}

/// Red-before/green-after (issue #939): with `halt()` latched first (here,
/// by `FaultyEngine` itself at the moment the fault fires, standing in for a
/// genuine concurrent `Node::shutdown()`), the apply task's own
/// `meta_apply_and_compact` must tolerate the injected `merge_batch`
/// failure — no panic — and must not have advanced anything on the tolerated
/// pass: `engine_applied_index()` stays exactly where it was before the
/// failing proposal (the election no-op's own index), never the failed
/// proposal's.
#[test]
fn apply_task_tolerates_a_halted_system_keyspace_merge_batch_failure() {
    let seed = 0x939_A17;
    let (mut sim, node, fail) = single_node_with_faulty_engine(seed, /* halt_first */ true);
    sim.run_for(Duration::from_secs(2)); // single-node self-election
    assert!(
        node.is_leader(),
        "single node must self-elect (seed={seed})"
    );
    let before = node.engine_applied_index();

    fail.store(true, Ordering::SeqCst);
    match node.propose(schema("t0")) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("proposal rejected: {other:?} (seed={seed})"),
    }

    // No panic here is the assertion: the apply task's own
    // `meta_apply_and_compact` hits the injected failure, observes `halted`
    // already latched (set by `FaultyEngine` itself), and tolerates it.
    sim.run_for(Duration::from_secs(2));
    assert!(
        node.is_halted(),
        "the fault handler must have latched halted before returning the error (seed={seed})"
    );
    assert_eq!(
        node.engine_applied_index(),
        before,
        "a tolerated failure must not advance the engine watermark (seed={seed})"
    );
    assert!(
        node.metadata().schemas.get("t0").is_none(),
        "a tolerated failure must not publish the failed proposal into the cache (seed={seed})"
    );
}

/// The identical fault, but WITHOUT `halted` latched first: a live
/// durability fault stays exactly as loud as before this fix — a hard
/// panic naming what failed, never a swallowed error (this is what makes
/// this test the "before" half of the previous test's "after": both target
/// the identical `merge_batch` call, only `halted` differs).
#[test]
#[should_panic(expected = "system-keyspace apply write failed while running")]
fn apply_task_panics_on_a_live_system_keyspace_merge_batch_failure() {
    let seed = 0x939_B17;
    let (mut sim, node, fail) = single_node_with_faulty_engine(seed, /* halt_first */ false);
    sim.run_for(Duration::from_secs(2));
    assert!(
        node.is_leader(),
        "single node must self-elect (seed={seed})"
    );

    fail.store(true, Ordering::SeqCst);
    match node.propose(schema("t0")) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("proposal rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
}

// =======================================================================
// Part 2: the consensus loop's own `persist_wal` WAL append/sync halted
// gate, over a real `DiskConfig` fault on the node's own `SimEnv` disk.
// =======================================================================

fn single_node_over_memory(seed: u64) -> (Simulator, FaultNode) {
    let sim = Simulator::new(seed);
    let node = RaftNode::start(sim.env(nid(0)), vec![nid(0)], MemoryEngine::new());
    (sim, node)
}

fn always_fail_disk() -> DiskConfig {
    let mut cfg = DiskConfig::default();
    cfg.set_error_prob(1.0);
    cfg
}

/// Red-before/green-after (issue #939): with `halt()` latched first, a
/// `DiskConfig` fault that fails every subsequent `append`/`sync` on this
/// node's own disk must not panic the consensus loop's `persist_wal` — the
/// pre-fix code hard-`.expect()`ed both calls unconditionally, live or
/// shutting down (see `crates/animus-control/CLAUDE.md`'s "never point
/// `DiskConfig` at a live node's disk in this crate's tests" note, which
/// this fix is what makes safe to do once `halt()` has been called first).
#[test]
fn persist_wal_tolerates_a_halted_wal_append_failure() {
    let seed = 0x939_C17;
    let (mut sim, node) = single_node_over_memory(seed);
    sim.run_for(Duration::from_secs(2)); // single-node self-election
    assert!(
        node.is_leader(),
        "single node must self-elect (seed={seed})"
    );

    node.halt();
    sim.set_disk_config_for(nid(0), always_fail_disk());

    match node.propose(schema("t0")) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("proposal rejected: {other:?} (seed={seed})"),
    }

    // No panic here is the assertion: `persist_wal`'s own `env.append`
    // fails immediately (every disk op is armed to fail), observes
    // `halted` already latched, and returns early instead of panicking.
    sim.run_for(Duration::from_secs(2));
}

/// The identical fault, but WITHOUT `halted` latched first: stays a hard
/// panic naming what failed.
#[test]
#[should_panic(expected = "wal append failed while running")]
fn persist_wal_panics_on_a_live_wal_append_failure() {
    let seed = 0x939_D17;
    let (mut sim, node) = single_node_over_memory(seed);
    sim.run_for(Duration::from_secs(2));
    assert!(
        node.is_leader(),
        "single node must self-elect (seed={seed})"
    );

    sim.set_disk_config_for(nid(0), always_fail_disk());

    match node.propose(schema("t0")) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("proposal rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));
}
