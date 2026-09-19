//! Issue #939: the `Freeze` and `SplitTablet` apply arms' own whole-range
//! **seal marker** writes (`storage.merge(&marker_key, ..)`) used to be a
//! bare `.expect(..)` with no `halted` gate — the same class of
//! teardown-artifact hard panic `flush_pending`/the WAL-compaction
//! `replace` path already tolerate (issue #278 item 1 and its follow-up),
//! just on a call those two fixes didn't reach. `merge_seal_marker_or_halted`
//! now applies the identical halted-gated tolerance to both sites.
//!
//! Mirrors `tests/batch_txn_resolve_apply_fault.rs`'s own `FaultyEngine`
//! pattern (issue #834): a `MemoryEngine` wrapper whose `merge` can be armed
//! to fail and calls `shutdown()` on the node **before** returning the
//! injected error — the deterministic, single-threaded-cooperative-scheduler
//! stand-in for a genuine concurrent `shutdown()` racing this in-flight
//! write (`MemoryEngine`'s async methods never really suspend, so there is
//! no `.await` point for a separately-scheduled task to interleave at; see
//! that file's own doc for the full argument for why this achieves the
//! identical "halted is already true by the time the error is observed"
//! contract as a genuine race).
//!
//! Both `Freeze` and `SplitTablet` route only ONE `storage.merge` call
//! through this fault before this regression's fix would panic — `Freeze`'s
//! own single seal marker write, and `SplitTablet`'s *first* `merge` (its
//! own identical seal marker; the second, fork-payload marker is never
//! reached once the seal marker itself is tolerated as failed) — so arming
//! the very first `merge` call deterministically hits the exact site this
//! issue is about, with no other `merge` call able to land first in either
//! scenario (a bare `propose_freeze`/`propose_split_tablet` with no prior
//! writes never populates the `pending` run `flush_pending`'s own
//! `merge_batch` would otherwise drain first).
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{
    Key, MemoryEngine, MemorySnapshot, MergeOp, Result as StorageResult, StorageEngine,
    StorageError, Value, Version, VersionedValue, WriteBatch,
};
use animus_tablet::{SplitChild, TabletId};
use futures::executor::block_on;

type FaultKvNode = RaftKvNode<SimEnv, FaultyEngine>;

/// A `MemoryEngine` wrapper whose `merge` can be armed to fail on its very
/// next call — standing in for a real `LsmEngine`'s `merge` hitting a disk
/// fault, without needing real I/O. See this file's module doc for why
/// arming the *first* `merge` call deterministically targets the seal
/// marker write in both scenarios this file exercises.
#[derive(Clone)]
struct FaultyEngine {
    inner: MemoryEngine,
    fail_next_merge: Arc<AtomicBool>,
    // Set once, right after the node it's wired into is constructed (see
    // `single_node_with_faulty_engine`). When the injected failure fires,
    // `merge` calls `shutdown()` on it *before* returning the error — see
    // this file's module doc.
    node: Arc<OnceLock<FaultKvNode>>,
}

impl FaultyEngine {
    fn new(fail_next_merge: Arc<AtomicBool>, node: Arc<OnceLock<FaultKvNode>>) -> Self {
        Self {
            inner: MemoryEngine::new(),
            fail_next_merge,
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
        // `swap` so a racing second call (there isn't one in these
        // single-node, single-command scenarios, but this keeps the
        // "exactly the next call" contract airtight) can't double-fire.
        if self.fail_next_merge.swap(false, Ordering::SeqCst) {
            if let Some(node) = self.node.get() {
                node.shutdown();
            }
            return Err(StorageError::Backend(
                "issue #939 regression: injected seal-marker merge failure".to_string(),
            ));
        }
        self.inner.merge(key, value, version).await
    }

    async fn merge_tombstone(&self, key: &[u8], version: Version) -> StorageResult<bool> {
        self.inner.merge_tombstone(key, version).await
    }

    async fn merge_batch(&self, ops: Vec<MergeOp>) -> StorageResult<()> {
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

fn test_children() -> [SplitChild; 2] {
    [
        SplitChild {
            id: TabletId(2),
            replicas: vec![nid(10), nid(11), nid(12)],
        },
        SplitChild {
            id: TabletId(3),
            replicas: vec![nid(13), nid(14), nid(15)],
        },
    ]
}

/// `Freeze`'s own seal-marker write now shares `flush_pending`'s
/// halted-gated tolerance (issue #939): a `storage.merge` failure while
/// `halted` exits the apply driver cleanly instead of hard-panicking via
/// the pre-fix bare `.expect("raftkv apply freeze marker")`, and — the
/// property that fix alone would NOT have proven — the group must not be
/// left claiming it froze when the marker never actually became durable.
#[test]
fn freeze_apply_tolerates_a_halted_seal_marker_merge_failure() {
    let seed = 0x939_F0E2;
    let (mut sim, node, fail) = single_node_with_faulty_engine(seed);
    sim.run_for(Duration::from_secs(2)); // single-node self-election
    assert!(
        node.is_leader(),
        "single node must self-elect (seed={seed})"
    );

    fail.store(true, Ordering::SeqCst);
    match node.propose_freeze() {
        ProposeResult::Accepted { .. } => {}
        other => panic!("freeze rejected: {other:?} (seed={seed})"),
    }

    // No panic here is the primary assertion: the apply task's own seal
    // marker merge hits the injected failure, observes `halted` already
    // latched (set by `FaultyEngine` itself, standing in for a genuine
    // concurrent `shutdown()`), and tolerates it instead of panicking.
    sim.run_for(Duration::from_secs(2));
    assert!(
        node.is_stopped(),
        "apply driver must exit cleanly after the tolerated seal-marker merge failure \
         (seed={seed})"
    );
    // The property a bare halted-tolerant `.expect` swap would NOT prove:
    // a marker that never became durable must not be treated as if it had
    // — `frozen` stays unlatched.
    assert!(
        !node.is_frozen(),
        "a tolerated (never-durable) freeze marker must not latch `frozen` (seed={seed})"
    );
}

/// The identical contract for `SplitTablet`'s own seal marker (issue #939,
/// the same idiom as `Freeze` above): a `storage.merge` failure while
/// `halted` must not latch `frozen`, and — specific to this command — must
/// not go on to write the fork-specific payload either (`pending_split()`
/// stays `None`).
#[test]
fn split_tablet_apply_tolerates_a_halted_seal_marker_merge_failure() {
    let seed = 0x939_5717;
    let (mut sim, node, fail) = single_node_with_faulty_engine(seed);
    sim.run_for(Duration::from_secs(2));
    assert!(
        node.is_leader(),
        "single node must self-elect (seed={seed})"
    );

    fail.store(true, Ordering::SeqCst);
    match node.propose_split_tablet(b"m".to_vec(), test_children()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("split-tablet rejected: {other:?} (seed={seed})"),
    }

    sim.run_for(Duration::from_secs(2));
    assert!(
        node.is_stopped(),
        "apply driver must exit cleanly after the tolerated seal-marker merge failure \
         (seed={seed})"
    );
    assert!(
        !node.is_frozen(),
        "a tolerated (never-durable) split seal marker must not latch `frozen` (seed={seed})"
    );
    assert_eq!(
        block_on(node.pending_split()),
        None,
        "a tolerated (never-durable) seal marker must short-circuit before the fork payload \
         is ever written (seed={seed})"
    );
}
