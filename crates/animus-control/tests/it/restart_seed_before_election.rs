//! Issue #1024 regression: the apply task's one-time startup seed
//! (`node.rs`'s `meta_apply_seed`) must publish `cache`/`engine_applied`/
//! `MetadataWatch` from the engine's own durable state **before** a
//! restarted node's consensus loop ever ticks its `RaftCore` — and therefore
//! before it can ever campaign or become leader (ADR 0038's 2026-09-21
//! amendment).
//!
//! Mechanism under test: `RaftCore::recovered` arms a real election deadline
//! 150-300ms out at the moment a restarted node's WAL replay finishes. A
//! single-voter node wins that election on its very next tick with no
//! network round trip at all. Before issue #1024's fix, the apply task's
//! startup seed (a full system-keyspace `entries()` scan, plus a watermark
//! read) was spawned fire-and-forget, racing that timer; on a real restart
//! under I/O contention the node could become leader before the scan
//! finished, serving `Metadata::default()`/watermark 0 as leader over
//! already-committed state. This test reproduces that race deterministically
//! by wrapping the restarted node's engine in [`SlowScan`], which
//! `env.sleep()`s well past the election window inside `entries()` — the
//! exact call `mirror::rebuild_metadata_from_engine` (the seed's rebuild
//! step) makes.
//!
//! Modeled on `tests/restart.rs::node_restarts_from_its_disk_and_rejoins`
//! for how a node is started/stopped/restarted on the same engine, and on
//! `tests/metadata_watch.rs` for the `MetadataWatch` API.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()`.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{MetaCommand, Metadata, NodeStatus, RaftNode};
use animus_env::{Env, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{
    Key, MemoryEngine, MergeOp, Result as StorageResult, StorageEngine, Value, Version,
    VersionedValue, WriteBatch,
};

/// How long `SlowScan::entries` sleeps — well past `RaftCore`'s 150-300ms
/// election window (`election_base` in `raft.rs`), so the pre-#1024 ordering
/// reliably loses the race (the node self-elects during the sleep, long
/// before it completes).
const SLOW_SCAN_DELAY: Duration = Duration::from_secs(2);

/// Upper bound on how long the test waits for the restarted node to become
/// leader at all — the *liveness* half of this regression: the fix must
/// delay ticking until the seed completes, never deadlock it.
const LEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// Granularity the test polls `is_leader()` at. Coarser than this risks
/// missing the FIRST instant leadership flips true — the whole point is to
/// catch the node serving as leader mid-seed, not some later, already-caught-
/// up moment.
const POLL_STEP: Duration = Duration::from_millis(2);

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

/// A `StorageEngine` wrapper that delegates every method to `inner`
/// unchanged, except `entries()` — the full system-keyspace scan
/// `mirror::rebuild_metadata_from_engine` performs as the apply task's
/// startup seed (`node.rs`'s `meta_apply_seed`) — which additionally
/// `env.sleep()`s `delay` of `SimEnv` virtual time first. Stands in for a
/// real engine scan taking a long time under I/O contention (issue #1024:
/// a CI runner's ~267 tests sharing one disk).
#[derive(Clone)]
struct SlowScan<E: Env, S: StorageEngine> {
    inner: S,
    env: E,
    delay: Duration,
}

impl<E: Env, S: StorageEngine> SlowScan<E, S> {
    fn new(inner: S, env: E, delay: Duration) -> Self {
        Self { inner, env, delay }
    }
}

#[async_trait::async_trait]
impl<E: Env, S: StorageEngine> StorageEngine for SlowScan<E, S> {
    type Snapshot = S::Snapshot;

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

    /// The one delayed method: `mirror::rebuild_metadata_from_engine` — the
    /// apply task's startup-seed rebuild step, and the sole caller this
    /// regression cares about — calls exactly this.
    async fn entries(&self) -> StorageResult<Vec<(Key, VersionedValue)>> {
        self.env.sleep(self.delay).await;
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

/// Red on the pre-#1024 ordering (`meta_apply_loop`'s seed spawned
/// fire-and-forget, racing the recovered core's own tick loop); green once
/// the seed runs inline in `drive` before the first tick (the shipped fix).
#[test]
fn restarted_node_never_leads_before_its_apply_seed_publishes_durable_state() {
    let seed = 0x1024_5EED;
    let mut sim = Simulator::new(seed);

    let engine = MemoryEngine::new();
    let node: RaftNode<SimEnv> = RaftNode::start(sim.env(nid(0)), vec![nid(0)], engine.clone());

    // A single voter self-elects on its very first tick — no peers, no
    // network round trip needed (`RaftCore::start_pre_vote`'s "already a
    // majority" shortcut).
    sim.run_for(Duration::from_secs(2));
    assert!(
        node.is_leader(),
        "test setup: single voter never self-elected before restart (seed={seed:#x})"
    );

    // Commit real, durable state so there is something to check for after
    // restart.
    for id in 0..5 {
        node.propose(upsert(id));
    }
    sim.run_for(Duration::from_secs(2));

    let pre_watermark = node.metadata_watch().latest();
    let pre_applied = node.engine_applied_index();
    let pre_metadata: Metadata = node.metadata();
    assert!(
        pre_watermark > 0 && pre_applied > 0,
        "test setup: expected nonzero committed watermark/applied index \
         before restart (seed={seed:#x}, pre_watermark={pre_watermark}, \
         pre_applied={pre_applied})"
    );
    assert_eq!(
        pre_metadata.members.len(),
        5,
        "test setup: expected 5 committed members before restart \
         (seed={seed:#x}, members={:?})",
        pre_metadata.members.keys().collect::<Vec<_>>()
    );

    // Stop the process: tasks + volatile state gone, WAL + engine on disk
    // survive — mirrors `restart.rs::node_restarts_from_its_disk_and_rejoins`.
    sim.stop(nid(0));

    // Restart on the SAME durable engine, wrapped so its apply task's
    // startup seed (the `entries()` scan) takes `SLOW_SCAN_DELAY` of virtual
    // time — well past the recovered core's own already-armed 150-300ms
    // election deadline.
    let restart_env = sim.env(nid(0));
    let slow_engine = SlowScan::new(engine.clone(), restart_env.clone(), SLOW_SCAN_DELAY);
    let node: RaftNode<SimEnv> = RaftNode::start(restart_env, vec![nid(0)], slow_engine);

    // Poll for the FIRST instant this node becomes leader, at fine
    // granularity, and check the invariant immediately — not after some
    // generous settle window, which would hide exactly the race this test
    // exists to catch.
    let mut elapsed = Duration::ZERO;
    while !node.is_leader() {
        assert!(
            elapsed < LEADER_TIMEOUT,
            "liveness: node never became leader within {LEADER_TIMEOUT:?} \
             after restart (seed={seed:#x}) — the issue #1024 fix must \
             delay ticking until the seed completes, never deadlock boot"
        );
        sim.run_for(POLL_STEP);
        elapsed += POLL_STEP;
    }

    // The core regression assertions (issue #1024): at the very first
    // instant this node is leader, its durable state must already be
    // published — never regressed relative to what was committed before the
    // restart.
    let post_watermark = node.metadata_watch().latest();
    let post_applied = node.engine_applied_index();
    let post_metadata: Metadata = node.metadata();
    assert!(
        post_watermark >= pre_watermark,
        "issue #1024: post-restart watermark ({post_watermark}) regressed \
         below the pre-restart watermark ({pre_watermark}) at the first \
         instant this node was leader (seed={seed:#x}, elapsed={elapsed:?}) \
         — a node must never become leader before its apply task's startup \
         seed has published the durable watermark"
    );
    assert!(
        post_applied >= pre_applied,
        "issue #1024: post-restart engine_applied_index ({post_applied}) \
         regressed below the pre-restart one ({pre_applied}) at the first \
         instant this node was leader (seed={seed:#x}, elapsed={elapsed:?})"
    );
    assert_eq!(
        post_metadata,
        pre_metadata,
        "issue #1024: metadata at the first instant this node was leader \
         after restart did not match the pre-restart committed state \
         (seed={seed:#x}, elapsed={elapsed:?}): got {} members, expected {} \
         members",
        post_metadata.members.len(),
        pre_metadata.members.len()
    );

    // Liveness, restated as a direct timing check: leadership must not have
    // preceded the seed's own delay — if it did, the node became leader
    // while the scan (and therefore the publish) was still in flight, which
    // is the exact bug this test exists to catch, independent of whether
    // the assertions above happened to also catch it.
    assert!(
        elapsed >= SLOW_SCAN_DELAY,
        "issue #1024: node became leader BEFORE its startup seed's engine \
         scan could possibly have completed (elapsed={elapsed:?} < seed \
         delay={SLOW_SCAN_DELAY:?}, seed={seed:#x}) — a node must never tick \
         its core, and so never campaign or become leader, before the apply \
         task's startup seed has published the durable Metadata"
    );
}
