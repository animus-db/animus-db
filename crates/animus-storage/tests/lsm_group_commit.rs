//! WAL **group commit** (ADR 0008): concurrent writes coalesce into one shared
//! `fsync`, an ack still means durable, and a crash loses exactly the writes
//! whose group `fsync` had not completed.
//!
//! These run under the deterministic cooperative `SimEnv` executor, so the
//! batching is a pure function of the seed. The first test drives many writes as
//! **separate tasks on one engine** (the way the data-plane serve loop and
//! anti-entropy concurrently touch a replica's engine), which all become ready in
//! the same scheduler drain cycle and therefore coalesce. The crash test wraps the
//! env so a chosen `fsync` is interrupted (its bytes never reach durable storage),
//! modelling power loss mid-group-commit, and asserts the acked prefix survives
//! while the interrupted batch is lost as a unit.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use animus_env::{
    BoxFuture, Clock, Disk, Env, EnvExt, Envelope, Nanos, Network, NodeId, Rng, Spawner,
};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, MergeOp, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";

/// A large flush threshold so these tests exercise the WAL group-commit path
/// without an intervening flush truncating the WAL mid-test.
fn opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 1 << 20,
        compaction_trigger: 100,
        target_table_bytes: 1 << 20,
        level_fanout: 8,
        // Large so the WAL never rotates mid-test: these tests exercise the
        // group-commit path on one segment, not rotation.
        wal_segment_bytes: 1 << 20,
        tombstone_grace_versions: 1 << 20,
    }
}

/// Concurrent writes — issued as independent tasks on one shared engine — coalesce
/// into far fewer WAL `fsync`s than there are writes, while every write is still
/// durable on reopen after a crash.
#[test]
fn concurrent_writes_share_one_fsync() {
    let seed = 0x6C0117;
    let mut sim = Simulator::new(seed);
    let writes = 32u64;
    {
        let engine = block_on(LsmEngine::open_with(sim.env(0), PREFIX, opts())).expect("open");
        // Spawn each write as its own task so they are all ready in the same drain
        // cycle and batch together (the leader yields once, letting the rest enqueue
        // before it flushes). A shared counter lets us wait for completion.
        let done = Arc::new(AtomicU64::new(0));
        for i in 0..writes {
            let e = engine.clone();
            let done = Arc::clone(&done);
            sim.env(0).spawn_task(async move {
                let k = format!("k{i:03}");
                e.put(k.as_bytes(), format!("v{i}").as_bytes(), i + 1)
                    .await
                    .expect("put");
                done.fetch_add(1, Ordering::Relaxed);
            });
        }
        sim.run();
        assert_eq!(
            done.load(Ordering::Relaxed),
            writes,
            "seed={seed}: every spawned write completed"
        );

        // The whole point: many concurrent writes, far fewer batch fsyncs — here
        // they all land in the same drain cycle and coalesce into a single fsync.
        let syncs = engine.wal_batch_sync_count();
        assert!(
            syncs < writes,
            "seed={seed}: expected group commit to coalesce ({syncs} fsyncs for {writes} writes)"
        );
        assert!(
            syncs <= 2,
            "seed={seed}: concurrent writes should batch tightly ({syncs} fsyncs for {writes})"
        );
        assert!(syncs >= 1, "seed={seed}: at least one fsync happened");
    }

    // Crash + reopen: every acked write is durable (an ack means durable, even
    // though it shared its fsync with others).
    sim.crash(0);
    let engine = block_on(LsmEngine::open_with(sim.env(0), PREFIX, opts())).expect("reopen");
    block_on(async {
        for i in 0..writes {
            let k = format!("k{i:03}");
            assert_eq!(
                engine.get(k.as_bytes()).await.unwrap().unwrap().value,
                format!("v{i}").as_bytes(),
                "seed={seed}: acked write {i} lost across crash",
            );
        }
    });
}

/// `merge_batch` collapses a whole run of per-key LWW merges into **one** WAL
/// `fsync` (the leaderful-Raft apply-path win), preserves per-key LWW semantics
/// (including a loser that a newer version already holds, and same-key ordering
/// within the batch), and every applied op is durable across a crash.
#[test]
fn merge_batch_coalesces_one_fsync_and_is_durable() {
    let seed = 0x8A7C41;
    let sim = Simulator::new(seed);
    {
        let engine = block_on(LsmEngine::open_with(sim.env(0), PREFIX, opts())).expect("open");
        block_on(async {
            // Seed one key at version 5 so a later batch op at version 3 loses LWW.
            engine.merge(b"loser", b"old", 5).await.expect("seed");
            let syncs_before = engine.wal_batch_sync_count();

            let ops = vec![
                MergeOp::put(b"a".to_vec(), b"1".to_vec(), 10),
                MergeOp::put(b"b".to_vec(), b"2".to_vec(), 11),
                MergeOp::tombstone(b"a".to_vec(), 12), // same-key: newer wins → a deleted
                MergeOp::put(b"loser".to_vec(), b"stale".to_vec(), 3), // < 5 → dropped
                MergeOp::put(b"c".to_vec(), b"3".to_vec(), 13),
            ];
            engine.merge_batch(ops).await.expect("merge_batch");

            // The whole batch shared a single fsync (vs one per op).
            let syncs = engine.wal_batch_sync_count() - syncs_before;
            assert_eq!(
                syncs, 1,
                "seed={seed}: merge_batch must coalesce to one fsync, got {syncs}"
            );

            // Semantics: b/c applied, a tombstoned by the newer same-key op, loser
            // unchanged (its version-3 op lost to the existing version 5).
            assert_eq!(engine.get(b"b").await.unwrap().unwrap().value, b"2");
            assert_eq!(engine.get(b"c").await.unwrap().unwrap().value, b"3");
            assert_eq!(
                engine.get(b"a").await.unwrap(),
                None,
                "same-key LWW: newer tombstone wins"
            );
            assert_eq!(
                engine.get(b"loser").await.unwrap().unwrap().value,
                b"old",
                "below-latest version must not overwrite",
            );
        });
    }

    // Crash + reopen: every applied op is durable (an ack means durable).
    sim.crash(0);
    let engine = block_on(LsmEngine::open_with(sim.env(0), PREFIX, opts())).expect("reopen");
    block_on(async {
        assert_eq!(engine.get(b"b").await.unwrap().unwrap().value, b"2");
        assert_eq!(engine.get(b"c").await.unwrap().unwrap().value, b"3");
        assert_eq!(engine.get(b"a").await.unwrap(), None);
        assert_eq!(engine.get(b"loser").await.unwrap().unwrap().value, b"old");
    });
}

/// A crash *during a group `fsync`* loses exactly the interrupted batch: writes
/// whose `fsync` completed (acked) survive; writes in the interrupted batch are
/// lost as a unit (they were never acked and never made visible).
#[test]
fn crash_drops_unfsynced_batch_tail() {
    let seed = 0xBA7C4;
    let sim = Simulator::new(seed);
    // Interrupt the *second* group fsync: the first batch is durable (acked); the
    // second batch's append lands in the buffer but its fsync never persists.
    let env = CrashEnv::new(sim.env(0), 2);

    {
        let engine = block_on(LsmEngine::open_with(env.clone(), PREFIX, opts())).expect("open");
        block_on(async {
            // First write: its own group commit -> fsync #1 (durable, acked).
            engine.put(b"durable", b"1", 1).await.expect("first put");

            // Second write: fsync #2 is interrupted, so the batch's bytes never
            // reach durable storage. `commit` surfaces the failure, so the write is
            // *not* acked and *not* applied to the memtable.
            let err = engine.put(b"lost", b"2", 2).await;
            assert!(
                err.is_err(),
                "seed={seed}: a write whose fsync was interrupted must not ack"
            );
        });
    }
    // Power loss drops the buffered (un-synced) tail.
    sim.crash(0);

    // Reopen on the durable disk: the acked write survives; the interrupted one is
    // gone (lost as a unit, exactly the writes whose group fsync did not complete).
    let engine = block_on(LsmEngine::open_with(sim.env(0), PREFIX, opts())).expect("reopen");
    block_on(async {
        assert_eq!(
            engine.get(b"durable").await.unwrap().unwrap().value,
            b"1",
            "seed={seed}: acked write must survive the crash"
        );
        assert_eq!(
            engine.get(b"lost").await.unwrap(),
            None,
            "seed={seed}: the un-fsynced batch tail must be lost"
        );
    });
}

/// An `Env` wrapper that delegates everything to an inner [`SimEnv`] but interrupts
/// the `nth` `sync`: that sync neither persists the buffered bytes nor errors at the
/// disk — instead it leaves the bytes un-durable and returns an error, modelling a
/// power loss landing in the middle of an `fsync`. A later `Simulator::crash` then
/// drops the buffer for real.
#[derive(Clone)]
struct CrashEnv {
    inner: SimEnv,
    syncs: Arc<AtomicU64>,
    crash_on: u64,
}

impl CrashEnv {
    fn new(inner: SimEnv, crash_on: u64) -> Self {
        Self {
            inner,
            syncs: Arc::new(AtomicU64::new(0)),
            crash_on,
        }
    }
}

#[async_trait::async_trait]
impl Disk for CrashEnv {
    async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.append(file, bytes).await
    }
    async fn sync(&self, file: &str) -> std::io::Result<()> {
        let n = self.syncs.fetch_add(1, Ordering::Relaxed) + 1;
        if n == self.crash_on {
            // Interrupted mid-fsync: the bytes are NOT made durable, and the caller
            // sees an error so it does not treat the write as committed.
            return Err(std::io::Error::other("fsync interrupted by crash"));
        }
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
}

#[async_trait::async_trait]
impl Clock for CrashEnv {
    fn now(&self) -> Nanos {
        self.inner.now()
    }
    async fn sleep(&self, dur: Duration) {
        self.inner.sleep(dur).await;
    }
}

impl Rng for CrashEnv {
    fn next_u64(&self) -> u64 {
        self.inner.next_u64()
    }
    fn fill_bytes(&self, dst: &mut [u8]) {
        self.inner.fill_bytes(dst);
    }
}

#[async_trait::async_trait]
impl Network for CrashEnv {
    async fn send(&self, to: NodeId, payload: Vec<u8>) {
        self.inner.send(to, payload).await;
    }
    async fn recv(&self) -> Envelope {
        self.inner.recv().await
    }
}

impl Spawner for CrashEnv {
    fn spawn(&self, fut: BoxFuture<'static, ()>) {
        self.inner.spawn(fut);
    }
}

impl Env for CrashEnv {
    fn node_id(&self) -> NodeId {
        self.inner.node_id()
    }
}
