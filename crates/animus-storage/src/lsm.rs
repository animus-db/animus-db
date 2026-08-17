//! `LsmEngine`: a real on-disk log-structured merge tree implementing the async
//! [`StorageEngine`] trait, doing **all** its I/O through the `Env` [`Disk`] seam
//! so it is deterministically crash-testable under simulation (ADR 0003, 0008).
//!
//! This is the "custom engine" of ADR 0008: rather than *borrow* a third-party
//! LSM, `LsmEngine` *is* the LSM, written against the same `StorageEngine` trait
//! and the same MVCC contract as [`MemoryEngine`] (it is observationally
//! identical to `MemoryEngine`), but built over the `Env` seam so its
//! crash-recovery is deterministically simulation-tested — which a borrowed
//! engine doing its own real I/O cannot offer.
//!
//! [`MemoryEngine`]: crate::MemoryEngine
//! [`Disk`]: animus_env::Disk
//!
//! ## On-disk layout
//!
//! All files live in one `Env`-disk namespace (the disk is already node-scoped;
//! we use a filename prefix so several engines can share one node's disk):
//!
//! - `<prefix>MANIFEST` — the durable source of truth: the ordered list of live
//!   SSTable files plus per-table metadata (key range, version range, index
//!   offset/len, size), the engine's monotonic `max_version`, and the **live WAL
//!   segment numbers**. Written **atomically** via [`Disk::replace`], so a crash
//!   sees either the whole old or whole new manifest, never a mix.
//! - `<prefix>wal-NNNNNN` — the write-ahead log, split into **numbered segments**
//!   of [`WalRecord`]s, each one framed `len(u32) | crc32(u32) | payload` (a
//!   compact hand-rolled binary encoding, not JSON — see the codec docs above
//!   `encode_wal`), `append`ed then `sync`ed **before** a write is acknowledged
//!   (an ack means durable). The group-commit coordinator appends to the active
//!   segment and rolls to a fresh one once it passes a byte threshold; a flush
//!   removes whole segments it has folded into an SSTable (see [`wal`]). Holds
//!   the writes not yet folded into an SSTable. Recovery tolerates a torn
//!   trailing frame (an un-synced write cut short by a crash) but treats any
//!   other malformed/corrupt frame as a hard error (see `decode_wal`), and
//!   truncates a recovered active segment's torn tail before further appends
//!   ride it (see `LsmEngine::open_with_metrics`).
//! - `<prefix>sst-NNNNNN` — immutable, sorted SSTables (see [`sstable`]).
//!
//! ## Write path
//!
//! Every mutation: serialize a [`WalRecord`], `append` + `sync` it to the active
//! WAL segment (durable first, via group commit), then apply it to the in-memory
//! memtable (a `BTreeMap` MVCC store, the same shape as [`MemoryEngine`]'s). When
//! the memtable crosses a size threshold it is **flushed** to a fresh SSTable, the
//! manifest is atomically swapped to add that table and record the surviving WAL
//! segments, and the WAL segments the flush fully covered are then `remove`d.
//!
//! ## Read path
//!
//! Reads merge the memtable (newest) with the live SSTables newest→oldest by
//! MVCC version: for a key the greatest version `≤` the query version wins, and a
//! tombstone at that version hides older values. SSTable lookups fetch only the
//! relevant block via [`Disk::read_at`] (guided by the in-memory per-table block
//! index), never the whole file. Two gates skip a table before any block read:
//! the per-table key range, and a per-table **Bloom filter** over the table's
//! keys (so a point-miss inside the key range still reads no block when the
//! Bloom proves the key absent).
//!
//! ## Compaction (leveled)
//!
//! Tables carry a **level**. **L0** holds freshly flushed tables, whose key
//! ranges may overlap. **L1+** hold *non-overlapping* runs: at most one table per
//! level can contain a given key, so read amplification is bounded by the number
//! of levels rather than the total table count.
//!
//! - **L0→L1**: once `compaction_trigger` L0 tables accumulate, all L0 tables are
//!   merged with every L1 table their combined key range overlaps, and the result
//!   is re-partitioned into non-overlapping L1 runs (each ≈`target_table_bytes`).
//! - **Ln→L(n+1)** (n≥1): when a level exceeds its table budget
//!   (`L1_TABLE_BUDGET * level_fanout^(n-1)`), its tables are merged with the
//!   overlapping L(n+1) tables and re-partitioned into L(n+1).
//!
//! Every distinct `(key, version)` record is preserved across a compaction (full
//! MVCC history, tombstones included), so the merged view of all live tables plus
//! the memtable stays observationally identical to [`MemoryEngine`]. The manifest
//! swap remains the single linearization point.
//!
//! ## Concurrency
//!
//! Writers coordinate through WAL group commit and the brief `Inner` lock only.
//! **Flushes and compactions (maintenance) are mutually exclusive** via an async
//! [`MaintenanceLock`] held across the whole operation: both allocate SSTable
//! sequence numbers from `manifest.next_seq` (only advanced at the final swap)
//! and both swap the manifest + readers, so an overlap — e.g. an admin
//! `flush_now`/`compact_now` racing the write path's `maybe_flush_and_compact`
//! from another task — would duplicate seqs and clobber manifests. A flush
//! clears the memtable **surgically** (only the exact `(key, version)` slots its
//! snapshot folded into the SSTable), so a write applied concurrently with the
//! SSTable build is never erased; and it re-checks `applies_in_flight == 0`
//! atomically with its snapshot + WAL-watermark sample, so a WAL segment is only
//! GC'd when every durable record it holds is provably in the new SSTable.
//!
//! ## Crash safety
//!
//! The manifest swap (`Disk::replace`) is the single linearization point.
//!
//! - **Mid-flush crash** (new SSTable's bytes written but not yet referenced by
//!   the manifest, or written-and-synced but the manifest swap not done): on
//!   reopen the manifest still names the *old* set and the WAL segments are intact,
//!   so the memtable is rebuilt from the WAL and nothing is lost. An orphan SSTable
//!   file not named by the manifest is simply ignored (and overwritten by the next
//!   flush, which reuses the next sequence number derived from the manifest).
//! - **Mid-compaction crash** (the merged output table(s) written but manifest
//!   not yet swapped): the manifest still names the old inputs, which are all
//!   intact (compaction only `remove`s them *after* the swap), so reads see the
//!   old set; the orphan output files (at seqs beyond the manifest's `next_seq`)
//!   are ignored. No torn-table read is possible because a
//!   table is only ever read once it is named by a synced manifest.
//! - **Mid-rotation / mid-WAL-GC crash**: a WAL segment is removed only *after* a
//!   manifest that no longer names it is durable. A crash before the swap recovers
//!   a manifest still naming the segment (intact on disk) and replays it; a crash
//!   after the swap but before the `remove` leaves an orphan segment file below the
//!   live set that recovery ignores (its records are already in the SSTable).
//!   Recovery also replays any segment file present beyond the manifest's highest
//!   recorded segment — those carry acked writes made after the last flush — so an
//!   un-flushed segment is never lost. A new segment's first record is `sync`ed
//!   before its write is acked, so a half-created (un-synced) segment a crash drops
//!   carried no ack.
//!
//! These properties are argued here and exercised in `tests/lsm_crash.rs`.

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use animus_env::{Env, EnvExt, Metric, MetricsHandle};
use serde::{Deserialize, Serialize};

use crate::{
    Key, MergeOp, Result, Snapshot, StorageEngine, StorageError, Value, Version, VersionedValue,
    WriteBatch, WriteOp,
};

mod bloom;
mod sstable;
mod wal;

use bloom::BloomFilter;
use sstable::{Record, SsTableMeta, SsTableReader, SsTableWriter};
use wal::GroupCommit;

/// Default memtable flush threshold: total bytes of buffered key+value data.
const DEFAULT_FLUSH_BYTES: usize = 64 * 1024;
/// Default L0 compaction trigger: number of L0 (flush-tier) SSTables that forces
/// an L0→L1 compaction.
const DEFAULT_COMPACTION_TRIGGER: usize = 4;
/// Default soft byte budget for one output SSTable when partitioning a leveled
/// compaction into non-overlapping runs.
const DEFAULT_TARGET_TABLE_BYTES: usize = 2 * 1024 * 1024;
/// Default fanout: level `n` (n≥1) holds up to `base * fanout^(n-1)` tables
/// before it is compacted down into level `n+1`.
const DEFAULT_LEVEL_FANOUT: usize = 4;
/// Base table budget for L1 (multiplied by the fanout for deeper levels).
const L1_TABLE_BUDGET: usize = 4;
/// Default WAL segment size budget: once the active segment passes this many
/// bytes, the next group-commit batch rotates to a fresh segment file. Sized near
/// the default flush threshold so a flush usually covers and removes one or more
/// whole segments.
const DEFAULT_WAL_SEGMENT_BYTES: u64 = 64 * 1024;
/// Default tombstone GC grace, in **versions**: a tombstone (and the versions it
/// shadows) is only reclaimed during compaction once it sits below
/// `max_version - this`, so any historical read within the most-recent
/// `this`-versions window is unaffected. Sized generously by default so GC is a
/// no-op under ordinary use; the data plane / tests lower it to reclaim sooner.
const DEFAULT_TOMBSTONE_GRACE_VERSIONS: Version = 1 << 20;
/// Max times a read re-snapshots + retries when a **concurrent compaction**
/// removed an SSTable file mid-read. Reads snapshot the reader set under a brief
/// lock then fetch blocks lock-free; a compaction swaps the readers and then
/// `remove`s the superseded files, so a lock-free read of a just-removed file gets
/// an empty (short) read. Retrying against the fresh post-compaction readers — the
/// merged tables hold the same data — makes the read consistent instead of a
/// spurious "short sstable block read". Reads are sub-second and compactions are
/// paced, so a couple of retries always suffice; the bound only guards against a
/// genuine, persistent read error (which is then surfaced, not retried forever).
const READ_COMPACTION_RETRIES: u32 = 32;
/// `LsmOptions::background_maintenance` write backpressure: how far the
/// memtable may grow past `flush_threshold_bytes` before a writer must wait
/// for the background flush/compaction to catch up. Sized generously so a
/// normal write burst never blocks; it exists only to bound worst-case memory
/// growth if maintenance falls behind (a slow disk, a burst far exceeding
/// flush throughput, or repeated injected faults).
const BACKPRESSURE_OVERSHOOT_FACTOR: usize = 4;
/// How long a backpressured writer waits between polls of the memtable size.
/// `env.sleep` keeps this deterministic under `SimEnv`.
const BACKPRESSURE_POLL: Duration = Duration::from_millis(1);
/// Bound on backpressure poll iterations, in case maintenance is persistently
/// failing (e.g. a disk that never recovers) — turns what would otherwise be
/// an unbounded retry loop into a clean, loud error.
const BACKPRESSURE_MAX_POLLS: u32 = 10_000;

/// Tuning knobs for an [`LsmEngine`]. Defaults are sized for tests; production
/// wiring can raise them.
#[derive(Clone, Copy, Debug)]
pub struct LsmOptions {
    /// Flush the memtable once its buffered key+value bytes exceed this.
    pub flush_threshold_bytes: usize,
    /// Compact L0 down into L1 once this many L0 (flush-tier) SSTables accumulate.
    pub compaction_trigger: usize,
    /// Soft byte budget per output SSTable when partitioning a leveled
    /// compaction's merged records into non-overlapping runs.
    pub target_table_bytes: usize,
    /// Level fanout: level `n` (n≥1) holds up to `L1_TABLE_BUDGET * fanout^(n-1)`
    /// tables before it cascades into level `n+1`.
    pub level_fanout: usize,
    /// WAL segment byte budget: once the active WAL segment exceeds this, the next
    /// group-commit batch rolls to a fresh segment file, so a flush can drop whole
    /// covered segments rather than rewriting one growing WAL.
    pub wal_segment_bytes: u64,
    /// Tombstone GC grace, in **versions**. During compaction a tombstone (and the
    /// versions it shadows) is reclaimed only once its version is at or below the
    /// **GC floor** = `max_version.saturating_sub(this)` — and only when no deeper,
    /// uncompacted level could still hold an older value for that key (which would
    /// otherwise resurface). This keeps every historical read at a version *above*
    /// the floor (the retained `[floor+1, max_version]` window) observationally
    /// identical to before GC. A larger value retains tombstones longer (set it
    /// above the maximum anti-entropy lag so a long-offline replica is still
    /// repaired with the delete before the tombstone is reclaimed; ADR 0010).
    pub tombstone_grace_versions: Version,
    /// **Opt-in fast path** (default `false`, safe): skip the cross-SSTable
    /// `latest_version_of` point read that `merge`/`merge_batch` normally do to
    /// decide their per-key LWW winner. Setting this is a contract from the
    /// caller: every `merge`/`merge_tombstone`/`merge_batch` version passed to
    /// this engine is already known to be monotonically increasing per key (true
    /// under the CP plane's monotonic Raft-log-index versions), so the LWW
    /// read-before-write is structurally always a winner and can be skipped.
    /// `merge_batch` still dedupes multiple ops for the same key *within one
    /// batch* (cheap, in-memory) — only the read against already-durable engine
    /// state is skipped. Leaderless/AP callers (ADR 0010), where two replicas can
    /// legitimately race with non-monotonic versions, must leave this `false`.
    pub trust_monotonic_versions: bool,
    /// **Opt-in** (default `false`, matches all prior behavior): move memtable
    /// flush + compaction off the write path's ack. When `false` (default), a
    /// write that crosses the flush threshold runs `flush`/compaction inline
    /// before its `put`/`merge`/etc. call returns — simple and fully synchronous,
    /// what every existing caller and test expects. When `true`, a write instead
    /// **triggers** a background task (`env.spawn_task`) to do that work and
    /// returns as soon as it is durable + applied; a
    /// **bounded-memtable-overshoot backpressure gate** (`await_backpressure`)
    /// makes writers wait (via `env.sleep`, deterministic under `SimEnv`) if the
    /// memtable grows far past the flush threshold, instead of growing without
    /// limit while maintenance catches up. This needs a driver that actually
    /// polls spawned tasks (e.g. `Simulator::run_for`/`run_until_quiescent`, or
    /// any real multi-threaded `ProdEnv` runtime) — a bare
    /// `futures::executor::block_on` of a single write never runs anything else,
    /// so backpressure would never resolve; every test in this crate that opts
    /// in drives the simulator explicitly (see `lsm_maintenance.rs`).
    pub background_maintenance: bool,
}

impl Default for LsmOptions {
    fn default() -> Self {
        Self {
            flush_threshold_bytes: DEFAULT_FLUSH_BYTES,
            compaction_trigger: DEFAULT_COMPACTION_TRIGGER,
            target_table_bytes: DEFAULT_TARGET_TABLE_BYTES,
            level_fanout: DEFAULT_LEVEL_FANOUT,
            wal_segment_bytes: DEFAULT_WAL_SEGMENT_BYTES,
            tombstone_grace_versions: DEFAULT_TOMBSTONE_GRACE_VERSIONS,
            trust_monotonic_versions: false,
            background_maintenance: false,
        }
    }
}

/// Per-key version history: `version -> Some(value)` or `None` (tombstone).
type History = BTreeMap<Version, Option<Value>>;

/// One durable mutation in the WAL. `merge`/`merge_tombstone` decide whether to
/// apply *before* logging, so they are recorded as a plain `Put`/`Delete` at the
/// chosen version; replay just re-inserts that `(key, version)` slot, which is
/// idempotent and order-independent.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum WalRecord {
    Put {
        key: Key,
        value: Value,
        version: Version,
    },
    Delete {
        key: Key,
        version: Version,
    },
    DeleteRange {
        start: Key,
        end: Key,
        keys: Vec<Key>,
        version: Version,
    },
    Batch {
        version: Version,
        ops: Vec<BatchOp>,
    },
    /// A group of per-key LWW merges, each carrying its **own** version (unlike
    /// `Batch`, which stamps one version on every op). Logged by `merge_batch`
    /// after the LWW decision, so replay is a pure per-op re-insert of each
    /// `(key, version)` slot — idempotent and order-independent, exactly as a
    /// run of individual `merge`/`merge_tombstone` records would replay.
    MergeBatch {
        ops: Vec<MergeRec>,
    },
}

/// One decided merge as logged in a [`WalRecord::MergeBatch`]: `value` `Some` is a
/// value slot, `None` a tombstone slot, at this op's own `version`.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct MergeRec {
    key: Key,
    value: Option<Value>,
    version: Version,
}

/// A batch op as logged: range deletes are pre-expanded to the affected keys so
/// replay is a pure re-insert (it doesn't need to consult live state).
#[derive(Clone, Debug, Serialize, Deserialize)]
enum BatchOp {
    Put { key: Key, value: Value },
    Delete { key: Key },
    DeleteKeys { keys: Vec<Key> },
}

/// The durable manifest: the live SSTable set plus engine metadata. Encoded with
/// a **compact binary codec** ([`encode_manifest`]) and written atomically with
/// [`Disk::replace`] — the single flush/compaction linearization point. The
/// serde derives remain so a *legacy* JSON manifest (written before the binary
/// codec) can still be read on open ([`decode_manifest`] falls back to
/// `serde_json` when the binary magic is absent).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Manifest {
    /// Highest sequence number ever allocated to an SSTable file. The next flush
    /// uses `next_seq + 1`, so an orphan file from a crashed flush is never
    /// reused (we always move forward).
    next_seq: u64,
    /// Live SSTables, oldest first (so newer versions are found by scanning the
    /// vector in reverse).
    tables: Vec<SsTableMeta>,
    /// The engine-wide monotonic floor (highest version ever written via the
    /// `put`/`delete`/`write_batch` contract, raised by `merge` too).
    max_version: Version,
    /// Live WAL segment numbers (ascending) the engine must replay on recovery —
    /// the files `<prefix>wal-NNNNNN` not yet fully folded into an SSTable. An
    /// empty list (e.g. a legacy manifest from the single-file-WAL era) is treated
    /// as "no segmented WAL recorded" and recovery falls back to the legacy
    /// `<prefix>wal` file (see [`LsmEngine::open_with`]).
    #[serde(default)]
    wal_segments: Vec<u64>,
}

/// Mutable in-memory state, guarded by a [`std::sync::Mutex`]. **No guard is
/// ever held across an `.await`**: disk I/O happens outside the lock; the lock is
/// taken only for brief synchronous mutations / reads of this struct (ADR 0003
/// determinism + `Send` correctness).
struct Inner {
    /// The active memtable.
    memtable: BTreeMap<Key, History>,
    /// Approximate buffered byte count of the memtable, for the flush threshold.
    memtable_bytes: usize,
    /// Open readers for the live SSTables, oldest first (parallel to
    /// `manifest.tables`). Each holds only metadata + the block index in memory.
    readers: Vec<SsTableReader>,
    /// The current durable manifest image.
    manifest: Manifest,
    /// Count of flushes performed (introspection / tests).
    flushes: u64,
    /// Count of compactions performed (introspection / tests).
    compactions: u64,
    /// Shared counter of SSTable blocks fetched from disk, wired into every
    /// reader so the engine can report read amplification (introspection /
    /// tests — e.g. that a Bloom-rejected point-miss reads zero blocks).
    block_reads: Arc<AtomicU64>,
    /// Writes that are **durable in the WAL but not yet applied to the memtable**:
    /// a writer increments this before logging and decrements it after the apply.
    /// A flush must not truncate the WAL while this is non-zero, or it would drop a
    /// durable-but-unapplied record that is in neither the memtable nor the new
    /// SSTable. With WAL group commit a writer yields between log and apply, so this
    /// window is now observable; the gate closes it (see [`LsmEngine::flush`]).
    applies_in_flight: u64,
    /// Refcount of currently-held [`LsmSnapshot`]s, keyed by their pinned
    /// version (several snapshots can share a version if taken with no writes
    /// in between). Registered by [`LsmEngine::snapshot`], released when the
    /// last `LsmSnapshot` at that version drops. Compaction's tombstone GC
    /// floor (`run_compaction`) is capped below the lowest held version so a
    /// long-held snapshot's reads are never affected by GC reclaiming history
    /// it still needs — see the module docs' tombstone-GC section.
    held_snapshots: BTreeMap<Version, u64>,
}

impl Inner {
    /// The highest version recorded for `key` across the memtable **and** all
    /// SSTables, tombstones included, or `None` if absent everywhere. Used by the
    /// per-key LWW `merge`/`merge_tombstone`.
    ///
    /// Note: this consults SSTable in-memory metadata only when the table's key
    /// range contains `key`; it never reads disk for the version (block reads are
    /// done by the async caller). Because `merge` decisions are made under the
    /// lock and we must not block, callers pass in the SSTable point lookups they
    /// already gathered — see [`LsmEngine::latest_version_of`].
    fn memtable_latest_version_of(&self, key: &[u8]) -> Option<Version> {
        self.memtable.get(key)?.keys().next_back().copied()
    }

    fn apply_put(&mut self, key: &[u8], value: &[u8], version: Version) {
        self.memtable_bytes += key.len() + value.len() + 16;
        self.memtable
            .entry(key.to_vec())
            .or_default()
            .insert(version, Some(value.to_vec()));
    }

    fn apply_delete(&mut self, key: &[u8], version: Version) {
        self.memtable_bytes += key.len() + 16;
        self.memtable
            .entry(key.to_vec())
            .or_default()
            .insert(version, None);
    }
}

/// Decrements [`Inner::applies_in_flight`] on drop, so the in-flight gate is
/// released on every path (a normal apply, an early-return error, or a panic).
struct InFlightGuard {
    inner: Arc<Mutex<Inner>>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.applies_in_flight = inner.applies_in_flight.saturating_sub(1);
        }
    }
}

/// An `Env`-agnostic **async mutex** serializing *maintenance* — memtable
/// flushes and compactions — against each other. Exactly one flush **or**
/// compaction runs at a time; overlapping ones would each read
/// `manifest.next_seq` before the other's final swap advances it (duplicate
/// SSTable seq numbers → one file overwritten, a corrupt manifest), race their
/// `write_manifest` swaps (last writer silently drops the other's tables /
/// WAL-segment survivors), and — for two flushes — each clear memtable state the
/// other's snapshot still relies on.
///
/// Unlike every `std::sync::Mutex` in this crate, this lock's guard **is** held
/// across the flush/compaction `.await`s — that is its whole point — which is
/// safe because acquiring/releasing only takes a brief internal `std` lock (never
/// held across an await) and the guard itself is a plain `&`-reference, so the
/// futures stay `Send` and scheduling stays deterministic under `SimEnv` (the
/// waiter list is an ordered `Vec`; wake order is the registration order).
/// Writers (`log_and_apply`) never take this lock, so the WAL group-commit
/// liveness and write throughput are untouched.
#[derive(Default)]
struct MaintenanceLock {
    state: Mutex<MaintenanceState>,
}

#[derive(Default)]
struct MaintenanceState {
    /// Whether a flush/compaction currently holds the lock.
    held: bool,
    /// Tasks parked waiting to acquire, in registration order.
    waiters: Vec<Waker>,
}

impl MaintenanceLock {
    /// Resolve to a guard once the lock is free. Contenders woken on release
    /// re-poll in registration order; exactly one wins and the rest re-park, so
    /// no wakeup is ever lost.
    fn acquire(&self) -> AcquireMaintenance<'_> {
        AcquireMaintenance { lock: self }
    }
}

struct AcquireMaintenance<'a> {
    lock: &'a MaintenanceLock,
}

impl<'a> Future for AcquireMaintenance<'a> {
    type Output = MaintenanceGuard<'a>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.lock.state.lock().expect("maintenance lock poisoned");
        if state.held {
            state.waiters.push(cx.waker().clone());
            Poll::Pending
        } else {
            state.held = true;
            Poll::Ready(MaintenanceGuard { lock: self.lock })
        }
    }
}

/// Releases the [`MaintenanceLock`] on drop (every exit path: success, an
/// early-return error, or a panic) and wakes all parked contenders so the next
/// one can acquire.
struct MaintenanceGuard<'a> {
    lock: &'a MaintenanceLock,
}

impl Drop for MaintenanceGuard<'_> {
    fn drop(&mut self) {
        let waiters = {
            let mut state = self.lock.state.lock().expect("maintenance lock poisoned");
            state.held = false;
            std::mem::take(&mut state.waiters)
        };
        for w in waiters {
            w.wake();
        }
    }
}

/// A real on-disk LSM storage engine. Cheap to clone; clones share state.
///
/// Open one with [`LsmEngine::open`]. All I/O flows through the `Env` it was
/// opened with, so it is deterministic under `SimEnv` and durable under
/// `ProdEnv`.
#[derive(Clone)]
pub struct LsmEngine<E: Env> {
    env: E,
    prefix: Arc<str>,
    opts: LsmOptions,
    inner: Arc<Mutex<Inner>>,
    /// WAL group-commit coordinator: concurrent writes batch their records and
    /// share a single `fsync`. Has its own lock (never the `Inner` lock), so a
    /// writer can park awaiting durability without serializing the memtable.
    wal: Arc<GroupCommit>,
    /// Serializes flushes and compactions against each other (see
    /// [`MaintenanceLock`]). Held across the whole flush/compaction, including
    /// its disk I/O; never taken by the write path.
    maintenance: Arc<MaintenanceLock>,
    /// Observability sink (ADR 0015). Defaults to `env.metrics()` at open (the
    /// no-op handle under `SimEnv`, a recording one under `ProdEnv`); a sim test
    /// threads a recording handle via [`open_with_metrics`](Self::open_with_metrics)
    /// to read storage counters back. Recording is observe-only — a relaxed atomic
    /// add at the real LSM site — and changes no engine behavior.
    metrics: MetricsHandle,
    /// Set while a background maintenance task (`LsmOptions::background_maintenance`)
    /// is in flight, so at most one runs at a time. See `trigger_background_maintenance`.
    maintenance_scheduled: Arc<AtomicBool>,
    /// The most recent background maintenance failure, if any (introspection —
    /// `background_maintenance` is fire-and-forget from the writer's point of
    /// view, so an error has nowhere else to surface; the next write that
    /// crosses the threshold retriggers maintenance regardless).
    background_error: Arc<Mutex<Option<String>>>,
}

impl<E: Env> LsmEngine<E> {
    fn manifest_file(&self) -> String {
        format!("{}MANIFEST", self.prefix)
    }

    fn sst_file(&self, seq: u64) -> String {
        format!("{}sst-{seq:06}", self.prefix)
    }

    /// Discover the WAL segments to replay on recovery: the manifest's recorded
    /// `live` segments, augmented with any present-on-disk segments that follow the
    /// last recorded one (created by writes after the last flush, or by a crash
    /// mid-GC, so not yet manifest-named — but they carry acks and must be
    /// replayed). Returns them ascending. An empty result means the legacy
    /// single-file WAL path (no segmented WAL on disk yet).
    async fn discover_wal_segments(env: &E, prefix: &str, live: &[u64]) -> Result<Vec<u64>> {
        // Start from the recorded set (already ascending in the manifest).
        let mut segs: Vec<u64> = live.to_vec();
        // Probe forward from just past the highest recorded segment (or 0 when the
        // manifest names none) for contiguous segment files written since. We stop
        // at the first gap: segments are allocated strictly increasing and never
        // reused, so a missing number means nothing higher exists.
        let mut next = live.last().map_or(0, |&hi| hi + 1);
        loop {
            let file = format!("{prefix}wal-{next:06}");
            if env.size(&file).await.map_err(io)? == 0 {
                // Either the file does not exist, or it exists but is empty (an
                // un-synced create dropped by a crash) — nothing to replay, and no
                // higher contiguous segment can exist.
                break;
            }
            segs.push(next);
            next += 1;
        }
        // When the manifest named no segments but probing from 0 found some, those
        // are the live set already; when probing found nothing and the manifest had
        // none, `segs` is empty and the caller takes the legacy path.
        Ok(segs)
    }

    /// Remove orphan WAL segment files that sit **below** the live (replayed) set —
    /// covered segments a crash-after-manifest-swap-before-`remove` leaked. `live`
    /// is the ascending set [`discover_wal_segments`](Self::discover_wal_segments)
    /// returned; every segment numbered below its minimum is a covered orphan whose
    /// records are already in an SSTable, so removing it is data-safe. No-op when
    /// the live set is empty (legacy single-file WAL) or starts at 0.
    ///
    /// Uses [`Disk::list`] (one directory listing) rather than probing segment
    /// numbers `0..lowest_live` one `env.size` call at a time: the probe loop
    /// costs one I/O call per *ever-rotated* segment number on **every** engine
    /// open (unbounded over the engine's lifetime — a long-lived engine that has
    /// rotated thousands of segments pays thousands of calls just to find
    /// nothing), where a listing costs one call regardless of history.
    async fn remove_orphan_wal_segments(env: &E, prefix: &str, live: &[u64]) -> Result<()> {
        let Some(&lowest_live) = live.iter().min() else {
            return Ok(());
        };
        let wal_prefix = format!("{prefix}wal-");
        for name in env.list().await.map_err(io)? {
            let Some(seg_str) = name.strip_prefix(&wal_prefix) else {
                continue;
            };
            let Ok(seg) = seg_str.parse::<u64>() else {
                continue; // not a `wal-NNNNNN` segment file (e.g. another engine's file)
            };
            if seg < lowest_live {
                env.remove(&name).await.map_err(io)?;
            }
        }
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("lsm storage poisoned")
    }

    /// Open (or create) an engine at `prefix` over `env`'s disk.
    ///
    /// Recovery: read the manifest → open the named SSTables (loading each one's
    /// block index) → replay the WAL into the memtable → restore `max_version`.
    /// A crash mid-flush or mid-compaction recovers the last durable manifest
    /// plus the intact WAL (see the module docs).
    ///
    /// # Errors
    /// Returns [`StorageError::Backend`] on a malformed manifest/SSTable or an
    /// I/O error from the disk.
    pub async fn open(env: E, prefix: impl Into<String>) -> Result<Self> {
        Self::open_with(env, prefix, LsmOptions::default()).await
    }

    /// [`open`](Self::open) with explicit [`LsmOptions`]. Records storage metrics
    /// (ADR 0015) into the env's own sink (`env.metrics()`) — the no-op handle
    /// under `SimEnv`, the recording one under `ProdEnv`. A sim test that wants to
    /// read storage counters back opens with [`open_with_metrics`](Self::open_with_metrics).
    ///
    /// # Errors
    /// As [`open`](Self::open).
    pub async fn open_with(env: E, prefix: impl Into<String>, opts: LsmOptions) -> Result<Self> {
        let metrics = env.metrics();
        Self::open_with_metrics(env, prefix, opts, metrics).await
    }

    /// [`open_with`](Self::open_with) with an explicit metrics [`MetricsHandle`]
    /// (ADR 0015), so a deterministic sim test can thread a *recording* handle in
    /// and read storage counters back without changing `animus-sim` (the same
    /// additive `*_with_metrics` pattern as `RaftNode::start_with_metrics` /
    /// `DataClient::with_metrics`). Production wiring uses [`open`](Self::open) /
    /// [`open_with`](Self::open_with), which forward `env.metrics()`.
    ///
    /// # Errors
    /// As [`open`](Self::open).
    pub async fn open_with_metrics(
        env: E,
        prefix: impl Into<String>,
        opts: LsmOptions,
        metrics: MetricsHandle,
    ) -> Result<Self> {
        let prefix: Arc<str> = Arc::from(prefix.into());
        let manifest_file = format!("{prefix}MANIFEST");

        // Load the durable manifest (or a fresh empty one). Decodes the compact
        // binary format, transparently falling back to a legacy JSON manifest.
        let manifest_bytes = env.read(&manifest_file).await.map_err(io)?;
        let manifest: Manifest = if manifest_bytes.is_empty() {
            Manifest::default()
        } else {
            decode_manifest(&manifest_bytes)?
        };

        // Open the SSTables the manifest names (reads their footer + index only).
        let block_reads = Arc::new(AtomicU64::new(0));
        let mut readers = Vec::with_capacity(manifest.tables.len());
        for meta in &manifest.tables {
            let file = format!("{prefix}sst-{:06}", meta.seq);
            readers.push(
                SsTableReader::open(&env, file, meta.clone())
                    .await?
                    .with_block_counter(Arc::clone(&block_reads))
                    .with_metrics(metrics.clone()),
            );
        }

        // Determine the live WAL segments to replay, then replay them in order
        // into the memtable. A torn trailing record (crash mid-append, never
        // synced/acked) is dropped by `decode`.
        //
        // The live set is the manifest's `wal_segments` *plus* any contiguous
        // higher-numbered segment files present on disk but not yet named by a
        // manifest — those were created by writes after the last flush (or by a
        // crash mid-GC), and they may hold acked-but-unflushed records, so they
        // must be replayed. This mirrors the orphan-SSTable rule, except a WAL
        // segment is recovered (it carries acks) rather than ignored.
        let segments = Self::discover_wal_segments(&env, &prefix, &manifest.wal_segments).await?;

        // Reclaim orphan WAL segment files left below the live set. A flush GCs a
        // covered segment by swapping the manifest (dropping it from `wal_segments`)
        // and *then* `remove`ing the file; a crash in that window leaves the file on
        // disk below the live set. Recovery already *ignores* it (its records are in
        // the SSTable, and `discover_wal_segments` only probes forward), but the file
        // would leak forever. Every segment numbered below the lowest live one is
        // exactly such a covered orphan (segments are allocated strictly increasing
        // and never reused), so it is data-safe to remove on open. Doing it here
        // closes the leak at the next reopen even if the GC `remove` never ran.
        Self::remove_orphan_wal_segments(&env, &prefix, &segments).await?;

        let mut memtable: BTreeMap<Key, History> = BTreeMap::new();
        let mut memtable_bytes = 0usize;
        let mut max_version = manifest.max_version;
        if segments.is_empty() {
            // Legacy migration: a directory written by the single-file-WAL era has
            // no recorded segments. Replay the old `<prefix>wal` file (if any) so
            // an upgrade loses nothing; the first new flush rewrites the layout to
            // segments and this file is left as a harmless orphan. (Pre-alpha: this
            // predates even the binary WAL codec, so an old JSON-encoded file no
            // longer decodes — no real deployment depends on it; a brand-new engine
            // with no legacy file just reads zero bytes here and replays nothing.)
            let legacy = format!("{prefix}wal");
            let wal_bytes = env.read(&legacy).await.map_err(io)?;
            let (records, _consumed) = decode_wal(&wal_bytes)?;
            for record in records {
                max_version = max_version.max(record_max_version(&record));
                apply_wal_record(&mut memtable, &mut memtable_bytes, record);
            }
        } else {
            // The highest-numbered segment is the one `GroupCommit` reopens as
            // *active* (see its constructor below): further appends ride it. Only
            // this segment can ever carry a crash-torn tail — older segments are
            // sealed and never appended to again once rotated past (see the module
            // docs' crash-safety argument) — so it's the only one that may need
            // resealing before new writes land after its recovered content.
            let active_seg = *segments.last().expect("segments is non-empty here");
            for &seg in &segments {
                let file = format!("{prefix}wal-{seg:06}");
                let wal_bytes = env.read(&file).await.map_err(io)?;
                let (records, consumed) = decode_wal(&wal_bytes)?;
                for record in records {
                    max_version = max_version.max(record_max_version(&record));
                    apply_wal_record(&mut memtable, &mut memtable_bytes, record);
                }
                // Seal a torn trailing tail on the segment that becomes active:
                // future appends must ride a clean boundary, never concatenate
                // onto leftover garbage from an unsynced write a crash
                // interrupted. Left in place, a second recovery would see the
                // next (acked, synced) record's bytes glued onto that garbage
                // with no frame boundary between them and lose it — exactly the
                // bug `acked_writes_after_torn_tail_recovery_survive_second_restart`
                // pins. `replace` is the same atomic primitive the manifest swap
                // uses, so this truncation is itself crash-safe.
                if seg == active_seg && consumed < wal_bytes.len() {
                    env.replace(&file, &wal_bytes[..consumed])
                        .await
                        .map_err(io)?;
                }
            }
        }

        let inner = Inner {
            memtable,
            memtable_bytes,
            readers,
            manifest: Manifest {
                max_version,
                ..manifest
            },
            flushes: 0,
            compactions: 0,
            block_reads,
            applies_in_flight: 0,
            held_snapshots: BTreeMap::new(),
        };

        // The WAL has been fully replayed into the memtable, so every recovered
        // record is already reflected in memory: the group-commit sequence space
        // resumes at 0 (the next new write is the first durable sequence). The
        // highest discovered segment becomes the active one; the rest are sealed,
        // so a later flush can GC the covered ones. An empty discovered set means a
        // fresh (or legacy) engine: the first write opens segment 0.
        let wal = Arc::new(GroupCommit::new(
            prefix.to_string(),
            &segments,
            opts.wal_segment_bytes,
        ));

        Ok(Self {
            env,
            prefix,
            opts,
            inner: Arc::new(Mutex::new(inner)),
            wal,
            maintenance: Arc::new(MaintenanceLock::default()),
            metrics,
            maintenance_scheduled: Arc::new(AtomicBool::new(false)),
            background_error: Arc::new(Mutex::new(None)),
        })
    }

    /// Durably log `record` then apply it to the memtable via `apply`, holding the
    /// **in-flight gate** across the whole window so a concurrent flush cannot
    /// truncate this still-unapplied record out of the WAL.
    ///
    /// The record is made durable via WAL **group commit** (the encoded record
    /// joins a shared pending batch and this returns only once a single `fsync`
    /// covering it has completed; ADR 0008), then `apply` mutates the memtable
    /// under the `Inner` lock. Durability precedes the in-memory apply, so an ack
    /// means durable; the apply runs only on a successful sync.
    async fn log_and_apply(
        &self,
        record: &WalRecord,
        apply: impl FnOnce(&mut Inner),
    ) -> Result<()> {
        // Enter the in-flight gate: a flush will not truncate the WAL while any
        // durable-but-unapplied record exists. A guard decrements on every path.
        self.lock().applies_in_flight += 1;
        let guard = InFlightGuard {
            inner: Arc::clone(&self.inner),
        };
        let bytes = encode_wal(record);
        // Observability (ADR 0015): record any WAL segment rotation this commit
        // performed (the active segment crossed its byte budget). Sampling the
        // coordinator's monotonic rotation counter around the commit attributes
        // the rotation to the write that caused it, with no behavior change.
        let rotations_before = self.wal.rotation_count();
        self.wal.commit(&self.env, bytes).await?;
        let rotated = self.wal.rotation_count() - rotations_before;
        if rotated > 0 {
            self.metrics
                .incr_by(Metric::StorageWalSegmentRotations, rotated);
        }
        {
            let mut inner = self.lock();
            apply(&mut inner);
        }
        drop(guard);
        Ok(())
    }

    /// Whether `meta` may contain `key`, recording the per-table Bloom outcome for
    /// observability (ADR 0015). When the key is inside the table's range and the
    /// table carries a Bloom, a "maybe" bumps `storage_bloom_hits` and a definite
    /// "no" bumps `storage_bloom_misses` (a block read saved). Tables with no Bloom,
    /// or a key outside the range, record nothing (no Bloom decision was made).
    /// Observe-only — returns exactly what [`SsTableMeta::may_contain`] would.
    fn may_contain_observed(&self, meta: &SsTableMeta, key: &[u8]) -> bool {
        let in_range = match (&meta.min_key, &meta.max_key) {
            (Some(lo), Some(hi)) => key >= lo.as_slice() && key <= hi.as_slice(),
            _ => false,
        };
        if in_range && meta.has_bloom {
            if meta.bloom.may_contain(key) {
                self.metrics.incr(Metric::StorageBloomHits);
            } else {
                self.metrics.incr(Metric::StorageBloomMisses);
            }
        }
        meta.may_contain(key)
    }

    /// The highest version recorded for `key` anywhere (memtable + SSTables),
    /// tombstones included. Reads SSTable blocks (async) for tables whose key
    /// range contains `key`, then folds in the memtable under a brief lock.
    /// Decide whether a read that snapshotted compaction generation `generation` should
    /// retry after a block-read error: a compaction has since run (so it may have
    /// `remove`d a file the read referenced — see [`READ_COMPACTION_RETRIES`]) and
    /// retries remain. Bumps `attempt`. If `false`, the error is genuine (no
    /// compaction raced, or retries exhausted) and the caller propagates it.
    fn raced_compaction(&self, generation: u64, attempt: &mut u32) -> bool {
        let raced = self.lock().compactions != generation;
        if raced && *attempt < READ_COMPACTION_RETRIES {
            *attempt += 1;
            true
        } else {
            false
        }
    }

    async fn latest_version_of(&self, key: &[u8]) -> Result<Option<Version>> {
        let mut attempt = 0;
        loop {
            // Snapshot the SSTable readers (cheap clones of metadata + index) +
            // the compaction generation under the lock, plus the memtable's own
            // latest, then read blocks lock-free.
            let (readers, generation, memtable_latest) = {
                let inner = self.lock();
                (
                    inner.readers.clone(),
                    inner.compactions,
                    inner.memtable_latest_version_of(key),
                )
            };
            let mut best = memtable_latest;
            let mut read_err = None;
            for reader in readers.iter().rev() {
                if !self.may_contain_observed(reader.meta(), key) {
                    continue;
                }
                match reader.latest(&self.env, key).await {
                    Ok(Some((v, _))) => best = Some(best.map_or(v, |b| b.max(v))),
                    Ok(None) => {}
                    Err(e) => {
                        read_err = Some(e);
                        break;
                    }
                }
            }
            match read_err {
                None => return Ok(best),
                Some(e) => {
                    if self.raced_compaction(generation, &mut attempt) {
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Read `key` as of `version`: the greatest `(version', slot)` with
    /// `version' ≤ version` across the memtable and SSTables (newest wins), with a
    /// tombstone hiding older values. Matches [`MemoryEngine`] semantics.
    ///
    /// [`MemoryEngine`]: crate::MemoryEngine
    async fn read_at(&self, key: &[u8], version: Version) -> Result<Option<VersionedValue>> {
        let mut attempt = 0;
        loop {
            let (readers, generation, memtable_hit) = {
                let inner = self.lock();
                let hit = inner
                    .memtable
                    .get(key)
                    .and_then(|h| h.range(..=version).next_back())
                    .map(|(&v, slot)| (v, slot.clone()));
                (inner.readers.clone(), inner.compactions, hit)
            };
            // Track the best (highest-version) hit seen so far. The memtable is the
            // newest source.
            let mut best: Option<(Version, Option<Value>)> = memtable_hit;
            let mut read_err = None;
            for reader in readers.iter().rev() {
                // Take the max-version hit across all sources; a key range that
                // can't contain `key` is skipped cheaply.
                if !self.may_contain_observed(reader.meta(), key) {
                    continue;
                }
                match reader.get_at(&self.env, key, version).await {
                    Ok(Some((v, slot))) => {
                        if best.as_ref().is_none_or(|(bv, _)| v > *bv) {
                            best = Some((v, slot));
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        read_err = Some(e);
                        break;
                    }
                }
            }
            if let Some(e) = read_err {
                if self.raced_compaction(generation, &mut attempt) {
                    continue;
                }
                return Err(e);
            }
            return Ok(
                best.and_then(|(v, slot)| slot.map(|value| VersionedValue { version: v, value }))
            );
        }
    }

    /// Scan `[start, end)` as of `version`, merging all sources, ordered by key.
    async fn scan_at(
        &self,
        start: &[u8],
        end: &[u8],
        version: Version,
    ) -> Result<Vec<(Key, VersionedValue)>> {
        let merged = self.merged_at(start, Some(end), version).await?;
        Ok(merged
            .into_iter()
            .filter_map(|(k, (v, slot))| {
                slot.map(|value| (k, VersionedValue { version: v, value }))
            })
            .collect())
    }

    /// Merge every source over `[start, end)` (`end = None` is unbounded) into
    /// `key -> (version, slot)` of the winning (greatest `≤ version`) record per
    /// key, ordered by key. Tombstones are retained as `(version, None)`.
    async fn merged_at(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        version: Version,
    ) -> Result<BTreeMap<Key, (Version, Option<Value>)>> {
        let upper = match end {
            Some(e) => Bound::Excluded(e),
            None => Bound::Unbounded,
        };
        let mut attempt = 0;
        loop {
            // Snapshot the readers, the compaction generation, **and** the memtable
            // range under a single lock, so the view is point-in-time consistent: a
            // concurrent **flush** (which moves keys from the memtable into a new
            // SSTable and clears them from the memtable) cannot drop those keys from
            // the result by landing between a readers snapshot and a separate,
            // later memtable read. The memtable range is small (bounded by the
            // flush threshold), so collecting it up front is cheap.
            let (readers, generation, mem_entries) = {
                let inner = self.lock();
                let mem: Vec<(Key, Version, Option<Value>)> = inner
                    .memtable
                    .range::<[u8], _>((Bound::Included(start), upper))
                    .filter_map(|(k, history)| {
                        history
                            .range(..=version)
                            .next_back()
                            .map(|(&v, slot)| (k.clone(), v, slot.clone()))
                    })
                    .collect();
                (inner.readers.clone(), inner.compactions, mem)
            };
            // Oldest first, so newer overwrites older; the memtable is applied last.
            let mut merged: BTreeMap<Key, (Version, Option<Value>)> = BTreeMap::new();
            let mut read_err = None;
            for reader in &readers {
                match reader.scan_at(&self.env, start, end, version).await {
                    Ok(rows) => {
                        for (k, v, slot) in rows {
                            merge_winner(&mut merged, k, v, slot);
                        }
                    }
                    Err(e) => {
                        read_err = Some(e);
                        break;
                    }
                }
            }
            if let Some(e) = read_err {
                if self.raced_compaction(generation, &mut attempt) {
                    continue;
                }
                return Err(e);
            }
            // Memtable last (newest), from the atomic snapshot taken above.
            {
                for (k, v, slot) in mem_entries {
                    merge_winner(&mut merged, k, v, slot);
                }
            }
            return Ok(merged);
        }
    }

    /// Every key's latest record across all sources (whole keyspace), tombstones
    /// included, as `(key, (version, slot))`.
    async fn merged_latest(&self) -> Result<BTreeMap<Key, (Version, Option<Value>)>> {
        self.merged_at(&[], None, Version::MAX).await
    }

    /// Flush the memtable to a new SSTable if it is over threshold, then run any
    /// due compactions. Called after every write (cheap when nothing is due).
    /// Disk I/O happens outside the lock; the manifest swap is the durability
    /// point. Compaction is driven to quiescence so a cascade (L0→L1→L2…) settles
    /// in one call.
    async fn maybe_flush_and_compact(&self) -> Result<()> {
        let should_flush = {
            let inner = self.lock();
            !inner.memtable.is_empty()
                && inner.memtable_bytes >= self.opts.flush_threshold_bytes
                // Don't flush while a concurrent write is durable-but-unapplied:
                // the flush's watermark = `durable_seq`, and only with no in-flight
                // apply is every durable record (seq ≤ watermark) already in the
                // memtable snapshot — the invariant that makes GCing a fully-covered
                // WAL segment safe. The writer re-attempts the flush from its own
                // `maybe_flush_and_compact` once it has applied. This check is only
                // a cheap decision-time hint that avoids acquiring the maintenance
                // lock; `flush` re-checks it authoritatively, atomically with its
                // snapshot. (See `Inner::applies_in_flight` and `flush`.)
                && inner.applies_in_flight == 0
        };
        if should_flush {
            self.flush().await?;
        }
        // Run compactions until none is due (bounded: each pass reduces a level's
        // table count, and deeper levels have larger budgets).
        while let Some(plan) = self.next_compaction() {
            self.run_compaction(plan).await?;
        }
        Ok(())
    }

    /// After a write is durable + applied: run flush/compaction **inline**
    /// (default — every existing caller and test expects a write that crosses
    /// the flush threshold to have flushed by the time it returns), or, when
    /// [`LsmOptions::background_maintenance`] opts in, trigger it in the
    /// background and apply write backpressure instead — see the field's docs
    /// for the tradeoff and what's needed to drive it (a real scheduler, not a
    /// bare `futures::executor::block_on`).
    async fn after_write_maintenance(&self) -> Result<()> {
        if self.opts.background_maintenance {
            self.trigger_background_maintenance();
            self.await_backpressure().await
        } else {
            self.maybe_flush_and_compact().await
        }
    }

    /// If the memtable is over the flush threshold, or a compaction is due, and
    /// no maintenance task is already running, spawn one (`env.spawn_task`) to
    /// do that work off the write path. At most one maintenance task runs at a
    /// time (`maintenance_scheduled`); a writer that finds one already in
    /// flight just leaves it — `maybe_flush_and_compact` re-reads memtable /
    /// compaction state itself when it runs, so nothing is missed, and the next
    /// write that still finds work due will trigger another task once this one
    /// finishes.
    fn trigger_background_maintenance(&self) {
        let should_flush = {
            let inner = self.lock();
            !inner.memtable.is_empty() && inner.memtable_bytes >= self.opts.flush_threshold_bytes
        };
        let due = should_flush || self.next_compaction().is_some();
        if !due {
            return;
        }
        if self
            .maintenance_scheduled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return; // a maintenance task is already in flight
        }
        let engine = self.clone();
        self.env.spawn_task(async move {
            if let Err(e) = engine.maybe_flush_and_compact().await {
                *engine.background_error.lock().expect("poisoned") = Some(e.to_string());
            }
            engine.maintenance_scheduled.store(false, Ordering::Release);
        });
    }

    /// Slow a writer down if the memtable has grown well past the flush
    /// threshold — maintenance may be falling behind a burst of writes, or
    /// stuck behind a slow/faulty disk. This is what bounds memtable growth
    /// once flush/compaction move off the write path (without it, writers
    /// could keep applying to the memtable indefinitely fast while a single
    /// background task tries to keep up). A writer only waits
    /// (`env.sleep`, deterministic under `SimEnv`); it never does the
    /// flush/compaction work itself.
    ///
    /// # Errors
    /// Returns [`StorageError::Backend`] if the memtable is still over the cap
    /// after [`BACKPRESSURE_MAX_POLLS`] polls — maintenance is not making
    /// progress (e.g. a persistently failing disk; see `background_error`),
    /// so this turns what would otherwise be an unbounded stall into a loud,
    /// clean error.
    async fn await_backpressure(&self) -> Result<()> {
        let cap = self
            .opts
            .flush_threshold_bytes
            .saturating_mul(BACKPRESSURE_OVERSHOOT_FACTOR);
        for _ in 0..BACKPRESSURE_MAX_POLLS {
            if self.lock().memtable_bytes <= cap {
                return Ok(());
            }
            // Make sure maintenance is actually working the backlog — normally
            // the write that pushed the memtable over threshold already
            // triggered it, but this is cheap insurance against any gap.
            self.trigger_background_maintenance();
            self.env.sleep(BACKPRESSURE_POLL).await;
        }
        let last_error = self.background_error.lock().expect("poisoned").clone();
        Err(StorageError::Backend(format!(
            "write backpressure: memtable stayed over the hard cap ({cap} bytes) \
             after {BACKPRESSURE_MAX_POLLS} polls — background maintenance is not \
             keeping up (last error: {last_error:?})"
        )))
    }

    /// Open a reader for `file`/`meta` with the engine's shared block-read
    /// counter wired in.
    async fn open_reader(&self, file: String, meta: SsTableMeta) -> Result<SsTableReader> {
        let counter = Arc::clone(&self.lock().block_reads);
        Ok(SsTableReader::open(&self.env, file, meta)
            .await?
            .with_block_counter(counter)
            .with_metrics(self.metrics.clone()))
    }

    /// Decide the next compaction to run, if any. Prefers an L0→L1 compaction
    /// (the flush tier fills fastest); otherwise the shallowest L≥1 over budget.
    fn next_compaction(&self) -> Option<CompactionPlan> {
        let inner = self.lock();
        let tables = &inner.manifest.tables;
        // Count tables per level.
        let mut by_level: BTreeMap<u32, usize> = BTreeMap::new();
        for t in tables {
            *by_level.entry(t.level).or_default() += 1;
        }
        // L0 → L1 when enough flush-tier tables piled up.
        if by_level.get(&0).copied().unwrap_or(0) >= self.opts.compaction_trigger {
            return Some(CompactionPlan { source_level: 0 });
        }
        // Otherwise the shallowest level ≥1 over its table budget cascades down.
        for (&level, &count) in by_level.range(1..) {
            if count > self.level_table_budget(level) {
                return Some(CompactionPlan {
                    source_level: level,
                });
            }
        }
        None
    }

    /// Max tables allowed at `level` (≥1) before it cascades into `level+1`:
    /// `L1_TABLE_BUDGET * level_fanout^(level-1)`.
    fn level_table_budget(&self, level: u32) -> usize {
        L1_TABLE_BUDGET.saturating_mul(self.opts.level_fanout.pow(level - 1))
    }

    /// Write the current memtable to a fresh SSTable, atomically add it to the
    /// manifest, then GC the WAL segments the flush fully covers.
    ///
    /// A no-op when the memtable is empty **or** a concurrent write is still
    /// durable-but-unapplied (`applies_in_flight > 0`) — see the gate below; the
    /// writer re-attempts from its own `maybe_flush_and_compact` once applied.
    async fn flush(&self) -> Result<()> {
        // Serialize against any concurrent flush or compaction. Two overlapping
        // flushes would both allocate `next_seq + 1` (duplicate SSTable seqs —
        // the second file overwrites the first, corrupting the manifest), race
        // their manifest swaps, and each clear memtable state the other's
        // snapshot depends on. Writers never take this lock, so the write path
        // (and WAL group-commit liveness) is unaffected.
        let _maintenance = self.maintenance.acquire().await;

        // Snapshot the memtable + the seq to allocate, lock-free for the write.
        // The `applies_in_flight == 0` gate is re-checked here, **atomically with
        // the snapshot and the watermark sample** — the callers' decision-time
        // check is only a cheap hint, and a write racing in between could
        // otherwise be durable (seq ≤ watermark) yet absent from the snapshot,
        // letting the WAL GC below drop its only durable copy. With the gate
        // holding under this lock, no writer sits between its WAL commit and its
        // memtable apply, and none can *start* one (entering the gate needs this
        // lock), so `durable_seq` is stable for the duration of the critical
        // section and every durable WAL record (seq ≤ `wal_watermark`) is in the
        // snapshot, folded into the new SSTable. That watermark is what later
        // tells us which WAL segments are fully covered (so they may be removed)
        // — a segment whose highest seq ≤ watermark holds only records now
        // durably in the SSTable.
        let (records, seq, mut new_manifest, wal_watermark) = {
            let inner = self.lock();
            if inner.memtable.is_empty() || inner.applies_in_flight > 0 {
                return Ok(());
            }
            let records = flatten_memtable(&inner.memtable);
            let seq = inner.manifest.next_seq + 1;
            let mut m = inner.manifest.clone();
            m.next_seq = seq;
            (records, seq, m, self.wal.durable_seq())
        };

        // Build + sync the new SSTable file (outside the lock). A flush always
        // lands at L0 (the overlapping flush tier).
        let file = self.sst_file(seq);
        let meta = SsTableWriter::write(&self.env, &file, seq, 0, &records).await?;
        self.env.sync(&file).await.map_err(io)?;
        let reader = self.open_reader(file, meta.clone()).await?;

        // Compute the WAL segments fully covered by this flush (all their records
        // ≤ watermark, so now in the SSTable). Record the *surviving* segment set
        // in the manifest before the swap, so the durable manifest never names a
        // segment we are about to remove — a crash mid-GC then recovers a manifest
        // that lists only the survivors, and the (orphaned) covered files, whose
        // data is in the SSTable, are simply ignored on recovery.
        let covered = self.wal.segments_covered_by(wal_watermark);
        let surviving: Vec<u64> = self
            .wal
            .live_segments()
            .into_iter()
            .filter(|s| !covered.contains(s))
            .collect();

        // Atomically swap the manifest to reference the new table and the surviving
        // WAL segments. Until this returns durably, a crash recovers the old
        // manifest + the intact WAL segments.
        new_manifest.tables.push(meta);
        new_manifest.wal_segments = surviving;
        self.write_manifest(&new_manifest).await?;

        // The new manifest is durable and no longer names the covered segments, so
        // their files can be removed (bounding WAL size — no whole-file rewrite).
        // Remove the files, then drop them from the coordinator's live set.
        for seg in &covered {
            self.env
                .remove(&self.wal.segment_file(*seg))
                .await
                .map_err(io)?;
        }
        self.wal.forget_segments(&covered);

        // Commit the in-memory swap: add the reader and **surgically** remove
        // exactly the `(key, version)` slots the snapshot flushed — never a
        // blanket `clear()`. A write applied while the SSTable was being built
        // (outside the lock; e.g. the Raft apply task racing an admin
        // `flush_now`) is in the memtable but *not* in `records`, so it must
        // survive here: clearing it would erase an acked, WAL-durable write from
        // visibility, and a later flush would advance the watermark past its seq
        // and GC its WAL segment — permanent loss. No racing write can overwrite
        // a snapshotted slot in place (`put`/`delete` enforce the monotonic
        // floor; `merge*` applies only strictly-newer versions per key), so
        // removing the snapshotted slots removes exactly the flushed data.
        {
            let mut inner = self.lock();
            inner.manifest = new_manifest;
            inner.readers.push(reader);
            for rec in &records {
                let now_empty = match inner.memtable.get_mut(&rec.key) {
                    Some(history) => {
                        history.remove(&rec.version);
                        history.is_empty()
                    }
                    None => false,
                };
                if now_empty {
                    inner.memtable.remove(&rec.key);
                }
            }
            // Recompute the byte accounting from the (small) residue: only the
            // writes that raced this flush remain.
            inner.memtable_bytes = memtable_bytes_of(&inner.memtable);
            inner.flushes += 1;
        }
        // Observability (ADR 0015): a flush actually committed (the manifest swap
        // is done and the new table is live).
        self.metrics.incr(Metric::StorageFlushes);
        Ok(())
    }

    /// Run one leveled compaction: merge every table at `source_level` with the
    /// tables at `source_level + 1` whose key range overlaps the source's, then
    /// re-partition the merged records into **non-overlapping** runs at the target
    /// level. Atomically swap the manifest to {survivors + new runs}, then remove
    /// the consumed input files.
    ///
    /// Every distinct `(key, version)` record is preserved, so the merged view is
    /// unchanged (observationally identical to [`MemoryEngine`]); only the
    /// physical layout changes. Crash safety is the same single-swap argument as
    /// before: the inputs stay named by the manifest (and intact on disk) until
    /// the swap commits, and the new files are orphans until then.
    async fn run_compaction(&self, plan: CompactionPlan) -> Result<()> {
        // Serialize against any concurrent flush or compaction: a compaction
        // allocates SSTable seqs from `manifest.next_seq` and swaps the manifest
        // + readers exactly like a flush does, so an overlap (e.g. an admin
        // `compact_now` racing a writer-driven `maybe_flush_and_compact`) has the
        // same duplicate-seq / clobbered-manifest hazard. The inputs are picked
        // *under* the lock below, so a stale `plan` (its level already compacted
        // by whoever held the lock first) degrades to a harmless re-plan or
        // no-op.
        let _maintenance = self.maintenance.acquire().await;
        let target_level = plan.source_level + 1;

        // Pick inputs under the lock: all readers at the source level, plus the
        // target-level readers overlapping the source's combined key range. Also
        // capture (a) the engine's monotonic floor (`max_version`), which fixes the
        // tombstone GC floor, (b) the lowest currently-held snapshot version (if
        // any), which further *caps* that floor so a live snapshot's reads are
        // never affected by GC, and (c) the key ranges of every table at a level
        // **deeper** than the target — a tombstone may only be fully reclaimed when
        // no such deeper table could still hold an older value for the key (which
        // would otherwise resurface once the tombstone is gone). Sampled together
        // under one lock so a snapshot registered concurrently with this read is
        // either fully accounted for or not yet started — never a torn view.
        let (input_readers, input_seqs, base_seq, max_version, held_floor, deeper_ranges) = {
            let inner = self.lock();
            let mut input_readers: Vec<SsTableReader> = Vec::new();
            let mut source_bounds: Option<(Key, Key)> = None;
            for (reader, meta) in inner.readers.iter().zip(&inner.manifest.tables) {
                if meta.level == plan.source_level {
                    if let (Some(lo), Some(hi)) = (&meta.min_key, &meta.max_key) {
                        source_bounds = Some(match source_bounds {
                            None => (lo.clone(), hi.clone()),
                            Some((slo, shi)) => (slo.min(lo.clone()), shi.max(hi.clone())),
                        });
                    }
                    input_readers.push(reader.clone());
                }
            }
            // Overlapping target-level tables (non-overlapping among themselves).
            if let Some((slo, shi)) = &source_bounds {
                for (reader, meta) in inner.readers.iter().zip(&inner.manifest.tables) {
                    if meta.level == target_level && ranges_overlap(meta, slo, shi) {
                        input_readers.push(reader.clone());
                    }
                }
            }
            let input_seqs: Vec<u64> = input_readers.iter().map(|r| r.meta().seq).collect();
            // Key ranges of all tables strictly below the target level (and not
            // themselves inputs — none are, since inputs are at source/target level).
            let deeper_ranges: Vec<(Key, Key)> = inner
                .manifest
                .tables
                .iter()
                .filter(|m| m.level > target_level)
                .filter_map(|m| match (&m.min_key, &m.max_key) {
                    (Some(lo), Some(hi)) => Some((lo.clone(), hi.clone())),
                    _ => None,
                })
                .collect();
            // The GC floor must stay *strictly below* every held snapshot version
            // (a snapshot at version V reads with `get_at(key, V)`, which the
            // module docs guarantee unaffected by GC only for versions `> floor`).
            let held_floor = inner
                .held_snapshots
                .keys()
                .next()
                .map(|v| v.saturating_sub(1));
            (
                input_readers,
                input_seqs,
                inner.manifest.next_seq,
                inner.manifest.max_version,
                held_floor,
                deeper_ranges,
            )
        };

        if input_readers.is_empty() {
            return Ok(());
        }

        // Observability (ADR 0015): the input tables this compaction merges away
        // ("segments compacted") and their on-disk bytes. Captured before the
        // inputs are consumed; recorded only once the compaction commits below.
        let merged_tables = input_readers.len() as u64;
        let merged_bytes: u64 = input_readers.iter().map(|r| r.meta().file_size).sum();

        // Merge all inputs into one sorted record stream, keeping every distinct
        // (key, version) so MVCC history / get_at is preserved.
        let mut merged: BTreeMap<(Key, Version), Option<Value>> = BTreeMap::new();
        for reader in &input_readers {
            for (k, v, slot) in reader.full_scan(&self.env).await? {
                merged.insert((k, v), slot);
            }
        }

        // Tombstone GC: reclaim obsolete tombstones (and the versions they shadow)
        // that have aged below the GC floor, without changing any read above it.
        // The drop in record count is exactly what GC reclaimed (observability).
        // Capped by `held_floor` so a live snapshot is never affected by GC (see
        // `LsmEngine::snapshot` / `Inner::held_snapshots`).
        let mut gc_floor = max_version.saturating_sub(self.opts.tombstone_grace_versions);
        if let Some(held_floor) = held_floor {
            gc_floor = gc_floor.min(held_floor);
        }
        let before_gc = merged.len();
        gc_obsolete_records(&mut merged, gc_floor, &deeper_ranges);
        let reclaimed = (before_gc - merged.len()) as u64;

        // Partition into non-overlapping runs of ≈target_table_bytes, splitting
        // only on a key boundary so each run owns a disjoint key range.
        let partitions = partition_records(&merged, self.opts.target_table_bytes);

        // Allocate sequence numbers and write each run at the target level.
        let mut new_metas: Vec<SsTableMeta> = Vec::with_capacity(partitions.len());
        let mut new_readers: Vec<SsTableReader> = Vec::with_capacity(partitions.len());
        let mut new_files: Vec<String> = Vec::with_capacity(partitions.len());
        let mut seq = base_seq;
        for records in &partitions {
            seq += 1;
            let file = self.sst_file(seq);
            let meta = SsTableWriter::write(&self.env, &file, seq, target_level, records).await?;
            self.env.sync(&file).await.map_err(io)?;
            let reader = self.open_reader(file.clone(), meta.clone()).await?;
            new_metas.push(meta);
            new_readers.push(reader);
            new_files.push(file);
        }

        // Build the new manifest: survivors (not consumed) + the new runs. A crash
        // before the swap keeps the old inputs (still named + intact); the new
        // files are orphans at seqs beyond `next_seq` and are ignored on recovery.
        let (new_manifest, old_files) = {
            let inner = self.lock();
            let mut tables: Vec<SsTableMeta> = Vec::new();
            let mut old_files: Vec<String> = Vec::new();
            for meta in &inner.manifest.tables {
                if input_seqs.contains(&meta.seq) {
                    old_files.push(self.sst_file(meta.seq));
                } else {
                    tables.push(meta.clone());
                }
            }
            tables.extend(new_metas.iter().cloned());
            // Keep tables ordered oldest→newest by seq so the parallel
            // readers/tables vectors stay consistent and reverse-scan finds newer
            // versions first.
            tables.sort_by_key(|m| m.seq);
            let new_manifest = Manifest {
                next_seq: seq,
                tables,
                max_version: inner.manifest.max_version,
                // Compaction does not touch the WAL: carry the live segment set
                // through unchanged so the swap doesn't drop it.
                wal_segments: inner.manifest.wal_segments.clone(),
            };
            (new_manifest, old_files)
        };
        self.write_manifest(&new_manifest).await?;

        // Commit in-memory: rebuild the parallel readers vector to match the new
        // manifest's table order, then remove the consumed input files.
        {
            let mut inner = self.lock();
            let mut readers_by_seq: BTreeMap<u64, SsTableReader> = BTreeMap::new();
            for (reader, meta) in inner.readers.iter().zip(&inner.manifest.tables) {
                if !input_seqs.contains(&meta.seq) {
                    readers_by_seq.insert(meta.seq, reader.clone());
                }
            }
            for (meta, reader) in new_metas.iter().zip(&new_readers) {
                readers_by_seq.insert(meta.seq, reader.clone());
            }
            // new_manifest.tables is sorted by seq; map it through readers_by_seq.
            let readers: Vec<SsTableReader> = new_manifest
                .tables
                .iter()
                .map(|m| {
                    readers_by_seq
                        .get(&m.seq)
                        .expect("reader for table")
                        .clone()
                })
                .collect();
            inner.readers = readers;
            inner.manifest = new_manifest;
            inner.compactions += 1;
        }
        // Observability (ADR 0015): a compaction actually committed. Record the
        // event, the tables + bytes it merged, and any tombstones it reclaimed.
        self.metrics.incr(Metric::StorageCompactions);
        self.metrics
            .incr_by(Metric::StorageCompactionTablesMerged, merged_tables);
        self.metrics
            .incr_by(Metric::StorageCompactionBytesMerged, merged_bytes);
        if reclaimed > 0 {
            self.metrics
                .incr_by(Metric::StorageTombstonesReclaimed, reclaimed);
        }
        for f in old_files {
            self.env.remove(&f).await.map_err(io)?;
        }
        Ok(())
    }

    /// Atomically persist the manifest using the compact binary codec. The
    /// `replace` is the single durability linearization point: a crash sees the
    /// whole old or whole new manifest, never a mix.
    async fn write_manifest(&self, manifest: &Manifest) -> Result<()> {
        let bytes = encode_manifest(manifest);
        self.env
            .replace(&self.manifest_file(), &bytes)
            .await
            .map_err(io)
    }

    /// The number of live SSTables (manifest-referenced). Test/introspection
    /// helper; used by crash and flush/compaction tests to assert that flushes
    /// and compactions actually happened.
    #[doc(hidden)]
    #[must_use]
    pub fn sstable_count(&self) -> usize {
        self.lock().manifest.tables.len()
    }

    /// Number of memtable flushes performed since open. Test/introspection.
    #[doc(hidden)]
    #[must_use]
    pub fn flush_count(&self) -> u64 {
        self.lock().flushes
    }

    /// Number of compactions performed since open. Test/introspection.
    #[doc(hidden)]
    #[must_use]
    pub fn compaction_count(&self) -> u64 {
        self.lock().compactions
    }

    /// Number of currently-held [`LsmSnapshot`]s (summed across all pinned
    /// versions). Test/introspection — used to check the tombstone-GC pin
    /// (`Inner::held_snapshots`) is released when a snapshot drops.
    #[doc(hidden)]
    #[must_use]
    pub fn held_snapshot_count(&self) -> u64 {
        self.lock().held_snapshots.values().sum()
    }

    /// The most recent [`LsmOptions::background_maintenance`] task failure, if
    /// any. Test/introspection — background maintenance is fire-and-forget, so
    /// this is how a test observes a failure that couldn't propagate to any
    /// writer directly (`await_backpressure` does surface it once the memtable
    /// is stuck over the hard cap).
    #[doc(hidden)]
    #[must_use]
    pub fn background_maintenance_error(&self) -> Option<String> {
        self.background_error.lock().expect("poisoned").clone()
    }

    /// Whether a background maintenance task is currently in flight.
    /// Test/introspection.
    #[doc(hidden)]
    #[must_use]
    pub fn background_maintenance_in_flight(&self) -> bool {
        self.maintenance_scheduled.load(Ordering::Acquire)
    }

    /// Number of WAL **group-commit batch `fsync`s** performed since open. With
    /// per-write fsyncs this would equal the write count; group commit makes it
    /// strictly smaller when writes coalesce. Test/introspection.
    #[doc(hidden)]
    #[must_use]
    pub fn wal_batch_sync_count(&self) -> u64 {
        self.wal.batch_sync_count()
    }

    /// Number of live WAL segments (sealed + the active one). Test/introspection —
    /// used by the rotation test to assert writes spanned multiple segments and a
    /// flush GC'd the covered ones.
    #[doc(hidden)]
    #[must_use]
    pub fn wal_segment_count(&self) -> usize {
        self.wal.segment_count()
    }

    /// The live WAL segment numbers (ascending). Test/introspection.
    #[doc(hidden)]
    #[must_use]
    pub fn wal_segments(&self) -> Vec<u64> {
        self.wal.live_segments()
    }

    /// Total SSTable blocks fetched from disk since open (read amplification).
    /// Test/introspection — used to assert a Bloom-rejected point-miss reads no
    /// block.
    #[doc(hidden)]
    #[must_use]
    pub fn block_read_count(&self) -> u64 {
        self.lock().block_reads.load(Ordering::Relaxed)
    }

    /// Reset the block-read counter to zero. Test/introspection.
    #[doc(hidden)]
    pub fn reset_block_reads(&self) {
        self.lock().block_reads.store(0, Ordering::Relaxed);
    }

    /// Live SSTable count per level, as `(level, count)` ascending. Test/
    /// introspection — used to assert leveled compaction keeps L1+ runs
    /// non-overlapping and bounded.
    #[doc(hidden)]
    #[must_use]
    pub fn level_table_counts(&self) -> Vec<(u32, usize)> {
        let inner = self.lock();
        let mut by_level: BTreeMap<u32, usize> = BTreeMap::new();
        for t in &inner.manifest.tables {
            *by_level.entry(t.level).or_default() += 1;
        }
        by_level.into_iter().collect()
    }

    /// Whether every level ≥1 has non-overlapping key ranges among its tables.
    /// Test/introspection — the leveled-compaction invariant.
    #[doc(hidden)]
    #[must_use]
    pub fn levels_non_overlapping(&self) -> bool {
        let inner = self.lock();
        let mut by_level: BTreeMap<u32, Vec<(Key, Key)>> = BTreeMap::new();
        for t in &inner.manifest.tables {
            if t.level >= 1
                && let (Some(lo), Some(hi)) = (&t.min_key, &t.max_key)
            {
                by_level
                    .entry(t.level)
                    .or_default()
                    .push((lo.clone(), hi.clone()));
            }
        }
        for ranges in by_level.values_mut() {
            ranges.sort();
            for pair in ranges.windows(2) {
                // pair[0].1 (hi) must be strictly less than pair[1].0 (lo).
                if pair[0].1 >= pair[1].0 {
                    return false;
                }
            }
        }
        true
    }

    /// Write an **orphan** SSTable-named file (un-synced, never referenced by the
    /// manifest) to model a crash mid-flush / mid-compaction: the bytes exist on
    /// the buffered disk but a crash will drop them and recovery must ignore them.
    /// Test-only.
    #[doc(hidden)]
    pub async fn test_write_orphan_sstable(&self, marker: &[u8]) {
        let seq = self.lock().manifest.next_seq + 1;
        let file = self.sst_file(seq);
        // Append some bytes without syncing and without touching the manifest.
        let _ = self.env.append(&file, marker).await;
    }

    /// Every `(version, is_tombstone)` record physically present **on disk** (across
    /// all live SSTables, not the memtable) for `key`, ascending by version.
    /// Test/introspection — lets the GC test assert that shadowed versions and a
    /// reclaimed tombstone are physically gone, while a within-grace tombstone is
    /// still present.
    #[doc(hidden)]
    pub async fn test_disk_versions_of(&self, key: &[u8]) -> Vec<(Version, bool)> {
        let readers = {
            let inner = self.lock();
            inner.readers.clone()
        };
        let mut out: BTreeMap<Version, bool> = BTreeMap::new();
        for reader in &readers {
            if !reader.meta().may_contain(key) {
                continue;
            }
            for (k, v, slot) in reader.full_scan(&self.env).await.unwrap_or_default() {
                if k == key {
                    out.insert(v, slot.is_none());
                }
            }
        }
        out.into_iter().collect()
    }

    /// Inject a **durable** orphan WAL segment file at `seg` (appended + synced),
    /// modelling a covered segment that a crash left on disk after the manifest
    /// swap dropped it from the live set but before the GC `remove` ran. It must be
    /// below the live set to be treated as a covered orphan. Test-only.
    #[doc(hidden)]
    pub async fn test_write_orphan_wal_segment(&self, seg: u64, bytes: &[u8]) {
        let file = self.wal.segment_file(seg);
        let _ = self.env.append(&file, bytes).await;
        let _ = self.env.sync(&file).await;
    }

    // ---- admin / debug introspection (ADR 0020) -------------------------
    // Read-only projections of LSM + WAL state for the admin interface. Pure
    // reads — a brief lock taken and dropped, or a file size/read through the
    // `Env` disk seam; they never mutate engine state (the observe-only rule,
    // ADR 0015). The `flush_now`/`compact_now` admin *actions* below do mutate,
    // and say so.

    /// A lean, read-only view of every live SSTable's metadata (ascending by
    /// sequence) for the `/admin/storage/lsm` debug view — omits the per-table
    /// bloom bit vector.
    #[must_use]
    pub fn sstable_views(&self) -> Vec<SsTableView> {
        let inner = self.lock();
        let mut views: Vec<SsTableView> = inner
            .manifest
            .tables
            .iter()
            .map(|t| SsTableView {
                seq: t.seq,
                level: t.level,
                min_key: t.min_key.clone(),
                max_key: t.max_key.clone(),
                min_version: t.min_version,
                max_version: t.max_version,
                file_size: t.file_size,
                has_bloom: t.has_bloom,
                format: t.format,
            })
            .collect();
        views.sort_by_key(|v| v.seq);
        views
    }

    /// The number of distinct keys currently buffered in the memtable.
    #[must_use]
    pub fn memtable_len(&self) -> usize {
        self.lock().memtable.len()
    }

    /// The memtable's approximate live byte size (the flush-threshold counter).
    #[must_use]
    pub fn memtable_bytes(&self) -> usize {
        self.lock().memtable_bytes
    }

    /// Live WAL segments with their on-disk byte sizes (ascending). Reads each
    /// segment file's size through the `Env` disk seam.
    pub async fn wal_segment_sizes(&self) -> Vec<(u64, u64)> {
        let segs = self.wal.live_segments();
        let mut out = Vec::with_capacity(segs.len());
        for seg in segs {
            let size = self
                .env
                .size(&self.wal.segment_file(seg))
                .await
                .unwrap_or(0);
            out.push((seg, size));
        }
        out
    }

    /// The highest WAL sequence currently durable (fsynced).
    #[must_use]
    pub fn wal_durable_seq(&self) -> u64 {
        self.wal.durable_seq()
    }

    /// Cumulative WAL segment rotations since open.
    #[must_use]
    pub fn wal_rotation_count(&self) -> u64 {
        self.wal.rotation_count()
    }

    /// Decode the records of WAL segment `seg` into read-only views (in file
    /// order), reading the segment file through the `Env` disk seam. An absent or
    /// unreadable segment, or one that fails to decode (e.g. corruption), yields
    /// an empty vec — this is a best-effort admin/debug view (ADR 0020), not the
    /// load-bearing recovery path (`LsmEngine::open_with_metrics`), which does
    /// surface a decode failure as a hard error.
    pub async fn wal_segment_records(&self, seg: u64) -> Vec<WalRecordView> {
        let bytes = self
            .env
            .read(&self.wal.segment_file(seg))
            .await
            .unwrap_or_default();
        decode_wal(&bytes)
            .map(|(records, _consumed)| records.into_iter().map(WalRecordView::from).collect())
            .unwrap_or_default()
    }

    /// **Admin action (ADR 0020):** force-flush the memtable to an SSTable now,
    /// then run any compactions that become due. A no-op flush if the memtable is
    /// empty or a concurrent write is still durable-but-unapplied (`flush`
    /// enforces the `applies_in_flight == 0` invariant atomically with its
    /// snapshot, so a forced flush never GCs a WAL segment whose records aren't
    /// yet in the snapshot); compactions still run. Safe to call from a separate
    /// task while writes stream: the maintenance lock inside `flush` /
    /// `run_compaction` serializes it against writer-driven flushes/compactions,
    /// and the surgical memtable clear preserves any write applied mid-flush.
    /// Idempotent.
    ///
    /// # Errors
    /// Propagates a flush/compaction I/O failure.
    pub async fn flush_now(&self) -> Result<()> {
        self.flush().await?;
        while let Some(plan) = self.next_compaction() {
            self.run_compaction(plan).await?;
        }
        Ok(())
    }

    /// **Admin action (ADR 0020):** run all currently-due compactions to
    /// quiescence (L0→L1→L2…). Idempotent — a no-op when nothing is due.
    ///
    /// # Errors
    /// Propagates a compaction I/O failure.
    pub async fn compact_now(&self) -> Result<()> {
        while let Some(plan) = self.next_compaction() {
            self.run_compaction(plan).await?;
        }
        Ok(())
    }
}

/// A lean, read-only view of one live SSTable's metadata for the admin/debug
/// interface (ADR 0020) — a projection of `SsTableMeta` that omits the bloom bit
/// vector. All fields are plain data so a consumer can render them as it likes.
#[derive(Clone, Debug)]
pub struct SsTableView {
    /// SSTable sequence number (its file is `sst-{seq:06}`).
    pub seq: u64,
    /// LSM level (0 = flush tier; 1+ = non-overlapping runs).
    pub level: u32,
    /// Smallest key in the table (`None` only for an empty table).
    pub min_key: Option<Key>,
    /// Largest key in the table.
    pub max_key: Option<Key>,
    /// Smallest MVCC version stored.
    pub min_version: Version,
    /// Largest MVCC version stored.
    pub max_version: Version,
    /// Total file size in bytes.
    pub file_size: u64,
    /// Whether a bloom filter was built for the table.
    pub has_bloom: bool,
    /// On-disk block format version (single format today: compression-capable
    /// framing + shared-prefix keys; a per-table tag for introspection).
    pub format: u32,
}

/// A read-only, serialization-light view of one WAL record for the admin
/// `/admin/storage/wal/segment` debug view (ADR 0020). Values are summarized by
/// length rather than echoed, so a dump of a large segment stays small.
#[derive(Clone, Debug)]
pub enum WalRecordView {
    /// A single-key put; `value_len` is the value's byte length.
    Put {
        key: Key,
        version: Version,
        value_len: usize,
    },
    /// A single-key delete (tombstone).
    Delete { key: Key, version: Version },
    /// A range delete; `keys` is the count of keys it tombstoned.
    DeleteRange {
        start: Key,
        end: Key,
        keys: usize,
        version: Version,
    },
    /// A write batch; `ops` is the operation count.
    Batch { version: Version, ops: usize },
    /// A per-key LWW merge batch; `ops` is the operation count and `max_version`
    /// the highest per-op version it carries.
    MergeBatch { ops: usize, max_version: Version },
}

impl From<WalRecord> for WalRecordView {
    fn from(r: WalRecord) -> Self {
        match r {
            WalRecord::Put {
                key,
                value,
                version,
            } => WalRecordView::Put {
                key,
                version,
                value_len: value.len(),
            },
            WalRecord::Delete { key, version } => WalRecordView::Delete { key, version },
            WalRecord::DeleteRange {
                start,
                end,
                keys,
                version,
            } => WalRecordView::DeleteRange {
                start,
                end,
                keys: keys.len(),
                version,
            },
            WalRecord::Batch { version, ops } => WalRecordView::Batch {
                version,
                ops: ops.len(),
            },
            WalRecord::MergeBatch { ops } => WalRecordView::MergeBatch {
                max_version: ops.iter().map(|o| o.version).max().unwrap_or(0),
                ops: ops.len(),
            },
        }
    }
}

#[async_trait::async_trait]
impl<E: Env> StorageEngine for LsmEngine<E> {
    type Snapshot = LsmSnapshot<E>;

    async fn put(&self, key: &[u8], value: &[u8], version: Version) -> Result<()> {
        {
            let inner = self.lock();
            if version <= inner.manifest.max_version {
                return Err(StorageError::NonMonotonicVersion {
                    got: version,
                    latest: inner.manifest.max_version,
                });
            }
        }
        self.log_and_apply(
            &WalRecord::Put {
                key: key.to_vec(),
                value: value.to_vec(),
                version,
            },
            |inner| {
                inner.apply_put(key, value, version);
                inner.manifest.max_version = version;
            },
        )
        .await?;
        self.after_write_maintenance().await
    }

    async fn merge(&self, key: &[u8], value: &[u8], version: Version) -> Result<bool> {
        // Per-key LWW: apply only if strictly newer than this key's own latest
        // anywhere in the engine. Skipped under `trust_monotonic_versions` (see
        // `LsmOptions`): the caller guarantees `version` is already newer than
        // anything this key holds, so the read is structurally always a winner.
        if !self.opts.trust_monotonic_versions
            && self
                .latest_version_of(key)
                .await?
                .is_some_and(|cur| version <= cur)
        {
            return Ok(false);
        }
        self.log_and_apply(
            &WalRecord::Put {
                key: key.to_vec(),
                value: value.to_vec(),
                version,
            },
            |inner| {
                inner.apply_put(key, value, version);
                inner.manifest.max_version = inner.manifest.max_version.max(version);
            },
        )
        .await?;
        self.after_write_maintenance().await?;
        Ok(true)
    }

    async fn merge_tombstone(&self, key: &[u8], version: Version) -> Result<bool> {
        // See `merge`: the read is skipped under `trust_monotonic_versions`.
        if !self.opts.trust_monotonic_versions
            && self
                .latest_version_of(key)
                .await?
                .is_some_and(|cur| version <= cur)
        {
            return Ok(false);
        }
        self.log_and_apply(
            &WalRecord::Delete {
                key: key.to_vec(),
                version,
            },
            |inner| {
                inner.apply_delete(key, version);
                inner.manifest.max_version = inner.manifest.max_version.max(version);
            },
        )
        .await?;
        self.after_write_maintenance().await?;
        Ok(true)
    }

    async fn merge_batch(&self, ops: Vec<MergeOp>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        // Decide per-op LWW winners *before* logging, matching `merge`: an op
        // takes effect only if its version is strictly greater than the key's
        // current latest — considering both the engine's state AND earlier
        // winners for the same key in this same batch (which aren't in the
        // memtable yet, since we apply only after the single sync). Logging just
        // the winners keeps WAL replay a pure re-insert, so a crash-recovered
        // memtable is byte-identical to applying the ops via individual `merge`s.
        //
        // Under `trust_monotonic_versions` (see `LsmOptions`), the per-op read
        // against *already-durable engine state* (`latest_version_of`) is
        // skipped — the caller guarantees every op's version is already newer
        // than anything the engine holds for that key. Multiple ops for the
        // *same* key within one batch are still deduped against each other
        // (cheap, in-memory, no read): the contract is about the engine's
        // durable state, not about a caller never repeating a key mid-batch.
        let mut in_batch: BTreeMap<Key, Version> = BTreeMap::new();
        let mut winners: Vec<MergeRec> = Vec::with_capacity(ops.len());
        for op in ops {
            let engine_latest = if self.opts.trust_monotonic_versions {
                None
            } else {
                self.latest_version_of(&op.key).await?
            };
            let prior = in_batch.get(&op.key).copied();
            let current = match (engine_latest, prior) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) | (None, Some(a)) => Some(a),
                (None, None) => None,
            };
            if current.is_some_and(|c| op.version <= c) {
                continue; // LWW loser: a newer version already holds this key.
            }
            in_batch.insert(op.key.clone(), op.version);
            winners.push(MergeRec {
                key: op.key,
                value: op.value,
                version: op.version,
            });
        }
        if winners.is_empty() {
            return Ok(());
        }
        // One WAL record, one `fsync`, one memtable-apply pass under the lock —
        // the whole point: N merges pay a single group-commit sync, not N.
        self.log_and_apply(
            &WalRecord::MergeBatch {
                ops: winners.clone(),
            },
            |inner| {
                for w in &winners {
                    match &w.value {
                        Some(value) => inner.apply_put(&w.key, value, w.version),
                        None => inner.apply_delete(&w.key, w.version),
                    }
                    inner.manifest.max_version = inner.manifest.max_version.max(w.version);
                }
            },
        )
        .await?;
        self.after_write_maintenance().await
    }

    async fn delete(&self, key: &[u8], version: Version) -> Result<()> {
        {
            let inner = self.lock();
            if version <= inner.manifest.max_version {
                return Err(StorageError::NonMonotonicVersion {
                    got: version,
                    latest: inner.manifest.max_version,
                });
            }
        }
        self.log_and_apply(
            &WalRecord::Delete {
                key: key.to_vec(),
                version,
            },
            |inner| {
                inner.apply_delete(key, version);
                inner.manifest.max_version = version;
            },
        )
        .await?;
        self.after_write_maintenance().await
    }

    async fn delete_range(&self, start: &[u8], end: &[u8], version: Version) -> Result<()> {
        if start > end {
            return Err(StorageError::InvalidRange);
        }
        {
            let inner = self.lock();
            if version <= inner.manifest.max_version {
                return Err(StorageError::NonMonotonicVersion {
                    got: version,
                    latest: inner.manifest.max_version,
                });
            }
        }
        // Expand to the live keys in range (memtable + SSTables) so the WAL record
        // and the apply are a pure key set — replay needs no live-state lookup.
        let keys = self.live_keys_in_range(start, end, version).await?;
        self.log_and_apply(
            &WalRecord::DeleteRange {
                start: start.to_vec(),
                end: end.to_vec(),
                keys: keys.clone(),
                version,
            },
            |inner| {
                for k in &keys {
                    inner.apply_delete(k, version);
                }
                inner.manifest.max_version = version;
            },
        )
        .await?;
        self.after_write_maintenance().await
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        for op in &batch.ops {
            if let WriteOp::DeleteRange { start, end } = op
                && start > end
            {
                return Err(StorageError::InvalidRange);
            }
        }
        {
            let inner = self.lock();
            if batch.version <= inner.manifest.max_version {
                return Err(StorageError::NonMonotonicVersion {
                    got: batch.version,
                    latest: inner.manifest.max_version,
                });
            }
        }
        // Pre-expand range deletes against current state (sees only ops before it
        // in *prior* state — matching MemoryEngine, which applies in order at one
        // version; within one batch all ops share the version, so ordering among
        // them is moot for which keys a range delete covers).
        let mut logged = Vec::with_capacity(batch.ops.len());
        for op in &batch.ops {
            match op {
                WriteOp::Put { key, value } => logged.push(BatchOp::Put {
                    key: key.clone(),
                    value: value.clone(),
                }),
                WriteOp::Delete { key } => logged.push(BatchOp::Delete { key: key.clone() }),
                WriteOp::DeleteRange { start, end } => {
                    let keys = self.live_keys_in_range(start, end, batch.version).await?;
                    logged.push(BatchOp::DeleteKeys { keys });
                }
            }
        }
        self.log_and_apply(
            &WalRecord::Batch {
                version: batch.version,
                ops: logged.clone(),
            },
            |inner| {
                for op in &logged {
                    match op {
                        BatchOp::Put { key, value } => inner.apply_put(key, value, batch.version),
                        BatchOp::Delete { key } => inner.apply_delete(key, batch.version),
                        BatchOp::DeleteKeys { keys } => {
                            for k in keys {
                                inner.apply_delete(k, batch.version);
                            }
                        }
                    }
                }
                inner.manifest.max_version = batch.version;
            },
        )
        .await?;
        self.after_write_maintenance().await
    }

    async fn get(&self, key: &[u8]) -> Result<Option<VersionedValue>> {
        self.read_at(key, Version::MAX).await
    }

    async fn get_at(&self, key: &[u8], version: Version) -> Result<Option<VersionedValue>> {
        self.read_at(key, version).await
    }

    async fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Key, VersionedValue)>> {
        if start > end {
            return Err(StorageError::InvalidRange);
        }
        self.scan_at(start, end, Version::MAX).await
    }

    async fn scan_at(
        &self,
        start: &[u8],
        end: &[u8],
        version: Version,
    ) -> Result<Vec<(Key, VersionedValue)>> {
        if start > end {
            return Err(StorageError::InvalidRange);
        }
        // Resolves to this type's own private inherent `scan_at` (line ~979)
        // — inherent methods take priority over trait methods of the same
        // name, exactly like `scan` above already relies on for `Version::MAX`.
        self.scan_at(start, end, version).await
    }

    async fn entries(&self) -> Result<Vec<(Key, VersionedValue)>> {
        let merged = self.merged_latest().await?;
        Ok(merged
            .into_iter()
            .filter_map(|(k, (v, slot))| {
                slot.map(|value| (k, VersionedValue { version: v, value }))
            })
            .collect())
    }

    async fn entries_at(&self, version: Version) -> Result<Vec<(Key, VersionedValue)>> {
        let merged = self.merged_at(&[], None, version).await?;
        Ok(merged
            .into_iter()
            .filter_map(|(k, (v, slot))| {
                slot.map(|value| (k, VersionedValue { version: v, value }))
            })
            .collect())
    }

    async fn entries_with_tombstones(&self) -> Result<Vec<(Key, Option<Value>, Version)>> {
        let merged = self.merged_latest().await?;
        Ok(merged
            .into_iter()
            .map(|(k, (v, slot))| (k, slot, v))
            .collect())
    }

    async fn scan_with_tombstones(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Key, Option<Value>, Version)>> {
        // `merged_at` is already ranged and tombstone-retaining — the same
        // machinery `entries_with_tombstones` uses, bounded instead of
        // whole-keyspace (ADR 0050: the split-build driver's per-chunk reads
        // must not pay O(engine) per chunk).
        let merged = self.merged_at(start, Some(end), Version::MAX).await?;
        Ok(merged
            .into_iter()
            .map(|(k, (v, slot))| (k, slot, v))
            .collect())
    }

    /// **Cheap, non-materializing** override (ADR 0034): reads only in-memory
    /// metadata already held for other purposes — the memtable (a real,
    /// range-scoped byte sum, since a `BTreeMap` range query costs nothing
    /// extra) plus every SSTable whose own `[min_key, max_key]` overlaps
    /// `[start, end)` at all (`sstable_overlaps`). No disk read, no block
    /// fetch, mirroring `sstable_views`/`memtable_len`'s introspection cost.
    ///
    /// **Bias: over-estimates, deliberately, like [`CpGroup::approx_key_count`]'s
    /// sibling key-count estimate** (`animusd`), for two reasons: (1) an
    /// SSTable that merely *overlaps* the range — rather than being wholly
    /// contained in it — counts its **entire** `file_size`, since no
    /// per-block byte breakdown is available without reading blocks; since
    /// ADR 0028 one physical engine can host several tablets' data on the
    /// same file (particularly at L0, the unpartitioned flush tier), so a
    /// table spanning a sibling tablet's keys inflates this estimate by that
    /// sibling's share. (2) the overlap check itself uses each table's own
    /// `[min_key, max_key]`, a superset of its actual on-disk key set. Both
    /// biases only ever over-count, never under-count, so a tablet that
    /// might need splitting is never silently missed — the auto-split loop's
    /// materializing confirm step (which reads real scoped bytes) corrects
    /// the estimate before a split actually commits. In practice this stays
    /// tight once data is compacted into range-partitioned, non-overlapping
    /// L1+ runs (leveled compaction's whole point); it is loosest for
    /// still-unflushed/L0 data on a heavily shared engine.
    async fn approx_bytes_in_range(&self, start: &[u8], end: Option<&[u8]>) -> Result<u64> {
        let inner = self.lock();
        let memtable_bytes: u64 = match end {
            Some(e) => inner
                .memtable
                .range(start.to_vec()..e.to_vec())
                .map(|(k, h)| history_bytes(k, h))
                .sum(),
            None => inner
                .memtable
                .range(start.to_vec()..)
                .map(|(k, h)| history_bytes(k, h))
                .sum(),
        };
        let sstable_bytes: u64 = inner
            .manifest
            .tables
            .iter()
            .filter(|t| sstable_overlaps(t, start, end))
            .map(|t| t.file_size)
            .sum();
        Ok(memtable_bytes + sstable_bytes)
    }

    fn snapshot(&self) -> LsmSnapshot<E> {
        let version = self.lock().manifest.max_version;
        self.hold_snapshot(version);
        LsmSnapshot {
            engine: self.clone(),
            version,
        }
    }

    fn latest_version(&self) -> Version {
        self.lock().manifest.max_version
    }
}

impl<E: Env> LsmEngine<E> {
    /// Live keys in `[start, end)` as of `version` (memtable + SSTables), for
    /// expanding a range delete. Wraps the merged read and keeps only live keys.
    async fn live_keys_in_range(
        &self,
        start: &[u8],
        end: &[u8],
        version: Version,
    ) -> Result<Vec<Key>> {
        let merged = self.merged_at(start, Some(end), version).await?;
        Ok(merged
            .into_iter()
            .filter_map(|(k, (_, slot))| slot.map(|_| k))
            .collect())
    }

    /// Register a hold on `version` (an [`LsmSnapshot`] pinned there is now
    /// live). Refcounted so several snapshots at the same version (taken with
    /// no writes in between) release independently.
    fn hold_snapshot(&self, version: Version) {
        *self.lock().held_snapshots.entry(version).or_insert(0) += 1;
    }

    /// Release one hold on `version`, taken by [`Self::hold_snapshot`].
    fn release_snapshot(&self, version: Version) {
        let mut inner = self.lock();
        if let Some(count) = inner.held_snapshots.get_mut(&version) {
            *count -= 1;
            if *count == 0 {
                inner.held_snapshots.remove(&version);
            }
        }
    }
}

/// A snapshot of an [`LsmEngine`] pinned at a version. Reads filter to records at
/// or below that version, so later (higher-version) writes are invisible.
///
/// Holding a snapshot **pins the tombstone-GC floor** below its version (see
/// [`LsmEngine::snapshot`] / the module docs' tombstone-GC section), so a
/// long-held snapshot's reads stay correct even if compaction runs while it is
/// alive; the pin is released when the last clone of this snapshot drops.
pub struct LsmSnapshot<E: Env> {
    engine: LsmEngine<E>,
    version: Version,
}

impl<E: Env> Clone for LsmSnapshot<E> {
    fn clone(&self) -> Self {
        self.engine.hold_snapshot(self.version);
        Self {
            engine: self.engine.clone(),
            version: self.version,
        }
    }
}

impl<E: Env> Drop for LsmSnapshot<E> {
    fn drop(&mut self) {
        self.engine.release_snapshot(self.version);
    }
}

#[async_trait::async_trait]
impl<E: Env> Snapshot for LsmSnapshot<E> {
    fn version(&self) -> Version {
        self.version
    }

    async fn get(&self, key: &[u8]) -> Option<VersionedValue> {
        self.engine.read_at(key, self.version).await.ok().flatten()
    }

    async fn scan(&self, start: &[u8], end: &[u8]) -> Vec<(Key, VersionedValue)> {
        if start > end {
            return Vec::new();
        }
        self.engine
            .scan_at(start, end, self.version)
            .await
            .unwrap_or_default()
    }
}

/// A chosen compaction: merge `source_level` (and the overlapping
/// `source_level + 1` tables) down into `source_level + 1`.
#[derive(Clone, Copy, Debug)]
struct CompactionPlan {
    source_level: u32,
}

/// Whether table `meta`'s key range overlaps the inclusive range `[lo, hi]`.
fn ranges_overlap(meta: &SsTableMeta, lo: &[u8], hi: &[u8]) -> bool {
    match (&meta.min_key, &meta.max_key) {
        (Some(mlo), Some(mhi)) => mlo.as_slice() <= hi && lo <= mhi.as_slice(),
        _ => false,
    }
}

/// Partition merged `(key, version) -> slot` records into non-overlapping runs,
/// each ≈`target_bytes` of record payload. A run boundary is only ever placed
/// between two **distinct keys**, so all versions of a key land in the same run
/// and runs own disjoint key ranges (the leveled-layout invariant). Returns at
/// least one (possibly empty) partition is avoided — an empty input yields no
/// partitions.
fn partition_records(
    merged: &BTreeMap<(Key, Version), Option<Value>>,
    target_bytes: usize,
) -> Vec<Vec<Record>> {
    let target = target_bytes.max(1);
    let mut partitions: Vec<Vec<Record>> = Vec::new();
    let mut current: Vec<Record> = Vec::new();
    let mut current_bytes = 0usize;
    let mut prev_key: Option<&Key> = None;

    for ((key, version), slot) in merged {
        let on_new_key = prev_key != Some(key);
        // Only split on a key boundary, and only once the current run is full.
        if on_new_key && current_bytes >= target && !current.is_empty() {
            partitions.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += key.len() + slot.as_ref().map_or(0, Vec::len) + 16;
        current.push(Record {
            key: key.clone(),
            version: *version,
            value: slot.clone(),
        });
        prev_key = Some(key);
    }
    if !current.is_empty() {
        partitions.push(current);
    }
    partitions
}

/// Reclaim obsolete tombstones (and the versions they shadow) from a compaction's
/// merged `(key, version) -> slot` records **in place**, preserving every read at
/// a version *above* `gc_floor`.
///
/// Operating per key on its versions in ascending order:
///
/// - The **floor anchor** is the greatest version `<= gc_floor`. Every version
///   strictly below it is shadowed for all reads at versions `>= ` the anchor's,
///   and reads at versions `> gc_floor` always see the anchor or a higher version,
///   so the sub-anchor versions are invisible to the retained window and are
///   dropped. (This compacts MVCC history below the floor for live keys too — pure
///   space reclamation that no `get_at(v > gc_floor)` can observe.)
/// - If the floor anchor is itself a **tombstone**, it reads as *absent*. With
///   everything older dropped, absence is preserved by simply removing it — **iff**
///   no deeper, uncompacted level overlaps the key (a deeper older value would
///   resurface, resurrecting the key). When a deeper table could hold the key we
///   keep the anchor tombstone (still dropping the versions below it).
///
/// Versions above `gc_floor` are never touched, so the `(gc_floor, max_version]`
/// window — including any tombstone in it — is observationally identical to before
/// GC, and the differential proptest against `MemoryEngine` stays green for it.
fn gc_obsolete_records(
    merged: &mut BTreeMap<(Key, Version), Option<Value>>,
    gc_floor: Version,
    deeper_ranges: &[(Key, Key)],
) {
    // Per key, gather its sub-floor versions (records are globally sorted by
    // (key, version), so they arrive grouped and ascending), decide which to drop,
    // and collect the decisions; then apply them in a second pass.
    let mut to_drop: Vec<(Key, Version)> = Vec::new();
    // Versions of the current key that are `<= gc_floor`, ascending: (version, is_tombstone).
    let mut floor_versions: Vec<(Version, bool)> = Vec::new();
    let mut cur_key: Option<Key> = None;

    // Closes out the current key: drop everything below its floor anchor, and the
    // anchor itself when it is a tombstone with no deeper level holding the key.
    fn flush_key(
        key: &Key,
        floor_versions: &mut Vec<(Version, bool)>,
        deeper_ranges: &[(Key, Key)],
        to_drop: &mut Vec<(Key, Version)>,
    ) {
        if let Some(&(anchor_v, anchor_is_tombstone)) = floor_versions.last() {
            for &(v, _) in floor_versions.iter() {
                if v < anchor_v {
                    to_drop.push((key.clone(), v));
                }
            }
            if anchor_is_tombstone && !key_overlaps_any(key, deeper_ranges) {
                to_drop.push((key.clone(), anchor_v));
            }
        }
        floor_versions.clear();
    }

    for ((key, version), slot) in merged.iter() {
        if cur_key.as_ref() != Some(key) {
            if let Some(prev) = &cur_key {
                flush_key(prev, &mut floor_versions, deeper_ranges, &mut to_drop);
            }
            cur_key = Some(key.clone());
        }
        if *version <= gc_floor {
            floor_versions.push((*version, slot.is_none()));
        }
    }
    if let Some(prev) = &cur_key {
        flush_key(prev, &mut floor_versions, deeper_ranges, &mut to_drop);
    }

    for k in to_drop {
        merged.remove(&k);
    }
}

/// Whether `key` falls inside any of the inclusive `[lo, hi]` ranges.
fn key_overlaps_any(key: &[u8], ranges: &[(Key, Key)]) -> bool {
    ranges
        .iter()
        .any(|(lo, hi)| key >= lo.as_slice() && key <= hi.as_slice())
}

/// Fold the winning record for `key` into `merged`: a strictly greater version
/// wins; equal versions are deterministic no-ops (the later source — memtable —
/// is applied last and so wins ties, matching newest-source-wins).
fn merge_winner(
    merged: &mut BTreeMap<Key, (Version, Option<Value>)>,
    key: Key,
    version: Version,
    slot: Option<Value>,
) {
    match merged.get(&key) {
        Some((cur, _)) if *cur > version => {}
        _ => {
            merged.insert(key, (version, slot));
        }
    }
}

/// Recompute the memtable's approximate byte accounting, matching what
/// `apply_put`/`apply_delete` accumulate: `key + value + 16` per
/// `(key, version)` slot. Used after a flush's surgical clear, where the
/// residue is only the writes that raced the flush (small), so a full
/// recomputation is cheap and exact.
fn memtable_bytes_of(memtable: &BTreeMap<Key, History>) -> usize {
    memtable
        .iter()
        .map(|(key, history)| history_bytes(key, history))
        .sum::<u64>() as usize
}

/// One key's full-history byte accounting: `key + value + 16` per
/// `(key, version)` slot — the same formula `apply_put`/`apply_delete`
/// accumulate into `memtable_bytes`, factored out so
/// [`approx_bytes_in_range`](StorageEngine::approx_bytes_in_range)'s
/// range-scoped sum uses the identical accounting as the whole-memtable one.
fn history_bytes(key: &Key, history: &History) -> u64 {
    history
        .values()
        .map(|slot| (key.len() + slot.as_ref().map_or(0, Vec::len) + 16) as u64)
        .sum()
}

/// Whether SSTable `t`'s own `[min_key, max_key]` range overlaps the
/// half-open range `[start, end)` (`end == None` unbounded above) at all — an
/// empty table (`min_key`/`max_key` both `None`) never overlaps. Used by
/// [`approx_bytes_in_range`](StorageEngine::approx_bytes_in_range)'s `LsmEngine`
/// override to decide which tables' `file_size` to count; see that override's
/// doc for why "overlaps at all" (rather than "fully contained") is the
/// deliberately over-counting choice.
fn sstable_overlaps(t: &SsTableMeta, start: &[u8], end: Option<&[u8]>) -> bool {
    let (Some(min_key), Some(max_key)) = (&t.min_key, &t.max_key) else {
        return false;
    };
    max_key.as_slice() >= start && end.is_none_or(|e| min_key.as_slice() < e)
}

/// Flatten a memtable into sorted SSTable records (`(key asc, version asc)`),
/// keeping every version (full MVCC history) and tombstones.
fn flatten_memtable(memtable: &BTreeMap<Key, History>) -> Vec<sstable::Record> {
    let mut out = Vec::new();
    for (key, history) in memtable {
        for (&version, slot) in history {
            out.push(sstable::Record {
                key: key.clone(),
                version,
                value: slot.clone(),
            });
        }
    }
    out
}

fn record_max_version(record: &WalRecord) -> Version {
    match record {
        WalRecord::Put { version, .. }
        | WalRecord::Delete { version, .. }
        | WalRecord::DeleteRange { version, .. }
        | WalRecord::Batch { version, .. } => *version,
        WalRecord::MergeBatch { ops } => ops.iter().map(|o| o.version).max().unwrap_or(0),
    }
}

fn apply_wal_record(memtable: &mut BTreeMap<Key, History>, bytes: &mut usize, record: WalRecord) {
    let mut put =
        |memtable: &mut BTreeMap<Key, History>, k: Key, v: Option<Value>, ver: Version| {
            *bytes += k.len() + v.as_ref().map_or(0, Vec::len) + 16;
            memtable.entry(k).or_default().insert(ver, v);
        };
    match record {
        WalRecord::Put {
            key,
            value,
            version,
        } => put(memtable, key, Some(value), version),
        WalRecord::Delete { key, version } => put(memtable, key, None, version),
        WalRecord::DeleteRange { keys, version, .. } => {
            for k in keys {
                put(memtable, k, None, version);
            }
        }
        WalRecord::Batch { version, ops } => {
            for op in ops {
                match op {
                    BatchOp::Put { key, value } => put(memtable, key, Some(value), version),
                    BatchOp::Delete { key } => put(memtable, key, None, version),
                    BatchOp::DeleteKeys { keys } => {
                        for k in keys {
                            put(memtable, k, None, version);
                        }
                    }
                }
            }
        }
        WalRecord::MergeBatch { ops } => {
            for op in ops {
                put(memtable, op.key, op.value, op.version);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Binary WAL record codec: length-prefixed + CRC32 framing
// ---------------------------------------------------------------------------
//
// Each record is framed as:
//   len(u32 BE) | crc32(u32 BE, of the `len`-byte payload only) | payload
//
// replacing the old newline-delimited `serde_json` encoding (a `Vec<u8>` value
// serializes as a decimal-number JSON array — 3-6x bigger than the raw bytes on
// the durability-critical fsync path). The payload itself is a compact,
// hand-rolled binary encoding of `WalRecord` (mirrors the MANIFEST codec's
// style: a tag byte, big-endian ints, length-prefixed byte strings — see
// `put_bytes`/`put_opt_bytes`/`Cursor` below, shared with the manifest codec).
//
// The CRC turns at-rest corruption of a durable record into a loud decode
// error instead of `decode_wal`'s old behavior of silently skipping *any*
// malformed line. Distinguishing a legitimate crash-torn trailing record (never
// synced, so never acked — tolerated) from real corruption (a durable record's
// bytes flipped at rest — must be loud) is not always possible from a single
// frame in isolation: a crash can leave a torn, possibly bit-flipped fragment
// at the tail that happens to *look* like a short or corrupt frame, and that
// must open cleanly. The rule `decode_wal` applies: a frame that fails to parse
// (not enough bytes, or a CRC mismatch) is tolerated **only if it is provably
// the last recoverable thing in the buffer** — i.e. no valid, checksummed frame
// exists anywhere after it (`wal_resync_point`). A crash can only ever tear the
// physical *end* of a file (the durable prefix from prior syncs is untouched —
// see the module docs' crash-safety argument), so any bad frame that is
// *followed* by more valid frames cannot be a torn tail; it can only be
// corruption of previously-durable data, and is reported as a hard error rather
// than silently dropping the (still-present, still-valid) records after it.

/// Bytes in one frame's header (`len: u32` + `crc32: u32`).
const WAL_FRAME_HEADER_BYTES: usize = 8;

/// Frame + encode one [`WalRecord`] for the WAL.
fn encode_wal(record: &WalRecord) -> Vec<u8> {
    let payload = encode_wal_record(record);
    let crc = crc32fast::hash(&payload);
    let mut out = Vec::with_capacity(WAL_FRAME_HEADER_BYTES + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(&payload);
    out
}

/// Attempt to parse one complete, checksum-valid, structurally well-formed WAL
/// frame starting at `bytes[pos..]`. `None` on any failure — not enough bytes
/// for the header/payload, a CRC mismatch, or a malformed payload — with no
/// judgment about *why* (the caller, [`decode_wal`], decides whether that is a
/// tolerable trailing tear or real corruption).
fn try_parse_wal_frame(bytes: &[u8], pos: usize) -> Option<(WalRecord, usize)> {
    if pos + WAL_FRAME_HEADER_BYTES > bytes.len() {
        return None;
    }
    let len = u32::from_be_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
    let crc = u32::from_be_bytes(
        bytes[pos + 4..pos + WAL_FRAME_HEADER_BYTES]
            .try_into()
            .ok()?,
    );
    let payload_start = pos + WAL_FRAME_HEADER_BYTES;
    let payload_end = payload_start.checked_add(len)?;
    if payload_end > bytes.len() {
        return None;
    }
    let payload = &bytes[payload_start..payload_end];
    if crc32fast::hash(payload) != crc {
        return None;
    }
    let record = decode_wal_record(payload).ok()?;
    Some((record, payload_end))
}

/// Scan forward from `start` for the first offset where a complete,
/// checksum-valid WAL frame parses. Used only to prove a parse failure earlier
/// in the buffer is *not* a legitimate torn tail (nothing recoverable can
/// follow a genuine tear, since a crash only ever tears the physical end of a
/// file).
fn wal_resync_point(bytes: &[u8], start: usize) -> Option<usize> {
    (start..bytes.len()).find(|&p| try_parse_wal_frame(bytes, p).is_some())
}

/// Decode a WAL segment's raw bytes into records plus how many leading bytes
/// formed complete, valid frames (the recovery point the segment should be
/// truncated to before further appends ride it — see
/// `LsmEngine::open_with_metrics`).
///
/// Tolerates **only** a genuinely torn trailing frame: an un-synced write cut
/// short by a crash, which was never acked and so is safe to drop silently. A
/// parse failure that is *not* the tail — i.e. a valid frame still exists
/// somewhere after it — can only be at-rest corruption of previously durable
/// data (a crash cannot touch anything but the physical end of the file), and
/// is surfaced loudly instead of silently truncating history.
///
/// # Errors
/// Returns [`StorageError::Backend`] when a frame fails to parse and a valid
/// frame is still found later in `bytes` (proof this was not a torn tail).
fn decode_wal(bytes: &[u8]) -> Result<(Vec<WalRecord>, usize)> {
    let mut records = Vec::new();
    let mut pos = 0usize;
    while pos < bytes.len() {
        match try_parse_wal_frame(bytes, pos) {
            Some((record, next)) => {
                records.push(record);
                pos = next;
            }
            None => {
                if wal_resync_point(bytes, pos + 1).is_some() {
                    return Err(StorageError::Backend(format!(
                        "corrupt WAL record at byte offset {pos}: a valid record \
                         still parses later in the file, so this is not a torn \
                         tail — refusing to silently drop history"
                    )));
                }
                break;
            }
        }
    }
    Ok((records, pos))
}

/// Encode a [`WalRecord`]'s payload (tag byte + fields; see the codec-level
/// docs above `try_parse_wal_frame`). Reuses `put_bytes`/`put_opt_bytes` from
/// the manifest codec below (plain free functions, not manifest-specific).
fn encode_wal_record(record: &WalRecord) -> Vec<u8> {
    let mut out = Vec::new();
    match record {
        WalRecord::Put {
            key,
            value,
            version,
        } => {
            out.push(0);
            put_bytes(&mut out, key);
            put_bytes(&mut out, value);
            out.extend_from_slice(&version.to_be_bytes());
        }
        WalRecord::Delete { key, version } => {
            out.push(1);
            put_bytes(&mut out, key);
            out.extend_from_slice(&version.to_be_bytes());
        }
        WalRecord::DeleteRange {
            start,
            end,
            keys,
            version,
        } => {
            out.push(2);
            put_bytes(&mut out, start);
            put_bytes(&mut out, end);
            out.extend_from_slice(&(keys.len() as u32).to_be_bytes());
            for k in keys {
                put_bytes(&mut out, k);
            }
            out.extend_from_slice(&version.to_be_bytes());
        }
        WalRecord::Batch { version, ops } => {
            out.push(3);
            out.extend_from_slice(&version.to_be_bytes());
            out.extend_from_slice(&(ops.len() as u32).to_be_bytes());
            for op in ops {
                encode_batch_op(&mut out, op);
            }
        }
        WalRecord::MergeBatch { ops } => {
            out.push(4);
            out.extend_from_slice(&(ops.len() as u32).to_be_bytes());
            for op in ops {
                encode_merge_rec(&mut out, op);
            }
        }
    }
    out
}

fn encode_batch_op(out: &mut Vec<u8>, op: &BatchOp) {
    match op {
        BatchOp::Put { key, value } => {
            out.push(0);
            put_bytes(out, key);
            put_bytes(out, value);
        }
        BatchOp::Delete { key } => {
            out.push(1);
            put_bytes(out, key);
        }
        BatchOp::DeleteKeys { keys } => {
            out.push(2);
            out.extend_from_slice(&(keys.len() as u32).to_be_bytes());
            for k in keys {
                put_bytes(out, k);
            }
        }
    }
}

fn encode_merge_rec(out: &mut Vec<u8>, rec: &MergeRec) {
    put_bytes(out, &rec.key);
    put_opt_bytes(out, &rec.value);
    out.extend_from_slice(&rec.version.to_be_bytes());
}

/// Decode one [`WalRecord`] from an already length-framed + CRC-verified
/// payload. Bounds-checked throughout via [`Cursor`] (shared with the manifest
/// codec), so a malformed payload is a clean error, never a panic; any trailing
/// bytes left after decoding are also rejected (the payload must be exactly
/// consumed — leftover bytes mean the length didn't match the content, which a
/// passing CRC makes vanishingly unlikely but is still checked).
fn decode_wal_record(bytes: &[u8]) -> Result<WalRecord> {
    let mut c = Cursor::new(bytes);
    let tag = c.u8()?;
    let record = match tag {
        0 => {
            let key = c.bytes()?;
            let value = c.bytes()?;
            let version = c.u64()?;
            WalRecord::Put {
                key,
                value,
                version,
            }
        }
        1 => {
            let key = c.bytes()?;
            let version = c.u64()?;
            WalRecord::Delete { key, version }
        }
        2 => {
            let start = c.bytes()?;
            let end = c.bytes()?;
            let n = c.u32()? as usize;
            let mut keys = Vec::with_capacity(n);
            for _ in 0..n {
                keys.push(c.bytes()?);
            }
            let version = c.u64()?;
            WalRecord::DeleteRange {
                start,
                end,
                keys,
                version,
            }
        }
        3 => {
            let version = c.u64()?;
            let n = c.u32()? as usize;
            let mut ops = Vec::with_capacity(n);
            for _ in 0..n {
                ops.push(decode_batch_op(&mut c)?);
            }
            WalRecord::Batch { version, ops }
        }
        4 => {
            let n = c.u32()? as usize;
            let mut ops = Vec::with_capacity(n);
            for _ in 0..n {
                ops.push(decode_merge_rec(&mut c)?);
            }
            WalRecord::MergeBatch { ops }
        }
        other => return Err(StorageError::Backend(format!("bad WAL record tag {other}"))),
    };
    if c.pos != bytes.len() {
        return Err(StorageError::Backend(
            "trailing bytes after WAL record".into(),
        ));
    }
    Ok(record)
}

fn decode_batch_op(c: &mut Cursor<'_>) -> Result<BatchOp> {
    let tag = c.u8()?;
    Ok(match tag {
        0 => BatchOp::Put {
            key: c.bytes()?,
            value: c.bytes()?,
        },
        1 => BatchOp::Delete { key: c.bytes()? },
        2 => {
            let n = c.u32()? as usize;
            let mut keys = Vec::with_capacity(n);
            for _ in 0..n {
                keys.push(c.bytes()?);
            }
            BatchOp::DeleteKeys { keys }
        }
        other => return Err(StorageError::Backend(format!("bad batch op tag {other}"))),
    })
}

fn decode_merge_rec(c: &mut Cursor<'_>) -> Result<MergeRec> {
    let key = c.bytes()?;
    let value = c.opt_bytes()?;
    let version = c.u64()?;
    Ok(MergeRec {
        key,
        value,
        version,
    })
}

// ---------------------------------------------------------------------------
// Compact binary MANIFEST codec
// ---------------------------------------------------------------------------
//
// The manifest is small but written on every flush/compaction, so a compact,
// dependency-free binary encoding beats JSON on both size and parse cost. The
// layout is a fixed header followed by length-prefixed records — all integers
// big-endian (`to_be_bytes`), all byte strings `u32`-length-prefixed:
//
//   MAGIC(4 = b"CMF1") | version(u8) | next_seq(u64) | max_version(u64)
//   | table_count(u32) | table[0] | table[1] | ...
//   | wal_seg_count(u32) | wal_seg[0](u64) | ...        (version >= 2 only)
//
// One table record (mirrors `SsTableMeta`):
//   seq(u64) | level(u32)
//   | min_key: opt_bytes | max_key: opt_bytes
//   | min_version(u64) | max_version(u64)
//   | index_offset(u64) | index_len(u64) | file_size(u64)
//   | has_bloom(u8) | bloom_k(u32) | bloom_bits: bytes | format(u32)
//
//   opt_bytes := present(u8) [ len(u32) bytes ]   (present 0 => None)
//   bytes     := len(u32) bytes
//
// Version history within the `CMF1` family:
//   v1 — header + tables (single-file WAL era; no `wal_segments`).
//   v2 — adds the trailing live WAL-segment list (WAL segment rotation).
// `decode_manifest` reads either: a v1 image yields an empty `wal_segments`
// (recovery then takes the legacy single-file WAL path), a v2 image reads the
// trailing list.
//
// Forward-compat: a legacy JSON manifest begins with `{` (0x7B), which can never
// be our magic's first byte (`C` = 0x43), so `decode_manifest` detects and falls
// back to `serde_json`.

/// Binary manifest magic: "CMF1" (AnimusDB ManiFest, format family 1).
const MANIFEST_MAGIC: [u8; 4] = *b"CMF1";
/// Binary manifest format version (within the `CMF1` family). v2 adds the live
/// WAL-segment list; v1 (no segment list) is still decoded.
const MANIFEST_VERSION: u8 = 2;

/// Append a length-prefixed (`u32`) byte string.
fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_be_bytes());
    out.extend_from_slice(b);
}

/// Append an optional length-prefixed byte string (1-byte present flag).
fn put_opt_bytes(out: &mut Vec<u8>, b: &Option<Vec<u8>>) {
    match b {
        Some(v) => {
            out.push(1);
            put_bytes(out, v);
        }
        None => out.push(0),
    }
}

/// Encode a [`Manifest`] in the compact binary format described above.
fn encode_manifest(m: &Manifest) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&MANIFEST_MAGIC);
    out.push(MANIFEST_VERSION);
    out.extend_from_slice(&m.next_seq.to_be_bytes());
    out.extend_from_slice(&m.max_version.to_be_bytes());
    out.extend_from_slice(&(m.tables.len() as u32).to_be_bytes());
    for t in &m.tables {
        out.extend_from_slice(&t.seq.to_be_bytes());
        out.extend_from_slice(&t.level.to_be_bytes());
        put_opt_bytes(&mut out, &t.min_key);
        put_opt_bytes(&mut out, &t.max_key);
        out.extend_from_slice(&t.min_version.to_be_bytes());
        out.extend_from_slice(&t.max_version.to_be_bytes());
        out.extend_from_slice(&t.index_offset.to_be_bytes());
        out.extend_from_slice(&t.index_len.to_be_bytes());
        out.extend_from_slice(&t.file_size.to_be_bytes());
        out.push(u8::from(t.has_bloom));
        let (bits, k) = t.bloom.as_parts();
        out.extend_from_slice(&k.to_be_bytes());
        put_bytes(&mut out, bits);
        out.extend_from_slice(&t.format.to_be_bytes());
    }
    // v2 trailer: the live WAL-segment list.
    out.extend_from_slice(&(m.wal_segments.len() as u32).to_be_bytes());
    for &seg in &m.wal_segments {
        out.extend_from_slice(&seg.to_be_bytes());
    }
    out
}

/// A forward-only cursor over manifest bytes, returning a backend error on any
/// short read (a truncated/corrupt manifest).
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.bytes.len() {
            return Err(StorageError::Backend("truncated manifest".into()));
        }
        let s = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn opt_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.bytes()?)),
            other => Err(StorageError::Backend(format!(
                "bad manifest option flag {other}"
            ))),
        }
    }
}

/// Decode a [`Manifest`]. Detects the compact binary format by its magic; if the
/// bytes are instead a legacy JSON manifest (no binary magic), falls back to
/// `serde_json` so an engine opened on an older directory still recovers.
fn decode_manifest(bytes: &[u8]) -> Result<Manifest> {
    if bytes.len() < 4 || bytes[..4] != MANIFEST_MAGIC {
        // Legacy JSON manifest (pre-binary). Read it for forward-compat.
        return serde_json::from_slice(bytes)
            .map_err(|e| StorageError::Backend(format!("corrupt manifest: {e}")));
    }
    let mut c = Cursor::new(bytes);
    let _magic = c.take(4)?;
    let version = c.u8()?;
    if version == 0 || version > MANIFEST_VERSION {
        return Err(StorageError::Backend(format!(
            "unsupported manifest version {version}"
        )));
    }
    let next_seq = c.u64()?;
    let max_version = c.u64()?;
    let table_count = c.u32()? as usize;
    let mut tables = Vec::with_capacity(table_count);
    for _ in 0..table_count {
        let seq = c.u64()?;
        let level = c.u32()?;
        let min_key = c.opt_bytes()?;
        let max_key = c.opt_bytes()?;
        let min_version = c.u64()?;
        let tbl_max_version = c.u64()?;
        let index_offset = c.u64()?;
        let index_len = c.u64()?;
        let file_size = c.u64()?;
        let has_bloom = c.u8()? != 0;
        let bloom_k = c.u32()?;
        let bloom_bits = c.bytes()?;
        let format = c.u32()?;
        tables.push(SsTableMeta {
            seq,
            level,
            min_key,
            max_key,
            min_version,
            max_version: tbl_max_version,
            index_offset,
            index_len,
            file_size,
            bloom: BloomFilter::from_parts(bloom_bits, bloom_k),
            has_bloom,
            format,
        });
    }
    // v2 trailer: the live WAL-segment list. A v1 image has none, so recovery
    // falls back to the legacy single-file WAL path.
    let wal_segments = if version >= 2 {
        let count = c.u32()? as usize;
        let mut segs = Vec::with_capacity(count);
        for _ in 0..count {
            segs.push(c.u64()?);
        }
        segs
    } else {
        Vec::new()
    };
    Ok(Manifest {
        next_seq,
        tables,
        max_version,
        wal_segments,
    })
}

fn io(e: std::io::Error) -> StorageError {
    StorageError::Backend(e.to_string())
}

#[cfg(test)]
mod manifest_tests {
    use super::*;

    fn sample_meta(seq: u64, with_bloom: bool) -> SsTableMeta {
        let bloom = if with_bloom {
            BloomFilter::build(&[b"a".as_slice(), b"bbb".as_slice(), b"cc".as_slice()])
        } else {
            BloomFilter::default()
        };
        SsTableMeta {
            seq,
            level: (seq % 3) as u32,
            min_key: Some(format!("min-{seq}").into_bytes()),
            max_key: Some(format!("max-{seq}").into_bytes()),
            min_version: seq * 10,
            max_version: seq * 10 + 5,
            index_offset: seq * 1000,
            index_len: 42,
            file_size: seq * 2000,
            bloom,
            has_bloom: with_bloom,
            format: 2,
        }
    }

    /// The binary codec is an exact round trip across an empty manifest and one
    /// with several tables (with and without a bloom, and an empty-key table).
    #[test]
    fn binary_manifest_round_trips() {
        for m in [
            Manifest::default(),
            Manifest {
                next_seq: 7,
                max_version: 1234,
                tables: vec![
                    sample_meta(1, true),
                    sample_meta(2, false),
                    SsTableMeta {
                        min_key: None,
                        max_key: None,
                        has_bloom: false,
                        bloom: BloomFilter::default(),
                        ..sample_meta(3, false)
                    },
                ],
                wal_segments: vec![2, 3, 5],
            },
        ] {
            let bytes = encode_manifest(&m);
            // Binary, not JSON: starts with our magic.
            assert_eq!(&bytes[..4], &MANIFEST_MAGIC);
            let back = decode_manifest(&bytes).expect("decode");
            assert_eq!(back.next_seq, m.next_seq);
            assert_eq!(back.max_version, m.max_version);
            assert_eq!(back.wal_segments, m.wal_segments, "wal segments round-trip");
            assert_eq!(back.tables.len(), m.tables.len());
            for (a, b) in m.tables.iter().zip(&back.tables) {
                assert_eq!(a.seq, b.seq);
                assert_eq!(a.level, b.level);
                assert_eq!(a.min_key, b.min_key);
                assert_eq!(a.max_key, b.max_key);
                assert_eq!(a.min_version, b.min_version);
                assert_eq!(a.max_version, b.max_version);
                assert_eq!(a.index_offset, b.index_offset);
                assert_eq!(a.index_len, b.index_len);
                assert_eq!(a.file_size, b.file_size);
                assert_eq!(a.has_bloom, b.has_bloom);
                assert_eq!(a.format, b.format);
                // Bloom round-trips bit-for-bit: same membership answers.
                assert_eq!(a.bloom.may_contain(b"a"), b.bloom.may_contain(b"a"));
                assert_eq!(a.bloom.as_parts(), b.bloom.as_parts());
            }
        }
    }

    /// The binary encoding is materially smaller than the old JSON encoding for a
    /// representative manifest (the point of the change).
    #[test]
    fn binary_is_smaller_than_json() {
        let m = Manifest {
            next_seq: 20,
            max_version: 99_999,
            tables: (1..=12).map(|s| sample_meta(s, true)).collect(),
            wal_segments: vec![18, 19, 20],
        };
        let bin = encode_manifest(&m);
        let json = serde_json::to_vec(&m).unwrap();
        assert!(
            bin.len() < json.len(),
            "binary manifest ({} bytes) not smaller than JSON ({} bytes)",
            bin.len(),
            json.len()
        );
    }

    /// A legacy JSON manifest still decodes (forward-compat fallback): tables get
    /// their serde defaults (`format = 1`, no bloom), so an old directory opens.
    #[test]
    fn legacy_json_manifest_still_decodes() {
        // A pre-format/pre-bloom JSON manifest: omit `format`, `bloom`, `has_bloom`.
        let json = br#"{
            "next_seq": 3,
            "max_version": 50,
            "tables": [
                {
                    "seq": 1,
                    "min_key": [97],
                    "max_key": [98],
                    "min_version": 1,
                    "max_version": 9,
                    "index_offset": 100,
                    "index_len": 20,
                    "file_size": 200
                }
            ]
        }"#;
        let m = decode_manifest(json).expect("legacy json decodes");
        assert_eq!(m.next_seq, 3);
        assert_eq!(m.max_version, 50);
        assert_eq!(m.tables.len(), 1);
        assert!(
            m.wal_segments.is_empty(),
            "legacy json manifest has no recorded WAL segments"
        );
        let t = &m.tables[0];
        assert_eq!(t.seq, 1);
        assert_eq!(
            t.format, 3,
            "a format-less manifest entry defaults to the current format"
        );
        assert!(!t.has_bloom, "legacy table has no bloom");
        assert_eq!(t.level, 0, "legacy table defaults to L0");
    }

    /// A **v1 binary** manifest (pre-segment-rotation: header + tables, no trailing
    /// WAL-segment list) still decodes, yielding an empty `wal_segments` so recovery
    /// falls back to the legacy single-file WAL.
    #[test]
    fn legacy_v1_binary_manifest_decodes_with_no_segments() {
        // Encode a v1 image by hand: the v2 encoder minus the trailing segment list,
        // with the version byte forced to 1.
        let m = Manifest {
            next_seq: 4,
            max_version: 77,
            tables: vec![sample_meta(1, true), sample_meta(2, false)],
            wal_segments: vec![1, 2], // present in memory but NOT written for v1
        };
        let mut v2 = encode_manifest(&m);
        // Drop the v2 trailer (u32 count + count*u64) to get the v1 body ...
        let trailer = 4 + m.wal_segments.len() * 8;
        v2.truncate(v2.len() - trailer);
        // ... and stamp the version byte (immediately after the 4-byte magic) to 1.
        v2[4] = 1;
        let back = decode_manifest(&v2).expect("v1 binary decodes");
        assert_eq!(back.next_seq, 4);
        assert_eq!(back.max_version, 77);
        assert_eq!(back.tables.len(), 2);
        assert!(
            back.wal_segments.is_empty(),
            "v1 binary manifest carries no WAL-segment list"
        );
    }
}
