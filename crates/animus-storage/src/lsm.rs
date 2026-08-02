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
//!   of [`WalRecord`]s, each one newline-framed JSON, `append`ed then `sync`ed
//!   **before** a write is acknowledged (an ack means durable). The group-commit
//!   coordinator appends to the active segment and rolls to a fresh one once it
//!   passes a byte threshold; a flush removes whole segments it has folded into an
//!   SSTable (see [`wal`]). Holds the writes not yet folded into an SSTable.
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
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use animus_env::Env;
use serde::{Deserialize, Serialize};

use crate::{
    Key, Result, Snapshot, StorageEngine, StorageError, Value, Version, VersionedValue, WriteBatch,
    WriteOp,
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
    async fn remove_orphan_wal_segments(env: &E, prefix: &str, live: &[u64]) -> Result<()> {
        let Some(&lowest_live) = live.iter().min() else {
            return Ok(());
        };
        for seg in 0..lowest_live {
            let file = format!("{prefix}wal-{seg:06}");
            if env.size(&file).await.map_err(io)? > 0 {
                env.remove(&file).await.map_err(io)?;
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

    /// [`open`](Self::open) with explicit [`LsmOptions`].
    ///
    /// # Errors
    /// As [`open`](Self::open).
    pub async fn open_with(env: E, prefix: impl Into<String>, opts: LsmOptions) -> Result<Self> {
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
                    .with_block_counter(Arc::clone(&block_reads)),
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
            // segments and this file is left as a harmless orphan.
            let legacy = format!("{prefix}wal");
            let wal_bytes = env.read(&legacy).await.map_err(io)?;
            for record in decode_wal(&wal_bytes) {
                max_version = max_version.max(record_max_version(&record));
                apply_wal_record(&mut memtable, &mut memtable_bytes, record);
            }
        } else {
            for &seg in &segments {
                let file = format!("{prefix}wal-{seg:06}");
                let wal_bytes = env.read(&file).await.map_err(io)?;
                for record in decode_wal(&wal_bytes) {
                    max_version = max_version.max(record_max_version(&record));
                    apply_wal_record(&mut memtable, &mut memtable_bytes, record);
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
        self.wal.commit(&self.env, bytes).await?;
        {
            let mut inner = self.lock();
            apply(&mut inner);
        }
        drop(guard);
        Ok(())
    }

    /// The highest version recorded for `key` anywhere (memtable + SSTables),
    /// tombstones included. Reads SSTable blocks (async) for tables whose key
    /// range contains `key`, then folds in the memtable under a brief lock.
    async fn latest_version_of(&self, key: &[u8]) -> Result<Option<Version>> {
        // Snapshot the SSTable readers (cheap clones of metadata + index) under
        // the lock, plus the memtable's own latest, then read blocks lock-free.
        let (readers, memtable_latest) = {
            let inner = self.lock();
            (inner.readers.clone(), inner.memtable_latest_version_of(key))
        };
        let mut best = memtable_latest;
        for reader in readers.iter().rev() {
            if !reader.meta().may_contain(key) {
                continue;
            }
            if let Some((v, _)) = reader.latest(&self.env, key).await? {
                best = Some(best.map_or(v, |b| b.max(v)));
            }
        }
        Ok(best)
    }

    /// Read `key` as of `version`: the greatest `(version', slot)` with
    /// `version' ≤ version` across the memtable and SSTables (newest wins), with a
    /// tombstone hiding older values. Matches [`MemoryEngine`] semantics.
    ///
    /// [`MemoryEngine`]: crate::MemoryEngine
    async fn read_at(&self, key: &[u8], version: Version) -> Result<Option<VersionedValue>> {
        let (readers, memtable_hit) = {
            let inner = self.lock();
            let hit = inner
                .memtable
                .get(key)
                .and_then(|h| h.range(..=version).next_back())
                .map(|(&v, slot)| (v, slot.clone()));
            (inner.readers.clone(), hit)
        };
        // Track the best (highest-version) hit seen so far. The memtable is the
        // newest source.
        let mut best: Option<(Version, Option<Value>)> = memtable_hit;
        for reader in readers.iter().rev() {
            // Take the max-version hit across all sources; a key range that can't
            // contain `key` is skipped cheaply.
            if !reader.meta().may_contain(key) {
                continue;
            }
            if let Some((v, slot)) = reader.get_at(&self.env, key, version).await? {
                if best.as_ref().is_none_or(|(bv, _)| v > *bv) {
                    best = Some((v, slot));
                }
            }
        }
        Ok(best.and_then(|(v, slot)| slot.map(|value| VersionedValue { version: v, value })))
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
        let readers = {
            let inner = self.lock();
            inner.readers.clone()
        };
        // Oldest first, so newer overwrites older; the memtable is applied last.
        let mut merged: BTreeMap<Key, (Version, Option<Value>)> = BTreeMap::new();
        for reader in &readers {
            for (k, v, slot) in reader.scan_at(&self.env, start, end, version).await? {
                merge_winner(&mut merged, k, v, slot);
            }
        }
        // Memtable last (newest).
        let upper = match end {
            Some(e) => Bound::Excluded(e),
            None => Bound::Unbounded,
        };
        {
            let inner = self.lock();
            for (k, history) in inner
                .memtable
                .range::<[u8], _>((Bound::Included(start), upper))
            {
                if let Some((&v, slot)) = history.range(..=version).next_back() {
                    merge_winner(&mut merged, k.clone(), v, slot.clone());
                }
            }
        }
        Ok(merged)
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
                // `maybe_flush_and_compact` once it has applied. (See
                // `Inner::applies_in_flight` and `flush`.)
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

    /// Open a reader for `file`/`meta` with the engine's shared block-read
    /// counter wired in.
    async fn open_reader(&self, file: String, meta: SsTableMeta) -> Result<SsTableReader> {
        let counter = Arc::clone(&self.lock().block_reads);
        Ok(SsTableReader::open(&self.env, file, meta)
            .await?
            .with_block_counter(counter))
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
    async fn flush(&self) -> Result<()> {
        // Snapshot the memtable + the seq to allocate, lock-free for the write.
        // The caller (`maybe_flush_and_compact`) only calls us with no writes
        // in-flight (`applies_in_flight == 0`), so at this instant every durable
        // WAL record (seq ≤ `wal_watermark`) is already applied to the memtable,
        // and the snapshot folds all of them into the new SSTable. That watermark
        // is what later tells us which WAL segments are fully covered (so they may
        // be removed) — a segment whose highest seq ≤ watermark holds only records
        // now durably in the SSTable.
        let (records, seq, mut new_manifest, wal_watermark) = {
            let inner = self.lock();
            if inner.memtable.is_empty() {
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

        // Commit the in-memory swap: clear the flushed memtable, add the reader.
        {
            let mut inner = self.lock();
            inner.manifest = new_manifest;
            inner.readers.push(reader);
            inner.memtable.clear();
            inner.memtable_bytes = 0;
            inner.flushes += 1;
        }
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
        let target_level = plan.source_level + 1;

        // Pick inputs under the lock: all readers at the source level, plus the
        // target-level readers overlapping the source's combined key range. Also
        // capture (a) the engine's monotonic floor (`max_version`), which fixes the
        // tombstone GC floor, and (b) the key ranges of every table at a level
        // **deeper** than the target — a tombstone may only be fully reclaimed when
        // no such deeper table could still hold an older value for the key (which
        // would otherwise resurface once the tombstone is gone).
        let (input_readers, input_seqs, base_seq, max_version, deeper_ranges) = {
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
            (
                input_readers,
                input_seqs,
                inner.manifest.next_seq,
                inner.manifest.max_version,
                deeper_ranges,
            )
        };

        if input_readers.is_empty() {
            return Ok(());
        }

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
        let gc_floor = max_version.saturating_sub(self.opts.tombstone_grace_versions);
        gc_obsolete_records(&mut merged, gc_floor, &deeper_ranges);

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
            if t.level >= 1 {
                if let (Some(lo), Some(hi)) = (&t.min_key, &t.max_key) {
                    by_level
                        .entry(t.level)
                        .or_default()
                        .push((lo.clone(), hi.clone()));
                }
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
        self.maybe_flush_and_compact().await
    }

    async fn merge(&self, key: &[u8], value: &[u8], version: Version) -> Result<bool> {
        // Per-key LWW: apply only if strictly newer than this key's own latest
        // anywhere in the engine.
        if self
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
        self.maybe_flush_and_compact().await?;
        Ok(true)
    }

    async fn merge_tombstone(&self, key: &[u8], version: Version) -> Result<bool> {
        if self
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
        self.maybe_flush_and_compact().await?;
        Ok(true)
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
        self.maybe_flush_and_compact().await
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
        self.maybe_flush_and_compact().await
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        for op in &batch.ops {
            if let WriteOp::DeleteRange { start, end } = op {
                if start > end {
                    return Err(StorageError::InvalidRange);
                }
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
        self.maybe_flush_and_compact().await
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

    async fn entries(&self) -> Result<Vec<(Key, VersionedValue)>> {
        let merged = self.merged_latest().await?;
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

    fn snapshot(&self) -> LsmSnapshot<E> {
        let version = self.lock().manifest.max_version;
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
}

/// A snapshot of an [`LsmEngine`] pinned at a version. Reads filter to records at
/// or below that version, so later (higher-version) writes are invisible.
#[derive(Clone)]
pub struct LsmSnapshot<E: Env> {
    engine: LsmEngine<E>,
    version: Version,
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
    }
}

/// Encode one WAL record as a newline-terminated JSON line (`serde_json` never
/// emits raw newlines, so framing is unambiguous; a torn trailing line is
/// dropped on replay).
fn encode_wal(record: &WalRecord) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(record).expect("wal record serializes");
    bytes.push(b'\n');
    bytes
}

/// Decode WAL bytes into records, ignoring a trailing partial line (a write torn
/// by a crash — it was never `sync`ed, so it was never acked).
fn decode_wal(bytes: &[u8]) -> Vec<WalRecord> {
    bytes
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_slice(line).ok())
        .collect()
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
        assert_eq!(t.format, 1, "legacy table defaults to format v1");
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
