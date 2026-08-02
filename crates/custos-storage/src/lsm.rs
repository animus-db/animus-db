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
//! [`Disk`]: custos_env::Disk
//!
//! ## On-disk layout
//!
//! All files live in one `Env`-disk namespace (the disk is already node-scoped;
//! we use a filename prefix so several engines can share one node's disk):
//!
//! - `<prefix>MANIFEST` — the durable source of truth: the ordered list of live
//!   SSTable files plus per-table metadata (key range, version range, index
//!   offset/len, size) and the engine's monotonic `max_version`. Written
//!   **atomically** via [`Disk::replace`], so a crash sees either the whole old
//!   or whole new manifest, never a mix.
//! - `<prefix>wal` — a write-ahead log of [`WalRecord`]s, each one newline-framed
//!   JSON, `append`ed then `sync`ed **before** a write is acknowledged (an ack
//!   means durable). Holds the writes not yet folded into an SSTable.
//! - `<prefix>sst-NNNNNN` — immutable, sorted SSTables (see [`sstable`]).
//!
//! ## Write path
//!
//! Every mutation: serialize a [`WalRecord`], `append` + `sync` it (durable
//! first), then apply it to the in-memory memtable (a `BTreeMap` MVCC store, the
//! same shape as [`MemoryEngine`]'s). When the memtable crosses a size threshold
//! it is **flushed** to a fresh SSTable, then the manifest is atomically swapped
//! to add that table and drop the (now-redundant) WAL, and a fresh WAL begins.
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
//!   reopen the manifest still names the *old* set and the WAL is intact, so the
//!   memtable is rebuilt from the WAL and nothing is lost. An orphan SSTable file
//!   not named by the manifest is simply ignored (and overwritten by the next
//!   flush, which reuses the next sequence number derived from the manifest).
//! - **Mid-compaction crash** (the merged output table(s) written but manifest
//!   not yet swapped): the manifest still names the old inputs, which are all
//!   intact (compaction only `remove`s them *after* the swap), so reads see the
//!   old set; the orphan output files (at seqs beyond the manifest's `next_seq`)
//!   are ignored. No torn-table read is possible because a
//!   table is only ever read once it is named by a synced manifest.
//!
//! These properties are argued here and exercised in `tests/lsm_crash.rs`.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use custos_env::Env;
use serde::{Deserialize, Serialize};

use crate::{
    Key, Result, Snapshot, StorageEngine, StorageError, Value, Version, VersionedValue, WriteBatch,
    WriteOp,
};

mod bloom;
mod sstable;

use sstable::{Record, SsTableMeta, SsTableReader, SsTableWriter};

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
}

impl Default for LsmOptions {
    fn default() -> Self {
        Self {
            flush_threshold_bytes: DEFAULT_FLUSH_BYTES,
            compaction_trigger: DEFAULT_COMPACTION_TRIGGER,
            target_table_bytes: DEFAULT_TARGET_TABLE_BYTES,
            level_fanout: DEFAULT_LEVEL_FANOUT,
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

/// The durable manifest: the live SSTable set plus engine metadata. Serialized
/// to JSON and written atomically with [`Disk::replace`].
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
}

impl<E: Env> LsmEngine<E> {
    fn manifest_file(&self) -> String {
        format!("{}MANIFEST", self.prefix)
    }

    fn wal_file(&self) -> String {
        format!("{}wal", self.prefix)
    }

    fn sst_file(&self, seq: u64) -> String {
        format!("{}sst-{seq:06}", self.prefix)
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

        // Load the durable manifest (or a fresh empty one).
        let manifest_bytes = env.read(&manifest_file).await.map_err(io)?;
        let manifest: Manifest = if manifest_bytes.is_empty() {
            Manifest::default()
        } else {
            serde_json::from_slice(&manifest_bytes)
                .map_err(|e| StorageError::Backend(format!("corrupt manifest: {e}")))?
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

        // Replay the WAL tail into the memtable. A torn trailing record (crash
        // mid-append, never synced/acked) is dropped by `decode`.
        let mut memtable: BTreeMap<Key, History> = BTreeMap::new();
        let mut memtable_bytes = 0usize;
        let mut max_version = manifest.max_version;
        let wal_file = format!("{prefix}wal");
        let wal_bytes = env.read(&wal_file).await.map_err(io)?;
        for record in decode_wal(&wal_bytes) {
            max_version = max_version.max(record_max_version(&record));
            apply_wal_record(&mut memtable, &mut memtable_bytes, record);
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
        };

        Ok(Self {
            env,
            prefix,
            opts,
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Durably log a record (append + sync) **before** it is applied in memory.
    async fn log(&self, record: &WalRecord) -> Result<()> {
        let bytes = encode_wal(record);
        let wal = self.wal_file();
        self.env.append(&wal, &bytes).await.map_err(io)?;
        self.env.sync(&wal).await.map_err(io)?;
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
            !inner.memtable.is_empty() && inner.memtable_bytes >= self.opts.flush_threshold_bytes
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
    /// manifest, drop the folded WAL, and start a fresh (empty) WAL + memtable.
    async fn flush(&self) -> Result<()> {
        // Snapshot the memtable + the seq to allocate, lock-free for the write.
        let (records, seq, mut new_manifest) = {
            let inner = self.lock();
            if inner.memtable.is_empty() {
                return Ok(());
            }
            let records = flatten_memtable(&inner.memtable);
            let seq = inner.manifest.next_seq + 1;
            let mut m = inner.manifest.clone();
            m.next_seq = seq;
            (records, seq, m)
        };

        // Build + sync the new SSTable file (outside the lock). A flush always
        // lands at L0 (the overlapping flush tier).
        let file = self.sst_file(seq);
        let meta = SsTableWriter::write(&self.env, &file, seq, 0, &records).await?;
        self.env.sync(&file).await.map_err(io)?;
        let reader = self.open_reader(file, meta.clone()).await?;

        // Atomically swap the manifest to reference the new table. Until this
        // returns durably, a crash recovers the old manifest + the intact WAL.
        new_manifest.tables.push(meta);
        self.write_manifest(&new_manifest).await?;

        // The WAL is now redundant (its writes live in the SSTable). Start fresh.
        self.env.replace(&self.wal_file(), &[]).await.map_err(io)?;

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
        // target-level readers overlapping the source's combined key range.
        let (input_readers, input_seqs, base_seq) = {
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
            (input_readers, input_seqs, inner.manifest.next_seq)
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

    /// Atomically persist the manifest. Bumps `max_version` from the live in-
    /// memory floor first so it is never lost across the swap.
    async fn write_manifest(&self, manifest: &Manifest) -> Result<()> {
        let bytes = serde_json::to_vec(manifest)
            .map_err(|e| StorageError::Backend(format!("manifest encode: {e}")))?;
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
        self.log(&WalRecord::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            version,
        })
        .await?;
        {
            let mut inner = self.lock();
            inner.apply_put(key, value, version);
            inner.manifest.max_version = version;
        }
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
        self.log(&WalRecord::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            version,
        })
        .await?;
        {
            let mut inner = self.lock();
            inner.apply_put(key, value, version);
            inner.manifest.max_version = inner.manifest.max_version.max(version);
        }
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
        self.log(&WalRecord::Delete {
            key: key.to_vec(),
            version,
        })
        .await?;
        {
            let mut inner = self.lock();
            inner.apply_delete(key, version);
            inner.manifest.max_version = inner.manifest.max_version.max(version);
        }
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
        self.log(&WalRecord::Delete {
            key: key.to_vec(),
            version,
        })
        .await?;
        {
            let mut inner = self.lock();
            inner.apply_delete(key, version);
            inner.manifest.max_version = version;
        }
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
        self.log(&WalRecord::DeleteRange {
            start: start.to_vec(),
            end: end.to_vec(),
            keys: keys.clone(),
            version,
        })
        .await?;
        {
            let mut inner = self.lock();
            for k in &keys {
                inner.apply_delete(k, version);
            }
            inner.manifest.max_version = version;
        }
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
        self.log(&WalRecord::Batch {
            version: batch.version,
            ops: logged.clone(),
        })
        .await?;
        {
            let mut inner = self.lock();
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
        }
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

fn io(e: std::io::Error) -> StorageError {
    StorageError::Backend(e.to_string())
}
