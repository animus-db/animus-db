//! Storage interface for AnimusDB.
//!
//! The [`StorageEngine`] trait is driven by what the distributed layer needs
//! (ADR 0004): point `put`/`get`, ordered range scan, atomic batch write,
//! consistent snapshots, MVCC versions, and range delete. It is deliberately
//! storage-engine-agnostic (ADR 0008). Two implementations live behind it: the
//! in-memory [`MemoryEngine`], a `BTreeMap`-backed MVCC store that is trivially
//! deterministic — ideal for simulation testing — and [`LsmEngine`], a custom
//! on-disk log-structured merge tree that does all its I/O through the `Env`
//! disk seam, so even a persistent engine stays deterministically
//! crash-testable under simulation.
//!
//! ## MVCC model
//!
//! Every key maps to a sorted set of `(version, Option<value>)` entries, where
//! `None` is a tombstone. A read *as of* version `v` returns the value of the
//! greatest entry with version `≤ v` (or nothing, if that entry is a tombstone
//! or no such entry exists). Writers assign versions and **must do so
//! monotonically**; the distributed layer supplies commit timestamps that
//! satisfy this. Given monotonic versions, a [`Snapshot`] taken at version `v`
//! is isolated from all later writes.

mod lsm;
mod memory;

pub use lsm::{LsmEngine, LsmOptions, LsmSnapshot, SsTableView, WalRecordView};
pub use memory::{MemoryEngine, MemorySnapshot};

/// A storage key.
pub type Key = Vec<u8>;
/// A stored value.
pub type Value = Vec<u8>;
/// An MVCC version / commit timestamp. Monotonic per the engine contract.
pub type Version = u64;

/// A value together with the version at which it was written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedValue {
    /// The version at which this value was written.
    pub version: Version,
    /// The value bytes.
    pub value: Value,
}

/// A single mutation within a [`WriteBatch`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteOp {
    /// Write `value` at `key`.
    Put { key: Key, value: Value },
    /// Tombstone `key`.
    Delete { key: Key },
    /// Tombstone every key in `[start, end)`.
    DeleteRange { start: Key, end: Key },
}

/// One per-key last-writer-wins merge, for [`StorageEngine::merge_batch`].
///
/// Unlike a [`WriteOp`] (which shares the batch's single version and enforces the
/// engine-wide monotonic floor), each `MergeOp` carries its **own** version and
/// applies with the same per-key LWW rule as [`merge`](StorageEngine::merge) /
/// [`merge_tombstone`](StorageEngine::merge_tombstone): it takes effect iff its
/// `version` is strictly greater than the key's current latest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeOp {
    /// The key to merge.
    pub key: Key,
    /// `Some(value)` merges a value; `None` merges a tombstone (delete).
    pub value: Option<Value>,
    /// The per-key LWW version this op carries.
    pub version: Version,
}

impl MergeOp {
    /// A value merge at `key` / `version`.
    #[must_use]
    pub fn put(key: impl Into<Key>, value: impl Into<Value>, version: Version) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
            version,
        }
    }

    /// A tombstone merge at `key` / `version`.
    #[must_use]
    pub fn tombstone(key: impl Into<Key>, version: Version) -> Self {
        Self {
            key: key.into(),
            value: None,
            version,
        }
    }
}

/// A set of mutations applied atomically at a single `version`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteBatch {
    /// The version stamped on every op in the batch.
    pub version: Version,
    /// The mutations, applied in order under one lock.
    pub ops: Vec<WriteOp>,
}

impl WriteBatch {
    /// Start an empty batch stamped at `version`.
    #[must_use]
    pub fn new(version: Version) -> Self {
        Self {
            version,
            ops: Vec::new(),
        }
    }

    /// Append a put.
    #[must_use]
    pub fn put(mut self, key: impl Into<Key>, value: impl Into<Value>) -> Self {
        self.ops.push(WriteOp::Put {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    /// Append a point delete (tombstone).
    #[must_use]
    pub fn delete(mut self, key: impl Into<Key>) -> Self {
        self.ops.push(WriteOp::Delete { key: key.into() });
        self
    }

    /// Append a range delete over `[start, end)`.
    #[must_use]
    pub fn delete_range(mut self, start: impl Into<Key>, end: impl Into<Key>) -> Self {
        self.ops.push(WriteOp::DeleteRange {
            start: start.into(),
            end: end.into(),
        });
        self
    }
}

/// Errors a storage engine can report.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// A scan or range delete was given `start > end`.
    #[error("invalid range: start > end")]
    InvalidRange,
    /// A write used a version not greater than the engine's latest, violating
    /// the monotonic-version contract.
    #[error("non-monotonic version: {got} <= {latest}")]
    NonMonotonicVersion { got: Version, latest: Version },
    /// An error from an underlying persistent backend.
    #[error("storage backend error: {0}")]
    Backend(String),
    /// `LsmOptions::level_fanout` was `<= 1`. The per-level table budget is
    /// `L1_TABLE_BUDGET * level_fanout^(level - 1)`, so at `level_fanout <= 1`
    /// it never grows with depth — a table set whose fully-merged size
    /// exceeds the L1 budget can cascade down through every level forever
    /// without ever settling. Validated at [`LsmOptions::validate`] /
    /// `LsmEngine::open*`, rather than left as a silent footgun.
    #[error("invalid LsmOptions: level_fanout must be >= 2, got {level_fanout}")]
    InvalidLevelFanout {
        /// The rejected value.
        level_fanout: usize,
    },
}

/// Result alias for storage operations.
pub type Result<T> = std::result::Result<T, StorageError>;

/// A sorted, versioned key/value store.
///
/// Implementations are cheap to clone (clones share state) and `Send + Sync`.
/// See the [crate docs](crate) for the MVCC model.
///
/// The I/O-ish methods are `async`: the in-memory engine satisfies them
/// trivially (no real awaiting), but an on-disk LSM reaches the async [`Disk`]
/// seam to read/flush SSTable blocks. `snapshot()` and `latest_version()` stay
/// synchronous — pinning a version and reading the current floor are cheap,
/// in-memory operations on every backend.
///
/// [`Disk`]: animus_env::Disk
#[async_trait::async_trait]
pub trait StorageEngine: Clone + Send + Sync {
    /// A consistent point-in-time read view.
    type Snapshot: Snapshot;

    /// Write `value` at `key` as of `version`.
    async fn put(&self, key: &[u8], value: &[u8], version: Version) -> Result<()>;

    /// Merge `value` at `key` with **per-key** last-writer-wins: apply iff
    /// `version` is strictly greater than the key's current latest version,
    /// returning whether it took effect.
    ///
    /// Unlike [`put`](StorageEngine::put), `merge` does **not** enforce the
    /// engine-wide monotonic-version contract — it compares only against the
    /// key's own history. This is the convergence primitive for leaderless
    /// replication: anti-entropy and read-repair re-apply a value at its
    /// *original* version (which may sit below the engine's latest), and `merge`
    /// is idempotent and commutative under it, so replicas converge to the
    /// highest version seen per key regardless of delivery order.
    async fn merge(&self, key: &[u8], value: &[u8], version: Version) -> Result<bool>;

    /// Merge a **tombstone** at `key` with per-key last-writer-wins: apply iff
    /// `version` is strictly greater than the key's current latest version,
    /// returning whether it took effect.
    ///
    /// This is the delete counterpart of [`merge`](StorageEngine::merge): a
    /// data-plane delete and its anti-entropy / read-repair propagation
    /// re-apply a tombstone at its *original* version, bypassing the
    /// engine-wide monotonic floor. Idempotent and commutative under per-key
    /// LWW alongside `merge`, so a value and a later tombstone (or vice versa)
    /// converge regardless of delivery order.
    async fn merge_tombstone(&self, key: &[u8], version: Version) -> Result<bool>;

    /// Apply many per-key LWW merges (and tombstone-merges) under a **single
    /// durable sync**, in order.
    ///
    /// Semantically identical to calling [`merge`](StorageEngine::merge) /
    /// [`merge_tombstone`](StorageEngine::merge_tombstone) for each op in `ops`
    /// order — each op takes effect iff its own `version` is strictly greater
    /// than the key's current latest (accounting for earlier ops in the same
    /// batch) — but a durable engine coalesces the whole batch into one WAL
    /// `fsync` instead of one per op. This is the write primitive for the
    /// leaderful-Raft apply path, which applies a run of committed commands
    /// sequentially and would otherwise pay a full `fsync` per command.
    ///
    /// The default implementation simply applies each op via `merge` /
    /// `merge_tombstone`; [`LsmEngine`] overrides it to batch the WAL sync.
    async fn merge_batch(&self, ops: Vec<MergeOp>) -> Result<()> {
        for op in ops {
            match op.value {
                Some(value) => {
                    self.merge(&op.key, &value, op.version).await?;
                }
                None => {
                    self.merge_tombstone(&op.key, op.version).await?;
                }
            }
        }
        Ok(())
    }

    /// Tombstone `key` as of `version`.
    async fn delete(&self, key: &[u8], version: Version) -> Result<()>;

    /// Tombstone every key in `[start, end)` as of `version`.
    async fn delete_range(&self, start: &[u8], end: &[u8], version: Version) -> Result<()>;

    /// Apply a batch of mutations atomically.
    async fn write_batch(&self, batch: WriteBatch) -> Result<()>;

    /// Read the latest value at `key`.
    async fn get(&self, key: &[u8]) -> Result<Option<VersionedValue>>;

    /// Read the value at `key` as of `version`.
    async fn get_at(&self, key: &[u8], version: Version) -> Result<Option<VersionedValue>>;

    /// Scan the latest values for keys in `[start, end)`, ordered by key.
    async fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Key, VersionedValue)>>;

    /// Scan the values **as of `version`** for keys in `[start, end)`, ordered
    /// by key — the range counterpart of [`get_at`](StorageEngine::get_at),
    /// and the primitive the CP data plane's MVCC snapshot reads (ADR 0018
    /// §2/PR2b) are built on. For every key that has *ever* had an entry in
    /// the range (including one now tombstoned or superseded), returns the
    /// greatest entry with version `≤ version`, omitting a key whose entry at
    /// that version is a tombstone or that never had one. Unlike
    /// [`scan`](StorageEngine::scan) (implicitly "as of now"), this can see a
    /// key that has since been overwritten or deleted, exactly like
    /// [`get_at`](StorageEngine::get_at) does for a single key — so, unlike
    /// [`snapshot`](StorageEngine::snapshot) (which only ever pins the
    /// engine's *current* latest version), it supports an arbitrary
    /// **past** version.
    ///
    /// No default implementation: deriving one from the rest of this trait
    /// would need per-key history within the range, and the trait's other
    /// range method ([`entries_with_tombstones`](StorageEngine::entries_with_tombstones))
    /// only ever exposes each key's *latest* record — not enough to answer
    /// "what did this key look like as of an earlier version." Both engines
    /// already carry this logic internally (it backs their own `scan`/`get_at`
    /// at `version = latest`), so this is a thin, direct implementation on
    /// each, not new logic.
    async fn scan_at(
        &self,
        start: &[u8],
        end: &[u8],
        version: Version,
    ) -> Result<Vec<(Key, VersionedValue)>>;

    /// Every live (non-tombstoned) latest entry, as `(key, versioned value)`,
    /// ordered by key. This is the full digest anti-entropy reconciles against;
    /// it is `scan` over the whole keyspace.
    async fn entries(&self) -> Result<Vec<(Key, VersionedValue)>>;

    /// Every live (non-tombstoned, as of `version`) entry across the *whole*
    /// keyspace, ordered by key — [`entries`](StorageEngine::entries)'s
    /// as-of-a-past-version counterpart, exactly like
    /// [`scan_at`](StorageEngine::scan_at) is to
    /// [`scan`](StorageEngine::scan). Needed only for an unbounded-above
    /// snapshot scan with no finite physical bound at all (a caller with no
    /// prefix to derive one from — a legacy/test-only shape; a real bounded
    /// caller should always prefer `scan_at`, same cost trade-off as
    /// `entries` vs `scan`).
    async fn entries_at(&self, version: Version) -> Result<Vec<(Key, VersionedValue)>>;

    /// Every key's latest entry **including tombstones**, as
    /// `(key, Option<value>, version)` where `None` is a tombstone, ordered by
    /// key. Unlike [`entries`](StorageEngine::entries) this retains deleted
    /// keys, so anti-entropy can propagate a delete to a replica that still
    /// holds the value (ADR 0010).
    async fn entries_with_tombstones(&self) -> Result<Vec<(Key, Option<Value>, Version)>>;

    /// [`entries_with_tombstones`](StorageEngine::entries_with_tombstones)'s
    /// range-scoped sibling: every key in `[start, end)`'s latest record
    /// **including tombstones**, ordered by key (ADR 0050: the split-build
    /// driver's bulk/tail passes need a bounded tombstone-retaining read —
    /// a delete during the build must ship to the child as a tombstone at
    /// its version, which [`scan`](StorageEngine::scan) filters out).
    ///
    /// The **default implementation is correct but not cheap** (filters the
    /// whole-keyspace `entries_with_tombstones` by range — the
    /// [`merge_batch`](StorageEngine::merge_batch) precedent); both bundled
    /// engines override it with a genuinely bounded read.
    async fn scan_with_tombstones(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Key, Option<Value>, Version)>> {
        Ok(self
            .entries_with_tombstones()
            .await?
            .into_iter()
            .filter(|(k, _, _)| k.as_slice() >= start && k.as_slice() < end)
            .collect())
    }

    /// A range-scoped **byte** estimate for `[start, end)` (`end == None` is
    /// unbounded above, matching the rest of this trait's range methods) — the
    /// footprint (key bytes + value bytes) of every live key in the range
    /// (ADR 0034: byte-based auto-split).
    ///
    /// The **default implementation is exact**: it scans the range (via
    /// [`scan`](StorageEngine::scan), or [`entries`](StorageEngine::entries)
    /// filtered by `start` when `end` is `None`) and sums `key.len() +
    /// value.len()`. This is correct for any backend — including
    /// [`MemoryEngine`], where materializing the range costs nothing extra —
    /// so a new `StorageEngine` implementor gets a working (if not
    /// necessarily *cheap*) answer for free, exactly like
    /// [`merge_batch`](StorageEngine::merge_batch)'s per-op default.
    /// [`LsmEngine`] overrides it with a **cheap, non-materializing
    /// over-estimate** built from its own SSTable/memtable metadata (no disk
    /// read) — see its override's doc for the estimator and its bias
    /// direction. Callers that need a fast periodic gate (the auto-split
    /// hot-path check) should prefer a backend that overrides this cheaply;
    /// callers that need an exact count can always materialize the range
    /// themselves instead.
    async fn approx_bytes_in_range(&self, start: &[u8], end: Option<&[u8]>) -> Result<u64> {
        let rows = match end {
            Some(e) => self.scan(start, e).await?,
            None => self
                .entries()
                .await?
                .into_iter()
                .filter(|(k, _)| k.as_slice() >= start)
                .collect(),
        };
        Ok(rows
            .iter()
            .map(|(k, v)| (k.len() + v.value.len()) as u64)
            .sum())
    }

    /// Take a consistent snapshot at the engine's current latest version.
    fn snapshot(&self) -> Self::Snapshot;

    /// The engine's current latest (highest) version, or 0 if empty.
    fn latest_version(&self) -> Version;
}

/// A consistent, immutable read view of a [`StorageEngine`] pinned at a version.
///
/// Given monotonic write versions, a snapshot is unaffected by writes that
/// happen after it is taken.
#[async_trait::async_trait]
pub trait Snapshot: Send + Sync {
    /// The version this snapshot reads as of.
    fn version(&self) -> Version;

    /// Read the value at `key` as of the snapshot version.
    async fn get(&self, key: &[u8]) -> Option<VersionedValue>;

    /// Scan keys in `[start, end)` as of the snapshot version, ordered by key.
    async fn scan(&self, start: &[u8], end: &[u8]) -> Vec<(Key, VersionedValue)>;
}
