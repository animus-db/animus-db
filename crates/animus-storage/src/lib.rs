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

    /// Every live (non-tombstoned) latest entry, as `(key, versioned value)`,
    /// ordered by key. This is the full digest anti-entropy reconciles against;
    /// it is `scan` over the whole keyspace.
    async fn entries(&self) -> Result<Vec<(Key, VersionedValue)>>;

    /// Every key's latest entry **including tombstones**, as
    /// `(key, Option<value>, version)` where `None` is a tombstone, ordered by
    /// key. Unlike [`entries`](StorageEngine::entries) this retains deleted
    /// keys, so anti-entropy can propagate a delete to a replica that still
    /// holds the value (ADR 0010).
    async fn entries_with_tombstones(&self) -> Result<Vec<(Key, Option<Value>, Version)>>;

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
