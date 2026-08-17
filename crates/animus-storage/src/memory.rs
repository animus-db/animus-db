//! In-memory, `BTreeMap`-backed MVCC storage engine (ADR 0008).
//!
//! Deterministic and dependency-free, so it is the engine used under simulation.
//! See the [crate docs](crate) for the MVCC model and version contract.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use crate::{
    Key, Result, Snapshot, StorageEngine, StorageError, Value, Version, VersionedValue, WriteBatch,
    WriteOp,
};

/// Per-key version history: `version -> Some(value)` or `None` (tombstone).
type History = BTreeMap<Version, Option<Value>>;

#[derive(Default)]
struct Inner {
    data: BTreeMap<Key, History>,
    max_version: Version,
}

impl Inner {
    /// Read `key` as of `version`: the greatest entry with version `≤ version`,
    /// unless that entry is a tombstone.
    fn read_at(&self, key: &[u8], version: Version) -> Option<VersionedValue> {
        let history = self.data.get(key)?;
        let (&v, slot) = history.range(..=version).next_back()?;
        slot.as_ref().map(|value| VersionedValue {
            version: v,
            value: value.clone(),
        })
    }

    /// The highest version recorded for `key`, tombstones included, or `None`
    /// if the key has never been written.
    fn latest_version_of(&self, key: &[u8]) -> Option<Version> {
        self.data.get(key)?.keys().next_back().copied()
    }

    fn scan_at(&self, start: &[u8], end: &[u8], version: Version) -> Vec<(Key, VersionedValue)> {
        let mut out = Vec::new();
        for key in self
            .data
            .range::<[u8], _>((Bound::Included(start), Bound::Excluded(end)))
            .map(|(k, _)| k)
        {
            if let Some(vv) = self.read_at(key, version) {
                out.push((key.clone(), vv));
            }
        }
        out
    }

    fn apply(&mut self, op: &WriteOp, version: Version) {
        match op {
            WriteOp::Put { key, value } => {
                self.data
                    .entry(key.clone())
                    .or_default()
                    .insert(version, Some(value.clone()));
            }
            WriteOp::Delete { key } => {
                self.data
                    .entry(key.clone())
                    .or_default()
                    .insert(version, None);
            }
            WriteOp::DeleteRange { start, end } => {
                let keys: Vec<Key> = self
                    .data
                    .range::<[u8], _>((
                        Bound::Included(start.as_slice()),
                        Bound::Excluded(end.as_slice()),
                    ))
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in keys {
                    self.data.entry(key).or_default().insert(version, None);
                }
            }
        }
    }

    /// Enforce the monotonic-version contract.
    fn check_monotonic(&self, version: Version) -> Result<()> {
        if version > self.max_version {
            Ok(())
        } else {
            Err(StorageError::NonMonotonicVersion {
                got: version,
                latest: self.max_version,
            })
        }
    }
}

/// In-memory MVCC storage engine. Cheap to clone; clones share state.
#[derive(Clone, Default)]
pub struct MemoryEngine {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryEngine {
    /// Create an empty engine.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("storage poisoned")
    }
}

#[async_trait::async_trait]
impl StorageEngine for MemoryEngine {
    type Snapshot = MemorySnapshot;

    async fn put(&self, key: &[u8], value: &[u8], version: Version) -> Result<()> {
        let mut inner = self.lock();
        inner.check_monotonic(version)?;
        inner.apply(
            &WriteOp::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            },
            version,
        );
        inner.max_version = version;
        Ok(())
    }

    async fn merge(&self, key: &[u8], value: &[u8], version: Version) -> Result<bool> {
        let mut inner = self.lock();
        // Per-key LWW: apply only if strictly newer than this key's own latest.
        if inner
            .latest_version_of(key)
            .is_some_and(|cur| version <= cur)
        {
            return Ok(false);
        }
        inner.apply(
            &WriteOp::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            },
            version,
        );
        inner.max_version = inner.max_version.max(version);
        Ok(true)
    }

    async fn merge_tombstone(&self, key: &[u8], version: Version) -> Result<bool> {
        let mut inner = self.lock();
        // Per-key LWW: apply only if strictly newer than this key's own latest.
        if inner
            .latest_version_of(key)
            .is_some_and(|cur| version <= cur)
        {
            return Ok(false);
        }
        inner.apply(&WriteOp::Delete { key: key.to_vec() }, version);
        inner.max_version = inner.max_version.max(version);
        Ok(true)
    }

    async fn delete(&self, key: &[u8], version: Version) -> Result<()> {
        let mut inner = self.lock();
        inner.check_monotonic(version)?;
        inner.apply(&WriteOp::Delete { key: key.to_vec() }, version);
        inner.max_version = version;
        Ok(())
    }

    async fn delete_range(&self, start: &[u8], end: &[u8], version: Version) -> Result<()> {
        if start > end {
            return Err(StorageError::InvalidRange);
        }
        let mut inner = self.lock();
        inner.check_monotonic(version)?;
        inner.apply(
            &WriteOp::DeleteRange {
                start: start.to_vec(),
                end: end.to_vec(),
            },
            version,
        );
        inner.max_version = version;
        Ok(())
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let mut inner = self.lock();
        inner.check_monotonic(batch.version)?;
        for op in &batch.ops {
            if let WriteOp::DeleteRange { start, end } = op
                && start > end
            {
                return Err(StorageError::InvalidRange);
            }
        }
        for op in &batch.ops {
            inner.apply(op, batch.version);
        }
        inner.max_version = batch.version;
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<VersionedValue>> {
        Ok(self.lock().read_at(key, Version::MAX))
    }

    async fn get_at(&self, key: &[u8], version: Version) -> Result<Option<VersionedValue>> {
        Ok(self.lock().read_at(key, version))
    }

    async fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Key, VersionedValue)>> {
        if start > end {
            return Err(StorageError::InvalidRange);
        }
        Ok(self.lock().scan_at(start, end, Version::MAX))
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
        Ok(self.lock().scan_at(start, end, version))
    }

    async fn entries(&self) -> Result<Vec<(Key, VersionedValue)>> {
        let inner = self.lock();
        let mut out = Vec::new();
        for key in inner.data.keys() {
            if let Some(vv) = inner.read_at(key, Version::MAX) {
                out.push((key.clone(), vv));
            }
        }
        Ok(out)
    }

    async fn entries_at(&self, version: Version) -> Result<Vec<(Key, VersionedValue)>> {
        let inner = self.lock();
        let mut out = Vec::new();
        for key in inner.data.keys() {
            if let Some(vv) = inner.read_at(key, version) {
                out.push((key.clone(), vv));
            }
        }
        Ok(out)
    }

    async fn entries_with_tombstones(&self) -> Result<Vec<(Key, Option<Value>, Version)>> {
        let inner = self.lock();
        let mut out = Vec::new();
        for (key, history) in &inner.data {
            // The greatest recorded version, value or tombstone.
            if let Some((&version, slot)) = history.iter().next_back() {
                out.push((key.clone(), slot.clone(), version));
            }
        }
        Ok(out)
    }

    async fn scan_with_tombstones(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Key, Option<Value>, Version)>> {
        let inner = self.lock();
        let mut out = Vec::new();
        for (key, history) in inner
            .data
            .range::<[u8], _>((Bound::Included(start), Bound::Excluded(end)))
        {
            if let Some((&version, slot)) = history.iter().next_back() {
                out.push((key.clone(), slot.clone(), version));
            }
        }
        Ok(out)
    }

    fn snapshot(&self) -> MemorySnapshot {
        let version = self.lock().max_version;
        MemorySnapshot {
            inner: Arc::clone(&self.inner),
            version,
        }
    }

    fn latest_version(&self) -> Version {
        self.lock().max_version
    }
}

/// A snapshot of a [`MemoryEngine`] pinned at a version. Reads filter to entries
/// at or below that version, so later (higher-version) writes are invisible.
#[derive(Clone)]
pub struct MemorySnapshot {
    inner: Arc<Mutex<Inner>>,
    version: Version,
}

#[async_trait::async_trait]
impl Snapshot for MemorySnapshot {
    fn version(&self) -> Version {
        self.version
    }

    async fn get(&self, key: &[u8]) -> Option<VersionedValue> {
        self.inner
            .lock()
            .expect("storage poisoned")
            .read_at(key, self.version)
    }

    async fn scan(&self, start: &[u8], end: &[u8]) -> Vec<(Key, VersionedValue)> {
        if start > end {
            return Vec::new();
        }
        self.inner
            .lock()
            .expect("storage poisoned")
            .scan_at(start, end, self.version)
    }
}
