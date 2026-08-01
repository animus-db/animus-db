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

impl StorageEngine for MemoryEngine {
    type Snapshot = MemorySnapshot;

    fn put(&self, key: &[u8], value: &[u8], version: Version) -> Result<()> {
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

    fn merge(&self, key: &[u8], value: &[u8], version: Version) -> Result<bool> {
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

    fn delete(&self, key: &[u8], version: Version) -> Result<()> {
        let mut inner = self.lock();
        inner.check_monotonic(version)?;
        inner.apply(&WriteOp::Delete { key: key.to_vec() }, version);
        inner.max_version = version;
        Ok(())
    }

    fn delete_range(&self, start: &[u8], end: &[u8], version: Version) -> Result<()> {
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

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let mut inner = self.lock();
        inner.check_monotonic(batch.version)?;
        for op in &batch.ops {
            if let WriteOp::DeleteRange { start, end } = op {
                if start > end {
                    return Err(StorageError::InvalidRange);
                }
            }
        }
        for op in &batch.ops {
            inner.apply(op, batch.version);
        }
        inner.max_version = batch.version;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<VersionedValue>> {
        Ok(self.lock().read_at(key, Version::MAX))
    }

    fn get_at(&self, key: &[u8], version: Version) -> Result<Option<VersionedValue>> {
        Ok(self.lock().read_at(key, version))
    }

    fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Key, VersionedValue)>> {
        if start > end {
            return Err(StorageError::InvalidRange);
        }
        Ok(self.lock().scan_at(start, end, Version::MAX))
    }

    fn entries(&self) -> Result<Vec<(Key, VersionedValue)>> {
        let inner = self.lock();
        let mut out = Vec::new();
        for key in inner.data.keys() {
            if let Some(vv) = inner.read_at(key, Version::MAX) {
                out.push((key.clone(), vv));
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

impl Snapshot for MemorySnapshot {
    fn version(&self) -> Version {
        self.version
    }

    fn get(&self, key: &[u8]) -> Option<VersionedValue> {
        self.inner
            .lock()
            .expect("storage poisoned")
            .read_at(key, self.version)
    }

    fn scan(&self, start: &[u8], end: &[u8]) -> Vec<(Key, VersionedValue)> {
        if start > end {
            return Vec::new();
        }
        self.inner
            .lock()
            .expect("storage poisoned")
            .scan_at(start, end, self.version)
    }
}
