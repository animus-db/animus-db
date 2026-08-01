//! Persistent [`StorageEngine`] backed by the pure-Rust `fjall` LSM
//! (feature `fjall`). This is the "borrow a storage engine" path of ADR 0008:
//! the distributed layer is unchanged; only the trait's backing implementation
//! differs. It is **not** used under simulation (which uses [`MemoryEngine`]).
//!
//! ## MVCC key encoding
//!
//! `fjall` is a plain ordered byte-KV store, so MVCC is layered on top. A
//! logical `(user_key, version)` maps to the physical key
//! `escape(user_key) || (u64::MAX - version)`:
//!
//! - `escape` is order-preserving *and* prefix-free (each `0x00` becomes
//!   `0x00 0x01`, terminated by `0x00 0x00`), so every physical key for one
//!   user key shares an unambiguous prefix and they sit contiguously — no user
//!   key's encoding is a prefix of another's.
//! - The inverted version suffix makes a user key's versions sort newest-first,
//!   so a read *as of* `v` is the first version `≤ v` in a prefix scan.
//!
//! Values carry a one-byte tag distinguishing a real value from a tombstone.
//!
//! [`MemoryEngine`]: crate::MemoryEngine

use std::path::Path;
use std::sync::{Arc, Mutex};

use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};

use crate::{
    Key, Result, Snapshot, StorageEngine, StorageError, Value, Version, VersionedValue, WriteBatch,
    WriteOp,
};

const TAG_VALUE: u8 = 0;
const TAG_TOMBSTONE: u8 = 1;
/// Meta key holding the highest version ever written (for the monotonic check
/// and snapshot pinning), surviving reopen.
const META_MAX_VERSION: &[u8] = b"max_version";

/// Order-preserving, prefix-free escape of a user key.
fn escape(user_key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + 2);
    for &b in user_key {
        out.push(b);
        if b == 0x00 {
            out.push(0x01);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
    out
}

/// Physical key for `(user_key, version)`.
fn phys_key(user_key: &[u8], version: Version) -> Vec<u8> {
    let mut k = escape(user_key);
    k.extend_from_slice(&(u64::MAX - version).to_be_bytes());
    k
}

/// Decode the version from a physical key (the trailing 8 bytes).
fn version_of(phys: &[u8]) -> Version {
    let suffix: [u8; 8] = phys[phys.len() - 8..]
        .try_into()
        .expect("8-byte version suffix");
    u64::MAX - u64::from_be_bytes(suffix)
}

fn encode_value(value: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(value.len() + 1);
    v.push(TAG_VALUE);
    v.extend_from_slice(value);
    v
}

fn tombstone() -> Vec<u8> {
    vec![TAG_TOMBSTONE]
}

/// Persistent storage engine. Cheap to clone; clones share the keyspace.
#[derive(Clone)]
pub struct FjallEngine {
    data: PartitionHandle,
    meta: PartitionHandle,
    _keyspace: Keyspace,
    max_version: Arc<Mutex<Version>>,
}

impl FjallEngine {
    /// Open (or create) an engine at `path`.
    ///
    /// # Errors
    /// Returns [`StorageError::Backend`] if the keyspace cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let keyspace = Config::new(path).open().map_err(backend)?;
        let data = keyspace
            .open_partition("data", PartitionCreateOptions::default())
            .map_err(backend)?;
        let meta = keyspace
            .open_partition("meta", PartitionCreateOptions::default())
            .map_err(backend)?;
        let max_version = match meta.get(META_MAX_VERSION).map_err(backend)? {
            Some(slice) => {
                let bytes: [u8; 8] = slice.as_ref().try_into().unwrap_or([0; 8]);
                u64::from_be_bytes(bytes)
            }
            None => 0,
        };
        Ok(Self {
            data,
            meta,
            _keyspace: keyspace,
            max_version: Arc::new(Mutex::new(max_version)),
        })
    }

    fn read_at(&self, user_key: &[u8], version: Version) -> Result<Option<VersionedValue>> {
        // The first physical key `>= phys_key(user_key, version)` is, if it
        // still belongs to this user key, the newest version `<= version`.
        let from = phys_key(user_key, version);
        let prefix = escape(user_key);
        match self.data.range(from..).next() {
            Some(item) => {
                let (k, v) = item.map_err(backend)?;
                if k.starts_with(&prefix) {
                    Ok(decode_value(&v, version_of(&k)))
                } else {
                    Ok(None) // moved past this user key
                }
            }
            None => Ok(None),
        }
    }

    fn scan_at(
        &self,
        start: &[u8],
        end: &[u8],
        version: Version,
    ) -> Result<Vec<(Key, VersionedValue)>> {
        let mut out = Vec::new();
        let lo = escape(start);
        let hi = escape(end);
        let mut last_prefix: Option<Vec<u8>> = None;
        for item in self.data.range(lo..hi) {
            let (k, v) = item.map_err(backend)?;
            let prefix_len = k.len() - 8;
            let prefix = k[..prefix_len].to_vec();
            // Entries are newest-first within a user key; only the first one we
            // see for each user key matters.
            if last_prefix.as_deref() == Some(&prefix) {
                continue;
            }
            last_prefix = Some(prefix);
            let ver = version_of(&k);
            if ver <= version {
                if let Some(vv) = decode_value(&v, ver) {
                    out.push((unescape_prefix(&k), vv));
                }
            } else {
                // The newest is too new; find the newest `<= version` for this key.
                let user_key = unescape_prefix(&k);
                if let Some(vv) = self.read_at(&user_key, version)? {
                    out.push((user_key, vv));
                }
            }
        }
        Ok(out)
    }

    /// The highest version recorded for `user_key`, tombstones included, or
    /// `None` if the key has never been written. Versions sort newest-first
    /// within a user key's prefix, so the first physical entry wins.
    fn latest_version_of(&self, user_key: &[u8]) -> Result<Option<Version>> {
        let prefix = escape(user_key);
        match self.data.range(prefix.clone()..).next() {
            Some(item) => {
                let (k, _) = item.map_err(backend)?;
                Ok(k.starts_with(&prefix).then(|| version_of(&k)))
            }
            None => Ok(None),
        }
    }

    /// Raise the persisted max version to at least `version`, without the
    /// monotonic check (used by [`merge`](StorageEngine::merge), whose versions
    /// need not exceed the engine-wide latest).
    fn raise_max(&self, version: Version) -> Result<()> {
        let mut max = self.max_version.lock().expect("max_version poisoned");
        if version > *max {
            *max = version;
            self.meta
                .insert(META_MAX_VERSION, version.to_be_bytes())
                .map_err(backend)?;
        }
        Ok(())
    }

    fn check_and_bump(&self, version: Version) -> Result<()> {
        let mut max = self.max_version.lock().expect("max_version poisoned");
        if version <= *max {
            return Err(StorageError::NonMonotonicVersion {
                got: version,
                latest: *max,
            });
        }
        *max = version;
        self.meta
            .insert(META_MAX_VERSION, version.to_be_bytes())
            .map_err(backend)?;
        Ok(())
    }

    fn write_op(&self, op: &WriteOp, version: Version) -> Result<()> {
        match op {
            WriteOp::Put { key, value } => self
                .data
                .insert(phys_key(key, version), encode_value(value))
                .map_err(backend),
            WriteOp::Delete { key } => self
                .data
                .insert(phys_key(key, version), tombstone())
                .map_err(backend),
            WriteOp::DeleteRange { start, end } => {
                // Tombstone every user key currently live in [start, end).
                let keys: Vec<Key> = self
                    .scan_at(start, end, version)?
                    .into_iter()
                    .map(|(k, _)| k)
                    .collect();
                for key in keys {
                    self.data
                        .insert(phys_key(&key, version), tombstone())
                        .map_err(backend)?;
                }
                Ok(())
            }
        }
    }
}

impl StorageEngine for FjallEngine {
    type Snapshot = FjallSnapshot;

    fn put(&self, key: &[u8], value: &[u8], version: Version) -> Result<()> {
        self.check_and_bump(version)?;
        self.write_op(
            &WriteOp::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            },
            version,
        )
    }

    fn merge(&self, key: &[u8], value: &[u8], version: Version) -> Result<bool> {
        // Per-key LWW: apply only if strictly newer than this key's own latest.
        if self
            .latest_version_of(key)?
            .is_some_and(|cur| version <= cur)
        {
            return Ok(false);
        }
        self.write_op(
            &WriteOp::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            },
            version,
        )?;
        self.raise_max(version)?;
        Ok(true)
    }

    fn merge_tombstone(&self, key: &[u8], version: Version) -> Result<bool> {
        // Per-key LWW: apply only if strictly newer than this key's own latest.
        if self
            .latest_version_of(key)?
            .is_some_and(|cur| version <= cur)
        {
            return Ok(false);
        }
        self.write_op(&WriteOp::Delete { key: key.to_vec() }, version)?;
        self.raise_max(version)?;
        Ok(true)
    }

    fn delete(&self, key: &[u8], version: Version) -> Result<()> {
        self.check_and_bump(version)?;
        self.write_op(&WriteOp::Delete { key: key.to_vec() }, version)
    }

    fn delete_range(&self, start: &[u8], end: &[u8], version: Version) -> Result<()> {
        if start > end {
            return Err(StorageError::InvalidRange);
        }
        self.check_and_bump(version)?;
        self.write_op(
            &WriteOp::DeleteRange {
                start: start.to_vec(),
                end: end.to_vec(),
            },
            version,
        )
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        for op in &batch.ops {
            if let WriteOp::DeleteRange { start, end } = op {
                if start > end {
                    return Err(StorageError::InvalidRange);
                }
            }
        }
        self.check_and_bump(batch.version)?;
        for op in &batch.ops {
            self.write_op(op, batch.version)?;
        }
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<VersionedValue>> {
        self.read_at(key, Version::MAX)
    }

    fn get_at(&self, key: &[u8], version: Version) -> Result<Option<VersionedValue>> {
        self.read_at(key, version)
    }

    fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Key, VersionedValue)>> {
        if start > end {
            return Err(StorageError::InvalidRange);
        }
        self.scan_at(start, end, Version::MAX)
    }

    fn entries(&self) -> Result<Vec<(Key, VersionedValue)>> {
        let mut out = Vec::new();
        let mut last_prefix: Option<Vec<u8>> = None;
        // Newest-first within each user key, so the first entry per prefix wins.
        for item in self.data.iter() {
            let (k, v) = item.map_err(backend)?;
            let prefix = k[..k.len() - 8].to_vec();
            if last_prefix.as_deref() == Some(&prefix) {
                continue;
            }
            last_prefix = Some(prefix);
            if let Some(vv) = decode_value(&v, version_of(&k)) {
                out.push((unescape_prefix(&k), vv));
            }
        }
        Ok(out)
    }

    fn entries_with_tombstones(&self) -> Result<Vec<(Key, Option<Value>, Version)>> {
        let mut out = Vec::new();
        let mut last_prefix: Option<Vec<u8>> = None;
        // Newest-first within each user key, so the first entry per prefix wins
        // — tombstones included (unlike `entries`).
        for item in self.data.iter() {
            let (k, v) = item.map_err(backend)?;
            let prefix = k[..k.len() - 8].to_vec();
            if last_prefix.as_deref() == Some(&prefix) {
                continue;
            }
            last_prefix = Some(prefix);
            let value = match v.split_first() {
                Some((&TAG_VALUE, payload)) => Some(payload.to_vec()),
                _ => None, // tombstone or empty
            };
            out.push((unescape_prefix(&k), value, version_of(&k)));
        }
        Ok(out)
    }

    fn snapshot(&self) -> FjallSnapshot {
        let version = *self.max_version.lock().expect("max_version poisoned");
        FjallSnapshot {
            engine: self.clone(),
            version,
        }
    }

    fn latest_version(&self) -> Version {
        *self.max_version.lock().expect("max_version poisoned")
    }
}

/// A snapshot of a [`FjallEngine`] pinned at a version.
#[derive(Clone)]
pub struct FjallSnapshot {
    engine: FjallEngine,
    version: Version,
}

impl Snapshot for FjallSnapshot {
    fn version(&self) -> Version {
        self.version
    }

    fn get(&self, key: &[u8]) -> Option<VersionedValue> {
        self.engine.read_at(key, self.version).ok().flatten()
    }

    fn scan(&self, start: &[u8], end: &[u8]) -> Vec<(Key, VersionedValue)> {
        if start > end {
            return Vec::new();
        }
        self.engine
            .scan_at(start, end, self.version)
            .unwrap_or_default()
    }
}

/// Decode a tagged value at `version` into a [`VersionedValue`] (or `None` for a
/// tombstone).
fn decode_value(raw: &[u8], version: Version) -> Option<VersionedValue> {
    match raw.split_first() {
        Some((&TAG_VALUE, payload)) => Some(VersionedValue {
            version,
            value: payload.to_vec(),
        }),
        _ => None, // tombstone or empty
    }
}

/// Recover the original user key from a physical key (drop the 8-byte version
/// suffix, then reverse the escape).
fn unescape_prefix(phys: &[u8]) -> Value {
    let escaped = &phys[..phys.len() - 8];
    let mut out = Vec::with_capacity(escaped.len());
    let mut i = 0;
    while i < escaped.len() {
        if escaped[i] == 0x00 {
            match escaped.get(i + 1) {
                Some(&0x00) => break, // terminator
                Some(&0x01) => {
                    out.push(0x00);
                    i += 2;
                }
                _ => i += 1,
            }
        } else {
            out.push(escaped[i]);
            i += 1;
        }
    }
    out
}

fn backend<E: std::fmt::Display>(err: E) -> StorageError {
    StorageError::Backend(err.to_string())
}
