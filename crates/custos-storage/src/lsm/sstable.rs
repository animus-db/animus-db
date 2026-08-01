//! Immutable on-disk SSTable: a sorted run of MVCC `(key, version, slot)`
//! records, laid out as checksummed **blocks** with an in-file **index** and a
//! **footer**, so a point read fetches only one block via [`Disk::read_at`]
//! instead of loading the whole file.
//!
//! ## File layout
//!
//! ```text
//! [ block 0 ] [ block 1 ] ... [ block K-1 ] [ index region ] [ footer ]
//! ```
//!
//! - A **block** is a concatenation of encoded records (`encode_record`)
//!   followed by a trailing `u32` CRC32 of the record bytes. Records are sorted
//!   by `(key asc, version asc)` across the whole table, and a block holds a
//!   contiguous run of them.
//! - The **index region** is `serde_json` of `Vec<BlockIndex>` — one entry per
//!   block giving its first key + byte offset + byte length. The reader loads it
//!   once on open and keeps it in memory (it is small: one entry per ~block).
//! - The **footer** is a fixed 24 bytes at end of file: `index_offset: u64`,
//!   `index_len: u64`, `MAGIC: u64`. The reader reads it with one `read_at` at
//!   `size - 24`, then reads the index region, then individual blocks on demand.
//!
//! A record's value slot is `Some(value)` or `None` (tombstone). The CRC lets a
//! read detect a corrupt/torn block; but note the manifest only references a
//! table whose bytes were `sync`ed before the (atomic) manifest swap, so a torn
//! block is never reachable in practice — the CRC is defence in depth.
//!
//! [`Disk::read_at`]: custos_env::Disk::read_at

use custos_env::Env;
use serde::{Deserialize, Serialize};

use crate::{Key, Result, StorageError, Value, Version};

/// Magic in the footer, identifying a CustosDB SSTable v1.
const MAGIC: u64 = 0x4355_5354_4F53_5331; // "CUSTOS S1"
/// Fixed footer size: `index_offset(8) + index_len(8) + magic(8)`.
const FOOTER_LEN: u64 = 24;
/// Soft target for a block's record bytes before starting a new block.
const TARGET_BLOCK_BYTES: usize = 4 * 1024;

const TAG_VALUE: u8 = 0;
const TAG_TOMBSTONE: u8 = 1;

/// One MVCC record as written to / read from an SSTable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    /// User key.
    pub key: Key,
    /// MVCC version.
    pub version: Version,
    /// `Some(value)` or `None` for a tombstone.
    pub value: Option<Value>,
}

/// One index entry: the first key of a block and where the block lives.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct BlockIndex {
    /// First (smallest `(key, version)`) record's key in this block.
    first_key: Key,
    /// Byte offset of the block in the file.
    offset: u64,
    /// Byte length of the block (record bytes + 4-byte trailing CRC).
    len: u64,
}

/// Per-table metadata stored in the manifest. Cheap to clone; carries no block
/// data, only the bounds and the index region's location.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SsTableMeta {
    /// Sequence number (file is `sst-{seq:06}`).
    pub seq: u64,
    /// Smallest user key in the table (`None` if the table is empty).
    pub min_key: Option<Key>,
    /// Largest user key in the table.
    pub max_key: Option<Key>,
    /// Smallest version in the table.
    pub min_version: Version,
    /// Largest version in the table.
    pub max_version: Version,
    /// Byte offset of the index region.
    pub index_offset: u64,
    /// Byte length of the index region.
    pub index_len: u64,
    /// Total file size in bytes.
    pub file_size: u64,
}

impl SsTableMeta {
    /// Whether `key` could possibly be in this table (cheap key-range gate; a
    /// bloom filter would refine this — deferred).
    pub fn may_contain(&self, key: &[u8]) -> bool {
        match (&self.min_key, &self.max_key) {
            (Some(lo), Some(hi)) => lo.as_slice() <= key && key <= hi.as_slice(),
            _ => false,
        }
    }
}

/// Encode one record: `key_len(u32) | key | version(u64) | tag(u8) |
/// value_len(u32) | value`. All integers little-endian.
fn encode_record(rec: &Record, out: &mut Vec<u8>) {
    out.extend_from_slice(&(rec.key.len() as u32).to_le_bytes());
    out.extend_from_slice(&rec.key);
    out.extend_from_slice(&rec.version.to_le_bytes());
    match &rec.value {
        Some(v) => {
            out.push(TAG_VALUE);
            out.extend_from_slice(&(v.len() as u32).to_le_bytes());
            out.extend_from_slice(v);
        }
        None => {
            out.push(TAG_TOMBSTONE);
            out.extend_from_slice(&0u32.to_le_bytes());
        }
    }
}

/// Decode the records in one block's record bytes (the block minus its trailing
/// CRC). Returns a backend error on a malformed block.
fn decode_block(bytes: &[u8]) -> Result<Vec<Record>> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let need = |i: usize, n: usize, len: usize| -> Result<()> {
        if i + n <= len {
            Ok(())
        } else {
            Err(StorageError::Backend("truncated sstable block".into()))
        }
    };
    while i < bytes.len() {
        need(i, 4, bytes.len())?;
        let klen = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        need(i, klen, bytes.len())?;
        let key = bytes[i..i + klen].to_vec();
        i += klen;
        need(i, 8, bytes.len())?;
        let version = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        i += 8;
        need(i, 1, bytes.len())?;
        let tag = bytes[i];
        i += 1;
        need(i, 4, bytes.len())?;
        let vlen = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        need(i, vlen, bytes.len())?;
        let value = match tag {
            TAG_VALUE => Some(bytes[i..i + vlen].to_vec()),
            TAG_TOMBSTONE => None,
            _ => return Err(StorageError::Backend("bad sstable record tag".into())),
        };
        i += vlen;
        out.push(Record {
            key,
            version,
            value,
        });
    }
    Ok(out)
}

/// Writes a sorted record slice to a new SSTable file via the `Env` disk.
pub struct SsTableWriter;

impl SsTableWriter {
    /// Write `records` (already sorted by `(key asc, version asc)`) to `file` and
    /// return its [`SsTableMeta`]. The caller `sync`s the file afterwards.
    ///
    /// # Errors
    /// Returns [`StorageError::Backend`] on an I/O error.
    pub async fn write<E: Env>(
        env: &E,
        file: &str,
        seq: u64,
        records: &[Record],
    ) -> Result<SsTableMeta> {
        // Start clean: a prior crashed flush may have left an orphan at this name
        // (we always move the seq forward, but be defensive).
        env.replace(file, &[]).await.map_err(io)?;

        let mut index: Vec<BlockIndex> = Vec::new();
        let mut offset: u64 = 0;
        let mut min_key: Option<Key> = None;
        let mut max_key: Option<Key> = None;
        let mut min_version = Version::MAX;
        let mut max_version = Version::MIN;

        let mut block_buf: Vec<u8> = Vec::new();
        let mut block_first_key: Option<Key> = None;

        // Flush the in-progress block: append `block_buf || crc` and index it.
        async fn flush_block<E: Env>(
            env: &E,
            file: &str,
            block_buf: &mut Vec<u8>,
            block_first_key: &mut Option<Key>,
            index: &mut Vec<BlockIndex>,
            offset: &mut u64,
        ) -> Result<()> {
            if block_buf.is_empty() {
                return Ok(());
            }
            let crc = crc32fast::hash(block_buf);
            let mut on_disk = std::mem::take(block_buf);
            on_disk.extend_from_slice(&crc.to_le_bytes());
            let len = on_disk.len() as u64;
            env.append(file, &on_disk).await.map_err(io)?;
            index.push(BlockIndex {
                first_key: block_first_key
                    .take()
                    .expect("non-empty block has a first key"),
                offset: *offset,
                len,
            });
            *offset += len;
            Ok(())
        }

        for rec in records {
            if min_key.is_none() {
                min_key = Some(rec.key.clone());
            }
            max_key = Some(rec.key.clone());
            min_version = min_version.min(rec.version);
            max_version = max_version.max(rec.version);

            if block_first_key.is_none() {
                block_first_key = Some(rec.key.clone());
            }
            encode_record(rec, &mut block_buf);

            if block_buf.len() >= TARGET_BLOCK_BYTES {
                flush_block(
                    env,
                    file,
                    &mut block_buf,
                    &mut block_first_key,
                    &mut index,
                    &mut offset,
                )
                .await?;
            }
        }
        flush_block(
            env,
            file,
            &mut block_buf,
            &mut block_first_key,
            &mut index,
            &mut offset,
        )
        .await?;

        // Write the index region.
        let index_offset = offset;
        let index_bytes = serde_json::to_vec(&index)
            .map_err(|e| StorageError::Backend(format!("sstable index encode: {e}")))?;
        let index_len = index_bytes.len() as u64;
        env.append(file, &index_bytes).await.map_err(io)?;

        // Write the fixed footer.
        let mut footer = Vec::with_capacity(FOOTER_LEN as usize);
        footer.extend_from_slice(&index_offset.to_le_bytes());
        footer.extend_from_slice(&index_len.to_le_bytes());
        footer.extend_from_slice(&MAGIC.to_le_bytes());
        env.append(file, &footer).await.map_err(io)?;

        let file_size = index_offset + index_len + FOOTER_LEN;
        Ok(SsTableMeta {
            seq,
            min_key,
            max_key,
            min_version: if records.is_empty() { 0 } else { min_version },
            max_version: if records.is_empty() { 0 } else { max_version },
            index_offset,
            index_len,
            file_size,
        })
    }
}

/// A read handle to an immutable SSTable: holds the metadata + the in-memory
/// block index, and fetches blocks from disk on demand. Cheap to clone.
#[derive(Clone)]
pub struct SsTableReader {
    file: String,
    meta: SsTableMeta,
    index: Vec<BlockIndex>,
}

impl SsTableReader {
    /// Open the table named `file` with known `meta`, loading its block index.
    ///
    /// # Errors
    /// Returns [`StorageError::Backend`] on an I/O error or a malformed index.
    pub async fn open<E: Env>(env: &E, file: String, meta: SsTableMeta) -> Result<Self> {
        let index = if meta.index_len == 0 {
            Vec::new()
        } else {
            let bytes = env
                .read_at(&file, meta.index_offset, meta.index_len as usize)
                .await
                .map_err(io)?;
            serde_json::from_slice(&bytes)
                .map_err(|e| StorageError::Backend(format!("corrupt sstable index: {e}")))?
        };
        Ok(Self { file, meta, index })
    }

    /// This table's metadata.
    pub fn meta(&self) -> &SsTableMeta {
        &self.meta
    }

    /// Read and verify the block at index entry `bi`, returning its records.
    async fn read_block<E: Env>(&self, env: &E, bi: &BlockIndex) -> Result<Vec<Record>> {
        let raw = env
            .read_at(&self.file, bi.offset, bi.len as usize)
            .await
            .map_err(io)?;
        if (raw.len() as u64) < 4 || (raw.len() as u64) != bi.len {
            return Err(StorageError::Backend("short sstable block read".into()));
        }
        let split = raw.len() - 4;
        let (rec_bytes, crc_bytes) = raw.split_at(split);
        let want = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        if crc32fast::hash(rec_bytes) != want {
            return Err(StorageError::Backend("sstable block crc mismatch".into()));
        }
        decode_block(rec_bytes)
    }

    /// The index of the block that may contain `key`: the last block whose
    /// `first_key <= key`. Blocks are ordered by first key.
    fn block_for_key(&self, key: &[u8]) -> Option<usize> {
        if self.index.is_empty() {
            return None;
        }
        // partition_point: count of blocks with first_key <= key.
        let p = self
            .index
            .partition_point(|b| b.first_key.as_slice() <= key);
        if p == 0 {
            // key precedes the first block's first key; only block 0 could hold a
            // key equal to its first key — but key < first_key here, so none.
            None
        } else {
            Some(p - 1)
        }
    }

    /// Newest record for `key` (greatest version), or `None`. Reads the one block
    /// that could hold `key`.
    pub async fn latest<E: Env>(
        &self,
        env: &E,
        key: &[u8],
    ) -> Result<Option<(Version, Option<Value>)>> {
        let Some(bidx) = self.block_for_key(key) else {
            return Ok(None);
        };
        let block = self.read_block(env, &self.index[bidx]).await?;
        Ok(block
            .into_iter()
            .filter(|r| r.key == key)
            .map(|r| (r.version, r.value))
            .max_by_key(|(v, _)| *v))
    }

    /// Record for `key` as of `version`: greatest version `≤ version`, or `None`.
    pub async fn get_at<E: Env>(
        &self,
        env: &E,
        key: &[u8],
        version: Version,
    ) -> Result<Option<(Version, Option<Value>)>> {
        let Some(bidx) = self.block_for_key(key) else {
            return Ok(None);
        };
        let block = self.read_block(env, &self.index[bidx]).await?;
        Ok(block
            .into_iter()
            .filter(|r| r.key == key && r.version <= version)
            .map(|r| (r.version, r.value))
            .max_by_key(|(v, _)| *v))
    }

    /// Scan `[start, end)` as of `version`: for each key in range the greatest
    /// version `≤ version`, as `(key, version, slot)`. Reads only the blocks that
    /// overlap the range.
    pub async fn scan_at<E: Env>(
        &self,
        env: &E,
        start: &[u8],
        end: Option<&[u8]>,
        version: Version,
    ) -> Result<Vec<(Key, Version, Option<Value>)>> {
        if self.index.is_empty() {
            return Ok(Vec::new());
        }
        // First block to read: the one that could hold `start` (or block 0 if
        // `start` precedes everything).
        let first = self.block_for_key(start).unwrap_or(0);
        // Collapse to the newest `(key) -> (version, slot)` with version <=
        // version, over the scanned range.
        let mut per_key: std::collections::BTreeMap<Key, (Version, Option<Value>)> =
            std::collections::BTreeMap::new();
        for bi in &self.index[first..] {
            // Stop once a block's first key is already past `end` (blocks are
            // ordered by first key, so nothing later can be in range).
            if let Some(e) = end {
                if bi.first_key.as_slice() >= e {
                    break;
                }
            }
            let block = self.read_block(env, bi).await?;
            for r in block {
                if r.key.as_slice() < start {
                    continue;
                }
                if let Some(e) = end {
                    if r.key.as_slice() >= e {
                        continue;
                    }
                }
                if r.version > version {
                    continue;
                }
                per_key
                    .entry(r.key)
                    .and_modify(|cur| {
                        if r.version > cur.0 {
                            *cur = (r.version, r.value.clone());
                        }
                    })
                    .or_insert((r.version, r.value));
            }
        }
        Ok(per_key
            .into_iter()
            .map(|(k, (v, slot))| (k, v, slot))
            .collect())
    }

    /// Every record in the table (all keys, all versions), for compaction.
    pub async fn full_scan<E: Env>(&self, env: &E) -> Result<Vec<(Key, Version, Option<Value>)>> {
        let mut out = Vec::new();
        for bi in &self.index {
            for r in self.read_block(env, bi).await? {
                out.push((r.key, r.version, r.value));
            }
        }
        Ok(out)
    }
}

fn io(e: std::io::Error) -> StorageError {
    StorageError::Backend(e.to_string())
}
