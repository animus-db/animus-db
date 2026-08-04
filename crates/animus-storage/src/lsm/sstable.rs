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
//! - A **block** holds a contiguous run of records (sorted by `(key asc,
//!   version asc)` across the whole table). Its on-disk shape depends on the
//!   table **format version** (carried by the footer magic):
//!   - **v1** (legacy, [`MAGIC_V1`]): `record_bytes || crc32(record_bytes)`,
//!     records full-key encoded.
//!   - **v2** (legacy, [`MAGIC_V2`]): `tag(u8) || payload || crc32(tag ||
//!     payload)`, where `tag` is [`BLOCK_STORED`] (payload is the raw record
//!     bytes) or [`BLOCK_LZ4`] (payload is the record bytes LZ4-compressed with a
//!     length prefix, via `lz4_flex`). The writer emits `LZ4` only when it is
//!     actually smaller, so an incompressible block is never inflated. The CRC
//!     covers `tag || payload`, so it guards the tag too. Records are full-key
//!     encoded.
//!   - **v3** ([`MAGIC_V3`], the current writer): identical block framing to v2
//!     (`tag || payload || crc`, payload optionally LZ4), but the records inside a
//!     block use **shared-prefix key encoding**: each record stores `shared(u32)`
//!     (the count of leading bytes its key shares with the previous record's key
//!     in the same block) and only its differing suffix. The block's first record
//!     stores its full key (`shared == 0`). Because SSTable records are sorted by
//!     key, adjacent keys share long prefixes (e.g. the `escape(table) || …`
//!     prefix every key in a table shares), so this shrinks the key bytes before
//!     LZ4 even sees them, and shrinks the decoded footprint. Reading is: fetch
//!     the block, verify the CRC, read the tag, decompress if `LZ4`, then decode
//!     the records, reconstructing each full key from the previous one.
//! - The **index region** is `serde_json` of `Vec<BlockIndex>` — one entry per
//!   block giving its first key + byte offset + byte length. The reader loads it
//!   once on open and keeps it in memory (it is small: one entry per ~block).
//! - The **footer** is a fixed 24 bytes at end of file: `index_offset: u64`,
//!   `index_len: u64`, `magic: u64` (the format version — [`MAGIC_V1`] or
//!   [`MAGIC_V2`]). The reader reads it with one `read_at` at `size - 24`, then
//!   reads the index region, then individual blocks on demand.
//!
//! A record's value slot is `Some(value)` or `None` (tombstone). The CRC lets a
//! read detect a corrupt/torn block; but note the manifest only references a
//! table whose bytes were `sync`ed before the (atomic) manifest swap, so a torn
//! block is never reachable in practice — the CRC is defence in depth.
//!
//! ## Format compatibility
//!
//! The block index + footer geometry is identical across v1/v2/v3; only the
//! per-block payload framing and the record key encoding differ. A reader takes
//! the format from [`SsTableMeta::format`] (defaulted to v1 for pre-format
//! manifests; the writer always stamps the current version) and decodes a legacy
//! v1 (no tag, full keys) or v2 (tag/LZ4, full keys) block accordingly, so tables
//! written by an older engine still read after an upgrade. New tables are always
//! v3 (compression + shared-prefix keys).
//!
//! [`Disk::read_at`]: animus_env::Disk::read_at

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use animus_env::{Env, Metric, MetricsHandle};
use serde::{Deserialize, Serialize};

use super::bloom::BloomFilter;
use crate::{Key, Result, StorageError, Value, Version};

/// Magic in the footer, identifying a AnimusDB SSTable **v1**: uncompressed
/// blocks (`record_bytes || crc`). The reader takes the format from the manifest
/// ([`SsTableMeta::format`]), not by re-reading the footer, so this is retained
/// for documentation and external tooling.
#[allow(dead_code)]
const MAGIC_V1: u64 = 0x4355_5354_4F53_5331; // "ANIMUS S1"
/// Magic in the footer, identifying a AnimusDB SSTable **v2**: compression-capable
/// blocks (`tag || payload || crc`, payload optionally LZ4) with **full-key**
/// records. Retained for legacy reads + external tooling.
#[allow(dead_code)]
const MAGIC_V2: u64 = 0x4355_5354_4F53_5332; // "ANIMUS S2"
/// Magic in the footer, identifying a AnimusDB SSTable **v3**: same block framing
/// as v2, but records inside a block use **shared-prefix key encoding** (each key
/// stores the count of leading bytes it shares with the previous key in the block,
/// then only its differing suffix). The current writer always stamps this.
const MAGIC_V3: u64 = 0x4355_5354_4F53_5333; // "ANIMUS S3"
/// The SSTable format version a fresh table is written in.
const FORMAT_CURRENT: u32 = 3;
/// Fixed footer size: `index_offset(8) + index_len(8) + magic(8)`.
const FOOTER_LEN: u64 = 24;
/// Soft target for a block's (uncompressed) record bytes before starting a new
/// block.
const TARGET_BLOCK_BYTES: usize = 4 * 1024;

const TAG_VALUE: u8 = 0;
const TAG_TOMBSTONE: u8 = 1;

/// v2 block tag: the payload is the raw (uncompressed) record bytes.
const BLOCK_STORED: u8 = 0;
/// v2 block tag: the payload is the record bytes LZ4-compressed with a length
/// prefix (`lz4_flex::compress_prepend_size`).
const BLOCK_LZ4: u8 = 1;

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
    /// Byte length of the on-disk block, including its framing and the 4-byte
    /// trailing CRC (v1: `record_bytes || crc`; v2: `tag || payload || crc`).
    len: u64,
}

/// Per-table metadata stored in the manifest. Carries no block data, only the
/// bounds, the index region's location, the LSM level, and the key Bloom filter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SsTableMeta {
    /// Sequence number (file is `sst-{seq:06}`).
    pub seq: u64,
    /// LSM level. `0` is the flush tier (overlapping ranges allowed); `1+` hold
    /// non-overlapping runs (leveled compaction). Defaults to `0` for manifests
    /// written before levels existed.
    #[serde(default)]
    pub level: u32,
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
    /// Bloom filter over the table's distinct user keys: a point read can skip
    /// this table when `bloom.may_contain(key)` is false. Defaults to an empty
    /// filter for manifests written before Blooms existed — an empty filter
    /// answers `false`, so to stay correct on such legacy tables we only consult
    /// the Bloom when it was actually built (see [`Self::may_contain`]).
    #[serde(default)]
    pub bloom: BloomFilter,
    /// Whether [`Self::bloom`] was built for this table (false for legacy tables
    /// recovered from a pre-Bloom manifest, where the Bloom must not be trusted).
    #[serde(default)]
    pub has_bloom: bool,
    /// On-disk block format version: `1` = legacy uncompressed blocks (full keys),
    /// `2` = compression-capable (`tag || payload || crc`, full keys), `3` =
    /// compression-capable + **shared-prefix key encoding**. Defaults to `1` so a
    /// table recovered from a pre-format manifest is decoded with the legacy reader;
    /// the writer stamps the current version (`3`).
    #[serde(default = "default_format")]
    pub format: u32,
}

/// serde default for [`SsTableMeta::format`]: a pre-format manifest predates
/// compression, so its tables are legacy v1.
fn default_format() -> u32 {
    1
}

impl SsTableMeta {
    /// Whether `key` could possibly be in this table. First the cheap key-range
    /// gate (`[min_key, max_key]`), then — if a Bloom filter was built — the
    /// Bloom, which can rule out keys inside the range that were never written.
    /// A legacy table without a Bloom (`has_bloom == false`) is gated by range
    /// only, preserving correctness across an engine upgrade.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        match (&self.min_key, &self.max_key) {
            (Some(lo), Some(hi)) => {
                if key < lo.as_slice() || key > hi.as_slice() {
                    return false;
                }
                !self.has_bloom || self.bloom.may_contain(key)
            }
            _ => false,
        }
    }
}

/// Length of the shared leading-byte prefix between two keys.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// Encode one record with **shared-prefix key encoding** (v3): `shared(u32) |
/// unshared_len(u32) | unshared_key | version(u64) | tag(u8) | value_len(u32) |
/// value`, where `key = prev_key[..shared] ++ unshared_key`. The first record in a
/// block passes `prev_key = &[]` (so `shared == 0`, the full key). All integers
/// little-endian.
fn encode_record_prefixed(rec: &Record, prev_key: &[u8], out: &mut Vec<u8>) {
    let shared = common_prefix_len(prev_key, &rec.key);
    let unshared = &rec.key[shared..];
    out.extend_from_slice(&(shared as u32).to_le_bytes());
    out.extend_from_slice(&(unshared.len() as u32).to_le_bytes());
    out.extend_from_slice(unshared);
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

/// Decode the records in one **v3** (shared-prefix) block's record bytes,
/// reconstructing each full key from the previous one. Returns a backend error on
/// a malformed block (incl. a `shared` length exceeding the previous key).
fn decode_block_prefixed(bytes: &[u8]) -> Result<Vec<Record>> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut prev_key: Vec<u8> = Vec::new();
    let need = |i: usize, n: usize, len: usize| -> Result<()> {
        if i + n <= len {
            Ok(())
        } else {
            Err(StorageError::Backend("truncated sstable block".into()))
        }
    };
    while i < bytes.len() {
        need(i, 4, bytes.len())?;
        let shared = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        need(i, 4, bytes.len())?;
        let unshared = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        need(i, unshared, bytes.len())?;
        if shared > prev_key.len() {
            return Err(StorageError::Backend(
                "sstable shared-prefix len exceeds previous key".into(),
            ));
        }
        let mut key = Vec::with_capacity(shared + unshared);
        key.extend_from_slice(&prev_key[..shared]);
        key.extend_from_slice(&bytes[i..i + unshared]);
        i += unshared;
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
        prev_key.clone_from(&key);
        out.push(Record {
            key,
            version,
            value,
        });
    }
    Ok(out)
}

/// Encode one record: `key_len(u32) | key | version(u64) | tag(u8) |
/// value_len(u32) | value`. All integers little-endian.
#[allow(dead_code)] // retained for symmetry with the v1/v2 decode path (legacy reads)
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
    /// Write `records` (already sorted by `(key asc, version asc)`) to `file` at
    /// LSM `level` and return its [`SsTableMeta`] (including a Bloom filter built
    /// over the distinct keys). The caller `sync`s the file afterwards.
    ///
    /// # Errors
    /// Returns [`StorageError::Backend`] on an I/O error.
    pub async fn write<E: Env>(
        env: &E,
        file: &str,
        seq: u64,
        level: u32,
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
        // Previous key within the current block, for shared-prefix encoding (v3).
        // Reset (empty) at each new block, so the block's first record stores its
        // full key (`shared == 0`).
        let mut prev_key: Vec<u8> = Vec::new();
        // Distinct keys for the Bloom filter. Records are sorted by key, so
        // pushing only when the key changes yields the distinct set in order.
        let mut distinct_keys: Vec<Key> = Vec::new();

        // Flush the in-progress block as a **v2** block: `tag || payload || crc`,
        // where the payload is LZ4-compressed iff that is strictly smaller than
        // the raw record bytes (so an incompressible block is stored verbatim,
        // never inflated). The CRC covers `tag || payload`.
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
            let raw = std::mem::take(block_buf);
            let compressed = lz4_flex::compress_prepend_size(&raw);
            let (tag, payload) = if compressed.len() < raw.len() {
                (BLOCK_LZ4, compressed)
            } else {
                (BLOCK_STORED, raw)
            };
            let mut on_disk = Vec::with_capacity(1 + payload.len() + 4);
            on_disk.push(tag);
            on_disk.extend_from_slice(&payload);
            let crc = crc32fast::hash(&on_disk);
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
            if distinct_keys.last().map(Vec::as_slice) != Some(rec.key.as_slice()) {
                distinct_keys.push(rec.key.clone());
            }

            if block_first_key.is_none() {
                block_first_key = Some(rec.key.clone());
                prev_key.clear(); // new block: first record stores its full key
            }
            encode_record_prefixed(rec, &prev_key, &mut block_buf);
            prev_key.clone_from(&rec.key);

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
        footer.extend_from_slice(&MAGIC_V3.to_le_bytes());
        env.append(file, &footer).await.map_err(io)?;

        let file_size = index_offset + index_len + FOOTER_LEN;
        let key_refs: Vec<&[u8]> = distinct_keys.iter().map(Vec::as_slice).collect();
        let bloom = BloomFilter::build(&key_refs);
        Ok(SsTableMeta {
            seq,
            level,
            min_key,
            max_key,
            min_version: if records.is_empty() { 0 } else { min_version },
            max_version: if records.is_empty() { 0 } else { max_version },
            index_offset,
            index_len,
            file_size,
            bloom,
            has_bloom: true,
            format: FORMAT_CURRENT,
        })
    }
}

/// A read handle to an immutable SSTable: holds the metadata + the in-memory
/// block index, and fetches blocks from disk on demand. Cheap to clone (the
/// `meta` and `index` sit behind `Arc`s).
#[derive(Clone)]
pub struct SsTableReader {
    file: Arc<str>,
    meta: Arc<SsTableMeta>,
    index: Arc<Vec<BlockIndex>>,
    /// Shared counter incremented on every block fetched from disk, for the
    /// engine's read-amplification introspection (tests). `None` until the engine
    /// wires one in via [`Self::with_block_counter`].
    block_reads: Option<Arc<AtomicU64>>,
    /// Observability sink (ADR 0015): a block fetched from disk bumps
    /// `storage_sstable_block_reads`. `None` until the engine wires one in via
    /// [`Self::with_metrics`]; recording is observe-only and changes no behavior.
    metrics: Option<MetricsHandle>,
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
        Ok(Self {
            file: Arc::from(file),
            meta: Arc::new(meta),
            index: Arc::new(index),
            block_reads: None,
            metrics: None,
        })
    }

    /// Attach a shared block-read counter (engine introspection). Returns `self`
    /// for chaining at open time.
    #[must_use]
    pub fn with_block_counter(mut self, counter: Arc<AtomicU64>) -> Self {
        self.block_reads = Some(counter);
        self
    }

    /// Attach the observability sink (ADR 0015), so a block fetched from disk bumps
    /// `storage_sstable_block_reads`. Returns `self` for chaining at open time.
    #[must_use]
    pub fn with_metrics(mut self, metrics: MetricsHandle) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// This table's metadata.
    pub fn meta(&self) -> &SsTableMeta {
        &self.meta
    }

    /// Read and verify the block at index entry `bi`, returning its records.
    /// Handles legacy v1 framing (`record_bytes || crc`, full keys), v2 framing
    /// (`tag || payload || crc`, full keys), and v3 (same framing, shared-prefix
    /// keys) per the table's [`SsTableMeta::format`].
    async fn read_block<E: Env>(&self, env: &E, bi: &BlockIndex) -> Result<Vec<Record>> {
        if let Some(counter) = &self.block_reads {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(metrics) = &self.metrics {
            metrics.incr(Metric::StorageSstableBlockReads);
        }
        let raw = env
            .read_at(&self.file, bi.offset, bi.len as usize)
            .await
            .map_err(io)?;
        if (raw.len() as u64) < 4 || (raw.len() as u64) != bi.len {
            return Err(StorageError::Backend("short sstable block read".into()));
        }
        let split = raw.len() - 4;
        let (framed, crc_bytes) = raw.split_at(split);
        let want = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        if crc32fast::hash(framed) != want {
            return Err(StorageError::Backend("sstable block crc mismatch".into()));
        }
        if self.meta.format <= 1 {
            // v1: the CRC'd bytes *are* the record bytes (no tag, no compression),
            // full-key encoded.
            return decode_block(framed);
        }
        // v2/v3: first byte is the block tag; the rest is the (maybe-compressed)
        // record payload. The record encoding then depends on the format: v2 stores
        // full keys, v3 stores shared-prefix keys.
        let (&tag, payload) = framed
            .split_first()
            .ok_or_else(|| StorageError::Backend("empty sstable block".into()))?;
        let decode = |bytes: &[u8]| -> Result<Vec<Record>> {
            if self.meta.format >= 3 {
                decode_block_prefixed(bytes)
            } else {
                decode_block(bytes)
            }
        };
        match tag {
            BLOCK_STORED => decode(payload),
            BLOCK_LZ4 => {
                let rec_bytes = lz4_flex::decompress_size_prepended(payload)
                    .map_err(|e| StorageError::Backend(format!("sstable block decompress: {e}")))?;
                decode(&rec_bytes)
            }
            other => Err(StorageError::Backend(format!(
                "bad sstable block tag {other}"
            ))),
        }
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
        for bi in self.index.iter() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use animus_env::Disk;
    use animus_sim::Simulator;
    use futures::executor::block_on;

    /// Round-trip a table whose blocks compress well (repetitive values): every
    /// record reads back identically, the table is stamped v2, and at least one
    /// block actually used LZ4 (the file is smaller than the raw record bytes).
    #[test]
    fn compressible_block_round_trips_and_shrinks() {
        let sim = Simulator::new(1);
        let env = sim.env(0);
        block_on(async {
            // Highly repetitive values across many records => the block payload
            // compresses, so the writer picks BLOCK_LZ4.
            let mut records = Vec::new();
            let mut raw_bytes = 0usize;
            for i in 0u32..2000 {
                let key = format!("key-{i:05}").into_bytes();
                let value = vec![b'A'; 64];
                raw_bytes += key.len() + value.len();
                records.push(Record {
                    key,
                    version: u64::from(i) + 1,
                    value: Some(value),
                });
            }
            let meta = SsTableWriter::write(&env, "t", 1, 0, &records)
                .await
                .unwrap();
            env.sync("t").await.unwrap();
            assert_eq!(
                meta.format, FORMAT_CURRENT,
                "writer stamps the current format"
            );
            assert!(
                meta.file_size < raw_bytes as u64,
                "expected compression to shrink the file: {} >= {}",
                meta.file_size,
                raw_bytes
            );

            let reader = SsTableReader::open(&env, "t".into(), meta).await.unwrap();
            let read_back = reader.full_scan(&env).await.unwrap();
            assert_eq!(read_back.len(), records.len());
            for (rec, (k, v, slot)) in records.iter().zip(&read_back) {
                assert_eq!((&rec.key, rec.version, &rec.value), (k, *v, slot));
            }
        });
    }

    /// A block of incompressible (high-entropy) bytes is stored verbatim, never
    /// inflated, and still round-trips. We assert the table is no larger than a
    /// bound just above the raw record bytes (framing + index + footer), proving
    /// the writer fell back to BLOCK_STORED rather than paying LZ4's expansion.
    #[test]
    fn incompressible_block_is_stored_not_inflated() {
        let sim = Simulator::new(2);
        let env = sim.env(0);
        block_on(async {
            // A pseudo-random, high-entropy value per record (seeded, deterministic)
            // that LZ4 cannot shrink.
            let mut records = Vec::new();
            let mut raw_bytes = 0usize;
            let mut state = 0x1234_5678_9abc_def0u64;
            for i in 0u32..500 {
                let key = format!("k{i:04}").into_bytes();
                let mut value = Vec::with_capacity(48);
                for _ in 0..48 {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    value.push((state >> 33) as u8);
                }
                raw_bytes += key.len() + value.len() + 16;
                records.push(Record {
                    key,
                    version: u64::from(i) + 1,
                    value: Some(value),
                });
            }
            let meta = SsTableWriter::write(&env, "t2", 1, 0, &records)
                .await
                .unwrap();
            env.sync("t2").await.unwrap();
            // Stored-not-compressed: the file must not be meaningfully larger than
            // the raw payload (a small headroom for per-block tag/crc + index +
            // footer). If the writer had LZ4'd an incompressible block it would be
            // *larger* than raw; this bound would then fail.
            assert!(
                meta.file_size <= raw_bytes as u64 + 4096,
                "incompressible table inflated: file={} raw={}",
                meta.file_size,
                raw_bytes
            );

            let reader = SsTableReader::open(&env, "t2".into(), meta).await.unwrap();
            let read_back = reader.full_scan(&env).await.unwrap();
            assert_eq!(read_back.len(), records.len());
            for (rec, (k, v, slot)) in records.iter().zip(&read_back) {
                assert_eq!((&rec.key, rec.version, &rec.value), (k, *v, slot));
            }
        });
    }

    /// The v3 shared-prefix codec round-trips records with every prefix relation
    /// (identical key, shared prefix, zero shared, empty value), and rejects a
    /// `shared` length that exceeds the previous key.
    #[test]
    fn prefix_codec_round_trips_varied_shared_prefixes() {
        let records = vec![
            Record {
                key: b"animus".to_vec(),
                version: 1,
                value: Some(b"x".to_vec()),
            },
            Record {
                key: b"animus".to_vec(),
                version: 2,
                value: None,
            }, // identical key
            Record {
                key: b"animusdb".to_vec(),
                version: 3,
                value: Some(b"y".to_vec()),
            }, // shares "animus"
            Record {
                key: b"banana".to_vec(),
                version: 4,
                value: Some(b"z".to_vec()),
            }, // 0 shared
            Record {
                key: b"banana".to_vec(),
                version: 5,
                value: Some(Vec::new()),
            }, // empty value
        ];
        let mut buf = Vec::new();
        let mut prev: Vec<u8> = Vec::new();
        for r in &records {
            encode_record_prefixed(r, &prev, &mut buf);
            prev.clone_from(&r.key);
        }
        assert_eq!(decode_block_prefixed(&buf).unwrap(), records);
        // shared=5 against an empty previous key (the first record) is malformed.
        let bad = [5u8, 0, 0, 0, 0, 0, 0, 0];
        assert!(decode_block_prefixed(&bad).is_err());
    }

    /// Shared-prefix encoding is much smaller than full-key encoding when adjacent
    /// keys share a long prefix — isolated from LZ4 by comparing the raw encoded
    /// buffers directly.
    #[test]
    fn prefix_encoding_is_smaller_than_full_keys() {
        let prefix = vec![b'p'; 60];
        let mut records = Vec::new();
        for i in 0u32..1000 {
            let mut key = prefix.clone();
            key.extend_from_slice(format!("{i:06}").as_bytes());
            records.push(Record {
                key,
                version: u64::from(i) + 1,
                value: Some(b"v".to_vec()),
            });
        }
        let mut full = Vec::new();
        for r in &records {
            encode_record(r, &mut full);
        }
        let mut pfx = Vec::new();
        let mut prev: Vec<u8> = Vec::new();
        for r in &records {
            encode_record_prefixed(r, &prev, &mut pfx);
            prev.clone_from(&r.key);
        }
        assert!(
            pfx.len() * 2 < full.len(),
            "prefixed {} not far below full-key {}",
            pfx.len(),
            full.len()
        );
        assert_eq!(decode_block_prefixed(&pfx).unwrap(), records);
    }

    /// A hand-built **legacy v2** (full-key, `BLOCK_STORED`) table still reads after
    /// the upgrade to v3 — the reader picks the decoder from `SsTableMeta::format`.
    #[test]
    fn legacy_v2_full_key_table_still_reads() {
        let sim = Simulator::new(3);
        let env = sim.env(0);
        block_on(async {
            let records = vec![
                Record {
                    key: b"aaa".to_vec(),
                    version: 1,
                    value: Some(b"1".to_vec()),
                },
                Record {
                    key: b"aab".to_vec(),
                    version: 2,
                    value: Some(b"2".to_vec()),
                },
                Record {
                    key: b"abc".to_vec(),
                    version: 3,
                    value: None,
                },
            ];
            // One v2 block: full-key records, framed STORED (`tag || payload || crc`).
            let mut payload = Vec::new();
            for r in &records {
                encode_record(r, &mut payload);
            }
            let mut on_disk = vec![BLOCK_STORED];
            on_disk.extend_from_slice(&payload);
            let crc = crc32fast::hash(&on_disk);
            on_disk.extend_from_slice(&crc.to_le_bytes());
            let block_len = on_disk.len() as u64;
            env.replace("v2t", &on_disk).await.unwrap();
            let index = vec![BlockIndex {
                first_key: b"aaa".to_vec(),
                offset: 0,
                len: block_len,
            }];
            let index_bytes = serde_json::to_vec(&index).unwrap();
            let index_offset = block_len;
            let index_len = index_bytes.len() as u64;
            env.append("v2t", &index_bytes).await.unwrap();
            let mut footer = Vec::new();
            footer.extend_from_slice(&index_offset.to_le_bytes());
            footer.extend_from_slice(&index_len.to_le_bytes());
            footer.extend_from_slice(&MAGIC_V2.to_le_bytes());
            env.append("v2t", &footer).await.unwrap();
            env.sync("v2t").await.unwrap();

            let meta = SsTableMeta {
                seq: 1,
                level: 0,
                min_key: Some(b"aaa".to_vec()),
                max_key: Some(b"abc".to_vec()),
                min_version: 1,
                max_version: 3,
                index_offset,
                index_len,
                file_size: index_offset + index_len + FOOTER_LEN,
                bloom: BloomFilter::default(),
                has_bloom: false,
                format: 2, // legacy full-key format
            };
            let reader = SsTableReader::open(&env, "v2t".into(), meta).await.unwrap();
            let got = reader.full_scan(&env).await.unwrap();
            assert_eq!(got.len(), 3);
            assert_eq!(got[0], (b"aaa".to_vec(), 1, Some(b"1".to_vec())));
            assert_eq!(got[2], (b"abc".to_vec(), 3, None));
            // Point read through the format<=2 branch.
            assert_eq!(
                reader.latest(&env, b"aab").await.unwrap(),
                Some((2, Some(b"2".to_vec())))
            );
        });
    }
}
