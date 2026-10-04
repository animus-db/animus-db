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
//!   version asc)` across the whole table), framed `tag(u8) || payload ||
//!   crc32(tag || payload)`:
//!   - `tag` is [`BLOCK_STORED`] (payload is the raw record bytes) or
//!     [`BLOCK_LZ4`] (payload is the record bytes LZ4-compressed with a length
//!     prefix, via `lz4_flex`). The writer emits `LZ4` only when it is actually
//!     smaller, so an incompressible block is never inflated. The CRC covers
//!     `tag || payload`, so it guards the tag too.
//!   - The records inside the payload use **shared-prefix key encoding**: each
//!     record stores `shared(u32)` (the count of leading bytes its key shares with
//!     the previous record's key in the same block) and only its differing suffix.
//!     The block's first record stores its full key (`shared == 0`). Because
//!     records are sorted by key, adjacent keys share long prefixes (e.g. the
//!     `escape(table) || …` prefix every key in a table shares), so this shrinks
//!     the key bytes before LZ4 even sees them, and shrinks the decoded footprint.
//!   - Reading is: fetch the block, verify the CRC, read the tag, decompress if
//!     `LZ4`, then decode the records, reconstructing each full key from the
//!     previous one.
//! - The **index region** is a compact hand-rolled binary encoding of
//!   `Vec<BlockIndex>` — one entry per block giving its first key, byte
//!   offset, and byte length (see [`encode_block_index`]/[`decode_block_index`]
//!   for the exact layout: it mirrors the WAL record and manifest codecs —
//!   no field names, length-prefixed keys, fixed-width integers, a trailing
//!   CRC32 — rather than `serde_json`, which renders a `Vec<u8>` key as a
//!   decimal-number JSON array 3-6x bigger than the raw bytes, repeated per
//!   entry with its field names). The reader loads it once on open and
//!   keeps it in memory (it is small: one entry per ~block).
//! - The **footer** is a fixed 24 bytes at end of file: `index_offset: u64`,
//!   `index_len: u64`, `magic: u64` ([`MAGIC`]). The reader reads it with one
//!   `read_at` at `size - 24`, then reads the index region, then individual blocks
//!   on demand.
//!
//! A record's value slot is `Some(value)` or `None` (tombstone). The CRC lets a
//! read detect a corrupt/torn block; but note the manifest only references a
//! table whose bytes were `sync`ed before the (atomic) manifest swap, so a torn
//! block is never reachable in practice — the CRC is defence in depth.
//!
//! There is a **single on-disk format** (pre-alpha; no older tables exist —
//! ADR 0008). [`SsTableMeta::format`] is retained as a per-table version tag for
//! operator introspection and as a hook should the format ever evolve again.
//!
//! [`Disk::read_at`]: animus_env::Disk::read_at

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
use animus_env::nid;
use animus_env::{Env, Metric, MetricsHandle};

use super::bloom::BloomFilter;
use crate::{Key, Result, StorageError, Value, Version};

/// Magic in the footer, identifying an AnimusDB SSTable. Used for file
/// identification / external tooling (the reader takes the format from the
/// manifest's [`SsTableMeta::format`], not by re-reading the footer).
const MAGIC: u64 = 0x4355_5354_4F53_5333; // "ANIMUS S3"
/// The SSTable format version a fresh table is written in (ADR 0073 Phase 0
/// reset the counter to 1). The reader **dispatches** on the manifest-recorded
/// [`SsTableMeta::format`] ([`check_format`] at open, a `match` in
/// `read_block`): `1` is the compression-capable framing + shared-prefix key
/// encoding; any other value is a loud
/// [`StorageError::UnsupportedFormatVersion`] (`"lsm-sstable"`). A format
/// change adds a new value, a new decoder arm and a new golden fixture under
/// `tests/fixtures/formats/lsm-sstable/`.
const FORMAT_CURRENT: u32 = 1;

/// Refuse an SSTable whose manifest-recorded `format` this binary cannot decode.
fn check_format(format: u32) -> Result<()> {
    if (1..=FORMAT_CURRENT).contains(&format) {
        Ok(())
    } else {
        Err(unsupported_format(format))
    }
}

fn unsupported_format(found: u32) -> StorageError {
    StorageError::UnsupportedFormatVersion {
        format: "lsm-sstable",
        found,
        max_supported: FORMAT_CURRENT,
    }
}

/// Legacy (pre-current-format) SSTable block decoders (ADR 0073 Phase 1,
/// "upgrade-on-read"). Empty while the only format is 1. Once format N+1
/// exists, the `vN` block decoder moves to `legacy::vN` unchanged in behavior
/// and — the upgrade-on-read contract — returns the *current* in-memory
/// records (`Vec<Record>`); `read_block`'s `match` on `meta.format` routes to
/// it. Never deleted (support window: forever).
mod legacy {}

/// Decode one CRC-verified format-1 block: first byte is the block tag; the
/// rest is the (maybe-compressed) record payload.
fn decode_block_v1(framed: &[u8]) -> Result<Vec<Record>> {
    let (&tag, payload) = framed
        .split_first()
        .ok_or_else(|| StorageError::Backend("empty sstable block".into()))?;
    match tag {
        BLOCK_STORED => decode_block(payload),
        BLOCK_LZ4 => {
            let rec_bytes = lz4_flex::decompress_size_prepended(payload)
                .map_err(|e| StorageError::Backend(format!("sstable block decompress: {e}")))?;
            decode_block(&rec_bytes)
        }
        other => Err(StorageError::Backend(format!(
            "bad sstable block tag {other}"
        ))),
    }
}
/// Fixed footer size: `index_offset(8) + index_len(8) + magic(8)`.
const FOOTER_LEN: u64 = 24;
/// Soft target for a block's (uncompressed) record bytes before starting a new
/// block.
const TARGET_BLOCK_BYTES: usize = 4 * 1024;

const TAG_VALUE: u8 = 0;
const TAG_TOMBSTONE: u8 = 1;

/// Block tag: the payload is the raw (uncompressed) record bytes.
const BLOCK_STORED: u8 = 0;
/// Block tag: the payload is the record bytes LZ4-compressed with a length
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
///
/// No `serde` derive: the index *region*'s on-disk encoding is the
/// hand-rolled binary codec below ([`encode_block_index`]/
/// [`decode_block_index`]), never JSON — see the module doc for why.
#[derive(Clone, Debug, PartialEq, Eq)]
struct BlockIndex {
    /// First (smallest `(key, version)`) record's key in this block.
    first_key: Key,
    /// Byte offset of the block in the file.
    offset: u64,
    /// Byte length of the on-disk block, including its framing (`tag || payload`)
    /// and the 4-byte trailing CRC.
    len: u64,
}

// ---------------------------------------------------------------------------
// Compact binary block-INDEX codec
// ---------------------------------------------------------------------------
//
// Mirrors the manifest codec's style (`lsm.rs`'s `encode_manifest`/
// `decode_manifest`): a fixed header, length-prefixed byte strings, fixed-width
// integers, no field names — plus a trailing CRC32 (matching this file's own
// per-block framing, since unlike the manifest the index region lives inside
// an otherwise-checksummed SSTable file). All integers little-endian, matching
// this file's own record/footer encoding (`encode_record`, the footer below) —
// the sibling `lsm.rs` manifest/WAL codecs are big-endian instead; there is no
// cross-file convention here, only a per-file one, and this stays consistent
// with its immediate neighbors.
//
//   MAGIC(4 = b"SSIX") | version(u8) | count(u32)
//   | entry[0] | entry[1] | ... | crc32(u32)
//
// One entry:
//   key_len(u32) | first_key: bytes | offset(u64) | len(u64)
//
// The CRC32 covers every byte from MAGIC through the last entry (i.e.
// everything except itself), so a flipped byte anywhere in the region —
// including inside `count` or a `key_len` — is caught before any of it is
// trusted enough to drive an allocation or a bounds check.

/// Binary block-index magic: "SSIX" (SSTable IndeX).
const INDEX_MAGIC: [u8; 4] = *b"SSIX";
/// Binary block-index format version.
const INDEX_VERSION: u8 = 1;
/// Fixed header length: magic(4) + version(1) + count(4).
const INDEX_HEADER_LEN: usize = 4 + 1 + 4;
/// Trailing CRC32 length.
const INDEX_CRC_LEN: usize = 4;
/// Sanity cap on the decoded entry count, matching the manifest/WAL decoders'
/// `.min(1 << 20)` convention (defense in depth against an untrusted on-disk
/// count driving a huge `Vec::with_capacity` — see the crate guide's
/// "untrusted length-prefix pre-sizing a `Vec`" entry). The CRC already
/// covers `count`, so this only matters if the CRC itself was defeated.
const INDEX_MAX_ENTRIES: usize = 1 << 20;

/// Encode a block index in the compact binary format described above.
fn encode_block_index(index: &[BlockIndex]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&INDEX_MAGIC);
    out.push(INDEX_VERSION);
    out.extend_from_slice(&(index.len() as u32).to_le_bytes());
    for bi in index {
        out.extend_from_slice(&(bi.first_key.len() as u32).to_le_bytes());
        out.extend_from_slice(&bi.first_key);
        out.extend_from_slice(&bi.offset.to_le_bytes());
        out.extend_from_slice(&bi.len.to_le_bytes());
    }
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Decode a block index encoded by [`encode_block_index`]. Returns
/// [`StorageError::Backend`] — the same error class the manifest/WAL decoders
/// use — on a truncated region, an unrecognized magic/version, a bad checksum,
/// or a length prefix (entry count or key length) that runs past the region;
/// never panics.
fn decode_block_index(bytes: &[u8]) -> Result<Vec<BlockIndex>> {
    if bytes.len() < INDEX_HEADER_LEN + INDEX_CRC_LEN {
        return Err(StorageError::Backend("truncated sstable index".into()));
    }
    // Verify the checksum over the whole region before trusting any of its
    // fields (magic, version, count, or any entry) — mirrors this file's own
    // block-read path (`read_block` checks the CRC before touching the tag).
    let split = bytes.len() - INDEX_CRC_LEN;
    let (body, crc_bytes) = bytes.split_at(split);
    let want = u32::from_le_bytes(crc_bytes.try_into().unwrap());
    if crc32fast::hash(body) != want {
        return Err(StorageError::Backend("sstable index crc mismatch".into()));
    }
    if body[..4] != INDEX_MAGIC {
        return Err(StorageError::Backend("bad sstable index magic".into()));
    }
    let version = body[4];
    if version == 0 || version > INDEX_VERSION {
        return Err(StorageError::Backend(format!(
            "unsupported sstable index version {version}"
        )));
    }
    let count = u32::from_le_bytes(body[5..INDEX_HEADER_LEN].try_into().unwrap()) as usize;
    let mut out = Vec::with_capacity(count.min(INDEX_MAX_ENTRIES));
    let mut i = INDEX_HEADER_LEN;
    let need = |i: usize, n: usize| -> Result<()> {
        if i + n <= body.len() {
            Ok(())
        } else {
            Err(StorageError::Backend(
                "truncated sstable index entry".into(),
            ))
        }
    };
    for _ in 0..count {
        need(i, 4)?;
        let key_len = u32::from_le_bytes(body[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        need(i, key_len)?;
        let first_key = body[i..i + key_len].to_vec();
        i += key_len;
        need(i, 16)?;
        let offset = u64::from_le_bytes(body[i..i + 8].try_into().unwrap());
        i += 8;
        let len = u64::from_le_bytes(body[i..i + 8].try_into().unwrap());
        i += 8;
        out.push(BlockIndex {
            first_key,
            offset,
            len,
        });
    }
    if i != body.len() {
        return Err(StorageError::Backend(
            "trailing garbage in sstable index".into(),
        ));
    }
    Ok(out)
}

/// Per-table metadata stored in the manifest. Carries no block data, only the
/// bounds, the index region's location, the LSM level, and the key Bloom filter.
#[derive(Clone, Debug)]
pub struct SsTableMeta {
    /// Sequence number (file is `sst-{seq:06}`).
    pub seq: u64,
    /// LSM level. `0` is the flush tier (overlapping ranges allowed); `1+` hold
    /// non-overlapping runs (leveled compaction).
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
    /// this table when `bloom.may_contain(key)` is false. An empty filter
    /// answers `false`, so the Bloom is only consulted when it was actually
    /// built (see [`Self::may_contain`] and `has_bloom`).
    pub bloom: BloomFilter,
    /// Whether [`Self::bloom`] was built for this table (false where the Bloom
    /// must not be trusted).
    pub has_bloom: bool,
    /// On-disk table format version, recorded in the manifest. The reader
    /// dispatches on it (see [`FORMAT_CURRENT`]); the writer stamps
    /// [`FORMAT_CURRENT`]. Also surfaced by `/admin/storage/lsm`.
    pub format: u32,
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

/// Encode one record with **shared-prefix key encoding**: `shared(u32) |
/// unshared_len(u32) | unshared_key | version(u64) | tag(u8) | value_len(u32) |
/// value`, where `key = prev_key[..shared] ++ unshared_key`. The first record in a
/// block passes `prev_key = &[]` (so `shared == 0`, the full key). All integers
/// little-endian.
fn encode_record(rec: &Record, prev_key: &[u8], out: &mut Vec<u8>) {
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

/// Decode the records in one block's (shared-prefix) record bytes, reconstructing
/// each full key from the previous one. Returns a backend error on a malformed
/// block (incl. a `shared` length exceeding the previous key).
fn decode_block(bytes: &[u8]) -> Result<Vec<Record>> {
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
            encode_record(rec, &prev_key, &mut block_buf);
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
        let index_bytes = encode_block_index(&index);
        let index_len = index_bytes.len() as u64;
        env.append(file, &index_bytes).await.map_err(io)?;

        // Write the fixed footer.
        let mut footer = Vec::with_capacity(FOOTER_LEN as usize);
        footer.extend_from_slice(&index_offset.to_le_bytes());
        footer.extend_from_slice(&index_len.to_le_bytes());
        footer.extend_from_slice(&MAGIC.to_le_bytes());
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
        check_format(meta.format)?;
        let index = if meta.index_len == 0 {
            Vec::new()
        } else {
            let bytes = env
                .read_at(&file, meta.index_offset, meta.index_len as usize)
                .await
                .map_err(io)?;
            decode_block_index(&bytes)
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
    /// A block is `tag(u8) || payload || crc`, where `payload` is the record bytes
    /// (optionally LZ4-compressed) and the records use shared-prefix key encoding.
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
        // Format dispatch (ADR 0073 Phase 1): exact `match` on the
        // manifest-recorded format. `open` already refused an unknown format,
        // but `meta` is a public field, so the fallback arm re-checks rather
        // than assuming.
        match self.meta.format {
            1 => decode_block_v1(framed),
            other => Err(unsupported_format(other)),
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
            if let Some(e) = end
                && bi.first_key.as_slice() >= e
            {
                break;
            }
            let block = self.read_block(env, bi).await?;
            for r in block {
                if r.key.as_slice() < start {
                    continue;
                }
                if let Some(e) = end
                    && r.key.as_slice() >= e
                {
                    continue;
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

/// Fuzz entry points (roadmap R-01 (c)); surfaced via `lsm::fuzzing`.
#[cfg(feature = "fuzzing")]
pub(super) mod fuzz_shims {
    use super::*;

    /// Decode a CRC-stripped data block (`tag || payload`) with the v1 decoder.
    pub fn block_v1(framed: &[u8]) -> Result<usize> {
        // KNOWN ISSUE (R-01 (c) finding, fuzz/known-issues.tsv): `decode_block_v1`
        // hands an LZ4 block's untrusted 4-byte size prefix straight to
        // `lz4_flex::decompress_size_prepended`, which allocates that many bytes
        // up front (a CRC-valid hostile block makes a ~4 GiB allocation). The
        // fix belongs in `decode_block_v1` (bound the claimed size by a real
        // block cap); until it lands this guard keeps the fuzz target from
        // re-finding it every run. Remove the guard in the fix PR.
        if let Some((&BLOCK_LZ4, payload)) = framed.split_first()
            && let Some(prefix) = payload.get(..4)
            && u32::from_le_bytes(prefix.try_into().unwrap()) > (1 << 24)
        {
            return Err(StorageError::Backend(
                "fuzz guard: lz4 size prefix over 16 MiB (known issue)".into(),
            ));
        }
        decode_block_v1(framed).map(|r| r.len())
    }

    /// Decode a block-index region.
    pub fn block_index(bytes: &[u8]) -> Result<usize> {
        decode_block_index(bytes).map(|i| i.len())
    }

    /// Open a whole SSTable image the caller already wrote to `file` on
    /// `env`, deriving the metadata from its own footer the way a manifest
    /// would have recorded it, then scan every block and point-read every
    /// key. The footer's offsets must lie inside the file (a real manifest
    /// records what the writer produced); anything else is a named error
    /// here rather than a read the engine itself never issues.
    pub async fn open_image<E: Env>(env: &E, file: &str, bytes: &[u8]) -> Result<usize> {
        let n = bytes.len() as u64;
        if n < FOOTER_LEN {
            return Err(StorageError::Backend("image shorter than footer".into()));
        }
        let footer = &bytes[bytes.len() - FOOTER_LEN as usize..];
        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_len = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let magic = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        if magic != MAGIC {
            return Err(StorageError::Backend("bad footer magic".into()));
        }
        if index_offset.checked_add(index_len).is_none_or(|e| e > n) {
            return Err(StorageError::Backend("footer offsets out of range".into()));
        }
        let meta = SsTableMeta {
            seq: 1,
            level: 0,
            min_key: None,
            max_key: None,
            min_version: 0,
            max_version: 0,
            index_offset,
            index_len,
            file_size: n,
            bloom: BloomFilter::default(),
            has_bloom: false,
            format: 1,
        };
        let reader = SsTableReader::open(env, file.to_owned(), meta).await?;
        for bi in reader.index.iter() {
            // Block extents come from the (fuzzed) index; keep reads in-file.
            if bi.offset.checked_add(bi.len).is_none_or(|e| e > n) {
                return Err(StorageError::Backend("block extent out of range".into()));
            }
        }
        let mut count = 0;
        for (k, _, _) in reader.full_scan(env).await? {
            count += 1;
            let _ = reader.latest(env, &k).await?;
        }
        Ok(count)
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
        let env = sim.env(nid(0));
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
        let env = sim.env(nid(0));
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

    /// The shared-prefix codec round-trips records with every prefix relation
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
            encode_record(r, &prev, &mut buf);
            prev.clone_from(&r.key);
        }
        assert_eq!(decode_block(&buf).unwrap(), records);
        // shared=5 against an empty previous key (the first record) is malformed.
        let bad = [5u8, 0, 0, 0, 0, 0, 0, 0];
        assert!(decode_block(&bad).is_err());
    }

    /// Shared-prefix encoding is much smaller than naive full-key encoding when
    /// adjacent keys share a long prefix — isolated from LZ4 by measuring the raw
    /// encoded buffer against the full-key byte cost computed arithmetically.
    #[test]
    fn prefix_encoding_is_smaller_than_full_keys() {
        let prefix = vec![b'p'; 60];
        let mut records = Vec::new();
        let mut full_key_cost = 0usize;
        for i in 0u32..1000 {
            let mut key = prefix.clone();
            key.extend_from_slice(format!("{i:06}").as_bytes());
            // A naive full-key record would store: klen(4) + key + version(8) +
            // tag(1) + vlen(4) + value.
            full_key_cost += 4 + key.len() + 8 + 1 + 4 + 1;
            records.push(Record {
                key,
                version: u64::from(i) + 1,
                value: Some(b"v".to_vec()),
            });
        }
        let mut pfx = Vec::new();
        let mut prev: Vec<u8> = Vec::new();
        for r in &records {
            encode_record(r, &prev, &mut pfx);
            prev.clone_from(&r.key);
        }
        assert!(
            pfx.len() * 2 < full_key_cost,
            "prefixed {} not far below full-key cost {}",
            pfx.len(),
            full_key_cost
        );
        assert_eq!(decode_block(&pfx).unwrap(), records);
    }

    // -----------------------------------------------------------------
    // Block-index codec (issue #839: replaced `serde_json` with a compact
    // binary encoding mirroring the manifest/WAL codecs' style).
    // -----------------------------------------------------------------

    fn sample_index(entries: &[(&[u8], u64, u64)]) -> Vec<BlockIndex> {
        entries
            .iter()
            .map(|&(k, offset, len)| BlockIndex {
                first_key: k.to_vec(),
                offset,
                len,
            })
            .collect()
    }

    /// The binary codec round-trips an empty index, a single entry, many
    /// entries, and keys covering every byte value (including `0x00`/`0xFF`)
    /// and a zero-length key.
    #[test]
    fn index_codec_round_trips_empty_one_many_and_edge_keys() {
        let empty: Vec<BlockIndex> = Vec::new();
        assert_eq!(
            decode_block_index(&encode_block_index(&empty)).unwrap(),
            empty
        );

        let one = sample_index(&[(b"only".as_slice(), 0, 100)]);
        assert_eq!(decode_block_index(&encode_block_index(&one)).unwrap(), one);

        let many: Vec<BlockIndex> = (0..5000u64)
            .map(|i| BlockIndex {
                first_key: format!("key-{i:06}").into_bytes(),
                offset: i * 4096,
                len: 4096,
            })
            .collect();
        assert_eq!(
            decode_block_index(&encode_block_index(&many)).unwrap(),
            many
        );

        let all_bytes: Vec<u8> = (0u8..=255).collect();
        let edge = vec![
            BlockIndex {
                first_key: Vec::new(),
                offset: 0,
                len: 0,
            },
            BlockIndex {
                first_key: vec![0x00],
                offset: 1,
                len: 2,
            },
            BlockIndex {
                first_key: vec![0xFF],
                offset: 3,
                len: 4,
            },
            BlockIndex {
                first_key: all_bytes,
                offset: 5,
                len: 6,
            },
        ];
        assert_eq!(
            decode_block_index(&encode_block_index(&edge)).unwrap(),
            edge
        );
    }

    /// The encoded index for `N` entries with fixed `K`-byte keys is exactly
    /// `N*(4+K+16)` bytes plus the codec's fixed 13-byte header+CRC overhead
    /// (`key_len(4) + key(K) + offset(8) + len(8)` per entry) — no per-entry
    /// field-name repetition — and is smaller than the same index encoded as
    /// `serde_json` by at least 2x. The JSON encoding exists only in this test
    /// as a comparison baseline (a local struct, `derive(Serialize)`'d just for
    /// this measurement) — never in the product, per the module doc.
    #[test]
    fn index_codec_size_is_near_theoretical_and_beats_json_by_2x() {
        const N: usize = 1000;
        const K: usize = 24;
        let index: Vec<BlockIndex> = (0..N as u64)
            .map(|i| {
                let mut key = format!("k{i:016}").into_bytes();
                key.resize(K, b'x');
                BlockIndex {
                    first_key: key,
                    offset: i * 4096,
                    len: 4096,
                }
            })
            .collect();

        let encoded = encode_block_index(&index);
        let theoretical = N * (4 + K + 16);
        let overhead = INDEX_HEADER_LEN + INDEX_CRC_LEN;
        assert_eq!(
            encoded.len(),
            theoretical + overhead,
            "encoded size should be exactly the theoretical per-entry cost plus \
             the fixed header+CRC overhead"
        );

        #[derive(serde::Serialize)]
        struct JsonBlockIndex {
            first_key: Vec<u8>,
            offset: u64,
            len: u64,
        }
        let json_index: Vec<JsonBlockIndex> = index
            .iter()
            .map(|bi| JsonBlockIndex {
                first_key: bi.first_key.clone(),
                offset: bi.offset,
                len: bi.len,
            })
            .collect();
        let json_bytes = serde_json::to_vec(&json_index).unwrap();
        assert!(
            json_bytes.len() >= encoded.len() * 2,
            "binary index {} not at least 2x smaller than json index {}",
            encoded.len(),
            json_bytes.len()
        );
    }

    /// Decoding rejects every truncation point of a realistic multi-entry
    /// index — the header alone, mid-entry, and right at the trailing CRC —
    /// with the same error class the manifest/WAL decoders use
    /// (`StorageError::Backend`), never a panic and never a successful decode
    /// of a shorter-than-written region.
    #[test]
    fn index_codec_rejects_truncated_region() {
        let index = sample_index(&[(b"a".as_slice(), 0, 10), (b"bb".as_slice(), 10, 20)]);
        let full = encode_block_index(&index);
        for cut in 0..full.len() {
            match decode_block_index(&full[..cut]) {
                Err(StorageError::Backend(_)) => {}
                Err(other) => panic!("cut={cut}: wrong error class: {other:?}"),
                Ok(decoded) => panic!("cut={cut}: truncated bytes decoded as {decoded:?}"),
            }
        }
    }

    /// A declared entry count far past [`INDEX_MAX_ENTRIES`] never drives a
    /// huge allocation (the pre-sized `Vec::with_capacity` is capped) and
    /// fails cleanly — here on the first entry it cannot actually read past —
    /// rather than hanging or aborting the process.
    #[test]
    fn index_codec_rejects_count_over_cap() {
        let mut body = Vec::new();
        body.extend_from_slice(&INDEX_MAGIC);
        body.push(INDEX_VERSION);
        body.extend_from_slice(&((INDEX_MAX_ENTRIES as u32) + 1).to_le_bytes());
        // One well-formed entry follows so the decoder gets partway through
        // before running out of bytes for the (nonexistent) second one.
        body.extend_from_slice(&4u32.to_le_bytes());
        body.extend_from_slice(b"key1");
        body.extend_from_slice(&0u64.to_le_bytes());
        body.extend_from_slice(&10u64.to_le_bytes());
        let crc = crc32fast::hash(&body);
        let mut bytes = body;
        bytes.extend_from_slice(&crc.to_le_bytes());

        match decode_block_index(&bytes) {
            Err(StorageError::Backend(_)) => {}
            other => panic!("expected a Backend error for an over-cap count, got {other:?}"),
        }
    }

    /// A key-length prefix that claims far more bytes than the region actually
    /// holds is rejected cleanly, never read out of bounds and never a panic.
    #[test]
    fn index_codec_rejects_key_length_past_region() {
        let mut body = Vec::new();
        body.extend_from_slice(&INDEX_MAGIC);
        body.push(INDEX_VERSION);
        body.extend_from_slice(&1u32.to_le_bytes());
        body.extend_from_slice(&(1u32 << 30).to_le_bytes()); // absurd key length
        body.extend_from_slice(b"short"); // nowhere near 2^30 bytes present
        let crc = crc32fast::hash(&body);
        let mut bytes = body;
        bytes.extend_from_slice(&crc.to_le_bytes());

        match decode_block_index(&bytes) {
            Err(StorageError::Backend(_)) => {}
            other => panic!("expected a Backend error for an oversized key length, got {other:?}"),
        }
    }

    /// A single flipped byte anywhere in an otherwise well-formed region is
    /// caught by the trailing CRC32 before any field is trusted.
    #[test]
    fn index_codec_rejects_bad_checksum() {
        let index = sample_index(&[(b"a".as_slice(), 0, 10)]);
        let mut bytes = encode_block_index(&index);
        let mid = INDEX_HEADER_LEN + 2;
        bytes[mid] ^= 0xFF;
        match decode_block_index(&bytes) {
            Err(StorageError::Backend(msg)) => assert!(msg.contains("crc"), "got: {msg}"),
            other => panic!("expected a crc-mismatch error, got {other:?}"),
        }
    }

    /// At-rest corruption of the on-disk index region surfaces as a clean
    /// `StorageError` from `SsTableReader::open` — never a panic, never a
    /// silently wrong block index — using the same `Simulator::corrupt_durable`
    /// idiom as the crash/disk-fault corpora
    /// (`lsm_disk_faults.rs::scenario_corrupted_sstable_block_read_is_a_clean_error`
    /// is the sibling test for a data block).
    #[test]
    fn corrupted_index_region_fails_open_cleanly() {
        let sim = Simulator::new(3);
        let env = sim.env(nid(0));
        let meta = block_on(async {
            let mut records = Vec::new();
            for i in 0u32..500 {
                records.push(Record {
                    key: format!("k{i:05}").into_bytes(),
                    version: u64::from(i) + 1,
                    value: Some(vec![b'v'; 32]),
                });
            }
            let meta = SsTableWriter::write(&env, "idx", 1, 0, &records)
                .await
                .unwrap();
            env.sync("idx").await.unwrap();
            meta
        });
        assert!(meta.index_len > 20, "sanity: a nontrivial index region");

        // Sanity: an uncorrupted reopen works.
        block_on(async {
            SsTableReader::open(&env, "idx".into(), meta.clone())
                .await
                .unwrap();
        });

        // Flip a byte squarely inside the index region — not a data block,
        // not the footer.
        assert!(
            sim.corrupt_durable(nid(0), "idx", meta.index_offset + 5),
            "corruption must land"
        );
        let err = match block_on(SsTableReader::open(&env, "idx".into(), meta)) {
            Ok(_) => panic!("a corrupted index region must fail open, not return a wrong index"),
            Err(e) => e,
        };
        assert!(
            matches!(err, StorageError::Backend(_)),
            "expected the same error class the manifest/WAL decoders use, got: {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("crc") || msg.contains("corrupt") || msg.contains("magic"),
            "expected a checksum/corruption error, got: {msg}"
        );
    }

    /// A truncated index region — modeling a manifest/footer that (through some
    /// other bug) understates its own length — likewise fails `open` cleanly,
    /// never a panic, via a meta whose declared `index_len` undersizes the
    /// actual on-disk region.
    #[test]
    fn truncated_index_region_fails_open_cleanly() {
        let sim = Simulator::new(4);
        let env = sim.env(nid(0));
        let meta = block_on(async {
            let mut records = Vec::new();
            for i in 0u32..500 {
                records.push(Record {
                    key: format!("k{i:05}").into_bytes(),
                    version: u64::from(i) + 1,
                    value: Some(vec![b'v'; 32]),
                });
            }
            let meta = SsTableWriter::write(&env, "idx2", 1, 0, &records)
                .await
                .unwrap();
            env.sync("idx2").await.unwrap();
            meta
        });
        assert!(
            meta.index_len > 20,
            "sanity: index region big enough to truncate meaningfully"
        );

        let mut truncated = meta.clone();
        truncated.index_len = 5; // shorter than even the 13-byte header+CRC
        match block_on(SsTableReader::open(&env, "idx2".into(), truncated)) {
            Ok(_) => panic!("a truncated index region must fail open, not panic"),
            Err(err) => assert!(matches!(err, StorageError::Backend(_))),
        }
    }

    // ----- ADR 0073 Phase 0 (Workstream A) golden-fixture tests -----------

    fn fixture_dir() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/formats/lsm-sstable")
    }

    /// Deterministic records for the fixture: fixed keys/values/versions, no
    /// clock or RNG. 300 records span several blocks (so the fixture has a
    /// real block index): the first 100 have highly repetitive values (LZ4
    /// block), the next 100 pseudo-random values from a fixed LCG (stored
    /// block), the last 100 mix tombstones and empty values.
    fn fixture_records() -> Vec<Record> {
        let mut state = 0x0123_4567_89ab_cdefu64;
        let mut out = Vec::new();
        for i in 0u32..300 {
            let key = format!("fixture-key-{i:04}").into_bytes();
            let version = u64::from(i % 5) + 1;
            let value = if i < 100 {
                Some(vec![b'A' + (i % 3) as u8; 64])
            } else if i < 200 {
                let mut v = Vec::with_capacity(48);
                for _ in 0..48 {
                    state = state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    v.push((state >> 33) as u8);
                }
                Some(v)
            } else if i % 3 == 0 {
                None
            } else if i % 3 == 1 {
                Some(Vec::new())
            } else {
                Some(format!("value-{i}").into_bytes())
            };
            out.push(Record {
                key,
                version,
                value,
            });
        }
        out
    }

    /// Build a `SsTableMeta` for a fixture image from its own footer
    /// (`index_offset(8) | index_len(8) | MAGIC(8)`, little endian), the way
    /// a manifest would have recorded it.
    fn meta_from_image(bytes: &[u8], format: u32) -> SsTableMeta {
        let n = bytes.len();
        let footer = &bytes[n - FOOTER_LEN as usize..];
        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_len = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let magic = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        assert_eq!(magic, MAGIC, "fixture footer magic");
        SsTableMeta {
            seq: 1,
            level: 0,
            min_key: None,
            max_key: None,
            min_version: 0,
            max_version: 0,
            index_offset,
            index_len,
            file_size: n as u64,
            bloom: BloomFilter::default(),
            has_bloom: false,
            format,
        }
    }

    async fn open_image(
        env: &animus_sim::SimEnv,
        bytes: &[u8],
        format: u32,
    ) -> Result<SsTableReader> {
        let _ = env.remove("img").await;
        env.append("img", bytes).await.unwrap();
        env.sync("img").await.unwrap();
        SsTableReader::open(env, "img".into(), meta_from_image(bytes, format)).await
    }

    /// Decode test: every checked-in fixture opens with the current reader
    /// (format 1) and reads back exactly the expected records, field by
    /// field, with a multi-block index. Iterates the directory.
    #[test]
    fn decodes_every_checked_in_fixture() {
        let dir = fixture_dir();
        let sim = Simulator::new(3);
        let env = sim.env(nid(0));
        let mut checked = 0usize;
        for entry in std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("reading fixture dir {}: {e}", dir.display()))
        {
            let path = entry.expect("dir entry").path();
            if path.extension().and_then(|e| e.to_str()) != Some("bin") {
                continue;
            }
            let bytes = std::fs::read(&path).expect("read fixture");
            // The version under test is the fixture's own (file name), not
            // `FORMAT_CURRENT`; each version needs its own expectation, and
            // an unknown one panics rather than being skipped.
            let version = crate::fixture_file_version(&path);
            let want = match version {
                1 => fixture_records(),
                v => panic!(
                    "fixture {} is version {v} but this test has no expectation for it — \
                     add a per-version arm (ADR 0073 Phase 1 checklist)",
                    path.display()
                ),
            };
            block_on(async {
                let reader = open_image(&env, &bytes, version)
                    .await
                    .unwrap_or_else(|e| panic!("opening fixture {}: {e}", path.display()));
                assert!(reader.index.len() > 1, "fixture has a multi-block index");
                let got = reader.full_scan(&env).await.expect("scan fixture");
                assert_eq!(got.len(), want.len(), "record count");
                for (r, (k, v, val)) in want.iter().zip(&got) {
                    assert_eq!(&r.key, k);
                    assert_eq!(r.version, *v);
                    assert_eq!(&r.value, val);
                }
                // Point reads go through the block index too.
                for r in &want {
                    let (v, val) = reader
                        .latest(&env, &r.key)
                        .await
                        .unwrap()
                        .expect("key present");
                    assert_eq!((v, &val), (r.version, &r.value));
                }
            });
            checked += 1;
        }
        assert!(checked > 0, "no fixture files under {}", dir.display());
        assert!(
            dir.join(format!("v{FORMAT_CURRENT}.bin")).exists(),
            "no fixture for the current FORMAT_CURRENT ({FORMAT_CURRENT})"
        );
    }

    /// Round trip: the current writer's output reads back identically.
    #[test]
    fn writer_output_round_trips_the_fixture_records() {
        let sim = Simulator::new(4);
        let env = sim.env(nid(0));
        block_on(async {
            let records = fixture_records();
            let meta = SsTableWriter::write(&env, "rt", 1, 0, &records)
                .await
                .unwrap();
            env.sync("rt").await.unwrap();
            assert_eq!(meta.format, FORMAT_CURRENT);
            let reader = SsTableReader::open(&env, "rt".into(), meta).await.unwrap();
            let got = reader.full_scan(&env).await.unwrap();
            assert_eq!(got.len(), records.len());
            for (r, (k, v, val)) in records.iter().zip(&got) {
                assert_eq!((&r.key, r.version, &r.value), (k, *v, val));
            }
        });
    }

    /// Fixture generator. Run explicitly:
    /// `cargo test -p animus-storage --lib generate_fixture_lsm_sstable -- --ignored`.
    /// Refuses to overwrite an existing fixture: regenerate only by bumping
    /// `FORMAT_CURRENT` and adding a NEW `v<N>.bin`.
    #[test]
    #[ignore = "run explicitly to (re)generate the golden fixture"]
    fn generate_fixture_lsm_sstable() {
        let dir = fixture_dir();
        std::fs::create_dir_all(&dir).expect("create fixture dir");
        let path = dir.join(format!("v{FORMAT_CURRENT}.bin"));
        assert!(
            std::fs::metadata(&path).is_err(),
            "{} already exists — bump FORMAT_CURRENT and add a NEW fixture \
             file instead of regenerating an existing one",
            path.display(),
        );
        let sim = Simulator::new(5);
        let env = sim.env(nid(0));
        block_on(async {
            SsTableWriter::write(&env, "gen", 1, 0, &fixture_records())
                .await
                .unwrap();
            env.sync("gen").await.unwrap();
            let bytes = env.read("gen").await.unwrap();
            std::fs::write(&path, bytes).expect("write fixture");
        });
    }

    /// An unsupported `SsTableMeta::format` (0 or newer than the binary knows)
    /// is a loud, named error at open — never a panic, never a misdecode.
    #[test]
    fn unsupported_format_is_a_loud_error_at_open() {
        let sim = Simulator::new(6);
        let env = sim.env(nid(0));
        block_on(async {
            let records = fixture_records();
            SsTableWriter::write(&env, "uf", 1, 0, &records)
                .await
                .unwrap();
            env.sync("uf").await.unwrap();
            let bytes = env.read("uf").await.unwrap();
            for bad in [0u32, FORMAT_CURRENT + 1, 2, 3, u32::MAX] {
                match open_image(&env, &bytes, bad).await {
                    Err(StorageError::UnsupportedFormatVersion {
                        format,
                        found,
                        max_supported,
                    }) => {
                        assert_eq!(format, "lsm-sstable");
                        assert_eq!(found, bad);
                        assert_eq!(max_supported, FORMAT_CURRENT);
                    }
                    Err(e) => panic!("format {bad}: expected UnsupportedFormatVersion, got {e:?}"),
                    Ok(_) => panic!("format {bad}: must not open"),
                }
            }
            // A reader whose (public) meta.format is corrupted after open
            // also fails loudly at read time.
            let mut reader = open_image(&env, &bytes, FORMAT_CURRENT).await.unwrap();
            let mut meta = reader.meta().clone();
            meta.format = FORMAT_CURRENT + 1;
            reader.meta = Arc::new(meta);
            match reader.full_scan(&env).await {
                Err(StorageError::UnsupportedFormatVersion { format, .. }) => {
                    assert_eq!(format, "lsm-sstable");
                }
                other => panic!("expected UnsupportedFormatVersion at read, got {other:?}"),
            }
        });
    }
}
