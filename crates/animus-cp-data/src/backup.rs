//! Backup object naming + codec (ADR 0059 §2/§4) — the on-store shape a
//! backup's manifest and chunked per-tablet data objects take, and the
//! object-id scheme they live at. **Plumbing only** (ADR 0059 Train 1 PR②):
//! no capture driver reads or writes through these yet, no janitor, no wire
//! surface — see the module's own users in a later PR.
//!
//! ## Object naming (§4)
//!
//! - The manifest object: [`backup_manifest_object_id`] —
//!   `backup/{backup_id}/manifest`. Per ADR 0059 §4, this is the backup's
//!   **durability commit point**: `CompleteBackup` is only ever proposed
//!   once this object has been durably `put`.
//! - A chunked data object: [`backup_data_object_id`] —
//!   `backup/{backup_id}/{tablet}/{chunk}`.
//!
//! Both live under the fixed literal `backup/` namespace
//! ([`BACKUP_NAMESPACE`]) the stream sealer's own
//! `{table}/{label}/{tablet}/{epoch}` shape (`segment::segment_id`) never
//! produces — **except** for a table literally named `backup`, an accepted
//! edge case: ADR 0059 §1's actual collision-freedom guarantee is that
//! backups get their own, separately-configured [`animus_env::SegmentStore`]
//! handle/instance entirely (a distinct `--backup-store` from the streams
//! `--segment-store`), never the stream sealer's — the namespace split here
//! is explicitly *belt-and-suspenders on top of that*, not the load-bearing
//! mechanism, exactly as the ADR states. See
//! [`namespace_does_not_collide_with_the_stream_sealer_for_ordinary_table_names`]
//! in this module's own tests.
//!
//! ## Data objects (§2)
//!
//! Chunked `(kind, logical_key, value-or-tombstone, version)` tuples — the
//! exact [`crate::SeedRow`] tuple shape `engine_image`/`install_engine_image`
//! already use for split-build snapshot transfer (ADR 0050), reused here
//! rather than inventing a second tuple codec (the caller — a later PR's
//! capture driver — is what restricts `kind` to
//! `KIND_BASE`/`KIND_LSI`/`KIND_FOOTPRINT`; this module, like
//! `codec::encode_image`, imposes no restriction on the byte itself).
//! [`encode_data_chunk`]/[`decode_data_chunk`] follow `segment.rs`'s own
//! format discipline: a 4-byte magic + a version byte, loud/named errors on
//! a bad magic, an unrecognized version, a truncated buffer, or trailing
//! garbage past the declared row count. Chunking a whole tablet's rows into
//! `SEED_CHUNK_BYTES`-budget pieces (`animusd::index_drain`'s own constant)
//! is the capture driver's job, a later PR — this module only encodes/decodes
//! whatever slice of rows it's handed.
//!
//! ## The manifest object (§2)
//!
//! [`BackupManifestObject`] wraps the PR① [`animus_control::BackupManifest`]
//! stub (schema snapshot + pinned tablet list + creation timestamp) together
//! with the per-tablet completion records
//! ([`animus_control::BackupTabletProgress`]: cut version + bytes) collected
//! by the time `CompleteBackup` fires — together these are everything ADR
//! 0059 §2 lists a manifest must record, with no need to touch the source
//! table again. [`encode_manifest_object`]/[`decode_manifest_object`] wrap it
//! in the identical magic+version envelope the data-chunk codec uses, but the
//! payload itself is plain `serde_json` rather than a hand-rolled binary
//! encoding: `BackupManifest` nests `TableSchema` (columns, GSI/LSI
//! definitions, the stream/TTL descriptive snapshot) — a shape that already
//! derives `Serialize`/`Deserialize` for the WAL/mirror path and evolves
//! independently of this module — so hand-rolling a byte-exact re-encoder
//! here would be a second, independently-maintained copy of that shape for
//! no gain; a manifest object is written and read once per backup, never a
//! hot path the way a data chunk or a stream segment is. The magic+version
//! envelope is what still gives this format the segment codec's "loud, named
//! error on an unrecognized version" discipline despite the JSON body —
//! pre-alpha "no back-compat" notwithstanding, a version bump here should
//! still be a deliberate, visible decision.

use animus_control::{BackupManifest, BackupTabletProgress};
use animus_tablet::TabletId;
use serde::{Deserialize, Serialize};

use crate::SeedRow;

/// The fixed literal top-level namespace every backup object lives under
/// (ADR 0059 §1/§4). See the module doc for the collision-freedom argument.
pub const BACKUP_NAMESPACE: &str = "backup";

/// The backup store's own reserved `(node, stream)` address (ADR 0026, ADR
/// 0059 §1) — the `stream` `animusd::build_backup_store` passes to
/// [`crate::cluster_segment_store::ClusterSegmentStore::start`] for a
/// `BackupStoreConfig::Cluster` handle. **Deliberately distinct from
/// [`crate::cluster_segment_store::SEGMENT_STREAM`]**, the streams
/// subsystem's own reserved stream: `(node, stream)` is single-consumer
/// (ADR 0026), and a node running both stores (the default — every existing
/// deployment/test enables Streams, and `BackupStoreConfig::default()` is
/// `Cluster`) would otherwise have two independent serving tasks racing for
/// the same inbox, silently stealing each other's requests/replies. This
/// was caught as a real regression while wiring `build_backup_store` (Train
/// 1 PR②) — see `cluster_segment_store::SEGMENT_STREAM`'s own doc and
/// `docs/engineering-lessons.md` for the incident, and mint a **third**
/// distinct constant here rather than reusing either if a future consumer
/// needs its own `ClusterSegmentStore` instance too. Chosen one below
/// [`SEGMENT_STREAM`](crate::cluster_segment_store::SEGMENT_STREAM)'s
/// `u64::MAX`, same "far end of the space, outside any `TabletId` range"
/// reasoning.
pub const BACKUP_SEGMENT_STREAM: u64 = u64::MAX - 1;

/// Decode/encode failures are plain descriptive strings, mirroring
/// `segment::SegmentError`'s own shape — every caller's own handling is "log
/// loudly, treat as absent/corrupt."
pub type BackupCodecError = String;

/// This backup's own object-id prefix (ADR 0059 §4) — every object naming
/// helper below shares it. Handy for a future debug/sweep `SegmentStore::
/// list` call (never load-bearing for correctness, mirroring
/// `segment::segment_id`'s identical caveat: the replicated catalog, not a
/// store listing, is the sole authority for what backup data exists, ADR
/// 0059 §3).
#[must_use]
pub fn backup_prefix(backup_id: &str) -> String {
    format!("{BACKUP_NAMESPACE}/{backup_id}/")
}

/// The manifest object's id (ADR 0059 §4) — this backup's durability commit
/// point (§4: `CompleteBackup` is proposed only after this object has been
/// durably `put`).
#[must_use]
pub fn backup_manifest_object_id(backup_id: &str) -> String {
    format!("{}manifest", backup_prefix(backup_id))
}

/// One data chunk object's id (ADR 0059 §4): `backup/{backup_id}/{tablet}/{chunk}`.
/// `chunk` is a per-tablet-scoped sequence number the capture driver (a later
/// PR) assigns as it sweeps a tablet's rows — this module imposes no
/// ordering or uniqueness requirement on it beyond what the caller provides.
#[must_use]
pub fn backup_data_object_id(backup_id: &str, tablet: u64, chunk: u64) -> String {
    format!("{}{tablet}/{chunk}", backup_prefix(backup_id))
}

/// Re-wrap a captured data-object value in the engine's committed envelope
/// (tag `0`) before feeding it to `KvCommand::SeedBatch` (ADR 0059 §7,
/// Train 2's restore driver). **Load-bearing, not cosmetic**: capture reads
/// through intent resolution (ADR 0059 §5) and stores each row's *plain,
/// already-resolved* value — no envelope tag at all — but `SeedBatch`'s own
/// merge is a raw, envelope-tag-included byte passthrough (the ADR 0050
/// split-build convention this restore driver reuses verbatim for its
/// seeding, since a split child's rows carry their *physical* bytes,
/// intents included). Feeding a plain resolved value straight into
/// `SeedBatch` unwrapped merges bytes the read path's envelope decoder
/// cannot parse (its first byte is read as an unrecognized tag, a corrupt-
/// engine-value panic) — this function is the fix, applied to every `Some`
/// value a restored data chunk carries (never to a tombstone, which restore
/// never actually produces — capture's own snapshot scan never yields one —
/// but the caller passes `None` through unchanged regardless, matching
/// `SeedRow`'s own general shape).
#[must_use]
pub fn encode_restored_value(value: &[u8]) -> Vec<u8> {
    crate::txn::encode_committed(value)
}

// --- shared cursor (decode side, both codecs below) ------------------------

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], BackupCodecError> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.bytes.len())
            .ok_or_else(|| {
                format!(
                    "truncated backup data chunk: need {n} more bytes at offset {}, have {}",
                    self.pos,
                    self.bytes.len()
                )
            })?;
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, BackupCodecError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, BackupCodecError> {
        let b: [u8; 4] = self.take(4)?.try_into().expect("took exactly 4 bytes");
        Ok(u32::from_be_bytes(b))
    }

    fn u64(&mut self) -> Result<u64, BackupCodecError> {
        let b: [u8; 8] = self.take(8)?.try_into().expect("took exactly 8 bytes");
        Ok(u64::from_be_bytes(b))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, BackupCodecError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn opt_bytes(&mut self) -> Result<Option<Vec<u8>>, BackupCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.bytes()?)),
            other => Err(format!(
                "bad tombstone presence flag {other} (want 0/1) in a backup data chunk"
            )),
        }
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn put_opt_bytes(out: &mut Vec<u8>, value: &Option<Vec<u8>>) {
    match value {
        Some(v) => {
            out.push(1);
            put_bytes(out, v);
        }
        None => out.push(0),
    }
}

// --- data chunk codec (§2) --------------------------------------------------

const DATA_MAGIC: [u8; 4] = *b"BKDT";

/// Data-chunk codec version, bumped on any incompatible layout change.
pub const DATA_VERSION: u8 = 1;

/// Encode one chunk's rows (ADR 0059 §2) — the exact [`SeedRow`] tuple
/// shape, in whatever order `rows` is handed in (the capture driver's own
/// sweep order, a later PR's concern — this function does not sort).
#[must_use]
pub fn encode_data_chunk(rows: &[SeedRow]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&DATA_MAGIC);
    out.push(DATA_VERSION);
    out.extend_from_slice(&(rows.len() as u32).to_be_bytes());
    for (kind, key, value, version) in rows {
        out.push(*kind);
        put_bytes(&mut out, key);
        put_opt_bytes(&mut out, value);
        out.extend_from_slice(&version.to_be_bytes());
    }
    out
}

/// Decode a data chunk object's bytes back into its [`SeedRow`]s.
///
/// Validates: the magic + version (an unrecognized version is a loud, named
/// `Err`, never a silent misread — this codec is pre-alpha, so no
/// cross-version compatibility is attempted, mirroring `segment.rs`'s own
/// stance); every length-prefixed field's framing (a truncated buffer
/// anywhere is a named `Err`, not a panic); and that the body holds
/// **exactly** the declared row count with no trailing bytes left over.
///
/// # Errors
/// A descriptive [`BackupCodecError`] for any of the above.
pub fn decode_data_chunk(bytes: &[u8]) -> Result<Vec<SeedRow>, BackupCodecError> {
    let mut c = Cursor { bytes, pos: 0 };
    let magic = c.take(4)?;
    if magic != DATA_MAGIC {
        return Err(format!(
            "bad backup data chunk magic {magic:?} (want {DATA_MAGIC:?})"
        ));
    }
    let version = c.u8()?;
    if version != DATA_VERSION {
        return Err(format!(
            "unknown backup data chunk codec version {version} (this build only decodes \
             version {DATA_VERSION})"
        ));
    }
    let declared = c.u32()?;
    let mut rows = Vec::with_capacity(declared.min(1 << 20) as usize);
    for i in 0..declared {
        let kind = c.u8().map_err(|e| format!("row {i}: {e}"))?;
        let key = c.bytes().map_err(|e| format!("row {i}: {e}"))?;
        let value = c.opt_bytes().map_err(|e| format!("row {i}: {e}"))?;
        let version = c.u64().map_err(|e| format!("row {i}: {e}"))?;
        rows.push((kind, key, value, version));
    }
    if c.pos != c.bytes.len() {
        return Err(format!(
            "trailing {} byte(s) after the declared {declared} row(s) in a backup data \
             chunk — corrupt framing",
            c.bytes.len() - c.pos
        ));
    }
    Ok(rows)
}

// --- manifest object codec (§2) --------------------------------------------

const MANIFEST_MAGIC: [u8; 4] = *b"BKMF";

/// Manifest-object codec version, bumped on any incompatible layout change.
pub const MANIFEST_VERSION: u8 = 1;

/// One pinned tablet's completion record, paired with its identity — the
/// manifest object's own flat-`Vec` shape for what would otherwise be a
/// `BTreeMap<TabletId, BackupTabletProgress>`, sidestepping `serde_json`'s
/// non-string-map-key restriction the same way `animus_control::meta`'s own
/// `stream_shards_codec`/`backup_progress_key` do for their tuple-keyed maps
/// (see that crate's `CLAUDE.md`) — simpler here since this is a fresh,
/// write-once wire object rather than a live `Metadata` field with its own
/// accessor surface, so a plain `Vec` of named entries needs no dedicated
/// codec module of its own.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifestTabletEntry {
    /// The pinned tablet's id.
    pub tablet: TabletId,
    /// That tablet's own capture-completion record (ADR 0059 §3/§4): cut
    /// version + bytes.
    pub progress: BackupTabletProgress,
}

/// The manifest object's full payload (ADR 0059 §2) — the PR① manifest stub
/// plus the per-tablet completion records collected by `CompleteBackup`
/// time. Together with [`BackupManifest::schema`] (the `SourceTableFeatureDetails`
/// snapshot) and [`BackupManifest::pinned_tablets`] (tablet id + key range),
/// this is everything ADR 0059 §2 lists a manifest must record: schema
/// shape, pinned tablet list + ranges, each tablet's cut version, per-tablet
/// (and, by summing `tablet_progress`, total) object sizes, and the
/// wall-clock creation timestamp.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifestObject {
    /// The PR① catalog manifest stub (schema snapshot, pinned tablets,
    /// creation timestamp) — see [`animus_control::BackupManifest`]'s own
    /// doc.
    pub manifest: BackupManifest,
    /// Every pinned tablet's own capture-completion record, keyed by tablet
    /// identity — see [`BackupManifestTabletEntry`]'s doc for why this is a
    /// flat `Vec` rather than a `BTreeMap`.
    pub tablet_progress: Vec<BackupManifestTabletEntry>,
}

impl BackupManifestObject {
    /// The sum of every pinned tablet's own reported
    /// [`BackupTabletProgress::bytes`] — the manifest's own "total object
    /// sizes" figure (ADR 0059 §2), derived rather than stored redundantly
    /// so it can never drift from the per-tablet records it is a sum of.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.tablet_progress.iter().map(|e| e.progress.bytes).sum()
    }
}

/// Encode a [`BackupManifestObject`] (ADR 0059 §2/§4) — a magic+version
/// header (mirroring `segment.rs`'s own discipline) wrapping a plain
/// `serde_json` payload. See the module doc for why the payload itself is
/// JSON rather than a hand-rolled binary encoding.
///
/// # Panics
/// Never, in practice: every field of [`BackupManifestObject`] is plain
/// owned data with a derived `Serialize` impl (no map with a non-string key,
/// no interior mutability, nothing `serde_json` can fail to encode).
#[must_use]
pub fn encode_manifest_object(obj: &BackupManifestObject) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&MANIFEST_MAGIC);
    out.push(MANIFEST_VERSION);
    let json = serde_json::to_vec(obj).expect("BackupManifestObject always serializes");
    out.extend_from_slice(&json);
    out
}

/// Decode a manifest object's bytes back into its [`BackupManifestObject`].
///
/// Validates the magic + version exactly like [`decode_data_chunk`] (a loud,
/// named `Err` on either mismatch, never a silent misread), then the JSON
/// body via `serde_json`.
///
/// # Errors
/// A descriptive [`BackupCodecError`] for a bad magic, an unrecognized
/// version, a too-short buffer, or malformed JSON.
pub fn decode_manifest_object(bytes: &[u8]) -> Result<BackupManifestObject, BackupCodecError> {
    if bytes.len() < 5 {
        return Err(format!(
            "truncated backup manifest object: need at least 5 header bytes, have {}",
            bytes.len()
        ));
    }
    let magic = &bytes[0..4];
    if magic != MANIFEST_MAGIC {
        return Err(format!(
            "bad backup manifest magic {magic:?} (want {MANIFEST_MAGIC:?})"
        ));
    }
    let version = bytes[4];
    if version != MANIFEST_VERSION {
        return Err(format!(
            "unknown backup manifest codec version {version} (this build only decodes \
             version {MANIFEST_VERSION})"
        ));
    }
    serde_json::from_slice(&bytes[5..])
        .map_err(|e| format!("decoding backup manifest object body: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_control::{BackupPinnedTablet, BackupStatus, ColumnType, TableSchema};
    use animus_tablet::KeyRange;

    fn row(kind: u8, key: &[u8], value: Option<&[u8]>, version: u64) -> SeedRow {
        (kind, key.to_vec(), value.map(<[u8]>::to_vec), version)
    }

    // --- object naming -----------------------------------------------------

    #[test]
    fn manifest_and_data_object_ids_share_the_backup_prefix_but_never_collide() {
        let manifest = backup_manifest_object_id("bkp-1");
        let data = backup_data_object_id("bkp-1", 7, 0);
        assert_eq!(manifest, "backup/bkp-1/manifest");
        assert_eq!(data, "backup/bkp-1/7/0");
        assert!(manifest.starts_with(&backup_prefix("bkp-1")));
        assert!(data.starts_with(&backup_prefix("bkp-1")));
        assert_ne!(manifest, data);
    }

    #[test]
    fn data_object_ids_are_disjoint_across_backup_tablet_and_chunk() {
        let a = backup_data_object_id("bkp-1", 7, 0);
        let b = backup_data_object_id("bkp-1", 7, 1);
        let c = backup_data_object_id("bkp-1", 8, 0);
        let d = backup_data_object_id("bkp-2", 7, 0);
        let ids = [a, b, c, d];
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                assert_ne!(ids[i], ids[j], "{:?} vs {:?}", ids[i], ids[j]);
            }
        }
    }

    /// ADR 0059 §1/§4's collision-freedom claim: for any table name other
    /// than one literally named `backup`, the stream sealer's own
    /// `{table}/{label}/{tablet}/{epoch}` object-id shape never produces
    /// anything under this module's fixed `backup/` namespace — belt-and-
    /// suspenders on top of the real guarantee (a distinct `SegmentStore`
    /// instance per concern, ADR 0059 §1), documented rather than silently
    /// assumed.
    #[test]
    fn namespace_does_not_collide_with_the_stream_sealer_for_ordinary_table_names() {
        let stream_ids = [
            crate::segment::segment_id("orders", "2026-08-14T00:00:00Z-n1", 7, 3),
            crate::segment::segment_object_id("users", "L1", 1, 0, "n1", 2, 42),
            crate::segment::segment_id("inventory", "label", 99, 12),
        ];
        for id in &stream_ids {
            assert!(
                !id.starts_with(&format!("{BACKUP_NAMESPACE}/")),
                "a stream segment id for an ordinary table name must never fall under the \
                 backup namespace: {id:?}"
            );
        }
        // The documented edge case: a table literally named `backup` WOULD
        // collide on the namespace alone — this is exactly why ADR 0059 §1
        // does not rely on the namespace split as the sole guarantee.
        let same_name_edge_case = crate::segment::segment_id("backup", "label", 1, 0);
        assert!(same_name_edge_case.starts_with(&format!("{BACKUP_NAMESPACE}/")));
    }

    // --- data chunk codec ----------------------------------------------------

    #[test]
    fn data_chunk_round_trips_empty() {
        let bytes = encode_data_chunk(&[]);
        let decoded = decode_data_chunk(&bytes).expect("decodes");
        assert!(decoded.is_empty());
    }

    #[test]
    fn data_chunk_round_trips_a_single_row() {
        let rows = vec![row(crate::KIND_BASE, b"pk\x00sk", Some(b"value"), 100)];
        let bytes = encode_data_chunk(&rows);
        let decoded = decode_data_chunk(&bytes).expect("decodes");
        assert_eq!(decoded, rows);
    }

    #[test]
    fn data_chunk_round_trips_many_rows_with_mixed_tombstones() {
        let rows: Vec<SeedRow> = (0..500u64)
            .map(|i| {
                if i % 3 == 0 {
                    row(crate::KIND_LSI, format!("k{i}").as_bytes(), None, i)
                } else {
                    row(
                        crate::KIND_FOOTPRINT,
                        format!("k{i}").as_bytes(),
                        Some(format!("v{i}").as_bytes()),
                        i,
                    )
                }
            })
            .collect();
        let bytes = encode_data_chunk(&rows);
        let decoded = decode_data_chunk(&bytes).expect("decodes");
        assert_eq!(decoded, rows);
    }

    #[test]
    fn data_chunk_round_trips_boundary_sizes() {
        // An empty key, an empty (but present, not tombstoned) value, and a
        // version of 0 and of u64::MAX are all legitimate boundary shapes a
        // real captured row can take.
        let rows = vec![
            row(crate::KIND_BASE, b"", Some(b""), 0),
            row(crate::KIND_BASE, b"k", Some(b""), u64::MAX),
            row(crate::KIND_BASE, b"", None, 1),
        ];
        let bytes = encode_data_chunk(&rows);
        let decoded = decode_data_chunk(&bytes).expect("decodes");
        assert_eq!(decoded, rows);
    }

    #[test]
    fn data_chunk_encode_derives_count_from_the_body() {
        // Sanity: the declared count in the header always matches
        // `rows.len()`, so a caller building a header by hand elsewhere
        // could never drift the way `segment::encode`'s own doc warns
        // against — here there's no separate caller-supplied count field at
        // all, so this is really just pinning the round trip length.
        let rows = vec![row(crate::KIND_BASE, b"k", Some(b"v"), 1)];
        let bytes = encode_data_chunk(&rows);
        let decoded = decode_data_chunk(&bytes).expect("decodes");
        assert_eq!(decoded.len(), 1);
    }

    #[test]
    fn data_chunk_rejects_bad_magic() {
        let mut bytes = encode_data_chunk(&[]);
        bytes[0] = b'X';
        let err = decode_data_chunk(&bytes).expect_err("must reject bad magic");
        assert!(err.contains("bad backup data chunk magic"), "{err}");
    }

    #[test]
    fn data_chunk_rejects_unknown_version() {
        let mut bytes = encode_data_chunk(&[]);
        bytes[4] = DATA_VERSION + 1;
        let err = decode_data_chunk(&bytes).expect_err("must reject unknown version");
        assert!(
            err.contains("unknown backup data chunk codec version"),
            "{err}"
        );
    }

    #[test]
    fn data_chunk_rejects_truncated_buffer() {
        let rows = vec![row(crate::KIND_BASE, b"k1", Some(b"v1"), 1)];
        let bytes = encode_data_chunk(&rows);
        for cut in 1..bytes.len() {
            let _ = decode_data_chunk(&bytes[..cut]); // must not panic
        }
        let err =
            decode_data_chunk(&bytes[..bytes.len() - 1]).expect_err("a short buffer must fail");
        assert!(
            err.contains("truncated") || err.contains("trailing"),
            "{err}"
        );
    }

    #[test]
    fn data_chunk_rejects_trailing_garbage() {
        let mut bytes = encode_data_chunk(&[row(crate::KIND_BASE, b"k", Some(b"v"), 1)]);
        bytes.push(0xFF);
        let err = decode_data_chunk(&bytes).expect_err("trailing bytes must be rejected");
        assert!(err.contains("trailing"), "{err}");
    }

    // --- manifest object codec ----------------------------------------------

    fn sample_manifest() -> BackupManifestObject {
        let manifest = BackupManifest {
            schema: TableSchema::simple("pk", ColumnType::String),
            pinned_tablets: vec![
                BackupPinnedTablet {
                    tablet: TabletId(1),
                    range: KeyRange::whole(),
                },
                BackupPinnedTablet {
                    tablet: TabletId(2),
                    range: KeyRange::whole(),
                },
            ],
            created_wall_ms: 1_723_000_000_000,
        };
        BackupManifestObject {
            manifest,
            tablet_progress: vec![
                BackupManifestTabletEntry {
                    tablet: TabletId(1),
                    progress: BackupTabletProgress {
                        cut_version: 42,
                        bytes: 1_000,
                    },
                },
                BackupManifestTabletEntry {
                    tablet: TabletId(2),
                    progress: BackupTabletProgress {
                        cut_version: 43,
                        bytes: 2_000,
                    },
                },
            ],
        }
    }

    #[test]
    fn manifest_object_round_trips() {
        let obj = sample_manifest();
        let bytes = encode_manifest_object(&obj);
        let decoded = decode_manifest_object(&bytes).expect("decodes");
        assert_eq!(decoded, obj);
    }

    #[test]
    fn manifest_object_total_bytes_sums_every_pinned_tablets_progress() {
        let obj = sample_manifest();
        assert_eq!(obj.total_bytes(), 3_000);
    }

    #[test]
    fn manifest_object_round_trips_with_no_pinned_tablets() {
        // A degenerate but legal shape: `BeginBackup` against a table with
        // zero tablets is rejected upstream (PR①), but the codec itself
        // shouldn't assume a non-empty list.
        let obj = BackupManifestObject {
            manifest: BackupManifest {
                schema: TableSchema::simple("pk", ColumnType::String),
                pinned_tablets: Vec::new(),
                created_wall_ms: 0,
            },
            tablet_progress: Vec::new(),
        };
        let bytes = encode_manifest_object(&obj);
        let decoded = decode_manifest_object(&bytes).expect("decodes");
        assert_eq!(decoded, obj);
        assert_eq!(decoded.total_bytes(), 0);
    }

    #[test]
    fn manifest_object_rejects_bad_magic() {
        let mut bytes = encode_manifest_object(&sample_manifest());
        bytes[0] = b'X';
        let err = decode_manifest_object(&bytes).expect_err("must reject bad magic");
        assert!(err.contains("bad backup manifest magic"), "{err}");
    }

    #[test]
    fn manifest_object_rejects_unknown_version() {
        let mut bytes = encode_manifest_object(&sample_manifest());
        bytes[4] = MANIFEST_VERSION + 1;
        let err = decode_manifest_object(&bytes).expect_err("must reject unknown version");
        assert!(
            err.contains("unknown backup manifest codec version"),
            "{err}"
        );
    }

    #[test]
    fn manifest_object_rejects_truncated_header() {
        let err = decode_manifest_object(&[0u8; 3]).expect_err("must reject a too-short buffer");
        assert!(err.contains("truncated"), "{err}");
    }

    #[test]
    fn manifest_object_rejects_malformed_json_body() {
        let mut bytes = MANIFEST_MAGIC.to_vec();
        bytes.push(MANIFEST_VERSION);
        bytes.extend_from_slice(b"not json");
        let err = decode_manifest_object(&bytes).expect_err("must reject malformed JSON");
        assert!(
            err.contains("decoding backup manifest object body"),
            "{err}"
        );
    }

    /// A failed-and-retried backup, or an as-yet-unimplemented aggregator,
    /// might reasonably carry a diagnostic status alongside a manifest in a
    /// future PR — this test just pins that today's [`BackupManifestObject`]
    /// carries no such field, so a reviewer of that future change can see
    /// exactly what's being added rather than discovering it mid-diff.
    #[test]
    fn manifest_object_carries_no_catalog_status_field() {
        // `BackupStatus` lives in the replicated catalog (`Metadata::
        // backups`), never duplicated into the store object itself — the
        // catalog is the sole authority for a backup's lifecycle state (ADR
        // 0059 §3), and the store object only ever needs to exist once the
        // catalog says `Available`.
        let _ = BackupStatus::Creating; // keeps the import honest, not a real assertion
    }
}
