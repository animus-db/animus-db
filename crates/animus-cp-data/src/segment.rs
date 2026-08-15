//! The stream-shard **segment codec** (ADR 0042/0043): the versioned binary
//! format a sealed shard's `SegmentStore` object is encoded in, plus the
//! **superset-slice rule**'s own implementation (ADR 0042 §10, ADR 0043 §A3).
//!
//! A segment is a self-describing header (which shard it is, its lineage, its
//! committed HLC range and record count) followed by a body of
//! length-prefixed `(source_key, packed_hlc, change_record)` triples, in
//! ascending `packed_hlc` order (ADR 0043 §A3 step 1: `pending_changes`' own
//! key order is token-then-pk-then-HLC, not commit order, so the seal step
//! re-sorts by the HLC suffix before encoding — this module trusts that
//! order rather than re-deriving it, since re-sorting on every decode would
//! be wasted work for a reader that already trusts a sealed object's own
//! construction).
//!
//! `change_record` is **opaque to this crate** (ADR 0043's own layering
//! rule): it is `animus-dynamo`/`animusd`'s `ChangeRecord`, already encoded
//! by the caller before this module ever sees it. This module only ever
//! moves its bytes.
//!
//! ## Object identity: ledger-named, write-once (as-built amendment)
//!
//! Every seal **attempt** writes its encoded bytes at a unique
//! [`segment_object_id`], not the shard's own deterministic [`segment_id`]
//! directly — the id rides as a new field on the catalog row
//! (`animus_control::StreamShardRow::object_id`) and every reader/sweep
//! resolves it from there rather than recomputing `segment_id` and assuming
//! that is where the winning bytes live. This closes a data-loss bug the
//! original (pre-amendment) deterministic-shared-id design had: two
//! independently-computed seal attempts for the tablet's **same** open
//! epoch (the realistic trigger is a brief dual-leadership window during a
//! write-burst-induced re-election) both derived the identical
//! `segment_id(table, label, tablet, epoch)` and raced to physically `put`
//! there — the catalog's own `SealStreamShard` apply arm correctly picked a
//! single winner (first-committer-wins on content), but the **store** had
//! no such adjudication, so whichever attempt's `put` landed chronologically
//! *last* won the physical bytes, independent of which attempt's *proposal*
//! won the catalog. When the loser's `put` landed after the winner's and
//! carried a *smaller* range (the realistic shape: a deposed attempt's own
//! snapshot was taken earlier), the object on disk ended up covering less
//! than the catalog's own committed `hlc_range` claimed — a silent,
//! permanent loss of exactly the gap. See `docs/engineering-lessons.md` and
//! ADR 0042 §10/ADR 0043 §A3's as-built amendment for the full incident.
//! With a unique id per attempt, two racing attempts' physical writes can
//! never collide in the first place — no adjudication needed at the store
//! layer at all. The store itself is now **write-once** per id (identical
//! bytes rewritten to the same id is a safe no-op; *different* bytes at an
//! already-written id is a hard error — see [`animus_env::SegmentStore`]'s
//! own doc), and a losing attempt's own object becomes a permanent orphan,
//! reaped by the segment janitor's own sweep (`animusd::segment_janitor`)
//! rather than ever being physically overwritten.
//!
//! ## The superset-slice rule (historical; retained as defense-in-depth)
//!
//! Before the amendment above, a deposed *same-attempt* leader's late `put`
//! could overwrite a segment object with a **superset** of the content the
//! catalog row actually committed (ADR 0042 §10) — the seal id was
//! deterministic, so a retried put from a stale leader landed at the same
//! object key, potentially carrying a few extra tail records the winning
//! leader's own put didn't (both leaders scanned the same watermark-to-now
//! range at slightly different "now"s). With ledger-named write-once ids
//! this specific race can no longer happen (each attempt's own object, at
//! its own id, is never overwritten by a *different* attempt's bytes) — but
//! **a reader still slices a fetched segment's content to the catalog row's
//! own committed `hlc_range` rather than trusting the object's raw extent**,
//! kept as cheap, harmless defense-in-depth against a future bug in this
//! class rather than removed outright. [`decode_and_slice`] (and its
//! lower-level sibling [`slice_to_hlc_range`]) is that discipline made
//! structural: a caller that decodes a segment and slices it can never
//! forget to, and can never slice against the wrong bound by hand.
//!
//! `hlc_range` is `(start_exclusive, end_inclusive)`: `start_exclusive` is
//! the watermark the seal scanned forward from (a record already visible to
//! an earlier shard is never repeated), `end_inclusive` is the committed
//! chain's own last included record's `packed_hlc` (`EndingSequenceNumber`).
//! Slicing keeps exactly the records with `start_exclusive < packed_hlc <=
//! end_inclusive` — both bounds, not just the upper one, so a hypothetical
//! superset on *either* end (never expected today, since every seal scans
//! forward from the same watermark, but not a documented invariant this
//! module should lean on) is still handled correctly.
//!
//! ## Versioning
//!
//! Every encoded segment starts with a 4-byte magic + a version byte. An
//! unrecognized version is a loud, immediate `Err` (never a silent
//! misinterpretation of a future format) — this codec is pre-alpha, so no
//! cross-version compatibility is attempted, mirroring `codec.rs`'s own
//! documented stance for the Raft wire/snapshot format.

/// First four bytes of every encoded segment — rejects a foreign payload
/// (e.g. a stray `serde_json` blob, or a different codec's bytes landing at
/// the same `SegmentStore` id by caller error) with a clear error instead of
/// a confusing tag mismatch deeper in.
const MAGIC: [u8; 4] = *b"SEGF";

/// Codec version, bumped on any incompatible layout change.
pub const VERSION: u8 = 1;

/// Decode/encode failures are plain descriptive strings, mirroring
/// `codec.rs`'s own `DecodeError` shape — this module has no error-recovery
/// logic that would benefit from a typed enum, and every caller's own
/// handling is "log loudly, treat as absent/corrupt."
pub type SegmentError = String;

/// A sealed shard's self-describing header (ADR 0043 §A3's "Segment format").
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentHeader {
    /// The base table this shard belongs to.
    pub table: String,
    /// The stream label active when this shard sealed (ADR 0042 §4).
    pub label: String,
    /// This shard's own id — `shardId-<tablet>-<epoch>` ([`shard_id`]).
    /// Carried explicitly (redundant with `tablet`/`epoch`, deliberately —
    /// ADR 0043 §A3 documents it as its own header field, and storing it
    /// lets a reader/debug tool display a shard's identity without
    /// recomputing it) and cross-checked against `tablet`/`epoch` on decode
    /// (a mismatch is corruption, not a legitimate case).
    pub shard_id: String,
    /// The source tablet this shard's change log belongs to.
    pub tablet: u64,
    /// The seal epoch (ADR 0042 §2): this shard's position in its tablet's
    /// own chain of closed shards.
    pub epoch: u64,
    /// This shard's own `ParentShardId` (ADR 0042 §2/ADR 0043 §A4), if any —
    /// absent only for a tablet's genuine root (an epoch-0 shard whose
    /// tablet has no split parent).
    pub parent_shard_id: Option<String>,
    /// `(start_exclusive, end_inclusive)` packed-HLC range this shard's
    /// **catalog row** committed — see the module doc's superset-slice
    /// section. Every record in a well-formed segment's body falls inside
    /// this range; [`slice_to_hlc_range`] is what enforces that a
    /// possibly-superset *object* never serves outside it.
    pub hlc_range: (u64, u64),
    /// The number of records the sealing leader's own scan counted — the
    /// catalog row's own `count` field, carried here too so a segment
    /// object is self-describing without a separate catalog lookup. Encode
    /// derives this from the actual body (`records.len()`), so it can never
    /// drift from the bytes that follow it; decode's own count is likewise
    /// exactly `records.len()` by construction (see `decode`'s doc).
    pub count: u64,
    /// The sealing leader's own wall-clock time (`env.now()`, never the raw
    /// OS clock — ADR 0003), for observability only.
    pub seal_wall_ms: u64,
}

/// One change-log record as stored in a segment's body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentRecord {
    /// The record's own logical key in the source tablet's `KIND_CHANGE`
    /// scope (token-leading, HLC-suffixed — see `RaftKvNode::pending_changes`'
    /// own doc in `lib.rs`).
    pub source_key: Vec<u8>,
    /// This record's own packed HLC (`hlc::pack`) — the DynamoDB Streams
    /// `SequenceNumber` (ADR 0042 §5).
    pub packed_hlc: u64,
    /// The opaque, already-encoded change record (`animus-dynamo`/
    /// `animusd`'s `ChangeRecord` bytes). Never interpreted by this crate.
    pub change_record: Vec<u8>,
}

/// A fully decoded segment: header + body, in the same shape [`encode`]
/// takes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    /// The segment's own header.
    pub header: SegmentHeader,
    /// The segment's records, in ascending `packed_hlc` order.
    pub records: Vec<SegmentRecord>,
}

/// This shard's own `ShardId` (ADR 0042 §2): `shardId-<tablet>-<epoch>`.
#[must_use]
pub fn shard_id(tablet: u64, epoch: u64) -> String {
    format!("shardId-{tablet}-{epoch}")
}

/// This shard's deterministic **prefix** (ADR 0043 §A3/§A7):
/// `{table}/{label}/{tablet}/{epoch}` — matches `FsSegmentStore`'s own
/// `/`-separated-subdirectory path mapping and `ClusterSegmentStore`'s
/// documented id shape byte-for-byte. **This is no longer, by itself, the id
/// any segment object is actually stored at** (the ledger-named-object
/// amendment above) — every real attempt's object lives at
/// [`segment_object_id`], which extends this prefix with an attempt-unique
/// suffix. Still useful on its own for a debug/list sweep (`SegmentStore::
/// list(segment_id(..))` finds every attempt — winning or orphaned — ever
/// written for this exact shard) and for a human grepping storage directly;
/// no production reader should build a full object id from this function
/// alone anymore — resolve `StreamShardRow::object_id` from the catalog
/// instead.
#[must_use]
pub fn segment_id(table: &str, label: &str, tablet: u64, epoch: u64) -> String {
    format!("{table}/{label}/{tablet}/{epoch}")
}

/// The unique **per-attempt** `SegmentStore` id one seal attempt's encoded
/// bytes are actually written at (the ledger-named-object amendment, see the
/// module doc): [`segment_id`]'s own deterministic prefix — kept for
/// greppability, and so every attempt for one shard still lists/sorts
/// together — plus a `/`-separated suffix unique to *this* attempt, never
/// reused even by a second attempt for the exact same `(tablet, epoch)`.
///
/// The suffix is `{proposer}-{term:x}-{nonce:016x}`, entirely Env-seamed
/// (deterministic under `SimEnv`, no wall clock/`OsRng`):
/// - `proposer` (the attempting leader's own [`NodeId`](animus_env::NodeId))
///   disambiguates across nodes — cluster-wide unique by construction (ADR
///   0040: one identity per node, enforced by the `RegisterNode`
///   registration CAS), and contains no `/` (every sanctioned `NodeId`
///   charset excludes it), so it never fractures the path shape.
/// - `term` (the proposer's own current Raft term for this tablet's group at
///   attempt time) disambiguates the SAME node's own re-elections/restarts —
///   a node that crashes and comes back up leading the same group again
///   does so at a strictly higher term (Raft's own guarantee), so even a
///   restart whose RNG stream happens to replay identically (a `SimEnv` node
///   reconstructed against the same seed) can never repeat a prior attempt's
///   suffix.
/// - `nonce` (a single fresh draw off the proposer's own `Rng`, ADR 0003) is
///   the within-term disambiguator — two attempts by the same node in the
///   same term (e.g. `ForceSeal` racing the ordinary seal tick) draw two
///   different values, since the RNG stream advances deterministically on
///   every draw and never repeats within one run.
///
/// No two attempts, anywhere, ever produce the same id — the write-once
/// store's whole safety argument rests on this.
#[must_use]
pub fn segment_object_id(
    table: &str,
    label: &str,
    tablet: u64,
    epoch: u64,
    proposer: &str,
    term: u64,
    nonce: u64,
) -> String {
    format!(
        "{}/{proposer}-{term:x}-{nonce:016x}",
        segment_id(table, label, tablet, epoch)
    )
}

// --- encode -----------------------------------------------------------

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Encode `header` + `records` into a segment object's bytes.
///
/// `header.count` is **not trusted from the caller** — encode always writes
/// `records.len() as u64` regardless of what `header.count` holds, so an
/// encoded segment's declared count can never drift from its actual body
/// (the caller-supplied field exists only so a decoded [`SegmentHeader`] can
/// be round-tripped back through `encode` unchanged; a fresh header built
/// for encoding may leave it at any placeholder value).
#[must_use]
pub fn encode(header: &SegmentHeader, records: &[SegmentRecord]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    put_bytes(&mut out, header.table.as_bytes());
    put_bytes(&mut out, header.label.as_bytes());
    put_bytes(&mut out, header.shard_id.as_bytes());
    out.extend_from_slice(&header.tablet.to_be_bytes());
    out.extend_from_slice(&header.epoch.to_be_bytes());
    match &header.parent_shard_id {
        Some(p) => {
            out.push(1);
            put_bytes(&mut out, p.as_bytes());
        }
        None => out.push(0),
    }
    out.extend_from_slice(&header.hlc_range.0.to_be_bytes());
    out.extend_from_slice(&header.hlc_range.1.to_be_bytes());
    out.extend_from_slice(&(records.len() as u64).to_be_bytes());
    out.extend_from_slice(&header.seal_wall_ms.to_be_bytes());
    for r in records {
        put_bytes(&mut out, &r.source_key);
        out.extend_from_slice(&r.packed_hlc.to_be_bytes());
        put_bytes(&mut out, &r.change_record);
    }
    out
}

// --- decode -------------------------------------------------------------

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], SegmentError> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.bytes.len())
            .ok_or_else(|| {
                format!(
                    "truncated segment: need {n} more bytes at offset {}, have {}",
                    self.pos,
                    self.bytes.len()
                )
            })?;
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, SegmentError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, SegmentError> {
        let b: [u8; 4] = self.take(4)?.try_into().expect("took exactly 4 bytes");
        Ok(u32::from_be_bytes(b))
    }

    fn u64(&mut self) -> Result<u64, SegmentError> {
        let b: [u8; 8] = self.take(8)?.try_into().expect("took exactly 8 bytes");
        Ok(u64::from_be_bytes(b))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, SegmentError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn string(&mut self) -> Result<String, SegmentError> {
        String::from_utf8(self.bytes()?).map_err(|e| format!("non-UTF8 string field: {e}"))
    }
}

/// Decode a segment's bytes back into its [`Segment`].
///
/// Validates: the magic + version (an unrecognized version is a loud, named
/// `Err`, never a silent misread); every length-prefixed field's framing
/// (a truncated buffer anywhere is a named `Err`, not a panic); that the
/// stored `shard_id` matches `shard_id(tablet, epoch)` (a mismatch is
/// corruption — this crate is the only writer, and it always derives the
/// two consistently); and that the body holds **exactly** the declared
/// record count with no trailing bytes left over (a short body is a
/// truncation `Err`; leftover bytes after the declared count is a "trailing
/// data" `Err` — either shape is corrupt framing, not a legitimate variant).
///
/// # Errors
/// A descriptive [`SegmentError`] for any of the above.
pub fn decode(bytes: &[u8]) -> Result<Segment, SegmentError> {
    let mut c = Cursor { bytes, pos: 0 };
    let magic = c.take(4)?;
    if magic != MAGIC {
        return Err(format!("bad segment magic {magic:?} (want {MAGIC:?})"));
    }
    let version = c.u8()?;
    if version != VERSION {
        return Err(format!(
            "unknown segment codec version {version} (this build only decodes version {VERSION})"
        ));
    }
    let table = c.string()?;
    let label = c.string()?;
    let shard_id_field = c.string()?;
    let tablet = c.u64()?;
    let epoch = c.u64()?;
    let expected_shard_id = shard_id(tablet, epoch);
    if shard_id_field != expected_shard_id {
        return Err(format!(
            "segment shard_id {shard_id_field:?} does not match tablet/epoch \
             (expected {expected_shard_id:?}) — corrupt header"
        ));
    }
    let has_parent = c.u8()?;
    let parent_shard_id = match has_parent {
        0 => None,
        1 => Some(c.string()?),
        other => {
            return Err(format!(
                "bad parent-shard-id presence flag {other} (want 0/1)"
            ));
        }
    };
    let hlc_start = c.u64()?;
    let hlc_end = c.u64()?;
    let declared_count = c.u64()?;
    let seal_wall_ms = c.u64()?;

    let mut records = Vec::with_capacity(declared_count.min(1 << 20) as usize);
    for i in 0..declared_count {
        let source_key = c.bytes().map_err(|e| format!("record {i}: {e}"))?;
        let packed_hlc = c.u64().map_err(|e| format!("record {i}: {e}"))?;
        let change_record = c.bytes().map_err(|e| format!("record {i}: {e}"))?;
        records.push(SegmentRecord {
            source_key,
            packed_hlc,
            change_record,
        });
    }
    if c.pos != c.bytes.len() {
        return Err(format!(
            "trailing {} byte(s) after the declared {declared_count} record(s) — corrupt framing",
            c.bytes.len() - c.pos
        ));
    }

    Ok(Segment {
        header: SegmentHeader {
            table,
            label,
            shard_id: shard_id_field,
            tablet,
            epoch,
            parent_shard_id,
            hlc_range: (hlc_start, hlc_end),
            count: declared_count,
            seal_wall_ms,
        },
        records,
    })
}

// --- the superset-slice rule --------------------------------------------

/// The superset-slice rule (ADR 0042 §10), applied to already-decoded
/// records: keep exactly the records with `start_exclusive < packed_hlc <=
/// end_inclusive`, in their original (ascending) order. A well-formed,
/// non-superset segment is unchanged by this (every record already falls
/// inside its own committed range); a superset object's extra tail records
/// are dropped.
#[must_use]
pub fn slice_to_hlc_range(
    records: &[SegmentRecord],
    committed_range: (u64, u64),
) -> Vec<SegmentRecord> {
    let (start_exclusive, end_inclusive) = committed_range;
    records
        .iter()
        .filter(|r| r.packed_hlc > start_exclusive && r.packed_hlc <= end_inclusive)
        .cloned()
        .collect()
}

/// Decode `bytes` and immediately slice to `committed_range` — the one call
/// a reader (PR6's `GetRecords` sealed-shard path) should make, so "decode a
/// segment object" and "trust its raw tail" can never be pulled apart by a
/// caller that forgets the second step. Returns the sliced records; the
/// header is available too, for a caller that wants the shard's own
/// metadata (e.g. to confirm identity before serving).
///
/// # Errors
/// Whatever [`decode`] returns for malformed bytes.
pub fn decode_and_slice(
    bytes: &[u8],
    committed_range: (u64, u64),
) -> Result<(SegmentHeader, Vec<SegmentRecord>), SegmentError> {
    let segment = decode(bytes)?;
    let records = slice_to_hlc_range(&segment.records, committed_range);
    Ok((segment.header, records))
}

/// This shard's `SequenceNumberRange`-shaped bounds, wall-clock-independent
/// (ADR 0042 §5): `TRIM_HORIZON` resolves to `hlc_range.0`, `LATEST`/
/// `EndingSequenceNumber` to `hlc_range.1` — a small convenience so a caller
/// building the DynamoDB Streams wire response doesn't have to know the
/// tuple's own field order.
#[must_use]
pub fn ending_sequence_number(header: &SegmentHeader) -> u64 {
    header.hlc_range.1
}

/// A convenience constructor for a fresh header a sealer is about to encode
/// (never a decoded one — `count` is a placeholder here, ignored by
/// [`encode`]). Kept out of a `Default` impl since every field but `count`
/// is genuinely required; this just spares a caller from writing `count: 0`
/// by hand at every call site.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn new_header(
    table: String,
    label: String,
    tablet: u64,
    epoch: u64,
    parent_shard_id: Option<String>,
    hlc_range: (u64, u64),
    seal_wall_ms: u64,
) -> SegmentHeader {
    SegmentHeader {
        table,
        label,
        shard_id: shard_id(tablet, epoch),
        tablet,
        epoch,
        parent_shard_id,
        hlc_range,
        count: 0,
        seal_wall_ms,
    }
}

/// Pack an [`crate::hlc::HlcTimestamp`] the same way `hlc::pack` does —
/// re-exported at this level only so this module's own tests can build
/// records without a second import; production callers already have
/// `hlc::pack` in scope.
#[cfg(test)]
fn pack(ts: crate::hlc::HlcTimestamp) -> u64 {
    crate::hlc::pack(ts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::HlcTimestamp;

    fn rec(key: &[u8], hlc: u64, payload: &[u8]) -> SegmentRecord {
        SegmentRecord {
            source_key: key.to_vec(),
            packed_hlc: hlc,
            change_record: payload.to_vec(),
        }
    }

    fn header(tablet: u64, epoch: u64, hlc_range: (u64, u64)) -> SegmentHeader {
        new_header(
            "orders".to_string(),
            "2026-08-14T00:00:00Z-n1".to_string(),
            tablet,
            epoch,
            (epoch > 0).then(|| shard_id(tablet, epoch - 1)),
            hlc_range,
            1_723_000_000_000,
        )
    }

    #[test]
    fn round_trips_a_populated_segment() {
        let records = vec![
            rec(b"k1", 100, b"change-1"),
            rec(b"k2", 200, b"change-2"),
            rec(b"k3", 300, b"change-3"),
        ];
        let h = header(7, 3, (50, 300));
        let bytes = encode(&h, &records);
        let decoded = decode(&bytes).expect("decodes");
        assert_eq!(decoded.header.table, "orders");
        assert_eq!(decoded.header.label, h.label);
        assert_eq!(decoded.header.shard_id, shard_id(7, 3));
        assert_eq!(decoded.header.tablet, 7);
        assert_eq!(decoded.header.epoch, 3);
        assert_eq!(decoded.header.parent_shard_id, Some(shard_id(7, 2)));
        assert_eq!(decoded.header.hlc_range, (50, 300));
        assert_eq!(decoded.header.count, 3);
        assert_eq!(decoded.header.seal_wall_ms, h.seal_wall_ms);
        assert_eq!(decoded.records, records);
    }

    #[test]
    fn round_trips_an_empty_body() {
        let h = header(1, 0, (0, 0));
        let bytes = encode(&h, &[]);
        let decoded = decode(&bytes).expect("decodes");
        assert_eq!(decoded.header.count, 0);
        assert_eq!(decoded.header.parent_shard_id, None);
        assert!(decoded.records.is_empty());
    }

    #[test]
    fn encode_derives_count_from_the_body_not_the_caller() {
        // A caller-supplied placeholder `count` (0, the `new_header`
        // default) must never leak into the encoded bytes.
        let h = header(1, 0, (0, 100));
        assert_eq!(h.count, 0);
        let bytes = encode(&h, &[rec(b"k", 10, b"v")]);
        let decoded = decode(&bytes).expect("decodes");
        assert_eq!(decoded.header.count, 1);
    }

    #[test]
    fn unknown_version_is_a_loud_named_error() {
        let h = header(1, 0, (0, 0));
        let mut bytes = encode(&h, &[]);
        bytes[4] = VERSION + 1; // the byte right after the 4-byte magic
        let err = decode(&bytes).expect_err("must reject an unknown version");
        assert!(err.contains("unknown segment codec version"), "{err}");
    }

    #[test]
    fn bad_magic_is_rejected() {
        let h = header(1, 0, (0, 0));
        let mut bytes = encode(&h, &[]);
        bytes[0] = b'X';
        let err = decode(&bytes).expect_err("must reject bad magic");
        assert!(err.contains("bad segment magic"), "{err}");
    }

    #[test]
    fn truncated_buffer_is_rejected_not_panicked() {
        let h = header(1, 0, (0, 300));
        let bytes = encode(&h, &[rec(b"k1", 100, b"change-1")]);
        for cut in 1..bytes.len() {
            // Every prefix short of the full encoding must fail cleanly.
            let truncated = &bytes[..cut];
            let _ = decode(truncated); // must not panic
        }
        let err = decode(&bytes[..bytes.len() - 1]).expect_err("a short buffer must not decode");
        assert!(
            err.contains("truncated") || err.contains("trailing"),
            "{err}"
        );
    }

    #[test]
    fn trailing_garbage_after_the_declared_count_is_rejected() {
        let h = header(1, 0, (0, 100));
        let mut bytes = encode(&h, &[rec(b"k", 100, b"v")]);
        bytes.push(0xFF);
        let err = decode(&bytes).expect_err("trailing bytes must be rejected");
        assert!(err.contains("trailing"), "{err}");
    }

    #[test]
    fn a_declared_count_higher_than_the_body_holds_is_a_truncation_error() {
        let h = header(1, 0, (0, 100));
        let bytes = encode(&h, &[rec(b"k", 100, b"v")]);
        // The header fields preceding the records are identical regardless
        // of record count, so encoding the same header with zero records
        // gives exactly the byte offset of the `count`/`seal_wall_ms` pair
        // (the last 16 bytes of that zero-record encoding) without hard-
        // coding the variable-length string field widths by hand.
        let zero_record_prefix_len = encode(&h, &[]).len();
        let count_offset = zero_record_prefix_len - 16;
        let mut count_bytes = [0u8; 8];
        count_bytes.copy_from_slice(&bytes[count_offset..count_offset + 8]);
        let count = u64::from_be_bytes(count_bytes);
        assert_eq!(count, 1);
        let mut bytes = bytes;
        bytes[count_offset..count_offset + 8].copy_from_slice(&2u64.to_be_bytes());
        let err = decode(&bytes).expect_err("an over-declared count must fail, not panic");
        assert!(
            err.contains("truncated") || err.contains("record 1"),
            "{err}"
        );
    }

    #[test]
    fn mismatched_shard_id_is_rejected() {
        let mut h = header(1, 0, (0, 0));
        h.shard_id = "shardId-1-99".to_string(); // doesn't match tablet=1/epoch=0
        let bytes = encode(&h, &[]);
        let err = decode(&bytes).expect_err("a mismatched shard_id must be rejected");
        assert!(err.contains("does not match tablet/epoch"), "{err}");
    }

    #[test]
    fn slice_drops_a_supersets_tail_and_keeps_order() {
        // The committed catalog row says the shard ends at hlc=200 (three
        // records), but a deposed leader's late `put` landed a superset
        // object carrying two extra records past that point.
        let superset = vec![
            rec(b"k1", 100, b"c1"),
            rec(b"k2", 150, b"c2"),
            rec(b"k3", 200, b"c3"),
            rec(b"k4", 250, b"c4-should-be-dropped"),
            rec(b"k5", 300, b"c5-should-be-dropped"),
        ];
        let sliced = slice_to_hlc_range(&superset, (0, 200));
        assert_eq!(sliced, superset[..3]);
    }

    #[test]
    fn slice_also_drops_anything_at_or_below_the_exclusive_start() {
        let records = vec![rec(b"k1", 50, b"c1"), rec(b"k2", 100, b"c2")];
        // start_exclusive == 50: the boundary record itself must NOT be
        // included (already visible to an earlier shard).
        let sliced = slice_to_hlc_range(&records, (50, 200));
        assert_eq!(sliced, vec![rec(b"k2", 100, b"c2")]);
    }

    #[test]
    fn slice_of_a_non_superset_segment_is_unchanged() {
        let records = vec![rec(b"k1", 10, b"c1"), rec(b"k2", 20, b"c2")];
        let sliced = slice_to_hlc_range(&records, (0, 20));
        assert_eq!(sliced, records);
    }

    #[test]
    fn decode_and_slice_composes_decode_and_slice_in_one_call() {
        let superset = vec![
            rec(b"k1", 100, b"c1"),
            rec(b"k2", 200, b"c2"),
            rec(b"k3", 300, b"c3-superset-tail"),
        ];
        let h = header(9, 1, (0, 200));
        let bytes = encode(&h, &superset);
        let (decoded_header, sliced) = decode_and_slice(&bytes, (0, 200)).expect("decodes");
        assert_eq!(decoded_header.tablet, 9);
        assert_eq!(sliced, superset[..2]);
    }

    #[test]
    fn ending_sequence_number_is_the_hlc_range_upper_bound() {
        let h = header(1, 0, (10, 999));
        assert_eq!(ending_sequence_number(&h), 999);
    }

    #[test]
    fn packed_hlc_helper_matches_hlc_pack() {
        let ts = HlcTimestamp {
            wall_ms: 5,
            logical: 1,
        };
        assert_eq!(pack(ts), crate::hlc::pack(ts));
    }

    /// The ledger-named-object amendment's own id: extends [`segment_id`]'s
    /// prefix byte-for-byte (so a debug `list(segment_id(..))` sweep still
    /// finds it) and two attempts differing in any one of proposer/term/
    /// nonce never collide.
    #[test]
    fn segment_object_id_extends_the_shard_prefix_and_disambiguates_every_axis() {
        let base = segment_id("orders", "L1", 7, 3);
        let a = segment_object_id("orders", "L1", 7, 3, "n1", 5, 42);
        assert!(
            a.starts_with(&format!("{base}/")),
            "object id must extend the shard's own deterministic prefix: {a:?}"
        );

        let different_proposer = segment_object_id("orders", "L1", 7, 3, "n2", 5, 42);
        let different_term = segment_object_id("orders", "L1", 7, 3, "n1", 6, 42);
        let different_nonce = segment_object_id("orders", "L1", 7, 3, "n1", 5, 43);
        let identical = segment_object_id("orders", "L1", 7, 3, "n1", 5, 42);

        assert_eq!(a, identical, "identical inputs must be a pure function");
        assert_ne!(a, different_proposer);
        assert_ne!(a, different_term);
        assert_ne!(a, different_nonce);
    }
}
