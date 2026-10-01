//! Compact, self-describing **binary codec** for the CP data plane's wire
//! messages and snapshot image (audit P2).
//!
//! `KvWire` / `RaftMsg<KvCommand>` and the engine snapshot image used to ride
//! `serde_json`, which renders every `Vec<u8>` key/value as a decimal byte array
//! (`[107,49,...]`) — roughly 3–4x the payload size on the hot replication path
//! and in every 1KB `InstallSnapshot` chunk's source image. This module is a
//! hand-rolled length-prefixed framing in the same style as `animus-storage`'s
//! manifest codec (no new dependency — the tree has no byte-transparent serde
//! format): a magic byte + version, `u8` enum tags, big-endian fixed-width
//! integers, and `u32`-length-prefixed byte strings.
//!
//! Scope: **wire + snapshot image only.** The Raft WAL stores `KvCommand` as
//! `serde_json` inside `WalRecord` lines in `animus-control`'s tagged
//! `CWL1`/`SWL1` envelopes — not this binary codec (its golden fixture is
//! `raftkv-wal/v1.bin`).
//!
//! ## Versioning
//!
//! Since ADR 0073 Phase 0 (workstream C) this is a **baselined format**:
//! [`VERSION`] restarted at `1`, every pre-baseline layout was dropped, and
//! golden fixtures (`tests/fixtures/formats/raftkv-wire/v1.bin` and
//! `raftkv-image/v1.bin`) pin the bytes. The frame shape stays the single
//! magic byte `0xCB` + a `u8` version. Decode failures are ADR 0073's shared
//! [`FormatError`], loud and named, never a panic or a silent misdecode:
//! empty input or a magic mismatch (a stray JSON payload, pre-baseline data)
//! is `FormatError::PreBaselineFormat`; version `0` or one newer than this
//! build is `FormatError::UnsupportedFormatVersion`; any other framing damage
//! (truncation, unknown tag, trailing bytes) is `FormatError::Malformed`. The
//! callers log the error (`tracing::warn!`) before dropping the message.
//!
//! **Decoding untrusted input is bounds-checked *and* allocation-safe** —
//! two distinct guarantees, not one. Every individual field read
//! (`Cursor::take` and everything built on it) is bounds-checked against
//! the remaining buffer, so a truncated or malformed frame is always a
//! clean `Err`, never an out-of-bounds panic. That alone is not enough: a
//! `u32`/`u64` **count** read off the wire (an `AppendEntries` entry count,
//! a `KindBatch`'s write count, …) used to be handed straight to
//! `Vec::with_capacity(n as usize)` to pre-size the collection *before* any
//! of its `n` elements were validated against the buffer — a single
//! corrupted or adversarial length-prefix byte could set `n` to a value
//! near `u32::MAX`, and the resulting many-GB-or-more allocation request
//! makes Rust's global allocator **abort the whole process**
//! (`handle_alloc_error`), which is not a catchable panic and therefore not
//! something a bounds-checked-reads guarantee alone prevents. Every such
//! site in this module (and its sibling engine-marker decoders in
//! `txn.rs`/`split.rs`) now caps the *requested capacity* at `.min(1 << 20)`
//! — the actual number of elements decoded is still governed solely by what
//! the buffer holds, so a legitimate message's cost is unchanged; only a
//! hostile/corrupted count's pre-allocation is bounded. See
//! `docs/engineering-lessons.md`'s "untrusted length-prefixed collection
//! pre-allocation" entry for the general pattern, and this module's own
//! `corrupted_append_entries_count_returns_a_graceful_error_not_an_alloc_abort`
//! / `decode_wire_never_panics_or_aborts_*` tests for the regression guard.

use std::collections::BTreeSet;

use animus_control::format::FormatError;
use animus_control::raft::{LogEntry, RaftMsg};
use animus_env::NodeId;
#[cfg(test)]
use animus_env::nid;
use animus_tablet::{KeyRange, SplitChild, TabletId};

use crate::hlc::HlcTimestamp;
use crate::txn::{TxnId, TxnOutcome, TxnWrite};
use crate::{ImageEntry, KvCommand, KvWire};

/// First byte of every encoded frame — rejects foreign payloads (e.g. a JSON
/// message from a mixed-version peer) with a clear error instead of a confusing
/// tag mismatch deeper in.
const MAGIC: u8 = 0xCB;
/// Wire/image codec version (ADR 0073 Phase 0, workstream C). Restarted at `1`
/// by the baseline reset: every pre-baseline layout (the old history ran to
/// `32`) was dropped. From the baseline on, an incompatible layout change is a
/// new version with a new golden fixture, never a rewrite of `1`.
const VERSION: u8 = 1;

/// Internal detail of a framing failure below the version header (what was
/// malformed); the public entry points wrap it as `FormatError::Malformed`.
type DecodeError = String;

/// Format names in [`FormatError`] messages (never encoded).
const WIRE_NAME: &str = "raftkv-wire";
const IMAGE_NAME: &str = "raftkv-image";

/// Validate the `magic || version` header, returning the version found and
/// the cursor positioned after it; the caller dispatches on the version.
/// Empty/foreign input is pre-baseline; version `0` or one newer than this
/// build is unsupported.
fn check_header<'a>(
    bytes: &'a [u8],
    format: &'static str,
) -> Result<(u8, Cursor<'a>), FormatError> {
    if bytes.first() != Some(&MAGIC) {
        return Err(FormatError::PreBaselineFormat { format });
    }
    let Some(&version) = bytes.get(1) else {
        return Err(FormatError::Malformed {
            format,
            detail: "truncated frame: magic byte without a version byte".to_owned(),
        });
    };
    if version == 0 || version > VERSION {
        return Err(FormatError::UnsupportedFormatVersion {
            format,
            found: version,
            max_supported: VERSION,
        });
    }
    let mut c = Cursor::new(bytes);
    c.pos = 2;
    Ok((version, c))
}

/// Retired format versions (ADR 0073 "The decoder pattern", point 4). Each
/// retired version `N` gets a submodule `legacy::vN` holding its frozen
/// decoder, the frozen shape type that decoder produces (`VNFoo`), and the
/// `From<VNFoo>` translation into the current in-memory type; the version
/// `match` in the public decoder routes to it. Kept forever, edited only by
/// mechanical compile fixes. Empty today: every version of this codec is
/// still v1, i.e. current.
mod legacy {}

/// The error for a version the dispatch `match` has no arm for.
fn unsupported(format: &'static str, found: u8) -> FormatError {
    FormatError::UnsupportedFormatVersion {
        format,
        found,
        max_supported: VERSION,
    }
}

// ---- primitive writers -----------------------------------------------------

fn put_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn put_bool(out: &mut Vec<u8>, v: bool) {
    out.push(u8::from(v));
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_be_bytes());
    out.extend_from_slice(b);
}

/// `serde_json`-encode `value` into one `put_bytes`-framed blob (this
/// crate's deeply-nested, evolving `KvCommand::KindEval` field types use
/// JSON-inside-the-envelope rather than a hand-rolled field-by-field
/// encoding).
fn put_json<T: serde::Serialize>(out: &mut Vec<u8>, value: &T) {
    put_bytes(
        out,
        &serde_json::to_vec(value).expect("KindEval field serializes"),
    );
}

fn put_opt_bytes(out: &mut Vec<u8>, b: &Option<Vec<u8>>) {
    match b {
        None => put_u8(out, 0),
        Some(b) => {
            put_u8(out, 1);
            put_bytes(out, b);
        }
    }
}

/// A node id as a length-prefixed UTF-8 string (ADR 0040 PR3: node ids are
/// validated strings now, not small dense `u64`s, so this replaces the old
/// fixed-width `u64` encoding — a persisted-format break, fresh clusters only).
fn put_node_id(out: &mut Vec<u8>, n: &NodeId) {
    put_bytes(out, n.as_str().as_bytes());
}

fn put_node_set(out: &mut Vec<u8>, s: &BTreeSet<NodeId>) {
    out.extend_from_slice(&(s.len() as u32).to_be_bytes());
    for n in s {
        put_node_id(out, n);
    }
}

fn put_opt_node_set(out: &mut Vec<u8>, s: &Option<BTreeSet<NodeId>>) {
    match s {
        None => put_u8(out, 0),
        Some(s) => {
            put_u8(out, 1);
            put_node_set(out, s);
        }
    }
}

// ---- primitive reader ------------------------------------------------------

/// A forward-only cursor over frame bytes; any short read is a loud decode
/// error (mirrors the storage manifest codec's `Cursor`). Bounds-checks
/// every individual read against the remaining buffer — but a caller that
/// reads a count via [`Cursor::u32`]/[`Cursor::u64`] and then pre-sizes a
/// collection with it must still cap that pre-allocation itself (see this
/// module's own doc comment above): this cursor alone cannot stop an
/// untrusted count from driving an oversized `Vec::with_capacity` before a
/// single element has been validated.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.pos + n > self.bytes.len() {
            return Err(format!(
                "truncated frame: wanted {n} bytes at offset {}, have {}",
                self.pos,
                self.bytes.len()
            ));
        }
        let s = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().expect("4B")))
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().expect("8B")))
    }

    fn bool(&mut self) -> Result<bool, DecodeError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(format!("invalid bool byte {other}")),
        }
    }

    fn bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    /// Read one [`put_json`]-framed blob back — the decode dual of every
    /// `KvCommand::KindEval` field that rides as `serde_json` inside the
    /// binary envelope. Safe against an untrusted length the same way
    /// [`Cursor::bytes`] already is: `bytes()` bounds-checks the frame
    /// against the remaining buffer BEFORE this ever allocates, so a
    /// corrupted length here still yields a loud `Err`, never an
    /// allocator abort.
    fn json<T: serde::de::DeserializeOwned>(&mut self) -> Result<T, DecodeError> {
        let raw = self.bytes()?;
        serde_json::from_slice(&raw).map_err(|e| format!("KindEval field decode: {e}"))
    }

    fn opt_bytes(&mut self) -> Result<Option<Vec<u8>>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.bytes()?)),
            other => Err(format!("invalid option tag {other}")),
        }
    }

    /// A node id: a length-prefixed UTF-8 string (ADR 0040 PR3). Bypasses
    /// [`NodeId::propose`]'s charset validation via `NodeId::new_unchecked` —
    /// this id was already validated once at whatever intake boundary first
    /// proposed it; a wire/snapshot round-trip is a trusted decode, not fresh
    /// untrusted input.
    fn node_id(&mut self) -> Result<NodeId, DecodeError> {
        let bytes = self.bytes()?;
        let s = String::from_utf8(bytes).map_err(|e| format!("node id is not UTF-8: {e}"))?;
        Ok(NodeId::new_unchecked(s))
    }

    fn node_set(&mut self) -> Result<BTreeSet<NodeId>, DecodeError> {
        let len = self.u32()?;
        let mut s = BTreeSet::new();
        for _ in 0..len {
            s.insert(self.node_id()?);
        }
        Ok(s)
    }

    fn opt_node_set(&mut self) -> Result<Option<BTreeSet<NodeId>>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.node_set()?)),
            other => Err(format!("invalid option tag {other}")),
        }
    }

    fn finish(self) -> Result<(), DecodeError> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(format!(
                "trailing garbage: {} bytes after frame end",
                self.bytes.len() - self.pos
            ))
        }
    }
}

// ---- KvCommand ---------------------------------------------------------------

fn put_key_range(out: &mut Vec<u8>, r: &KeyRange) {
    put_bytes(out, &r.start);
    put_opt_bytes(out, &r.end);
}

fn read_key_range(c: &mut Cursor<'_>) -> Result<KeyRange, DecodeError> {
    Ok(KeyRange {
        start: c.bytes()?,
        end: c.opt_bytes()?,
    })
}

/// ADR 0018 §2/PR2: an [`HlcTimestamp`] as fixed-width `(wall_ms: u64,
/// logical: u32)`.
fn put_ts(out: &mut Vec<u8>, ts: HlcTimestamp) {
    put_u64(out, ts.wall_ms);
    out.extend_from_slice(&ts.logical.to_be_bytes());
}

fn read_ts(c: &mut Cursor<'_>) -> Result<HlcTimestamp, DecodeError> {
    let wall_ms = c.u64()?;
    let logical = u32::from_be_bytes(c.take(4)?.try_into().expect("4B"));
    Ok(HlcTimestamp { wall_ms, logical })
}

/// ADR 0018 §2/PR5: an `Option<HlcTimestamp>` — mirrors [`put_opt_bytes`]'s
/// presence-tag shape (`KvCommand::TxnAbort`'s `orphan_created_ts`).
fn put_opt_ts(out: &mut Vec<u8>, ts: &Option<HlcTimestamp>) {
    match ts {
        None => put_u8(out, 0),
        Some(ts) => {
            put_u8(out, 1);
            put_ts(out, *ts);
        }
    }
}

fn read_opt_ts(c: &mut Cursor<'_>) -> Result<Option<HlcTimestamp>, DecodeError> {
    match c.u8()? {
        0 => Ok(None),
        1 => Ok(Some(read_ts(c)?)),
        other => Err(format!("bad opt_ts tag {other}")),
    }
}

/// ADR 0018 §2/PR3: a [`TxnId`] as `(ts, node)`.
fn put_txn_id(out: &mut Vec<u8>, id: &TxnId) {
    put_ts(out, id.ts);
    put_node_id(out, &id.node);
}

fn read_txn_id(c: &mut Cursor<'_>) -> Result<TxnId, DecodeError> {
    Ok(TxnId {
        ts: read_ts(c)?,
        node: c.node_id()?,
    })
}

/// ADR 0018 §2/PR4: `TxnOutcome`'s decision travels explicitly inside
/// `KvCommand::TxnResolve` — see that variant's doc.
fn put_txn_outcome(out: &mut Vec<u8>, o: &TxnOutcome) {
    match o {
        TxnOutcome::Committed { commit_ts } => {
            put_u8(out, 0);
            put_ts(out, *commit_ts);
        }
        TxnOutcome::Aborted => put_u8(out, 1),
    }
}

fn read_txn_outcome(c: &mut Cursor<'_>) -> Result<TxnOutcome, DecodeError> {
    Ok(match c.u8()? {
        0 => TxnOutcome::Committed {
            commit_ts: read_ts(c)?,
        },
        1 => TxnOutcome::Aborted,
        other => return Err(format!("unknown TxnOutcome tag {other}")),
    })
}

/// A `(row kind, logical key, value)` write list — `KvCommand::KindBatch`'s
/// own `writes` shape, and (ADR 0046 A1) a `txn::TxnWrite`'s
/// `kind_writes` payload. Shared here so the two never silently drift.
fn put_kind_writes(out: &mut Vec<u8>, writes: &[crate::KindWrite]) {
    out.extend_from_slice(&(writes.len() as u32).to_be_bytes());
    for (kind, k, v) in writes {
        put_u8(out, *kind);
        put_bytes(out, k);
        put_opt_bytes(out, v);
    }
}

fn read_kind_writes(c: &mut Cursor<'_>) -> Result<Vec<crate::KindWrite>, DecodeError> {
    let n = c.u32()?;
    // `n` is an untrusted wire count read before any of its elements are
    // validated against the remaining buffer — cap the *requested
    // capacity* (never the number of elements actually decoded, which
    // stays governed solely by what the buffer holds) so a corrupted/
    // hostile `n` near `u32::MAX` can't demand a many-GB allocation and
    // trigger an allocator abort. Mirrors the `.min(1 << 20)` idiom this
    // crate's `backup.rs`/`segment.rs` decoders already use.
    let mut writes = Vec::with_capacity(n.min(1 << 20) as usize);
    for _ in 0..n {
        writes.push((c.u8()?, c.bytes()?, c.opt_bytes()?));
    }
    Ok(writes)
}

/// A `(key prefix, encoded record)` optional change-log record —
/// `KvCommand::KindBatch`'s own `change_log` shape, and (ADR 0046 A1)
/// a `txn::TxnWrite`'s `change_log` payload.
fn put_change_log(out: &mut Vec<u8>, change_log: &Option<(Vec<u8>, Vec<u8>)>) {
    match change_log {
        None => put_u8(out, 0),
        Some((prefix, record)) => {
            put_u8(out, 1);
            put_bytes(out, prefix);
            put_bytes(out, record);
        }
    }
}

#[allow(clippy::type_complexity)]
fn read_change_log(c: &mut Cursor<'_>) -> Result<Option<(Vec<u8>, Vec<u8>)>, DecodeError> {
    Ok(match c.u8()? {
        0 => None,
        1 => Some((c.bytes()?, c.bytes()?)),
        other => return Err(format!("invalid change_log tag {other}")),
    })
}

/// `KindBatch.change_log`'s multi-record shape (see the
/// field's own doc for why a marker-table batch carries one record per
/// item in a single entry). Count-prefixed, unlike the tagged `Option`
/// form `TxnWrite` keeps.
fn put_change_logs(out: &mut Vec<u8>, change_log: &[(Vec<u8>, Vec<u8>)]) {
    out.extend_from_slice(&(change_log.len() as u32).to_be_bytes());
    for (prefix, record) in change_log {
        put_bytes(out, prefix);
        put_bytes(out, record);
    }
}

#[allow(clippy::type_complexity)]
fn read_change_logs(c: &mut Cursor<'_>) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DecodeError> {
    let n = c.u32()?;
    // Capped pre-allocation against an untrusted wire count — see
    // `read_kind_writes`'s comment above for why.
    let mut out = Vec::with_capacity(n.min(1 << 20) as usize);
    for _ in 0..n {
        out.push((c.bytes()?, c.bytes()?));
    }
    Ok(out)
}

fn put_command(out: &mut Vec<u8>, c: &KvCommand) {
    match c {
        KvCommand::Put { key, value, ts } => {
            put_u8(out, 0);
            put_bytes(out, key);
            put_bytes(out, value);
            put_ts(out, *ts);
        }
        KvCommand::Batch { puts, ts } => {
            put_u8(out, 1);
            out.extend_from_slice(&(puts.len() as u32).to_be_bytes());
            for (k, v) in puts {
                put_bytes(out, k);
                put_bytes(out, v);
            }
            put_ts(out, *ts);
        }
        KvCommand::KindBatch {
            writes,
            change_log,
            ts,
        } => {
            put_u8(out, 12);
            put_kind_writes(out, writes);
            put_change_logs(out, change_log);
            put_ts(out, *ts);
        }
        KvCommand::KindEval {
            schema,
            pk,
            sk,
            op,
            condition,
            ttl_expired,
            ts,
        } => {
            put_u8(out, 16);
            put_json(out, schema);
            put_json(out, pk);
            put_json(out, sk);
            put_json(out, op);
            put_json(out, condition);
            put_bool(out, *ttl_expired);
            put_ts(out, *ts);
        }
        KvCommand::KindEvalBatch { entries, ts } => {
            put_u8(out, 17);
            out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
            for entry in entries {
                put_json(out, &entry.schema);
                put_json(out, &entry.pk);
                put_json(out, &entry.sk);
                put_json(out, &entry.op);
                put_json(out, &entry.condition);
                put_bool(out, entry.ttl_expired);
            }
            put_ts(out, *ts);
        }
        KvCommand::SeedBatch { rows, ts } => {
            put_u8(out, 13);
            out.extend_from_slice(&(rows.len() as u32).to_be_bytes());
            for (kind, logical, value, version) in rows {
                put_u8(out, *kind);
                put_bytes(out, logical);
                put_opt_bytes(out, value);
                out.extend_from_slice(&version.to_be_bytes());
            }
            put_ts(out, *ts);
        }
        KvCommand::Delete { key, ts } => {
            put_u8(out, 2);
            put_bytes(out, key);
            put_ts(out, *ts);
        }
        KvCommand::Cas {
            key,
            expected,
            value,
            ts,
        } => {
            put_u8(out, 3);
            put_bytes(out, key);
            put_opt_bytes(out, expected);
            put_bytes(out, value);
            put_ts(out, *ts);
        }
        KvCommand::NoOp => put_u8(out, 5),
        KvCommand::ReadCeiling { ts } => {
            put_u8(out, 7);
            put_ts(out, *ts);
        }
        KvCommand::Freeze { ts } => {
            put_u8(out, 14);
            put_ts(out, *ts);
        }
        KvCommand::SplitTablet {
            split_key,
            children,
            ts,
        } => {
            put_u8(out, 15);
            put_bytes(out, split_key);
            for child in children {
                out.extend_from_slice(&child.id.0.to_be_bytes());
                out.extend_from_slice(&(child.replicas.len() as u32).to_be_bytes());
                for r in &child.replicas {
                    put_node_id(out, r);
                }
            }
            put_ts(out, *ts);
        }
        KvCommand::TxnStage {
            txn_id,
            record_key,
            record_table,
            is_anchor,
            writes,
            spans,
            conditions,
            ts,
        } => {
            put_u8(out, 8);
            put_txn_id(out, txn_id);
            put_bytes(out, record_key);
            put_bytes(out, record_table.as_bytes());
            put_bool(out, *is_anchor);
            // ADR 0046 A1: each write is a `txn::TxnWrite` —
            // base key/value plus an optional derived kind-scope payload,
            // encoded with the SAME `put_kind_writes`/`put_change_log`
            // helpers `KindBatch` itself uses (never a second copy).
            out.extend_from_slice(&(writes.len() as u32).to_be_bytes());
            for w in writes {
                put_bytes(out, &w.key);
                put_opt_bytes(out, &w.value);
                put_kind_writes(out, &w.kind_writes);
                put_change_log(out, &w.change_log);
                // ADR 0049 §3: the stage marker shares change_log's own
                // tagged-Option `(prefix, record)` encoding.
                put_change_log(out, &w.stage_marker);
                // `Option<txn::PendingTxnWrite>` as one JSON blob (same
                // JSON-inside-the-envelope choice as `put_json`).
                put_json(out, &w.pending);
            }
            out.extend_from_slice(&(spans.len() as u32).to_be_bytes());
            for (table, span) in spans {
                put_bytes(out, table.as_bytes());
                put_key_range(out, span);
            }
            out.extend_from_slice(&(conditions.len() as u32).to_be_bytes());
            for (k, expected) in conditions {
                put_bytes(out, k);
                put_opt_bytes(out, expected);
            }
            put_ts(out, *ts);
        }
        KvCommand::TxnCommit {
            txn_id,
            record_key,
            ts,
        } => {
            put_u8(out, 9);
            put_txn_id(out, txn_id);
            put_bytes(out, record_key);
            put_ts(out, *ts);
        }
        KvCommand::TxnAbort {
            txn_id,
            record_key,
            ts,
            orphan_created_ts,
        } => {
            put_u8(out, 10);
            put_txn_id(out, txn_id);
            put_bytes(out, record_key);
            put_ts(out, *ts);
            put_opt_ts(out, orphan_created_ts);
        }
        KvCommand::TxnResolve {
            txn_id,
            record_key,
            keys,
            outcome,
            ts,
        } => {
            put_u8(out, 11);
            put_txn_id(out, txn_id);
            put_bytes(out, record_key);
            out.extend_from_slice(&(keys.len() as u32).to_be_bytes());
            for k in keys {
                put_bytes(out, k);
            }
            put_txn_outcome(out, outcome);
            put_ts(out, *ts);
        }
    }
}

fn read_command(c: &mut Cursor<'_>) -> Result<KvCommand, DecodeError> {
    Ok(match c.u8()? {
        0 => KvCommand::Put {
            key: c.bytes()?,
            value: c.bytes()?,
            ts: read_ts(c)?,
        },
        1 => {
            let n = c.u32()?;
            // Capped pre-allocation against an untrusted wire count — see
            // `read_kind_writes`'s comment for why.
            let mut puts = Vec::with_capacity(n.min(1 << 20) as usize);
            for _ in 0..n {
                puts.push((c.bytes()?, c.bytes()?));
            }
            KvCommand::Batch {
                puts,
                ts: read_ts(c)?,
            }
        }
        12 => {
            let writes = read_kind_writes(c)?;
            let change_log = read_change_logs(c)?;
            KvCommand::KindBatch {
                writes,
                change_log,
                ts: read_ts(c)?,
            }
        }
        2 => KvCommand::Delete {
            key: c.bytes()?,
            ts: read_ts(c)?,
        },
        3 => KvCommand::Cas {
            key: c.bytes()?,
            expected: c.opt_bytes()?,
            value: c.bytes()?,
            ts: read_ts(c)?,
        },
        5 => KvCommand::NoOp,
        7 => KvCommand::ReadCeiling { ts: read_ts(c)? },
        14 => KvCommand::Freeze { ts: read_ts(c)? },
        15 => {
            let split_key = c.bytes()?;
            let mut children = Vec::with_capacity(2);
            for _ in 0..2 {
                let id = TabletId(c.u64()?);
                let n = c.u32()?;
                // Capped pre-allocation against an untrusted wire count —
                // see `read_kind_writes`'s comment for why.
                let mut replicas = Vec::with_capacity(n.min(1 << 20) as usize);
                for _ in 0..n {
                    replicas.push(c.node_id()?);
                }
                children.push(SplitChild { id, replicas });
            }
            let children: [SplitChild; 2] = children
                .try_into()
                .map_err(|_| "SplitTablet children must have exactly 2 entries".to_string())?;
            KvCommand::SplitTablet {
                split_key,
                children,
                ts: read_ts(c)?,
            }
        }
        8 => {
            let txn_id = read_txn_id(c)?;
            let record_key = c.bytes()?;
            let record_table = String::from_utf8(c.bytes()?)
                .map_err(|_| "TxnStage record_table not utf8".to_string())?;
            let is_anchor = c.bool()?;
            let n = c.u32()?;
            // Capped pre-allocation against an untrusted wire count — see
            // `read_kind_writes`'s comment for why.
            let mut writes = Vec::with_capacity(n.min(1 << 20) as usize);
            for _ in 0..n {
                let key = c.bytes()?;
                let value = c.opt_bytes()?;
                let kind_writes = read_kind_writes(c)?;
                let change_log = read_change_log(c)?;
                let stage_marker = read_change_log(c)?;
                let pending = c.json()?;
                writes.push(TxnWrite {
                    key,
                    value,
                    kind_writes,
                    change_log,
                    stage_marker,
                    pending,
                });
            }
            let n = c.u32()?;
            // Capped pre-allocation against an untrusted wire count — see
            // `read_kind_writes`'s comment for why.
            let mut spans = Vec::with_capacity(n.min(1 << 20) as usize);
            for _ in 0..n {
                let table = String::from_utf8(c.bytes()?)
                    .map_err(|_| "TxnStage span table not utf8".to_string())?;
                spans.push((table, read_key_range(c)?));
            }
            let n = c.u32()?;
            let mut conditions = Vec::with_capacity(n.min(1 << 20) as usize);
            for _ in 0..n {
                conditions.push((c.bytes()?, c.opt_bytes()?));
            }
            KvCommand::TxnStage {
                txn_id,
                record_key,
                record_table,
                is_anchor,
                writes,
                spans,
                conditions,
                ts: read_ts(c)?,
            }
        }
        9 => KvCommand::TxnCommit {
            txn_id: read_txn_id(c)?,
            record_key: c.bytes()?,
            ts: read_ts(c)?,
        },
        10 => KvCommand::TxnAbort {
            txn_id: read_txn_id(c)?,
            record_key: c.bytes()?,
            ts: read_ts(c)?,
            orphan_created_ts: read_opt_ts(c)?,
        },
        11 => {
            let txn_id = read_txn_id(c)?;
            let record_key = c.bytes()?;
            let n = c.u32()?;
            // Capped pre-allocation against an untrusted wire count — see
            // `read_kind_writes`'s comment for why.
            let mut keys = Vec::with_capacity(n.min(1 << 20) as usize);
            for _ in 0..n {
                keys.push(c.bytes()?);
            }
            let outcome = read_txn_outcome(c)?;
            KvCommand::TxnResolve {
                txn_id,
                record_key,
                keys,
                outcome,
                ts: read_ts(c)?,
            }
        }
        16 => KvCommand::KindEval {
            schema: c.json()?,
            pk: c.json()?,
            sk: c.json()?,
            op: c.json()?,
            condition: c.json()?,
            ttl_expired: c.bool()?,
            ts: read_ts(c)?,
        },
        17 => {
            let n = c.u32()?;
            // Capped pre-allocation against an untrusted wire count — see
            // `read_kind_writes`'s comment for why.
            let mut entries = Vec::with_capacity(n.min(1 << 20) as usize);
            for _ in 0..n {
                entries.push(crate::KindEvalEntry {
                    schema: c.json()?,
                    pk: c.json()?,
                    sk: c.json()?,
                    op: c.json()?,
                    condition: c.json()?,
                    ttl_expired: c.bool()?,
                });
            }
            KvCommand::KindEvalBatch {
                entries,
                ts: read_ts(c)?,
            }
        }
        13 => {
            let n = c.u32()?;
            // Capped pre-allocation against an untrusted wire count — see
            // `read_kind_writes`'s comment for why.
            let mut rows = Vec::with_capacity(n.min(1 << 20) as usize);
            for _ in 0..n {
                let kind = c.u8()?;
                let logical = c.bytes()?;
                let value = c.opt_bytes()?;
                let version = c.u64()?;
                rows.push((kind, logical, value, version));
            }
            KvCommand::SeedBatch {
                rows,
                ts: read_ts(c)?,
            }
        }
        other => return Err(format!("unknown KvCommand tag {other}")),
    })
}

// ---- LogEntry<KvCommand> -----------------------------------------------------

fn put_entry(out: &mut Vec<u8>, e: &LogEntry<KvCommand>) {
    put_u64(out, e.term);
    put_u64(out, e.index);
    put_command(out, &e.command);
    put_opt_node_set(out, &e.config);
    put_opt_node_set(out, &e.learners);
}

fn read_entry(c: &mut Cursor<'_>) -> Result<LogEntry<KvCommand>, DecodeError> {
    Ok(LogEntry {
        term: c.u64()?,
        index: c.u64()?,
        command: read_command(c)?,
        config: c.opt_node_set()?,
        learners: c.opt_node_set()?,
    })
}

// ---- RaftMsg<KvCommand> ------------------------------------------------------

#[allow(clippy::enum_glob_use)]
fn put_raft(out: &mut Vec<u8>, m: &RaftMsg<KvCommand>) {
    match m {
        RaftMsg::PreVote {
            term,
            candidate,
            last_log_index,
            last_log_term,
        } => {
            put_u8(out, 0);
            put_u64(out, *term);
            put_node_id(out, candidate);
            put_u64(out, *last_log_index);
            put_u64(out, *last_log_term);
        }
        RaftMsg::PreVoteResp { term, granted } => {
            put_u8(out, 1);
            put_u64(out, *term);
            put_bool(out, *granted);
        }
        RaftMsg::RequestVote {
            term,
            candidate,
            last_log_index,
            last_log_term,
        } => {
            put_u8(out, 2);
            put_u64(out, *term);
            put_node_id(out, candidate);
            put_u64(out, *last_log_index);
            put_u64(out, *last_log_term);
        }
        RaftMsg::RequestVoteResp { term, granted } => {
            put_u8(out, 3);
            put_u64(out, *term);
            put_bool(out, *granted);
        }
        RaftMsg::AppendEntries {
            term,
            leader,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } => {
            put_u8(out, 4);
            put_u64(out, *term);
            put_node_id(out, leader);
            put_u64(out, *prev_log_index);
            put_u64(out, *prev_log_term);
            out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
            for e in entries {
                put_entry(out, e);
            }
            put_u64(out, *leader_commit);
        }
        RaftMsg::AppendEntriesResp {
            term,
            success,
            match_index,
            needs_snapshot,
        } => {
            put_u8(out, 5);
            put_u64(out, *term);
            put_bool(out, *success);
            put_u64(out, *match_index);
            put_bool(out, *needs_snapshot);
        }
        RaftMsg::InstallSnapshot {
            term,
            leader,
            last_index,
            last_term,
            offset,
            data,
            total,
            done,
            config,
            learners,
        } => {
            put_u8(out, 6);
            put_u64(out, *term);
            put_node_id(out, leader);
            put_u64(out, *last_index);
            put_u64(out, *last_term);
            put_u64(out, *offset);
            put_bytes(out, data);
            put_u64(out, *total);
            put_bool(out, *done);
            put_opt_node_set(out, config);
            put_opt_node_set(out, learners);
        }
        RaftMsg::InstallSnapshotResp {
            term,
            last_index,
            next_offset,
        } => {
            put_u8(out, 7);
            put_u64(out, *term);
            put_u64(out, *last_index);
            put_u64(out, *next_offset);
        }
        RaftMsg::Heartbeat { node } => {
            put_u8(out, 8);
            put_node_id(out, node);
        }
        RaftMsg::TimeoutNow { term } => {
            put_u8(out, 9);
            put_u64(out, *term);
        }
        RaftMsg::Quiesce { term, commit_index } => {
            put_u8(out, 10);
            put_u64(out, *term);
            put_u64(out, *commit_index);
        }
        RaftMsg::WakeRequest { term } => {
            put_u8(out, 11);
            put_u64(out, *term);
        }
        // Issue #667 (P0 Raft safety): this crate's `RaftKvNode` never calls
        // `RaftCore::begin_cluster_check` (only `animus-control`'s own
        // `node.rs` driver does), so these two variants never actually ride
        // this wire in production — but `RaftMsg<KvCommand>` is the same
        // generic type either way, and this match must stay exhaustive.
        // Encoded for completeness/forward-compat rather than `unreachable!`,
        // on the same footing as every other variant here.
        RaftMsg::ClusterProbe => {
            put_u8(out, 12);
        }
        RaftMsg::ClusterProbeResp {
            term,
            committed_index,
            config,
            ever_heard_from_prober,
        } => {
            put_u8(out, 13);
            put_u64(out, *term);
            put_u64(out, *committed_index);
            put_node_set(out, config);
            put_bool(out, *ever_heard_from_prober);
        }
        // Issue #1061: the explicit removal notice and its ack.
        RaftMsg::Removed {
            term,
            removal_index,
            removal_term,
            config,
            learners,
        } => {
            put_u8(out, 14);
            put_u64(out, *term);
            put_u64(out, *removal_index);
            put_u64(out, *removal_term);
            put_node_set(out, config);
            put_node_set(out, learners);
        }
        RaftMsg::RemovedAck {
            term,
            removal_index,
        } => {
            put_u8(out, 15);
            put_u64(out, *term);
            put_u64(out, *removal_index);
        }
    }
}

fn read_raft(c: &mut Cursor<'_>) -> Result<RaftMsg<KvCommand>, DecodeError> {
    Ok(match c.u8()? {
        0 => RaftMsg::PreVote {
            term: c.u64()?,
            candidate: c.node_id()?,
            last_log_index: c.u64()?,
            last_log_term: c.u64()?,
        },
        1 => RaftMsg::PreVoteResp {
            term: c.u64()?,
            granted: c.bool()?,
        },
        2 => RaftMsg::RequestVote {
            term: c.u64()?,
            candidate: c.node_id()?,
            last_log_index: c.u64()?,
            last_log_term: c.u64()?,
        },
        3 => RaftMsg::RequestVoteResp {
            term: c.u64()?,
            granted: c.bool()?,
        },
        4 => {
            let term = c.u64()?;
            let leader = c.node_id()?;
            let prev_log_index = c.u64()?;
            let prev_log_term = c.u64()?;
            let n = c.u32()?;
            // Capped pre-allocation against an untrusted wire count — see
            // `read_kind_writes`'s comment for why. This is the exact site
            // a corrupted `AppendEntries` entry-count field once reached to
            // trigger `SIGABRT` via `handle_alloc_error` (reproduced via
            // `cargo test -p animus-test --test raftkv_linearizable`).
            let mut entries = Vec::with_capacity(n.min(1 << 20) as usize);
            for _ in 0..n {
                entries.push(read_entry(c)?);
            }
            RaftMsg::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: c.u64()?,
            }
        }
        5 => RaftMsg::AppendEntriesResp {
            term: c.u64()?,
            success: c.bool()?,
            match_index: c.u64()?,
            needs_snapshot: c.bool()?,
        },
        6 => RaftMsg::InstallSnapshot {
            term: c.u64()?,
            leader: c.node_id()?,
            last_index: c.u64()?,
            last_term: c.u64()?,
            offset: c.u64()?,
            data: c.bytes()?,
            total: c.u64()?,
            done: c.bool()?,
            config: c.opt_node_set()?,
            learners: c.opt_node_set()?,
        },
        7 => RaftMsg::InstallSnapshotResp {
            term: c.u64()?,
            last_index: c.u64()?,
            next_offset: c.u64()?,
        },
        8 => RaftMsg::Heartbeat { node: c.node_id()? },
        9 => RaftMsg::TimeoutNow { term: c.u64()? },
        10 => RaftMsg::Quiesce {
            term: c.u64()?,
            commit_index: c.u64()?,
        },
        11 => RaftMsg::WakeRequest { term: c.u64()? },
        12 => RaftMsg::ClusterProbe,
        13 => RaftMsg::ClusterProbeResp {
            term: c.u64()?,
            committed_index: c.u64()?,
            config: c.node_set()?,
            ever_heard_from_prober: c.bool()?,
        },
        14 => RaftMsg::Removed {
            term: c.u64()?,
            removal_index: c.u64()?,
            removal_term: c.u64()?,
            config: c.node_set()?,
            learners: c.node_set()?,
        },
        15 => RaftMsg::RemovedAck {
            term: c.u64()?,
            removal_index: c.u64()?,
        },
        other => return Err(format!("unknown RaftMsg tag {other}")),
    })
}

// ---- KvWire --------------------------------------------------------------

/// Encode a [`KvWire`] message to its binary frame.
pub(crate) fn encode_wire(w: &KvWire) -> Vec<u8> {
    let mut out = Vec::new();
    put_u8(&mut out, MAGIC);
    put_u8(&mut out, VERSION);
    match w {
        KvWire::Raft(m) => {
            put_u8(&mut out, 0);
            put_raft(&mut out, m);
        }
        KvWire::ReadProbe { term, epoch } => {
            put_u8(&mut out, 1);
            put_u64(&mut out, *term);
            put_u64(&mut out, *epoch);
        }
        KvWire::ReadProbeAck { term, epoch } => {
            put_u8(&mut out, 2);
            put_u64(&mut out, *term);
            put_u64(&mut out, *epoch);
        }
        KvWire::HeartbeatBatch(entries) => {
            put_u8(&mut out, 3);
            out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
            for (stream, msg) in entries {
                put_u64(&mut out, *stream);
                put_raft(&mut out, msg);
            }
        }
    }
    out
}

/// Decode a binary frame into a [`KvWire`] message. Errors are descriptive and
/// the caller logs them loudly before dropping the message.
pub(crate) fn decode_wire(bytes: &[u8]) -> Result<KvWire, FormatError> {
    let (version, c) = check_header(bytes, WIRE_NAME)?;
    match version {
        1 => decode_wire_v1(c),
        found => Err(unsupported(WIRE_NAME, found)),
    }
}

/// The version-1 (current) `raftkv-wire` body decoder. When v2 lands this
/// moves, frozen, into [`legacy`].
fn decode_wire_v1(c: Cursor<'_>) -> Result<KvWire, FormatError> {
    decode_wire_body(c).map_err(|detail| FormatError::Malformed {
        format: WIRE_NAME,
        detail,
    })
}

fn decode_wire_body(mut c: Cursor<'_>) -> Result<KvWire, DecodeError> {
    let wire = match c.u8()? {
        0 => KvWire::Raft(read_raft(&mut c)?),
        1 => KvWire::ReadProbe {
            term: c.u64()?,
            epoch: c.u64()?,
        },
        2 => KvWire::ReadProbeAck {
            term: c.u64()?,
            epoch: c.u64()?,
        },
        3 => {
            let n = c.u32()?;
            // Capped pre-allocation against an untrusted wire count — same
            // discipline as `read_raft`'s own `AppendEntries` entry-count
            // read (see this module's own doc, "untrusted length-prefixed
            // collection pre-allocation").
            let mut entries = Vec::with_capacity(n.min(1 << 20) as usize);
            for _ in 0..n {
                let stream = c.u64()?;
                let msg = read_raft(&mut c)?;
                entries.push((stream, msg));
            }
            KvWire::HeartbeatBatch(entries)
        }
        other => return Err(format!("unknown KvWire tag {other}")),
    };
    c.finish()?;
    Ok(wire)
}

// ---- snapshot image --------------------------------------------------------

/// Encode the engine snapshot image (`(key, value-or-tombstone, version)`
/// entries) shipped in `InstallSnapshot` chunks.
///
/// `max_ts` (issue #804) is the sender's own apply-task
/// `max_applied_ts` at image-build time — an upper bound on every `ts` any
/// entry folded into this tablet has ever committed, whether or not that
/// entry's apply wrote a row. See `lib.rs`'s `engine_image` doc.
pub(crate) fn encode_image(entries: &[ImageEntry], max_ts: Option<HlcTimestamp>) -> Vec<u8> {
    let mut out = Vec::new();
    put_u8(&mut out, MAGIC);
    put_u8(&mut out, VERSION);
    put_opt_ts(&mut out, &max_ts);
    out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for (kind, key, value, version) in entries {
        put_u8(&mut out, *kind);
        put_bytes(&mut out, key);
        put_opt_bytes(&mut out, value);
        put_u64(&mut out, *version);
    }
    out
}

/// Decode an engine snapshot image. Loud on any malformation (a partial
/// transfer never reaches this — chunks are reassembled to `total` first).
/// Returns the sender's `max_ts` header (see [`encode_image`]) alongside the
/// row entries.
pub(crate) fn decode_image(
    bytes: &[u8],
) -> Result<(Option<HlcTimestamp>, Vec<ImageEntry>), FormatError> {
    let (version, c) = check_header(bytes, IMAGE_NAME)?;
    match version {
        1 => decode_image_v1(c),
        found => Err(unsupported(IMAGE_NAME, found)),
    }
}

/// The version-1 (current) `raftkv-image` body decoder. When v2 lands this
/// moves, frozen, into [`legacy`].
fn decode_image_v1(c: Cursor<'_>) -> Result<(Option<HlcTimestamp>, Vec<ImageEntry>), FormatError> {
    decode_image_body(c).map_err(|detail| FormatError::Malformed {
        format: IMAGE_NAME,
        detail,
    })
}

fn decode_image_body(
    mut c: Cursor<'_>,
) -> Result<(Option<HlcTimestamp>, Vec<ImageEntry>), DecodeError> {
    let max_ts = read_opt_ts(&mut c)?;
    let n = c.u32()?;
    // Capped pre-allocation against an untrusted wire count — see
    // `read_kind_writes`'s comment for why.
    let mut entries = Vec::with_capacity(n.min(1 << 20) as usize);
    for _ in 0..n {
        entries.push((c.u8()?, c.bytes()?, c.opt_bytes()?, c.u64()?));
    }
    c.finish()?;
    Ok((max_ts, entries))
}

#[cfg(test)]
pub(crate) mod tests {
    use proptest::prelude::*;

    use super::*;

    fn roundtrip(w: &KvWire) {
        let bytes = encode_wire(w);
        let back = decode_wire(&bytes).expect("decodes");
        // KvWire has no PartialEq (RaftMsg doesn't derive it); compare via the
        // debug form, which covers every field.
        assert_eq!(format!("{w:?}"), format!("{back:?}"));
    }

    /// A distinct [`HlcTimestamp`] fixture per test entry, so the round-trip
    /// proves the field is actually threaded through (not accidentally
    /// defaulted the same everywhere).
    fn ts(wall_ms: u64, logical: u32) -> HlcTimestamp {
        HlcTimestamp { wall_ms, logical }
    }

    /// One log entry per `KvCommand` variant (plus a membership-carrying
    /// entry). Shared by the round-trip test and the golden-fixture tests
    /// (`format_fixture_tests.rs`); changing it changes what the checked-in
    /// fixtures are expected to decode to, so it is append-only.
    pub(crate) fn sample_entries() -> Vec<LogEntry<KvCommand>> {
        vec![
            LogEntry {
                term: 3,
                index: 17,
                command: KvCommand::Put {
                    key: b"k".to_vec(),
                    value: vec![0, 255, 128],
                    ts: ts(1, 0),
                },
                config: None,
                learners: None,
            },
            LogEntry {
                term: 3,
                index: 18,
                command: KvCommand::Batch {
                    puts: vec![
                        (b"a".to_vec(), b"1".to_vec()),
                        (Vec::new(), Vec::new()), // empty key/value survive
                    ],
                    ts: ts(2, 5),
                },
                config: Some([1, 2, 3].into_iter().map(nid).collect()),
                learners: Some([9].into_iter().map(nid).collect()),
            },
            // `KindBatch` (its own `conditions` OCC seatbelt deleted in ADR
            // 0054 step 4b): exercises a tombstone write alongside a
            // change-log record, so the round trip still proves every
            // remaining field.
            LogEntry {
                term: 3,
                index: 18,
                command: KvCommand::KindBatch {
                    writes: vec![
                        (crate::KIND_BASE, b"base-key".to_vec(), Some(b"v".to_vec())),
                        (crate::KIND_LSI, b"lsi-key".to_vec(), None), // a tombstone
                    ],
                    change_log: vec![(b"change-prefix".to_vec(), b"record".to_vec())],
                    ts: ts(2, 6),
                },
                config: None,
                learners: None,
            },
            LogEntry {
                term: 4,
                index: 19,
                command: KvCommand::Cas {
                    key: b"c".to_vec(),
                    expected: None,
                    value: b"v".to_vec(),
                    ts: ts(3, 0),
                },
                config: None,
                learners: None,
            },
            LogEntry {
                term: 4,
                index: 21,
                // ADR 0050 rung 4: a split-build seed chunk —
                // a value row, a tombstone row, distinct kinds, carried
                // versions.
                command: KvCommand::SeedBatch {
                    rows: vec![
                        (0, b"seed-base".to_vec(), Some(b"raw-bytes".to_vec()), 42),
                        (1, b"seed-lsi".to_vec(), None, 7),
                    ],
                    ts: ts(3, 1),
                },
                config: None,
                learners: None,
            },
            LogEntry {
                term: 4,
                index: 20,
                command: KvCommand::Cas {
                    key: b"c".to_vec(),
                    expected: Some(b"old".to_vec()),
                    value: b"new".to_vec(),
                    ts: ts(4, 1),
                },
                config: None,
                learners: None,
            },
            LogEntry {
                term: 4,
                index: 21,
                command: KvCommand::Delete {
                    key: b"d".to_vec(),
                    ts: ts(5, 0),
                },
                config: None,
                learners: None,
            },
            LogEntry {
                term: 6,
                index: 23,
                command: KvCommand::ReadCeiling { ts: ts(7, 0) },
                config: None,
                learners: None,
            },
            LogEntry {
                term: 7,
                index: 24,
                command: KvCommand::TxnStage {
                    txn_id: TxnId {
                        ts: ts(8, 0),
                        node: nid(3),
                    },
                    record_key: b"record".to_vec(),
                    record_table: "orders".to_string(),
                    is_anchor: true,
                    writes: vec![
                        TxnWrite {
                            key: b"k1".to_vec(),
                            value: Some(b"v1".to_vec()),
                            // ADR 0046 A1: a kind-write payload + change-log
                            // record staged alongside the base write —
                            // exercises the derived-write wire shape.
                            kind_writes: vec![(1u8, b"k1-lsi".to_vec(), Some(b"lsi-row".to_vec()))],
                            change_log: Some((b"k1-change-prefix".to_vec(), b"record".to_vec())),
                            // The ADR 0049 §3 stage marker.
                            stage_marker: Some((
                                b"k1-change-prefix".to_vec(),
                                b"stage-marker".to_vec(),
                            )),
                            // ADR 0054 step 4a: no apply-time
                            // evaluation for this write — the sibling write
                            // just below exercises the `Some` case.
                            pending: None,
                        },
                        // ADR 0054 step 4a: a write awaiting
                        // apply-time evaluation — exercises every
                        // `PendingTxnWrite` field (the identical
                        // `serde_json`-blob types `KindEval` above already
                        // exercises, now nested one level deeper inside the
                        // `Option` the JSON blob covers).
                        TxnWrite::pending_eval(
                            b"k2".to_vec(),
                            None,
                            crate::PendingTxnWrite {
                                schema: animus_item::WriteSchema {
                                    key: animus_item::TableSchema::simple("pk"),
                                    lsis: Vec::new(),
                                    change_records_carry_images: false,
                                },
                                pk: animus_item::AttributeValue::S("bob".to_owned()),
                                sk: None,
                                op: crate::KindEvalOp::Delete,
                                condition: Some(animus_item::ConditionExpression::AttributeExists(
                                    "pk".to_owned(),
                                )),
                                ttl_expired: false,
                            },
                        ),
                    ],
                    spans: vec![(
                        "orders".to_string(),
                        KeyRange::new(b"k1".to_vec(), Some(b"k1\x00".to_vec())),
                    )],
                    conditions: vec![
                        (b"k1".to_vec(), Some(b"expected1".to_vec())),
                        (b"k2".to_vec(), None), // must be absent
                    ],
                    ts: ts(8, 1),
                },
                config: None,
                learners: None,
            },
            LogEntry {
                term: 7,
                index: 25,
                command: KvCommand::TxnCommit {
                    txn_id: TxnId {
                        ts: ts(8, 0),
                        node: nid(3),
                    },
                    record_key: b"record".to_vec(),
                    ts: ts(9, 0),
                },
                config: None,
                learners: None,
            },
            LogEntry {
                term: 7,
                index: 26,
                command: KvCommand::TxnAbort {
                    txn_id: TxnId {
                        ts: ts(8, 0),
                        node: nid(3),
                    },
                    record_key: b"record".to_vec(),
                    ts: ts(9, 1),
                    orphan_created_ts: None,
                },
                config: None,
                learners: None,
            },
            // ADR 0018 §2/PR5's orphan-record fix: the `Some` branch of
            // `orphan_created_ts` (a recovery pusher synthesizing an
            // abort tombstone for a `txn_id` with no record at all).
            LogEntry {
                term: 7,
                index: 26,
                command: KvCommand::TxnAbort {
                    txn_id: TxnId {
                        ts: ts(8, 0),
                        node: nid(3),
                    },
                    record_key: b"record".to_vec(),
                    ts: ts(9, 1),
                    orphan_created_ts: Some(ts(7, 5)),
                },
                config: None,
                learners: None,
            },
            LogEntry {
                term: 7,
                index: 27,
                command: KvCommand::TxnResolve {
                    txn_id: TxnId {
                        ts: ts(8, 0),
                        node: nid(3),
                    },
                    record_key: b"record".to_vec(),
                    keys: vec![b"k1".to_vec(), b"k2".to_vec()],
                    outcome: crate::txn::TxnOutcome::Committed {
                        commit_ts: ts(9, 0),
                    },
                    ts: ts(9, 2),
                },
                config: None,
                learners: None,
            },
            // ADR 0058 Train 2 rung 3: the in-place split fork
            // — a split key plus two children, each with its own replica
            // set (exercises the in-place fork wire shape).
            LogEntry {
                term: 8,
                index: 29,
                command: KvCommand::SplitTablet {
                    split_key: b"m".to_vec(),
                    children: [
                        SplitChild {
                            id: TabletId(2),
                            replicas: vec![nid(1), nid(2), nid(3)],
                        },
                        SplitChild {
                            id: TabletId(3),
                            replicas: vec![nid(4), nid(5)],
                        },
                    ],
                    ts: ts(10, 0),
                },
                config: None,
                learners: None,
            },
            // ADR 0054 step 2: the self-contained evaluated
            // write — exercises every one of its four `serde_json`-blob
            // fields (`schema`/`pk`/`sk`/`op`/`condition`) at once.
            LogEntry {
                term: 8,
                index: 30,
                command: KvCommand::KindEval {
                    schema: animus_item::WriteSchema {
                        key: animus_item::TableSchema::composite("pk", "sk"),
                        lsis: vec![animus_item::LsiDef {
                            name: "byAge".to_owned(),
                            sort_attribute: "age".to_owned(),
                            projection: animus_item::Projection::KeysOnly,
                        }],
                        change_records_carry_images: true,
                    },
                    pk: animus_item::AttributeValue::S("alice".to_owned()),
                    sk: Some(animus_item::AttributeValue::N("42".to_owned())),
                    op: crate::KindEvalOp::Update {
                        key_item: [(
                            "pk".to_owned(),
                            animus_item::AttributeValue::S("alice".to_owned()),
                        )]
                        .into_iter()
                        .collect(),
                        actions: vec![animus_item::UpdateAction::Remove(vec![
                            animus_item::PathSegment::Field("stale".to_owned()),
                        ])],
                    },
                    condition: Some(animus_item::ConditionExpression::AttributeExists(
                        "pk".to_owned(),
                    )),
                    ttl_expired: true,
                    ts: ts(11, 0),
                },
                config: None,
                learners: None,
            },
            // Issue #996 layer 1: the batched sibling — two
            // independent entries in one `KindEvalBatch`, each exercising
            // its own `put_json`-blob fields, plus one whose `condition`
            // is `None` (the `Put` case, no update actions) to cover both
            // shapes in one roundtrip.
            LogEntry {
                term: 8,
                index: 31,
                command: KvCommand::KindEvalBatch {
                    entries: vec![
                        crate::KindEvalEntry {
                            schema: animus_item::WriteSchema {
                                key: animus_item::TableSchema::simple("pk"),
                                lsis: Vec::new(),
                                change_records_carry_images: false,
                            },
                            pk: animus_item::AttributeValue::S("bob".to_owned()),
                            sk: None,
                            op: crate::KindEvalOp::Put(
                                [(
                                    "pk".to_owned(),
                                    animus_item::AttributeValue::S("bob".to_owned()),
                                )]
                                .into_iter()
                                .collect(),
                            ),
                            condition: None,
                            ttl_expired: false,
                        },
                        crate::KindEvalEntry {
                            schema: animus_item::WriteSchema {
                                key: animus_item::TableSchema::composite("pk", "sk"),
                                lsis: vec![animus_item::LsiDef {
                                    name: "byAge".to_owned(),
                                    sort_attribute: "age".to_owned(),
                                    projection: animus_item::Projection::All,
                                }],
                                change_records_carry_images: true,
                            },
                            pk: animus_item::AttributeValue::S("carol".to_owned()),
                            sk: Some(animus_item::AttributeValue::N("7".to_owned())),
                            op: crate::KindEvalOp::Delete,
                            condition: Some(animus_item::ConditionExpression::AttributeExists(
                                "pk".to_owned(),
                            )),
                            ttl_expired: true,
                        },
                    ],
                    ts: ts(12, 0),
                },
                config: None,
                learners: None,
            },
            LogEntry {
                term: 6,
                index: 28,
                command: KvCommand::NoOp,
                config: None,
                learners: None,
            },
            LogEntry {
                term: 7,
                index: 29,
                command: KvCommand::Freeze { ts: ts(9, 3) },
                config: Some([1, 2, 3].into_iter().map(nid).collect()),
                learners: Some([4].into_iter().map(nid).collect()),
            },
        ]
    }

    /// One `RaftMsg` per variant, the `AppendEntries` carrying
    /// [`sample_entries`].
    pub(crate) fn sample_msgs() -> Vec<RaftMsg<KvCommand>> {
        let entries = sample_entries();
        vec![
            RaftMsg::PreVote {
                term: 7,
                candidate: nid(2),
                last_log_index: 9,
                last_log_term: 6,
            },
            RaftMsg::PreVoteResp {
                term: 7,
                granted: true,
            },
            RaftMsg::RequestVote {
                term: 7,
                candidate: nid(2),
                last_log_index: 9,
                last_log_term: 6,
            },
            RaftMsg::RequestVoteResp {
                term: 7,
                granted: false,
            },
            RaftMsg::AppendEntries {
                term: 7,
                leader: nid(2),
                prev_log_index: 16,
                prev_log_term: 3,
                entries,
                leader_commit: 15,
            },
            RaftMsg::AppendEntriesResp {
                term: 7,
                success: true,
                match_index: 23,
                needs_snapshot: true,
            },
            RaftMsg::InstallSnapshot {
                term: 7,
                leader: nid(2),
                last_index: 16,
                last_term: 3,
                offset: 1024,
                data: vec![9; 300],
                total: 4096,
                done: false,
                config: Some([2, 4].into_iter().map(nid).collect()),
                learners: Some([5].into_iter().map(nid).collect()),
            },
            RaftMsg::InstallSnapshotResp {
                term: 7,
                last_index: 0,
                next_offset: 2048,
            },
            RaftMsg::Heartbeat { node: nid(11) },
            RaftMsg::TimeoutNow { term: 7 },
            RaftMsg::Quiesce {
                term: 7,
                commit_index: 23,
            },
            RaftMsg::WakeRequest { term: 7 },
            RaftMsg::Removed {
                term: 7,
                removal_index: 19,
                removal_term: 5,
                config: [2, 4].into_iter().map(nid).collect(),
                learners: [5].into_iter().map(nid).collect(),
            },
            RaftMsg::RemovedAck {
                term: 7,
                removal_index: 19,
            },
        ]
    }

    /// The `HeartbeatBatch` samples: a two-message batch and the empty batch.
    pub(crate) fn sample_heartbeat_batches() -> Vec<Vec<(u64, RaftMsg<KvCommand>)>> {
        vec![
            vec![
                (
                    7u64,
                    RaftMsg::AppendEntries {
                        term: 3,
                        leader: nid(0),
                        prev_log_index: 10,
                        prev_log_term: 2,
                        entries: Vec::new(),
                        leader_commit: 10,
                    },
                ),
                (
                    12u64,
                    RaftMsg::AppendEntries {
                        term: 5,
                        leader: nid(0),
                        prev_log_index: 4,
                        prev_log_term: 1,
                        entries: Vec::new(),
                        leader_commit: 4,
                    },
                ),
            ],
            Vec::new(),
        ]
    }

    /// Every `KvWire` variant: each `RaftMsg` variant, both probes, and the
    /// heartbeat batches.
    pub(crate) fn sample_wires() -> Vec<KvWire> {
        let mut out: Vec<KvWire> = sample_msgs().into_iter().map(KvWire::Raft).collect();
        out.push(KvWire::ReadProbe { term: 7, epoch: 42 });
        out.push(KvWire::ReadProbeAck { term: 7, epoch: 42 });
        out.extend(
            sample_heartbeat_batches()
                .into_iter()
                .map(KvWire::HeartbeatBatch),
        );
        out
    }

    #[test]
    fn every_wire_variant_round_trips() {
        for w in sample_wires() {
            roundtrip(&w);
        }
    }

    /// ADR 0044 phase 2 (C-02 PR 2): a batched-heartbeat frame round-trips,
    /// including the empty-batch edge case (never sent in practice — the
    /// flush loop skips an empty buffer — but the decoder must not choke on
    /// one either).
    #[test]
    fn heartbeat_batch_round_trips() {
        for batch in sample_heartbeat_batches() {
            roundtrip(&KvWire::HeartbeatBatch(batch));
        }
    }

    #[test]
    fn image_round_trips_including_tombstones() {
        let entries: Vec<ImageEntry> = vec![
            (crate::KIND_BASE, b"a".to_vec(), Some(vec![0, 1, 255]), 3),
            (crate::KIND_BASE, b"b".to_vec(), None, 9), // tombstone
            (crate::KIND_LSI, b"a".to_vec(), Some(vec![7]), 4),
            (crate::KIND_CHANGE, b"a".to_vec(), Some(vec![8]), 5),
            (crate::KIND_FOOTPRINT, Vec::new(), Some(Vec::new()), 0),
        ];
        let bytes = encode_image(&entries, None);
        assert_eq!(decode_image(&bytes).expect("decodes"), (None, entries));
    }

    /// Issue #804: the `max_ts` header round-trips distinctly from `None`,
    /// proving the field is actually threaded through the wire bytes rather
    /// than defaulted.
    #[test]
    fn image_max_ts_header_round_trips() {
        let entries: Vec<ImageEntry> = vec![(crate::KIND_BASE, b"a".to_vec(), Some(vec![1]), 3)];
        let max_ts = HlcTimestamp {
            wall_ms: 4300,
            logical: 99,
        };
        let bytes = encode_image(&entries, Some(max_ts));
        assert_eq!(
            decode_image(&bytes).expect("decodes"),
            (Some(max_ts), entries)
        );
    }

    fn malformed_detail(err: FormatError) -> String {
        match err {
            FormatError::Malformed { detail, .. } => detail,
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn version_is_the_phase_0_baseline() {
        assert_eq!(VERSION, 1, "ADR 0073 Phase 0 baseline");
    }

    #[test]
    fn decode_failures_are_loud_and_named() {
        let pre = |format| FormatError::PreBaselineFormat { format };
        let unsupported = |format, found| FormatError::UnsupportedFormatVersion {
            format,
            found,
            max_supported: VERSION,
        };

        // Empty input and a foreign payload (JSON, the pre-binary encoding)
        // are pre-baseline, never a confusing tag error deep inside.
        assert_eq!(decode_wire(&[]).unwrap_err(), pre("raftkv-wire"));
        assert_eq!(decode_image(&[]).unwrap_err(), pre("raftkv-image"));
        assert_eq!(
            decode_wire(b"{\"Raft\":{}}").unwrap_err(),
            pre("raftkv-wire")
        );
        assert_eq!(decode_image(b"[]").unwrap_err(), pre("raftkv-image"));
        // Wrong magic even with a valid-looking version.
        assert_eq!(
            decode_wire(&[MAGIC ^ 1, VERSION, 1]).unwrap_err(),
            pre("raftkv-wire")
        );

        // Version 0, one past this build, and 255 are unsupported, on both
        // entry points (32 is the pre-baseline codec's last version).
        for bad in [0u8, VERSION + 1, 32, 255] {
            assert_eq!(
                decode_wire(&[MAGIC, bad, 1]).unwrap_err(),
                unsupported("raftkv-wire", bad)
            );
            assert_eq!(
                decode_image(&[MAGIC, bad, 0, 0, 0, 0, 0]).unwrap_err(),
                unsupported("raftkv-image", bad)
            );
        }
        let msg = decode_wire(&[MAGIC, VERSION + 1, 1])
            .unwrap_err()
            .to_string();
        assert!(msg.contains("unsupported format version"), "got: {msg}");

        // A magic byte alone has no version byte: framing damage.
        assert!(malformed_detail(decode_wire(&[MAGIC]).unwrap_err()).contains("truncated"));

        // Truncated frame.
        let good = encode_wire(&KvWire::ReadProbe { term: 1, epoch: 2 });
        let err = decode_wire(&good[..good.len() - 1]).unwrap_err();
        assert!(malformed_detail(err).contains("truncated"));

        // Trailing garbage is rejected (a frame must be exactly one message).
        let mut padded = good.clone();
        padded.push(0);
        let err = decode_wire(&padded).unwrap_err();
        assert!(malformed_detail(err).contains("trailing"));

        // Unknown enum tag.
        let err = decode_wire(&[MAGIC, VERSION, 9]).unwrap_err();
        assert!(malformed_detail(err).contains("unknown KvWire tag"));
    }

    /// Every strict prefix of a valid frame is a clean `Err` (never a
    /// panic, never a partial decode), for a frame carrying every command
    /// variant and for an image.
    #[test]
    fn every_truncation_of_a_valid_frame_is_an_err() {
        for w in sample_wires() {
            let bytes = encode_wire(&w);
            for cut in 0..bytes.len() {
                let err = decode_wire(&bytes[..cut]).expect_err("prefix must not decode");
                if cut == 0 {
                    assert!(matches!(err, FormatError::PreBaselineFormat { .. }));
                } else {
                    assert!(
                        matches!(err, FormatError::Malformed { .. }),
                        "cut {cut}: {err:?}"
                    );
                }
            }
        }
        let image = encode_image(
            &[(crate::KIND_BASE, b"k".to_vec(), Some(vec![1, 2]), 3)],
            Some(ts(5, 1)),
        );
        for cut in 0..image.len() {
            let err = decode_image(&image[..cut]).expect_err("prefix must not decode");
            match (cut, &err) {
                (0, FormatError::PreBaselineFormat { .. }) => {}
                (c, FormatError::Malformed { .. }) if c > 0 => {}
                _ => panic!("cut {cut}: {err:?}"),
            }
        }
    }

    /// Regression for the process-abort DoS this module's `with_capacity`
    /// fix closes: a corrupted `AppendEntries` entry-count field pushed to
    /// just under `u32::MAX`, with none of the (nonexistent) declared
    /// entries actually present in the buffer. Before the fix,
    /// `read_raft`'s `Vec::with_capacity(n as usize)` would request an
    /// allocation of ~`n * size_of::<LogEntry<KvCommand>>()` bytes —
    /// hundreds of GB — which Rust's global allocator handles by aborting
    /// the whole process (`handle_alloc_error`, not a catchable panic).
    /// This exact shape (`read_raft`'s entry-count field) was reproduced
    /// live via `cargo test -p animus-test --test raftkv_linearizable`
    /// before the fix; now it must return a graceful `Err`.
    #[test]
    fn corrupted_append_entries_count_returns_a_graceful_error_not_an_alloc_abort() {
        let mut bytes = vec![
            MAGIC, VERSION, 0, /* KvWire::Raft */
            4, /* RaftMsg::AppendEntries */
        ];
        bytes.extend_from_slice(&7u64.to_be_bytes()); // term
        put_node_id(&mut bytes, &nid(1)); // leader
        bytes.extend_from_slice(&16u64.to_be_bytes()); // prev_log_index
        bytes.extend_from_slice(&3u64.to_be_bytes()); // prev_log_term
        bytes.extend_from_slice(&(u32::MAX - 1).to_be_bytes()); // corrupted entry count
        // No entry bytes follow at all — the declared count vastly exceeds
        // what the buffer actually holds.
        let err = decode_wire(&bytes).unwrap_err();
        assert!(malformed_detail(err).contains("truncated"));
    }

    proptest! {
        /// Fuzz `decode_wire`/`decode_image` over arbitrary byte sequences.
        /// The only contract that matters here: every input decodes to
        /// `Ok` or `Err`, and nothing panics or aborts the process.
        /// `proptest` turns a panic into a shrunk, reported failing case;
        /// an allocator abort would instead kill the whole test binary —
        /// exactly the failure mode this guards against, so a green run of
        /// this test is itself part of the regression proof.
        #[test]
        fn decode_wire_never_panics_or_aborts_on_arbitrary_bytes(
            bytes in proptest::collection::vec(any::<u8>(), 0..512)
        ) {
            let _ = decode_wire(&bytes);
        }

        #[test]
        fn decode_image_never_panics_or_aborts_on_arbitrary_bytes(
            bytes in proptest::collection::vec(any::<u8>(), 0..512)
        ) {
            let _ = decode_image(&bytes);
        }

        /// A sharper-targeted fuzz than pure random bytes: a syntactically
        /// valid frame prefix through `AppendEntries`' own entry-count
        /// field, that field forced into the "would have demanded a
        /// many-GB pre-fix allocation" range, followed by a short,
        /// always-insufficient tail — the exact untrusted-length-prefix
        /// shape the process-abort bug lived in, swept over a wide range
        /// of counts and trailing-byte shapes rather than one fixed case.
        #[test]
        fn decode_wire_never_panics_or_aborts_with_a_huge_declared_entry_count(
            n in 1_000_000u32..=u32::MAX,
            tail in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            let mut bytes = vec![MAGIC, VERSION, 0 /* KvWire::Raft */, 4 /* AppendEntries */];
            bytes.extend_from_slice(&7u64.to_be_bytes()); // term
            put_node_id(&mut bytes, &nid(1)); // leader
            bytes.extend_from_slice(&16u64.to_be_bytes()); // prev_log_index
            bytes.extend_from_slice(&3u64.to_be_bytes()); // prev_log_term
            bytes.extend_from_slice(&n.to_be_bytes()); // declared entry count
            bytes.extend_from_slice(&tail); // never enough bytes for `n` real entries
            let result = decode_wire(&bytes);
            prop_assert!(
                result.is_err(),
                "a huge declared entry count with insufficient trailing bytes must fail gracefully"
            );
        }
    }

    #[test]
    fn binary_framing_is_much_smaller_than_json_for_byte_payloads() {
        // The motivating case (audit P2): serde_json renders Vec<u8> as a
        // decimal array (~3-4x). Guard the win so a codec regression is caught.
        let value = vec![200u8; 1024];
        let wire = KvWire::Raft(RaftMsg::AppendEntries {
            term: 1,
            leader: nid(0),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: 1,
                index: 1,
                command: KvCommand::Put {
                    key: b"key".to_vec(),
                    value: value.clone(),
                    ts: ts(1, 0),
                },
                config: None,
                learners: None,
            }],
            leader_commit: 0,
        });
        let binary = encode_wire(&wire).len();
        // What the old encoding paid for the same message.
        let json = serde_json::to_vec(&serde_json::json!({
            "Raft": {"AppendEntries": {
                "term": 1, "leader": 0, "prev_log_index": 0, "prev_log_term": 0,
                "entries": [{"term": 1, "index": 1,
                             "command": {"Put": {"key": b"key".to_vec(), "value": value}},
                             "config": null}],
                "leader_commit": 0,
            }}
        }))
        .expect("json")
        .len();
        assert!(
            binary * 3 < json,
            "binary frame ({binary}B) should be well under a third of JSON ({json}B)"
        );
    }
}
