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
//! Scope: **wire + snapshot image only.** The Raft WAL keeps the shared
//! `PersistedState` (serde_json) encoding — it is `animus-control`'s format,
//! common to both planes.
//!
//! Pre-alpha: no cross-version wire/disk compatibility is required (mixed-codec
//! clusters are not supported), but decode failures stay **loud**: every
//! malformed input yields a descriptive `Err` that the driver logs
//! (`tracing::warn!`) before dropping the message — never a silent
//! misinterpretation (the magic/version check rejects a stray JSON payload
//! outright).
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
/// Codec version, bumped on any incompatible layout change. `2`: `KvCommand`'s
/// `Put`/`Batch`/`Delete`/`Cas` variants gained a `fence: KeyRange` field. `3`:
/// `KvCommand::Split` (tag 4) is gone — split is now a single control-plane
/// command, never a data-plane one (ADR 0028). `4`: `RaftMsg::TimeoutNow` (tag
/// 9, ADR 0029 leadership transfer) added. `5` (ADR 0018 §2/PR2): every
/// mutating `KvCommand` variant gained a `ts: HlcTimestamp` field, and a new
/// `KvCommand::Seal` variant (tag 6) was added — pre-alpha, no cross-version
/// wire/disk compatibility is required (no live deployments), so a mixed-
/// version decode fails loudly on the version check below rather than
/// silently misreading the new field. `6` (ADR 0018 §2/PR2b):
/// `KvCommand::ReadCeiling` (tag 7) was added. `7` (ADR 0018 §2/PR3):
/// `KvCommand::TxnStage`/`TxnCommit`/`TxnAbort`/`TxnResolve` (tags 8-11)
/// were added — pre-alpha, no cross-version compatibility required, so
/// again a mixed-version decode fails loudly rather than silently
/// misreading the new variants.
/// `8` (ADR 0018 §2/PR4): `TxnStage` gained `record_table: String`/
/// `is_anchor: bool` (multi-participant staging — see `KvCommand::TxnStage`'s
/// doc); `TxnResolve` gained `outcome: TxnOutcome` (the decision travels
/// explicitly instead of being re-derived from a local record) — again a
/// clean version bump, no wire/disk back-compat required.
/// `9` (ADR 0018 §2/PR5): `TxnStage.spans` changed from `Vec<KeyRange>` to
/// `Vec<(String, KeyRange)>` — every span now carries its own table name,
/// closing a real gap PR3/PR4 left open (see `txn::TxnRecord::intent_spans`'s
/// doc for the full account). Same house convention: a clean bump, no
/// cross-version compatibility.
/// `10` (ADR 0018 §2/PR5, orphan-record fix): `TxnAbort` gained
/// `orphan_created_ts: Option<HlcTimestamp>` — a recovery pusher that finds
/// no record at all synthesizes one directly in the `Aborted` state (see
/// `KvCommand::TxnAbort`'s doc). Same house convention.
/// `11` (ADR 0018 §2 apply-time write-key conditions amendment):
/// `TxnStage` gained `conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>` —
/// own-key byte-level OCC preconditions checked at apply (see
/// `KvCommand::TxnStage`'s doc). `12` (ADR 0041 §3): every snapshot
/// `ImageEntry` gained a leading **row-kind** byte, so one image carries every
/// one of a tablet's per-kind storage scopes. Same house convention: a clean
/// bump, no cross-version compatibility.
/// `13` (ADR 0042/0043): `ALL_KINDS` grows to admit the DynamoDB Streams row
/// kinds — `KIND_CURSOR` (`0x04`, this PR) now, `KIND_STREAM`/
/// `KIND_STREAM_META` (`0x05`/`0x06`) in a later PR. The `ImageEntry` layout
/// itself is unchanged (the kind byte was already a generic `u8` since `12`,
/// and an unknown kind is already dropped-with-warn on decode); this bump is
/// the same house convention as every prior one — a version marker for a
/// meaningful semantic change, not a wire-format one — and is deliberately
/// **one bump covering all three new kinds** across the whole Streams PR
/// stack, so the two later kinds land without a further bump.
/// `14` (ADR 0018 §2 write-loss amendment — Bug 3): `TxnResolve` gained a
/// `fence: KeyRange` field, closing the one key-writing `KvCommand` variant
/// that used to carry no apply-time fence check at all — see
/// `KvCommand::TxnResolve`'s doc. Same house convention: a clean bump, no
/// cross-version compatibility.
/// `15` (ADR 0046 "evaluate at leader" seatbelt, PR1): `KindBatch` gained a
/// `conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>` field — own-key byte-level
/// OCC preconditions checked at apply, modeled on `TxnStage`'s own
/// `conditions` field added in version `11` (see `KvCommand::KindBatch`'s
/// doc). Same house convention: a clean bump, no cross-version
/// compatibility.
/// `16` (ADR 0046 "materialize-at-resolve", `TxnStage` kind-writes stack
/// PR1): `TxnStage.writes`' element changed from the bare `(Vec<u8>,
/// Option<Vec<u8>>)` tuple to the named `txn::TxnWrite` struct, which adds
/// two fields per write — `kind_writes: Vec<crate::KindWrite>`
/// and `change_log: Option<(Vec<u8>, Vec<u8>)>` — the derived kind-scope
/// payload a transactional write against an indexed/streamed table stages
/// alongside its base value (see `TxnWrite`'s doc). Same house convention:
/// a clean bump, no cross-version compatibility.
/// `17` (ADR 0049 Train A rung-1 fixup): `KindBatch.change_log` changed
/// from `Option<(Vec<u8>, Vec<u8>)>` to `Vec<(Vec<u8>, Vec<u8>)>` — a
/// marker-table batch commits one entry per tablet carrying every item's
/// marker record (the entry-granularity throughput contract; see the
/// field's own doc). `TxnWrite.change_log` keeps its `Option` shape.
/// `18` (ADR 0049 §3, Train A rung 3): `TxnWrite` gained `stage_marker:
/// Option<(Vec<u8>, Vec<u8>)>` — the image-less stage-marker record
/// `TxnStage`'s apply arm materializes at the stage entry's own `ts` (see
/// the field's own doc). Encoded with the same tagged-`Option` shape
/// `change_log` uses (`put_change_log`/`read_change_log` — never a second
/// copy). Same house convention: a clean bump, no cross-version
/// compatibility.
/// `19` (ADR 0050 Train B rung 4): new `KvCommand::SeedBatch` (tag 13) — the
/// split-build driver's version-carrying row-transfer command (see the
/// variant's own doc). Rows are `(kind, logical, Option<value>, version)`
/// with the standard `fence`/`ts` tail.
/// `20` (ADR 0050 Train B rung 5): new `KvCommand::Freeze` (tag 14) — the
/// split-cutover freeze, a bare `ts` (no fence, no keys; see the variant's
/// own doc). Same house convention: a clean bump, no cross-version
/// compatibility.
/// `21` (ADR 0050 Train B rung 7, the deletion sweep): the `fence: KeyRange`
/// field is **deleted from every variant that carried it** — with immutable
/// tablet ranges (rung 2) and route-time `Active` filtering (rung 3), a
/// stamped fence and the group's own range could never again disagree, so
/// the field was pure inert bytes on every entry. `KvCommand::Seal` (tag 6)
/// is deleted with its last proposer (the reconciler's zero-copy handoff
/// seal); its durable-marker core lives on as `Freeze`'s own marker (see
/// `seal.rs`). Same house convention: a clean bump.
/// `22` (ADR 0058 Train 1): `LogEntry` gained a `learners: Option<BTreeSet<NodeId>>`
/// field (the non-voting membership class's config-in-log counterpart to
/// `config`) and `RaftMsg::InstallSnapshot` gained the identical field —
/// both encoded with the same `put_opt_node_set`/`opt_node_set` helper
/// `config` already uses. Same house convention: a clean bump, no
/// cross-version compatibility.
/// `23` (ADR 0058 Train 2 rung 3): new `KvCommand::SplitTablet` (tag 15) —
/// the in-place split's single-entry atomic fork (see the variant's own
/// doc): a `split_key: Vec<u8>` plus two `animus_tablet::SplitChild`
/// `(id, replicas)` pairs and the standard trailing `ts`. Same house
/// convention: a clean bump, no cross-version compatibility.
/// `24` (issue #554): `RaftMsg::AppendEntriesResp` gained a `needs_snapshot:
/// bool` field (the follower-to-leader "my state machine is behind its own
/// log's compacted start" signal — see `animus_control::raft::RaftCore::
/// state_machine_behind`'s doc), encoded with `put_bool`/`c.bool()!` right
/// after `match_index`, matching `success`'s own encoding. Same house
/// convention: a clean bump, no cross-version compatibility (the `serde`
/// side's own `#[serde(default)]` is unrelated — it only protects the WAL's
/// `serde_json` path, per this crate's own doc: "a field added to the
/// shared `LogEntry`/`RaftMsg` types needs an explicit encode/decode arm
/// here too").
/// `25` (ADR 0054 step 2): new `KvCommand::KindEval` (tag 16) — the
/// self-contained evaluated write apply evaluates in commit order (see the
/// variant's own doc). Its rich, evolving nested types (`WriteSchema`,
/// `AttributeValue`/`Option<AttributeValue>`, `KindEvalOp`,
/// `Option<ConditionExpression>`) are each `serde_json`-encoded into one
/// `put_bytes`-framed blob apiece rather than hand-encoded field-by-field —
/// the same "JSON inside the binary envelope" convention `backup.rs`'s
/// `BackupManifestObject` already uses for `TableSchema`'s own
/// multi-field, evolving shape, for the identical reason: this is a
/// low-frequency, deeply-nested payload (unlike the hot per-key
/// `Vec<u8>`s every other variant's fields already are), so a field added
/// to any of these four types needs no codec change here at all. `ts`
/// stays the standard trailing fixed-width encoding. Same house
/// convention otherwise: a clean bump, no cross-version compatibility.
/// `26` (ADR 0054 step 4a): `TxnStage.writes`' element (`txn::TxnWrite`)
/// gained `pending: Option<txn::PendingTxnWrite>` — a write awaiting
/// apply-time evaluation (see that field's own doc). Encoded as one
/// `put_json`-framed blob covering the whole `Option` (the same "JSON
/// inside the binary envelope" convention version `25` established,
/// since `PendingTxnWrite` nests the identical rich, evolving types
/// `KvCommand::KindEval` already JSON-encodes) — `serde_json` renders
/// `None` as `null` and `Some(..)` as the object, so one blob covers both
/// cases with no separate tag byte needed. Same house convention: a clean
/// bump, no cross-version compatibility.
/// `27` (ADR 0054 step 4b): `KvCommand::KindBatch` lost its own-key
/// `conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>` OCC seatbelt field (ADR
/// 0046 PR1) — every production caller passed an empty `Vec` once step 3/4a
/// moved every write producer onto apply-time evaluation, so the field
/// carried no live signal left to check. Removed from both the encode and
/// decode arms (tag `12`); `TxnStage`'s OWN separate `conditions` field
/// (introduced alongside it in version `11`, apply-time write-key
/// preconditions for a *transaction's* own-key writes) is untouched — the
/// two were always independent fields on different variants that happened
/// to share a name and a byte-level OCC shape, not one shared mechanism.
const VERSION: u8 = 27;

/// A decode failure: a description of what was malformed, surfaced loudly by
/// the caller (logged + dropped; never silently misread).
pub(crate) type DecodeError = String;

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

/// `serde_json`-encode `value` into one `put_bytes`-framed blob (version
/// `25` — see the const's own doc for why this crate's deeply-nested,
/// evolving `KvCommand::KindEval` field types use JSON-inside-the-envelope
/// rather than a hand-rolled field-by-field encoding).
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
/// own `writes` shape, and (ADR 0046 A1, version `16`) a `txn::TxnWrite`'s
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
/// `KvCommand::KindBatch`'s own `change_log` shape, and (ADR 0046 A1,
/// version `16`) a `txn::TxnWrite`'s `change_log` payload.
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

/// `KindBatch.change_log`'s multi-record shape (version `17` — see the
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
            // ADR 0046 A1, version 16: each write is a `txn::TxnWrite` —
            // base key/value plus an optional derived kind-scope payload,
            // encoded with the SAME `put_kind_writes`/`put_change_log`
            // helpers `KindBatch` itself uses (never a second copy).
            out.extend_from_slice(&(writes.len() as u32).to_be_bytes());
            for w in writes {
                put_bytes(out, &w.key);
                put_opt_bytes(out, &w.value);
                put_kind_writes(out, &w.kind_writes);
                put_change_log(out, &w.change_log);
                // Version 18: the stage marker shares change_log's own
                // tagged-Option `(prefix, record)` encoding.
                put_change_log(out, &w.stage_marker);
                // Version 26: `Option<txn::PendingTxnWrite>` as one JSON
                // blob (see the `VERSION` const's own doc).
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
    }
    out
}

/// Decode a binary frame into a [`KvWire`] message. Errors are descriptive and
/// the caller logs them loudly before dropping the message.
pub(crate) fn decode_wire(bytes: &[u8]) -> Result<KvWire, DecodeError> {
    let mut c = Cursor::new(bytes);
    let magic = c.u8()?;
    if magic != MAGIC {
        return Err(format!("bad magic byte {magic:#04x} (want {MAGIC:#04x})"));
    }
    let version = c.u8()?;
    if version != VERSION {
        return Err(format!("unsupported codec version {version}"));
    }
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
        other => return Err(format!("unknown KvWire tag {other}")),
    };
    c.finish()?;
    Ok(wire)
}

// ---- snapshot image --------------------------------------------------------

/// Encode the engine snapshot image (`(key, value-or-tombstone, version)`
/// entries) shipped in `InstallSnapshot` chunks.
pub(crate) fn encode_image(entries: &[ImageEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    put_u8(&mut out, MAGIC);
    put_u8(&mut out, VERSION);
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
pub(crate) fn decode_image(bytes: &[u8]) -> Result<Vec<ImageEntry>, DecodeError> {
    let mut c = Cursor::new(bytes);
    let magic = c.u8()?;
    if magic != MAGIC {
        return Err(format!("bad magic byte {magic:#04x} (want {MAGIC:#04x})"));
    }
    let version = c.u8()?;
    if version != VERSION {
        return Err(format!("unsupported codec version {version}"));
    }
    let n = c.u32()?;
    // Capped pre-allocation against an untrusted wire count — see
    // `read_kind_writes`'s comment for why.
    let mut entries = Vec::with_capacity(n.min(1 << 20) as usize);
    for _ in 0..n {
        entries.push((c.u8()?, c.bytes()?, c.opt_bytes()?, c.u64()?));
    }
    c.finish()?;
    Ok(entries)
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn every_wire_variant_round_trips() {
        let entries = vec![
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
                // ADR 0050 rung 4 (version 19): a split-build seed chunk —
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
                            // exercises the version-16 wire shape.
                            kind_writes: vec![(1u8, b"k1-lsi".to_vec(), Some(b"lsi-row".to_vec()))],
                            change_log: Some((b"k1-change-prefix".to_vec(), b"record".to_vec())),
                            // Version 18: the ADR 0049 §3 stage marker.
                            stage_marker: Some((
                                b"k1-change-prefix".to_vec(),
                                b"stage-marker".to_vec(),
                            )),
                            // Version 26 (ADR 0054 step 4a): no apply-time
                            // evaluation for this write — the sibling write
                            // just below exercises the `Some` case.
                            pending: None,
                        },
                        // Version 26 (ADR 0054 step 4a): a write awaiting
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
            // ADR 0058 Train 2 rung 3 (version 23): the in-place split fork
            // — a split key plus two children, each with its own replica
            // set (exercises the version-23 wire shape).
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
            // ADR 0054 step 2 (version 25): the self-contained evaluated
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
            LogEntry {
                term: 6,
                index: 28,
                command: KvCommand::NoOp,
                config: None,
                learners: None,
            },
        ];
        let msgs: Vec<RaftMsg<KvCommand>> = vec![
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
        ];
        for m in msgs {
            roundtrip(&KvWire::Raft(m));
        }
        roundtrip(&KvWire::ReadProbe { term: 7, epoch: 42 });
        roundtrip(&KvWire::ReadProbeAck { term: 7, epoch: 42 });
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
        let bytes = encode_image(&entries);
        assert_eq!(decode_image(&bytes).expect("decodes"), entries);
    }

    #[test]
    fn decode_failures_are_loud_and_descriptive() {
        // A JSON payload (the old encoding / a foreign message) fails the magic
        // check, not some confusing tag error deep inside.
        let err = decode_wire(b"{\"Raft\":{}}").unwrap_err();
        assert!(err.contains("bad magic"), "got: {err}");

        // Unknown version.
        let err = decode_wire(&[MAGIC, 99, 0]).unwrap_err();
        assert!(err.contains("version"), "got: {err}");

        // Truncated frame.
        let good = encode_wire(&KvWire::ReadProbe { term: 1, epoch: 2 });
        let err = decode_wire(&good[..good.len() - 1]).unwrap_err();
        assert!(err.contains("truncated"), "got: {err}");

        // Trailing garbage is rejected (a frame must be exactly one message).
        let mut padded = good.clone();
        padded.push(0);
        let err = decode_wire(&padded).unwrap_err();
        assert!(err.contains("trailing"), "got: {err}");

        // Unknown enum tag.
        let err = decode_wire(&[MAGIC, VERSION, 9]).unwrap_err();
        assert!(err.contains("unknown KvWire tag"), "got: {err}");

        // Image: same loud contract.
        let err = decode_image(b"[]").unwrap_err();
        assert!(err.contains("bad magic"), "got: {err}");
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
        assert!(err.contains("truncated"), "got: {err}");
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
