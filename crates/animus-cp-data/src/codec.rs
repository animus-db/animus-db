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

use std::collections::BTreeSet;

use animus_control::raft::{LogEntry, RaftMsg};
use animus_env::NodeId;
#[cfg(test)]
use animus_env::nid;
use animus_tablet::KeyRange;

use crate::hlc::HlcTimestamp;
use crate::txn::{TxnId, TxnOutcome};
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
const VERSION: u8 = 12;

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
/// error (mirrors the storage manifest codec's `Cursor`).
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

fn put_command(out: &mut Vec<u8>, c: &KvCommand) {
    match c {
        KvCommand::Put {
            key,
            value,
            fence,
            ts,
        } => {
            put_u8(out, 0);
            put_bytes(out, key);
            put_bytes(out, value);
            put_key_range(out, fence);
            put_ts(out, *ts);
        }
        KvCommand::Batch { puts, fence, ts } => {
            put_u8(out, 1);
            out.extend_from_slice(&(puts.len() as u32).to_be_bytes());
            for (k, v) in puts {
                put_bytes(out, k);
                put_bytes(out, v);
            }
            put_key_range(out, fence);
            put_ts(out, *ts);
        }
        KvCommand::Delete { key, fence, ts } => {
            put_u8(out, 2);
            put_bytes(out, key);
            put_key_range(out, fence);
            put_ts(out, *ts);
        }
        KvCommand::Cas {
            key,
            expected,
            value,
            fence,
            ts,
        } => {
            put_u8(out, 3);
            put_bytes(out, key);
            put_opt_bytes(out, expected);
            put_bytes(out, value);
            put_key_range(out, fence);
            put_ts(out, *ts);
        }
        KvCommand::NoOp => put_u8(out, 5),
        KvCommand::Seal { range, ts } => {
            put_u8(out, 6);
            put_key_range(out, range);
            put_ts(out, *ts);
        }
        KvCommand::ReadCeiling { ts } => {
            put_u8(out, 7);
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
            fence,
            ts,
        } => {
            put_u8(out, 8);
            put_txn_id(out, txn_id);
            put_bytes(out, record_key);
            put_bytes(out, record_table.as_bytes());
            put_bool(out, *is_anchor);
            out.extend_from_slice(&(writes.len() as u32).to_be_bytes());
            for (k, v) in writes {
                put_bytes(out, k);
                put_opt_bytes(out, v);
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
            put_key_range(out, fence);
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
            fence: read_key_range(c)?,
            ts: read_ts(c)?,
        },
        1 => {
            let n = c.u32()?;
            let mut puts = Vec::with_capacity(n as usize);
            for _ in 0..n {
                puts.push((c.bytes()?, c.bytes()?));
            }
            KvCommand::Batch {
                puts,
                fence: read_key_range(c)?,
                ts: read_ts(c)?,
            }
        }
        2 => KvCommand::Delete {
            key: c.bytes()?,
            fence: read_key_range(c)?,
            ts: read_ts(c)?,
        },
        3 => KvCommand::Cas {
            key: c.bytes()?,
            expected: c.opt_bytes()?,
            value: c.bytes()?,
            fence: read_key_range(c)?,
            ts: read_ts(c)?,
        },
        5 => KvCommand::NoOp,
        6 => KvCommand::Seal {
            range: read_key_range(c)?,
            ts: read_ts(c)?,
        },
        7 => KvCommand::ReadCeiling { ts: read_ts(c)? },
        8 => {
            let txn_id = read_txn_id(c)?;
            let record_key = c.bytes()?;
            let record_table = String::from_utf8(c.bytes()?)
                .map_err(|_| "TxnStage record_table not utf8".to_string())?;
            let is_anchor = c.bool()?;
            let n = c.u32()?;
            let mut writes = Vec::with_capacity(n as usize);
            for _ in 0..n {
                writes.push((c.bytes()?, c.opt_bytes()?));
            }
            let n = c.u32()?;
            let mut spans = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let table = String::from_utf8(c.bytes()?)
                    .map_err(|_| "TxnStage span table not utf8".to_string())?;
                spans.push((table, read_key_range(c)?));
            }
            let n = c.u32()?;
            let mut conditions = Vec::with_capacity(n as usize);
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
                fence: read_key_range(c)?,
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
            let mut keys = Vec::with_capacity(n as usize);
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
        other => return Err(format!("unknown KvCommand tag {other}")),
    })
}

// ---- LogEntry<KvCommand> -----------------------------------------------------

fn put_entry(out: &mut Vec<u8>, e: &LogEntry<KvCommand>) {
    put_u64(out, e.term);
    put_u64(out, e.index);
    put_command(out, &e.command);
    put_opt_node_set(out, &e.config);
}

fn read_entry(c: &mut Cursor<'_>) -> Result<LogEntry<KvCommand>, DecodeError> {
    Ok(LogEntry {
        term: c.u64()?,
        index: c.u64()?,
        command: read_command(c)?,
        config: c.opt_node_set()?,
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
        } => {
            put_u8(out, 5);
            put_u64(out, *term);
            put_bool(out, *success);
            put_u64(out, *match_index);
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
            let mut entries = Vec::with_capacity(n as usize);
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
        },
        7 => RaftMsg::InstallSnapshotResp {
            term: c.u64()?,
            last_index: c.u64()?,
            next_offset: c.u64()?,
        },
        8 => RaftMsg::Heartbeat { node: c.node_id()? },
        9 => RaftMsg::TimeoutNow { term: c.u64()? },
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
    let mut entries = Vec::with_capacity(n as usize);
    for _ in 0..n {
        entries.push((c.u8()?, c.bytes()?, c.opt_bytes()?, c.u64()?));
    }
    c.finish()?;
    Ok(entries)
}

#[cfg(test)]
mod tests {
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
                    fence: KeyRange::whole(),
                    ts: ts(1, 0),
                },
                config: None,
            },
            LogEntry {
                term: 3,
                index: 18,
                command: KvCommand::Batch {
                    puts: vec![
                        (b"a".to_vec(), b"1".to_vec()),
                        (Vec::new(), Vec::new()), // empty key/value survive
                    ],
                    fence: KeyRange::new(b"a".to_vec(), Some(b"z".to_vec())),
                    ts: ts(2, 5),
                },
                config: Some([1, 2, 3].into_iter().map(nid).collect()),
            },
            LogEntry {
                term: 4,
                index: 19,
                command: KvCommand::Cas {
                    key: b"c".to_vec(),
                    expected: None,
                    value: b"v".to_vec(),
                    fence: KeyRange::whole(),
                    ts: ts(3, 0),
                },
                config: None,
            },
            LogEntry {
                term: 4,
                index: 20,
                command: KvCommand::Cas {
                    key: b"c".to_vec(),
                    expected: Some(b"old".to_vec()),
                    value: b"new".to_vec(),
                    fence: KeyRange::new(b"a".to_vec(), None),
                    ts: ts(4, 1),
                },
                config: None,
            },
            LogEntry {
                term: 4,
                index: 21,
                command: KvCommand::Delete {
                    key: b"d".to_vec(),
                    fence: KeyRange::whole(),
                    ts: ts(5, 0),
                },
                config: None,
            },
            LogEntry {
                term: 5,
                index: 22,
                command: KvCommand::Seal {
                    range: KeyRange::new(b"m".to_vec(), Some(b"z".to_vec())),
                    ts: ts(6, 2),
                },
                config: None,
            },
            LogEntry {
                term: 6,
                index: 23,
                command: KvCommand::ReadCeiling { ts: ts(7, 0) },
                config: None,
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
                        (b"k1".to_vec(), Some(b"v1".to_vec())),
                        (b"k2".to_vec(), None), // a staged delete
                    ],
                    spans: vec![(
                        "orders".to_string(),
                        KeyRange::new(b"k1".to_vec(), Some(b"k1\x00".to_vec())),
                    )],
                    conditions: vec![
                        (b"k1".to_vec(), Some(b"expected1".to_vec())),
                        (b"k2".to_vec(), None), // must be absent
                    ],
                    fence: KeyRange::whole(),
                    ts: ts(8, 1),
                },
                config: None,
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
            },
            LogEntry {
                term: 6,
                index: 28,
                command: KvCommand::NoOp,
                config: None,
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
            },
            RaftMsg::InstallSnapshotResp {
                term: 7,
                last_index: 0,
                next_offset: 2048,
            },
            RaftMsg::Heartbeat { node: nid(11) },
            RaftMsg::TimeoutNow { term: 7 },
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
                    fence: KeyRange::whole(),
                    ts: ts(1, 0),
                },
                config: None,
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
