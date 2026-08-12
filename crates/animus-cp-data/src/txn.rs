//! Transaction records + the value envelope (ADR 0018 §2, PR3): the
//! single-participant "degenerate 2PC" that lands the record/intent/resolve
//! machinery ahead of true multi-participant coordination (PR4).
//!
//! ## The value envelope
//!
//! Every value the CP apply path merges into the engine (`Put`/`Batch`/`Cas`,
//! and a `TxnResolve`'s final rewrite) is now a 1-byte-tagged [`Envelope`]:
//! tag `0` = [`Envelope::Committed`] (the rest of the bytes are the value,
//! byte-for-byte what the caller supplied — unwrapped again at every read
//! path before it ever reaches a client); tag `1` = [`Envelope::Intent`], a
//! provisional write staged by `KvCommand::TxnStage` naming the transaction
//! that staged it, where to find its decision (the txn record's own logical
//! key — see below), and the value the key will take if the transaction
//! commits (`None` = the key will become a tombstone, a staged delete).
//! Tombstones themselves carry no value at all (the engine's own per-key
//! tombstone bit, `merge_tombstone`) and are never enveloped — the tag only
//! ever applies to an actual value.
//!
//! Fresh clusters only (no live-deployment migration path, per house
//! convention) — but see `codec.rs`'s `VERSION` bump for the wire/image
//! format, which fails loudly on a mixed-version decode rather than silently
//! misreading a pre-envelope value as raw client bytes.
//!
//! ## The transaction record
//!
//! A single [`TxnRecord`] is the atomic commit point (ADR 0018 §3): its
//! `status` moves `Pending` -> `Committed{commit_ts}` or `Pending` ->
//! `Aborted`, once, and every reader/resolver's decision is a pure function
//! of that one flip. Per the PR1 amendment (§3), the record lives **inside**
//! the first (anchor) participant's own tablet — not a separate always-on
//! system tablet — so it replicates through that tablet's own Raft log,
//! ships with its `engine_image` snapshots, and moves with a split/merge
//! exactly like the anchor's own data would. This is the opposite locality
//! choice from the range-seal/read-ceiling markers (`seal.rs`/`ceiling.rs`),
//! which are deliberately **engine-global** (outside every `StorageScope`) —
//! a txn record instead has to be an ordinary **in-scope logical key** of
//! this specific tablet, so it moves/splits/scans exactly like user data.
//!
//! ### Key scheme + disjointness proof
//!
//! [`record_key`] returns `token(8 bytes) || [0x00, RECORD_TAG] ||
//! encode(txn_id)`, where `token` is the **anchor key's own** 8-byte
//! partition token (ADR 0022: every data-plane key leads with
//! `animus_tablet::partition_token`, unconditionally — `TOKEN_BYTES = 8`).
//! Living inside the anchor's own token range is what makes this key both
//! (a) always within the tablet's own range at stage time (the anchor's own
//! write is itself in-range, so this token is live) and (b) move correctly
//! across a split/merge that keeps the anchor's own token together with the
//! rest of that tablet.
//!
//! **Disjointness from every real data-plane key sharing that same
//! token**, proved structurally rather than assumed: a real key's logical
//! form (after its leading token) is always `escape(pk) ++ rk`
//! (`animus_tablet::escape`, ADR 0022/0023), for *some* partition key `pk`
//! and row key `rk` — both otherwise arbitrary, client-controlled bytes, so
//! no fixed suffix can be proven disjoint from `escape(pk) ++ rk` in
//! general (`rk` alone could be crafted to match almost anything that
//! follows `escape(pk)`). The one structural fact `escape` guarantees is
//! about its own first two output bytes: `escape` never emits a lone
//! `0x00` — every literal `0x00` byte in `pk` is doubled to `0x00 0x01`,
//! and the whole encoding is always terminated by `0x00 0x00`. So
//! `escape(pk)` (which has length >= 2 for *every* `pk`, including empty)
//! starts with `0x00` in exactly two cases: `pk` is empty
//! (`escape(pk) == [0x00, 0x00]`) or `pk`'s own first byte is `0x00`
//! (`escape(pk)` starts `[0x00, 0x01, ..]`). There is **no** `pk` for which
//! `escape(pk)`'s first two bytes are `[0x00, X]` for any `X` outside
//! `{0x00, 0x01}`. [`RECORD_TAG`] (`0x02`) is exactly such an `X` — so
//! `[0x00, RECORD_TAG, ..]` can never equal the first two bytes of any
//! `escape(pk) ++ rk`, for *any* `pk`/`rk` whatsoever, regardless of what
//! `rk` itself contains. Since a real key's post-token suffix is always
//! `escape(pk) ++ rk`, and our marker's post-token suffix always begins
//! `[0x00, RECORD_TAG]`, the two can never collide.
//!
//! (Contrast `seal.rs`'s marker, which proves disjointness from *every*
//! table's whole keyspace via the control plane's `RESERVED_NAMESPACE`
//! reservation — that trick only works for an **engine-global** key with no
//! `StorageScope` prefix of its own. A txn record's disjointness has to be
//! proved *inside* one table's own token space instead, since it must live
//! there, and no analogous "reserved partition key" mechanism exists for
//! user data — hence the different, `escape`-structural argument above.)
//!
//! **A residual, documented, not closed by PR3**: a tablet split's
//! `split_key` is an arbitrary existing row's own key
//! (`animusd::auto_split_loop`'s byte-weighted median), not necessarily
//! token-aligned, so in principle a single token's rows (and, per this
//! design, its txn record) could end up split across two sibling tablets by
//! a split racing an in-flight transaction. PR3 is deliberately
//! single-participant/single-tablet in scope; split-vs.-in-flight-txn
//! interaction is a PR4+ concern (mirroring how the range seal itself
//! needed a dedicated amendment once genuine concurrent splits were
//! exercised) and is not solved here.
//!
//! ## Resolution semantics
//!
//! A reader that encounters an [`Envelope::Intent`] looks up the named
//! record (in this same tablet's scope — the single-participant invariant)
//! and acts on its status. A `Committed` record at or before the read's own
//! timestamp serves the staged value. An `Aborted` record — or a
//! `Committed` one **after** the read's timestamp, which is equally
//! invisible to that snapshot — serves whatever this key held **immediately
//! before** the intent, restored by rewinding to the version just below the
//! intent's own applied version (`get_at(key, intent_version - 1)`), never
//! by writing a tombstone (which would incorrectly shadow that older,
//! still-live committed value). A `Pending` record is a bounded retry at a
//! point read (`RaftKvNode::read_resolved`) or a silent omission at a scan
//! (full push/wait scheduling for both is PR4).
//!
//! Decode functions here treat malformed bytes as a hard bug (this crate
//! only ever reads back what it itself wrote) rather than a recoverable
//! error, mirroring `seal.rs`/`ceiling.rs`'s "an engine-internal marker
//! should never be malformed" doctrine.

use animus_env::NodeId;
use animus_tablet::{KeyRange, TOKEN_BYTES};
use serde::{Deserialize, Serialize};

use crate::hlc::HlcTimestamp;

/// The second byte of a txn record key's lead pair (see the module doc's
/// disjointness proof): any value other than `0x00`/`0x01` works; `0x02` is
/// as good as any other.
const RECORD_TAG: u8 = 0x02;

/// A transaction's identity: its own commit-attempt timestamp plus the node
/// that minted it (ADR 0018 §2/PR3) — the node tiebreak is load-bearing:
/// different tablet groups run independent `Hlc` instances that never
/// witness each other directly, so two different groups' leaders can in
/// principle mint the identical `(wall_ms, logical)` pair. `Ord` derives in
/// field order (`ts`, then `node`), giving a total, deterministic order with
/// no separate tiebreak logic. Serializable for the Raft WAL (it rides
/// inside `KvCommand`, which the shared control-plane `serde_json`
/// `PersistedState` format persists) — **never** used as a map key anywhere
/// (`serde_json` can't serialize a non-string-keyed map; every place this
/// travels is a plain struct field or `Vec`).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TxnId {
    pub ts: HlcTimestamp,
    pub node: NodeId,
}

/// A transaction's decision state (ADR 0018 §3): the one thing that ever
/// changes about a [`TxnRecord`] after it's created, and only ever once
/// (`Pending` -> `Committed`/`Aborted` — apply asserts against a second,
/// conflicting flip; see `lib.rs`'s `TxnCommit`/`TxnAbort` apply arms).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TxnStatus {
    Pending,
    Committed { commit_ts: HlcTimestamp },
    Aborted,
}

/// The transaction-status record (ADR 0018 §3): the atomic commit point.
/// Lives at [`record_key`] inside the anchor tablet's own `StorageScope` —
/// see the module doc.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TxnRecord {
    pub txn_id: TxnId,
    pub status: TxnStatus,
    /// **ADR 0018 §2/PR5**: every key this transaction staged **anywhere**
    /// — every participant's writes, the anchor's own included — as
    /// `(table, span)` pairs, `span` the point-span
    /// (`[key, immediate_successor(key))`) shape [`immediate_successor`]
    /// builds. This is a structural fix to a gap PR3/PR4 left open: as
    /// those PRs shipped, `intent_spans` was only ever populated from the
    /// anchor's *own* writes (`txn_stage_participant` passed `spans:
    /// Vec::new()`, "no local record is ever created here") and carried no
    /// table name — so recovery (PR5) had no way to learn which *other*
    /// tablets/tables a transaction touched, or where to route a
    /// cross-tablet verification/resolve query for them. The coordinator
    /// (`animusd::ClientCtx::cp_txn`) already computes the full write set
    /// grouped by `(table, tablet)` before staging anything, so it hands
    /// the anchor's stage the complete cross-participant list up front
    /// (mirroring exactly how PR4 closed the analogous `record_table`
    /// routing gap). A recovery pusher walks every entry here, routes to
    /// `table`'s tablet by `span.start` (the exact key), and asks that
    /// tablet's leader whether it still holds a live intent for this txn
    /// (`RaftKvNode::txn_verify_staged`) or resolves it once the record has
    /// decided (`ClientCtx::txn_resolve_participant`).
    pub intent_spans: Vec<(String, KeyRange)>,
    pub created_ts: HlcTimestamp,
}

/// A transaction's **decided** outcome (ADR 0018 §2/PR4) — the `Committed`/
/// `Aborted` half of [`TxnStatus`], carried explicitly wherever a decision
/// must travel to a tablet that doesn't (and, for a non-anchor participant,
/// structurally can't) hold the record itself: [`KvCommand::TxnResolve`]'s
/// `outcome` field, and the wire reply to a cross-tablet status query. Unlike
/// `TxnStatus` this crate keeps `pub(crate)`, this is `pub` — a multi-
/// participant coordinator (`animusd`) constructs/matches it directly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxnOutcome {
    Committed { commit_ts: HlcTimestamp },
    Aborted,
}

/// A transaction's status as observed by a caller with **no local record
/// access** (ADR 0018 §2/PR4) — the public mirror of [`TxnStatus`] a
/// cross-tablet `TxnStatus` query (or any other external caller) reads back.
/// `From`/`Into` `TxnStatus` round-trip losslessly; kept as a distinct public
/// type rather than making `TxnStatus` itself `pub` so this crate's internal
/// record-storage representation stays free to change independently of the
/// wire-facing shape.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxnDecisionStatus {
    Pending,
    Committed { commit_ts: HlcTimestamp },
    Aborted,
}

impl TxnStatus {
    /// The public mirror of this status (ADR 0018 §2/PR4) — see
    /// [`TxnDecisionStatus`]'s doc.
    #[must_use]
    pub(crate) fn to_public(&self) -> TxnDecisionStatus {
        match self {
            TxnStatus::Pending => TxnDecisionStatus::Pending,
            TxnStatus::Committed { commit_ts } => TxnDecisionStatus::Committed {
                commit_ts: *commit_ts,
            },
            TxnStatus::Aborted => TxnDecisionStatus::Aborted,
        }
    }
}

/// The 1-byte-tagged value envelope every apply-path write now wraps its
/// value in — see the module doc.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Envelope {
    Committed(Vec<u8>),
    Intent {
        txn_id: TxnId,
        /// The logical key of this transaction's record — carried directly
        /// in the intent (rather than re-derived from the intent's own
        /// key's token) so any reader can look it up with no extra
        /// context, regardless of how many distinct tokens a
        /// single-tablet transaction's writes happen to span.
        record_key: Vec<u8>,
        /// **ADR 0018 §2/PR4**: the name of the table whose tablet owns
        /// `record_key`. A record's key alone (`token || [0x00, RECORD_TAG]
        /// || txn_id`) does **not** identify which table's tablet ring it
        /// belongs to — tablets are table-scoped (ADR 0022/0023), so two
        /// different tables' rings can and do assign the identical
        /// partition token to different rows. A reader on a *different*
        /// tablet than the anchor's (a non-anchor participant's own read,
        /// or any reader racing an unresolved intent) has no other way to
        /// route a cross-tablet status query to the record's actual owner.
        /// A single-participant transaction (PR3) never needed this since
        /// the record was always local; carrying it here costs one string
        /// per intent and closes the routing gap PR4 needs.
        record_table: String,
        staged_value: Option<Vec<u8>>,
    },
}

// ---- the anchor-token-derived record key -----------------------------------

/// The logical key of `txn_id`'s transaction record, anchored at
/// `anchor_token` (the anchor write's own 8-byte partition token — see the
/// module doc's disjointness proof for why this exact shape is safe).
///
/// # Panics
/// If `anchor_token` is not exactly [`TOKEN_BYTES`] long — every real
/// data-plane key leads with a full token (ADR 0022); a caller passing
/// anything else has already broken a load-bearing invariant elsewhere.
#[must_use]
pub(crate) fn record_key(anchor_token: &[u8], txn_id: &TxnId) -> Vec<u8> {
    assert_eq!(
        anchor_token.len(),
        TOKEN_BYTES,
        "txn::record_key: anchor token must be exactly {TOKEN_BYTES} bytes (ADR 0022)"
    );
    let mut out = Vec::with_capacity(TOKEN_BYTES + 2 + 16);
    out.extend_from_slice(anchor_token);
    out.push(0x00);
    out.push(RECORD_TAG);
    put_txn_id(&mut out, txn_id);
    out
}

/// Whether logical key `k` is a txn-record key of the shape [`record_key`]
/// produces — used to filter these internal keys out of every client-facing
/// scan (see `lib.rs`'s `resolve_scan_rows`) and out of `has_data`'s
/// presence check.
#[must_use]
pub(crate) fn is_record_key(k: &[u8]) -> bool {
    k.len() >= TOKEN_BYTES + 2 && k[TOKEN_BYTES] == 0x00 && k[TOKEN_BYTES + 1] == RECORD_TAG
}

// ---- binary encode/decode (this crate's compact style, mirroring seal.rs) -

fn put_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_u32(out, b.len() as u32);
    out.extend_from_slice(b);
}

fn put_opt_bytes(out: &mut Vec<u8>, b: Option<&[u8]>) {
    match b {
        None => put_u8(out, 0),
        Some(b) => {
            put_u8(out, 1);
            put_bytes(out, b);
        }
    }
}

fn put_ts(out: &mut Vec<u8>, ts: HlcTimestamp) {
    out.extend_from_slice(&ts.wall_ms.to_be_bytes());
    out.extend_from_slice(&ts.logical.to_be_bytes());
}

fn put_key_range(out: &mut Vec<u8>, r: &KeyRange) {
    put_bytes(out, &r.start);
    put_opt_bytes(out, r.end.as_deref());
}

/// ADR 0018 §2/PR5: a `TxnRecord::intent_spans` entry — the table name
/// alongside its span, so a recovery pusher can route to the right tablet.
fn put_table_span(out: &mut Vec<u8>, table: &str, r: &KeyRange) {
    put_bytes(out, table.as_bytes());
    put_key_range(out, r);
}

fn put_txn_id(out: &mut Vec<u8>, id: &TxnId) {
    put_ts(out, id.ts);
    put_bytes(out, id.node.as_str().as_bytes());
}

/// A forward-only cursor over marker bytes; mirrors `seal.rs`'s inline
/// cursor (each engine-marker module keeps its own small, self-contained
/// copy rather than sharing one — see that module's doc).
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let s = self.bytes.get(self.pos..self.pos + n)?;
        self.pos += n;
        Some(s)
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn bytes(&mut self) -> Option<Vec<u8>> {
        let len = self.u32()? as usize;
        Some(self.take(len)?.to_vec())
    }

    fn opt_bytes(&mut self) -> Option<Option<Vec<u8>>> {
        match self.u8()? {
            0 => Some(None),
            1 => Some(Some(self.bytes()?)),
            _ => None,
        }
    }

    fn ts(&mut self) -> Option<HlcTimestamp> {
        let wall_ms = u64::from_be_bytes(self.take(8)?.try_into().ok()?);
        let logical = u32::from_be_bytes(self.take(4)?.try_into().ok()?);
        Some(HlcTimestamp { wall_ms, logical })
    }

    fn key_range(&mut self) -> Option<KeyRange> {
        Some(KeyRange {
            start: self.bytes()?,
            end: self.opt_bytes()?,
        })
    }

    /// The exact inverse of [`put_table_span`].
    fn table_span(&mut self) -> Option<(String, KeyRange)> {
        let table = String::from_utf8(self.bytes()?).ok()?;
        let range = self.key_range()?;
        Some((table, range))
    }

    fn node_id(&mut self) -> Option<NodeId> {
        let bytes = self.bytes()?;
        let s = String::from_utf8(bytes).ok()?;
        Some(NodeId::new_unchecked(s))
    }

    fn txn_id(&mut self) -> Option<TxnId> {
        Some(TxnId {
            ts: self.ts()?,
            node: self.node_id()?,
        })
    }
}

/// Wrap `value` as a committed envelope (tag `0`).
#[must_use]
pub(crate) fn encode_committed(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 1);
    out.push(0);
    out.extend_from_slice(value);
    out
}

/// Encode an intent envelope (tag `1`) — see [`Envelope::Intent`].
#[must_use]
pub(crate) fn encode_intent(
    txn_id: &TxnId,
    record_key: &[u8],
    record_table: &str,
    staged_value: Option<&[u8]>,
) -> Vec<u8> {
    let mut out = vec![1];
    put_txn_id(&mut out, txn_id);
    put_bytes(&mut out, record_key);
    put_bytes(&mut out, record_table.as_bytes());
    put_opt_bytes(&mut out, staged_value);
    out
}

/// Decode a value envelope. Every value this crate's apply path ever merges
/// into the engine is one of these two shapes — a decode failure means the
/// engine holds bytes this crate never wrote, a hard bug, not a recoverable
/// condition (mirrors `seal.rs`/`ceiling.rs`'s doctrine for their own
/// markers).
///
/// # Panics
/// If `bytes` is empty or the tag/fields don't match either shape.
#[must_use]
pub(crate) fn decode_envelope(bytes: &[u8]) -> Envelope {
    assert!(
        !bytes.is_empty(),
        "txn: empty value envelope (corrupt engine value)"
    );
    match bytes[0] {
        0 => Envelope::Committed(bytes[1..].to_vec()),
        1 => {
            let mut c = Cursor {
                bytes: &bytes[1..],
                pos: 0,
            };
            let txn_id = c.txn_id().expect("txn: malformed intent envelope (txn_id)");
            let record_key = c
                .bytes()
                .expect("txn: malformed intent envelope (record_key)");
            let record_table = String::from_utf8(
                c.bytes()
                    .expect("txn: malformed intent envelope (record_table)"),
            )
            .expect("txn: malformed intent envelope (record_table not utf8)");
            let staged_value = c
                .opt_bytes()
                .expect("txn: malformed intent envelope (staged_value)");
            Envelope::Intent {
                txn_id,
                record_key,
                record_table,
                staged_value,
            }
        }
        other => panic!("txn: unknown envelope tag {other} (corrupt engine value)"),
    }
}

/// Encode a [`TxnRecord`] for storage as an ordinary in-scope engine value
/// (never envelope-tagged — a record is never read via the normal
/// `Envelope` path, only via [`decode_record`] at the specific record key).
#[must_use]
pub(crate) fn encode_record(r: &TxnRecord) -> Vec<u8> {
    let mut out = Vec::new();
    put_txn_id(&mut out, &r.txn_id);
    match &r.status {
        TxnStatus::Pending => put_u8(&mut out, 0),
        TxnStatus::Committed { commit_ts } => {
            put_u8(&mut out, 1);
            put_ts(&mut out, *commit_ts);
        }
        TxnStatus::Aborted => put_u8(&mut out, 2),
    }
    put_u32(&mut out, r.intent_spans.len() as u32);
    for (table, span) in &r.intent_spans {
        put_table_span(&mut out, table, span);
    }
    put_ts(&mut out, r.created_ts);
    out
}

/// The exact inverse of [`encode_record`]. `None` on malformed input (see
/// the module doc — an engine-internal marker this crate itself wrote
/// should never be malformed).
#[must_use]
pub(crate) fn decode_record(bytes: &[u8]) -> Option<TxnRecord> {
    let mut c = Cursor { bytes, pos: 0 };
    let txn_id = c.txn_id()?;
    let status = match c.u8()? {
        0 => TxnStatus::Pending,
        1 => TxnStatus::Committed { commit_ts: c.ts()? },
        2 => TxnStatus::Aborted,
        _ => return None,
    };
    let n = c.u32()?;
    let mut intent_spans = Vec::with_capacity(n as usize);
    for _ in 0..n {
        intent_spans.push(c.table_span()?);
    }
    let created_ts = c.ts()?;
    Some(TxnRecord {
        txn_id,
        status,
        intent_spans,
        created_ts,
    })
}

/// The immediate lexicographic successor of `key` (no byte string sorts
/// strictly between `key` and `key ++ [0x00]`) — used to build a point
/// key's own `KeyRange` span for `TxnRecord::intent_spans`, mirroring
/// `ts_cache::point_span`'s identical reasoning.
#[must_use]
pub(crate) fn immediate_successor(key: &[u8]) -> Vec<u8> {
    let mut end = key.to_vec();
    end.push(0);
    end
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_env::nid;
    use animus_tablet::escape;

    fn ts(wall_ms: u64, logical: u32) -> HlcTimestamp {
        HlcTimestamp { wall_ms, logical }
    }

    fn txn(wall_ms: u64) -> TxnId {
        TxnId {
            ts: ts(wall_ms, 0),
            node: nid(1),
        }
    }

    // ---- disjointness proof, exercised directly ----------------------------

    #[test]
    fn record_key_never_collides_with_any_escaped_pk_plus_rk() {
        let token = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let id = txn(42);
        let marker = record_key(&token, &id);

        // Every case the module doc's proof enumerates: pk empty, pk
        // starting with 0x00, and pk not starting with 0x00 — each with an
        // arbitrary rk appended (including one that tries to mimic the
        // marker's own tail).
        let cases: Vec<Vec<u8>> = vec![
            escape(b""),
            [escape(b""), marker[TOKEN_BYTES..].to_vec()].concat(),
            escape(b"\x00"),
            escape(b"\x00tail"),
            [escape(b"\x00"), marker[TOKEN_BYTES..].to_vec()].concat(),
            escape(b"users"),
            [escape(b"users"), b"row1".to_vec()].concat(),
            [escape(b"users"), marker[TOKEN_BYTES..].to_vec()].concat(),
        ];
        for suffix in cases {
            let real_key = [token.to_vec(), suffix].concat();
            assert_ne!(
                real_key, marker,
                "a real data key must never equal the txn record marker"
            );
        }
    }

    #[test]
    fn is_record_key_identifies_only_markers() {
        let token = [9u8; TOKEN_BYTES];
        let marker = record_key(&token, &txn(1));
        assert!(is_record_key(&marker));

        let real_key = [token.to_vec(), escape(b"users"), b"row".to_vec()].concat();
        assert!(!is_record_key(&real_key));

        let empty_pk_key = [token.to_vec(), escape(b"")].concat();
        assert!(!is_record_key(&empty_pk_key));
    }

    #[test]
    #[should_panic(expected = "must be exactly")]
    fn record_key_panics_on_a_short_token() {
        let _ = record_key(&[1, 2, 3], &txn(1));
    }

    // ---- envelope round-trips ----------------------------------------------

    #[test]
    fn committed_envelope_round_trips_including_empty_value() {
        for value in [b"hello".to_vec(), Vec::new(), vec![0, 0, 0xFF]] {
            let bytes = encode_committed(&value);
            match decode_envelope(&bytes) {
                Envelope::Committed(v) => assert_eq!(v, value),
                Envelope::Intent { .. } => panic!("expected Committed"),
            }
        }
    }

    #[test]
    fn intent_envelope_round_trips_put_and_delete_intents() {
        let id = txn(7);
        let record = record_key(&[0; TOKEN_BYTES], &id);
        for staged in [Some(b"v".to_vec()), None] {
            let bytes = encode_intent(&id, &record, "orders", staged.as_deref());
            match decode_envelope(&bytes) {
                Envelope::Intent {
                    txn_id,
                    record_key: rk,
                    record_table,
                    staged_value,
                } => {
                    assert_eq!(txn_id, id);
                    assert_eq!(rk, record);
                    assert_eq!(record_table, "orders");
                    assert_eq!(staged_value, staged);
                }
                Envelope::Committed(_) => panic!("expected Intent"),
            }
        }
    }

    #[test]
    #[should_panic(expected = "empty value envelope")]
    fn decode_envelope_panics_on_empty_bytes() {
        let _ = decode_envelope(&[]);
    }

    #[test]
    #[should_panic(expected = "unknown envelope tag")]
    fn decode_envelope_panics_on_unknown_tag() {
        let _ = decode_envelope(&[9, 1, 2, 3]);
    }

    // ---- record round-trips -------------------------------------------------

    #[test]
    fn record_round_trips_every_status() {
        let spans = vec![
            (
                "orders".to_string(),
                KeyRange::new(b"a".to_vec(), Some(b"m".to_vec())),
            ),
            ("shipments".to_string(), KeyRange::new(b"m".to_vec(), None)),
        ];
        for status in [
            TxnStatus::Pending,
            TxnStatus::Committed {
                commit_ts: ts(5, 1),
            },
            TxnStatus::Aborted,
        ] {
            let record = TxnRecord {
                txn_id: txn(3),
                status,
                intent_spans: spans.clone(),
                created_ts: ts(2, 0),
            };
            let bytes = encode_record(&record);
            let back = decode_record(&bytes).expect("decodes");
            assert_eq!(back, record);
        }
    }

    #[test]
    fn decode_record_rejects_malformed_input() {
        assert_eq!(decode_record(&[1, 2, 3]), None);
        assert_eq!(decode_record(&[]), None);
    }

    #[test]
    fn immediate_successor_contains_exactly_the_key() {
        let end = immediate_successor(b"k");
        let span = KeyRange::new(b"k".to_vec(), Some(end));
        assert!(span.contains(b"k"));
        assert!(!span.contains(b"k\x00"));
        assert!(!span.contains(b"ka"));
    }
}
