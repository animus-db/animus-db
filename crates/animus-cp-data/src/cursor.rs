//! Consumer cursor rows (ADR 0042/0043 foundation, `KIND_CURSOR = 0x04`): a
//! per-tablet, per-consumer HLC watermark the change-log lifecycle rework
//! (ADR 0041 §4a's deferred design, now specified by ADR 0042 §7) rests on.
//!
//! A cursor row's value is a packed-HLC **watermark `W`**: "every change-log
//! record (`KIND_CHANGE`) this tablet has ever applied with `hlc <= W` is
//! fully consumed by this tag." Two tags exist today, `"gsi"` (the GSI
//! drain's own reconcile cursor) and `"copier"` (the stream copier's cursor,
//! landing with the shard subsystem) — the type here is deliberately generic
//! over the tag string rather than a closed enum, since ADR 0042's own
//! roadmap expects more consumer kinds later (per-CQL-CDC-consumer cursors
//! named as a follow-up).
//!
//! ## Why a scalar per-tag watermark is sound
//!
//! `assert_ts_monotonic` (see this crate's Key invariants doc, `lib.rs`) is a
//! hard invariant: every change record a tablet group ever applies has a
//! strictly greater `ts` than every one applied before it. A single scalar
//! "everything at or below this ts is done" is therefore a complete,
//! unambiguous cursor — there is no reordering within one tablet's own
//! applied sequence a positional/key cursor would need to account for
//! (contrast `pending_changes`' own note that change-log **key** order is
//! *token-then-pk-then-HLC*, not global commit order, which is exactly what
//! rules a positional cursor out and a timestamp one in).
//!
//! ## Key scheme + disjointness proof
//!
//! [`cursor_key`] returns `token(8 bytes) || [0x00, CURSOR_TAG] ||
//! consumer.as_bytes()`, where `token` is this tablet's own live
//! `KeyRange::start`, truncated (zero-padded if shorter) to
//! [`TOKEN_BYTES`] — mirroring `txn.rs`'s `record_key` scheme exactly (`token
//! || [0x00, TAG] || id`), with `CURSOR_TAG` (`0x03`) taking the next unused
//! value after `seal.rs`'s marker doesn't share this scope at all and
//! `txn.rs`'s own `RECORD_TAG` (`0x02`).
//!
//! Unlike a txn record, a cursor row lives in its **own** row-kind scope
//! (`KIND_CURSOR`), physically disjoint from `KIND_BASE` by construction
//! (every kind is a distinct `StorageScope` prefix — see `StorageScope::
//! with_kind`'s own doc) — so cursor rows can never collide with a real
//! client key at all, regardless of byte content. The disjointness argument
//! below is still worth stating structurally, for the same reason `txn.rs`
//! states its own even though `RaftKvNode::txn_stage` never scans the base
//! scope for record keys either: it is what lets [`parse_cursor_key`]
//! recover a tag from a raw scanned key (the min-over-rows rule, ADR 0042
//! §7, needs to enumerate every row in a tablet's `KIND_CURSOR` scope and
//! group by tag) with a fixed, unambiguous byte offset, rather than a scan
//! for the marker pair that a pathological token's own bytes could in
//! principle spoof.
//!
//! `animus_tablet::escape`'s structural guarantee (restated from `txn.rs`):
//! `escape(pk)` never emits a lone `0x00` byte — every literal `0x00` in `pk`
//! is doubled to `0x00 0x01`, and the whole encoding is terminated by `0x00
//! 0x00`. So the only two bytes that can ever follow a `0x00` inside a real
//! `escape(pk) ++ rk` suffix are `0x00` (the empty-pk / end-of-encoding
//! terminator) or `0x01` (an escaped literal zero, encoding continues).
//! `CURSOR_TAG` (`0x03`) is neither, so `[0x00, CURSOR_TAG, ..]` can never
//! be mistaken for the start of a real key's post-token suffix at the
//! byte offset [`TOKEN_BYTES`] — the same argument `txn.rs`'s
//! `RECORD_TAG` (`0x02`) relies on, transplanted to this scope.
//!
//! ## Two value conventions, side by side
//!
//! A cursor row's **key** ([`cursor_key`]) is one scheme shared by every
//! consumer; its **value** is not — two conventions exist today, chosen
//! per-tag by what that consumer needs to express:
//!
//! - **The packed-HLC watermark** ([`encode_watermark`]/[`decode_watermark`]):
//!   "every change-log record this tablet has applied with `hlc <= W` is
//!   fully consumed by this tag." Used by the `"gsi"` tag (the GSI drain's
//!   own reconcile cursor, ADR 0042 §7) — sound there because
//!   `assert_ts_monotonic` makes a single scalar ts a complete, unambiguous
//!   position in one tablet's own applied sequence (see this module's own
//!   argument above).
//! - **The raw last-scanned-base-key** ([`encode_backfill_cursor`]/
//!   [`decode_backfill_cursor`], ADR 0045 §2): "this is the last `KIND_BASE`
//!   partition prefix this tag's forward sweep has seeded." The backfill
//!   seeder (`animusd::index_drain`) walks `KIND_BASE`, not `KIND_CHANGE` —
//!   it has no HLC to record at all until it *writes* one (the very
//!   change-log record it seeds), so a watermark convention doesn't fit;
//!   the position that names "where the sweep left off" is a physical base
//!   key, not a timestamp. Tag convention: `format!("backfill:{index_name}")`,
//!   one row per index currently `Creating`/being seeded (see that module's
//!   own doc for why per-index rather than one shared cursor).
//!
//! Both conventions share one `KIND_CURSOR` row-kind scope and the one
//! [`cursor_key`] builder — only the 8-vs-variable-length **value** bytes
//! differ, and a reader always knows which convention applies from its own
//! tag, never from inspecting the bytes.
//!
//! [`ConsumerOffset`] is an additive, unifying wrapper over the two
//! conventions above — for a future consumer that wants to hold either
//! shape without hard-coding which one its own tag uses (a per-CQL-CDC-
//! consumer cursor is the concrete future case ADR 0042's own roadmap
//! names, see this doc's own intro). It delegates to the existing
//! `encode_watermark`/`decode_watermark`/`encode_backfill_cursor`/
//! `decode_backfill_cursor` free functions rather than re-implementing
//! either encoding — those functions, and every existing caller of them,
//! are unchanged.
//!
//! **A residual, documented gap** (mirroring `txn.rs`'s own "not closed
//! here" note about `split_key` not being token-aligned): `cursor_key`
//! truncates the tablet's live `range.start` to its leading [`TOKEN_BYTES`]
//! bytes, which is exactly this tablet's own genuine ADR 0022 partition
//! token *when* `range.start` is empty (the ring's own start, zero-padded)
//! or a prior split's boundary key (whose own leading `TOKEN_BYTES` are a
//! real Murmur3 token by construction). It is **not** proven here that the
//! resulting key is always strictly less than this tablet's own
//! `range.end` in every pathological case (e.g. a table using a `Binary`
//! partition key whose first byte is `0x00`, split at a boundary
//! immediately following `range.start`'s own token) — in the ordinary case
//! the marker sits at the very start of the tablet's own key space, far
//! below any split point chosen deeper into its rows, and `assert_ts_
//! monotonic`/`with_kind`'s shared-range narrowing never depends on this for
//! *correctness of tag disjointness*, only for the cursor row surviving a
//! future `narrow_scope`/`erase_scope`/`engine_image` bound unclipped. Left
//! for a future PR's corpus to stress (a table with `Binary` keys starting
//! `0x00`, split near its own first tablet's boundary) exactly as `txn.rs`
//! defers its own split-alignment gap to PR4+.

use animus_tablet::TOKEN_BYTES;

use crate::hlc::{self, HlcTimestamp};

/// The second byte of a cursor row key's lead pair (see the module doc's
/// disjointness proof) — the next unused tag after `txn.rs`'s own
/// `RECORD_TAG` (`0x02`); `seal.rs`/`ceiling.rs`'s markers live outside every
/// `StorageScope` entirely, so they never share this numbering.
const CURSOR_TAG: u8 = 0x03;

/// The tablet-token component of [`cursor_key`]: `range_start` truncated
/// (zero-padded if shorter) to [`TOKEN_BYTES`]. Split out of `cursor_key`
/// so a caller that already has a *parsed* row's own token (from
/// [`parse_cursor_key`]) can compute *this* tablet's own token to compare
/// against, without rebuilding a whole key. Its original caller — the ADR
/// 0042 §7 trim janitor's merge-residue cleanup (`animusd::index_drain`),
/// which told "this row is this tablet's own" from "a
/// still-physically-present absorbed sibling's" after a merge widened a
/// survivor's scope over it — no longer exists: tablet merge was removed
/// entirely (ADR 0044, tablets are split-only). This function (and its
/// `lib.rs` consumer, [`RaftKvNode::cursor_rows_with_token`]) currently has
/// **no production caller**; kept because a future consumer needing the
/// same token-vs-physical-presence disambiguation would otherwise have to
/// reinvent it.
#[must_use]
pub fn token_of(range_start: &[u8]) -> [u8; TOKEN_BYTES] {
    let mut token = [0u8; TOKEN_BYTES];
    let n = range_start.len().min(TOKEN_BYTES);
    token[..n].copy_from_slice(&range_start[..n]);
    token
}

/// This tablet's own cursor-row key for `consumer` (see the module doc for
/// the full scheme + disjointness proof). `range_start` is the tablet's
/// live `KeyRange::start` at the moment of the call (`RaftKvNode::
/// scope_range().start`) — truncated (zero-padded if shorter) to
/// [`TOKEN_BYTES`] by [`token_of`].
#[must_use]
pub fn cursor_key(range_start: &[u8], consumer: &str) -> Vec<u8> {
    let token = token_of(range_start);
    let mut out = Vec::with_capacity(TOKEN_BYTES + 2 + consumer.len());
    out.extend_from_slice(&token);
    out.push(0x00);
    out.push(CURSOR_TAG);
    out.extend_from_slice(consumer.as_bytes());
    out
}

/// Recover `(token, consumer)` from a raw `KIND_CURSOR`-scoped key, or
/// `None` if it isn't shaped like one (wrong length, wrong lead pair, or a
/// non-UTF8 tag — this crate only ever writes tags it itself minted as
/// valid UTF-8 strings, so a decode failure here is a defensive read, not an
/// expected case). See the module doc for why the fixed offset (rather than
/// a scan for the lead pair) is what makes this unambiguous.
#[must_use]
pub fn parse_cursor_key(key: &[u8]) -> Option<(&[u8], &str)> {
    if key.len() < TOKEN_BYTES + 2 {
        return None;
    }
    if key[TOKEN_BYTES] != 0x00 || key[TOKEN_BYTES + 1] != CURSOR_TAG {
        return None;
    }
    let tag = std::str::from_utf8(&key[TOKEN_BYTES + 2..]).ok()?;
    Some((&key[..TOKEN_BYTES], tag))
}

/// A cursor row's stored value: `hlc::pack(ts)` as 8 big-endian bytes — the
/// same packed representation the engine's own MVCC version uses (`hlc.rs`),
/// reused here as an ordinary value byte string rather than a version number.
#[must_use]
pub fn encode_watermark(ts: HlcTimestamp) -> Vec<u8> {
    hlc::pack(ts).to_be_bytes().to_vec()
}

/// The dual of [`encode_watermark`]. `None` on anything but exactly 8 bytes —
/// this crate only ever reads back what it itself wrote (mirroring `seal.rs`/
/// `ceiling.rs`'s "an engine-internal marker should never be malformed"
/// doctrine), so a caller sees this as a defensive read, not an expected case.
#[must_use]
pub fn decode_watermark(bytes: &[u8]) -> Option<HlcTimestamp> {
    let arr: [u8; 8] = bytes.try_into().ok()?;
    Some(hlc::unpack(u64::from_be_bytes(arr)))
}

/// A backfill cursor row's stored value (ADR 0045 §2) — the raw last-seeded
/// `KIND_BASE` partition prefix, verbatim, **not** a packed HLC (see the
/// module doc's "Two value conventions" section for why this tag needs a
/// different shape than [`encode_watermark`]). Identity today (there is
/// nothing to encode — the bytes already are the value), kept as a named
/// function rather than writing the raw `Vec<u8>` inline at each call site,
/// so the convention has one place to document and one place to change if
/// it ever needs a header.
#[must_use]
pub fn encode_backfill_cursor(last_seeded_prefix: &[u8]) -> Vec<u8> {
    last_seeded_prefix.to_vec()
}

/// The dual of [`encode_backfill_cursor`] — currently just an owned copy of
/// the stored bytes; see that function's doc for why there is no decode
/// failure mode to report (unlike [`decode_watermark`], which validates an
/// 8-byte packed HLC, this convention has no fixed shape to validate against).
#[must_use]
pub fn decode_backfill_cursor(bytes: &[u8]) -> Vec<u8> {
    bytes.to_vec()
}

/// A consumer cursor's **value**, unified across the two conventions this
/// module's own doc lays out side by side (the packed-HLC watermark and the
/// raw last-scanned-base-key) — additive: [`encode_watermark`]/
/// [`decode_watermark`]/[`encode_backfill_cursor`]/[`decode_backfill_cursor`]
/// and every existing caller of them are untouched. This exists for a
/// **future** generic consumer that wants to hold either offset shape
/// without hard-coding which convention its own tag uses (the module doc's
/// own "per-CQL-CDC-consumer cursors named as a follow-up" case) — no
/// current caller constructs one.
///
/// There is deliberately no single `decode(bytes) -> ConsumerOffset`: the
/// module doc's own disjointness note applies here too — "a reader always
/// knows which convention applies from its own tag, never from inspecting
/// the bytes" — so decoding is convention-specific, mirroring the two free
/// functions it wraps ([`ConsumerOffset::decode_watermark`]/
/// [`ConsumerOffset::decode_key_pos`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsumerOffset {
    /// The packed-HLC watermark convention — see [`encode_watermark`].
    Watermark(HlcTimestamp),
    /// The raw last-scanned-base-key convention — see
    /// [`encode_backfill_cursor`].
    KeyPos(Vec<u8>),
}

impl ConsumerOffset {
    /// Encode to the wire bytes of whichever convention this value holds —
    /// delegates to [`encode_watermark`]/[`encode_backfill_cursor`], never
    /// re-implementing either.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        match self {
            ConsumerOffset::Watermark(ts) => encode_watermark(*ts),
            ConsumerOffset::KeyPos(key) => encode_backfill_cursor(key),
        }
    }

    /// Decode `bytes` as the packed-HLC watermark convention — delegates to
    /// [`decode_watermark`], so `None` on anything but exactly 8 bytes.
    #[must_use]
    pub fn decode_watermark(bytes: &[u8]) -> Option<Self> {
        decode_watermark(bytes).map(ConsumerOffset::Watermark)
    }

    /// Decode `bytes` as the raw backfill-cursor convention — delegates to
    /// [`decode_backfill_cursor`], which never fails (see that function's
    /// own doc for why there is no decode-failure mode for this
    /// convention).
    #[must_use]
    pub fn decode_key_pos(bytes: &[u8]) -> Self {
        ConsumerOffset::KeyPos(decode_backfill_cursor(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CURSOR_TAG, ConsumerOffset, cursor_key, decode_backfill_cursor, decode_watermark,
        encode_backfill_cursor, encode_watermark, parse_cursor_key,
    };
    use crate::hlc::HlcTimestamp;
    use animus_tablet::TOKEN_BYTES;

    #[test]
    fn round_trips_through_build_and_parse() {
        let range_start = b"abcdefgh-and-then-more-row-key-bytes";
        let key = cursor_key(range_start, "gsi");
        let (token, tag) = parse_cursor_key(&key).expect("a well-formed key must parse");
        assert_eq!(token, &range_start[..TOKEN_BYTES]);
        assert_eq!(tag, "gsi");
    }

    #[test]
    fn zero_pads_a_short_range_start() {
        // The ring's own first tablet has an empty (unbounded) start.
        let key = cursor_key(b"", "copier");
        let (token, tag) = parse_cursor_key(&key).expect("must parse");
        assert_eq!(token, [0u8; TOKEN_BYTES]);
        assert_eq!(tag, "copier");

        let key2 = cursor_key(b"ab", "copier");
        let (token2, _) = parse_cursor_key(&key2).expect("must parse");
        assert_eq!(token2, [b'a', b'b', 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn distinct_tags_never_collide_for_the_same_token() {
        let range_start = b"same-tablet-boundary";
        let gsi = cursor_key(range_start, "gsi");
        let copier = cursor_key(range_start, "copier");
        assert_ne!(gsi, copier);
    }

    #[test]
    fn rejects_too_short_input() {
        assert_eq!(parse_cursor_key(&[0u8; TOKEN_BYTES + 1]), None);
        assert_eq!(parse_cursor_key(&[]), None);
    }

    #[test]
    fn rejects_a_key_missing_the_lead_pair() {
        let mut bytes = vec![0u8; TOKEN_BYTES];
        bytes.extend_from_slice(b"gsi"); // no [0x00, CURSOR_TAG] at all
        assert_eq!(parse_cursor_key(&bytes), None);

        // Right first byte, wrong second byte (not CURSOR_TAG).
        let mut bytes = vec![0u8; TOKEN_BYTES];
        bytes.push(0x00);
        bytes.push(CURSOR_TAG.wrapping_add(1));
        bytes.extend_from_slice(b"gsi");
        assert_eq!(parse_cursor_key(&bytes), None);
    }

    #[test]
    fn rejects_a_non_utf8_tag() {
        let mut bytes = vec![0u8; TOKEN_BYTES];
        bytes.push(0x00);
        bytes.push(CURSOR_TAG);
        bytes.push(0xFF); // invalid UTF-8 on its own
        assert_eq!(parse_cursor_key(&bytes), None);
    }

    #[test]
    fn watermark_round_trips() {
        let ts = HlcTimestamp {
            wall_ms: 123_456_789,
            logical: 42,
        };
        let bytes = encode_watermark(ts);
        assert_eq!(bytes.len(), 8, "a packed HLC watermark is 8 bytes");
        assert_eq!(decode_watermark(&bytes), Some(ts));
    }

    #[test]
    fn watermark_rejects_the_wrong_length() {
        assert_eq!(decode_watermark(&[0u8; 7]), None);
        assert_eq!(decode_watermark(&[0u8; 9]), None);
        assert_eq!(decode_watermark(&[]), None);
    }

    #[test]
    fn backfill_cursor_round_trips_an_arbitrary_length_prefix() {
        let prefix = b"\x01\x02\x03\x04\x05\x06\x07\x08some-partition-prefix\x00\x00";
        let bytes = encode_backfill_cursor(prefix);
        assert_eq!(bytes, prefix);
        assert_eq!(decode_backfill_cursor(&bytes), prefix);
    }

    #[test]
    fn backfill_cursor_and_watermark_use_the_same_key_scheme_under_distinct_tags() {
        let range_start = b"same-tablet-boundary";
        let watermark_row = cursor_key(range_start, "gsi");
        let backfill_row = cursor_key(range_start, "backfill:by-status");
        assert_ne!(watermark_row, backfill_row);
        let (token, tag) = parse_cursor_key(&backfill_row).expect("must parse");
        assert_eq!(token, &range_start[..TOKEN_BYTES]);
        assert_eq!(tag, "backfill:by-status");
    }

    #[test]
    fn consumer_offset_watermark_round_trips_through_the_existing_convention() {
        let ts = HlcTimestamp {
            wall_ms: 987_654,
            logical: 7,
        };
        let offset = ConsumerOffset::Watermark(ts);
        let bytes = offset.encode();
        assert_eq!(
            bytes,
            encode_watermark(ts),
            "must delegate to encode_watermark, not a second encoding"
        );
        assert_eq!(ConsumerOffset::decode_watermark(&bytes), Some(offset));
    }

    #[test]
    fn consumer_offset_key_pos_round_trips_through_the_existing_convention() {
        let prefix = b"\x00\x00\x00\x00\x00\x00\x00\x01some-partition".to_vec();
        let offset = ConsumerOffset::KeyPos(prefix.clone());
        let bytes = offset.encode();
        assert_eq!(
            bytes,
            encode_backfill_cursor(&prefix),
            "must delegate to encode_backfill_cursor, not a second encoding"
        );
        assert_eq!(ConsumerOffset::decode_key_pos(&bytes), offset);
    }

    #[test]
    fn consumer_offset_decode_watermark_rejects_the_wrong_length_like_its_delegate() {
        assert_eq!(ConsumerOffset::decode_watermark(&[0u8; 7]), None);
    }
}
