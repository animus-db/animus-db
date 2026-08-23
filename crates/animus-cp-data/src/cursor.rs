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
//! roadmap expects more consumer kinds to be named as a follow-up.
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
//! [`cursor_key`] returns `range_start (verbatim, untruncated) || [0x00,
//! CURSOR_TAG] || consumer.as_bytes() || len(range_start) as a 2-byte
//! big-endian trailer`, where `range_start` is this tablet's own live
//! `KeyRange::start` — the **whole** value, never truncated. `CURSOR_TAG`
//! (`0x03`) is the next unused value after `seal.rs`'s marker (which
//! doesn't share this scope at all) and `txn.rs`'s own `RECORD_TAG`
//! (`0x02`); the leading-bytes-then-tag shape otherwise mirrors `txn.rs`'s
//! `record_key` scheme (`token || [0x00, TAG] || id`), except a cursor key
//! carries its own length trailer instead of relying on `TAG` living at a
//! fixed byte offset (see "Why untruncated, and why a trailer" below for
//! why a cursor row needs that where a txn record doesn't).
//!
//! Unlike a txn record, a cursor row lives in its **own** row-kind scope
//! (`KIND_CURSOR`), physically disjoint from `KIND_BASE` by construction
//! (every kind is a distinct `StorageScope` prefix — see `StorageScope::
//! with_kind`'s own doc) — so cursor rows can never collide with a real
//! client key at all, regardless of byte content, and [`parse_cursor_key`]
//! only ever has to disambiguate rows this crate itself wrote.
//!
//! **Why untruncated, and why a trailer, not a fixed offset (ADR fix for
//! issue #355).** A prior revision of this scheme truncated `range_start`
//! to a fixed [`TOKEN_BYTES`]-wide prefix, which let [`parse_cursor_key`]
//! recover `(token, tag)` at a fixed byte offset — but a tablet's
//! `range.start` is a split's own `split_key`, and a real split point is
//! chosen from live row content (`byte_weighted_median`), essentially
//! never `TOKEN_BYTES` long. Every writer of a cursor row that ever reaches
//! `animusd::ClientCtx`'s key-routed write path (`cp_kind_write_raw`,
//! which resolves the target tablet by comparing the write's own key
//! bytes against each tablet's declared `range` — see
//! `animusd::topology::tablet_for_key`) needs its own cursor key to
//! lexicographically fall inside `[range.start, range.end)`, and a
//! truncated key can sort *below* `range.start` the instant the byte just
//! past the truncation point is non-zero — true of almost any real split
//! child, which silently misrouted (not rejected) the write onto a
//! sibling tablet instead. Emitting `range_start` **verbatim** as the
//! key's own leading bytes fixes that structurally: the key is `range_start`
//! extended with more bytes, so it always compares strictly greater
//! (`key > range_start`, satisfying the inclusive lower bound) — the
//! `>= range.start` half of every consumer's containment check is now true
//! by construction, not by luck.
//!
//! That leaves recovering `tag` from a key whose leading `range_start`
//! component is now variable-length and untrusted-content (it's real row
//! bytes, not a value this module controls) — a fixed byte offset no
//! longer works, and scanning forward for the `[0x00, CURSOR_TAG]` marker
//! would need the same escape-discipline argument the old scheme used to
//! avoid (fragile, and this module shouldn't have to reason about
//! `animus_dynamo::escape`'s encoding to stay correct). [`cursor_key`]
//! instead appends `range_start`'s own byte length as a fixed 2-byte
//! big-endian **trailer** — the last thing in the key — so
//! [`parse_cursor_key`] reads the trailer first (an O(1), always-present
//! fixed-width suffix) and then slices `range_start`/`tag` from a length it
//! now knows exactly, no content scanning or escape argument required.
//! `u16` is generous headroom over any real DynamoDB key (partition +
//! sort key stay in the low kilobytes); [`cursor_key`] hard-`expect`s
//! rather than silently truncating if this is ever violated, mirroring
//! `hlc::pack`'s own doctrine that a silent encoding overflow must never
//! quietly collapse distinct values.
//!
//! **A residual, documented gap, narrower than before**: it is still not
//! proven that `range_start || [0x00, CURSOR_TAG] || consumer || len` is
//! always strictly less than this tablet's own `range.end` — only a
//! pathological `range.end` that happens to extend `range_start` with
//! *exactly* the same marker/tag/trailer bytes a real consumer would write
//! could violate it, vanishingly unlikely for any real split boundary
//! (itself real row content, `byte_weighted_median`-chosen) to reproduce
//! byte-for-byte. Left for a future PR's corpus to stress, exactly as the
//! prior revision of this doc deferred the analogous (and much easier to
//! trigger) truncation gap, and as `txn.rs` defers its own split-alignment
//! gap.
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
//! shape without hard-coding which one its own tag uses (see this doc's
//! own intro for the future-consumer-kinds roadmap). It delegates to the existing
//! `encode_watermark`/`decode_watermark`/`encode_backfill_cursor`/
//! `decode_backfill_cursor` free functions rather than re-implementing
//! either encoding — those functions, and every existing caller of them,
//! are unchanged.
//!
//! ## Split classification (ADR 0046 third as-built amendment; ADR 0050)
//!
//! Under copy-based splits (ADR 0050) a child is born with empty cursor and
//! change-log scopes, so **every** consumer offset is
//! [`SplitPolicy::RestartFromScratch`] — ADR 0046 principle 3 strengthened
//! to "no consumer offset ever crosses a split":
//!
//! | Consumer tag | `SplitPolicy` | Why |
//! |---|---|---|
//! | `"gsi"` | [`SplitPolicy::RestartFromScratch`] | A copy-based split child's cursor scope starts empty (CURSOR is never seeded, ADR 0050 rung 4); the drain's reconcile sweep is idempotent, so it restarts over the child's own range — ADR 0045 §5 Fork A/F1's argument, now structural. |
//! | `"backfill:{index_name}"` | [`SplitPolicy::RestartFromScratch`] | Identical reasoning: the backfill seeder walks `KIND_BASE` and a fresh child's empty cursor just means "re-sweep this (narrower) range from the start." |
//! | the stream seal watermark | [`SplitPolicy::RestartFromScratch`] | A child's change log is born empty and its shard chain starts at its own epoch 0; the parent's chain is closed by the pre-cutover final seal, with lineage frozen in `Metadata::split_lineage` (fork F9) — no watermark inheritance exists to classify (the zero-copy design's `InheritFrozenBasis` policy retired with `stream_split_basis`, Train B rung 7). |
//!
//! [`classify_tag`] enumerates the `KIND_CURSOR` side of this table in code
//! (checked by this module's own `every_known_cursor_tag_prefix_is_
//! classified` test) — a new consumer tag must earn a deliberate entry
//! here before it ships, not fall through silently.

use crate::hlc::{self, HlcTimestamp};

/// The second byte of a cursor row key's lead pair (see the module doc's
/// disjointness proof) — the next unused tag after `txn.rs`'s own
/// `RECORD_TAG` (`0x02`); `seal.rs`/`ceiling.rs`'s markers live outside every
/// `StorageScope` entirely, so they never share this numbering.
const CURSOR_TAG: u8 = 0x03;

/// This tablet's own cursor-row key for `consumer` (see the module doc for
/// the full scheme + disjointness proof). `range_start` is the tablet's
/// live `KeyRange::start` at the moment of the call (`RaftKvNode::
/// scope_range().start`) — embedded **verbatim, untruncated** (issue #355:
/// a truncated token could sort below a non-token-aligned `range_start`,
/// silently misrouting a routed write onto a sibling tablet).
#[must_use]
pub fn cursor_key(range_start: &[u8], consumer: &str) -> Vec<u8> {
    let len = u16::try_from(range_start.len())
        .expect("a tablet's own range_start must fit a u16-length cursor-key trailer");
    let mut out = Vec::with_capacity(range_start.len() + 2 + consumer.len() + 2);
    out.extend_from_slice(range_start);
    out.push(0x00);
    out.push(CURSOR_TAG);
    out.extend_from_slice(consumer.as_bytes());
    out.extend_from_slice(&len.to_be_bytes());
    out
}

/// Recover `(range_start, consumer)` from a raw `KIND_CURSOR`-scoped key, or
/// `None` if it isn't shaped like one (a too-short trailer, wrong lead pair,
/// or a non-UTF8 tag — this crate only ever writes tags it itself minted as
/// valid UTF-8 strings, so a decode failure here is a defensive read, not an
/// expected case). See the module doc for why the trailing length (rather
/// than a fixed byte offset) is what makes this unambiguous now that
/// `range_start` is embedded at its own real, variable length.
#[must_use]
pub fn parse_cursor_key(key: &[u8]) -> Option<(&[u8], &str)> {
    if key.len() < 2 {
        return None;
    }
    let (body, len_bytes) = key.split_at(key.len() - 2);
    let range_start_len = u16::from_be_bytes([len_bytes[0], len_bytes[1]]) as usize;
    if body.len() < range_start_len + 2 {
        return None;
    }
    let (range_start, rest) = body.split_at(range_start_len);
    if rest[0] != 0x00 || rest[1] != CURSOR_TAG {
        return None;
    }
    let tag = std::str::from_utf8(&rest[2..]).ok()?;
    Some((range_start, tag))
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
/// own future-consumer-kinds roadmap) — no current caller constructs one.
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

/// What a split does to one consumer's offset (ADR 0046 principle 3, third
/// as-built amendment) — see the module doc's "Split classification" table
/// for the full per-tag rationale.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SplitPolicy {
    /// The child's cursor row for this tag reads empty right after a split
    /// (this crate's own `KIND_CURSOR` scheme guarantees it — `cursor_key`
    /// embeds `range.start`, and `narrow_scope` never moves rows), and the
    /// consumer simply restarts its sweep/reconcile from scratch over its
    /// own, strictly narrower range. Safe by construction: every consumer
    /// using this policy is independently idempotent (a full re-run is
    /// always harmless), so "start over" costs bounded extra work, never
    /// correctness.
    RestartFromScratch,
}

/// Classify a `KIND_CURSOR` tag by its [`SplitPolicy`] — `None` for a tag
/// this crate doesn't yet know about. See the module doc's classification
/// table for the reasoning behind each entry, and this module's own
/// `every_known_cursor_tag_prefix_is_classified` test, which exists so a
/// **new** consumer tag fails a test here — with a message pointing back
/// at this function and the table above — instead of silently shipping
/// with no split-behavior decision on record.
#[must_use]
pub fn classify_tag(tag: &str) -> Option<SplitPolicy> {
    if tag == GSI_CURSOR_TAG_FOR_CLASSIFICATION {
        return Some(SplitPolicy::RestartFromScratch);
    }
    if tag.starts_with(BACKFILL_CURSOR_TAG_PREFIX_FOR_CLASSIFICATION) {
        return Some(SplitPolicy::RestartFromScratch);
    }
    None
}

/// The `"gsi"` tag, restated here (rather than imported from
/// `animusd::index_drain::GSI_TAG`, which this crate cannot depend on —
/// `animus-cp-data` sits *below* `animusd` in the dependency graph) so
/// [`classify_tag`] has a named constant instead of a bare literal. Must
/// stay byte-identical to that crate's own `GSI_TAG`; the module doc's own
/// "Two value conventions" section documents this same string independently
/// for the same reason.
const GSI_CURSOR_TAG_FOR_CLASSIFICATION: &str = "gsi";

/// The `"backfill:"` tag prefix, restated here for the same reason
/// [`GSI_CURSOR_TAG_FOR_CLASSIFICATION`] is — must stay byte-identical to
/// `animusd::index_drain::backfill_tag`'s own `format!("backfill:{index_name}")`.
const BACKFILL_CURSOR_TAG_PREFIX_FOR_CLASSIFICATION: &str = "backfill:";

#[cfg(test)]
mod tests {
    use super::{
        CURSOR_TAG, ConsumerOffset, SplitPolicy, classify_tag, cursor_key, decode_backfill_cursor,
        decode_watermark, encode_backfill_cursor, encode_watermark, parse_cursor_key,
    };
    use crate::hlc::HlcTimestamp;

    #[test]
    fn round_trips_through_build_and_parse() {
        let range_start = b"abcdefgh-and-then-more-row-key-bytes";
        let key = cursor_key(range_start, "gsi");
        let (start, tag) = parse_cursor_key(&key).expect("a well-formed key must parse");
        assert_eq!(start, &range_start[..]);
        assert_eq!(tag, "gsi");
    }

    #[test]
    fn embeds_a_short_range_start_verbatim() {
        // The ring's own first tablet has an empty (unbounded) start.
        let key = cursor_key(b"", "copier");
        let (start, tag) = parse_cursor_key(&key).expect("must parse");
        assert_eq!(start, b"");
        assert_eq!(tag, "copier");

        let key2 = cursor_key(b"ab", "copier");
        let (start2, _) = parse_cursor_key(&key2).expect("must parse");
        assert_eq!(start2, b"ab");
    }

    /// Issue #355's own precondition: a real split's `range.start` is chosen
    /// from live row content (`byte_weighted_median`), essentially never
    /// `TOKEN_BYTES` long — this must round-trip exactly like any other
    /// length, with nothing special happening at that one width.
    #[test]
    fn embeds_a_non_token_aligned_range_start_verbatim() {
        let range_start = b"token(8b)+a-real-row-key-tail-thats-longer-than-a-bare-token";
        assert_ne!(
            range_start.len(),
            8,
            "must exercise a non-token-aligned width"
        );
        let key = cursor_key(range_start, "gsi");
        let (start, tag) = parse_cursor_key(&key).expect("must parse");
        assert_eq!(start, &range_start[..]);
        assert_eq!(tag, "gsi");
    }

    /// The routing-correctness property issue #355 needed: a cursor key must
    /// lexicographically sort **at or above** its own tablet's `range.start`
    /// (`KeyRange::contains`'s inclusive lower bound), for every length —
    /// never just at `TOKEN_BYTES`. True by construction now: the key is
    /// `range_start` extended with more bytes, never truncated.
    #[test]
    fn cursor_key_never_sorts_below_its_own_range_start() {
        for range_start in [
            &b""[..],
            &b"ab"[..],
            &b"exactly8"[..],
            &b"a real non-token-aligned split boundary key, quite long"[..],
        ] {
            for tag in ["gsi", "backfill:by-status", "copier"] {
                let key = cursor_key(range_start, tag);
                assert!(
                    key.as_slice() >= range_start,
                    "cursor_key({range_start:?}, {tag:?}) = {key:?} sorted below its own \
                     range_start"
                );
            }
        }
    }

    #[test]
    fn distinct_tags_never_collide_for_the_same_range_start() {
        let range_start = b"same-tablet-boundary";
        let gsi = cursor_key(range_start, "gsi");
        let copier = cursor_key(range_start, "copier");
        assert_ne!(gsi, copier);
    }

    #[test]
    fn rejects_too_short_input() {
        assert_eq!(parse_cursor_key(&[0u8; 1]), None);
        assert_eq!(parse_cursor_key(&[]), None);
    }

    #[test]
    fn rejects_a_key_whose_trailer_overstates_its_own_length() {
        // A 2-byte trailer claiming a range_start longer than the rest of
        // the key actually has room for.
        let bytes = 0xFFFFu16.to_be_bytes().to_vec();
        assert_eq!(parse_cursor_key(&bytes), None);
    }

    #[test]
    fn rejects_a_key_missing_the_lead_pair() {
        let range_start = b"abc";
        let mut bytes = range_start.to_vec();
        bytes.extend_from_slice(b"gsi"); // no [0x00, CURSOR_TAG] at all
        bytes.extend_from_slice(&(range_start.len() as u16).to_be_bytes());
        assert_eq!(parse_cursor_key(&bytes), None);

        // Right first byte, wrong second byte (not CURSOR_TAG).
        let mut bytes = range_start.to_vec();
        bytes.push(0x00);
        bytes.push(CURSOR_TAG.wrapping_add(1));
        bytes.extend_from_slice(b"gsi");
        bytes.extend_from_slice(&(range_start.len() as u16).to_be_bytes());
        assert_eq!(parse_cursor_key(&bytes), None);
    }

    #[test]
    fn rejects_a_non_utf8_tag() {
        let range_start = b"abc";
        let mut bytes = range_start.to_vec();
        bytes.push(0x00);
        bytes.push(CURSOR_TAG);
        bytes.push(0xFF); // invalid UTF-8 on its own
        bytes.extend_from_slice(&(range_start.len() as u16).to_be_bytes());
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
        let (start, tag) = parse_cursor_key(&backfill_row).expect("must parse");
        assert_eq!(start, &range_start[..]);
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

    /// Every `KIND_CURSOR` tag prefix this codebase constructs today must
    /// have a [`SplitPolicy`] on record (ADR 0046's third as-built
    /// amendment) — found by grepping every `cursor_key`/`cursor::cursor_key`
    /// call site and cursor-tag constant across the workspace
    /// (`animusd::index_drain::GSI_TAG`/`backfill_tag`; no other production
    /// caller constructs a `KIND_CURSOR` tag as of this writing — the
    /// module doc's own "Two value conventions" section documents the same
    /// two). **If this test fails for a tag you just added**: that tag has
    /// shipped with no conscious decision about what a split does to its
    /// cursor row. Stop, read the module doc's "Split classification"
    /// table, pick [`SplitPolicy::RestartFromScratch`] (safe if your
    /// consumer's reconciliation is idempotent and a fresh child simply
    /// restarting is affordable) or argue for
    /// a deliberate policy decision (ADR 0046 principle 3 — under ADR 0050
    /// every offset restarts from scratch, so a new policy variant needs a
    /// design review first), add your tag to
    /// [`classify_tag`], and add it to this test's own list.
    #[test]
    fn every_known_cursor_tag_prefix_is_classified() {
        let exact_tags = ["gsi"];
        for tag in exact_tags {
            assert_eq!(
                classify_tag(tag),
                Some(SplitPolicy::RestartFromScratch),
                "cursor tag {tag:?} has no SplitPolicy classification — see \
                 this test's own doc comment"
            );
        }

        let prefixed_tag_samples = ["backfill:example-index", "backfill:by-status"];
        for tag in prefixed_tag_samples {
            assert_eq!(
                classify_tag(tag),
                Some(SplitPolicy::RestartFromScratch),
                "cursor tag {tag:?} has no SplitPolicy classification — see \
                 this test's own doc comment"
            );
        }
    }

    #[test]
    fn classify_tag_rejects_an_unrecognized_tag() {
        assert_eq!(
            classify_tag("some-future-consumer"),
            None,
            "an unclassified tag must classify as None, never silently \
             default to a policy"
        );
    }
}
