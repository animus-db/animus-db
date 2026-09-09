//! The durable **HLC apply high-water-mark marker** (issue #804, ADR 0018 §2
//! amendment): the local-restart safety net for the group-start witness
//! (`RaftKvNode::start_inner`), mirroring `ceiling.rs`'s marker exactly but
//! generalized to every ts-bearing entry, not just `ReadCeiling`.
//!
//! # The gap this closes
//!
//! Every mutating `KvCommand`'s apply is supposed to keep the group's `Hlc`
//! from ever minting below anything already committed, via **witnessing**
//! (`hlc.rs`'s module doc, `lib.rs`'s Key invariants "Witnessing" bullet).
//! Two of the four witness points read back *engine* state rather than
//! scanning committed *log* entries — `storage.latest_version()`, at group
//! start and at `InstallSnapshot` install — on the assumption that the
//! engine's own highest written MVCC version is always at least as high as
//! the highest `ts` any committed entry ever carried. That assumption breaks
//! for a committed-and-applied entry whose outcome writes **no row at all**:
//! a `Cas` whose `expected` never matched, a condition-failed `KindBatch`/
//! `KindEval`, an aborted transaction, `Freeze`/`SplitTablet`/`ReadCeiling`
//! themselves absent their own durable marker. Such an entry's `ts` is real
//! (`assert_ts_monotonic` runs on it) but `storage.latest_version()` never
//! moves for it — so once the log is compacted past it, WAL replay
//! (`witness_append_entries`'s sibling at recovery) can no longer see it
//! either, and the group-start witness silently undercounts.
//!
//! # The fix
//!
//! Exactly `ceiling.rs`'s trick, generalized: durably `merge` a single
//! per-tablet marker key, engine-global (`RESERVED_NAMESPACE`-prefixed,
//! matching no row kind), at MVCC version `hlc::pack(max_applied_ts)` —
//! `max_applied_ts` being the apply task's own running high-water mark
//! (the same value `assert_ts_monotonic` maintains, threaded through
//! `apply_and_compact`). Since `merge`'s per-key LWW unconditionally raises
//! the engine's *global* `manifest.max_version` (`LsmEngine::merge`) the
//! instant this key's own version increases, this durably makes
//! `storage.latest_version()` reflect the true committed max — including
//! every no-op entry — with **zero further changes** to the existing
//! group-start witness call, exactly as `ceiling.rs`'s own doc argues for
//! its narrower case.
//!
//! Written at **compaction time** (`apply_and_compact`'s WAL-rewrite block),
//! not on every ordinary apply pass — mirroring `applied.rs`'s own
//! `applied_marker_key` cadence and for the identical two reasons (see that
//! call site's comment): between compactions, WAL recovery replay alone
//! already witnesses every entry (compacted or not) unconditionally, so a
//! marker write on every commit would only add SSTable dead-space risk with
//! no correctness gain; only once a compaction truncates the WAL prefix does
//! this marker's write become the only remaining witness for whatever no-op
//! entries that prefix contained. Written **before** the WAL rewrite, same
//! direction `applied_marker_key` is: a crash in between leaves the marker
//! caught up but `snapshot_index` still at its old (lower) value — never a
//! replica that believes it witnessed more than its own un-truncated WAL can
//! still prove on replay.
//!
//! # Why the `InstallSnapshot`-to-a-different-replica half needs its own path
//!
//! `engine_image`'s scan (this crate's snapshot-image builder) only
//! classifies rows that fall inside one of `ALL_KINDS`' own scopes — every
//! `RESERVED_NAMESPACE` marker, this one included, is deliberately excluded
//! (see that function's own doc), so *this sender's own* marker row never
//! crosses via a snapshot the way a real row would. That half is closed by
//! carrying `max_applied_ts` in the image's own header instead of as a row
//! (`codec::encode_image`/`decode_image`, version `29`), folded with the
//! sender's own `storage.latest_version()` at image-build time so a sender
//! that restarted since its last apply still ships the true mark (its
//! `apply_and_compact`'s own doc, issue #804's follow-up finding) — see
//! `lib.rs`'s `engine_image`/`install_engine_image` doc. **The receiver, on
//! install, mints its own fresh marker row from that header value** in the
//! same `merge_batch` as the rows (`install_engine_image`'s own doc) —
//! necessary because an in-memory-only `hlc.witness` of the header would be
//! forgotten by a restart of the *receiver* itself before its own next
//! compaction, at which point this replica's WAL has none of the sender's
//! compacted-away entries to fall back on replaying.

use animus_control::syskv::RESERVED_NAMESPACE;
use animus_tablet::escape;

use crate::hlc::HlcTimestamp;

/// The segment distinguishing this marker from `seal.rs`'s/`ceiling.rs`'s
/// own markers and the control plane's system-keyspace entities, all
/// sharing the same `RESERVED_NAMESPACE` prefix on a combined node. Chosen
/// not to collide with any `syskv::EntityKind::as_str()` segment or the
/// other markers' own tags.
const HWM_TAG: &[u8] = b"cp_hlc_hwm";

/// The physical, engine-global key holding `tablet`'s durable HLC apply
/// high-water mark. Disjointness from every table's own physical keys and
/// from `seal.rs`/`ceiling.rs`/`split.rs`'s own markers follows the
/// identical argument as `ceiling.rs::ceiling_marker_key` (same
/// `RESERVED_NAMESPACE` prefix, same `escape` injective/prefix-free
/// property, a tag segment that is not a prefix of — and is not prefixed
/// by — any other marker's own tag).
pub(crate) fn hwm_marker_key(tablet: u64) -> Vec<u8> {
    let mut out = escape(RESERVED_NAMESPACE.as_bytes());
    out.extend_from_slice(&escape(HWM_TAG));
    out.extend_from_slice(&tablet.to_be_bytes());
    out
}

/// The value stored at the marker: just the timestamp, for admin/debug
/// legibility — production logic only ever relies on the *version* this
/// marker's `merge` call durably raises `storage.latest_version()` to,
/// never on decoding this value back.
pub(crate) fn encode_hwm_value(ts: HlcTimestamp) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    out.extend_from_slice(&ts.wall_ms.to_be_bytes());
    out.extend_from_slice(&ts.logical.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hwm_marker_key_is_disjoint_from_the_ceiling_and_seal_markers() {
        let hwm = hwm_marker_key(7);
        let ceiling = crate::ceiling::ceiling_marker_key(7);
        assert_ne!(hwm, ceiling);
        assert!(!hwm.starts_with(&ceiling) && !ceiling.starts_with(&hwm));
    }

    #[test]
    fn hwm_marker_key_is_stable_and_tablet_scoped() {
        assert_eq!(hwm_marker_key(1), hwm_marker_key(1));
        assert_ne!(hwm_marker_key(1), hwm_marker_key(2));
    }
}
