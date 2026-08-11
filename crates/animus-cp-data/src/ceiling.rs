//! The **committed read ceiling**'s durable marker (ADR 0018 §2/PR2b): the
//! leader-change/restart safety net for the read-timestamp cache
//! (`ts_cache.rs`).
//!
//! A leader-local `Hlc`/`TsCache` dies with the process; the fix is
//! **ordering-based**, mirroring `seal.rs`'s range seal: a leader that wants
//! to serve reads at or above its current ceiling proposes
//! `KvCommand::ReadCeiling { ts }` through its own Raft log first, and no
//! leader may ever serve a read at a timestamp not strictly below the
//! highest `ReadCeiling` **committed** in its group's log (`lib.rs`'s
//! `committed_ceiling`). Every applied entry's `ts` (including a
//! `ReadCeiling`'s) is folded into the group's `Hlc` via the same witnessing
//! chain PR2 already built for MVCC-version ordering (`command_ts` covers
//! every mutating-or-not variant uniformly), so a live process that merely
//! changes leaders — no restart — never loses this fact: it already
//! witnessed the ceiling's `ts` the moment it received the `AppendEntries`
//! carrying it, as a follower, before it could ever campaign.
//!
//! The residual case witnessing-via-message-receipt cannot cover is a
//! **process restart after compaction has already truncated the
//! `ReadCeiling` entry out of the log** (a read-only workload can accumulate
//! many ceiling proposals with no interleaved write, so `engine_applied` —
//! and hence the compaction trigger — advances past them same as any other
//! applied entry). `seal.rs` closed the analogous gap for range seals with a
//! durable **engine marker key**; this module is the same fix, minimized to
//! a single per-tablet key (no per-range keying needed — there is exactly
//! one ceiling per tablet, always overwritten with the newest value): apply
//! writes it via an ordinary `merge` (per-key LWW, so a monotonically
//! increasing `ts` — guaranteed, since a leader's own `Hlc::mint` only ever
//! increases — always wins), which durably raises the shared engine's own
//! `latest_version()`. That means the **existing** group-start witness
//! (`hlc.witness(hlc::unpack(storage.latest_version()), ..)`, already in
//! `RaftKvNode::start_inner`) automatically re-derives a floor above the
//! ceiling on any future restart, with zero further changes to the
//! witnessing chain — the marker's whole job is to make sure that one
//! already-existing witness point has something to see.

use animus_control::syskv::RESERVED_NAMESPACE;
use animus_tablet::escape;

use crate::hlc::HlcTimestamp;

/// The segment distinguishing this crate's ceiling marker from `seal.rs`'s
/// own marker and the control plane's system-keyspace entities, all of which
/// can share the same `RESERVED_NAMESPACE` prefix on a **combined** node
/// (ADR 0026/0028's shared-engine model). Chosen not to collide with any
/// `syskv::EntityKind::as_str()` segment or `seal.rs`'s `SEAL_TAG`.
const CEILING_TAG: &[u8] = b"cp_ceiling";

/// The physical, engine-global key holding `tablet`'s committed read
/// ceiling. **Provable disjointness from every table's own physical keys**
/// mirrors `seal.rs::seal_marker_key`'s argument exactly (same
/// `RESERVED_NAMESPACE` prefix, same `escape` injective/prefix-free
/// property) — see that module's doc for the full proof; the only
/// difference is the tag segment (`CEILING_TAG` vs `SEAL_TAG`), which keeps
/// the two markers' key spaces disjoint from each other too (neither tag is
/// a prefix of the other).
///
/// Unlike the seal marker, this is keyed by **`tablet` alone** — there is
/// exactly one ceiling per tablet, always overwritten in place (a
/// re-proposed `ReadCeiling` simply merges a newer `ts` over the same key,
/// per-key LWW), never one-per-range.
pub(crate) fn ceiling_marker_key(tablet: u64) -> Vec<u8> {
    let mut out = escape(RESERVED_NAMESPACE.as_bytes());
    out.extend_from_slice(&escape(CEILING_TAG));
    out.extend_from_slice(&tablet.to_be_bytes());
    out
}

/// The value stored at the ceiling marker: just the timestamp — the key
/// alone already identifies which tablet, and there is no range to record.
pub(crate) fn encode_ceiling_value(ts: HlcTimestamp) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    out.extend_from_slice(&ts.wall_ms.to_be_bytes());
    out.extend_from_slice(&ts.logical.to_be_bytes());
    out
}

/// The exact inverse of [`encode_ceiling_value`]. `None` on malformed input —
/// an engine-internal marker this crate itself wrote should never be
/// malformed; a decode failure indicates real corruption, surfaced loudly by
/// the caller rather than silently misread.
pub(crate) fn decode_ceiling_value(bytes: &[u8]) -> Option<HlcTimestamp> {
    if bytes.len() != 12 {
        return None;
    }
    let wall_ms = u64::from_be_bytes(bytes[0..8].try_into().ok()?);
    let logical = u32::from_be_bytes(bytes[8..12].try_into().ok()?);
    Some(HlcTimestamp { wall_ms, logical })
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_tablet::escape;

    #[test]
    fn ceiling_value_round_trips() {
        for ts in [
            HlcTimestamp::zero(),
            HlcTimestamp {
                wall_ms: 12_345,
                logical: 7,
            },
            HlcTimestamp {
                wall_ms: u64::MAX >> 20,
                logical: (1 << 20) - 1,
            },
        ] {
            let bytes = encode_ceiling_value(ts);
            assert_eq!(decode_ceiling_value(&bytes), Some(ts));
        }
    }

    #[test]
    fn decode_rejects_malformed_input() {
        assert_eq!(decode_ceiling_value(&[1, 2, 3]), None);
        assert_eq!(decode_ceiling_value(&[]), None);
    }

    #[test]
    fn distinct_tablets_get_distinct_keys() {
        assert_ne!(ceiling_marker_key(1), ceiling_marker_key(2));
    }

    #[test]
    fn key_disjoint_from_table_scope_prefixes_and_the_seal_marker() {
        for table in ["", "users", "orders"] {
            let table_prefix = escape(table.as_bytes());
            let marker = ceiling_marker_key(7);
            assert!(
                !marker.starts_with(&table_prefix) || table_prefix.is_empty(),
                "ceiling marker must not fall inside table {table:?}'s own scope"
            );
        }
        // Disjoint from seal.rs's own marker namespace (distinct tag segments,
        // neither a prefix of the other — "cp_ceiling" vs "cp_seal").
        let ceiling_ns = ceiling_marker_key(7);
        let seal_tag_escaped = escape(b"cp_seal");
        let ns_prefix = escape(RESERVED_NAMESPACE.as_bytes());
        assert!(!ceiling_ns[ns_prefix.len()..].starts_with(&seal_tag_escaped));
    }
}
