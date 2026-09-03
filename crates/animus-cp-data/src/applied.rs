//! The **engine-persisted applied watermark** (issue #554): the durable half
//! of `engine_applied` — the highest Raft log index this tablet's OWN engine
//! has actually merged, recorded as an ordinary key in the engine itself.
//!
//! Before this module existed, `drive()` seeded the in-memory `engine_applied`
//! atomic from `RaftCore::recovered`'s `core.last_applied()`, which
//! `recovered` sets to the replica's own **`snapshot_index`** (the log's own
//! compaction base — see `RaftCore::recovered`'s doc), never from anything
//! the engine itself durably attests to. That seed is a sound proxy only when
//! the engine and the log are both intact after a restart (the ordinary
//! case). It is unsound the moment the log survives a restart but the engine
//! does not (a wiped/rebuilt engine reopened fresh, `host.rs`'s
//! destroy-and-reopen recovery): the freshly-opened engine holds nothing, yet
//! the seed claimed it was caught up through `snapshot_index`, and because
//! the replica's log TAIL (the entries after `snapshot_index`) still matches
//! the leader's, `replicate_to`'s `next_index <= snapshot_index` check never
//! fires — the leader is never told this replica needs a fresh
//! `InstallSnapshot`, and everything the log's own compaction already
//! discarded (the whole prefix through `snapshot_index`) is silently gone
//! from this replica forever. **A log that matches the leader proves nothing
//! about the engine beneath it** — see `docs/engineering-lessons.md`'s
//! matching Code-patterns entry.
//!
//! The fix: the state machine's applied watermark must be the state
//! machine's own, not derived from the log. Rather than writing this marker
//! on every ordinary apply pass (mixed into the same batch as whatever
//! Put/Delete effects just merged), `apply_and_compact` writes it **only**
//! at the two moments `RaftCore::snapshot_index` itself can change: durably
//! (`storage.merge`, before the WAL rewrite) at compaction
//! (`RaftCore::snapshot_upto`, threshold-triggered or on-demand-image), and
//! in the SAME `merge_batch` as a received snapshot's rows at install
//! (`install_engine_image`). This is deliberately coarser than "every
//! commit" — the marker only ever needs to track `snapshot_index`, which
//! only moves at these two points, so writing it more often would buy
//! nothing; writing it into the SAME memtable batch as ordinary per-commit
//! rows was tried first and reverted (`docs/engineering-lessons.md`'s
//! matching entry) because it defeats `LsmEngine::clone_to_filtered`'s
//! whole-file split dead-space exclusion for any table it happens to ride
//! in (this marker's key sorts above every row kind's own byte range, so a
//! table carrying it no longer looks single-kind to that check) — never a
//! correctness bug (`trim_split_child` backstops it regardless), but a real
//! regression to the ADR 0058 dead-space win that would fire on nearly
//! every split instead of rarely. `drive()` reads the marker back at
//! startup (0 for a fresh/empty engine, exactly like every other
//! never-yet-written key) instead of trusting the log's own idea of where
//! compaction left off. See `RaftKvNode`'s "needs-snapshot" state
//! (`RaftCore::state_machine_behind`/`set_state_machine_behind`,
//! `animus-control::raft`) for what a replica does once this watermark is
//! found to be below its own recovered `snapshot_index`.
//!
//! Same disjointness proof as `seal.rs`/`ceiling.rs`'s own markers (an
//! engine-global key under `RESERVED_NAMESPACE`, distinguished from every
//! other marker by its own tag segment, none of which is a prefix of
//! another) — see `seal.rs`'s module doc for the full argument. Keyed by
//! `tablet` alone, exactly like `ceiling.rs`'s marker: there is exactly one
//! applied watermark per tablet, always overwritten in place with the
//! highest index this engine has merged (per-key LWW at that index as the
//! MVCC version, so it can only ever move forward).

use animus_control::syskv::RESERVED_NAMESPACE;
use animus_tablet::escape;

/// The segment distinguishing this crate's applied-watermark marker from
/// `seal.rs`'s (`"cp_seal"`), `ceiling.rs`'s (`"cp_ceiling"`), and the
/// control plane's own system-keyspace entities — none of which is a prefix
/// of `"cp_applied"` or vice versa.
const APPLIED_TAG: &[u8] = b"cp_applied";

/// The physical, engine-global key holding `tablet`'s durable applied
/// watermark: the highest Raft log index this tablet's own engine has
/// merged.
pub(crate) fn applied_marker_key(tablet: u64) -> Vec<u8> {
    let mut out = escape(RESERVED_NAMESPACE.as_bytes());
    out.extend_from_slice(&escape(APPLIED_TAG));
    out.extend_from_slice(&tablet.to_be_bytes());
    out
}

/// The value stored at the applied-watermark marker: just the index — the
/// key alone already identifies which tablet.
pub(crate) fn encode_applied_value(index: u64) -> Vec<u8> {
    index.to_be_bytes().to_vec()
}

/// The exact inverse of [`encode_applied_value`]. `None` on malformed
/// input — an engine-internal marker this crate itself wrote should never be
/// malformed; a decode failure indicates real corruption, surfaced loudly by
/// the caller (an `.expect()`, mirroring `seal.rs`/`ceiling.rs`'s own
/// discipline) rather than silently misread as "no watermark yet."
pub(crate) fn decode_applied_value(bytes: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(bytes.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_tablet::escape;

    #[test]
    fn applied_value_round_trips() {
        for index in [0u64, 1, 63, 64, 65, u64::MAX] {
            let bytes = encode_applied_value(index);
            assert_eq!(decode_applied_value(&bytes), Some(index));
        }
    }

    #[test]
    fn decode_rejects_malformed_input() {
        assert_eq!(decode_applied_value(&[1, 2, 3]), None);
        assert_eq!(decode_applied_value(&[]), None);
    }

    #[test]
    fn distinct_tablets_get_distinct_keys() {
        assert_ne!(applied_marker_key(1), applied_marker_key(2));
    }

    #[test]
    fn key_disjoint_from_table_scope_prefixes_and_the_other_markers() {
        for table in ["", "users", "orders"] {
            let table_prefix = escape(table.as_bytes());
            let marker = applied_marker_key(7);
            assert!(
                !marker.starts_with(&table_prefix) || table_prefix.is_empty(),
                "applied marker must not fall inside table {table:?}'s own scope"
            );
        }
        let ns_prefix = escape(RESERVED_NAMESPACE.as_bytes());
        let marker = applied_marker_key(7);
        for other_tag in [&b"cp_seal"[..], &b"cp_ceiling"[..]] {
            let other = escape(other_tag);
            assert!(!marker[ns_prefix.len()..].starts_with(&other));
        }
    }
}
