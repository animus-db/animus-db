//! The **split-child trim-complete marker** (ADR 0058 Train 2 rung 3's G4
//! contract, closing issue's own fix): the durable half of `host.rs`'s
//! `trim_split_child`, mirroring `seal.rs`/`ceiling.rs`'s discipline exactly
//! (an engine-global key under `RESERVED_NAMESPACE`, surviving compaction —
//! though unlike those two this key is never read by the apply path at all,
//! only by the reconciler itself, before a child's Raft group ever starts).
//!
//! # Why this exists
//!
//! `host.rs::materialize_split_child` clones a split child's engine from its
//! parent, then trims it (drops the sibling's own BASE/LSI/FOOTPRINT range
//! and the whole CHANGE/CURSOR scopes — see `trim_split_child`'s own doc) —
//! **two separate steps**, only the first of which the pre-existing G4
//! contract had a durable completion signal for
//! (`EngineFactory::probe(child.id)`). A crash — or a real trim failure, e.g.
//! a faulty `delete_range` — between the two steps left `probe` reporting
//! "cloned" with no way to tell that trim itself never finished. The
//! `already_cloned` resume branch trusted `probe` alone and skipped straight
//! to reopening the engine and starting the group, **permanently** serving
//! the sibling's rows and leaking the parent's whole change log/cursors
//! across the split (violating ADR 0046 principle 3, "no consumer offset
//! ever crosses a split") — reproduced by `tests/split_trim_failure.rs`.
//!
//! The fix adds a second, more specific durable signal: this marker, written
//! by `trim_split_child` as its own **last** step, once every `delete_range`
//! call has already succeeded. `materialize_split_child`'s resume branch now
//! checks it (not `probe` alone): marker present means trim genuinely
//! completed; marker absent means it did not — safe to conclude because
//! **trim always finishes, marker included, strictly before this function
//! ever starts the child's own Raft group** (`RaftKvNode::start_hosted*` is
//! only ever called after `trim_split_child` returns `Ok`), so an absent
//! marker proves the child's group has never run and re-running trim's
//! (idempotent) `delete_range` calls can never clobber real committed group
//! state — see `trim_split_child`'s own doc for the sequencing this
//! argument rests on.

use animus_control::syskv::RESERVED_NAMESPACE;
use animus_tablet::escape;

/// The segment distinguishing this marker from `seal.rs`'s `SEAL_TAG`,
/// `ceiling.rs`'s `CEILING_TAG`, `split.rs`'s `SPLIT_TAG`, `hwm.rs`'s
/// `HWM_TAG`, `applied.rs`'s `APPLIED_TAG`, and every `syskv::EntityKind`
/// segment — chosen not to be a prefix of, or share a prefix with, any of
/// them.
const TRIM_TAG: &[u8] = b"cp_split_trim";

/// The physical, engine-global key recording that `child`'s own split-trim
/// step (`trim_split_child`) has fully completed. Provable disjointness from
/// every table's own physical keys and every other marker mirrors
/// `ceiling.rs::ceiling_marker_key`'s argument exactly (same
/// `RESERVED_NAMESPACE` prefix, same `escape` injective/prefix-free
/// property, distinct tag segment) — see that module's doc for the full
/// proof. Keyed by `child` alone: a tablet is trimmed by this mechanism at
/// most once (a split child never re-clones once its group has started).
pub(crate) fn trim_marker_key(child: u64) -> Vec<u8> {
    let mut out = escape(RESERVED_NAMESPACE.as_bytes());
    out.extend_from_slice(&escape(TRIM_TAG));
    out.extend_from_slice(&child.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_tablet::escape;

    #[test]
    fn distinct_children_get_distinct_keys() {
        assert_ne!(trim_marker_key(1), trim_marker_key(2));
    }

    #[test]
    fn key_disjoint_from_table_scope_prefixes() {
        for table in ["", "users", "orders"] {
            let table_prefix = escape(table.as_bytes());
            let marker = trim_marker_key(7);
            assert!(
                !marker.starts_with(&table_prefix) || table_prefix.is_empty(),
                "trim marker must not fall inside table {table:?}'s own scope"
            );
        }
    }

    #[test]
    fn key_disjoint_from_sibling_markers() {
        let ns_prefix = escape(RESERVED_NAMESPACE.as_bytes());
        let trim_key = trim_marker_key(7);
        assert!(trim_key.starts_with(&ns_prefix));
        for other_tag in [
            &b"cp_seal"[..],
            &b"cp_ceiling"[..],
            &b"cp_split"[..],
            &b"cp_hlc_hwm"[..],
            &b"cp_applied"[..],
        ] {
            let other_escaped = escape(other_tag);
            let rest = &trim_key[ns_prefix.len()..];
            assert!(
                !rest.starts_with(&other_escaped),
                "trim marker tag must not collide with {other_tag:?}'s escaped tag"
            );
        }
    }
}
