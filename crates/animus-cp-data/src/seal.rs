//! Range-seal markers (ADR 0018 §2 amendment, PR2): the structural
//! replacement for the retired `version_floor` cross-group-LWW fix.
//!
//! `version_floor` made *any* successor-group write beat *any* source-group
//! write structurally (by construction of the version number itself), even a
//! write still in the source's own commit pipeline that applies **late**
//! (after the successor already started serving). HLC witnessing alone can't
//! close that residual race — a witness can only react to what it has
//! already seen, and the in-flight write hasn't been seen by anyone yet — and
//! a timing bound is forbidden as a *correctness* mechanism (ADR 0017 §3).
//!
//! The replacement is **ordering-based**: when a source tablet hands off a
//! range (a split's `NarrowScope`), its own leader proposes a
//! [`KvCommand::Seal`](crate::KvCommand::Seal) for exactly that
//! range through its **own** Raft log. Every replica of that group applies
//! its log in the same order, so every replica agrees on the exact log
//! position the range became sealed — and any mutating entry ordered *after*
//! the seal, for a key inside the sealed range, is rejected at apply
//! (`crate::apply_and_compact`), regardless of how far behind the timestamp
//! embedded in that entry happens to be (an entry can only be *ordered*
//! after the seal by genuinely committing after it in this group's own Raft
//! log — see `crate::MAX_APPLIED_TS_DOC` — so "later-ordered" and
//! "higher-timestamped" coincide within one group). This closes the wide-fence
//! residual: a leader that hasn't yet learned about the split can still only
//! append to the *same* log the seal already occupies a position in.
//!
//! The seal's durable witness for a **successor** group (a split child) is a
//! **marker key written directly into the shared engine**, deliberately
//! **outside every `StorageScope`** (ADR 0026/0028) so a co-hosted successor
//! can observe it with no scope machinery of its own.

use animus_control::syskv::RESERVED_NAMESPACE;
use animus_tablet::{KeyRange, escape};

use crate::hlc::HlcTimestamp;

/// The segment distinguishing this crate's marker keys from the control
/// plane's own system-keyspace entities — which, on a **combined** node,
/// live under the exact same `RESERVED_NAMESPACE` prefix in the exact same
/// shared engine (`animus_control::syskv`'s `Metadata` mirror). Chosen not to
/// collide with any `syskv::EntityKind::as_str()` segment.
const SEAL_TAG: &[u8] = b"cp_seal";

/// The physical, engine-global key for the range-seal marker `tablet`'s
/// group proposes for `range`.
///
/// **Provable disjointness from every table's own physical keys.** A table's
/// `StorageScope` prefix is always `animus_tablet::escape(table_name)`
/// (`animusd::table_scope_prefix`), and `animus_control::syskv::
/// is_reserved_name` rejects any table name equal to or prefixed by
/// `RESERVED_NAMESPACE` — enforced at `Metadata::apply`'s `CreateTableSchema`
/// arm, the sole authoritative (replicated, every-replica-agrees) gate. Since
/// `escape` is **injective and prefix-free** (its own doc: "no key's
/// encoding prefixes another's" — every embedded `0x00` byte doubles to
/// `0x00 0x01`, so the `0x00 0x00` terminator can occur at most once, at the
/// very end, for any input), `escape(RESERVED_NAMESPACE)` can never equal
/// nor be a prefix of `escape(other_table_name)` for any `other_table_name !=
/// RESERVED_NAMESPACE` — and no schema can ever be registered under that
/// name. So no user table's physical key range can ever contain, or be
/// contained within, this marker's key space.
///
/// *(An earlier draft of this design proposed a bare `[0x00, 0x00]` lead
/// pair. That does NOT hold: `escape("")` — the legacy whole-keyspace
/// tablet's own `StorageScope` prefix, `table_scope_prefix("")` in `animusd`
/// — is EXACTLY `[0x00, 0x00]`. Reusing the control plane's
/// already-enforced `RESERVED_NAMESPACE` closes that hole instead of
/// inventing a second bespoke reservation that would need its own
/// enforcement wired through `is_reserved_name`.)*
///
/// **Keyed by `(tablet, range)`, not `tablet` alone** — a deliberate
/// deviation from a tablet-id-only key: a single source tablet can propose
/// more than one seal over its lifetime (successive splits each hand off a
/// *different* range), and a tablet-id-only key would let a later seal
/// silently overwrite an earlier one's stored range before every waiting
/// successor has had a chance to observe it. Keying by the full `(tablet,
/// range)` pair makes every seal a tablet ever proposes its own, permanent
/// marker — re-proposing the identical `(tablet, range)` (the idempotent
/// re-propose-each-tick loop) simply refreshes the same key with a newer
/// `ts`, which is harmless (the range component is unchanged).
pub(crate) fn seal_marker_key(tablet: u64, range: &KeyRange) -> Vec<u8> {
    let mut out = seal_marker_prefix(tablet);
    put_range(&mut out, range);
    out
}

/// The scan-bound prefix covering every seal marker `tablet`'s group has
/// ever proposed (any range) — lets a successor enumerate a specific
/// parent tablet's markers without knowing the exact range up front (it
/// only knows its *own* range, which the marker's range must **contain**,
/// not necessarily equal — see the module doc).
fn seal_marker_prefix(tablet: u64) -> Vec<u8> {
    let mut out = escape(RESERVED_NAMESPACE.as_bytes());
    out.extend_from_slice(&escape(SEAL_TAG));
    out.extend_from_slice(&tablet.to_be_bytes());
    out
}

/// The smallest physical key strictly greater than every key under
/// `tablet`'s own seal-marker prefix — always `Some` for this specific
/// construction (the prefix always ends in an escaped ASCII tag followed by
/// raw tablet-id bytes, never all-`0xFF`).
fn seal_marker_scan_bound(tablet: u64) -> (Vec<u8>, Vec<u8>) {
    let start = seal_marker_prefix(tablet);
    let end = prefix_upper_bound(&start)
        .expect("a seal-marker prefix (escaped ASCII tag + raw tablet-id bytes) is never all 0xFF");
    (start, end)
}

/// The bound `tablet`'s seal-marker prefix scan uses to fetch every marker
/// it has ever proposed. Exposed for the async gathering step
/// (`host::Reconciler::gather_facts`) to drive the actual engine scan —
/// this module stays I/O-free.
pub(crate) fn scan_bound(tablet: u64) -> (Vec<u8>, Vec<u8>) {
    seal_marker_scan_bound(tablet)
}

/// The smallest byte string strictly greater than every string with this
/// `prefix` — the standard prefix-upper-bound idiom (duplicated from
/// `crate::prefix_upper_bound`'s private copy to keep this module
/// self-contained and independently unit-testable).
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(&last) = out.last() {
        if last == 0xFF {
            out.pop();
        } else {
            *out.last_mut().expect("just checked non-empty") = last + 1;
            return Some(out);
        }
    }
    None
}

fn put_range(out: &mut Vec<u8>, range: &KeyRange) {
    out.extend_from_slice(&(range.start.len() as u32).to_be_bytes());
    out.extend_from_slice(&range.start);
    match &range.end {
        Some(e) => {
            out.push(1);
            out.extend_from_slice(&(e.len() as u32).to_be_bytes());
            out.extend_from_slice(e);
        }
        None => out.push(0),
    }
}

/// The value stored at a seal marker's key: the sealed range (redundant with
/// the key bytes for a point lookup, but load-bearing for the prefix-scan
/// gating check, which must recover each entry's own range to test
/// containment) plus the HLC commit timestamp it sealed at
/// (observability/debugging only — the gating check only needs the range).
pub(crate) fn encode_seal_value(range: &KeyRange, ts: HlcTimestamp) -> Vec<u8> {
    let mut out = Vec::new();
    put_range(&mut out, range);
    out.extend_from_slice(&ts.wall_ms.to_be_bytes());
    out.extend_from_slice(&ts.logical.to_be_bytes());
    out
}

/// The exact inverse of [`encode_seal_value`]. `None` on malformed input — an
/// engine-internal marker this crate itself wrote should never be malformed;
/// a decode failure indicates real corruption, surfaced loudly by the caller
/// rather than silently misread.
pub(crate) fn decode_seal_value(bytes: &[u8]) -> Option<(KeyRange, HlcTimestamp)> {
    struct Cursor<'a> {
        bytes: &'a [u8],
        pos: usize,
    }
    impl Cursor<'_> {
        fn take(&mut self, n: usize) -> Option<Vec<u8>> {
            let s = self.bytes.get(self.pos..self.pos + n)?;
            self.pos += n;
            Some(s.to_vec())
        }
        fn u8(&mut self) -> Option<u8> {
            let b = *self.bytes.get(self.pos)?;
            self.pos += 1;
            Some(b)
        }
    }
    let mut c = Cursor { bytes, pos: 0 };
    let start_len = u32::from_be_bytes(c.take(4)?.try_into().ok()?) as usize;
    let start = c.take(start_len)?;
    let has_end = c.u8()?;
    let end = if has_end == 1 {
        let end_len = u32::from_be_bytes(c.take(4)?.try_into().ok()?) as usize;
        Some(c.take(end_len)?)
    } else {
        None
    };
    let wall_ms = u64::from_be_bytes(c.take(8)?.try_into().ok()?);
    let logical = u32::from_be_bytes(c.take(4)?.try_into().ok()?);
    Some((KeyRange { start, end }, HlcTimestamp { wall_ms, logical }))
}

/// Whether physical key `k` belongs to *any* tablet's seal-marker namespace
/// — used by teardown/GC paths to prove they never sweep a marker (markers
/// live outside every `StorageScope`, so an ordinary scoped erase can't reach
/// them, but a whole-engine `entries()` fallback could if not careful).
#[cfg(test)]
pub(crate) fn is_seal_marker_key(k: &[u8]) -> bool {
    let ns = escape(RESERVED_NAMESPACE.as_bytes());
    let tag = escape(SEAL_TAG);
    k.starts_with(&ns) && k[ns.len()..].starts_with(&tag)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(start: &[u8], end: Option<&[u8]>) -> KeyRange {
        KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec))
    }

    #[test]
    fn key_disjoint_from_table_scope_prefixes() {
        // A real table's StorageScope prefix is escape(table_name); none of
        // these can ever equal or prefix-match the seal marker's own key,
        // by the injective/prefix-free argument in this module's doc.
        for table in ["", "users", "__animus_system_backup", "orders"] {
            let table_prefix = escape(table.as_bytes());
            let marker = seal_marker_key(7, &r(b"a", Some(b"z")));
            assert!(
                !marker.starts_with(&table_prefix) || table_prefix.is_empty(),
                "marker key must not fall inside table {table:?}'s own scope"
            );
            // The only way `table_prefix` could be empty is `StorageScope::whole()`,
            // which never comes from `escape` (whole() uses a raw empty Vec,
            // not escape("")) — so real table prefixes are always non-empty
            // and this assertion has teeth for every case above.
            assert!(!table_prefix.is_empty() || table.is_empty());
        }
    }

    #[test]
    fn reserved_namespace_prefix_never_equals_an_escaped_table_name() {
        // escape("") == [0,0], which IS the legacy whole-keyspace tablet's
        // StorageScope prefix under animusd::table_scope_prefix(""). Confirm
        // our reserved namespace's escape does NOT collide with it.
        let empty_table_prefix = escape(b"");
        let ns_prefix = escape(RESERVED_NAMESPACE.as_bytes());
        assert_ne!(empty_table_prefix, ns_prefix);
        assert!(!ns_prefix.starts_with(&empty_table_prefix) || empty_table_prefix.is_empty());
    }

    #[test]
    fn seal_value_round_trips() {
        for (range, ts) in [
            (
                r(b"a", Some(b"z")),
                HlcTimestamp {
                    wall_ms: 12_345,
                    logical: 7,
                },
            ),
            (r(b"", None), HlcTimestamp::zero()),
            (
                r(b"m", Some(b"m")),
                HlcTimestamp {
                    wall_ms: 1,
                    logical: 0,
                },
            ),
        ] {
            let bytes = encode_seal_value(&range, ts);
            let (r2, ts2) = decode_seal_value(&bytes).expect("decodes");
            assert_eq!(r2, range);
            assert_eq!(ts2, ts);
        }
    }

    #[test]
    fn distinct_ranges_for_the_same_tablet_get_distinct_keys() {
        let k1 = seal_marker_key(3, &r(b"a", Some(b"m")));
        let k2 = seal_marker_key(3, &r(b"m", Some(b"z")));
        assert_ne!(k1, k2, "different handed-off ranges must not collide");
    }

    #[test]
    fn scan_bound_covers_every_key_for_the_tablet_but_no_other() {
        let (start, end) = scan_bound(5);
        let mine1 = seal_marker_key(5, &r(b"a", Some(b"m")));
        let mine2 = seal_marker_key(5, &r(b"m", None));
        let other_tablet = seal_marker_key(6, &r(b"a", Some(b"m")));
        assert!(mine1.as_slice() >= start.as_slice() && mine1.as_slice() < end.as_slice());
        assert!(mine2.as_slice() >= start.as_slice() && mine2.as_slice() < end.as_slice());
        assert!(
            !(other_tablet.as_slice() >= start.as_slice()
                && other_tablet.as_slice() < end.as_slice())
        );
    }

    #[test]
    fn is_seal_marker_key_identifies_only_markers() {
        let marker = seal_marker_key(1, &r(b"a", Some(b"z")));
        assert!(is_seal_marker_key(&marker));
        let table_key = {
            let mut k = escape(b"users");
            k.extend_from_slice(b"row1");
            k
        };
        assert!(!is_seal_marker_key(&table_key));
    }
}
