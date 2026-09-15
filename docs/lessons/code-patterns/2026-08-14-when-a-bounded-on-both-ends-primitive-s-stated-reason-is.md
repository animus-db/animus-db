# When a "bounded on both ends" primitive's stated reason is "every caller scans one contiguous sub-range," that's a fact about the callers that exist *today*, not an invariant of the primitive — check whether a new caller's shape still satisfies it before reusing it unmodified.

**When a "bounded on both ends" primitive's stated reason is "every caller
scans one contiguous sub-range," that's a fact about the callers that
exist *today*, not an invariant of the primitive — check whether a new
caller's shape still satisfies it before reusing it unmodified.**
`RaftKvNode::local_scan_kind`/`linearizable_scan_kind` (ADR 0041 §3) took a
mandatory `end: &[u8]`, documented as deliberate: every existing caller (an
LSI `Query`, the GSI drain's `pending_changes`) always had a finite bound
in hand, so an unbounded form "would make an accidental full-tablet read
easy to write." That reasoning is sound for those callers and wrong as a
universal law: a table-wide LSI `Scan`'s fan-out (added in the same ADR's
§5 follow-up) needs its tail tablet scanned unbounded-above, and *no
finite byte string the caller could construct actually bounds it* — an LSI
row's trailing base-sort-key segment has no length limit, so any fixed
suffix (even one built from the maximum-width token) can be exceeded by a
longer real key sharing its prefix. The fix wasn't "find a big enough
bound" (structurally impossible) but recognizing the primitive already had
the right shape one level up: `local_scan`/`linearizable_scan` (the base
scope's equivalent) had solved exactly this by deriving the bound from
**the scope's own** `StorageScope::physical_bounds()` when the caller
passes `None`, rather than trusting the caller to supply one — mirroring
that (changing `end` to `Option<&[u8]>`, falling back to the kind scope's
own `physical_bounds()`) closed the gap without reopening the
accidental-full-scan risk the original design worried about, because the
bound still comes from the scope's own prefix, never `entries()`. The
general move: before copying "bounded because every caller today has a
bound" onto a new caller that structurally can't have one, check whether a
*sibling* primitive already solved the same problem for the *other* scope
— the fix is usually "generalize that," not "invent a new escape hatch."
(`crates/animus-cp-data/src/lib.rs::local_scan_kind`/
`linearizable_scan_kind`; `crates/animusd/src/lib.rs::cp_scan_kind_table`;
ADR 0041 §5's 2026-08-13 as-built note, 2026-08-13.)
