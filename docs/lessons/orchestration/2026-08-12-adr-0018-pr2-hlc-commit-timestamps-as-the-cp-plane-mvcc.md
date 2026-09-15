# ADR 0018 PR2 (HLC commit timestamps as the CP-plane MVCC version, the range-seal design replacing `version_floor`) — three generalizable lessons:

**ADR 0018 PR2 (HLC commit timestamps as the CP-plane MVCC version, the
range-seal design replacing `version_floor`) — three generalizable lessons:**
- **A "provably disjoint reserved key prefix" claim needs an actual proof
  against the escaping scheme in use, not a plausibility check.** The
  original design for the seal marker's physical key proposed a bare
  `[0x00, 0x00]` lead pair, reasoning informally that `animus_tablet::escape`
  never emits it mid-string. True, but incomplete: `escape("")` — the
  **empty** table name, i.e. exactly the legacy whole-keyspace tablet's own
  `StorageScope` prefix — **is** `[0x00, 0x00]`, a real collision with an
  already-shipped physical key space. The fix was to re-derive disjointness
  from `escape`'s actual documented property (injective + prefix-free) and
  anchor the new key under an *already-enforced* reservation
  (`animus_control::syskv::RESERVED_NAMESPACE`, which `is_reserved_name`
  already forbids any table from claiming) rather than inventing a second,
  separately-unenforced reservation. When a design proposes a new "reserved"
  byte prefix for anything, check it against every *existing* prefix
  convention in the codebase (including the empty/default case), and prefer
  reusing an already-enforced reservation over minting a new one that would
  need its own enforcement wired through separately.
- **Retiring a version-space-based invariant can silently orphan a
  non-obvious consumer of its arithmetic.** `RaftKvNode::erase_scope`
  stamped its tombstone version as `last_applied() + 1` — not itself named
  `version_floor` anywhere, so a grep for that literal name did not surface
  it, but it depended on exactly the same property (`effective_version`
  monotonically exceeding everything this group ever stamped) the floor
  scheme provided. Removing the floor without updating `erase_scope` left a
  real, silent regression (GC tombstones landing at a version *below* live
  HLC-packed data, so per-key LWW would keep the "erased" value forever) —
  caught only by re-running the existing `narrow_scope.rs` test, not by any
  grep. When retiring an invariant, grep for what it *guaranteed*
  ("strictly exceeds everything ever stamped"), not just its name — a
  caller can depend on the guarantee via completely different code with no
  lexical connection to the mechanism providing it.
- **A new field on a replicated state machine (`Metadata`) is not complete
  until it's mirrored.** Adding `split_parents` (and, at the time, its
  merge-side counterpart `absorbed_by` — since removed, ADR 0044) to
  `Metadata` and updating `apply` was not enough — `animus-control`'s
  system-keyspace mirror (`syskv.rs`'s `EntityKind`, `mirror.rs`'s
  `apply_and_derive_mirror`/`apply_key_write`) has to independently learn
  to derive and decode writes for the new field, or the incremental
  delta-consumer path (`WatchMetadata`, ADR 0038 PR5) silently diverges
  from a full-fetch `Metadata` — caught by the crate's own differential-
  oracle test (`incremental_delta_apply_matches_direct_apply_for_deletes`),
  which is exactly what that test exists to catch. Any new `Metadata`
  field needs a matching `EntityKind` + key builder + both directions of
  `apply_key_write`, not just a field and an `apply` arm — grep
  `mirror.rs`'s exhaustive `match command` (no wildcard, by design) to find
  every site a new command variant must touch.
- **A marker key meant to persist "this event happened, for this specific
  range" must be keyed by (proposer, range), not by proposer alone, the
  moment the same proposer can raise the event more than once over its
  lifetime with different payloads.** A tablet-id-only seal-marker key (as
  originally specified) would let a *second* split of the same source
  tablet silently overwrite the first split's marker with a narrower
  range before every waiting successor had observed it — a genuine,
  easy-to-miss liveness/correctness hazard for exactly the "one source,
  several splits over its life" case the design otherwise handles fine.
  Keying by `(proposer, payload)` instead makes every event's marker its
  own permanent record; re-verify this shape whenever a marker-key design
  assumes "this only ever happens once per identity."
