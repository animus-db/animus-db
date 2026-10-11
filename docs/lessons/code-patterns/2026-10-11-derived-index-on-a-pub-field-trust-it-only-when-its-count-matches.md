# A derived index next to a public field: trust it only while its recorded size matches

**Context.** `Metadata.tablets` is a `pub BTreeMap` that tests, fixture builders
and other crates insert into directly. Issue #1192 needed a table -> tablet-ids
index so `CreateTablet` stopped scanning every tablet.

**Lesson.** Do not make the field private to keep the index exact (it ripples
through every crate that clones or moves the map). Keep the index private,
`#[serde(skip)]`, excluded from `PartialEq`, record the element count it was
built for, and have readers fall back to the scan when that count differs from
the live map (rebuild lazily in the `&mut` path). Direct edits and fresh decodes
are then correct by construction; only an equal-count in-place edit slips
through, which a recompute-from-scratch helper plus a random-command property
test catches. Also check whether the "cached max" is needed at all: on a
`BTreeMap`, `keys().next_back()` is O(log n), and the old `keys().max()` was the
needless linear scan.

**Batching an iterated pure step.** When a pure "one move per call" planner is
called in a loop by a bulk consumer, add a plan API that carries incremental
state across moves and keep the old body as a `#[doc(hidden)]` reference the
equivalence property test compares against; leave the production tick on the
single-step path so its churn bound is unchanged.
