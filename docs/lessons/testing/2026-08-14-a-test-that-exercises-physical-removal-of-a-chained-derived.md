# A test that exercises physical *removal* of a chained/derived-numbered entity for the first time needs at least two generations in the chain, not one — a single-entry test can pass for the wrong reason (or, worse, hang) because the very row it means to reclaim is structurally unreclaimable alone.

**A test that exercises physical *removal* of a chained/derived-numbered
entity for the first time needs at least two generations in the chain,
not one — a single-entry test can pass for the wrong reason (or, worse,
hang) because the very row it means to reclaim is structurally
unreclaimable alone.** Building the DynamoDB Streams segment janitor
(ADR 0043 §A9, round-3 PR7), the first retention test wrote one item,
sealed it, waited for its row to be marked *and physically removed*, and
timed out — not a bug in the removal logic, but in the test's own
premise: `index_drain::seal_now`'s epoch numbering is "the chain's own
highest existing row, plus one" (a design that only holds while the
catalog never shrinks), so the janitor correctly refuses to ever
physically remove a tablet's *current* highest-epoch row while the
tablet still exists (removing it would let a future seal silently reuse
the same epoch number for different data). A single-write test's only
row is *always* the current max, so it can never be reclaimed by design
— the fix was two writes/seals in sequence, so the first stops being the
max once the second exists. General rule: before writing a test (or
reviewing PR-added retention/GC/reclaim code) for "the Nth generation of
a chained identity gets removed," check whether identity derivation for
that chain reads *only currently-present* entries (a count, a `max()`, a
`last()`) rather than an independent, ever-increasing counter — if so,
removing the wrong generation (or testing removal with too few
generations present) is a live correctness hazard, not just a
test-construction detail. (`crates/animusd/src/segment_janitor.rs`,
`crates/animusd/tests/stream_janitor.rs`, ADR 0043 §A9, round-3 PR7,
2026-08-14.)
