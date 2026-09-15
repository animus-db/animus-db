# A sibling scenario's own convention is not automatically this scenario's own scope — check the conversion target directly before copying it (ADR 0061 rung M, C-13 PR 5)

Converting `tests/seed_join_allocated.rs`'s `two_concurrent_allocated_
joins_get_distinct_ids` into a `SimCluster` sibling, an early draft created
three tables and closed with a put/get forwarding proof through each
joined node — mirroring scenario (a)'s own established convention in the
same module (a single joiner gets that exact treatment, and it works
reliably there). It flaked at one of five pinned `_over_seeds` values with
a genuine, reproducible `"no CP group leader reachable"` — root-caused
(never widened a budget, never `#[ignore]`d) by direct, temporary
instrumentation to: with TWO joiners promoted back to back rather than
one, the placement reconciler has strictly more elapsed virtual time
before the forwarding proof runs, and it used that window to move MULTIPLE
tablets' replicas across both new nodes at once — a real, if transient,
mid-reconfigure window the single-joiner scenarios never hit because their
own promotion-to-proof window is too short for the reconciler to get there
first (each already states this reasoning in its own doc). The fix was not
a retry wrapper around the flaky assertion — re-reading the REAL test's
own body directly (not the module's accumulated habit of extending each
new scenario the same way the last one was extended) showed it never made
a forwarding proof at all: only distinct/minted ids and real-detector
promotion. Dropping the tables and the forwarding proof from the new
scenario entirely — matching the conversion target's own actual scope,
not a sibling scenario's convention — removed the race at its root with no
loss of coverage (the forwarding proof already exists elsewhere, for the
case where it's actually safe to make it as a point-in-time assertion).
**The general rule**: when building the Nth scenario in a module that
already has an established per-scenario shape, matching that shape by
habit is not the same as matching what THIS scenario's own conversion
target actually asserts — re-derive the new scenario's scope from the real
test/mechanism being converted, the same "re-grep, don't propagate an
inherited label" discipline this log already records for roadmap
inventories and closing-rung pointers, and apply it to a sibling
scenario's own convention too. A flake that only appears at higher
concurrency (here: two joiners instead of one) is often a sign that an
assertion copied from a lower-concurrency sibling has quietly picked up an
implicit precondition (a short promotion-to-proof window) that no longer
holds — the fix is to check whether the assertion was ever actually
required, not to make it more patient.
