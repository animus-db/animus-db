# Wiring a background loop into a shared fixture retroactively stales every doc comment that was written when the loop didn't exist yet

**Wiring a background loop into a shared fixture retroactively stales
every doc comment that was written when the loop didn't exist yet** (ADR
0061 rung I, C-09 PR 4). C-08's `sim_cluster_admin.rs::ttl_tables_
lists_a_ttl_enabled_table` (rung H) carried a doc comment explaining
that `reaper.deleted_total` stays 0 "because the reaper never actually
runs under `SimEnv`" — true when it was written. C-09 PR 2 then spawned
the TTL reaper unconditionally in `SimCluster::new`/`restart`, making
that claim false, but the scenario's own *observable behavior* didn't
change (it still never writes an expired item, so the counter still
reads 0) — nothing failed, nothing flagged it, and the stale reasoning
would have sat there indefinitely if a later PR touching the same file
for an unrelated reason hadn't reread it. The general lesson: when a PR
makes a previously-absent primitive (a loop, a driver, a capability)
real under a shared test fixture, grep that fixture's own sibling
modules for doc comments/scenario docs that assert or lean on its
*absence* — "no primitive drives X", "X never runs here", "this stays 0
because Y is unbuilt" — and fix the ones your own PR's tree touches even
when the assertion they explain doesn't need to change, because a
correct assertion with an now-incorrect justification is exactly the
kind of thing a future reader trusts and gets misled by. A parallel
finding from the same PR: keeping a real-socket test's exact name across
a `SimCluster` conversion (rather than folding its assertions into an
existing, differently-named sibling scenario) is what made this stale
comment discoverable at all — a literal `grep` for the original test
name landed directly on the doc comment that needed fixing.
