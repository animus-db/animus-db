# Merging two roles onto one shared resource silently breaks any aggregation code that assumed they were always distinct — audit every "sum across roles" call site, not just the assembly code that merged them.

**Merging two roles onto one shared resource silently breaks any
aggregation code that assumed they were always distinct — audit every
"sum across roles" call site, not just the assembly code that merged
them.** ADR 0040 PR1 merged a combined node's two internal `ProdEnv`s
(control + raftkv) into one shared env. `ClientCtx::metrics_text`/
`metrics_json` had always summed "the control-role sink" +
"the raftkv-role sink" as two `MetricsHandle` snapshots, on the correct
pre-merge assumption that they were two distinct `Arc`-backed sinks; after
the merge, both handles are clones of the identical sink (`ProdEnv::
metrics()` is shared across every clone of one env, by design — see
`animus-env/CLAUDE.md`), so summing their snapshots silently double-counts
every counter for every combined node, forever, with no compile error and
no test failure unless a test asserts an *exact* counter value (most
don't). Caught only by re-reading the aggregation code while doing the
merge, not by any gate. Fix: add `MetricsHandle::is_same_sink` (`Arc::
ptr_eq` on the inner sink) and skip the second push when it's true — a
small, generically reusable escape hatch for exactly this "two things that
used to be different are now the same thing" class of bug. **General
check: whenever two previously-independent resources (envs, connections,
caches, sinks) get merged into one, grep for every site that iterates
"each of the N distinct resources" and confirm it still holds — the
n-way sum/aggregate is the shape most likely to go silently, quietly
wrong.** (`animus-env::metrics.rs`, `animusd::ClientCtx::metrics_text`/
`metrics_json`, ADR 0040 PR1.)
