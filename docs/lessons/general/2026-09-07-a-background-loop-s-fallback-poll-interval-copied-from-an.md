# A background loop's fallback poll interval, copied from an unrelated caller's own convergence-check cadence, is a cost multiplied by every long `run_for` window a corpus drives (ADR 0061 rung D4 PR 1)

`SimCluster`'s new per-node reconciler-driving loop (`spawn_reconciler_
loop`) needed a fallback interval — how often to re-tick when
`metadata_watch()` doesn't wake it. The first draft set it to 50ms,
reasoning that it should match `SimCluster::poll_until`'s own 50ms
convergence-check step, "so a reconciler that reacts at least that often."
That reasoning sounds plausible and is wrong: `poll_until` polls a
*predicate* cheaply; it never depends on the reconciler's own internal
tick cadence to converge, because every real hosting/reconfigure decision
is driven by `metadata_watch()`'s own wake, which resolves in near-zero
virtual time on an actual commit **regardless of the fallback's length**.
The fallback only bounds a missed-wake safety net — a case that, in
practice, never fires in this fixture's own test suite.

What the 50ms choice actually cost: `sim_cluster_corpus.rs`'s own
scenarios each drive several seconds of virtual time (`SETTLE`/a fault
window/`DRAIN`), and every node pays for a full reconciler tick
(`gather_facts` + `plan`, non-trivial work even when nothing changed)
every 50ms of it. Measured directly (`ANIMUS_SIMCLUSTER_SEEDS=3`,
`--nocapture` to see per-scenario progress): ~3s/scenario, vs. ~1.75s/
scenario before this rung — at `ANIMUS_SIMCLUSTER_SEEDS=10` that
extrapolates to several real minutes for the corpus alone, which looked,
mid-run, indistinguishable from a hang (steady CPU and growing memory,
no progress markers under a captured, non-`--nocapture` test run) until
a smaller-depth `--nocapture` probe showed it was making completely
normal per-scenario progress, just slower than before. Widening the
fallback to 200ms (still 2.5x more responsive than the analogous
production constant, `RECONCILE_FALLBACK_INTERVAL` = 500ms) cut
per-scenario time to ~2.1s and brought the whole crate's `cargo test -p
animusd --lib` wall time back down to within noise of the pre-change
baseline, with zero change in which scenario converges or how — proving
the original 50ms bought no correctness or convergence-speed benefit at
all, only cost.

**The general lesson**: when a new polling/fallback constant needs a
value and an existing, unrelated constant happens to be sitting right
there (a caller's own convergence-check step, a sibling fixture's poll
interval), matching it "to be safe" is not free — the new constant's own
*actual* cost model may be completely different (here: driven once per
long virtual-time window per node, not once per assertion), and nothing
about "it matches the neighbor" proves it's sized correctly for where
it's actually used. Measure the thing you're actually adding — a
seed-depth corpus, a fault-injection loop, anything that multiplies a
per-tick cost by scenario count × seed depth × node count — before
shipping a plausible-sounding value, the same way a benchmark file in
this repo measures a real I/O cost before choosing a design instead of
reasoning about it in the abstract. And when a background test run looks
stalled, check for actual progress (a `--nocapture` re-run at a smaller
depth, or a process's own CPU/memory trend) before assuming either "it's
hung" or "it's fine" — both guesses were available here and only one was
right.
