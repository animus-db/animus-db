# A checkpoint commit whose full-suite gate can't run yet because of a pre-existing environment leak is not a signal to go bisect the new tests for a bug (ADR 0061 rung F, C-06 PR 6, 2026-09-08)

C-06 PR 6 (`crates/animusd/src/sim_cluster_dynamo_partiql.rs`, 27 new
scenarios / 54 new tests on top of PR 5's 10) was checkpoint-committed with
its module verified correct in isolation (`cargo test -p animusd --lib
sim_cluster_dynamo_partiql -- --test-threads=1`: 64 passed) but its
full-suite gate explicitly deferred: the module's own 54 extra `sim_
cluster_*` tests, riding on top of the tier's existing ~30 modules, were
enough additional per-test leakage (the two independent reference cycles
the two amendments immediately above this entry's own ADR counterparts
fixed) to push a full `cargo test -p animusd --lib` run's resident memory
past the sandbox ceiling before completion — a symptom that looks, from
the outside, exactly like "the new tests are the problem." They were not:
once the leak was fixed (in two unrelated PRs, entirely outside this PR's
own diff), the full suite completed clean on the first try with the new
module's 54 tests unchanged from the checkpoint.

**The generalizable lesson**: when a checkpoint commit's own message says
a gate is blocked on a *named, external, already-diagnosed* cause (here:
"blocked on a pre-existing per-test memory leak in the SimCluster tier...
that leak gets its own PR first," not "this module's tests are failing" or
"unverified"), the finishing pass's first move should be confirming that
blocker is actually resolved (check the fix landed, re-run the module
alone if it's cheap) — not re-deriving from scratch whether the new code
itself has a problem the checkpoint author already ruled out. Conflating
"a resource ceiling was hit while running N new tests" with "one of the N
new tests has a bug" wastes a bisection pass on code that was never the
cause; the actual fix, both times here, landed in a completely different
crate (`animus-sim`, `animus-node`) with zero lines touched in the test
module that merely tripped over it. The tell that the checkpoint's own
diagnosis was trustworthy: it named the specific mechanism ("resident
memory grows monotonically across the whole lib suite, ~40 MB per test")
rather than a vague "flaky" or "times out," which is exactly the kind of
root-caused claim that generalizes correctly once verified rather than
needing to be re-investigated.
