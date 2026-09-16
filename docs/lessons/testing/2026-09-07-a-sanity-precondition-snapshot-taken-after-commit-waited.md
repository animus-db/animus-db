# A "sanity" precondition snapshot taken after commit-waited setup steps races every background convergence loop the system runs, not just the one the test is about (issue #690).

**A "sanity" precondition snapshot taken after commit-waited setup steps
races every background convergence loop the system runs, not just the
one the test is about (issue #690).**
`crates/animusd/tests/cp_rebalance.rs::
cluster_grown_to_five_nodes_rebalances_existing_tablets` writes one key
into each of six tables sequentially, commit-waiting each `put`, then
reads the tablet map ONCE and asserted, as a "sanity" precondition
before the real convergence assertion, that two specific nodes held
EXACTLY zero replicas. That exact-zero snapshot is not guaranteed: each
of the six commit-waited puts gives the control plane's own
`reconcile_loop`/`rebalance_step` (`REBALANCE_EVERY_N_TICKS`,
`animus-control/src/node.rs`) another chance to fire, so on a loaded
runner one rebalance move can land before the sixth put even returns —
CI observed exactly `{"n0": 5, "n1": 6, "n2": 6, "n3": 1, "n4": 0}`, an
early rebalance move, not a broken test. A transient failure-detector
`Down` belief on a provisioning node (ADR 0012) can independently skip a
node for one tablet's initial placement. Neither cause is a bug in the
rebalancer under test — the test's own setup phase was never insulated
from the very background loops the test exists to observe. **The fix is
never to assert the exact pre-convergence layout a setup phase happens
to produce — assert only the property the setup actually guarantees**:
here, that a real imbalance exists (`imbalance(&initial_counts) >= 2`,
which six sequential first-`min(N,3)`-Active-member puts under ADR 0023
provisioning do reliably produce) and that the two nodes in question
trail the busiest node (rather than pinning them at exactly zero) — weak
enough to survive an early partial rebalance, still strong enough that a
no-op or already-fully-converged planner cannot pass vacuously. General
form: any "sanity, before the real assertion" snapshot taken after a
setup phase that itself commit-waits (each wait is a scheduling point
for every other tick-driven loop in the process) must be re-derived from
first principles — what does the setup structurally guarantee, not what
did one observed run happen to produce — the same discipline this file's
entry on "eventual properties get a converged-or-timeout poll, never a
fixed-deadline one-shot assert" already applies to the test's *main*
assertion; a precondition snapshot is exactly as exposed to this race as
the property under test and needs the identical scrutiny, not a pass
because it merely runs first. **Same bug, different costume, in issue
#699**: `crates/animusd/tests/shared_wal_liveness.rs`'s load phase ran
writers for a fixed `LOAD_DURATION` and then asserted every table
completed more than `COMPACT_THRESHOLD` writes as a non-vacuity check —
a wall-clock-window write count is the identical "eventual property
observed as a one-shot" shape, just measured in throughput instead of a
map snapshot; the fix (as here) was converge-or-timeout — keep writing
until the count target is met, bounded by a generous stall timeout,
never widen the window to move the threshold.
