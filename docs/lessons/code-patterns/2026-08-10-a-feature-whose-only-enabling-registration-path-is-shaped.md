# A feature whose only enabling *registration* path is shaped by the startup config silently caps the cluster at its born size — and a test that "proves" growth by starting every node up front only proves the planner, never the actual growth path.

**A feature whose only enabling *registration* path is shaped by the startup
config silently caps the cluster at its born size — and a test that "proves"
growth by starting every node up front only proves the planner, never the
actual growth path.** ADR 0029's rebalancer worked perfectly in
`cp_rebalance.rs` (5 nodes started together, `Active` from bootstrap) — but a
cluster grown *after* bring-up had no path in at all: `bootstrap` computes
the raftkv ids it registers from `control_ids.len()` at the process's own
start, so a node added later is never proposed as a member by anyone, ever.
The tell was in the problem statement, not the code: "the passing grow-test
starts all 5 nodes up front by its own admission" is a giveaway that the
test exercises the *decision* (given a balanced-vs-imbalanced membership,
does the planner converge) but never the *registration* that would put a
genuinely-new node into that membership in the first place. General check
when auditing "does X actually support growth/scale-out": find the one
function that turns "a node exists" into "the system knows about it," and
ask whether it can only ever run at the size the system started at.
Delivering online growth (ADR 0030) then surfaced a second-order version of
the same lesson: hardening a *different* gap (a declared-but-never-booted
node staying a permanent placement-eligible phantom, since the failure
detector only judges members it has heard from) by making the detector treat
an untracked `Active` member as demotable broke several *existing*
`animus-control` sim tests that had, for years, modeled "Active data members"
by proposing `UpsertMember` directly with **no heartbeat simulated at all** —
a fine way to test placement logic in isolation right up until a change
makes "declared but silent" meaningfully different from "declared and about
to heartbeat." A change to shared detection/liveness semantics needs its
blast radius checked against every test that manages membership *without*
wiring up the corresponding liveness mechanism, not just the tests for the
feature being changed. (`animusd::bootstrap`,
`animus-control::node::detect_loop`; `animusd/tests/cluster_growth.rs`;
`animus-control/tests/{placement_auto_reconcile,placement_rebalance,
placement_reconcile,prod_liveness}.rs`.)
