# A fixture one step from the goal hides a multi-step loop bug

**Context.** The nightly `upgrade-kind-nightly` job (`scripts/e2e-kind.sh`,
`E2E_UPGRADE=1`) failed on every one of its 38 runs (2026-10-07 to
2026-11-13) with `spec.upgrade.finalize: Auto did not finalize the cluster
version`: after the roll, `active` was 3 while every node's `own_range.max`
was 10.

**Root cause.** `POST /admin/cluster-version/finalize` raises the cluster
version by exactly one. `animus-roll` knows this (`finish` returns
`Finalize { to: active + 1 }` until `active >= goal`), but the operator's
shell (`animus_operator::roll::step`) set `status.upgrade.phase = Complete`
after *any* successful finalize. `Complete` turns the next reconcile into
`Stage::Steady`, so the roll stopped after one step. The `controlNodes`
growth roll earlier in the same run did the same (1 -> 2).

**Why the unit tests missed it.** Every operator finalize fixture
(`roll_tests.rs`'s `cv_view`) had `active = 1` and `own_range.max = 2`: the
goal was exactly one step away, so "complete after one finalize" and
"complete when the goal is reached" were indistinguishable. The pure
`animus-roll` tests and the `animusd` `sim_cluster_roll_orchestrator` corpus
loop on `decide` until `Complete`, so they never exercised the operator's own
"what does a successful effect mean" mapping.

**Lesson.** When code drives a loop toward a goal one step at a time, at least
one test fixture must start two or more steps away from the goal. A fixture
one step away cannot tell "done after one step" from "done at the goal".
And an imperative shell that reinterprets an effect's success (here, "the
finalize call succeeded" read as "the roll is complete") needs its own test
of that mapping. A test of the pure decision core does not cover it.

**Regression test.**
`controller::roll_tests::auto_finalize_steps_one_version_at_a_time_up_to_the_goal`
(1 -> 2 stays `FinalizePending`, 2 -> 3 completes, then no further finalize).
It fails on the pre-fix code.
