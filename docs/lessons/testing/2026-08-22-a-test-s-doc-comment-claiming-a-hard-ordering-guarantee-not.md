# A test's doc comment claiming a "hard ordering guarantee, not a timing race" is itself a bug when the guarantee it names is really a wall-clock coincidence — and the confident wording is *why* it survived review.

**A test's doc comment claiming a "hard ordering guarantee, not a timing
race" is itself a bug when the guarantee it names is really a wall-clock
coincidence — and the confident wording is *why* it survived review.**
Issue #285's regression test
(`confirm_futility_tests::an_unrelated_evaluated_write_is_not_stalled_
behind_another_writes_confirm_wait`, `animusd/src/lib.rs`) built its
"write A is slow" scenario with a concurrent filler flood racing real
apply backlog against real time, then asserted `!slow.is_finished()`
when the unrelated write returned, with a comment insisting this
followed "by construction." It doesn't: on a CPU-starved runner the
flood is starved right along with everything else, so it can fail to
build any backlog at all, and the "slow" write finishes first — CI
reproduced this exactly, one parallel run of commit `97289e2` green and
one red, the red one logging the unrelated write taking 104ms with write
A *already* done. The fix's own #285 property (`rmw_lock` is not held
across the confirm-poll) is a real structural guarantee; "write A is
still in flight when write B returns" is not — it depends on which of
two independently-starvable things loses its race, which is exactly what
load inverts. The general lesson: when a test's comment asserts something
is guaranteed "by construction," check that literally nothing about the
assertion's truth depends on relative timing between two things that can
each independently slow down under load — if it does, the comment is
overclaiming, and overclaiming is worse than not commenting at all,
because "by construction, not a race" is precisely the sentence that
waves off the scrutiny that would have caught it being a race. The fix
here (not a timeout bump, per the standing rule below) replaced the real
backlog race with a `#[cfg(test)]` hook
(`dynamo::rmw285_confirm_gate`) that holds write A's propose+confirm
phase open for a fixed, generous delay under the test's own control —
deterministic and immune to scheduler load — and rewrote the comment to
say plainly that the remaining `elapsed < GATE_DELAY / 2` margin is a
generous-but-finite budget, not a proof. See the "flaky `ProdEnv`
integration test is a real bug" and "compare wall-clock against a clean
run" entries above for the family this belongs to — this one adds: the
fix for a race dressed up as a guarantee is to either remove the race
(control the timing yourself) or, if that's truly unreachable, say in
the comment that it's a margin, not a guarantee, so the next reader knows
what they're actually relying on. (#285, 2026-08-22.)
