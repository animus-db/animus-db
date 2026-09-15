# A regression test for "does one writer's held lock stall an unrelated writer" is more reliably proven by a structural ordering check than by an absolute wall-clock threshold

**A regression test for "does one writer's held lock stall an unrelated
writer" is more reliably proven by a structural ordering check than by an
absolute wall-clock threshold** (issue #285). The first design measured
the unrelated write's elapsed time against a fixed millisecond bound —
but on a resource-constrained sandbox, real backlog-induced apply lag
turned out to have high, non-linear variance run to run (the *same*
filler-flood configuration measured anywhere from ~150ms to over
`CLIENT_TIMEOUT`, 10s, depending on ambient scheduler contention from
concurrently-running sibling tests), and generous-but-fixed thresholds
either passed spuriously under light backlog or risked flaking under
heavy contention. The property under test doesn't actually need a
number: `!slow_task.is_finished()` at the exact instant the unrelated
write returns is a **hard ordering guarantee**, not a timing race —
pre-fix, the unrelated write literally cannot even acquire the node-wide
lock until the slow task's entire call (including its confirm-poll) has
already returned and dropped the guard, so it can never observe the slow
task as still in flight; post-fix, the slow task keeps grinding through
its backlogged confirm well after releasing the lock, so the unrelated
write routinely finishes first. Keep a loose absolute ceiling alongside
it only as a hang guard (generous enough to never be the discriminating
assertion), not as the property being proven. General form: when a
regression is fundamentally about *ordering* (did A block on B, or not),
prefer asserting the ordering directly (`JoinHandle::is_finished`, a
shared flag, a channel) over inferring it from a wall-clock threshold —
the latter only ever approximates the former, and approximates it worse
the more the environment's real-time behavior varies.
(`crates/animusd/src/lib.rs::confirm_futility_tests::
an_unrelated_evaluated_write_is_not_stalled_behind_another_writes_confirm_wait`.)
