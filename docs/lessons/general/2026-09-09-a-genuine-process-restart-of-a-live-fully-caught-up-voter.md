# A genuine process restart of a live, fully-caught-up voter inside an otherwise-active 3-node group pegs one CPU core indefinitely — found while building issue #804's fix, NOT caused by it, and not fixed here (2026-09-09)

While writing a regression for issue #804 fix (1) — proving the receiver's
own durable `hwm.rs` marker survives a restart of the receiver itself, not
just an in-memory `hlc.witness` — the literal scenario described (`sim.stop`
+ a fresh `RaftKvNode::start` of the just-caught-up-via-`InstallSnapshot`
replica, immediately, while the OTHER two replicas of its 3-node group stay
fully live) hung every time: one CPU core pinned at ~99%, no panic, no
progress `run_for` would ever return from even for a 50ms window. Two `gdb`
backtraces taken several seconds apart, on the single OS thread `SimEnv`'s
cooperative executor runs on, showed the SAME task (the restarted replica's
own apply task) at two DIFFERENT points inside `apply_and_compact`'s commit-
effects loop — genuinely making forward progress, not stuck at one
instruction — yet still running after 6+ minutes of wall time for what
should be, at most, a few hundred committed entries. **Confirmed independent
of today's fix**: reverting `lib.rs`/`codec.rs` to this branch's own base
commit (before either fix (1) or (2) existed) and re-running the identical
test scenario reproduces the exact same hang, at the exact same call site
(`apply_and_compact`'s first `core.lock()`, per `gdb`). Neither silencing
the other two replicas first (`sim.crash` before the restarted one's own
`sim.stop`, isolating it completely) nor zeroing its clock skew changed
anything — ruling out both "contention with live peers" and "the
differential-skew mechanism this file's own scenarios need" as causes.

**Not root-caused, and deliberately not fixed here** — per this repo's own
convention (`CLAUDE.md`'s Conventions section: "An incidental pre-existing
bug discovered during a task gets its own separate PR, never a drive-by
fix folded into an unrelated diff"), and because a genuine fix requires
first finding what unboundedly keeps `apply_and_compact` reporting
`did_work = true` forever on a restart shaped exactly this way — worth its
own investigation (a filed issue, `gdb`-attached to a fresh repro, is the
fastest next step: attach with `gdb -p <pid> -batch -ex "thread apply all
bt"`, `pgrep -f target/debug/deps/<test-binary>` finds the pid; the hang
shows up within the first `run_for(50ms)` tick after the restart, so a
short, cheap repro is all a follow-up session needs).

**The general lesson**: when a new SCENARIO SHAPE (not a new assertion, not
a new fixture helper — a genuinely new sequence of `sim.crash`/`sim.stop`/
`RaftKvNode::start` calls) hangs, don't assume it must be your own change
that caused it just because you're mid-way through writing that change —
check the hang against the base commit BEFORE spending more time reading
the code you just wrote. A `git stash` of only the `src/` files (keeping the
test file's own new mechanics) is a five-minute check that turns "is my fix
broken" into "is this test *shape* broken," which are very different next
steps: the former means fix the code; the latter means redesign the test
(here: prove the same fact one layer down — `RaftKvNode::
engine_latest_version()` reads `storage.latest_version()` directly, the
exact same read a future restart's group-start witness would use, with no
restart needed to observe it — rather than block on root-causing an
unrelated, pre-existing hang to land the regression this session actually
owed).
