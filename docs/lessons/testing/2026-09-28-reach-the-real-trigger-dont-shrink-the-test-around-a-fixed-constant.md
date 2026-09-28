# Reach the real trigger via a test-only override, not by shrinking the test around a fixed constant — and a "while writing" convergence claim needs a rate under the peer's own throughput ceiling

`crates/animus-cp-data/tests/learner_snapshot_livelock_under_continuous_writer.rs`
(issue #1064 part 2) went through two rounds of the same underlying mistake
while proving `apply_and_compact`'s `emergency_ceiling_hit` exemption for a
learner's in-flight `InstallSnapshot` transfer.

## Lesson 1: a production safety-valve constant sized for production log
volumes may be unreachable, by an unambiguous margin, inside any real-time
budget a `SimEnv` test can afford at a write rate that isn't itself the bug
under test — and the fix is a **test-only override of the constant**, not a
slower or larger test.

The first draft of this file tried to reach `COMPACT_DEFER_EMERGENCY_CEILING`
at its real, compiled-in production size (4096 entries) by running a longer
sustained-write phase. It could not do so by a margin that reliably
discriminated the pre-fix code from the fix: `behind` "brushed" the ceiling
but didn't cross it by enough, every run, at any write volume this suite
could afford in real wall-clock time (a `SimEnv` step still costs real CPU
even though the *virtual* clock it advances is free).

The fix was not "run longer" — it was adding a narrow, `Option`-shaped
test-only override (`RaftKvNode::set_compact_tuning_for_test`, mirroring
`RaftCore::enable_quiescence`'s own "`Option` field, `None` default, plain
public setter" shape) that lets a test set
`COMPACT_DEFER_EMERGENCY_CEILING`/`COMPACT_THRESHOLD` to a small value
(128–256 entries) for that one test alone, with the production default
completely untouched (every existing caller passes nothing, gets the
compiled-in constant). At that scale, the same write rate crosses the
ceiling many times over an affordable round count, giving a wide, unambiguous
red/green margin instead of a coin-flip "brushes it, maybe."

**General rule**: when a fault-injection test needs to reliably cross a
constant sized for production scale, don't fight the constant with a bigger
test — add a narrow, additive-default test seam that can shrink the
constant for that one test, and confirm the production default is
untouched by grepping for other callers/tests of the same knob.

## Lesson 2: "the learner converges while the writer keeps running" is a
different, and sometimes impossible, claim from "the learner eventually
converges once the writer stops" — and the difference is queueing math, not
code correctness.

The coordinator reviewing this fix correctly rejected the first version's
write-then-drain shape (write for N rounds, stop, then poll for convergence)
as hiding exactly the class of bug this issue is about: the whole mechanism
being tested is behavior *while sustained writes are still landing*, and
"drain after stopping" changes the property under test into a much weaker
one.

Rewriting to check `learner_caught_up` **inside** the write loop (writer
never stopped) immediately exposed a second, independent problem: the file's
original write rate (5 writes/ms = 5000/sec) was *itself* above the
learner's own steady-state `AppendEntries` throughput ceiling —
`MAX_APPEND_ENTRIES_BATCH` (512 entries) divided by the peer's own
round-trip cost (200ms) is 2560 entries/sec, well under 5000/sec. At a rate
above that ceiling, the **AppendEntries-only control** (a learner with no
snapshot involved at all) also never converged while the writer kept
running — not because of any bug, but because the leader was producing
work faster than that one peer could ever physically drain it, an
unavoidable, unbounded backlog by definition. No fix to the snapshot
mechanism under test could make that converge; it isn't the property this
issue is about.

The fix was lowering the write rate (and, since virtual time is free,
compensating with more/longer rounds to still cross the emergency ceiling
many times) to comfortably under that ceiling (roughly 5x margin), so the
control's own convergence is a genuine property of the chosen rate, not an
artifact of eventually stopping.

**General rule**: before asserting "X converges while a writer keeps
running" for any replica with a bounded per-message batch cap and a
non-zero round-trip cost, compute that peer's own steady-state throughput
ceiling (batch cap ÷ round-trip time) and pick a write rate comfortably
under it. Prove this with a control scenario that has nothing else to go
wrong (no snapshot, no compaction) — if the control itself doesn't converge
while writing, the rate is the problem, not the mechanism under test.
