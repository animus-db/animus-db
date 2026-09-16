# `Simulator::run_until_quiescent` cannot prove a specific subsystem's timerlessness once *any other* task in the same run keeps its own independent, deliberately-never-eliminated safety-poll timer alive.

**`Simulator::run_until_quiescent` cannot prove a specific subsystem's
timerlessness once *any other* task in the same run keeps its own
independent, deliberately-never-eliminated safety-poll timer alive.** Found
while writing the ADR 0044 phase-1 PR3 quiescence corpus
(`quiesce/3-core-state-machine`, 2026-08-16): the plan asked for
`run_until_quiescent(max_steps) == true` as proof that an idle, quiesced
`RaftKvNode` group posts zero `SimEnv` timeline events. That's unreachable
by construction — the apply task's own idle back-off (PR1) races
`ApplyPending` against a 250ms `APPLY_SAFETY_POLL` **forever**, regardless
of Raft activity, a deliberate design (a missed/lost `ApplySignal` must
still converge, not stall). One node's apply task alone keeps a scheduled
timer event alive at all times, so `run_until_quiescent` can never observe
a truly empty timeline for a *live* group — quiesced or not. This isn't a
defect in the quiescence work; it's a different subsystem's already-shipped
trade-off surfacing at a test assertion that assumed no other timer existed
anywhere in the run. **General rule**: before reaching for
`run_until_quiescent` (or any "the whole sim went idle" assertion) to prove
*one* mechanism's timerlessness, check whether anything else in the same
process — a different task, a different subsystem's own safety poll — has
an independent timer that would prevent it from ever firing, even if the
mechanism under test is working perfectly. When it does, assert the
mechanism's own state directly instead (here, `RaftCore::next_deadline() ==
None` on every replica) rather than inferring it from a whole-sim
observation that a co-located concern can foil. See `tests/quiescence.rs`'s
module doc for the full reasoning kept where the next person will read it.
