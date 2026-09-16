# A directed placement decision can be undone by a transient failure-detector false positive — and the mechanism that protects it stops working the moment it's marked `done`

**Mechanism**: `Metadata::split_placing_reconcile` (ADR 0062 §2) drives a
split child's replicas toward a directed-Placing `target`, pausing (never
retargeting) while a target member is merely `Down`, and only recomputing a
fresh target via `replan` once `retarget_ready_this_tick`'s dwell
(`SPLIT_PLACING_RETARGET_DWELL`, 5s) says a member has been continuously
non-`Active` long enough to treat as genuinely gone. That 5s dwell applies
identically whether the target has already been achieved (`t.replicas ==
target`) or is still mid-convergence — but retargeting an *already-achieved*
target is strictly more disruptive than retargeting one still converging:
`replan`'s only remaining eligible candidates for a split child are typically
its own pre-split siblings, so discarding an achieved decision can converge
the tablet right back toward the set the split was moving it away from.

Reproduced directly, more than once, over a real multi-node `ProdEnv`
cluster (`crates/animusd/tests/split_placing_two_replica_diff_e2e.rs`) with
no synthetic fault injection at all — just this repo's own ordinary
background CPU/disk contention on a shared sandbox (confirmed via `uptime`
and concurrent `cargo test` processes from other sessions at the time): a
target member's control-plane liveness status flips `Down` for long enough
to cross the dwell (a false positive — the process itself never died, its
heartbeat was merely delayed), and the tablet's `voter_history` shows it
reach the correct directed-Placing target and then, seconds later, get
retargeted away from it.

**Two layers to the fix, and the second is only a partial one — read
before assuming this is closed**:

1. `retarget_ready_this_tick` now uses a *longer*, separate dwell
   (`SPLIT_PLACING_RETARGET_DWELL_ACHIEVED`, 30s) once `t.replicas` already
   equals the stored `target` — a deliberate asymmetry, not an oversight:
   ADR 0062 §2's own stated goal ("the target is never recomputed while
   it is healthy, which is what makes it stable") is extended to the point
   right after the target is realized, where undoing it costs the most. A
   genuinely (not falsely) dead member still self-heals, just more slowly.
   See `crates/animus-control/src/node.rs`'s `SPLIT_PLACING_RETARGET_DWELL_
   ACHIEVED` doc and the new regression,
   `split_placing_phase_holds_an_already_achieved_target_past_the_base_
   dwell` (`crates/animus-control/tests/placement_split_placing.rs`).

2. **This only protects the narrow window between achieving the target and
   `MarkSplitPlacingDone` firing — `animusd::split_placing_completion`'s own
   settle window (`SPLIT_PLACING_DONE_SETTLE`) is just 1.5s.** The instant
   `done` is set, the tablet falls under *ordinary* `Metadata::reconcile()`
   (ADR 0005's violation-driven repair), which has **no dwell or hysteresis
   at all** — it reacts to any observed non-`Active` replica immediately.
   A false positive arriving after `done` (the overwhelmingly likely case,
   given the 1.5s window) is not protected by anything this fix adds, and
   was directly reproduced hitting exactly this path (a second, independent
   `ProdEnv` run whose `voter_history` reached the target, was marked
   `done` almost immediately, and diverged again seconds later with no
   dwell in the trace at all — the divergence composition matches an
   un-gated `replan` call, not a gated retarget). **This is not
   split-placing-specific — it is a property of `Metadata::reconcile()`
   itself, so it can affect any tablet's placement, not only a
   freshly-split one.** Filed as a separate, more precisely scoped
   follow-up rather than attempted here: fixing it means deciding whether
   ordinary violation repair should gain its own dwell (a change with a
   much wider blast radius — every tablet in the cluster, not just
   split children — and a real availability trade-off: a genuinely dead
   node's replica would stay unrepaired for the dwell window too).

**A related, but distinct, finding in the same test, needing no production
fix**: `reconfigure_step`'s step 1 (remove a `Down` extra voter) has
documented, unconditional priority over its own learner-add sequencing
(ADR 0058 Train 1) — so a false-positive `Down` on an *original* (non-target)
replica can legitimately skip the "add both new members as learners, then
remove both stale voters" path entirely, alternating add/remove one member
at a time and never passing through the over-replicated N+2 intermediate a
test asserting on it expected. This is not a bug — `reconfigure_step`'s own
doc states the priority explicitly ("this is failure repair... nothing to
wait for") — it is a **test invariant** that assumed the common path was the
only legal one. Fixed by turning the hard assertion into a diagnostic
(`eprintln!`) while keeping the properties that actually matter (never below
the 3-voter floor, correct final target) as hard assertions.

**General rule**: when a control-plane mechanism reacts to a liveness
signal (a failure detector's `Down`/`Active` transition) to make or unmake a
placement decision, ask two separate questions, not one: (1) is there a
dwell/hysteresis at all before reacting, and (2) does that same protection
apply *after* the decision has been realized, or does the mechanism hand
off to a different, less-protected code path (here: ordinary `reconcile()`)
the moment it's satisfied? A dwell that only covers the "still converging"
half of a mechanism's lifetime can be strictly narrower than it looks — most
of a real production window is spent in the "already done" state, which is
exactly where this gap lived.

**Also fixed in the same investigation**: `crates/animusd/tests/
split_placing_two_replica_diff_e2e.rs`'s background writer used
`let _ = writer.await;`, discarding the writer task's own `JoinError` — see
issue #619's own lesson entry for the general form of that bug. Its own
`put` helper's retry budget was widened from 20s to 45s with a measured
justification (this file's own doc comment on the constant), matching this
file's other "under load" budgets (`join_extra`/`await_cutover_of`, both
60s) rather than the narrower, inconsistent 20s it had carried.
