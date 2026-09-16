# A once-per-consensus-loop-tick history still misses a transient state when the mutation is driven synchronously from a DIFFERENT task than the one doing the sampling — record at the mutation's own call site, not at any sampling cadence, however fast (2026-09-16, issue #944)

**A once-per-consensus-loop-tick history still misses a transient state
when the mutation is driven synchronously from a DIFFERENT task than the
one doing the sampling — record at the mutation's own call site, not at
any sampling cadence, however fast (2026-09-16, issue #944).** This is a
refinement of the 2026-09-05 `voter_history` lesson ("assert a transient
state from a history the system records itself, never an external poll"),
found while applying that exact fix to a sibling case where it didn't
work.

`crates/animusd/tests/learner_reconfigure.rs`'s
`spare_replacement_passes_through_an_observable_learner_state_and_keeps_serving`
polled `/admin/raftkv` externally every 100ms to catch a newcomer passing
through ADR 0058 Train 1's learner phase before promotion — exactly the
poll-races-a-transient-shut shape the 2026-09-05 `voter_history` lesson
already named. The natural fix looked like a direct application of that
lesson's own resolution: add a `RaftKvNode`-owned history, recorded once
per consensus-loop iteration (the same "recompute live, same lock
acquisition" cadence `voter_history` itself uses), and assert against that
instead of a live sample. **It still flaked, immediately, under load** —
because unlike a voter-set change (which only ever advances via this
group's own network-replicated log, so the consensus loop's own
per-message processing genuinely bounds how much can happen between two
samples), `reconfigure_step`'s add-learner-then-promote pair is proposed by
the **host reconciler's own task**, calling `RaftCore::add_learner`/
`promote_learner` **synchronously, directly on the shared core**, entirely
outside the consensus/drive loop's own scheduling. Two (or three) such
calls can land back-to-back, mutating the core repeatedly, before the
drive loop's task next gets a chance to run its own sampling line — no
tick interval closes that gap, because the sampler and the mutator are
different tasks with no ordering relationship at all. (The same hazard
exists on a follower for a different reason: `log_append` runs once per
entry inside a single `AppendEntries` batch, so several config-changing
entries can apply before the loop that "ticks once per message" gets back
to its own sampling line.)

**The fix that actually closed it**: move the recording INTO the exact
function every real transition already funnels through —
`RaftCore::apply_config` (`animus-control/src/raft.rs`) — rather than
sampling `config()`/`learners()` from any layer above, at any cadence.
`apply_config` is called exactly once per genuine `(voters, learners)`
change, synchronously, by whichever caller made it happen (a leader's own
local propose, or a follower's per-entry `log_append`), so a small bounded
ring appended there (`config_history`) cannot coalesce two transitions
the way an external — or even an internal, once-per-loop-iteration —
sampler can.

**General form**: "record a durable history instead of polling" only
closes the miss if the recording happens on the SAME control-flow path as
the mutation, once per mutation. A history sampled by a periodic loop is
just a differently-shaped poll — faster and internal, but still a poll —
the instant the state it's watching can change from a task/call path the
loop doesn't control. Before trusting a "sampled once per tick" history to
prove a transient state was reached, ask: can the value this ring records
change more than once between two ticks, driven by something other than
this exact loop? If yes, move the recording call into the mutation's own
choke point (the one function/method every path to that mutation shares),
not into whatever loop happens to poll it fastest.

**Also encountered investigating this** (not the fix, but relevant to
anyone reproducing #944 locally): under heavy synthetic CPU load
(`taskset -c 0,1` plus several `yes` processes pinned to the same cores),
this same test's *convergence* poll can also declare victory on a
DIFFERENT, already-documented eventual-property race — issue #610's own
lesson (`2026-09-16-a-faster-bootstrap-time-schema-proposal-makes-initial-
tablet-placement-an-eventual-property.md`): the tablet's *initial* 3-voter
set is no longer guaranteed to be nodes 0,1,2 just because they were
created first, so a test hardcoding "node 3 is the idle spare" can
occasionally be wrong about which node was actually idle. That is a
separate, already-tracked defect (issue #610) and not something this fix
touches — it only surfaced because reproducing #944 required generating
enough load to expose #944's own race, which incidentally also exposed
#610's at a much lower rate.
