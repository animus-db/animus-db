# A fixed-timeout consumer of a driver's stop contract is only as tight as the driver loop's own preemption granularity — an inline wait inside a serial reconciler tick turns one slow stop into node-wide starvation

`animus_cp_data::host::Reconciler::teardown` (`Release`/`Reclaim`) used to
call `RaftKvNode::shutdown()` and then block **inline**, polling
`is_stopped()`, for up to `RECLAIM_STOP_TIMEOUT` (10s) before giving up.
That bound was chosen against the driver's documented contract
("`shutdown()` — the node stops … within one election-timeout tick"), which
looked like a generous, safe margin. It wasn't: the bound is only as tight
as how often the driver's own loops actually re-check the flag that
contract depends on.

## The mechanism

`shutdown()` sets one `AtomicBool` (`halted`). The consensus loop's own
top-of-iteration check picks that up quickly. The **apply task** did not:
`apply_and_compact` drains a whole pass's worth of committed-not-yet-
applied entries (`RaftCore::drain_apply`) into one `Vec` and then ran the
entire `for entry in effects` loop with no `halted` recheck between
entries — the checked-once-per-*pass*, not once-per-*entry*, granularity
`flush_pending`'s own doc already flagged for its own narrower "up to ten
merges inside one entry" case. A tablet with a real backlog (a slow/loaded
engine, or simply many entries committed while the apply task was busy)
could take most of `RECLAIM_STOP_TIMEOUT` — or, worse, indefinitely, if a
single entry's own engine I/O never returns — to actually observe `halted`
at all.

`Reconciler::tick` runs every planned `HostAction` for every hosted tablet
**serially, in one `.await` chain** (ADR 0031's own "decide once, execute
sequentially" shape). So the 10s bound wasn't "this one teardown might take
up to 10s" — it was "this ENTIRE tick, for every other tablet this node
hosts, is blocked for up to 10s," and since a timed-out teardown didn't
confirm completion, `plan` re-emitted the identical action on the very
next tick, so the node-wide stall repeated every tick until the slow
driver finally stopped on its own.

## The fix, generalized past this one incident

Two independent changes, each closing a different half of the problem:

1. **Preemption granularity must match the caller's own patience.** If a
   consumer is going to bound its wait on "the driver observes a flag,"
   the driver's own hot loop must re-check that flag at the same grain the
   consumer cares about — here, once per committed entry, not once per
   drained batch of entries. A generic-sounding contract ("stops within
   one tick") can quietly mean "one tick of the *coarsest* loop that has
   to notice," not the loop the caller is actually timing.
2. **A serial orchestrator must never let one slow dependency's own stop
   condition become an inline wait on the orchestrator's own critical
   path.** The fix here (`RECLAIM_STOP_GRACE`, ~500ms — a few polls, sized
   to the *common* case of one persist round plus one apply pass) is:
   wait briefly for the fast path, and if that doesn't clear, **park** the
   still-live handle in a side table and return immediately, letting a
   later, cheap sweep (run once at the top of every subsequent tick, before
   planning) finish the teardown whenever the driver actually stops. The
   *bound* that used to gate "how long do we wait" becomes purely
   observational — a warning and a metric bump once a parked driver has
   been stuck longer than that bound, never a thing anything else's
   liveness depends on.

The generalizable shape: whenever a serial loop conditionally must wait on
one participant's own stop/settle signal before moving on to unrelated
work, prefer *halt the participant, hand its liveness check to the next
tick's cheap sweep, move on* over *block this tick until it settles* — the
inline-wait version silently couples every unrelated participant's own
progress to whichever one is slowest to notice it's been asked to stop.
