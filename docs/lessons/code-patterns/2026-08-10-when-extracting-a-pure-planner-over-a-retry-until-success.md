# When extracting a pure planner over a retry-until-success async teardown loop, the planner must NOT eagerly mutate its own successor state to reflect "the action I just emitted will succeed" — because the real execution is async and can fail/time out, and the planner has no way to know.

**When extracting a pure planner over a retry-until-success async teardown
loop, the planner must NOT eagerly mutate its own successor state to reflect
"the action I just emitted will succeed" — because the real execution is
async and can fail/time out, and the planner has no way to know.** Porting
`animusd`'s `cp_gc_tablet`-driven reclaim/release teardown into
`animus-cp-data::host::plan` (ADR 0031 PR3), the real code only removes a
tablet from its `minted` claim set *after* shutdown + erase + WAL deletion
all actually succeed (a timeout re-registers the handle and leaves `minted`
untouched, so the next tick retries the whole teardown). A naive pure
`plan(state) -> (actions, next_state)` that removes the tablet from
`LocalState::hosted` the moment it emits a `Reclaim`/`Release` action would
silently break that retry contract the instant the caller wires it in: a
timed-out teardown's tablet would vanish from `hosted` in the *returned*
state regardless, and if the caller trusts that as ground truth for its next
`plan` call, the tablet is never revisited again — a permanent leak with no
error, indistinguishable from a successful teardown from the planner's own
point of view. Fixed by leaving `hosted` untouched for `Reclaim`/`Release`
and giving the caller an explicit `LocalState::confirm_torn_down` to call
**only** once its own async teardown has actually completed — so an
un-confirmed action is simply re-planned identically on the next call,
mirroring the real loop's tick-based retry exactly. General check when
extracting a pure "decide what to do" function out of a loop that also does
fallible I/O for the same resource: does the loop's *bookkeeping* removal
happen at decision time or at confirmed-completion time in the original code
— if the latter, the pure function's successor state must preserve that
asymmetry (add eagerly is fine when the action can't practically fail;
remove must wait for a caller-reported confirmation). Verified with a
dedicated unit test that drives `plan` twice without confirming and asserts
the identical action re-appears, then confirms and asserts it stops.
(`animus-cp-data::host::{LocalState::confirm_torn_down, plan}`;
`host::tests::a_pending_reclaim_is_replanned_until_confirmed_torn_down`.)
