# A newly-joined control-only *non-voter*'s own self-registration retry can never observe its own commit landing — so it keeps re-proposing (and can clobber a concurrent writer's update to the same replicated entry) for the *entire* bounded retry window, not just until the first successful commit.

**A newly-joined control-only *non-voter*'s own self-registration retry
can never observe its own commit landing — so it keeps re-proposing (and
can clobber a concurrent writer's update to the same replicated entry)
for the *entire* bounded retry window, not just until the first
successful commit.** `ClientCtx::register_node_addrs`'s doc already
describes it as "best-effort... re-proposing each tick" bounded by
`SCHEMA_COMMIT_TIMEOUT` (10s) — the *intended* shape is: propose, then
stop once `effective_metadata()` (this node's own applied view) reflects
it. That confirmation path silently assumes the caller's own applied
state eventually reflects the commit — true for every existing caller
(a combined/data node is either already a real voter, or an ADR 0030
growth node reading the `remote_metadata_sync_loop` mirror through
`effective_metadata()`), but **false for a genuine control-only non-voter**
(ADR 0037's own "quiet non-voter until `change_membership` adds it"
shape): its `ControlHandle::Local` has no mirror substitution and its own
`RaftCore` never receives real replication while it isn't a voter, so
`effective_metadata()` stays a permanently-empty default the whole time —
the confirmation condition can *never* become true, so the loop keeps
firing on every `SCHEMA_POLL_INTERVAL` tick until the full 10s elapses,
regardless of whether the relay actually landed on the first attempt.
Building the PR4 regression test above, calling `admin_add_control_member`
(which stamps `NodeAddrs.control`) while this window was still open let a
*later* self-registration retry (still proposing the node's original,
`control: None` self-registration) silently overwrite the admin action's
write straight back to `None` — reproduced 100% of the time when the
admin action ran immediately after the non-voter's own bring-up, and
confirmed by direct inspection (dumping the non-voter's own applied
`Metadata`, which stayed completely empty throughout — the "never
observes its own commit" half of the diagnosis, not a race that only
sometimes loses). Fixed at the call site, not the mechanism: the test
now waits for self-registration to land **on the real cluster** (an
original voter's applied `Metadata`, not the non-voter's own) *and* then
waits out the remainder of the fixed 10s retry-exhaustion window before
driving any other write to that same node's address-book entry —
mirroring what the real operator runbook's own "confirm it's up first"
step (plan §3) already achieves in practice (a real "start the process,
then go confirm health" gap is almost always ≥10s). **General check
before trusting a "propose then confirm via my own state" retry helper
for a new caller class**: does *this* caller's own read of "did it land"
ever actually observe the commit, or does it structurally read a view
that can't reflect it yet (a permanent non-voter, a disconnected mirror,
a stale cache)? If it can't, the retry isn't "best-effort until
confirmed" — it's "unconditionally retry for the full bound," which is a
much bigger window for a concurrent writer to lose a race in.
