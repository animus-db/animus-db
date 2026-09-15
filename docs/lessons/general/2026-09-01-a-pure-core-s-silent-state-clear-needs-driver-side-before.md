# A pure core's silent state clear needs driver-side before/after diffing, not a hook into the core (issue #313)

`RaftCore::transfer_leadership`'s deadline-timeout abort (`tick` clearing
`transfer_target` when the target never stepped down in time) had no log,
metric, or trace anywhere in the path — an operator (or a test) could not
tell "transfer in progress" from "transfer was silently dropped moments
ago," which is what made a real CI flake (a leadership-change test) hard to
read from the outside.

The core (`animus-control::raft`) is deliberately synchronous and I/O-free
(ADR 0003's sync/driver split) — it has no metrics handle and no
`tracing` calls anywhere in the file, by design, so a leadership-transfer
metric/log could not be added *inside* `tick`/`handle` without threading a
new dependency into the one place this codebase works hardest to keep
pure. The fix instead follows the pattern this driver already uses for
election metrics (`record_transition`, keyed off a `before_role`/
`after_role` diff around each `tick`/`handle` step): read
`RaftCore::transfer_target()` before and after the step, and derive the
event from the diff plus `after_role`. A `Some -> None` clear while still
`Leader` is unambiguously the deadline abort (the only path that can
produce it); a `Some -> None` clear while no longer `Leader` is a routine
step-down (transfer succeeded, or superseded by a different election) and
doesn't need a metric.

**The general lesson**: when a pure, I/O-free core needs a new externally
observable event, don't reach for adding a side-effecting call inside it —
check whether the driver that already steps it can derive the event from a
before/after diff of an accessor the core already exposes (or a cheap new
one). This keeps the core's purity invariant intact and usually costs
nothing (the driver already snapshots role/term every iteration for
exactly this reason) rather than opening a debate about whether "just one
`tracing::warn!` inside the core" is a harmless exception.

A second, narrower lesson from the same issue: **don't trust an issue's own
speculation about what an unused function was *for* without checking**.
The issue guessed `RaftCore::set_election_timeout` (zero call sites) might
be "the missing `TimeoutNow` half" of the transfer mechanism. Reading
`handle_timeout_now` directly showed a received `TimeoutNow` already
campaigns immediately via `start_election`, bypassing the election timer
entirely — so a timeout-*base* setter could never have been "the missing
half" of a mechanism that doesn't touch the timer at all. `git blame`
confirmed the real, narrower intent from the setter's own commit message
(pre-vote hardening, ADR 0009): an "assembly layer" was meant to widen the
timeout for a node doing real disk I/O, and that assembly layer was simply
never built. Both the getter and the setter existed with zero *production*
callers, but they weren't equally dead: the getter (`election_timeout()`,
a pure read) was reusable for the new abort log's "budget" field, while the
setter (an unwired mutation with an aspirational doc comment describing a
caller that doesn't exist) was deleted outright rather than kept in limbo.
