# Not every sibling site sharing a "halted-window" shape is deterministically regression-testable the same way — check whether the racing work source bypasses the driver loop's own gate before assuming a test can be ported.

**Not every sibling site sharing a "halted-window" shape is deterministically
regression-testable the same way — check whether the racing work source
bypasses the driver loop's own gate before assuming a test can be ported.**
Extending the persist_wal halted-gate to the apply task's `flush_pending`
(`merge_batch`, issue #278 item 1 follow-up) is the *same* fix idiom, but
**not** the same test idiom: `persist_wal`'s racing record reaches the core's
pending-persist buffer via a bare *synchronous* `put()` call that never
touches the driver loop's own `halted` check at all, so a `put`-then-
`shutdown()` synchronous beat deterministically lands the fault "mid-window"
under `SimEnv` (no task polling happens between two synchronous test-body
calls). `flush_pending`'s data source (`drain_apply`) only becomes non-empty
through the apply task's *own prior progress* — a test can't populate it via
a bypass call — and its whole effects loop, once entered *after* that same
iteration's own `halted` check already passed, runs uninterrupted to
completion under `SimEnv` (disk ops resolve without yielding, so nothing
hands control back to the executor mid-loop for an external driver to land a
`shutdown()` "between" two calls inside it). Before porting a regression test
idiom to a sibling call site with a superficially identical shape, trace
*how* the racing work gets queued, not just where the two `.expect()`s sit —
a bypass-the-check source is portable, a same-task-progress source is not
(short of new pre-emption machinery this codebase doesn't have). Documented,
not test-covered, in `animus-cp-data/CLAUDE.md`'s driver-split section.
