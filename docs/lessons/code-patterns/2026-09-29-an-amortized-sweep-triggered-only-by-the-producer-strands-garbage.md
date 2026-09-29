# An amortized sweep triggered only by the producer strands garbage when the producer goes quiet; a burst's high-water threshold must not outlive the burst

`ProdEnv::Spawner::spawn` pruned finished `AbortHandle`s from `Inner::tasks`
(the fix for #1062, see
`2026-09-28-registering-every-spawned-handle-for-shutdown-is-its-own-unbounded-leak.md`)
only inside `spawn`, once `tasks.len()` reached a threshold recomputed after
each sweep as `max(FLOOR, 2 * live-at-that-sweep)`. Issue #1105 found two
defects in that shape:

1. **The threshold was a remembered peak, not current state.** After a burst
   with 50k tasks in flight at sweep time the threshold became 100k. Once the
   burst finished, a node that went quiet (or spawned slowly) kept up to ~2x
   the *peak* live count of finished handles, each pinning a finished task
   `Cell`, until spawns reached the stale threshold - on a quiet node, never.
   The advertised invariant "bounded to ~2x the live count" was false at every
   instant after a burst.
2. **The test only passed by scheduler luck** (about 1 in 6 failures): it
   claimed to spawn one more task per poll iteration to trigger a sweep, but
   only called `yield_now`.

**Lesson**: when garbage is produced by *consumers finishing* (task
completion), an amortized sweep triggered only by the *producer* (spawn) has
no bound when the producer stops. Drive the sweep from the event that creates
the garbage, and compute the threshold from *current* counts
(`finished >= max(FLOOR, live)`), never from a value remembered across
sweeps. This keeps the amortized O(1) cost (a sweep costs O(live+finished)
and only runs after >= max(FLOOR, live) completions) while making the bound
hold at every point in time.

Mechanics worth remembering: (a) run the completion hook from an RAII guard
so normal return, panic unwind and abort all fire it, and create the guard
*outside* the async block, since a future aborted before its first poll never
runs its body (the first version of the regression test caught exactly this);
(b) a guard that takes a lock from `Drop` means the spawn path must never hold
that lock across `tokio::spawn`, which can drop the future inline; (c) a
counter reset to 0 at sweep can undercount by the few tasks that are counted
but not yet `is_finished()`, bounded by concurrency and harmless (it only
delays the next sweep slightly).

Regression test shape: gate M tasks live so every spawn-time sweep sees them
live, release the gate, spawn *nothing further*, and converged-or-timeout poll
for the bound.
