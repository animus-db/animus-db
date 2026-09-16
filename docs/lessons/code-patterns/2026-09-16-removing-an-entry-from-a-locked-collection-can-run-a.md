# Removing an entry from a lock-protected collection can run arbitrary `Drop` glue on the removed value — if that glue ever re-locks the same mutex, the removal must happen after the lock is released, not inline

**Removing an entry from a lock-protected collection can run arbitrary
`Drop` glue on the removed value while the lock is still held — if that
glue ever re-locks the same mutex, this is a guaranteed self-deadlock, not
a rare race.** `animus-sim`'s `Simulator::stop`/`shutdown` remove entries
from `SimState.tasks: BTreeMap<TaskId, Option<BoxFuture<'static, ()>>>`
while holding `SimState`'s own mutex (`stop`'s `st.tasks.remove(&task)`
inside a function-scoped `let mut st = self.shared.lock();`; `shutdown`'s
`st.tasks.clear()`). This was harmless for years, because nothing stored
in `tasks` had a `Drop` impl that touched the simulator's own state.
Issue #837 added exactly that: `Sleep::drop` (cleaning up a cancelled
timer) re-locks the same `SimState` mutex. The instant a task removed by
`stop`/`shutdown` was parked mid-`sleep` (holding a live `Sleep` inside its
suspended future), dropping that future — which happens *inline*, as part
of evaluating `tasks.remove(&task)`/`tasks.clear()` while the guard is
still alive — ran `Sleep::drop`, which tried to lock the mutex the calling
thread already held. `std::sync::Mutex` is not reentrant, so this hangs
forever; found immediately by writing a regression test and watching it
time out under a `timeout`-guarded run, not by any assertion failing.

**General rule**: before adding a `Drop` impl to any type that can end up
stored inside a lock-protected collection (directly, or nested inside a
larger value like a boxed future's captured state), grep every call site
that removes/clears entries from that collection *while holding the lock*
— `.remove(..)`, `.clear()`, `.take()`, replacing a slot's value, or
dropping the collection itself — and check whether the new `Drop` glue can
reach back into the same lock. The fix is never "make the lock reentrant"
(that just hides ordering bugs); it's to make the removal defer the actual
drop until after the lock is released — collect what's removed into an
owned value that outlives the lock guard, release the guard, *then* let
the collected value drop. Two ways to get the ordering right in Rust
without a nested block: declare the collector variable **before** the
`let ... = lock()` binding (locals drop in reverse declaration order, so
the guard — declared later — drops first), and/or add an explicit
`drop(guard)` call before the collector's own drop point, which documents
the ordering for the next editor instead of relying on declaration order
alone surviving a refactor. Verify the fix actually closes the hazard by
temporarily reverting to the naive (inline-drop) form and confirming a
regression test genuinely hangs (`timeout <n> <test binary>`, checking for
exit code 124) — a race like this can't be proven by a passing test alone,
since a lock that happens to be released "in time" by luck of scheduling
would pass too; here the hang is deterministic and total (single-threaded
cooperative executor, not a scheduling race), which made it easy to
confirm both ways.

This is a different failure shape from the sibling lesson on
`docs/lessons/code-patterns/2026-08-16-holding-a-node-local-lock-across-a-call-that-can-recurse.md`
(a function holding a lock across an explicit downstream call that grows
to reach the same lock) — here the reentrant call is *implicit*, triggered
by ordinary container cleanup running a stored value's destructor, which
is easy to miss because nothing at the removal call site looks like a
function call at all. See `crates/animus-sim/CLAUDE.md`'s `Sleep`/`stop`/
`shutdown` sections and `crates/animus-sim/tests/sleep_drop.rs` (issue
#837) for the concrete instance and its regression tests.
