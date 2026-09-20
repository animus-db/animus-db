# Porting a sibling's "halted-gated durability assert" pattern only requires the latch-and-tolerate half; a driver with no stop-the-loop mechanism of its own doesn't need one invented to benefit from it

**Mechanism**: `animus-cp-data::RaftKvNode`'s `halted: Arc<AtomicBool>` does
two separable jobs at once: (1) it is a *durability-tolerance* latch — an
I/O failure at a WAL/engine write site is a hard panic unless `halted` is
set, in which case it is a tolerated teardown artifact — and (2) its driver
loops (the consensus loop, the apply task) each poll it at their own
top-of-loop and exit once it is true, so `shutdown()` also eventually stops
the tasks it latches. Porting this pattern to `animus-control::RaftNode`
(issue #939, ADR 0038's 2026-09-19 amendment) surfaced that these two jobs
are independent: this crate's consensus loop and apply task have **no**
exit path of their own at all — they were designed to run for the life of
the process, with no `shutdown()`/`is_stopped()` concept anywhere in this
crate. Job (1) — gating the four bare `.expect()`s that used to panic
unconditionally on any I/O failure — needed porting regardless, since
`animusd::Node::shutdown`/`shutdown_and_wait` already hard-`task.abort()`
this driver exactly like it does the CP-data one, racing the identical
class of "directory removed out from under a still-running write" teardown
artifact. Job (2) did not: inventing a loop-exit mechanism this crate never
had, just to make the port feel "complete" relative to its sibling, would
have been solving a problem nobody asked for and widening the change's
blast radius (touching `drive`'s and `meta_apply_loop`'s own loop bodies,
explicitly out of scope for this pass) for no correctness gain — `halt()`
here only ever changes what a *subsequent* I/O failure means, never when
the surrounding `task.abort()` actually stops the task, and that was
already sufficient: the latch only needs to be set *before* the abort for
the tolerance to apply, which `animusd::Node::halt_local_control()` being
called immediately alongside `ClusterEdgeState::halt_hosted_cp_groups()`
already guarantees.

**General form**: when porting a "halted-gated durability assert" idiom
from one driver to a sibling, decompose it into "what a failure means"
(port always, it's the actual correctness fix) and "how/when the driver
stops" (port only if the destination lacks an equivalent it already
relies on some other way — here, plain `task.abort()` from outside). Don't
assume feature parity with the source is required for correctness; check
what closes the specific race the fix targets, not what makes the two
implementations structurally identical.
