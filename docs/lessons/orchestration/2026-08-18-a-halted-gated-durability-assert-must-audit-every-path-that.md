# A halted-gated durability assert must audit every path that can abruptly stop the guarded driver, not just the one path someone remembered to wire it into

**A halted-gated durability assert must audit every path that can
abruptly stop the guarded driver, not just the one path someone
remembered to wire it into** (issues #282/#279, 2026-08-18).
`animus-cp-data`'s WAL/apply I/O tolerance (`persist_wal`/
`flush_pending`) hard-panics on a live I/O error and tolerates one only
while a group's `halted: AtomicBool` is set — but that flag latches via
`RaftKvNode::shutdown()`, a distinct concept from `animusd::Node::
shutdown()` (raw task-abort, the doc-blessed "kill node N"
fault-injection idiom), and only `Node::shutdown_graceful` (the restart-
test path, via `shutdown_all_cp_groups`) called it. Two much more common
ways to abruptly stop a node never did: bare `Node::shutdown()` itself
only `task.abort()`ed and tore down the env, and `Node` had no `Drop`
impl at all, so a test that panics mid-poll and drops its `Vec<Node>`
left every hosted driver task for the `#[tokio::test(multi_thread)]`
runtime's own teardown to hard-cancel later, mid-I/O — with `halted`
never latched either way. Both windows turn a routine kill or panic
unwind racing an I/O hiccup into an unconditional panic
indistinguishable from a genuine live durability fault. Fix: factor the
latch step out (`ClusterEdgeState::halt_hosted_cp_groups` — a cheap,
synchronous snapshot-and-store over every locally-registered group, no
wait for the driver to actually stop) and call it first from bare
`shutdown()`/`shutdown_and_wait()` too, plus from a new `Drop for Node`
that does nothing else — deliberately preserving the pre-existing
"dropping a `Node` without `shutdown()` leaves its tasks running"
contract, since only the assert those still-live tasks can now safely
race is what needed fixing. Safe to call from `Drop` specifically
because `RaftKvNode::shutdown()` bottoms out in a plain `AtomicBool`
store plus two `Notify` wakes: no I/O, no lock held across an `.await`,
no dependency on a live tokio runtime (`Drop` can run inside or outside
one) — verify this before ever calling anything from `Drop`, since most
async primitives are not this safe. General form: when a correctness
assert is gated on a flag latched by some "graceful shutdown" call, grep
every way the guarded resource can be torn down — an explicit graceful
call, a bare/forceful kill idiom, AND an implicit `Drop` from a panic
unwind — not just the one call site whichever earlier fix's test
happened to exercise. (`crates/animus-cp-data/tests/shutdown.rs`,
`crates/animusd/src/lib.rs`.)
