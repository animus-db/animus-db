# **A per-test `Node::shutdown()` teardown (plain, non-draining abort) racing

**A per-test `Node::shutdown()` teardown (plain, non-draining abort) racing
a still-in-flight driver I/O op surfaces as a noisy `tokio-rt-worker` panic
that has nothing to do with the test's own assertions — `Node::shutdown_
graceful` (or `shutdown_and_wait`) exists precisely to close this window
and should be the default choice for ordinary end-of-test cleanup; plain
`shutdown()` is for a *deliberate* fault (a documented "kill node N",
"crash the leader", or "the process goes away without decommissioning"
scenario) where the abrupt, non-cooperative abort is the point. Sweeping a
test tree for this: a `for node in &nodes { node.shutdown(); }` (or single
final call) at the very end of a test body, with nothing observing that
node afterward, is teardown — swap it. A `nodes[kill_idx].shutdown()` (or
similarly-named) mid-test, with a comment about killing/crashing a node and
the test continuing to assert against the *survivors*, is a deliberate
fault injection — leave it. A `stop()`/`restart_same_addrs`-style helper
used before rebinding the same addresses needs the graceful form
regardless (see this file's own "long-poll request in flight at kill"
entry and `animusd/CLAUDE.md`'s `Node::shutdown()` gotcha for why a bare
`shutdown()` doesn't reliably free ports either). issue #278 item 1
(`crates/animus-cp-data/src/lib.rs::persist_wal`,
`animusd/tests/backfill_seeder.rs` and the crate's whole `tests/` tree).
