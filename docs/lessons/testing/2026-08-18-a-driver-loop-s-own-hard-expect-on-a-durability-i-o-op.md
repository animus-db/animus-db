# A driver loop's own hard-`.expect()` on a durability I/O op should be gated by the same halted/shutdown latch its sibling error-handling site already uses, not panic unconditionally

**A driver loop's own hard-`.expect()` on a durability I/O op should be
gated by the same halted/shutdown latch its sibling error-handling site
already uses, not panic unconditionally** — mirror the existing idiom
(`animus-cp-data`'s apply-task compaction path already tolerated
`env.replace` failing *only while `halted`*) rather than inventing a new
shape. The two failure classes need to stay distinguishable: while running,
the identical I/O error is a genuine durability fault and must stay a loud
panic (crash-stop-before-ack); while halted, the same error is an artifact
of the teardown itself (an aborted task's blocking-pool op surfacing
`"background task failed"`, or a test's `TempDir` deleting the file out
from under a still-draining loop) and should be tolerated — return early
with **no** durability bookkeeping advanced (never claim a write is durable
that never landed) and let the caller's own halted-check exit the loop on
its next pass. **This is deterministically regression-testable under
`SimEnv`** despite looking like a real-thread race: `animus-sim`'s
`DiskConfig::set_error_prob(1.0)` (via `Simulator::set_disk_config_for`)
forces every subsequent disk op on one node to fail, and since `SimEnv`
only polls a node's driver task inside `run_for`/`run_until`, two
*synchronous* calls back-to-back from the test body — mint a pending write,
then latch `halted` — are guaranteed to land before the driver is next
polled, so its next `persist_wal`-shaped pass finds the fault and the
latch together, deterministically, no thread races or timing sleeps
needed. Proof the test has teeth: temporarily reverting the fix reproduces
the exact pre-fix panic message. (`animus-cp-data/tests/shutdown.rs::
a_halted_nodes_pending_write_tolerates_a_wal_fault_with_no_panic`.)
