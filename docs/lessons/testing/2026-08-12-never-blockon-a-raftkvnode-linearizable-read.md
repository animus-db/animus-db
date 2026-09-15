# Never `block_on` a `RaftKvNode` linearizable read (`linearizable_get`/ `_scan`, `read_at`/`scan_at`) — it hangs forever, not just "runs synchronously."

**Never `block_on` a `RaftKvNode` linearizable read (`linearizable_get`/
`_scan`, `read_at`/`scan_at`) — it hangs forever, not just "runs
synchronously."** `futures::executor::block_on` polls its future on its
own local executor, entirely separate from `Simulator`'s; a read barrier's
`.await` points (confirmation polling, at minimum) are timeline events
registered against the `Simulator`'s own clock, resolved only when
`Simulator::run_for`/`run_until` is *actively called* to step them. Call
`block_on` on such a future and nothing ever drives that clock forward —
the calling thread blocks on a future that can structurally never
complete. The fix (already documented in `animus-cp-data/CLAUDE.md`'s
Tests section, but easy to violate by habit when reaching for a "just
read this value" one-liner alongside genuinely synchronous calls like
`node.put(..)`): always drive a linearizable read as a spawned task +
`run_for`, the same shape every other test in the suite already uses —
never mix in a bare `block_on` for "just one more read" partway through a
test that's otherwise correctly using the spawned-task pattern. A hang
with near-zero CPU time consumed over the whole wall-clock duration (not
a busy spin) is the tell: something is waiting on a clock nobody is
advancing. (`crates/animus-cp-data/tests/ts_cache.rs`.)
