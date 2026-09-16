# A third costume of the same bug, in `animus-control` itself, issue #741

**A third costume of the same bug, in `animus-control` itself, issue
#741**: `crates/animus-control/tests/prod_liveness.rs`'s
`large_metadata_catch_up_stays_live` polled `node0.snapshot_index() >=
200 && node1.snapshot_index() >= 200` on a flat 600×50ms (30s) deadline,
and failed once on CI (`prod-liveness-scattered`) with `node0 snap=0,
node1 snap=64` — one replica's own apply task made **zero** forward
progress for the entire 30s window while its sibling made some (but not
enough). `snapshot_index()` only advances when the ADR 0038 apply task
(`meta_apply_and_compact`, `node.rs`) compacts — gated on **its own**
`engine_applied_index` crossing `SNAPSHOT_THRESHOLD` past the current
base, a task the consensus loop deliberately never waits on (see this
file's own `node.rs` doc comment: decoupling apply from `drive` is what
keeps a slow apply pass from stalling Raft's heartbeat/election
servicing). That decoupling is exactly what removes any
contention-independent bound on how long `snapshot_index()` takes to
advance — the identical DRIVER_APPLIED shape this section's own entries
above already name for `animusd`'s `engine_applied_index`-gated reads,
now confirmed in the plane that *originates* the mechanism, not just a
downstream consumer of it. Investigated live: 20/20 passed locally under
3 CPU-pinning `yes` spinners on a 4-core box, but one run finished in
27.5s against the 30s deadline — a genuine near-miss, not a clean
margin — and a from-scratch red-before attempt against the un-fixed code
under heavier (6-spinner) contention independently pushed the *other*
phase of the same test (node 2's own 12s catch-up budget) to 10.14s,
confirming apply/driver-loop timing in this test is measurably
contention-sensitive throughout, consistent with (never contradicting)
the CI failure's own asymmetric signature. Fixed the same way as the two
entries above: replaced the flat deadline with a progress-gated poll —
track each node's own `engine_applied_index()` (the exact counter
compaction is gated on) between ticks, and fail only once **both**
nodes' watermarks have made zero forward progress for a generous idle
window (`COMPACT_IDLE_STALL`, 60s, matching `animusd/tests/support::
IDLE_STALL_TIMEOUT`'s own convention) with the target still unmet, or a
much larger overall backstop (`COMPACT_OVERALL_BACKSTOP`, 150s) expires
— a livelock guard, not the normal exit path. The enclosing test's own
`timeout(..)` budget was widened from 90s to 210s to give this poll room
to legitimately use its new backstop; this is *not* "a longer deadline on
the wrong precondition" (the thing Session operating mode forbids) —
the precondition itself changed from "elapsed wall-clock time" to
"apply-task forward progress," and the poll fails fast the moment that
progress genuinely stops, regardless of how much of the outer budget is
left. No `SimEnv` regression was added: `wal_compaction.rs` and
`install_snapshot.rs` already prove the compaction *mechanism* (and its
O(chunk) `InstallSnapshot` cost) deterministically under `SimEnv`: this
file's own module doc states plainly that the real-thread integration
guard exists precisely because *scheduling contention on real threads*
is the one thing `SimEnv`'s virtual clock structurally cannot represent
— there is no interleaving to encode as a seed here, only real elapsed
time under real OS contention, which is this test's whole reason for
being a `ProdEnv` test at all.
