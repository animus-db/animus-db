# The `dynamo_index_scan` full-workspace flake signature, adjudicated 2026-08-14

**The `dynamo_index_scan` full-workspace flake signature, adjudicated
2026-08-14**: an intermittent `raftkv wal sync` expect-panic
(`animus-cp-data/src/lib.rs`, around the WAL-sync `.expect(..)` in the
apply task's `flush_wal`, roughly line 4098) surfacing on a tokio worker
thread only during `cargo test --workspace`-scale multi-node `ProdEnv`
teardown — the persist task racing the node's own shutdown for the same
disk handle, the same "`abort()` is a request, not a guarantee" family
already documented above (`ProdEnv::shutdown`/`Node::shutdown` abort
spawned tasks without waiting for them to actually stop, so a
still-in-flight `sync()` can observe a half-torn-down env). Confirmed 5/5
green solo (`cargo test -p animusd --test dynamo_index_scan`) — the panic
only reproduces under concurrent whole-workspace CPU/IO contention, never
in isolation. **Before suspecting the state machine (a real
`assert_ts_monotonic`-class bug) for a teardown-adjacent panic in any
`ProdEnv` integration test, run the one failing test binary solo first** —
if it's consistently green alone, the failure signature is almost
certainly this same shutdown-race family, not new logic in whatever this
session happens to be touching. Named here as its own entry (rather than
folded into the existing "abort() is a request" entry) so a future grep
for `dynamo_index_scan` or `raftkv wal sync` finds the adjudication
directly. (`crates/animus-cp-data/src/lib.rs`,
`crates/animusd/tests/dynamo_index_scan.rs`, adjudicated during ADR
0042/0043 round-3 PR4, 2026-08-14.)
