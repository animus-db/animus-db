# A sharded CI tier's wall time is dominated by the per-shard cold compile, not the test count riding on top of it — measure both parts separately before touching the partition matrix (2026-09-07, ADR 0061 rung D3 PR 4)

D3 converted 20 of `crates/animusd/tests/*.rs`'s 120 real-thread `ProdEnv`
binaries (103 of its 521 tests) to deterministic `SimCluster` coverage
instead — a 20% shrink by both files and tests in the tier
`prod-liveness-animusd`'s four nextest shards partition. The naive
expectation: a 20% smaller tier should tolerate fewer, or need fewer,
shards. Measured instead: the slowest shard's own wall time dropped only
598s → 564s (run 34103555943 → run 34123273726) — a 6% drop against a 20%
test-count drop, and nowhere near enough to justify going from 4
partitions to 3. The reason is structural, not a measurement fluke: every
shard in this workflow rebuilds `-p animusd` from a cold cache
(`cache-targets: false`, deliberate per this workflow's own comment on why
caching `target/` isn't worth it yet), and that compile alone sits under a
floor of roughly five minutes regardless of how many tests run afterward.
Shrinking the test count only trims the *execution* portion sitting on
top of that fixed floor — going to fewer partitions would concentrate more
of both the (shrinking) execution time and the (fixed, per-shard) compile
floor onto each remaining shard, which *raises* the max shard wall time,
not lowers it. **General lesson**: before changing a CI matrix's shard/
partition count in response to a test-count change (a conversion to a
faster tier, tests deleted as redundant, a corpus depth knob lowered),
measure actual wall time per shard and split it into its compile-time
floor and its execution-time portion separately — "N% fewer tests" does
not imply "N% less wall time" or "proportionally fewer shards needed"
when a large fixed cost (a cold-cache rebuild, a fixed bring-up sequence,
a per-shard toolchain install) sits under every shard independent of what
runs after it. The same shape recurs anywhere a workload is partitioned
across parallel workers with a per-worker fixed cost: the fixed cost sets
a floor no amount of load-shrinking below it can beat, and the only way to
find that floor is to measure it, not infer it from the load's own size.
