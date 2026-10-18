# A sim fixture op that always runs a fixed virtual-time budget retains trace per call

`SimCluster::put` ran through `spawn_and_capture`, which always advances the
whole `OP_BUDGET` (12s of virtual time) after spawning the op. On a 6-node
cluster that is thousands of idle control-plane heartbeat events per call, and
`Simulator` keeps every one in its append-only `trace` (the byte-identical-trace
oracle), so about 5 MB stayed live per put and a few thousand preloaded items
OOM-killed the cell (issue #1305).

Lesson: the trace is deliberately unbounded (tests read slices of it), so
memory is controlled by how much virtual time a fixture op burns, not by
trimming the trace. Use the early-returning `spawn_and_capture_fast` for ops
with no reliance on the implicit clock advance, and regression-test with a
deterministic count (`sim.trace().len()` delta and virtual time per op), never
process RSS. See `put_retains_bounded_trace_per_call` in `sim_cluster.rs`.
