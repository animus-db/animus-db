# A process-wide static default sink behind a seam trait silently couples tests that share a binary; nextest's process-per-test masks it.

**A process-wide static default sink behind a seam trait silently couples
tests that share a binary, and a per-test-process runner (nextest) masks it.**
`Env::metrics()` defaulted to `MetricsHandle::noop()`, a `static OnceLock`
*recording* sink, and `SimEnv` never overrode it — so every node of every
`Simulator` in the process incremented the same counters. Tests asserting
`env.metrics().get(..)` before/after deltas (`departing_removal_notice.rs`,
`reconciler.rs`, `reconciler_stop_timing.rs`) passed under nextest but failed
under plain `cargo test` the moment one of them shared a test binary with
another that bumped the same counter (deltas of 2 and 4 instead of 0). The
root-cause fix was not to serialize or relocate tests but to scope the
sim-observable state to the simulator instance: `SimEnv::metrics()` now returns
a lazily-created recording handle keyed by `(Simulator, node)`, mirroring
`ProdEnv`'s one-sink-per-process. **When a seam trait has a default that is a
process-global (static) sink, treat it as write-only: anything a sim test can
read back must live in the simulator's own state, and CI must run plain
`cargo test` (not only nextest) so cross-test coupling cannot hide.** A
regression test should put two independent simulators in one process and assert
each reads exactly its own count.
