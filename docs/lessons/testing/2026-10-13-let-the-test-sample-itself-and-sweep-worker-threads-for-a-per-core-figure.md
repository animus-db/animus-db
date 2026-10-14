# Let the density test sample itself, and sweep worker threads for a per-core figure

Two things made the ADR 0079 calibration cheaper and more honest than the
2026-10-12 round:

- **Self-sampling.** The ignored density test now reads `/proc/self/stat`,
  `/proc/self/status` and `/proc/self/fd` and prints `cpu_ticks`, `rss_kb`,
  `fds`, `threads` (and, for `LsmEngine`, file/sstable/byte counts) on its own
  `BP-LIVE` lines. There is no external sampler to attach to the wrong pid
  (see `2026-10-12-sample-the-test-binary-not-cargo-test.md`), and a sweep
  script only has to grep the final `phase=held` line (`hold_cores`).
- **A fixed `worker_threads` only gives a ceiling in cores, not a per-core
  figure, and the per-core figure is not constant.** With the runtime at 1, 2
  and 3 workers the clean awake ceiling (all led, held, `max_term` single
  digit) was about 1100-1300, 1500 and 2300-2700 groups, at 1.0, 1.75 and 2.76
  process cores: groups per process-core falls from about 1200 to about 900
  as workers are added (cross-thread wakeups and contention). A per-core
  budget must be read from the 1-worker run; the 2-worker number understates
  it by about a third. Use `ANIMUS_BP_LIVE_ASSERT=0` for such sweeps so a
  non-converging cell reports a verdict line instead of aborting the sweep, and
  `ANIMUS_BP_LIVE_CONVERGE_SECS` so a failing cell does not run 10 minutes.

Also: `SimCluster::put` burns a full `OP_BUDGET` (12 virtual s) per call and
the sim trace retains about 5 MB per call at 6 nodes (value size is
irrelevant), so preloading 100 writes per tablet across a dozen tablets
exhausted 15 GB and was OOM-killed with no message. Preload through
`SimCluster::put_many_fast` (one task, early-returning, chunks of 100).
