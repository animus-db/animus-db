# Sample the test binary, not `cargo test`, when measuring CPU/RSS

When wrapping a ProdEnv measurement run in a `/proc/<pid>` sampler, `cargo test`
is a parent process: its pid reports ~0 CPU, ~60 MB RSS and 1 thread while the
test binary does the work. A sweep sampled that way produced plausible-looking
CSVs that were silently useless and had to be redone. Build with `--no-run`,
run `target/release/deps/<test>-<hash> <filter> --ignored --nocapture`
directly, and sanity-check the first sample (threads > 1, CPU ticks rising).

Related: the density rig runs three nodes on a `worker_threads = 2` runtime, so
process CPU saturates near 2 cores; a "1.9 cores" figure at high G is the cap,
not the demand (`docs/density-measurements.md`).
