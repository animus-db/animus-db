# One `tests/it/main.rs` per crate for SimEnv tests; real-thread tests stay their own binary, and a CI filter that matches nothing passes green.

**Every `tests/*.rs` file is its own crate that cargo links separately, so
~300 integration files meant ~300 links; merging the SimEnv/pure ones into a
single `tests/it/main.rs` per crate (`mod foo;` per former file, `git mv`'d so
history follows) cut the test-binary count and the clean `--all-targets`
build.** Measured numbers are in the PR that landed this.

What to keep in mind when adding or moving tests:

- **Only merge tests that are safe to share a process.** `ProdEnv`,
  `multi_thread` real-time liveness, real sockets, and anything touching
  process-global state (env vars, cwd, `/etc/hosts`, signal handlers, global
  subscribers) stays a separate `tests/<name>.rs` target. Merging those raises
  in-binary concurrency under plain `cargo test` and reproduces the spurious
  `ProdEnv` timeouts documented elsewhere in this directory. `animusd`'s 100
  `tests/*.rs` are all real-thread, so none moved.
- **Selecting a former file is now a name filter on the module path**
  (`--test it foo::`), not `--test foo`. A CI/nightly step whose filter
  matches no test exits 0, so after renaming, check each invocation with
  `-- --list` (count > 0) — a nightly corpus that silently runs zero tests is
  a gate violation. A `::`-containing value must also be YAML-safe (`run: |`),
  since `foo:: ` followed by a space parses as a mapping.
- **Relative paths shift one directory.** `include_str!("../../x")` is
  relative to the source file; `CARGO_MANIFEST_DIR`-relative fixture paths are
  unaffected. Fixtures are never moved (ADR 0073).
- **nextest runs a process per test, `cargo test` runs a binary's tests as
  threads of one process**, so the merged binaries gain nothing from sharing a
  process and lose nothing under nextest. Doc tests are not run by nextest;
  keep an explicit `cargo test --doc` step so coverage does not shrink.
- **rustc already links with rust-lld on x86_64-unknown-linux-gnu** (1.90+):
  check `readelf -p .comment <binary>` ("Linker: LLD") before adding a linker
  override; none was needed.
- **`SimEnv::metrics()` is a process-wide shared no-op sink.** A sim test
  that asserts a before/after delta on `env.metrics().get(..)` (rather than
  threading its own `MetricsHandle::recording()` in, as
  `departing_removal_notice`'s sibling test does) races with every other test
  in the same process that bumps that metric. Merging
  `departing_removal_notice` into the shared binary made two of its tests fail
  (`CpSnapshotShips` delta 2 / 4 instead of 0) under plain `cargo test`; they
  passed under nextest (process per test). It and the other two files that read
  sim counters this way (`reconciler`, `reconciler_stop_timing`) stay separate
  binaries. The root-cause fix is a per-`SimEnv` recording handle in
  `animus-sim` (not done here).
