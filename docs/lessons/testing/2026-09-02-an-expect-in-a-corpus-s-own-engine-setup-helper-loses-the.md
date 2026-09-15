# An `.expect()` in a corpus's own engine-setup helper loses the seed the moment it panics — the failure fires before `assert_scenario_ok`'s own seed-carrying message ever runs (2026-09-02, issue #554).

**An `.expect()` in a corpus's own engine-setup helper loses the seed the
moment it panics — the failure fires before `assert_scenario_ok`'s own
seed-carrying message ever runs (2026-09-02, issue #554).**
`raftkv_linearizable.rs`'s LSM tier reopens a node's durable engine via
`lsm_engine()` (`LsmEngine::open_with(..).expect("open lsm engine")`),
called both for the initial open and for the `StopRestart` nemesis's
reopen. Every OTHER panic in this corpus's runner (`assert_scenario_ok`)
carries `scenario={name} seed={seed}` in its message by construction —
but a panic *before* that point, inside the scenario's own setup, has no
such message, and nothing upstream prints which of the corpus's ~1000+
scenarios was even running. `corpus-deep`'s nightly `ANIMUS_RAFTKV_LSM=1
ANIMUS_RAFTKV_SEEDS=40` run hit exactly this for two consecutive nights
(byte-identical panic both times — deterministic, not a flake) with no
way to replay it. **Fix**: every corpus loop in the file (there were
four: the plain corpus, the WAL-faults pass, the LSM representative
subset, and the LSM full corpus) now runs each scenario through a shared
`run_scenario_identified(s, || run(s))` wrapper — `eprintln!`s
`scenario={name} seed={seed}` before the run (visible under
`--nocapture`, the CI convention every corpus step already uses) *and*
`std::panic::catch_unwind`s the run, re-panicking with the same prefix
regardless of output buffering. This immediately identified the actual
failing scenario (`fsync_lie_stop_restart_early_3_s03`,
seed=15482874842363184593) on the very next repro run, replayable at
`ANIMUS_RAFTKV_SEEDS=4` (or any depth covering that seed variant) in
under two minutes instead of the full 40-seed, ~3-minute nightly sweep.
**General rule**: any fault-injection corpus whose scenario-runner
function bakes seed reporting into its own *assertion* messages
(`assert_scenario_ok`-shaped) has a structural blind spot for anything
that panics *before* that function runs — engine/fixture setup chief
among them, since setup is exactly the code least likely to have been
written with "this might panic mid-corpus" in mind. Wrap the *whole*
per-scenario call, not just the assertions, the same way this fix does;
the wrapper is generic over any `FnOnce() -> ScenarioResult`, so it costs
nothing to reuse from every loop in a corpus file, not just the one that
happened to be gated in CI.
