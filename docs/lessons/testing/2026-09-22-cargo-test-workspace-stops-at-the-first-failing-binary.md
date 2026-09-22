# `cargo test --workspace` stops at the first failing binary, not the end

`cargo test` (without `--no-fail-fast`) runs test binaries in sequence and
aborts the whole invocation the moment one binary reports a failure — it
does **not** run the remaining binaries first and summarize at the end.
Concretely: a plain `cargo test --workspace` run that hits one flaky
real-socket integration test partway through the `crates/animusd/tests/`
binary list (alphabetical order) silently skips every binary that would
have run after it — dozens of test files, in the animusd tree alone —
with no indication in the final output that anything was skipped beyond
the `error: test failed, to rerun pass ...` line.

This is easy to miss because the failing run's own tail output *looks*
complete (a normal `test result: FAILED. N passed; 1 failed; ...` block),
and it is tempting to treat "the gate ran and found exactly one failure"
as "the gate ran to completion and found exactly one failure." Those are
different claims. A one-off flake in an early-sorting file (e.g.
`decommission.rs`, which sorts well before `dynamo_*.rs`/`stream_*.rs`/
`tests_*.rs`) can mask the true pass/fail state of the majority of the
integration suite.

**Fix**: once a `cargo test --workspace` run reports any failure, re-run
with `--no-fail-fast` before drawing any conclusion about the rest of the
suite — don't assume "only that one test is red" until you've actually
seen every other binary's own result. Isolate the flaky test separately
(`cargo test -p <crate> --test <binary>`) to judge whether it's a real
regression or environmental noise, but that isolated re-run does not
substitute for confirming the rest of the workspace suite actually ran.

Found while gating a 4-layer stacked PR series (ADR 0072, DynamoDB service
limits): `crates/animusd/tests/decommission.rs::
decommission_drains_removes_and_allows_id_reuse` failed once under
`cargo test --workspace`-level port contention (`join_fresh_deadline`'s
documented TOCTOU class), unrelated to the stack under test; the plain
`cargo test --workspace` run stopped right there, and re-running with
`--no-fail-fast` was needed to actually exercise the remaining ~80 binaries
in the same crate.
