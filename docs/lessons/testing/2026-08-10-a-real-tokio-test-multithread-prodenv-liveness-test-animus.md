# A real `#[tokio::test(multi_thread)]` ProdEnv liveness test (`animus- control/tests/prod_liveness.rs::large_metadata_catch_up_stays_live`) can fail under `cargo test --workspace`'s full parallel run while passing instantly (both before and after the same code change) in isolation (`cargo test -p animus-control --test prod_liveness`) — pure CPU/thread contention from dozens of concurrently-running test binaries starving its real-time catch-up budget, not a regression.

**A real `#[tokio::test(multi_thread)]` ProdEnv liveness test (`animus-
control/tests/prod_liveness.rs::large_metadata_catch_up_stays_live`) can
fail under `cargo test --workspace`'s full parallel run while passing
instantly (both before and after the same code change) in isolation
(`cargo test -p animus-control --test prod_liveness`) — pure CPU/thread
contention from dozens of concurrently-running test binaries starving its
real-time catch-up budget, not a regression.** Confirmed by running the
isolated binary against both the working tree and a `git stash`-clean
checkout of the same commit: both pass in ~2s solo. Per the root
`CLAUDE.md`'s "a flaky ProdEnv test is a real bug, don't bump the
timeout" rule, the right response to a `--workspace`-only failure in a
test that is unrelated to your diff is to **reproduce it in isolation
first** — if it's solid alone (and, ideally, also solid alone on the
pre-change commit), it's machine-load flakiness in the *harness*, not a
logic bug your change introduced, and the fix (if any) belongs in that
test's real-time budget/CI parallelism, not in the unrelated change you're
landing.
