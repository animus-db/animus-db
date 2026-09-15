# `cargo test --exact` with an incomplete test name runs ZERO tests and exits 0 — which reads as a pass.

**`cargo test --exact` with an incomplete test name runs ZERO tests and
exits 0 — which reads as a pass.** `--exact` matches the *full* path
including the module (`confirm_futility_tests::the_test`), not the leaf
name. Give it a partial name and you get `test result: ok. 0 passed;
0 failed; N filtered out` and a zero exit code. Every shell loop that
counts successes by exit status will report a clean sweep having run
nothing. This fooled the same session three separate times, including
once where "6 passed under load" was six runs of nothing, and once while
*verifying a fix* — the most expensive place to be wrong, because a
green-looking mutation test is indistinguishable from a fix that works.
**The rule: a zero test count is a failure, not a pass.** Assert on
`1 passed` (or the expected count), never on the exit code alone, and
establish a baseline run that the test executes *before* trusting any
mutation or repetition result built on it. The `N filtered out` figure is
the tell.
