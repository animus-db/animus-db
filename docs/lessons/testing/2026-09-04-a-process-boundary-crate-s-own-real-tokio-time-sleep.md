# A process-boundary crate's own real-`tokio::time::sleep` bounded-retry loop (the kind ADR 0061 Decision 4 explicitly allows outside the `Env` seam — `animus-operator`'s scale-down drain poll, `animus-cli`'s process-boundary loops) still needs its bound proven by a test, and `#[tokio::test(start_paused = true)]` proves it without ten minutes of real wall-clock wait or touching the production code at all

**A process-boundary crate's own real-`tokio::time::sleep` bounded-retry
loop (the kind ADR 0061 Decision 4 explicitly allows outside the `Env`
seam — `animus-operator`'s scale-down drain poll, `animus-cli`'s
process-boundary loops) still needs its bound proven by a test, and
`#[tokio::test(start_paused = true)]` proves it without ten minutes of
real wall-clock wait or touching the production code at all** —
`tokio`'s virtual clock auto-advances to the next pending timer whenever
nothing else is runnable, so a 120-iteration × 5s poll loop that never
satisfies its completion condition resolves near-instantly, and the test
still exercises the *real* `tokio::time::sleep` call the lint-allowed
code actually makes (ADR 0061 rung E1, `animus-operator`'s
`drain_and_remove_node_is_bounded_when_drain_never_completes`). This is
the outside-the-`Env`-seam analogue of the `SimEnv`
converged-or-timeout-poll rule earlier in this section: don't assert a
bound by reading the source and trusting it, prove it by making the
never-succeeds case happen and checking the loop actually stops.
