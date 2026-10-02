# Core `last_applied` is not the `DRIVER_APPLIED` apply frontier — assert on `engine_applied_index`.

**In a `DRIVER_APPLIED` state machine (ADR 0038) the core's `last_applied()` only
means "handed to the apply task", not "applied".** The apply task drains the core
(`drain_apply`) and then applies to the engine, publishes `cache`, and bumps
`engine_applied`, in a separate async task. When that task stalls, core
`last_applied` still equals `commit_index`, so every check built on it (and on
`commit_index`) looks healthy while `metadata()` goes unboundedly stale and
`pending_apply` grows. The 2026-09-30 `wal_lock` starvation was invisible to the
existing slow-disk test for exactly this reason. Liveness tests for the apply path
must assert on `engine_applied_index()` (and on `metadata()` contents), sampled
*during* the load: after the load stops, a stalled task usually drains and the
evidence is gone. Test: `animus-control/tests/apply_not_starved_by_wal_lock.rs`.
