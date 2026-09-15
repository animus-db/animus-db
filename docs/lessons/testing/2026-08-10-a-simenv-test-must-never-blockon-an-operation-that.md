# A `SimEnv` test must never `block_on` an operation that internally polls `env.sleep()` (e.g. `linearizable_get`/`linearizable_scan`)

**A `SimEnv` test must never `block_on` an operation that internally polls
`env.sleep()` (e.g. `linearizable_get`/`linearizable_scan`)** — those only
resolve while `Simulator::run_for` is advancing virtual time; calling one
directly under `block_on` hangs forever with no panic, burning wall-clock
silently. Spawn it as a task and drive it via `sim.run_for` instead (the
`lin_read`-style helper pattern in `tests/read_index.rs`). (PR #31.)
