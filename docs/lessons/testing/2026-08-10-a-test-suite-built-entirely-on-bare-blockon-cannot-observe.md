# A test suite built entirely on bare `block_on` cannot observe a `env.spawn_task`-backed background feature — check the harness before defaulting a new async-offload feature on.

**A test suite built entirely on bare `block_on` cannot observe a
`env.spawn_task`-backed background feature — check the harness before
defaulting a new async-offload feature on.** Storage's tests never drive
`Simulator::run_for`/`run_until`, so a new "move maintenance to a spawned
task" feature would silently never run under the existing suite. Shipped
correctly as additive and default-OFF rather than rewriting the test
harness to flip it on. Corollary of "SimEnv proves logic, not real-thread
liveness" — but also a warning to CHECK the harness shape before assuming a
feature can default on. (PR #32.)
