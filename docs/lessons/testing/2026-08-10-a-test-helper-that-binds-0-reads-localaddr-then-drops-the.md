# A test helper that binds `:0`, reads `local_addr()`, then *drops* the listener has a port TOCTOU — retry the (allocate-fresh-ports + start) as a unit.

**A test helper that binds `:0`, reads `local_addr()`, then *drops* the listener
has a port TOCTOU — retry the (allocate-fresh-ports + start) as a unit.** The
freed ephemeral port can be stolen by another test binary before the real bind,
so the subsequent `run_node` rebind fails `AddrInUse` intermittently under
`cargo test --workspace` (it flaked the `animusd` restart tests' *first* bring-up).
Wrap the bring-up in a bounded retry that re-allocates fresh ports each attempt
(`start_single_node` → `(Node, ClusterConfig)`). A same-address **restart** must
reuse the captured config (it's testing same-address recovery), so it can't
re-allocate — retry the *rebind in time* instead (the thief is another binary's
momentary `free_addrs` probe): `tests/support/mod.rs::restart_same_addrs`. The
window was once "acceptably tiny", but every retried bring-up added to the suite
raises probe pressure on everyone else — under `--workspace` load the restart
tests flaked ~2 in 5 full runs until retried. Both retries are bounded, so a
genuinely occupied port still fails. (`animusd` tests.)
