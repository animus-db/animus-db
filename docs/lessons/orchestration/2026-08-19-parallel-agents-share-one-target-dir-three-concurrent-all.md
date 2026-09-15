# Parallel agents share one `target/` dir; three concurrent `--all-targets` builds exhaust the session disk (2026-08-19).

**Parallel agents share one `target/` dir; three concurrent
`--all-targets` builds exhaust the session disk (2026-08-19).** Fanning three
implementation agents across disjoint crates avoids *source* conflicts but
not *build* conflicts: each ran its own `cargo build --workspace
--all-targets` in the same `target/`, which grew past 22 GB and hit ENOSPC —
killing builds mid-link (`ld terminated with signal 7`), producing a
transient compile error in one agent from another's half-written file, and
eventually filling the harness's own scratch filesystem so that even `df`
could not run. Deletes still succeed when writes don't, and
`target/debug/incremental` is the cheapest large thing to drop first.
**Rules for a parallel fan-out on this repo**: give each agent a
crate-scoped gate (`cargo clippy -p <crate>`, `cargo test -p <crate>`) and
keep the one workspace-wide `--all-targets` build for the orchestrator to
run *serially* at the end; set `CARGO_PROFILE_DEV_DEBUG=0` for validation
passes (debug info dominates target size and changes nothing the gates
check); and tell agents to stop and report on ENOSPC rather than polling for
space, since a blocked agent burns its context waiting on a condition only
the orchestrator can clear. The orchestrator should also re-run every gate
itself afterwards — an agent whose build was killed by someone else's disk
usage will honestly report "inconclusive," and two of the three fixes here
reached the working tree never having been compiled (one did not: a
`MutexGuard` held across an `.await` in a new fault-injection test made the
future non-`Send`, which only the serial re-run caught).
