# "Sweep for the retry-amplification shape" has to be re-run every time a new hand-rolled propose loop lands — the sweep's own corollary caught its third instance

**"Sweep for the retry-amplification shape" has to be re-run every time a
new hand-rolled propose loop lands — the sweep's own corollary caught its
third instance** (issue #268, 2026-08-17). `ClientCtx::provision_tablet`
re-proposed `CreateTablet`/`SetTabletPolicy` on every 50ms poll tick for
its whole 10s commit budget, exactly the unpaced shape `propose_and_await`
fixed one layer up ("the pattern's most common instance was hiding one
layer below") — measured at 264 `CreateTablet` + 240 `SetTabletPolicy`
proposals for six tables' worth of first-put provisioning under a
deliberately slowed (~80ms-fsync) disk, each duplicate a real control-log
append fsynced and replicated under exactly the slow-commit conditions
that made the wait long. On a starved 2-vCPU CI runner this
self-amplification is what turned "commit is slow" into "provision burns
its whole 10s budget, twice in a row" — the direct mechanism behind
cp_txn.rs's 25s seed-put flake. Fixed with the same
`SCHEMA_PROPOSE_PATIENCE` pacing (inline, not via `propose_and_await`,
because the create arm must re-derive its allocator id + replica set
fresh per proposal — the `trigger_split` stale-allocator lesson — and the
needed command switches to `SetTabletPolicy` mid-loop); regression:
`tests/provision_propose_pacing.rs`, which pins the leader's own log
growth while provisioning grinds against a quorumless control plane.
**Known remaining instances of the shape, deliberately left for their own
PR** (they are not on the flake's path): `dynamo.rs`'s seven hand-rolled
propose-then-poll loops (`create_table`'s schema + per-index waits,
`enable_stream`/`disable_stream`, `create_index`, `set_index_status`,
`drop_table_index`) — all fixed-command loops that could ride
`propose_and_await` directly — and, pathological-state-only,
`detect_loop`'s per-tick re-propose of an uncommittable liveness
transition (visible while a control plane has lost quorum, i.e. while
nothing can commit anyway; bounded by member count per tick).
