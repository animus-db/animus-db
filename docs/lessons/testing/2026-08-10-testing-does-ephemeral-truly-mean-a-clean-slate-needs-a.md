# Testing "does `--ephemeral` truly mean a clean slate" needs a fresh directory per incarnation, not just flipping the backend flag on a same-dir restart — same-dir + `--ephemeral` is not the same claim as same-dir + durable, and asserting the wrong one gives a false pass.

**Testing "does `--ephemeral` truly mean a clean slate" needs a fresh
directory per incarnation, not just flipping the backend flag on a
same-dir restart — same-dir + `--ephemeral` is not the same claim as
same-dir + durable, and asserting the wrong one gives a false pass.**
Writing ADR 0038 PR4's control-plane restart matrix, the naive test for
"`--ephemeral` control-only restart loses `Metadata`" was: propose a
schema, hard-shutdown, restart on the *same* dir with `StorageBackend::
Memory` again, assert the schema is gone. That assertion is false for a
small scenario (a handful of proposals): the control Raft's own `raft.wal`
is always real `ProdEnv` disk I/O regardless of the system-keyspace
engine's backend choice (`animusd/CLAUDE.md`'s pre-existing "`--ephemeral`
does NOT make the control/raftkv WALs ephemeral" gotcha, previously only
documented for the data plane), and compaction is gated on a threshold
(`SNAPSHOT_THRESHOLD = 64` uncompacted commands, or a follower genuinely
needing an `InstallSnapshot`, `animus-control/src/node.rs`) — with well
under that many commands and no follower, the full uncompacted log tail is
still on disk, so a same-dir restart replays it back into the fresh
(empty) memory engine and the "ephemeral" schema reappears anyway,
regardless of engine backend. The correct way to test "does a genuinely
fresh, stateless incarnation start empty" is a **fresh directory** per
incarnation (mirroring what a real ephemeral pod replacement actually is —
new disk, same address identity) — same-dir `--ephemeral` restart is a
*different*, real, and separately-interesting claim ("does this specific
small-scale scenario's residual WAL tail get replayed"), not a substitute
for it. See `crates/animusd/tests/control_metadata_restart.rs::
ephemeral_control_only_restart_does_not_carry_over_metadata`.
