# A sim "restart" must preserve exactly the state production persists — here the WAL and the syskv engine as a PAIR; an indices-only "caught up" check cannot see missing state

`SimCluster::restart` kept the control Raft WAL but handed the fresh `RaftNode`
a brand-new `MemoryEngine` as its system-keyspace engine. Under `DRIVER_APPLIED`
(ADR 0038) the engine, not the WAL/snapshot, holds `Metadata`: the apply task
seeds from the engine (empty, watermark 0) and replays only the log above the
compaction base. Past `SNAPSHOT_THRESHOLD` (64) entries the restarted node
reported `commit == applied ==` the leader's with only the post-snapshot tail
of `Metadata` (1 of 40 tablets). Below 64 entries nothing was compacted, so a
full replay hid it. A real node reopens its persistent engine next to its WAL;
retaining one half of the pair and resetting the other builds a state
production never reaches.

- Rule: a fixture restart keeps every durable artifact production keeps, and
  resets only volatile state. Check the pair, not the part.
- Rule: "caught up" assertions on indices (`commit`, `applied`,
  `engine_applied_index`) prove nothing about content; compare the `Metadata`.
- Fixed by `SimCluster::control_syskv` (one retained engine per control node;
  fresh only for a genuinely new node). Regression:
  `animus-control` `restart_retained_syskv_engine` and `animusd`
  `sim_cluster_scale_restart_past_compaction_matches_leader`.
- Latent product hazard (filed separately): a wiped syskv dir over a
  retained compacted WAL boots silently with partial `Metadata`.
