# One gate per release surface; additive fields skip at their default

`Gate::GlobalTables` gates only `ConvertTableToGlobal`. The fields it introduces
(`TableSchema.global`, `PlacementPolicy.allowed_values`) are
`#[serde(default, skip_serializing_if = ...)]`, so an ordinary table encodes
byte-for-byte as before and the existing golden fixtures stay untouched; only a
converted table carries them, in a new *shaped* fixture (`vN-<shape>.json`)
because the version tag does not change. The preferred-leader command
(`SetGlobalPreferredLeader`) ships in the same release, so it shares the gate
(maintainer decision, M2): one finalize step, one set of mixed-version cells. A
surface that ships in a *later* release gets its own gate. Also add state-based guards (not just the
command gate) for combinations a gate cannot see, e.g. TTL/LSI on a global table.
