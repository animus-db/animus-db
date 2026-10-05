# A gated wire surface: decode purely, decide the gate at the emit site

When a request shape becomes acceptable only behind a cluster-version gate
(here `UpdateTable` `ReplicaUpdates` behind `Gate::GlobalTables`), do not make
the pure decoder reject it: the decoder has no cluster state, so it either
rejects always (wrong once the gate opens) or never knows the gate. Decode to a
typed, **undigested** operation (`Operation::UpdateTableGlobal`) and let the
`animusd` handler check the gate first, returning the pre-gate rejection text
byte for byte; only then validate. A separate enum variant also avoids touching
every existing `UpdateTable { .. }` literal. Also: reject the *never supported*
sub-actions (`Delete`/`Update`) before the mode check, or a request that is
wrong in two ways reports the less useful reason (the EVENTUAL message for a
`Delete`). The conversion is relayed like any schema command, so add a
follower-connected ProdEnv test (`schema_ddl_relay`) with a real finalize.
