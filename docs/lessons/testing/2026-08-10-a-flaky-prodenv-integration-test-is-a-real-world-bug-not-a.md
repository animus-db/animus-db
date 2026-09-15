# A flaky `ProdEnv` integration test is a real-world bug, not a determinism hole — the determinism guarantee (ADR 0003) is `SimEnv`-only.

**A flaky `ProdEnv` integration test is a real-world bug, not a determinism
hole — the determinism guarantee (ADR 0003) is `SimEnv`-only.** The `animusd`
tests run over `ProdEnv` (real sockets/time/threads) and *poll with timeouts,
not deterministic assertions* — so an intermittent failure there means a
genuine timing/durability race, exactly the class `SimEnv` can't catch. Debug it
(don't just bump the timeout): `create_table_survives_node_restart` flaked
because (a) its post-restart probe raced the Raft **catalog recovery** — gate on
the recovered artifact (`await_table_schema` polls `has_table_schema`), the
pattern the sibling GSI test already used; and (b) deeper, the control plane
**applied + acked a proposal before its WAL was fsynced** (apply-before-fsync),
so an abrupt teardown lost the acked schema. Both are now fixed — see the
durable-before-visible pattern below. **A real-time restart test must wait for
the recovered state and tear down gracefully.**
