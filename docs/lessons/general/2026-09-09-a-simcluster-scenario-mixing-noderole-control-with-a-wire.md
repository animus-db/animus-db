# A `SimCluster` scenario mixing `NodeRole::Control` with a wire-issued `CreateTable` needs at least one `NodeRole::Data`/`Both` node, or `await_table_serveable` spins forever (ADR 0061 rung L, C-12 PR 4a)

`create_table_via_wire` (this crate's own `sim_cluster_console.rs` helper,
reused by every `sim_cluster_*` module that issues a real DynamoDB
`CreateTable` request) does the full production sequence: propose the
schema, provision the tablet, then block on `ClientCtx::
await_table_serveable` until the freshly-minted tablet's own Raft group has
actually elected and can serve a read. A bare `NodeRole::Control`-only
`SimCluster` has zero nodes eligible to become a replica of anything — the
tablet can never elect, so the wait never resolves, and the call spins
until its own timeout ("table tablet did not provision in time") rather
than failing fast or erroring cleanly. This is structurally different from
`SimCluster::create_table`/`create_table_with_replication`'s own
hand-hosted path (which bypasses the wire and mints replicas directly on
whichever node indices the caller names) — only the WIRE path
(`create_table_via_wire`, and by extension `SimClusterHandle::dynamo`
issuing a real `CreateTable`) has this requirement.

**The general form**: any `SimCluster` scenario that mixes `NodeRole::
Control`-only nodes with a real wire `CreateTable` needs at least one
`NodeRole::Data`/`Both` node in its role array, even if the scenario's own
subject has nothing to do with the data plane (e.g. proving a schema
proposal commits and relays correctly among CONTROL-only nodes) — the
DDL call itself, not the property under test, is what needs somewhere to
land. When a scenario's own subject is genuinely control-plane-only,
prefer widening the role array by one `Data` node with a comment
explaining why (as `schema_ddl_via_control_node_commits_and_relays` now
does) over trying to prove the property some other way — the DDL call is
otherwise unavoidable groundwork, not part of what's being asserted.
