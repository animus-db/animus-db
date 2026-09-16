# `SimCluster::create_table`'s hand-hosted replica set is positional (`0..replication`) — wrong for a mixed control+data cluster; a wire-created table's recorded RF is a fixed *target*, not a snapshot (ADR 0061 rung M, C-13 PR 3)

Building the data-only sibling of `join_via_seed`'s own combined-mode sim
test, the obvious-looking shortcut for seeding a table before the join was
`SimCluster::create_table`/`create_table_with_replication` — every other
scenario in `sim_cluster_seed_join.rs` already uses it. That shortcut
hand-hosts the tablet on node indices `0..replication`, unconditionally —
correct for an all-`NodeRole::Both` cluster (`SimCluster::new`'s own
default), but silently wrong for a mixed control+data cluster
(`SimCluster::new_with_roles`), where indices `0..control_count` are
CONTROL-ONLY nodes that can never host a CP-data group at all. Every
`sim_cluster_*` module that already mixes roles (`sim_cluster_control_
data_split.rs`, `sim_cluster_split_cluster.rs`) avoids this by always
provisioning through the real wire (`create_table_via_wire`, a `CreateTable`
dispatched through `ClientCtx::provision_tablet`), which picks its initial
replica set from `Metadata::members` — a set that, thanks to `RegisterNode`'s
`claims_membership` gate, never contains a control-only node in the first
place. **The general rule this generalizes to**: in any `SimCluster`
scenario built with `new_with_roles`, reach for the wire path first: the
hand-hosted shortcut's `0..replication` convention is only ever safe when
every node in that range is data-capable, which a mixed-role cluster does
not guarantee just because the caller picked a small `replication` number.

A second, non-obvious fact fell out of reading `ClientCtx::
provision_tablet`'s own doc while diagnosing this: the policy a wire
`CreateTable` records is always `PlacementPolicy::simple("cp-rf",
MAX_REPLICATION_FACTOR)` — the *target*, never however many candidates
happened to be `Active` at creation time. A table created against a
cluster with fewer `Active` data-capable members than `MAX_REPLICATION_
FACTOR` (3) is silently under-replicated relative to its own recorded
policy from the moment it's created — which means a data-only node that
joins later and reaches `Active` doesn't need a load-balance heuristic to
gain a replica of every pre-existing table; `reconcile_placement`'s
ordinary violation-repair path (the same one that replaces a killed
replica) does it as a plain policy-satisfaction repair, deterministically,
the instant capacity exists. This is a *stronger* guarantee than `tests/
data_join.rs`'s own doc comment implies ("seed enough tables that the
rebalancer is guaranteed to move something," framed as a balance
concern) — worth knowing before reflexively seeding several tables out of
caution: one table already suffices whenever the pre-join replica count is
below `MAX_REPLICATION_FACTOR`, though mirroring the original test's own
table count one-for-one is still the right choice when the point is a
faithful conversion, not a minimal reproduction.
