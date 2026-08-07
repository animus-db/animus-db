# ADR 0013 — Replicated table-schema catalog in the control plane

- **Status:** Accepted
- **Date:** 2026-08-02

## Context

Both wire adapters declare table schemas, and both keep them in **per-process,
in-memory catalogs**:

- `animus-dynamo`'s `SchemaRegistry` records a `CreateTable`'s key attributes
  (partition key + optional sort key) and GSIs;
- `animus-cql`'s `Catalog` records a `CREATE TABLE`'s keyspaces and ordered typed
  columns with a partition-key index.

Their own module docs flag the same limitation: the catalog is **not durable**,
so a `CreateTable` / `CREATE TABLE` is lost on restart, and in single-process
`--cluster N` mode one catalog is shared across all in-process nodes (a coupling
that disappears in a real one-process-per-node deployment, leaving each process
with a *different* catalog). A table created on one node is simply not known on
another, and nothing survives a crash.

The control plane (ADR 0001, 0009) already owns the cluster's
strongly-consistent, durable, Raft-replicated metadata — membership, the tablet
map, placement policies — in `Metadata`, mutated by `MetaCommand`s applied by a
deterministic state machine and recovered from the WAL/snapshot. Table schemas
are exactly the same kind of small, cluster-wide, must-agree, must-survive
metadata. They belong there.

As with everything in this project, the state machine must stay deterministic
(ADR 0003): `Metadata::apply` is a pure function, `RaftCore` stays sync and
I/O-free, and only `BTree*` collections are used.

## Decision

We will add a **replicated table-schema catalog** to the control plane, in the
same shape as the existing metadata.

- **State.** `Metadata` gains a `schemas: SchemaCatalog` field — a `BTreeMap`
  from `TableName` (an opaque, case-sensitive identifier the adapter namespaces)
  to a `TableSchema`. Because it lives inside `Metadata`, it is Raft-replicated,
  durable, and recovered from the WAL/snapshot for free: the snapshot is a full
  `Metadata` image, so `schemas` rides along with no change to `persist.rs` or
  the `InstallSnapshot` path.

- **A shape that fits both adapters.** `TableSchema` models the union of the two
  adapters' needs: a required `partition_key` (by name), an ordered list of
  `clustering_keys` (DynamoDB's optional sort key is the one-element case, CQL's
  clustering columns the general case), and a set of typed `columns`
  (`ColumnDef { name, ty }`). `ColumnType` is the union of the CQL scalar types
  (`Int`/`BigInt`/`Uuid`/…) and the DynamoDB key-attribute families
  (`String`/`Number`/`Binary`/`Bool`), so each adapter records its declared type
  faithfully and recovers it on read. The control plane never *interprets* a
  value — it only stores the declared schema — so the breadth is about fidelity
  for the adapters, not validation.

- **Mutations.** Two new `MetaCommand`s, applied deterministically in
  `Metadata::apply`:
  - `CreateTableSchema { table, schema }` — rejected (no state change) if a
    schema for `table` already exists (a create does not silently overwrite) or
    if the schema is malformed (`TableSchema::validate` checks that the partition
    key and clustering keys name real, unique columns, etc.); otherwise records
    it.
  - `DropTableSchema { table }` — idempotent: applies as a no-op if no schema is
    registered.

- **Read accessors.** `Metadata::{table_schema, has_table_schema, table_schemas}`
  expose the catalog for the adapters to consume, and `SchemaCatalog` offers
  `get`/`contains`/`iter`/`table_names`. Mutation is *only* through the two
  `MetaCommand`s.

Wiring the adapters to actually consume this catalog (replacing their in-memory
registries) was a **deliberate follow-up** — and is now done. Both the CQL
(`animusd::cql`) and DynamoDB (`animusd::dynamo`) edges propose
`CreateTableSchema`/`DropTableSchema` on `CREATE TABLE`/`DROP TABLE` and wait for
commit, and resolve reads/writes against the replicated `Metadata` rather than a
per-process catalog. The CQL edge maps its `CqlType` onto `ColumnType` (and back),
keys tables `keyspace.table`, and reaches the leader through a process-global set
of registered control handles (the same mechanism the DynamoDB edge uses); it also
adds `ALTER TABLE ... ADD` on top (a non-atomic drop+recreate of the schema, since
in-place schema evolution is still future work — see Consequences).

## Consequences

- Table schemas are now **durable and cluster-wide consistent**: a committed
  `CreateTableSchema` survives restart, replicates to every node, and is agreed
  by Raft — exactly like membership and placement. Proven end-to-end under
  `SimEnv` in `animus-control/tests/schema_catalog.rs` (propose two schemas,
  reject a duplicate and a malformed one on the state machine, kill the leader,
  assert the schemas survive and the survivors' catalogs agree, then drop one and
  see it replicate), reproducible from a seed.
- `Metadata::apply` stays a pure deterministic function and `RaftCore` stays
  I/O-free; the only new types are plain data with `BTree*` collections.
- **Adapters now consume it.** Both wire edges route `CREATE TABLE`/`DROP TABLE`
  through these `MetaCommand`s and resolve against `Metadata`, so a created table
  is durable and cluster-agreed across the wire — proven over real TCP with a node
  restart in `animusd/tests/cql_durable_schema.rs` (CQL) and the DynamoDB edge's
  schema test. DDL proposals route to the control-plane leader via a
  process-global set of registered control handles (working for the in-process
  `--cluster N` mode; cross-process proposal forwarding over the network is still
  future work — DDL otherwise commits when the connected node is the leader).
- **Secondary-index *definitions* now replicate.** A table's `TableSchema` carries
  an ordered `indexes: Vec<IndexDef>` (GSI/LSI: name, kind, hash/sort attributes,
  projection), mutated by two new deterministic `MetaCommand`s —
  `CreateTableIndex { table, index }` (rejected if the table is unknown or the
  resulting schema is malformed, e.g. an LSI with no sort attribute; replaces an
  index of the same name) and `DropTableIndex { table, index }` (idempotent). They
  ride the same Raft/WAL/snapshot path as the rest of `Metadata`, so an index
  *definition* is durable and cluster-agreed, recovered on restart from the
  replicated catalog rather than per-process memory. Read it via
  `Metadata::table_indexes(table)` (and `TableSchema::index`). Proven end-to-end
  under `SimEnv` in `animus-control/tests/schema_indexes.rs` (create a GSI → a
  second node sees it; reject an index on a phantom table + a malformed LSI;
  restart a node → the definition survives; drop → it disappears cluster-wide),
  reproducible from a seed. The DynamoDB adapter bridges its `SecondaryIndex` ↔
  `IndexDef` (`animus_dynamo::schema::{index_to_control, index_to_dynamo,
  indexes_to_dynamo}`) and rebuilds its index-maintenance machinery from the
  catalog via `SchemaRegistry::sync_indexes` (preserving entry data for an
  unchanged index, clearing it for a changed-shape one). **`animusd` now wires this
  end to end:** its DynamoDB edge (`animusd::dynamo::create_table`) proposes one
  `CreateTableIndex` per declared GSI/LSI (after the table schema commits, since the
  command is rejected unless the table is known) and waits for each to replicate,
  and its catalog-mirror path (`mirror_catalog_schema`) reconciles the local
  registry to `Metadata::table_indexes` via `sync_indexes` — so a created index
  definition is durable + cluster-agreed and a restarted/follower node rebuilds its
  index machinery from the catalog, not process memory. Proven over the real
  DynamoDB JSON/HTTP wire in `animusd/tests/dynamo_schema.rs`
  (`create_table_index_replicates_to_second_node`, `..._survives_node_restart`).
- **Index *data* is not replicated — it is lazily backfilled from a live
  base-table scan, not left to silently return incomplete results.** The index
  *entry data* — the actual indexed rows — still lives only at the wire edge,
  maintained by observed `note_put`/`note_delete` writes (the same
  cluster-agreed-definition-but-edge-local-data split ADR 0013 always intended:
  replicate the small, must-agree *shape*; keep the large, easily-rederived
  *data* at the edge). What closes the gap this section used to flag as future
  work: a freshly restarted node (or a follower/second node that never observed
  the writes) does **not** silently serve an empty/incomplete index forever.
  `SchemaRegistry` tracks a per-index `backfilled` flag (`false` for a newly
  created or shape-changed index — including one just rebuilt from the
  replicated catalog by `sync_indexes`), and the DynamoDB edge's
  `backfill_index_if_needed` runs **once, lazily, on the first query** against
  such an index: a single linearizable base-table scan (the same
  `DataClient::scan` a base `Query`/`Scan` already uses), replayed through
  `note_put` to populate every index of the table in one pass, then
  `mark_table_backfilled`. This is chosen over the alternative of *deriving*
  every index query from a live base-table scan (no edge-local index at all):
  a GSI/LSI's whole purpose is an *alternate* key ordering, which a range scan
  over the base table's own key order cannot serve without scanning (and
  filtering) the entire table per query — the one-time backfill keeps the
  steady-state query O(index size), pays the base-table-scan cost exactly once
  per (re)creation of an index's machinery, and needs no new control-plane
  state or replicated index tablets. Correctness is proven end to end over the
  real DynamoDB wire in `animusd/tests/dynamo_schema.rs`:
  `create_table_index_survives_node_restart` (a restart wipes the registry; the
  first post-restart GSI query still returns the pre-restart item, **without
  re-writing it**) and `create_table_index_replicates_to_second_node` (a write
  via node 0, queried via node 1, whose registry never observed that write —
  the GSI query on node 1 rebuilds the index from the replicated *definition*
  and backfills its *data* from a live scan). **A write racing the backfill's
  scan is not lost or duplicated**: the scan runs without the registry lock (it
  is a network read), so a concurrent write's own `note_put` can land — and
  correctly index the item's *current* value — before the backfill's replay of
  its (older) scanned value would otherwise apply. `SchemaRegistry` tracks
  which keys were touched by a real `note_put`/`note_delete` since the backfill
  became pending (`touched_since_backfill`) and the replay skips any such key,
  so it can only ever *seed* a key nobody has independently already indexed
  correctly — never revert one. Unit-proven in
  `animus-dynamo/src/registry.rs::racing_write_during_backfill_is_not_reverted_by_the_stale_replay`
  (plus a sibling test that an untouched key is still seeded normally).
  Same-table, same-node correctness (a write immediately followed by a query on
  the *same* connection with no restart in between) was already correct before
  this ADR — nothing here changes that path.
- **Costs / follow-up:** CQL keyspace objects are still **not** modelled here —
  the schema captures key structure + typed columns + index definitions; keyspace
  metadata can extend `SchemaCatalog` later without changing the replication
  mechanism. In-place schema *evolution* is still future work: the CQL edge's
  `ALTER TABLE ADD` is a non-atomic drop+recreate, and there is no
  `AlterTableSchema` `MetaCommand` yet.
