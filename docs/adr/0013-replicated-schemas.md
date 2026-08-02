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
registries) is a **deliberate follow-up** — this ADR delivers the control-plane
substrate and the accessors.

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
- **Costs / follow-up:** the adapters still hold their own in-memory catalogs
  until a separate change routes their `CreateTable`/`CREATE TABLE` through a
  `CreateTableSchema` proposal and resolves reads against `Metadata`. Secondary
  indexes (DynamoDB GSIs) and CQL keyspace objects are **not** modelled here yet —
  the schema captures key structure + typed columns, which is the shared core;
  index/keyspace metadata can extend `TableSchema`/`SchemaCatalog` later without
  changing the replication mechanism. Schema *evolution* (`ALTER`) is also future
  work.
