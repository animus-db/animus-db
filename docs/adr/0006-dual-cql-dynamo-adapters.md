# ADR 0006 — Dual CQL + DynamoDB adapters over a common core

- **Status:** Accepted
- **Amended for v1 (ADR 0019):** the adapters route through the **CP data
  plane** — `DataClient` and the quorum coordinator are deleted with the AP
  plane; the "native quorum range scan" below is now the CP `cp_scan`
  (linearizable, leader-served), and per-request **consistency levels are
  currently inert** (the CQL edge decodes but ignores `[consistency]`; CP reads
  are always linearizable — `consistency_quorum` survives only as the mapping
  for AP's eventual return).
- **Audit note (2026-08-06):** "common core" holds at the *storage/data-plane*
  layer (one `StorageEngine`, one replicated schema catalog, one routing path),
  but **not at the adapter layer**: `animus-cql` and `animus-dynamo` share no
  code (no cross-dependency), and the load-bearing key conventions are
  re-implemented per edge — the ADR 0022 token+key layout is built independently
  by the DynamoDB edge (token over *escaped* pk bytes), the CQL edge (token over
  *raw* pk bytes, unescaped), and the admin seeder, and `escape()` itself exists
  twice (`animus-dynamo` and `animus-tablet`) with no equality test. Safe today
  only because tablets are table-scoped (ADR 0023) so the layouts never share a
  keyspace. A shared key-layout/RMW helper crate consumed by both edges is the
  standing follow-up before any cross-adapter keyspace exists.
- **Date:** 2026-08-01

## Context

Adoption of a new database is gated by migration cost. Two of the largest
NoSQL ecosystems — Cassandra (CQL wire protocol) and DynamoDB (HTTP/JSON API) —
share the Dynamo-lineage data model (ADR 0004). Wire compatibility with either
lets existing applications migrate with little or no code change. This
compatibility is the project's long-term wedge.

## Decision

We will expose **both** a CQL wire-protocol adapter (`animus-cql`) and a
DynamoDB API adapter (`animus-dynamo`) as thin translation layers over the
common map-of-maps core (ADR 0004) and the distributed planes (ADR 0001). The
adapters translate surface syntax and semantics to core operations; they do not
each carry their own engine.

Two slices now exist. First, `animus-dynamo` provides a DynamoDB-style **item
API** (`PutItem`/`GetItem`/`DeleteItem`/`Query`) mapped directly onto the
`StorageEngine` core, demonstrating that the Dynamo-lineage data model
translates cleanly. Second, a **DynamoDB JSON wire protocol** is now served:
`animus-dynamo::wire` is the pure, deterministic translation between the
DynamoDB AttributeValue JSON (`{"S":..}` / `{"N":..}` / `{"B":..}` /
`{"BOOL":..}` / `{"NULL":..}`) and the in-memory item model, and `animusd`
exposes a real HTTP/1.1 endpoint that decodes `X-Amz-Target:
DynamoDB_20120810.{CreateTable,PutItem,GetItem,DeleteItem,Query}` requests and
routes the resulting keys/values **through the distributed data plane** (the
same quorum coordinator the plain-TCP client API uses) rather than a local
engine. The HTTP edge is production-only I/O (hand-rolled over a tokio
`TcpListener`, mirroring `ProdEnv`'s placement of real I/O); everything below it
stays on the `Env`-based paths. The data plane has no native delete yet (ADR
0010), so `DeleteItem` writes a tombstone value that `GetItem` reads back as
absent.

The surface now extends past the original three point ops:

- **`CreateTable` + per-table schemas, now consuming the replicated catalog
  (ADR 0013).** A `CreateTable` request **proposes a `MetaCommand::CreateTableSchema`
  to the control-plane leader** and waits until it commits in `Metadata`, then
  resolves subsequent `PutItem`/`GetItem`/`Query`/`Scan` key attributes from the
  **replicated** `Metadata::table_schema(...)` (translated DynamoDB key attrs ↔
  the control plane's `TableSchema` by `animus_dynamo::schema`). So a created
  table is now **durable and cluster-agreed**: its key schema survives a restart
  (it rode the Raft WAL, not the in-memory registry) and is known on every node.
  A request against a never-created table still falls back to the legacy `pk`/`sk`
  convention. The DynamoDB edge reaches the leader through a process-global set of
  registered control handles (the same process-global pattern the in-memory
  registry uses); in a one-process-per-node deployment that is the node's own
  handle, so `CreateTable` must target the leader (or a node that can reach it).
  **Secondary-index *definitions* now replicate too (ADR 0013):** the per-table
  GSI/LSI declarations live in the table's replicated `TableSchema.indexes`
  (`MetaCommand::{CreateTableIndex, DropTableIndex}`), so index existence/shape is
  cluster-agreed and survives restart. Only the index *entry data* (the actual
  indexed rows) is still maintained in edge-local memory, rebuilt from observed
  writes. The former observation-built
  **written-key index** that backed `Query`/`Scan` is **gone** — base `Query`/`Scan`
  now use the data plane's native range scan (below), so they no longer depend on
  any in-memory tracked key set.
- **`Query`.** Partition-key equality plus an optional sort-key condition (`=`,
  `BETWEEN`, `begins_with`), returning matching items in sort order. A base-table
  `Query` is a **native quorum range scan** (`DataClient::scan`) over the
  partition's contiguous data-plane key sub-range `[escape(table) || escape(pk), …)`
  — no in-memory tracking — applying the sort-key condition on the recovered sort
  bytes after the scan. An **index** `Query` still resolves base keys from the
  in-memory GSI/LSI index (the scan covers the base keyspace, not an index's
  alternate ordering) and quorum-reads each.
- **Conditional writes.** A `ConditionExpression` subset
  (`attribute_not_exists(a)`, `attribute_exists(a)`, `a = :v`) gates `PutItem` /
  `DeleteItem`: the edge quorum-reads the current item under the coordinator
  lock and rejects a failing predicate with `ConditionalCheckFailedException`.
- **`Scan`.** A full-table **native quorum range scan** (`DataClient::scan`) over
  the table's whole data-plane range `[escape(table), …)` across all partitions —
  no in-memory key index. It paginates with `Limit` + `ExclusiveStartKey`/
  `LastEvaluatedKey` (the cursor is a truncated page's last storage key, surfaced
  to the client as that item's key-attribute map) and applies an optional
  `FilterExpression` (the same predicate subset as a conditional write) after the
  read. Because the scan reads live storage, the cursor advances over real keys —
  correct even after a restart or on a node that never observed the write.
- **Secondary indexes (GSI + LSI).** `CreateTable` may declare any number of
  secondary indexes. A **global** secondary index (`GlobalSecondaryIndexes`) has
  a `HASH` key attribute plus an optional `RANGE` (a composite GSI); a **local**
  secondary index (`LocalSecondaryIndexes`) shares the base partition `HASH` and
  adds an alternate `RANGE` sort attribute. Their **definitions** are replicated in
  the control plane's table-schema catalog (ADR 0013) — `TableSchema.indexes`,
  mutated by `MetaCommand::{CreateTableIndex, DropTableIndex}` — so index
  existence/shape is durable + cluster-agreed; the edge rebuilds its in-memory
  index-maintenance machinery from those definitions. The registry maintains, per
  index, an
  `escape(hash) [|| escape(sort)] || base_key` index on every write/delete (it
  stores only base keys, not item copies, so the base item stays authoritative),
  and a `Query` with an `IndexName` resolves a hash value back to its base storage
  keys — narrowed by an optional sort-key condition on a composite GSI / LSI (a
  hash-only GSI rejects one) — which are quorum-read like a base query. Each index
  carries a **declared projection** (`ALL` / `KEYS_ONLY` / `INCLUDE
  NonKeyAttributes`): an index `Query` with no explicit `ProjectionExpression`
  returns exactly the index's projected attribute set (`KEYS_ONLY` ⇒ the base + index
  key attributes; `INCLUDE` ⇒ those plus the listed non-key attributes), applied at
  the edge after the base item is read (the index stores only base keys, never item
  copies, so the projection bounds what is *returned*, not what is stored).
- **Document & set attribute types.** The AttributeValue codec carries the
  document types `M` (map) and `L` (list) and the set types `SS`/`NS`/`BS`
  (string/number/binary sets, kept sorted + deduplicated so the in-memory form is
  canonical), alongside the scalars. Stored items serialize them transparently.
- **Projection expressions, incl. document paths.** GetItem/Query/Scan accept a
  `ProjectionExpression` (a comma-separated list of **dotted document paths**
  `a.b.c`, with `#alias` placeholders per segment via `ExpressionAttributeNames`)
  or the legacy `AttributesToGet` array; the edge keeps only the requested paths
  after the read, **reconstructing the nested map structure** each path reaches
  (projecting `a.b` yields `{a:{b:..}}`). List-index paths (`a[0]`) remain deferred
  (a `[` is rejected). For `Scan` the `FilterExpression` sees the whole item before
  projection trims it.
- **`ReturnValues`.** PutItem/DeleteItem accept `ReturnValues: NONE` (default) or
  `ALL_OLD`; the edge reads the prior item once (reusing it for any condition
  check, so no double read) and echoes it under `Attributes` for `ALL_OLD`.
  `UpdateItem` additionally accepts `ALL_NEW` (the item after the update).
- **`UpdateItem`.** A read-modify-write of one item: the edge reads the current
  item under the coord lock, applies an `UpdateExpression`'s `SET attr = :v` /
  `REMOVE attr` clauses (top-level attributes; `#alias`/`:value` placeholders
  resolved; `ADD`/`DELETE` arithmetic deferred), gating on an optional
  `ConditionExpression`, then quorum-writes the new item (an upsert when the key
  was absent) and echoes `NONE`/`ALL_OLD`/`ALL_NEW`.
- **`BatchWriteItem`.** A batch of `PutRequest`/`DeleteRequest`s grouped by table
  in `RequestItems`, applied request-by-request through the same write path (no
  cross-request atomicity, matching DynamoDB). Always replies
  `{"UnprocessedItems":{}}` (every request is processed).
- **`TransactWriteItems`.** A list of condition-gated `Put`/`Delete`/`Update`/
  `ConditionCheck` actions, each honoring its `ConditionExpression`. **Not yet
  truly atomic:** there is no cross-action rollback (full ACID transactional
  writes route through Accord, ADR 0011, which is deferred), so a failed condition
  rejects the request but actions sequenced before it have already applied. The
  documented gap is the all-or-nothing guarantee; the assert-then-write use is
  served correctly.

A third slice exists on the CQL side: a **Cassandra CQL v4 binary protocol** is
served alongside the DynamoDB endpoint. `animus-cql` is the pure, deterministic
protocol layer; `animusd::cql` is the production-only I/O edge (real tokio
sockets + hand-rolled framing, no third-party CQL/Cassandra crate). It now
carries a real type system and a schema catalog rather than a fixed `(pk, v)`
convention:

- **A type/value system.** `animus_cql::types` models the common scalar CQL
  types — `text`, `int`, `bigint`, `boolean`, `blob`, `uuid` — with
  encode/decode of cell bytes (the contents of a protocol `[bytes]`) and literal
  parsing. Result frames carry proper `[column metadata]` with the real type ids,
  and bound values decode/type-check against the column type.
- **`CREATE TABLE` + keyspaces — control-plane replicated (ADR 0013).**
  `CREATE KEYSPACE`, `USE <keyspace>`, and `CREATE TABLE (... PRIMARY KEY (col))`
  declare a schema (one partition-key column + typed columns). `CREATE TABLE` now
  **proposes the schema into the control plane's Raft-replicated catalog**
  (`MetaCommand::CreateTableSchema`, keyed `keyspace.table`) and waits for it to
  commit, so the table is **durable** (recovered from the Raft WAL/snapshot on
  restart) and **cluster-agreed**, replacing the old per-process in-memory
  catalog. `INSERT`/`SELECT`/`UPDATE`/`DELETE` resolve the schema from the
  replicated `Metadata`. The `animusd` edge maps the CQL type system onto the
  shared `ColumnType` vocabulary and reaches the leader through a process-global
  set of registered control handles (mirroring the DynamoDB edge). A row is
  serialized to one data-plane value (a versioned blob of `(schema column index,
  cell)` pairs) keyed by `escape(table) || pk_key_bytes`. Keyspaces themselves are
  not separately replicated (the catalog models tables); the edge keeps a
  process-local keyspace set plus treats a keyspace with a replicated `ks.table`
  as existing — replicating keyspace metadata is future work.
- **`DROP TABLE` / `ALTER TABLE ... ADD`.** `DROP TABLE [IF EXISTS]` proposes
  `DropTableSchema` and waits for it to replicate. `ALTER TABLE ... ADD <col>
  <type>` appends columns by dropping + recreating the replicated schema with the
  extended column list (column indices are preserved, so stored rows still decode)
  — not atomic across the two steps; an in-place schema-mutation command is future
  work.
- **`BATCH`.** `BEGIN [UNLOGGED|LOGGED] BATCH <mutation>; ... APPLY BATCH` applies
  a sequence of `INSERT`/`UPDATE`/`DELETE` statements in order (not atomically;
  CQL logged-batch atomicity is future work).
- **Prepared statements.** `PREPARE` parses + resolves a statement's `?` bind
  markers against the catalog and replies `RESULT/Prepared` (a
  content-addressed statement id + the bind-variable metadata); `EXECUTE` decodes
  the bound cells against that metadata and runs the statement on the same path
  as `QUERY`. The id is a stable hash of the statement text, so a driver's
  prepare-then-execute path works across connections.

The recognizer (`parse_statement`) accepts `USE` / `CREATE KEYSPACE` /
`CREATE TABLE` / `INSERT` / `SELECT` / `UPDATE` / `DELETE` / `DROP TABLE` /
`ALTER TABLE ... ADD` / `BATCH` (with `?` markers and `keyspace.table` names);
anything outside the subset is rejected cleanly with a CQL `ERROR` frame.
`INSERT`/`UPDATE`/`DELETE`/`EXECUTE`/`BATCH` reply `RESULT/Void`, `SELECT` replies
a typed `RESULT/Rows`, and `USE`/`CREATE`/`DROP`/`ALTER` reply
`SetKeyspace`/`SchemaChange`. Everything routes through the **same quorum
coordinator** the plain-TCP and DynamoDB edges use; everything below the socket
stays on the `Env`-based paths.

The CQL surface now also covers the row-mutation and key-modeling gaps:

- **Clustering columns / compound primary keys.** `CREATE TABLE` accepts
  `PRIMARY KEY (pk, ck1, ck2, ...)` — a single partition-key column plus any
  number of clustering columns (composite multi-column *partition* keys are
  still rejected). Because the data plane offers only point read/write/delete
  (no quorum range scan), the **whole partition** — every row sharing a partition
  key — is stored as one data-plane value keyed by `escape(table) ||
  pk_key_bytes`, an ordered map of clustering-key blob → row. A `SELECT pk = ?`
  returns every row in **clustering order**; adding `AND ck = ?` (every
  clustering column, in order) selects one row. The clustering blob is the
  order-preserving `to_key_bytes` of each clustering value, so a `BTreeMap` over
  it yields clustering order for free.
- **`UPDATE` and `DELETE`.** Both address a row (or partition) by a primary-key
  `WHERE`, routed through the same coordinator. They are **read-modify-write** of
  the partition value at the edge (read the partition, apply the mutation, write
  it back); `UPDATE` is an upsert of non-key cells, `DELETE` removes one row (full
  primary key) or the whole partition (partition-key-only `WHERE`). A `DELETE`
  that empties the partition issues a **data-plane delete/tombstone** (ADR 0010)
  on the key, so it reads back absent and propagates like any tombstone.
- **Consistency levels.** The QUERY/EXECUTE `[consistency]` is decoded and mapped
  (`consistency_quorum`) to a per-request R/W quorum over the tablet's replica
  count — `ONE`→1, `QUORUM`/`LOCAL_QUORUM`→majority, `ALL`→all,
  `TWO`/`THREE`→that many (clamped) — instead of being ignored: the edge
  overrides the `TabletView`'s `r`/`w` per request.

Both adapters now **consume the control plane's replicated table-schema catalog**
(ADR 0013): `CreateTable`/`CREATE TABLE` proposes a `CreateTableSchema` and waits
for commit, so schemas are durable + cluster-agreed (the in-memory per-process
catalogs are gone). The edge maps each adapter's type system onto the shared
`ColumnType` vocabulary and routes the proposal to the control-plane leader via a
process-global set of registered control handles.

What remains. DynamoDB: atomic `TransactWriteItems` (via Accord, ADR 0011),
`BatchGetItem`, list-index document paths (`a[0]`), `ADD`/`DELETE`
`UpdateExpression` arithmetic, and durable control-plane-replicated **secondary-index
*data*** (the index *definitions* now replicate via ADR 0013, but the GSI/LSI
*entry data* — the indexed rows — is still rebuilt from observed writes at the edge).
CQL: composite (multi-column) partition keys, per-column `DELETE`, atomic
logged `BATCH`, in-place `ALTER`, range/`IN`/`ORDER BY`/`LIMIT` predicates with a
native quorum range scan (so a partition need not be one value), collection/UDT
types, paging, authentication, `LWT`/conditional writes, and replicated
**keyspace** metadata (only tables are replicated today). (Now done: both adapters
consume the replicated schema catalog so `CreateTable`/`CREATE TABLE` is durable +
cluster-agreed, and DynamoDB **secondary-index definitions** now replicate in the
same catalog (ADR 0013) so index existence/shape survives restart; DynamoDB
per-index projections, document-path projections,
`UpdateItem`/`BatchWriteItem`/`TransactWriteItems`, document/set types,
`ReturnValues`, composite/multiple GSIs + LSI; CQL clustering/compound primary
keys, `UPDATE`/`DELETE`, consistency levels, `DROP`/`ALTER ADD`/`BATCH`; and a
**native quorum range scan** in the data plane (`DataClient::scan`), now backing
DynamoDB base-table `Query`/`Scan` so they no longer track written keys in memory —
the CQL side still stores a whole partition as one value, but the same primitive can
later carry CQL range/`LIMIT` predicates.)

## Consequences

- Migrating applications can point at AnimusDB with minimal change once the
  adapters exist, which is the adoption wedge.
- Maintaining a single core under two surfaces forces the core to stay
  general-purpose and prevents either surface from leaking into the engine.
- Semantic gaps between CQL and DynamoDB (consistency knobs, type systems,
  conditional writes) will surface as adapter complexity; building the core
  first lets us discover the right shared abstractions before committing.
- **Audit finding (2026-08-06, confirmed — the per-node lock is fixed in
  PR #21; the cross-node CAS remains future work): the DynamoDB edge's
  read-modify-write paths were not atomic even per node.** Conditional
  `PutItem`/`DeleteItem`, `UpdateItem`, and `TransactWriteItems` each do
  read → evaluate → write **without taking the per-node `rmw_lock`** (the CQL
  edge holds it for every RMW), and the CP write below is a blind Raft put (no
  CAS), so nothing compensates: two concurrent `attribute_not_exists` puts on
  one key both succeed. Minimum fix is taking `rmw_lock` on every DynamoDB RMW
  path (per-node atomicity, like CQL); the real fix — needed for cross-node
  atomicity on both edges — is a CP-group CAS/conditional-write primitive
  (`Cas` exists in the CP command set; route conditional writes through it).
