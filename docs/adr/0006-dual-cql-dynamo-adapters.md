# ADR 0006 — Dual CQL + DynamoDB adapters over a common core

- **Status:** Accepted
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

- **`CreateTable` + per-table schemas.** A `CreateTable` request records a
  table's key schema (partition `HASH` + optional sort `RANGE` attribute names)
  in a `SchemaRegistry`, so the key convention is no longer hard-coded; later
  requests resolve their key attributes against it. A request against a
  never-created table falls back to the legacy `pk`/`sk` convention. The
  registry is **in-memory and not durable** (lost on restart; shared across
  nodes in single-process `--cluster N` dev mode) — replicating schemas through
  the control plane is future work.
- **`Query`.** Partition-key equality plus an optional sort-key condition (`=`,
  `BETWEEN`, `begins_with`), returning matching items in sort order. The data
  plane exposes only point read/write/delete (no quorum range scan), so the
  registry additionally tracks per-table written item keys; `Query` selects the
  partition's contiguous matching sub-range and quorum-reads each key through
  the coordinator (an honest range scan over a *tracked*, in-memory keyspace).
- **Conditional writes.** A `ConditionExpression` subset
  (`attribute_not_exists(a)`, `attribute_exists(a)`, `a = :v`) gates `PutItem` /
  `DeleteItem`: the edge quorum-reads the current item under the coordinator
  lock and rejects a failing predicate with `ConditionalCheckFailedException`.
- **`Scan`.** A full-table read over the same per-table key index, walked across
  all partitions (`scan_keys`) and quorum-read key by key. It paginates with
  `Limit` + `ExclusiveStartKey`/`LastEvaluatedKey` (the cursor is a page's last
  base storage key, surfaced to the client as the key item's AttributeValue map)
  and applies an optional `FilterExpression` (the same predicate subset as a
  conditional write) after the read. Same in-memory-keyspace caveat as `Query`.
- **Secondary indexes (GSI + LSI).** `CreateTable` may declare any number of
  secondary indexes. A **global** secondary index (`GlobalSecondaryIndexes`) has
  a `HASH` key attribute plus an optional `RANGE` (a composite GSI); a **local**
  secondary index (`LocalSecondaryIndexes`) shares the base partition `HASH` and
  adds an alternate `RANGE` sort attribute. The registry maintains, per index, an
  `escape(hash) [|| escape(sort)] || base_key` index on every write/delete (it
  stores only base keys, not item copies, so the base item stays authoritative),
  and a `Query` with an `IndexName` resolves a hash value back to its base storage
  keys — narrowed by an optional sort-key condition on a composite GSI / LSI (a
  hash-only GSI rejects one) — which are quorum-read like a base query. Deferred:
  per-index projection attribute lists (every index projects `ALL`).
- **Document & set attribute types.** The AttributeValue codec carries the
  document types `M` (map) and `L` (list) and the set types `SS`/`NS`/`BS`
  (string/number/binary sets, kept sorted + deduplicated so the in-memory form is
  canonical), alongside the scalars. Stored items serialize them transparently.
- **Projection expressions.** GetItem/Query/Scan accept a `ProjectionExpression`
  (a comma-separated list of top-level attribute names, with `#alias`
  placeholders via `ExpressionAttributeNames`) or the legacy `AttributesToGet`
  array; the edge keeps only the requested attributes after the read. Top-level
  only — a document-path name (`a.b`) is rejected. For `Scan` the
  `FilterExpression` sees the whole item before projection trims it.
- **`ReturnValues`.** PutItem/DeleteItem accept `ReturnValues: NONE` (default) or
  `ALL_OLD`; the edge reads the prior item once (reusing it for any condition
  check, so no double read) and echoes it under `Attributes` for `ALL_OLD`.

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
- **`CREATE TABLE` + keyspaces.** `CREATE KEYSPACE`, `USE <keyspace>`, and
  `CREATE TABLE (... PRIMARY KEY (col))` record a schema (one partition-key
  column + typed columns) in an in-memory `Catalog`, so `INSERT`/`SELECT` resolve
  their columns against the declared schema. A row is serialized to one
  data-plane value (a versioned, self-describing blob of `(schema column index,
  cell)` pairs) keyed by `escape(table) || pk_key_bytes`. The catalog is
  **in-memory and not durable** (lost on restart; shared across in-process nodes
  in `--cluster N` dev mode) — replicating schemas through the control plane is
  future work, exactly as on the DynamoDB side.
- **Prepared statements.** `PREPARE` parses + resolves a statement's `?` bind
  markers against the catalog and replies `RESULT/Prepared` (a
  content-addressed statement id + the bind-variable metadata); `EXECUTE` decodes
  the bound cells against that metadata and runs the statement on the same path
  as `QUERY`. The id is a stable hash of the statement text, so a driver's
  prepare-then-execute path works across connections.

The recognizer (`parse_statement`) accepts `USE` / `CREATE KEYSPACE` /
`CREATE TABLE` / `INSERT` / `SELECT` / `UPDATE` / `DELETE` (with `?` markers and
`keyspace.table` names); anything outside the subset is rejected cleanly with a
CQL `ERROR` frame. `INSERT`/`UPDATE`/`DELETE`/`EXECUTE` reply `RESULT/Void`,
`SELECT` replies a typed `RESULT/Rows`, and `USE`/`CREATE` reply
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

What remains. DynamoDB: per-index projection attribute lists (every index
projects `ALL`), document-path projections (`a.b`),
`UpdateItem`/`BatchWriteItem`/`TransactWrite`, and durable
control-plane-replicated table schemas + key/index state (plus a native quorum
range scan so `Query`/`Scan` need not track keys). CQL: composite (multi-column)
partition keys, the remaining statement kinds (`BATCH`/`ALTER`/`DROP`, per-column
`DELETE`), range/`IN`/`ORDER BY`/`LIMIT` predicates with a native quorum range
scan (so a partition need not be one value), collection/UDT types, paging,
authentication, `LWT`/conditional writes, and durable control-plane-replicated
schemas. (Now done: clustering/compound primary keys, `UPDATE`/`DELETE`, CQL
consistency levels; DynamoDB document/set types, projection, `ReturnValues`,
composite/multiple GSIs + LSI.)

## Consequences

- Migrating applications can point at AnimusDB with minimal change once the
  adapters exist, which is the adoption wedge.
- Maintaining a single core under two surfaces forces the core to stay
  general-purpose and prevents either surface from leaking into the engine.
- Semantic gaps between CQL and DynamoDB (consistency knobs, type systems,
  conditional writes) will surface as adapter complexity; building the core
  first lets us discover the right shared abstractions before committing.
