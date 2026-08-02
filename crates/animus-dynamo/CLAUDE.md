# CLAUDE.md — animus-dynamo

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

A DynamoDB-style **item API** plus the **DynamoDB JSON wire encoding** over the
common storage core (ADR 0006) — the data-model + surface-syntax halves of the
adapter wedge. The transport (HTTP, sockets) and the distributed routing live in
`animusd`; this crate stays pure and deterministic.

## Entry points

- `AttributeValue` — scalars (`S`/`N`/`B`/`Bool`/`Null`), **document** types
  `M` (map) / `L` (list), and **set** types `SS`/`NS`/`BS` (string/number/binary
  sets, kept sorted + deduplicated so equality and storage are canonical). Only
  scalar types are valid key attributes (document/set `key_bytes()` is empty;
  the schema layer never routes them as keys). `Item`, `TableSchema` (`simple` /
  `composite`).
- `Table<S: StorageEngine>` — `put_item`, `get_item`, `delete_item`, `query` /
  `query_with` (the local-engine item API; `query_with` takes an optional
  `SortKeyCondition`).
- `condition` module — `SortKeyCondition` (`Equals` / `Between` / `BeginsWith`,
  with `matches`) and `ConditionExpression` (`AttributeNotExists` /
  `AttributeExists` / `Equals`, with `evaluate(current)`) — pure predicates for
  `Query` sort conditions and conditional writes.
- `registry` module — `SchemaRegistry`: a pure, in-memory per-table schema map
  (`create_table` / `create_table_with_indexes` / `create_table_legacy` /
  `extract_key`) plus a per-table written-key index (`note_put` /
  `note_delete` / `query_keys` / `scan_keys`) that backs the distributed `Query`
  and `Scan` (the data plane has no quorum range scan), and per-table secondary
  indexes (`index_query_keys` + `index_is_composite` + `index_projected_attributes`).
  `SecondaryIndex` is either a `GlobalSecondaryIndex` (name + hash key attribute +
  optional sort attribute + `IndexProjection`) or a `LocalSecondaryIndex` (name +
  alternate sort attribute + `IndexProjection`, hashing by the base partition key).
  Any number of indexes per table. `IndexProjection` is `All` / `KeysOnly` /
  `Include(names)`; `index_projected_attributes` resolves it to the returned
  attribute set (`None` ⇒ all). `RegistryError` carries the failure cause (incl.
  `IndexSortMismatch` for a sort condition against a hash-only index).
  **Note:** `animusd` now keeps the *table key schema* in the **control plane's
  replicated catalog** (ADR 0013) and uses this registry only for the GSI/LSI
  declarations + the written-key index (both still in-memory, rebuilt from writes).
- `schema` module — the pure bridge between this crate's DynamoDB `TableSchema`
  (partition key + optional sort key) and the control plane's `TableSchema`
  (`animus_control`: partition key + ordered clustering keys + typed columns):
  `to_control(schema, key_types)` (DynamoDB simple/composite → control schema,
  recording key columns with their `AttributeType`) and `to_dynamo(control)` (back,
  taking the first clustering key as the DynamoDB sort key, ignoring extra CQL
  clustering columns). `animusd` uses it to propose/read schemas via the catalog.
- `storage_key(pk, sk)` — the data-plane key for an item, exposed so a caller
  can route an item through `animus-data` without instantiating a local `Table`.
- `wire` module — the DynamoDB JSON translation: `decode_request(target, body)
  -> Operation` (CreateTable/PutItem/GetItem/DeleteItem/Query/Scan/**UpdateItem**/
  **BatchWriteItem**/**TransactWriteItems**; `CreateTable` decodes
  `GlobalSecondaryIndexes` (hash-only or composite) + `LocalSecondaryIndexes`,
  each with an optional `Projection` (`ALL`/`KEYS_ONLY`/`INCLUDE`), `Query` an
  optional `IndexName` + a sort condition (allowed on a composite GSI / LSI),
  `Scan` a `Limit`/`ExclusiveStartKey`/`FilterExpression`, GetItem/Query/Scan an
  optional `ProjectionExpression`/`AttributesToGet`, Put/DeleteItem an optional
  `ReturnValues`, plus the existing `ConditionExpression` on writes and
  `KeyConditionExpression` on Query; `UpdateItem` decodes a `SET`/`REMOVE`
  `UpdateExpression` into `Vec<UpdateAction>` + `UpdateReturnValues`
  (`NONE`/`ALL_OLD`/`ALL_NEW`); `BatchWriteItem` a `RequestItems` map of
  `Put`/`Delete` `WriteRequest`s per table; `TransactWriteItems` a list of
  `TransactAction` (`Put`/`Delete`/`Update`/`ConditionCheck`)). The AttributeValue
  codec encodes/decodes the full type set incl. `M`/`L`/`SS`/`NS`/`BS`.
  `Projection` (with `apply` / the free `project`) is a pure **dotted document-path**
  filter (`a.b`, reconstructing nested maps); `ReturnValues` (`None`/`AllOld`)
  drives `write_response`, `UpdateReturnValues` drives `update_response`, and
  `apply_update` applies the `SET`/`REMOVE` actions. Plus `encode_item` /
  `get_item_response` / `empty_response` / `query_response` / `scan_response` /
  `create_table_response` / `batch_write_response`, `WireError` (carries the
  DynamoDB `__type` code, incl. `conditional_check_failed`), and
  `encode_stored_item` / `encode_tombstone` / `decode_stored_item` (the data-plane
  value encoding, with a tombstone for delete).

## What's non-obvious

- The `wire`, `condition`, `registry`, and `schema` modules are all **pure** — no
  I/O, no storage, no network, `BTreeMap`/`BTreeSet` only (ADR 0003).
  `animusd::dynamo` owns the HTTP edge, **proposes `CreateTable`'s key schema into
  the control plane's replicated catalog (ADR 0013)** and reads schemas back from
  `Metadata`, holds one process-wide `SchemaRegistry` (now only GSI/LSI + the
  written-key index) behind a lock, and routes decoded ops through the data plane.
- Storage key = `escape(partition_key) || sort_key`, using an order-preserving,
  prefix-free escape (no key's encoding prefixes another's). So a partition's
  items are contiguous and sort-ordered, and `query` is one range scan. Numbers (`N`) are carried as text and sort lexicographically (a
  documented simplification). `SortKeyCondition::matches` compares the same
  key-bytes, so it agrees with the scan range.
- `Query` / `Scan` over the **distributed** plane: the data plane
  (`animus-data`) has no quorum range scan, so the registry tracks written item
  keys per table. `query_keys` returns a partition's matching sub-range;
  `scan_keys` walks the whole ordered index across partitions with a cursor
  (`ExclusiveStartKey`/`LastEvaluatedKey` pagination, returned as `ScanPage`);
  `animusd` quorum-reads each key. The key index (and the schema map) are
  **in-memory and not durable** — rebuilt only from observed writes.
  `Table::query_with` is the *local-engine* equivalent (a real engine scan),
  used by the item-API tests.
- **Secondary indexes** (any number per table, GSI + LSI): `note_put` extracts
  each index's hash attribute (for an LSI: the base partition key) and, for a
  composite index, its sort attribute, recording an
  `escape(hash) [|| escape(sort)] || base_key` entry (re-indexing on overwrite,
  since attributes may change — the stale entry is dropped by recovering the base
  key past 1 or 2 escaped segments); `note_delete` drops it. Only base keys are
  stored — never item copies — so the base item is the single source of truth.
  `index_query_keys` resolves a hash value's contiguous entry sub-range back to
  its base keys, optionally narrowing by a `SortKeyCondition` on the recovered
  sort bytes (a hash-only index rejects a sort condition with
  `IndexSortMismatch`). The escapes are prefix-free, so the sort value and base
  key are recoverable and one hash value's entries are contiguous and
  sort-ordered.
- `CreateTable` records a schema in the registry; `create_table_legacy` registers
  the old `pk`/`sk` convention (sort key optional) so pre-`CreateTable` clients
  keep working unchanged. **In `animusd` the authoritative key schema is the
  replicated catalog (ADR 0013)** — the registry's copy is a lazily-rebuilt mirror
  (so its GSI/key-index machinery has a schema); a table absent from the catalog
  is the legacy fallback.
- The `Table` item API uses a monotonic version counter seeded from
  `engine.latest_version()`; the wire path instead lets the data-plane
  coordinator assign quorum-derived versions (see `animusd`).
- `B` (binary) and `BS` elements are base64 on the wire; the codec is
  self-contained (no new dep).
- **Projection** supports **dotted document paths**: `ProjectionExpression` is a
  comma-separated list of paths `a.b.c` (with `#alias` placeholders per segment via
  `ExpressionAttributeNames`), or the legacy `AttributesToGet` array (top-level
  names). `Projection::apply` reconstructs the nested map structure a path reaches
  (`a.b` ⇒ `{a:{b:..}}`). A list-index path (`a[0]`, any `[`) is still rejected.
  Projection is applied at the edge (`animusd`) after the read; for `Scan` the
  `FilterExpression` sees the whole item *before* projection trims it. An index
  `Query` with no explicit projection falls back to the index's declared
  `IndexProjection`.
- **`ReturnValues`** supports `NONE` (default) and `ALL_OLD` on Put/Delete; the
  edge reads the prior item once (reusing it for any `ConditionExpression` check,
  so there is no double read) and `write_response` echoes it under `Attributes`.
  `UpdateItem` additionally supports `ALL_NEW` (`update_response`); `UPDATED_OLD`/
  `UPDATED_NEW` remain deferred.
- **`UpdateItem`/`BatchWriteItem`/`TransactWriteItems`** are decoded here and run
  at the `animusd` edge. `UpdateItem` is read-modify-write of one item applying
  `SET`/`REMOVE` (upsert when absent); `BatchWriteItem` applies `Put`/`Delete`
  per request (no batch atomicity); `TransactWriteItems` applies condition-gated
  `Put`/`Delete`/`Update`/`ConditionCheck` in order — **honoring each condition
  but without cross-action rollback** (true ACID via Accord, ADR 0011, is deferred).
- **Still deferred** (don't represent as a full adapter): truly atomic
  `TransactWriteItems`, `BatchGetItem`, list-index document paths (`a[0]`),
  `ADD`/`DELETE` `UpdateExpression` arithmetic, durable/replicated **secondary-index
  + written-key state** (only the *table key schema* is in the control plane), and
  a native quorum range scan (so `Query`/`Scan` need not track keys). The
  `Scan`/`Query` `FilterExpression` reuses the `ConditionExpression` predicate
  subset (`attribute_exists`/`attribute_not_exists`/`a = :v`), not the fuller
  filter grammar. `animus-cql` would map onto the same core the same way.

## Tests

`cargo test -p animus-dynamo` — `item_api.rs` over `MemoryEngine` (incl.
`query_with` sort conditions), plus `wire`, `condition`, `registry`, and `schema`
unit tests (JSON decode/encode incl. document/set types + document-path projection
+ ReturnValues + UpdateItem/BatchWriteItem/TransactWriteItems decode + index
projection types, base64 round-trip, tombstone, sort/condition predicates,
key-index range queries, scan pagination, GSI/LSI write/overwrite/delete + index
query, multiple GSIs, composite-index sort narrowing, and the DynamoDB↔control
`TableSchema` bridge). The wire protocol is exercised end-to-end over real HTTP in
`animusd`'s `tests/dynamo_wire.rs` (Put/Get/Delete), `tests/dynamo_extended.rs`
(CreateTable/Query/conditional writes), `tests/dynamo_indexes.rs` (Scan with
pagination + filter, and a GSI write-then-query), `tests/dynamo_documents.rs`
(document/set types, projection, `ReturnValues: ALL_OLD`, multiple + composite
GSIs, and an LSI), and `tests/dynamo_schema.rs` (**CreateTable consuming the
replicated catalog — surviving a node restart**, plus UpdateItem/BatchWriteItem/
TransactWriteItems, document-path projection, and a `KEYS_ONLY` GSI projection).
