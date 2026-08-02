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
  indexes (`index_query_keys` + `index_is_composite`). `SecondaryIndex` is either
  a `GlobalSecondaryIndex` (name + hash key attribute + optional sort attribute,
  i.e. hash-only or composite) or a `LocalSecondaryIndex` (name + alternate sort
  attribute, hashing by the base partition key). Any number of indexes per table.
  `RegistryError` carries the failure cause (incl. `IndexSortMismatch` for a sort
  condition against a hash-only index).
- `storage_key(pk, sk)` — the data-plane key for an item, exposed so a caller
  can route an item through `animus-data` without instantiating a local `Table`.
- `wire` module — the DynamoDB JSON translation: `decode_request(target, body)
  -> Operation` (CreateTable/PutItem/GetItem/DeleteItem/Query/Scan; `CreateTable`
  decodes `GlobalSecondaryIndexes` (hash-only or composite) +
  `LocalSecondaryIndexes`, `Query` an optional `IndexName` + a sort condition
  (allowed on a composite GSI / LSI), `Scan` a
  `Limit`/`ExclusiveStartKey`/`FilterExpression`, GetItem/Query/Scan an optional
  `ProjectionExpression`/`AttributesToGet`, Put/DeleteItem an optional
  `ReturnValues`, plus the existing `ConditionExpression` on writes and
  `KeyConditionExpression` on Query). The AttributeValue codec encodes/decodes
  the full type set incl. `M`/`L`/`SS`/`NS`/`BS`. `Projection` (with `apply` /
  the free `project`) is a pure top-level attribute filter; `ReturnValues`
  (`None`/`AllOld`) drives `write_response`. Plus `encode_item` /
  `get_item_response` / `empty_response` / `query_response` / `scan_response` /
  `create_table_response`, `WireError` (carries the DynamoDB `__type` code, incl.
  `conditional_check_failed`), and `encode_stored_item` / `encode_tombstone` /
  `decode_stored_item` (the data-plane value encoding, with a tombstone for
  delete).

## What's non-obvious

- The `wire`, `condition`, and `registry` modules are all **pure** — no I/O, no
  storage, no network, `BTreeMap`/`BTreeSet` only (ADR 0003). `animusd::dynamo`
  owns the HTTP edge, holds one process-wide `SchemaRegistry` behind a lock, and
  routes decoded ops through the data plane.
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
  keep working unchanged.
- The `Table` item API uses a monotonic version counter seeded from
  `engine.latest_version()`; the wire path instead lets the data-plane
  coordinator assign quorum-derived versions (see `animusd`).
- `B` (binary) and `BS` elements are base64 on the wire; the codec is
  self-contained (no new dep).
- **Projection** is **top-level only**: `ProjectionExpression` accepts a
  comma-separated list of attribute names (with `#alias` placeholders resolved
  against `ExpressionAttributeNames`), or the legacy `AttributesToGet` array; a
  document-path name (containing `.` or `[`) is rejected so the limitation is
  explicit. Projection is applied at the edge (`animusd`) after the read; for
  `Scan` the `FilterExpression` sees the whole item *before* projection trims it.
- **`ReturnValues`** supports `NONE` (default) and `ALL_OLD`; the edge reads the
  prior item once (reusing it for any `ConditionExpression` check, so there is no
  double read) and `write_response` echoes it under `Attributes` for `ALL_OLD`.
  `UPDATED_OLD`/`ALL_NEW`/`UPDATED_NEW` are `UpdateItem`-only and rejected.
- **Still deferred** (don't represent as a full adapter): per-index projection
  attribute lists (every index here projects `ALL`), document-path projections,
  `UpdateItem`/`BatchWriteItem`/`TransactWrite`, durable/replicated table schemas
  + key/index state, and a native quorum range scan (so `Query`/`Scan` need not
  track keys). The `Scan`/`Query` `FilterExpression` reuses the
  `ConditionExpression` predicate subset
  (`attribute_exists`/`attribute_not_exists`/`a = :v`), not the fuller filter
  grammar. `animus-cql` would map onto the same core the same way.

## Tests

`cargo test -p animus-dynamo` — `item_api.rs` over `MemoryEngine` (incl.
`query_with` sort conditions), plus `wire`, `condition`, and `registry` unit
tests (JSON decode/encode incl. document/set types + projection + ReturnValues,
base64 round-trip, tombstone, sort/condition predicates, key-index range queries,
scan pagination, GSI/LSI write/overwrite/delete + index query, multiple GSIs, and
composite-index sort narrowing). The wire protocol is exercised end-to-end over
real HTTP in `animusd`'s `tests/dynamo_wire.rs` (Put/Get/Delete),
`tests/dynamo_extended.rs` (CreateTable/Query/conditional writes),
`tests/dynamo_indexes.rs` (Scan with pagination + filter, and a GSI
write-then-query), and `tests/dynamo_documents.rs` (document/set types,
projection, `ReturnValues: ALL_OLD`, multiple + composite GSIs, and an LSI).
