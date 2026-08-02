# CLAUDE.md — animus-dynamo

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

A DynamoDB-style **item API** plus the **DynamoDB JSON wire encoding** over the
common storage core (ADR 0006) — the data-model + surface-syntax halves of the
adapter wedge. The transport (HTTP, sockets) and the distributed routing live in
`animusd`; this crate stays pure and deterministic.

## Entry points

- `AttributeValue` (S/N/B/Bool/Null), `Item`, `TableSchema` (`simple` /
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
  and `Scan` (the data plane has no quorum range scan), and per-table GSI
  indexes (`index_query_keys`). `GlobalSecondaryIndex` is the (name +
  key-attribute) declaration; `RegistryError` carries the failure cause.
- `storage_key(pk, sk)` — the data-plane key for an item, exposed so a caller
  can route an item through `animus-data` without instantiating a local `Table`.
- `wire` module — the DynamoDB JSON translation: `decode_request(target, body)
  -> Operation` (CreateTable/PutItem/GetItem/DeleteItem/Query/Scan; `CreateTable`
  decodes a `GlobalSecondaryIndexes` declaration, `Query` an optional
  `IndexName`, `Scan` a `Limit`/`ExclusiveStartKey`/`FilterExpression`, plus the
  existing `ConditionExpression` on writes and `KeyConditionExpression` on
  Query), `encode_item` / `get_item_response` / `empty_response` /
  `query_response` / `scan_response` / `create_table_response`, `WireError`
  (carries the DynamoDB `__type` code, incl. `conditional_check_failed`), and
  `encode_stored_item` / `encode_tombstone` / `decode_stored_item` (the
  data-plane value encoding, with a tombstone for delete).

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
- **GSI** (one hash-only index per table): `note_put` extracts the indexed
  attribute and records an `escape(gsi_value) || base_key` entry (re-indexing on
  overwrite, since the attribute may change); `note_delete` drops it. Only base
  keys are stored — never item copies — so the base item is the single source of
  truth. `index_query_keys` resolves a GSI value's contiguous entry sub-range
  back to its base keys (recovered past the prefix-free `escape`), which the
  caller quorum-reads. A GSI query is hash-equality only (no sort condition).
- `CreateTable` records a schema in the registry; `create_table_legacy` registers
  the old `pk`/`sk` convention (sort key optional) so pre-`CreateTable` clients
  keep working unchanged.
- The `Table` item API uses a monotonic version counter seeded from
  `engine.latest_version()`; the wire path instead lets the data-plane
  coordinator assign quorum-derived versions (see `animusd`).
- `B` (binary) is base64 on the wire; the codec is self-contained (no new dep).
- **Still deferred** (don't represent as a full adapter): projection
  expressions, `ReturnValues`, document/set types, composite/multiple GSIs and
  local secondary indexes, durable/replicated table schemas + key/GSI indexes,
  and a native quorum range scan (so `Query`/`Scan` need not track keys). The
  `Scan`/`Query` `FilterExpression` reuses the `ConditionExpression` predicate
  subset (`attribute_exists`/`attribute_not_exists`/`a = :v`), not the fuller
  filter grammar. `animus-cql` would map onto the same core the same way.

## Tests

`cargo test -p animus-dynamo` — `item_api.rs` over `MemoryEngine` (incl.
`query_with` sort conditions), plus `wire`, `condition`, and `registry` unit
tests (JSON decode/encode, base64 round-trip, tombstone, sort/condition
predicates, key-index range queries, scan pagination, GSI write/overwrite/delete
+ index query). The wire protocol is exercised end-to-end over real HTTP in
`animusd`'s `tests/dynamo_wire.rs` (Put/Get/Delete), `tests/dynamo_extended.rs`
(CreateTable/Query/conditional writes), and `tests/dynamo_indexes.rs` (Scan with
pagination + filter, and a GSI write-then-query).
