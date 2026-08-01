# CLAUDE.md — custos-dynamo

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

A DynamoDB-style **item API** plus the **DynamoDB JSON wire encoding** over the
common storage core (ADR 0006) — the data-model + surface-syntax halves of the
adapter wedge. The transport (HTTP, sockets) and the distributed routing live in
`custosd`; this crate stays pure and deterministic.

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
  (`create_table` / `create_table_legacy` / `extract_key`) plus a per-table
  written-key index (`note_put` / `note_delete` / `query_keys`) that backs the
  distributed `Query` (the data plane has no quorum range scan). `RegistryError`
  carries the failure cause.
- `storage_key(pk, sk)` — the data-plane key for an item, exposed so a caller
  can route an item through `custos-data` without instantiating a local `Table`.
- `wire` module — the DynamoDB JSON translation: `decode_request(target, body)
  -> Operation` (CreateTable/PutItem/GetItem/DeleteItem/Query, with optional
  `ConditionExpression` on writes and a `KeyConditionExpression` on Query),
  `encode_item` / `get_item_response` / `empty_response` / `query_response` /
  `create_table_response`, `WireError` (carries the DynamoDB `__type` code,
  incl. `conditional_check_failed`), and `encode_stored_item` /
  `encode_tombstone` / `decode_stored_item` (the data-plane value encoding, with
  a tombstone for delete).

## What's non-obvious

- The `wire`, `condition`, and `registry` modules are all **pure** — no I/O, no
  storage, no network, `BTreeMap`/`BTreeSet` only (ADR 0003). `custosd::dynamo`
  owns the HTTP edge, holds one process-wide `SchemaRegistry` behind a lock, and
  routes decoded ops through the data plane.
- Storage key = `escape(partition_key) || sort_key`, using the same
  order-preserving, prefix-free escape as `custos-storage`'s fjall backend. So a
  partition's items are contiguous and sort-ordered, and `query` is one range
  scan. Numbers (`N`) are carried as text and sort lexicographically (a
  documented simplification). `SortKeyCondition::matches` compares the same
  key-bytes, so it agrees with the scan range.
- `Query` over the **distributed** plane: the data plane (`custos-data`) has no
  quorum range scan, so the registry tracks written item keys per table and
  `query_keys` returns the partition's matching sub-range; `custosd` quorum-reads
  each. The key index (and the schema map) are **in-memory and not durable** —
  rebuilt only from observed writes. `Table::query_with` is the *local-engine*
  equivalent (a real engine scan), used by the item-API tests.
- `CreateTable` records a schema in the registry; `create_table_legacy` registers
  the old `pk`/`sk` convention (sort key optional) so pre-`CreateTable` clients
  keep working unchanged.
- The `Table` item API uses a monotonic version counter seeded from
  `engine.latest_version()`; the wire path instead lets the data-plane
  coordinator assign quorum-derived versions (see `custosd`).
- `B` (binary) is base64 on the wire; the codec is self-contained (no new dep).
- **Still deferred** (don't represent as a full adapter): `Scan`,
  projection/filter expressions, `ReturnValues`, document/set types, secondary
  indexes, durable/replicated table schemas, and a native quorum range scan (so
  `Query` need not track keys). `custos-cql` would map onto the same core the
  same way; it's still a skeleton.

## Tests

`cargo test -p custos-dynamo` — `item_api.rs` over `MemoryEngine` (incl.
`query_with` sort conditions), plus `wire`, `condition`, and `registry` unit
tests (JSON decode/encode, base64 round-trip, tombstone, sort/condition
predicates, key-index range queries). The wire protocol is exercised end-to-end
over real HTTP in `custosd`'s `tests/dynamo_wire.rs` (Put/Get/Delete) and
`tests/dynamo_extended.rs` (CreateTable/Query/conditional writes).
