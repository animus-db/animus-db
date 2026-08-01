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
- `Table<S: StorageEngine>` — `put_item`, `get_item`, `delete_item`, `query`
  (the local-engine item API).
- `storage_key(pk, sk)` — the data-plane key for an item, exposed so a caller
  can route an item through `custos-data` without instantiating a local `Table`.
- `wire` module — the DynamoDB JSON translation: `decode_request(target, body)
  -> Operation` (PutItem/GetItem/DeleteItem), `encode_item` / `get_item_response`
  / `empty_response`, `WireError` (carries the DynamoDB `__type` code), and
  `encode_stored_item` / `encode_tombstone` / `decode_stored_item` (the
  data-plane value encoding, with a tombstone for delete).

## What's non-obvious

- The `wire` module is **pure** — no I/O, no storage, no network. It is the
  surface-syntax translation only; `custosd::dynamo` owns the HTTP edge and
  routes decoded ops through the data plane.
- Storage key = `escape(partition_key) || sort_key`, using the same
  order-preserving, prefix-free escape as `custos-storage`'s fjall backend. So a
  partition's items are contiguous and sort-ordered, and `query` is one range
  scan. Numbers (`N`) are carried as text and sort lexicographically (a
  documented simplification).
- The `Table` item API uses a monotonic version counter seeded from
  `engine.latest_version()`; the wire path instead lets the data-plane
  coordinator assign quorum-derived versions (see `custosd`).
- `B` (binary) is base64 on the wire; the codec is self-contained (no new dep).
- **Still deferred** (don't represent as a full adapter): `Query`/`Scan` over
  the wire, conditional writes, `ReturnValues`, document/set types, secondary
  indexes, and `CreateTable` / per-table schemas (the wire edge uses a fixed
  `pk`/`sk` convention). `custos-cql` would map onto the same core the same way;
  it's still a skeleton.

## Tests

`cargo test -p custos-dynamo` — `item_api.rs` over `MemoryEngine`, plus `wire`
unit tests (JSON decode/encode, base64 round-trip, tombstone). The wire protocol
is exercised end-to-end over real HTTP in `custosd`'s `tests/dynamo_wire.rs`.
