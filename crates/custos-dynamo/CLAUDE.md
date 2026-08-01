# CLAUDE.md — custos-dynamo

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

A DynamoDB-style **item API** over the common storage core (ADR 0006) — the
data-model half of the adapter wedge.

## Entry points

- `AttributeValue` (S/N/B/Bool/Null), `Item`, `TableSchema` (`simple` /
  `composite`).
- `Table<S: StorageEngine>` — `put_item`, `get_item`, `delete_item`, `query`.

## What's non-obvious

- This is the **API/data-model mapping only**. The DynamoDB HTTP/JSON wire
  protocol, conditional writes, secondary indexes, and the distributed request
  path (running over `custos-data` instead of a local engine) are all future
  work. Don't represent it as a full DynamoDB adapter.
- Storage key = `escape(partition_key) || sort_key`, using the same
  order-preserving, prefix-free escape as `custos-storage`'s fjall backend. So a
  partition's items are contiguous and sort-ordered, and `query` is one range
  scan. Numbers (`N`) are carried as text and sort lexicographically (a
  documented simplification).
- Writes use a monotonic version counter seeded from `engine.latest_version()`.
- `custos-cql` would map onto the same core the same way; it's still a skeleton.

## Tests

`cargo test -p custos-dynamo` — `item_api.rs` over `MemoryEngine`.
