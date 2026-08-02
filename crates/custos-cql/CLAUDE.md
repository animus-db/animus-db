# CLAUDE.md — custos-cql

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

CQL (Cassandra) wire-protocol adapter over the common core (ADR 0006). This
crate is the **pure, deterministic protocol layer**: frame encode/decode, the
`STARTUP`/`OPTIONS` handshake bodies, a CQL recognizer, a type/value system, an
in-memory schema catalog, schema resolution + row serialization, and the
response-body builders. It is I/O-free (no `tokio`, no `Env`, only `std`), so it
stays deterministic and unit-testable. The socket edge that drives it lives in
`custosd::cql` and is production-only I/O, exactly like the DynamoDB HTTP edge.

## Module map

- `frame` — the 9-byte v4 header (version `0x04` req / `0x84` resp, flags,
  `i16` stream, opcode, `i32` length) + body primitives (`[string]`,
  `[long string]`, `[bytes]`, `[short bytes]`, `[string map]`, `[string list]`,
  `[string multimap]`). `Frame::body_len` sizes the body read; `Frame::decode`
  validates a **request**; `Frame::encode_response` frames a reply. Opcodes:
  `STARTUP`/`OPTIONS`/`QUERY`/`PREPARE`/`EXECUTE` + `READY`/`SUPPORTED`/
  `RESULT`/`ERROR`.
- `types` — `CqlType`/`CqlValue` for `text`, `int`, `bigint`, `boolean`, `blob`,
  `uuid`. `encode`/`decode` move the cell bytes that ride inside a `[bytes]`;
  `parse_literal` turns textual `INSERT`/`WHERE` literals into typed values;
  `to_key_bytes` is the order-preserving partition-key encoding (ints xor the
  sign bit so byte order matches numeric order).
- `catalog` — `Catalog` of keyspaces → `TableSchema` (ordered typed columns +
  the partition-key index). **In-memory, not durable** (see below).
- `query::parse_statement` — recognizes `USE` / `CREATE KEYSPACE` /
  `CREATE TABLE` / `INSERT` / `SELECT` into a `Statement` tree, with `?` bind
  markers (`Term::Bind`) and `keyspace.table` names. **Not** a full grammar.
- `plan` — schema resolution: `plan_insert`/`plan_select` resolve a parsed
  statement (+ bound `CqlValue`s) against the catalog, type-check, and produce a
  `WritePlan`/`ReadPlan` (data-plane key + encoded value / projection).
  `insert_bind_types`/`select_bind_types` resolve a statement's `?` markers for
  `PREPARE`. `schema_of` turns a `CreateTable` into a `TableSchema`.
- `response` — request-body parsers (`parse_startup`, `parse_query_request`,
  `parse_prepare_request`, `parse_execute_request`) and reply builders
  (`ready`, `supported`, `void_result`, `set_keyspace_result`,
  `schema_change_result`, `prepared_result`, `typed_rows_result`, `error`).

## What's non-obvious

- **Row storage format (`plan`).** A row is one data-plane value keyed by
  `data_key(table, pk.to_key_bytes())`. The value is a versioned, self-describing
  blob: a format byte, a `u16` non-key-column count, then for each present
  non-key column a `(u16 schema column index, [bytes] cell)`. The partition key
  is **not** stored in the value (it round-trips through the key and is echoed
  back from the predicate on `SELECT`). `ColumnSpec` carries each column's
  `schema_index` so a `SELECT` can match a projected column to its stored cell.
- **Type ids in metadata.** `typed_rows_result` writes a real `[column metadata]`
  block (`Global_tables_spec`) with each column's CQL type id, and the row cells
  are the type's wire bytes — no longer everything-is-varchar.
- **Bind markers + EXECUTE.** A `?` is a `Term::Bind`. `PREPARE` resolves the
  markers' column types (`*_bind_types`) and advertises them in
  `RESULT/Prepared`; `EXECUTE` decodes each `[bytes]` cell with the marker's type
  before planning. A type-mismatched bound value is rejected (clean ERROR).
- **The prepared-statement id is content-addressed** (a hash of the statement
  text), so it is stable across connections — but the *store* that maps id →
  statement lives at the `custosd` edge, not here (this crate is stateless).
- `Frame::decode` deliberately only decodes **requests** (`0x04`). Responses use
  `0x84`; a test that reads a reply builds the `Frame` struct directly (see
  `custosd/tests/cql_wire.rs`).
- Determinism (ADR 0003): no `HashMap`/`HashSet`; the catalog and row decode use
  `BTreeMap`, and the `[string map]` parser returns a `BTreeMap`. No clock/RNG/IO.

## Limitations (documented)

- The catalog is **in-memory and not durable** — schemas are lost on restart;
  replicating them through the control plane is future work (ADR 0006). At the
  `custosd` edge the catalog is **process-global** (shared across all nodes'
  listeners in one process), so in `--cluster N` dev mode a `CREATE TABLE` on one
  node is visible on another — but a one-process-per-node deployment has a
  per-process catalog.
- A table has a **single partition-key column** and no clustering columns;
  `SELECT` supports only a `pk = value` equality predicate. Composite/clustering
  keys are rejected loudly by the parser.
- The requested consistency level and most query flags are parsed past and
  ignored.

## Tests

`cargo test -p custos-cql` — unit tests for frame round-trips, the type system
(value/cell/literal round-trips), the catalog, the recognizer (all statement
kinds + rejections), and the planner (typed INSERT→SELECT round-trip, bind
resolution + type-mismatch rejection). The end-to-end wire test (STARTUP →
CREATE KEYSPACE/USE/CREATE TABLE → PREPARE → EXECUTE → typed SELECT over real
TCP) lives in `custosd/tests/cql_wire.rs`.

## When you extend this

- Clustering columns + composite partition keys, more statement kinds
  (`UPDATE`/`DELETE`/`BATCH`/`ALTER`/`DROP`), collection/UDT types, paging,
  authentication, conditional writes (`LWT`), honoring consistency, and durable
  control-plane-replicated schemas are the next steps. Keep all
  parsing/encoding/planning here (pure) and only the socket loop + the shared
  catalog/prepared-statement state in `custosd`.
