# CLAUDE.md — animus-cql

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

CQL (Cassandra) wire-protocol adapter over the common core (ADR 0006). This
crate is the **pure, deterministic protocol layer**: frame encode/decode, the
`STARTUP`/`OPTIONS` handshake bodies, a CQL recognizer, a type/value system, an
in-memory schema catalog, schema resolution + row serialization, and the
response-body builders. It is I/O-free (no `tokio`, no `Env`, only `std`), so it
stays deterministic and unit-testable. The socket edge that drives it lives in
`animusd::cql` and is production-only I/O, exactly like the DynamoDB HTTP edge.

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
  `to_key_bytes` is the order-preserving partition/clustering-key encoding (ints
  xor the sign bit so byte order matches numeric order).
- `catalog` — `Catalog` of keyspaces → `TableSchema` (ordered typed columns +
  the partition-key index + the ordered `clustering_keys` indices).
  **In-memory, not durable** (see below).
- `query::parse_statement` — recognizes `USE` / `CREATE KEYSPACE` /
  `CREATE TABLE` (with compound `PRIMARY KEY (pk, ck1, ...)`) / `INSERT` /
  `SELECT` / `UPDATE` / `DELETE` into a `Statement` tree, with `?` bind markers
  (`Term::Bind`), `keyspace.table` names, and a `WHERE` of equality `Predicate`s.
  **Not** a full grammar (single-column partition key, equality only).
- `plan` — schema resolution + the **partition** (de)serialization. `plan_insert`
  / `plan_update` / `plan_delete` / `plan_select` resolve a parsed statement (+
  bound `CqlValue`s) against the catalog, type-check, and produce an
  `InsertPlan`/`UpdatePlan`/`DeletePlan`/`ReadPlan`. `Partition` is the unit of
  storage (decode/encode/`rows_matching`); `*_bind_types` resolve a statement's
  `?` markers for `PREPARE`; `schema_of` turns a `CreateTable` into a
  `TableSchema`; `encode_clustering` builds the order-preserving clustering blob.
- `response` — request-body parsers (`parse_startup`, `parse_query_request`,
  `parse_prepare_request`, `parse_execute_request` — each now carrying the
  `Consistency`) and reply builders (`ready`, `supported`, `void_result`,
  `set_keyspace_result`, `schema_change_result`, `prepared_result`,
  `typed_rows_result`/`typed_rows_multi`, `error`). `consistency_quorum` maps a
  CQL `Consistency` to a per-request quorum size over a tablet's replica count.

## What's non-obvious

- **A *partition* is the unit of storage (`plan`).** The data plane has only
  point read/write/delete (no quorum range scan), so everything a
  `SELECT pk = ?` returns must live under **one** data-plane key. With clustering
  columns a partition key maps to *many* rows, so the whole partition is one
  data-plane value keyed by `data_key(table, pk.to_key_bytes())`: a format byte
  (`ROW_FORMAT_V2`), a `u16` row count, then per row a length-prefixed clustering
  blob and the row's `(u16 schema index, u32 len, cell)` non-key cells. Decoding
  into `Partition` keys rows by their **order-preserving clustering blob**
  (a `BTreeMap`), so `rows_matching(prefix)` yields rows in **clustering order**.
  Neither the partition key nor clustering keys are stored in a row's cells — the
  pk round-trips through the data-plane key, clustering values are the row's map
  key (decoded back via the schema). `INSERT`/`UPDATE`/`DELETE` are therefore
  **read-modify-write at the edge** (read the partition, mutate, write back), and
  a `DELETE` that empties the partition tombstones the data-plane key.
- **Consistency is honored.** `Consistency::from_short` decodes the QUERY/EXECUTE
  `[consistency]`; `consistency_quorum(level, replicas)` maps it to a per-request
  quorum size (`ONE`→1, `QUORUM`→majority, `ALL`→all, `TWO`/`THREE`→that many,
  clamped to `1..=replicas`). The `animusd` edge overrides the `TabletView`'s
  `r`/`w` with that size per request rather than always using the node default.
- **Type ids in metadata.** `typed_rows_result` writes a real `[column metadata]`
  block (`Global_tables_spec`) with each column's CQL type id, and the row cells
  are the type's wire bytes — no longer everything-is-varchar.
- **Bind markers + EXECUTE.** A `?` is a `Term::Bind`. `PREPARE` resolves the
  markers' column types (`*_bind_types`) and advertises them in
  `RESULT/Prepared`; `EXECUTE` decodes each `[bytes]` cell with the marker's type
  before planning. A type-mismatched bound value is rejected (clean ERROR).
- **The prepared-statement id is content-addressed** (a hash of the statement
  text), so it is stable across connections — but the *store* that maps id →
  statement lives at the `animusd` edge, not here (this crate is stateless).
- `Frame::decode` deliberately only decodes **requests** (`0x04`). Responses use
  `0x84`; a test that reads a reply builds the `Frame` struct directly (see
  `animusd/tests/cql_wire.rs`).
- Determinism (ADR 0003): no `HashMap`/`HashSet`; the catalog and row decode use
  `BTreeMap`, and the `[string map]` parser returns a `BTreeMap`. No clock/RNG/IO.

## Limitations (documented)

- The catalog is **in-memory and not durable** — schemas are lost on restart;
  replicating them through the control plane is future work (ADR 0006). At the
  `animusd` edge the catalog is **process-global** (shared across all nodes'
  listeners in one process), so in `--cluster N` dev mode a `CREATE TABLE` on one
  node is visible on another — but a one-process-per-node deployment has a
  per-process catalog.
- A table has a **single partition-key column** (composite/multi-column
  partition keys are rejected loudly by the parser), but may have any number of
  **clustering columns**. `SELECT`/`UPDATE`/`DELETE` accept a `pk = value`
  predicate optionally followed by clustering-key equality predicates in order
  (`AND ck = value`); there are no range predicates, `IN`, `ORDER BY`, or
  `LIMIT`. `INSERT`/`UPDATE` require the full primary key.
- Because a partition is one data-plane value, a partition with very many rows is
  a large value (no per-row paging). Acceptable for the subset; native quorum
  range scan is future work (shared with the DynamoDB side, ADR 0006).
- `UPDATE`/`DELETE` are upsert/whole-row only — no per-column `DELETE`, no `IF`
  (`LWT`), no counters or collection mutation.
- The requested **consistency level is honored** (mapped to the data-plane
  quorum); other query flags (paging state, serial consistency, timestamps) are
  parsed past and ignored.

## Tests

`cargo test -p animus-cql` — unit tests for frame round-trips, the type system
(value/cell/literal round-trips), the catalog, the recognizer (all statement
kinds incl. compound primary keys + `UPDATE`/`DELETE` + rejections), the planner
(typed INSERT→SELECT round-trip, clustering-ordered partition decode, UPDATE/
DELETE plans, bind resolution + type-mismatch rejection), and the
consistency→quorum mapping. End-to-end wire tests over real TCP live in
`animusd/tests/cql_wire.rs` (STARTUP → CREATE KEYSPACE/USE/CREATE TABLE →
PREPARE → EXECUTE → typed SELECT) and `animusd/tests/cql_clustering.rs`
(compound primary key → INSERT several rows out of order → clustering-ordered
SELECT → single-row SELECT → UPDATE → single-row + whole-partition DELETE, at
QUORUM consistency).

## When you extend this

- Composite (multi-column) partition keys, `BATCH`/`ALTER`/`DROP`, per-column
  `DELETE`, range/`IN`/`ORDER BY`/`LIMIT` predicates with native quorum range
  scan (so a partition need not be one value), collection/UDT types, paging,
  authentication, conditional writes (`LWT`), and durable
  control-plane-replicated schemas are the next steps. Keep all
  parsing/encoding/planning here (pure) and only the socket loop + the shared
  catalog/prepared-statement state in `animusd`.
