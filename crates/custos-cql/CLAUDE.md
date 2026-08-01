# CLAUDE.md — custos-cql

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

CQL (Cassandra) wire-protocol adapter over the common core (ADR 0006). This
crate is the **pure, deterministic protocol layer**: frame encode/decode, the
`STARTUP`/`OPTIONS` handshake bodies, a tiny `QUERY` recognizer, and the
response-body builders. It is I/O-free (no `tokio`, no `Env`, only `std`), so it
stays deterministic and unit-testable. The socket edge that drives it lives in
`custosd::cql` and is production-only I/O, exactly like the DynamoDB HTTP edge.

## What's implemented (a minimal CQL v4 subset)

- `frame` — the 9-byte v4 header (version `0x04` req / `0x84` resp, flags,
  `i16` stream, opcode, `i32` length) + body primitives (`[string]`,
  `[long string]`, `[bytes]`, `[string map]`, `[string list]`,
  `[string multimap]`). `Frame::body_len` lets the socket reader size the body
  read; `Frame::decode` validates a **request**; `Frame::encode_response`
  frames a reply.
- `response` — `parse_startup`, `parse_query_request`, and builders: `ready`,
  `supported` (advertises CQL 3.0.0, no compression), `void_result` (INSERT),
  `rows_result` (SELECT, one `(pk, v)` row or empty), `error`.
- `query::parse_query` — recognizes exactly `INSERT INTO t (pk, v) VALUES (..)`
  and `SELECT * FROM t WHERE pk = ..`. Case-insensitive keywords, optional
  trailing `;`, single-quoted strings (with `''` escape) or bare word/number
  literals. It is **not** a CQL grammar.

## What's non-obvious

- **No schema catalog yet.** A row is a fixed `(pk, v)` pair; the data-plane key
  is `data_key(table, pk) = u32-len-prefixed(table) || pk_bytes` (length-escaped
  so two tables never collide, mirroring `custos-dynamo`'s storage key). The
  stored value is the `v` column's raw text bytes.
- **Every column is `varchar`.** `rows_result` advertises both columns as
  `0x000D` and ships the text bytes; there is no type system yet.
- **The request consistency level + query flags are parsed past but ignored.**
  Honoring them (and bound values / prepared statements) is future work.
- `Frame::decode` deliberately only decodes **requests** (`0x04`). Responses use
  `0x84`; a test that reads a reply builds the `Frame` struct directly (see
  `custosd/tests/cql_wire.rs`).
- Determinism (ADR 0003): no `HashMap`/`HashSet`; the `[string map]` parser
  returns a `BTreeMap`.

## Tests

`cargo test -p custos-cql` — unit tests for frame round-trips, malformed-frame
rejection, the handshake bodies, and the query recognizer. The end-to-end wire
test (handshake + INSERT + SELECT over real TCP) lives in
`custosd/tests/cql_wire.rs`.

## When you extend this

- A real type system + column metadata, a proper CQL grammar (clustering
  columns, batches, prepared statements, more statement kinds), `CREATE TABLE` /
  keyspaces, paging, authentication, and honoring consistency are the next
  steps. Keep all parsing/encoding here (pure) and only the socket loop in
  `custosd`.
