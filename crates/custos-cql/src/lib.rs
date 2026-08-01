//! CQL (Cassandra) wire-protocol adapter over the common CustosDB core.
//!
//! This crate implements a **minimal, pure, deterministic** subset of the
//! Apache Cassandra **CQL v4 binary protocol** (ADR 0006): frame
//! encoding/decoding, the `STARTUP`/`OPTIONS` handshake, and a tiny `QUERY`
//! path that recognizes a single-row `INSERT` and a single-key `SELECT`. The
//! CQL surface maps onto the same Dynamo-lineage core as the DynamoDB adapter:
//! a CQL partition key + clustering columns correspond to a partition key +
//! sort key over the `StorageEngine`, the mapping `custos-dynamo` already
//! demonstrates.
//!
//! Everything here is I/O-free and side-effect-free, so it stays deterministic
//! (ADR 0003). The socket edge that drives it lives in `custosd` and is
//! production-only I/O like the DynamoDB HTTP endpoint.
//!
//! ## What is implemented
//!
//! - [`frame`]: the v4 frame header (version/flags/stream/opcode/length) plus a
//!   handful of body primitives ([`frame::read_string`], [`frame::write_string`],
//!   …) and the `[string map]` / `[string list]` used by the handshake.
//! - [`frame::Opcode`]: the request/response opcodes this subset speaks.
//! - The handshake: `STARTUP → READY`, `OPTIONS → SUPPORTED`.
//! - [`query::parse_query`]: a deliberately tiny CQL recognizer that extracts
//!   the operation + primary key (+ value) from a simple `INSERT`/`SELECT`. It
//!   is **not** a CQL grammar; see its docs for the exact accepted shape.
//!
//! ## Storage mapping (fixed convention, no schema yet)
//!
//! There is no `CREATE TABLE` / schema catalog yet, so a row is modelled as a
//! single `(pk, v)` pair keyed by the partition key. The data-plane key is
//! `escape(table) || pk_bytes` (so tables share one keyspace without colliding,
//! mirroring `custos-dynamo`'s storage key); the stored value is the `v`
//! column's text bytes. `SELECT * FROM t WHERE pk = '..'` returns that single
//! row's `pk` and `v` columns. This is enough to round-trip a value end to end;
//! a real type system and parser are future work.

pub mod frame;
pub mod query;
pub mod response;

pub use frame::{Flags, Frame, FrameError, Opcode, REQUEST_VERSION, RESPONSE_VERSION};
pub use query::{Query, QueryError, data_key, parse_query};
pub use response::{QueryRequest, parse_query_request, parse_startup};
