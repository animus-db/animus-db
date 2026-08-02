//! CQL (Cassandra) wire-protocol adapter over the common AnimusDB core.
//!
//! This crate implements a **pure, deterministic** subset of the Apache
//! Cassandra **CQL v4 binary protocol** (ADR 0006): frame encoding/decoding, the
//! `STARTUP`/`OPTIONS` handshake, a CQL recognizer for a practical statement
//! subset, a small **type/value system**, an in-memory **schema catalog**, and
//! **prepared statements**. The CQL surface maps onto the same Dynamo-lineage
//! core as the DynamoDB adapter: a CQL partition key corresponds to a partition
//! key over the `StorageEngine`.
//!
//! Everything here is I/O-free and side-effect-free, so it stays deterministic
//! (ADR 0003 — `BTreeMap`/`BTreeSet` only, no clock/RNG/IO). The socket edge
//! that drives it lives in `animusd::cql` and is production-only I/O like the
//! DynamoDB HTTP endpoint.
//!
//! ## What is implemented
//!
//! - [`frame`]: the v4 frame header (version/flags/stream/opcode/length) plus
//!   body primitives ([`frame::read_string`], [`frame::read_bytes`],
//!   [`frame::write_short_bytes`], …) and the handshake notations.
//! - [`frame::Opcode`]: the request/response opcodes this subset speaks
//!   (`STARTUP`/`OPTIONS`/`QUERY`/`PREPARE`/`EXECUTE` + `READY`/`SUPPORTED`/
//!   `RESULT`/`ERROR`).
//! - [`types`]: the [`types::CqlType`]/[`types::CqlValue`] system for `text`,
//!   `int`, `bigint`, `boolean`, `blob`, and `uuid` — encode/decode cell bytes
//!   and parse literals.
//! - [`catalog`]: an in-memory [`catalog::Catalog`] of keyspaces + table schemas
//!   so `INSERT`/`SELECT` resolve columns against a real schema.
//! - [`query`]: [`query::parse_statement`], a CQL recognizer for
//!   `USE` / `CREATE KEYSPACE` / `CREATE TABLE` (with compound primary keys) /
//!   `INSERT` / `SELECT` / `UPDATE` / `DELETE` (with `?` bind markers). It is
//!   **not** a full CQL grammar; see its docs.
//! - [`plan`]: schema resolution + the *partition* (de)serialization format —
//!   turns a parsed statement (+ bound values) into a concrete data-plane
//!   operation. A partition (all rows sharing a partition key, ordered by
//!   clustering key) is one data-plane value, so reads/writes stay point ops.
//! - [`response`]: parsers for the request bodies and builders for every reply,
//!   including the typed `RESULT/Rows` metadata and `RESULT/Prepared`.
//!
//! ## Limitations (documented)
//!
//! - The catalog is **in-memory and not durable** — schemas are lost on restart
//!   and, in single-process `--cluster N` dev mode, shared across in-process
//!   nodes (see [`catalog`]). Control-plane-replicated schemas are future work,
//!   mirroring the DynamoDB side (ADR 0006).
//! - A table has a **single partition-key column** (composite partition keys are
//!   not supported), but may have **clustering columns** (a compound primary
//!   key); `SELECT`/`UPDATE`/`DELETE` accept a partition-key equality predicate
//!   optionally followed by clustering-key equality predicates in order.
//! - The requested **consistency level** is honored — mapped to the data-plane
//!   R/W quorum ([`response::consistency_quorum`]) — but most other query flags
//!   are parsed past and ignored.

pub mod catalog;
pub mod frame;
pub mod plan;
pub mod query;
pub mod response;
pub mod types;

pub use catalog::{Catalog, CatalogError, Column, TableSchema};
pub use frame::{Flags, Frame, FrameError, Opcode, REQUEST_VERSION, RESPONSE_VERSION};
pub use plan::{
    ColumnSpec, DeletePlan, InsertPlan, Partition, PlanError, ReadPlan, Row, UpdatePlan,
    encode_clustering, plan_delete, plan_insert, plan_select, plan_update, schema_of,
};
pub use query::{
    AlterTable, Batch, CreateTable, Delete, DropTable, Insert, Predicate, QueryError, Select,
    Statement, Term, Update, data_key, parse_statement,
};
pub use response::{
    Consistency, ExecuteRequest, QueryRequest, consistency_quorum, parse_execute_request,
    parse_prepare_request, parse_query_request, parse_startup,
};
pub use types::{CqlType, CqlValue, ValueError};
