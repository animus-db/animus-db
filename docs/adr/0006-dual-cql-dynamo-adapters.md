# ADR 0006 — Dual CQL + DynamoDB adapters over a common core

- **Status:** Accepted
- **Date:** 2026-08-01

## Context

Adoption of a new database is gated by migration cost. Two of the largest
NoSQL ecosystems — Cassandra (CQL wire protocol) and DynamoDB (HTTP/JSON API) —
share the Dynamo-lineage data model (ADR 0004). Wire compatibility with either
lets existing applications migrate with little or no code change. This
compatibility is the project's long-term wedge.

## Decision

We will expose **both** a CQL wire-protocol adapter (`custos-cql`) and a
DynamoDB API adapter (`custos-dynamo`) as thin translation layers over the
common map-of-maps core (ADR 0004) and the distributed planes (ADR 0001). The
adapters translate surface syntax and semantics to core operations; they do not
each carry their own engine.

Two slices now exist. First, `custos-dynamo` provides a DynamoDB-style **item
API** (`PutItem`/`GetItem`/`DeleteItem`/`Query`) mapped directly onto the
`StorageEngine` core, demonstrating that the Dynamo-lineage data model
translates cleanly. Second, a **minimal DynamoDB JSON wire protocol** is now
served: `custos-dynamo::wire` is the pure, deterministic translation between
the DynamoDB AttributeValue JSON (`{"S":..}` / `{"N":..}` / `{"B":..}` /
`{"BOOL":..}` / `{"NULL":..}`) and the in-memory item model, and `custosd`
exposes a real HTTP/1.1 endpoint that decodes `X-Amz-Target:
DynamoDB_20120810.{PutItem,GetItem,DeleteItem}` requests and routes the
resulting keys/values **through the distributed data plane** (the same quorum
coordinator the plain-TCP client API uses) rather than a local engine. The HTTP
edge is production-only I/O (hand-rolled over a tokio `TcpListener`, mirroring
`ProdEnv`'s placement of real I/O); everything below it stays on the `Env`-based
paths. The data plane has no native delete yet (ADR 0010), so `DeleteItem`
writes a tombstone value that `GetItem` reads back as absent.

A third slice now exists on the CQL side: a **minimal Cassandra CQL v4 binary
protocol** is served alongside the DynamoDB endpoint. `custos-cql` is the pure,
deterministic protocol layer — frame header (version/flags/stream/opcode/length)
encode/decode, the body primitives the handshake needs, and a deliberately tiny
CQL recognizer that extracts the operation + primary key (+ value) from a single
`INSERT INTO t (pk, v) VALUES (..)` or `SELECT * FROM t WHERE pk = ..` (it is not
a CQL grammar). `custosd` exposes a real TCP endpoint that does the
`STARTUP → READY` (and `OPTIONS → SUPPORTED`) handshake and routes those two
statements **through the same quorum coordinator** the plain-TCP and DynamoDB
edges use; an `INSERT` replies `RESULT/Void`, a `SELECT` replies `RESULT/Rows`.
Like the DynamoDB endpoint it is production-only I/O (real tokio sockets +
hand-rolled framing, no third-party CQL/Cassandra crate); everything below the
socket stays on the `Env`-based paths. There is no schema catalog yet, so a row
is a fixed `(pk, v)` pair keyed by the partition key (data-plane key
`escape(table) || pk_bytes`).

What remains is the rest of both surfaces. DynamoDB: `Query`/`Scan` over the
wire, conditional/`ReturnValues` semantics, document/set attribute types, an
explicit `CreateTable` with per-table key schemas (the wire edge currently uses
a fixed `pk`/`sk` convention). CQL: a real type system + column metadata, a
proper CQL grammar (prepared statements, batches, clustering columns, more
statement kinds), `USE`/keyspaces and `CREATE TABLE`, paging, authentication,
and honoring the requested consistency level (currently ignored). Both surfaces
share the same fixed key-attribute convention until `CreateTable` lands.

## Consequences

- Migrating applications can point at CustosDB with minimal change once the
  adapters exist, which is the adoption wedge.
- Maintaining a single core under two surfaces forces the core to stay
  general-purpose and prevents either surface from leaking into the engine.
- Semantic gaps between CQL and DynamoDB (consistency knobs, type systems,
  conditional writes) will surface as adapter complexity; building the core
  first lets us discover the right shared abstractions before committing.
