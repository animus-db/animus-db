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

What remains is the rest of the surface: `Query`/`Scan` over the wire,
conditional/`ReturnValues` semantics, document/set attribute types, an explicit
`CreateTable` with per-table key schemas (the wire edge currently uses a fixed
`pk`/`sk` convention), and the parallel CQL binary protocol framing plus a
parser/type system. `custos-cql` stays a skeleton that maps onto the same core
in the same way.

## Consequences

- Migrating applications can point at CustosDB with minimal change once the
  adapters exist, which is the adoption wedge.
- Maintaining a single core under two surfaces forces the core to stay
  general-purpose and prevents either surface from leaking into the engine.
- Semantic gaps between CQL and DynamoDB (consistency knobs, type systems,
  conditional writes) will surface as adapter complexity; building the core
  first lets us discover the right shared abstractions before committing.
