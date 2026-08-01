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

A first slice now exists: `custos-dynamo` provides a DynamoDB-style **item API**
(`PutItem`/`GetItem`/`DeleteItem`/`Query`) mapped directly onto the
`StorageEngine` core, demonstrating that the Dynamo-lineage data model
translates cleanly. What remains is the surface-specific work — the DynamoDB
HTTP/JSON wire protocol, and for CQL the binary protocol framing plus a
parser/type system — and wiring the adapters to the distributed request path
rather than a local engine. `custos-cql` stays a skeleton that maps onto the
same core in the same way.

## Consequences

- Migrating applications can point at CustosDB with minimal change once the
  adapters exist, which is the adoption wedge.
- Maintaining a single core under two surfaces forces the core to stay
  general-purpose and prevents either surface from leaking into the engine.
- Semantic gaps between CQL and DynamoDB (consistency knobs, type systems,
  conditional writes) will surface as adapter complexity; building the core
  first lets us discover the right shared abstractions before committing.
