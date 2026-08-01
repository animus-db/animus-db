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

The adapters are **explicitly out of scope for the first milestones** and must
not be started until the M4 vertical slice proves the distributed architecture.
The crate skeletons exist only to fix the boundary.

## Consequences

- Migrating applications can point at CustosDB with minimal change once the
  adapters exist, which is the adoption wedge.
- Maintaining a single core under two surfaces forces the core to stay
  general-purpose and prevents either surface from leaking into the engine.
- Semantic gaps between CQL and DynamoDB (consistency knobs, type systems,
  conditional writes) will surface as adapter complexity; building the core
  first lets us discover the right shared abstractions before committing.
