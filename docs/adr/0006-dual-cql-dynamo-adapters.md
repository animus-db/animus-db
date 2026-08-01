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
translates cleanly. Second, a **DynamoDB JSON wire protocol** is now served:
`custos-dynamo::wire` is the pure, deterministic translation between the
DynamoDB AttributeValue JSON (`{"S":..}` / `{"N":..}` / `{"B":..}` /
`{"BOOL":..}` / `{"NULL":..}`) and the in-memory item model, and `custosd`
exposes a real HTTP/1.1 endpoint that decodes `X-Amz-Target:
DynamoDB_20120810.{CreateTable,PutItem,GetItem,DeleteItem,Query}` requests and
routes the resulting keys/values **through the distributed data plane** (the
same quorum coordinator the plain-TCP client API uses) rather than a local
engine. The HTTP edge is production-only I/O (hand-rolled over a tokio
`TcpListener`, mirroring `ProdEnv`'s placement of real I/O); everything below it
stays on the `Env`-based paths. The data plane has no native delete yet (ADR
0010), so `DeleteItem` writes a tombstone value that `GetItem` reads back as
absent.

The surface now extends past the original three point ops:

- **`CreateTable` + per-table schemas.** A `CreateTable` request records a
  table's key schema (partition `HASH` + optional sort `RANGE` attribute names)
  in a `SchemaRegistry`, so the key convention is no longer hard-coded; later
  requests resolve their key attributes against it. A request against a
  never-created table falls back to the legacy `pk`/`sk` convention. The
  registry is **in-memory and not durable** (lost on restart; shared across
  nodes in single-process `--cluster N` dev mode) — replicating schemas through
  the control plane is future work.
- **`Query`.** Partition-key equality plus an optional sort-key condition (`=`,
  `BETWEEN`, `begins_with`), returning matching items in sort order. The data
  plane exposes only point read/write/delete (no quorum range scan), so the
  registry additionally tracks per-table written item keys; `Query` selects the
  partition's contiguous matching sub-range and quorum-reads each key through
  the coordinator (an honest range scan over a *tracked*, in-memory keyspace).
- **Conditional writes.** A `ConditionExpression` subset
  (`attribute_not_exists(a)`, `attribute_exists(a)`, `a = :v`) gates `PutItem` /
  `DeleteItem`: the edge quorum-reads the current item under the coordinator
  lock and rejects a failing predicate with `ConditionalCheckFailedException`.

What remains is the rest of the surface: `Scan`, projection/filter expressions,
`ReturnValues`, document/set attribute types, secondary indexes, durable
control-plane-replicated table schemas (and a native quorum range scan so
`Query` need not track keys), and the parallel CQL binary protocol framing plus
a parser/type system. `custos-cql` stays a skeleton that maps onto the same core
in the same way.

## Consequences

- Migrating applications can point at CustosDB with minimal change once the
  adapters exist, which is the adoption wedge.
- Maintaining a single core under two surfaces forces the core to stay
  general-purpose and prevents either surface from leaking into the engine.
- Semantic gaps between CQL and DynamoDB (consistency knobs, type systems,
  conditional writes) will surface as adapter complexity; building the core
  first lets us discover the right shared abstractions before committing.
