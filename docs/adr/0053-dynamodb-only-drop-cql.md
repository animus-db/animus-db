# ADR 0053 — v1 ships DynamoDB only; the CQL wire adapter is dropped

- **Status:** Accepted (supersedes ADR 0006's dual-adapter decision)
- **Date:** 2026-08-22

## Context

ADR 0006 committed AnimusDB to **two** wire-compatible adapters over one
storage core: a DynamoDB JSON/HTTP edge (`animus-dynamo`) and a Cassandra
CQL v4 binary-protocol edge (`animus-cql`), on the theory that wire
compatibility with either of the two largest NoSQL ecosystems widens the
adoption wedge, and that building both simultaneously would force the
shared core (the replicated schema catalog, ADR 0013; the escape/token
key primitives, ADR 0022/0023) to stay genuinely general rather than
accreting DynamoDB-specific assumptions.

That bet has been running for a long time. ADR 0006's own audit notes
recorded the running cost: the two edges never actually shared adapter
code (`escape()` exists independently in `animus-dynamo` and used to in
`animus-cql`, with no equality test between them), CQL trailed DynamoDB
badly on feature completeness (no atomic logged `BATCH`, no in-place
multi-column primary keys, no collection/UDT types, no authentication, no
`LWT`), and every wire-shape decision after the early rounds — secondary
indexes (ADR 0041), Streams (ADR 0042/0043), the universal kind-write path
(ADR 0049), TTL (ADR 0051) — was designed, built, and proven against
DynamoDB's semantics first, with CQL support added or left behind
after the fact. The maintainer's priority is a strongly-consistent,
DynamoDB-compatible store; CQL compatibility was not converging toward
that goal at a pace that justified its maintenance cost.

## Decision

**AnimusDB v1 serves DynamoDB only.** The CQL wire adapter is dropped:

- The `animus-cql` crate is **deleted** in its entirety.
- `animusd`'s CQL listener (`crates/animusd/src/cql.rs`), its dashboard
  loopback client (`cql_client.rs`), the CQL port (`RoleAddrs::cql`, the
  seventh slot in the per-node port stride), the `POST /admin/data/cql`
  dashboard proxy, and the Data Browser's CQL query panel are all removed.
  The per-node port stride shrinks from seven slots to six
  (internal/client/dynamo/admin/intra/console); every port after the
  removed slot renumbers down by one, in-process only — see the root
  `CLAUDE.md`'s no-back-compat policy.
- The CQL-only replicated-catalog surface — `Metadata::keyspaces`,
  `MetaCommand::CreateKeyspace`/`DropKeyspace`, `Metadata::has_keyspace`,
  and `syskv::EntityKind::Keyspace`/`keyspace_key` — is removed from
  `animus-control`, since nothing else in the system ever wrote or read a
  keyspace name.
- The parts of the replicated schema catalog (ADR 0013) that are
  **genuinely shared** — `TableSchema`'s `clustering_keys: Vec<String>`
  general shape (DynamoDB's optional sort key is its one-element case),
  and `ColumnType`'s non-DynamoDB scalar variants (`Int`/`BigInt`/`Bool`/
  `Uuid`) — are **kept as-is**. The catalog stores only a declared type
  and never interprets a value, so the wider vocabulary costs nothing to
  retain, and narrowing it now would be a speculative optimization against
  a shape nothing currently exercises differently. `MetaCommand::
  ReplaceTableSchema` (CQL's atomic `ALTER TABLE … ADD` sink) is likewise
  kept: it is a generic, independently-tested control-plane primitive with
  its own relay-allowlist coverage, not something reachable only through
  the deleted adapter — its one dead caller (`animusd`'s own wrapper
  around it) is what was actually removed.
- Every `cql`/`CQL` reference across the workspace — doc comments, ADRs,
  `CLAUDE.md` guides, the dashboard — is updated to describe the
  DynamoDB-only shape, or, where the mention is historical rationale
  (e.g. why an escape function is duplicated instead of shared, or why a
  key-encoding module speaks of "adapters" plural), reworded to note the
  history rather than describing live machinery.

The code remains **retrievable from git history** — deleted, not archived
dormant-but-compiling, matching the disposition ADR 0019 set for the
deferred AP data plane rather than ADR 0019's own original (and later
revised) intent to keep dormant code compiling.

## Why

**Focus.** v1's identity (ADR 0019) is a strongly-consistent, linearly
scalable, DynamoDB-compatible store. A second wire protocol that has
never been a design or testing priority is a maintenance liability with
no adoption payoff yet realized — every future wire-shape decision would
otherwise need to keep asking "and what does this mean for CQL," for a
protocol no evidence suggests anyone is depending on today.

**A layering opportunity, not yet taken.** With exactly one wire protocol,
the constraint that kept DynamoDB's item model (`AttributeValue`s,
`ConditionExpression`/`UpdateExpression` evaluation) living entirely above
the CP data plane — so that `animus-cp-data` stayed adapter-agnostic
between two different item models — no longer applies. Today a
conditional/update write still evaluates at the wire edge and proposes a
plain kind-write (ADR 0046 U3's "evaluate at leader" shape,
`kind_write_item_at_leader`), because the data plane's own apply path has
no notion of a DynamoDB expression and must not depend on `animus-dynamo`
while a second adapter with an incompatible item model existed. Dropping
CQL removes that reason: a future change could move item-model evaluation
down into the CP data plane's own apply function, letting a conditional
write commit and evaluate in one Raft round trip instead of a
read-then-propose pair. This ADR does not do that — it only removes the
constraint that made it structurally awkward. Whether to actually pursue
it is a separate, future decision.

## Consequences

- **Smaller surface, one wire shape to reason about.** Every future ADR
  touching the wire layer, the schema catalog's consumers, or the
  key-encoding primitives has one adapter to consider, not two whose
  feature sets had already diverged substantially.
- **No migration path for a CQL client.** There was no meaningful CQL
  adoption to migrate away from; this is a clean removal, not a deprecation
  cycle.
- **The port stride shrinks.** Any external tooling or documentation that
  assumed the seven-port-per-node layout (ADR 0047) must be updated; the
  six-port layout is a clean break, per the root `CLAUDE.md`'s no-back-compat
  policy — no live deployment exists to keep compatible with.
- **The dual-plane-shaped "common core, two surfaces" argument ADR 0006
  made for the schema catalog is no longer being exercised by two
  adapters**, but the catalog's general shape survives anyway (see
  Decision) — it costs nothing idle and remains available if a second wire
  protocol is ever revisited. Reviving CQL, or any other adapter, means
  reintroducing its edge crate and wiring from git history; the shared
  substrate it would consume is still there.
- **This ADR does not touch DynamoDB behavior.** No DynamoDB wire
  operation, response shape, or semantic changed; every change here is
  either a deletion of CQL-only code or a documentation update describing
  the resulting DynamoDB-only shape.

This ADR amends ADR 0006 (whose dual-adapter decision is superseded) and
ADR 0047 (whose seven-port stride shrinks to six). It follows ADR 0019's
precedent for how this repo retires a subsystem: delete the code, record
the decision and its rationale here, and leave the history in git for
whoever revives it.
