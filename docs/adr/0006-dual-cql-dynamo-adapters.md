# ADR 0006 — Dual CQL + DynamoDB adapters over a common core

- **Status:** Accepted
- **Amended for v1 (ADR 0019):** the adapters route through the **CP data
  plane** — `DataClient` and the quorum coordinator are deleted with the AP
  plane; the "native quorum range scan" below is now the CP `cp_scan`
  (linearizable, leader-served), and per-request **consistency levels are
  currently inert** (the CQL edge decodes but ignores `[consistency]`; CP reads
  are always linearizable — `consistency_quorum` survives only as the mapping
  for AP's eventual return).
- **Audit note (2026-08-06):** "common core" holds at the *storage/data-plane*
  layer (one `StorageEngine`, one replicated schema catalog, one routing path),
  but **not at the adapter layer**: `animus-cql` and `animus-dynamo` share no
  code (no cross-dependency), and the load-bearing key conventions are
  re-implemented per edge — the ADR 0022 token+key layout is built independently
  by the DynamoDB edge (token over *escaped* pk bytes), the CQL edge (token over
  *raw* pk bytes, unescaped), and the admin seeder, and `escape()` itself exists
  twice (`animus-dynamo` and `animus-tablet`) with no equality test. Safe today
  only because tablets are table-scoped (ADR 0023) so the layouts never share a
  keyspace. A shared key-layout/RMW helper crate consumed by both edges is the
  standing follow-up before any cross-adapter keyspace exists.
- **Date:** 2026-08-01

## Context

Adoption of a new database is gated by migration cost. Two of the largest
NoSQL ecosystems — Cassandra (CQL wire protocol) and DynamoDB (HTTP/JSON API) —
share the Dynamo-lineage data model (ADR 0004). Wire compatibility with either
lets existing applications migrate with little or no code change. This
compatibility is the project's long-term wedge.

## Decision

We will expose **both** a CQL wire-protocol adapter (`animus-cql`) and a
DynamoDB API adapter (`animus-dynamo`) as thin translation layers over the
common map-of-maps core (ADR 0004) and the distributed planes (ADR 0001). The
adapters translate surface syntax and semantics to core operations; they do not
each carry their own engine.

Two slices now exist. First, `animus-dynamo` provides a DynamoDB-style **item
API** (`PutItem`/`GetItem`/`DeleteItem`/`Query`) mapped directly onto the
`StorageEngine` core, demonstrating that the Dynamo-lineage data model
translates cleanly. Second, a **DynamoDB JSON wire protocol** is now served:
`animus-dynamo::wire` is the pure, deterministic translation between the
DynamoDB AttributeValue JSON (`{"S":..}` / `{"N":..}` / `{"B":..}` /
`{"BOOL":..}` / `{"NULL":..}`) and the in-memory item model, and `animusd`
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

- **`CreateTable` + per-table schemas, now consuming the replicated catalog
  (ADR 0013).** A `CreateTable` request **proposes a `MetaCommand::CreateTableSchema`
  to the control-plane leader** and waits until it commits in `Metadata`, then
  resolves subsequent `PutItem`/`GetItem`/`Query`/`Scan` key attributes from the
  **replicated** `Metadata::table_schema(...)` (translated DynamoDB key attrs ↔
  the control plane's `TableSchema` by `animus_dynamo::schema`). So a created
  table is now **durable and cluster-agreed**: its key schema survives a restart
  (it rode the Raft WAL, not the in-memory registry) and is known on every node.
  A request against a never-created table still falls back to the legacy `pk`/`sk`
  convention. The DynamoDB edge reaches the leader through a process-global set of
  registered control handles (the same process-global pattern the in-memory
  registry uses); in a one-process-per-node deployment that is the node's own
  handle, so `CreateTable` must target the leader (or a node that can reach it).
  **Secondary-index *definitions* now replicate too (ADR 0013):** the per-table
  GSI/LSI declarations live in the table's replicated `TableSchema.indexes`
  (`MetaCommand::{CreateTableIndex, DropTableIndex}`), so index existence/shape is
  cluster-agreed and survives restart. Only the index *entry data* (the actual
  indexed rows) is still maintained in edge-local memory, rebuilt from observed
  writes. The former observation-built
  **written-key index** that backed `Query`/`Scan` is **gone** — base `Query`/`Scan`
  now use the data plane's native range scan (below), so they no longer depend on
  any in-memory tracked key set.
- **`Query`.** Partition-key equality plus an optional sort-key condition (`=`,
  `BETWEEN`, `begins_with`), returning matching items in sort order. A base-table
  `Query` is a **native quorum range scan** (`DataClient::scan`) over the
  partition's contiguous data-plane key sub-range `[escape(table) || escape(pk), …)`
  — no in-memory tracking — applying the sort-key condition on the recovered sort
  bytes after the scan. An **index** `Query` still resolves base keys from the
  in-memory GSI/LSI index (the scan covers the base keyspace, not an index's
  alternate ordering) and quorum-reads each.
  **Audit note (2026-08-22):** `Query` now **paginates** exactly like `Scan`
  below — `Limit` + `ExclusiveStartKey`/`LastEvaluatedKey`, pushed down so a
  small page reads roughly `Limit` rows of the queried partition/index
  sub-range rather than the whole thing, never the whole table. This closes
  what had been a real fidelity gap (a `Query` used to answer a whole
  partition in one uncapped shot); it is a fidelity completion of the
  pagination contract this ADR already specified for `Scan`, not a new
  design decision. (The "in-memory GSI/LSI index" language above predates
  ADR 0041, which replaced it with materialized data-plane index rows — see
  that ADR and `crates/animus-dynamo/CLAUDE.md` for the current index
  design; unrelated to this pagination note.)
  **Audit note (2026-08-22, second):** `Query` also now honours a
  `FilterExpression`. It had been decoded for `Scan` only, so a `Query`
  carrying one **silently returned unfiltered results** — a wrong-data
  divergence rather than a missing-feature one. The semantics are `Scan`'s
  verbatim: the filter runs after the key condition has selected what to
  evaluate and after `Limit` has capped it, so a filtered-out row still
  counts toward `ScannedCount`, still consumes a `Limit` slot, and can still
  be the row `LastEvaluatedKey` points at — hence a page may return fewer
  items than `Limit`, or none, and still carry a cursor. Still **not**
  enforced (a permissive divergence, tracked with that group): DynamoDB
  rejects a `FilterExpression` naming a key attribute of the table or index
  being queried; we accept it.
  **Audit note (2026-08-22, fourth):** `Query`/`Scan` now honour `Select`.
  It had not been decoded at all, so `Select: COUNT` — the "how many match,
  don't send them" form — **silently returned every item**: both the wrong
  response shape and an unbounded payload for the one request whose purpose
  is to avoid one. `COUNT` now suppresses `Items` while changing nothing
  about what is read: the filter still runs, `Limit` still caps what is
  examined, and a truncated `COUNT` page still carries a `LastEvaluatedKey`.
  `Count` is therefore the matches on *this page*, not of the whole query.
  The other three values describe attribute selection this adapter already
  performed, and are now **validated** the way DynamoDB validates them
  rather than silently accepted: `SPECIFIC_ATTRIBUTES` requires a
  projection, every other value forbids one, `ALL_PROJECTED_ATTRIBUTES`
  requires an `IndexName`, and an unknown value is rejected.
  Still **not** enforced (a divergence tracked with the permissive group):
  DynamoDB rejects `ALL_ATTRIBUTES` against a GSI that does not project
  every attribute, and serves it on an **LSI** by fetching the missing
  attributes from the base row; this adapter returns the index's declared
  projection in both cases (ADR 0041), so such a request quietly yields
  fewer attributes than AWS would. Closing it means a base-row fetch for
  the LSI case, which is its own change.
  **Audit note (2026-08-22, fifth — three silent-wrongness bugs):** the
  expression parsers shared a naive `split_once('=')` and discarded the
  attribute name a key condition named. Three consequences, all of which
  returned plausible wrong answers rather than errors:
  (1) `price >= :p` was cut into an equality against an attribute literally
  named `price >`, so a `FilterExpression` matched nothing and a
  `ConditionExpression` could never hold — and the same cut narrowed a
  sort-key range `sk <= :v` to an exact-match query, which is the main
  reason to have a sort key at all;
  (2) `#alias` was resolved for `ProjectionExpression` but **not** for
  `FilterExpression`/`ConditionExpression`, so `#p = :v` compared against an
  attribute named `#p` — always false, and inconsistent within a single
  request; aliases are mandatory for DynamoDB's reserved words, so this hit
  ordinary schemas;
  (3) the key condition's attribute name was dropped, so
  `KeyConditionExpression: "notthekey = :v"` was served as a partition-key
  query against whatever value it named.
  Fixed at the root with one comparator-aware splitter (longest operator
  first) used by all three parsers, alias resolution in every predicate
  form, and the key attribute names carried on `Operation::Query` for the
  `animusd` edge to check against the catalog (decode has none). Operators
  that cannot yet be represented are now **rejected by name** rather than
  truncated; the operators themselves arrive with the expression-surface
  work.
  **Audit note (2026-08-22, sixth — the expression surface):**
  `FilterExpression`/`ConditionExpression` supported exactly three forms
  (`attribute_exists`, `attribute_not_exists`, `a = :v`), so every
  comparison, range, membership test and function was a
  `ValidationException`. The surface now covers the comparison operators
  (`=`, `<>`, `<`, `<=`, `>`, `>=`), `BETWEEN`, `IN`, `begins_with`,
  `contains`, `attribute_type` and `size`, across `Query`/`Scan` filters and
  conditional writes alike (one decoder serves both). Boolean composition
  (`AND`/`OR`/`NOT` with parentheses) is the remaining piece and lands
  separately.
  **One decision worth recording:** filter comparisons order numbers
  **numerically**, deliberately unlike `AttributeValue::key_bytes`, whose
  lexicographic number order is a documented simplification of *key*
  ordering. A key's order must agree with how rows are stored; a filter is
  evaluated in memory over an item and carries no such constraint, so
  inheriting the simplification would make `price > :p` quietly wrong
  (9 would outrank 10). The comparison is done on the decimal text rather
  than through an `f64`, because DynamoDB permits 38 significant digits and
  a float round-trip would silently collapse exactly the large numeric
  identifiers people use as keys. Equality is numeric-aware for the same
  reason: `1.10`, `1.1` and `-0`/`0` are the same number written
  differently. A missing attribute, and any ordering across incomparable
  types, is false for every operator — `<>` included, since DynamoDB has no
  three-valued logic here.
  **Audit note (2026-08-22, seventh — boolean composition):** the predicate
  grammar now composes with `AND`/`OR`/`NOT` and parentheses, completing the
  `FilterExpression`/`ConditionExpression` surface. Precedence is DynamoDB's:
  `NOT` binds tightest, then `AND`, then `OR`, with parentheses overriding
  and chains left-associative. Two parser edges are worth knowing because
  either would return plausible wrong rows rather than erroring: a
  `BETWEEN`'s own ` AND ` belongs to the term, not the combinator, so the
  splitter tracks how many `AND`s the `BETWEEN`s at the current depth still
  owe; and the top-level split skips parenthesised groups, so
  `(a OR b) AND c` is not cut inside the group while `(a) OR (b)` is still
  recognised as a disjunction rather than one group. Composition is
  short-circuiting, which also preserves each leaf's "false when the
  attribute is absent" under `NOT` — `NOT attribute_exists(a)` agrees with
  `attribute_not_exists(a)` precisely because the leaf is false rather than
  unknown.
  **Audit note (2026-08-22, eighth — `ADD`/`DELETE`, and a write-path
  constraint):** `UpdateExpression` gains `ADD` and `DELETE` for the **set**
  types (union and difference). **Numeric `ADD` is deliberately refused**, and
  the reason is a property of the write path rather than of the arithmetic.
  `ClientCtx::cp_kind_write_item` retries `kind_write_item_at_leader`, which
  re-reads the old image and re-applies the actions; and a write that landed
  can still report a retryable error, since a failed OCC seatbelt is
  documented as indistinguishable from a fence miss. Every update action
  before this one was **idempotent**, so re-application converged to the same
  state and the retry was free. `+1` is not idempotent: measured, ten
  concurrent increments with two accepted responses left the counter at 431.
  Set union and difference *are* idempotent, so they are safe on the same
  path and are pinned by a concurrent test. Supporting numeric `ADD`
  correctly needs a once-only guarantee on the kind-write path — an
  idempotency token or an equivalent — which is an ADR 0046/0049 change, not
  a wire-adapter one. Refusing with an explanatory error is the honest
  interim: a silently over-counted counter cannot be detected by the client
  from the response, which makes it worse than a rejection.
  Noted in passing, and **not** fixed here: a validation error raised at the
  leader (an `ADD` type mismatch, say) is re-wrapped as `InternalServerError`
  crossing the forwarding boundary instead of keeping its
  `ValidationException` code, so it surfaces as a 500 where DynamoDB returns
  400. That affects every leader-raised validation error, not just this one.
  **Audit note (2026-08-22, ninth — `BatchGetItem`):** the operation was
  unsupported (a wire test asserted `UnknownOperationException`). It is now
  served as **independent point reads**, deliberately not through the
  quiescent multi-get `TransactGetItems` uses: DynamoDB's `BatchGetItem`
  offers no cross-item atomicity, so borrowing the transactional path would
  have bought a guarantee the API does not promise and paid its cost on
  every call. Projection and `ConsistentRead` are scoped per **table**, not
  per key, matching the wire shape. A key matching nothing is reported by
  **omission** from that table's list, unlike `TransactGetItems`'s
  positional response, which must stay index-aligned and so carries an empty
  object per miss. `UnprocessedKeys` is always empty: every requested key is
  read before responding rather than shedding load. The 100-key request cap
  is not enforced, which belongs with the permissive-divergence group.
  **Audit note (2026-08-22, tenth — parallel `Scan`):** `Segment`/
  `TotalSegments` were absent entirely, so a client could not split a
  full-table scan across workers. They now map onto the **token ring**:
  every data-plane key leads with an 8-byte big-endian partition token (ADR
  0022), so segment `i` of `n` owns `[i·2⁶⁴/n, (i+1)·2⁶⁴/n)` — disjoint and
  jointly covering by construction, which is exactly DynamoDB's contract.
  The boundary arithmetic is done in `u128` (a single segment would overflow
  computing 2⁶⁴ in `u64`), and the last segment's upper bound is unbounded
  rather than 2⁶⁴ so nothing falls off the end of the ring. The same slicing
  applies to a base-table scan, a GSI scan (its hidden table shares the key
  layout) and an LSI scan (kind-scoped but still token-led). A cursor is
  clamped into its segment, so a cursor from one worker cannot walk another
  worker's rows. Giving `Segment` without `TotalSegments` (or the reverse) is
  rejected rather than silently scanning the whole table, which would make
  every worker in a fleet return every item.
  **Audit note (2026-08-22, eleventh — at-most-once for non-idempotent
  writes):** `ClientCtx::cp_kind_write_item`'s retry loop re-entered
  `kind_write_item_at_leader`, which re-reads the old image and re-applies —
  a fresh read-modify-write, not a replay of the original proposal. It now
  skips that retry for a **non-idempotent** write
  (`dynamo::kind_write_is_idempotent`: everything is idempotent except a
  numeric `ADD`). DynamoDB's guarantee is **at-most-once per request**, not
  exactly-once — a *client* retrying an `ADD` that applied double-counts
  there too — so the requirement is only that the service never re-applies on
  its own, which needs no idempotency token. Measured: ten concurrent
  increments moved the counter by exactly ten, against 431 before.
  **Numeric `ADD` nonetheless stays refused,** for a second and now sharper
  reason: `cp_kind_local` confirms a write by probing that the value it
  produced is present, and a concurrent update supersedes that value, so the
  request reports "superseded before its effect appeared ... retry" although
  it applied — measured at 8 of 10 requests under the same load. Retrying is
  precisely what double-counts, so the advertised remedy corrupts the
  counter. Unblocking it means confirming on the **proposal** (did my entry
  commit and apply?) rather than on the value — which is
  `docs/engineering-lessons.md`'s existing rule that a proposer must
  distinguish never-accepted from accepted-unconfirmed, and an ADR 0046/0049
  change rather than a wire-adapter one. The at-most-once fix stands on its
  own regardless: it is what stops the *service* over-counting.
  **Audit note (2026-08-22, twelfth — `UPDATED_OLD`/`UPDATED_NEW`):**
  `UpdateItem`'s `ReturnValues` had `NONE`/`ALL_OLD`/`ALL_NEW` but not the
  `UPDATED_*` pair, which reports only the attributes an update actually
  changed. They are a **diff of the two images**, not a projection of one,
  and the asymmetry is the point: an attribute the update *created* has no
  previous value so `UPDATED_OLD` omits it, and one it *removed* has no new
  value so `UPDATED_NEW` omits it, so each is reported by exactly one of the
  two. Key attributes fall out naturally, since an update never changes them
  and so they never differ. An update that changes nothing omits
  `Attributes` entirely rather than returning an empty map, as DynamoDB
  does. `update_response` already received both images, so this needed no
  new plumbing on the write path.
  **Audit note (2026-08-22, thirteenth — numeric `ADD` unblocked):**
  the refusal recorded in the eighth and eleventh notes is lifted. Two
  write-path fixes had to land first, and neither was a wire-adapter
  concern. `ClientCtx::cp_kind_write_item` no longer re-applies a
  non-idempotent write on its own, so **at-most-once per request** holds —
  DynamoDB's own guarantee, under which a *client* retry of an `ADD` that
  applied double-counts there too. And a `KindBatch` now records what it did
  at apply time, so a write that applied is acknowledged even when a
  concurrent update immediately overwrites it; before that, confirmation
  compared the written value back and told 8 of 10 concurrent increments to
  retry although they had applied, which is precisely what double-counts.
  Measured with both in place: ten concurrent increments are all accepted
  and leave the counter at exactly ten, against 431 originally. The exact
  decimal arithmetic (38 significant digits, no `f64` round-trip) that
  shipped unused with the eighth note is now on the wire.
- **Conditional writes.** A `ConditionExpression` subset
  (`attribute_not_exists(a)`, `attribute_exists(a)`, `a = :v`) gates `PutItem` /
  `DeleteItem`: the edge quorum-reads the current item under the coordinator
  lock and rejects a failing predicate with `ConditionalCheckFailedException`.
- **`Scan`.** A full-table **native quorum range scan** (`DataClient::scan`) over
  the table's whole data-plane range `[escape(table), …)` across all partitions —
  no in-memory key index. It paginates with `Limit` + `ExclusiveStartKey`/
  `LastEvaluatedKey` (the cursor is a truncated page's last storage key, surfaced
  to the client as that item's key-attribute map) and applies an optional
  `FilterExpression` (the same predicate subset as a conditional write) after the
  read. Because the scan reads live storage, the cursor advances over real keys —
  correct even after a restart or on a node that never observed the write.
- **Secondary indexes (GSI + LSI).** `CreateTable` may declare any number of
  secondary indexes. A **global** secondary index (`GlobalSecondaryIndexes`) has
  a `HASH` key attribute plus an optional `RANGE` (a composite GSI); a **local**
  secondary index (`LocalSecondaryIndexes`) shares the base partition `HASH` and
  adds an alternate `RANGE` sort attribute. Their **definitions** are replicated in
  the control plane's table-schema catalog (ADR 0013) — `TableSchema.indexes`,
  mutated by `MetaCommand::{CreateTableIndex, DropTableIndex}` — so index
  existence/shape is durable + cluster-agreed; the edge rebuilds its in-memory
  index-maintenance machinery from those definitions. The registry maintains, per
  index, an
  `escape(hash) [|| escape(sort)] || base_key` index on every write/delete (it
  stores only base keys, not item copies, so the base item stays authoritative),
  and a `Query` with an `IndexName` resolves a hash value back to its base storage
  keys — narrowed by an optional sort-key condition on a composite GSI / LSI (a
  hash-only GSI rejects one) — which are quorum-read like a base query. Each index
  carries a **declared projection** (`ALL` / `KEYS_ONLY` / `INCLUDE
  NonKeyAttributes`): an index `Query` with no explicit `ProjectionExpression`
  returns exactly the index's projected attribute set (`KEYS_ONLY` ⇒ the base + index
  key attributes; `INCLUDE` ⇒ those plus the listed non-key attributes), applied at
  the edge after the base item is read (the index stores only base keys, never item
  copies, so the projection bounds what is *returned*, not what is stored).
- **Document & set attribute types.** The AttributeValue codec carries the
  document types `M` (map) and `L` (list) and the set types `SS`/`NS`/`BS`
  (string/number/binary sets, kept sorted + deduplicated so the in-memory form is
  canonical), alongside the scalars. Stored items serialize them transparently.
- **Projection expressions, incl. document paths.** GetItem/Query/Scan accept a
  `ProjectionExpression` (a comma-separated list of **dotted document paths**
  `a.b.c`, with `#alias` placeholders per segment via `ExpressionAttributeNames`)
  or the legacy `AttributesToGet` array; the edge keeps only the requested paths
  after the read, **reconstructing the nested map structure** each path reaches
  (projecting `a.b` yields `{a:{b:..}}`). List-index paths (`a[0]`) remain deferred
  (a `[` is rejected). For `Scan` the `FilterExpression` sees the whole item before
  projection trims it.
- **`ReturnValues`.** PutItem/DeleteItem accept `ReturnValues: NONE` (default) or
  `ALL_OLD`; the edge reads the prior item once (reusing it for any condition
  check, so no double read) and echoes it under `Attributes` for `ALL_OLD`.
  `UpdateItem` additionally accepts `ALL_NEW` (the item after the update).
- **`UpdateItem`.** A read-modify-write of one item: the edge reads the current
  item under the coord lock, applies an `UpdateExpression`'s `SET attr = :v` /
  `REMOVE attr` clauses (top-level attributes; `#alias`/`:value` placeholders
  resolved; `ADD`/`DELETE` arithmetic deferred), gating on an optional
  `ConditionExpression`, then quorum-writes the new item (an upsert when the key
  was absent) and echoes `NONE`/`ALL_OLD`/`ALL_NEW`.
- **`BatchWriteItem`.** A batch of `PutRequest`/`DeleteRequest`s grouped by table
  in `RequestItems`, applied request-by-request through the same write path (no
  cross-request atomicity, matching DynamoDB). Always replies
  `{"UnprocessedItems":{}}` (every request is processed).
- **`TransactWriteItems`.** A list of condition-gated `Put`/`Delete`/`Update`/
  `ConditionCheck` actions, each honoring its `ConditionExpression`. **Not yet
  truly atomic:** there is no cross-action rollback (full ACID transactional
  writes route through Accord, ADR 0011, which is deferred), so a failed condition
  rejects the request but actions sequenced before it have already applied. The
  documented gap is the all-or-nothing guarantee; the assert-then-write use is
  served correctly.

A third slice exists on the CQL side: a **Cassandra CQL v4 binary protocol** is
served alongside the DynamoDB endpoint. `animus-cql` is the pure, deterministic
protocol layer; `animusd::cql` is the production-only I/O edge (real tokio
sockets + hand-rolled framing, no third-party CQL/Cassandra crate). It now
carries a real type system and a schema catalog rather than a fixed `(pk, v)`
convention:

- **A type/value system.** `animus_cql::types` models the common scalar CQL
  types — `text`, `int`, `bigint`, `boolean`, `blob`, `uuid` — with
  encode/decode of cell bytes (the contents of a protocol `[bytes]`) and literal
  parsing. Result frames carry proper `[column metadata]` with the real type ids,
  and bound values decode/type-check against the column type.
- **`CREATE TABLE` + keyspaces — control-plane replicated (ADR 0013).**
  `CREATE KEYSPACE`, `USE <keyspace>`, and `CREATE TABLE (... PRIMARY KEY (col))`
  declare a schema (one partition-key column + typed columns). `CREATE TABLE` now
  **proposes the schema into the control plane's Raft-replicated catalog**
  (`MetaCommand::CreateTableSchema`, keyed `keyspace.table`) and waits for it to
  commit, so the table is **durable** (recovered from the Raft WAL/snapshot on
  restart) and **cluster-agreed**, replacing the old per-process in-memory
  catalog. `INSERT`/`SELECT`/`UPDATE`/`DELETE` resolve the schema from the
  replicated `Metadata`. The `animusd` edge maps the CQL type system onto the
  shared `ColumnType` vocabulary and reaches the leader through the node's own
  `ClusterEdgeState` (threaded via `ClientCtx`, not a process `OnceLock` — ADR
  0031 PR2, mirroring the DynamoDB edge). A row is serialized to one data-plane
  value (a versioned blob of `(schema column index, cell)` pairs) keyed by
  `escape(table) || pk_key_bytes`. **Keyspace metadata is also control-plane
  replicated now (ADR 0013), no longer future work:** `Metadata` carries a
  `keyspaces: BTreeSet<String>` field mutated by
  `MetaCommand::CreateKeyspace`/`DropKeyspace` and read via
  `Metadata::has_keyspace`, so `CREATE KEYSPACE`/`USE` are durable and
  cluster-agreed exactly like a table schema, not a process-local set.
- **`DROP TABLE` / `ALTER TABLE ... ADD`.** `DROP TABLE [IF EXISTS]` proposes
  `DropTableSchema` and waits for it to replicate. `ALTER TABLE ... ADD <col>
  <type>` appends columns via `MetaCommand::ReplaceTableSchema` — **one atomic
  in-place replacement, no longer future work**: the former drop-then-recreate
  could strand the table schema-less on a crash between the two commands, or
  let a concurrent reader see the table momentarily missing; the new column is
  appended after existing columns (indices preserved), so stored rows still
  decode under the new schema (`animusd::cql::alter_table`).
- **`BATCH`.** `BEGIN [UNLOGGED|LOGGED] BATCH <mutation>; ... APPLY BATCH` applies
  a sequence of `INSERT`/`UPDATE`/`DELETE` statements in order (not atomically;
  CQL logged-batch atomicity is still future work).
- **Prepared statements.** `PREPARE` parses + resolves a statement's `?` bind
  markers against the catalog and replies `RESULT/Prepared` (a
  content-addressed statement id + the bind-variable metadata); `EXECUTE` decodes
  the bound cells against that metadata and runs the statement on the same path
  as `QUERY`. The id is a stable hash of the statement text, so a driver's
  prepare-then-execute path works across connections.

The recognizer (`parse_statement`) accepts `USE` / `CREATE KEYSPACE` /
`CREATE TABLE` / `INSERT` / `SELECT` / `UPDATE` / `DELETE` / `DROP TABLE` /
`ALTER TABLE ... ADD` / `BATCH` (with `?` markers and `keyspace.table` names);
anything outside the subset is rejected cleanly with a CQL `ERROR` frame.
`INSERT`/`UPDATE`/`DELETE`/`EXECUTE`/`BATCH` reply `RESULT/Void`, `SELECT` replies
a typed `RESULT/Rows`, and `USE`/`CREATE`/`DROP`/`ALTER` reply
`SetKeyspace`/`SchemaChange`. Everything routes through the **same quorum
coordinator** the plain-TCP and DynamoDB edges use; everything below the socket
stays on the `Env`-based paths.

The CQL surface now also covers the row-mutation and key-modeling gaps:

- **Clustering columns / compound primary keys.** `CREATE TABLE` accepts
  `PRIMARY KEY (pk, ck1, ck2, ...)` — a single partition-key column plus any
  number of clustering columns (composite multi-column *partition* keys are
  still rejected). Because the data plane offers only point read/write/delete
  (no quorum range scan), the **whole partition** — every row sharing a partition
  key — is stored as one data-plane value keyed by `escape(table) ||
  pk_key_bytes`, an ordered map of clustering-key blob → row. A `SELECT pk = ?`
  returns every row in **clustering order**; adding `AND ck = ?` (every
  clustering column, in order) selects one row. The clustering blob is the
  order-preserving `to_key_bytes` of each clustering value, so a `BTreeMap` over
  it yields clustering order for free.
- **`UPDATE` and `DELETE`.** Both address a row (or partition) by a primary-key
  `WHERE`, routed through the same coordinator. They are **read-modify-write** of
  the partition value at the edge (read the partition, apply the mutation, write
  it back); `UPDATE` is an upsert of non-key cells, `DELETE` removes one row (full
  primary key) or the whole partition (partition-key-only `WHERE`). A `DELETE`
  that empties the partition issues a **data-plane delete/tombstone** (ADR 0010)
  on the key, so it reads back absent and propagates like any tombstone.
- **Consistency levels.** The QUERY/EXECUTE `[consistency]` is decoded and mapped
  (`consistency_quorum`) to a per-request R/W quorum over the tablet's replica
  count — `ONE`→1, `QUORUM`/`LOCAL_QUORUM`→majority, `ALL`→all,
  `TWO`/`THREE`→that many (clamped) — instead of being ignored: the edge
  overrides the `TabletView`'s `r`/`w` per request.

Both adapters now **consume the control plane's replicated table-schema catalog**
(ADR 0013): `CreateTable`/`CREATE TABLE` proposes a `CreateTableSchema` and waits
for commit, so schemas are durable + cluster-agreed (the in-memory per-process
catalogs are gone). The edge maps each adapter's type system onto the shared
`ColumnType` vocabulary and routes the proposal to the control-plane leader via a
process-global set of registered control handles.

What remains. DynamoDB: atomic `TransactWriteItems` (via Accord, ADR 0011),
`BatchGetItem`, list-index document paths (`a[0]`), `ADD`/`DELETE`
`UpdateExpression` arithmetic, and durable control-plane-replicated **secondary-index
*data*** (the index *definitions* now replicate via ADR 0013, but the GSI/LSI
*entry data* — the indexed rows — is still rebuilt from observed writes at the edge).
CQL: composite (multi-column) partition keys, per-column `DELETE`, atomic
logged `BATCH`, in-place `ALTER`, range/`IN`/`ORDER BY`/`LIMIT` predicates with a
native quorum range scan (so a partition need not be one value), collection/UDT
types, paging, authentication, `LWT`/conditional writes, and replicated
**keyspace** metadata (only tables are replicated today). (Now done: both adapters
consume the replicated schema catalog so `CreateTable`/`CREATE TABLE` is durable +
cluster-agreed, and DynamoDB **secondary-index definitions** now replicate in the
same catalog (ADR 0013) so index existence/shape survives restart; DynamoDB
per-index projections, document-path projections,
`UpdateItem`/`BatchWriteItem`/`TransactWriteItems`, document/set types,
`ReturnValues`, composite/multiple GSIs + LSI; CQL clustering/compound primary
keys, `UPDATE`/`DELETE`, consistency levels, `DROP`/`ALTER ADD`/`BATCH`; and a
**native quorum range scan** in the data plane (`DataClient::scan`), now backing
DynamoDB base-table `Query`/`Scan` so they no longer track written keys in memory —
the CQL side still stores a whole partition as one value, but the same primitive can
later carry CQL range/`LIMIT` predicates.)

## Consequences

- Migrating applications can point at AnimusDB with minimal change once the
  adapters exist, which is the adoption wedge.
- Maintaining a single core under two surfaces forces the core to stay
  general-purpose and prevents either surface from leaking into the engine.
- Semantic gaps between CQL and DynamoDB (consistency knobs, type systems,
  conditional writes) will surface as adapter complexity; building the core
  first lets us discover the right shared abstractions before committing.
- **Audit finding (2026-08-06, confirmed — the per-node lock is fixed in
  PR #21; the cross-node CAS remains future work): the DynamoDB edge's
  read-modify-write paths were not atomic even per node.** Conditional
  `PutItem`/`DeleteItem`, `UpdateItem`, and `TransactWriteItems` each do
  read → evaluate → write **without taking the per-node `rmw_lock`** (the CQL
  edge holds it for every RMW), and the CP write below is a blind Raft put (no
  CAS), so nothing compensates: two concurrent `attribute_not_exists` puts on
  one key both succeed. Minimum fix is taking `rmw_lock` on every DynamoDB RMW
  path (per-node atomicity, like CQL); the real fix — needed for cross-node
  atomicity on both edges — is a CP-group CAS/conditional-write primitive
  (`Cas` exists in the CP command set; route conditional writes through it).
