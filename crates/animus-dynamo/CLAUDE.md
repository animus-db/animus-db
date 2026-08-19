# CLAUDE.md — animus-dynamo

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

A DynamoDB-style **item API** plus the **DynamoDB JSON wire encoding** over the
common storage core (ADR 0006) — the data-model + surface-syntax halves of the
adapter wedge. The transport (HTTP, sockets) and the distributed routing live in
`animusd`; this crate stays pure and deterministic.

## Entry points

Module-by-module pointers — every module here is pure (no I/O/storage/
network, `BTreeMap`/`BTreeSet` only, ADR 0003); see each module's own doc
comment for its full type/method inventory.

- `AttributeValue`/`Item`/`TableSchema` — the DynamoDB type system (scalars,
  document `M`/`L`, set `SS`/`NS`/`BS`) and the simple/composite key schema.
- `Table<S: StorageEngine>` — the local-engine item API (`put_item`/
  `get_item`/`delete_item`/`query`/`query_with`), used by this crate's own
  tests.
- `condition` — `SortKeyCondition` and `ConditionExpression`: pure predicates
  for `Query` sort conditions and conditional writes.
- `registry` — `SchemaRegistry`: a pure, in-memory per-table schema +
  secondary-index-**shape** map (`sync_indexes` resyncs definitions to a
  desired set). **Neither the base table's items nor an index's entries are
  tracked here (ADR 0041 §5)** — both reads are native data-plane range
  scans (`animusd::dynamo`'s `run_gsi_query`/`run_lsi_query`); this registry
  is definition-shape bookkeeping only, mirroring the control plane's
  replicated catalog (ADR 0013).
- `schema` — the pure bridge between this crate's DynamoDB `TableSchema` and
  `animus_control`'s replicated `TableSchema`/`IndexDef`, both directions.
- `storage_key(pk, sk)` — the data-plane key for an item.
- `index` (**ADR 0041 — the codec every layer of materialized secondary
  indexes is built on**: the write path, the GSI drain, and the native index
  read path all construct/parse keys through these same functions, so every
  layer agrees by construction). Two contracts worth stating explicitly:
  - **Row kinds are separate `StorageScope`s, not bytes in the key** (ADR
    0041 §3) — because a tablet is a range over *token* space (a kind above
    the token would break `KeyRange`/the router/split), **and** because
    `RaftKvNode::txn_stage` asserts a logical key leads with the token,
    slicing `anchor[..TOKEN_BYTES]` and deriving every txn intent span from
    it. A GSI row needs no kind at all — it lives in its own hidden table
    (`index_table_name(base, index)` = `<base>$<index>`).
  - **`parse_gsi_row_key` takes a `composite` flag** because a hash-only
    index's `escape(base_pk)` sits exactly where a composite index's
    `escape(isort)` would, so the layout is otherwise ambiguous.
- `wire` — the DynamoDB JSON translation (`decode_request` →
  `Operation`, covering CreateTable/Put/Get/Delete/Query/Scan/UpdateItem/
  BatchWriteItem/TransactWriteItems/TransactGetItems/UpdateTable/
  DescribeTable/UpdateTimeToLive/DescribeTimeToLive, plus the response
  encoders). One gotcha: `GetItem`/`Query`/
  `Scan` decode `ConsistentRead` **but this crate never enforces it** —
  whether `true` is legal depends on an index's replicated *kind* (GSI vs
  LSI), which lives in the control-plane catalog this crate never sees, so
  the field rides through to `animusd::dynamo::run_index_query` to reject.
- `streams_wire` (ADR 0042 §3/§5/§6/§7) — the `DynamoDBStreams_20120810`
  service's own pure wire layer. `parse_shard_id`/`parse_stream_arn` are the
  inverses of `animus_cp_data::segment::shard_id`/`wire::stream_arn`,
  **duplicated rather than depending on `animus-cp-data`** — this crate
  stays dependency-light by re-deriving small byte-shape functions instead
  of pulling in a whole sibling crate, the same precedent its other
  cross-crate duplications follow.
- `ttl` (ADR 0051) — the pure DynamoDB-TTL expiry predicate: `expires_at`
  (an item's declared expiry epoch second under a table's TTL attribute, or
  `None` when the attribute is absent or not a usable `N`) and `is_expired`
  (strictly-less-than "now", not less-or-equal — an expiry equal to "now" has
  not yet expired). We are **AWS-faithful on reads**: nothing here filters a
  `GetItem`/`Query`/`Scan` result — an expired item stays visible until a
  background reaper (`animusd::ttl_reaper`, driven by `env.wall_now()` —
  **never** `env.now()`, which is monotonic-since-start and carries no
  calendar meaning) deletes it; this module is exactly the predicate that
  reaper calls. A TTL attribute of the
  wrong type (anything but `N`) is silently never-expiring, matching AWS,
  which ignores rather than errors. `N` values are parsed under a
  deliberately narrow grammar (`-?[0-9]+(\.[0-9]+)?`, truncated toward
  zero) — **exponent notation (`1.7e9`) is a valid DynamoDB `N` in general
  but is NOT accepted as a TTL value here** (documented non-expiry, not a
  misparse); a negative value folds to `Some(0)` rather than wrapping into
  the `u64` return type. The load-bearing safety property is
  `MAX_PAST_EXPIRY_SECS` (5 years, matching AWS): an expiry further in the
  past than that is treated as **not expired** — the guard against a client
  writing milliseconds instead of seconds and having the reaper read that as
  "expire immediately" across a whole table.

## What's non-obvious

- The `wire`, `condition`, `registry`, `schema`, and `index` modules are all
  **pure** — no I/O, no storage, no network, `BTreeMap`/`BTreeSet` only (ADR 0003).
  `animusd::dynamo` owns the HTTP edge, **proposes `CreateTable`'s key schema into
  the control plane's replicated catalog (ADR 0013)** and reads schemas back from
  `Metadata`, holds one process-wide `SchemaRegistry` (now purely key-schema +
  index-*shape* bookkeeping, ADR 0041 §5 — no index entries at all) behind a
  lock, and routes decoded ops through the data plane.
- This crate's `storage_key` = `escape(partition_key) || sort_key`, using an
  order-preserving, prefix-free escape (no key's encoding prefixes another's).
  So a partition's items are contiguous and sort-ordered, and `query` is one
  range scan. Numbers (`N`) are carried as text and sort lexicographically (a
  documented simplification). `SortKeyCondition::matches` compares the same
  key-bytes, so it agrees with the scan range. **The stored data-plane key adds
  a prefix at the `animusd` edge** (`dynamo.rs::item_key`, ADR 0022/0023):
  `partition_token(escape(pk)) || escape(pk) || sk` — a Murmur3 token spreads
  partitions across the table's hash ring, and there is **no table prefix**
  (tables are separated by per-table tablets, the table is a routing argument).
- `Query` / `Scan` over the **distributed** plane use the CP data plane's
  **linearizable range scan** (`animusd`'s `native_scan` → `ClientCtx::cp_scan`,
  ReadIndex on each tablet leader, forwarded cross-process), not a tracked key
  set. A base `Query` scans the partition's contiguous sub-range
  `[token(pk) || escape(pk), …)`; a `Scan` fans out across the table's tablets
  in token order and paginates with `Limit` +
  `ExclusiveStartKey`/`LastEvaluatedKey` over the **live** keys the scan returns.
  **An index `Query` (ADR 0041 §5) is now a *second* native range scan, not an
  in-memory lookup**: a GSI `Query` scans the index's own hidden table
  (`index_table_name`) over its token-prefixed hash-value range (`animusd`'s
  `run_gsi_query`), and an LSI `Query` scans the base table's own tablet over
  its `KIND_LSI` scope (`ClientCtx::cp_scan_kind`/`run_lsi_query`) — neither
  reads the base keyspace or this crate's registry. Either way a sort
  condition narrows the scan (an `Equals` GSI condition) or filters the
  decoded rows by recovering the sort segment from the row's own key
  (`index::parse_gsi_row_key`/`parse_lsi_row_key`); an index row's *stored*
  value is already the declared projection (applied by the writer/drain), so
  neither path ever reads the base item back.
  The range math (escape is prefix-free, ending `0x00 0x00`, so the first key past
  a prefix bumps the last byte to `0x01`) lives at the `animusd` edge.
  `Table::query_with` is the *local-engine* equivalent (a real engine scan),
  used by the item-API tests.
  **`Scan` with `IndexName` (ADR 0041 §5, 2026-08-13) reuses the same two
  scans**, just table-wide instead of one hash value: a GSI `Scan` fans across
  the hidden table's *own* tablets (an ordinary `cp_scan`, nothing new); an
  LSI `Scan` needed a genuinely new primitive, since an LSI `Query` is
  scoped to one partition/tablet by construction but a `Scan` must sweep the
  base table's *whole* ring — `ClientCtx::cp_scan_kind_table`, `cp_scan`'s
  kind-scoped fan-out sibling. One partition's rows across every declared
  LSI interleave within `KIND_LSI` (sorted by index name ahead of the
  alt-sort value — `index::lsi_index_prefix`'s doc), so an LSI `Scan` filters
  each raw row to the requested index by its own key
  (`parse_lsi_row_key`), skipping a foreign index's row without consuming a
  `Limit` slot — the exact same windowed-continuation trick a base `Scan`
  already uses to skip a DynamoDB delete tombstone. `Scan` has no sort
  condition at all (index or not — DynamoDB's own contract), so there is no
  `Equals`-narrowing analogue here; `FilterExpression`/pagination otherwise
  work identically to a base `Scan`, with an index-appropriate
  `LastEvaluatedKey` cursor shape (see `animusd/CLAUDE.md`'s DynamoDB wire
  entry for exactly what that shape is and why).
- **Secondary indexes** (any number per table, GSI + LSI) are materialized
  **replicated data-plane rows** (ADR 0041), not anything this crate's
  registry tracks. An LSI row is written atomically with the base row
  (`animusd::dynamo::kind_writes_for_item`, one `KvCommand::KindBatch` Raft
  entry); a GSI row is materialized asynchronously by a per-node drain
  (`animusd::index_drain`) from a change-log record the same write leaves.
  The `index` module's key builders (`gsi_row_key`/`lsi_row_key`/etc.) are
  the byte layout every layer — the writer, the drain, and both native read
  paths — agrees on.
- `CreateTable` records a schema in the registry; `create_table_legacy` registers
  the old `pk`/`sk` convention (sort key optional) so pre-`CreateTable` clients
  keep working unchanged. **In `animusd` the authoritative key schema is the
  replicated catalog (ADR 0013)** — the registry's copy is a lazily-rebuilt mirror
  (so its GSI/key-index machinery has a schema); a table absent from the catalog
  is the legacy fallback.
- The `Table` item API uses a monotonic version counter seeded from
  `engine.latest_version()`; the wire path instead lets the data-plane
  coordinator assign quorum-derived versions (see `animusd`).
- `B` (binary) and `BS` elements are standard padded base64 on the wire; the
  codec is self-contained (no new dep). `wire` also exposes an **unpadded
  base64url** pair (`base64url_encode`/`base64url_decode`, strict/canonical
  decode) — the display encoding for `animusd`'s admin/dashboard surfaces, not
  used by the DynamoDB wire itself.
- **Projection** supports **dotted document paths**: `ProjectionExpression` is a
  comma-separated list of paths `a.b.c` (with `#alias` placeholders per segment via
  `ExpressionAttributeNames`), or the legacy `AttributesToGet` array (top-level
  names). `Projection::apply` reconstructs the nested map structure a path reaches
  (`a.b` ⇒ `{a:{b:..}}`). A list-index path (`a[0]`, any `[`) is still rejected.
  Projection is applied at the edge (`animusd`) after the read; for `Scan` the
  `FilterExpression` sees the whole item *before* projection trims it. An index
  `Query` with no explicit projection falls back to the index's declared
  `IndexProjection`.
- **`ReturnValues`** supports `NONE` (default) and `ALL_OLD` on Put/Delete; the
  edge reads the prior item once (reusing it for any `ConditionExpression` check,
  so there is no double read) and `write_response` echoes it under `Attributes`.
  `UpdateItem` additionally supports `ALL_NEW` (`update_response`); `UPDATED_OLD`/
  `UPDATED_NEW` remain deferred.
- **`UpdateItem`/`BatchWriteItem`/`TransactWriteItems`/`TransactGetItems`** are
  decoded here and run at the `animusd` edge. `UpdateItem` is read-modify-write
  of one item applying `SET`/`REMOVE` (upsert when absent); `BatchWriteItem`
  applies `Put`/`Delete` per request (no batch atomicity — a DynamoDB-faithful
  design choice, unlike `TransactWriteItems` below). **`TransactWriteItems` is
  atomic since ADR 0018 §2/PR7**: every condition-gated `Put`/`Delete`/`Update`/
  `ConditionCheck` action commits whole-or-nothing across however many
  tablets/tables it spans, via `animusd`'s `ClientCtx::cp_txn` — see that
  ADR's PR7 amendment for the condition-evaluation/precondition layering
  (and a documented cross-node OCC limitation for a write action's own
  condition, found while building it). **`TransactGetItems`** (new, PR7) is a
  consistent multi-key read — a quiescence-confirmed serializable snapshot,
  not a wait-free one; see the same amendment.
- **Secondary-index *entries* are now replicated data-plane rows, not
  edge-local state — and there is no backfill (ADR 0041 §5).** Before this,
  index entries were an edge-local, in-memory map rebuilt from observed
  writes, with a lazy restart/cross-node backfill (`animusd`'s
  `backfill_index_if_needed`, `SchemaRegistry::note_put`/`note_delete`/
  `touched_since_backfill`) papering over what a given process never
  observed — all **deleted**. A restarted node or a node that never observed
  the writes now returns complete GSI/LSI results because the index's own
  hidden table (GSI) or `KIND_LSI` scope (LSI) *is* the durable, replicated
  data (`animusd/tests/dynamo_schema.rs`
  `create_table_index_survives_node_restart` /
  `create_table_index_replicates_to_second_node`) — nothing to rebuild, and
  nothing that ever needed a stale-write race guard. There is deliberately no
  backfill mechanism today because indexes are only declarable at
  `CreateTable` time, so a pre-existing item that predates an index can never
  exist. **`UpdateTable` now backfills for real (ADR 0045)**: a real
  backfill mechanism exists (`animusd::index_drain`'s seeder arm — the GSI
  drain applied to every pre-existing key, not a new mechanism, ADR 0041
  §5's own prediction) and `GlobalSecondaryIndexUpdates` decodes both
  `Create` and `Delete` (`wire::IndexUpdate`, ADR 0045 §6), and `animusd`
  dispatches both: `Create` (`animusd::dynamo::create_index`) adds a
  `Creating`-status `IndexDef` to a possibly-populated table and lets the
  backfill seeder + completion aggregator converge it to `Active`; `Delete`
  runs the four-step convergent drop cascade (ADR 0045 §5). `DescribeTable`
  reports each index's real status (`CREATING`/`ACTIVE`/`DELETING`) plus a
  per-index `Backfilling: true` while `Creating` — matching real DynamoDB's
  own shape (the attribute lives inside each `GlobalSecondaryIndexes[]`
  entry, not at the table level), via a side channel kept separate from
  `SecondaryIndex` itself (mirroring `StreamDescription`'s own precedent) —
  and `Query`/`Scan` reject a non-`Active` GSI with `ValidationException`.
  `GlobalSecondaryIndexUpdates` and `StreamSpecification` are mutually
  exclusive in one `UpdateTable` call (ADR 0045 §6 Fork C, a deliberate AWS
  deviation) and exactly one `GlobalSecondaryIndexUpdates` element is
  accepted per call; adding an LSI this way stays rejected (LSIs are
  create-time-only in real DynamoDB too).
- **Still deferred** (don't represent as a full adapter): `BatchGetItem`,
  list-index document paths (`a[0]`), `ADD`/`DELETE` `UpdateExpression`
  arithmetic, `TransactWriteItems`/`TransactGetItems` idempotency tokens
  (`ClientRequestToken`) and full per-action `CancellationReasons` fidelity
  (ADR 0018 §2/PR7 shipped atomicity itself; these wire-fidelity details
  remain simplified). `DescribeTimeToLive`'s `TimeToLiveStatus` (ADR 0051)
  only ever renders `ENABLED`/`DISABLED`, never AWS's transient
  `ENABLING`/`DISABLING` — this adapter's `UpdateTimeToLive` takes effect
  synchronously, so there is no in-flight state to report. The
  `Scan`/`Query` `FilterExpression` reuses the `ConditionExpression` predicate
  subset (`attribute_exists`/`attribute_not_exists`/`a = :v`), not the fuller
  filter grammar. `animus-cql` would map onto the same core the same way.
  **Every write op maintains a table's secondary indexes** — and since ADR
  0049 (the universal kind-write path) **every Dynamo write op on every
  table commits through `KindBatch`**: `PutItem`/`DeleteItem`/`UpdateItem`/
  `BatchWriteItem` all route through `animusd`'s evaluate-at-leader
  primitive (`kind_write_item_at_leader`, ADR 0046 U3), and
  `TransactWriteItems` stages a derived kind-writes/change-log payload
  inside the base write's intent, materialized at resolve (ADR 0018's
  2026-08-16 amendment — the old wholesale rejection of indexed/streamed
  tables is long gone). A table with no stream and no index writes an
  **image-less marker record** (`ChangeRecord::marker`, ADR 0049 §1)
  instead of a full-image one — `ChangeRecord::consumer_hidden` is the one
  predicate the Streams read path filters markers and the backfill's
  `seeded` records with; change-log consumers themselves treat every
  record as a dirty-key signal and ignore both flags.

## Tests

`cargo test -p animus-dynamo` — `item_api.rs` over `MemoryEngine`, plus unit
tests for `wire`/`streams_wire`/`condition`/`registry`/`schema`/`index`/`ttl`
(JSON decode/encode, the index key-layout invariants, iterator-token
round-trip, response-shape encoders, and `ttl`'s expiry-boundary table —
absent/wrong-type attributes, future/past/equal-to-now, fractional
truncation, the negative-value fold, and both sides of the
`MAX_PAST_EXPIRY_SECS` window). The rejection of `ConsistentRead` on
a GSI `Query`/`Scan` is `animusd`-only (this crate never sees the
replicated catalog needed to know an index's kind) and is end-to-end
tested in `animusd`'s `tests/dynamo_consistent_read.rs`/
`tests/dynamo_index_scan.rs`. The wire protocol is exercised end-to-end
over real HTTP in `animusd`'s `tests/dynamo_*.rs` (wire, extended,
indexes, documents, schema, gsi_drain, kind_scan, index_scan, txn — see
that crate's `CLAUDE.md`'s Tests section). **Every GSI query assertion in
those files is a converged-or-timeout poll** (ADR 0041's own
eventually-consistent contract); an LSI query stays a plain immediate
assertion.
