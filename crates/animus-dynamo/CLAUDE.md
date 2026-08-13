# CLAUDE.md — animus-dynamo

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

A DynamoDB-style **item API** plus the **DynamoDB JSON wire encoding** over the
common storage core (ADR 0006) — the data-model + surface-syntax halves of the
adapter wedge. The transport (HTTP, sockets) and the distributed routing live in
`animusd`; this crate stays pure and deterministic.

## Entry points

- `AttributeValue` — scalars (`S`/`N`/`B`/`Bool`/`Null`), **document** types
  `M` (map) / `L` (list), and **set** types `SS`/`NS`/`BS` (string/number/binary
  sets, kept sorted + deduplicated so equality and storage are canonical). Only
  scalar types are valid key attributes (document/set `key_bytes()` is empty;
  the schema layer never routes them as keys). `Item`, `TableSchema` (`simple` /
  `composite`).
- `Table<S: StorageEngine>` — `put_item`, `get_item`, `delete_item`, `query` /
  `query_with` (the local-engine item API; `query_with` takes an optional
  `SortKeyCondition`).
- `condition` module — `SortKeyCondition` (`Equals` / `Between` / `BeginsWith`,
  with `matches`) and `ConditionExpression` (`AttributeNotExists` /
  `AttributeExists` / `Equals`, with `evaluate(current)`) — pure predicates for
  `Query` sort conditions and conditional writes.
- `registry` module — `SchemaRegistry`: a pure, in-memory per-table schema map
  (`create_table` / `create_table_with_indexes` / `create_table_legacy` /
  `extract_key`) plus per-table secondary-index **shape** bookkeeping
  (`index_is_composite` + `index_projected_attributes` query it). **Neither the
  base table's items nor a secondary index's entries are tracked here (ADR
  0041 §5)** — a base `Query`/`Scan` uses the data plane's native quorum
  range scan (`DataClient::scan`), and an index `Query` is now a *second*
  native range scan (over a GSI's own hidden table or an LSI's colocated
  `KIND_LSI` scope, `animusd::dynamo`'s `run_gsi_query`/`run_lsi_query`) —
  neither reads this registry at all. What survives is purely the shape a
  table needs regardless of any index entry: `SecondaryIndex` is either a
  `GlobalSecondaryIndex` (name + hash key attribute + optional sort attribute
  + `IndexProjection`) or a `LocalSecondaryIndex` (name + alternate sort
  attribute + `IndexProjection`, hashing by the base partition key). Any
  number of indexes per table. `IndexProjection` is `All` / `KeysOnly` /
  `Include(names)`; `index_projected_attributes` resolves it to the returned
  attribute set (`None` ⇒ all) — used by neither read path today (an index
  row's *stored* value is already projected by the writer/drain), kept as
  definition-level API. `RegistryError` carries the failure cause (incl.
  `IndexSortMismatch` for a sort condition against a hash-only index, though
  that check itself now lives at the `animusd` edge, against the replicated
  catalog's `IndexDef`, not this registry).
  `sync_indexes(table, schema, &[SecondaryIndex])` reconciles a table's index
  *definitions* to a desired set (registering the table if absent) — a plain
  resync now, not a merge: there is no per-index entry data left to preserve
  or discard across a shape change, so a removed index is simply gone and a
  changed/new one simply replaces/appears — how the edge rebuilds its
  key/index-*definition* bookkeeping from the **replicated** definitions (ADR
  0013) rather than process-local `create_table_with_indexes` state.
  **Note:** `animusd` keeps the *table key schema* **and the GSI/LSI
  definitions** in the **control plane's replicated catalog** (ADR 0013); this
  registry mirrors only that definition shape now, nothing about index
  contents (deleted: `note_put`/`note_delete`, `index_query_keys`,
  `touched_since_backfill`/`mark_table_backfilled`/`index_needs_backfill`,
  and the per-index `entries`/`entry_by_base` maps — see ADR 0041 §5's
  as-built note).
- `schema` module — the pure bridge between this crate's DynamoDB `TableSchema`
  (partition key + optional sort key) and the control plane's `TableSchema`
  (`animus_control`: partition key + ordered clustering keys + typed columns):
  `to_control(schema, key_types)` (DynamoDB simple/composite → control schema,
  recording key columns with their `AttributeType` — `key_types` come from the
  `CreateTable` request's `AttributeDefinitions`, decoded into
  `Operation::CreateTable.key_types`; the `animusd` edge now passes them, so a
  numeric/binary key is recorded as `Number`/`Binary` rather than defaulting to
  `String`) and `to_dynamo(control)` (back,
  taking the first clustering key as the DynamoDB sort key, ignoring extra CQL
  clustering columns). It also bridges **secondary-index definitions**:
  `index_to_control(&SecondaryIndex, base_pk)` ↔ `index_to_dynamo(&IndexDef)` /
  `indexes_to_dynamo(&[IndexDef])` (an LSI's control `hash_attribute` is the base
  partition key). `animusd` uses it to propose/read schemas + index definitions via
  the catalog.
- `storage_key(pk, sk)` — the data-plane key for an item, exposed so a caller
  can route an item through `animus-data` without instantiating a local `Table`.
- `index` module (**ADR 0041 — the codec every layer of materialized secondary
  indexes is built on**: the write path, the GSI drain, and the native index
  read path all construct/parse keys through these same functions) — every
  byte layout materialized secondary indexes introduce, kept pure so every
  layer agrees by construction. Like `storage_key` these are
  **within-table** keys: the ADR 0022 partition token is prepended at the
  `animusd` edge, and each builder documents *which* value to token-hash (the
  base partition key for base/LSI/marker keys, the **index hash value** for a GSI
  row — that difference is why a GSI is a separate table). Row kinds are
  **separate `StorageScope`s, not bytes in the key** (ADR 0041 §3): `KIND_BASE`
  (`base_row_key`, byte-identical to `storage_key` — ADR 0022's layout is
  *unchanged*), `KIND_LSI` (`lsi_row_key` / `lsi_index_prefix`), `KIND_CHANGE`
  (`change_record_key` / `change_prefix`), `KIND_FOOTPRINT` (`footprint_key`),
  enumerated by `ALL_KINDS`; `range_end` bounds any prefix. The kind rides in the
  scope prefix — `escape(table) || KIND || token || …` — because a tablet is a
  range over *token* space (a kind above the token would break `KeyRange`/the
  router/split) **and** because `RaftKvNode::txn_stage` asserts a logical key
  leads with the token, slicing `anchor[..TOKEN_BYTES]` and deriving every txn
  intent span from it. All four scopes share one tablet group and one `KeyRange`,
  so a `PutBatch` still writes every kind atomically and a split moves them
  together. Two keys in different scopes can be byte-identical (a footprint key
  and a base partition prefix are both bare `escape(pk)`) — the scope prefix is
  what separates them, which is why there is no `row_kind()`. A GSI row needs no
  kind at all
  (`gsi_row_key` / `gsi_hash_prefix` / `gsi_hash_sort_prefix`), living in the
  hidden table `index_table_name(base, index)` = `<base>$<index>`
  (`split_index_table_name` / `is_index_table_name`). `parse_gsi_row_key` /
  `parse_lsi_row_key` recover the base key by peeling escaped segments —
  `parse_gsi_row_key` takes a `composite` flag because a hash-only index's
  `escape(base_pk)` sits exactly where a composite index's `escape(isort)` would,
  so the layout is otherwise ambiguous. `IndexFootprint` (of `ItemFootprint` /
  `FootprintEntry`) records *where* an item's GSI rows are, never their values —
  the drain deletes whatever it names that a recomputation didn't produce, which
  is what makes a stale row structurally impossible. `ChangeRecord` carries
  `base_sk` + old/new images and `event_name()`
  (`INSERT`/`MODIFY`/`REMOVE`).
- `wire` module — the DynamoDB JSON translation: `decode_request(target, body)
  -> Operation` (CreateTable/PutItem/GetItem/DeleteItem/Query/Scan/**UpdateItem**/
  **BatchWriteItem**/**TransactWriteItems**/**TransactGetItems**; `CreateTable`
  decodes `GlobalSecondaryIndexes` (hash-only or composite) + `LocalSecondaryIndexes`,
  each with an optional `Projection` (`ALL`/`KEYS_ONLY`/`INCLUDE`), `Query` an
  optional `IndexName` + a sort condition (allowed on a composite GSI / LSI),
  `Scan` a `Limit`/`ExclusiveStartKey`/`FilterExpression`, GetItem/Query/Scan an
  optional `ProjectionExpression`/`AttributesToGet`, Put/DeleteItem an optional
  `ReturnValues`, plus the existing `ConditionExpression` on writes and
  `KeyConditionExpression` on Query; `UpdateItem` decodes a `SET`/`REMOVE`
  `UpdateExpression` into `Vec<UpdateAction>` + `UpdateReturnValues`
  (`NONE`/`ALL_OLD`/`ALL_NEW`); `BatchWriteItem` a `RequestItems` map of
  `Put`/`Delete` `WriteRequest`s per table; `TransactWriteItems` a list of
  `TransactAction` (`Put`/`Delete`/`Update`/`ConditionCheck`) — **atomic since
  ADR 0018 §2/PR7**, via `animusd`'s `ClientCtx::cp_txn`, not merely decoded
  here; `TransactGetItems` (new, PR7) a list of `TransactGet` (table + key +
  optional projection) — a consistent multi-key read, `run_transact_get` in
  `animusd`). The AttributeValue codec encodes/decodes the full type set incl.
  `M`/`L`/`SS`/`NS`/`BS`.
  `Projection` (with `apply` / the free `project`) is a pure **dotted document-path**
  filter (`a.b`, reconstructing nested maps); `ReturnValues` (`None`/`AllOld`)
  drives `write_response`, `UpdateReturnValues` drives `update_response`, and
  `apply_update` applies the `SET`/`REMOVE` actions. Plus `encode_item` /
  `get_item_response` / `empty_response` / `query_response` / `scan_response` /
  `create_table_response` / `batch_write_response`, `WireError` (carries the
  DynamoDB `__type` code, incl. `conditional_check_failed`), and
  `encode_stored_item` / `encode_tombstone` / `decode_stored_item` (the data-plane
  value encoding, with a tombstone for delete).

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
  exist; `UpdateTable` (adding an index to a populated table) will need a real
  backfill when it lands — a reuse of the GSI drain applied to every key
  rather than one, not a new mechanism (ADR 0041 §5).
- **Still deferred** (don't represent as a full adapter): `BatchGetItem`,
  list-index document paths (`a[0]`), `ADD`/`DELETE` `UpdateExpression`
  arithmetic, `TransactWriteItems`/`TransactGetItems` idempotency tokens
  (`ClientRequestToken`) and full per-action `CancellationReasons` fidelity
  (ADR 0018 §2/PR7 shipped atomicity itself; these wire-fidelity details
  remain simplified). The
  `Scan`/`Query` `FilterExpression` reuses the `ConditionExpression` predicate
  subset (`attribute_exists`/`attribute_not_exists`/`a = :v`), not the fuller
  filter grammar. `animus-cql` would map onto the same core the same way.
  **A real, pre-existing gap**: only `animusd`'s single-item `PutItem`/
  `DeleteItem` path goes through `index_aware_write` (ADR 0041 §2/§4) —
  `UpdateItem`, `BatchWriteItem`, and `TransactWriteItems` all still commit
  through the plain single-key/batch write primitives, so none of them
  maintain a table's LSI rows or GSI change-log records at all. A secondary
  index on a table written exclusively through those three ops will silently
  never see those writes; see `docs/engineering-lessons.md`.

## Tests

`cargo test -p animus-dynamo` — `item_api.rs` over `MemoryEngine` (incl.
`query_with` sort conditions), plus `wire`, `condition`, `registry`, and `schema`
unit tests (JSON decode/encode incl. document/set types + document-path projection
+ ReturnValues + UpdateItem/BatchWriteItem/TransactWriteItems/TransactGetItems
decode + index projection types, base64 round-trip, tombstone, sort/condition
predicates, `sync_indexes` adding/dropping index *definitions* (no entry data
left to preserve, ADR 0041 §5) and `index_is_composite`/
`index_projected_attributes` reflecting a declared shape, and the DynamoDB↔control
`TableSchema` + `IndexDef` bridge), plus `index` unit tests (ADR 0041: the base
layout being ADR 0022 unchanged, every kind leading with `escape(pk)` so all four
share one tablet, byte-identical keys in different scopes not being a collision,
prefix-freedom across a partition whose key prefixes another's, change records
sorting in commit order, base-key recovery from composite/hash-only GSI rows and
from LSI rows — including values containing `0x00` bytes, two LSIs on one
partition not interleaving, footprint round-trip + sort-invariance under
out-of-order insertion, change-record round-trip + event naming, and
`peel_escaped` rejecting malformed segments).
The wire protocol is exercised end-to-end over real HTTP in
`animusd`'s `tests/dynamo_wire.rs` (Put/Get/Delete), `tests/dynamo_extended.rs`
(CreateTable/Query/conditional writes), `tests/dynamo_indexes.rs` (Scan with
pagination + filter, and a GSI write-then-query), `tests/dynamo_documents.rs`
(document/set types, projection, `ReturnValues: ALL_OLD`, multiple + composite
GSIs, and an LSI), `tests/dynamo_schema.rs` (**CreateTable consuming the
replicated catalog — surviving a node restart**, plus UpdateItem/BatchWriteItem/
TransactWriteItems, document-path projection, a `KEYS_ONLY` GSI projection, and
**`scan_and_query_read_live_storage_after_restart`** — base `Query`/`Scan` return
the rows from live storage after a restart wipes the registry, proving they no
longer depend on any in-memory written-key tracking), `tests/dynamo_gsi_drain.rs`
(ADR 0041 §4/§5 — the drain materializing + pruning a GSI's hidden-table rows,
then a real `Query` against it), `tests/kind_scan.rs` (ADR 0041 §5 — the LSI
`Query` native read path forwarding correctly through a non-leader node, and a
bare `KindScan`'s refusal), and `tests/dynamo_txn.rs`
(ADR 0018 §2/PR7 — atomic `TransactWriteItems`/`TransactGetItems` over a
genuine multi-process, pre-split-table cluster: cross-tablet atomic
visibility through a follower-connected client, a failing `ConditionCheck`
cancelling the whole transaction, `TransactGetItems` never observing a torn
pair under a concurrent writer, same-node concurrent transactions racing a
shared conditioned key resolving to one winner, and `/admin/txns` showing a
pending record during a simulated coordinator stall then clearing once
recovery decides it). **Every GSI query assertion in these files is a
converged-or-timeout poll** (ADR 0041's own eventually-consistent contract);
an LSI query stays a plain immediate assertion.
