# CLAUDE.md — animus-dynamo

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

A DynamoDB-style **item API** plus the **DynamoDB JSON wire encoding** over the
common storage core (ADR 0006) — the data-model + surface-syntax halves of the
adapter wedge. The transport (HTTP, sockets) and the distributed routing live in
`animusd`; this crate stays pure and deterministic.

**The pure item model itself now lives in `animus-item`, below this crate**
(ADR 0054 step 1 — see `crates/animus-item/CLAUDE.md`): `AttributeValue`/
`Item`/`TableSchema`, the key-encoding primitives, `condition`, `index`,
`numkey`, the `UpdateExpression` data model + apply-time evaluator, the
stored-item codec, and the item-size formula. This crate re-exports all of it
unchanged (every `animus_dynamo::X`/`animus_dynamo::wire::X` path below still
resolves) and keeps the parts that are genuinely wire/JSON concerns: the HTTP
request/response codec (`wire`), the `UpdateExpression`/`ConditionExpression`
**string parser** (needs `ExpressionAttributeNames`/`Values`, JSON-specific),
`registry`/`schema` (the catalog bridge), `capacity` (`ConsumedCapacity`
response shaping — re-exports the item-size formula itself from
`animus-item`), `sigv4`, `streams_wire`, and `ttl`.

## Entry points

Module-by-module pointers — every module here is pure (no I/O/storage/
network, `BTreeMap`/`BTreeSet` only, ADR 0003); see each module's own doc
comment for its full type/method inventory.

- `AttributeValue`/`Item`/`TableSchema` — the DynamoDB type system (scalars,
  document `M`/`L`, set `SS`/`NS`/`BS`) and the simple/composite key schema.
  **Defined in `animus-item`, re-exported here** (ADR 0054 step 1).
- `Table<S: StorageEngine>` — the local-engine item API (`put_item`/
  `get_item`/`delete_item`/`query`/`query_with`), used by this crate's own
  tests. Stays here (it depends on `animus-storage`'s `StorageEngine`, which
  `animus-item` deliberately does not).
- `condition` — `SortKeyCondition` and `ConditionExpression`: pure predicates
  for `Query` sort conditions and conditional writes. **Defined in
  `animus-item::condition`, re-exported here** (ADR 0054 step 1).
- `registry` — `SchemaRegistry`: a pure, in-memory per-table schema +
  secondary-index-**shape** map (`sync_indexes` resyncs definitions to a
  desired set). **Neither the base table's items nor an index's entries are
  tracked here (ADR 0041 §5)** — both reads are native data-plane range
  scans (`animusd::dynamo`'s `run_gsi_query`/`run_lsi_query`); this registry
  is definition-shape bookkeeping only, mirroring the control plane's
  replicated catalog (ADR 0013).
- `schema` — the pure bridge between this crate's DynamoDB `TableSchema` and
  `animus_control`'s replicated `TableSchema`/`IndexDef`, both directions.
  **`index_to_control` resolves an index's own `hash_attribute_type`/
  `sort_attribute_type` (issue #319/W-05) from its `key_types` parameter**
  — the same decoded `AttributeDefinitions` pairs `to_control` already
  resolves the base table's key columns from — so a `CreateTable`/
  `UpdateTable` caller that declares an index-only key attribute's type
  gets it durably recorded on `IndexDef`, not just the base table's own
  keys. `index_attribute_types(&[IndexDef]) -> Vec<(String, String)>` is
  the reverse direction: the `(name, type)` pairs `animusd::dynamo::
  describe_table`/`delete_table` `.extend()` onto their own base
  `key_types` map before calling `wire::describe_table_response`/
  `delete_table_response` — see that pair's own doc, and
  `docs/engineering-lessons.md`'s "threading a per-attribute type through a
  bridge" entry for why this is a name-keyed edge merge rather than a
  field threaded through `SecondaryIndex`/`GlobalSecondaryIndex`/
  `LocalSecondaryIndex` (registry.rs), which never needed to carry it.
- `storage_key(pk, sk)` — the data-plane key for an item. **Defined in
  `animus-item`, re-exported here** (ADR 0054 step 1).
- `index` (**ADR 0041 — the codec every layer of materialized secondary
  indexes is built on**: the write path, the GSI drain, and the native index
  read path all construct/parse keys through these same functions, so every
  layer agrees by construction). **Defined in `animus-item::index`,
  re-exported here** (ADR 0054 step 1) — moved because a future
  `animus-cp-data` apply-path evaluator will need to derive these same
  rows and cannot depend on this wire crate. Two contracts worth stating
  explicitly:
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
  BatchWriteItem/BatchGetItem/TransactWriteItems/TransactGetItems/UpdateTable/
  DescribeTable/DeleteTable/ListTables/UpdateTimeToLive/DescribeTimeToLive/
  TagResource/UntagResource/ListTagsOfResource/DescribeLimits/
  DescribeEndpoints, plus the response encoders). **Resource tagging
  (roadmap W-06)**: `table_arn`/`parse_table_arn` are this adapter's own
  table-ARN codec (`arn:aws:dynamodb:animus:0:table/<table>`, mirroring
  `stream_arn`/`backup_arn`'s identical placeholder-region/account
  convention with no further suffix) — `TagResource`/`UntagResource`/
  `ListTagsOfResource`'s `ResourceArn` decodes through it, and it also now
  backs `TableArn` in `table_description_object` (`CreateTable`/
  `DescribeTable`/`UpdateTable`/`DeleteTable`'s shared response builder),
  which didn't render one before this. A malformed or non-table `ResourceArn`
  (e.g. a well-formed *stream* ARN) is a decode-time `ValidationException`
  here; a well-formed table ARN naming a table that doesn't exist is
  `animusd`'s call (`ResourceNotFoundException`, since only it holds the
  replicated catalog). `TableSchema::tags: BTreeMap<String, String>`
  (`animus-control`) is wire metadata only — nothing here or in
  `animus-control` interprets a tag's key or value. `DescribeLimits`/
  `DescribeEndpoints` decode to unit-like variants (no request fields, table()
  → `None`) — `DescribeLimits`' response is four honest static constants
  (this adapter has no capacity-billing meter at all); `DescribeEndpoints`'
  is built by `animusd` from its own bound DynamoDB listen address, since
  this crate has no node identity to report. `Query` and `Scan` share `decode_limit`/`decode_exclusive_start_key`/
  `decode_predicate`/`decode_select` — same `Limit`/`ExclusiveStartKey`/`FilterExpression`/`Select`
  contract, so fixing one fixes both; do not fork them.
  One gotcha: `GetItem`/`Query`/
  `Scan` decode `ConsistentRead` **but this crate never enforces it** —
  whether `true` is legal depends on an index's replicated *kind* (GSI vs
  LSI), which lives in the control-plane catalog this crate never sees, so
  the field rides through to `animusd::dynamo::run_index_query` to reject.
  The same field also **selects a read path** at that edge since ADR 0055
  (`true` = the linearizable ReadIndex read, `false` = served from any
  replica's applied state) — likewise not this crate's business, but worth
  knowing before describing the flag as decorative anywhere: it stopped
  being accept-and-ignore.
  `DeleteTable`/`ListTables` are read-only-here operations too: `DeleteTable`
  decodes to a bare table name (the existence check, the actual drop via
  `ClientCtx::drop_table`, and the `ResourceNotFoundException` are all
  `animusd`'s, since this crate never sees the replicated catalog); `wire`
  does own the **pure** `ListTables` pagination contract though
  (`paginate_table_names` — default/cap-100 `Limit`,
  `ExclusiveStartTableName` positioning via a sorted-slice binary search, and
  `LastEvaluatedTableName` reported only when truncated), so `animusd`'s
  `list_tables` only has to build the already-filtered, already-sorted
  candidate name list (excluding a materialized GSI's hidden
  `<base>$<index>` table, `index::is_index_table_name`) and hand it to this
  crate's pagination + response encoder.
- `streams_wire` (ADR 0042 §3/§5/§6/§7) — the `DynamoDBStreams_20120810`
  service's own pure wire layer. `parse_shard_id`/`parse_stream_arn` are the
  inverses of `animus_cp_data::segment::shard_id`/`wire::stream_arn`,
  **duplicated rather than depending on `animus-cp-data`** — this crate
  stays dependency-light by re-deriving small byte-shape functions instead
  of pulling in a whole sibling crate, the same precedent its other
  cross-crate duplications follow.
- `sigv4` (ADR 0057) — client-edge SigV4 verification and signing: canonical
  request / string-to-sign / HMAC-SHA256 signing-key chain, plus
  constant-shape signature comparison. This is a **deliberate, narrow
  widening of the crate's charter** flagged by that ADR (until now:
  decode/encode only) — it stays inside the same purity rules as every other
  module here (no `Env`, no I/O, `BTreeMap` only), and in particular **no
  clock call of its own**: `verify(req, credentials, now_epoch_ms)` takes
  "now" as a parameter, mirroring exactly how `ttl.rs` takes
  `now_epoch_secs` rather than reading a clock — the caller (`animusd`)
  reads `env.wall_now()` (ADR 0051 discipline) and passes the result in.
  `SigV4Error`'s `error_code()`/`type_name()`/`message()` render the
  AWS-faithful `com.amazon.coral.service#…` wire shape (ADR 0057's
  error-mapping table); the module doc comment documents and justifies the
  check order (structural → unknown key → skew → scope/signature compare)
  the ADR leaves to the implementation. **`parse_credential(req) ->
  Result<ParsedCredential, SigV4Error>` (ADR 0066 §3, S-02 step 3)** does
  only `verify`'s own step-1 structural parse (`Authorization`'s
  `Credential` scope: access key id + region), stopping short of any
  credential-store lookup or crypto — added because `animus-node`'s
  merged catalog-then-bootstrap gate (`sigv4_gate::merged_sigv4_gate`)
  needs the access key id up front, to look up the replicated catalog's
  candidate secrets one at a time, before it can call `verify` at all;
  `region` rides along for `animusd::authz`'s `AccessDeniedException`
  message, which synthesizes a table ARN from the caller's own credential
  scope rather than inventing a region. `canonical_request`/
  `string_to_sign`/`sign` are exported (beyond what `verify` alone would
  need) specifically so the vendored test-vector suite below can assert
  each intermediate stage, and so a hand-rolled test signer (`animusd`,
  ADR 0057's e2e tests) can produce a real `Authorization` header without
  duplicating the HMAC chain.
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
  range scan. **Numbers (`N`) are carried through an order-preserving byte
  encoding (`numkey`, ADR 0063)** — sign class byte, biased exponent, digit
  run — so the stored/scanned key order equals DynamoDB's own numeric order,
  the same guarantee `S`/`B` already had (UTF-8 byte order and raw byte order
  respectively, both already matching DynamoDB for those types).
  `SortKeyCondition::matches`, the **in-memory filter** applied to the rows a
  range scan returns, also compares `N` **numerically**, over the decimal
  text (not the stored bytes — it's handed an already-typed
  `AttributeValue`), matching what the *scan order* now separately
  guarantees at the byte level. `SortKeyCondition` carries
  one `Compare(Comparator, AttributeValue)` variant (reusing the same
  `Comparator` `ConditionExpression` does) for all five `KeyConditionExpression`
  comparators DynamoDB supports (`=`, `<`, `<=`, `>`, `>=` — `<>` stays
  rejected at decode: it is not in AWS's own `KeyConditionExpression`
  grammar), plus `Between` and `BeginsWith` — every comparator is a **filter**
  over the whole scanned partition/index sub-range, never a narrower key-range
  bound (`run_gsi_query`'s `Equals`-only prefix narrowing is the one
  exception, an engine-level optimization that still falls back to filtering
  everything else). A caller holding only a sort key's **raw on-disk bytes**
  (off an engine/tablet scan, with no type tag) must call
  `SortKeyCondition::matches_raw`, not `matches` directly with the bytes
  wrapped in `AttributeValue::B` — every production call site used to do
  exactly that, which silently defeated the numeric compare above for `N`
  (the raw bytes are the `numkey` encoding, not decimal text, and `B`-vs-`N`
  falls back to a byte compare) even after `matches` itself went numeric;
  `matches_raw` reinterprets the raw bytes as the condition's own declared
  type first — decoding them via `numkey::decode` for `N` (not
  reinterpreting as UTF-8 the way `S` would be).
  **Range/`BETWEEN` filtering, and result *ordering* (`ScanIndexForward`),
  are both correct for `N`** end to end (base table, GSI, LSI), including
  mixed digit counts, negatives, and decimals — a page ordered by
  `ScanIndexForward` agrees with DynamoDB's own numeric ordering, closing the
  gap ADR 0063 exists to close (a byte-range scan bound derived from a
  numeric predicate, e.g. tightening `BETWEEN` past a filter, is the one
  follow-up that ADR deliberately leaves unscheduled — see its "What this
  ADR does not do" section). **The stored data-plane
  key adds a prefix at the `animusd` edge** (`dynamo.rs::item_key`, ADR
  0022/0023): `partition_token(escape(pk)) || escape(pk) || sk` — a Murmur3
  token spreads partitions across the table's hash ring, and there is **no
  table prefix** (tables are separated by per-table tablets, the table is a
  routing argument).
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
- **Projection** supports **dotted document paths and list-index segments**
  (W-02): `ProjectionExpression` is a comma-separated list of paths `a.b.c`/
  `a[0]`/`a[0].b`/`matrix[1][2]` (with `#alias` placeholders per `.`-separated
  segment, resolved via `ExpressionAttributeNames` — the index suffix rides
  straight through an alias unchanged), or the legacy `AttributesToGet` array
  (top-level names only, no `.`/`[` — unchanged). Each path decodes to
  `Vec<PathSegment>` (`Field(String)`/`Index(usize)`, `wire::parse_projection_path`),
  not a plain string, so `Projection::apply` never re-parses at apply time.
  `Projection::apply` reconstructs the nested structure a path reaches: a
  `Field` chain rebuilds nested `M`s (`a.b` ⇒ `{a:{b:..}}`); an `Index`
  reconstructs the parent `L` **compacted** to just the selected elements, in
  ascending index order — DynamoDB's own documented contract ("project `a[1]`
  and `a[3]`" yields a two-element list, not a sparse one). Implemented via an
  intermediate `Proj` accumulator tree (`Empty`/`Whole`/`Map`/`List`, the
  `List` variant keeping a sorted `BTreeMap<usize, Proj>` until a final
  `finalize` pass compacts it) rather than mutating `Item`/`AttributeValue`
  directly — needed because a list projection must merge overlapping/nested
  index selections (`a[1]` and `a[1].b`) before it knows how many elements the
  final compacted list has. An out-of-range index, or an index applied to a
  non-`L` value, yields nothing for that path, exactly like a `Field` into a
  non-`M`/absent key. Malformed index syntax (`a[`, `a[x]`, `a[-1]`, unbalanced
  brackets) is a decode-time `ValidationException` (`parse_index_chain`) — the
  digits-only check is what rejects a negative index. Projection is applied at
  the edge (`animusd`) after the read; for `Scan` the `FilterExpression` sees
  the whole item *before* projection trims it. An index `Query` with no
  explicit projection falls back to the index's declared `IndexProjection`.
- **`ReturnValues`** supports `NONE` (default) and `ALL_OLD` on Put/Delete; the
  edge reads the prior item once (reusing it for any `ConditionExpression` check,
  so there is no double read) and `write_response` echoes it under `Attributes`.
  `UpdateItem` additionally supports `ALL_NEW` and `UPDATED_OLD`/`UPDATED_NEW`
  (`update_response`, `changed_attributes` diffing the old/new images against
  whichever side changed) — see `animusd`'s `tests/dynamo_updated_return_values.rs`.
- **`UpdateItem`/`BatchWriteItem`/`TransactWriteItems`/`TransactGetItems`** are
  decoded here and run at the `animusd` edge. `UpdateItem` is read-modify-write
  of one item applying `SET`/`REMOVE`/`ADD`/`DELETE` (upsert when absent) —
  see the dedicated `UpdateExpression` bullet below (issue #375/W-01) for the
  full grammar; `BatchWriteItem`
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
- **`TransactWriteItems` `ClientRequestToken` idempotency is implemented**
  (ADR 0018's 2026-08-24 amendment): `wire::decode_transact_write` decodes
  an optional token (1..=36 chars) onto `Operation::TransactWriteItems`, and
  `wire::transact_write_fingerprint` hashes the decoded `Vec<TransactAction>`
  (SHA-256, deterministic — every nested type is `BTreeMap`-backed) so
  `animusd::dynamo::run_transact` can dedupe a retried token against a
  durable record on the reserved internal `__animus_txn_idempotency` table
  (`internal_tables` module, `is_internal_table_name`/`TXN_IDEMPOTENCY_TABLE`)
  — an ordinary schema-registered, TTL-reaped table, invisible to
  `ListTables`/animusd console (ADR 0052's "AnimusDB Data Console")/every
  client-facing data or DDL operation.
  New `WireError` constructors `idempotent_parameter_mismatch`/
  `transaction_in_progress` carry the two new failure shapes a reused token
  can produce. `TransactGetItems` carries no such token — AWS gives reads
  nothing to deduplicate. See the ADR amendment and `animusd::dynamo::
  run_transact`'s own doc for the full protocol (including the deliberate
  `PENDING`→`TransactionInProgressException` conservative narrowing).
  **The 2026-08-27 amendment closed issue #298's "deep shape A" residual**
  (a client-level retry of an un-tokened `TransactWriteItems` racing its own
  already-committed first attempt): the conditional-claim `Put` already
  guaranteed at-most-once *execution*, but `run_transact` used to record a
  possibly-wrong `CANCELLED` outcome for a genuinely ambiguous `cp_txn`
  failure (a leader move mid stage, or similar — nothing this crate's own
  wire layer sees or needs to change for). `animusd/CLAUDE.md`'s Multi-
  participant transactions section has the fix; nothing here changed beyond
  what this bullet already describes — the bug lived entirely in
  `animusd`'s outcome bookkeeping, not in this crate's decode/fingerprint
  layer.
- **`TransactionCanceledException` carries AWS's real per-action
  `CancellationReasons` array** (ADR 0018's 2026-08-24 `CancellationReasons`
  amendment, issue #374 C2, shipped as C2a then C2b): `WireError.reasons`
  (`Option<Vec<CancellationReason>>`) and
  `WireError::transaction_canceled_with_reasons` — one entry per
  `TransactItems` action (`None`/`ConditionalCheckFailed`/
  `TransactionConflict`, `Item` echoed only for a `ConditionalCheckFailed`
  entry whose own action asked `ReturnValuesOnConditionCheckFailure:
  "ALL_OLD"`); the plain aggregate-only `WireError::transaction_canceled`
  stays for the cases with no single responsible action to name (a cached
  `CANCELLED` idempotency replay, a structural/routing abort). See the ADR
  amendment for the full design, including why `TransactionConflict`'s
  practical reachability through this wire is narrower than it looks (every
  DynamoDB write action reads its item's current value before ever staging,
  so the apply-time `IntentBlocked` guard `TransactionConflict` maps from is
  reached by the raw client protocol's plain writes, not ordinary
  `TransactWriteItems` contention).
- **Per-table throttling's config surface (ADR 0065 §5(b), W-08 step 4)**:
  `CreateTable` decodes `BillingMode` (`PROVISIONED`/`PAY_PER_REQUEST`,
  default `PAY_PER_REQUEST` — this adapter's own default, not real
  DynamoDB's legacy `PROVISIONED` one) and, when `PROVISIONED`, a required
  `ProvisionedThroughput` (`decode_create_table_throughput` /
  `decode_provisioned_throughput`, both units `>= 1` or a
  `ValidationException`; `ProvisionedThroughput` alongside `PAY_PER_REQUEST`
  is also rejected) into `Operation::CreateTable`'s new `throughput:
  Option<animus_control::ProvisionedThroughput>` field. `UpdateTable` gained
  a **third** mutually-exclusive change alongside a stream or index change
  (`Operation::UpdateTable`'s new `throughput_update: Option<Option<..>>>` —
  outer `Some` means this call touches throughput, inner `Some(spec)` =
  `PROVISIONED`, inner `None` = `PAY_PER_REQUEST`): `decode_update_table`
  routes to it only when neither an index nor a stream change is present in
  the same call; `reject_billing_mode_combined_with_other_change` handles
  the case where one *is* — a bare `BillingMode: "PAY_PER_REQUEST"`
  restatement alongside a real stream/index change is still tolerated (the
  pre-existing precedent, a common SDK/CLI habit), but `PROVISIONED` or a
  bare `ProvisionedThroughput` combined with either is a genuine second
  change, rejected. `UNSUPPORTED_UPDATE_TABLE_KEYS` no longer lists
  `ProvisionedThroughput` — it is a modeled change now, not a blanket
  rejection. `table_description_object` (shared by `CreateTable`/
  `DescribeTable`/`UpdateTable`/`DeleteTable`'s response builders, all four
  of which gained a `throughput: Option<&ProvisionedThroughput>` parameter)
  renders `BillingModeSummary`/`ProvisionedThroughputDescription` —
  `PAY_PER_REQUEST` reports `0`/`0` units + `NumberOfDecreasesToday: 0`,
  matching real DynamoDB's own documented shape for that billing mode, never
  omitting the keys. `animus_control::schema::ProvisionedThroughput { read_
  units, write_units }` is the replicated type (`animus-control`'s own
  entry has the full `TableSchema.throughput`/`MetaCommand::
  SetTableThroughput` design) — this crate imports and re-uses it directly
  rather than minting a wire-local duplicate.
- **Per-table throttling's wire fidelity (ADR 0065, W-08 step 3)**: two new
  `WireError` constructors carry the AWS-faithful shapes for a throttle
  refusal — `WireError::provisioned_throughput_exceeded(message)` (a `400`,
  `__type: "...ProvisionedThroughputExceededException"`, for a single-item
  `PutItem`/`GetItem`/`Query`/`Scan`/etc. refusal) and
  `CancellationReason::throttling_error()` (code `"ThrottlingError"`, no
  `Item` — used by `TransactWriteItems`' own `CancellationReasons[]` when a
  staged write is throttled, alongside the existing
  `ConditionalCheckFailed`/`TransactionConflict`/`None` variants above). All
  enforcement itself lives in `animusd` (see that crate's own ADR 0065
  entry); this crate only supplies the two wire shapes. `BatchGetItem`/
  `BatchWriteItem` never fail wholesale for a throttle refusal — DynamoDB's
  own batch contract is "ship what succeeded, name the rest" regardless of
  *why* an item didn't make it, so a throttled key/item is folded into the
  same `UnprocessedKeys`/`UnprocessedItems` machinery a capacity-exceeded
  batch already uses, not a distinct error shape.
  `wire::batch_get_response(tables: &[(String, Vec<Item>)], unprocessed:
  &[(String, Item)]) -> String` and `wire::batch_write_response(unprocessed:
  &[(String, WriteRequest)]) -> String` both gained an `unprocessed`
  parameter for this (a signature change, not a new function — every
  existing caller needed a compiler-driven update, the identical
  E0061-arity-fan-out idiom the `animusd`-side `ClusterConfig` field
  additions describe): `batch_get_response` renders `UnprocessedKeys`
  grouped by table name (AWS's own shape — `{"TableName": {"Keys":
  [...]}}`), and `batch_write_response` renders `UnprocessedItems` in the
  original per-request shape (`PutRequest`/`DeleteRequest`, not a bare
  item) so a retried request round-trips through the identical decoder a
  fresh `BatchWriteItem` call would use. An empty `unprocessed` slice still
  renders the field present as `{}` (matching the pre-existing
  `batch_get_response_groups_by_table` test's own expectation), never
  omitted — a caller checks for an empty object, exactly like real
  DynamoDB.
- `BatchGetItem` is implemented (`decode_batch_get` in `wire.rs`).
- **`UpdateExpression` (issue #375/roadmap W-01) is now the full documented
  subset** (ADR 0054's 2026-09-05 step-1 amendment: `PathSegment`/
  `UpdateOperand`/`UpdateExpr`/`UpdateAction` and the evaluator this bullet
  describes — `apply_update` and everything it calls — now live in
  `animus-item::update`, re-exported here; every reference below to a
  function living "in `wire.rs`" means the **parser** producing a
  `Vec<UpdateAction>` from request JSON, which is the half that stayed,
  since it needs `ExpressionAttributeNames`/`Values`. See that crate's
  `CLAUDE.md` for the full account of the split): `SET path = expr` (`expr`
  is one `UpdateOperand` — a `:value`, a
  document path read from the item, or `if_not_exists(path, default)`/
  `list_append(a, b)` — or `operand + operand`/`operand - operand`, exactly
  one arithmetic operator, both sides `N`), `REMOVE path`, `ADD path :v`, and
  `DELETE path :v`, in any order, any number of clauses. Every target/
  operand `path` is a full **document path** (`a`, `a.b`, `a[0]`,
  `#n.b[1]`) — the identical [`PathSegment`]-based grammar
  `ProjectionExpression` uses (W-02), reused verbatim via
  `parse_update_path`/`parse_projection_path` in `wire.rs` rather than
  reimplemented; `SET`/`REMOVE` were the first to gain this (ADD/DELETE
  followed, since it turned out to be "trivial" once the get/set/remove
  document-path primitives existed for `SET`/`REMOVE`). Grammar built in
  three layers, each its own commit: function calls (`if_not_exists`/
  `list_append`, SET-only) first, then `+`/`-` arithmetic as first-class
  tokens (`UpdateToken::Plus`/`Minus` — an unaliased attribute name
  containing a literal `+`/`-` needs `#alias`, mirroring the pre-existing
  `.`/`[` rule), then nested-path targets last (the data-model change to
  `UpdateAction`/`UpdateOperand`, from a bare `String` to `Vec<PathSegment>`).
  **Evaluation happens as the fold applies** (`eval_update_expr`/
  `eval_update_operand`, now in `animus-item::update`, called from
  `apply_update` — which itself still always runs at the leader, under the
  same `rmw_lock`-guarded scope ADD's read-modify-write already used, via
  `animusd::dynamo::kind_write_item_at_leader`; `apply_update`'s call sites
  in `animusd` are unchanged — ADR 0054 step 1 is a pure relocation, not
  yet the move to the tablet's actual Raft-apply path that name evokes),
  against the item as the fold has
  built it so far — a documented simplification of DynamoDB's own
  within-one-expression ordering semantics (see `UpdateOperand`'s own doc),
  not a modeled property. `SET`'s target path validates that every segment
  but the last already exists before evaluating the expression
  (`document_path_parent_exists`) — `SET a.b = :v` on an absent `a` is a
  `ValidationException` ("The document path provided in the update
  expression is invalid for update"), matching AWS; only the *final*
  segment may be new, and a list index past the current length **appends**
  rather than padding (AWS's own documented `SET list[n]` behavior beyond
  bounds). `REMOVE` on a missing path (including a missing parent) is a
  no-op, and `REMOVE a[i]` compacts the list (`Vec::remove`, no sparse
  hole). **Overlapping targets in one expression are rejected**
  (`validate_no_overlapping_targets`, `O(n²)` in the action count, always
  small) — `SET a = :x, a.b = :y` is a `ValidationException` ("Two document
  paths overlap with each other"), checked across every action's target
  (`SET`/`REMOVE`/`ADD`/`DELETE` alike) by prefix comparison of their
  `Vec<PathSegment>`s. `condition::negate_numeric` is new (issue #375 PR2)
  — `SET a = a - :x` is implemented as `add_numeric(a, negate_numeric(x))`,
  DynamoDB's only subtraction path, the identical "add a negated operand"
  shape this module's own differential-test `negate` helper already
  exercised `add_numeric` with, now exposed for production use rather than
  test-only. **Residual gap**: within-expression path-read ordering (see
  above) — DynamoDB's own semantics here are not fully pinned down or
  tested against; this crate documents its own choice rather than claiming
  exact AWS fidelity on that one point. **`Query` now paginates exactly like `Scan`**
  (`decode_query` parses `Limit`/`ExclusiveStartKey` the same way
  `decode_scan` does, sharing the decode helpers; found and closed while
  building animusd console's Items tab, ADR 0052 PR4, which needed it and
  had to work around its absence): `animusd::dynamo::run_base_query`/
  `run_gsi_query`/`run_lsi_query` push `limit` down via
  `paginated_table_examine`/`paginated_kind_examine_one` bounded to the
  `Query`'s own partition/index sub-range (never the whole table — unlike a
  `Scan`'s unbounded-above range), reusing the identical `LastEvaluatedKey`
  cursor shapes `run_gsi_scan`/`run_lsi_scan` already established
  (`gsi_key_item_of`/`lsi_key_item_of`) so a `Query` page and a `Scan` page
  over the same index agree by construction. An `ExclusiveStartKey` is
  validated against its target's **exact** key-attribute-name set
  (`validate_query_cursor_shape`) before use — a GSI/LSI cursor also carries
  the base table's own key attributes, so a laxer "needed attributes
  present" check would silently accept a cursor from a different `Query`;
  a mismatch is `ValidationException`, matching DynamoDB. The Data
  Console's Items tab itself (`console.rs`'s `QueryItemsRequest`) does not
  yet expose `limit`/`exclusive_start_key` to use this — a separate,
  not-yet-done console-side follow-up, tracked in that module's own doc.
  `DescribeTimeToLive`'s `TimeToLiveStatus` (ADR 0051)
  only ever renders `ENABLED`/`DISABLED`, never AWS's transient
  `ENABLING`/`DISABLING` — this adapter's `UpdateTimeToLive` takes effect
  synchronously, so there is no in-flight state to report. The
  `Scan`/`Query` `FilterExpression` shares `ConditionExpression` wholesale —
  same decoder, same `evaluate` — not a narrower predicate subset; the two
  surfaces are indistinguishable by construction, which is what let the
  `size()` fix below land once for both. `ConditionExpression::evaluate`
  returns `Result<bool, ConditionError>`, not a bare `bool`: a missing
  attribute is still `Ok(false)` on every leaf (DynamoDB has no
  three-valued logic), but `size()`/`begins_with()`/`contains()` are
  *functions* with a fixed operand-type domain, and applying one to an
  **existing** attribute outside that domain (`size()` on an `N`/`BOOL`/
  `NULL`; `begins_with()` on anything but `S`/`B`; `contains()` on anything
  but `S`/`SS`/`NS`/`BS`/`L`) is `Err(ConditionError)` — a real DynamoDB
  `ValidationException` at evaluation time, matching AWS's own wording
  (`"Incorrect operand type for operator or function; operator or
  function: <fn>, operand type: <TYPE>"`). An ordinary comparator
  (`Compare`/`Between`/`In`) type mismatch between two *supplied* operands
  is unaffected and stays `Ok(false)`, DynamoDB's own documented comparator
  behavior. `animusd`'s `WireError: From<ConditionError>` maps it to the
  wire `ValidationException` at every evaluation call site (conditional
  writes, `Scan`/`Query` filters, `TransactWriteItems`'
  `ConditionCheck`) — grep `condition.rs`'s own module doc before touching
  this again; the false-vs-error distinction is easy to blur back together.
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

**ADR 0054 step 1 moved `condition`/`index`/`numkey`'s own tests, plus
`apply_update`'s and the stored-item codec's, into `animus-item`** (they
moved with the code they test — no assertion changed) — see that crate's
`CLAUDE.md` Tests section. What follows here describes what stayed.

`cargo test -p animus-dynamo` — `item_api.rs` over `MemoryEngine`, plus unit
tests for `wire`/`streams_wire`/`registry`/`schema`/`ttl`
(JSON decode/encode, the `UpdateExpression`/`ConditionExpression` **parser**
— as opposed to the evaluator, whose own tests moved — iterator-token
round-trip, response-shape encoders, and `ttl`'s expiry-boundary table —
absent/wrong-type attributes, future/past/equal-to-now, fractional
truncation, the negative-value fold, and both sides of the
`MAX_PAST_EXPIRY_SECS` window), and `sigv4`'s own unit tests (a correctly
signed request accepted; tampered body / wrong secret / unknown key /
absent or malformed `Authorization` / a `SignedHeaders` missing `host` /
scope-date mismatch / non-`aws4_request` terminal / malformed `X-Amz-Date` /
both skew directions including the exact ±5 minute boundary, all rejected
with the expected `SigV4Error` variant; `error_code()`/`type_name()`/
`message()` asserted against ADR 0057's table verbatim).
`tests/sigv4_vectors_test.rs` runs `sigv4` against AWS's own vendored
`aws-sig-v4-test-suite` (`tests/sigv4_vectors/`, see that directory's
`README.md` for provenance) — the independent, AWS-authored compatibility
oracle ADR 0057 substitutes for a real-SDK smoke test (which cargo-deny's
license allow-list rules out, see the ADR): each case parses a `.req`
fixture, asserts the canonical request/string-to-sign/`Authorization`
against the suite's own precomputed `.creq`/`.sts`/`.authz`, then re-verifies
the signed request through `sigv4::verify`. The rejection of `ConsistentRead` on
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
