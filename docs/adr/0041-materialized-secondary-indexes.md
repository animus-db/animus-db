# ADR 0041 — Materialized secondary indexes: GSI as a hidden table (async, over a change log), LSI colocated (atomic)

- **Status:** Proposed
- **Date:** 2026-08-13
- **Amends:** [ADR 0013](0013-replicated-schemas.md) (the index *definitions* it
  replicated now have index *data* to match),
  [ADR 0023](0023-table-scoped-tablets.md) (a GSI is a table-scoped ring of its
  own), [ADR 0028](0028-shared-storage-single-command-split.md) (a tablet group
  owns several `StorageScope`s, one per row kind, sharing its key range),
  [ADR 0034](0034-byte-based-auto-split.md) (the byte estimate bounds to the base
  scope, so auto-split ignores change-log churn). ADR 0022's key layout is
  **unchanged** — the row kind lives in the scope prefix, above the token.
- **Depends on:** [ADR 0018](0018-cross-tablet-transactions.md) (HLC timestamps;
  apply-time write-key conditions), [ADR 0024](0024-drop-table-data-gc.md) (drop
  cascade), [ADR 0034](0034-byte-based-auto-split.md) (index tablets auto-split
  like any other)

## Context

DynamoDB secondary indexes are already decoded, validated, replicated, and
queryable at the surface: `CreateTable` accepts `GlobalSecondaryIndexes` and
`LocalSecondaryIndexes` with `ALL`/`KEYS_ONLY`/`INCLUDE` projections, the
definitions replicate through the control-plane catalog as `IndexDef` (ADR 0013),
and `Query` accepts an `IndexName` plus a sort condition.

What does not exist is the index itself.

Index **entries** live in a process-local `BTreeMap` inside
`animus_dynamo::registry::SchemaRegistry`, maintained by `note_put` / `note_delete`
on writes that happen to pass through that node. Consequences, all of them
load-bearing:

- **A node that never observed the writes has an empty index.** So does a node
  that restarted. `animusd::dynamo::backfill_index_if_needed` patches this on the
  first query against such an index with a **full base-table scan**, replayed
  through `note_put`. That is O(table) per node, per index, on a read.
- **The scan races live writes**, which needed its own defence
  (`SchemaRegistry::touched_since_backfill`, so the replay does not revert a
  concurrent write with its own stale scanned value).
- **Only base keys are stored**, never projected attributes, so every index hit
  costs a base-table read per matched item.
- **The index is invisible to the rest of the system**: it does not split
  (ADR 0028/0034), is not reclaimed on drop (ADR 0024), is not scoped (ADR
  0028), and does not appear in the dashboard. (It also didn't merge, under
  ADR 0033, at this ADR's writing — moot since ADR 0044 removed merge
  entirely.)

The surface is faithful; the substrate is a per-process cache. This ADR makes
index entries first-class replicated data-plane rows and deletes the in-memory
machinery entirely.

### The consistency question

DynamoDB's own contract is asymmetric, and the asymmetry is not incidental:

- A **local** secondary index shares the base item's partition key, so DynamoDB
  maintains it inside the same partition, atomically. LSI reads may set
  `ConsistentRead=true`.
- A **global** secondary index is maintained **asynchronously**. GSI reads are
  eventually consistent, and `ConsistentRead=true` against a GSI is an error.

The tempting alternative — maintain every index synchronously via ADR 0018's
2PC — was considered and **rejected**. It is less machinery (no intent record, no
drain, no repair), and it is strictly *stronger* than DynamoDB, which sounds like
a free win. It is not:

1. **It couples write availability to index availability.** A write would need
   the base tablet's leader *and* every index tablet's leader simultaneously.
   Adding a GSI to a table would make that table's writes strictly *less*
   available — three GSIs means four Raft groups that must all be healthy through
   any rolling restart or election. In DynamoDB an unavailable GSI never fails a
   base write; the index falls behind instead. That decoupling is the contract,
   not a performance detail.
2. **Write latency would scale with index count**, turning "add an index" into a
   write-path regression. DynamoDB writes are O(1) in the number of GSIs.
3. **Exceeding a compatibility target is a trap.** An application that
   accidentally read-your-writes through a GSI here would work, then fail rarely
   and subtly against real DynamoDB. For a drop-in-compatible adapter, matching
   the guarantee beats exceeding it.
4. **It hollows out API surface that exists to describe asynchrony** —
   `IndexStatus`, `Backfilling`, GSI capacity — leaving fields that are present
   and semantically vacuous.

Note also that the direction is one-way: strong→async later is a *weakening* that
breaks anything which came to depend on the strength.

## Decision

**We will materialize secondary-index entries as replicated data-plane rows,
matching DynamoDB's consistency contract exactly: LSI maintained atomically with
the base write, GSI maintained asynchronously by a drain over a durable per-tablet
change log — the same log DynamoDB Streams will later consume.**

### 1. A GSI is a hidden table

Each global secondary index gets its own table in the tablet map, named
`"<base>$<index>"`. It therefore inherits, with no new distributed machinery: its
own per-table hash ring (ADR 0023), per-tablet Raft (ADR 0017), byte-based
auto-split (ADR 0034), drop-table GC (ADR 0024), `StorageScope`
isolation (ADR 0028), the native linearizable range scan, and the dashboard.
(It also inherited merge, under ADR 0033, at this ADR's writing; tablets are
split-only now, ADR 0044.)

Its tablets are provisioned by the existing lazy path
(`ClientCtx::provision_tablet`), on the same terms as any user table.

`$` is not a legal character in a DynamoDB table or index name
(`[A-Za-z0-9_.-]`) nor in a CQL identifier, so the name cannot collide with a
user table — but this is **enforced, not assumed**: `Metadata::apply`'s
`CreateTableSchema` arm rejects any user table name containing `$`, alongside the
existing `syskv::is_reserved_name` gate. Index tables get no `TableSchema` entry
of their own; the authoritative shape stays the base table's `IndexDef`.

**As-built correction (2026-08-14, ADR 0042/0043 round-3 salvage):** the
apply-time rejection described above was **not actually wired in** until
this fix — this paragraph described the intended design from the start, but
no code path ever rejected a `$`-containing user table name before then (an
audit found the gap while re-grounding the streams work in what the tree
actually enforces). Closed at the same single call site this paragraph
names, so the claim is now true, not merely intended.

### 2. An LSI is colocated in the base table's tablets

A local secondary index hashes by the base partition key, so its rows share the
base row's **token** and therefore its tablet. We place them there, which makes
base row and LSI rows one `ClientRequest::PutBatch` — **one Raft log entry, one
commit round, one apply**. LSI maintenance is thus atomic with the base write and
strongly consistent, with no intent, no drain, and no 2PC.

> **As-built corrective note (2026-08-13, per-operation write coverage
> closed).** §2/§4's `index_aware_write`/`kind_writes_for_item` mechanism
> shipped wired into the single-item `PutItem`/`DeleteItem` handlers only;
> `UpdateItem`, `BatchWriteItem`, and `TransactWriteItems` kept committing
> through the plain pre-ADR-0041 write primitives, so a table's secondary
> indexes silently never saw a write made exclusively through any of those
> three ops (documented as a known gap in the §5 PR, invisible while the
> now-deleted edge-local in-memory index still papered over it for a single
> observing process). Coverage is now:
>
> - **`UpdateItem`** routes its single final write through
>   `index_aware_write`, passing the RMW's own before/after images — an exact
>   mirror of `PutItem`'s call shape, since both are "one item, one new
>   value, an optional prior value already in hand."
> - **`BatchWriteItem`** keeps the original `cp_batch_write` fast path
>   (one Raft entry per tablet, no per-item read) for any table with **no**
>   secondary index — unchanged, pays nothing. A table with at least one
>   index instead routes each `Put`/`Delete` request through
>   `index_aware_write` **individually**, reading the old item first (the
>   LSI diff needs it — a real, unavoidable extra read, paid only by indexed
>   tables) under the node's RMW lock for that one request's span. This is
>   **per-item atomicity only**, matching `BatchWriteItem`'s own pre-existing
>   non-atomic contract — one request's outcome was never allowed to depend
>   on another's, before or after this fix.
> - **`TransactWriteItems` is rejected up front** (`ValidationException`,
>   *"transactional writes on an indexed table are not yet supported (ADR
>   0041: TxnStage kind-write extension pending)"*) whenever any `Put`/
>   `Delete`/`Update` action targets a table with at least one secondary
>   index — a bare `ConditionCheck` doesn't count, so a transaction may still
>   write freely to unindexed tables alongside a `ConditionCheck` on an
>   indexed one. This is a **deliberate, not a stopgap, choice**: `cp_txn`'s
>   `KvCommand::TxnStage` only ever stages the base row, with no multi-kind-
>   write extension (the equivalent of `KindBatch` for a transaction's own
>   apply). Staging just the base row would commit the item while silently
>   never writing its LSI rows or change-log record — the table's indexes go
>   **permanently stale** with no error, no warning, and no drain input ever
>   produced for that write. In a pre-alpha, correctness-first system, a loud
>   rejection of an unsupported combination is strictly better than a silent,
>   permanent wrong answer. The genuine fix — extending `TxnStage` so a
>   transaction's own apply can stage a multi-kind atomic write, the
>   transactional analogue of `cp_kind_write` — is a real `animus-cp-data`
>   protocol change and is intentionally deferred as a named follow-up, not
>   folded into this correctness fix.
>
> Regression coverage: `animusd/tests/dynamo_index_writes.rs`.

### 3. Row kinds are separate storage scopes, not a discriminator in the key

A base tablet now holds four kinds of row. They are separated **above the
partition token**, in the `StorageScope` prefix — not by a discriminator byte
inside the key:

```
physical:  escape(table) || KIND || token(escape(pk)) || escape(pk) || …
           └────── scope prefix ──────┘└──────── logical key ────────┘

KIND 0x00  base rows        logical: token || escape(pk) || sk
KIND 0x01  LSI rows         logical: token || escape(pk) || escape(index) || escape(alt_sort) || sk
KIND 0x02  change records   logical: token || escape(pk) || hlc
KIND 0x03  footprints       logical: token || escape(pk)
```

All four scopes belong to **one tablet's Raft group** and share **one
`KeyRange`** — literally the same `Arc<Mutex<KeyRange>>`, so a split's
`narrow_scope` moves every kind in one call (as would a merge's
`widen_scope`, were merge not removed entirely — ADR 0044, tablets are
split-only). One
`PutBatch` to that group therefore still writes base + LSI + change record +
footprint as a single Raft entry, which is what §2 and §4 rest on. This is the
column-family shape Cockroach and TiKV use for the same reason.

**Why not a discriminator inside the logical key.** It was the original design
here, and it is wrong three ways. Interleaving the kinds within each partition
means a full `Scan` traverses all four to return one; the LSM mixes the
high-churn change log with comparatively stable base rows in the same SSTables,
so *trimming the log rewrites SSTables full of base rows*; and ADR 0034's byte
estimate starts reacting to log churn rather than base-data volume. Separating in
the prefix fixes all three, and lets `approx_bytes` bound to the base scope
alone.

**Why not above the token in the *logical* key either.** Two independent
reasons. A tablet *is* a `[start, end)` range over token space and the router
resolves key→tablet by the token prefix, so a kind above the token would stop a
tablet's ownership being one contiguous range — breaking `KeyRange`, `contains`,
`split_at`, the router, and split/merge together (ADR 0022/0023). And the
transaction layer **asserts** the property outright: `RaftKvNode::txn_stage`
requires `anchor.len() >= animus_tablet::TOKEN_BYTES` and slices
`anchor[..TOKEN_BYTES]` as the token, then derives every `TxnRecord::intent_span`
as a `KeyRange` over those same keys. A kind byte ahead of the token would have
forced a revision of every span, fence, record key and seal marker in the 2PC
machinery (ADR 0018) — far more dangerous than the index feature itself. Keeping
the kind in the scope prefix leaves all of it untouched.

A **GSI row** lives in its own hidden table's tablets (§1), so it needs neither a
kind nor a scope of its own:

```
GSI row    token(escape(ihash)) || escape(ihash) || escape(isort) || escape(base_pk) || base_sk
```

The trailing `escape(base_pk) || base_sk` both disambiguates two items sharing an
index key and makes the base key recoverable by peeling escaped segments.
`escape(isort)` is absent for a hash-only GSI. Recovery needs to be told whether
the index is composite: a hash-only index's `escape(base_pk)` sits in exactly the
byte position a composite index's `escape(isort)` would, so the key alone cannot
report its own shape.

**Index row values carry the projection** (`ALL` / `KEYS_ONLY` / `INCLUDE`), so
an index `Query` is one range scan that returns items directly — no base-table
fan-out read. This is both DynamoDB's behaviour and the point of an index.

*(The **base logical key is unchanged** from ADR 0022 — `token || escape(pk) ||
sk` — which is the part that matters: no code that builds, parses, fences, or
spans a key moves, and the CQL edge needs no change at all. `Put`/`PutBatch`
keep meaning "the base kind". **Physical** keys do shift, by the one kind byte
now in every scope prefix — including for `StorageScope::whole()`, which stops
being an identity transform — and the snapshot image gains a per-entry kind tag.
Both are format breaks the repo's no-live-deployments constraint makes free, but
"nothing moves on disk" would be the wrong claim: a cluster's bytes are laid out
differently, they are simply laid out consistently.)*

### 4. GSI maintenance is a per-tablet change log plus a derivative drain

A write to a table with at least one GSI writes, **in one `PutBatch` to the base
tablet**: the base row, every LSI row, an updated **footprint**, and a **change
record**. The write is acknowledged at that point — base-write latency and
availability are unchanged by the presence of a GSI.

- The **footprint** (`0x03`) records which index rows this base key currently
  occupies — the `(index, hash, sort)` triples, not the values.
- The **change record** (`0x02 || hlc`) records one mutation of one base key: its
  HLC commit timestamp, and the old and new item images. Records are
  **append-only and non-collapsing** — two writes to one key are two records,
  ordered by HLC within the partition.

A background **drain** — one per node, sweeping the tablet groups this node
leads, in the exact shape of `txn_resolver_loop` (`animusd`, ADR 0018 §2/PR5) —
consumes change records past its cursor: for each dirty base key, read the base
row and its footprint, recompute the desired index rows from the base row's
*current* value, write the additions, delete the stale rows named by the
footprint, write the new footprint, and advance the cursor.

The drain is **derivative, not delta-based** — it treats a change record purely
as a *signal that a key is dirty*, and reconciles toward the base row's current
value rather than replaying the record's images. That single choice is what makes
the three hard failure modes disappear:

- **Idempotent**: a crash anywhere re-runs the whole reconciliation harmlessly.
- **Self-superseding**: several records for one key collapse into one
  reconciliation toward the current value, so there are never stale deltas to
  order against each other.
- **Orphan-free**: a stale index row is, by construction, one named in the
  footprint but absent from the recomputation — there is no separate class of
  orphan needing its own sweeper.

The cursor advances only past records the reconciliation actually covered
(ADR 0018's apply-time write-key conditions guard the update), so a write landing
mid-drain re-dirties the key rather than being skipped.

The cursor and the set of dirty keys are tracked in memory and **rebuilt from the
engine at group start**, the same discipline `TxnTracker` uses — so a leader that
inherits a tablet after restart or election resumes from the durable cursor
rather than from nothing.

> **As-built corrective note (2026-08-13, the drain's landing).** Two
> mechanisms this section describes came out simpler in implementation:
>
> - **There is no cursor.** Consuming a record *is* the trim: the drain
>   deletes the records it has reconciled in the same Raft entry that writes
>   the updated footprint, so "position in the log" is simply "whatever
>   records still exist," durable and rebuilt-for-free at group start — no
>   in-memory cursor, no rebuild scan, nothing to advance. A separate cursor
>   only becomes necessary when records must **outlive** their first
>   consumer — exactly the Streams case — and is deferred to that ADR
>   alongside its retention window (§4a's trim-policy language reads
>   accordingly).
> - **The footprint is written only by the drain, never by the base write.**
>   The writer's atomic batch is base row + LSI rows + change record; the
>   footprint records what the drain last *materialized* (which trails the
>   base write by design), so having the writer update it would claim rows
>   that don't exist yet. It is keyed by partition, not by item, holding one
>   `ItemFootprint` per sort key.
>
> Also settled at landing: the hidden index table's first tablet is
> provisioned **lazily by the drain** (first tick with records to apply);
> stale GSI rows are pruned with a genuine engine delete, not a
> `StoredItem::Tombstone` sentinel value — the sentinel exists so a base
> `DeleteItem` stays observable to conditional reads and to this very change
> log, but an index row is wholly derived and nothing would ever reclaim a
> sentinel from a hidden table (the LSI path's `KindBatch` `None`-value prune
> is the same choice, made inline).

### 4a. Why the log is non-collapsing: DynamoDB Streams

A collapsing dirty-marker would be marginally cheaper and is all the GSI drain
needs on its own. The log is deliberately append-only and image-carrying because
**DynamoDB Streams is the next feature after this one**, and in DynamoDB the two
are not independent: GSI propagation and Streams ride the same internal change
log. Making that true here costs almost nothing today — the record is written in
the same `PutBatch` either way, from the same discriminator space — and
retrofitting it later would be an on-disk format break plus a second write path.

Streams then becomes **a second consumer of the same log**, reading records
*literally and in order* where the GSI drain reads them *derivatively*. The
shared substrate is: the atomic co-write with the base row, the row-kind
discriminator, the leader-swept consumer loop, the cursor rebuilt at group start,
and at-least-once delivery with an HLC sequence for dedup. A partition's record
range maps naturally onto a stream shard.

The log is trimmed behind the **slowest consumer** — today the GSI drain alone
(whose consumption *is* the trim; see §4's as-built note), which is also what
bounds its growth. The Streams ADR extends that policy with a retention window
and per-consumer iterators — the point where a genuine cursor first becomes
necessary, since records must then outlive the drain's own consumption; the
**shards, iterators, `StreamViewType` projections, and the wire surface are all
deferred to it**. Only the record format, the ordering, and the trim are settled
here.

### 5. Reads, lifecycle, and what gets deleted

- An index `Query`/`Scan` is a native CP range scan over the index's keyspace
  (its own table for a GSI, the `0x01` sub-range for an LSI) with `Limit` /
  `ExclusiveStartKey` / `LastEvaluatedKey`, served from the index row's projected
  value.
- `ConsistentRead=true` against a **GSI** is rejected, as DynamoDB does; against
  an LSI it is honoured and already true.
- A base `Query`/`Scan` reads the **base scope** and sees nothing else — no
  filtering, no partially-filtered pages, and no change-log bytes traversed. Each
  access pattern touches exactly one scope, which is the point of §3.
- `drop_table` cascades to every index table's tablets (ADR 0024); LSI rows and
  markers are reclaimed with the base table's own tablets automatically.
- **Deleted**: `note_put`, `note_delete`, `index_query_keys`,
  `backfill_index_if_needed`, `touched_since_backfill`, and the registry's
  in-memory index entry maps. `SchemaRegistry` keeps only definition
  reconciliation (`sync_indexes`, `index_projected_attributes`,
  `index_is_composite`).

> **As-built corrective note (2026-08-13, the native read path's landing).**
> A GSI `Query` scans the hidden table directly — `partition_token(ihash) ||
> escape(ihash)`, narrowed to `escape(ihash) || escape(isort)` for an `Equals`
> sort condition, filtering every other shape on the recovered sort segment
> (`animus_dynamo::index::parse_gsi_row_key`) — mirroring the drain's own
> `gsi_row_key` byte-for-byte, with **no per-key base-table read-back**: a row's
> stored value is already the drain's projected image. A hidden table with no
> tablet yet (an index that has never drained anything) reads as **empty**
> rather than waiting on routing, the same gate `ClientCtx::cp_get` uses for an
> unprovisioned table — this is the eventually-consistent contract surfacing at
> read time, not a bug.
>
> An LSI `Query` needed one new primitive each in `animus-cp-data` and
> `animusd`: `RaftKvNode::linearizable_scan_kind` (the ReadIndex-barrier dual of
> `local_scan_kind`, since a non-base scope only ever holds committed values —
> no intent resolution needed) and `ClientCtx::cp_scan_kind` (routes by the
> scan's start key, since an LSI query is scoped to one base partition — one
> tablet by construction — verifying start/end resolve to the same tablet, the
> read-side scope pre-check `cp_scan_local` already does for a base scan, and
> forwarding via a new **internal-only** `ClientRequest::KindScan`, refused bare
> exactly like `KindWrite`, handled only inside `cp_serve_forwarded`). Neither
> index `Query` supports `Limit`/`ExclusiveStartKey`/`LastEvaluatedKey`
> pagination — a pre-existing gap this PR did not close, since a base `Query`
> never gained it either (only `Scan` did).

> **As-built corrective note (2026-08-13, the drop-table cascade and
> `ConsistentRead` fidelity gaps closed).** Two gaps this section's body
> already *describes as decided* shipped unimplemented with the §5 read-path
> PR above, and are closed now:
>
> - **`drop_table` did not actually cascade.** `ClientCtx::drop_table` dropped
>   only the base schema + the base table's own tablets — a table with GSIs
>   left every `<base>$<index>` hidden table (tablets, data, and all)
>   orphaned forever, and worse, the GSI *definitions* died with the base
>   schema in the same call, so a retry after a mid-drop crash couldn't even
>   enumerate what needed cleaning up. The fix orders three steps for
>   crash-and-retry convergence: (1) read `metadata_fresh` — a **permanent**
>   decision, since step 2 deletes the defs this step needs — and drop each
>   global index's hidden table's tablets first, while they're still
>   enumerable; (2) drop the base schema; (3) drop the base table's own
>   tablets. LSI rows need no separate step — they, the change log, and the
>   footprints all live in the base table's own tablet group's sibling
>   scopes, and `CpGroup::erase_scope` already iterates every `kind_scopes`
>   entry, not just the base one. A crash between any two steps leaves a
>   state a re-run completes, because each step is independently idempotent
>   (`MetaCommand::DropTableTablets`'s apply is a no-op when there is nothing
>   left to drop, keyed on the tablet map, not the schema catalog — a hidden
>   index table needs no schema entry of its own for this to work). A
>   **second sweep**, run after step 3 and keyed on the tablet map itself
>   rather than the (by-then-gone) index definitions, catches the belt-and-
>   suspenders case: the GSI drain provisions a hidden table's first tablet
>   *lazily*, and can race a fresh one into existence concurrently with the
>   drop (a change record draining mid-drop); this sweep also mops up any
>   orphan a **pre-fix** drop left behind, since it depends on nothing but
>   the tablet map's own naming convention
>   (`animus_dynamo::split_index_table_name`). The drain's own writes racing
>   a concurrent drop were already handled: `index_drain_loop` logs and
>   swallows both `provision_tablet` and `reconcile_partition` errors
>   (best-effort convergence, unchanged by this fix), and the reconciler's
>   `Reclaim` teardown removes a dropped table's groups from
>   `hosted_groups()`, so the drain simply stops sweeping them once gone.
>   Regression: `animusd/tests/drop_table_index_cascade.rs`.
> - **`ConsistentRead` was not decoded at all**, so the rejection this
>   section's body already describes as DynamoDB's contract had nothing to
>   act on. `animus_dynamo::wire::decode_request` now decodes an optional
>   `ConsistentRead` boolean (default `false`) on `GetItem`/`Query`/`Scan`
>   alike — `decode_consistent_read`, shared by all three — but this crate
>   never enforces it: whether `true` is legal depends on an index's
>   replicated *kind*, which lives in the control-plane catalog this crate
>   never sees. `animusd::dynamo::run_index_query` is the one place that
>   ever rejects it, exactly matching the accept/reject matrix this section's
>   body already stated: `ValidationException` when the queried index is
>   `IndexKind::Global`; accepted-and-ignored against an LSI `Query`, a base
>   `Query`/`Scan`, and a base `GetItem` alike, since every one of those
>   reads is already linearizable here regardless of what the client asked
>   for. Regression (decode): `animus-dynamo`'s `wire` unit tests. Regression
>   (rejection/acceptance): `animusd/tests/dynamo_consistent_read.rs`.
>
> Neither gap was reachable from `CreateTable`-declared indexes without a
> multi-step scenario (a populated GSI, then a drop; an explicit
> `ConsistentRead: true`), which is why both shipped unnoticed with the read
> path itself.

> **As-built corrective note (2026-08-13, `Scan` with `IndexName` — the last
> functional gap in this ADR's scope).** This section's body already says
> "An index `Query`/`Scan`" throughout, as though both shipped together; in
> fact only `Query` did with the §5 read-path PR above — a `Scan` against a
> secondary index returned `ValidationException: unsupported operation` (no
> `IndexName` decode existed at all) until now. `Operation::Scan` gained an
> `index: Option<String>` (decoded from `IndexName`, `animus-dynamo::wire`);
> `animusd::dynamo::run_scan` dispatches exactly like `run_query` — base table
> when absent, else `run_gsi_scan`/`run_lsi_scan` by the index's replicated
> kind, with the identical `ConsistentRead`-against-a-GSI rejection.
>
> **The pagination cursor, decided explicitly rather than left implicit**: a
> GSI `Scan`'s `LastEvaluatedKey`/`ExclusiveStartKey` is `{index hash attr,
> index sort attr?, base pk, base sk?}` — real DynamoDB's own GSI cursor
> shape — because the hidden table's engine key is `escape(ihash) ||
> escape(isort)? || escape(base_pk) || base_sk` (§3's `gsi_row_key`), not the
> base table's key, so resuming needs the *whole* row key, not just the
> index's own half of it. An LSI `Scan`'s cursor is `{alt-sort attr, base pk,
> base sk?}` for the parallel reason. Both attribute sets are always present
> in a stored index row regardless of its declared projection (`ALL`/
> `KEYS_ONLY`/`INCLUDE` all keep the key attributes — §2's `projected_item`),
> so building either cursor never needs a base-table read-back. This is a
> genuinely different shape from a base `Scan`'s cursor (`{pk, sk?}`) — an
> index `Scan`'s `LastEvaluatedKey` is *not* interchangeable with a base
> `Scan`'s, by design, matching DynamoDB itself.
>
> **A GSI `Scan` reuses the base `Scan`'s own pagination machinery
> unmodified** (`animusd::dynamo::paginated_table_examine`, factored out of
> the base path's loop) — fanning across the hidden table's *own* tablets via
> the ordinary `cp_scan`, no new CP primitive. **An LSI `Scan` needed one**:
> unlike an LSI `Query` (scoped to one base partition, hence one tablet by
> construction), a `Scan` must sweep the *whole* base table's ring, so
> `ClientCtx::cp_scan_kind_table` is `cp_scan`'s per-table fan-out generalized
> to a kind scope — identical range math, one `KindScan` per overlapping
> tablet instead of one base `Scan` request per tablet.
>
> **`end: None` (unbounded above) on a kind-scoped scan is new, and it is a
> real primitive change, not a convenience.** §3 already establishes that no
> discriminator rides inside a kind-scoped logical key, so a scan of it was
> always going to need *some* upper bound; before this, every caller
> (an LSI `Query`, the GSI drain's own `pending_changes`) supplied one that
> was always finite by construction. A table-wide LSI `Scan`'s fan-out has no
> such luxury for its tail tablet: no finite byte string can bound an LSI
> row's keyspace in general, because its trailing base-sort-key segment has
> no length limit (the same reason `StorageScope::physical_bounds` exists at
> all for the *base* scope, §4a's `local_scan`/`linearizable_scan`). The fix
> mirrors that existing precedent exactly rather than inventing a new one:
> `RaftKvNode::local_scan_kind`/`linearizable_scan_kind` changed from a
> mandatory `end: &[u8]` to `end: Option<&[u8]>`, deriving the bound from
> **the kind scope's own** `physical_bounds()` when the caller has none to
> give — never from the caller, and never a whole-engine `entries()` scan, so
> it stays confined to this one scope of this one tablet exactly as before.
>
> **Filtering an interleaved-but-foreign row without spending a `Limit`
> slot.** §3 places every LSI's rows in one shared `KIND_LSI` scope per
> partition, sorted by index name ahead of the alt-sort value
> (`lsi_index_prefix`) — fine for a `Query`, which already narrows to one
> index's own sub-prefix, but a table-wide `Scan`'s per-tablet fetch window
> necessarily crosses every index sharing that space. `run_lsi_scan` filters
> each raw row to the requested index by its own key (`parse_lsi_row_key`)
> and, on a miss, skips it **without counting it toward `Limit`** — the exact
> windowed-continuation discipline the base `Scan` already uses to skip a
> DynamoDB delete tombstone, generalized (`paginated_kind_examine`, the kind-
> scoped twin of the base path's `paginated_table_examine`) rather than
> reimplemented.
>
> `ConsistentRead: true` against a GSI `Scan` is rejected exactly like a GSI
> `Query`; against an LSI `Scan` (and the base table's own) it is accepted,
> also exactly like `Query`. Regression: `animus-dynamo`'s `wire` unit tests
> (decode) and `animusd/tests/dynamo_index_scan.rs` (end to end — pagination
> draining every row, the `ConsistentRead` matrix, an LSI `Scan`'s
> no-cross-index-leakage issued through every node of the cluster in turn,
> and a `FilterExpression` over LSI-scanned rows).
>
> **As-built amendment (2026-08-16, per-tablet limit on `KindScan`).** Until
> now, `ClientCtx::cp_scan_kind_table`'s per-tablet fan-out did *not* thread
> `Limit` into each tablet's own `KindScan` the way its base-scope sibling
> `cp_scan` threads it into each `Scan` — it fetched every overlapping
> tablet's whole matching sub-range and truncated only once, in the
> coordinator, after every tablet's reply was already in hand. `cp_scan_kind_table`
> now computes `remaining` per tablet across the fan-out exactly as `cp_scan`
> does, `ClientRequest::KindScan` gained a `#[serde(default)] limit:
> Option<usize>` field (so an older peer's un-limited `KindScan` still
> decodes), and `RaftKvNode::local_scan_kind`/`linearizable_scan_kind` gained
> the matching `limit: Option<usize>` parameter, truncating **after** the
> intent-drop filter — the identical ordering `local_scan`'s own `limit`
> already uses, so a still-`Pending` row interleaved in the requested range
> can never silently consume one of the caller's requested slots.
>
> **This is a per-tablet limit, not pushdown, and the wording matters**:
> `StorageEngine::scan` has no limit parameter of its own, so a tablet still
> reads its **whole** matching `[start, end)` sub-range off the engine
> exactly as before — the change saves wire payload size and coordinator-side
> memory for a table-wide `Scan` whose per-tablet share vastly exceeds what
> the caller's own `Limit` still needs, never engine I/O. An LSI `Query`
> (`ClientCtx::cp_scan_kind`, single-tablet) is unaffected and still passes no
> limit at all — it has no `Limit` parameter to begin with (the pre-existing,
> still-open DynamoDB-fidelity gap noted just above). Behavior-preserving by
> construction: regression is `animusd/tests/dynamo_index_scan.rs`'s
> split-table `Limit`-walk (the identical `by-score` pagination proof this
> section's own regression note already describes, run again after splitting
> `events`'s one tablet in two, so the fan-out now spans two tablets and two
> possibly-different group leaders) plus a primitive-level `local_scan_kind`
> case in `animus-cp-data/tests/kind_batch.rs` proving `limit` truncates to
> exactly the requested count.

Adding or dropping an index on a **populated** table (`UpdateTable`, with an
`IndexStatus` lifecycle and a backfill) is **deferred to a follow-up**; indexes
remain declarable at `CreateTable` time, as today. The backfill is the drain
applied to every key rather than one, so it is a reuse of §4, not a new
mechanism.

## Consequences

**Easier.**

- Index data becomes ordinary replicated data: it splits, is reclaimed, is
  scoped, is observable, and survives restarts, with no bespoke code for any
  of it. (It also merged, under ADR 0033, at this ADR's writing; tablets are
  split-only now, ADR 0044.)
- An index query stops costing a base-table read per match, and stops ever
  costing a full base-table scan.
- Because the kinds are physically separated (§3), base reads never traverse
  change-log bytes, trimming the log never rewrites SSTables full of base rows,
  and ADR 0034's auto-split keeps measuring base-data volume rather than log
  churn.
- The largest correctness caveat in the Dynamo adapter — "index entry data is
  edge-local, not replicated" — goes away, along with the `touched_since_backfill`
  race defence that existed only to prop it up.
- The `UpdateTable` backfill, when it lands, is a reuse of the drain.
- **DynamoDB Streams becomes a second consumer of an existing log** rather than a
  new subsystem: the atomic co-write, the discriminator, the consumer loop, the
  durable cursor, and at-least-once-with-dedup are all already paid for here.

**Harder, and knowingly accepted.**

- **GSI reads are eventually consistent.** This is the DynamoDB contract, but it
  is weaker than everything else this codebase serves, and it is the first
  non-linearizable read path in v1 (ADR 0019). Tests asserting index contents
  must use a converged-or-timeout poll, never a fixed-deadline one-shot assert.
- **A new background loop** joins the reconciler, GC, auto-split, and txn-resolver
  loops. It is modelled on the last of these, but it is another thing that must
  make progress under fault injection.
- **A tablet group now owns several storage scopes rather than one.** That is a
  new PR into `animus-cp-data`: sibling scopes sharing the base scope's
  `KeyRange`, a `KvCommand` variant carrying a multi-kind atomic batch, a
  per-entry kind tag in the snapshot image codec, `erase_scope` iterating scopes,
  and kind-scoped scan accessors. Existing `Put`/`PutBatch` keep meaning "the base
  kind", so no existing command, fence, or transaction path moves — but "which
  scope does this key belong to" becomes a question every future data-plane
  feature has to answer, where before there was only one answer.
- **Write amplification is real**: an indexed write carries a footprint and a
  change record even when no index attribute changed, and the record holds both
  images. A later optimization can skip the footprint when the recomputation is
  provably a no-op, but the record must stay for Streams.
- **The log must be trimmed or the base tablet grows without bound.** Choosing a
  non-collapsing log for §4a's sake means trim is load-bearing from day one, not
  a later nicety — a drain that stalls now costs disk as well as index freshness.
- **Index lag needs observability** — an admin surface and a metric for the
  drain's backlog, or an operator has no way to see a drain that has stalled.

## Amendment (2026-08-14, ADR 0042/0043)

§4/§4a's "cursor deferred to Streams" language is now made concrete by ADR
0042/0043: the change log gains a genuine multi-consumer cursor (a
`KIND_CURSOR` row per `(tablet, consumer tag)`, holding a packed-HLC
watermark) and a **min-over-rows** rule for split (and, at the time,
merge) convergence — merge has since been removed entirely, ADR 0044 — in
place of §4's as-built "no cursor, consumption is trim" design, which only
ever had to reason about one consumer (the GSI drain). The GSI drain itself
is reworked to write its own cursor row (tag `"gsi"`) in its own **separate,
trailing** write — never fused into any one partition's own footprint-update
entry — issued only once *every* partition a pass dirtied has had its
footprint update independently confirmed durable. This is what actually
preserves the crash property §4's as-built note relied on (the cursor only
ever covers reconciliations whose footprint actually landed): fusing the
cursor bump into a single partition's own commit would be sound for a tick
that reconciles exactly one partition, but unsound the moment one tick
dirties more than one (the ordinary case under sustained writes) — a crash
between two partitions' own footprint-confirming writes would still leave
the cursor naming the tick's max HLC, over-claiming a partition whose
footprint never landed. Trim becomes "the minimum watermark over every
*expected, present*
consumer tag," generalizing §4a's "trimmed behind the slowest consumer"
language from "the GSI drain alone" to any combination of a GSI and/or a
stream. §4a's own prediction — *"Streams becomes a second consumer of the
same log"* — is exactly what ships: no change to the record format, the
per-partition HLC ordering, or the atomic co-write this ADR established:
ADR 0042/0043 only had to add the multi-consumer cursor/trim machinery on
top.

## Amendment (2026-08-16, ADR 0046 U3 — evaluate-at-leader)

§2/§4's write path (`kind_writes_for_item`, unchanged in mechanism) used to
be *evaluated* at whichever edge node received the request
(`index_aware_write`, now deleted): each node read the item's prior value
and diffed the LSI/change-record image locally, serialized only by a
**node-local** `ctx.data().rmw_lock`. Two edge nodes writing the same item
never contended on the same lock, so both could read → diff against the
same stale prior value; the loser's stale LSI row orphaned forever (nothing
reconciles a stale LSI row once written — only the GSI drain, being a full
re-derivation, self-heals) and a stream's `OLD_IMAGE` fidelity went stale
the same way. This is Fork U's decision, recorded in ADR 0046 ("the tablet
log model," draft PR #222 as of this writing — not yet merged, referenced
here in prose only): **U3, evaluate-at-leader**. The edge now forwards a
logical `ClientRequest::KindWriteItem { table, pk, sk, op, condition }` to
the item's own tablet leader (`ClientCtx::cp_kind_write_item`, zero hops if
local); `dynamo::kind_write_item_at_leader` reads `old`, evaluates
`condition`, computes `new`, and *then* calls `kind_writes_for_item` —
identical diff logic, just moved onto the node every write of this item now
actually reaches, which is what makes `rmw_lock` **there** a real
cross-node serialization point instead of a per-node one. `UpdateItem`'s
base-value read-modify-write folds into the same mechanism
(`KindWriteOp::Update`), closing an identical, previously unguarded
lost-update hazard on its own base value. A `KindBatch.conditions` OCC
seatbelt (new in this amendment's companion PR1, `animus-cp-data` codec
v15) covers the one gap the leader-side lock alone can't: a transaction
resolver's recovery push, which never takes `rmw_lock` — unreachable today
(a transaction is rejected outright on an indexed/streamed table) but real
once that restriction lifts.

**Named gap, deliberately not closed here**: a *plain* (unindexed,
unstreamed) table's `PutItem`/`DeleteItem` `ConditionExpression`,
`UpdateItem`'s base value on such a table, and CQL's own read-modify-write
(`cql.rs`) all still rely on nothing but the edge-local `rmw_lock` — there
is no tablet-log hook for a bare `cp_write` to evaluate against, and this
amendment's mechanism is specific to the `KindBatch` path. Closing it would
need the identical evaluate-at-leader treatment extended to `cp_write`
itself, not attempted here.

## Amendment (2026-08-15, ADR 0045)

§5's own deferral — "Adding or dropping an index on a **populated** table
… is deferred to a follow-up … The backfill is the drain applied to every
key rather than one, so it is a reuse of §4, not a new mechanism" — is now
closed, and closed exactly as predicted. ADR 0045 adds an `IndexStatus`
lifecycle (`Creating`/`Active`/`Deleting`) to `IndexDef`, a leader-local
**backfill seeder** arm of the same `change_consumer_loop` this ADR's §4
drain already runs in, and a control-leader completion aggregator copying
`stream_shards`' per-tablet-catalog-row shape (ADR 0042/0043). The seeder
seeds one synthetic, no-content change-log record per pre-existing
partition so §4's drain reconciles it exactly as it would a live write's —
no new write path into `reconcile_partition`, confirming §4a's own framing
that Streams and now backfill are both "another consumer/producer of the
same log," never a parallel mechanism. `UpdateTable`'s
`GlobalSecondaryIndexUpdates` is the wire surface; the four-step drop
cascade is `drop_table`'s own GSI cascade (this ADR's "as-built corrective
note" above) generalized to one index instead of every one. See ADR 0045
for the full design, its Fork A/B/C/D decisions, and its own deviations-from-
AWS table.
