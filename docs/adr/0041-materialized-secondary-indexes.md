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
  (ADR 0028/0034), does not merge (ADR 0033), is not reclaimed on drop (ADR 0024),
  is not scoped (ADR 0028), and does not appear in the dashboard.

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
auto-split (ADR 0034), merge (ADR 0033), drop-table GC (ADR 0024), `StorageScope`
isolation (ADR 0028), the native linearizable range scan, and the dashboard.

Its tablets are provisioned by the existing lazy path
(`ClientCtx::provision_tablet`), on the same terms as any user table.

`$` is not a legal character in a DynamoDB table or index name
(`[A-Za-z0-9_.-]`) nor in a CQL identifier, so the name cannot collide with a
user table — but this is **enforced, not assumed**: `Metadata::apply`'s
`CreateTableSchema` arm rejects any user table name containing `$`, alongside the
existing `syskv::is_reserved_name` gate. Index tables get no `TableSchema` entry
of their own; the authoritative shape stays the base table's `IndexDef`.

### 2. An LSI is colocated in the base table's tablets

A local secondary index hashes by the base partition key, so its rows share the
base row's **token** and therefore its tablet. We place them there, which makes
base row and LSI rows one `ClientRequest::PutBatch` — **one Raft log entry, one
commit round, one apply**. LSI maintenance is thus atomic with the base write and
strongly consistent, with no intent, no drain, and no 2PC.

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
`narrow_scope` and a merge's `widen_scope` move every kind in one call. One
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

The log is trimmed behind the **slowest cursor** — today the GSI drain alone,
which is also what bounds its growth. The Streams ADR extends that policy with a
retention window and per-consumer iterators; the **shards, iterators,
`StreamViewType` projections, and the wire surface are all deferred to it**. Only
the record format, the ordering, and the trim are settled here.

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
  reconciliation (`sync_indexes`).

Adding or dropping an index on a **populated** table (`UpdateTable`, with an
`IndexStatus` lifecycle and a backfill) is **deferred to a follow-up**; indexes
remain declarable at `CreateTable` time, as today. The backfill is the drain
applied to every key rather than one, so it is a reuse of §4, not a new
mechanism.

## Consequences

**Easier.**

- Index data becomes ordinary replicated data: it splits, merges, is reclaimed,
  is scoped, is observable, and survives restarts, with no bespoke code for any
  of it.
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
