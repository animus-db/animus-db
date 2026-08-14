# ADR 0042 — DynamoDB Streams: semantics, API, and the change-log lifecycle

- **Status:** Proposed
- **Date:** 2026-08-14
- **Amends:** [ADR 0013](0013-replicated-schemas.md) (a `StreamSpecification`
  replicates in the catalog alongside a table's schema and index defs),
  [ADR 0024](0024-drop-table-data-gc.md) (drop cascades to a table's hidden
  stream table too), [ADR 0028](0028-shared-storage-single-command-split.md)
  (the kind-scope set a tablet group owns grows again, `KIND_CURSOR`),
  [ADR 0033](0033-tablet-merge.md) and
  [ADR 0034](0034-byte-based-auto-split.md) (a stream table's tablets are
  exempt from merge and auto-split — see [ADR 0043](0043-stream-shard-subsystem.md)),
  [ADR 0035](0035-control-plane-separate-deployment.md) (a third,
  dedicated **streams** role joins the control/data assemblies — see ADR
  0043), [ADR 0041](0041-materialized-secondary-indexes.md) (§4/§4a's
  "cursor deferred to Streams" language is now specified here; the change log
  ADR 0041 built for the GSI drain becomes a second consumer's substrate,
  exactly as §4a predicted).
- **Depends on:** [ADR 0018](0018-cross-tablet-transactions.md) (HLC
  timestamps — a cursor watermark and a shard position are both built on the
  same monotonic apply-time `ts`), [ADR 0043](0043-stream-shard-subsystem.md)
  (the stream-shard subsystem this ADR's contract is served from).

## Context

ADR 0041 gave every base tablet of an indexed table a durable, per-tablet,
HLC-ordered **change log** (`KIND_CHANGE`): one record per mutation, carrying
old and new item images, trimmed behind whichever consumer is furthest
behind. §4a of that ADR named the plan explicitly: *"Streams becomes a
second consumer of the same log... Only the record format, the ordering, and
the trim are settled [t]here"* — shards, iterators, view-type projection,
the wire surface, and a real multi-consumer trim policy were deferred.

This ADR specifies that second consumer: DynamoDB Streams, AWS's own
change-data-capture API for a table (`GetRecords`/`GetShardIterator`/
`DescribeStream`/`ListStreams`, the `DynamoDBStreams_20120810` service). It
also finishes the change-log lifecycle ADR 0041 left as "cursor rebuilt at
group start" for a single consumer (the GSI drain) into a real multi-consumer
design, since a table can now have a GSI, a stream, both, or neither, and the
log must be trimmed correctly in every combination.

DynamoDB's own contract, verified against AWS's documentation, is the
compatibility target:

- **Per-item ordering is an explicit guarantee**: *"for each item that is
  modified... the stream records appear in the same sequence as the actual
  modifications to the item."* There is **no cross-item or cross-shard
  ordering guarantee** — two different items' records may arrive in any
  relative order, even across shards.
- **Parent-before-child processing is the documented lineage discipline**: a
  consumer (Lambda, KCL) must finish a shard before its child shards, because
  an item's records can span a shard boundary when the table resizes. A
  closed shard has an `EndingSequenceNumber` and eventually drains to a null
  `NextShardIterator`.
- **Items are nailed to a shard** by `(table, partition key)` and only move
  via an explicit *lineage event* (a resharding), never freely.
- DynamoDB Streams is architecturally a **separate, Kinesis-shaped service**
  populated from the table's own replication log — not a queryable view of
  the table itself.
- The contract is **eventually consistent** by design, and **exactly-once
  within the stream** — no duplicate records for one write.
- **Pricing is consumer-side only** (`GetRecords` calls); writes never pay
  for a stream that happens to be enabled, matching the base-write-latency
  argument ADR 0041 already made for GSIs.

## Decision

**Streams read the same durable per-tablet change log ADR 0041 introduced,
through a new consumer — the stream copier — that forwards records into a
small number of dedicated, fixed-range **stream-shard tablets** (ADR 0043).
`GetRecords` serves directly off a shard's own applied state, with no
`ReadIndex` barrier. The change-log lifecycle becomes genuinely
multi-consumer: an HLC-watermark **cursor row** per (tablet, consumer), and a
**min-over-rows** rule that makes split/merge convergence a property of the
scan rather than an event handler.**

### 1. The consumer contract

**Per-item ordering.** A shard is a pure function of the item's partition
key (ADR 0043 §1): every record for one partition key lands in exactly one
shard (until a lineage event, ADR roadmap item (a) below), in the same order
the copier read them off the source tablet's own change log — which is HLC
commit order, since the log is append-only and HLC-ordered per partition
(ADR 0041 §4a). Per-item order is therefore preserved by construction, not
enforced by a special case.

**No cross-item or cross-shard ordering.** Two different partition keys can
land in the same shard (many-to-one) or different shards; nothing here
orders them relative to each other, matching AWS exactly. A consumer that
needs cross-item causality must derive it itself, as it must against real
DynamoDB.

**Eventual consistency via copier lag.** The copier (ADR 0043 §4) drains a
source tablet's change log into shard tablets on its own schedule, gated by
leadership and bounded batch size. A `GetRecords` caller sees exactly what
the copier has forwarded and the shard leader has applied — this *is*
AWS's own model (Streams always lags the table's replication log by some
bounded interval); there is no stronger guarantee to give here, and giving
one would misrepresent the adapter as more synchronous than the service it
adapts.

**Exactly-once in the log.** A record is admitted to a shard iff its
`source_hlc` strictly exceeds the last-admitted HLC for that partition key
(ADR 0043 §5's dedupe row) — simultaneously the ordering guard (a smaller or
equal `source_hlc` can only be a retried, already-forwarded batch) and the
idempotency guard for a copier retry after a lost acknowledgement. No
`(pk, source_hlc)` pair is ever recorded twice.

### 2. API surface

Streams share the existing DynamoDB HTTP listener, dispatched by
`X-Amz-Target: DynamoDBStreams_20120810.*`:

| Operation | Served by | Notes |
|---|---|---|
| `ListStreams` | any node | Pure function of the replicated catalog: one entry per table with an active `StreamSpec`. |
| `DescribeStream` | any node | Pure function of `(StreamSpec, tablets_for_table(stream_table))` — no stream-specific state in `Metadata` beyond the `StreamSpec` itself (ADR 0043 §6). |
| `GetShardIterator` | the shard leader (for `LATEST`/`AT_SEQUENCE_NUMBER`/`AFTER_SEQUENCE_NUMBER`); pure otherwise (`TRIM_HORIZON` needs the durable marker, also leader-served) | Mints a stateless position token. |
| `GetRecords` | the shard leader | Leader-local applied-state read — §7 below. |

Enablement rides the existing DynamoDB table operations, not a separate
Streams-only endpoint:

- **`CreateTable` with `StreamSpecification`** (`StreamEnabled`,
  `StreamViewType`) enables a stream at table creation.
- **`UpdateTable` with only a `StreamSpecification`** (no key/throughput/
  index changes) enables, disables, or changes the view type of an existing
  table's stream. This is the *only* shape of `UpdateTable` this adapter
  accepts today (an index-adding `UpdateTable` remains a named follow-up,
  ADR 0041 §5's own deferred item, orthogonal to this one).
- **`DescribeTable`** gains a minimal `StreamSpecification` +
  `LatestStreamArn`/`LatestStreamLabel` in its response, reusing the
  replicated `StreamSpec`.

### 3. View types: both images stored, projected at read

A shard record's stored form always carries **both** the old and new item
images (ADR 0043's `KIND_STREAM` record, `{source_key, source_hlc,
ChangeRecord}}` — `ChangeRecord` is the same type ADR 0041 introduced for the
GSI drain, already carrying both images). `StreamViewType`
(`NEW_IMAGE`/`OLD_IMAGE`/`NEW_AND_OLD_IMAGES`/`KEYS_ONLY`) is a **read-time
projection** applied when a `GetRecords` response is built, never a
storage-time decision. This means changing a stream's `StreamViewType`
(disable + re-enable, since a live view-type change is not a real DynamoDB
operation either) never requires a backfill or a different storage format —
the same principle ADR 0041 §5 used for index projections.

### 4. ARN / label model

A stream's identity is `(table, label)`, where `label` is minted fresh every
time a stream is **enabled** (including re-enabling after a disable — see §9)
and never reused. The ARN surfaced to clients is a synthetic but
DynamoDB-shaped string embedding the table name and label
(`arn:aws:dynamodb:<region>:<account>:table/<table>/stream/<label>`, region/
account are fixed placeholder values, matching this adapter's existing
DynamoDB ARN conventions elsewhere). `DescribeStream`/`GetRecords`/
`GetShardIterator` all validate the label embedded in their request's stream
ARN against the table's *current* `StreamSpec.label` — a mismatch (a stale
ARN from a disabled-then-re-enabled stream) is `ResourceNotFoundException`,
never silently served against the new stream. This is what makes disable-then-
re-enable "mint a new label ⇒ a new, empty stream" (§9) a real guarantee
rather than a naming convention a client could paper over.

### 5. Sequence numbers: per-shard gapless positions

A DynamoDB sequence number is, here, the **decimal string encoding of a
shard-local `u64` position** — apply-assigned, strictly increasing, and
gapless (the position counter advances only when a record is actually
admitted; see ADR 0043 §5). This is a deliberate, documented deviation from
real DynamoDB's own (unspecified, opaque) sequence-number format, chosen
because it is directly corpus-checkable (ADR 0043 §8's completeness checker
asserts gaplessness from `0`) and because every request shape
(`AT_SEQUENCE_NUMBER`/`AFTER_SEQUENCE_NUMBER`) maps onto it with no
translation layer:

- `AT_SEQUENCE_NUMBER(n)` → serve from position `n`.
- `AFTER_SEQUENCE_NUMBER(n)` → serve from position `n + 1`.
- `TRIM_HORIZON` → the shard's durable `trim_horizon` marker (the oldest
  position retention has not yet reclaimed).
- `LATEST` → the shard's `next_position` at the moment the iterator is
  minted (i.e. "nothing yet, wait for new records").

**Positions are apply-assigned in arrival order at the shard, not keyed by
the record's own source HLC.** This is the one place this ADR deviates from
"HLC everywhere" on purpose: source tablets copy toward a shard at
independent lags, so a record from a tablet that is briefly behind would, if
positions were source-HLC-keyed, need to insert *below* a position an
iterator has already consumed past — a silent loss no retry can repair. A
monotonic, shard-local admission counter has no such hazard: it only ever
grows, so "already past this position" is a permanent, correct fact the
instant it becomes true. See ADR 0043 §7 for the rejected source-HLC-keyed
alternative in full.

### 6. Iterators: stateless position tokens, no expiry

A shard iterator is an opaque, stateless token — `base64({label, shard
generation, shard index, position})` — carrying everything needed to resume
a poll with no server-side session state. Real DynamoDB shard iterators
expire after 15 minutes; **this adapter's do not** (a documented deviation).
There is no operational reason to expire a token that costs nothing to keep
valid (unlike Kinesis, there is no separate iterator-store service whose
capacity this would protect), and expiring iterators would only add a
failure mode a compatible client already has to handle defensively but
would rarely actually exercise here. The generation field (reserved from day
one, always `0` until the roadmap item (a) below ships) is what lets a
future resharding mint iterators whose identity survives a generation cut
without a wire-format break.

### 7. The leader-local `GetRecords` read: safety and a warning

**`GetRecords` is served from the shard leader's own locally-applied state,
with no `ReadIndex` read-barrier round.** This is a deliberate departure
from every other read path in the CP data plane (ADR 0017), which otherwise
defaults to linearizable ReadIndex reads, and it needs its own safety
argument:

A shard's applied state has three properties together that make this safe:
it is **append-only** (a record, once admitted, is never mutated — only
trimmed, which only ever removes a *prefix*); it is **positional** (an
iterator names an exact position, not "the latest state of key K"); and it
serves only **committed-and-applied** records (`applied ⊆ committed`,
ADR 0017's own invariant — a leader never serves anything it hasn't itself
durably applied). Given all three, the *worst possible* staleness a
leader-local read can produce is a **stale prefix**: a `GetRecords` call
might not yet see a record the shard's Raft group has *committed* but not
yet *applied* locally (a bounded, sub-heartbeat lag, exactly the same gap
`engine_applied` vs. `last_applied` describes elsewhere in this codebase),
but it can never see a record out of order, never see one that never
existed, and never see one too early. That staleness is **indistinguishable
from ordinary copier lag** under the stream's own eventually-consistent
contract (§1) — a client polling `GetRecords` already has to tolerate "not
there yet, poll again," and a leader that is a few milliseconds behind its
own apply queue produces exactly that experience, nothing worse.

**This is deliberate and must never be "upgraded" to a `ReadIndex` barrier.**
Doing so would add a full quorum round-trip to every `GetRecords` poll —
turning what should be a purely local read into the single most expensive
operation in the whole streams path, for a consistency guarantee the
contract in §1 already declines to make. A future contributor tempted to
"fix" an apparent staleness bug here should re-read this section first: the
staleness is the contract, not a defect.

### 8. Change-log lifecycle rework

ADR 0041 §4's as-built note settled "no cursor, consumption is trim" for a
**single** consumer (the GSI drain). A table can now have a GSI, a stream,
both, or neither, so the log needs a real multi-consumer cursor design:

**Cursor semantics.** A cursor row's value is a **packed-HLC watermark
`W`**: *"every change record this tablet has applied with `hlc <= W` is
fully consumed by this tag."* This is sound because of a hard invariant this
codebase already asserts at apply (`assert_ts_monotonic`, `animus-cp-data`):
every change record a tablet group ever applies has a strictly greater `ts`
than every one applied before it. A scalar watermark is therefore a
complete, unambiguous cursor with no positional bookkeeping needed — in
sharp contrast to the change log's own **key** order, which is
token-then-pk-then-HLC, not global commit order (`pending_changes`'s own
documented shape), and is exactly why a *positional* cursor would not work
here. Two tags exist today: `"gsi"` (the GSI drain's own reconcile cursor,
reworked to write this row atomically with its footprint update) and
`"copier"` (the stream copier's, landing with ADR 0043). See ADR 0043's
`cursor` module for the row's key/value encoding and disjointness proof.

**The min-over-rows rule.** A tablet's own `KIND_CURSOR` scope can, after a
merge, hold more than one row for the same tag — one per absorbed tablet's
own lineage, physically present on the shared engine the whole time
(`StorageScope::with_kind` shares one live `KeyRange` across every kind, so
widening a survivor's scope exposes rows a sibling wrote while it was its
own tablet). The **effective watermark for a tag is the minimum over every
row of that tag** in the tablet's own `KIND_CURSOR` scope, not just the row
this exact tablet identity most recently wrote:

- **Split**: the retained (left) child keeps the same `range.start` as its
  parent, so it continues to read/write the *same* cursor row — nothing
  changes. The new right child starts with **no** row for a range it has
  never been the subject of a cursor update on; the min-over-rows rule (over
  an *empty* set) reads as `W = 0` — no watermark, so it re-copies its own
  slice of not-yet-trimmed records from scratch. Any overlap this produces
  (a record the left child's cursor already covered but the right child's
  fresh copy re-sends) is absorbed downstream: the shard's own dedupe row
  (ADR 0043 §5) rejects the duplicate, and the GSI drain's reconciliation is
  already idempotent by construction (ADR 0041 §4).
- **Merge**: the absorbed sibling's own cursor row, for the *same* tag,
  survives physically on the shared engine and becomes visible the instant
  the survivor's scope widens over it. Taking the **minimum** across both
  rows is the whole point: if the survivor instead trusted only its own
  (typically higher, since it was presumably a busier tablet) watermark, it
  would silently claim records the absorbed sibling's own copier/drain had
  never actually forwarded — the one genuine **data-loss hazard** this rule
  exists to close, and the scenario ADR 0043's split/merge corpus is
  specifically built to catch (a survivor's own higher row, trusted alone,
  is proven to under-cover in that corpus).

**Trim = min over *present* consumers, no separate low-water mark.** Which
tags are *expected* on a tablet is derived from the table's own schema: a
GSI-bearing table expects a `"gsi"` row; a streamed table expects a
`"copier"` row; neither, both, or one. An **expected tag with no row at
all** reads as `W = 0`, which blocks trim entirely (never trim past a
consumer that hasn't started) — the same safe default the min-over-rows
rule already produces for a fresh split child. The janitor (folded into the
unified change-consumer loop, ADR 0043 §4) deletes change records
`hlc <= min(watermark over every expected, present tag)` in bounded
`KindBatch` batches. There is no separate trim low-water marker to keep in
sync with the cursor rows — trim is a pure function of "what does this
table's current schema expect, and what do those rows currently say,"
recomputed every janitor tick.

**Transitions.** Enabling a stream means records start flowing on the next
write visible through the replicated catalog; the copier begins copying on
its own next tick, and the expected-tag rule holds trim back until its first
cursor row lands (so nothing is trimmed out from under a copier that simply
hasn't started yet). Disabling stops the copier; its now-stale `"copier"`
row is no longer *expected* (the schema no longer names a stream), so trim
proceeds on the GSI's row alone (or unconditionally, if there is no GSI
either); the stale row itself is erased along with the stream table's own
tablets (§9). A one-line janitor cleanup also drops any row tagged with a
consumer no longer expected at all, so a disabled-then-dropped stream never
leaves a permanent, silently-ignored row behind.

### 9. Enable / disable / drop semantics

- **Enable**: committing `SetTableStream{Some(spec)}` mints a fresh `label`;
  `CreateStreamShards` then provisions the fixed shard tablets (ADR 0043
  §6). A crash between the two is repaired by the copier's own lazy
  backstop re-proposing `CreateStreamShards` idempotently on its next tick
  (mirroring the GSI drain's own lazy hidden-table provisioning, ADR 0041
  §4's as-built note). There is **no backfill**: only records written after
  enablement ever appear, matching DynamoDB's own behavior.
- **Disable (v1 semantics, owner-decided)**: `SetTableStream{None}` followed
  by dropping the stream table's tablets outright (ADR 0024's reclaim GC).
  Every existing iterator and ARN for that stream dies immediately —
  `ResourceNotFoundException` on next use, by the label-mismatch check in
  §4, not by any grace period. Re-enabling mints a fresh label, hence a
  genuinely new, empty stream; there is no way to "resume" a disabled
  stream's old records. This is a deliberate, documented simplification of
  AWS's own behavior — see the committed roadmap below.
- **Drop table**: the drop cascade (ADR 0024, already extended once for GSI
  hidden tables by ADR 0041 §5) extends again: a streamed table's hidden
  stream table's tablets drop in the same enumerable first step as its GSI
  hidden tables, and the second, tablet-map-keyed sweep learns the stream
  table's naming shape (`is_stream_table_name`) so a copier-raced lazy
  `CreateStreamShards` can never orphan a stream table across a drop, the
  same belt-and-suspenders argument ADR 0041 §5 already made for a
  GSI's hidden table.

### 10. Deviations from AWS, summarized

| Area | Real DynamoDB Streams | This adapter | Why |
|---|---|---|---|
| Shard count | Elastic, grows automatically with throughput | Fixed at enable (`--stream-shards`, default 1); manual/automatic growth is roadmap item (a) | Simpler v1; growth-compatible by construction (ADR 0043 §3) |
| Sequence numbers | Opaque, AWS-internal format | Decimal string of a gapless `u64` position | Corpus-checkable, no translation layer |
| Shard iterators | Expire after 15 minutes | Never expire | No session-store capacity to protect; see §6 |
| `GetRecords` consistency | Documented as eventually consistent | Leader-local applied-state read, no barrier | Same observable contract, cheaper — see §7 |
| Post-disable readability | Records remain readable for ~24h after disable | Immediate teardown | v1 simplification; roadmap item (b) |
| Resharding | Automatic, throughput-driven, adjacent-parent-aware (Kinesis lineage) | Manual only in v1; generation-cut, doubling-only when it lands (roadmap item (a)) | Simpler mechanism sufficient for a single-parent DynamoDB Shard API |

### 11. Committed roadmap

These are **planned work this ADR commits to**, not open-ended follow-ups —
the mechanism for each is already designed, only sequenced out of this
initial stack for reviewability.

**(a) Automatic shard growth via generation-cut resharding.** DynamoDB's own
`Shard` API carries a single `ParentShardId` field — unlike Kinesis, a child
shard has *exactly one* parent, never two adjacent ones merging. Growth here
is therefore **generation-cut, doubling-only** (`N → 2N`, never an arbitrary
reshape): close the *entire* current shard generation at once, open a fresh
generation with twice as many shards, and let the closed generation's
records drain out via ordinary retention (§9's ~24h target, roadmap item
(b)) rather than migrating any data. Each closed shard's `EndingSequenceNumber`
is its last admitted position; each new shard's single `ParentShardId` names
the one old shard whose token sub-range it was carved from. The **per-copier
discipline** that makes this safe without migrating any dedupe state:
finish and acknowledge the in-flight batch under the *old* shard map first,
durably advance the source tablet's cursor, and only then switch to routing
by the *new* map — never straddle a generation cut mid-batch. Manual
admin-triggered growth ships first (an explicit "double this stream's
shards" action); throughput-based automatic triggering (mirroring ADR 0034's
byte-based auto-split trigger) follows once the manual path is proven.
Operationally: **grow rarely, grow by 2×** — this is not a fine-grained
elastic-scaling mechanism, and is not meant to be one.

**(b) AWS-faithful ~24h post-disable readability.** Real DynamoDB keeps a
disabled stream's records readable for roughly 24 hours before reclaiming
them. A future PR replaces the immediate-teardown disable (§9) with: mark
the stream disabled (no new copier writes, `ListStreams`/new
`GetShardIterator(LATEST)` stop advertising it) but leave the shard tablets
and their existing records in place; a janitor drops the shard table once
every record has aged out past the retention window, mirroring the shard
retention mechanism ADR 0043 already builds for ordinary trim.

### 12. Named follow-ups (not part of the committed roadmap)

- **`TxnStage` kind-writes**: lifts ADR 0041 §2's `TransactWriteItems`
  rejection on an indexed *or streamed* table, once the transaction
  machinery gains a multi-kind atomic write extension.
- **CQL CDC**: a change-data-capture surface for the CQL adapter over the
  same underlying log/shard machinery.
- **Follower-served `GetRecords`**: relaxing §7's leader-only restriction,
  once a bounded-staleness follower read primitive exists generally.
- **KCL-fidelity extras**: the finer points of AWS's Kinesis Client Library
  contract (checkpointing conventions, lease-table shapes) beyond the raw
  API surface specified here.

Explicitly **not** adopted, per owner decision: serving-tier extraction and
S3 archival of stream records. Both were considered and declined as out of
scope for this adapter.

## Consequences

**Easier.**

- Streams become a real second consumer of an already-durable, already-
  ordered log, exactly as ADR 0041 §4a predicted — no new write-path
  machinery, no new consistency model to invent.
- The change-log lifecycle becomes genuinely correct for *any* combination
  of GSI/stream/neither on a table, closing a gap ADR 0041 left implicit
  (a single-consumer cursor design that never had to answer "what if there
  are two").
- `GetRecords`'s leader-local read keeps the hottest path in this whole
  feature cheap — no quorum round per poll.

**Harder, and knowingly accepted.**

- **A second background loop role** (the copier) joins the GSI drain inside
  one unified change-consumer loop, per tablet this node leads — more that
  must make progress under fault injection, though it inherits the same
  proven shape.
- **The min-over-rows rule is subtle** and, done wrong, is a silent
  data-loss hazard (a survivor trusting only its own watermark). The
  split/merge corpus (ADR 0043 §8) exists specifically because this is easy
  to get backwards.
- **Fixed shard counts mean a hot single-partition-key stream cannot scale
  out its own consumption** until resharding (roadmap item (a)) ships;
  today's default of one shard is a deliberate v1 simplification, not a
  performance recommendation for every workload.
- **Immediate disable teardown** is a real behavioral gap versus AWS until
  roadmap item (b) lands — a client that relies on post-disable readability
  today will observe `ResourceNotFoundException` instead.
