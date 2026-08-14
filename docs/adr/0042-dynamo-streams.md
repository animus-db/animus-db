# ADR 0042 — DynamoDB Streams: semantics, API, and the change-log lifecycle

- **Status:** Accepted — implemented (round-3 stack). PR map: PR0 salvage →
  PR1 this ADR + ADR 0043 rewrite → PR2 `SegmentStore` trait/`Sim`/`Fs` →
  PR3 `ClusterSegmentStore` → PR4 segment codec + shard catalog + merge
  guard → PR5 the sealer → PR6 read path + wire API → PR7 segment janitor →
  PR8 lineage corpus + `ProdEnv` e2e + nightly (this PR).
- **Date:** 2026-08-14 (round-3 rewrite; supersedes this ADR's own round-2
  text in place — see [ADR 0043](0043-stream-shard-subsystem.md)'s Context
  for the round 1→2→3 fork history. The round-2 text is retrievable from git
  history and from the `adr-0042/5`/`adr-0042/6` archive branches, kept
  specifically so that history stays recoverable.)
- **Amends:** [ADR 0013](0013-replicated-schemas.md) (a `StreamSpecification`
  replicates in the catalog alongside a table's schema and index defs),
  [ADR 0024](0024-drop-table-data-gc.md) (drop cascades to a streamed
  table's segment catalog rows and objects — there is no hidden stream table
  to drop), [ADR 0028](0028-shared-storage-single-command-split.md) (the
  kind-scope set stays at five; streams add **no** new kind — the hot shard
  *is* the existing `KIND_CHANGE` scope), [ADR 0033](0033-tablet-merge.md)
  (`MergeTablets` is rejected on a streamed **base** table — an explicit v1
  stopgap, not a permanent exemption for a separate shard table, since no
  such table exists), [ADR 0034](0034-byte-based-auto-split.md) (auto-split
  is what creates every stream shard boundary past epoch 0 — token-aligned
  split keys on a streamed table, F11), [ADR 0035](0035-control-plane-separate-deployment.md)
  (round 3 needs no dedicated streams node role — see that ADR's amendment,
  now a superseded note), [ADR 0041](0041-materialized-secondary-indexes.md)
  (§4/§4a's "cursor deferred to Streams" language is specified here; the
  change log ADR 0041 built for the GSI drain becomes, unmodified, the
  literal storage for a stream's hot shard — not a second consumer copying
  it elsewhere, as round 2 had it).
- **Depends on:** [ADR 0018](0018-cross-tablet-transactions.md) (HLC
  timestamps — a stream's `SequenceNumber` is the same packed HLC the MVCC
  version and a GSI cursor watermark already use), [ADR 0043](0043-stream-shard-subsystem.md)
  (in-place sealing, the segment catalog, and the `SegmentStore` this ADR's
  contract is served through).

## Context

ADR 0041 gave every base tablet of an indexed table a durable, per-tablet,
HLC-ordered **change log** (`KIND_CHANGE`): one record per mutation, carrying
old and new item images, trimmed behind whichever consumer is furthest
behind. §4a of that ADR named the plan explicitly: *"Streams becomes a
second consumer of the same log... Only the record format, the ordering, and
the trim are settled [t]here"* — shards, iterators, view-type projection,
the wire surface, and a real multi-consumer trim policy were deferred. This
ADR specifies that: DynamoDB Streams, AWS's own change-data-capture API for a
table (`GetRecords`/`GetShardIterator`/`DescribeStream`/`ListStreams`, the
`DynamoDBStreams_20120810` service).

**This is the third architectural round for this feature**, each one moving
closer to what DynamoDB itself actually does; ADR 0043's Context section
tells that story in full (a separate, dedicated shard subsystem with its own
Raft groups and a copying consumer, replaced by in-place sealing of the
table's own log). This ADR only records the round-3 **decision** — the
consumer-visible contract — not the abandoned intermediate designs.

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
- **DynamoDB partitions split and never merge — verified, and load-bearing
  for this round's design.** AWS's own `Shard` type carries exactly one
  `ParentShardId`, never two; there is no `AdjacentParentShardId` field at
  all (Kinesis, which DynamoDB Streams is modeled on, has one, for its own
  merge-capable shards). Its absence here is not an oversight — it is the
  fossil of DynamoDB's own decision that a table partition never merges
  back into another. This directly licenses F1 below: rejecting tablet
  merge on a streamed table matches what the system being adapted actually
  does, not a workaround this adapter invented.
- DynamoDB Streams is architecturally a **separate, Kinesis-shaped service**
  populated from the table's own replication log — not a queryable view of
  the table itself.
- The contract is **eventually consistent** by design, and **exactly-once
  within the stream** — no duplicate records for one write.
- **Pricing is consumer-side only** (`GetRecords` calls); writes never pay
  for a stream that happens to be enabled, matching the base-write-latency
  argument ADR 0041 already made for GSIs.

## Decision

**A stream's hot tail is the source tablet's own `KIND_CHANGE` change log,
read directly by its leader — no copier, no second Raft group, no dedupe
row. A shard is a *seal epoch*: the tablet's leader seals its own log into
an immutable segment on size/age triggers, opens a fresh epoch, and a
consumer walks a chain of closed shards followed by exactly one open, hot
shard per tablet. `GetRecords` serves a closed shard from the segment store
and an open shard from the leader's own applied state, with no `ReadIndex`
barrier on either. The seal watermark that licenses hot-log trim is a
committed catalog row, never a cursor tag — this closes the last
single-consumer assumption ADR 0041 §4 left in place.**

### 1. The consumer contract

**Per-item ordering.** Every record for one partition key is written by the
same tablet's single Raft group, in the same commit order every other
`KIND_CHANGE` consumer already sees (ADR 0041 §4a) — per-item order is
structural, not a special case a shard boundary could violate: sealing never
reorders, it only draws a line under a prefix of the same append-only log.

**No cross-item or cross-shard ordering.** Two different partition keys can
land in the same tablet (many-to-one, since a shard is per-tablet, not
per-key) or, after a split, in different tablets/shards; nothing here orders
them relative to each other, matching AWS exactly.

**Eventual consistency via seal-and-serve, not copier lag.** Round 2's
consistency story was "the copier hasn't forwarded it yet." Round 3 has no
copier: an open shard's records are visible to `GetRecords` the moment the
tablet's own leader has applied them (bounded by the ordinary
commit-then-apply lag every CP read already tolerates), and a closed
shard's records are visible once the seal's segment `put` and its catalog
row are both durable. The consumer-observable shape is unchanged — a poll
sees "not there yet, poll again" under exactly the same eventually
consistent contract real DynamoDB Streams documents — the mechanism
producing that lag is just structurally simpler.

**Exactly-once, and now structural rather than dedupe-row-enforced.** A
record is admitted to the log exactly once, at apply, by the same
`KvCommand::KindBatch` entry that writes the base row (`animus-cp-data`'s
`change_log` field, completed with `hlc::pack(ts)` at apply — see that
type's own doc). There is no separate admission decision downstream to get
wrong: sealing only ever copies already-durable, already-ordered records
into a segment, and a record's identity is its own position in that log, not
something a copier could re-derive incorrectly on retry. Round 2's
per-partition dedupe row (guarding a copier's own retry) is gone because
there is no longer a second write path for it to guard.

### 2. Shard model: seal epochs (F2)

**A shard is a seal epoch.** Per tablet, there is a chain of **closed**
shards — each one exactly the content of one committed segment object — and
exactly **one open** shard: the hot tail, i.e. whatever the tablet's
`KIND_CHANGE` scope currently holds above the last sealed watermark.
`ShardId` is `shardId-<tabletId>-<epoch>`, epoch counting up from `0` at the
tablet's first seal (or inherited, for a split child — see §"Split lineage,"
ADR 0043 §A4). This is **live from day one**, not a deferred roadmap item as
round 2's fixed-shard-count design had it: a tablet's shard chain grows every
time its own leader seals, with no separate provisioning command and no
shard-count knob to tune.

**`ParentShardId` chains epochs, immediately, using DynamoDB's own single-
parent shape** (the verified fact above — this adapter's lineage graph
literally cannot need `AdjacentParentShardId`, because nothing here merges
either): a routine seal's child shard names the same tablet's own previous
epoch as its parent; a split child's **epoch-0** shard names the parent
*tablet's* own last shard as its parent. A consumer therefore walks the
identical DynamoDB discipline — finish a shard before its named parent's
children — whether the lineage event was an ordinary seal tick or a table
resize, with no adapter-specific case to special-case.

**Sealing never invalidates an open-shard iterator.** `GetShardIterator`/
`GetRecords` resolve a shard id against the segment catalog at *serve* time,
not at mint time: if the shard has since been sealed, the iterator simply
drains whatever the resulting segment holds and then reports a null
`NextShardIterator` naming the tablet's new open shard — exactly the
"closed shard eventually drains to null" contract AWS documents, produced
here as a side effect of resolving lazily rather than a special transition
the iterator format has to encode.

### 3. API surface

Streams share the existing DynamoDB HTTP listener, dispatched by
`X-Amz-Target: DynamoDBStreams_20120810.*`:

| Operation | Served by | Notes |
|---|---|---|
| `ListStreams` | any node | Pure function of the replicated catalog (F7) — the store is never load-bearing for a metadata read. Includes a `DISABLED`-but-unreaped stream during its F12-b grace window. |
| `DescribeStream` | any node | Pure function of `Metadata` (schema + tablet map + segment catalog rows): sealed shards from catalog rows, the open shard per currently-tabled `tablets_for_table` entry (only while `ENABLED`). Paginated (`ExclusiveStartShardId`/`LastEvaluatedShardId`) — a busy tablet churns roughly a shard a seal-age interval. |
| `GetShardIterator` | any node for a closed shard's `TRIM_HORIZON`/`AT`/`AFTER`; the tablet's own leader for `LATEST` on an open shard | Mints a stateless position token; no barrier either way. |
| `GetRecords` | any node (closed shard, via the store) or the tablet's own leader (open shard, local scan) | §7/§8 below — the split in this row is the whole reason `GetRecords` needs two different serve paths in this design. |

Enablement rides the existing DynamoDB table operations, unchanged from
round 2:

- **`CreateTable` with `StreamSpecification`** enables a stream at creation.
- **`UpdateTable` with only a `StreamSpecification`** enables, disables, or
  changes the view type of an existing table's stream — still the *only*
  shape of `UpdateTable` this adapter accepts (an index-adding `UpdateTable`
  stays ADR 0041 §5's own deferred item).
- **`DescribeTable`** carries a minimal `StreamSpecification` +
  `LatestStreamArn`/`LatestStreamLabel`, reusing the replicated `StreamSpec`.

### 4. ARN / label model, and catalog-based resolution (F12-b)

A stream's identity is `(table, label)`; `label` is minted fresh on every
**enable** (including a re-enable after a disable) and never reused. The
synthetic ARN embeds both (`arn:aws:dynamodb:<region>:<account>:table/<table>/stream/<label>`).

**Round 2 resolved a request's ARN against `Metadata`'s *current*
`StreamSpec.label` alone** — a mismatch was unconditionally
`ResourceNotFoundException`. **F12-b changes this**, because a disabled
stream now stays listed and readable for a grace window (§9 below): request
resolution goes through the **segment catalog's own rows for the requested
label**, not just whichever label the table's live schema currently names.
A label with at least one live (unexpired) catalog row is valid, whether or
not it is the table's *current* stream — this is what lets a just-disabled
stream and a freshly re-enabled one coexist in `ListStreams`/`DescribeStream`
during the grace window (§9). `ResourceNotFoundException` fires only once a
label has **zero** catalog rows left — the ordinary retention sweep having
reaped every one.

### 5. Sequence numbers: the packed HLC, unchanged across sealing (F4)

**`SequenceNumber` is the decimal string of the record's own packed HLC**
(`hlc::pack`, the same `u64 = (wall_ms << 20) | logical` this codebase
already uses as the MVCC version and a cursor watermark — see
`animus-cp-data`'s `hlc` module). This is a deliberate, documented deviation
from real DynamoDB's own opaque sequence-number format, and it is **stable
across sealing by construction**: a record's HLC never changes when its
shard closes, so an iterator token minted against an open shard remains
meaningful, unmodified, once that shard is sealed — no translation layer,
no round-2-style shard-local counter that would need reassigning at a
lineage event.

- `AT_SEQUENCE_NUMBER(n)` / `AFTER_SEQUENCE_NUMBER(n)` seek to (or just past)
  HLC `n` — a binary search within a fetched, hlc-range-sliced segment for a
  closed shard, or a bounded `hlc > n` scan of the tablet's own log for an
  open one.
- `TRIM_HORIZON` → the shard's own start (its parent's sealed end-HLC, or
  `0` for a tablet's epoch-0 shard).
- `LATEST` → the open shard's current max HLC + a not-yet-existent tick
  ("nothing yet, wait for new records"); on an already-sealed shard this
  collapses to its end — the immediate-null iterator path.

Round 2's whole rejected-alternative argument here (an apply-assigned
monotonic position vs. a source-HLC-keyed one, chosen because a *copier*
lagged independently across shards) no longer applies: there is no copier,
so there is no "briefly behind" writer to protect against. The tablet's own
single Raft group is the sole assigner of order for its own log, at the
same commit the base write lands in — using that HLC directly is simply the
timestamp this codebase's MVCC and cursor machinery already agree on.

### 6. Iterators: stateless position tokens, no expiry (carried deviation)

A shard iterator is an opaque, stateless token —
`base64({label, shard_id, position})`, `shard_id` being the round-3
`shardId-<tabletId>-<epoch>` string and `position` its packed-HLC sequence
number — carrying everything needed to resume a poll with no server-side
session state. Real DynamoDB shard iterators expire after 15 minutes;
**this adapter's do not**, unchanged from round 2's documented deviation:
there is no separate iterator-store service whose capacity an expiry would
protect, and expiring would only add a failure mode a compatible client
already defends against but would rarely exercise here.

### 7. Hot-shard reads: leader-local, no `ReadIndex` barrier, and a warning (F8)

**`GetRecords` against an *open* shard is served from the tablet's own
leader's locally-applied `KIND_CHANGE` state, with no `ReadIndex`
read-barrier round** — the same departure from ADR 0017's default
linearizable-read discipline round 2 made, now reasoned about directly
against the tablet's own log rather than a separate shard tablet's:

The log is **append-only until reclaimed** (a record is never mutated, only
deleted by the trim janitor, and only once a sealed, K-replicated segment
already durably holds it — never dropped from being the *only* copy); it is
**positional** (an iterator names an exact HLC, not "the latest state of key
K"); and it serves only **committed-and-applied** records
(`applied ⊆ committed`, ADR 0017's own invariant). Given all three, the
worst a leader-local read can produce is a **stale prefix** — a `GetRecords`
call might not yet see a record its own Raft group has committed but not
yet locally applied (a bounded, sub-heartbeat lag) — never an out-of-order,
fabricated, or premature record. That staleness is indistinguishable from
the stream's own eventually-consistent contract (§1): a client already
tolerates "not there yet, poll again."

**This is deliberate and must never be "upgraded" to a `ReadIndex`
barrier.** Doing so would add a full quorum round-trip to every
`GetRecords` poll for a consistency guarantee this ADR's own contract
already declines to make. Served via a new internal-only
`ClientRequest::StreamHotRead` (refused bare, handled only inside
`cp_serve_forwarded` — grepped into every gating call site per this
codebase's own house lesson on adding a forwarded command variant, and
regression-tested through a follower-connected node).

### 8. The seal watermark is a catalog row, replacing the "copier" cursor tag (F10)

ADR 0041 §4's as-built note settled "no cursor, consumption is trim" for a
single consumer. Round 2 generalized this to a genuine multi-consumer
`KIND_CURSOR` design — a per-`(tablet, tag)` HLC watermark row, with `"gsi"`
and `"copier"` as the two tags, and a min-over-rows rule for split/merge
convergence. **Round 3 keeps the `"gsi"` half of that design completely
unchanged** (ADR 0041's GSI drain still writes its own trailing cursor row,
still generalizes correctly under a split/merge via min-over-rows) **and
deletes the `"copier"` half outright** — there is no consumer left to write
that tag's row, because there is no copier.

**In its place: a streamed table's effective trim watermark is derived
directly from the segment catalog**, not a row a consumer writes. A
tablet's watermark is its own shard chain's **last sealed end-HLC**
(`0`/absent if it has never sealed) — a split child inherits its *parent
tablet's* chain's last sealed end-HLC as its own initial watermark, exactly
mirroring how the GSI cursor's min-over-rows rule already treats a fresh
child (§"Split lineage," ADR 0043 §A4). The change-consumer loop's trim
janitor (ADR 0043 §A6) computes this from the very `Metadata` snapshot it
already holds each tick — no new read, and one less kind of durable row a
disabled-then-dropped stream could ever leave stale behind (there is
nothing to sweep: an un-reaped catalog row for a `DISABLED` stream simply
stops being an *expected* tag the moment the label's rows expire, per F12-b
below).

**Trim = min(gsi row if the table has GSIs, catalog watermark if the table
is streamed or its label still has live catalog rows).** An expected
watermark with nothing to derive it from yet (a stream that has never sealed
once) reads as absent, blocking trim entirely — the same safe default ADR
0041/round-2 already established for a cold consumer.

### 9. The durability invariant, and why trim is licensed only by a committed catalog row

**At every instant, every acknowledged streamed write is recoverable from
Raft-replicated hot state, or from a catalog-committed, K-replicated
segment — never from neither.** This is the mandate this whole subsystem
exists to uphold, and it is why trim is gated the way it is: a record is
only ever deleted from the hot `KIND_CHANGE` log once the segment
containing it has been durably `put` to `SegmentStore` on all K replicas
**and** the `SealStreamShard` catalog row committing that fact has itself
committed through the control plane's own Raft (ADR 0043 §A3/§A7). Trim
never runs off "the put returned `Ok`" alone — only off the catalog row,
because the row, not the store's own internal state, is what every reader
(and every future trim decision) actually trusts. See ADR 0043 §A3/§A9 for
the exact ordering this enforces at seal time and at recovery.

### 10. The superset-slice rule

**A deposed leader's late `put` can overwrite a segment object with a
superset of the content the catalog row actually committed** — the seal id
is deterministic (`{table}/{label}/{tablet}/{epoch}`), so a retried put from
a stale leader lands at the same object key, potentially carrying a few
extra records the winning leader's own put didn't (both leaders scanned the
same watermark-to-now range, at slightly different "now"s). **Readers must
slice a fetched segment's content to the catalog row's own committed
`hlc_range` and never serve the object's raw tail.** This is the corollary
of §9's ordering: the catalog row, not the object, is the ground truth for
what a shard actually contains; the object is allowed to be a harmless
superset because nothing ever trusts it un-sliced. See ADR 0043 §A3/§A7 for
the mechanism and a dedicated corpus scenario.

### 11. Enable / disable / drop semantics, and F12-b's disable grace

- **Enable**: `SetTableStream{Some(spec)}` mints a fresh `label`. No new
  tablets to provision — the hot shard is the table's existing tablets'
  existing `KIND_CHANGE` scope, so there is nothing for a lazy backstop to
  repair here (round 2's `CreateStreamShards`-crash-recovery concern does
  not exist in round 3). There is **no backfill**: only records written
  after enablement ever appear, matching DynamoDB's own behavior.
- **Disable, F12-b (reverses round 2's immediate-teardown design):** on
  `SetTableStream{None}`, every currently-hosting tablet leader performs one
  **final seal** of its own hot tail — so every record written before
  disable, undelivered or not, reaches the readable (segment) tier before
  the write gate closes. `StreamStatus` becomes `DISABLED`, but the stream
  stays **listed and readable**: its catalog rows and segment objects are
  reaped by the **ordinary retention sweep** (ADR 0043 §A9), no dedicated
  disable janitor. Re-enabling **mints a new label**, so a `DISABLED` stream
  and its `ENABLED` successor **coexist** in `ListStreams`/`DescribeStream`
  for as long as the old label's rows haven't yet aged out — §4's
  catalog-row-based resolution is what makes a request against the old
  label keep working correctly during that window. `ResourceNotFoundException`
  only once the old label's last catalog row has been reaped.
- **Drop table**: the cascade removes the streamed label's segment catalog
  rows and objects (both labels, if a grace-window pair currently exists) —
  there is no hidden table's tablets to enumerate and drop, unlike ADR 0041
  §5's GSI cascade or round 2's own stream-table cascade.

### 12. Merge stopgap (F1)

**v1 rejects `MergeTablets` on a base table with an enabled stream, at
apply time, on every replica.** This is an explicit **stopgap**, not a
permanent design choice: Guillaume has separately decided that tablet merge
is being removed **globally** (split-only tablets), in its own ADR and
deletion stack, scheduled to land **after** this streams stack merges
(decided 2026-08-14). When that ADR ships, this guard becomes dead code the
split-only ADR deletes along with `MergeTablets` itself — this ADR's text
should be read as bridging until then, not as this feature's permanent
position on merge.

The guard exists because a shard's lineage (§2, ADR 0043 §A4) assumes a
tablet's own range only ever *narrows* (split) — a merge widening two
tablets' ranges back together would require inventing exactly the
adjacent-parent lineage AWS itself never had to build (§Context's verified
fact above). A workaround — disable, merge, re-enable — is honest under
F12-b: it simply starts a genuinely new stream identity, which is what a
real DynamoDB customer resizing a table differently would also effectively
get.

**This makes the adapter, in this one respect, more capable than real
DynamoDB, not less**: AWS can *never* merge a table partition, streamed or
not. This adapter merely matches that for as long as merge exists at all,
and will exceed it once merge is removed and the guard along with it.

### 13. Knob defaults (F6)

`--stream-seal-bytes` (4 MiB), `--stream-seal-age` (4h), `--stream-retention`
(24h) — all independently configurable; tests use small values. The
size/age pair are OR-gated triggers on the change-consumer loop's seal arm
(ADR 0043 §A3); the 4h default deliberately echoes AWS's own observed
~4-hour shard rollover rhythm, not an arbitrary round number.

### 14. F11: token-aligned splits on a streamed table

Auto-split's chosen split key is rounded **down** to its own 8-byte token
boundary when the source table is streamed. This preserves the
partition-key/shard affinity a change-record's own token-leading key relies
on (ADR 0022) — an unaligned split key would risk separating one token's
records, and hence one shard's, across the split boundary — and, as a side
effect, narrows a pre-existing `txn.rs` residual noted in ADR 0018's own PR3
amendment about a split racing an in-flight transaction's own token.

### 15. Deviations from AWS, summarized

| Area | Real DynamoDB Streams | This adapter | Why |
|---|---|---|---|
| Shard count / growth | Elastic, throughput-driven | Grows automatically with every auto-split and seal tick — no separate resharding mechanism at all | Auto-split (ADR 0034) already drives tablet topology; a shard's own lineage rides it for free |
| Sequence numbers | Opaque, AWS-internal format | Decimal string of a packed HLC, stable across sealing | Corpus-checkable; no translation layer between hot and sealed reads |
| Shard iterators | Expire after 15 minutes | Never expire | No session-store capacity to protect; see §6 |
| `GetRecords` consistency | Documented eventually consistent | Leader-local (open) / store-served-and-sliced (closed), no barrier | Same observable contract, cheaper — see §7/§9/§10 |
| Post-disable readability | Records remain readable ~24h | Records remain readable until ordinary retention reaps them (F12-b) | Now AWS-faithful, not a v1 gap — see §11 |
| Tablet/partition merge | Never happens, ever | Rejected on a streamed table (v1 stopgap; global removal scheduled) | Matches AWS's own never-merge invariant; see §12 |
| `StartingSequenceNumber` | The first record's own actual sequence number (inclusive) | A shard's `hlc_range.0` — the record HLC's own **exclusive** lower bound (round-3 PR6) | Kept for internal position-convention consistency: every position this adapter carries (an iterator's own `position`, a segment's `slice_to_hlc_range` bound, `index_drain::hot_read`'s `from_position`) is uniformly "the exclusive floor the next read filters `hlc > position` against" — giving `StartingSequenceNumber` its own, inclusive convention would be the one position value in the whole subsystem that meant something different, a correctness trap for exactly the kind of code (a corpus checker, a future maintainer) that greps for "position" and assumes one meaning |
| `GetShardIterator` on an unknown/stale shard id | Documented as `ResourceNotFoundException` in some cases | `TrimmedDataAccessException`, matching `GetRecords`'s own outcome for the identical condition (round-3 PR6) | One error mapping for "this shard id doesn't currently resolve to anything live," shared by both operations that can hit it, rather than two different exceptions for what is, from this adapter's own state, the same fact |

### 16. Named follow-ups (not part of the committed design)

- **`TxnStage` kind-writes**: lifts the rejection of `TransactWriteItems` on
  an indexed *or streamed* table, once the transaction machinery gains a
  multi-kind atomic write extension (unchanged from ADR 0041 §2's original
  deferral).
- **CQL CDC**: a change-data-capture surface for the CQL adapter over the
  same underlying log/segment machinery.
- **Follower-served `GetRecords`**: relaxing §7's leader-only restriction for
  an open shard, once a bounded-staleness follower-read primitive exists
  generally.
- **S3 `SegmentStore` implementation**: a trait-conforming object-store
  backend, swapped in as a pure durability *upgrade* over the default
  cluster-replicated store (ADR 0043 §A7/§A7b) — never required for
  correctness.
- **`AdjacentParentShardId`-style extension**: only needed if tablet merge
  is ever revived *under* an active stream after the split-only ADR lands —
  documented as a shape, not built (ADR 0043 §A5).
- **CLI threading for the split-deployment/data-only argv paths (PR5
  deferral)**: `--stream-seal-bytes`/`--stream-seal-age`/`--stream-retention`/
  `--segment-store` are wired for `--cluster N`/`--config FILE --node I`
  (combined mode) only — `--cluster-control N --cluster-data M`,
  `animusd control`/`animusd data --config FILE --node I`, and `animusd data
  --seed ADDR` all still default silently to the production knobs
  (`StreamSealKnobs::default()`/`SegmentStoreConfig::default()`) with no CLI
  override. Every layered-wrapper function these flags would thread through
  already exists (`animusd`'s `_streams`-suffixed convention, see that
  crate's `CLAUDE.md`); this is argv plumbing, not new mechanism.
- **The control-only-leader segment-janitor scope gap (PR7, ADR 0043 §A9's
  own "control-only-leader scope gap" note)**: a control-only leader can
  mark catalog rows expired and react to a table drop (both `Metadata`-only
  decisions), but cannot physically delete segment objects or repair
  under-replicated ones — those need a `SegmentStoreHandle`, which today
  only exists on a node with a data role. In a **pure** split deployment
  (control-only nodes are the *only* control voters) this never runs at
  all; extending `SegmentStoreHandle` provisioning to a control-only node
  is the fix, out of PR7's scope. Not a correctness bug — a marked row is
  already invisible to `DescribeStream`, only "this leadership stint can't
  finish the physical reclaim."

Explicitly **not** adopted, per owner decision: a serving-tier extraction or
any S3 *archival* feature beyond the plain trait-swap follow-up above — both
were considered and declined as out of scope for this adapter.

## Consequences

**Easier.**

- **No second write path.** The hot shard *is* the change log ADR 0041
  already built and already tests; exactly-once is structural, not a dedupe
  row a copier could get subtly wrong.
- **Shard lineage is free.** Growth, splitting, and per-item shard affinity
  all ride the tablet lifecycle this codebase already has fully tested
  (ADR 0031, ADR 0034) — there is no separate resharding mechanism to design,
  test, or operate.
- **The disable-grace behavior is now AWS-faithful from day one**, not a
  documented gap with a roadmap item to eventually close it.

**Harder, and knowingly accepted.**

- **Two tiers of storage now exist for one logical stream** (hot Raft state
  and a K-replicated segment store), and the durability invariant (§9) and
  superset-slice rule (§10) are both subtle, load-bearing properties a
  future change could silently violate — the corpus (ADR 0043's Testing
  plan) exists specifically because getting this wrong is a real data-loss
  or torn-read hazard, not a cosmetic one.
- **The merge stopgap (§12) is a real, if temporary, capability
  restriction** — an operator cannot merge a streamed base table's tablets
  today, full stop, even though the workaround (disable/merge/re-enable) is
  honest about what it costs.
- **Catalog-row-based label resolution (§4/§11) is more state to reason
  about at read time** than round 2's simple schema-label equality check —
  a `DescribeStream`/`GetRecords` request's validity now depends on
  retention timing, not just the current schema, for the lifetime of a
  disable grace window.
