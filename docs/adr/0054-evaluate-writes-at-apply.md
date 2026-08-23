# ADR 0054 — Evaluate writes at apply, not before the log

**Status:** Proposed. Supersedes the evaluate-at-leader half of ADR 0046 and
retires the OCC seatbelt introduced there. Depends on ADR 0053 (DynamoDB-only)
for its layering.

## Context

A DynamoDB write today is evaluated at the tablet's Raft leader **before** the
log. `kind_write_item_at_leader` (`crates/animusd/src/dynamo.rs`) takes the
node's `rmw_lock`, reads the item's committed bytes, evaluates any client
`ConditionExpression`, computes the new item, derives the LSI row diff and the
stream `ChangeRecord` (`kind_writes_for_item`, same file), releases the
lock, and proposes a `KindBatch` carrying the **finished bytes**.

Because the read happened outside the log, the entry cannot simply be applied —
the state it was computed against may have changed by the time it applies. So
every such entry also carries an apply-time OCC precondition, the *seatbelt*:
`vec![(base_key, raw_old)]` (built in `kind_write_item_at_leader`, `dynamo.rs`),
checked byte-for-byte on every replica (`apply_and_compact`'s `KvCommand::
KindBatch` arm, `crates/animus-cp-data/src/lib.rs`). If the key no longer holds
the bytes the leader read, the batch no-ops whole and records
`KindBatchOutcome::ConditionFailed`.

### What that costs

The window between propose and apply is real, and under load it is wide. Two
writers to one key both read the same before-image; whichever applies second
finds the key changed and is refused. `ADD` is not idempotent, so the service
cannot retry on the client's behalf — that arbitration belongs to the client —
and the client receives a 500 for a write that simply lost a race.

This is not hypothetical. CI measured 2 of 10 concurrent increments refused, and
the same commit produced one green and one red `prod-liveness` run in parallel.
Notably it does **not** reproduce by adding concurrency: forty concurrent writers
still acknowledged forty, because evaluation serialises on the leader's
`rmw_lock` and each request's apply completes before the next evaluation reads.
What reproduces it is a **starved apply task** — evaluation racing ahead of
apply. That distinction matters for anyone trying to reproduce this.

The reference system does not behave this way. DynamoDB advertises atomic
counters: `ADD` increments "unconditionally, without interfering with other write
requests." It can promise that because its leader evaluates the update *inside*
its serialized stream for the item — there is no gap between reading and
logging. Same topology as ours; different point of evaluation.

### Why the obvious fixes do not work

Recorded here so they are not re-proposed:

- **Hold `rmw_lock` until propose.** Buys nothing. The committed value does not
  change until *apply*, so a second writer that acquires the lock after the
  first has proposed still reads the stale before-image. The window is
  propose→apply, not read→propose.
- **Hold `rmw_lock` until apply.** Closes the window, but this is one *node-wide*
  lock, so it stalls every unrelated item's write behind one slow confirm —
  precisely what issue #285 removed. And it could never be the correctness
  mechanism anyway: it is node-local, and `txn_resolver_loop`'s recovery pushes
  never take it.
- **Retry a write the outcome proves did not apply.** Tempting, because
  `ConditionFailed` really is proof rather than inference. Rejected: the
  service would be arbitrating on the client's behalf, and its safety would rest
  on the unenforced convention that `KindBatch.conditions` only ever carries our
  own seatbelt and never something client-visible.
- **A leader-side overlay of in-flight values.** A second memtable that must
  never be read from, plus it makes each write's success depend on its
  predecessor's — converting nine independent failures into all-or-nothing, and
  baking speculation into stream images that external consumers read.
- **Log the delta (`ADD 1`) only.** Covers just the commutative subset; `Put`,
  `SET`, `DELETE` and conditional writes still need a before-image, leaving two
  write paths and the defect live on most of them.

Each treats a symptom. The cause is that the new value is computed outside the
log.

## Decision

**Move evaluation into the state machine.** A `KindBatch` entry carries the
*operation*, not the computed result. Apply reads the current item in commit
order, evaluates the condition and the update, derives the index rows and the
change record, and writes — all at a single point in a total order, where no
stale before-image can exist.

Three mechanisms make that possible.

### 1. The entry is self-contained

Apply has **no access to control-plane `Metadata`** — this is not an omitted
parameter but a deliberately closed boundary: `apply_and_compact`'s signature
(`crates/animus-cp-data/src/lib.rs`) has no catalog handle, and the only
`Metadata` mentions in the crate are doc prose. Wiring a live catalog read into
apply would be actively wrong: `Metadata` is replicated by a *separate* Raft
group, so two replicas applying the same entry against different catalog
versions would derive different index rows and **diverge**. Today that is
impossible by construction, because only the leader reads the catalog and the
*result* is what gets replicated.

So the entry carries the schema slice it was accepted under — the table's key
schema, its index definitions, and whether change records carry images. Those
are exactly the three lookups `kind_writes_for_item` performs
(`Metadata::table_indexes`, `schema_for`, `table_change_records_carry_images`,
all in `crates/animusd/src/dynamo.rs`). Apply becomes a pure function of
`(entry, engine state)`, deterministic by construction rather than by a
coincidence of catalog timing.

The leader still reads `Metadata` — but it no longer reads the *item*, and the
item is the thing that goes stale.

### 2. Evaluation moves below the wire adapter

`animus-cp-data` is a protocol-agnostic KV state machine; making it depend on a
wire crate would invert the layering. ADR 0053 removes the second wire protocol,
so the item model is now simply the data model, and the resolution is to extract
`AttributeValue`, `Item`, stored-item encode/decode, the update-expression and
condition evaluators, and the LSI/change-record derivation into a crate *below*
both. `animus-dynamo` keeps HTTP/JSON; `animus-cp-data` depends on the item
crate, never on the wire.

The evaluator is already fit to move: no `HashMap`/`HashSet` anywhere in
`animus-dynamo`, sorted-vector set operations, and exact decimal-string
arithmetic for numeric `ADD` that deliberately avoids `f64`. It introduces no
new ADR 0003 violation. The extraction is nonetheless substantial — `wire.rs` is
6,169 lines and needs sub-extraction, not a wholesale move — and lands as its
own no-behaviour-change PR first.

### 3. Results come back from apply

Every consumer that today reads the leader's return value needs a new path:
`ReturnValues`, the stream images, `collection_bytes` (ADR 0006's
`ItemCollectionMetrics`), and `ConditionalCheckFailedException`.
`ConsumedCapacity` needs nothing new — it is already derived at the edge from
the returned images (`write_capacity`, `crates/animusd/src/dynamo.rs`), so it
follows for free.

`KindBatchOutcome` carries no payload today, and its own doc already worries
about unbounded growth (the `KindBatchOutcomes` doc, `crates/animus-cp-data/src/lib.rs`).
Attaching full images to all 8,192 retained entries on every replica would be
wasteful, so:

- the **decision** (`Applied` / `ConditionFailed` / `Sealed`) stays replicated
  and recorded identically on every replica, exactly as now;
- the **payload** (old image, new image, collection bytes) is retained only by
  the node that proposed the entry, and only long enough for its own confirm
  poll — bounded by in-flight writes rather than by a fixed retention count.

A follower never needs the payload; nobody is waiting on it there. If leadership
changes before the proposer reads it, the payload is gone — but that case is
already an ambiguous outcome the client must arbitrate, so it loses nothing.

## Consequences

**Removed.** The OCC seatbelt for evaluated writes. `rmw_lock`. Refusals caused
by contention. The retry-versus-arbitrate question for the contended case.
Speculation in stream images — they become contiguous by construction, each
`old_image` byte-identical to its predecessor's `new_image`, because each is
computed against the state its own entry applies to.

**Not removed, deliberately.** Ambiguity. A client whose connection dies between
propose and apply still cannot tell whether its write landed, and a retry of a
non-idempotent `ADD` still double-counts — as it does on DynamoDB, which
documents exactly this. Closing that requires an idempotency token
(`ClientRequestToken`) and is tracked separately. This ADR absorbs *contention*;
it does not pretend to absorb ambiguity.

**The main risk, to be measured rather than argued.** Today N items evaluate
concurrently at the edge and apply cheaply writes precomputed bytes. After, the
single apply task per tablet performs every item's read-modify-write serially.
That is the inverse of #285's trade: same-key writes stop serialising, but *all*
evaluation for a tablet starts to. Engine reads should dominate, and apply
already reads for the conditions check — but this must be benchmarked, not
assumed. The existing harness is `bench_plain_table_put_wall_clock`
(`crates/animusd/src/dynamo.rs`), whose ADR 0049 §5 baseline is 4.69 ms/op;
that is the before/after gate. Nothing today isolates the apply loop alone, so
a narrower harness is likely needed.

**Producers.** The ordinary write path, `BatchWriteItem`'s images arm, the TTL
reaper, the admin seeder, and `txn_stage_local` all evaluate and move together.
The unevaluated fast arms (marker writes, raw client-protocol writes) and the
index-backfill seeder write no item images and are unaffected. Split copy uses
`SeedBatch` and GC bypasses the write pipeline; neither is a `KindBatch`
producer and neither is touched.

## Sequencing

1. Extract the item model below the wire adapter. No behaviour change.
2. Add the schema slice to `KindBatch` and the leader-local result payload,
   both unused. No behaviour change.
3. Move evaluation to apply for one operation end to end, behind the existing
   test suite, with the write-path benchmark run before and after.
4. Move the remaining producers; delete the seatbelt and `rmw_lock`.

Each step is independently revertible, and only step 3 changes observable
behaviour.
