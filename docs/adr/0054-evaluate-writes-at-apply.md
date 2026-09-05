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

## As-built amendment (2026-09-05, Sequencing step 1 — item model extracted)

Step 1 landed: `AttributeValue`/`Item`/`TableSchema`, the key-encoding
primitives (`escape`/`storage_key`/`numkey`), `ConditionExpression`/
`SortKeyCondition` (`condition`), the `UpdateExpression` data model and its
apply-time evaluator (`apply_update` and everything it calls), the
stored-item codec, the item-size formula, and the GSI/LSI row/footprint/
change-record derivation (`index`) moved into a new crate, `animus-item`,
below both `animus-dynamo` and `animus-cp-data` — see
`crates/animus-item/CLAUDE.md`. `animus-dynamo` re-exports all of it
unchanged; every `animus_dynamo::X`/`animus_dynamo::wire::X` path a caller
used before this step still resolves, and no test's assertion changed (a
pure move, 324 tests before and after, redistributed rather than dropped).

**What did not move, and why.** The `UpdateExpression`/`ConditionExpression`
**string parser** — tokenizing a request's expression text and resolving its
`#alias`/`:placeholder` references against `ExpressionAttributeNames`/
`ExpressionAttributeValues` — stayed in `animus-dynamo::wire`. That is
JSON/wire-decode work, not part of the pure item-mutation model: it needs
`serde_json::Value`, which would have pulled wire-shaped decoding back into
the crate this step exists to keep pure below the wire. Only the *data types*
the parser produces (`PathSegment`/`UpdateOperand`/`UpdateExpr`/
`UpdateAction`) and the *evaluator* that consumes them (`apply_update`)
moved — exactly the boundary "evaluation moves below the wire adapter"
describes. `animus-dynamo`'s `registry`/`schema` (the catalog bridge),
`capacity` (`ConsumedCapacity` response shaping, though it now re-exports the
item-size formula itself from `animus-item`), `sigv4`, `streams_wire`, and
`ttl` are unaffected.

`apply_update` still runs exactly where it always did — the tablet leader,
under `rmw_lock`, via `kind_write_item_at_leader` — unchanged by this step;
only *where the code lives* changed. Step 2 (adding the schema slice to
`KindBatch` and the leader-local result payload) and step 3 (moving
evaluation to apply for real) are still ahead; `animus-cp-data` does not yet
depend on `animus-item`.

Status stays **Proposed**: only step 3 changes observable behaviour, and it
has not landed.

## As-built amendment (2026-09-05, Sequencing step 2 — the self-contained
entry, the apply-side evaluator, and the leader-local result payload landed,
unwired)

Step 2 landed, matching the ADR's own description almost exactly, with one
deliberate deviation from its literal wording (carrying the operation in a
*new* variant rather than adding fields to `KindBatch`) and one design
decision the ADR left open (the outcome mapping for an evaluator-rejected
write). `animus-cp-data` now depends on `animus-item` directly (`cargo tree
-i animus-item` gains one new dependent; no `animus-env`/wire-crate
duplication).

**The schema slice (mechanism 1) — `animus_item::WriteSchema`.** A new
`animus-item` module, `write_schema`, holds exactly the three lookups
`kind_writes_for_item` used to perform against `Metadata`
(`Metadata::table_indexes` filtered to `Local`, `schema_for`,
`table_change_records_carry_images`), frozen into three fields: `key:
TableSchema`, `lsis: Vec<LsiDef>` (name/sort-attribute/projection — no
`hash_attribute`, since an LSI's is always the base table's own partition
key), and `change_records_carry_images: bool`. **Deliberately no GSI list**:
a write commits no GSI row directly (the asynchronous drain derives those
later from the change-log record), so a GSI needs no entry in this slice at
all — narrower than the ADR's own illustrative `gsis: Vec<GsiDef>` sketch,
because tracing `kind_writes_for_item`'s actual reads found it never needed
one. `Projection` is a small, pure, `animus-item`-local copy of
`animus_control::schema::IndexProjection` (the identical "duplicate rather
than invert the dependency" call the crate's own doc already makes for
`animus-tablet`'s `escape`). `animusd::dynamo::write_schema_for(meta, table)`
builds it, next to the pre-existing `schema_for`.

**The pure evaluation core — `animus_item::derive_kind_writes`.** The whole
body of `kind_writes_for_item` (the LSI diff loop, the change-record
assembly) moved into this function verbatim — a byte-identical extraction,
proven by every pre-existing `animusd` test that exercises indexes/streams
staying green unmodified (`dynamo_indexes`, `dynamo_index_writes`,
`dynamo_streams`, `dynamo_update_add_delete`) plus a new differential test
in `animus-cp-data` comparing its output against a hand-built `KindBatch`.
`kind_writes_for_item` is now a thin wrapper: build a `WriteSchema`, call
`derive_kind_writes`, return its two fields as the pre-existing tuple. It
takes `token_prefix`/`kind_base`/`kind_lsi` as plain parameters rather than
computing or importing them, since `animus-item` still has no
`animus-tablet` dependency (no token hashing) and sits below the crate that
defines the `KIND_*` byte constants (`animus-cp-data`) — the caller supplies
both.

**The entry — a new variant, `KvCommand::KindEval`, not new fields on
`KindBatch`.** The ADR's Sequencing text reads "add the schema slice to
`KindBatch`"; building it, a `KindBatch` entry already carries the leader's
*finished* writes/change-log and a byte-level OCC seatbelt — fields for an
unevaluated *operation* plus a schema slice would sit unused beside them
until step 3, and step 3 needs `KindBatch`'s existing bytes/semantics to
keep working for every producer not yet cut over. A sibling variant keeps
the two entry shapes independently revertible, exactly as the ADR's own
"Each step is independently revertible" line asks for, and avoids growing
`KindBatch`'s codec shape for fields step 3 would delete again once the
cutover finishes. `KindEval` carries `schema: WriteSchema`, `pk:
AttributeValue`, `sk: Option<AttributeValue>`, `op: KindEvalOp`
(`Put(Item)`/`Delete`/`Update{key_item, actions}` — the wire-decode-free
mirror of `animus-node`'s `KindWriteOp`), `condition:
Option<ConditionExpression>` (the client's own rich expression, not a
byte-level OCC pair — apply's own read already *is* current state, so no
seatbelt is needed), `ttl_expired: bool`, and `ts: HlcTimestamp`. No
`base_key`/token field: apply derives both from `pk`/`sk` via
`animus_tablet::partition_token`, which `animus-cp-data` already depends on,
removing a leader-computed value that could disagree with `pk` in principle.
Codec version `25` (tag `16`): the four rich, evolving nested field types
(`WriteSchema`, `AttributeValue`×2, `KindEvalOp`, `Option<ConditionExpression>`)
each ride as one `serde_json`-encoded blob inside the binary envelope — the
same convention `backup.rs`'s `BackupManifestObject` already uses for
`TableSchema`'s own multi-field, evolving shape, rather than a hand-encoded
field-by-field layout for four types that will keep growing fields.

**Apply, in commit order.** `KvCommand::KindEval`'s apply arm: drain the
pending run (so the read observes every earlier write this same apply pass
committed, mirroring `KindBatch`'s own `conditions` read); compute the base
key; check the seal/freeze gate first (`Sealed`, identical to `KindBatch`);
read the current value, unwrap its envelope — an **unresolved intent from a
concurrent transaction** is treated as `ConditionFailed` (ambiguous, never
guessed at, mirroring `KindBatch.conditions`'/`Cas`'s identical foreign-
intent discipline: the proposer's cue is "retry", the same as any other
no-op); evaluate `condition` (`Ok(false)` → `ConditionFailed`; `Err` → see
below); compute `new` via `op` (`Update`'s `apply_update` `Err` → see below);
derive `writes`/`change_log` via `derive_kind_writes`; materialize via the
one shared `materialize_derived` helper `KindBatch`'s own arm and
`TxnResolve`'s commit branch already call — no third copy.

**The outcome mapping — a new `KindBatchOutcome::Rejected { key, code,
message }`, reusing the existing `KindBatchOutcomes` map rather than a
`KindEval`-specific one.** The ADR left this open ("a new outcome variant
`Rejected{code}` — decide and document"). Decided: `Rejected` is distinct
from `ConditionFailed` because it carries genuinely different information a
later wire-mapping step needs — a false `ConditionExpression` maps to
`ConditionalCheckFailedException`, while a `ConditionExpression` that
returns `Err` (a domain violation, e.g. `size()` on the wrong type) or an
`apply_update` `Err` (a malformed update, a type mismatch, or the
post-update item over the size cap) both map to `ValidationException` with a
real message worth preserving. `code`/`message` copy `ConditionError`'s/
`UpdateError`'s own fields verbatim — this crate stays protocol-agnostic and
does not interpret them beyond copying them through. `code` is a `String`
even though both source types currently only ever produce
`"ValidationException"`, since nothing here assumes that stays true. The
wire-level mapping to a real `WireError`/HTTP response is left entirely to
step 3 — `animusd::classify_kind_batch_outcome`'s existing wildcard already
treats an unrecognized `KindBatchOutcome` variant as `Inconclusive` (safe,
since nothing produces `KindEval` yet), and step 3 should fold `Rejected`
into the `NoOp` arm alongside `ConditionFailed`/`Sealed` when it wires a
real producer.

**The leader-local result payload (mechanism 3) — `KindEvalResult`/
`KindEvalResults`, a *separate* structure from `KindBatchOutcomes`, exactly
as the ADR specifies and for the reason it gives (memory on followers): a
`KindEval` entry's `old`/`new` images are a normal by-product of every
replica's own evaluation, but only the node that proposed a given entry ever
wants them back. `RaftKvNode::propose_kind_eval` registers this node's
interest in an accepted entry's index into a bounded `BTreeSet` **while
still holding the same `core` mutex the apply task needs to lock before it
can ever drain that entry** — a real ordering guarantee (not a hopeful
race window) on every executor, `SimEnv`'s single-threaded cooperative
scheduler and a genuine second OS thread under `ProdEnv` alike, since the
apply task's `drain_apply` call cannot proceed until the registration's own
critical section releases the lock. Apply's `KindEval` arm calls `fill`
only when the entry actually applied (`Applied`, never on a rejection/no-op
— there is no old/new pair worth keeping for those), consulting the
`interested` set and promoting to a real, term-paired entry only on a hit.
`RaftKvNode::take_kind_eval_result(index, term)` removes the slot on read
(never a peek), mirroring `kind_batch_outcome`'s identical index-and-term
identity discipline for the identical reason (an uncommitted entry's index
can be reoccupied by a different command after a leadership change). Both
`interested` and `results` are bounded by the same generous `RETAIN = 8192`
`KindBatchOutcomes` already uses, for a slot nobody ever reads back.

**Tests** (`animus-cp-data`, all `SimEnv`, seed-reproducible): a
differential test comparing a `KindEval`'s own apply-derived rows (base,
LSI, and the change record's *value*, ignoring its key's HLC suffix, which
legitimately differs between two independent groups) against a `KindBatch`
built by hand from the identical `derive_kind_writes` call; a false
condition leaving every replica's row untouched; two `ADD` proposals issued
back-to-back before either applies — the ADR's own motivating property —
both landing with zero refusals (final value `+2`); the leader-local
payload's three properties (the proposer sees it, a non-registered replica
never does, a second read finds it gone); the frozen/sealed gate rejecting a
`KindEval` exactly like a `KindBatch`; and a crash/restart replaying two
`KindEval` entries to the identical committed state, including the correct
final LSI row and the stale one's removal. `animus-item`'s own
`write_schema` module carries the pure-function unit tests for
`derive_kind_writes` itself (insert, LSI diff, unchanged-sort-attribute
no-delete-reput, delete).

**No producer proposes `KindEval` yet.** `kind_write_item_at_leader` still
evaluates at the leader and proposes `KindBatch`, byte-identical to before
this step. Step 3 (moving evaluation to apply for one operation end to end,
behind the existing test suite, with the write-path benchmark run before and
after) is still ahead — see that step's own handoff notes for what it needs
from this one: `propose_kind_eval`'s exact signature, the `Rejected`
outcome's `code`/`message` shape, and `classify_kind_batch_outcome`'s
wildcard needing an explicit `Rejected` arm once a real caller exists.

Status stays **Proposed**: only step 3 changes observable behaviour, and it
has not landed.
