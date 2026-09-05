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

## As-built amendment (2026-09-05, Sequencing step 3 — the single-item write
path cut over, seatbelt kept as a double-check)

Step 3 landed: `kind_write_item_at_leader` (`crates/animusd/src/dynamo.rs`)
no longer builds the write's finished bytes itself. For every `PutItem`/
`DeleteItem`/`UpdateItem` it serves — every table, since ADR 0049 made the
kind-write path universal, indexed/streamed/plain alike, conditional or
not — it now builds the `WriteSchema` slice (`write_schema_for`, already
landed in step 2) and a `KindEvalOp` mirror of the client's operation, and
proposes a `KvCommand::KindEval` via a new sibling of `cp_kind_local`,
`ClientCtx::cp_kind_eval_local` (`write_path.rs`). The wire-visible result —
`Ok{old, new}` / `ConditionFailed` / a `ValidationException` — now comes
from **apply's own confirmed decision**, read back via `classify_kind_batch_
outcome` (reused verbatim, with the `Rejected` arm step 2 anticipated now
wired in) and, on `Applied`, `RaftKvNode::take_kind_eval_result`. This is
the ADR's whole point landing for real: apply evaluates the condition and
the update against the current committed value **in commit order**, so two
concurrent evaluators of one key no longer race a stale before-image.

**The motivating regression is fixed, not just re-labeled.**
`dynamo_update_add_delete.rs::concurrent_increments_all_land_exactly_once`
(ten concurrent `ADD`s on one key) now asserts `counter == WRITERS` and zero
refusals — the doc comment that used to explain why 2-of-10 refusals were
*correct behaviour* now explains why they no longer happen. A sibling,
`concurrent_conditional_add_all_land_exactly_once`, proves the same property
for a **conditioned** `ADD` (`attribute_exists(pk)`, genuinely true for
every racing writer) — the condition is now evaluated fresh at apply, so
contention cannot make a genuinely-true condition spuriously fail either.

**The seatbelt is kept, for this step only, as a double-check — not as a
gate.** `kind_write_item_at_leader` still takes `ctx.data().rmw_lock`,
reads its own `old` via `ClientCtx::cp_get_local_resolving`, and evaluates
`condition`/`op` against it (`predict_kind_eval_decision`) — but that
prediction is used **only** to compare against apply's confirmed outcome
(`report_kind_eval_seatbelt_mismatch`) and never to decide what the client
sees. A disagreement never fails the request (apply's decision is
authoritative by construction) — it increments a new metric,
`Metric::KindEvalSeatbeltMismatch` (`kind_eval_seatbelt_mismatch` on
`/metrics`), and logs both decisions via `tracing::warn!`. The metric's own
doc names the two possible directions explicitly: the leader predicting
`ConditionFailed`/`Rejected` from a before-image a concurrent write
supersedes before apply — reading fresher state — actually applies the
write is **expected**, the contention this ADR exists to absorb; the
reverse (apply rejecting a write the leader's own read predicted would
succeed) is the direction worth investigating as a real evaluator
divergence. A dedicated regression
(`dynamo.rs::stream_write_path_tests::
a_stale_leader_prediction_that_apply_supersedes_ticks_the_mismatch_metric_and_still_succeeds`)
manufactures the expected direction deterministically (via the existing
issue #285 `rmw285_confirm_gate` test hook) and asserts the request still
succeeds — landing the write the leader's own stale read would have
refused — while the counter ticks.

**The lost-payload residual (mechanism 3) resolved.** If apply confirms
`Applied` but this node's own leader-local `old`/`new` slot is gone
(bounded retention aged it out under an unusually slow confirm, or
leadership churned between accept and confirm), an idempotent write
recovers with a best-effort re-read of the current value as `new` (`old`
stays `None`, genuinely unrecoverable); a non-idempotent write (`ADD`)
returns the identical ambiguous, non-retried error the pre-existing
confirm-timeout path already used (`"CP kind write did not commit in
time"`, reused verbatim rather than inventing a new one) — the ADR's own
"Not removed, deliberately: Ambiguity" consequence, not a new gap.

**Error mapping is byte-identical.** `KindBatchOutcome::Rejected{code,
message}` maps back to `WireError::validation(message)` for
`"ValidationException"` (the only code either `ConditionError`/
`UpdateError` produce today) — the same mapping `From<ConditionError>`/
`From<UpdateError>` already performed pre-cutover, so
`dynamo_expression_surface.rs`'s `size_of_an_existing_number_attribute_is_a_
validation_exception` and the rest of that suite needed no changes.

**What did NOT move.** `eval_kind_txn_write`/`txn_stage_local`
(`TransactWriteItems`' own evaluation) still evaluate at the leader and
propose `KindBatch` — ADR 0054's Sequencing text moves them in step 4,
alongside deleting `rmw_lock`, the `KindBatch.conditions` seatbelt, and this
step's own mismatch metric. `BatchWriteItem`, the TTL reaper
(`ttl_reaper.rs`), and the admin seeder (`admin::action_data_seed`) all
route through `kind_write_item_at_leader`, so they cut over for free —
`dynamo_ttl.rs`, `batch_write.rs`, `dynamo_batch_get.rs`, and
`stream_write_path_tests::admin_seed_writes_through_the_kind_path_on_both_
table_shapes` all stayed green unmodified. Forwarding needed no new
`ClientRequest` variant: a forwarded write already lands on the leader's
own node before `kind_write_item_at_leader` (and its `cp_kind_eval_local`
propose) ever runs — `control_only.rs::
mixed_cluster_put_via_control_node_forwards_to_data_node` is unmodified and
green.

**Benchmark gate (ADR 0049 §5's harness,
`dynamo::stream_write_path_tests::bench_plain_table_put_wall_clock`,
`#[ignore]`d — run with `cargo test -p animusd --lib bench_plain_table_put
-- --ignored --nocapture`), 200 sequential `PutItem`s on a plain
(unindexed, unstreamed) table, one node, 3 runs each, same host/session:**

| Run | Parent (`6fb818b`, leader-evaluated) | This commit (apply-evaluated) |
|-----|---------------------------------------|--------------------------------|
| 1   | 3.13 ms/op                            | 3.15 ms/op                     |
| 2   | 3.15 ms/op                            | 3.15 ms/op                     |
| 3   | 3.27 ms/op                            | 3.26 ms/op                     |

No measurable regression — the two medians (3.15 ms/op both) are identical
within run-to-run noise. The ADR's own risk section named the apply
task's now-serial per-item evaluation as "the main risk, to be measured
rather than argued": for a plain table's unconditional `Put`, apply's own
read-then-evaluate cost is the same single engine read
`kind_write_item_at_leader`'s old leader-side read already paid — moving
*where* that read happens does not add a second one, so the risk did not
materialize for this workload. (The seatbelt double-check kept for this
step *does* add a second, redundant read+evaluate on the leader — the very
thing step 4 removes — so this number is a conservative upper bound on
step 3's own overhead, not a preview of the post-step-4 steady state.)

Status stays **Proposed**: step 4 (moving the remaining producers and
deleting the seatbelt/`rmw_lock`/this step's own mismatch metric) is still
ahead, and the ADR's Sequencing text reserves Accepted for after it lands.

## As-built amendment (2026-09-05, Sequencing step 4a — the transaction
stage moved to apply-time evaluation)

Step 4a landed: `TransactWriteItems`' own evaluation — the one producer
step 3's own as-built amendment named as "did NOT move" — now evaluates at
apply too, closing the propose→apply staleness window for a transactional
write the identical way step 3 closed it for the ordinary write path.

**The self-contained payload — `txn::PendingTxnWrite`.** A `KvCommand::
TxnStage` entry's own `txn::TxnWrite` gained a new field, `pending:
Option<PendingTxnWrite>` (codec version 26): when `Some`, `value`/
`kind_writes`/`change_log` are ignored at propose time (left at their zero
values) and computed instead by `TxnStage`'s own apply arm. `PendingTxnWrite`
carries exactly `KvCommand::KindEval`'s own fields minus `ts` (the
enclosing stage entry already carries one, shared by every write staged in
it): `schema: WriteSchema`, `pk`/`sk`, `op: KindEvalOp`, `condition:
Option<ConditionExpression>`, `ttl_expired`. `stage_marker` is deliberately
**not** part of this — it is a pure function of `pk`/`sk` alone (an
image-less dirty-key marker, ADR 0049 §3), carries no state that could go
stale, and stays built at propose time exactly as before.

**Apply, in commit order, reusing step 2's evaluator verbatim.** `TxnStage`'s
apply arm evaluates every `pending` write only after the entry's existing
structural gates pass (fence/seal/foreign-intent/already-decided/the
byte-level `conditions` OCC check) — a pending write's own evaluation never
masks a more fundamental rejection. For each: read the key's current
committed value (post-`flush_pending`, mirroring the byte-OCC read's own
discipline), then call the identical `evaluate_kind_eval` function
`KvCommand::KindEval`'s own arm calls — no second evaluator, per this ADR's
own principle. A `ConditionFailed`/`Rejected` decision (the latter a new
`StageOutcome::Rejected { key, code, message }` variant, the `TxnStage`
sibling of step 2's `KindBatchOutcome::Rejected`) no-ops the **whole**
stage, matching `TxnStage`'s pre-existing whole-or-nothing discipline. An
`Applied` decision's derived `writes` (base row + kind-scope rows) and
`change_log` are substituted for that write's own (empty) fields when the
stage's existing merge loop builds the intent envelope.

**Same-txn WAL replay does not double-apply.** A pending write's own base
key, once staged, holds this transaction's own intent — a genuine
possibility on an ordinary restart (no compaction has run, so replay
reprocesses the stage entry against an already-intact engine; see
`animus-cp-data/CLAUDE.md`'s "engine_applied vs last_applied" entry).
Re-evaluating `op`/`condition` against the intent's own already-computed
value would treat it as the pre-stage baseline and, for a non-idempotent
update like `ADD`, double-apply. Fixed structurally: when the base key's
current envelope is an `Intent` naming this exact `txn_id`, apply skips
evaluation entirely and reuses the intent's own already-computed `staged_
value`/`kind_writes`/`change_log` verbatim — deterministically identical to
what a fresh evaluation would have produced the first time, since it *is*
that computation's own output. A foreign intent here is unreachable (the
whole-stage foreign-intent gate already rejected the entry before this
per-write loop ever runs). Regression: `tests/txn_kind_writes.rs::
pending_add_stage_survives_a_same_engine_restart_without_double_applying`
(a real same-engine restart with an `ADD`, proving the final value is
exactly base+delta once, not twice).

**No apply-time OCC seatbelt is carried for an evaluated write.** The
pre-4a design's mandatory own-key `conditions` entry (ADR 0046 Fork C1,
`(key, raw_old)`) existed to guard the window between the leader's read and
its own propose call; since apply's own read now IS that evaluation point,
there is no window left to guard. `ClientCtx::txn_stage_local` no longer
reads/evaluates at all — it builds `PendingTxnWrite` and appends it
unevaluated, so `conditions` stays whatever the caller passed (empty, for
every Dynamo kind-write-path caller today) rather than being populated with
a seatbelt entry. `dynamo::eval_kind_txn_write` — the leader-side evaluator
this replaces — is deleted outright (not merely unwired), along with
`KindTxnWriteEval`; `dynamo::kind_write_op_to_eval_op` factors out the
`KindWriteOp` → `KindEvalOp` mirror both this producer and step 3's now
share, rather than duplicating the match inline at each call site.

**Tests** (`animus-cp-data/tests/txn_kind_writes.rs`, `SimEnv`,
seed-reproducible): a pending Put's resolved rows are byte-identical to
`animus_item::derive_kind_writes`'s own direct output for the identical
operation (`pending_eval_stage_resolves_to_the_same_rows_kind_eval_would_
derive`); a false condition rejects the whole stage and stages nothing
(`pending_eval_stage_with_a_false_condition_records_condition_failed_and_
stages_nothing`); a `KvCommand::KindEval` racing a still-unresolved pending
stage on the same key gets `ConditionFailed` against the foreign intent and
succeeds once the transaction resolves
(`a_kind_eval_racing_a_still_unresolved_pending_stage_gets_condition_
failed_then_succeeds_after_resolve`); and the same-txn replay regression
above. `codec.rs::every_wire_variant_round_trips` gained a `pending:
Some(..)` write exercising every `PendingTxnWrite` field through the
version-26 binary codec. The whole pre-existing suite (`txn_conditions.rs`,
`txn_multi.rs`, `txn_recovery.rs`, `kind_eval.rs`, and the rest of this
crate) stayed green unmodified except for the new `pending: None` field on
every existing hand-built `TxnWrite` literal (a mechanical addition, no
behavior change for a non-`pending` write). `ANIMUS_TXN_SEEDS=5` against
`animus-test`'s `txn_serializable` corpus, and `animusd`'s whole
`dynamo_txn*`/`txn_recovery_participant_spans`/`dynamo_index_writes`/
`dynamo_streams` suites, stayed green unmodified.

**One pre-existing `animusd` regression retired, not adapted**: `lib.rs`'s
`issue_412_tests::txn_prepare_pushing_retries_a_leader_moved_read_failure_
to_success` asserted `ClientCtx::txn_prepare_pushing`'s retry loop around a
leader-side read failure inside `eval_kind_txn_write` — a mechanism this
step deletes outright (there is no leader-side read left to fail). No
replacement was written: the property it protected (a transactional
write's own condition/update evaluation retrying a transient failure) has
no analogue in the new design, since evaluation now happens deterministically
inside apply with no read that can fail independently of the entry's own
commit. The module doc records why.

**What did NOT move.** `BatchWriteItem`, the TTL reaper, and the admin
seeder still route through `kind_write_item_at_leader` (step 3, unchanged)
and are unaffected by this step. Step 3's own seatbelt double-check
(`rmw_lock`, `predict_kind_eval_decision`, `Metric::
KindEvalSeatbeltMismatch`) and the now-provably-dead `KindBatch.conditions`/
`cp_kind_local` production path are step 4b's work, not this one's — this
step only moves the transactional producer; it does not yet delete anything
step 3 or earlier left behind.

Status stays **Proposed**: step 4b (deleting `rmw_lock`, the OCC seatbelt,
and the step-3 double-check; flipping this ADR to Accepted) is still ahead.
