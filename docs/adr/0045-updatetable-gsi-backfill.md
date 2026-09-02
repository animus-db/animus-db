# ADR 0045 — `UpdateTable` GSI add/drop on a populated table, with drain-driven backfill

- **Status:** Accepted — implemented (8-PR stack). PR map: PR1 `IndexDef.status`
  + `SetIndexStatus` catalog plumbing → PR2 `Metadata::index_backfill` +
  `MarkIndexBackfilled` + the completion aggregator (`index_backfill_loop`) →
  PR3 the backfill seeder arm → PR4 split-during-backfill fault-injection
  corpus → PR5 the drop-index cascade → PR6 `UpdateTable` Create +
  `DescribeTable` reporting + read-path gating → PR7 Console surfacing → PR8
  this ADR.
- **Date:** 2026-08-15.
- **Amends:** [ADR 0041](0041-materialized-secondary-indexes.md) (closes its
  §5 deferral — "adding or dropping an index on a populated table … is
  deferred to a follow-up"; the backfill mechanism is, as §5 predicted, a
  reuse of the GSI drain rather than a new one), [ADR 0013](0013-replicated-schemas.md)
  (`IndexDef` gains a replicated `status` field), [ADR 0044](0044-split-only-tablets.md)
  (a post-split child's backfill posture rests on tablets never merging back
  together — see §5).
- **Depends on:** [ADR 0041](0041-materialized-secondary-indexes.md) (the
  per-tablet change log and derivative drain the seeder rides verbatim),
  [ADR 0042](0042-dynamo-streams.md)/[ADR 0043](0043-stream-shard-subsystem.md)
  (`Metadata::stream_shards`'s per-tablet-catalog-row/first-committer-wins/
  control-leader-aggregator shape is copied for `index_backfill`; `cursor.rs`'s
  `KIND_CURSOR` scope and tag convention host the new backfill cursor
  alongside the existing HLC watermarks), [ADR 0018](0018-cross-tablet-transactions.md)
  (HLC timestamps stamp a seeded change record identically to a live one).

## Context

ADR 0041 built materialized secondary indexes — a GSI as a hidden table
maintained asynchronously by a derivative drain over a per-tablet change log
— but explicitly deferred one piece: adding or dropping an index on a
**populated** table. Until this ADR, every index had to be declared at
`CreateTable` time, when the table is empty by construction, so there was
never any pre-existing data to catch up on.

`UpdateTable`'s `GlobalSecondaryIndexUpdates` closes that gap. Real
DynamoDB's own answer is well known: a newly added GSI enters a `CREATING`/
`Backfilling` state, is asynchronously populated from every existing item,
and becomes queryable only once that backfill completes. This ADR is the
mechanism that reproduces that behavior here — and, per ADR 0041 §5's own
prediction, it does so almost entirely by reusing machinery that already
exists: the change log, the derivative drain, and the per-tablet-catalog-row/
control-leader-aggregator shape ADR 0042/0043 already established for stream
shards.

## Decision

**Backfilling a newly declared GSI is the ADR 0041 drain applied to every
pre-existing key instead of one freshly written key — a leader-local seeder
arm forward-sweeps a tablet's base rows once, seeding one synthetic
change-log record per partition so the ordinary drain reconciles it exactly
as if a live write had touched it. No new write path into `reconcile_partition`
at all; backfill's only job is *coverage*, not correctness.**

### 1. The status lifecycle

`IndexDef` (`animus-control::schema`) gains `status: IndexStatus`, one of
`Creating | Active | Deleting`:

- **`Creating`** — declared but not yet fully backfilled. Writes made *since*
  declaration are already covered regardless of status (§2); rows that
  predate declaration may not be materialized yet. The drain still maintains
  it (excluded only once `Deleting`), so it is never left further behind
  while backfill catches up.
- **`Active`** — fully backfilled and queryable.
- **`Deleting`** — being torn down. The drain/seeder stop touching it; its
  hidden table is being reclaimed. Never observed by a query (rejected at the
  wire edge before it can matter).

A just-created table's indexes are constructed `status: Active` directly —
they are empty by construction (ADR 0041 §5), so there is nothing to
backfill and no reason to pass through `Creating` at all. Only an
`UpdateTable`-added index on an already-populated table starts `Creating`.
`#[serde(default = "IndexStatus::active")]` on the field means this only
matters for deserializing a status-less fixture (no live deployments exist
to migrate, per root `CLAUDE.md`); a status-less record predates this ADR
entirely, so it was never actually mid-backfill and `Active` is the correct
default, not merely a convenient one.

Status transitions through a new `MetaCommand::SetIndexStatus { table,
index, status }`, mirroring `SetTableMode`'s minimal shape: rejects an
absent table or index, no-ops if already at the target, and otherwise
mutates in place via a new `TableSchema::set_index_status` — deliberately
**not** `upsert_index`'s whole-struct replace, so a status transition can
never clobber a concurrently-updated copy of the rest of the definition.

### 2. Why no backfilled record is ever lost or double-applied

This is the load-bearing fact the whole design rests on, and it was already
true before this ADR shipped a single line of new code: `animusd::dynamo`'s
`table_takes_kind_write_path` gates on index/stream **presence**, never
**status**. The instant `CreateTableIndex{status: Creating}` commits, every
write to that table — regardless of the new index's own status — already
produces a genuine change-log record marking its partition dirty. **No write
made after the index's declaration can ever be missed.** Backfill's entire
remaining job is covering rows that existed *before* declaration.

- **Nothing is lost**: every partition that ever held a row gets at least
  one dirty-marker after `Creating` commits — from a live write (unconditional
  on status) or from the seeder's own forward sweep (§3). A row inserted into
  a partition the seeder has already passed is covered by the live-write
  path; one the seeder hasn't reached yet is covered on arrival, or
  redundantly by both — harmless.
- **Nothing is double-applied**: this is impossible by construction, not by
  careful bookkeeping. `reconcile_partition` (ADR 0041 §4) never reads a
  change record's *content* — it re-derives the desired index rows from a
  live scan of the partition's *current* base rows, every time it runs. N
  dirty-markers for one partition collapse into "reconcile once more against
  current state," which is idempotent. Backfill introduces zero new writes
  into that function; it only ever produces more of the same kind of signal
  the drain was already built to consume.
- A live write racing the seeder's scan front is never a special case:
  whichever value `reconcile_partition` observes when it runs is the correct
  thing to materialize. Redundant reconciliation is a wasted tick, never a
  wrong answer.

This is why the mechanism needed **zero new `ClientRequest` variants** for
its steady-state operation — the seeder is leader-local (an arm of the
existing per-tablet `change_consumer_loop`) and the completion aggregator
proposes only `MetaCommand`s. (One narrow, internal-only exception exists on
the drop path — §6.)

### 3. The seeder mechanism

A new arm of `animusd::index_drain::change_consumer_loop` — the **backfill
seeder** — runs on each tablet's own current leader, the identical placement
discipline as the GSI drain and seal arms (not the tablet-host reconciler).
For any table with at least one index in `Creating` status, on a tick where
this tablet leads:

1. **Cursor.** Read a per-index backfill cursor: the existing `KIND_CURSOR`
   scope and `cursor::cursor_key` builder, with a new tag convention
   `format!("backfill:{index_name}")` — a sibling of the existing HLC
   watermark rows (`"gsi"`, per-stream tags), but storing a **raw
   last-seeded base-key prefix**, not a packed HLC. `cursor.rs`'s module doc
   documents the two value conventions side by side.
2. **Partition enumeration.** Scan the tablet's `KIND_BASE` scope forward
   from the cursor, enumerating distinct partition-key prefixes using the
   same "bump the last byte" skip-ahead trick `reconcile_partition` already
   uses — one iteration per partition, not per item (footprints are
   partition-scoped, ADR 0041 §4).
3. **Seed.** For each newly discovered partition, bounded to
   `BACKFILL_SEED_BATCH` (256) per tick, propose a `KvCommand::KindBatch`
   carrying **only** a change-log entry for that partition's footprint-key
   prefix — no base-row write. Apply stamps a fresh `hlc::pack(ts)` exactly
   as it does for any live write's change record, so the seeded record is,
   by construction, indistinguishable from one a live write would have
   produced, and lands ahead of the `"gsi"` cursor watermark for the
   ordinary drain to pick up with **zero changes to
   `drain_tablet`/`reconcile_partition`**.
4. **Advance the cursor** to the last partition prefix seeded.
5. **Report completion.** Once a tick's scan reaches the end of the tablet's
   *current* range, propose `MetaCommand::MarkIndexBackfilled` (§4) — and
   keep re-deriving and re-proposing this on every later tick rather than
   treating it as a one-shot side effect, so a crash/restart or a later
   split (§5) is handled by the same code path, not a special case.

`change_consumer_loop`'s `gsis` filter — which arm of the drain runs at all
— was widened from "every `Global` index" to indexes with status in
`{Creating, Active}` (excluding `Deleting`), one line, so a backfilling
index keeps being drained exactly like an already-`Active` one throughout.

**Confirming a seed write needed a new primitive, not a reused one.** The
drain's existing confirm helpers (`cp_kind_write_raw`'s last-write probe,
`cp_kind_local`'s base-row probe) all poll for an exact `(kind, key,
expected value)` the *caller* chose before proposing — sound only when the
caller can predict the write's own key ahead of time. A change-log record's
key ends in an HLC suffix minted *inside* the propose call, under the
group's own lock, specifically so it agrees with the entry's log position —
there is structurally nothing to predict and poll for. The seeder confirms
by **applied index** instead: `engine_applied_index() >= index` after a
genuine `ProposeResult::Accepted { index }`, the same confirm-by-index
primitive linearizable reads themselves already gate on. `MarkIndexBackfilled`
at the end of a range tolerates outright rejection (an already-dropped
tablet racing the proposal, §4's `MarkIndexBackfilled` apply-arm guard) —
the seeder does not treat that as an error, since the tablet it was
reporting for no longer needs a completion signal at all.

### 4. Completion aggregation and the `Active` flip

A new replicated catalog, `Metadata::index_backfill: BTreeMap<(TabletId,
String), ()>`, mirrors `Metadata::stream_shards`'s shape exactly: "this
tablet finished seeding change-log coverage for this index," keyed
`(tablet, index name)` — a tablet id already implies its table, so `table`
is not part of the key. The value is always `()`; presence alone is the
fact. It rides the same `serde_json` tuple-map-key workaround `stream_shards`
needed (a `(TabletId, String)` key cannot serialize as a JSON object key at
all — `MapKeySerializer` errors "key must be a string" the moment the map is
non-empty), encoded via `#[serde(with = "index_backfill_codec")]` as a flat
array of `{tablet, index}` objects. The populated-map JSON round-trip test
was written from day one, per the `stream_shards` precedent's own logged
lesson.

`MetaCommand::MarkIndexBackfilled { table, index, tablet }` is an idempotent
insert, proposed by the seeder (§3 step 5). It rejects an absent table, an
index name that isn't one of the table's current indexes, and — beyond what
`stream_shards`' own dual needed — **a tablet not currently scoped to the
table**, via a cheap `Metadata::tablets` lookup. This last check is not
merely defensive: without it, a command landing *after* its own tablet has
already been dropped (a table/tablet-drop race with an in-flight seeder
proposal) would insert a permanent orphan row that `DropTableTablets`'s own
prune has already run past and will never revisit.

A new **control-plane-leader-only** loop, `index_backfill_loop`
(`animusd::index_backfill`), owns the flip: each tick, for every table with
an index currently `Creating`, if the table currently has **at least one**
tablet and **every** tablet currently in a fresh read of
`Metadata::tablets_for_table(table)` has a matching `index_backfill` row,
propose `SetIndexStatus{Active}`. The **≥1-tablet guard is load-bearing, not
defensive boilerplate**: an empty tablet set is vacuously "every tablet
reported" under a naive `.all()`, which would flip a table's index to
`Active` before its first tablet has even been provisioned — a real bug
caught before ship, not a hypothetical. Spawned unconditionally on every
node shape that can become control leader (combined and control-only), the
same "run everywhere, self-gate on `leader_handle()`" idiom
`segment_janitor_loop`/`detect_loop`/`orphan_sweep_loop` already use — never
on a data-only node, which never registers a local control `RaftNode` at
all. Unlike the segment janitor, this loop touches **only** replicated
`Metadata` (a tablet-map read, a schema-catalog read, a `MetaCommand`
proposal) — no `SegmentStoreHandle`, no data role of any kind — so it has
**no** control-only-leader scope gap: a pure control-only leader drives the
flip exactly like a combined-node leader does. This was explicitly proven,
not assumed, by `tests/index_backfill.rs::control_only_leader_drives_the_flip`
(ADR 0043 §A9's segment-janitor phases 2/3 do have such a gap, for the
opposite reason — they need a data role's `SegmentStoreHandle`; this
aggregator was checked against exactly that precedent and found to not
inherit it).

`index_backfill` rows are pruned on both drop paths: `DropTableTablets`'s
apply arm retains only rows whose tablet id wasn't dropped; `DropTableIndex`'s
apply arm retains only rows that don't match the dropped index name scoped
to that table's own current tablets (never a bare "match on index name
alone" — a distinct table could happen to declare a same-named index, and
its rows must not be swept up by an unrelated table's drop).

### 5. Split behavior during backfill

Kind scopes share one tablet's `KeyRange`, so a split's `narrow_scope` moves
every kind together — but the **cursor row's key** is
`cursor_key(range.start, tag)`: the **left** child keeps `range.start` (its
cursor is found, resumes); the **right** child's `cursor_key(new_start, tag)`
reads empty ("never started").

**Fork A (Guillaume, "go as recommended"): no split-lineage-aware cursor
inheritance.** A post-split right child simply restarts its own, narrower
scan from the beginning, rather than mirroring ADR 0043 §A4's parent-
watermark-inheritance shape. This is unconditionally correct by §2's own
idempotence argument — a redundant reconciliation is never wrong, only
wasted work — and the extra cost is **geometrically bounded**: each split
child's own range is strictly narrower than its parent's, so a chain of
splits during one backfill converges the same way a geometric series
converges, never re-scanning an unbounded amount of data. This is recorded
here as a **deliberate, named simplification**, not an oversight; split-
lineage-aware inheritance remains a later optimization (§7), never a
correctness requirement. Because tablets are split-only (ADR 0044) — a split
child's range can never later widen back out through a merge — the
geometric bound is permanent once a split commits, not merely true at the
instant of the split.

Completion (§4) is evaluated per-tablet against each tablet's **current**
range every tick; a split arriving after a tablet has already reported
"done" reintroduces its two not-yet-done children into the completion set on
the aggregator's very next tick — never a premature `Active` flip. This is
the same live-tablet-map-every-tick discipline the segment janitor already
relies on (ADR 0043 §A9).

**The cursor-persistence fence bug this uncovered, and its fix.** The
seeder's cursor-advance write initially reused `cp_kind_write_raw`'s
auto-derived fence — always the tablet's own *current live* range. That
fence rejected the cursor's own persistence write on **every** real split,
at every seed, with no fault injection needed: `cursor::cursor_key`
truncates its `range_start` argument to a bare token (sound for the key's
own disjointness from real client data, unsound for range-*containment*,
since a split's own `split_key` is essentially never token-aligned) — so the
truncated cursor key sorts *below* a right child's own, longer `range.start`
the moment the byte right after the token is non-zero, true of essentially
any real partition key's leading escaped byte. The seed *data* writes (keyed
by real base keys, which do satisfy the fence) kept succeeding while only
the cursor's own persistence silently failed, masking the bug behind a
liveness symptom rather than a loud error: the sweep would restart from
scratch every tick instead of resuming, invisible in any fixture small
enough for one tick's `BACKFILL_SEED_BATCH` to cover a whole side in one
pass regardless. `animus-test`'s `backfill_fault_corpus.rs` found it on its
very first run of a split-during-backfill cell. The fix,
`advance_backfill_cursor`, gives the cursor's own write a dedicated path —
`group.put_kind_batch_fenced(.., KeyRange::whole())` — bypassing the
auto-derived narrow fence entirely: a cursor row's identity is already fully
captured by its own token (disjoint from base data by row-kind, ADR 0041
§3) and immutable across a narrowing, so it needs no range-fencing at all.
This is the same reasoning `seal.rs`/`ceiling.rs`'s engine-global markers
already rely on for a different flavor of range-independent bookkeeping
key. One direct, load-bearing consequence: this fix is also what makes
left-child resumption after a split actually *work* (rather than merely be
harmless to skip) — before it, a left child's own cursor-resume write would
have hit the identical fence rejection the moment its own range narrowed.

### 6. The drop-index cascade

`DropTableIndex`'s pre-existing single atomic removal is unsafe alone for a
`Creating`/backfilling index: the hidden table's tablets need reclaiming,
and the whole-table-drop cascade (ADR 0041's "as-built corrective note")
doesn't apply to a single-index drop. `dynamo.rs::drop_index` is that
cascade's single-index sibling — the `UpdateTable` `GlobalSecondaryIndexUpdates`
`Delete` path — a **four-step convergent sequence**, each step independently
idempotent so a crash between any two resumes correctly on retry:

1. `SetIndexStatus{Deleting}` — the `gsis` filter (§3) excludes `Deleting`,
   so the drain/seeder stop touching the index from their very next tick,
   before anything is torn down.
2. `MetaCommand::DropTableTablets` scoped to the index's own hidden table
   (`index_table_name`) — the exact primitive `ClientCtx::drop_table`'s own
   GSI cascade uses, just for one index instead of every one.
3. `DropTableIndex` — removes the catalog definition (also prunes
   `index_backfill` rows for that index, folded into its own apply arm —
   §4).
4. A belt-and-suspenders re-scan mirroring `drop_table`'s own defense: the
   drain provisions a hidden table's first tablet lazily and can race step
   2's drop, re-creating it after that step's own commit-wait already
   observed zero tablets. A final sweep for a tablet still named exactly the
   hidden table's name catches that race and drops it too.

**Deviation from the original plan's "zero new `ClientRequest` variants"
claim.** §2 established that steady-state backfill needs none — but the
drop path uncovered a real gap the plan hadn't accounted for: the seeder's
own backfill cursor (`KIND_CURSOR`, tag `backfill:{index_name}`) is keyed
purely by index **name**. Left alone across a drop, that row survives
exactly where it was; harmless in isolation (it just names a scan position
for an index that no longer exists), until a **later** `CreateTableIndex`
proposes a *new* index under the *same name* — its fresh seeder reads the
stale row, believes it has already scanned up to the deleted index's old
position, and skips every partition before that point. The recreated index
can flip `Active` having backfilled **nothing**: a silent, non-crashing
correctness bug, not a crash. The fix is a new internal-only
`ClientRequest::ClearBackfillCursor { tablet, index }` (refused bare,
`ForceSeal`-shaped — real handling only inside `cp_serve_forwarded`) and
`ClientCtx::clear_backfill_cursor_for_table`, run **twice** around the
cascade: once immediately after step 1 (the index's `Deleting` transition
has already committed, so the seeder's own gate excludes it from every *new*
tick going forward) and once more at the very end, alongside step 4. This
closes the practical race window — a seeder tick that had already read the
schema (as still-`Creating`) a moment before the `Deleting` transition
landed could still write a fresh, stale value after the first clear — but
**this is a documented residual posture, not a proven-closed one**: running
the delete twice narrows the window without a formal proof it is zero,
matching the same posture `cursor.rs`'s own module doc already takes for a
different, unrelated byte-alignment gap. The `create_drop_recreate_
same_index_name_backfills_from_scratch` regression is the test that would
catch a regression here; it and the family of drop tests around it pass
reliably.

### 7. Wire semantics

`animus_dynamo::wire::decode_update_table` decodes `GlobalSecondaryIndexUpdates`
into `Operation::UpdateTable.index_update: Option<IndexUpdate>` — exactly one
element, `Create(SecondaryIndex)` or `Delete(String)` only (no throughput
`Update`; a `LocalSecondaryIndexUpdates`-shaped element is rejected at the
`animusd` edge, since LSIs are create-time-only in real DynamoDB).

**Fork C (Guillaume, "go as recommended"): an index change and a stream
change in the same `UpdateTable` call are mutually rejected** with a
`ValidationException` — "UpdateTable supports either a
GlobalSecondaryIndexUpdates change or a StreamSpecification change in this
adapter." This keeps "exactly one supported change per call" as a single,
simple invariant rather than reasoning about their interaction; see the
deviations table below.

`animusd::dynamo::update_table` dispatches: `Create` validates client-side
(duplicate name; a name colliding with the reserved namespace or containing
the hidden-table name separator; `Local` kind rejected as defense-in-depth,
since the wire decoder never actually produces one for an add), bridges via
the existing `schema_bridge::index_to_control` **overriding `status` to
`Creating`**, and proposes `CreateTableIndex` with a **presence-by-name**
commit-wait — deliberately not "status == `Creating`" (see the as-built
note below) — no `provision_tablet` call, since the drain lazily provisions
the hidden table's first tablet the same way it always has. `Delete` runs
§6's cascade.

`describe_table`/`index_desc` thread each index's real status through a
**side channel** kept off `SecondaryIndex` itself (Fork D, Guillaume, "go as
recommended") — `wire::describe_table_response`'s new `index_statuses`
parameter, mirroring the existing `StreamDescription` separate-bridge
precedent — so `SecondaryIndex` stays a pure `CreateTable`-input shape while
`DescribeTable` reports real `CREATING`/`ACTIVE`/`DELETING` plus, correctly
placed **inside each `GlobalSecondaryIndexes[]` entry** (see the as-built
note below), `Backfilling: true` while `Creating`. `run_index_query`/
`run_index_scan` reject `Query`/`Scan` against a non-`Active` GSI with
`ValidationException`, beside the existing `ConsistentRead`-against-a-GSI
rejection.

**Deviations from AWS, summarized** (format per ADR 0042 §15):

| Area | Real DynamoDB | This adapter | Why |
|---|---|---|---|
| Index change + stream change in one `UpdateTable` call | Both may be requested together | Rejected outright with `ValidationException` | Keeps "exactly one supported change per call" a single invariant (Fork C) rather than reasoning about the interaction of two independent async lifecycles in one commit |
| `GlobalSecondaryIndexUpdates` array | Accepts multiple elements per call | Exactly one element per call | Matches this adapter's existing single-change-per-`UpdateTable` posture; no observed real workload needs more |
| Throughput / `ProvisionedThroughput` on an index update | A real, billed dimension | Not modeled at all — silently absent from the decoded shape | No capacity model anywhere in this adapter (pre-existing, table-wide deviation, not new here) |
| LSI add/drop via `UpdateTable` | Does not exist in real DynamoDB either | Rejected identically (`ValidationException`) | Not a deviation — matches AWS's own invariant; stated for completeness |
| `Backfilling` placement | Per-index, inside each `GlobalSecondaryIndexes[]` entry; absent once finished | Same — corrected from an earlier internal design sketch that had it table-level (see as-built note) | Verified against the real API shape, not an internal doc's paraphrase of it |
| Backfill progress observability | Internal, not exposed via any documented API | Console-only: "N of M tablets seeded," from `Metadata::index_backfill` | A real, non-fabricated fact this adapter happens to have on hand; not a wire-API surface |

### 8. Console surfacing

`dashboard_browser.js`'s index table gets a real **Status** column reading
`IndexDef.status` off the replicated schema JSON: `Creating` renders as the
same neutral "forming" pill the Tablets view already uses for its own
transitional state (ADR 0021 §7's "health means redundancy loss, not
transition in progress" principle, reused rather than inventing a second
severity vocabulary), alongside "backfilling — N of M tablets" derived from
`Metadata::index_backfill`'s own per-tablet rows — **no fabricated
percentage**, since the tablet-count ratio is the only real progress fact
available. `Deleting` renders dimmed. The index-selector dropdown used by
`Query`/`Scan` disables any non-`Active` index with a status suffix
("— backfilling" / "— deleting") explaining why, rather than letting a
selection reach the read-path rejection (§7) with no visible reason.

Beyond the original plan's scope, PR7 added a scope addition: a "+ Add
index" (GSI-only) form and a per-row "Drop" action (behind
`window.confirm`, matching the existing destructive-action pattern used
elsewhere in the Data Browser) — both driving `UpdateTable` through the
existing `/admin/data/dynamo` proxy, no new admin route.

## As-built notes: three plan claims corrected during implementation

The original plan (see the PR8 task brief; the plan document itself is
`/home/guillaume/.claude/jobs/c0a0e7ef/tmp/updatetable-backfill-plan.md`)
got three specifics wrong or incomplete relative to what actually shipped —
recorded here rather than silently, since a design doc lagging the
mechanism is exactly the failure mode this repo's own engineering practices
warn against:

- **The `Metadata`-mirror match arm for `SetIndexStatus`/`MarkIndexBackfilled`
  was not itself in the plan** — `animus-control::mirror`'s
  `apply_and_derive_mirror` is an exhaustive `match` with no wildcard, by
  design, so the compiler refused to build until both new `MetaCommand`
  variants got a deliberate mirror decision. This is the general "a new
  field on a replicated state machine is not complete until it's mirrored"
  lesson already logged in `docs/engineering-lessons.md`, now with a fresh
  instance: `EntityKind::IndexBackfill` plus both directions of
  `apply_key_write` were added alongside the field itself, not as an
  afterthought.
- **`DescribeTable`'s `Backfilling` flag was sketched as table-level** ("any
  index `Creating`") in the plan's §6. Real DynamoDB places `Backfilling`
  **per-index**, inside each `GlobalSecondaryIndexes[]` entry, present only
  while that specific index is backfilling — absent, never `false`, once
  finished. Building to the plan's wording as written would have shipped a
  plausible-looking but wrong wire shape with no test catching it (every
  test in the same PR would have been written against the same wrong
  premise). Caught by re-deriving the real API shape independently instead
  of trusting the plan's paraphrase of it — now the shipped, tested
  behavior, and its own logged lesson (`docs/engineering-lessons.md`)
  generalizes the rule: an internal design doc is a plan, not a spec of the
  external contract it merely describes.
- **The plan's "zero new `ClientRequest` variants" claim held for
  steady-state backfill (§2) but not for the drop path.** §6's
  `ClearBackfillCursor` is the one exception, discovered only once the
  create-drop-recreate scenario was actually tested — a real, if narrow,
  deviation from the plan's stated design win, recorded rather than quietly
  absorbed.

## Named follow-ups

- **Phantom no-image seed records on a table streamed mid-backfill.** A
  seeded change-log record carries no old/new item image (§3 step 3 writes
  only a footprint-key change-log entry, deliberately, since the seeder's
  whole point is *coverage*, not replaying content). If DynamoDB Streams is
  enabled on a table **while** a GSI backfill is still in flight, each
  pre-existing partition's seed record surfaces as one phantom, no-image
  event in a later `GetRecords` call — a known, accepted low-fidelity
  interaction between two independently-shipped features, not a regression
  in either one alone. Closing it would mean either giving the seeder a real
  base-row read (turning a cheap partition-prefix sweep into a full-item
  scan, for a case that only matters when both features are combined) or
  teaching the Streams read path to filter seed-originated records — neither
  attempted here.

  **Update: closed by the "E1 mark + filter" fix (2026-08-16).** A
  `#[serde(default)] seeded: bool` field on `animus_dynamo::ChangeRecord`,
  set `true` only at `backfill_seed_tick`'s own record-construction site;
  both `dynamo_streams.rs::get_records_sealed`/`get_records_open` filter it
  out via one shared predicate (`is_seeded`) before a record ever reaches
  `stream_record_json` — a fully-filtered page still yields a well-formed
  `GetRecords` response (empty `Records`, a live `NextShardIterator`), since
  both branches already derive `next_position`/`exhausted` from the
  pre-filter page. The GSI drain is untouched: it never reads the flag (or
  any record content), exactly per this ADR's own "coverage, not replay"
  argument above. **Deliberately not fixed by giving the seeder a real
  base-row image instead** — that was the other option named above, and it
  would have been the *wrong* fix, not just a costlier one: real DynamoDB
  emits **no** stream event at all for a GSI backfill's own coverage sweep
  over pre-existing data, so synthesizing an image would fabricate an event
  no real client would ever see — a fidelity regression relative to AWS's
  own contract, not an improvement. Filtering the marker out is the
  fidelity-*correct* fix (matching what DynamoDB actually does), independent
  of which option happened to be cheaper. Regression:
  `animusd/tests/stream_backfill_seed_filter.rs` (a real `UpdateTable`-
  triggered backfill on a streamed, populated table, concurrent live writes
  racing it, `GetRecords` drained to convergence — zero phantom-shaped
  events, every real write delivered exactly once) and
  `animus-test/tests/backfill_fault_corpus.rs::
  streamed_mid_backfill_seed_flag_never_misclassified` (the flag's own
  correctness under adversarial seeder/live-write interleaving, at
  `ANIMUS_BACKFILL_SEEDS` depth).
- **Split-lineage cursor inheritance (§5 Fork A) as a pure optimization.**
  The current restart-from-scratch behavior is unconditionally correct and
  geometrically bounded; inheriting a parent's cursor position (ADR 0043
  §A4's shape) would only shrink the redundant-rescan cost further, never
  change a correctness property. Worth revisiting only if a workload with
  very frequent splits during a very large backfill makes the redundant
  work measurable.
- ~~`TxnStage` kind-writes~~ **Shipped (2026-08-16, ADR 0046 A1/U3)** —
  `TransactWriteItems` on an indexed or streamed table no longer rejects;
  see `docs/adr/0018-cross-tablet-transactions.md`'s 2026-08-16 amendment.
  Orthogonal to this ADR, confirmed at the time: a backfilling (`Creating`)
  index takes the identical kind-write-path as an already-`Active` one — a
  transactional write against a table with a `Creating` GSI stages and
  resolves the same LSI/change-log payload either way, and the backfill
  seeder's own synthetic change records (§2/§3) are unaffected (they never
  go through `TxnStage` at all).

## Consequences

**Easier.**

- A GSI added to a populated table behaves like DynamoDB's own
  `CREATING`/`Backfilling`/`ACTIVE` lifecycle, not a silent gap or a
  synchronous stop-the-world scan.
- The mechanism cost almost nothing new: no new row kind, no new write path
  into the drain, no new command for the steady-state case. Backfill is
  provably just "the existing drain, run over old data as well as new."
- The completion-catalog/control-leader-aggregator shape is now proven twice
  (`stream_shards`/`segment_janitor_loop`, and now `index_backfill`/
  `index_backfill_loop`) — a third such subsystem has a template to copy
  rather than a design to invent.

**Harder, and knowingly accepted.**

- **A fourth arm on `change_consumer_loop`** (GSI drain, seal, backfill
  seeder, hot-trim) is one more thing that must make progress under fault
  injection, on top of the three that already had to.
- **The drop path needed a genuinely new internal RPC** (`ClearBackfillCursor`)
  the original design didn't anticipate, and its own correctness posture is
  "residual race window narrowed, not proven zero" — an honest gap, not a
  silent one, but a gap nonetheless.
- **A table streamed mid-backfill would have seen phantom no-image
  records** — a real, named limitation at the time this section was
  written, but **closed by the "E1 mark + filter" fix (2026-08-16)**; see
  the "Named follow-ups" section above for the mechanism and its
  regression tests.
