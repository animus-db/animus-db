# ADR 0049 — The universal kind-write path: every tablet has a change log

- **Status:** Accepted — not yet implemented (delivery "Train A"; ADR 0050's
  split pivot depends on this ADR and lands as its own, later train).
- **Date:** 2026-08-16
- **Amends:** [ADR 0041](0041-materialized-secondary-indexes.md) (the
  `KindBatch` path stops being conditional on a table's index/stream
  declarations), [ADR 0042](0042-dynamo-streams.md)/[ADR
  0043](0043-stream-shard-subsystem.md) (the change log every streamed table
  already has becomes a property of every table; sealing/segments are
  unchanged), [ADR 0046](0046-tablet-log-model.md) (principle 2 — "everything
  asynchronous is a consumer or producer of the change-record stream" — is
  made universal instead of conditional), [ADR
  0006](0006-dual-cql-dynamo-adapters.md) (CQL writes migrate onto the kind
  path), [ADR 0018](0018-cross-tablet-transactions.md) (staging gains an
  image-less marker record; materialize-at-resolve is unchanged).
- **Depends on:** ADR 0041 (the `KindBatch` command and kind scopes), ADR
  0048 (the quiescence veto seam this ADR's trim obligation must respect).

## Context

Today a tablet has two write paths. A table with a stream or a secondary
index routes every mutation through `KvCommand::KindBatch`
(`table_takes_kind_write_path`): one Raft entry atomically materializes the
base row, every LSI row, and a change-log record whose HLC is completed at
apply (ADR 0041 §4a, ADR 0046 principle 1). Every other table — including
every CQL table — uses the plain `Put`/`Batch`/`Delete` commands and writes
no change record at all.

Two pressures make that conditionality the wrong shape:

1. **The split pivot (ADR 0050) needs a universal delta feed.** A copy-based
   split builds two children while the parent keeps serving; the build's
   live tail must observe every parent mutation. A tail that scans the
   parent's rows per pass costs O(range) per pass regardless of delta size;
   a tail that consumes a change log costs O(delta) — but only if the log
   exists on *every* table, unconditionally. The alternative — flipping a
   table onto the kind path only for the duration of a split — was examined
   and rejected during the pivot's fork review: a mid-workflow write-path
   mode flip has a propagation window during which stale-routed edges keep
   proposing plain writes that emit no records (silent copy holes), so the
   flip would have to be apply-time enforced *and* audited per write entry
   point, and the capture flag would need its own lifecycle (clear on
   cutover/abort, never leak). Making the path unconditional deletes the
   flip, the audit, and the lifecycle: **coverage becomes structural —
   there is one write path — instead of audited.**
2. **The conditionality is the last un-unified seam in the tablet log
   model.** ADR 0046 named the shape (a tablet is a log; async work consumes
   its change-record stream) but left it conditional on a table's
   declarations. Every consumer built since (GSI drain, streams sealer,
   backfill seeder) and every consumer named as future work (CQL CDC, ADR
   0042 §16) assumes the log; the split-build tail is the fourth consumer.
   A model where the log's existence depends on which features a table has
   enabled makes every new consumer start with "first, ensure the log
   exists" — a mode flip of exactly the kind rejected above.

## Decision

**Every table routes every mutation through the kind-write path.**
`table_takes_kind_write_path` becomes unconditionally true (and is then
inlined away); the plain-write proposers stop being reachable from any wire
adapter. Concretely:

### 1. Marker records for tables with no stream and no GSI

A change record exists to serve consumers. A streamed table's consumers
(the sealer; `GetRecords`) need both images; a GSI'd table's drain needs a
dirty-key signal (ADR 0041 §4's drain deliberately re-reads current base
rows rather than replaying images). A table with neither has no standing
consumer — but the split-build tail (ADR 0050) and any future consumer
still need the dirty-key signal.

So a table with no stream and no GSI writes an **image-less marker record**:
the ordinary `KIND_CHANGE` key (`token || escape(pk) || hlc::pack(ts)`, HLC
completed at apply exactly as today) with a payload carrying no before/after
images — the same shape ADR 0045's backfill seeder already writes
(`seeded: true`, no images), which the drain machinery already consumes.
Overhead per write is a fixed few tens of bytes riding the *same* Raft
entry — no extra fsync, no extra round trip. A table that later enables a
stream starts writing full-image records from that point (unchanged from
today: a stream's records begin at `CreateTable`-with-stream or
`UpdateTable`-enable; markers are never retroactively upgraded).

The record-shape rule, stated once: **images follow the stream declaration;
the record itself follows nothing — it always exists.**

### 2. CQL writes migrate onto the kind path

CQL tables today use the plain proposers. They move to `KindBatch` with
marker records (CQL has no LSI/GSI/stream surface yet, so there is no
derived state to evaluate). Because there is nothing to read-before-write,
CQL writes do **not** need the ADR 0046 Decision-1 evaluate-at-leader
funnel — no extra hop, no `rmw_lock`; the edge builds the physical rows
(base + marker) directly, exactly as cheap as the plain path it replaces.
This incidentally does most of the mechanical work for the parked CQL CDC
item (ADR 0042 §16): the log CDC would consume now exists.

### 3. Transactional staging emits a stage-marker record

ADR 0046 Decision 2 stands unchanged: a *derived* row or change record is
never staged as an intent in a kind scope; kind scopes hold only committed
values; a transaction's images materialize at `TxnResolve` at the resolving
entry's own commit HLC (`materialize_derived`).

What this ADR adds is one marker: **`KvCommand::TxnStage`'s apply arm also
writes an image-less marker record** (same shape as §1's, flagged as a
stage marker) for the anchor key it stages. The marker is a dirty-key hint,
not an image — consumers that re-read rows (the drain pattern; ADR 0050's
split-build tail) observe "this key changed state" and re-read whatever is
physically there, intent envelope included. This closes the one hole the
split pivot's fork F7 found: an intent staged *after* a bulk copy pass has
passed its key would otherwise be invisible to a change-log tail until
resolve — which may happen after the parent is gone. Consumers that replay
images (the sealer) skip stage markers exactly as they skip the backfill's
seeded records today: no images, nothing to seal into a segment's record
stream beyond what resolve later materializes. Per-item ordering (ADR 0042
§9) is unaffected: the marker's HLC precedes the resolve-materialized
record's HLC by construction (stage applies before resolve in the anchor's
own log).

### 4. Trim covers every table; consumers register terms

The hot-trim arm (ADR 0043 §A6, generalized from ADR 0041's trim janitor)
already implements the required policy: a tablet's `KIND_CHANGE` scope is
trimmed behind the minimum of its *expected consumer terms*, and **"zero
expected terms → trim everything"** already exists for the
`DISABLED`-stream case. A plain table has zero standing terms, so its
markers are transient by the existing rule — written, then promptly trimmed
on the same per-tablet loop. A consumer that needs the log holds a term for
exactly as long as it needs it: the stream watermark and GSI cursor as
today; ADR 0050's split-build driver registers a term for the duration of a
build (so trim can never outrun the tail) and drops it at cutover.

Two knock-on obligations, named:

- **The `change_consumer_loop` now ticks every led tablet**, not only
  streamed/indexed ones. Its idle-tick cost must stay compatible with ADR
  0048 quiescence: a tablet whose markers are trimmed and whose triggers
  are cold must veto nothing — the loop's existing cheap `approx_bytes_kind
  (KIND_CHANGE) == 0` gate is the idle fast-path, and the quiescence-veto
  interaction (a tablet trims its last markers, then quiesces) gets an
  explicit test.
- **`expected_consumer_tags` semantics are unchanged** — this ADR adds no
  standing term to any table; it only makes the log's *existence*
  unconditional. The trim rule's authority stays the catalog/cursor state
  it reads today.

### 5. Bench gate

The plain-table hot write path changes (one extra small row per write,
same entry). Train A lands with a before/after write-path benchmark on a
non-streamed, non-indexed table (the `engine_bench` harness shape), and the
delta is recorded in this ADR as an as-built amendment. The expectation is
low single-digit percent (marker bytes ride an entry whose fsync and
replication round-trip dominate); if measurement contradicts that
expectation, that is a finding to bring back to review, not to absorb
silently.

## What deliberately does NOT change

- **`KindBatch`'s apply arm and `materialize_derived`** — one shared
  materialization function (ADR 0046's binding consequence) gains a caller
  pattern, not a sibling copy.
- **Sealing, segments, the `SegmentStore`, shard lineage** (ADR 0043) — a
  stream is still a seal epoch of the table's own log; only *which tables
  have a log* changes.
- **The GSI drain, backfill seeder, and their cursors** — untouched;
  markers on never-indexed tables are invisible to them (no `"gsi"` term
  exists to hold trim).
- **LSI synchronous semantics** (ADR 0041) and **materialize-at-resolve**
  (ADR 0018/0046) — unchanged; §3's marker is a hint beside them, never a
  second materialization path.
- **The plain `Put`/`Delete`/`Batch`/`Cas` `KvCommand` variants stay** —
  internal machinery (tests, admin/system paths, `SeedBatch`'s future
  sibling in ADR 0050) still uses them; they simply stop being what wire
  adapters propose for user tables.

## Consequences

- **Every tablet is a log with an observable change feed, unconditionally**
  — ADR 0046 principle 2 becomes a property of the system rather than of a
  table's feature declarations. The split pivot's tail (ADR 0050), and any
  future consumer (CQL CDC first among them), starts from "consume the
  log," never "first create the log."
- **The rejected alternative is recorded**: build-scoped change capture (a
  per-split mode flip) fails on the flip's propagation window and the
  per-entry-point audit burden; freeze-early (no tail at all) fails on
  write-unavailability proportional to tablet size. Both were examined in
  the 2026-08-16 fork review and rejected in favor of always-on.
- **Costs, stated honestly:** a permanent, small, bench-gated per-write
  overhead on tables that never enable any consumer; trim work on every
  table's loop tick (bounded by the zero-terms fast path); a CQL write-path
  migration (mechanical — no derived state — but touching every CQL write
  test); and one more record shape (the stage marker) for consumers to
  classify, with the classification enforced the same way
  `every_known_cursor_tag_prefix_is_classified` already enforces cursor
  tags.
- The `KIND_CHANGE` scope exists on every tablet from birth. Snapshot
  images (`engine_image`) already iterate `ALL_KINDS` generically; no codec
  change is required by this ADR (ADR 0050's storage rework is where the
  codec moves).

## Testing plan

- Per-entry-point marker emission: dynamo writes (plain and RMW), CQL
  writes, `TxnStage` (stage marker), `TxnResolve` (materialized record
  ordering after the stage marker), the admin seeder — each asserted to
  produce exactly one record per mutation with apply-time HLC.
- Trim of a consumer-less table: markers written under load are trimmed by
  the existing zero-terms rule; `KIND_CHANGE` returns to empty; the tablet
  then quiesces (ADR 0048 veto interaction, explicit test).
- A table enabling a stream mid-life: records switch from marker to
  full-image at the declared point; the sealer's first segment contains no
  marker-era images (the AWS contract: a stream begins at enable).
- The write-path bench gate (§5), recorded as an as-built amendment.
- Corpus-level: the existing stream/backfill corpora run unchanged
  (streamed tables' behavior is identical); a new corpus dimension drives
  plain-table marker load under fault injection to prove trim/quiesce
  liveness.
