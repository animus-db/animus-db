# ADR 0050 — Per-tablet physical storage and copy-based symmetric splits

- **Status:** Accepted — **implemented** (Train B, rungs 1–8; see the
  2026-08-17 as-built amendment at the bottom). **Supersedes [ADR
  0028](0028-shared-storage-single-command-split.md) in full** (both
  halves: the shared per-node engine and the metadata-only zero-copy
  split).
- **Date:** 2026-08-16
- **Amends:** [ADR 0002](0002-tablets-unit-of-placement.md) (tablet
  lifecycle: `Building`/`Splitting` states, retire-at-cutover), [ADR
  0034](0034-byte-based-auto-split.md) (the trigger/median/cooldown survive;
  what they *trigger* becomes a workflow), [ADR
  0043](0043-stream-shard-subsystem.md) (§A4's zero-copy split lineage —
  frozen basis, read-side range fence, seal range-CAS — is retired; sealing,
  segments, catalog, janitor survive), [ADR 0044](0044-split-only-tablets.md)
  (split-only stands; the cheap-groups roadmap's "storage side is already
  amortized" pillar reverses), [ADR 0046](0046-tablet-log-model.md)
  (principle 3 strengthens: no consumer offset ever crosses a split), [ADR
  0048](0048-cp-group-quiescence.md) (quiescence untouched; a retired
  parent's group is *removed*, which is strictly cheaper than quiesced; the
  `hot_read` scope-transition latch it shipped is retired with the residual
  it narrowed), [ADR 0029](0029-replica-rebalancing.md) (rebalance is frozen
  for non-`Active` tablets; a replica move ships a per-tablet engine), [ADR
  0018](0018-cross-tablet-transactions.md) (intents cross a split by being
  copied; resolves chase by key), [ADR 0024](0024-drop-table-data-gc.md)
  (drop-table GC becomes file deletion).
- **Depends on:** [ADR 0049](0049-universal-kind-write-path.md) (the
  universal change log the build's live tail consumes), ADR 0031 (the
  reconciler that gains the lifecycle arms), ADR 0022/0023 (tokens and
  table-scoped tablets, unchanged).
- **Decision record:** all nine forks (plus one sub-fork) were decided by
  Guillaume in the 2026-08-16 fork review; overrides of the written
  recommendations are marked below.

## Context

### Why move at all

Animus's split (ADR 0028) is a zero-copy, metadata-only range narrowing:
`SplitTablet` narrows the parent's range and mints a sibling over the same
physical rows on the same shared engine. It is instant and cheap — and it is
the single largest source of production bug families this codebase has had:
the split-watermark data loss (#216), the split-seal duplication (#220,
which needed *three* stacked defense layers: the frozen
`stream_split_basis`, the `in_declared_range` read-side fence, and the
apply-time `SealStreamShard` range-CAS), the `hot_read` staleness residual
that ADR 0048 could only narrow, the per-entry write fences and range seals
of ADR 0028 §3/ADR 0018 §2, and the cross-group LWW hazards that killed
tablet merge (ADR 0044). Every one of these defends the same seam: **two
live Raft groups sharing physical rows across an un-synchronized handoff,
observed through independently-lagging caches.**

DynamoDB — whose wire semantics this project already implements
byte-faithfully and whose split-only lifecycle ADR 0044 already adopted —
does not have that seam. An AWS partition split is a long-running background
workflow (the ATC '22 paper): the parent serves while two children are
built as copies, the write path cuts over, the parent retires. The "atomic"
shard close is a write-path cutover, not a distributed transaction. Guillaume
decided on 2026-08-16 to move Animus to that shape.

### The recon finding that forces the storage decision

Tablet identity does not appear in physical keys. A key is
`escape(table) || kind || token || escape(pk) || …`; a tablet's identity
lives *only* in its live-narrowable `range` (`StorageScope`, ADR 0028). Two
children of one table co-hosted on one node's shared engine would therefore
write **byte-identical physical keys** — "copy the parent's data into both
children" has nowhere to put the copies. Zero-copy was not merely convenient
under this layout; it was the only split the layout permits. The pivot
therefore forces an addressing change, and the fork review chose the
strongest form:

### Fork decisions (2026-08-16)

| # | Fork | Decided |
|---|------|---------|
| F2 | Storage shape | **Per-tablet physical engines now** ("rung 2" — override of the shared-engine-plus-key-prefix recommendation) |
| F2b | Keys inside a tablet's engine | `kind \|\| logical_key` — no table/tablet bytes; identity lives in the engine's file namespace |
| F3 | Seed transport | Drain through the child's own Raft log (`SeedBatch` merges at carried versions) |
| F4 | Live tail | Change-log consumer (ADR 0049) + one bulk scan pass |
| F5 | Child placement | **Fresh placement at mint** (override — placement engine picks final homes; one data movement) |
| F6 | Parent after cutover | Removed from the tablet map; the existing `Reclaim` arm tears down |
| F7 | In-flight transactions | Copy intents; resolves chase (no cutover veto, no force-aborts) |
| F8 | Availability | Bounded sub-second, retry-masked write blip at freeze→cutover; reads never pause |
| F9 | Lineage | New `split_lineage` map written at `CutoverSplit` apply; `split_parents` deleted |

## Decision 1 — per-tablet physical storage

**Each tablet a node hosts gets its own `StorageEngine`** (matching the
node's `StorageBackend` choice), opened by the reconciler when it hosts the
tablet and deleted when it reclaims it. Engine files live under a
per-tablet file namespace on the `Env` `Disk` seam — the same
naming-is-identity mechanism `raftkv.wal.<tablet>` already uses. Physical
keys inside a tablet's engine are **`kind || logical_key`**: the table
prefix and any tablet identity are gone from key bytes, because an engine
holds exactly one tablet of exactly one table.

What this dissolves (all recon-verified against main @ 4329824):

- **`StorageScope` in its entirety as a confinement mechanism.** No live
  `Arc<Mutex<KeyRange>>`, no `narrow`/`widen`, no `physical_bounds`
  prefix-upper-bound trick, no `strip_in_range`, no `prefix_for`
  construction site. A tablet's declared `KeyRange` still exists in
  `Metadata` (routing needs it) — but it is **immutable from birth**, and
  nothing physical is derived from it.
- **`erase_scope` → delete the engine's files.** Instant, real space
  reclaim (no `merge_tombstone` + compaction-GC wait). Drop-table GC (ADR
  0024) and split-retire teardown become the same file deletion.
- **`has_data` → "the engine exists / is non-empty";
  `approx_bytes` → exact engine file sizes** (ADR 0034's overlap-estimate
  bias and the confirm-step materialization shrink accordingly).
- The engine-sharing subtleties ADR 0028 documented — multiple
  independently-versioned writers, the `merge`-not-`delete_range` rule's
  cross-tenant rationale, the L0 overlap estimate — stop describing
  anything.

What returns, stated honestly (this is a reversal of ADR 0028's
consolidation): per-tablet memtable/compaction state and a per-tablet
engine WAL; cross-tablet write fsync amortization on one shared engine is
lost. The pathologies that originally drove ADR 0028 do **not** return:
they were the per-tablet `NodeId`/env minting (`Coresident`, the sibling
pool) and the two-phase split — both already dead via ADR 0026 stream
addressing and this ADR's own single-authority workflow. **Named
investigation item (Train B rung 1): the idle cost of a per-tablet
`LsmEngine`** — an idle engine must cost approximately nothing (memory and
background work), or quiescence (ADR 0048) is undermined from the storage
side; this is verified, not assumed, before the rung merges. ADR 0044's
cheap-groups roadmap is amended to carry storage in the per-group cost it
commits to keeping cheap.

The control plane keeps its own engine exactly as today (ADR 0038's
system keyspace is per-node and never was tablet-scoped). Dev-mode
`--cluster N` opens N×hosted-tablets engines in one process; per-engine
memory overhead is accepted pre-alpha and falls under the idle-cost item.

## Decision 2 — the copy-based split workflow

`Tablet` gains `state: {Active, Building, Splitting}` (serde-default
`Active` — the first per-tablet lifecycle state; the closest prior art is
`IndexStatus`, on indexes). Routing (`tablets_for_table` consumers) serves
only `Active` tablets; auto-split and rebalance skip non-`Active` tablets
entirely (a `Building` child is never moved mid-build; a `Splitting` parent
is never re-split).

### Stage 1 — `BeginSplit`

A new `MetaCommand::BeginSplit { parent, expected_epoch, split_key,
children: [(id, replicas); 2] }`, epoch-CAS-gated like every placement
mutation. The proposer computes the split key exactly as today (ADR 0034's
byte-weighted median; the F11 token-alignment rule for streamed tables is
kept — one partition key's records must land wholly in one child) and
consults the **placement engine for each child's replica set** (fork F5:
children are born at their final homes — RF, residency, spread, and balance
evaluated at mint, so the copy is the *only* data movement; the AWS-fused
shape, replacing "split in place, rebalance later"). Apply: parent →
`Splitting` (still fully serving), two children minted `Building` with
immutable half-ranges, ids from the monotonic allocator (never reused;
apply-time floor unchanged), policy inherited. Every relay/forward
allowlist is audited in the same change (the missed-allowlist lesson).

The reconciler hosts a `Building` child like any fresh tablet (open its
empty engine, form its group) — children are live Raft groups from birth,
just unroutable.

### Stage 2 — the build

The **split driver** runs on the parent group's leader node (leader-gated,
like the sealer arm; it follows Raft leadership on crash/re-lead). It:

1. **Registers a trim term** on the parent's change log for the duration of
   the build (ADR 0049 §4), so the tail can never be trimmed out from
   under it.
2. **Bulk pass:** scans the parent engine once per child (`KIND_BASE` +
   `KIND_LSI` + `KIND_FOOTPRINT` rows — with MVCC versions and tombstones —
   filtered to the child's half-range) and proposes the rows into the child
   group as **`KvCommand::SeedBatch`** entries: large batches of
   `(kind, key, value, version)` applied as version-carrying merges (the
   `install_engine_image` semantics, as a proposable command). Copy kinds
   are exactly those three: **never `KIND_CHANGE`** (a child re-serving
   parent change records is the #220 duplication class reintroduced by
   hand — children are born with *empty* change logs) and **never
   `KIND_CURSOR`** (cursor rows are consumer-owned and classified
   restart-from-scratch, ADR 0046's consolidation). Intent envelopes and
   transaction records are base rows and copy as such (fork F7).
3. **Tail passes:** consumes the parent's change log past a cursor —
   markers and full records alike are dirty-key signals; the driver re-reads
   each dirty row (current value, current version, intent envelope
   included — stage markers, ADR 0049 §3, are what make a freshly staged
   intent visible here) and ships it as a further `SeedBatch`. O(delta) per
   pass; converges under the freeze-lag threshold.
4. Everything is **idempotent**: re-proposing a row at the same carried
   version is a no-op merge, so crash/re-lead recovery is "re-run the
   pass" — no build progress is persisted in `Metadata`.

`SeedBatch` deliberately emits nothing into the child's change log (it is
history transfer, not new mutation — a stream consumer already saw these
records in the parent's shards). Child follower catch-up and laggard repair
are the child group's ordinary `AppendEntries`/`InstallSnapshot` — no new
Raft machinery anywhere (fork F3; the reviewed alternative, image-seeded
formation, needed a new bootstrap-snapshot mode *and* the propose path
anyway for deltas).

HLC/version handoff: the child's group already witnesses
`storage.latest_version()` at start and every apply re-witnesses; a child's
own post-cutover writes out-version every copied row because the parent
froze before the final rows were read — ordered by the parent's own log,
not by a racy witness (the exact soundness gap that killed merge's
group-start witness, closed by construction here).

### Stage 3 — freeze

When the tail's lag is under threshold, the driver proposes
**`KvCommand::Freeze`** into the *parent's* log — the terminal, whole-range
descendant of ADR 0018 §2's range seal, and its replacement. After its
apply the parent rejects every write, `TxnStage` included, with the
existing retryable-error convention; linearizable **reads keep serving**
(safe: nothing anywhere accepts writes for the range until cutover). The
freeze's log position defines the final state. Then, in order: the final
tail pass; the **streams final seal** (seal the parent's `KIND_CHANGE` to
end-of-log — its open shard id is unchanged by sealing, so an in-flight
iterator drains it and walks to the children per the ordinary lineage
discipline); the **GSI-drain veto** (the parent's `"gsi"` cursor reaches
the freeze; converges — the parent is frozen) and backfill-seeder idle
veto. There is deliberately **no transaction veto** (fork F7): pending
intents were copied; a resolve arriving during the freeze window is
refused retryably like any write and lands on the child seconds later,
which holds the copied intent + record and materializes at its own commit
position — `materialize-at-resolve` unchanged, no force-aborts ever, and
cutover latency never depends on a foreign coordinator.

### Stage 4 — `CutoverSplit`

A new `MetaCommand::CutoverSplit { parent, expected_epoch }`. Apply,
atomically in one control-log entry: children → `Active`, **parent removed
from `tablets`** (fork F6), and **`Metadata::split_lineage[child] =
(parent, parents_final_epoch, cutover_ts)`** written for both children
(fork F9) — written at the one moment the parent's shard chain is complete
and immutable, so lineage derivation is race-free by construction (where
`stream_split_basis` needed two further defense layers, this needs zero).
Never pruned, tiny, and the Console's lineage source.

Client behavior across the flip (fork F8): a stale-routed write hits the
frozen parent, gets the retryable refusal, re-resolves, and lands on a
child — the same retry shape as an election wait. Expected write blip for
the range: sub-second (final delta + one control commit + watch/long-poll
propagation), and the freeze-lag threshold is the tuning knob. A
stale-routed *read* served by the parent before its host learns of the
cutover reads frozen state that no child write can yet contradict only
until the children activate; past that it is refused as the group tears
down — the ms-scale cache-lag residual class ADR 0043/0048 already
accepted, now bounded by teardown instead of standing forever.

`trigger_split`'s confirmation predicate ("the source's epoch advanced")
is replaced by the workflow's own states — kickoff confirms on parent →
`Splitting`; the admin/CLI surface becomes explicitly asynchronous
(`POST /admin/tablet/split` starts a workflow or reports one in flight; a
split-status read reports stage and lag; `POST /admin/stream/grow` follows).

### Stage 5 — retire

The removed parent is a hosted-but-absent tablet on every node that hosted
it: the reconciler's **existing unconditional `Reclaim` arm** stops the
group, deletes the engine, deletes the WAL. Hosted-but-absent regains two
causes (dropped table; split-retired) — safe, unlike the old two-vanish-
reasons hazard, because both demand the identical action (children own
copies; erase). Sealed stream history is untouched by teardown:
`get_records_sealed` needs only the replicated catalog row and the
`SegmentStore` (recon-verified), and catalog rows are keyed by tablet id
and never pruned. **Rider:** the segment janitor's max-epoch removal guard
keys on "tablet still exists" and is taught that a retired tablet's final
shards expire by ordinary retention.

### Stage 3–5 as-built notes (2026-08-17, Train B rung 5)

Three deviations from the prose above, found by the rung's own teeth:

1. **The freeze's contract is USER data (base/LSI rows), not "every
   write."** A pure consumer-bookkeeping batch — a cursor row, a footprint,
   a change-log-only entry (the backfill seeder's synthetic records) —
   still applies on a frozen parent. Without this the GSI-drain and
   backfill cutover vetoes deadlock against the very freeze that made the
   parent drainable (the drain/seeder must WRITE their offsets to finish);
   caught red by the revived split-during-backfill e2e. Safe: bookkeeping
   kinds are never copied to children (CHANGE/CURSOR) or self-healing on
   them (FOOTPRINT).
2. **The endgame ships a FINAL IMAGE — one full re-scan of the frozen
   parent — after the final tail pass**, because transaction decisions
   (`TxnCommit`/`TxnAbort`) and resolves rewrite base rows with **no
   change record of their own**: an O(delta) tail structurally misses
   them, and a child inheriting a stale `Pending` record for an
   acked-committed transaction is the in-doubt-recovery-aborts-a-commit
   class fork F7 exists to prevent. Cost: a second full read+wire pass per
   split, accepted for v1; apply-side decision/resolve markers restoring
   O(delta) are a named follow-up. The image is gated on the apply task
   reaching the freeze-window commit floor (no decision can apply
   mid-scan unseen).
3. **A `Building` child runs no consumer arms** (drain/seeder/seal/trim).
   Every consumer restarts from scratch at activation (the classified
   policy) — and running them early is actively harmful: a child's
   token-truncated `"gsi"` cursor key routes to the still-routable parent
   and lands in the parent's own cursor scope, where min-over-rows drags
   the parent's watermark down forever, deadlocking the GSI veto (the
   split-child-cursor-unreadable shape, poisoning the parent this time).

Also as-built: `Freeze` is implemented as the whole-range entry of the
existing sealed-set discipline (its durable marker re-latches `frozen` at
group start, surviving log compaction), plus a propose-side latch in every
`animusd` write/txn helper returning the retryable
`"tablet frozen for split cutover…; retry"`; `TxnResolve`/decides landing
on a frozen parent are refused retryably and chase to the children
post-cutover (F7, proven end-to-end by the racing-transactions e2e).

## What this deletes (the defense stack for the seam that no longer exists)

All recon-verified with production call sites; deleted in Train B's final
sweep, after the new mechanism is proven; mechanism-gone lessons move to
the archive per house rule:

- `narrow_scope` / the live `Arc<Mutex<KeyRange>>` / `widen_scope` (already
  caller-less) / `KeyRange::abuts` (merge residue);
- `HostAction::NarrowScope` and `HostAction::ProposeSeal` +
  `parent_seal_observed` gating; the reconciler corpus's split-narrow
  scenarios (replaced, not just removed);
- the range seal (`seal.rs`, whole module) — replaced by `Freeze`;
- the per-entry write fences on seven `KvCommand` variants, their apply
  checks, and the pre-propose scope checks — replaced by the route-time
  `Active` filter plus the frozen parent's refusal (one cheap pre-propose
  key∈declared-range guard is kept as a routing-bug tripwire; it reads an
  immutable range, no lock);
- `Metadata::stream_split_basis`, `effective_stream_shard_watermark`'s
  inheritance, `animus_tablet::split_basis::effective` (its one caller
  dies), and `SealStreamShard.expected_range` + its apply-time CAS;
- `in_declared_range` (all three sites) and `hot_read_scope_ok` (the ADR
  0048 latch) — retired *with the residual they defended*, not as an
  accepted gap;
- `Metadata::split_parents` (fork F9) and the old
  `MetaCommand::SplitTablet` + epoch-advance confirm.

ADR 0046 principle 3 is superseded by a strictly stronger invariant: **a
split ends the parent's log and starts two empty ones; no consumer offset
ever crosses a split.** The cursor `SplitPolicy` classification collapses
to `RestartFromScratch`-only (`InheritFrozenBasis`, already
zero-constructor, is retired).

## Consequences

- **Splits are O(data) background IO events.** The drain pays roughly 2×
  disk writes per child replica (child Raft WAL + engine) and the bytes
  cross the network once to each child replica's home (fork F5 made those
  the final homes). Transient extra space until the parent's engines are
  deleted. Bulk-load split storms become IO events: per-node concurrent-
  build caps/throttling and **presplit at `CreateTable`** (children of an
  empty parent build instantly; needs the one-tablet-per-table gate
  relaxed) are named roadmap items, deliberately out of Train B.
- **A mis-timed split is no longer free** (ADR 0034's "cheap to have made"
  mitigation stops applying); the trigger's thresholds inherit real weight.
- **`CancelSplit`** (abandon a build: erase `Building` children — safe,
  they were never routable) is roadmap, not Train B; a wedged build leaves
  the parent serving indefinitely, only blocking further splits of itself.
- **The `Building` state is the natural shape for the open
  CreateTable-first-write formation race** — noted as a beneficiary;
  fixed as its own PR per house rule, never folded in.
- Future storage headroom: SSTable-level seed cloning (a per-tablet engine
  makes file-granularity copy possible later); never a second split
  mechanism.
- **Train sequencing constraint, stated plainly:** zero-copy split is
  impossible across per-tablet engines, so Train B rung 1 (storage)
  disables the old split and parks its tests; the new workflow revives
  splitting in later rungs of the same train, which merges bottom-up as
  one. Split is offline mid-train — acceptable pre-alpha under the
  fresh-clusters policy, and main never sees the intermediate state.

## Testing plan

- **Split-workflow fault corpus** (SimEnv, seed-replayable, own
  `ANIMUS_SPLIT_SEEDS` knob): parent-leader crash/re-lead in every stage
  (mid-bulk, mid-tail, post-freeze pre-cutover, post-cutover pre-reclaim);
  control-plane failover mid-`BeginSplit`/mid-`CutoverSplit`;
  double-`BeginSplit` epoch-CAS race; child replica crash and partition
  during the drain (remote children are first-class per F5); wedged build
  resume; writes and scans racing the freeze (linearizability held; the
  blip bounded and retry-masked).
- **Streams exactly-once across the new lineage**: `stream_lineage_corpus`
  reworked (basis-inheritance cells die; new cells assert the parent's
  final-shard close, child epoch-0 `ParentShardId` via `split_lineage`, and
  no loss/duplication across cutover); the D8 e2e walk adapted — its
  distinct-eventID residual class should measure ~0, which is itself a
  teeth-check on this ADR's central claim.
- **Transactions** (F7): intents staged/resolved/aborted straddling every
  stage; reader-triggered recovery-resolve landing on a child's copied
  intent; a resolve retried through the freeze window.
- **Reconciler corpus**: lifecycle scenarios replace split-narrow ones
  (Building host, cutover flip, parent reclaim, per-tablet engine
  open/close/delete under crash/restart).
- **Storage rung teeth**: the idle per-tablet engine cost measurement
  (Decision 1's named item) and a multi-engine-per-node ProdEnv load test
  (the ADR 0028 follow-up that was never written, now mandatory in its
  inverted form).
- Red→green discipline per cell where the old shape can express the
  hazard; all five gates per rung; ProdEnv multi-thread acceptance for the
  driver and freeze (SimEnv proves ordering, not liveness); bench: split
  wall-clock, write amplification, and the parent's serve latency under a
  live build.

## As-built amendment (2026-08-17, Train B complete — rungs 1–8)

The train delivered as planned with the deviations below; net across the
delete-heavy rungs the codebase is **~3,200 lines lighter**. Status is
implemented; ADR 0028 is superseded in full.

**Rung 1 — per-tablet engines.** The gating measurement passed
emphatically: an idle `LsmEngine` costs **~1 KB RSS**, creates **zero
files** at open, and spawns **zero tasks/timers** (`open` is passive;
production uses `background_maintenance: false`) — quiescence is safe from
the storage side, and dev-mode `--cluster N` is unaffected. `Release` and
`Reclaim` collapse to one behavior (whole-engine file deletion;
`TeardownKind` survives as intent labeling). Surprise: `CreateTable` DOES
provision the bootstrap tablet — a stale fixture comment said otherwise.

**Rung 2 — F2b keys.** `StorageScope` keeps its name, slimmed to
`{prefix: [] | [kind], range /* immutable */}`; **no codec bump** (the
image wire layout was already physical-agnostic). Kind-scope bounds are
finite by construction, so every "unbounded above" branch died. Marker
disjointness now rests on first-byte ordering (reserved `0x5F` vs kinds
`0x00..=0x04`), not escape-prefix-freedom.

**Rung 3 — lifecycle.** Six under-specified points settled: cutover
recomputes its children from the map (replay-safe); `cutover_wall_ms` is
proposer-stamped; cutover bumps child epochs; the kickoff confirm re-arms
on stray epoch bumps; under-satisfiable placement falls back to
parent-inherit; and "children unroutable" required filtering **both scan
fan-outs**, not just point routing (a `Building` child overlaps its
un-narrowed parent).

**Rung 4 — the build.** The tail cursor is a **packed-HLC watermark,
never a key position** — a key cursor silently lost racing writes while
reporting converged (`pending_changes`' key order is prefix-then-HLC, not
commit order). Confirm is by applied index, never a value probe
(version-carrying merges make probes hang on correctly-no-op'd batches).
256 KiB `SeedBatch` chunks; `Forwarded{SeedRows}` rides serde_json (~4×
inflation, accepted). The tail is O(pending) per tick — the trim hold
grows the pending set over a build's bounded duration; an O(delta)
commit-ordered read is a named follow-up.

**Rung 5 — freeze/cutover.** The freeze is **writer-classified** (user
data blocked; consumer bookkeeping passes) — the uniform "reject every
write" form deadlocked the cutover vetoes against the freeze itself. A
**final image** replaced the planned final-tail-only endgame: transaction
decisions/resolves rewrite rows with no change record, so an O(delta)
tail structurally misses them (a child inheriting a stale `Pending`
record for an acked commit is the class fork F7 exists to prevent).
`Building` tablets run **no consumer arms** (a child's token-truncated
cursor row would poison the parent's trim watermark — the memory-recorded
`split-child-gsi-cursor-unreadable` shape, now dead by construction).
`TxnResolve` during the freeze is rejected retryably; the resolve chases
to the child.

**Rung 6 — lineage.** The segment janitor needed zero production changes
— retirement was **safe by construction** (the drop rule keys on schema,
not tablet presence; the live-chain max-epoch pin never applied), proven
by reversion teeth rather than assumed. A retired parent's open shard id
becomes its sealed catalog row under the same id, so consumer-error
concerns dissolved structurally. Lineage is transitive through retired
ancestors purely on wire data. F11 binds corpus split keys to 8-byte
tokens.

**Rung 7 — the sweep.** `ceiling.rs` was **kept** (load-bearing ADR 0018
MVCC read-ceiling machinery, not fence-era). `SealStreamShard`'s
`expected_range` CAS was **deleted** (verified inert: the
first-committer-wins content match, `object_id` included, closes the
dueling-seal race independently, and ranges never mutate while a tablet
exists). One production-safety nuance: an immediate `BeginSplit`+
`CutoverSplit` round against a live cluster deadlocks (children first
observed post-cutover, all replicas empty at a bumped epoch, everyone
hosts as a quiet non-voter) — real workflows always host children while
`Building`; pure-metadata fixtures may take the immediate round, populated
fixtures must run the genuine workflow.

**Rung 8 — acceptance, bench, and two liveness/latency finds.** The
committed bench (`split_build.rs::bench_split_…`, `#[ignore]`d) drove a
*continuous* writer and found the shipped convergence predicate
("tail pass shipped zero new records") **never fires on a
continuously-written parent** — the hot tablet that most needs splitting
could never freeze. Fix: `SPLIT_MAX_TAIL_PASSES` (25 post-bulk chasing
passes ≈ 5 s) freezes regardless; the post-freeze drain + image still
transfer everything — the residue only sizes the write blip, which is
exactly stage 4's stated knob. Second find: the unfiltered final image
re-shipped the whole table *inside the freeze window* (~2 s of blip at
2,000 rows, scaling with table size). Fix: the final image is filtered by
a **pre-bulk version floor** (a read-only pass over the copy kinds before
the bulk starts; apply order == HLC order makes any bulk-missed rewrite
out-version the floor; deliberately not `latest_version()`, which the
read-ceiling marker future-shifts), plus the endgame phases now fall
through within one driver tick instead of parking 200 ms per phase.
Measured after the fixes (3 nodes, N=2,000 × 256 B, sequential client,
WSL2): **build 12.8 s (156 rows/s, including the deliberate 5 s
tail-chase bound); serve-during-build ≈ idle (116 ms vs 112 ms median —
the 3-node linearizable-read cadence dominates both); write blip 458 ms —
inside fork F8's sub-second contract.** Deep corpus at
`ANIMUS_SPLIT_SEEDS=40` / `ANIMUS_STREAM_SEEDS=40` /
`ANIMUS_RECONCILER_SEEDS=40` / `ANIMUS_QUIESCE_SEEDS=40` /
`ANIMUS_TXN_SEEDS=5`: all green (nightly workflow extended with the
three new knobs). Acceptance: the multi-split soak (≥3 cutovers on a
streamed+GSI'd table under mixed plain/transactional/GSI load — zero
lost writes, exactly-once lineage delivery, GSI convergence, every
retired parent's engine files deleted on every node), concurrent splits
of two tables racing to completion, and a split-deployment
(control-only + data-only) cluster running one full split. The soak's
GSI-convergence + reclaim pair also closes the
`split-child-gsi-cursor-unreadable` bug class end to end.

**Named follow-ups, deliberately not in the train:** apply-side
decision/resolve markers restoring an O(delta) endgame; an O(delta)
commit-ordered tail read; presplit at `CreateTable`; `CancelSplit`;
per-node concurrent-build caps/IO throttling; SSTable-level seed cloning;
per-tablet admin storage introspection (`/admin/storage/lsm` shows only
the syskv engine today).

## Amendment (2026-08-19) — the tail's cost is the delta, not the table

Reported from a dev cluster (`--cluster-control 3 --cluster-data 5
--auto-split 4000`, 20,000 seeded rows): the split "worked but was
painfully slow." Measured on the parent's leader, the build copied all
20,000 rows in **4 s** and then spent **70 more seconds** appending
~6,000 Raft entries *per child* while both children's key counts stayed
completely flat — ~85% of the build's wall clock re-copying rows it
already had. Two independent causes in `tail_pass`, both fixed here:

1. **The tail shipped one `SeedBatch` per dirty unit.** `ship()` was
   called inside the per-unit loop, so every partition key bought its own
   consensus round + apply-confirm (plus a forwarded hop for an off-node
   child) — while the bulk pass batched to `SEED_CHUNK_BYTES` and moved
   thousands of rows per round. The tail now accumulates rows per child
   across units and flushes on the same byte budget. Nothing depends on
   one unit's rows sharing an entry (a `SeedBatch` is idempotent at its
   carried versions, and the bulk path's chunk boundaries already cut
   across rows); F11's *which child* rule is a routing question and is
   untouched.
2. **`tail_hlc` started at 0.** The first tail pass therefore classified
   every change record in the (trim-held) log as fresh and re-shipped the
   whole table one unit at a time — every merge an idempotent no-op. It
   is now captured in the same pre-pass that computes
   `bulk_version_floor`, by the same monotonicity argument that floor
   already rests on: the record set read *before* the bulk scan describes
   rows the bulk image contains by construction, and any write applied
   after that read carries a strictly higher HLC
   (`assert_ts_monotonic`), so the tail still sees everything the image
   could have missed. The endgame's final image (signal-less txn
   decisions/resolves) is unchanged and still backstops the classes no
   change record announces.

Measured after, same 20,000-row cluster: **~8 Raft entries per child
instead of ~6,000**, `split_rows_shipped` 32,000 → 20,500, freeze at 42 s
instead of 85 s, and the full two-generation cascade to 8 tablets in 56 s
instead of 93 s — with all 20,000 keys present at the end. The build is
now bounded by real data movement (one 256 KB chunk per round) plus
whatever was genuinely written during it.

Cause 1 was a plain oversight, not a stated trade-off; cause 2 was the
unstated cost of a conservative watermark. The O(pending) *scan* per tail
pass the rung-4 notes accepted is unchanged and its follow-up above still
stands — this amendment removes the per-row consensus rounds and the
redundant whole-table re-ship, not the re-scan. Regression:
`tests/split_build.rs::split_build_tail_does_not_re_ship_the_bulk_image_
row_by_row` (a child's own `commit_index` at cutover as the batch-size
meter — 302 entries for a 600-row split before, ≤64 budget after).

## Amendment (2026-08-19) — investigated and rejected: replacing the
## version-floor pre-pass with `engine_applied_index()`

The build's three full engine scans (version-floor pre-pass, bulk copy,
final image, `index_drain.rs::split_driver_tick`) were flagged as a
measured cost: on a quiet 20,000-row split the pre-pass alone materializes
every `KIND_BASE`/`KIND_LSI`/`KIND_FOOTPRINT` row purely to compute a
`u64` (`SplitBuild::bulk_version_floor`). The proposed fix — drop the scan
and read `group.engine_applied_index()` instead, on the premise that "the
Raft log index is the MVCC version" (a line that was, at the time, still
standing in `crates/animusd/CLAUDE.md`) — was investigated and rejected
**before any code changed**, because that premise is stale: ADR 0018
§2/PR2 (2026-08-11, well before this ADR) retired the interim
`version_floor`-scaled Raft-index encoding and made the engine's MVCC
version a **packed HLC commit timestamp** — `hlc::pack(ts) = (wall_ms <<
20) | logical`, minted by the proposing leader at *propose* time and
carried in the `KvCommand` itself (`animus-cp-data/src/hlc.rs`,
`KvCommand`'s own doc comment). A group's `engine_applied_index()` is a
Raft log index — a small monotonic entry count — which is not the same
value space as a row's packed-HLC version at all, so it cannot stand in
for `bulk_version_floor`:

- **Under any real (`ProdEnv`) workload it under-filters.** `wall_ms` is
  milliseconds since the `Env` clock's epoch shifted left 20 bits; even a
  freshly-started cluster's HLC versions dwarf any plausible log index
  within the first tick. Using the index as the floor would make `ver >
  floor` true for essentially every real row, so the final image would
  silently degenerate back into the unfiltered whole-table re-ship rung 8's
  own fix (above) exists to prevent — no scan saved, and the exact
  regression this train already paid down.
- **Under `SimEnv` it can over-filter, which is the unsound direction.**
  The simulated clock and the Raft log advance on independent schedules; a
  build's log index can exceed an early row's small `wall_ms` faster than
  the row's own HLC catches up, so a log-index floor could sit *above* a
  genuinely-early version and cause the final image to skip a row that
  needed re-shipping — exactly the "floor too high" hazard
  `bulk_version_floor`'s own doc comment names as the reason it is
  deliberately not `latest_version()`.

No code changed: the pre-pass scan (`index_drain.rs::split_driver_tick`,
the `for kind in SEED_KINDS { ... floor = floor.max(ver) ... }` block)
stays as the sound source of the floor. The stale premise itself was
corrected in the same change (`crates/animusd/CLAUDE.md`'s "CP writes need
no client-assigned version" gotcha). The version-floor scan's actual cost
remains open: a cheaper *sound* substitute — e.g. a per-copy-kind running
max maintained incrementally by the apply path, so no read-only pass is
needed at all — is a named follow-up, not attempted here (it changes the
apply path's bookkeeping, a materially larger and riskier change than a
drop-in read substitution, and deserves its own fork review rather than
being folded into a "just read something O(1) instead" task).
