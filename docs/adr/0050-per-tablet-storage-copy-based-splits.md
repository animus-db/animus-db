# ADR 0050 — Per-tablet physical storage and copy-based symmetric splits

- **Status:** Accepted — not yet implemented (delivery "Train B"; depends on
  ADR 0049's Train A landing first). **Supersedes [ADR
  0028](0028-shared-storage-single-command-split.md) in full once
  implemented** (both halves: the shared per-node engine and the
  metadata-only zero-copy split).
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
