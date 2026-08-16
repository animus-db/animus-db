# ADR 0044 — Tablets are split-only: tablet merge removed

- **Status:** Accepted — implemented (this stack). Supersedes
  [ADR 0033](0033-tablet-merge.md) entirely.
- **Date:** 2026-08-14

## Context

Tablet merge (ADR 0033, shipped 2026-08-07) was built as split's operator-
driven dual: given two tablets with adjacent ranges and identical replica
sets, it widened the surviving tablet to cover both and tore the other
down without erasing its data (a sibling now served that range on the same
node-shared engine). The motivating case was real — a table that split
eagerly (or under a since-corrected trigger, ADR 0034) could shrink back
down, and merge let an operator reclaim the per-tablet Raft-group overhead
(one WAL file, one voter-set-tracking group, one set of election/heartbeat
timers) of a tablet that no longer needed to be its own group.

Guillaume decided on 2026-08-14 to remove tablet merge entirely: going
forward, **tablets are split-only**. Two independent lines of evidence
converged on this call.

### The DynamoDB precedent

This codebase's DynamoDB Streams work (ADR 0042/0043) already had to
verify, byte-for-byte, how AWS's own service behaves: **DynamoDB
partitions split under load and never merge, full stop.** The DynamoDB
Streams `Shard` API carries exactly one `ParentShardId` field, never two,
and has no `AdjacentParentShardId` at all — a field Kinesis (the service
DynamoDB Streams is modeled on) *does* carry, because Kinesis shards *can*
merge. Its absence in DynamoDB's own wire shape is not an oversight; it is
the fossil of AWS's own decision that a table partition never merges back
into another, ever. A table that scales down keeps its partition count
forever — dilution is a documented, accepted cost of running DynamoDB at
scale, not a gap AWS is racing to close.

This precedent is not "a NoSQL service with weaker guarantees took a
shortcut we don't have to." DynamoDB's own 2022 USENIX ATC paper
("Amazon DynamoDB: A Scalable, Predictable, and Highly Available Key-value
Store") describes each partition as replicated by **Multi-Paxos** —
structurally the same animal as an Animus tablet's own per-tablet Raft
group: a consensus-replicated shard, not a consensus-free slice of data.
AWS did not avoid merge because their partitions are cheaper to run than
ours by nature. They made partitions cheap to run at rest instead of
building a mechanism to shrink their count. That is the roadmap this ADR
commits to as well (see "The cheap-groups roadmap," below) — merge was
solving the wrong end of the problem.

### The correctness crack PR1 found

Beyond cost, merge's own correctness story had a real crack, found while
deleting it. ADR 0033's cross-group LWW hazard fix depended on **witnessing**
plus, later, a range seal: a merge survivor's group-start witness reads the
shared engine's own `latest_version()` at the moment that replica's group
*starts* (or re-forms). The now-deleted `merge_widens_survivor_and_absorbs_
sibling_unerased`-style regression tests passed reliably — but only because
the test harness always started the survivor's group *after* the absorbed
side had already written everything it was going to write. A survivor
whose own group start predates some of the absorbed side's later writes —
a legitimately reachable ordering in production, just not one any test
happened to construct — would witness a stale `latest_version()` and would
not be guaranteed to out-version those writes. The seal (ADR 0018 §2
amendment) closed the *proposer-side* half of this (a source group can't
keep writing into a range it already handed off), but the survivor's own
group-start witnessing argument was never rigorously sound against every
possible group-start ordering; it happened to hold in every test that was
ever written for it. That is a fragile foundation to keep carrying forward,
independent of whether merge is worth its cost.

## Decision

**Tablets are split-only.** The merge machinery is deleted across both
rungs it touched:

- **`animus-control`** (the metadata/producer half): `MetaCommand::
  MergeTablets` and its apply arm (including the ADR 0042/0043 "F1" stopgap
  that rejected merge on a streamed base table); `Metadata::merged_tablets`/
  `absorbed_by`; the `syskv::EntityKind::Merged`/`AbsorbedBy` system-keyspace
  mirror; every test exercising any of the above.
- **`animus-cp-data` + `animusd` + `animus-cli`** (the data-plane/wire/admin
  half): the tablet-host reconciler's `HostAction::WidenScope`/`Absorb` and
  `TeardownKind::Absorb` (including the drain-before-halt fix that made
  `Absorb`'s teardown safe); `animusd`'s `ClientRequest::MergeTablets`,
  `trigger_merge`, the `POST /admin/tablet/merge` route, and the
  `merge_tablets` allowlist/tracing entries; `animus-cli`'s `merge`
  subcommand.

**What stays, and why:**

- **`KvCommand::Seal` and its engine-global seal markers (`seal.rs`)** —
  the ADR 0018 §2 amendment's range-seal mechanism. It is not merge-only:
  split's own `NarrowScope` handoff proposes exactly the same seal, for the
  identical reason (a source group must stop accepting writes to a range
  it has handed off before a successor starts serving it). Deleting merge
  only removes the seal's *other* caller (`Absorb`) and the reconciler
  gate that waited on it (a merge survivor's `WidenScope`); split's own
  seal-propose/seal-wait pair (`HostAction::ProposeSeal`, `TabletFacts::
  parent_seal_observed`/`Metadata::split_parents`) is unaffected.
- **`split_parents` provenance** (`Metadata::split_parents`, never pruned)
  — the seal-observation gate a fresh split child still needs. Merge's own
  mirror-image field, `absorbed_by`, is what got deleted.
- **Auto-split** (ADR 0034) — unaffected. A tablet's byte-based split
  trigger, split-point selection, and cooldown discipline have nothing to
  do with merge; only the (never-built) inverse, an automatic *merge*
  trigger, was ever out of scope, and now there is no manual merge for an
  automatic one to have eventually mirrored either.
- **The raw `widen_scope` `StorageScope` setter** (`animus-cp-data::
  RaftKvNode::widen_scope`) — kept as a distinctly-named, distinctly-
  documented primitive (the dual of `narrow_scope`), exercised directly by
  `tests/cursor_scope.rs` to prove the ADR 0042 §7 min-over-rows cursor
  read stays correct against an arbitrarily widened scope. It has **no
  production caller**: nothing in this codebase calls it now that merge is
  gone. It stays because a raw scope-widening primitive is generically
  useful test/audit infrastructure (the same reasoning that keeps ADR
  0028's write fence wired even where it currently has no production
  caller) — a future scope-mutating feature, if one is ever built, would
  otherwise have to reinvent it from scratch.

## The cheap-groups roadmap

Removing merge means a tablet's per-group overhead (a Raft WAL file,
election/heartbeat timers, a voter-set-tracking group) is now genuinely
permanent — it never shrinks back down once split creates it. That
overhead is a real cost this ADR does not pretend away; the position this
project is taking, matching the DynamoDB precedent above, is that the
right fix is making that per-group cost cheap enough not to matter, not
building a way to un-split. None of the following ships in this stack —
they are named follow-ups, in the rough order they would pay off:

1. **Quiescence — THE FIRST WIN, likely ~80% of the win. CLOSED by ADR
   0048 (phase 1).** An idle Raft group (no proposals, no client traffic)
   has no structural reason to keep ticking election timers or exchanging
   heartbeats at all; it can go fully dormant and wake on its first write.
   **Correction found while implementing (ADR 0048):** the apply task's own
   5ms idle poll, not named here, turned out to be the larger of the two
   idle-wakeup sources (~200 wakeups/s/group vs. ~20-40/s from heartbeats/
   inbound messages) — both are now closed. Today every hosted tablet
   group ticks forever regardless of load, which is the single largest
   avoidable cost a large, mostly-cold tablet fleet pays. If group-count
   cost is ever observed to bite in practice, this is the mitigation to
   schedule first — before reaching for anything below.
2. **Heartbeat amortization.** Coalesce liveness traffic per node *pair*,
   not per group: one heartbeat between two nodes can carry leadership/
   commit-index state for every group that pair co-hosts, the same way
   CockroachDB and TiKV amortize Raft heartbeats at fleet scale. Never pay
   a per-group heartbeat cost once a node pair hosts many groups together.
3. **Asymmetric replicas.** DynamoDB's own "log replicas" precedent: a
   quorum member that holds the replication log durably but carries no
   engine state and never leads — an ultra-cheap voter whose only job is
   making up quorum size, not serving reads or storing a full copy.
4. **Fleet-scale amortization.** An observation about where 1–3 actually
   pay off, not a mechanism of its own: at high replica density, fixed
   per-node costs (one process, one set of background loops) dominate and
   get amortized across many groups for free. A small cluster cannot hide
   per-group overhead behind fleet scale the way a large one can — which
   is exactly why 1–3 matter *more* for Animus than for a hyperscale
   fleet, not less.

**The storage side of this is already amortized, today** (ADR 0028): one
shared **engine** per node, scoped by `StorageScope`, so a tablet's storage
footprint was never the problem merge was reclaiming. **Doc-drift fix (ADR
0048):** this used to also claim a shared *WAL*; that part was never true —
each group still holds its own Raft WAL file
(`animus_cp_data::wal_file(stream)`, `raftkv.wal.{stream}`), and
`animus_control::SharedWal` is built and unit-tested but unwired. The
remaining gap was per-group Raft timers/heartbeats **and** the apply task's
own idle poll — see ADR 0048 for the as-built quiescence mechanism that
closes this, and the finding that the apply-poll term was actually the
larger of the two.

## Shrink-in-place and dilution

A table that splits under load and later bulk-deletes or TTLs most of its
rows **keeps every tablet it ever split into, forever.** There is no
mechanism, after this ADR, that reduces a table's tablet count once it has
grown — this is the direct, accepted cost of going split-only, and it is
called out here on its own rather than left implicit in "merge is gone."

This is the same trade DynamoDB itself makes and documents as normal
operation: dilution (a table's partition count not shrinking after its
data does) is an accepted, expected cost of running at scale, not a defect
AWS is trying to fix. Quiescence (above) mitigates the *idle* cost of a
diluted table's now-unnecessary tablets — a cold group that never ticks
costs close to nothing — but it does not, and cannot, reduce the tablet
*count* itself.

Any future story for actually reducing a table's tablet count is
explicitly **not** a revival of merge. It would be a from-scratch redesign
— for example, repartitioning a table's data into a freshly-provisioned
table with fewer, larger tablets and cutting traffic over — never a
widen-and-absorb of two existing tablets. This door is closed deliberately:
merge's own correctness story (see Context) was never as solid as it
looked, and a future count-reduction mechanism should not inherit that
history by starting from the same shape.

## Consequences

- `MetaCommand::MergeTablets`, `Metadata::merged_tablets`/`absorbed_by`,
  `HostAction::WidenScope`/`Absorb`, `TeardownKind::Absorb`, and every
  wire/admin/CLI surface that reached them are deleted. A hosted-but-now-
  absent tablet is unconditionally `Reclaim`ed (erased) — there is no
  second case to disambiguate anymore, and the `merged`/`absorbed_by`
  markers that used to make that disambiguation possible are gone with
  the mechanism they existed for.
- ADR 0042 §12's "F1" merge-stopgap (rejecting `MergeTablets` on a
  streamed base table) is moot and deleted along with `MergeTablets`
  itself — there is no merge left to reject, on a streamed table or any
  other.
- **Named follow-up, deliberately not done in this stack**: the min-over-
  rows tolerance in `animus-cp-data`'s `cursor_min_watermark` (ADR 0042 §7)
  exists to handle more than one cursor row per tag showing up in one
  tablet's `KIND_CURSOR` scope — historically the shape a merge survivor's
  widened scope produced. Under split-only tablets, a tablet's own scope
  only ever narrows, so the scenario that rule exists to resolve no longer
  structurally arises; whether to simplify `cursor_min_watermark` down to a
  single-row read is a smaller, separate change, evaluated on its own.
- The cheap-groups roadmap above (quiescence, heartbeat amortization,
  asymmetric replicas, fleet-scale amortization) is the accepted long-term
  answer to per-tablet overhead that merge used to partially, unsoundly
  paper over. None of it ships here.
- The engineering-lessons entries this stack's deletion produced —
  the never-pruned-marker/two-vanish-reasons lesson, the absorb-drain
  data-loss postmortem, and the version_floor-retirement note — are
  archived verbatim in `docs/engineering-lessons-archive.md` (their
  mechanisms are gone; the still-general lessons keep a pointer in
  `docs/engineering-lessons.md`).

This ADR supersedes [ADR 0033](0033-tablet-merge.md) in full and amends
[ADR 0002](0002-tablets-unit-of-placement.md) (the tablet lifecycle model),
[ADR 0018](0018-cross-tablet-transactions.md) (the range-seal mechanism
loses its merge-side caller), [ADR 0029](0029-replica-rebalancing.md) (the
rebalance/merge replica-divergence interaction is now moot),
[ADR 0034](0034-byte-based-auto-split.md) (merge is no longer what makes
an over-eager split reversible — nothing is), and
[ADR 0042](0042-dynamo-streams.md)/[ADR 0043](0043-stream-shard-subsystem.md)
(the F1 stopgap and its escape-hatch language are both retired).
