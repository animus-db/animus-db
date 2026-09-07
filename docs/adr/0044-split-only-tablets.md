# ADR 0044 — Tablets are split-only: tablet merge removed

- **Status:** Accepted — implemented (this stack). Supersedes
  [ADR 0033](0033-tablet-merge.md) entirely.
- **2026-08-16 note:** split-only stands unchanged, but
  [ADR 0050](0050-per-tablet-storage-copy-based-splits.md) (accepted, in
  delivery) reverses this ADR's "the storage side of this is already
  amortized (one shared engine)" pillar: storage returns to per-tablet
  engines, so per-tablet storage cost joins the per-group costs the
  cheap-groups roadmap commits to keeping cheap (0050 names the idle-engine
  cost measurement as a gating item). A split's copy cost also gives the
  dilution trade below a second leg: tablet count still never shrinks, and
  creating one now costs real IO.
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

## Amendment (2026-09-06): phase 2 investigation (C-02 PR 1)

Investigation only, no decision change: `docs/design/heartbeat-send-sites.md`
maps every heartbeat send site named by the cheap-groups roadmap's
follow-up 2 ("Heartbeat amortization," above) ahead of building it. Summary
of its findings:

- **Two, unrelated, identically-named "heartbeat" mechanisms exist.**
  `animus_control::RaftCore::heartbeat_interval` (50ms) is the Raft
  protocol's own empty-`AppendEntries` heartbeat, instantiated once
  cluster-wide by the control plane and once **per hosted tablet group** by
  `animus_cp_data::RaftKvNode` (ADR 0016/0017) — this is the actual C-02
  target, since it multiplies with a node's own led-tablet-group count `G`.
  `animus_control::node::HEARTBEAT_INTERVAL` (100ms) is the unrelated ADR
  0012 node-liveness ping, already scoped **per node**, not per group — it
  does not multiply with `G` and needs no amortization. The roadmap's own
  "`HEARTBEAT_INTERVAL` users" phrasing names the second, but the cost
  problem this phase describes is entirely about the first.
- **The roadmap's "`animus-cp-data`'s host module" pointer is corrected**:
  the per-group send site is `animus_cp_data::lib.rs`'s per-group `drive`
  loop (one independent async task per hosted group), not `host.rs` (the
  ADR 0031 reconciler, which decides what to host but sends no Raft
  message itself).
- **Cost model**: a node leading `G` groups at RF 3 (`P = 2` peers/group)
  sends `≈ 40 × G` outbound `AppendEntries`/sec purely from heartbeat
  cadence, before any real write. In a `--cluster 3`/RF-3 default (every
  node pair co-hosts every tablet), this is **linear in total tablet count
  `T`** per node pair (`≈ 80T/3` msgs/sec) today, for a fixed node-pair
  count (3 pairs) — the map's own baseline test
  (`crates/animus-cp-data/tests/heartbeat_cost.rs`) measures this directly
  via `Metric::CpAppendEntriesSent` (5 co-hosted groups ≈ 5x 1 group's
  traffic, seed-reproducible: 234 vs. 1166 sends over an identical window,
  ratio ≈ 4.98). ADR 0048 quiescence already zeros the *idle* term; this
  phase targets the *active* term quiescence cannot touch.
- **The crux (per-group vs. per-node-pair)**: a heartbeat's *arrival*
  resets that group's own election timer — this is inherently per-group,
  since every tablet group is an independent `RaftCore` (this is also this
  plane's *entire* failure-detection mechanism — there is no ADR-0012-style
  liveness ping for CP-data groups, unlike the control plane). What IS
  already per-node-pair and needs no new work is the **transport**
  (`ProdEnv` already pools one TCP connection per destination address,
  `crates/animus-env/src/prod.rs`). What is NOT amortized is the **frame
  count**: `G` separate `send_stream` calls (one per co-hosted group) still
  write `G` separate frames onto that one shared connection every interval,
  instead of one combined frame a receiver demuxes back into `G` per-group
  deliveries.
- **Candidate shape for PR 2** (sketch only): a per-node
  `HeartbeatBatcher` inside `animus-cp-data`, below the per-group
  `RaftCore::tick`, coalescing every co-hosted group's small per-tick
  payload into one frame per destination per interval on a reserved stream
  id, demuxed at the receiver back into per-group `RaftCore::handle` calls
  — preserving every per-group semantic (§3 of the map) while amortizing
  only the wire framing. Gated behind a flag, off by default, mirroring
  `enable_quiescence`'s own additive-default shape.

See the document itself for the full per-message-type map, the exact
`file:line` citations, the test-plan cell list (seed knob
`ANIMUS_HEARTBEAT_SEEDS`), and the open questions PR 2 inherits.

## Amendment (2026-09-06): phase 2 batcher behind a flag (C-02 PR 2)

Implements the PR 1 investigation's own candidate shape (above), landing
`animus_cp_data::heartbeat_batch::HeartbeatBatcher` — off by default, so
this PR is additive-default with zero behavior change until a node opts
in. Decisions the design doc left open, closed here:

- **Reserved stream id: `u64::MAX - 2`** (`HEARTBEAT_BATCH_STREAM`), a
  constant, not locally chosen per node — both ends of a link must agree
  on it to demux correctly, so it has to be a fixed, cluster-wide value
  like `PRIMARY_STREAM`/`SEGMENT_STREAM`/`BACKUP_SEGMENT_STREAM`, not a
  per-node choice. It sits at the same far end of the `u64` space as this
  crate's other two reserved streams (`cluster_segment_store::
  SEGMENT_STREAM = u64::MAX`, `backup::BACKUP_SEGMENT_STREAM = u64::MAX -
  1`) — a `TabletId` (`animus_tablet::TabletId`, monotonic from 1, never
  reused) can never plausibly reach this range, so it can never collide
  with a real group's own `stream = tablet_id` address. ADR 0026's
  single-consumer-per-`(node, stream)` rule is why a fourth distinct
  constant was needed rather than reusing one of the other three: two
  independent serving tasks bound to the same stream would race for the
  same inbox.
- **Responses are not batched.** The `AppendEntriesResp` a demuxed
  heartbeat produces ships back on the responding group's own stream,
  individually, exactly as today. Two reasons: the demux is a serial,
  on-arrival dispatch with no natural aggregation point the way the
  deadline-driven send side has, and an `AppendEntriesResp` is exactly the
  traffic class `animus_control::persist_round::ships_before_durable`
  gates on a durability round — batching it would need either reproducing
  that gating outside the drive loop or deferring the batch until every
  constituent response's own round lands, for a direction the PR 1 cost
  model never measured as the problem. Every per-group invariant (§3 of
  the design doc) still holds either way, since the demuxed message is fed
  through the identical `core.handle` the wire-arrived path already uses —
  batching only ever touches the request direction's own transport.
- **Hosted-group lookup table: owned inside `animus-cp-data`, not
  `animusd`'s `ClusterEdgeState`/`host::Reconciler`.** Each
  `HeartbeatBatcher` keeps its own `stream -> HeartbeatInbox` map,
  populated by `RaftKvNode`'s own `drive` loop at start/teardown. This
  keeps the demux working under a bare `SimEnv` test with no `animusd` in
  the loop, and needs no second registry kept in sync with the reconciler's
  own `hosted` map.
- **Flag shape and reach: additive-default, threaded exactly like
  `--quiesce-after`.** `host::Reconciler::enable_heartbeat_batching()`
  mirrors `enable_quiescence`'s "opt in once, applies to every group hosted
  from then on" contract; `animusd`'s `--heartbeat-batch` CLI flag (a bare
  boolean, no value — the batcher's own flush cadence is fixed at
  `RaftCore::heartbeat_interval`, so there is no companion duration to
  parse) and `cluster_settings.heartbeat_batch` config-file field reach
  `--config FILE --node I`, `--cluster N`, and `animusd data --config`
  through the identical wrapper chain `--quiesce-after` already threads
  through, including that knob's own same documented gaps on
  `--cluster-control`/`--cluster-data`, `join`, and `data --seed` (every
  one of those paths hardcodes `false`, matching `--quiesce-after`'s own
  `Duration::ZERO` at the identical call sites).
- **Metrics keep the PR 1 recommendation**: `Metric::CpAppendEntriesSent`
  still counts one per logical per-group heartbeat, recorded at
  `HeartbeatBatcher::register` instead of the ordinary outbound-send path
  when batching is on, so its meaning is unchanged whether or not the flag
  is set. Two new counters observe the batcher itself:
  `Metric::CpHeartbeatFramesSent` (one per physical frame — flat in the
  number of co-hosted groups sharing a destination) and
  `Metric::CpHeartbeatDemuxDropped` (a frame naming a not-currently-hosted
  group — an ordinary, harmless release/host race, never a panic).
- **Measured (`crates/animus-cp-data/tests/heartbeat_batch_corpus.rs`,
  cell (a))**: with every group's leader forced to the same physical node
  (so leadership doesn't spread across the cluster as group count grows —
  see that test's own doc), 1 group vs. 5 groups gives `frames1=238`,
  `frames5=238` (ratio 1.00) against `logical1=238`, `logical5=1190`
  (ratio 5.00) — physical frames stay flat while the logical per-group
  count keeps scaling with `G`, exactly the amortization this phase set
  out to prove.
- **What PR 3's cutover flips**: turning the flag on by default (or
  removing it, if the maintainer decides the batcher should be the only
  path) — this PR intentionally ships the mechanism proven correct and
  measured, but inert in production until that decision is made
  explicitly, per this repo's own "(2) batcher behind a flag; (3) cutover"
  plan.

See `crates/animus-cp-data/src/heartbeat_batch.rs`'s own module doc for
the full sender/receiver design and
`crates/animus-cp-data/tests/heartbeat_batch_corpus.rs` for the
fault-injection corpus (seed knob `ANIMUS_HEARTBEAT_SEEDS`): frame-vs-
logical scaling, every per-group invariant under batching over a long
run, a genuine partition losing a whole batched frame at once (elections
still fire), a lossy-but-connected link at a rate the unbatched path
already tolerates (no spurious elections), an unknown group in a received
frame (dropped and counted), and leader/follower kill converging with
batching on.

## Amendment (2026-09-06): phase 2 cutover — batching on by default (C-02 PR 3)

Flips the flag PR 2 shipped: `--heartbeat-batch`/`cluster_settings.
heartbeat_batch` now default **ON** (`animusd::main::DEFAULT_HEARTBEAT_
BATCH = true`) rather than off — a node started with no flag at all now
gets the per-node `HeartbeatBatcher` from the moment it hosts its first CP
group. The mechanism itself, landed and measured by PR 2, is **unchanged**
— this PR only flips which behavior a caller gets by default and keeps the
opt-out:

- **The opt-out is kept, not removed** — of the two options PR 2's own
  closing paragraph named ("turning the flag on by default... or removing
  it, if the maintainer decides the batcher should be the only path"), the
  maintainer chose the former: an operator can still reach the mechanism
  switch in the field via **`--no-heartbeat-batch`** (a bare boolean flag,
  the mirror of `--heartbeat-batch`) or `cluster_settings.heartbeat_batch:
  false`. `--heartbeat-batch` itself is now a no-op restating the default
  (kept for explicit/scripted invocations and back-compat with a PR-2-era
  invocation) rather than deleted outright.
- **Reach and gaps are byte-for-byte identical to PR 2's own** — same
  wrapper chain, same entry points (`--config`/`--node`, `--cluster N`,
  `animusd data --config` via `cluster_settings.heartbeat_batch`), same
  documented gaps (`--cluster-control`/`--cluster-data`, `join`/`data
  --seed`, and every narrower test wrapper) where the knob hardcodes
  `false` regardless of the new default — exactly the same shape
  `--quiesce-after`'s own default-ON resolution has always had (that
  flag's `DEFAULT_QUIESCE_AFTER_SECS` is applied only in
  `quiesce_after_duration`, called at the two real deployment-path CLI
  entry points; every narrower wrapper hardcodes `Duration::ZERO`
  regardless). This PR did not widen reach — a follow-up closing one of
  `--quiesce-after`'s own documented gaps should close the matching
  `--heartbeat-batch` one in the same change, per `crates/animusd/
  CLAUDE.md`'s own note.
- **Why default-ON now, not at PR 2**: PR 2's own corpus
  (`heartbeat_batch_corpus.rs`, `ANIMUS_HEARTBEAT_SEEDS`) already proved
  every per-group invariant holds under batching — election timers, term,
  commit index, ReadIndex confirmation, a genuine partition losing a whole
  frame, a lossy-but-connected link, an unknown group in a frame, and
  leader/follower kill, all converging correctly. What PR 2 could not
  prove is real-thread liveness (`SimEnv` proves logic and ordering, not
  real OS-thread/timer scheduling — root `CLAUDE.md`'s standing lesson).
  This PR adds that proof: `crates/animusd/tests/
  heartbeat_batch_liveness.rs`, a real `ProdEnv` 3-node cluster hosting
  three tablet groups with batching on by default (no flag passed) —
  stable leader/term under continuous traffic for a fixed wall interval,
  then re-election within a bounded budget after killing the physical node
  leading the most groups, with reads/writes continuing to work throughout
  via the survivors. Run 5x locally with no flake before this cutover
  landed. Combined with the whole existing `cargo test --workspace` /
  `prod-liveness-*` suite set passing unmodified with batching on by
  default, this closes the "proven correct... but inert in production"
  gap PR 2's own closing note named.
- **The PR 1 baseline (`heartbeat_cost.rs`) now pins the amortization as
  default behavior, not merely an opt-in capability** — its own single
  test used to host `G` independent, unbatched groups (no shared node env,
  since nothing to batch existed yet) and assert `AppendEntries` traffic
  scales with `G`; it now hosts `G` groups co-hosted on the SAME three
  physical nodes with the batcher attached (mirroring PR 2's own corpus
  cell (a) exactly — `RaftKvNode::start_hosted_campaigning_with_batcher`/
  `start_hosted_with_batcher`, deterministic fixed leader) and asserts
  `Metric::CpHeartbeatFramesSent` stays flat (`frames1=238`, `frames5=238`,
  ratio 1.00) while `Metric::CpAppendEntriesSent` — the logical per-group
  count — keeps scaling with `G` exactly as before (`logical1=238`,
  `logical5=1190`, ratio 5.00). The flag-OFF proof this displaced moved to
  `heartbeat_batch_corpus.rs` as a new explicit opt-out cell
  (`batching_off_frames_scale_with_groups_like_the_old_default`),
  independent `Simulator` worlds exactly like the old `heartbeat_cost.rs`
  shape — `RaftKvNode::start_hosted_with_batcher(.., None)` calls `env.
  metrics()` internally, which is `SimEnv`'s no-op handle unless a caller
  injects one, and there is no co-hosted-AND-injectable-metrics-AND-no-
  batcher constructor, so the unbatched proof still needs
  `start_with_metrics`'s own independent-world shape (this was hit as a
  real bug while building this cutover — an early draft tried co-hosting
  the unbatched cell and got zero traffic recorded everywhere; see
  `docs/engineering-lessons.md`).
- **No production code changed beyond `animusd`'s own default resolution**
  — `crates/animus-cp-data/src/heartbeat_batch.rs` is untouched by this
  PR; every mechanism decision PR 2's own amendment above recorded (the
  reserved stream id, response-batching, receiver-lookup ownership,
  `CpAppendEntriesSent`'s unchanged meaning) stands as-is.

This closes the C-02 stack: "(1) investigation, (2) batcher behind a flag,
(3) cutover" is now complete. See `crates/animusd/CLAUDE.md`'s "Heartbeat
batching" section for the CLI/config plumbing detail and
`docs/design/heartbeat-send-sites.md`'s own closing note for the design
doc's final account.

## Amendment (2026-09-07): phase 3 assessment (C-03) — deferred, not built

Roadmap item C-03 asked whether the cheap-groups roadmap's follow-up 3
("Asymmetric replicas," above — DynamoDB's own "log replica" precedent, a
quorum member holding the Raft log durably but carrying no engine state
and never leading) is still worth building now that C-02 (heartbeat
amortization) and C-05 (`SharedWal`, ADR 0028) have both landed and
defaulted on. **This is an assessment only — no mechanism ships here.**

### What phase 3 targeted, restated precisely

Item 3's own text names the cost this ADR opened with (above, "Removing
merge means a tablet's per-group overhead... is now genuinely permanent"):
**"a Raft WAL file, election/heartbeat timers, a voter-set-tracking
group"** — paid by *every* replica of every group, leader and follower
alike, not just the leader. A log-only replica removes the "WAL file" and
"engine" halves of that on non-leading replicas by construction (no
materialized state to keep, so nothing to apply, compact, or serve reads
from); it does not by itself touch the "voter-set-tracking group" half —
the in-memory `RaftCore`/`RaftKvNode` bookkeeping and its one `drive` task
per hosted group, which a log-only replica still needs (it is still a
voter, still receives and acks `AppendEntries`).

### What has already been closed since this ADR's own text, item by item

- **Election/heartbeat timers, and the apply task's own idle poll** (the
  larger of the two, per ADR 0048's own correction) — closed by ADR 0048
  quiescence (phase 1), default on at 5s (`--quiesce-after`,
  `main::DEFAULT_QUIESCE_AFTER_SECS`). Applies to every hosted replica,
  voter or (since ADR 0058 Train 1) learner alike — see
  `crates/animus-control/CLAUDE.md`'s learner-membership entry: "a fully-idle
  group's learners stop ticking too."
- **The active-load heartbeat cost** — quiescence only zeros the *idle*
  term; a busy group still ticks `RaftCore::heartbeat_interval` on every
  peer. C-02's `HeartbeatBatcher` (default on since its PR 3 cutover)
  coalesces every co-hosted group's heartbeat toward the same destination
  node into one physical frame per interval — `Metric::
  CpHeartbeatFramesSent` stays flat in group count `G` while `Metric::
  CpAppendEntriesSent` (the logical per-group count) keeps scaling with
  `G`, per the phase-2-cutover amendment's own measured numbers above
  (`frames1=238, frames5=238` vs. `logical1=238, logical5=1190`).
- **The "one WAL file per group" cost, active-load** — C-05's `SharedWal`
  (default on since its PR 3 cutover) replaces every hosted group's
  private WAL file with one shared file per node. Reproduced this session,
  same host as the committed
  [`docs/design/shared-wal-fsync-benchmark.md`](../design/shared-wal-fsync-benchmark.md)
  numbers (`cargo bench -p animus-cp-data --bench wal_fsync_bench`,
  `ANIMUS_BENCH_ROUNDS=5 ANIMUS_BENCH_GROUPS=1,8,32,128`, ext4 on
  `/dev/vda`): at `K=128` concurrently-active groups, per-group files p50
  = 11.87ms/round (128.00 fsyncs/round) vs. `SharedWal` p50 = 1.62ms/round
  (2.00 fsyncs/round) — a ~7.3x latency cut and a 64x fsync-count cut,
  consistent with that document's own three-run table (10.5–11.2ms →
  1.4–1.6ms at K=128, held across runs).

That closes two of the three named per-group cost items outright, and
both are default-on in production today — not opt-in capabilities someone
still has to enable. What phase 3's "carries no engine state" clause
targets is specifically the third: the per-tablet storage engine (ADR
0050).

### What remains, measured this session

`cargo test -p animus-storage --test idle_engine_cost --features
prod-heavy -- --ignored --nocapture` (same host, same session as the WAL
bench above): **100 idle `LsmEngine` instances → 811,008 bytes RSS delta,
~8,110 bytes/engine** — under 0.4% of the 2 MiB/engine sanity ceiling
`idle_engine_cost.rs`'s own gating assertion uses (ADR 0050's own rung-1
gating claim). The file's always-on structural companion test
(`idle_engine_open_is_passive_no_files_no_tasks`) also holds: an
unwritten engine's `open()` creates no files, spawns no task, arms no
timer. **The one per-group cost phase 3 would remove that the first two
closures above don't already reach is, on today's measurement, already
negligible at idle** — there is very little idle-engine cost left for a
log-only replica to save.

### What is not measured, and is not sized here

The per-group **in-memory** `RaftCore`/`RaftKvNode` bookkeeping itself
(voter/learner sets, `next_index`/`match_index` maps, `TxnTracker`, and
the one `drive` async task per hosted group) has no equivalent RSS/CPU
harness — `idle_engine_cost.rs` measures only the storage layer below it.
A comparable measurement (host `N` `RaftKvNode`s on one `ProdEnv` node
with quiescence + heartbeat batching + `SharedWal` all enabled, diff
RSS/CPU before and after, mirroring `idle_engine_cost.rs`'s own shape) is
sized at roughly a day (M) and was not built in this assessment, per its
own ~30-minute measurement budget. This is the one number that could
still argue for phase 3's storage-engine-agnostic half (a log-only
replica also runs a lighter-weight core than a full voter would need to,
even leaving the engine question aside) and nobody has produced it.

### The design reason not to build phase 3 as originally sketched

[ADR 0055](0055-eventually-consistent-reads.md) (2026-08-23) shipped
*after* this ADR's own text and changes the calculus more than either
measurement above does. It depends on **every replica of a tablet**
carrying a full applied engine, precisely so a `ConsistentRead: false`
read can be served from *any* replica's own local state with no leader
round trip — closing what that ADR calls, in its own words, "the single
most conspicuous scaling gap in v1": "No read scaling at all... a read
path that funnels a tablet's entire read volume through one node." A
phase-3 log-only replica carries no engine by definition and so could
never serve one of these reads. Converting any of a table's ordinary RF3
voters into a log-only member — the natural reading of item 3's own
"asymmetric replicas" framing — would directly shrink the read-scaling
fan-out ADR 0055 exists to buy back. The two designs were written on
opposite sides of that later decision and now pull against each other at
the default replication factor.

### Recommendation: defer, not size

Two of phase 3's three named cost pillars are closed and default-on
(timers, WAL); the third (idle storage-engine footprint) is measured
negligible; and the one thing phase 3 would still uniquely buy — a
lighter-weight per-group core, unmeasured — would come at the direct
expense of ADR 0055's read-scaling story if built the way this ADR
originally sketched it (an ordinary voter converted to log-only at RF3).
That is not "no, never" — it is "not this design, not without a number
that isn't measured yet."

If phase 3 is ever revisited, it is not the same feature this ADR
originally proposed. It would need to be re-scoped as **extra,
narrowly-asymmetric voters added *beyond* a full-copy, read-serving
quorum** — e.g., RF3 full copies (unchanged, still every replica
eligible for ADR 0055's local eventual reads) plus N additional log-only
members purely for write-durability/failure-domain spread at RF > 3 —
never a wholesale conversion of an ordinary replica, and it would need
ADR 0055's own `stale_read_ready()` "any replica" fan-out amended
alongside it to exclude a log-only member by construction, not by
convention. Two conditions would reopen it: **(a)** the unmeasured
per-group `RaftCore`/task cost above, once actually measured, shows a
real bite at a realistic per-node tablet density (hundreds to thousands
of hosted groups on one node — this fleet size has not been produced or
tested anywhere in this codebase yet); or **(b)** a deployment need for
RF > 3 driven by failure-domain spread rather than read scaling, where
the added replicas' storage/compute cost (not their read capacity) is
specifically what is worth cutting. Neither condition holds today.

`docs/roadmap.md`'s C-03 entry is updated to record this outcome.
