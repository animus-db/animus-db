# ADR 0043 — The stream-shard subsystem

- **Status:** Proposed
- **Date:** 2026-08-14
- **Amends:** [ADR 0028](0028-shared-storage-single-command-split.md) (the
  kind-scope set a tablet group owns grows to seven: `KIND_CURSOR` (base
  tablets, this ADR's foundation PR) plus `KIND_STREAM`/`KIND_STREAM_META`
  (shard tablets); the tablet map now also hosts groups that are
  structurally exempt from ever splitting), [ADR 0033](0033-tablet-merge.md)
  and [ADR 0034](0034-byte-based-auto-split.md) (a stream table's tablets
  are exempt from merge and auto-split — their ranges are fixed at
  provisioning), [ADR 0024](0024-drop-table-data-gc.md) (drop cascade
  extends to a table's hidden stream table), [ADR 0013](0013-replicated-schemas.md)
  (`StreamSpec` replicates in the catalog), [ADR 0035](0035-control-plane-separate-deployment.md)
  (a third **streams** role assembly joins the control/data pair).
- **Depends on:** [ADR 0042](0042-dynamo-streams.md) (the contract this
  subsystem serves), [ADR 0018](0018-cross-tablet-transactions.md) (HLC
  timestamps, apply-time monotonicity), [ADR 0031](0031-tablet-host-reconciler.md)
  (the per-node tablet-host reconciler this subsystem needs zero new code
  from, by construction — see §1).

## Context

ADR 0042 specifies *what* DynamoDB Streams means for this adapter: per-item
ordering, eventual consistency via copier lag, exactly-once in the log,
leader-local reads, and a multi-consumer change-log lifecycle. This ADR
specifies *how* it is built: where stream records physically live, how they
get there from a table's own change log, and how the whole thing inherits
this codebase's existing distributed machinery — placement, replication,
snapshotting, membership change, drop-GC — rather than reinventing any of
it.

Verified against `main` at the time of writing:

- `KvCommand::KindBatch` (`crates/animus-cp-data/src/lib.rs`) is the
  existing multi-kind atomic batch primitive ADR 0041 introduced; its apply
  arm completes a change-log record's key with `hlc::pack(ts)` at apply
  time, from the entry's own commit timestamp — the "structural,
  apply-assigned suffix" pattern this ADR's shard position assignment
  (§5) mirrors exactly.
- `ALL_KINDS` (`crates/animus-cp-data/src/lib.rs`) is the single place a row
  kind is registered — a group derives one `StorageScope` sibling per entry,
  the snapshot image iterates it, drop-table GC erases each in turn. `KIND_CURSOR
  = 0x04` already exists there (landed alongside ADR 0042; see that ADR's
  foundation PR and `crates/animus-cp-data/src/cursor.rs`).
- `pending_changes` (`crates/animus-cp-data/src/lib.rs`) is a whole-
  `KIND_CHANGE`-scope sweep whose key order is per-partition-key commit
  order, **not** global commit order (token-then-pk-then-HLC) — the fact
  that shapes both the cursor design (ADR 0042 §8) and why shard positions
  cannot be source-HLC-keyed (§5 below).
- `MetaCommand::CreateTablet` (`crates/animus-control/src/meta.rs`) carries
  a `range` but its apply arm **rejects a second tablet for a table that
  already has one** — "one `CreateTablet` per table" is a deliberate
  race-safety property of ADR 0023's provision-at-create design. Provisioning
  *N* shard tablets atomically therefore needs a genuinely new command
  (§6), not a loop of `CreateTablet` calls.
- `host.rs` (the ADR 0031 per-node tablet-host reconciler) is table-agnostic
  over the tablet map: it hosts, repairs, absorbs, and reclaims whatever
  tablets `Metadata` names, with zero awareness of what table a tablet
  belongs to. Modeling a stream shard as an ordinary (if permanently
  unsplittable) tablet means this subsystem needs **no new reconciler
  code** — the single largest structural win this design rests on.

## Decision

**A DynamoDB stream's shards are the tablets of a hidden, per-stream table
with a fixed number of equal token-range tablets, placed only on a new
dedicated **streams** node role. Two new `KvCommand` variants
(`StreamAppend`/`StreamTrim`) and three new row kinds carry the actual
records; a unified per-node change-consumer loop (the existing GSI drain,
a new stream copier, and a log janitor) moves data from a source table's
change log into the shard tablets.**

### 1. Shards are tablets of a hidden per-stream table

A streamed table `T` gets a hidden table `T$streams$<label>` (two `$`s —
illegal in a DynamoDB/CQL identifier, so it can never collide with a real
table name, the same enforcement ADR 0041 §1 already added for
`CreateTableSchema`; `is_stream_table_name`/`split_stream_table_name`
join the existing `is_index_table_name`/`split_index_table_name` pair, and
every `$`-classifier call site enumerated in the implementing PR). Its
tablets partition token space into `N` **fixed, equal** ranges — fixed at
`CreateStreamShards` time (§3, §6), never split or merged afterward. Which
range contains `token(pk)` **is** the shard identity (`shardId-<i>` on the
wire for the `i`-th range, plus a reserved generation field so a future
resharding — ADR 0042 roadmap item (a) — is additive to the identity scheme
rather than a breaking change to it) — the ordinary `cp_route(stream_table,
key)` already resolves an append to the right shard leader, hinted retry
included, with no new routing primitive.

Because a stream shard is *just a tablet of a table*, it inherits, with
zero new distributed machinery: hosting and repair (the ADR 0031
reconciler), placement and rebalancing (`animus-placement`'s pure
`replan`/`rebalance_step`, constrained by §2 below), snapshot catch-up
(`engine_image`/`InstallSnapshot`), single-server membership change, and
drop-table GC (ADR 0024). Split and merge are explicitly **guarded off** at
the state-machine level (every replica agrees, not merely convention):
`Metadata::apply`'s `SplitTablet`/`MergeTablets` arms reject a stream
table's tablets outright, `animusd::auto_split_loop` skips them the same
way it already skips a GSI's hidden table, and the GSI drain's own
table-classification logic gains the stream case alongside the index one.

### 2. Placement: a dedicated streams role

**Stream tablets are placed only on nodes running a new, dedicated
**streams** role — never colocated on ordinary data nodes.** This is an
explicit departure from the plan's original recommendation (colocation, on
the reasoning that a stream shard is "just another tablet"); the owner's
rationale is payload-profile separation: a data node's tablets see mixed
point reads/writes, while a stream shard sees pure sequential
append-then-scan traffic plus periodic retention-trim churn — different
enough access patterns that sharing a node's engine and its LSM compaction
schedule with ordinary table data is undesirable at any real scale, even
though nothing here would be *incorrect* about colocating them.

Mechanically, this is a third role assembly in the shape ADR 0035 already
established for the control/data split: `animusd streams --config FILE
--node I` runs a node hosting only stream-shard tablets (no control
`RaftCore`, `Metadata` from a mirror exactly like a data-only node); a
**combined** node (`animusd --cluster N`, or `--config FILE --node I` with
no explicit role) carries all three roles, so single-process dev clusters
keep working unmodified. Placement enforcement reuses the **existing**
label/residency policy machinery (`animus-placement`'s `required_labels`,
ADR 0005) — a streams-role node is labeled accordingly at startup, and
`CreateStreamShards`'s own replica selection (§6) is constrained to nodes
carrying that label, the identical mechanism that already keeps
failure-domain-spread residency rules working for ordinary tables. Default
replication factor is 3, same policy machinery as any other tablet — a
stream shard is not special-cased in the placement *engine*, only
*labeled* into a disjoint node pool.

`host.rs`'s reconciler needs **no new code** for this — a streams-role node
simply never runs `host.rs`'s per-tablet logic for anything but the tablets
`Metadata` places on it, and `Metadata` never places a stream tablet on a
node lacking the label in the first place (the placement engine's own
constraint, enforced before a tablet ever reaches a reconciler at all).

### 3. Shard count: fixed at enable, growth-compatible by construction

`--stream-shards` (default **1**) fixes a stream's shard count at
`CreateStreamShards` time; there is no elastic shard count in v1. The
number is deliberately small by default because per-item ordering (ADR 0042
§1) already caps how much a single hot partition key benefits from more
shards, and because ADR 0042's roadmap item (a) — generation-cut,
doubling-only resharding — is designed to be the actual scaling lever, not
a same-generation shard-count edit.

**Growth-compatibility is a first-class constraint on this PR's design,
not an afterthought deferred entirely to the roadmap item:** the shard
identity scheme (§1) reserves a generation component from day one; iterator
tokens (ADR 0042 §6) carry that identity so a token minted against
generation 0 remains meaningful (or cleanly rejected) once generation 1
exists; and `CreateStreamShards`'s own apply guard (§6) is written to be
**generation-aware** — "this table already has shard tablets" rejects a
second `CreateStreamShards` for the *same* generation, but a future
generation-cut command is an **extension**, not a violation of that guard,
so the roadmap item needs no rework of this PR's state-machine contract
when it lands.

### 4. Storage: three new kinds, one version bump

| Kind | Selector | Where | Holds |
|---|---|---|---|
| `KIND_CURSOR` | `0x04` | base tablets of a GSI'd/streamed table | Consumer cursor rows (ADR 0042 §8) — landed with ADR 0042's foundation PR. |
| `KIND_STREAM` | `0x05` | stream-shard tablets | Records: key = position (`u64`, big-endian), value = `{source_key, source_hlc, ChangeRecord}`. |
| `KIND_STREAM_META` | `0x06` | stream-shard tablets | Per-partition-key dedupe rows (`token || escape(pk) → last-admitted packed HLC`) plus two engine-wide markers, `next_position` and `trim_horizon`, under a `[0x00, tag]` lead pair mirroring `txn.rs`'s own escape-disjointness proof. |

`ALL_KINDS` grows to seven; the snapshot codec's `VERSION` bump to 13
(landed with the `KIND_CURSOR` foundation PR) is deliberately **one bump
covering all three new kinds** across this whole stack, since the
`ImageEntry` layout itself does not change again for `KIND_STREAM`/
`KIND_STREAM_META` — only a new, already-generic kind byte.

**Why `StorageEngine`-under-scopes and not a bespoke append-only segment
store**, which would be the more obviously "log-shaped" choice for
something this sequential:

- **Durability and crash recovery come for free.** The engine's own
  WAL/apply path is already sim-tested for crash recovery through the `Env`
  disk seam (ADR 0003); a bespoke segment file format would need its own
  from-scratch recovery story.
- **Snapshot shipping comes for free.** `engine_image`/`InstallSnapshot`
  already iterate `ALL_KINDS`; a new kind is carried with no new wire
  format and no new catch-up code path for a slow or replaced shard
  replica.
- **Scope-bounded erasure for GC and retention.** Drop-table GC
  (`erase_scope`) and this ADR's own retention trim both need "delete
  everything in this range, in this kind, on this tablet" — exactly what a
  `StorageScope` already gives for free.
- **Physical separation from other kinds means retention churn never
  touches unrelated bytes.** `KIND_STREAM`'s LSM tombstone churn from
  routine trim is an accepted cost, but it stays confined to its own scope
  — it can never force a compaction rewrite of, say, a co-hosted stream
  shard's own dedupe rows (a different kind, different scope, same
  argument ADR 0041 §3 already made for keeping the change log physically
  separate from base rows).

### 5. The two new `KvCommand`s

Both are **fence-less**, like `Seal`/`ReadCeiling` — a shard tablet's range
never changes (§1's split/merge exemption), and positions are not token
keys, so there is no crossover window for a fence to guard.

**`StreamAppend { records: Vec<EncodedRecord>, ts }`** — proposed by the
copier, applied on the shard leader (and every replica, identically):
flush any pending merge work (the same precedent `Cas`'s apply arm already
establishes for ordering merges before deciding), then per record, in
order: read the dedupe row for the record's own partition key; **admit iff
`record.source_hlc` strictly exceeds the last-admitted HLC there**; on
admission, write the record at the shard's current `next_position`,
increment it, and update the dedupe row to this record's `source_hlc`. Every
step is deterministic and replica-agnostic — no clock, no RNG, so every
replica reaches the identical decision for the identical entry. The
assigned positions are recorded in a driver-side outcomes map keyed by
Raft log index, exactly the shape `CasResults`/`StageOutcomes` already use
elsewhere in this crate. **The copier polls this outcomes map directly,
never "wait until applied, then assume the outcome"** — the ADR 0018 §4
corpus already found and fixed this exact gotcha once (`stage_outcome`): a
snapshot install can advance a replica's `engine_applied` past a given log
index without that replica ever individually applying (hence recording an
outcome for) the entry at that index, so a wait-then-fetch two-step is
unsound and a direct poll is the only correct shape.

**`StreamTrim { before_position: u64, ts }`** — proposed by the shard
leader's own periodic retention tick, carrying an **already-computed**
bound: the leader scans forward from its durable `trim_horizon` while
`record.source_hlc.wall_ms < now - retention`, and the resulting position
is what the command carries — apply itself touches no wall clock at all,
keeping the actual decision deterministic and replica-agnostic exactly like
every other apply arm in this crate. Apply deletes every record below
`before_position`, advances `trim_horizon` to it, and prunes any dedupe row
whose last-admitted wall time has aged out of the retention window.
**Dedupe-row trim safety**: a duplicate can only ever arrive from a copier
re-reading behind its own *durable* cursor, and that cursor regresses by at
most one unacknowledged batch — a window of seconds, not hours — so
trimming a dedupe row on the same multi-hour-to-day retention window
leaves an enormous, asserted-in-the-corpus safety margin before a
resurrected duplicate could ever be wrongly re-admitted.

### 6. New `MetaCommand`s

Each needs an `mirror.rs::apply_and_derive_mirror` arm, an
`is_relayable_command` decision, and no new `syskv::EntityKind` (a
`StreamSpec` rides inside the existing table schema catalog entry, §4's
`KIND_CURSOR` and this section's shard tablets are ordinary tablet-map
rows).

**`SetTableStream { table, spec: Option<StreamSpec> }`** — enable (mints a
fresh `label`, see ADR 0042 §4) or disable a table's stream. Relayable —
same class as any other schema-catalog mutation.

**`CreateStreamShards { table, shards: Vec<(TabletId, KeyRange,
Vec<NodeId>)> }`** — one atomic apply minting all `N` shard tablets for a
table's stream at once. **Why this can't just be `N` calls to the existing
`MetaCommand::CreateTablet`**: `CreateTablet`'s own apply arm (`meta.rs`)
rejects a second tablet for a table that already has one — a deliberate
race-safety property (two nodes racing differently-allocated tablet ids for
the *first* tablet of a table both propose `CreateTablet`, and only the
first commits) that would make every shard after the first for the *same*
table a guaranteed rejection if reused naively. `CreateStreamShards`
therefore needs its own apply-time guard: **rejected if the table already
has any shard tablets** (first-committer-wins, mirroring `CreateTablet`'s
own race-safety argument, just generalized to "the first N-tablet batch
wins" instead of "the first one tablet wins") — and, per §3's
growth-compatibility requirement, this guard is written **generation-aware**
so a future generation-cut command extends rather than violates it.

The proposer is the DynamoDB edge's `SetTableStream`-enable handler, with
the copier acting as a **lazy backstop** exactly mirroring the GSI drain's
own lazy hidden-table provisioning (ADR 0041 §4's as-built note) — a crash
between `SetTableStream` and `CreateStreamShards` committing is repaired by
the copier idempotently re-proposing on its next tick, never left stuck.
Replica selection picks ids and replica sets the same way
`provision_tablet` already does for an ordinary table, recording the
**target** RF policy per the existing lesson about not baking a
point-in-time replica count into a tablet's own record (`animusd/src/
lib.rs`); per §2, selection is additionally constrained to nodes carrying
the streams-role label.

### 7. The unified change-consumer loop

ADR 0041's GSI drain and this ADR's stream copier are **two arms of one
per-node loop**, replacing the standalone `index_drain_loop` — the "one
event-driven loop" philosophy ADR 0031 already established for the tablet
host reconciler, applied to change-log consumption. Per tick, per source
tablet this node leads:

1. Read the tablet's cursor rows (`KIND_CURSOR`, both tags present), and
   compute each consumer's effective watermark by the min-over-rows rule
   (ADR 0042 §8).
2. `pending = pending_changes()` filtered to records above the copier's own
   watermark, grouped by destination shard (the fixed range containing the
   record's own token) — within one group, the change log's own key order
   already gives per-partition-key HLC order, so no separate sort is
   needed.
3. For each destination shard, forward a bounded `StreamAppend` batch to
   the shard leader over the ordinary `cp_route` on the stream table (ADR
   0043 §1's routing inheritance). **An append counts only once the
   outcomes map confirms the applied positions** (§5) — an `Accepted`
   propose result is never treated as "committed," the same
   `ProposeResult` doctrine this codebase applies everywhere durability
   matters.
4. Only after **every** destination shard for this tick's batch has
   confirmed: propose the copier's own cursor-row advance (an ordinary
   `KindBatch` write into `KIND_CURSOR`) to the new watermark.
   **Advance-only-on-full-confirmation is what makes this
   durable-before-visible**: a crash anywhere in steps 2–4 leaves the
   cursor at its old value, so the next tick simply re-reads from there —
   any records a partially-completed tick already forwarded are silently
   absorbed by the shard's own dedupe row (§5), never double-counted or
   lost.

Failure modes, each converging without operator intervention: a
destination shard group without quorum fails step 3, so the cursor never
advances and records simply accumulate in the (bounded, still-trimming-for-
other-consumers) change log until the shard recovers; a source tablet's
leadership moving mid-tick means the new leader's own next tick resumes
from the last *durable* cursor row, re-sending anything the old leader
hadn't yet confirmed; a copier process crash mid-batch is indistinguishable
from the leadership-move case from the shard's point of view. Observability
(metrics seam, ADR 0015, plus an `/admin/streams` surface) lands **with**
this same PR: `copier_backlog_records`, `copier_lag_ms`,
`change_log_bytes`, `stream_append_dup_rejected`, `shard_trim_horizon`.

The **janitor** is the loop's third arm: once every *expected, present*
consumer's watermark is known (step 1's own computation, generalized across
both tags), it deletes change-log records at or below the minimum in
bounded `KindBatch` batches — the trim policy ADR 0042 §8 specifies,
folded into the same tick rather than a fourth standalone loop.

### 8. Rejected alternatives

**Source-HLC-keyed shard positions**, instead of an apply-assigned
monotonic counter. Rejected: source tablets copy toward a shard at
independent lags, so a briefly-behind source's record would need to insert
*below* a position an iterator has already consumed past under this
scheme — a silent, unrecoverable loss. See ADR 0042 §5 for the full
argument; this alternative is unsound, not merely inconvenient.

**Slot-indirection for an elastic shard count** (a level of indirection
mapping "logical slot" to "current shard," so `N` could change without a
lineage event). Rejected on both branches of its own trade-off: either the
indirection is itself versioned per-item (in which case it silently
reintroduces exactly the migration machinery a fixed generation-cut
resharding was meant to avoid), or it is coarser than per-item (in which
case it breaks the per-item ordering guarantee ADR 0042 §1 exists to
preserve). Neither shape is actually simpler than the generation-cut design
this ADR commits to as roadmap item (a).

**Kafka-style add-only partitions** (grow the partition count without ever
closing an old one, relying on a hash-mod-N reassignment). Rejected for the
same reason as slot indirection: changing `N` changes which partition a
given key hashes to for *every* existing key, which breaks per-item
ordering across the boundary of the count change — exactly the property
DynamoDB Streams (and this ADR) guarantee never breaks except through an
explicit, documented lineage event.

**A bespoke append-only segment store**, instead of `StorageEngine` under
new kind scopes. Rejected per §4's argument: it would forgo durability,
crash recovery, and snapshot shipping this codebase already has fully
sim-tested, in exchange for marginally more "log-shaped" storage this
system's actual access pattern (bounded per-shard throughput, not a
firehose) does not need.

**A new crate for the shard subsystem.** Rejected: the two new
`KvCommand`s, the row kinds, and the pure key-codec helpers are a small,
tightly-coupled extension of `animus-cp-data`'s existing apply path (the
exact shape `txn.rs`/`seal.rs` already establish for a self-contained
module inside this crate) — a new crate would only add a dependency edge
with nothing to show for it.

**Colocated placement on ordinary data nodes** (the plan's original
recommendation). Superseded by owner decision: see §2 for the
payload-profile-separation rationale. Structurally, colocation would have
been strictly *less* work (no third role assembly); it was declined
anyway because the operational argument — isolating a stream's sequential
append/retention-trim churn from a data node's mixed point-read/write
workload — was judged to matter more than saving that implementation
effort.

## Consequences

**Easier.**

- A stream shard is an ordinary tablet-map row: it is hosted, repaired,
  placed, rebalanced, snapshotted, and reclaimed with **zero new
  reconciler code** — the single largest simplification this design rests
  on, inherited directly from ADR 0031's table-agnostic reconciler.
- Growth (ADR 0042 roadmap item (a)) is additive to this PR's own
  contracts (the shard identity scheme, the iterator token shape, and
  `CreateStreamShards`'s own guard are all written generation-aware from
  day one), so it does not require reopening this ADR's core design when it
  ships.
- The streams role reuses the exact placement/residency/config-assembly
  machinery ADR 0035 already built for control/data separation — a third
  role is a data point proving that machinery generalizes, not a one-off.

**Harder, and knowingly accepted.**

- **A third role assembly** is more deployment-shape surface area
  (`animusd streams`, config parsing, `gen-config` updates, combined-mode
  carrying all three roles) for operators and for this codebase's own test
  matrix to cover.
- **Two new `KvCommand` variants** mean two more gating call sites to keep
  in the relay/forwarding allowlist (`is_relayable_command`,
  `cp_serve_forwarded`, admin filters) — the exact class of mistake this
  codebase's own engineering-lessons log already warns is a silent,
  compiler-invisible bimodal flake if missed.
- **Retention trim adds real LSM tombstone churn** to every shard tablet,
  on top of what ADR 0041 already accepted for the base change log; this is
  the second tier of that same accepted cost, now paid by nodes running the
  dedicated streams role instead of ordinary data nodes.
- **The min-over-rows rule (ADR 0042 §8) is shared, load-bearing state**
  between this ADR's copier and ADR 0041's GSI drain — a bug in one
  consumer's cursor discipline can, in principle, affect the other's trim
  safety on a table that has both. The corpus (§8's completeness/ordering
  checker, ADR 0042 roadmap) is what keeps this honest across releases.
