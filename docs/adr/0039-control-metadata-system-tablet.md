# ADR 0039 — Control-plane metadata as a genuine data-plane tablet (Option B, revisited)

- **Status:** Proposed. This is a design-only ADR: it is **not scheduled for
  implementation**. It records the shape a future "system tablet" would take
  if the team ever revisits ADR 0038's Option B, and — this ADR's main
  contribution — the concrete condition under which that revisit is actually
  worth it, which is narrower than ADR 0038's rejection table assumed.
- **Date:** 2026-08-10
- **Amends/relates to:** ADR 0038 (the per-node system-keyspace design this
  would eventually supersede, *if* the revisit criterion below is ever met —
  until then the two coexist as "shipped" vs. "documented next step"); reuses
  ADR 0016/0017/0028 (the per-tablet CP data plane and its shared-storage/
  split machinery); ADR 0031 (the per-node tablet-host reconciler); ADR 0037
  (control-plane membership change, reused here as the meta-tablet's own
  reconfiguration primitive); depends on ADR 0018 (cross-tablet transactions,
  still Proposed) for its actual scaling payoff, per the finding below; touches
  one boundary of ADR 0035 (a control-only node's storage shape).

## Context

ADR 0038 gave the control plane's `Metadata` a `DRIVER_APPLIED` state machine
backed by a per-node system-keyspace engine slice (`syskv`), fixing durability
and snapshot/compaction cost from O(whole-cluster-state) to O(touched
entities) per mutation. Its own options table named a "bootstrap system
tablet" (Option B) as the eventual right answer *if* control-plane scale
itself — not the data plane — ever became the bottleneck, and rejected
building it now because nothing in front of the team required it, and because
it is "a second major consensus-integration project on par with ADR
0016/0017." This ADR takes that revisit seriously: it works out the concrete
design (bootstrap, repair, migration, scale numbers) and, in doing so, finds a
dependency ADR 0038 did not surface — Option B's payoff is gated on ADR 0018,
not on this ADR alone. See "The headline finding" below.

### What ADR 0038 fixed, and what it explicitly did not

ADR 0038 fixed **durability and compaction cost**: every mutation now
derives and persists only the system-keyspace writes it actually implies (one
tablet, one member, one schema entry), not a whole-`Metadata` re-serialize.
It did **not** fix, and named as the future trigger: *"the control Raft
log/WAL itself — not the data plane — is the throughput ceiling."* Concretely,
two things stay O(whole-cluster-state) even after ADR 0038:

1. **Cold/rejoining-voter catch-up.** A control voter that is new or has been
   partitioned long enough to fall behind the compacted prefix is caught up
   via `InstallSnapshot`, which — even with ADR 0038's lazy, engine-scan-built
   image — still ships **one** monolithic image, from **one** leader, in
   `SNAPSHOT_CHUNK_BYTES = 1024`-byte chunks (`animus-control/src/raft.rs`).
   The chunking makes each round trip cheap; it does not reduce the **number**
   of round trips, which is `image_bytes / 1024`.
2. **Sustained mutation throughput.** Every member heartbeat flip, every
   tablet epoch-CAS (split/merge/rebalance/reconfigure), and every schema DDL
   cluster-wide still serializes through **one** Raft leader's **one** log.
   Nothing about ADR 0038 parallelizes this — it made each entry cheaper to
   make durable, not the log itself capable of accepting more than one
   leader's worth of entries per unit time.

**Concrete scale numbers**, grounding ADR 0038's own "thousands of tablets, a
few hundred members" scale reference instead of leaving it a qualitative
claim: `syskv` values are plain `serde_json` (confirmed in `mirror.rs` — no
compact binary codec the way `animus-cp-data`'s `codec.rs` uses for the data
plane specifically to avoid JSON's byte-array inflation). A `Tablet` entity
(id, optional table name, range, replica list, epoch, version floor) runs
roughly 200–400 bytes JSON with field names included; a `NodeAddrs` entry
(four socket-address strings plus role) runs roughly 300–400 bytes. At 10,000
tablets and a few hundred members — ADR 0038's own scale reference — the
syskv image is on the order of a few megabytes. At 1024 bytes/chunk that is
several thousand `InstallSnapshot` round trips to catch up **one** voter:
tens of seconds at a generous few-millisecond RTT, minutes at a plausible
100,000-tablet cluster. The `WatchMetadata` incremental-delta path (ADR 0038
PR5's `DeltaRing`) does not change this ceiling either: its default bound
(1024 entries / 4 MiB) means any watcher falling further behind than that
during, say, a large rebalance storm falls back to exactly this same
full-image path, through the same one leader.

### The headline finding: Option B's payoff is gated on ADR 0018

A **single, never-split** meta tablet — the smallest version of Option B —
does not actually fix either number above. It replaces ADR 0038's bespoke
apply task with `animus-cp-data`'s existing tablet-hosting machinery (a real
architectural simplification, see "What carries over" below), but a single
Raft group is still one leader, one log, one `InstallSnapshot` image, no
matter whose plumbing hosts it. The only thing that would actually relieve
either ceiling is **splitting the meta tablet** once it grows — the same byte-
threshold auto-split every user table already gets (ADR 0034) — so that
metadata mutation throughput and cold-catch-up cost are spread across more
than one Raft group and leader.

But splitting the meta tablet reintroduces exactly the problem this codebase
has deferred once already: `SplitTablet`, `MergeTablets`, and
`CasTabletReplicas` all read and atomically update **more than one** logical
entity today (`MergeTablets` reads two tablets' epochs from one snapshot;
placement CAS reasons about a tablet against the current member set). That is
free — a single in-memory struct, one apply, one Raft group — as long as
"the tablet map" lives in one tablet. The moment it is split across two
meta-tablets, an operation that spans the split boundary needs cross-tablet
atomicity, which is precisely **ADR 0018** (cross-tablet transactions),
**still Proposed and unbuilt**.

**This is the central, debatable point of this ADR:** shipping a single
unsplit meta tablet is real, bounded plumbing work that buys architectural
uniformity but *not* the scaling win that is Option B's entire justification.
The scaling win only arrives once the meta tablet can be split, which is
downstream of ADR 0018 landing (or of a narrower, meta-tablet-only two-phase
commit scoped just to the handful of commands that cross tablet-map
boundaries). Any future decision to schedule this ADR should treat it as
**paired with, or sequenced after, ADR 0018** — not as an independent,
self-justifying project.

## Decision (the shape, not a build plan)

### 1. Bootstrap circularity

Reserve `TabletId(0)` permanently as the meta tablet. This costs nothing
today: `Metadata::next_free_tablet_id` already starts allocation at `1` on an
empty cluster (`crates/animus-control/src/meta.rs`), so no existing or future
cluster can ever mint id `0` by accident — the reservation is a documentation
change, not a code change.

The meta tablet's replica set is **defined to always equal the control
group's own live Raft voter set** — it is not a value stored inside the
tablet map (reading the tablet map to place tablet 0 is exactly the
circularity to avoid), and it is not a separately-CASed "root pointer" value
either (considered and rejected below). Concretely: **tablet 0's Raft group
is the same physical Raft group as the control plane's own `RaftCore`** —
same log, same WAL, same voters, same leader election. A cold-starting node
discovers it exactly the way it discovers the control quorum today (ADR
0032's seed-address / `JoinInfo` bootstrap) — no new discovery primitive,
because "is a control voter" and "hosts tablet 0" are the same fact by
construction, not two facts that happen to agree.

*Alternative considered: a separate root-pointer command.* A
`MetaCommand`-shaped `CasMetaTabletReplicas` that lets the meta tablet's
replica set diverge from the control voter set (e.g., 5 control voters for
quorum stability, only 3 of them hosting tablet 0's log for latency) was
considered and rejected for v1 of this design: it reintroduces exactly the
kind of special-cased repair logic §2 below shows is unnecessary if the two
sets are simply kept identical, for a benefit (independent tuning of two
group sizes) nothing today asks for.

### 2. Who reconciles/repairs the meta tablet itself

Nothing new. Because "hosting tablet 0" is structurally identical to "being a
control voter," **ADR 0037's existing `change_membership`/
`transfer_leadership` primitives and admin surface
(`control-add`/`control-remove`/`control-grow`) are the meta tablet's own
reconfiguration mechanism**, with no new code. The ordinary per-node
`host::Reconciler` (ADR 0031) — which decides "should I host tablet T" by
*reading* the tablet map — never runs for tablet 0 at all; it is excluded
from its purview the same way it is already excluded from
`next_free_tablet_id`'s allocation space.

**Failure envelope.** Total quorum loss (a majority of control voters gone)
is *exactly* today's ADR 0037 control-quorum-loss story: no automatic
replace-on-failure (deliberately operator-driven there already), the same
count-only quorum guard and its documented stranding risk, the same "surviving
minority + manual `control-add` of fresh replicas, InstallSnapshot-caught-up"
recovery path. Option B introduces **no new failure mode here** — it inherits
ADR 0037's failure analysis verbatim, including its accepted risks, because
the meta tablet's liveness question and the control group's liveness question
are the same question. The one operational change: "back up cluster
metadata" now means backing up tablet 0's Raft log/snapshot (an
`animus-cp-data`-shaped artifact) rather than a whole-`Metadata` JSON blob —
a small but real change to any backup/restore tooling built against ADR
0038's shape.

### 3. What stays in the control Raft log vs. moves to the meta tablet

Under this design, nothing control-plane-specific survives beyond what
`RaftCore` already tracks structurally as its own voter configuration
(config-in-log, no state-machine involvement needed). There is no separate
"control state machine" left at all: the tablet map, schema catalog, node
address book, keyspaces, placement policies, and both monotonic id allocators
(today's `syskv::EntityKind` set, unchanged) become tablet 0's own KV rows.

Importantly, tablet 0's apply logic does **not** need to be rewritten as
dumb single-key `put`/`delete`/`cas` operations. `RaftCore<C, S>` (ADR 0016)
is already generic over its command and state-machine types — tablet 0 can
run essentially `RaftCore<MetaCommand, Metadata>`-shaped apply logic
(multi-entity epoch-CAS, schema validation, the exact pure functions
`Metadata::apply` already implements) **unchanged**, just executed inside
`animus-cp-data`'s `RaftKvNode` apply path instead of `animus-control`'s
bespoke `meta_apply_loop`. A multi-key atomic update (e.g. `MergeTablets`
reading two tablets' epochs) is not a hard problem as long as both keys live
in the *same* Raft group — it is exactly as easy as today's `KvCommand::Batch`
(N keys, one log entry, one apply). It only becomes a cross-tablet-transaction
problem once metadata is *split* across more than one meta-tablet — which is
precisely the headline finding above.

Does the control plane dissolve as a deployment role? **No.** ADR 0035's
three deployment shapes (control-only / data-only / combined) are unchanged;
a control-only node still exists, it just additionally hosts one tablet (via
`animus-cp-data`'s machinery) instead of running a bespoke apply task. One
correction to ADR 0035 this design would need: it currently documents a
control-only node as having "no storage engine, no `raftkv` env" (true again
as of ADR 0038, which gave it a *dedicated system-keyspace* engine, not a
`raftkv` env) — under this design a control-only node would have a minimal
`raftkv`-shaped env scoped to exactly one tablet.

### 4. Migration from ADR 0038's keyspace (online, not fresh-bootstrap)

ADR 0038 could assume a fresh cluster bring-up ("pre-alpha, no back-compat
promise"). This design explicitly cannot make that assumption — by the time
it might be built, ADR 0038 will already be running in deployed clusters.
Reuse ADR 0038's own proven delivery shape (encode → shadow mirror → cutover)
one level up:

1. **Seed.** Stand up a real tablet-0 `RaftKvNode`, seeded from a one-time
   bulk replay of the current syskv engine scan — literally reusing
   `mirror::rebuild_metadata_from_engine`'s existing scan-to-entity pipeline
   as the seed source, expressed as one large `put_batch`.
2. **Shadow.** Dual-write for at least one full compaction cycle: every
   control-plane mutation continues to derive syskv writes *and* is proposed
   against tablet 0, differentially checked for agreement — the same
   discipline `apply_engine.rs` already proves for ADR 0038's own cutover,
   one level up.
3. **Cutover.** Swap the apply task's write path from "derive syskv writes
   from `Metadata::apply`" to "propose the equivalent op against tablet 0 and
   wait for its own apply," and swap the read cache from "syskv scan" to a
   tablet-0 ReadIndex scan/tail.

The genuinely new migration-engineering cost versus ADR 0038: this cutover
has to be provable safe against a **live** cluster with existing tablet ids,
not a fresh bring-up — the shadow phase's differential-agreement window is
load-bearing in a way ADR 0038 never had to prove under real traffic.

### 5. Concrete revisit criterion

Revisit this ADR only when either is true, both directly observable with
metrics/mechanisms that already exist:

- A rejoining or long-partitioned control voter's `InstallSnapshot` catch-up
  (the existing `snapshot_installs` metric, ADR 0015) regularly takes
  multi-second-to-minutes in production — which only happens once tablet +
  member + schema entity count reaches the tens of thousands, per the byte
  estimate above.
- The control leader's own append/commit rate (observable via
  `append_entries_sent` plus control-WAL fsync latency) shows unbounded
  proposal-queue growth under the existing `reconcile_loop`/`detect_loop`/
  heartbeat cadence — the concrete signature of one leader's one log being
  the actual ceiling, not a proxy for it.

Neither condition is met today, and — per the headline finding — meeting one
is necessary but not sufficient to make building this worthwhile: the team
should also have a settled (or settling) ADR 0018 design before scheduling
this, since the throughput relief requires the meta tablet to actually split.

## What carries over verbatim vs. gets replaced

**Carries over unchanged:**

- `syskv.rs`'s key encoding and `EntityKind` set — becomes tablet 0's literal
  logical schema, not a storage-mirror detail bolted onto a separate struct.
- `mirror.rs`'s `apply_and_derive_mirror`/`rebuild_metadata_from_engine`/
  `apply_key_write` decode logic — the entity ⇄ key/value mapping is
  identical; only the *source* of those writes changes (a committed tablet-0
  apply instead of the control apply task's `Metadata::apply`).
- The `DeltaRing`/`WatchMetadata` incremental-delta *shape* — actually gets
  **simpler**: tablet 0's own committed writes already are the delta: no
  `MetaCommand`-to-`KeyWrite` derivation step is needed at all.
- `RaftCore`, `DRIVER_APPLIED`, chunked `InstallSnapshot`, durable-before-
  visible gating — entirely unchanged; this is the same generic core
  `animus-cp-data` already proves works for a KV-shaped state machine.
- ADR 0037's `change_membership`/`transfer_leadership` — reused for free as
  tablet 0's own reconfiguration mechanism (§2 above).
- All of `animus-cp-data`'s `StorageScope`/split/merge/rebalance/
  `host::Reconciler` machinery — becomes directly applicable to metadata
  itself. This is the mechanical meaning of "metadata becomes shardable":
  nothing new needs to be built for it, it is inherited by virtue of tablet 0
  being an ordinary tablet.

**Gets replaced:**

- `Metadata::apply` as a privileged, single-process, whole-struct decision
  point served by a bespoke apply task — replaced by the same pure apply
  logic running inside tablet 0's ordinary `RaftKvNode` apply path.
- `meta_apply_loop`/`meta_apply_and_compact` (`animus-control/src/node.rs`) —
  retired; `animus-cp-data`'s existing consensus-loop/apply-task split does
  the identical job.
- The whole-`Metadata` `serde_json` snapshot format — replaced by tablet 0's
  ordinary engine-checkpoint snapshot, the same mechanism any tablet already
  uses.

## Non-goals (of this ADR, and of the design it describes if ever built)

- **Not scheduled for implementation.** This is a design record for a future
  debate, not a build plan.
- Does not change per-tablet CP data-plane mechanics for user tables.
- Does not itself split the meta tablet. The design describes a single,
  unsplit tablet 0 as the first increment; splitting it — the step that
  actually delivers the scaling payoff — is explicitly out of scope here and
  gated on ADR 0018 per the headline finding.
- Does not change `Metadata::reconcile`/`rebalance`'s placement-decision
  algorithms — only where their input data physically lives.
- Does not change ADR 0035's deployment-shape boundaries beyond the one
  correction noted in §3 (a control-only node gains a minimal single-tablet
  `raftkv`-shaped env).
- Does not solve cross-tablet transactions (ADR 0018) — it depends on that
  ADR's outcome rather than restating it.

## Consequences (if this were ever built)

**What would become easier:**

- One mental model ("everything is a tablet") instead of two hosting/apply/
  snapshot mechanisms to maintain in parallel.
- ADR 0038's `syskv`/`mirror` investment is not wasted — it becomes the
  schema tablet 0 adopts, not throwaway work.
- A control-only node gets the same split/snapshot/hosting machinery as any
  data node instead of a second, bespoke implementation to keep in sync with
  it.

**Costs and risks knowingly accepted, if scheduled:**

- **Real migration engineering against a live cluster**, unlike ADR 0038's
  fresh-bootstrap escape hatch — the shadow/differential-agreement phase (§4)
  is load-bearing under real traffic, not just a pre-alpha convenience.
- **Every `~ctx.control` call site ADR 0038 already audited needs
  re-auditing**: some O(1) struct-field reads become tablet-shaped scans
  (bounded, but a real latency-shape change worth measuring, not assuming
  away).
- **Shipping only the single-meta-tablet increment is schema/plumbing churn
  without the scaling benefit** — the throughput ceiling this ADR exists to
  eventually relieve is not actually relieved until paired with a meta-tablet
  split, which needs ADR 0018. A team scheduling this work should have an
  explicit answer to "why now, given the payoff isn't here yet" before
  starting, not treat "it's more uniform" alone as sufficient justification.

## What we'd prototype first

1. **Single, never-split tablet 0** running `MetaCommand`/`Metadata`-shaped
   apply logic hosted via `animus-cp-data`'s `RaftKvNode`/`host::Reconciler`,
   replacing ADR 0038's bespoke apply task. Goal: prove **parity** with ADR
   0038's current shape (not yet a scaling win), and specifically prove that
   ADR 0037's `change_membership` cleanly doubles as tablet 0's own
   reconfiguration with no special-casing.
2. **Only after (1) is solid, and only once ADR 0018 has a settled design**:
   attempt an actual meta-tablet split, working out cross-tablet atomicity
   for the `SplitTablet`/`MergeTablets`/`CasTabletReplicas`-shaped operations
   that would now cross a meta-tablet boundary. If ADR 0018 is not moving,
   that is the concrete argument for prioritizing it — this design's real
   payoff is unreachable without it.

## See also

- `docs/adr/0038-control-metadata-system-keyspace.md` — the shipped design
  this would eventually supersede, and the numbers (`SNAPSHOT_CHUNK_BYTES`,
  entity byte estimates) this ADR grounds its scale analysis in.
- `docs/adr/0018-cross-tablet-transactions.md` — the dependency the headline
  finding surfaces; still Proposed.
- `crates/animus-cp-data/CLAUDE.md` — the tablet-hosting/split/snapshot
  machinery this design reuses in full.
- `crates/animus-control/CLAUDE.md` — `meta.rs`/`syskv.rs`/`mirror.rs`, the
  pieces that carry over largely unchanged.
