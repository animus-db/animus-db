# ADR 0018 — Cross-tablet transactions on the CP plane (2PC over per-tablet Raft + HLC + MVCC)

- **Status:** Accepted — in delivery (PR1: HLC + sim clock skew landed; PR2:
  HLC commit timestamps as the CP-plane MVCC version + the range-seal design
  landed; PR2b: MVCC snapshot reads at a timestamp + the read-timestamp
  cache/logged read ceiling landed; PR3: single-participant transactions —
  the value envelope + the txn record/intent/resolve machinery through one
  Raft group — landed; PR4: multi-participant 2PC across tablet Raft
  groups, the wire-level coordinator, foreign-intent resolution, and
  uncertainty-interval read restarts — landed; PR5: in-doubt transaction
  recovery + the per-node intent-resolver background task — landed; PR6-PR7
  sequenced). See the "Amendment (2026-08-11, PR1)" section for the
  build-time decisions settled at the start of delivery, the "Amendment
  (2026-08-11, PR2)" section for the range-seal design that replaces
  `version_floor`, the "Amendment (2026-08-11, PR2b)" section for the read
  path + serializability write-push mechanism, the "Amendment (2026-08-12,
  PR3)" section for the record/intent/resolve machinery, the "Amendment
  (2026-08-12, PR4)" section for multi-participant 2PC, and the "Amendment
  (2026-08-12, PR5)" section below for in-doubt recovery + the resolver.
- **Date:** 2026-08-03

## Context

The leaderful CP data plane (ADR 0017) gives **single-tablet** linearizability:
each tablet is its own Raft group, and reads/writes *within* one tablet are
linearizable. It gives **no atomicity across tablets** — a logical operation that
touches keys in two tablets is two independent Raft commits, with no guarantee
both land or neither does, and no agreed order relative to other such operations.
ADR 0017 §5 explicitly deferred cross-tablet atomic transactions as "the
designated next step." This ADR settles that design.

The forces and prior decisions that shape it:

- **Determinism is the correctness story (ADR 0003).** Every distributed
  behavior must be establishable under `SimEnv`, byte-reproducible from a seed.
  In particular there is **no TrueTime hardware** and no special clock: time is
  the `Env` `Clock` seam (virtual under `SimEnv`). Any design that needs
  real-time bounds for *safety* is out (ADR 0017 §3's lease analysis: a timing
  assumption may gate *liveness*, never *correctness*).
- **The CP plane already provides per-tablet durable, replicated, linearizable
  logs (ADR 0017).** Each tablet's Raft group commits an ordered, fsynced command
  log; the Raft index is already used as the per-key MVCC version. This is exactly
  the per-range substrate a range-partitioned transactional store builds on.
- **We already have a leaderless transaction layer: Accord (ADR 0011).** Accord
  does multi-key, **cross-shard** transactions today — execution timestamps,
  dependency sets, recovery, per-shard consensus, MVCC snapshot reads — via
  local execution (the data-plane frontier was deleted with the AP plane, ADR
  0019; `animus-consensus` is a testbed, not wired into `animusd`). So
  "cross-shard atomic transactions" is not unsolved in the codebase; the open
  question is specifically how the **CP (Raft) plane** gets them, and whether it
  should reuse Accord or get its own mechanism.
- **Pluggable replication is the frame (ADR 0016).** AnimusDB deliberately offers
  both a leaderless-AP plane and a leaderful-CP plane as modular choices. Each
  plane having its *native* transaction story (rather than one bolted onto the
  other) is consistent with that frame.
- The control plane (`animus-control`) owns the tablet map, placement, and the
  schema catalog, and already replicates per-table replication mode (ADR 0017
  #3a). It is the natural authority for *which* tablets a transaction spans.

Two candidate designs were named in ADR 0017 §5:

1. **2PC across the per-tablet Raft groups**, with HLC transaction timestamps and
   MVCC snapshot reads — the Spanner/CockroachDB model.
2. **Accord layered atop the Raft groups** as the durable store.

## Decision

**We will implement cross-tablet transactions on the CP plane as two-phase commit
(2PC) across the per-tablet Raft groups, ordered by Hybrid Logical Clock (HLC)
timestamps, with MVCC snapshot reads — the CockroachDB model.** Accord (ADR 0011)
remains the leaderless transaction layer for the AP lineage; the two transaction
systems stay **separate**, one per plane, not merged.

### 1. Why 2PC-over-Raft, not Accord-over-Raft

The per-tablet Raft group is already a per-range **atomic-commit participant with
durable voting**: a "prepared" record and a "commit" record are ordinary committed
Raft entries, so a participant's vote and decision survive crashes and leader
change with no new durability machinery. 2PC is then the *minimal* addition for
cross-range atomicity — it is an atomic-commit protocol, **not** a second consensus
protocol (the consensus that makes each vote durable is the participant's own Raft).
This is the proven CockroachDB/Spanner layering: Raft per range for replication,
2PC across ranges for atomicity, a clock for ordering.

Layering **Accord** atop the Raft groups was rejected as the CP mechanism:

- Accord is **leaderless** — its value is reaching agreement *without* a per-shard
  leader. The CP plane deliberately *has* a per-tablet leader (Raft). Running a
  leaderless coordinator over leaderful participants is redundant: two agreement
  mechanisms stacked, with Accord's leaderless advantage neutralized by the Raft
  leader underneath.
- Accord already carries its own durability and per-shard consensus (ADR 0011); a
  Raft group underneath would be a *third* durable log in the path.
- Keeping Accord as the **AP-plane** transaction option and 2PC/HLC as the
  **CP-plane** option gives each plane its native, well-matched protocol (ADR
  0016) and keeps each implementation focused.

### 2. Timestamps: HLC, not TrueTime — serializable, not externally consistent

Transactions are ordered by **Hybrid Logical Clock** timestamps: a `(physical,
logical)` pair where `physical` is drawn from the `Env` `Clock` and `logical`
breaks ties / preserves causality when physical time does not advance. HLC needs
**no special hardware** and is deterministic under `SimEnv` (physical component
from the virtual clock), so it satisfies ADR 0003 — unlike Spanner's TrueTime,
which we cannot reproduce in simulation and which the determinism mandate forbids
as a *safety* dependency.

The guarantee we therefore provide is **serializability** (CockroachDB's level),
**not Spanner-style external/strict serializability**. Clock skew is handled with
a bounded **uncertainty interval** (a read may have to wait out, or restart at a
higher timestamp past, values written within the interval) — a *liveness*
cost (an occasional read restart), never a correctness one, exactly the
liveness-only discipline ADR 0017 §3 demands. We **knowingly accept** the absence
of external consistency as the price of running without TrueTime.

The CP plane already versions each key by its Raft log index (ADR 0017). The
transaction layer adds the **HLC commit timestamp** as the MVCC version visible to
snapshot reads; reconciling the within-tablet Raft-index order with the
cross-tablet HLC order (the commit timestamp is stamped into the committed value)
is a load-bearing implementation detail settled at build time.

### 3. The protocol

- **Coordinator.** Any node can coordinate a transaction (it need not host a
  participant). It assigns the transaction an HLC start timestamp, buffers writes
  as **intents**, and serves reads from an MVCC snapshot at the read timestamp.
- **Intents + prepare.** A write is staged as an *intent* (a provisional value
  tagged with the transaction id) committed **through the owning tablet's Raft
  group** — so the intent is durable and replicated by Raft. Prepare = every
  participant group has durably logged its intents and votes to commit.
- **Transaction record.** A single **transaction-status record** (committed /
  aborted / pending), itself Raft-replicated in a designated participant's group,
  is the atomic commit point: flipping it to *committed* at the HLC commit
  timestamp atomically commits the whole transaction. Intents are then resolved
  (rewritten as committed MVCC values) asynchronously.
- **Reads** resolve intents they encounter against the transaction record (commit
  → read the intent's value at its commit ts; abort → ignore; pending → wait or
  push), and observe a consistent MVCC snapshot at the read timestamp.
- **Recovery (coordinator failure).** Because intents and the transaction record
  are Raft-durable, a crashed coordinator leaves no ambiguity that cannot be
  resolved: any actor encountering a pending intent can drive the transaction
  record to a decision (commit iff all intents are present, else abort) — the
  CockroachDB "no blocking on a dead coordinator" property. This is the
  cross-tablet analogue of Accord's recovery (ADR 0011) and the CP plane's own
  membership/split recovery (ADR 0017). **Built in PR5** — see the "Amendment
  (2026-08-12, PR5)" section for the concrete push protocol, the grace-period
  liveness knob, the decision-semantics fix that makes duelling deciders legal,
  and the per-node resolver background task.

### 4. Determinism and verification

Every element rides existing deterministic seams: 2PC messages over `Env`
`Network`, intent/record durability over the Raft groups' `Env` `Disk`, HLC
physical time over `Env` `Clock`. The behavior is therefore `SimEnv`-reproducible.

It is verified the way the rest of the system is (ADR 0014): extend the Elle
corpus with a **multi-tablet CP workload** — transactions whose keys span two or
more tablets (Raft groups) — and assert **serializability** (`check_cycles`, the
safety property, scaled to the deep tier) under fault injection (coordinator crash
mid-2PC, participant-group leader kill, partition during prepare, clock skew
within and beyond the uncertainty interval). Unlike the single-tablet
`RaftPerTablet` corpus (ADR 0017), a multi-tablet workload can form genuine
cross-tablet cycles, so `check_cycles` has real teeth here. The negative control
(`animus-test`) remains the proof the checker can reject.

## Consequences

**Enabled:**

- Serializable, atomic **cross-tablet transactions** on the CP plane — the
  multi-tablet counterpart to ADR 0017's single-tablet linearizability — built on
  the Raft groups already in place, with 2PC + HLC + MVCC as the only new
  mechanism.
- A clean two-plane transaction story: **AP + Accord** (leaderless, ADR 0011) and
  **CP + 2PC/HLC** (leaderful), each native to its plane (ADR 0016).
- Reuse of the Elle harness (ADR 0014) for cross-tablet serializability checking,
  and of the CP plane's recovery discipline for in-doubt transactions.

**Costs and risks knowingly accepted:**

- **Serializable, not externally consistent.** Without TrueTime we do not provide
  Spanner's strict serializability; clock skew is absorbed by uncertainty-interval
  read restarts (a liveness cost). This is a deliberate trade for determinism and
  commodity clocks.
- **2PC latency + blocking surface.** A cross-tablet commit is two coordinated
  rounds over Raft-commit latency; a prepared-but-undecided transaction holds
  intents until resolved. The Raft-durable transaction record + recovery bound the
  blocking (no permanent block on a dead coordinator), but contention on hot keys
  rises versus single-tablet ops.
- **Intent write amplification + MVCC GC.** Provisional intents are extra Raft
  writes that must be resolved and eventually garbage-collected; MVCC versions
  accumulate and need pruning — new background work, residency-bounded like the AP
  plane's repair (ADR 0010, 0005).
- **HLC reconciliation with the Raft-index MVCC version** (ADR 0017) is subtle and
  must be gotten exactly right, or a snapshot read could observe a torn order — the
  highest-risk implementation detail, to be sim-verified before trust.
- **Two transaction systems to maintain** (Accord + 2PC/HLC). Accepted as the cost
  of the pluggable-replication design; they share the `Env` seam, the Elle harness,
  and the recovery philosophy, but are distinct codepaths.

**Follow-up (implementation sequencing, each a green-keeping increment):**

1. HLC over the `Env` `Clock` seam (a pure, unit-tested clock type), plus the MVCC
   read path at a snapshot timestamp on the CP plane.
2. Single-participant "transaction" (intents + transaction record through one Raft
   group) — the degenerate 2PC, to land the record/intent/resolve machinery.
3. Multi-participant 2PC across two+ Raft groups: prepare, commit, async intent
   resolution.
4. Recovery: resolve in-doubt transactions off a crashed coordinator from the
   Raft-durable intents + record, ballot-fenced like the CP plane's other recovery.
5. The **multi-tablet Elle corpus** (serializability under faults + clock skew),
   the safety net that lets the prior steps be trusted.

This ADR builds on ADR 0017 (the per-tablet Raft groups it makes participants),
ADR 0011 (Accord, the parallel leaderless transaction layer it deliberately does
*not* extend here), ADR 0016 (pluggable replication), ADR 0014 (the Elle
verification it reuses), and ADR 0003 (the determinism mandate that rules out
TrueTime). The control plane (ADR 0001) remains the metadata authority.

## Amendment (2026-08-11, PR1)

PR1 (`crates/animus-cp-data/src/hlc.rs` + `animus-sim`'s per-node clock skew)
is the first follow-up increment landing. Four build-time decisions were
settled going in, sharpening the Decision section above:

1. **The engine MVCC version is the packed HLC directly — `(wall_ms << 20) |
   logical`, no node-id bits.** The Decision section above only says "the HLC
   commit timestamp [becomes] the MVCC version"; PR1 settles the *encoding*:
   `hlc::pack`/`hlc::unpack` fold in nothing beyond the HLC itself, replacing
   the floor-scaled-Raft-index scheme (`effective_version = floor *
   VERSION_FLOOR_SCALE + index`, ADR 0017/`animus-cp-data`'s current
   `mvcc_version` invariant) from PR2 onward. Unlike
   `animus-consensus::node::mvcc_version`'s `(logical, node)` encoding, a
   string `NodeId` (ADR 0040) cannot be bit-packed into the low bits at all —
   and per-key monotonicity across concurrent writers to the *same* key is
   not this encoding's job to guarantee; that is the transaction layer's job,
   via a per-tablet timestamp cache plus write-conflict pushes, asserted at
   apply time (later PRs). `pack`/`unpack` hard-`assert!` their bit budgets
   (never `debug_assert!`) for the same reason `mvcc_version` does: a silent
   collision would be silent MVCC corruption, not a recoverable error.
2. **Serializability, not merely snapshot isolation, via a per-tablet
   read-timestamp cache + read-span refresh/restart.** The Decision section's
   "serializable, not externally consistent" already rules out Spanner-style
   external consistency; this settles the specific mechanism for full
   serializability (as opposed to the weaker snapshot isolation an MVCC
   timestamp alone gives): each tablet tracks the highest read timestamp
   observed per key range, and a write that would land below an already-read
   timestamp is pushed forward or the reader's span is refreshed/restarted —
   the CockroachDB read-timestamp-cache mechanism, deferred to the PR that
   lands the read path (after PR2's MVCC versioning).
3. **The transaction record lives in the first participant's tablet group,**
   keyed under a reserved sub-keyspace derived from the anchor key (the
   CockroachDB model referenced throughout the Decision section) — not a
   separate always-present system tablet. This is the concrete shape of
   "a single transaction-status record... Raft-replicated in a designated
   participant's group" (§3), settled for the PR that lands the record/intent
   machinery (Follow-up step 2).
4. **Delivery scope for the wire-facing surface**: atomic Dynamo
   `TransactWriteItems`/`TransactGetItems` plus an `/admin/txns` observability
   endpoint are in scope for this delivery. CQL LWT/atomic `BATCH` and Dynamo
   idempotency-token/`CancellationReasons` fidelity are explicitly deferred
   follow-ups, tracked separately from the Follow-up sequencing list above —
   the CP transaction *mechanism* (2PC/HLC/MVCC/recovery/Elle corpus) is this
   ADR's scope; wire-protocol fidelity beyond the two Dynamo atomic APIs is
   not blocking it.

PR1 itself adds only the pure `Hlc`/`HlcTimestamp`/`pack`/`unpack` primitives
(no MVCC/storage integration yet — that is PR2) and an opt-in, default-zero,
read-side-only per-node clock skew knob in `animus-sim`
(`Simulator::set_clock_skew_for`), which PR1's own `hlc_skew.rs` integration
test uses to prove the causality property this whole ADR's clock design rests
on: a node whose clock reads *ahead* mints a timestamp, a node whose clock
reads *behind* witnesses it, and the behind node's own next mint still
strictly exceeds the ahead node's — clock skew perturbs readings, never
causality.

## Amendment (2026-08-11, PR2)

PR2 wires the HLC into `animus-cp-data`'s apply path — the engine's MVCC
version is now `hlc::pack(cmd.ts)`, replacing the interim
`version_floor`-scaled Raft-index scheme (ADR 0017/the crate's prior "Raft log
index is the MVCC version" invariant) — and adds the **range seal**, the
mechanism that closes the one residual race a structural version-space
separation covered but plain HLC witnessing cannot.

*(Corrective note, 2026-08-12: as shipped, PR2 implicitly assumed "mint order
== log order" on a leader without enforcing it — two concurrent proposers
could mint in one order and append in the other, inverting applied `ts` order
under real-thread load (caught by `assert_ts_monotonic`, the hard assert this
amendment prescribed, via a `ProdEnv` multi-thread test — `SimEnv` cannot
express a preemption between two non-yielding calls). Fixed by
`propose_ordered`: minting and appending are one critical section under the
group's existing propose lock, plus a `last_proposed_ts` strict-floor so
ceiling/push logic also orders against proposed-but-not-yet-applied entries.
Mint order **is** log order — enforced, not assumed.)*

*(Corrective note #2, 2026-08-12: as shipped, PR2's reconciler gating (§3
below) also had an unretried one-shot proposal bug, independent of the
mint-order bug above — caught deterministically by a genuine multi-process
split-cluster deployment (control-only + data-only roles, ADR 0035), where it
permanently stalled both a split's child hosting and a merge's survivor
widening. The seal proposal used to be bundled as a side effect of the same
tick that performed the local, irreversible action it was supposed to
precede — `NarrowScope`'s local scope mutation (leader-gated propose inline),
or `Absorb`'s teardown (leader-gated propose, then a drain-wait gated only on
"nothing pending locally", not on the seal itself). Both are one-shot: the
local mutation happens unconditionally regardless of leadership, and once it
has happened the condition that would re-trigger the proposal attempt is
gone — so if leadership isn't held by the replica processing that exact tick
(a leadership change mid-handoff; an independent per-node reconcile timer in
a genuine multi-process deployment, not a combined node's synchronized
loops), the seal is never proposed by anyone, ever. Worse on the absorb side:
a follower's "nothing pending locally" drain check is satisfied trivially
before the leader has even proposed the seal, so a fast follower could tear
its own copy down first — destroying quorum before the leader's own,
now-orphaned proposal could ever commit.
Fixed two ways: (1) the seal proposal is now `plan`'s
`HostAction::ProposeSeal`, derived from a **persistent condition**
(`TabletFacts::pending_seals`) re-checked from scratch every tick — "does a
covering seal marker exist in my own engine yet" — independent of whether
this replica's local scope/teardown state has already changed, so whichever
replica eventually holds leadership gets its chance, however leadership
shuffles relative to the local mutation. (2) `Reconciler::teardown`'s Absorb
drain now additionally requires a **locally-observed committed** seal
covering this tablet's own scope before proceeding — never "nothing pending
locally" alone. This gate is self-supporting, not a deadlock risk: requiring
every absorbed replica to observe the seal before tearing down is exactly
what keeps every replica — hence the quorum needed to commit the seal in the
first place — alive for as long as it takes the seal to actually commit; a
genuinely quorum-dead group (an unrelated double failure) correctly stalls
loudly instead of tearing down early, per this system's usual
correctness-over-liveness doctrine for a durability/visibility gate. See
`crates/animus-cp-data/CLAUDE.md`'s Key Invariants entry and
`docs/engineering-lessons.md` for the full mechanism and the diagnostic
story.)*

### 1. Why `version_floor` had to go, and why witnessing alone isn't enough

`version_floor` worked by construction: a fresh/widened group's stamped
version was scaled into a strictly higher numeric *band* than anything a
different group on the same shared engine could ever stamp, so ordering was
guaranteed **regardless of timing** — even a source-group write still stuck
in its own commit pipeline, applying *after* the successor had already started
serving, could never outrank the successor. That structural guarantee is
exactly what a *timestamp*-based version cannot reproduce for free: two
different groups' `Hlc` instances are only ordered by what each has actually
minted or witnessed, and a write **still in flight** (proposed, not yet
applied) hasn't been witnessed by anyone. Witnessing — folding a group's own
recovered log, a received `AppendEntries`, an installed snapshot, or (at group
start) the shared engine's own `latest_version()` into its `Hlc` — closes
every case where causality has *already been observed by someone*, but it
cannot see a write that hasn't landed anywhere yet. A timing bound to close
that gap is exactly what ADR 0017 §3 forbids as a *correctness* mechanism.

### 2. The range seal: an ordering-based fence, not a version-space one

The replacement is structural in a different sense: instead of separating
version *numbers*, it separates **log positions**. When a source tablet hands
off a range (a split's `NarrowScope`, or a merge's `Absorb`), its leader
proposes a **`KvCommand::Seal { range, ts }`** through its own Raft log before
the range is considered handed off. Every replica of that group applies its
log in the same total order, so every replica agrees on the exact position
the seal occupies — and the apply-time rule is simple: **any later-ordered
mutating entry whose key falls in a sealed range is rejected**, exactly like a
fence miss, regardless of the entry's own embedded timestamp. Because within
one group log order and HLC order coincide (a single leader's `Hlc::mint` is
monotonic; a leader change is covered by witnessing the outgoing leader's
last entry before the incoming one ever mints), "later-ordered" and
"higher-timestamped" are the same test — but it is the **log position**, not
the numeric comparison, that is authoritative, which is what lets the seal
reject a write whose *proposer* simply hadn't learned about the split yet (the
"wide fence, un-ticked leader" case) even though that write's own timestamp,
minted after the fact, would otherwise look perfectly legitimate.

The seal's durable witness for a co-hosted **successor** (a split child, or a
merge survivor) is a **marker key written directly into the shared engine**,
deliberately outside every `StorageScope` (ADR 0026/0028) so a successor can
observe it with no scope machinery — keyed by `(source tablet id, sealed
range)` rather than tablet id alone (a tablet can seal more than once over its
lifetime; a tablet-id-only key would let a later seal silently overwrite an
earlier one's stored range before every waiting successor observed it).

**Key disjointness** was re-derived, not assumed: an earlier draft of this
design proposed a bare `[0x00, 0x00]` lead pair, reasoning that
`animus_tablet::escape` never emits it as an interior byte pair. That
reasoning was correct but incomplete — `escape("")` (the *legacy
whole-keyspace tablet's own* `StorageScope` prefix, `animusd::
table_scope_prefix("")`) is **exactly** `[0x00, 0x00]`, a genuine collision.
The shipped design instead reuses `animus_control::syskv::RESERVED_NAMESPACE`
— already the sole, replicated-state-machine-enforced (`is_reserved_name`, at
`CreateTableSchema`) reservation no user table may ever claim — and proves
disjointness from `escape`'s own documented injective/prefix-free property:
`escape(RESERVED_NAMESPACE)` can never equal or prefix-match
`escape(other_table_name)` for any name that isn't itself
`RESERVED_NAMESPACE`, and no schema can ever register under that name. See
`crates/animus-cp-data/src/seal.rs`'s module doc for the full argument.

### 3. Reconciler gating: the other half of the mechanism

A seal only protects a range while a source group keeps mutating it; the
matching obligation is that a **successor must not start serving that range
until it can see the seal**. The tablet-host reconciler (`animus-cp-data`'s
`host` module) gates on exactly this: a split child's `HostAction::Host` is
deferred until this node's own engine contains the parent's seal marker
covering the child's range (`Metadata::split_parents`, new provenance,
mirroring `merged_tablets`'s never-pruned discipline); a merge survivor's
`HostAction::WidenScope` is deferred until the absorbed tablet's seal marker
covers the widened portion (`Metadata::absorbed_by`, the reverse-direction
provenance). Both facts are gathered as bounded, tablet-scoped engine scans
(`gather_facts`), keeping `plan` itself pure.

*(As shipped, the seal proposal itself was a one-shot side effect bundled
into the same tick as the local `NarrowScope` mutation or the `Absorb`
drain-wait — see Corrective note #2 above for the bug this produced and its
fix: proposing the seal is now `plan`'s own `HostAction::ProposeSeal`, a
persistent condition re-derived every tick, decoupled entirely from the
local scope mutation / teardown timing; and the absorbed side's drain-wait
additionally requires the seal to be locally observed as **committed**, not
merely "nothing pending locally", before a replica may tear itself down.)*

**Liveness, not correctness, is what a stalled source jeopardizes**: a
split/merge successor waiting for a source-group leader to seal stalls if that
source group has no live quorum — but this is exactly the same liveness
dependency every other cross-group handoff in this system already has (the
data the successor would serve is owned by that same quorum), never a
correctness gate on timing.

### 4. What ships with PR2, what's deferred

Also landing: witnessing at the four points needed for the design to be
sound at all (WAL recovery, on every received `AppendEntries` entry, on
snapshot install, and at group start off `latest_version()`); a hard,
non-`debug` assert that every applied entry's `ts` strictly exceeds the
previous one applied by the same group (the load-bearing monotonicity
invariant — a failure means the witnessing chain itself is broken, not a
recoverable condition); and `erase_scope`'s tombstone version moving from
`last_applied() + 1` to a freshly minted `ts` (the same reasoning applies:
`Hlc::mint` is guaranteed to exceed everything this group ever stamped).

Deferred to later PRs in the sequence: the per-tablet read-timestamp cache
and read-span refresh/restart mechanism for full serializability (PR1's
Amendment §2 already named this; PR2 only lands the write-side version
scheme it will sit on top of), and the transaction record/intent machinery
itself (Follow-up steps 2+).

## Amendment (2026-08-11, PR2b)

PR2b lands two things: the MVCC **read path** at an explicit HLC timestamp
(`RaftKvNode::read_at`/`scan_at`), and the concrete mechanism for PR1
Amendment §2's promised **write-conflict push** — the per-tablet
read-timestamp cache plus a **logged read ceiling** that makes served reads
recoverable across a leader change. Both live in `animus-cp-data`
(`ts_cache.rs`, `ceiling.rs`, and `RaftKvNode`'s propose/read paths in
`lib.rs`); see that crate's `CLAUDE.md` for the file-level entry points.

### 1. Snapshot reads: `read_at`/`scan_at`

`read_at(key, ts)` and `scan_at(start, end, ts)` run the same ReadIndex
barrier as `linearizable_get`/`_scan` (quorum-confirmed leadership,
`engine_applied` caught up), then read the value(s) with MVCC version `≤
hlc::pack(ts)` — `storage.get_at`/`scan_at` (a new `StorageEngine::scan_at`,
alongside `get_at`; both engines already carried this logic internally, so
exposing it is a thin, direct addition, not new logic) instead of the
latest.

**Semantics, precisely — this is a building block, not a transaction's
read.** The result reflects every write with commit `ts' ≤ ts` that was
already committed *and applied* on this leader before the barrier
confirmed. A write with `ts' ≤ ts` still **in flight** (proposed, not yet
committed/applied) at barrier time is *not* guaranteed to be reflected;
closing that gap across multiple keys/tablets — so a multi-tablet
transaction's read sees a single consistent snapshot regardless of what any
one tablet's commit pipeline happens to be doing at read time — is the
transaction protocol's job (intents, PR3+), not this primitive's.

A `read_at`/`scan_at` whose `ts` has not yet been covered by a **committed
read ceiling** (§3 below) is **refused**: both return the same
`Option<Option<_>>` shape `linearizable_get_served` already established
(outer `None` = not served — a failed barrier or, new here, an uncovered
`ts`; inner `Some(None)` = genuinely absent). Unlike `linearizable_get`/
`_scan` (which mint their own serve `ts` and so can always drive the
ceiling forward themselves before serving), `read_at`/`scan_at` take a
caller-supplied `ts` and deliberately do **not** rubber-stamp it forward —
a caller gets the ceiling to cover its `ts` some other way first (the
simplest: any ordinary linearizable read on the same group) and retries.

### 2. The read-timestamp cache: write-conflict push

The serializability half of PR1 Amendment §2: a write must never commit at
a `ts` `≤` a `ts` at which the affected keys were already served to a
reader — otherwise a reader could have already returned a snapshot that a
later, lower-timestamped write silently invalidates.

`ts_cache.rs`'s `TsCache` is **leader-local, in-memory, best-effort
acceleration** — not the safety mechanism itself (that is §3). A
two-generation rotating `BTreeMap<(start, end), HlcTimestamp>` (no
`HashMap`/`HashSet`, per ADR 0003): every served read (`linearizable_get`/
`_scan`, `read_at`/`scan_at`) bumps the span it read at the `ts` it was
actually served at (a point read's span is `[key, key ++ [0x00])` — the
immediate lexicographic successor, so it covers exactly that one key).
`current` accumulates entries; once it exceeds a bound (4096), it rotates
into `previous` (discarded), folding the dropped generation's highest `ts`
into a coarse `low_water` floor that never regresses. **Over-conservative
eviction is safe, never wrong**: a write pushed above a floor higher than
strictly necessary is still a correct write, just a marginally
later-timestamped one — the whole design only ever errs toward pushing
writes *later*, never *earlier*.

At propose time (`put`/`put_batch`/`delete`/`cas`, via `mint_pushed`), the
leader mints its usual `ts`, computes `floor = ts_cache.max_overlapping(keys)`
(folding the committed ceiling in too, via `raise_low_water` — see §3), and
— if `ts` doesn't strictly exceed `floor` — witnesses `floor` into the
group's `Hlc` and re-mints. One retry always suffices (`Hlc::witness`'s own
contract guarantees the result strictly exceeds what it witnessed),
asserted, not merely assumed.

### 3. The logged read ceiling: leader-change safety

A leader-local cache dies with the leader — a **new** leader's fresh cache
starts empty, and could otherwise stamp a write below a read its
*predecessor* served. The fix is **ordering-based**, mirroring the range
seal's shape (§2 of the PR2 amendment): a leader that wants to serve a
read at or above the ceiling it currently believes is committed proposes
`KvCommand::ReadCeiling { ts }` through its **own** Raft log first, and no
leader may ever serve a read at a `ts` not strictly below the highest
`ReadCeiling` **committed and applied** in its group's log
(`RaftKvNode::committed_ceiling`, a lock-free atomic the apply task
advances). The candidate is `Hlc::uncertainty_upper(serve_ts)` (`serve_ts.
wall_ms + max_offset`) — a comfortable margin so ceiling proposals amortize
to roughly one per `HLC_MAX_OFFSET` (500ms) of wall time under continuous
reads, not one per read; the common case (already covered) proposes
nothing at all.

**Safety argument.** Every served read had a `ts` strictly below some
committed ceiling. On a **live leader change** (no restart), the new
leader witnessed that ceiling's `ts` via ordinary `AppendEntries` receipt
— `command_ts` (the single function both `witness_append_entries` and WAL
recovery already fold into the group's `Hlc`) covers `ReadCeiling` exactly
like every other variant — **before it could ever campaign**, since Raft
leader completeness requires it to have every entry its predecessor
committed. So the new leader's own future mints (and hence every write it
proposes, further pushed by `mint_pushed` if needed) strictly exceed that
ceiling, which strictly exceeds every read it covered. By induction this
holds across any chain of leader changes.

A **liveness** note, not a correctness one: a group that cannot commit (no
quorum) cannot advance its ceiling, so it cannot serve a read above its
current one either — reads degrade exactly when writes do, no new
availability class.

**A documented residual, not closed by this PR**: the argument above relies
on a *live* replica's in-memory `Hlc` retaining what it witnessed. A
process **restart** re-seeds `Hlc` from the recovered WAL tail plus the
engine's `latest_version()` (`start_inner`'s existing group-start witness);
a `ReadCeiling` entry carries no fence and makes no *scoped* engine write,
so — like any other applied entry — it is eligible for compaction once
`engine_applied` passes it, same as an ordinary write. A **read-only**
workload (many ceiling proposals, zero interleaved writes) can therefore
have its `ReadCeiling` entries compacted out of the log before any
ordinary write's `ts` (which *would* durably raise `latest_version()`)
happens to follow it. To close this gap regardless, apply also durably
**merges a small marker key** (`ceiling.rs`, one key per tablet, always
overwritten — disjointness proof mirrors `seal.rs`'s) at
`hlc::pack(ceiling)`: this durably raises `storage.latest_version()`, so
the *already-existing* group-start witness re-derives a floor covering the
ceiling on any future restart with zero further changes to the witnessing
chain, and `drive`'s recovery reads the marker back to seed
`committed_ceiling` directly (mirroring how `sealed` is rebuilt from its
own engine marker, not log replay). This is a **deviation from a strictly
in-memory design** — a considered fix to a real gap found while writing
this safety argument, not the "no engine write" shape first sketched, and
flagged here precisely for that reason.

**A second regression this PR's own gate run caught**: the ceiling
candidate must be disambiguated against another `ensure_ceiling_above`
call that independently computes the *same* millisecond-granular margin
(`uncertainty_upper` collapses to `logical: 0`) — but disambiguating via
`Hlc::witness` (the obvious choice) drags the *proposing leader's own*
`Hlc` forward to match a margin that is deliberately `HLC_MAX_OFFSET` in
the future, so the very next ordinary read's mint lands close to (and soon
exceeds) the ceiling just committed — turning the intended O(1) amortized
proposal rate into O(N). The fix is a **separate** CAS ratchet
(`last_ceiling_candidate`) that disambiguates the candidate sequence
without ever touching the clock ordinary reads/writes share. See
`RaftKvNode::next_ceiling_candidate`'s doc for the full account; regression
covered by `tests/ts_cache.rs`'s amortization test (which caught both this
and the original collision independently, at `ANIMUS_RAFTKV_SEEDS`-driven
depth via the shared corpus).

### 4. What ships with PR2b, what's still deferred

Landing: `read_at`/`scan_at`; `TsCache` + the propose-time write-push;
`KvCommand::ReadCeiling` (internal-only — proposed exclusively by a group's
own leader, never forwarded from a client, so no `animusd` command-relay
allowlist needs updating); the durable ceiling marker; `StorageEngine::
scan_at`/`entries_at` (new, additive trait methods alongside `get_at`/
`entries`).

Deferred: the transaction record/intent machinery itself (Follow-up step
2), which is what will actually *use* `read_at`/`scan_at` as its snapshot
read primitive and the write-push/ceiling design as the ordering
substrate a transaction's commit timestamp is chosen against.

## Amendment (2026-08-12, PR3)

PR3 lands Follow-up step 2: the **single-participant "degenerate 2PC"** —
the transaction record + intent + resolve machinery through **one** Raft
group. This is the first PR that actually stages/decides/resolves a
transaction rather than only building primitives it will need; PR4
generalizes it across multiple participant groups.

### 1. The value envelope

Every value the CP apply path merges into the engine (`Put`/`Batch`/`Cas`,
and a `TxnResolve`'s final rewrite) is now a 1-byte-tagged envelope: tag
`0` = a committed value (the rest of the bytes are the value, byte-for-byte
what the caller supplied); tag `1` = an intent, naming the staging
transaction, its record's own logical key, and the value the key will take
if the transaction commits (`None` = a staged delete). Tombstones
themselves stay untagged (the engine's own per-key tombstone bit) — the
envelope only ever wraps an actual value. Every read path
(`local_get`/`linearizable_get`/`_served`/`read_at`/`local_scan`/`scan_at`)
unwraps it before a value ever reaches a caller; a scan additionally
filters out the record marker keys below. `animus-cp-data/src/codec.rs`'s
`VERSION` was bumped alongside the four new `KvCommand` variants (below),
so a mixed-version decode fails loudly rather than silently misreading a
pre-envelope value — this codebase's standing "fresh clusters only, no
live-deployment migration path" rule (no wire/WAL back-compat is required)
means no encode-time fallback was needed.

### 2. The transaction record: identity, key scheme, and locality

A `TxnId` is `(HlcTimestamp, NodeId)` — the timestamp is the transaction's
own stage-time commit-attempt `ts`, and the node is a tiebreak: different
tablet groups run independent `Hlc` instances that never witness each
other directly, so two different groups' leaders can in principle mint
the identical `(wall_ms, logical)` pair. A `TxnRecord` holds `{txn_id,
status: Pending|Committed{commit_ts}|Aborted, intent_spans, created_ts}`;
`status` moves once, `Pending` -> `Committed`/`Aborted`, and every
reader/resolver's decision is a pure function of that one flip.

Per the PR1 amendment's decision 3, the record lives **inside** the first
(anchor) participant's own tablet, not a separate always-on system tablet
— unlike the range-seal/read-ceiling markers (`seal.rs`/`ceiling.rs`),
which are deliberately **engine-global** (outside every `StorageScope`),
a txn record has to be an ordinary in-scope logical key of one specific
tablet, so it replicates through that tablet's own Raft log, ships with
`engine_image` snapshots, and moves with a split/merge exactly like the
anchor's own data would.

That locality choice means the seal/ceiling markers' disjointness trick
(reserve a name — `RESERVED_NAMESPACE` — no user table may ever claim,
since the marker lives *outside* every scope) doesn't apply: a record has
to be provably disjoint from an arbitrary table's own row keys, which are
fully client-controlled bytes with no analogous reservation available.
The record key is `token(8 bytes) || [0x00, 0x02] || encode(txn_id)`,
where `token` is the anchor write's own 8-byte partition token (ADR 0022
— every data-plane key leads with one, unconditionally). Disjointness is
proved structurally from `animus_tablet::escape`'s own encoding rule
(never emits a lone `0x00`; every literal `0x00` byte doubles to `0x00
0x01`; the whole encoding always terminates `0x00 0x00`): a real key's
post-token suffix, `escape(pk) ++ rk`, can only ever start `[0x00, 0x00]`
(empty `pk`) or `[0x00, 0x01, ..]` (`pk` starting with a literal `0x00`)
when it starts with `0x00` at all — never `[0x00, 0x02, ..]`, for *any*
`pk`/`rk` whatsoever, however the fully-arbitrary `rk` suffix is chosen.
See `animus-cp-data/src/txn.rs`'s module doc for the full proof and
`docs/engineering-lessons.md`'s Code-patterns entry for the general
technique (find a byte position the *encoding itself* constrains, not a
naming convention, when a marker must live inside client-controlled key
space).

**A residual, documented, not closed by this PR**: a tablet split's
`split_key` is an arbitrary existing row's own key
(`animusd::auto_split_loop`'s byte-weighted median), not necessarily
token-aligned, so in principle a single token's rows — and, per this
design, its txn record — could end up split across two sibling tablets by
a split racing an in-flight transaction. PR3 is deliberately
single-participant/single-tablet in scope; split-vs.-in-flight-txn
interaction is a PR4+ concern, mirroring how the range seal itself needed
a dedicated amendment once genuine concurrent splits were exercised
(the PR2 amendment's corrective note #2).

### 3. Four new `KvCommand` variants, one Raft group

`TxnStage { txn_id, record_key, writes, spans, fence, ts }` creates/
refreshes the `Pending` record and merges every write as an intent —
whole-or-nothing against `fence`/the range seal, exactly like `Batch`: a
partial stage would let a reader observe some of a transaction's intents
but not others. `TxnCommit`/`TxnAbort { txn_id, record_key, ts }` flip the
record `Pending -> Committed{commit_ts: ts}`/`Aborted` — deliberately
**no** `fence`, like `Seal`/`ReadCeiling`: a 2PC decision must be durable
and final regardless of any later range change, and neither ever touches
user data, only the record key. Re-applying the identical decision on WAL
replay is an idempotent no-op; a *conflicting* second decision (a
different `commit_ts`, or committing an already-aborted record) is a
protocol-bug hard assert, not a silently-tolerated case. `TxnResolve {
txn_id, record_key, keys, ts }` rewrites each key still holding that
txn's intent to its final form per the record's already-decided status:
committed → the staged value (or a real tombstone, for a staged delete);
aborted → the value the key held **immediately before** the intent,
restored forward at `ts` by rewinding to the version just below the
intent's own applied version (`get_at(key, intent_version - 1)`) — never
a tombstone, which would incorrectly shadow that older, still-live
committed value. A key whose stored value is no longer that exact intent
(already resolved, or overwritten by something newer) is left untouched.

`RaftKvNode::txn_stage`/`txn_decide`/`txn_write` are the leader-side API:
`txn_write` is the one-shot convenience (stage, mint a fresh commit ts,
commit, resolve — deliberately **three** log entries, fully synchronous;
PR4 collapses/parallelizes this across multiple participant groups, not
here); `txn_stage`/`txn_decide` split it for a caller (or a test) that
needs to abort instead, or drive the phases independently.

### 4. The read path: resolving an intent

A read that encounters an intent looks up its named record (in this same
tablet's scope — the single-participant invariant) and acts on its
status: `Committed` at or before the read's own timestamp serves the
staged value; `Aborted` — or a `Committed` **after** the read's timestamp,
equally invisible to that snapshot — serves the pre-intent value via the
rewind described above; `Pending` is a **bounded retry** at a point read
(`local_get`/`linearizable_get`/`read_at`, `RaftKvNode::read_resolved`,
push/wait scheduling deferred to PR4) or a **silent omission** at a scan
(`local_scan`/`scan_at`/`linearizable_scan`, non-blocking by design in
this PR — full push/wait for a scan is also PR4). `local_get` itself
never retries at all (a raw, non-blocking peek, its existing documented
contract) — only the barrier-gated `linearizable_get`/`read_at` retry.

A `Cas` whose current-value read hits a pending intent fails
deterministically (`false`, never a guess at a match or an absence) —
every replica reaches the identical decision, so contention correctness
is preserved; PR4 revisits CAS-vs-in-flight-txn interaction (push/abort
the blocking transaction instead of just failing).

### 5. What ships with PR3, what's still deferred

Landing: the value envelope; `TxnId`/`TxnStatus`/`TxnRecord`/`Envelope`
(`txn.rs`); the four new `KvCommand` variants + their wire codec support
(`codec.rs` `VERSION` bump); `txn_stage`/`txn_decide`/`txn_write`;
scan-side record-marker filtering; a `SimEnv` test suite (commit path,
abort path, a committed delete's real tombstone, a pending read blocking
then serving once committed, intent/record markers never leaking into a
scan, crash/restart WAL-replay idempotency, snapshot-catchup carrying
records/intents like ordinary data, and a stage into an already-sealed
range being rejected wholesale) plus a `ProdEnv` concurrent hammer
extending the PR2 mint/propose-ordering regression's coverage to the new
commands.

Deferred to PR4+: multi-participant 2PC across two or more Raft groups
(prepare/commit/async resolution as genuinely separate network round
trips, not all local to one group); in-doubt recovery off a crashed
coordinator; push/wait scheduling for a `Pending` read (rather than a
bounded retry-then-fail) and for a scan; CAS-vs-in-flight-txn interaction
beyond a deterministic fail; the split-vs-in-flight-txn interaction noted
in §2; and the multi-tablet Elle corpus (Follow-up step 5), the safety
net that lets this and the prior steps be trusted at depth.

## Amendment (2026-08-12, PR4)

PR4 lands Follow-up step 3: **multi-participant 2PC across two or more
Raft groups** — the coordinator that generalizes PR3's degenerate,
single-group "2PC" into a genuine cross-tablet (and, since tablets are
table-scoped, ADR 0022/0023, potentially cross-table) atomic transaction —
plus two mechanisms the multi-participant design exposed a real need for:
foreign-intent read resolution, and uncertainty-interval read restarts.

### 1. The record-key routing question, answered

PR3's `KvCommand::TxnStage` assumed the record it creates always lives in
the *same* tablet as the stage that creates it (true by construction for a
single-participant transaction). PR4 breaks that: a non-anchor
participant's own `TxnStage` must merge intents referencing the **anchor's**
record, which lives on a different tablet — and, since tablets are
table-scoped, potentially a different **table's** ring entirely, whose
token space is independent of this tablet's own (two tables' rings can and
do assign the identical partition token to different rows). A record's key
(`token || [0x00, 0x02] || encode(txn_id)`, `txn.rs`) therefore does **not**
by itself identify which table's tablet owns it — exactly the gap flagged
as a stop-and-report item going in. **Confirmed as a real gap, and closed
structurally**: `Envelope::Intent` gained a `record_table: String` field
(the anchor's own table name), stamped into every intent `KvCommand::
TxnStage` merges, anchor and participant stages alike. A reader that can't
resolve an intent locally now has everything it needs — `record_table` +
`record_key` — to route a cross-tablet `TxnStatus` query to the record's
actual owner (§3 below). `KvCommand::TxnStage` also gained `is_anchor: bool`
(only an anchor stage's `record_key` is checked against/lives in this
group's own `fence`; a participant stage's `writes` still are, but
`record_key` is never touched here at all) and `record_table`.

A second, related simplification: `KvCommand::TxnResolve` no longer
re-derives its committed/aborted outcome by reading `record_key` locally
(PR3's shape) — it now carries an explicit `outcome: TxnOutcome` field. This
isn't just a PR4-specific patch: a non-anchor participant's own tablet
never holds the record at all, so the old "read it locally" path would
have silently done nothing (a `None` record, treated as `Pending` by PR3's
existing fence-miss-style doctrine) for every participant resolve. Carrying
the decision explicitly is sound uniformly for the anchor's own resolve too
(same code path, `RaftKvNode::txn_resolve`, used by both) and removes a
local-record dependency `TxnResolve` never actually needed for correctness
— the coordinator (or, for the single-participant case, `txn_decide`
itself) always already knows the decision by the time it proposes a
resolve.

### 2. The protocol, concretely (`RaftKvNode` primitives + `animusd::
ClientCtx::cp_txn`)

The primitives (`animus-cp-data`, `lib.rs`):

- `txn_stage(table, writes) -> (TxnId, record_key)` — PR3's method,
  unchanged in shape but now also embeds `record_table = table` into every
  intent, and is the **anchor**-only entry point (`is_anchor: true`).
- `txn_stage_participant(txn_id, record_key, record_table, writes) ->
  stage_ts` — new: a non-anchor participant's stage, referencing an
  already-known anchor record (`is_anchor: false`); creates/touches no
  record.
- `txn_commit_at_least(txn_id, record_key, min_ts) -> commit_ts` — new: the
  anchor commits its record at a ts that strictly exceeds **both**
  `min_ts` (the coordinator's candidate — see below) and this group's own
  log floor (`mint_at_least`, the same witness-and-floor shape `mint_pushed`/
  `propose_seal` already use) — returning the **actual** ts used, which may
  exceed `min_ts` if this group's own floor already had. This returned
  value, never the caller's original candidate, is the transaction's
  canonical `commit_ts`.
- `txn_resolve(txn_id, record_key, keys, outcome) -> ts` — new: the one
  low-level resolve primitive, used identically for the anchor's own keys
  and every other participant's.
- `txn_status_local(record_key) -> TxnDecisionStatus` — new: a
  ReadIndex-barrier-consistent read of this tablet's own record, for a
  caller that already knows it's talking to the record's owner (the
  cross-tablet query's server side).
- `linearizable_get_served_fast(key) -> FastRead` — new: like
  `linearizable_get_served` but a single, non-blocking resolution attempt;
  `FastRead::Foreign(IntentInfo)` (carrying `txn_id`/`record_key`/
  `record_table`/`staged_value`) is the new outcome a foreign intent
  produces, alongside the existing `Value`/`Pending`.
- `resolve_intent_given_status(key, read_ts, txn_id, status) ->
  Option<Vec<u8>>` — new: finishes a read given an externally-obtained
  status (from a `TxnStatus` round trip), re-checking the key still holds
  that exact intent before applying the same commit/abort logic PR3's local
  path uses.

The coordinator (`animusd::ClientCtx::cp_txn`, reachable via the new
`ClientRequest::Txn { writes, preconditions }`):

1. Group `writes` (`(table, key, Option<value>)`) by owning tablet
   (auto-provisioning each distinct table's first tablet on demand, as
   `cp_write` does). The **first** write's tablet is the **anchor**.
2. **Prepare**: stage the anchor first (it mints the `TxnId`/record key
   every participant needs), then every other participant **concurrently**
   (`futures::future::join_all`) via `ClientCtx::txn_prepare`, which routes
   exactly like every other CP op (serve locally, or forward one hop via
   the new `ClientRequest::TxnPrepare`). Any participant's stage failing
   aborts: propose `TxnAbort` on the anchor (`RaftKvNode::txn_decide`'s
   bundled abort+resolve) and best-effort resolve-abort every participant
   that *did* stage, then return the failure.
3. **Commit**: `candidate = max(anchor's own stage ts, every participant's
   acked stage ts)`; `commit_ts = ` the anchor's `txn_commit_at_least`
   result at that candidate — **the single Raft commit on the anchor's
   record is the atomic commit point** (the same argument PR3's decision
   already established, now for N participants: once that one entry
   commits, the transaction *is* committed, full stop, regardless of
   whether any participant's own intents are ever resolved).
4. **Resolve**: every participant (anchor's own keys included) is resolved
   with the canonical `commit_ts` via `ClientCtx::txn_resolve_participant`
   (routed like `TxnPrepare`, via the new `ClientRequest::TxnResolve`) —
   **before** this call returns to the client, not async-post-ack (see §5's
   "what PR5 owns" for why).

### 3. Reads meeting a foreign intent

A reader (`animusd::ClientCtx::cp_get_local_resolving`, the wire-facing
counterpart of PR3's `cp_get_local`) tries
`linearizable_get_served_fast` first. On `FastRead::Foreign(info)`, it
routes a new `ClientRequest::TxnStatus { table: info.record_table,
record_key: info.record_key }` to that tablet's leader (locally or
forwarded, same routing as any other CP op), which answers with
`RaftKvNode::txn_status_local`. A `Committed`/`Aborted` reply lets the
reader finish via `resolve_intent_given_status`; a `Pending` reply (or a
failed status query) reports a retryable "transaction still pending"
error — the caller's own retry loop (`cp_read`'s `"; retry"` handling)
tries again. A **locally**-`Pending` intent (the single-participant/anchor
case, unchanged from PR3) still falls back to the bounded internal wait
(`linearizable_get_served`).

**Scope of this PR's foreign-intent handling**: wired into the point-read
path (`Get`) only — `Scan`/`read_at` keep PR3's existing local-only
resolution (a still-unresolved foreign intent is silently omitted from a
scan, or reported as an ordinary "not found locally" for `read_at`). Full
push/wait scheduling for a scan, and pushing a blocking read rather than
retrying, are still PR5+ concerns per the PR3 amendment's own deferral list
— this PR only adds the *cross-tablet routing* half for the one path that
needed it to demonstrate atomic multi-tablet visibility end to end.

### 4. Uncertainty-interval read restarts

The Decision section's promised mechanism (§2: "a read may have to wait
out, or restart at a higher timestamp past, values written within the
interval") lands here: `RaftKvNode::read_at` now restarts **once** at
`Hlc::uncertainty_upper(ts)` when it observes no value at `ts` but a
version exists in `(ts, uncertainty_upper(ts)]` — over-conservative,
never wrong (the restart only ever moves the serve timestamp later, so it
can only pick up more committed data, never lose any), and bounded to one
restart (the recursive call disables further restarts). Counted via the
new `Metric::CpUncertaintyRestarts` (append-only, after
`CpReadCeilingProposals`). Not yet wired into `linearizable_get_served`
(which serves at "latest", where the question doesn't apply the same way)
or into scans — a snapshot-read-specific mechanism for now, matching where
the ADR's own language ("a read") was narrowest.

### 5. What PR5 owns (deferred, not closed here)

- **In-doubt recovery**: nothing here resolves a transaction left
  `Pending` forever by a coordinator that crashed between prepare and
  commit/abort. The anchor's record is durable (Raft-replicated), so the
  *information* needed to resolve it exists; PR5 is where a resolver
  actually acts on it (a background task, or a reader's own push, per the
  Decision section's "any actor encountering a pending intent can drive the
  transaction record to a decision" promise).
- **Push/wait scheduling for a still-`Pending` foreign or local intent**:
  this PR's coordinator and its foreign-intent read path both retry-then-
  give-up (bounded); actually *pushing* the blocking transaction (aborting
  a stale one, or waiting more intelligently) is PR5's resolver-task scope.
- **The intent-resolver background task** itself, and the `/admin/txns`
  observability surface (PR7).

### 6. Deliberate deviations from the spec, flagged honestly

- **Resolve is synchronous, not async-post-ack.** The protocol sketch calls
  for acking the client once the anchor commits, then resolving
  participants asynchronously. This PR resolves every participant
  **before** returning to the client instead: the infrastructure that would
  make an un-awaited async resolve *safe to abandon* (a background
  resolver retrying it, PR5) doesn't exist yet, so doing it inline is
  simpler and strictly safer in the meantime, at the cost of a small amount
  of extra client-visible latency. Revisit once PR5 lands.
- **The single-tablet case is not special-cased onto `RaftKvNode::
  txn_write`.** `cp_txn`'s general N-participant path degenerates to zero
  participants for a single-tablet transaction, which costs the identical
  three log entries (stage/commit/resolve) `txn_write` does — so nothing is
  lost by using one uniform code path instead of a dedicated fast path, and
  the risk of two divergent implementations is avoided. `txn_write` itself
  is untouched and still used directly by `animus-cp-data`'s own tests.
- **Condition-reads (`cp_txn`'s `preconditions`) refresh by value, not by
  HLC timestamp.** The spec describes evaluating preconditions at a read
  timestamp `R` and refreshing via a timestamped re-read only if the final
  `commit_ts` exceeds `R`. Exposing an ordinary linearizable read's serve
  timestamp back to a wire caller isn't plumbed on the client protocol yet
  (only `read_at`'s caller-chosen `ts` is) — implementing this precisely
  would need a new primitive. `cp_txn` instead re-checks every precondition
  **by value** (an ordinary linearizable read, once before staging and once
  right before the commit decision) and aborts on any mismatch — correct
  for the stated goal (catching a conflicting write that lands between
  prepare and commit) without the extra wire primitive, but not
  byte-for-byte the ADR's mechanism. Flagged as a follow-up, not silently
  substituted.
- **A wire-reachable panic, found and fixed during PR4's own test
  writing**: `RaftKvNode::txn_stage`'s hard `assert!` that its anchor key is
  at least `TOKEN_BYTES` long was a sound "caller invariant" when only
  trusted internal callers (tests, a token-shaped Dynamo/CQL key) ever
  reached it. `ClientRequest::Txn` is the first wire-facing caller that can
  hand it an arbitrary client-supplied key — an unvalidated short key would
  have panicked the whole node process. `ClientCtx::cp_txn` now validates
  every write's key length up front and returns a clean, client-facing
  error instead of ever reaching that assert. See `docs/engineering-
  lessons.md` for the general lesson (a wire-reachable caller of a method
  with a documented "caller invariant, not a recoverable condition" assert
  must itself validate that invariant, not trust the assert to protect the
  process).

### 7. Tests

`animus-cp-data/tests/txn_multi.rs` (`SimEnv`, deterministic): two- and
three-participant atomic commits (visible on every replica of every
group); abort cleanup (every staged key reverts, nothing left dangling);
foreign-intent resolution end to end (`FastRead::Foreign` →
`txn_status_local` → `resolve_intent_given_status`, the exact round trip
`animusd` performs over the network); a stage into an already-sealed range
as a true engine-level no-op (the coordinator can't distinguish it from a
genuine stage via the propose outcome alone — directly confirmed via
`local_get`); a participant leader-kill during prepare converging to a
clean abort with no half-staged intent surviving re-election; and a
five-seed reproducibility sweep of the two-participant commit shape.

`animusd/tests/cp_txn.rs` (`ProdEnv`, real 3-process cluster + a genuine
pre-split table): a multi-tablet transaction committing atomically and
being read back via a different node than it was issued through; the
**follower-connected regression** — the identical transaction issued from
**every** node in turn (proving the `TxnPrepare`/`TxnDecide`/`TxnResolve`
forwarding arms this PR adds to `cp_serve_forwarded` are wired correctly —
a missing arm here is exactly the bimodal per-process flake the house
lesson on forwarding-enum additions warns about); several transactions run
concurrently, each individually atomic; and a violated precondition
aborting the whole transaction with neither participant's write landing.

## Amendment (2026-08-12, PR5)

PR5 lands Follow-up step 4: **in-doubt recovery** off a crashed coordinator,
plus the per-node **intent-resolver background task** that both drives it
proactively and lets PR4's synchronous, blocking resolve become asynchronous
and best-effort — the deliberate deviation PR4's own amendment flagged and
promised to revisit once this landed.

### 1. Decision semantics: the log position is the ballot, not who proposed first

PR3 made a second, *conflicting* decision on an already-decided record
(`Committed` → `Abort`, or a commit at a different `commit_ts`) an assert —
sound when only one actor (the coordinator) could ever propose a decision.
Recovery makes a **second, independent decider** a normal part of the
protocol, so duelling deciders are now legal: a still-live coordinator's
commit can race a recovery pusher's abort (or vice versa), and *both*
proposals are individually well-formed. The fix is not a new consensus
mechanism — it is recognizing that one already exists: **the anchor's own
Raft log is the sole arbiter**. A `TxnRecord` lives in exactly one Raft log
(the anchor's), every replica of that group applies its log in the same
total order, and `TxnStatus::Pending -> Committed/Aborted` moves exactly
once — so whichever proposal's entry the log orders **first** is definitionally
the one that gets to flip it; every later, conflicting proposal for the
same `txn_id` finds the record already decided and is a **logged no-op**
(`tracing::warn!`, naming both outcomes), never a panic. No Accord-style
ballots (ADR 0011) are needed for this, unlike a genuinely leaderless
protocol: a ballot exists to establish a *total order* among independent
proposers with no other arbiter — here the log position already **is**
that total order, for free, because every decision proposal for one record
funnels through the same one Raft group.

The one case that stays a hard assert: two committed flips at **two
different** `commit_ts` values. That is impossible by construction (this
match arm runs once per applied log entry, in one group's own totally
ordered log — there is no way for "two different commits both won the
same log position"), so it remains what it always was: proof the witnessing
chain itself is broken, not a recoverable protocol outcome. See
`apply_and_compact`'s `TxnCommit`/`TxnAbort` arms in `animus-cp-data/src/lib.rs`
for the exact four-way match (win / idempotent replay / duelling-decider
no-op / impossible-conflict assert).

*(Corrective note, 2026-08-12, PR6: "impossible by construction" was wrong
— found live, deterministically, by the multi-tablet transaction corpus's
`participant_leader_kill_early` scenario (seed 2743871795844702347), no
exotic fault sequence needed. `txn_commit_at_least`'s own `mint_at_least`
is not idempotent across calls — each proposes a **fresh** `commit_ts` —
so two independent, individually well-formed deciders (a still-live
coordinator whose own round trip is genuinely slow, and the recovery
resolver acting past `RECOVERY_GRACE`) can each conclude "commit" for the
same `txn_id` and each get their own `TxnCommit` entry accepted, with
**different** minted timestamps. This is not a contrived edge case:
`animusd`'s own `CLIENT_TIMEOUT` (10s, the budget `cp_forward`'s
hinted-retry uses during prepare) is *longer* than `RECOVERY_GRACE` (5s),
so a coordinator whose commit round trip is merely slow — a leader
election taking a few seconds, well within ordinary fault tolerance — can
still be genuinely in flight past the point recovery is allowed to take
over. Fixed: this arm is now the same **legal, logged no-op** as the
`Aborted` arm below it — "same outcome, different timestamp" is exactly as
safe as "different outcome" duelling, since whichever entry the log orders
first still wins unconditionally and every real caller already re-reads
the record's actual decided status before resolving anything (never a
stale, losing `commit_ts` — see the torn-resolve audit this fix's own
review performed, confirming `ClientCtx::cp_txn`/`txn_recover`/
`txn_resolver_loop` all already source every resolve's `outcome` from a
post-decision re-read). The one case that remains genuinely impossible —
and stays a hard assert — is two **conflicting** decisions (`Committed` at
two different ts, both claiming to be the *actual* content, as opposed to
a second attempt at the *same* logical decision) racing to the same log
position, which one sequential log structurally rules out. Regression:
`animus-cp-data/tests/txn_recovery.rs`'s
`duelling_commits_at_different_timestamps_the_second_is_a_no_op_never_a_panic`
+ its seed sweep. A related gap this fix's own verification surfaced: the
snapshot-catchup path (`apply_and_compact`'s `install_engine_image` branch)
never rebuilt a replica's in-memory `TxnTracker` from the freshly-installed
image, unlike `start_inner`'s identical rebuild at group start (for the
identical reason — a snapshot skips the individual `TxnStage`/`TxnCommit`
entries the tracker relies on) — a replica catching up via `InstallSnapshot`
could be left with a stale `pending` entry for an already-decided
transaction. Fixed the same way, by calling `rebuild_txn_tracker` there too.)*

Every caller that decides — `animusd::ClientCtx::txn_decide_anchor` (the
ordinary coordinator path) and `ClientCtx::txn_recover` (recovery, below) —
must **re-read the record's actual status** after proposing and report
*that*, never assume its own proposal won: `RaftKvNode::txn_commit_at_least`/
`txn_abort` (a new abort-only primitive, the dual of `txn_commit_at_least`
with no inline resolve) return only the *proposed* ts, so
`txn_decide_anchor` always follows up with `txn_status_local` and returns a
`TxnOutcome` — the record's real, decided outcome — not a bare timestamp.
`ClientResponse::TxnDecided` changed shape to match (`{ outcome: TxnOutcome
}`, not `{ ts }`) — internal-only wire type, never sent bare, so this is a
clean break with no back-compat concern (house convention: fresh clusters
only).

### 1b. Staging over another transaction's unresolved intent: writers push, never overwrite (task #16)

A second, distinct durability hole the multi-tablet corpus found at depth
(`ANIMUS_TXN_SEEDS=10`, `coordinator_abandon_prepare_s01`, seed
16358087571531249382 — no fault injection needed, just ordinary sequential
same-key traffic from one client) — genuinely a **different** bug from §1
above, not another symptom of the same one. As originally shipped (PR3),
`KvCommand::TxnStage`'s apply merges every write as a fresh `Envelope::
Intent` **unconditionally**, exactly like an ordinary `Put` — no check
against whatever the key currently holds. Single-writer-per-key workloads
make this reachable in the most ordinary way: a client's transaction stages
its own anchor key, is abandoned before ever deciding (a crashed or merely
slow coordinator, `abandon_after_prepare` in the corpus's own workload
model), and a *later* transaction from the *same* client stages the *same*
key again — overwriting the first transaction's still-`Pending` intent
with its own.

That overwrite doesn't erase the old intent — MVCC keeps every version, so
the first transaction's intent survives, just no longer the *latest*
version. The corruption surfaces later: if the *second* (overwriting)
transaction is the one that eventually gets decided `Aborted` (its own
participants never staged, say), its abort-restore does what §"The value
envelope" above describes — a **one-hop-back** `get_at(key, its own intent
version - 1)` — and that one hop back lands on the **first** transaction's
still-live intent, not a genuinely committed value or true absence. The
restore then blindly re-merges that raw intent envelope as the key's new
value, at the *second* transaction's own resolve-time mint — a timestamp
strictly *higher* than the first transaction's own eventual, correct
`commit_ts` (whenever recovery gets around to deciding it). Once a later,
correct resolve tries to write the real value at that lower, correct
`commit_ts`, it loses via ordinary per-key LWW: the wrong, higher-ts
restore already won. The genuinely committed value becomes **permanently**
unreadable, not merely delayed — physically still present in the MVCC
history, but unreachable, since every read/resolve path only ever looks
one version back.

Two shapes were considered for the fix, one rejected:

- **Chase the version chain back multiple hops on the *read* side**
  (keep overwriting legal; when a restore meets a prior intent, keep
  walking backward until a `Committed` value or true absence). Rejected as
  unsound: an intermediate hop skipped over this way could belong to a
  transaction that *later commits* — and that transaction's own eventual
  resolve-rewrite would then lose to the restore's higher ts the exact
  same unrepairable way. The corruption just moves to a different
  transaction; it doesn't go away.
- **Reject the overwrite at *apply* time** (shipped): `KvCommand::
  TxnStage`'s apply now checks every target key's *current* value before
  merging anything; if it's an `Envelope::Intent` naming a **different**
  `txn_id`, the whole stage is a no-op — whole-or-nothing, exactly like a
  fence/seal miss (same-txn re-staging, a WAL-replay re-application, is
  unaffected — matched by `txn_id` equality, never mere presence of *an*
  intent). This is CockroachDB's writers-push-intents discipline, and it
  makes the corrupt chain **structurally unrepresentable**: a key can hold
  at most one transaction's unresolved intent at a time, so an
  abort-restore's one-hop-back lookback is now *always* sound. See
  `KvCommand::TxnStage`'s own doc (`animus-cp-data/src/lib.rs`) for the
  full argument, including why a plain `Put`/`Batch`/`Cas` over a foreign
  intent is *not* similarly rejected (analyzed safe: it's a genuine
  overwrite serialized strictly after the intent's own transaction, so
  that transaction's eventual resolve-rewrite correctly loses to it via
  ordinary LWW — no corrupt chain results, since nothing tries to look
  "one hop back" past an ordinary write the way abort-restore does past an
  intent).

**The other half of this fix is proposer-side, not just apply-side**: a
stage call returning `Some(ts)` only ever meant "this entry applied" (the
same footgun §1's own duelling-decider fix already had to correct for
`txn_commit_at_least`/`txn_abort` — never "my content genuinely landed").
Once a blocked stage can no-op at apply, a coordinator that doesn't check
would go on to commit a transaction **without one of its own writes ever
having happened** — a new, worse atomicity violation than the one this fix
exists to close. `animusd::ClientCtx::txn_prepare_pushing` (wrapping
`txn_prepare`) and the corpus's own `stage_anchor_pushing`/
`stage_participant_pushing` now verify every staged key via
`RaftKvNode::txn_verify_staged` (the same primitive a recovery push already
uses to check a participant's own stage) after each attempt, retrying
(bounded — a short backoff, giving the blocking transaction room to clear
via its own coordinator or `txn_resolver_loop`'s passive sweep past
`RECOVERY_GRACE`) before reporting a client-facing conflict error.
Deliberately **not** yet implemented: proactively identifying and pushing
the *specific* blocking transaction by name — the read-side machinery to
attribute a *local* (same-group) pending intent to a specific `txn_id`
doesn't exist yet (`ResolveStep`/`FastRead`'s `Pending` variant carries no
identity), so today's retry is a passive backoff, not an active push. Worth
closing separately if this proves too slow in practice; noted, not
deferred silently.

Regression: `animus-cp-data/tests/txn_recovery.rs`'s
`stage_over_a_foreign_pending_intent_no_ops_then_a_pushed_retry_succeeds`
(the apply-time rejection plus the proposer-side push-and-retry, end to
end) and `abort_restore_never_meets_another_transactions_intent`
(reconstructs the exact three-transaction sequence that used to corrupt
the chain and proves it can no longer arise); the corpus's own depth cell
(`coordinator_abandon_prepare_s01`, noted above) is the end-to-end
regression, green at `ANIMUS_TXN_SEEDS=10`.

### 2. `intent_spans` didn't cover what recovery needs — a real gap, closed structurally

Recovery's "for each span in the record's `intent_spans`, ask the owning
participant whether it's staged" (§3 below) assumes the anchor's record
already knows every participant. It didn't: PR3/PR4 shipped `intent_spans`
populated **only from the anchor's own writes** (`txn_stage_participant`
passed `spans: Vec::new()`, "no local record is ever created here" — sound
for PR3's single-participant case, silently insufficient once PR4 added
real participants). The record had zero visibility into which *other*
tablets/tables a transaction touched, and no table name to route by even if
it had spans for them.

Closed exactly like PR4 closed the analogous `record_table` gap:
`animusd::ClientCtx::cp_txn` already computes the full write set grouped by
`(table, tablet)` *before* staging anything, so it can hand the anchor's
stage the complete cross-participant list up front.
`TxnRecord::intent_spans`/`KvCommand::TxnStage.spans` changed from
`Vec<KeyRange>` to `Vec<(String, KeyRange)>` — every key any participant
ever staged, table name attached, the anchor's own writes included — and
`RaftKvNode::txn_stage_anchor` (a new method; `txn_stage` becomes a thin
single-participant wrapper calling it with an empty participant list) takes
the caller-supplied cross-participant spans and merges them with the
anchor's own. This is an internal wire/record-format change only (`codec.rs`
`VERSION` bumped 8 → 9); no back-compat concern per house convention.

### 2b. A gap the `intent_spans` review caught: orphan records and the resurrection guard

Review of §2's fix surfaced an adjacent corner it did not by itself close:
PR4's prepare phase stages every participant **concurrently**, so a
participant's own `TxnStage` can succeed and be discovered by a reader
while the *anchor's* own `TxnStage` — which is what would create the
transaction's record at all — never lands. This is not hypothetical: it is
PR4's own documented fence/seal-miss gap ("a participant's stage into an
already-sealed range is a true no-op... the coordinator can't distinguish
it from a genuine stage via the propose outcome alone"), now recognized to
apply symmetrically to the **anchor's** own stage — `wait_applied` only
confirms the entry *applied*, never that its whole-or-nothing content
check actually succeeded, so a coordinator can believe `txn_stage_anchor`
succeeded (and go on to stage participants for real) even though no record
was ever created.

Two consequences, both closed this pass:

1. **A pusher's `TxnStatus`/`TxnRecordView` query can find no record at
   all.** There is no `created_ts` to grace-gate against in that case.
   `IntentInfo` gained a `version: HlcTimestamp` field (the orphaned
   intent's own applied timestamp, unpacked from its engine version — the
   only trustworthy substitute clock a pusher still holds) and
   `ClientCtx::txn_recover` gained an `intent_ts_hint: Option<HlcTimestamp>`
   parameter threaded from it. Past grace on that substitute, the pusher
   **synthesizes** a fresh `TxnRecord` directly in the `Aborted` state (a
   CRDB-style "abort tombstone") via a new primitive,
   `RaftKvNode::txn_abort_orphan` (`KvCommand::TxnAbort` gained an
   `orphan_created_ts: Option<HlcTimestamp>` field — `Some` means
   "synthesize if absent" instead of the ordinary "missing record is a
   fence-miss no-op"). **An absent record can only ever decide abort,
   never commit** — committing requires positively verifying every
   participant staged, which requires a candidate participant list the
   record alone would have provided; with no record, there is nothing to
   verify against, so aborting is the only sound decision (mirroring §3's
   safety argument: a recovery abort is always a legitimate outcome, never
   data loss, since nothing had committed yet).
2. **A late-arriving genuine anchor `TxnStage` for that same `txn_id` is a
   resurrection hazard.** Without a guard, it would unconditionally
   overwrite the tombstone back to `Pending` (and re-stage the anchor's own
   intents, which nothing would ever now resolve, since the record's own
   `intent_spans` — fixed at whichever creation happened first — likely
   doesn't name them). `apply_and_compact`'s `TxnStage` arm now checks,
   before merging anything, whether a **decided** record for this exact
   `txn_id` already exists at `record_key` (only meaningful for
   `is_anchor: true` — a non-anchor participant's own tablet never holds
   the record to check against, which is fine: that side's stale intents
   are still resolved on demand the moment any reader hits them, §4,
   unaffected by whether the anchor's own tablet resurrected anything) —
   if so, the **whole entry no-ops** (logged, `tracing::warn!`), exactly
   the same "first decision wins" principle §1 already established for
   duelling deciders, now extended to record **creation** itself, not just
   flips of an existing one.

Regression: `animus-cp-data/src/lib.rs`'s in-crate
`pr5_orphan_and_resurrection_tests` module (not the external
`tests/txn_recovery.rs` — reproducing "a late `TxnStage` for an
**already-known** `txn_id`" needs `pub(crate)` access:
`txn::record_key`, a direct `KvCommand::TxnStage` construction, and the
private `propose_ordered_aux`/`mint_pushed` primitives, since the public
`RaftKvNode::txn_stage_anchor` always mints a *fresh* `TxnId` and so cannot
express "the identical transaction arrives late" at all). The one test
drives the full scenario: a participant stages against a hand-built
txn_id/record_key with no anchor record ever created; a pusher creates the
orphan-abort tombstone; the genuine (late) anchor `TxnStage` for that exact
identity no-ops against it (confirmed directly — the anchor's own key is
never written); a still-live coordinator's own commit attempt also no-ops
(via §1's existing mechanism) and the record stays `Aborted`; and resolving
everywhere leaves no zombie `Pending` intent anywhere. No assert fires at
any step.

### 3. The recovery protocol: "push"

Any actor holding a foreign-or-local `Pending` intent past
[`RECOVERY_GRACE`](../../crates/animus-cp-data/src/lib.rs) (5s of HLC wall
time — a pure liveness knob; grace only affects *when* a push may act,
never *what* it decides once it does, per §1's argument) may drive the
transaction to a decision. `animusd::ClientCtx::txn_recover(record_table,
record_key, txn_id)` is the pusher, callable both from a reader that just
hit a stale intent (§4) and from the resolver loop's own sweep (§5):

1. **Read the record** (`RaftKvNode::txn_record_view` — the recovery-view
   dual of `txn_status_local`, also returning `intent_spans`/`created_ts` —
   reached over the wire via a new internal-only `ClientRequest::
   TxnRecordView`). Already decided → resolve every participant and return
   the decision; nothing more to do.
2. **Grace check.** `Pending` and `now < created_ts.wall_ms + RECOVERY_GRACE`
   → decline (report `Pending`, propose nothing) — a still-live coordinator
   is given room to finish its own ordinary in-flight commit.
3. **Verify every participant.** `Pending` and stale: for each `(table,
   span)` in `intent_spans`, ask that table's tablet leader whether it
   still holds a live intent for `txn_id` over `span`
   (`RaftKvNode::txn_verify_staged`, a new primitive — a bounded scoped scan
   of the engine for the raw envelope, since `span` is always the exact
   single-key point-span `txn::immediate_successor` builds; reached over the
   wire via a new internal-only `ClientRequest::TxnVerify`). All staged →
   propose `TxnCommit` (`txn_commit_at_least`, floored at the record's own
   `created_ts`); any missing (or any verify query itself failing —
   conservative: "not confirmed staged" reads as "missing") → propose
   `TxnAbort` (`txn_abort`).
4. **Re-read and act on the actual outcome** (§1) — either proposal may
   lose to a concurrent decision.
5. **Resolve** every participant per the final, actual decision
   (`ClientCtx::recovery_resolve`, grouping `intent_spans` by table and
   issuing one `txn_resolve_participant` call per table — the exact same
   primitive the ordinary coordinator path uses).

**Safety argument.** A recovery *commit* requires every participant
independently verified staged — exactly the coordinator's own commit
precondition — so a recovery commit and a coordinator's own commit are
**the same decision**, arrived at independently; there is no scenario where
recovery commits something the coordinator would have aborted. A recovery
*abort* can race a still-live coordinator's late prepare/commit — the
coordinator's subsequent `TxnCommit` simply no-ops against the already-
`Aborted` record (§1) and the client correctly sees an abort, a legitimate
outcome (a genuinely slow coordinator loses no data — nothing had committed
yet). This is why grace is liveness-only: whether recovery even attempts
step 3 changes only *when* a decision might be pushed, never *what* it
decides.

### 4. Read-path push — lift, scoped to the foreign-intent path

The ADR's Decision section (§3) already promised "a read may wait or push"
for a `Pending` intent; PR3/PR4 shipped only the "wait" half (a bounded
retry, then not-served). PR5 lifts this for the **foreign-intent** read
path (`animusd::ClientCtx::cp_get_local_resolving`, the one that already has
a network round trip available to it): a still-`Pending` — or failed —
`TxnStatus` query now calls `txn_recover` before giving up, rather than
immediately reporting "retry." `txn_recover`'s own grace check means this
never disturbs an ordinary in-flight transaction; it only shortens the
window in which a stale one is visible as "pending, retry."

`txn_recover` is also where §2b's record-absent branch actually executes:
`cp_get_local_resolving` already carries the foreign intent's own applied
version (`IntentInfo::version`) from the `Foreign` read outcome, so it can
hand `txn_recover` an `intent_ts_hint` unconditionally — the ordinary
record-exists path ignores it entirely, and only the record-absent branch
consults it as the grace-check substitute for a `created_ts` that was never
written. The reader then resolves its **own** key directly against
whatever status comes back, never through the (possibly empty)
`intent_spans` of a freshly-synthesized tombstone — see §2b's safety
argument for why an orphan tombstone can't know about any participant.

**Deliberately not lifted for the locally-`Pending` case** (the
single-participant/anchor read path, `RaftKvNode`'s own bounded
`read_resolved` retry) — that retry lives inside `animus-cp-data`, with no
network layer to reach other participants even if it wanted to push
immediately; it still relies on the resolver loop (§5) to eventually push a
stale local record, which converges within `RECOVERY_GRACE` plus one
resolver tick regardless. **Scans keep the existing silent-omission
behavior** (unchanged from PR3/PR4) — full scan-push scheduling remains
future work, per the same scoping PR4's own amendment already established
for the point-read-only foreign-intent path.

### 5. The resolver background task

Per-group tracking (`animus-cp-data`'s `TxnTracker`, `lib.rs`): every group
that ever anchors a transaction maintains `pending: BTreeMap<TxnId,
(record_key, created_ts)>` (inserted when a `TxnStage` with `is_anchor:
true` first creates the record; removed the moment this group's own apply
flips `Pending -> Committed/Aborted` — a losing, later conflicting decision
touches neither map, since the winning one already did) and
`unresolved_decided: BTreeMap<TxnId, (record_key, TxnOutcome)>` (inserted on
that same transition; removed once *any* `TxnResolve` for that `txn_id`
applies on this group). The second map is a **documented, deliberately
approximate but still-safe** signal — a group can only observe resolves
that land on *itself*, so "removed" really means "the anchor's own local
resolve happened," not "every participant's intents were rewritten"; a
resolver that stops tracking slightly early never loses correctness (a
straggling remote intent is still resolved on demand the moment any reader
hits it, §4), only background promptness is marginally weaker in that
residual case. **Rebuilt at group start** (`rebuild_txn_tracker`) via one
bounded scope scan for `txn::is_record_key` markers — deliberately not
derived from log replay (compaction can truncate a `TxnStage`/`TxnCommit`
entry out of the log long before the record's own lifecycle is done, the
same reasoning `sealed`/`committed_ceiling` already document for their own
rebuild-from-engine-marker designs), the same accepted cost `has_data`/
`engine_image` already pay. Exposed via `RaftKvNode::pending_txns`/
`unresolved_decided` — cheap, lock-and-clone, no barrier.

A documented residual: since a `TxnRecord` is never pruned once decided (no
record/intent GC exists yet — accepted future work, per the ADR's own
Consequences section on MVCC GC), a restart's rebuild scan re-adds **every**
historical decided record it finds to `unresolved_decided`, not just
genuinely-still-unresolved ones — the resolver loop then harmlessly
re-attempts an already-resolved `TxnResolve` for these (idempotent, a
no-op) until its own tracking entry clears again. A real cost at scale, not
a correctness issue; record/intent GC is out of PR5's scope.

`animusd::txn_resolver_loop` (data-role-gated, spawned alongside the
tablet-host reconciler and `auto_split_loop` in both `BoundNode::start_with`
and `BoundDataNode::start_data_with`): every `TXN_RESOLVER_INTERVAL` (1s,
plain fixed interval — no jitter, matching every sibling loop's own shape),
for each tablet group this node currently **leads**
(`ctx.edge.hosted_groups()`, empty on a control-only node), pushes every
`pending_txns()` entry via `txn_recover` (declining harmlessly if still
within grace) and fans a resolve out for every `unresolved_decided()` entry
(re-reading the record's own `intent_spans` via `txn_record_view` first,
since the tracker only carries `(record_key, outcome)`).

### 6. `cp_txn`'s resolve becomes asynchronous — the PR4 deviation, lifted

PR4's own amendment flagged resolving every participant *before* acking the
client as a deliberate, temporary deviation from the ADR's "ack, then
resolve asynchronously" design, specifically because the infrastructure
that would make an un-awaited resolve *safe to abandon* didn't exist yet.
It now does: once the anchor's commit is durable (the atomic commit point,
unchanged), `cp_txn` returns immediately and spawns a background,
best-effort resolve of every participant — the anchor's own keys included,
now resolved via the identical `txn_resolve_participant` call as every
other participant, not a special inline step inside
`txn_decide_anchor` (which resolves nothing at all now — see §1). A crash
right after this spawn leaves nothing ambiguous: the commit is already
durable and visible (a foreign-intent read resolves it on the fly, §4;
`unresolved_decided` tracks it for the resolver loop, §5) — this is
strictly *safer* than the interim synchronous shape, not merely faster, since
a stuck/slow participant's resolve latency (or forwarding failure) no
longer holds the client's own response hostage.

The **abort** paths (a failed prepare, a failed precondition re-check, or a
commit attempt that itself lost to a recovery abort) still resolve
**synchronously** before returning — there is no successful ack to speed up
on an error return, so the extra safety margin there costs nothing.

### 7. What ships with PR5, what's still deferred

Landing: the decision-semantics fix (§1); the `intent_spans` structural fix
(§2) and the adjacent orphan-record/resurrection-guard fix it surfaced on
review (§2b); the full recovery push protocol + two new internal wire
requests, `TxnRecordView`/`TxnVerify` (§3); the foreign-intent read-path
push (§4); the per-group `TxnTracker` + rebuild-at-start +
`txn_resolver_loop` (§5); async post-ack resolve on the commit path (§6);
three new metrics (`CpTxnRecoveredCommitted`/`CpTxnRecoveredAborted`/
`CpTxnResolverRuns`).

Deferred: `/admin/txns` observability (PR7, unchanged from the PR4
amendment's own note); record/intent GC (accepted future cost, §5); push
scheduling for a scan and for a locally-`Pending` read (§4); the
multi-tablet Elle corpus (Follow-up step 5, PR6) — the safety net that lets
this and every prior step be trusted at depth under fault injection.

### 8. Tests

`animus-cp-data/tests/txn_recovery.rs` (`SimEnv`, deterministic): a recovery
push commits when every participant genuinely staged past grace (both keys
visible on every replica of both groups); a push aborts when a participant
never staged (every value restored); a recovery abort beating a late
coordinator commit with no assert (driving both proposals explicitly,
confirming the actual status is the abort); two duelling recoverers'
conflicting proposals converging on one identical status with no assert
(zero intervening sim time, mirroring `cross_group_lww.rs`'s in-flight-race
technique); a push declining before grace elapses; an orphan intent with no
record anywhere (§2b) — the anchor's own range sealed first so its stage
silently no-ops, leaving a real, minted `(txn_id, record_key)` with no
record ever written on the anchor and a genuine participant intent
referencing it — decided abort past grace via `txn_abort_orphan`, the
synthesized tombstone confirmed to carry empty `intent_spans` (proving
`push`'s own `recovery_resolve` pass over it is correctly a no-op), and the
triggering intent resolved away by the caller directly, restoring its
pre-transaction committed value; `pending_txns` surviving a genuine process
restart via the rebuild scan (a single-voter group, mirroring
`witnessing.rs`'s own restart idiom); and a five-seed reproducibility sweep
of the headline recovery-commit shape.

`animus-cp-data/src/lib.rs`'s in-crate `pr5_orphan_and_resurrection_tests`
module (§2b): the orphan-abort-then-late-anchor-stage-then-late-coordinator-
commit regression, requiring `pub(crate)` access (`txn::record_key`, a
direct `KvCommand::TxnStage` construction, `propose_ordered_aux`/
`mint_pushed`) an external integration test cannot reach, since the public
`txn_stage_anchor` always mints a fresh `TxnId` and so cannot express "the
identical, already-referenced transaction arrives late."

`animusd/tests/cp_txn.rs` (`ProdEnv`, real 3-process cluster + a genuine
pre-split table): a coordinator crash between prepare and decide — driven
by sending the internal `TxnPrepare` wire requests directly (mirroring
exactly what `cp_txn` does over the network) and then simply never sending
`TxnDecide`/`TxnResolve`, since `cp_txn` runs synchronously inside one
request handler with no separate long-lived coordinator process to
literally kill — converging to a committed read from an uninvolved node
within grace + resolver margin; and the dual, a commit already applied but
never resolved, converging via ordinary reads with no grace wait needed at
all (the record is already decided).
