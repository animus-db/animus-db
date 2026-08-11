# ADR 0018 — Cross-tablet transactions on the CP plane (2PC over per-tablet Raft + HLC + MVCC)

- **Status:** Accepted — in delivery (PR1: HLC + sim clock skew landed; PR2:
  HLC commit timestamps as the CP-plane MVCC version + the range-seal design
  landed; PR3-PR7 sequenced). See the "Amendment (2026-08-11, PR1)" section
  for the build-time decisions settled at the start of delivery, and the
  "Amendment (2026-08-11, PR2)" section below for the range-seal design that
  replaces `version_floor`.
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
  membership/split recovery (ADR 0017).

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
(`gather_facts`), keeping `plan` itself pure. The absorbed side proposes its
seal from inside `Reconciler::teardown`'s existing Absorb drain-wait (ADR
0033) — the same drain that already guarantees the absorbed group's committed
log is fully applied before its WAL is deleted now also guarantees the seal
itself is durably observable by the time the drain completes.

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
