# ADR 0018 — Cross-tablet transactions on the CP plane (2PC over per-tablet Raft + HLC + MVCC)

- **Status:** Accepted — in delivery (PR1: HLC + sim clock skew landed; PR2:
  HLC commit timestamps as the CP-plane MVCC version + the range-seal design
  landed; PR2b: MVCC snapshot reads at a timestamp + the read-timestamp
  cache/logged read ceiling landed; PR3-PR7 sequenced). See the "Amendment
  (2026-08-11, PR1)" section for the build-time decisions settled at the
  start of delivery, the "Amendment (2026-08-11, PR2)" section for the
  range-seal design that replaces `version_floor`, and the "Amendment
  (2026-08-11, PR2b)" section below for the read path + serializability
  write-push mechanism.
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
