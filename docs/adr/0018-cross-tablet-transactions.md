# ADR 0018 — Cross-tablet transactions on the CP plane (2PC over per-tablet Raft + HLC + MVCC)

- **Status:** Accepted — implemented (PR1-PR7 + corpus-found fixes + the
  apply-time write-key conditions follow-up + the 2026-08-24
  `ClientRequestToken` idempotency amendment + the 2026-08-27 amendment
  closing issue #298's "deep shape A" double-materialize mechanism + a
  second 2026-08-27 amendment confirming — and narrowing, not categorically
  closing — that round's own genuine-`TransactionConflict` residual (plus
  two sibling bugs found and fully closed the same way) — `SplitMode::
  InPlace` **still** stays pinned to `Copy`: the `TransactionConflict`
  residual recurred once more even with the fix active, and a NEW, more
  severe "acked write lost with no error anywhere" residual was also found,
  both during that round's own attempted 30-run gate (see that amendment's
  §4/§6). The root structural gap both trace back to — `KvCommand::
  TxnResolve` has no outcome channel telling its own proposer a fence-miss
  no-op from a genuine resolve — is named, not built (that amendment's §3).
  CQL transactional surface, CancellationReasons fidelity, and manual
  txn-resolution admin actions remain separately deferred. PR1: HLC + sim
  clock skew; PR2:
  HLC commit timestamps as the CP-plane MVCC version + the range-seal
  design; PR2b: MVCC snapshot reads at a timestamp + the read-timestamp
  cache/logged read ceiling; PR3: single-participant transactions — the
  value envelope + the txn record/intent/resolve machinery through one Raft
  group; PR4: multi-participant 2PC across tablet Raft groups, the
  wire-level coordinator, foreign-intent resolution, and
  uncertainty-interval read restarts; PR5: in-doubt transaction recovery +
  the per-node intent-resolver background task; PR6: the multi-tablet Elle
  serializability corpus + the protocol hardening fixes it found; PR7:
  atomic Dynamo `TransactWriteItems`, the new `TransactGetItems`, and the
  `/admin/txns` observability surface; the **follow-up (2026-08-12)**:
  apply-time write-key conditions — upgrading a `TransactWriteItems` write
  action's own `ConditionExpression` from PR7's same-node-only protection
  to full cross-node OCC, closing that PR's own documented deviation. See
  the "Amendment (2026-08-11, PR1)" section for the build-time decisions
  settled at the start of delivery, the "Amendment (2026-08-11, PR2)"
  section for the range-seal design that replaces `version_floor`, the
  "Amendment (2026-08-11, PR2b)" section for the read path +
  serializability write-push mechanism, the "Amendment (2026-08-12, PR3)"
  section for the record/intent/resolve machinery, the "Amendment
  (2026-08-12, PR4)" section for multi-participant 2PC, the "Amendment
  (2026-08-12, PR5)" section for in-doubt recovery + the resolver, the
  "Amendment (2026-08-12, PR7)" section for the Dynamo transactional
  surface + observability (PR6's corpus findings are cited there directly,
  since PR6 itself landed no separate ADR amendment), and the "Amendment
  (2026-08-12, follow-up)" section below for the apply-time conditions
  mechanism.
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
permanently stalled a split's child hosting. The seal proposal used to be
bundled as a side effect of the same tick that performed `NarrowScope`'s
local scope mutation (leader-gated propose inline) — one-shot: the local
mutation happens unconditionally regardless of leadership, and once it has
happened the condition that would re-trigger the proposal attempt is gone —
so if leadership isn't held by the replica processing that exact tick (a
leadership change mid-handoff; an independent per-node reconcile timer in a
genuine multi-process deployment, not a combined node's synchronized loops),
the seal is never proposed by anyone, ever.
Fixed: the seal proposal is now `plan`'s
`HostAction::ProposeSeal`, derived from a **persistent condition**
(`TabletFacts::pending_seals`) re-checked from scratch every tick — "does a
covering seal marker exist in my own engine yet" — independent of whether
this replica's local scope state has already changed, so whichever replica
eventually holds leadership gets its chance, however leadership shuffles
relative to the local mutation. See `crates/animus-cp-data/CLAUDE.md`'s Key
Invariants entry for the mechanism as it stands today. The original
diagnostic story also covered tablet merge's mirror-image half
(`Reconciler::teardown`'s Absorb drain gated on a **locally-observed
committed** seal, not merely "nothing pending locally") — merge and that
half of the fix were removed by [ADR 0044](0044-split-only-tablets.md); the
full original text is archived verbatim in
`docs/engineering-lessons-archive.md`, since the seal-ordering lesson itself
generalizes to split's still-live `NarrowScope` gating.)*

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
off a range (a split's `NarrowScope`), its leader
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

The seal's durable witness for a co-hosted **successor** (a split child) is
a **marker key written directly into the shared engine**,
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
covering the child's range (`Metadata::split_parents`, provenance that is
never pruned — tablet ids are never reused, so an entry can never resurrect
a wrong decision for some later tablet reusing the id). This fact is
gathered as a bounded, tablet-scoped engine scan (`gather_facts`), keeping
`plan` itself pure. (Tablet merge had a mirror-image gate here — a merge
survivor's `HostAction::WidenScope` deferred on the absorbed tablet's seal
via `Metadata::absorbed_by` — until [ADR 0044](0044-split-only-tablets.md)
removed merge and this half of the reconciler along with it;
`parent_seal_observed`/`split_parents` are what remains.)

*(As shipped, the seal proposal itself was a one-shot side effect bundled
into the same tick as the local `NarrowScope` mutation — see Corrective note
#2 above for the bug this produced and its fix: proposing the seal is now
`plan`'s own `HostAction::ProposeSeal`, a persistent condition re-derived
every tick, decoupled entirely from the local scope mutation timing.)*

**Liveness, not correctness, is what a stalled source jeopardizes**: a
split successor waiting for a source-group leader to seal stalls if that
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
`engine_image` snapshots, and moves with a split exactly like the anchor's
own data would.

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

**Corrective note (2026-08-12, task #18): the paragraph above describes what
the *primitive* (`RaftKvNode::txn_stage_anchor`) correctly supports — it does
not describe what `ClientCtx::cp_txn` actually did.** As shipped by this PR
(and unchanged through every subsequent PR up to and including PR7), `cp_txn`
never called `txn_stage_anchor` with a real cross-participant list at all —
its anchor-stage call site went through `RaftKvNode::txn_stage` (equivalently,
`txn_stage_anchor` with an **empty** participant list), so every production
multi-participant transaction's `intent_spans` named only the anchor's own
keys, never any other participant's. `animus-cp-data`'s own tests
(`tests/txn_recovery.rs`, and `animus-test/tests/txn_serializable.rs`) never
caught this because they call `txn_stage_anchor` directly with a hand-built
participant list — they exercise the primitive, not `cp_txn`'s wiring of it.

**Why this was a real, exploitable atomicity violation, not merely an
observability gap**: §3's recovery decides `all_staged` by verifying only the
spans the record actually lists (`ClientCtx::txn_recover`'s `for (table, span)
in &view.intent_spans` loop). A transaction whose coordinator staged the
anchor and then died **before ever attempting a participant's own stage**
was, to recovery, indistinguishable from a single-participant transaction
that staged completely — `all_staged` came back `true` trivially (the
too-short list's one entry genuinely had staged), so recovery **committed**
a transaction whose participant write never happened anywhere. One half of
an intended atomic write became durably visible while the other half
silently never landed, and nothing ever revisited the decision. Confirmed
live: a wire-level test driving the exact `TxnPrepare` bytes the unfixed
`cp_txn` sent in this scenario reliably reproduced a wrongly-`Committed`
record and a visible anchor-key value within ~7s (well under
`RECOVERY_GRACE` + margin) — the two existing PR5 coordinator-crash
regressions (`animusd/tests/cp_txn.rs`) never exercised this failure mode
because both always staged *every* participant genuinely before letting
recovery run, so the verification loop's incompleteness — checking a list
that was silently too short — was never actually reached against a case
where it would have mattered.

**The fix**: `cp_txn` now computes the participant `(table, span)` list from
the same `groups` map it already builds for staging (everything left in
`groups` after the anchor's own entry is removed *is* every other
participant) and threads it through `txn_prepare`/`txn_prepare_pushing` into
the anchor's stage call — locally via `CpGroup::txn_stage` (now calling
`txn_stage_anchor` directly instead of the single-participant `txn_stage`
convenience), or over the wire via a new
`ClientRequest::TxnPrepare::participant_spans` field (`#[serde(default)]`,
internal-only, no back-compat concern). Regression:
`animusd/tests/txn_recovery_participant_spans.rs`'s
`anchor_only_stage_with_a_declared_but_unstaged_participant_recovers_to_abort`
— the anchor stages with the (now correctly populated) participant span
included, the participant is deliberately never staged, and recovery is
confirmed to decide `Aborted`, never letting the anchor's key become
visible.

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

## Amendment (2026-08-12, PR7)

PR7 lands the last item of the delivery scope PR1's amendment (decision 4)
named: atomic Dynamo `TransactWriteItems`, the new `TransactGetItems`, and
the `/admin/txns` observability surface — rewiring `animusd::dynamo`'s
transactional edge onto `ClientCtx::cp_txn`/`cp_read` instead of the
serial, documented-non-atomic loop it shipped with, and flipping this ADR's
own Status line to **implemented**.

### 1. `TransactWriteItems` becomes atomic

`dynamo.rs::run_transact` no longer applies each action in list order with
no rollback (the old, honestly-documented gap). It now: resolves every
action's key, evaluates its `ConditionExpression` (if any) via one
ordinary linearizable pre-read, computes each `Put`/`Delete`/`Update`
action's write payload, and stages the whole batch as one
[`ClientCtx::cp_txn`](../../crates/animusd/src/lib.rs) call — whole-or-
nothing across however many tablets/tables the actions span. A condition
that evaluates false rejects the request **before `cp_txn` is ever
called** (nothing staged, nothing to unwind); a `cp_txn` abort (a lost 2PC
race, or a `ConditionCheck` precondition that changed between prepare and
commit) is reported the same way. Every key may be touched by at most one
action (validated up front, matching DynamoDB's own rule) — `cp_txn`'s
`writes` has no per-key conflict concept of its own. The GSI/LSI edge
index (`note_put`/`note_delete`) is updated **after** the atomic commit
lands, never per-action, so an aborted transaction never leaks a write
into a secondary index.

**Failure shape**: any condition failure or `cp_txn` abort is now a
`TransactionCanceledException` — the real DynamoDB exception type for a
transaction — not the bare `ConditionalCheckFailedException` a single-item
conditional write returns (the old code used the latter even inside a
transaction; this was a minor pre-existing inaccuracy this rewrite
corrects as a side effect of getting the exception type right, not a
new regression). **Simple form only**: one message, not AWS's per-action
`CancellationReasons` array — deferred, per PR1's amendment decision 4.

**A real design flaw found and fixed while building this, not merely
anticipated**: an early version fed *every* condition-gated action's
observed value into `cp_txn`'s precondition mechanism, including a
`Put`/`Delete`/`Update`'s own key. `cp_txn`'s precondition check re-reads
a key once before staging and again right before the commit decision —
but a **write** action's own key, by the time that re-read runs, already
holds *this same transaction's* own freshly-staged, still-`Pending`
intent. The re-read cannot resolve until the transaction itself decides,
which hasn't happened yet (the decide step is later in `cp_txn`'s own
control flow), so it blocks in `ClientCtx::cp_read`'s retry loop —
**not indefinitely**, but until the *background* `txn_resolver_loop`
(a separate task) eventually pushes the now-stale-past-`RECOVERY_GRACE`
record to a decision via in-doubt recovery, several seconds later, at
which point the read finally returns the freshly-committed value — which
of course differs from what the pre-stage check observed, so `cp_txn`
reports a spurious "value changed" cancellation. This was caught by an
existing regression, `animusd/tests/dynamo_schema.rs`'s
`extended_surface`, which went from passing in under a second to failing
several seconds later with exactly this misleading error.

**The fix, and the resulting asymmetry**: only a `ConditionCheck`'s
observed value becomes a `cp_txn` precondition. A `ConditionCheck`'s key
is, by construction, never one this same transaction writes (the
duplicate-item-per-transaction rule guarantees this), so its re-read
observes an ordinary committed value throughout — exactly the cross-key
read-modify-write guard `cp_txn`'s own precondition design already exists
for (see the PR4 amendment §6's own note on this). A **write** action's
own `ConditionExpression` is protected only by the one-time pre-read this
function performs, itself serialized against this node's other RMWs by
`ctx.data().rmw_lock` held across the whole call — the identical
guarantee a single-item conditional `PutItem`/`DeleteItem`/`UpdateItem`
always had, no stronger. **This is a known, documented limitation, not
silently accepted**: two `TransactWriteItems` requests racing a
write-action's own condition on the same key through *different* nodes
have no OCC protection today (same-node races are still correctly
serialized, `animusd/tests/dynamo_txn.rs`'s
`concurrent_transact_write_items_on_a_shared_key_resolve_one_winner`).
Closing it fully would need a new primitive — a `cp_txn` precondition
variant that can distinguish "unchanged, but now covered by this same
transaction's own in-flight intent" from "changed by someone else,"
which does not exist yet. Flagged as follow-up, not solved here.

### 2. `TransactGetItems`: a consistent multi-key read

New — no non-atomic prior implementation to replace. `dynamo.rs::
run_transact_get` reads every requested key **concurrently**
(`futures::future::join_all`, via the ordinary [`ClientCtx::cp_read`]
machinery — linearizable, intent-resolving, works from any node), and
only accepts the round once **two consecutive concurrent rounds agree
byte-for-byte on every key** (`quiescent_multi_get`). Bounded to
[`TRANSACT_GET_MAX_ROUNDS`] (4) rounds; a snapshot that never quiesces
(sustained contention on one of the requested keys) reports a retryable
`TransactionCanceledException` rather than ever returning a possibly-torn
result.

**Semantics, honestly**: this is a **serializable snapshot via
quiescence-confirmation, retry-on-contention** — not a wait-free
snapshot, and not externally consistent (consistent with this ADR's own
Decision section §2, which already rules out Spanner-style external
consistency). If nothing changed between two independent observations, no
transaction was in flight touching any involved key during that whole
window, so the read is genuinely consistent — not merely probably so, but
also not obtained without possible retries under contention.

**Why this exact design, not a simpler one — the PR6 corpus's own
findings, cited directly since PR6 landed no separate ADR amendment of its
own.** The multi-tablet Elle serializability corpus
(`animus-test/tests/txn_serializable.rs`, a sibling PR) needed three
designs for its own analogous "read several keys as of one moment"
problem before it stopped producing false-positive torn reads, and the
final one is what this PR reuses:

1. **A single coordinator-minted `read_at` snapshot timestamp** —
   rejected as **structurally unsound**, not merely awkward:
   `RaftKvNode::mint_pushed`'s write-conflict floor stamps a subsequent
   write *above* whatever ceiling a prior future-padded read already
   pushed that group's committed ceiling to, and since a group's `Hlc`
   only ever ratchets forward, that becomes a **permanent** floor no
   fixed or dynamically-sampled margin can close (the "ceiling ratchet"
   problem). Found by the corpus's `participant_leader_kill_early` and
   independently by its plain `baseline` scenario.
2. **Force-resolve once, then read sequentially** — undermined by a slow
   key observing a much later moment than a fast one (real sim/wall time
   elapses between reads in the same "snapshot").
3. **A single concurrent round** (this PR's own design, minus the second
   confirming round) — narrows the window to one round trip but doesn't
   eliminate it; group-to-group `ReadIndex` latency still varies. Found
   by the corpus's `compound_abandon_prepare_and_partition` scenario.

Two-round agreement (design 3 plus the confirming round) is what actually
closes it, for the reason stated above. `dynamo.rs`'s `quiescent_multi_get`
is a direct port of the corpus's `quiescent_multi_read`, substituting
`ClientCtx::cp_read` (which already gives cross-process forwarding and
on-the-fly intent resolution) for the corpus's own in-test
`linearizable_get_served` calls.

### 3. `/admin/txns` (ADR 0020 discipline)

`GET /admin/txns` — a **pure observer**, no gated action — mirrors
`/admin/raftkv`'s own shape exactly: one entry per hosted tablet
(`CpGroup::txn_view`, `crates/animusd/src/lib.rs`), node-local (a
cluster-wide picture is a client-side fan-out over every node's own
`/admin/txns`, same as `/admin/raftkv`). Each hosted group's entry lists:

- **`pending`** — this group's `TxnTracker::pending` (record_key,
  created_ts, age vs. `RECOVERY_GRACE`, and — the one field costing a real
  ReadIndex round trip rather than the cheap lock-and-clone the rest of
  this view is — a best-effort `intent_spans` summary per participant, via
  `RaftKvNode::txn_record_view`).
- **`unresolved_decided`** — this group's `TxnTracker::unresolved_decided`
  (record_key, the decided `TxnOutcome`).

**No manual-resolution POST action** — deferred. The existing
`txn_resolver_loop`/`ClientCtx::txn_recover` machinery (ADR 0018 §2/PR5)
already drives every record listed here to a decision, and every
participant to a resolve, with no operator action required; this endpoint
is observability only. No dashboard tab either (ADR 0021/0035 scope
discipline) — a future PR's concern if operators want one.

### 4. Metrics

Four new, append-only `Metric` variants (`animus-env/src/metrics.rs`):
`DynamoTransactWritesCommitted`/`DynamoTransactWritesCanceled`
(`run_transact`'s two terminal outcomes, including the all-`ConditionCheck`
fallback path) and `DynamoTransactGetsOk`/`DynamoTransactGetsRetried`
(`quiescent_multi_get`'s outcome — `Retried` covers both "needed more than
the first confirming round to quiesce" and "never quiesced, reported
retryable" cases; one counter for both, since both are the identical
contention signal and PR7's scope capped this delivery at four new
variants total).

### 5. What ships with PR7, what's still deferred

Landing: atomic `TransactWriteItems` via `cp_txn`; the new
`TransactGetItems` (quiescence-confirmed multi-key read); `/admin/txns`;
four metrics; `animusd/tests/dynamo_txn.rs` (cross-tablet atomic
visibility through a follower-connected client; a failing `ConditionCheck`
cancelling a whole transaction that would have partially applied under
the old serial loop; `TransactGetItems` never observing a torn pair under
a concurrent writer; same-node concurrent transactions racing a shared
conditioned key resolving to exactly one winner with the loser's own
other key never landing; `/admin/txns` showing a pending record during a
simulated coordinator stall and clearing once recovery decides it).

Deferred (per PR1's amendment decision 4, unchanged): CQL LWT/atomic
`BATCH`; Dynamo `ClientRequestToken` idempotency (**closed by the
2026-08-24 amendment below**); full per-action `CancellationReasons`
fidelity. Newly identified and deferred by this PR: cross-node OCC for a
write action's own `ConditionExpression` (§1's documented asymmetry
above); `/admin/txns` manual resolution actions (§3). Record/intent GC
remains out of scope (ADR 0018 §2/PR5's own note).

No new `ClientRequest`/wire variant was needed — `cp_txn`/`cp_read`
already existed and already route/forward correctly (proven generically
by `cp_txn.rs`'s own follower-connected regression); this PR is entirely
an `animusd::dynamo` + `animus-dynamo::wire` + `animus-env::metrics` +
`animusd::admin`/`CpGroup` change.

## Amendment (2026-08-12, follow-up)

This follow-up closes the one item the PR7 amendment §1 identified and
deferred: a `TransactWriteItems` write action's own `ConditionExpression`
had only same-node protection (`ctx.data().rmw_lock`), not the cross-node
OCC every other condition-gated path in this system gets. It does not
change any wire-facing behavior — the failure shape, exception type, and
`ConditionCheck` semantics are unchanged — only *which mechanism* protects
a write's own condition, and *how strong* that protection is.

### 1. The primitive: byte-level OCC checked at apply, not a re-read

`animus_cp_data::KvCommand::TxnStage` gains a `conditions: Vec<(Vec<u8>,
Option<Vec<u8>>)>` field — `(key, expected)` pairs, `expected: Some(bytes)`
meaning the key's current *committed* value must equal `bytes` exactly,
`None` meaning it must be absent. This is deliberately **not** a rich
expression evaluator: `animus-cp-data` already speaks exactly this
byte-level shape for `Cas`, and the layering rule this amendment settles is
the same one that shape already implies — the data plane speaks bytes; a
richer caller (the Dynamo edge) evaluates its own `ConditionExpression`
against a pre-read and compiles a true evaluation down to "the value must
still be exactly what I just read," the same OCC primitive `Cas` already
gives a single-key conditional write.

**Evaluated at *apply*, inside `TxnStage`'s own arm, against the key's
pre-intent committed value** — envelope-unwrapped, the identical read
discipline `Cas`'s own apply arm already uses (`Envelope::Committed(v)` →
`Some(v)`; absent → `None`; a *foreign* intent → never a match, since "the
current committed value" is ambiguous while one is live, mirroring `Cas`'s
own "an intent always fails the swap" rule). **Same-txn re-staging (a
WAL-replay re-application) is handled the same way `TxnStage`'s existing
writer-push-intents guard already handles it**: if the key already holds
*this exact transaction's own* intent (the entry being replayed already
applied it once), the condition is trusted rather than re-evaluated
against an envelope that no longer holds "the value before this stage" at
all — replay-safe by the same reasoning the pre-existing `blocked_by` check
already relies on.

**Priority against the pre-existing whole-or-nothing gates, since a
condition failure is a *new* reason a stage can no-op alongside the
existing ones**: an already-decided record or a fence/seal miss is checked
first (this replica structurally cannot serve the stage at all, regardless
of any condition); a foreign unresolved intent on a target key is checked
next (the current committed value is ambiguous, so evaluating a condition
at all would be unsound, not just redundant); only once both of those pass
are this stage's own `conditions` evaluated. **Any** condition failing
no-ops the *whole* stage, composing with the pre-existing whole-or-nothing
behavior (a multi-key stage with one failing condition stages **none** of
its keys, not just the conditioned one).

### 2. `StageOutcome`: distinguishing *why*, not just *whether*

Every `TxnStage` now records a `StageOutcome` at apply time, keyed by Raft
log index exactly like `Cas`'s own `CasResults` (`RaftKvNode::
stage_outcome`) — `Staged` (landed), `ConditionFailed { key }` (a **final**
cancellation — the condition was checked against the current committed
value, so retrying the identical stage changes nothing), `IntentBlocked {
key, txn_id }` (the pre-existing foreign-intent no-op, ADR 0018 §2/PR6,
now named instead of only inferred after the fact via a separate
`txn_verify` read), and `Fenced` (a structural rejection — a fence/seal
miss, or a late anchor stage racing an already-decided recovery outcome).
`txn_stage_anchor`/`txn_stage_participant` return it directly (`Option<(..,
StageOutcome)>` instead of the bare `Option<(..)>` that used to mean only
"the entry applied"); `animusd`'s `ClientRequest::TxnPrepare`/
`ClientResponse::TxnPrepared` carry `conditions`/`outcome` across the wire
the same way (internal-only variants, no back-compat concern, per house
convention).

**This also simplifies `animusd::ClientCtx::txn_prepare_pushing`**: PR6's
own fix for the foreign-intent case had no way to learn *why* a stage
no-op'd from the propose layer alone, so it re-read every staged key via a
separate `txn_verify` round trip after the fact just to infer "was I
blocked." Since the apply arm now reports the reason directly,
`txn_prepare_pushing` branches on the returned `StageOutcome` instead:
`Staged` succeeds, `IntentBlocked` retries (bounded, backed off — unchanged
behavior), `ConditionFailed`/`Fenced` fail immediately, never retried. This
removes the extra round trip entirely — a genuine simplification, not just
a rename.

**A corpus-found correctness gap in the new introspection itself, not the
condition mechanism**: an early version of `txn_stage_anchor`/
`txn_stage_participant` paired `wait_applied(index)` (which only confirms
`engine_applied_index() >= index`) with a hard `stage_outcome(index)
.expect(..)` — reasoning that "applied" and "outcome recorded" were the
same fact, true for every other command here. They are not, for
`TxnStage` specifically: a replica catching up via a **snapshot install**
(after losing leadership, `apply_and_compact`'s `install_engine_image`
branch) can advance `engine_applied` straight past `index` without ever
individually processing — hence recording an outcome for — the entry at
that exact index (the image globs many commands' effects together,
discarding any per-entry outcome for anything it already covers).
`ANIMUS_TXN_SEEDS=5` over the multi-tablet corpus (`animus-test/tests/
txn_serializable.rs`) hit this deterministically as a hard panic, not a
hang or a wrong answer. Fixed by replacing the two-step wait-then-fetch
with a single polling loop over `stage_outcome` directly (a new
`wait_stage_outcome`, mirroring `compare_and_swap`'s own outcome-polling
loop — which never had this bug, since it was never split into two steps
in the first place): `None` on timeout, exactly like every other
propose-and-wait method's pre-existing "give up, caller retries" contract,
never a hard-`expect`ed fact that turns out not to be guaranteed. See
`docs/engineering-lessons.md` for the general lesson this generalizes to.

### 3. The coordinator: `cp_txn` gains `write_conditions`

`animusd::ClientCtx::cp_txn` gains a third parameter, `write_conditions:
Vec<(String, Vec<u8>, Option<Vec<u8>>)>` — `(table, key, expected)`, where
`key` MUST be one of `writes`' own keys (validated; an `Err` otherwise).
This is a **structurally distinct** mechanism from the pre-existing
`preconditions` parameter, not an overload of it: `preconditions` is
`cp_txn`'s own cross-key re-read-based OCC (checked once before staging,
once more before the commit decision) — sound only for a key this
transaction does *not* write, which is exactly why feeding a write's own
condition through it was the PR7-documented stall bug (the re-read would
retry against this same transaction's own in-flight intent, blocking until
the *background* resolver forced a decision past `RECOVERY_GRACE`).
`write_conditions` instead threads straight down to the owning tablet's own
`TxnStage` `conditions` field — grouped by `(table, tablet)` alongside
`writes` itself, no re-read, no self-reference to stall against.

### 4. `dynamo.rs::run_transact`: two mechanisms, matched to two cases

A `Put`/`Delete`/`Update` action's own `ConditionExpression` is still
evaluated exactly as before (one linearizable pre-read, the existing
expression evaluator) — what changes is where the *result* goes: instead
of being dropped (protected only by `ctx.data().rmw_lock` serializing this
node's own conditional writes), a true evaluation's observed bytes become a
`write_conditions` entry. A `ConditionCheck` action's observed value is
still routed to `preconditions`, unchanged — its key is never one this
transaction writes (the pre-existing duplicate-item-per-transaction rule
guarantees this), so the cross-key mechanism was always sound for it and
still is. `ctx.data().rmw_lock` remains held across the whole call, but is
no longer what makes a write's own condition correct across nodes — it
now only serializes this node's own conditional writes against each other
for throughput/ordering, the identical role it plays for a plain
single-item `PutItem`/`DeleteItem`/`UpdateItem`.

**Semantics, precisely**: this is full OCC on a write's own conditioned
key — a concurrent committed change to that key between the pre-read and
the stage's apply cancels the transaction with `TransactionCanceledException`
(correct DynamoDB behavior: the condition is evaluated against the item at
transaction time), and a spurious cancellation on an ABA-identical value is
impossible, since the check is byte-for-byte equality against the exact
value read, not a version counter.

### 5. What this closes, what stays deferred

Closes the PR7 amendment §1's own documented gap in full: "two
`TransactWriteItems` requests racing a write-action's own condition on the
same key through *different* nodes" now resolve to exactly one winner,
proven by `animusd/tests/dynamo_txn.rs`'s
`cross_node_racing_own_key_conditional_writes_resolve_exactly_one_winner`
(issued through two different nodes' Dynamo listeners, unlike the
pre-existing same-node
`concurrent_transact_write_items_on_a_shared_key_resolve_one_winner`,
which stays green unchanged); `own_key_condition_failure_cancels_a_multi_
tablet_transaction_wholly` proves an own-key condition failure on one
tablet still cancels the whole cross-tablet transaction, no partial state
on the other tablet; `own_key_condition_completes_quickly_with_no_
recovery_grace_stall` proves the PR7 stall-bug's exact shape (a condition
on a write's own key) now completes in well under `RECOVERY_GRACE`, not
merely "doesn't hang forever" — the regression that motivated PR7's own
fix stays dead by construction (the new mechanism has no re-read to stall
on at all, not just a faster one).

Unchanged, still deferred: CQL LWT/atomic `BATCH`; Dynamo
`ClientRequestToken` idempotency (**closed by the 2026-08-24 amendment
below**); full per-action `CancellationReasons` fidelity (a condition
failure is still reported as one message, not AWS's per-action array);
`/admin/txns` manual resolution actions. Record/intent GC remains out of
scope (ADR 0018 §2/PR5's own note).

Codec: `animus-cp-data`'s wire/image `VERSION` bumped 10 → 11
(`TxnStage.conditions`) — internal format only, no cross-version
compatibility required (no live deployments, house convention).

## Amendment (2026-08-15, `mint_pushed` clock-witnessing-runaway fix)

This amendment fixes a live bug in PR2b's write-conflict push (§2/§3 above):
`RaftKvNode::mint_pushed` had its own, independent route to the exact
`Hlc::witness`-poisoning hazard the PR2b amendment's own §3 already named and
fixed for `ensure_ceiling_above`/`next_ceiling_candidate`. It does not change
any wire-facing behavior or the codec version — the fix is entirely internal
to how a write's timestamp is computed.

### 1. The bug

`mint_pushed` folded the **live** `committed_ceiling()` into `ts_cache`'s
`low_water` on **every** mint (`cache.raise_low_water(self.committed_ceiling())`,
previously unconditional), then, whenever the honest mint fell at or below
the resulting floor (the ceiling is deliberately `Hlc::uncertainty_upper`
— `HLC_MAX_OFFSET` (500ms) — ahead of real time, so this was the *common*
case, not an edge one), called `self.hlc.witness(floor, ..)` to push past
it. Witnessing a value `HLC_MAX_OFFSET` in the future permanently ratchets
the group's shared `Hlc` into that fiction — exactly the hazard §3's own
`next_ceiling_candidate` doc already described and built a separate CAS
ratchet to avoid. `mint_pushed` was PR2b's *other* caller of the identical
unsafe pattern, never covered by that fix.

The result is a self-sustaining feedback loop under interleaved reads and
writes: a write's `mint_pushed` witnesses the ceiling forward → the next
read mints near that poisoned clock and almost immediately exceeds the
(now merely 500ms old) ceiling → `ensure_ceiling_above` proposes a fresh
`ReadCeiling` another `HLC_MAX_OFFSET` further out → the next write folds
*that* in as its floor and witnesses it too. Each round adds roughly one
`HLC_MAX_OFFSET`, **independent of how much real (virtual) time actually
elapses** — probe-verified as a k×`HLC_MAX_OFFSET` runaway lattice, and
reproduced deterministically by `tests/ts_cache.rs`'s
`interleaved_reads_and_writes_never_let_minted_timestamps_outrun_real_time`
(pre-fix: committed ceiling reached 102s of manufactured time after only
20s of real elapsed simulated time). The manufactured `ReadCeiling` churn
this produces on the propose path also starves genuine log entries behind
it.

### 2. The fix, part A: per-term ceiling absorption

The unconditional per-mint fold is strictly stronger than the safety
property needs. **The committed ceiling's write-floor role only exists to
cover a *predecessor* leader's reads** — reads served by *this* leader are
already covered by its own `ts_cache` entries, bumped at their real serve
`ts` by every `linearizable_get`/`_scan`/`read_at`/`scan_at` call. A
predecessor's ceiling, meanwhile, is fixed as of this leader's own
takeover: Raft leader completeness means the new leader already witnessed
the prior ceiling's `ts` via ordinary `AppendEntries` receipt before it
could ever campaign, and a deposed leader cannot commit a fresher ceiling
after losing leadership (its own `ensure_ceiling_above`/`propose_ordered`
calls require it). So absorbing `committed_ceiling()` into `ts_cache`'s
`low_water` **once per term** — the first time this leader mints in a
given term — is exactly as safe as absorbing it on every mint, while
breaking the feedback loop: every mint after the first in a term is
floored only by the per-key `ts_cache`/`last_proposed_ts`, both of which
reflect *real* served/proposed timestamps, never a manufactured
future-shifted one.

**New invariant, replacing the unconditional-fold reading of §2 above**: a
write's floor covers (a) every read *this* leader has itself served
(`ts_cache`'s per-span entries, bumped at real serve `ts`), and (b) every
read any *predecessor* could have served, via the ceiling absorbed once at
this leader's first mint of its current term. `RaftKvNode::mint_pushed`
tracks the absorption via a new field, `last_absorbed_term` (an
`AtomicU64`, sentinel `u64::MAX` so the very first mint on a fresh group
still absorbs), compared against the Raft `term` — which `mint_pushed`
cannot read via `term()` itself (it always runs inside `propose_ordered`/
`propose_ordered_aux`'s already-held `core` lock, so a second `lock()`
call would deadlock); `propose_ordered`/`propose_ordered_aux` now read
`core.term()` once and hand it down to their `build` closure for exactly
this reason.

### 3. The fix, part B: no-witness push

Independent of part A (defense in depth: even a floor that is occasionally
still ceiling-derived must not poison the clock), `mint_pushed`'s
witness-and-retry branch is replaced with pure arithmetic: a new
`hlc::bump_strictly_above(ts)` computes the next value that strictly
exceeds `ts` (bump `logical` by one, carrying into `wall_ms` on
`LOGICAL_BITS` overflow) with **no** `Hlc::witness` call and no mutation of
`Hlc`'s own persistent state — the identical bump rule
`next_ceiling_candidate`'s own CAS-ratchet branch already used inline,
factored out into this shared, unit-tested function so both call sites
stay byte-for-byte identical by construction rather than by discipline.
Monotonicity across a leader's own successive proposes still holds without
witnessing: the floor `mint_pushed` bumps above already includes
`last_proposed_ts` (this leader's own last-*logged* ts, from
`propose_ordered`'s existing floor-tracking — unchanged by this
amendment), so each pushed write's ts strictly exceeds every ts this
leader has minted or proposed so far, the same property `Hlc::witness`
would have provided, without its side effect on `self.hlc`.

### 4. What this closes, what stays true

`next_ceiling_candidate`'s own doc already named the "never witness a
future-shifted value" rule; this amendment is the fix for the second,
independent path to the same `Hlc::witness` sink that rule never covered.
Regression: `tests/ts_cache.rs`'s
`interleaved_reads_and_writes_never_let_minted_timestamps_outrun_real_time`
(new — proven to fail against the pre-fix code, per above) and the
pre-existing `leader_change_never_lets_a_write_undercut_a_served_read_even_
under_extreme_clock_skew` (unchanged, still green — the safety argument in
§3 above that this amendment's new invariant rests on). See
`docs/engineering-lessons.md` for the general "a fix must cover every path
to a dangerous primitive's sink, not just the caller that surfaced it"
lesson this incident is an instance of.

No codec change (no `KvCommand`/wire-image field touched) and no change to
`ensure_ceiling_above`/`next_ceiling_candidate` beyond factoring out the
shared bump helper.

## Amendment (2026-08-15, quiescent-round uniform-single-shot reads — PR2 of
the torn-pair-fix stack)

`TransactGetItems`'s quiescent-round reader (`animusd::dynamo::
quiescent_multi_get`, §2's own "Amendment 2026-08-12, PR7" above) had a
second, independent bug in the same family as this file's `mint_pushed`
amendment just above: both are cases where a mechanism proven correct in
isolation broke once combined with an asymmetry in how its inputs actually
behave.

### 1. The bug

`quiescent_multi_get` reads every key of a round via `ClientCtx::cp_read`,
which resolves an unresolved intent via `ClientCtx::cp_get_local_resolving`
— and that function is **deliberately asymmetric** by design, correctly so
for its real job (serving a plain `GetItem`, a genuinely single-key read):

- `FastRead::Pending` (a **local** intent — the record lives in this same
  tablet, the anchor case) falls through to the **bounded blocking chase**
  (`RaftKvNode::linearizable_get_served`, up to `INTENT_WAIT_TIMEOUT` = 5s):
  correct for a lone reader, since waiting out a contended intent is the
  right behavior when there is nothing else to compare the read against.
- `FastRead::Foreign` (a **cross-tablet** intent — the participant case)
  gets a single status query + push attempt and, if still undecided,
  reports a retryable `"; retry"` error immediately — `cp_read`'s own
  *outer* loop (a 50ms poll) is the retrier, not this call.

For a `TransactGetItems` round, this asymmetry is fatal: the round's own
correctness argument ("accept once two consecutive rounds agree
byte-for-byte") implicitly assumes every key in a round samples
*approximately the same instant*. Under a tight, back-to-back writer
alternating two keys of the *same* repeatedly-re-run transaction, the local
key's blocking chase and the foreign key's immediate-give-up-then-50ms-retry
have systematically different latencies, so the two keys of one round
sample two different, unrelated instants — and because the skew is
*systematic* (not random per round), two consecutive rounds can agree
byte-for-byte on the exact same torn pair, satisfying the quiescence check
while reporting a snapshot that was never true at any single instant.
Reproduced directly by `animusd/tests/dynamo_txn.rs::
transact_get_items_never_observes_a_torn_pair_under_concurrent_writes`
(two keys, sum-to-zero writes, `a + b == 0` asserted on every accepted
round).

### 2. The fix: every key of a round is a uniform, non-blocking, single-shot sample

The invariant a quiescent round actually needs: **one round samples one
instant for every key** — not "every key eventually converges," which is
what `cp_get_local_resolving` gives a lone `GetItem` and exactly what broke
here. `quiescent_multi_get` now calls a new primitive,
`ClientCtx::cp_read_snapshot` (→ `cp_get_local_snapshot`), instead of
`cp_read`:

- **`FastRead::Pending` and `FastRead::Foreign` now carry the identical
  `IntentInfo` payload** (`animus_cp_data::ResolveStep`/`FastRead`, both
  variants) — the local-anchor case used to carry nothing (there was no
  cross-tablet resolver to hand it to), but the *record* itself is always
  addressable by `(record_table, record_key)` regardless of whether it
  happens to live in this same tablet or a different one:
  `ClientCtx::txn_status`'s existing `cp_route`-based routing resolves back
  to a local, in-process call transparently when it does.
- Both variants now route through one shared function, `confirm_or_push`: a
  single status query, and — only if still `Pending` — a single
  `txn_recover` push attempt (same `RECOVERY_GRACE` liveness gate as
  before: a young, still-live transaction is never disturbed). **Never a
  second query, never a sleep-and-retry** inside this function.
- A still-undecided outcome (or a resolve landing on a race — something
  else resolved the key between the status query and the resolve call)
  maps to a new `SnapshotRead::Unresolved` outcome, wire-distinguishable
  via a new `ClientResponse::Unresolved` (the internal-only
  `ClientRequest::GetSnapshot`/`cp_serve_forwarded` forwarding path mirrors
  `KindWrite`/`KindScan`'s existing refused-bare convention) — **never**
  `cp_get_local_resolving`'s retryable `"; retry"` `Error`, since the two
  callers' outer loops act on the two outcomes differently.
- `quiescent_multi_get`'s round loop: if *any* key of a round comes back
  `Unresolved`, the whole round is discarded — `previous` resets to `None`,
  never compared — since a round with one unresolved key proves nothing
  about whether the *other* keys' values are stale (this is exactly the
  case the old design let slip through as a false-positive "quiesced"
  accept).

`cp_get_local_resolving` itself, and hence plain `GetItem`/`cp_read`, is
**unchanged** — the local-`Pending` blocking chase stays exactly as PR3
built it, since it remains the correct behavior for a genuinely single-key
read. Only `TransactGetItems`'s own round primitive moved to the uniform
shape.

### 3. Composition with the `mint_pushed` fix (this file's amendment just above)

The two fixes are independent but compounding: the `mint_pushed` fix closes
a *clock*-side hazard (a leader's own minted timestamps runaway-diverging
from real time under interleaved reads/writes on one group); this amendment
closes a *read-shape*-side hazard (one multi-key round sampling
different instants across groups). Either alone leaves a path to a torn
`TransactGetItems` snapshot under sustained concurrent writes to the same
key pair; both together close every mechanism this stack's investigation
found.

### 4. A third, pre-existing, unrelated bug found during acceptance testing — explicitly out of scope here

Solo re-runs of `transact_get_items_never_observes_a_torn_pair_under_
concurrent_writes` against this PR (10 runs on the `mint_pushed`-only
baseline, 20 runs with this amendment applied) show the test still failing
at a similar, high rate (baseline 7/10 failed; with this fix, 17/20 failed)
— **not** because either torn-pair mechanism above still fires (a dedicated
new `SimEnv` regression proves the fixed round design itself sound at
depth — see §5 below and `animus-test/tests/txn_serializable.rs::
tight_pair_transactions_never_observe_a_torn_snapshot`, 0 failures across
30+ seeds) — but because of a **third, distinct, already-documented
pre-existing bug**: `docs/engineering-lessons.md`'s 2026-08-14 entry
("A pre-existing, timing-sensitive flake found incidentally...") already
adjudicated this exact test as genuinely flaky on `main`, unrelated to the
PR it was found alongside, with its own baseline numbers (4/10, 4/10, 5/10
failures across three points in history). Debugging this PR's own solo
runs traced the *mechanism*, not just the symptom: the participant key
("b") stops receiving any further writes partway through the writer's
loop (observed stuck anywhere from step 4 to step 14 out of 15) while the
anchor key ("a") continues committing correctly all the way to the end,
and the writer's own `TransactWriteItems` calls never observe a failure —
i.e., this is a **write-side 2PC participant-write-loss** bug (the
participant's own intent silently stops advancing while the coordinator
keeps reporting success), not a read-side snapshot-timing one, so it sits
entirely outside what either torn-pair-fix PR targets. Per this repo's
"separate PRs for incidental bugs" convention (and the engineering-lessons
entry's own explicit instruction), this is left for its own, dedicated
root-cause delivery — see `docs/engineering-lessons.md`'s updated entry for
the refreshed baseline numbers and the mechanism-level lead this
investigation leaves behind.

### 5. Tests

New: the SimEnv regression proving the *fixed* uniform-single-shot design
sound at the protocol level, `animus-test/tests/txn_serializable.rs::
tight_pair_transactions_never_observe_a_torn_snapshot` (a dedicated
scenario, not part of the `Scenario`/`run_scenario` Elle-history corpus
above — the property under test is a numeric sum invariant across one
contended key pair, not a `Mop::Append`/`Read` serializability claim).
Existing acceptance test unchanged: `animusd/tests/dynamo_txn.rs::
transact_get_items_never_observes_a_torn_pair_under_concurrent_writes` —
still the real wire-level check, now also the regression for §4's residual
bug once that gets its own fix.

## Amendment (2026-08-15, write-loss fix — PR3 of the torn-pair-fix stack)

This amendment closes the §4 residual bug the PR2 amendment above left
explicitly out of scope: a **participant-write-loss** bug on a split
table's cross-tablet transaction, root-caused as a coordinator-side
routing defect with no apply-time guard against it, not an HLC/clock issue
(the PR1/PR2 fixes above remain independently correct and unaffected).

### 1. The bug

`ClientCtx::recovery_resolve` (the resolve half of both `txn_recover`'s
in-doubt push and `txn_resolver_loop`'s periodic sweep — see §3/§5 of the
PR5 amendment) grouped a transaction's `intent_spans` by **table name
alone**, with no tablet dimension, before issuing one `txn_resolve_
participant` call per group. A table with more than one tablet (any split
table) can have two participants' keys share one table name but live on
two different Raft groups; grouping by table name alone bundles both into
one call, and that call's own `cp_route(table, &keys[0])` resolves a
single leader from the *first* key alone — so the rest of the bundle rides
along to whichever tablet the first key happens to belong to, not
necessarily its own.

Before this fix, `KvCommand::TxnResolve` carried no `fence` at all — every
*other* key-writing variant (`Put`/`Batch`/`Delete`/`Cas`/`TxnStage`)
already had one (ADR 0028), but `TxnResolve` was reasoned, incorrectly, to
need none: "every key here was already fence-checked at `TxnStage` time."
That reasoning silently assumed `keys` could only ever be a set the
*applying* tablet itself had staged — true for every in-crate caller, but
not something the type enforced, and the coordinator-side grouping bug is
exactly the counterexample. With no fence, the misrouted tablet applied
the resolve for a key it doesn't own, directly onto the *same physical
key* the owning tablet separately maintains (ADR 0028: a table's tablets
share one `StorageScope` prefix on a shared engine — only the logical
`KeyRange` differs) — stamped with the wrong tablet's own clock. The
owning tablet's own clock never learns of that foreign version and can
never mint above it again: every future write to that key silently loses
the per-key LWW race in `StorageEngine::merge` (whose `Result<bool>`
"took effect" outcome was discarded via `.expect(..)` at every apply arm
that merges a value), while the coordinator's own client-facing ack is
computed independently of whether the merge that's supposed to back it up
actually landed — an acked write silently and permanently lost, with the
transaction's own record correctly `Committed` throughout. `txn_resolver_
loop`'s 1s `unresolved_decided` sweep (not grace-gated, unlike the
in-doubt `txn_recover` path) races the coordinator's own `resolve_all` on
essentially every transaction against a split table, so this fired on
most fast transactions, not just a rare crash-recovery window.

### 2. The fix: correct grouping at the source, a fence as the structural seatbelt

Two independent, complementary changes:

- **`ClientCtx::recovery_resolve` now groups by `(table, tablet)`**, not
  table alone: each key's *own current* tablet is re-resolved right before
  grouping (`tablet_for`, the same primitive `cp_route` itself uses),
  mirroring the same discipline `cp_txn`'s own stage-time key grouping
  already used (that path was never affected by this bug, and the audit
  below explains why). A key whose tablet can't be resolved *right now* (a
  genuinely transient routing gap) is skipped, not fatal to the rest of
  this best-effort resolve — unchanged from before. Every other
  `recovery_resolve`/`txn_resolve_participant` caller in the crate
  (`cp_txn`'s own `resolve_all`, `txn_recover`, `txn_resolver_loop`) was
  audited and found to already route correctly: `resolve_all` builds its
  own `(table, tablet)`-keyed `staged` map directly from the *same*
  per-participant stage calls it just issued, never regrouping through
  `intent_spans` at all, so it was never exposed to this defect.
- **`KvCommand::TxnResolve` gains a `fence: KeyRange`**, stamped at
  propose time from the routed group's own live `scope_range()` — byte-
  for-byte the same discipline `TxnStage`'s own `fence` already uses — and
  enforced at apply exactly like `TxnStage`'s: **whole-or-nothing**, every
  key in `keys` must fall inside `fence` (and not a since-sealed range) or
  the entire entry is a no-op, never a partial resolve. This is the
  structural seatbelt: even if a caller (present or, more importantly,
  future) makes the identical grouping mistake again, the wrong tablet
  now safely **rejects** the resolve instead of silently corrupting the
  other tablet's physical key. Fail-before/pass-after (`animus-cp-data/
  tests/fenced_commands.rs::txn_resolve_misrouted_to_the_wrong_tablet_is_
  rejected_by_its_own_fence`): with the `!all_in_fence` gate temporarily
  removed, this same test reproduces the corruption directly — group B's
  physical intent for its own key gets silently rewritten `Committed`
  under group A's clock by a resolve proposed on group A's own log, purely
  from A having been handed a foreign key with no fence to catch it.
  Restoring the gate makes the resolve a clean no-op instead, leaving the
  intent physically untouched; a subsequent correctly-routed resolve from
  B's own group still resolves it normally. Codec version bumped 13 → 14
  for the new field (pre-alpha, no cross-version wire/disk compatibility
  required).

An earlier design considered **witnessing** `TxnResolve`'s carried
`commit_ts` into a non-anchor participant's clock at apply — folding the
anchor-minted commit timestamp into the participant's own `Hlc` so its
future mints would provably exceed it, the same "witness a just-observed
timestamp" pattern the crate's four existing witnessing points already
use (see this file's Key invariants entry in `animus-cp-data/CLAUDE.md`).
It was **abandoned**: even gated to a genuine non-anchor participant (never
the anchor re-witnessing its own already-folded value), it reignited a
clock-witnessing runaway under sustained cross-group transaction + read
load — confirmed super-linear in round count by a dedicated regression,
`animus-cp-data/tests/ts_cache.rs::cross_group_txn_traffic_never_lets_
either_groups_clock_run_away` (kept as a permanent guard against
reintroducing this design: green at ~10.6s of ceiling drift over a 30s run
without the witness, vs. 37.5s/82s at 80/160 rounds with it — the
runaway's growth tracked round count, not real elapsed time, exactly PR1's
own outlawed pattern relocated to a new call site). The real fix needed no
new witnessing chain at all — closing the routing bug at the source, with
the fence as a structural backstop, is sufficient and does not touch
`Hlc` in any new way.

### 3. Kept from the investigation, unrelated to the fix above

- **`mint_at_least`'s witness → `bump_strictly_above` swap**: a *second*,
  independent, previously-unfixed route to the exact `Hlc::witness`
  poisoning sink the PR1 amendment (`mint_pushed`) closed — same pattern,
  same fix, found opportunistically while investigating this bug (see
  `hlc.rs`'s `bump_strictly_above` doc in `animus-cp-data/CLAUDE.md`).
  Worth keeping regardless of this amendment's own fix.
- **The merge-took-no-effect seatbelt** (`surface_suspicious_merge_noop`,
  `Metric::CpMergeTookNoEffect`/`CpMergeTookNoEffectUnexplained`): surfaces
  a `storage.merge`/`merge_tombstone` call returning `Ok(false)` at the
  three apply-arm sites that used to discard it via `.expect(..)`
  (`TxnStage`'s intent write, `TxnResolve`'s commit/abort-restore writes,
  `Cas`'s swap) — metric + a capped `tracing::warn!` only, **deliberately
  not a hard assert (not even `debug_assert!`)**: an earlier draft's assert
  fired on legitimate, already-tested scenarios (an application-level
  retry landing an identical entry a second time within one process
  lifetime) the replay-vs-fresh distinguisher doesn't yet account for. This
  is exactly the signal that would have caught this bug directly (a
  provable, live "the write I just decided upon didn't actually land")
  had it existed before this investigation; kept as a permanent, if
  currently soft, guard against the next bug shaped like it.

### 4. What this closes, what stays true

Closes the acked-participant-write-loss mechanism at its root
(coordinator-side misrouting) and structurally (the fence), for every
current and future `TxnResolve` caller. Does not change wire-facing
behavior beyond the codec version bump; does not touch `Hlc`/witnessing at
all. The full three-bug torn-pair-fix stack (PR1: `mint_pushed` clock
runaway; PR2: quiescent-round read-shape; PR3: this write-loss fix) is
required together for `transact_get_items_never_observes_a_torn_pair_
under_concurrent_writes` to pass reliably solo — see `docs/engineering-
lessons.md` for the acceptance numbers at each layer and the general
lessons this incident leaves behind.

### 5. Tests

New: `animus-cp-data/tests/fenced_commands.rs::txn_resolve_misrouted_to_
the_wrong_tablet_is_rejected_by_its_own_fence` (the fence, proven
fail-before/pass-after per §2 above) and `animusd/tests/txn_recovery_
participant_spans.rs::recovery_resolve_correctly_commits_both_tablets_of_
a_two_tablet_transaction` (a genuine two-tablet transaction resolved only
by the automatic recovery sweep, asserted via a table `Scan` rather than a
point `Get` — a point read resolves a still-`Pending` intent on demand at
read time regardless of whether the physical resolve landed, which would
mask this exact bug; a `Scan` does not). The cross-group clock-runaway
regression from §2 above (`cross_group_txn_traffic_never_lets_either_
groups_clock_run_away`) stays green as a permanent guard against
reintroducing the abandoned witnessing design. Existing acceptance test
unchanged: `animusd/tests/dynamo_txn.rs::transact_get_items_never_
observes_a_torn_pair_under_concurrent_writes` — the real wire-level check
this whole three-PR stack exists to make pass reliably.

## Amendment (2026-08-16, `TxnStage` kind-writes — lifts the indexed/streamed rejection)

The PR7 amendment above (§1) recorded, as a known-and-documented gap, that
a write action against an indexed or streamed table made the whole
transaction reject outright: `KvCommand::TxnStage` only ever staged the
base row, so committing an LSI/GSI/stream-bearing write inside a
transaction would have left that table's derived state permanently stale
with no signal. `docs/adr/0046-tablet-log-model.md` named the underlying
shape (a tablet's log deterministically materializes everything colocated
with it) and settled the mechanism question this amendment implements;
read that ADR first — this amendment states only the transaction-specific
consequences.

### 1. A1 — materialize-at-resolve, not intent-staging

The obvious-looking fix — stage LSI rows and the change-log record as
intents in their own kind scopes, the same way a base row is staged today
— is **rejected** (ADR 0046 Decision 2), for two reasons that generalize
past this specific feature:

- Every consumer of a kind scope (the GSI drain, the Streams sealer, the
  backfill seeder) scans **forward from an HLC watermark** and is defined
  to **skip an intent outright** — only a base-scope reader ever resolves
  one. A change record staged as an intent at `ts=10` and resolved at
  `ts=40`, after a consumer's watermark has already passed 10, is silently
  skipped forever: a permanently lost GSI update or stream event, with no
  error.
- It breaks the invariant every non-base reader relies on: a kind scope
  only ever holds committed values (`RaftKvNode::local_get_kind`'s own
  doc). Staging one there would require giving every one of those readers
  a resolution path they were never built to need.

The adopted mechanism instead: `TxnWrite` (the `TxnStage.writes` element,
now a named struct, not a bare tuple) carries an optional derived
`kind_writes`/`change_log` payload alongside its own `key`/`value`. The
payload rides **inside the base write's own intent envelope**, opaque
until `KvCommand::TxnResolve`'s commit branch materializes it — via a
single shared `materialize_derived` helper `KvCommand::KindBatch`'s own
apply arm also calls, never a second copy (ADR 0046's binding decision) —
at the resolve entry's own locally-minted `ts`. Abort discards the payload
entirely: nothing is ever written to a kind scope for an aborted
transaction. Kind scopes keep holding only committed values, unchanged for
every existing reader.

Apply-time validation requires every `kind_writes` key to lead with its
own write's base key's own partition token (ADR 0022) — a validated
rejection folded into the same structural `Fenced`/whole-or-nothing gate a
fence/seal miss already uses, never an `assert!` (this payload is
wire-reachable via `ClientRequest::TxnPrepare`). `TxnResolve`'s own
whole-or-nothing fence check is extended to cover every `kind_writes` key
it is about to materialize, not just the base keys in `keys` — the #213
lesson ("every key-writing command must carry and enforce the apply-time
fence") applies directly, since these are new key-writing surface.

### 2. B1 — the change record's key position, and an amendment's own scope cut

The change record's **key position** comes from the resolve entry's own
locally-minted `ts` (monotone in this tablet's own log, so no consumer can
ever skip it) — **never** the transaction's true `commit_ts` (minted on
the anchor's, possibly different, group; stamping a foreign clock's
version into this group's own keyspace is exactly the acked-write-loss
mechanism the PR3-of-the-torn-pair-stack amendment above closed).

The plan carrying this amendment also proposed an **informational**
`commit_ts` field inside the change record itself, purely so
`ApproximateCreationDateTime` reports the transaction's real commit
instant rather than the (usually very close, but not identical) resolve
instant. **This sub-piece is not implemented, and is a deliberate scope
cut, not an oversight**: `eval_kind_txn_write` (the U3 evaluator, §4
below) builds the change record's bytes at **stage** time, strictly before
the anchor's `commit_ts` exists — there is no correctness-preserving place
left to patch the real value in afterward without either (a) making
`materialize_derived` stop treating change-log bytes as opaque (violating
ADR 0043's own layering rule and the "one shared helper" binding decision
in the same stroke), or (b) growing a second, `commit_ts`-aware
materialization path that would immediately start drifting from the first
the moment either is touched alone. The **load-bearing** half of B1 — key
position is resolve-derived, never commit-derived — is implemented and
tested (`animus-cp-data/tests/txn_kind_writes.rs`,
`animusd/tests/dynamo_streams.rs`); the informational-timestamp
refinement is left as a named follow-up if real operational need for
sub-second `ApproximateCreationDateTime` precision on transactional writes
ever materializes.

### 3. C1 — the mandatory own-key condition

Every kind-payload-bearing write gets a mandatory own-key OCC condition
(`TxnStage.conditions`, which already existed for exactly this shape) —
`(key, raw_old)`, the exact bytes the U3 evaluator's own read observed.
This is deliberately **redundant with, not a substitute for**, holding
`ctx.data().rmw_lock` across the evaluate-then-stage span (§4): the lock
already closes the race for every write reachable through it; the
condition is the belt-and-suspenders seatbelt for the one thing the lock
can't cover — a `txn_resolver_loop` recovery push resolving a *different*
transaction's intent on the same key, which never takes `rmw_lock`. ADR
0046 predicted exactly this framing ("if U3 is later chosen the condition
becomes a redundant-but-harmless seatbelt") before this amendment shipped.

### 4. Fork U decided as U3 — evaluate at the participant's own leader

A write action's kind payload is evaluated **at the tablet's own current
leader**, at stage time — never precomputed by the coordinator from a
possibly-stale read, and never derived at apply time (ADR 0046 Decision
1 rejects both U1-as-standing-design and U2). `dynamo::
eval_kind_txn_write` mirrors `kind_write_item_at_leader`'s existing
non-transactional U3 shape exactly: read the old image, evaluate the
caller's `ConditionExpression`, compute the new value, defer to
`kind_writes_for_item` for the LSI/change-log diff — all under the same
`ctx.data().rmw_lock` the ordinary write path already serializes on,
closing the identical cross-node race for the transactional path that
lock closes for the plain one.

### 5. D1 — awaited, bounded, parallel resolve for kind-write-path transactions

`cp_txn`'s pre-existing "ack, then asynchronously resolve" shape (§6 of
the PR5 amendment above) is unaffected for a plain transaction. **Scope
re-worded by ADR 0049 (2026-08-16), which made the kind path universal and
this clause's original predicate constant-true**: the awaited branch is
keyed on a pending write against a table whose change records **carry
images** (`table_change_records_carry_images` — an index or a stream: the
consumers whose visibility the bound protects), never on the kind path
itself; a marker-only transaction keeps the fire-and-forget sequential
spawn (re-universalizing the awaited-parallel shape reproduced exactly the
torn-pair instability the paragraph below records — see ADR 0049's
as-built amendment). For a
transaction staging such an images-table write, the ack instead
awaits `resolve_all` under a short fixed budget
(`TXN_RESOLVE_ALL_AWAIT_BUDGET`, 2s) before returning — a timeout still
acks (delayed, never denied; `txn_resolver_loop` remains the safety net
for whatever the bound didn't cover) — because the LSI row and the
GSI/stream change record only exist from resolve onward (materialize-at-
resolve, A1); an unconditional async-ack window would leave a committed
write readable on the base table but transiently absent from its own
index/stream. The awaited path also parallelizes across participants
(`resolve_all_parallel`, `futures::future::join_all`), so the fixed budget
buys one round trip's worth of latency regardless of participant count,
not `O(participants)`.

**A regression found and reverted while building this**: parallelizing
`resolve_all` **universally** (replacing the plain-transaction fire-and-
forget spawn's own sequential loop too) measurably destabilized
`animusd/tests/dynamo_txn.rs::transact_get_items_never_observes_a_torn_
pair_under_concurrent_writes` under sustained concurrent load — a
pre-existing, already timing-sensitive regression test. The fix keeps two
siblings: `resolve_all` stays sequential (the plain-transaction path,
proven stable), `resolve_all_parallel` is new and used only by the
awaited-bounded kind-write-path branch, which is the only branch D1
actually needs it for.

### 6. A genuine self-deadlock found and fixed, unrelated to the mechanism above

`dynamo.rs::run_transact` used to hold `ctx.data().rmw_lock` across its
entire span, including the `cp_txn` call at the end. Once a kind-write-path
action's evaluation (`eval_kind_txn_write`, inside `ClientCtx::
txn_stage_local`) also takes this same node-local lock — reachable
in-process the instant a write targets a locally-led kind-write-path
table, i.e. on every combined-role/single-node deployment — the outer hold
became a real, immediate self-deadlock on a non-reentrant
`tokio::sync::Mutex`. Fixed by scoping the guard to end **before** the
`cp_txn` call (it only ever needed to cover the pre-read/evaluate span for
plain-table condition checks, never the transaction's own staging).

### 7. Tests

New: `animus-cp-data/tests/txn_kind_writes.rs` (commit/abort/double-
resolve/crash-recovery/split-fence/byte-identical-helper scenarios, plus a
sabotage-then-restore teeth-proof on the extended fence coverage) and one
kind-bearing participant added to `tests/txn_multi.rs`. Replaced (not
merely extended): `animusd/tests/dynamo_index_writes.rs`'s and
`tests/dynamo_streams.rs`'s wholesale-rejection tests, now positive
coverage (cross-tablet LSI+GSI transaction across a real split, abort
leaves no index row/stream event). `crates/animus-test/tests/
txn_serializable.rs`'s corpus gained a `kind_consistency` check (every
committed transaction's `KIND_LSI`-mirrored row converges to exactly the
same value as its own base row, on both compared replicas) and `tests/
stream_lineage_corpus.rs` gained a transactional-write cell under a
leader-kill fault injection — see `docs/engineering-lessons.md` for the
test-harness lesson the corpus extension's own development surfaced (a
RMW-shaped write sharing a write-only cell's own keyspace needs the
identical kind payload, or the corpus's own workload mix — not the
mechanism — produces exactly the "silently stale derived row" symptom the
check exists to catch).

## Amendment (2026-08-24, `ClientRequestToken` idempotency)

This amendment closes the one item of PR1's amendment decision 4 that
survived every later PR7/follow-up sweep untouched: Dynamo
`TransactWriteItems`'s `ClientRequestToken` idempotency. `TransactGetItems`
needs no equivalent — a read has nothing to deduplicate — and full
per-action `CancellationReasons` fidelity remains deferred; that is a
separate follow-up (issue #374's C2), not part of this change.

### 1. Composed primitives, not a new mechanism

The entire feature is a **preflight** wrapped around `run_transact`'s
existing atomic-commit machinery, built from primitives that already
existed for other reasons — no new Raft command, no new `ClientRequest`
variant, no change to `cp_txn`/`KvCommand::TxnStage` at all:

- **Storage**: an ordinary catalog table (ADR 0013), written and read
  through the same `ClientCtx::cp_kind_write_item`/`raw_quorum_read`
  primitives every other Dynamo write/read uses.
- **Conditional claim**: `ConditionExpression::AttributeNotExists`, the
  identical mechanism a client's own `attribute_not_exists(pk)` conditional
  `PutItem` uses — "first committer wins" for a token is exactly "first
  committer wins" for an item.
- **Expiry**: the table's TTL attribute (ADR 0051) and its existing reaper
  — zero reaper code touched.
- **Fingerprint**: `serde_json::to_vec` of the decoded `Vec<TransactAction>`
  (`BTreeMap`-backed throughout, ADR 0003, hence deterministic regardless of
  the client's own JSON key order) hashed with `sha2`, the identical crypto
  dependency ADR 0057's SigV4 already added to `animus-dynamo`.

Composing existing primitives instead of inventing a bespoke idempotency
mechanism is a deliberate choice, not an accident of convenience: every
property the feature needs (durability, replication, conditional
first-writer-wins, expiry, leader-routing/forwarding, crash recovery) is a
property those primitives were already independently proven to have, by
their own existing test suites. A hand-rolled mechanism would have had to
re-earn every one of those properties from scratch.

### 2. A schema-registered internal table, not the `$`-prefixed hidden-table convention

The idempotency records live in `__animus_txn_idempotency`
(`animus_dynamo::internal_tables::TXN_IDEMPOTENCY_TABLE`): an **ordinary**,
schema-registered catalog table — real `CreateTableSchema`/`SetTableTtl`
entries, a real tablet, real replication — made invisible to clients only
by a name check at every entry point (`ListTables`, the Data Console's
table-summary/detail projections, and every client-facing data/DDL wire
operation), not by any structural exemption from the schema catalog.

**This is deliberately not** the `$`-prefixed hidden-table convention a
materialized GSI/LSI already uses (`animus_dynamo::index::
index_table_name`, `<base>$<index>`), even though that convention looks
like the obvious fit for "an internal table a client should never see."
Tracing why that convention cannot serve this feature is itself
instructive, and is why this section exists:

- `Metadata::apply`'s `CreateTableSchema` arm **rejects** any table name
  containing `$` outright (the guard that stops a hidden index table from
  ever colliding with a user table's own name) — a `$`-named table
  therefore never gets a `Metadata.schemas` entry of its own at all.
- The ADR 0051 TTL reaper's own per-tick sweep requires **both** a
  `table_ttl` entry **and** a `table_schema` entry for a table before it
  will ever scan it (`ttl_reaper.rs`'s leader-gated per-table loop) — and
  `SetTableTtl`'s own apply arm has the identical rejection `CreateTableSchema`
  does for a `$`-named table, so a hidden-table-shaped idempotency table
  could never be TTL-enabled in the first place, let alone reaped.

So the `$`-name-plus-TTL-reaper combination the naive design would reach
for is not merely awkward — it is structurally impossible given the two
existing guards, neither of which this amendment touches or should touch
(the `$` guard is exactly what keeps a hidden index table's identity
collision-free; loosening it to admit this one table would be a far more
invasive change than picking a different name). `__animus_txn_idempotency`
was chosen specifically because it clears both guards unmodified: it
contains no `$`, and `animus_control::syskv::is_reserved_name` only tests
a different, longer prefix (`__animus_system`), so the two names cannot
collide either. The consequence worth stating plainly: this table needed
its own **client-visibility** guard (§3 below) precisely because it is
*not* exempted from the schema catalog the way a hidden index table is —
being an ordinary table is what buys the free TTL reaping, and the price of
that is that every client-facing surface must be taught to hide it, rather
than the schema catalog hiding it structurally.

### 3. Visibility guards

A reserved-name predicate (`animus_dynamo::internal_tables::
is_internal_table_name`, the one place any current or future internal
table name is ever tested — kept as a single predicate so a second internal
table joins the same check instead of every call site growing its own)
gates every place a `TableName` reaches this table:

- `ListTables` filters it out of the catalog projection.
- The Data Console's `console_table_summaries`/`console_table_detail`
  exclude it, mirroring the existing belt-and-suspenders exclusion those
  two functions already apply to a hidden GSI/LSI table.
- `animusd::dynamo::run_operation` rejects it for every single-table
  operation up front (`Operation::table()`, covering `GetItem`/`PutItem`/
  `DeleteItem`/`Query`/`Scan`/`UpdateItem`/`CreateTable`/`UpdateTable`/
  `DeleteTable`/`DescribeTable`/`UpdateTimeToLive`/`DescribeTimeToLive`),
  and `BatchWriteItem`/`BatchGetItem`/`TransactWriteItems`/
  `TransactGetItems` — which span multiple tables, so `Operation::table()`
  cannot cover them — are each guarded at their own per-table entry point.

A data or read operation naming the table gets `ResourceNotFoundException`
— indistinguishable, from any client's point of view, from a table that
was never created. A `CreateTable`/`UpdateTable`/`DeleteTable`/
`UpdateTimeToLive` naming it gets `ValidationException` instead: the name
genuinely *is* reserved, a stronger and more honest signal than "does not
exist" for an operation that is trying to establish or change that exact
identity.

### 4. The `PENDING` outcome: a deliberate conservative narrowing

A retried token whose stored record is still `PENDING` gets a retryable
`TransactionInProgressException`, not a wait, and not a re-run.

Real DynamoDB's own documented contract is looser: a same-fingerprint
retry racing its own still-in-flight original request is tolerated, and
eventually serves whatever outcome the original settles on. This adapter
does not implement that looser contract, on purpose. Closing the gap fully
would mean either blocking the retry until the original's `cp_txn` call
resolves (turning an idempotency preflight into a second consensus wait
with no natural timeout of its own) or building a way to observe a
*specific* transaction's own in-flight resolution from outside it — a
primitive that does not exist today and would duplicate machinery
`txn_resolver_loop`/`ClientCtx::txn_recover` (ADR 0018 §2/PR5) already
owns for a different purpose. Neither is worth building for a narrow,
retryable window: `TransactionInProgressException` is itself a
documented, expected AWS exception a client's own SDK retry policy already
handles, so the observable behavior is "retry once more," not a hard
failure — the same shape a client already needs to tolerate for the
ordinary transient conditions DynamoDB itself can return.

### 5. The best-effort outcome update, and why a lost one self-heals

After `run_transact` reaches a terminal outcome — commit or cancel,
including both `ConditionCheck`-triggered cancellation sites — a
best-effort `Put` updates the token's record from `PENDING` to
`COMMITTED`/`CANCELLED`, conditioned on the stored `fingerprint` still
matching (so a race against the TTL reaper reclaiming this exact record,
followed by an unrelated request reusing the identical token value, can
never overwrite a foreign record with our own outcome). Every failure of
this update — a lost race, a routing hiccup, a crash — is silently
ignored: no retry, no error surfaced to the client whose transaction just
committed or was cancelled.

This is sound specifically **because** the conditional claim `Put` at the
start of the preflight, not the outcome update at the end, is what
guarantees the transaction itself executes at most once per token. The
outcome update is pure cache-freshness bookkeeping layered on top of an
already-safe protocol, not part of the safety argument:

- A committed/cancelled transaction whose outcome update is lost simply
  leaves its record `PENDING` until the TTL reaper reclaims it
  (`TXN_IDEMPOTENCY_TTL_SECS`, ten minutes).
- A retry that lands **before** the TTL reclaims it observes stale
  `PENDING` and gets an over-conservative `TransactionInProgressException`
  (§4) — annoying, retryable, but never wrong.
- A retry that lands **after** reclaim finds no record at all, retries the
  claim once (the record-missing branch already needed for the ordinary
  claim-then-reap race), and — since the original transaction is genuinely
  done — either claims a fresh record and re-runs the *already-decided*
  actions (safe only because every write action in the original committed
  set is DynamoDB-conditioned the same way it always was: a
  `PutItem`/`Update` with no condition is idempotent by construction, and a
  conditioned one simply re-evaluates against current state) or observes
  `TransactionInProgressException` again.

The property that must hold, and does: a lost outcome update can only ever
make a *future* retry more conservative (an unnecessary
`TransactionInProgressException`, or — in the reclaimed-and-reused-token
tail case — a fresh re-run of actions that were already safely
DynamoDB-conditioned to begin with), never less — it can never turn into a
silent double-commit of a transaction that already durably committed once,
because that guarantee never depended on the outcome update landing.

### 6. The `rmw_lock` placement constraint

`run_transact` already documents (§1 of the PR7 amendment, and this
function's own doc) that `ctx.data().rmw_lock` — the node-local,
non-reentrant lock serializing this node's conditional writes — must never
be held across a `cp_kind_write_item` call, because that call can re-enter
the same lock on a combined-role node hosting the target tablet's own
leader. Every idempotency-table read/write this amendment adds is subject
to the identical constraint, and is placed accordingly:

- The whole preflight (claim `Put`, any cached-record read) runs entirely
  **before** `rmw_lock` is acquired.
- Every outcome update runs entirely **after** it is dropped — including
  the one in-loop `ConditionCheck` failure site, which used to `return`
  directly from inside the lock's scope. That early return is now deferred
  (captured in a local `condition_check_failure` variable, the loop broken
  out of, the lock dropped, *then* the outcome update runs and the error is
  returned) specifically so this amendment's own outcome-update call can
  never execute while the lock is held.

This is the identical hazard `docs/engineering-lessons.md` already records
against this exact function, from the earlier, unrelated fix that first
established the "scope `rmw_lock` to end before `cp_txn`" rule ("Holding a
node-local lock across a call that can recurse back into the same lock, on
the same node, is a self-deadlock waiting for the one deployment shape that
makes the recursion local") — restated here because a less careful
implementation of this exact feature would have reintroduced precisely that
class of bug, in a new call site the original fix never had to consider.

### 7. Tests

`animusd/tests/dynamo_txn_idempotency.rs` (real `ProdEnv`, converged-or-
timeout polling throughout): a same-token/same-fingerprint retry after
commit returns cached success with the effect applied exactly once; a
same-token/different-actions retry is `IdempotentParameterMismatchException`;
token dedup survives a leader failover of the internal table's own
tablet (a 3-node cluster, the node currently leading that tablet killed
mid-scenario, the retry issued through a surviving node); and the
visibility guards (`ListTables` omission, `ResourceNotFoundException` on a
direct `PutItem`/`GetItem`, `ValidationException` on `CreateTable`) all in
one suite. `animus-dynamo`'s own `wire.rs` unit tests cover
`ClientRequestToken` decode (present/absent/both length boundaries/over
the cap/empty) and fingerprint stability (two JSON-key-reordered but
logically identical requests fingerprint identically; a genuinely
different action fingerprints differently). The existing `dynamo_txn.rs`
and `cp_txn.rs` suites — proving atomicity and cross-tablet 2PC unrelated
to idempotency — stay green unchanged, confirming this amendment added a
preflight in front of the existing machinery rather than perturbing it.

### 8. What ships with this amendment, what's still deferred

Landing: `ClientRequestToken` decode + validation (`animus-dynamo::wire`);
a deterministic per-request fingerprint; the reserved internal
`__animus_txn_idempotency` table and its lazy bootstrap; the conditional-
claim/cached-outcome preflight and the best-effort outcome update
(`animusd::dynamo::run_transact`); the `ListTables`/Data Console/every
client-facing wire-operation visibility guards; two new `WireError`
constructors (`idempotent_parameter_mismatch`/`transaction_in_progress`).
`ClientRequestToken` idempotency is therefore **removed from this ADR's own
deferred list** (§5 of the PR7 amendment, and the follow-up amendment's own
deferred summary, both updated in place to point here).

Still deferred, unchanged: CQL LWT/atomic `BATCH`; full per-action
`CancellationReasons` fidelity (issue #374's C2 — a separate follow-up, not
part of this change); `/admin/txns` manual resolution actions. Record/intent
GC remains out of scope (ADR 0018 §2/PR5's own note). Newly named by this
amendment, not pursued here: real DynamoDB's looser "tolerate and eventually
serve a same-fingerprint retry racing its own still-in-flight original"
contract (§4's documented conservative narrowing) — a future refinement if
real operational need for it ever materializes, not a correctness gap in
what shipped.

## Amendment (2026-08-24, per-action `CancellationReasons`)

This amendment closes the one item every prior amendment's own deferred list
kept naming and deferring further: AWS's per-action `CancellationReasons`
array on a `TransactionCanceledException`. Landed as two commits, C2a then
C2b (issue #374's own split), each independently gated and green.

### 1. The wire shape

`WireError` gains `reasons: Option<Vec<CancellationReason>>`
(`animus-dynamo::wire`) — `Some` only on a `TransactionCanceledException`
minted with per-action detail in hand
(`WireError::transaction_canceled_with_reasons`); `None` for every other
error, including the pre-existing aggregate-only
`WireError::transaction_canceled` (kept, for the cases below that still have
no single responsible action to name). `to_json` renders a
`"CancellationReasons"` sibling of `__type`/`message` only when `reasons` is
`Some`. One `CancellationReason` per `TransactItems` action, in the
request's own order:

- `Code: "None"`, `Message: null` (present, never omitted) for every action
  that was not itself the cause.
- `Code: "ConditionalCheckFailed"`, `Message: "The conditional request
  failed"`, and — only when that exact action requested
  `ReturnValuesOnConditionCheckFailure: "ALL_OLD"` **and** the old image was
  cheaply in hand at the point of failure — an `Item` field, DynamoDB-wire-
  encoded exactly like a `GetItem` result.
- `Code: "TransactionConflict"` for a lost race against another
  transaction's own still-unresolved intent — never carries `Item` (no old
  image is ever in hand at the point this is minted).

The aggregate `message` on a `_with_reasons` error is derived from the array
itself (`Transaction cancelled, please refer cancellation reasons for
specific reasons [<codes>]`, AWS's own bracketed wording) — never supplied
separately, so the two can't drift.

`ReturnValuesOnConditionCheckFailure` (`NONE`/`ALL_OLD`, decoded per action —
`wire::decode_rvocf`) is new: every `TransactAction` variant that carries a
condition (`Put`/`Delete`/`Update`/`ConditionCheck`) gained an `rvocf`
field, `#[serde(default)]` so `ClientRequestToken` fingerprinting (ADR
0018's 2026-08-24 amendment, above) stays stable for a request that omits
it.

### 2. Three sites, one shared builder

`dynamo.rs::run_transact` has exactly three places a `TransactionCanceled`
can originate, and this amendment reaches all three — the third only after
C2b's own boundary work (§4 below):

1. **The in-loop `ConditionCheck` evaluation** (a transaction with at least
   one write action, so `cp_txn` is eventually called): the failing action's
   own index is known the instant its `cond.evaluate` returns false.
2. **The all-`ConditionCheck` `writes.is_empty()` fallback** (`cp_txn` is
   never called — see the PR7 amendment §"all-ConditionCheck corner case"):
   `preconditions[i]` corresponds to `actions[i]` by construction (only a
   `ConditionCheck` ever pushes a precondition in this branch, in the same
   iteration order), so a mismatch's index falls out of the loop counter
   directly.
3. **`cp_txn`'s own `Err` return** (C2b): correlated back to an action index
   via `action_keys`, a `(table, key)` list `run_transact` builds in its main
   loop (the same one that resolves every action's key regardless) —
   `TxnAbortReason::ConditionFailed`/`TransactionConflict` name a `(table,
   key)`, matched by position against `action_keys`.

All three build their `Vec<CancellationReason>` through one shared helper,
`dynamo::cancellation_reasons_for(actions, failing_index, reason)` —
`CancellationReason::none()` at every other index, so the three sites can
never disagree about the array's shape.

### 3. `TxnAbortReason`: a typed reason across the `cp_txn` boundary

C2b's core addition is `animusd::TxnAbortReason` (`pub(crate)`, `lib.rs`,
near the other `cp_txn`-adjacent types): `ConditionFailed { table, key }`,
`TransactionConflict { table, key }`, `Other(String)`. `cp_txn`'s own return
type becomes `Result<HlcTimestamp, TxnAbortReason>` — previously `Result<_,
String>`, aggregate by construction. Threading this required widening
`txn_stage_local`/`txn_prepare`/`txn_prepare_pushing`'s own error types to
match (the minimal set that must carry a typed reason, per the "adding a
field beats adding a variant" instinct applied to a `Result`'s error type
instead of an enum — `docs/engineering-lessons.md`); every deeper
`Result<_, String>` helper (`split_group`, `check_preconditions`,
`provision_tablet`, `txn_decide_anchor`, …) kept its shape, wrapped with
`.map_err(TxnAbortReason::Other)` at the boundary that needs to cross it.

The mapping, precisely:

| Source | `TxnAbortReason` | `CancellationReason.Code` |
|---|---|---|
| `txn_stage_local`'s own write-condition eval (`eval_kind_txn_write` returns `Ok(None)`) | `ConditionFailed { table, key }` (`key` = `dynamo::item_key(&p.pk, p.sk.as_ref())`) | `ConditionalCheckFailed` |
| `StageOutcome::ConditionFailed { key }` (apply-time byte-level OCC, ADR 0018 §2's own amendment) | `ConditionFailed { table, key }` | `ConditionalCheckFailed` |
| `StageOutcome::IntentBlocked` surviving every `txn_prepare_pushing` retry | `TransactionConflict { table, key }` (`key` = the last-seen blocked key) | `TransactionConflict` |
| `StageOutcome::Fenced`, a routing failure, a precondition re-check mismatch, any other internal error | `Other(String)` | *(none — aggregate fallback)* |

**Never conflate the two typed variants**: `ConditionFailed` is permanent (a
condition evaluated against a fixed observed value — retrying the identical
request changes nothing); `TransactionConflict` is a transient lost race (a
client's own retry can succeed). This is the same distinction the ADR 0018
§2/PR6 `StageOutcome` doc already draws between its own `ConditionFailed`
and `IntentBlocked` variants — `TxnAbortReason` inherits it rather than
re-deriving it.

### 4. The forwarded hop: a marker-prefixed string, no new wire variant

`ClientRequest::TxnPrepare`'s reply channel is `ClientResponse::TxnPrepared`
or `ClientResponse::Error(String)` — a plain string, the same shape
`dynamo::encode_relayed_error`/`decode_relayed_error` already solve for the
forwarded `KindWriteItem` hop. `TxnAbortReason::encode`/`decode` follow the
identical convention: `encode` renders `serde_json::to_string(self)` behind
a `"txn-abort-reason:"` marker prefix; `decode` strips the marker and
`serde_json::from_str`s the rest, falling back to `Other(raw)` for anything
unmarked or undecodable (an older peer's plain string, a genuinely internal
error, or a corrupted payload) — never a panic, never a silently wrong
variant. `cp_serve_forwarded`'s `TxnPrepare` arm encodes on its `Err` path;
`txn_prepare`'s own `Forward` branch decodes on receipt. No new
`ClientResponse` variant, no `ClientRequest::surface_of` table entry, no
`is_relayable_command` gate to remember — the same reasoning that made a
field win over a variant for ADR 0055's `stale` flag applies here too.

`write_action_condition_failure_survives_the_forwarding_hop`
(`animusd/tests/dynamo_txn_cancellation.rs`) proves the round trip for
real: a pre-split table, a failing write-action condition on the participant
tablet, issued through **every** node's own Dynamo listener in turn (some
route locally, some forward one or two hops) — the right index is flagged
regardless of which node received the request.

### 5. `TransactionConflict`'s reachability is narrower than it looks

`StageOutcome::IntentBlocked` — the source of `TransactionConflict` — is
only ever produced by `TxnStage`'s own apply-time writer-push-intents guard,
reached when a **plain, already-known-value** write proposes directly with
no preceding read (`TxnTableWrite::plain`, the raw client protocol's own
shape). A DynamoDB write action is never that: since ADR 0049's universal
kind-write-path gate, every one evaluates through
`ClientCtx::txn_stage_local` → `dynamo::eval_kind_txn_write`, which reads
the item's **current value first** (`cp_get_local_resolving`) before ever
proposing. When another transaction already holds an unresolved intent on
that exact key, this read is what blocks or errors — confirmed empirically
while building the e2e suite below: routing a real `TransactWriteItems`
write at an intent-held key through the DynamoDB edge consistently produced
`TxnAbortReason::Other("...old-image read failed: CP group leader moved;
retry")` after blocking for `INTENT_WAIT_TIMEOUT` (5s), never
`IntentBlocked` — the coordinator's own read resolves (or fails) the
question long before any stage proposal reaches the apply-time guard that
would answer it differently.

The mapping itself is real machinery, not dead code: it is reached by the
**raw client protocol**'s plain writes (`ClientRequest::Txn` with
`TxnTableWrite::plain`, e.g. `animus-cli`, or any caller of `ClientCtx::
cp_txn` that supplies an already-known value instead of a pending kind
write) — proven end to end by
`write_action_intent_conflict_flags_transaction_conflict`, which stages a
raw, never-decided `TxnPrepare` against a key and then races a second raw
`ClientRequest::Txn` write against it, asserting the second loses with
exactly `TransactionConflict`'s own wording. This is a genuine, narrower-
than-hoped reachability finding about the *DynamoDB* surface specifically,
not a gap in the typed-reason machinery itself; a future change that moves
`eval_kind_txn_write`'s own read to tolerate (rather than block on) a
foreign pending intent would widen it, but is out of scope here.

### 6. Inherited granularity limits — not new ones

- **First-failure-wins, not every-failure-reported**: the in-loop
  `ConditionCheck` scan (§2 site 1) `break`s at the first failing action,
  exactly like the pre-existing whole-or-nothing behavior it always had — a
  request with two failing `ConditionCheck`s only ever reports the first one
  in list order, the rest silently `None`. AWS's own real behavior is the
  same for this shape (a `ConditionCheck` failure aborts the scan
  immediately), so this is not a new simplification.
- **First-participant-wins in `cp_txn`'s own fan-out**: `first_err` (the
  pre-existing field this amendment retypes from `Option<String>` to
  `Option<TxnAbortReason>`) keeps only the first participant failure
  observed, by iteration order over `join_all`'s results — a transaction
  whose anchor AND a participant both fail independently only ever reports
  the participant order happened to surface first. Also pre-existing
  behavior, not a new gap.
- **`Other`/unmatched-key always falls back to the aggregate shape,
  deliberately, never a guessed index**: `dynamo.rs`'s own site-3 matching
  (§2) only builds a `CancellationReason` array when `action_keys` contains
  an exact `(table, key)` match for the typed reason; anything else —
  `Other`, or a `(table, key)` this coordinator never itself resolved
  (should not happen, but is not asserted against) — degrades to
  `WireError::transaction_canceled`'s existing aggregate-only shape. Never
  index-zero, never a best guess.

### 7. Interaction with `ClientRequestToken` idempotency (this file's prior
amendment)

A cached `CANCELLED` outcome replay (a retried token whose original attempt
already failed) stays **aggregate-only** — `transact_write_idempotency_
preflight` returns `WireError::transaction_canceled("cached cancelled
outcome for this ClientRequestToken")` unchanged. The idempotency record
(`__animus_txn_idempotency`) never persisted the original attempt's
`CancellationReasons`, only a bare outcome tag (`PENDING`/`COMMITTED`/
`CANCELLED`) — adding a reasons column there is a distinct, not-yet-needed
widening (the retry is already told the transaction was cancelled; only the
per-action detail is lost on replay, not the fact itself), left for a future
change if real operational need for it materializes.

### 8. Tests

`animus-dynamo::wire`'s own unit tests (`cargo test -p animus-dynamo`): the
exact JSON shape (`Message: null` always present, `Item` present only when
earned, the bracketed aggregate message), `ReturnValuesOnConditionCheckFailure`
decode (present/absent/invalid), and that a plain `transaction_canceled`
never gains a stray `CancellationReasons` key. `animusd::lib.rs`'s in-crate
`txn_abort_reason_tests` (`cargo test -p animusd --lib`): `TxnAbortReason::
encode`/`decode`'s marker-prefixed round trip, including both fallback
shapes (an unmarked string, a marked-but-undecodable payload) degrading to
`Other` rather than panicking — the reachability case §5 already covers end
to end for the *transaction-conflict* variant specifically, so this unit
suite is scoped to the encode/decode mechanism itself.

`animusd/tests/dynamo_txn_cancellation.rs` (real `ProdEnv`, `cargo test -p
animusd --test dynamo_txn_cancellation`) — seven scenarios: a `ConditionCheck`
failure inside a mixed write+check transaction, and the all-`ConditionCheck`
fallback, each flagging the right index (C2a); the `ALL_OLD`/`Item` echo
rule; a successful transaction carrying no `CancellationReasons` at all; a
write action's own condition failing single-node and across the forwarding
hop (C2b, §4); and `TransactionConflict` reachability via the raw protocol
(§5). The pre-existing `dynamo_txn.rs`/`cp_txn.rs`/`dynamo_txn_idempotency.rs`
suites — atomicity, cross-tablet 2PC, and idempotency, all unrelated to
per-action reason fidelity — stay green unchanged, confirming this amendment
only enriched the failure-reporting path rather than perturbing the
commit/abort machinery underneath it.

### 9. What ships with this amendment, what's still deferred

Landing: `WireError.reasons` + `CancellationReason` + `transaction_canceled_
with_reasons` (`animus-dynamo::wire`); per-action `ReturnValuesOnCondition
CheckFailure` decode; the three `dynamo.rs::run_transact` sites building a
full per-action array; `animusd::TxnAbortReason` and its threading through
`cp_txn`/`txn_stage_local`/`txn_prepare`/`txn_prepare_pushing`; the
marker-prefixed encode/decode across the forwarded `TxnPrepare` hop. **Full
per-action `CancellationReasons` fidelity is therefore removed from this
ADR's own deferred list** (the PR7 amendment §5, the follow-up amendment's
own deferred summary, and this file's `ClientRequestToken` amendment §8,
all stale as of this landing).

Still deferred: CQL LWT/atomic `BATCH` (moot — CQL itself was dropped, ADR
0053); `/admin/txns` manual resolution actions; a persisted-reasons column
on the idempotency record for a cached-`CANCELLED` replay (§7). Record/intent
GC remains out of scope (ADR 0018 §2/PR5's own note). Newly named by this
amendment: `TransactionConflict`'s narrow practical reachability through the
DynamoDB wire specifically (§5) — not a correctness gap (the typed mapping
is real and end-to-end tested via the raw protocol), but a fidelity ceiling
worth knowing about before reaching for this array to debug a real
DynamoDB-edge lost-race scenario.

## Amendment (2026-08-26, issue #298 shape B: an unconfirmed recovery query is never evidence of absence)

### 1. The bug

`ClientCtx::txn_recover` (§2/PR5's recovery protocol) makes two decisions
that must only ever be built on **confirmed** facts: (a) "every participant
verified staged" (`all_staged`, feeding a Commit/Abort choice on a
`Pending` record) and (b) "no record exists at all" (feeding an orphan
`Abort` tombstone once past `RECOVERY_GRACE`). Both reads this logic
depends on — `txn_verify` and `txn_record_view` — can fail for a reason
that has nothing to do with the fact being queried: a transient routing
hiccup, or this replica's own read barrier failing, most commonly while a
participant's tablet is mid-fork/cutover under a high split cadence. Before
this amendment, both call sites folded that failure into the same bucket as
a genuine negative result (`Ok(false)` for verify, "no record" for the
record view), so an unconfirmed query could push recovery to **Abort** a
transaction whose own coordinator (`cp_txn`) was concurrently committing,
or had already committed — permanently losing an already-acked write. This
is a live instance of the "duelling decider" hazard §2/PR5 accepts as
*legal only because both deciders are assumed to reach an objectively
correct decision from independently verified state* — an unconfirmed query
breaks that assumption outright, it does not merely weaken it.

Caught live during a `SplitMode::InPlace`-unpinned soak (ADR 0058's G5
row): a captured `all_staged=false`/`Aborted` decision immediately
preceding the soak's "acked write lost" panic on the identical item.

### 2. The fix: an unconfirmed query is UNKNOWN, propagated as a decline, never as a negative

`ClientCtx::txn_recover`'s `all_staged` loop now distinguishes `Ok(true)`
(staged), `Ok(false)` (genuinely not staged), and `Err(_)` (could not
verify) as three distinct outcomes — only the first two may ever feed
`all_staged`; any `Err` makes the whole push **inconclusive**, and the call
declines (`Pending`, proposing nothing) rather than guessing. The identical
fix applies to the record-view side: `RaftKvNode::txn_record_view`
(`animus-cp-data`) is widened to the `stale_get_served`/
`linearizable_get_served` "served" discipline already used elsewhere in
that crate — `Option<Option<TxnRecordView>>`, outer `None` meaning **not
served** (decline), `Some(None)` meaning a **definitively confirmed**
absence (the only value that may feed the orphan-abort branch), and
`Some(Some(view))` meaning found. This propagates through `ClientCtx::
txn_record_view` (now `Result<Option<TxnRecordView>, String>`) and the
`ClientRequest::TxnRecordView`/`ClientResponse::TxnRecordViewReply` wire
pair. A `txn_resolver_loop`-local grace tracker (mirroring
`unresolved_decided`'s own lookup-failure tracker from the issue #298
residuals commit) logs+meters once (`Metric::
CpTxnRecoveryStuckInconclusive`) if a transaction stays `Pending` —
declining on repeated inconclusive queries — well past `RECOVERY_GRACE`: a
pure liveness signal, since correctness never depends on how long recovery
takes to actually decide, only on never deciding wrong.

### 3. Why this does not reopen the recovery-abort case's liveness

Recovery still makes progress in the ordinary case (a genuinely non-staged
participant, or a genuinely absent record, both still confirmable once the
transient condition — a split, a leadership change — clears): the decline
here is retried every `txn_resolver_loop` tick, the same 1-second cadence
that already drives every other recovery push. The only behavior change is
that a **permanently** unreachable participant now stalls that one
transaction's own resolution (safely — its intents stay `Pending`, and the
on-demand foreign-intent read-path push, §2/PR5 §3, still resolves them
the moment any reader hits one directly) instead of resolving it to a
wrong, unrecoverable Abort. This is the same "an intermittent condition
needs a converged-or-timeout poll, not a fixed-deadline guess" discipline
the root `CLAUDE.md` states as a standing rule; the fix simply stops this
one code path from guessing.

### 4. The decide path's own duelling-decider guard was already sound

The apply-time `TxnCommit`/`TxnAbort` arms (`animus-cp-data`) already
implement first-applied-wins with a hard assert only on two genuinely
*conflicting* decisions racing the same log position (impossible in one
sequential log) — a same-outcome-different-ts race, or a commit-vs-abort
race, are both logged no-ops for the losing entry, and every caller
(`cp_txn`, `txn_recover`) already re-reads the record's actual applied
status rather than trusting its own proposed outcome. This amendment did
not need to touch that guard — it was the source-side inputs to the
*decision*, not the decision's own commit-time arbitration, that were
unsound.

### 5. Tests

`animus-cp-data/tests/txn_record_view_served.rs` (new): the fixed
primitive's own "served" contract, deterministic (`SimEnv`) — a genuinely
absent key answers `Some(None)`, a deposed/partitioned leader's own barrier
failure answers the outer `None`, a real staged record answers
`Some(Some(view))`. `animus-test/tests/txn_serializable.rs`'s own `push`/
`resolver_tick` (a `SimEnv` reimplementation of this exact protocol, this
file's §4/PR6 corpus) carried the identical two conflations in its own
test-double logic and was fixed to match. The animusd-level fix itself has
no dedicated deterministic regression (`animusd` has no `animus-sim`
dependency, a standing, named gap) — confirmed instead via a captured live
trace from the real `ProdEnv` soak matching the predicted symptom exactly,
both before (diagnostic fires, panic follows) and after (30 consecutive
clean runs) the fix.

### 6. What ships with this amendment, what's still deferred

Landing: the `all_staged`/`txn_record_view` inconclusive-vs-negative fix
described above; the `txn_resolver_loop` stuck-recovery grace tracker;
`Metric::CpTxnRecoveryVerifyInconclusive`/`CpTxnRecoveryStuckInconclusive`.

Still deferred, named but not fixed by this amendment: a **separate**,
deeper issue #298 mechanism found while investigating this one — a
coordinator-side stage/commit confirmation loss (`txn_prepare`'s own `Err`
results, e.g. "CP group leader moved during participant stage/anchor
commit; retry") is reported to the client as a retryable error carrying
the house "; retry" convention, but — unlike a `StageOutcome::Fenced`
outcome, which is *proven* to have applied nothing — a confirmation loss
does not prove the underlying stage/commit failed to land. A client-level
retry (an un-tokened `TransactWriteItems`, exactly DynamoDB's own
documented duplicate-execution risk) can then legitimately restage over an
already-committed value with a fresh `txn_id`, producing the literal
"shape 5" duplicate-delivery signature from a mechanism distinct from
either the seatbelt `KvCommand::TxnStage` gained in the sibling shape A fix
or this amendment's own recovery-side fix. See `docs/engineering-
lessons.md`'s issue #298 shape A amendment for the full account.

## Amendment (2026-08-26, issue #298 shape A: `TxnStage` must not resurrect an already-resolved key)

`KvCommand::TxnStage`'s apply-time writer-push-intents guard (§2/PR6 task
#16, "Writers push intents, never overwrite one") rejects a stage whose
target key already holds a *different* transaction's unresolved `Intent` —
but had no check at all for a key that is already `Envelope::Committed`
(or restored-post-abort). A stale or duplicate `TxnStage` propose for the
SAME `txn_id`, landing after that transaction's own resolve already ran,
was therefore never rejected: it silently resurrected the key back into
`Intent`, and a later resolve (the ordinary flow, the resolver-loop safety
net, or a recovery push) re-materialized its derived change-log record a
second time, at a fresh HLC — the literal "shape 5" two-sequence-numbers-
for-one-write signature. Caught live during the same `SplitMode::InPlace`
soak that caught this file's shape B amendment: `delivered=146/144`, one
member of a transactional write pair duplicated under a single sealed
shard.

**Fix**: a new bounded, best-effort per-group tracker,
`TxnTracker::recently_resolved` (`physical_key -> txn_id`), populated at
every `TxnResolve` apply (commit or abort restore alike — both leave
nothing of that `txn_id` at the key). `TxnStage`'s apply arm now checks
every write key against it **by `(key, txn_id)` identity**, never presence
alone (the same discipline the `KindBatchOutcome` false-ack fix
established): a match folds into the same `Fenced` outcome bucket as the
existing `already_decided` check. A *different*, genuinely later
transaction reusing the same physical key — the ordinary write path for
any key that isn't brand new — is unaffected; only same-identity
resurrection is rejected. Deliberately not rebuilt at group start (unlike
`pending`/`unresolved_decided`): its whole job is catching a stale re-stage
that arrives shortly after its own resolve within the same process uptime,
so starting empty after a restart is exactly as safe as any other
eviction. Regression (red/green proven): `pr5_orphan_and_resurrection_
tests::a_resolved_key_rejects_a_same_txn_restage_issue_298_shape_a`.

**This fix closes a real, independently-worth-fixing structural gap, but
tracing the captured trace's own `txn_id`s showed it is NOT what produced
the live duplicate delivery** — the resurrecting stage there used a
genuinely different, freshly-minted `txn_id` (a client-level retry racing
its own already-committed first attempt; see this file's shape B amendment
§6 and `docs/engineering-lessons.md`'s matching entry for the full
account of that deeper, still-open mechanism). Both fixes ship together
because each is independently correct and worth having regardless of which
one the next live trace turns out to hit.

## Amendment (2026-08-27, issue #298 "deep shape A" residual: a client-level un-tokened retry, closed)

This amendment closes the one residual the shape B/shape A amendments above
both named and deliberately left open: **a client-level retry of an
un-tokened `TransactWriteItems` racing its own already-committed first
attempt** — the proof-soak's own remaining reason the `SplitMode::InPlace`
un-pin (ADR 0058's G5 row) stayed blocked.

### 1. Design decision: extend the existing token→outcome record, don't derive `TxnId` from the token

Two designs were on the table going in:

- **Derive `TxnId` deterministically from the `ClientRequestToken`** (plus a
  table-set/content hash for the mismatch check), so a same-token retry
  literally *is* the same transaction and the existing txn-record/decision
  machinery makes replay idempotent for free.
- **A durable `token → (fingerprint, outcome)` record**, checked as a
  preflight in front of an otherwise-unmodified `cp_txn` — the design the
  2026-08-24 `ClientRequestToken` amendment above already built and shipped.

The token→outcome record was already live on `main` when this round began
(landed three days earlier, per this file's own amendment above) — so the
real decision this round faced was not "which design," but "is the shipped
design sound, or does it need replacing." Tracing `cp_txn`/`KvCommand::
TxnStage`/`TxnResolve`/`RaftKvNode::txn_recover` (per this round's own
instruction to derive rather than guess) found the shipped design
structurally sound — its **conditional-claim `Put`** (§1 of the prior
amendment, `attribute_not_exists(pk)`) already guarantees a transaction
executes **at most once** per token regardless of what its own outcome
bookkeeping records: a same-token retry racing a still-`PENDING` claim
never reaches `cp_txn` a second time, full stop. The residual was never "the
retry can re-execute the transaction" — it was a **narrower, specific bug**
one layer up: what `run_transact` recorded as that transaction's *outcome*
could be wrong, which mattered because a wrong outcome record — not a
re-execution — is what let a duplicate-execution symptom reach the wire.
Fixing that bug in place is a small, targeted change; re-deriving `TxnId`
from the token would have been a substantially larger rearchitecture
(threading the token through every txn-record/intent/resolve primitive
`animus-cp-data` owns) to fix a bug that doesn't live in `TxnId`'s identity
at all. **Deterministic-`TxnId`-from-token remains a valid alternative
design for a future round if the token→outcome record ever proves
insufficient for a reason this round didn't find** — nothing here forecloses
it — but it was not needed to close this residual.

### 2. The actual bug: an unconfirmed `cp_txn` outcome recorded as a confirmed `CANCELLED`

`dynamo.rs::run_transact`'s `Err(e)` arm (the `cp_txn` call site) recorded
**every** `cp_txn` failure as `TXN_IDEMPOTENCY_CANCELLED`, with no
distinction between the two structurally different reasons `TxnAbortReason`
carries:

- **Definite**: `ConditionFailed`/`TransactionConflict` — a condition
  genuinely evaluated false, or an intent genuinely still blocked past every
  retry `txn_prepare_pushing` budgets for. The transaction provably did not
  commit; `CANCELLED` is correct.
- **Ambiguous**: an `Other` reason carrying the house-wide `"; retry"`
  retryability suffix (`TxnAbortReason::is_ambiguous`, new) — a leader moved
  mid anchor/participant stage, no leader was reachable at all, or
  `StageOutcome::Fenced` fired naming "a concurrent in-doubt-recovery
  decision." **This coordinator could not confirm what happened** — the
  underlying transaction may have committed via a path this exact call never
  observed (its own participant's leadership moved to a replica that went on
  to stage and commit normally, or a concurrent recovery sweep already
  decided `Commit` for this exact `txn_id`). Recording `CANCELLED` here told
  a future same-token retry (and the client) the write **definitely never
  happened** when it may already have — the false-negative half of exactly
  the "an unconfirmed outcome is UNKNOWN, never evidence of a specific
  result" defect class `docs/engineering-lessons.md`'s issue #298 shape
  B/shape A amendments already fixed twice over in `txn_recover`'s own two
  queries (`all_staged`'s inconclusive-vs-negative fold,
  `txn_record_view`'s `None`-conflation) — this is the same class's third
  instance, one layer further out, in the `ClientRequestToken` outcome
  cache rather than in recovery itself.

A client that received that false `CANCELLED` and, per ordinary DynamoDB
client behavior, retried with a **fresh** token (having been told the first
attempt definitely failed) would race its own already-committed write with
a second, independent transaction — the literal mechanism the shape B
amendment's §6 named as the live trigger for the "delivered over expected"
signature. Note the asymmetry with `StageOutcome::Fenced` reached via the
**already-resolved-same-`txn_id`** path (the shape A amendment's own fix,
`TxnTracker::recently_resolved`): that Fenced is *proven* a no-op (this
exact `txn_id` already resolved this exact key — nothing to record either
way, since the caller already knows its own attempt's fate from the earlier
resolve). The Fenced reachable from `txn_prepare_pushing`'s own stage
attempt (this amendment's concern) carries no such proof — it can equally
mean "a **different** decider already resolved this **fresh** `txn_id`
Commit," which is exactly the ambiguity that matters here.

### 3. The fix

- **`TxnAbortReason::is_ambiguous(&self) -> bool`** (`animusd::lib.rs`):
  `false` for both typed variants (always definite); for `Other(msg)`, `msg.
  ends_with("; retry")` — reusing the identical house-wide retryability
  convention `Self::read_should_retry` already tests for the unrelated
  CP-read retry loop, rather than inventing a second one. Audited every
  `TxnAbortReason::Other` construction site in the anchor/participant
  stage/decide chain to confirm the convention already held everywhere
  except one: `txn_prepare`'s `CpRoute::None` arm ("no CP group leader
  reachable for txn prepare") was missing the suffix — a real gap (no
  leader reachable *right now* is exactly as transient/ambiguous as a
  leader having just moved), fixed by appending it there rather than
  widening the predicate to paper over an inconsistent message.
- **`TxnAbortReason::is_safe_to_retry_fresh(&self) -> bool`** — the narrow,
  **allowlisted** subset of `is_ambiguous` where retrying `cp_txn` from
  scratch with a fresh `TxnId` is provably safe: a frozen-tablet refusal
  (`FROZEN_REFUSAL`), no route reachable, a leader-side read failure
  (`"txn prepare: leader-side evaluation failed:"`, always a pre-propose
  read), and `StageOutcome::Fenced`'s own stage-time message. Every one of
  these occurs **before** any Raft propose for this transaction could have
  applied, so a fresh attempt can never race an already-landed one; for
  `Fenced` specifically, tracing its one apply-time source
  (`animus_cp_data`'s `already_decided`) shows its "concurrent in-doubt-
  recovery decision" wording can only fire when the stored record's
  `txn_id` equals the CURRENT attempt's own — impossible for a freshly-
  minted `TxnId`, so a fresh retry hitting `Fenced` is always its other,
  genuinely structural cause (a stale route or an out-of-fence range).
  **`dynamo.rs::run_transact`'s `cp_txn` call site retries only this
  allowlisted subset**, bounded by `CLIENT_TIMEOUT`/`SCHEMA_POLL_INTERVAL`
  (the identical deadline-bounded-loop shape `cp_read`/`cp_kind_write_item`
  already use for this exact class of transient error) — a fresh `cp_txn`
  call (a new `TxnId`) each attempt. `ConditionFailed`/`TransactionConflict`
  and every OTHER ambiguous reason (including every DECIDE-phase
  confirmation loss — see the allowlist-vs-denylist account below) still
  return immediately, unretried.
- **If a retry-eligible failure exhausts its budget still ambiguous, or the
  failure was never retry-eligible to begin with, the idempotency record is
  never marked `CANCELLED`** — left exactly as the preflight's claim `Put`
  left it (`PENDING`, mirroring `transact_write_idempotency_preflight`'s own
  §4 conservative narrowing), so it self-heals via the ADR 0051 TTL reaper
  instead of freezing a possibly-wrong terminal answer. The client gets a
  genuine `TransactionInProgressException` — the same documented,
  SDK-tolerated exception a real still-in-flight original produces — never
  a false `TransactionCanceledException`. New
  `Metric::DynamoTransactWritesAmbiguous` (liveness-only; correctness never
  depends on it firing) counts this case.

**Allowlist, not a denylist — found the hard way, twice, in this
amendment's own proof-soak.** The first implementation retried on
`is_ambiguous() && !is_confirmation_loss()`, where `is_confirmation_loss`
named the two STAGE-time leader-moved messages as the only excluded
reasons. This reproduced the literal `delivered=146/144` duplicate-pair
signature immediately: `resolve_all`'s own DECIDE-phase confirmation-loss
messages ("CP group leader moved during anchor commit/abort", "after
decide", "during orphan abort", and their forwarded-hop equivalents) were
never on the excluded list, so they fell through to "safe to retry" by
default — and a confirmed DECIDE, unlike a confirmed STAGE, fully
materializes every participant's derived writes, so retrying one with a
fresh `TxnId` is exactly the double-materialize race this amendment exists
to close. The fix was not to enumerate more excluded messages (a denylist
approach that had already missed a whole call site once) but to invert the
predicate entirely: `is_safe_to_retry_fresh` names only the reasons proven
safe, and everything this file does not yet know about — including any
future call site's own new `"; retry"` wording — defaults to the
conservative, never-retried bucket. See `docs/engineering-lessons.md`'s
matching entry for the general lesson this generalizes to.

**What this does NOT do, deliberately**: it does not attempt to observe a
specific in-flight transaction's own eventual resolution from outside it
(the §4 narrowing's own named-but-rejected alternative) — the internal retry
loop above absorbs the overwhelming common case (the ADR 0050 F8
frozen-cutover blip and analogous pre-propose rejections, resolving within
seconds, well inside `CLIENT_TIMEOUT`) without that machinery; the rarer
non-allowlisted ambiguous tail stays exactly as conservative as an ordinary
still-`PENDING` original, self-healing via TTL.

### 4. The soak's own client, made production-faithful

`animusd/tests/streams_e2e.rs`'s proof soak
(`multi_split_soak_streamed_gsi_table_under_mixed_load`) issued every
`TransactWriteItems` **without** a `ClientRequestToken` and retried an
un-tokened request verbatim on a retryable error — exactly the residual's
own precondition, and exactly what a real DynamoDB client is documented not
to do. Fixed: the soak now mints one token per transactional write
(`format!("soak-txn-{i:04}")`) and reuses the identical request body (token
included) across every retry attempt — `dynamo_retrying_transact`'s own
retry loop already resent `body` verbatim, so this alone makes every retry
a safe no-op against the durable idempotency record instead of a second,
racing execution. `dynamo_retrying_transact`'s retryable-status
classification also widened to include `TransactionInProgressException`
(previously only the `"; retry"`-suffixed `TransactionCanceledException`
shape) — the wire-visible status a same-token retry now gets while its
original attempt's outcome is still `PENDING` or genuinely ambiguous.

### 5. Tests

`animusd/tests/dynamo_txn_idempotency.rs` gained two scenarios beyond the
2026-08-24 amendment's own four:

- `same_token_retry_after_a_killed_connection_is_exactly_once_including_
  the_stream` — the residual's own literal client-observable shape: a
  request is fully sent and its connection abandoned before any response is
  read (a real killed-connection/timeout ambiguity), then retried with the
  identical token over a fresh connection. Asserts the retry is a cached
  no-op and — since a repeated `PutItem` of the same item is
  indistinguishable from itself on the data alone — that the table's stream
  carries **exactly one** record per item, never two.
- `a_participant_leader_kill_racing_a_tokened_transaction_never_falsely_
  cancels` — a genuinely **server-side** ambiguous outcome: a two-table
  transaction's participant tablet leader is killed immediately before the
  request is issued (racing the election, not waiting it out first). Asserts
  the client-observable contract holds regardless of which internal shape
  the race takes (`cp_forward`'s own hinted retry absorbing the blip
  entirely, or `run_transact`'s new bounded internal retry over a genuinely
  ambiguous outcome): eventual success, never a `TransactionCanceledException`
  surfacing for an unconfirmed outcome, a same-token retry converging to a
  real terminal answer (not stuck `TransactionInProgressException`), and
  exactly one stream record for the participant item.

`TxnAbortReason::is_ambiguous`'s own in-crate unit test
(`txn_abort_reason_tests::is_ambiguous_classifies_by_the_house_retry_
suffix`) proves the classification directly: both typed variants always
`false`; an `Other` `true` only when `"; retry"`-suffixed.
`is_safe_to_retry_fresh`'s own sibling unit test
(`is_safe_to_retry_fresh_is_a_narrow_allowlist_not_a_denylist`) proves the
allowlist boundary directly, including every DECIDE-phase message the
first (denylist) implementation missed.

### 6. What ships with this amendment

`TxnAbortReason::is_ambiguous`/`is_safe_to_retry_fresh`; `run_transact`'s
bounded internal `cp_txn` retry restricted to the allowlisted subset;
never recording a possibly-wrong `CANCELLED` for any ambiguous outcome
(retried-and-exhausted, or never retry-eligible to begin with); the
`CpRoute::None` message fix; `Metric::DynamoTransactWritesAmbiguous`; the
soak's own token-bearing, production-faithful retry client. This closes
the residual named in the shape B amendment's §6 and the shape A
amendment's own closing paragraph above.

### 7. Soak result

The mandated un-pinned `SplitMode::InPlace` proof soak ran 24 clean of 25
total across two contention-free batches, with neither the double-
materialize signature this amendment closes nor the pre-existing lineage-
delivery-timeout residual recurring even once. One new, different residual
surfaced — a genuine (non-ambiguous) `TransactionConflict` cancellation,
likely `is_safe_to_retry_fresh`'s own fresh-`TxnId` retries racing a prior
attempt's still-unresolved intent on the same key faster than
`txn_resolver_loop` clears it — not confirmed live before this round's time
ran out. Correctness-safe either way (a definite `TransactionConflict` is
correctly never recorded `CANCELLED`-then-retried into a double-
materialize; this is a spurious-failure cost, not a data-safety one). Per
the mandate's own "any failure keeps the pin" instruction, `SplitMode::
InPlace` stays **not** un-pinned — see `docs/engineering-lessons.md`'s
matching entry for the full tally, hypothesis, and un-pin decision.

## Amendment (2026-08-27, issue #298 "genuine `TransactionConflict`" residual: confirmed and fixed, but the un-pin stays blocked on a NEW residual)

This round picked up the §7 residual immediately above: the one genuine
(non-ambiguous) `TransactionConflict` the prior amendment's own proof-soak
surfaced but did not confirm live. Diagnostics (`txn_diag_298`-targeted
`tracing` lines, removed before this amendment's own fixes landed) were
added to `cp_txn`/`txn_prepare_pushing`/`run_transact`, and the un-pinned
soak was re-run until captured.

### 1. The hypothesis, confirmed live — plus two siblings found the same way

The captured trace matched the hypothesis exactly: a fresh-`TxnId` retry's
own anchor stage hit `StageOutcome::IntentBlocked` naming, as the blocker,
**the coordinator's own immediately-prior attempt** at the identical
logical write (`is_safe_to_retry_fresh`'s own fresh-`TxnId` retry, ADR
0018's 2026-08-27 amendment above). Tracing *why* that prior attempt's
intent was still live despite its own transaction having already decided
found the precise mechanism: `resolve_all`'s own resolve of the aborted
first attempt's anchor key returned `Ok(())` (`ClientCtx::
txn_resolve_participant`'s `CpRoute::Local` branch discards
`RaftKvNode::txn_resolve`'s own `Option<HlcTimestamp>` signal), but the
underlying `KvCommand::TxnResolve` entry had, in fact, silently no-op'd —
**its own `fence` check rejected it** because the target tablet's range had
shifted (a concurrent split) between the coordinator's `cp_route` lookup
and the entry's actual apply. Unlike `KvCommand::TxnStage`, `TxnResolve`
has no per-attempt outcome channel (`StageOutcome`'s own doc explains why
that distinction matters) — its only signal is "did this entry apply,"
which a fenced no-op satisfies exactly as well as a genuine resolve. The
proposer therefore had no way to learn its own resolve never took effect.

Confirming this live surfaced two further, independently real defects
along the way — not the originally-hypothesized mechanism, but each a
concrete cause of a spurious cancellation under the same un-pinned soak,
found and fixed in the same round per this repo's own "capture, don't
guess" discipline:

- **A wrapping `format!` silently downgraded an already-classified
  message** (`ClientCtx::txn_prepare_pushing`'s exhaustion arm for a
  retryable-shaped `Other` that never even reached its own propose, issue
  #412's own mechanism): the pre-existing code nested the last such message
  inside new sentence text ending in a bare `")"`, moving the house `"; retry"`
  suffix `TxnAbortReason::is_ambiguous` keys on to *before* the closing
  paren — and, independently, moving the message out from under
  `is_safe_to_retry_fresh`'s `starts_with("txn prepare: leader-side
  evaluation failed:")` check. A message that was — before wrapping —
  *already* proven safe to retry with a fresh `TxnId` (this exact stage
  never reached its own propose) was silently reclassified as a **definite**
  failure, recorded `CANCELLED` on the very first `cp_txn` attempt. **Fix**:
  stopped synthesizing a new sentence around `msg` at all —
  `Err(TxnAbortReason::Other(msg))`, passed through byte-for-byte, so this
  arm can never accidentally alter a classification the underlying reason
  already earned. See `docs/engineering-lessons.md`'s matching entry for
  the general "a wrapping `format!` around a classified message is itself a
  classification bug" lesson this generalizes to.
- **A decide-time `FROZEN_REFUSAL` is not safe to retry with a fresh
  `TxnId` once every participant has already staged** — `cp_txn`'s
  "everyone staged, decide commit" branch propagated a hard `txn_decide_
  anchor` failure (most commonly the identical `FROZEN_REFUSAL` string a
  stage-time freeze also produces) straight through `is_safe_to_retry_
  fresh`'s allowlist via `?`. `is_safe_to_retry_fresh`'s own safety
  argument ("an anchor staged without every participant confirming can
  only ever be recovered as Abort") does not hold here: by construction of
  reaching this branch, **every** participant DID stage, so `ClientCtx::
  txn_recover`'s own independent `all_staged`-driven decision can
  legitimately **commit** the original `txn_id` at any moment — a fresh
  retry racing that is exactly the double-materialize hazard the
  2026-08-24/2026-08-27 amendments exist to close, confirmed live via the
  soak's own `delivered=146/144` signature (both keys of one transactional
  pair delivered twice — the original attempt's own eventual recovery-driven
  commit, and the fresh retry's independent one). **Fix**: `ClientCtx::
  txn_decide_anchor_retrying` — retries `txn_decide_anchor` with the SAME
  `txn_id` (never a fresh one) while its own attempt fails outright,
  bounded by `CLIENT_TIMEOUT`, mirroring `cp_kind_write_item`'s issue #288
  freeze-refusal retry shape (routing already re-resolves fresh every
  attempt via `txn_decide_anchor`'s own `cp_route` call). Retrying the SAME
  decision is always safe — a repeat `TxnCommit`/`TxnAbort` propose for an
  already-decided record is a logged no-op (§4's first-applied-wins
  doctrine) — and never abandons already-staged work. All three of
  `cp_txn`'s own decide call sites (the ordinary commit path and both abort
  paths) now go through this wrapper; the two abort paths' own error-wrap
  messages were fixed to preserve the `"; retry"` suffix at the same time
  (the identical bug class as the first fix above, caught by the same
  audit).

### 2. The confirmed mechanism's own fix: push, don't just back off

`StageOutcome::IntentBlocked` gained two fields — `record_key`/
`record_table`, copied straight from the blocking `Envelope::Intent` (ADR
0018 §2/PR4's own `record_key`/`record_table` fields, already carried by
every intent so a foreign READ can chase it — see `IntentInfo`'s doc) — so
a WRITE-side pusher can reach the blocker's own decision with no second
read. `ClientCtx::push_resolution_if_decided` is the write-side sibling of
the foreign-intent READ path's `confirm_or_push`/`resolve_intent_given_
status`: on `IntentBlocked`, before backing off and retrying,
`txn_prepare_pushing` now queries the blocker's own decision
(`ClientCtx::txn_status`) and, if it is already `Committed`/`Aborted`,
actively pushes its resolution (`txn_resolve_participant`, which
re-resolves `cp_route` fresh — the exact thing that sidesteps the ORIGINAL
resolve's stale-fence race, since by now routing correctly reaches
whatever tablet the key belongs to) before the next stage attempt. A
still-`Pending`/unconfirmable blocker is left alone — never pushed via
`txn_recover` (that risks aborting a genuinely live coordinator before
`RECOVERY_GRACE`) — so GENUINE, still-in-flight cross-transaction
contention is completely unaffected: it still only ever resolves via the
existing backoff-and-retry, `txn_resolver_loop`'s passive sweep, or the
blocking coordinator's own eventual decision, exactly as before.

**Deliberately does not fix `TxnResolve`'s own missing outcome channel** —
the deeper structural gap this incident traces back to (§3 below) — since
the write-side push closes the actual reachable symptom (a stage blocked on
a blocker that is CONFIRMED decided) without needing to touch `KvCommand::
TxnResolve`'s apply arm, the fence gate, or the resolve proposer's return
type at all. A future round is free to add a `ResolveOutcome` channel
mirroring `StageOutcome`'s if a reason to do so beyond this incident
surfaces; this fix does not foreclose it and does not depend on it.

### 3. Named, not fixed: `KvCommand::TxnResolve` has no outcome channel

The root structural gap this whole investigation traces back to:
`RaftKvNode::txn_resolve`'s only signal is `wait_applied(index).await.
then_some(ts)` — "did this entry apply," never "did it actually resolve
anything." A fence-miss no-op and a genuine resolve are indistinguishable
to the caller. `KvCommand::TxnStage` solved the identical problem with
`StageOutcome` (§2's own amendment); `TxnResolve` never got the same
treatment. This amendment's fix (§2) closes the one reachable consequence
(a stage blocked on a confirmed-decided blocker) without needing this
larger change, but the gap itself remains: any OTHER caller that assumes
`txn_resolve`'s `Some(ts)` means "resolved" rather than "applied" inherits
the identical hazard. Left for a future round.

### 4. Found, NOT fixed, and still blocking the un-pin: an acked write lost with no error anywhere

Chasing the residual above through further soak runs surfaced a fourth,
independent, and more severe failure: a `TransactWriteItems` call returned
`Ok` (no retry, no ambiguous status, `cp_txn`'s own `txn_decide_anchor_
retrying` call succeeding on its very first attempt) for a two-key
transaction, and a later `ConsistentRead: true` `GetItem` for one of its
own keys came back **completely absent** — not a chased-and-served
still-`Pending` intent, not a `TransactionInProgressException`, just an
empty read, the identical "acked write silently and permanently lost"
signature the 2026-08-26 shape B amendment closed one mechanism of.

**Not this round's shape B mechanism recurring** — no `txn_recover`/
`txn_verify` inconclusive-query activity appears anywhere in the captured
trace for this transaction; it staged, decided `Committed`, and (since
`soak`'s a stream+GSI table) entered the awaited `resolve_all_parallel`
path with no logged failure. **Leading hypothesis, not confirmed**: the
identical `TxnResolve`-has-no-outcome-channel gap named in §3 above,
compounding — under the un-pinned soak's own cascading-split cadence, a
committed transaction's OWN resolve can hit the identical stale-fence
no-op §1 describes, and if EVERY subsequent resolution attempt (the
awaited `resolve_all_parallel` call, and `txn_resolver_loop`'s own passive
per-second sweep afterward) keeps racing a FRESH split before it applies,
the write's own intent could in principle never actually resolve on any
tablet that ever holds it, while every attempt keeps reporting apparent
success — a genuine, deeper consequence of §3's gap this round's own
targeted fix does not reach, since `push_resolution_if_decided` only fires
at STAGE time (when a fresh attempt collides with a live foreign intent),
never on the ORIGINAL committer's own resolve path. **Not root-caused
live**: this round's diagnostics (instrumented as far as `resolve_all_
parallel`) captured occasional individual resolve failures/timeouts
elsewhere in the same soak (always non-fatal, absorbed by the passive
resolver loop within the test's own verification window) but never caught
this exact transaction's own resolve attempt in the act. Recorded here per
this file's own "capture the raw shape, don't re-derive it" instruction so
the next round starts from a real, narrowed hypothesis instead of zero.

### 5. Tests

`ClientCtx::push_resolution_if_decided`'s own regression
(`crates/animusd/src/lib.rs`'s in-crate `issue_298_conflict_tests` module,
`a_fresh_stage_pushes_a_decided_blockers_resolution_instead_of_conflicting`):
constructs the confirmed end state directly (stages a transaction, decides
it `Aborted`, deliberately never resolves it — the same state a stale-fence
no-op leaves, without needing to reproduce the fence race itself), calls
`txn_prepare`(a single, unretried attempt) to observe the real
`IntentBlocked`, invokes the fix under test directly, then calls
`txn_prepare` again and asserts `Staged`. Deliberately avoids
`txn_prepare_pushing`'s own sleep-based retry loop and completes in well
under a second specifically so it can never be coincidentally saved by
`txn_resolver_loop`'s independent one-second passive sweep — an earlier
version of this test, built around the full retrying entry point, passed
identically whether the fix was present or `return`-stubbed out, because
local single-voter leader election alone (~750ms) left just enough real
time before the test's own assertions for the background sweep to
occasionally win the race instead; red/green was only genuinely proven
once the test called the fix directly. Verified both ways (temporarily
disabling `push_resolution_if_decided` reproduces the identical
`IntentBlocked` on the second `txn_prepare` call the fix exists to clear).
`animus-cp-data/tests/txn_conditions.rs`'s pre-existing `StageOutcome::
IntentBlocked` pattern match was updated for the two new fields (`..`,
unaffected by them). No dedicated regression for the `txn_decide_anchor_
retrying` fix or the message-wrapping fix beyond the soak's own capture —
both are `animusd`-level fixes over real routing/timing races
(`animusd` has no `animus-sim` dependency, the same standing, named gap
the 2026-08-24 amendment's own animusd-level fix noted) — confirmed via
the captured live trace matching the predicted symptom exactly, both
before (the failure) and after (30 consecutive soak runs attempted; see
§6).

### 6. Soak result and un-pin decision: NOT taken

With all three fixes in place, the mandated 30-run un-pinned `SplitMode::
InPlace` soak was attempted (41 total runs across the fix-confirmation and
gate phases, tallied honestly rather than stopping at a clean-looking
subset). The message-wrapping/decide-retry residuals (§1's two siblings)
did not recur in any run once their own fixes landed. **The genuine
`TransactionConflict` residual this amendment set out to close DID recur
once more, 17 clean runs into the formal gate**, even with `push_
resolution_if_decided` active — the captured trace shows the identical
"resolve reports success but the intent stays live" shape §1 describes
recurring a SECOND time on the SAME key before a fresh stage attempt
exhausts into `TransactionConflict` again, but the run's own log level
(`warn`, not `debug`) does not capture whether `push_resolution_if_decided`
fired and found the blocker still `Pending`/unconfirmable (plausible: the
STATUS query it depends on is just as reachable to the same stale-fence-
causing split cadence as the original resolve was), or fired and its own
push attempt hit the identical fence race a second time. **Not root-caused
to that level of detail** — recorded here rather than re-derived, per this
file's own standing instruction, so a future round can instrument
`push_resolution_if_decided` itself directly rather than re-discovering
this gap. This confirms §3's own point sharper than intended: the fix
closes the mechanism as originally captured and as directly unit-tested,
but does not categorically close every path to the same symptom while
`TxnResolve`'s own missing outcome channel remains open — a single,
sufficiently adversarial split cadence can still reach it. **§4's
newly-found "acked write lost, no error anywhere" residual also
recurred** during the same investigation, alongside occasional
recurrences of the pre-existing lineage-delivery-timeout residual (ADR
0058's G5 row) — all three verified in isolation per this file's own
standing "a timing-sensitive failure needs an isolated rerun before being
treated as real" discipline, and all genuinely reproduce (not
host-contention artifacts of this investigation's own back-to-back local
runs).

Per the mandate's own "any failure keeps the pin" instruction: `SplitMode::
InPlace` stays **not** un-pinned. This round closes three confirmed, real
bugs (each independently worth having, each proven by a captured
before/after trace or a red/green regression test) and meaningfully
narrows the residual's own reachable surface, but does not close it
categorically, and §4's own residual is a genuine, unresolved
data-loss-shaped hazard under the un-pinned soak's own cadence — shipping
the un-pin without root-causing both would trade a known, narrow,
already-tracked gap (ADR 0050 rung 8's own copy-workflow acceptance soak
staying pinned) for two unknown ones. The clearest path to actually closing
this class, per §3's own finding, is giving `KvCommand::TxnResolve` a
`StageOutcome`-shaped outcome channel so a resolve's own proposer — and
`push_resolution_if_decided` — can tell a fence-miss no-op from a genuine
resolve, rather than continuing to patch each individually-discovered
symptom. See `docs/engineering-lessons.md`'s
matching entry for the full tally and the next round's starting point.

## Amendment (2026-08-29): `KvCommand::TxnResolve` gains a `ResolveOutcome` channel, closing §3

This round builds exactly the fix §3/§6 above named and left for later:
`KvCommand::TxnResolve`'s apply arm now records a `ResolveOutcome`
(`Resolved` / `Fenced` / `OutcomeMismatch`) per Raft log index, paired with
the entry's own term — the identical shape and term-identity discipline
`StageOutcome`/`StageOutcomes` already established for `TxnStage`, and
`CasResults`/`KindBatchOutcomes` before that (see this repo's
`animus-cp-data/CLAUDE.md` "Key invariants" section for the shared
doctrine: an *accepted* entry is not yet a *committed* one, so an index
alone can be reoccupied by a different command after a leadership change —
only `(index, term)` together identify one entry). `RaftKvNode::
txn_resolve` now returns `Option<(HlcTimestamp, ResolveOutcome)>` instead
of the old, ambiguous `Option<HlcTimestamp>` — every in-crate and
`animusd` caller was updated to check the outcome explicitly rather than
treating `Some(ts)` alone as success.

### 1. The fix's actual shape

- **`ResolveOutcome`** (`animus-cp-data::txn`): `Resolved` (every key in
  `keys` fell inside this entry's fence and was resolved, or had already
  been resolved — an idempotent replay is not a failure); `Fenced` (the
  whole entry no-op'd because a key fell outside the group's live fence or
  into a sealed range — the exact "target tablet's range shifted between
  `cp_route` and apply" case §1 above traces the write-loss symptom to);
  `OutcomeMismatch` (the pre-existing PR6 defense-in-depth check — the
  carried `outcome` didn't match the anchor's own decided record —
  reported as its own variant rather than folded into `Fenced`, since
  re-routing would not help a mismatch the way it helps a genuine
  fence-miss).
- **A real, independent bug found and fixed while wiring this in**: the
  apply arm used to call `txn_tracker.unresolved_decided.remove(&txn_id)`
  **unconditionally**, before ever computing whether the resolve actually
  fenced or mismatched. Since `unresolved_decided` is exactly what
  `txn_resolver_loop`'s passive per-second sweep uses to find
  decided-but-unresolved transactions to keep pushing, a Fenced resolve
  used to erase this group's own memory that the transaction still needed
  resolving — even though nothing was actually written. This is a
  concrete, previously-silent contributor to the "resolve reports success
  but the intent stays live" shape §1 captured: on the specific group that
  fence-missed, the passive safety net had already given up, by
  construction, the very same tick the fence-miss happened. Fixed:
  the removal now runs only when `resolve_outcome == ResolveOutcome::
  Resolved`. Caught by this round's own regression before it ever reached
  a live soak — `animus-cp-data/tests/txn_recovery.rs::
  pending_txns_reflects_applies_across_restart` (pre-existing) started
  failing the moment the conditional-clear landed, because it resolved
  with a stale `commit_ts` (`txn_id.ts` — the pre-decision candidate, not
  the actual decided value `commit_at_least` returns) that no longer
  matched the record's real status, an `OutcomeMismatch` this fix now
  correctly refuses to treat as done. The test's own pre-existing
  assumption ("resolving always clears the tracker") was itself masking
  this exact class of bug; fixed by resolving with the real decided
  `commit_ts` instead, restoring the property under test.
- **The coordinator-side fix** (`animusd::ClientCtx`): `txn_resolve_
  participant` now returns `Result<ResolveOutcome, String>` instead of
  `Result<(), String>` — a `CpRoute::None` (nowhere to route this to right
  now) is now a genuine `Err`, never silently `Ok(())`, since that used to
  be indistinguishable from "resolved." A new bounded-retry wrapper,
  `txn_resolve_participant_retrying` (`TXN_RESOLVE_FENCED_RETRY_ATTEMPTS`
  = 3, backed off by the existing `TXN_STAGE_PUSH_BACKOFF`), re-resolves
  `cp_route` **fresh** on every attempt — the actual fix for the
  acknowledged-write-loss bug: a fenced resolve now triggers a re-route
  and retry against whatever tablet currently owns the key, instead of
  being silently swallowed. Every existing best-effort caller
  (`resolve_all`, `resolve_all_parallel`, `recovery_resolve`,
  `push_resolution_if_decided`) now goes through this wrapper instead of
  the raw one-shot primitive. The wire reply for a forwarded
  `ClientRequest::TxnResolve` changed from a bare `ClientResponse::PutOk`
  (indistinguishable success) to `ClientResponse::TxnResolved { outcome:
  ResolveOutcome }`, carrying the real outcome across the forwarded hop
  too.
- **`txn_decide`'s single-tablet convenience path is deliberately
  unchanged** (`animus-cp-data`): it has no routing/metadata layer of its
  own to re-route through on a `Fenced` outcome (that facility only exists
  in `animusd`), so its own internal resolve call still discards the
  outcome — the background resolver loop and the on-demand foreign-intent
  push remain its safety net, exactly as they already were for a resolve
  that never applies at all.

### 2. Tests

`animus-cp-data/tests/txn_resolve_outcome.rs` (new): two deterministic
`SimEnv` scenarios reproducing the exact ambiguity and its fix directly at
the primitive level, using the same two-group anchor/participant harness
shape `tests/txn_multi.rs` already uses (a single-group anchor-only
transaction's own resolve can reconstruct its value on a plain read purely
from the locally-held decided record + intent even when the physical
resolve never lands — `RaftKvNode::resolve_decided`'s own read-time
reconstruction — which would mask the symptom under test; a genuine
participant, holding no local copy of the anchor's record, cannot):

- `an_ordinary_resolve_reports_resolved_and_the_value_lands` — the negative
  control: no split racing it, `ResolveOutcome::Resolved`, value visible.
- `a_participants_resolve_racing_a_split_reports_fenced_not_a_false_success`
  — a real in-place split fork (`KvCommand::SplitTablet`, reusing
  `Freeze`'s whole-range seal) on the **participant**'s own tablet,
  between the anchor's commit and the participant's own resolve call:
  asserts `ResolveOutcome::Fenced`, and that the participant's own key
  stays genuinely unreachable (`local_get` reports `None` on every
  replica, since the group holds no local record to reconstruct the value
  from) — the "looks lost" shape a caller must not mistake for done —
  while the anchor's own decision stays durably `Committed` throughout.

**What this does NOT include, and why**: a full end-to-end `animusd`-level
reproduction (a real coordinator racing a real `TransactWriteItems` against
a real cluster mid-split, asserting the write survives via a
`ConsistentRead: true` follow-up read) was considered but not built as a
new test. `animusd` has no `SimEnv`-driven cluster harness capable of
driving a real split (see that crate's own CLAUDE.md, "SimEnv `ClientCtx`
harness" section — schema DDL and `trigger_split` are named, documented
blockers of that specific harness), so a deterministic version isn't
possible today; a real `ProdEnv` version would have to reproduce the same
kind of timing race this ADR's own soak needed many runs to hit reliably,
which is a poor fit for a single, fast, always-green CI regression. Instead,
`animusd/tests/cp_txn.rs::decided_but_unresolved_record_survives_its_own_
tablet_splitting_before_resolve` (pre-existing, unmodified, and reverified
green under this change) already covers the equivalent real-cluster
end-to-end property with a deliberately engineered (not racy) split
ordering, and the coordinator-side retry logic this round adds
(`txn_resolve_participant_retrying`) is exercised by that same suite plus
every other `animusd` transaction/split integration test (`cp_txn.rs`,
`dynamo_txn.rs`, `dynamo_txn_cancellation.rs`,
`txn_recovery_participant_spans.rs`, `inplace_split_e2e.rs`,
`split_lifecycle.rs`, `split_build.rs`), all reverified green.

### 3. Effect on the `SplitMode::InPlace` un-pin (ADR 0058)

**This round does not attempt the mandated un-pin soak, and does not touch
`--split-mode`'s default.** It closes the specific structural gap §3/§6
named as blocking a categorical fix, and independently fixes one concrete,
previously-silent contributor to the write-loss symptom (the unconditional
`unresolved_decided` clear, above) — but per this file's own "any failure
keeps the pin" mandate, only a fresh, clean 30-run un-pinned soak (ADR
0058's own gate) can actually move the pin, and that soak was not run this
round. See ADR 0058's matching note.
