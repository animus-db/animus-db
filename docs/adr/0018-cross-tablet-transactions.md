# ADR 0018 — Cross-tablet transactions on the CP plane (2PC over per-tablet Raft + HLC + MVCC)

- **Status:** Proposed
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
  dependency sets, recovery, per-shard consensus, MVCC snapshot reads — over the
  AP lineage (local execution or the data-plane frontier). So "cross-shard atomic
  transactions" is not unsolved in the codebase; the open question is specifically
  how the **CP (Raft) plane** gets them, and whether it should reuse Accord or get
  its own mechanism.
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
