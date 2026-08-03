# ADR 0016 — Pluggable replication mode: per-tablet Raft (CP) alongside the AP data plane

- **Status:** Proposed
- **Date:** 2026-08-03

## Context

AnimusDB today has exactly one replication strategy for user data: the
**leaderless AP data plane** (ADR 0001) — `serve_replica` + the `DataClient`
quorum coordinator, tunable `R + W > N`, with eventual convergence via read-repair
+ anti-entropy + hinted handoff (ADR 0010) and per-tablet epoch fencing (ADR 0002).
It is available under partition and resolves conflicts by LWW; multi-key atomicity
and a serialization order are added *above* it by Accord (ADR 0011).

There is demand for the other major point on the CAP spectrum: a **strongly
consistent (CP) mode** where each tablet is its own consensus group with a single
leader, giving linearizable single-key (single-tablet) reads and writes directly,
at the cost of write-availability during leader loss. This is how range-partitioned
CP stores (Bigtable/Spanner, CockroachDB, TiKV) — and, internally, modern DynamoDB —
actually replicate.

Three questions must be settled before any code:

1. **One project or two?** Is a CP mode a modular addition or a separate system?
2. **Which per-tablet consensus protocol** — Raft or Multi-Paxos?
3. **How does it compose** with the existing control plane, placement, storage,
   wire adapters, and the deterministic-simulation discipline (ADR 0003)?

Relevant existing assets:

- **`animus-control`'s `RaftCore`** (ADR 0009): a synchronous, I/O-free,
  `Env`-driven Raft state machine — election, log replication, the current-term
  commit rule, WAL durability, snapshotting, chunked `InstallSnapshot`, and the
  durable-before-visible apply gate. It is *currently hardcoded* to the control
  plane's state machine (`metadata: Metadata`, `applied: Vec<MetaCommand>`,
  `apply()` → `Metadata::apply`).
- **`animus-consensus`'s `shard.rs`** (`ShardedOwner` / `ShardRouter`): the
  precedent for **one consensus group per tablet** — a physical node hosts one
  group instance per local tablet, each on its **own** `Env` id (the inbox is
  single-consumer), routed from the *existing* tablet map. No new control-plane
  state was needed.
- The **wire edges** (`animus-dynamo`, `animus-cql`) already route all reads/writes
  through `ClientCtx` → `DataClient`, not through replication internals directly.
- The **control plane** already owns `Metadata` = membership + tablet map +
  placement policies + the replicated schema catalog (ADR 0013), and drives
  placement reconciliation (ADR 0005) and failure detection (ADR 0012).
- **Accord** (ADR 0011) already provides leaderless, conflict-aware, often-one-RTT
  consensus (the EPaxos family) at the transaction layer.

## Decision

### 1. One project, with a pluggable replication backend

We will keep a **single, modular project** and introduce a **replication-backend
seam**, not a second system. The shared backbone is the large majority of AnimusDB
and is identical in both modes: the `Env` seam + simulator (ADR 0003), the
`animus-control` Raft **control plane** as the metadata authority, `RaftCore`,
`animus-storage`, `animus-tablet`, `animus-placement`, the wire adapters, and the
`animusd` node assembly. Only the **serving path** diverges: how a tablet's
replicas agree on writes and serve reads, and the consistency/availability point
that follows.

Concretely, both `animus-data` (AP quorum) and a new CP backend will implement one
seam — the shape `DataClient` already exposes:

- a client surface (`read` / `write` / `scan`) consumed by the wire edges via
  `ClientCtx`;
- a per-node serve loop consumed by the `animusd` assembly;
- a placement hook consumed by the control-plane reconciler.

Components stay generic over `E: Env` (monomorphized, never `dyn`); the backend is
selected by configuration. The **end-state** is *per-table* mode selection: because
the control plane already replicates a schema catalog (ADR 0013), a table can
declare `consistency: eventual | linearizable`, and an AP table and a CP table can
coexist in one cluster over the same control plane, storage, and wire — the natural
generalization, not a special case.

### 2. The per-tablet consensus protocol is Raft (not Multi-Paxos)

Raft and Multi-Paxos are the same family — leader-based log replication with
majority-intersecting quorums and identical safety/liveness guarantees; Raft is
"Multi-Paxos with the ambiguous parts pinned down." The choice is engineering
tradeoffs, and for *this* use case (range-partitioned, single-region-ish, strong
per-tablet consistency) Raft wins decisively:

- **We already have a proven `RaftCore`** — sync, I/O-free, durable, snapshotted,
  sim-tested, and recently hardened (durable-before-visible, role-aware apply).
  Per-tablet CP mode is a *reuse* of it, not a from-scratch protocol.
- **Understandability → fewer ways to be subtly wrong**, which matters most for
  safety-critical in-house consensus and aligns with verify-by-simulation:
  Multi-Paxos's multi-decree / leader-lease / membership corners are notoriously
  underspecified (more corners to get right *and* to sim-verify).
- **Contiguous-log reasoning** keeps recovery, snapshotting, and `InstallSnapshot`
  clean (all already implemented), and **membership change** is well-specified.

Multi-Paxos's genuine advantages do not pay for themselves here, because each is
either inapplicable, retrofittable onto Raft, or already covered elsewhere:

- **Out-of-order commit / pipelining** (Paxos accepts slot N+1 with a hole at N;
  Raft stalls): the one genuinely Raft-hard edge — and even it has a Raft-shaped
  answer in production (**ParallelRaft**, PolarFS). Deferred unless a workload needs
  it.
- **Flexible quorums** (FPaxos: election and replication quorums need only
  intersect, so writes can use a small quorum): unused for a single-DC RF 3/5
  tablet, and **"Flexible Paxos" applies to Raft too** — we can relax Raft's quorum
  rule *if* geo-distribution ever makes a small write quorum the headline feature.
- **Leaderless / one-RTT** (the EPaxos direction): **already provided by Accord**
  (ADR 0011). The per-tablet CP mode's job is specifically *simple, strong,
  leader-based per-range* consensus — Raft's home turf — not to re-implement a
  leaderless protocol we already run a layer up.

### 3. Composition

- **`RaftCore` is generalized to `RaftCore<SM>`** over its state machine + command
  type. Every safety-critical part (election, replication, the commit rule, WAL,
  snapshot/`InstallSnapshot`, the durable/visibility gates) is state-machine
  agnostic; only `apply()` and the snapshot-image type touch the SM. The control
  plane keeps its `Metadata` SM; the CP data backend supplies a **KV SM** whose
  `apply` merges into a per-replica `StorageEngine`.
- **Per-tablet groups** follow the `ShardedOwner` precedent: a node hosts one Raft
  group per local tablet, each on its own `Env` id, routed from the tablet map. No
  new partitioning concept — tablets remain the unit (ADR 0002).
- **Leadership + linearizable reads** are new surface: route a key to its tablet's
  current leader, follow `NotLeader` redirects, and serve linearizable reads via
  read-index / leader lease. Per-tablet leadership + membership changes are driven
  through the control plane's existing placement/metadata machinery (or
  self-elected by each group and reported up — to be decided in the build ADR).
- **The AP convergence machinery is simply absent** in CP mode (Raft gives
  consistency directly): no read-repair, anti-entropy, hinted handoff, or LWW.
- **Determinism is preserved**: both modes run over `Env` and are sim-testable. The
  Elle/corpus harness gains a `RaftPerTablet` topology alongside `Authoritative` /
  `Frontier` (ADR 0014), so the existing linearizability/serializability checkers
  run against CP mode directly.

## Consequences

**Easier / unlocked:**

- A second, strongly-consistent product point without forking the project — one
  control plane, storage, wire, sim harness, and node assembly serve both.
- Per-table consistency selection becomes a natural feature (Dynamo-style AP table
  and a linearizable table in one cluster).
- CP mode is *simpler* in the convergence dimension — no eventual-consistency
  reconciliation to reason about.

**Costs we knowingly accept:**

- **`RaftCore<SM>` generalization** touches the most safety-critical code in the
  repo. Bounded (only `apply` + snapshot type are SM-specific) but must be
  re-sim-tested in both instantiations.
- **New surface**: a Raft-routing coordinator, per-tablet leader leases /
  transfer-on-failure / rebalancing — none of which the AP plane needed.
- **Cross-tablet transactions fork**: AP mode layers Accord on the frontier; CP
  mode wants 2PC across Raft groups (the Spanner/Cockroach model), though Accord
  could still sit atop the Raft groups as the durable store. A layer above
  replication, not a blocker, but a real design decision deferred to the build ADR.
- **Operational load**: N tablets × RF replicas = many Raft groups, each with its
  own timers/heartbeats and `Env` id. Manageable (Cockroach/TiKV do it) but it is
  the cost AP mode avoids.
- **Availability trade is inherent**: a CP tablet is write-unavailable during
  election / quorum loss, where the AP plane stays up via sloppy quorum + hinted
  handoff. That is the point of offering both, not a defect.

**Follow-up work (sequenced):**

1. This ADR (the decision). 
2. Generalize `RaftCore` → `RaftCore<SM>` as a no-behavior-change refactor (control
   plane keeps passing today's `Metadata` SM; full existing suite stays green).
3. The per-tablet Raft KV backend + the replication-backend seam + the Raft-routing
   coordinator (its own build ADR, with the leadership-ownership and cross-tablet
   transaction decisions).
4. Per-table mode selection via the schema catalog; a `RaftPerTablet` topology in
   the Elle corpus.

**Future escape hatches (explicitly out of scope now):** flexible quorums on Raft
(geo-distributed small write quorums) and ParallelRaft-style out-of-order commit —
both retrofittable onto the Raft backend if a workload ever justifies them.

This ADR extends ADR 0001 (which fixed *one* AP data plane) to a pluggable
replication mode; ADR 0001's two-plane split (AP/CP data vs the Raft control plane)
is otherwise unchanged — the control plane remains the metadata authority in both
modes.
