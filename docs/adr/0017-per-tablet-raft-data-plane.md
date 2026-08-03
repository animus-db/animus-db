# ADR 0017 — Per-tablet Raft data plane (leaderful, linearizable KV)

- **Status:** Accepted — **Stages A–D implemented** in `animus-raftdata`
  (linearizable single-tablet KV, compaction + streaming snapshots, single-server
  membership change, tablet split), all sim-tested; the plane's linearizability is
  verified by a dedicated **Elle corpus** (`animus-test/tests/raftkv_linearizable.rs`);
  the **automatic membership-change trigger** and **in-band split group creation**
  are wired under `SimEnv`; and **Stage 3a** of the production assembly runs the CP
  plane in `animusd` over `ProdEnv` with per-table AP/CP routing. The remaining work
  is **Stage 3b** integration plumbing (dynamic CP placement/split/reconfigure over
  `ProdEnv` — the `ProdEnv` side of `Coresident` + control-plane address
  distribution — and cross-process CP routing), not new mechanism.
- **Date:** 2026-08-03

## Context

ADR 0016 decided that AnimusDB will offer a **strongly-consistent, leaderful**
replication mode — each tablet its own Raft group, single leader, linearizable
single-tablet reads/writes — as a modular addition alongside the leaderless AP
data plane, and chose **Raft** (not Multi-Paxos). Step 2 of that ADR is done:
`RaftCore` is now generic over its command and state machine (`RaftCore<C, S>`,
`StateMachine<C>` trait), with the control plane as one instantiation and the
existing suite green.

This ADR is the **build design** for the leaderful data plane (ADR 0016 step 3),
settling the decisions deferred there. The shape is essentially a "Cockroach-lite"
CP data plane: per-range (per-tablet) Raft, range splits, replica rebalancing on
membership change, backed by the on-disk LSM, all over the deterministic `Env`
seam.

Load-bearing constraints and prior decisions that shape it:

- **`RaftCore` is synchronous and I/O-free** (ADR 0009): all consensus logic runs
  in a pure state machine; the driver owns the `Env` and does I/O. The control
  plane's `Metadata::apply` is in-memory and *synchronous*, called inside the
  core.
- **`StorageEngine` (the `LsmEngine`) is `async`** (ADR 0004/0008): applying a
  committed KV write is async disk I/O.
- **Determinism is the correctness story** (ADR 0003): every safety property must
  be establishable under `SimEnv`. A safety property that depends on real wall
  time is *not* simulation-verifiable.
- The control plane (`animus-control`) already owns the **tablet map**,
  **placement** (ADR 0005), **failure detection** (ADR 0012), and the **schema
  catalog** (ADR 0013), and has `SplitTablet`/`MergeTablets`/`CasTabletReplicas`
  `MetaCommand`s.
- The **`ShardedOwner`/`ShardRouter`** precedent (`animus-consensus`) shows how a
  physical node hosts one consensus group per local tablet, each on its own `Env`
  id, routed from the existing tablet map.

Decisions taken with the maintainer before this draft: LSM-backed state with
streaming snapshots (no in-memory-first stage); **deliver reconfiguration** —
tablet split on cluster growth and replica move on node failure; **ReadIndex**
reads (leases explicitly *not* a casual optimization — see below); cross-tablet
transactions deferred but recorded as the next step.

## Decision

We will build the leaderful data plane as a new crate (`animus-raftdata`),
**additive** — the AP `animus-data` plane is untouched and no dual-mode seam is
built yet. The control plane remains the metadata authority for both planes.

### 1. Topology, hosting, and leadership

- **One Raft group per tablet**, `RaftCore<KvCommand, _>`, hosted on the
  `ShardedOwner` precedent: a physical node runs one group instance per local
  tablet, each on its **own** `Env` id (the inbox is single-consumer), with the
  replica set + key range read from the **existing tablet map** — no new
  control-plane partitioning concept (tablets remain the unit, ADR 0002).
- A new **`RaftKvNode<E>` driver** drives a tablet group over `Env`. It is *not*
  `RaftNode`: it has **no `reconcile_loop`/`detect_loop`** (those are
  control-plane-only) and adds the data-plane responsibilities below (apply
  effects, ReadIndex, client routing).
- **Leadership is self-elected and reported up**, not assigned: each group runs
  Raft's own election; its current leader is observable and surfaced into the
  control plane's view for routing. The control plane owns *placement* (which
  nodes hold a tablet), not *which replica leads* — the Cockroach/TiKV model.

### 2. State machine, apply path, and snapshots (the LSM decision's consequence)

The KV state machine is **LSM-backed** (`StorageEngine`), not in-memory. Because
`StorageEngine` is `async` and `RaftCore` is synchronous, **the apply step moves
out of the sync core into the async driver** — the same sync-core/async-driver
split `AccordCore` already uses (the core decides *order* and emits effects; the
driver does the *I/O*):

- The sync core agrees the committed, durable order of `KvCommand`s and exposes a
  **drain of newly-applicable committed entries** (gated by the durable-before-
  visible watermark from ADR 0009, so only fsynced entries are applied/visible).
- The **driver applies** each committed `KvCommand` to the `LsmEngine`
  asynchronously, then advances the core's `last_applied`. The in-core,
  synchronous `StateMachine::apply` path (step 2) **remains** for the in-memory
  control plane; the data plane uses this **effects** path. (Mechanically: factor
  "apply committed entries" so the control plane keeps its in-core convenience
  while the KV plane applies via the driver — the precise trait/struct shape is a
  Stage-A implementation detail, but the *architecture* is: order in the core,
  apply in the driver.)
- **Snapshots stream from the engine**, not from a serialized in-memory clone. The
  step-2 trait's `Default + Clone + Serialize` whole-image model does not scale to
  a large tablet. Instead the snapshot is produced from an **engine checkpoint /
  ordered scan** and ingested as a byte stream. The existing **chunked
  `InstallSnapshot`** machinery is reused unchanged — it already ships opaque bytes
  in offset-addressed chunks; only the *source* of those bytes generalizes from
  `serde_json::to_vec(state)` to "read the engine checkpoint." Install ingests the
  stream into a fresh engine.

### 3. Linearizable reads: ReadIndex

Reads use **ReadIndex**: the leader records `readIndex = commit_index`, confirms
it is still leader via one heartbeat round to a quorum, waits until its applied
(and durable) state reaches `readIndex`, then serves locally. No log entry, **no
wall-clock assumption** — only a quorum confirmation plus the applied watermark we
already track. It composes with durable-before-visible (a ReadIndex read reflects
only `min(commit, durable)` state). The no-op-on-election (`become_leader` already
appends one) gives the leader a committed current-term entry so `readIndex` is
sound. Follower reads (a follower asks the leader for `readIndex`, waits for its
own applied state) are a natural later extension, enabled by this design and by
the follower-applies-on-commit behavior from the durable-before-visible work.

**Leader leases are explicitly NOT adopted, and are a cautionary path, not a
recommended optimization.** They are recorded here only so a future reader
understands the hazard and the bar to clear before even considering them:

- A lease replaces the ReadIndex round-trip with a **bet on real elapsed time**,
  which turns a timing property into a **safety-critical** one — a violation
  yields a *silent stale read*. Every other timing assumption in the system
  (election timeouts, failure detection — ADR 0012) is **liveness-only**: a
  violation costs at most an extra election, never correctness. Leases are the
  one place a clock bug corrupts a read.
- The practical killer is **not** steady clock drift (which is tiny) but **process
  pauses** (VM migration, GC, scheduler starvation): a paused leader's clock
  effectively stops, so it serves a read inside a lease that has actually expired
  while a new leader took over.
- Logical clocks (Lamport/vector/HLC) **cannot** substitute: they provide *order*,
  not *elapsed real time*, and a lease races a real-time election timeout. HLC
  *manages* a physical-clock assumption (and is the right tool for cross-tablet
  transaction timestamps — §5), it does not remove it. The industry answer to
  "trustworthy time lease" is TrueTime-style *better clocks*, not logical clocks.
- The determinism story makes it worse *here*: today's `SimEnv` has a single
  perfect virtual clock, so it **cannot exercise a lease-safety violation** — the
  hole would be invisible to the deterministic suite (the `ProdEnv`-only class our
  durability/flaky-test lessons keep surfacing).

**Therefore leases are off the table unless and until** `SimEnv` gains a **per-node
clock skew + pause injection model** (the `Env` already hands out a per-node clock,
so this is feasible) that makes a lease-safety violation a deterministic,
seed-reproducible failure the linearizability checker catches — *and* a dedicated
safety analysis is written. Even then they remain an optimization to weigh against
the round-trip they save, not a default. ReadIndex is the baseline indefinitely.

### 4. Reconfiguration

Both forms of reconfiguration are in scope and are driven by the **control plane**
(it already detects failures and owns placement); the data-plane groups execute
the mechanics.

- **Replica movement on node failure** requires **Raft membership change**, which
  `RaftCore` does **not** have today (`all_nodes` is fixed at construction; there
  are no conf-change messages). We will add **single-server membership change**
  (add-one / remove-one, the safer, simpler variant — not general joint
  consensus), as a committed configuration entry in the Raft log. Flow: the
  failure detector marks a node `Down` (ADR 0012) → the placement reconciler
  (ADR 0005) decides a replacement replica → it proposes a membership change to
  that tablet's group to add the spare and remove the dead node; the new replica
  catches up via `InstallSnapshot` + log. This is safety-critical new consensus
  code and lands with fault-injecting sim tests.
- **Tablet split on cluster growth** uses a **Raft split trigger** (the Cockroach
  range-split model): when the control plane decides to split a tablet (e.g. a new
  node joins and the rebalancer spreads load, or a size threshold), it commits a
  `SplitTablet` in `Metadata`; the owning group applies a **special committed Raft
  command** that atomically divides the key range and hands the right-hand state
  off to a **new group bootstrapped at the split point**. Splitting a *live
  consensus group's data* is the most intricate operation here and is sequenced
  last. (Merge — the inverse — is deferred with cross-tablet work.)

### 5. Cross-tablet transactions: deferred (the explicit next step)

This data plane gives **single-tablet** linearizability only. **Cross-tablet
atomic transactions are out of scope for this ADR and are the designated next
step** after it lands. The likely design — to be settled in its own ADR — is
**2PC across the per-tablet Raft groups** (the Spanner/Cockroach model), with
**HLC** transaction timestamps for ordering and MVCC snapshot reads; Accord
(ADR 0011) is the alternative, layered atop the Raft groups as the durable store.
Until then, a transaction spanning tablets has no atomicity guarantee in this
mode.

## Consequences

**Enabled:**

- A strongly-consistent, linearizable per-tablet KV mode, durable on the existing
  LSM, fully sim-testable — the CP counterpart to the AP data plane.
- Reuse of the proven `RaftCore` (now generic) and the chunked-`InstallSnapshot`
  machinery; the control plane's placement + failure detection drive
  reconfiguration with minimal new policy.

**Costs and risks knowingly accepted:**

- **Apply-path refactor**: moving KV apply into the async driver (effects model)
  is a real change to how committed entries become visible; it must preserve the
  durable-before-visible invariant.
- **Membership change is new, safety-critical consensus code** in `RaftCore`.
  Single-server change reduces the risk surface vs joint consensus, but it must be
  sim-verified under partitions and crashes before it is trusted.
- **Tablet split is intricate** (dividing a live group's range + state, bootstrapping
  a new group atomically) and is the highest-risk, last-sequenced piece.
- **Operational load**: N tablets × RF replicas = many Raft groups, each with its
  own timers/heartbeats and `Env` id (the cost AP mode avoids).
- **Availability trade is inherent**: a tablet is write-unavailable during election
  / quorum loss (vs the AP plane's sloppy-quorum availability) — the point of the
  mode, not a defect.
- **Leases remain a documented hazard, not a feature** (§3): no lease reads without
  the `SimEnv` clock-injection model and a dedicated safety analysis.
- **No cross-tablet atomicity** until the follow-up ADR.

### Implemented now (Stages A + B) — a usable single-tablet KV data plane

The `animus-raftdata` crate provides a working, fault-tolerant, **linearizable
single-tablet KV** store today: `RaftKvNode<E, S>` runs one tablet's Raft group
over `Env`, backed by any `StorageEngine`. It serves `put`/`delete` (replicated +
durable once committed), **linearizable `linearizable_get`** (ReadIndex — a
read-barrier quorum probe, no log entry, no wall clock; a deposed leader returns
`None`, never stale), survives leader kills + rejoin, and **compacts** its WAL
(snapshotting the engine image) so a lagging/restarted follower catches up via a
streaming `InstallSnapshot`. All sim-tested (`tests/single_tablet.rs`,
`read_index.rs`, `snapshot_catchup.rs`) and reproducible from a seed. It reuses
the generic `RaftCore<C, S>` with a `DRIVER_APPLIED` KV state machine, leaving the
control plane unchanged. **Not yet:** dynamic membership (Stage C), tablet split
(Stage D), per-table mode selection / wiring into `animusd`, cross-tablet txns.

**Implementation sequencing (each a green-keeping increment):**

- **Stage A — apply + snapshot abstraction.** ✅ Done (A.1 externalized apply, A.2
  compaction + streaming `InstallSnapshot`). Proven on a single-tablet `SimEnv`
  group.
- **Stage B — the data plane path.** ✅ Done (B.1 `RaftKvNode` driver + write path,
  B.2 ReadIndex reads). End-to-end single-tablet linearizable KV, sim-tested with
  faults (leader kill + rejoin, deposed-leader-no-stale-read).
- **Stage C — reconfigure on failure.** ✅ Done. Single-server Raft membership
  change in `RaftCore` (config-in-log, carried through snapshots + `InstallSnapshot`),
  `change_membership` (single-server delta, one-in-flight, no leader self-removal,
  removed node stops campaigning). `tests/membership.rs` (add/remove, reconfigure
  off a crashed node, rejections, reproducibility). The **automatic trigger** is
  now wired (SimEnv, per the maintainer's decision): `RaftKvNode::reconfigure_step`
  takes one single-server step toward a desired voter set, and
  `spawn_reconfigure_loop` drives it from an **epoch-driven pull** — each group
  leader polls the control plane's replicated `Metadata.tablets[t].replicas` and
  reconfigures itself, no new control→data command. `tests/reconfigure_trigger.rs`
  proves the full cascade under one `SimEnv` (control plane + heartbeats + a Raft KV
  group + a spare): a crash → detector `Down` → reconciler `CasTabletReplicas` →
  the surviving leader removes the dead node and adds the same-zone spare, which
  catches up and the group keeps serving (seed sweep + reproducibility).
  *Remaining:* the `ProdEnv`/`animusd` production assembly (hosting per-tablet
  groups + leader-reporting for client routing) — see Stage-D-style integration
  plumbing below.
- **Stage D — tablet split.** ✅ Done. A committed `Split { at }` agrees the point;
  each replica tombstones the handed-off range `[at, ∞)`; that range seeds a new
  independent group (`range_snapshot` → `start_seeded`). `tests/split.rs`.
  **In-band new-group creation is now done** (the deferred `Env`-seam extension):
  a new **`Coresident` sub-trait** (`fn sibling(&self, id) -> Self`, implemented for
  `SimEnv`) lets a replica mint a co-resident inbox at runtime; the driver gained an
  optional **split hook** (`start_with_split_hook` + `in_band_split_hook`) so on
  apply each original replica mints `sibling(my_new_id)` and starts its own
  new-tablet replica there, seeded with the handed-off range — the new group forms
  entirely from the apply path with no external handoff. `Coresident` is a *separate*
  trait (not part of `Env`), bound only on the split path, so `ProdEnv` and every
  other `E: Env` are untouched; the external-handoff `split.rs` is unchanged
  (hook = `None`). `tests/split_in_band.rs`. *Remaining:* recovery-idempotency (the
  hook fires on every apply, so a `Split` re-applied after a crash would mint twice)
  and control-plane-driven new-id allocation — part of the `ProdEnv`/`animusd`
  production assembly.
- **`RaftPerTablet` Elle corpus** (ADR 0014/0016 step 4). ✅ Done. A self-contained
  linearizability corpus for this plane in `animus-test`
  (`tests/raftkv_linearizable.rs`): a single-key list-append workload over one Raft
  group, recorded as an Elle `History` and checked with the proven
  `check_cycles`/`check_durability`/`check_convergence`. It is *not* a `Topology`
  variant of the Accord corpus — the leaderful plane is **single-tablet,
  non-transactional KV**, so it cannot reuse the multi-key transactional Accord
  workload; instead it reuses the checkers + `Recorder` model. Serializability is a
  **sound** check here (the plane is the serialization authority — a forked/stale
  read shows as a cycle) and is asserted on a frozen, name-seeded scenario set
  (baselines + leader-kill/follower-kill/partition-leader/lossy × early/mid/late ×
  3- and 5-replica), with a depth knob (`ANIMUS_RAFTKV_SEEDS`, default 1; held at
  depth 20 / 360 scenarios). Convergence + durability use the same
  converged-or-timeout poll as the Accord runner.
- **Production assembly — Stage 3a (`animusd`).** ✅ Done. The leaderful CP plane
  now runs in the assembled node over `ProdEnv`: per-table replication mode lives
  in the replicated schema catalog (`ReplicationMode` + `MetaCommand::SetTableMode`,
  `animus-control`), and `animusd` hosts a statically-placed per-tablet Raft group
  on a 4th internal `raftkv` role (id `300+i`, its own listener/dir), routing a
  CP-mode table's client reads/writes to the group leader (`ClientCtx::cp_put`/
  `cp_get` via the per-cluster `ClusterEdgeState` group registry). `tests/cp_plane.rs`
  drives it over real TCP (CP write/read round-trip across nodes; AP plane
  untouched). **Stage 3b** (remaining integration plumbing): dynamic CP
  placement/split/reconfigure over `ProdEnv` (the `ProdEnv` side of `Coresident`
  via a pre-bound listener pool + control-plane address distribution), cross-process
  CP client routing, and per-CP-group failure detection.
- **Next ADR — cross-tablet transactions** (2PC over the groups + HLC; or Accord
  atop them).

This ADR builds on ADR 0016 (the pluggable-replication decision) and ADR 0009
(the in-house Raft it extends); the control plane (ADR 0001) remains the metadata
authority and is unchanged.
