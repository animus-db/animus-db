# ADR 0025 — EPaxos leaderless consensus: an isolated exploration core

Status: Accepted (exploration; not on any data path)

## Context

v1 is CP-only (ADR 0019): client traffic routes through the leaderful per-tablet
Raft data plane (ADR 0016/0017), under a Raft control plane (ADR 0009). The
project also carries a mature **Accord** slice (`animus-consensus`, ADR 0011) — a
leaderless, dependency-tracking consensus in the **EPaxos lineage** — which is
sim-hardened but not wired into the v1 client path.

We want to reduce the per-tablet-leader bottleneck by moving toward **leaderless**
consensus, and to build **EPaxos** (Moraru et al., SOSP 2013) specifically — both
to evaluate it against Accord on this codebase's own deterministic harness and for
the understanding that building it yields. EPaxos and Accord are close cousins:
Accord is essentially EPaxos with a Lamport **timestamp** added, which replaces
EPaxos's dependency-graph SCC execution with a total-order execution and
simplifies recovery. The interesting question is whether EPaxos's smaller fast
quorum is worth its harder recovery here.

Ripping out two working Raft planes to build an unproven protocol would be
reckless. The safe path is to build EPaxos **isolated first**, prove it against the
same fault-injecting simulator and (later) the Elle corpus that hardened Accord,
and only then consider wiring it into the data plane.

## Decision

Add a new crate **`animus-epaxos`** implementing EPaxos in the same shape as
`animus-consensus`: a synchronous, I/O-free `EPaxosCore` state machine driven by a
thin `EPaxosNode` over the `Env` seam (ADR 0003), so it runs deterministically
under `SimEnv`. It is **not wired into any data path** and depends on nothing but
`animus-env` (+ `serde`); it cannot destabilize the shipping planes.

The **first milestone (this ADR)** is the steady-state agreement:

- Instance-space identity `InstanceId { replica, slot }` — a command lives in a
  slot its command leader owns; there is **no timestamp** anywhere.
- `PreAccept → Commit` (fast path) / `PreAccept → Accept → Commit` (slow path),
  agreeing the two command attributes `deps` (interfering instances) and `seq`
  (one above the max `seq` of deps — the execution cycle-breaker).
- Fast-path quorum `f + ⌊(f+1)/2⌋` (the EPaxos bound), floored at a majority.
- Durable-before-visible: a WAL fsync before shipping dependent messages, with
  replica-view recovery on restart.

Deliberately **deferred** (the surface to build onto, each mirroring an existing
Accord piece): the Tarjan **SCC executor** (agree order → run against a
`StorageEngine`), the **`Prepare` recovery** sub-protocol (take over a dead
command leader — EPaxos's hardest part, and what makes the small fast quorum
fault-recoverable), message retry, failure detection, WAL snapshotting, read-only
commands, and arbitrary write values.

## Consequences

- We can build and correctness-test EPaxos incrementally against the deterministic
  simulator with zero risk to v1. Each deferred piece has a proven analogue in
  `animus-consensus` to reference.
- Until `Prepare` recovery lands, a dead command leader **strands** its instance
  and the small fast quorum is not yet fault-recoverable — so this core is not a
  candidate for any data path yet. The acceptance tests are therefore no-fault
  and assert **agreement on attributes**, not executed state or failover.
- Two leaderless cores now coexist (`animus-consensus` Accord, `animus-epaxos`
  EPaxos). This is intentional for the evaluation; a future ADR will decide which
  (if either) becomes the data-plane consensus, and Raft removal — if it happens —
  will be gated on that core passing the Elle corpus under fault injection, not on
  this exploration.
- Keeps the deviation posture of ADR 0009: an in-house protocol over `Env` so the
  simulator can drive it, rather than an external library.
