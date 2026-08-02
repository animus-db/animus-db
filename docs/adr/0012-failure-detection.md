# ADR 0012 — Heartbeat-based failure detection in the control plane

- **Status:** Accepted
- **Date:** 2026-08-01

## Context

The control plane (ADR 0001, 0009) owns cluster membership: each `Member` has a
`NodeStatus` of `Joining`/`Active`/`Leaving`/`Down`. Placement
auto-reconciliation (ADR 0005) already **reacts** to `Down` — the leader's
`reconcile_loop` recomputes a tablet's replica set from the `Active` members and
commits a `CasTabletReplicas`, moving placement off a `Down` member while
preserving residency + spread.

But nothing **produces** `Down` automatically. Until now a member was marked
`Down` only by an operator proposing `UpsertMember{status: Down}` manually. So
the autonomy we built reacts to failures only once a human has already declared
one. A crashed or partitioned member is not detected, its tablets are not moved,
and the cluster does not self-heal.

The missing piece is automatic **detection**: observe member liveness and drive
the `Active`/`Down` status transitions that the rest of the system already knows
how to act on. As with everything in this project, it must be deterministic
(ADR 0003): no wall clock, no unseeded randomness, reproducible from a seed under
`SimEnv`.

## Decision

We will add a **heartbeat-based failure detector** to the control plane, keeping
the *decision* pure (like the placement engine, ADR 0005) and the *timing/IO* in
the `Env`-driven node (like the Raft split, ADR 0009).

- **Heartbeats.** A member emits a periodic `RaftMsg::Heartbeat { node }` to every
  control node on an `Env` timer (`heartbeat_loop` → `env.sleep(HEARTBEAT_INTERVAL)`,
  never the wall clock). The heartbeat rides the existing `RaftMsg` wire enum (and
  thus the single per-node inbox) but carries **no Raft term** and is **not
  consensus traffic**: the node driver intercepts it in its `recv` arm and feeds
  it to the detector, never handing it to the `RaftCore`.

- **Detector (pure).** `FailureDetector` records each member's last-heartbeat
  instant and, given `now` and a fixed `timeout`, classifies each tracked member
  *alive* (a heartbeat within `timeout`) or *dead*. It is a plain interval+timeout
  detector: no clock, no randomness, iteration over a `BTreeMap` — a pure function
  of `(recorded heartbeats, now, timeout)`. A φ-accrual variant was considered and
  deliberately not adopted: it adds estimation state for no benefit at this slice
  and a naive implementation risks nondeterminism.

- **Driver (timing/IO).** `RaftNode`'s `detect_loop` ticks on an `Env` timer and,
  **when leader**, computes the `UpsertMember{status}` transitions needed to bring
  each tracked member's replicated status in line with its liveness verdict
  (`liveness_transitions`, a pure helper), then proposes them through Raft. It is
  **idempotent**: a member already at the status its liveness implies yields no
  proposal, so a steady cluster produces no churn. The `timeout` (several
  heartbeat intervals) absorbs a single delayed/dropped heartbeat, so a healthy
  member does not flap. Only members the detector tracks (have heartbeated at
  least once) are judged, so a freshly-registered member is never marked `Down`
  before its first heartbeat; `Joining`/`Leaving` are left alone (operator-driven
  lifecycle). The transition preserves the member's labels, so liveness never
  disturbs residency/spread.

- **Cascade.** Because a committed `Down` is exactly what the placement
  reconciler reacts to (ADR 0005), a detected failure cascades automatically into
  tablet re-placement; a detected recovery (`Down` → `Active`) restores the member
  as a placement candidate. No new wiring between the two is needed.

This keeps all consensus logic in the sync `RaftCore` (the detector and its
transitions live outside it; the only `RaftMsg` change is an intercepted,
term-less heartbeat) and does not relax any Raft safety rule — transitions are
ordinary `UpsertMember` log entries committed under the current term.

## Consequences

- The cluster now **self-heals** on real failures: a crashed/partitioned member
  is detected, marked `Down`, and its tablets re-placed with no operator — and it
  returns to `Active` (and to placement) when it recovers. This is proven
  end-to-end under fault injection in
  `custos-control/tests/failure_detection.rs` (a member crashes, the leader
  commits `Down` on every control node, placement reconciles off it preserving
  residency + spread, then the member restarts and returns to `Active`),
  reproducible from a seed.
- The detector decision is pure and unit-tested in isolation
  (`detector.rs::tests`); the leader's verdict is a deterministic function of
  `Env`-supplied time.
- Detector state is **per-node volatile** and not replicated: only the
  *transitions* it drives are (as `UpsertMember` log entries). A new leader starts
  with a cold detector and re-learns liveness over one `timeout` window as
  heartbeats keep arriving on every control node. This is acceptable for this
  slice; persisting/replicating raw liveness would buy little.
- **Now wired in production.** `custosd`'s node assembly spawns `heartbeat_loop`
  on every data node (over its data-role `ProdEnv`) and registers the **data
  nodes** as the cluster's `Active` members, so the leader's `detect_loop` tracks
  the nodes that actually hold data; a killed node's silence is detected, marked
  `Down`, and cascades into placement re-reconciliation onto a spare. Proven live
  over real `ProdEnv`/TCP in `custosd/tests/self_heal.rs` (the deterministic sim
  coverage in `custos-control/tests/failure_detection.rs` remains the source of
  truth).
- **Deferred:** tuning the timeout adaptively (or a φ-accrual detector) under real
  network jitter; a grace period after a leader change before acting on a cold
  detector; and heartbeating only the leader (vs. all control nodes) once leader
  discovery is cheap.
