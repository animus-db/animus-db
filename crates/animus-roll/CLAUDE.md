# CLAUDE.md — animus-roll

The rolling-upgrade **state machine** (ADR 0073 Phase 3, workstream P3-C): a pure,
I/O-free, `Env`-free, async-free crate. `decide(&Config, &Observation) -> Action` returns the
one next step of a rolling upgrade; it keeps **no state**, so a restarted driver (or a second
one) decides identically from the same observation (D5/D7 "derived, never stored").

**Why a crate of its own.** `animus-cli` depends on `animusd` (heavy) and `animus-operator`
on no workspace crate at all, so neither side could host a module the other reuses without a
dependency the other should not take. This crate has one dependency (`serde_json`, only for
the `json` adapters) and both consumers can depend on it. Production consumers: P3-B (`animus
cluster roll`, a supervisor that restarts nothing itself) and P3-D (the operator's
partition driver). `animusd` uses it as a dev-dependency (`sim_cluster_roll_orchestrator`).

## Shape

- `Observation`: `era_active`, `active`, **`goal`** (the cluster version this roll finalizes
  to, fixed by the caller for the whole roll: recomputing it from `active` makes every node
  look old the moment Finalize lands), `can_finalize` + `finalize_blockers`, per node `NodeObs
  { id, role, status, platform, reported_new, health }`, the control leader, and the caller's
  clocks `in_flight_for` / `settled_for`. `Platform::{Old, Restarting, New}` is **the
  platform's fact** (systemd / pod revision / test harness); `Health::{Ok, NotOk,
  Unavailable, Unreachable}` is one node's `GET /admin/roll-health`.
- `Action`: `Restart`, `TransferControlLeadership`, `Wait` (a node in flight), `Blocked(Block)`,
  `AwaitEra` (Phase 1 -> B2: no era yet), `Soak`, `ReadyToFinalize` (manual), `Finalize` (opt-in
  auto), `Complete`.
- `json::observation(view, &Inputs)` / `json::parse_health` build an observation from the admin
  bodies; tolerant of missing fields (an old node).

## Rules the tests pin (change one and a named unit test fails)

- Order: data-only first, control voters next, the control leader last (ties by id); mirrors
  `animusd`'s `roll.remaining` (`sim_cluster_roll_orchestrator` asserts the parity).
- One node below the gate: any `Restarting` node, or a `New` node that is not yet `Active`,
  reporting (era) and `Health::Ok`, holds every restart (`Wait`, or `Blocked(NodeStalled)` past
  `Config::stall_after`).
- The gate: every member `Active`; every other node's roll-health `Ok` (`NotOk` and `Unreachable`
  block; `Unavailable` is tolerated only from a node still on the old binary, and only until a
  node is on the new one: then at least one verdict must exist).
- A control leader is never restarted: `TransferControlLeadership` to an Active control node
  (preferring one already on the new binary) first; tablet leaders are never moved.
- Finalize: never with a blocker, a false `can_finalize`, or an unhealthy verdict; `Manual`
  only ever *offers* it; `Auto { soak }` finalizes after `settled_for >= soak`.
- Timeouts are the caller's; this crate never reads a clock.

## Not here / limits

- It does not restart anything or call any endpoint; the caller executes the `Action`.
- A first roll over **Phase 1** binaries has no `cluster-version` endpoint on the old nodes at
  all; the caller must build the observation from `/admin/status` itself (the machine already
  handles `era_active = false`, platform-only completion and `AwaitEra`).
- The PDB `maxUnavailable >= 1` / cluster-shape floor (maintainer decision 6) is the operator's
  precondition, not the machine's.
