# CLAUDE.md — custos-consensus

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

Accord-style **leaderless transaction consensus** (ADR 0011) — a first, minimal
slice. Each transaction gets a unique, totally-ordered timestamp; a coordinator
agrees with the replicas on an *execution* timestamp and a *dependency* set via
PreAccept → (fast path) Commit, or PreAccept → Accept → Commit (slow path). No
leader. This is the layer that will eventually give the AP data plane multi-key
atomicity and a strict serialization order.

## Entry points

- `timestamp.rs` — `Timestamp { logical, node }` (totally ordered, unique) and
  the per-node `LogicalClock` (`witness` to advance past a peer; `mint` for a
  strictly-greater fresh stamp).
- `core.rs` — `AccordCore`: a **synchronous, I/O-free** state machine mirroring
  `custos-control`'s `RaftCore`. `submit(keys)` starts a transaction this node
  coordinates; `handle(from, msg)` processes an inbound message. Both return
  `Vec<Out>` and never touch `Env`. Holds the replica view (`txns`) and the
  coordinator view (`coordinating`) plus the reached `decisions`.
- `message.rs` — `AccordMsg` (PreAccept/PreAcceptOk/Accept/AcceptOk/Commit),
  (de)serialized with `serde_json` over the `Network`'s `Vec<u8>` payloads.
- `node.rs` — `AccordNode<E>`: the thin `Env` driver (a plain `recv` loop; no
  timers in this slice).

## What's non-obvious

- **All protocol logic is in the sync `AccordCore`**; the driver only does I/O.
  Keep it that way — don't reach for `Env` inside the core.
- Unlike `RaftCore`, the core takes **no `now`/`entropy`**: timestamps are
  logical (not wall-clock) and there are no timers, so determinism rests purely
  on logical clocks + `BTreeMap`/`BTreeSet` iteration order. Don't introduce a
  `HashMap`/`HashSet` (lint-enforced) or any time source.
- A node is **both coordinator and replica**: `submit` applies the transaction
  to the node's own replica state and seeds the coordinator's reply set with the
  node's own PreAcceptOk, so the coordinator counts itself toward the quorum.
- **Fast-path gating is the subtle part** (`advance_from_pre_accept`): the fast
  path needs a fast quorum agreeing on `t0` *and* reporting identical deps. Do
  not escalate to the slow path while the fast path is still reachable — but DO
  escalate once every replica has answered and the fast path didn't fire (the
  all-agree-on-`t0`-but-deps-differ case), or it stalls forever. That bug bit
  during development; the multi-seed test guards it.
- The **fast quorum is a placeholder `ceil(3N/4)`**, not Accord's exact bound.
  See ADR 0011.
- Conflict = intersecting key sets. `Key` is a bare `u64` for now (the real
  system keys by partition/range).

## Deferred (see ADR 0011)

Execution/Apply, the full dependency wait-graph, durability/recovery (no WAL —
contrast `RaftCore`), coordinator failover, timeouts/retries/livelock handling,
sharding/placement (one global replica set), and wiring the Elle cycle checker
(`custos-test`). The sync-core boundary is where each of these slots in later.

## Tests

`cargo test -p custos-consensus` — unit tests on the timestamp/clock, plus
`tests/accord_commit.rs` under `SimEnv`: single-transaction fast-path commit on
all replicas, two conflicting transactions committing in a consistent timestamp
order (including a 64-seed sweep), disjoint-transaction independence, and
trace reproducibility from a seed. Use `run_for` (the driver has no perpetual
timers today, but follow the house convention).
