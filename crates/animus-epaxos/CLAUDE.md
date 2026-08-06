# CLAUDE.md — animus-epaxos

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

A from-scratch **EPaxos** (Moraru et al., SOSP 2013) leaderless consensus core
(ADR 0025) — an **isolated exploration**, deliberately **not wired into any data
path**. It exists to build, understand, and correctness-test EPaxos against the
same deterministic simulator (ADR 0003) that hardened the Accord slice
(`animus-consensus`, ADR 0011), and to compare the two protocols directly. Nothing
in v1 depends on it; it depends only on `animus-env` (+ `serde`), so it cannot
destabilize the shipping planes.

Same architectural shape as `animus-consensus`: a **synchronous, I/O-free**
`EPaxosCore` state machine driven by a thin `EPaxosNode<E>` over the `Env` seam.
All protocol logic is in the core; the driver only does I/O.

## How EPaxos differs from Accord (why this crate exists)

- **Order primitive.** EPaxos is *instance-space native*: a command lives in an
  `InstanceId { replica, slot }` its command leader owns, and order is a
  **dependency graph** (`deps`) plus a per-command sequence number (`seq`). There
  is **no timestamp** — contrast `animus-consensus`'s Lamport `Timestamp`. This is
  the crux of the user's interest: EPaxos has no timestamp reconciliation at all.
- **Fast quorum.** `f + ⌊(f+1)/2⌋` (`core::EPaxosCore::fast_quorum`), smaller than
  Accord's simplified `N-1` — but only *fault-recoverable* once `Prepare` recovery
  lands. That trade (smaller quorum ↔ harder recovery) is the whole EPaxos-vs-Accord
  question.
- **Execution.** Order is recovered by a Tarjan **SCC** pass over the committed
  dependency graph, ordered within a cycle by `seq` (then instance id) — versus
  Accord executing in a total timestamp order with no SCCs. (Executor deferred.)

## Entry points

- `instance.rs` — `InstanceId { replica, slot }`, totally ordered; the command
  identity and dependency-graph node.
- `core.rs` — `EPaxosCore`: the sync state machine. `submit(keys)` starts a command
  this node leads; `handle(from, msg)` processes an inbound message; both return
  `Vec<Out>` and never touch `Env`. Holds the **replica view** (`instances`) and
  the **coordinator view** (`coordinating`), plus `decisions` (leader-side
  observability) and a `pending` buffer of `WalRecord`s. `Decision { instance, seq,
  deps, fast_path }` and `Status` (NotSeen<PreAccepted<Accepted<Committed<Executed)
  are the public observability types. `drain_persist` hands records to the driver;
  `recovered` rebuilds the replica view.
- `message.rs` — `EPaxosMsg` (PreAccept/PreAcceptOk/Accept/AcceptOk/Commit),
  `serde_json` over the `Network`'s `Vec<u8>` payloads. Recovery messages
  (`Prepare`/`PrepareOk`) are deferred.
- `persist.rs` — `WalRecord` (PreAccepted/Accepted/Committed) + `PersistedState`
  (replay/encode/decode), mirroring `animus-consensus::persist`. Only replica
  facts are made durable so far.
- `node.rs` — `EPaxosNode<E>`: the `Env` driver. `submit` + a `recv` loop;
  `persist_then_ship` fsyncs the WAL **before** shipping dependent messages
  (durable-before-visible); `drive` recovers from the WAL on startup.

## What's non-obvious

- **All protocol logic is in the sync `EPaxosCore`**; the driver only does I/O.
  Keep it that way — don't reach for `Env` inside the core. Like `AccordCore` and
  unlike `RaftCore`, the core takes **no clock and no randomness**: order is
  graph + seq, so determinism rests purely on `BTreeMap`/`BTreeSet` iteration
  order. Don't introduce a `HashMap`/`HashSet` (lint-enforced).
- **A node is both leader and replica.** `submit` seeds the coordinator's own
  `PreAcceptOk` so it counts toward the fast quorum (mirrors `AccordCore::submit`).
- **Fast path fires iff every fast-quorum reply reports identical `(seq, deps)`**
  equal to the leader's proposal; otherwise the slow path adopts the **max `seq` +
  union `deps`** across replies and runs `Accept`. See `advance_coordinator` — it
  computes the next action while holding only the `coordinating` borrow, then
  releases it before calling the `&mut self` commit/accept helpers (the borrow
  dance you must keep).
- **A replica merges its own conflicts into a PreAccept** (`replica_pre_accept`):
  the reply's `deps` = leader deps ∪ this replica's conflicting instances, `seq` =
  max. This is why two conflicting commands always end up with a **dependency
  edge** between them — any two quorums intersect, and the intersecting replica saw
  both, so it reports the earlier one as a dep of the later. The acceptance test
  asserts exactly this (`assert_consistent_and_dependent`).
- **`Accept`/`Commit` are authoritative**: a replica adopts the coordinator's
  `(seq, deps)` verbatim (not a merge), so every replica agrees on the committed
  attributes. Agreement is what the tests check (execution order is deferred).
- **Durable-before-visible** (mirrors the other planes): the driver fsyncs the WAL
  before shipping the messages that depend on it, and no lock is held across an
  `.await` (drain under the lock, drop it, then do I/O in a spawned task).
- **`InstanceId` is a map key.** It's a plain struct field / set element on the
  wire and WAL (fine), and `PersistedState` keys a `BTreeMap` on it *without*
  serializing that map — because `serde_json` cannot serialize a struct-keyed map
  (the same gotcha `animus-consensus` hit with `Timestamp`). If you later add a
  `Snapshot` record carrying the instance map, ride it as a `Vec<(InstanceId, _)>`,
  never a `BTreeMap`.

## Deferred (the "build onto" surface — see ADR 0025)

Each mirrors a proven piece in `animus-consensus`:

- **SCC executor** — Tarjan over the committed dependency graph, execute SCCs in
  reverse-topological order, within a cycle by `(seq, instance)`. Agree order →
  run against a `StorageEngine`. This is the piece that makes EPaxos EPaxos; it's
  where the "unbounded dependency chain" behavior shows up. Add storage to
  `EPaxosNode` (generic `S: StorageEngine`, `MemoryEngine` default) when it lands.
- **`Prepare` recovery** — take over a dead command leader. EPaxos's hardest part
  (fast-path witness reasoning); until it exists a dead leader **strands** its
  instance and the small fast quorum is not fault-recoverable. Write it against a
  TLA+ spec — this is where EPaxos has historically shipped bugs.
- **Message retry, failure detection, WAL snapshotting, read-only commands,
  arbitrary write values** — all exist in `animus-consensus`; each slots in at the
  sync-core boundary.

## Tests

`cargo test -p animus-epaxos` — `tests/epaxos_commit.rs` (SimEnv, 3 nodes): an
uncontended command commits on the fast path on every replica; two conflicting
commands agree on attributes on every replica and have a dependency edge (incl. a
64-seed sweep); disjoint commands are independent; trace reproducibility. Drive
with `run_for` (the pattern the project uses for protocols that will grow perpetual
timers — this skeleton has none yet).

**If you learn a generalizable lesson while working here, record it in the root
`CLAUDE.md` Engineering-practices section (and this guide) before you finish.**
