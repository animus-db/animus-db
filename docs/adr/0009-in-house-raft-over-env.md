# ADR 0009 — In-house Raft over the `Env` seam (deviation from openraft)

- **Status:** Accepted
- **Date:** 2026-08-01

## Context

The bootstrap brief suggests `openraft` (or `raft-rs`) for control-plane
consensus. Independently, ADR 0003 makes determinism non-negotiable: *all*
nondeterminism — time, task scheduling, network, randomness — must flow through
the `Env` seam so a run is byte-reproducible from a seed, and the M3 acceptance
criteria require exactly that (leader election and leader-kill survival,
replayable from a seed, under `SimEnv`).

`openraft` drives its own time (timers), spawns its own `tokio` tasks, and owns
its own RPC scheduling. None of that goes through our `Env`, so it cannot be
driven by the single-threaded, virtual-clock `SimEnv`; its election timeouts and
task interleavings would be real-time and nondeterministic. Making it
deterministic would mean forking it or adopting `madsim` wholesale now — a much
larger commitment than M3 warrants.

## Decision

For the M3 control-plane skeleton we will implement a **small, self-contained
Raft** (leader election + log replication + commit + apply) as a *synchronous*
`RaftCore` state machine that runs entirely over `Env`: a thin per-node driver
owns the `Env` and feeds the core timer ticks and decoded messages, and the core
returns outbound messages and applies committed entries. All randomness
(election-timeout jitter) and time come from `Env`. This keeps the control plane
fully deterministic and replayable.

The core implements the safety-critical Raft rules (term/vote handling, log
up-to-dateness for votes, `AppendEntries` consistency check with conflict
truncation, commit only of current-term entries via majority `matchIndex`).

## Consequences

- The control plane is deterministic and testable under simulation today, which
  is the whole point of the project.
- We own and must maintain a Raft implementation. It is deliberately minimal.
  Durability is implemented (follow-up to M3): the core emits a write-ahead log
  of hard-state/log/snapshot records that the driver `fsync`s before acting, and
  recovers from on startup (see `persist.rs`). The log is offset by a
  state-machine **snapshot**; on a threshold the node snapshots its applied state
  and **truncates** the covered log prefix, and the WAL is rewritten to its live
  image (snapshot + hard state + log tail) via an atomic `Disk::replace`
  (temp-file + rename in production) — so both the log and the WAL are bounded by
  the live tail. A follower that has fallen behind the leader's compacted prefix
  is caught up by an `InstallSnapshot` RPC. Recovery restores the snapshot and
  re-applies the tail, so each committed command lands exactly once relative to
  the snapshot base (no double-applied CAS). The full WAL write/compact/recover
  flow is diagrammed in [`docs/wal.md`](../wal.md). **Still deferred:** a full
  in-simulation process *restart-and-rejoin* test (the simulator cannot yet stop
  and replace a node's tasks; recovery is validated at the `RaftCore` level), and
  chunked snapshot transfer (the snapshot ships in a single message).
- If we later need the maturity of `openraft`, the `Env`-driven boundary (a sync
  core + an I/O driver) is a clean place to swap implementations, and a `madsim`
  backend behind `Env` (ADR 0003) would let a third-party Raft run
  deterministically.
- This ADR supersedes the brief's dependency suggestion for the control plane;
  ADR 0001 (two-plane architecture) is otherwise unchanged.
