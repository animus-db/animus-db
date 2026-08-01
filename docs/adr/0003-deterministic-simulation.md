# ADR 0003 — Deterministic simulation testing and the `Env` seam

- **Status:** Accepted
- **Date:** 2026-08-01

## Context

Distributed-systems bugs hide in rare interleavings of message reordering,
partitions, crashes, and clock skew. Such interleavings are nearly impossible to
reproduce with real networks and wall clocks, so a bug seen once in CI may never
be seen again. The state of the art (FoundationDB, TigerBeetle) is to make the
*entire* system run on top of a single deterministic substrate so that any run
is byte-for-byte reproducible from a seed.

## Decision

All nondeterminism flows through a single `Env` seam — a set of traits
(`Clock`, `Rng`, `Network`, `Disk`, `Spawner`) combined into an `Env`
supertrait. Components are **generic over `E: Env`** (monomorphized, not `dyn`),
so the same code runs in production and under simulation with no branches.

- `ProdEnv` provides real time, `tokio` task spawning, TCP, real `fsync`, and
  OS randomness.
- `SimEnv` (crate `custos-sim`) provides a virtual clock, a seeded ChaCha RNG, an
  in-memory network with controllable delay/drop/reorder/partition, a fake disk
  that distinguishes synced from un-synced bytes (a "crash" drops un-synced
  bytes), and a cooperative single-threaded run-queue.

System code must never call `std::time::*`, spawn raw tasks, touch real
sockets/disk, use unseeded RNG, or iterate a `HashMap`/`HashSet` (use
`BTreeMap`/`BTreeSet`). This is enforced in review and by `clippy.toml`. A
failing simulation run prints its seed for one-command replay.

## Consequences

- Every distributed behavior gets a reproducible, fault-injecting test, and
  shrinking a failure to a minimal seed becomes possible.
- There is an upfront cost: the `Env` seam must be designed carefully and all
  subsystems must be written against it from day one. Retrofitting is expensive,
  so we pay this cost first (milestone M1).
- We forgo some convenient APIs (wall clock, `tokio::spawn`, `HashMap` iteration)
  in system code.
- The seam is designed so a future move to `madsim` is a drop-in replacement of
  the simulation backend, not a rewrite of the system code.
