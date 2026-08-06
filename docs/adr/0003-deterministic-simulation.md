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
- `SimEnv` (crate `animus-sim`) provides a virtual clock, a seeded ChaCha RNG, an
  in-memory network with controllable delay/drop/reorder/partition, a fake disk
  that distinguishes synced from un-synced bytes (a "crash" drops un-synced
  bytes), and a cooperative single-threaded run-queue.

### Known fidelity limits (audit, 2026-08-06)

The seam is honoured throughout, but both envs are weaker than the paragraph
above reads, and the gaps are exactly where prod-only bugs have already hidden:

- **`ProdEnv` durability:** `sync` is a file-fd `sync_all` only — neither
  `append`'s file *creation* nor `replace`'s *rename* fsyncs the parent
  **directory**, so a just-created WAL segment or a completed manifest swap can
  be lost by a power crash even after `sync`/`replace` returned. **Fixed in
  PR #27** (directory-fsync chain on first sync / after rename), which also
  root-caused a worse latent bug the audit missed: `append` dropped its
  `tokio::fs::File` without `flush().await`, so a write could still sit in
  tokio's user-space buffer when a later `sync` (a different fd) fsynced —
  and two sequential appends via separate handles could land **inverted** on
  disk (the long-standing `lsm_concurrent` flake; independently found and
  fixed in PRs #26 and #27).
- **`SimEnv` disk faults:** the sim disk never returns an error, never leaves a
  *partial* (torn) tail on crash — it drops the whole un-synced buffer
  atomically — and cannot corrupt a byte. Storage error-handling, torn-record
  recovery, and checksum-mismatch paths are therefore unreachable under
  simulation. **Closed in PR #24**: opt-in, seed-driven `DiskConfig`
  (error injection, torn-tail-on-crash, corruption; default-off,
  byte-identical traces) — whose first run found two real WAL data-loss bugs
  (see ADR 0008), proving the gap was load-bearing.
- **Network:** reordering is emergent from per-message jitter (there is no
  explicit reorder/duplication knob), and all nodes share **one** virtual
  clock — per-node skew/drift, listed in the Context as a target bug class, is
  not yet modeled.
- **Threading:** the sim is single-threaded and cooperative, so it proves logic
  and ordering, never real-thread liveness — any concurrency primitive needs a
  timeout-guarded `multi_thread` test over `ProdEnv` (see the root `CLAUDE.md`
  practice entry; found via the WAL group-commit deadlock).

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
