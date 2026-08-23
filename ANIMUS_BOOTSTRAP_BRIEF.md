# AnimusDB — Bootstrap Brief for Claude Code

**Historical document.** This is the founding brief written before the first
commit; it records the original scope and vision. Three load-bearing scope
changes since: v1 is **CP-only** (ADR 0019 deferred the leaderless AP data
plane described below, and its `animus-data` crate is deleted); v1 is
**DynamoDB-only** (ADR 0053 dropped the CQL wire adapter described below,
`animus-cql`); and AP's deferral became **permanent** (ADR 0019's 2026-08-23
amendment — with CQL gone no shipping wire can express a per-table replication
mode, so the `animus-consensus` Accord crate described below, its Elle corpus,
and the `ReplicationMode` seam are deleted too). Read `CLAUDE.md` and `docs/adr/README.md` for the current
architecture; treat this file as a record of intent, not current state.

You are bootstrapping a new open-source project, **AnimusDB**, and delivering its
first milestones. Read this whole brief before writing code. Work in small,
reviewable, vertical slices and keep everything testable under simulation from
the very first commit.

---

## 1. Mission

AnimusDB is a **masterless, linearly-scalable, open-source NoSQL database written
in Rust** (no JVM). It descends from the Dynamo lineage shared by Cassandra and
DynamoDB: a wide-column / key-value model over a partitioned, sorted *map-of-maps*
storage primitive. It pairs a **leaderless AP data plane** (tunable consistency)
with a small **strongly-consistent, Raft-backed transactional control plane**, shards
data into **tablets** (splittable, movable key ranges), and is **topology-aware** so
data can be pinned to regions/jurisdictions for per-group **data residency**.
Correctness is established by **deterministic simulation testing (DST)** from day one.

The long-term wedge is wire compatibility with **CQL (Cassandra)** and the
**DynamoDB API** on a common core — but those adapters are explicitly out of scope
for the first milestones.

---

## 2. Non-negotiable principles (these shape every decision)

1. **Determinism first.** All nondeterminism — time, randomness, network, disk,
   task scheduling — flows through a single `Env` seam. System code must never call
   `std::time::*`, `tokio::spawn`, real sockets, `std::fs`, or iterate a `HashMap`
   in logic. Use `BTreeMap`, seeded RNG, and `Env` for everything. This is the
   load-bearing constraint of the project; enforce it in review and lints.
2. **Two planes, never blurred.** The **control plane** is consistent (Raft) and
   owns cluster metadata. The **data plane** is leaderless/AP and serves reads and
   writes. A control-plane outage must NOT take down the data plane — data nodes
   keep serving on cached metadata; only topology changes block.
3. **Borrow storage, innovate on distribution.** Do not write a storage engine yet.
   Hide storage behind a trait and back it with an in-memory impl (and optionally
   RocksDB). The risk and differentiation live in the distributed layer.
4. **Vertical slices over horizontal layers.** Prefer a thin end-to-end path that a
   simulator can exercise over a "finished" subsystem in isolation.
5. **Every distributed behavior ships with a fault-injecting simulation test.**

---

## 3. Tech & conventions

- **Language:** Rust, stable toolchain, latest edition. Cargo **workspace**.
- **Async:** `tokio` in production; under simulation the `Env` controls the runtime
  (we may adopt `madsim` later — design so that's a drop-in, not a rewrite).
- **Quality gates:** `rustfmt`, `clippy -D warnings`, `cargo-deny` (licenses +
  advisories), `cargo test`. All green in CI before merge.
- **Proven dependencies (don't reinvent these):** `openraft` or `raft-rs` for
  control-plane consensus; `rust-rocksdb` or `fjall` for the optional real storage
  backend; `proptest` for property tests; `tracing` for structured logs; `serde`.
- **License:** **AGPL-3.0**, and require a **CLA** (DCO + CLA bot) on all
  contributions to preserve future licensing optionality.

---

## 4. Repository layout (Cargo workspace)

```
animusdb/
├── Cargo.toml                # workspace
├── LICENSE                   # AGPL-3.0
├── README.md
├── CONTRIBUTING.md           # build, test, CLA
├── CODE_OF_CONDUCT.md
├── rustfmt.toml
├── deny.toml
├── .github/workflows/ci.yml
├── docs/adr/                 # architecture decision records
└── crates/
    ├── animus-env            # Env traits + ProdEnv + SimEnv handle
    ├── animus-sim            # the deterministic simulator (clock, net, disk, scheduler, faults)
    ├── animus-storage        # StorageEngine trait + in-memory impl (+ rocksdb feature)
    ├── animus-control        # control-plane RSM: metadata model, Raft log, epochs, reconciler
    ├── animus-data           # leaderless data plane: quorum read/write, routing, fencing
    ├── animus-tablet         # tablet model (later: split/merge/placement)
    ├── animus-placement      # placement groups + topology labels + residency (later)
    ├── animus-consensus      # Accord-style transactional escalation (later)
    ├── animus-cql            # CQL adapter (later)
    ├── animus-dynamo         # DynamoDB adapter (later)
    ├── animus-test           # Elle-style history recorder + checker; shared test harness
    ├── animusd               # node server binary
    └── animus-cli            # operator CLI
```

---

## 5. Architecture Decision Records to scaffold (`docs/adr/`)

Create an ADR template and write short stubs (Context / Decision / Consequences) for:

- **0001** Masterless AP data plane + Raft control plane (two-plane architecture)
- **0002** Tablets as the unit of placement and migration
- **0003** Deterministic simulation testing and the `Env` seam
- **0004** Dynamo-lineage storage primitive (partitioned sorted map-of-maps)
- **0005** Placement groups + topology-aware data residency
- **0006** Dual CQL + DynamoDB adapters over a common core
- **0007** AGPL-3.0 + CLA
- **0008** Borrowed storage engine first (RocksDB / fjall), custom LSM deferred

---

## 6. First contributions (ordered — one milestone per PR)

### M0 — Repo scaffold
- Cargo workspace with the crate skeletons above (empty `lib.rs` + crate-level docs).
- LICENSE, README (mission + status badge), CONTRIBUTING (CLA + build/test),
  CODE_OF_CONDUCT, rustfmt/clippy/deny configs.
- GitHub Actions CI: fmt check, clippy `-D warnings`, build, test, cargo-deny.
- ADR template + stubs 0001–0008.
- **Acceptance:** `cargo build`, `cargo test`, `cargo clippy`, `cargo fmt --check`
  all pass in CI on a clean checkout.

### M1 — The `Env` seam (the foundational piece — do this carefully)
- `animus-env`: traits `Clock`, `Rng`, `Network`, `Disk`, `Spawner`, and an `Env`
  supertrait. Components are generic over `E: Env` (monomorphized, not `dyn`).
- `ProdEnv`: real clock, tokio spawn, TCP, `tokio::fs` + real fsync, `OsRng`.
- `animus-sim`: `SimEnv` over a single shared `SimState` —
  virtual clock, seeded `ChaCha` RNG, in-memory network (per-node inboxes,
  controllable delay/drop/reorder/partition), fake disk (tracks synced vs.
  un-synced bytes; a "crash" drops un-synced), cooperative run-queue.
  A `Simulator` loop advances virtual time, dispatches the earliest event, exposes
  fault-injection hooks, and is driven by a single seed.
- **Acceptance:** a test spawns several tasks exchanging timed messages under
  `SimEnv` with a fixed seed and produces a **byte-identical event trace across
  repeated runs**; injecting a partition is reproducible from the seed; a failing
  run prints its seed for replay.

### M2 — Storage interface + trivial implementation
- `animus-storage`: a `StorageEngine` trait driven by what the distributed layer
  needs: `put`/`get`, ordered range scan, atomic batch write, **consistent
  snapshot**, **MVCC timestamps/versions**, range delete.
- In-memory `BTreeMap`-backed impl; optional RocksDB impl behind a `rocksdb` feature.
- **Acceptance:** `proptest` suites for the in-memory impl — key/value round-trips,
  range-scan ordering, and snapshot isolation between a snapshot read and concurrent
  writes.

### M3 — Control-plane RSM skeleton
- `animus-control`: a Raft-replicated metadata state machine (via `openraft`) holding
  **membership** (`NodeId → {topology labels, status}`) and a **single-table tablet
  map** (one tablet → replica set + monotonic epoch). Metadata mutations are
  **compare-and-swap transactions** (precondition = expected epoch). Runs over `Env`.
- **Acceptance:** under `SimEnv`, a 3-node control group elects a leader, applies
  metadata transitions in total order, and **survives a leader kill** (re-elects with
  no metadata divergence) — all replayable from a seed.

### M4 — Thin end-to-end vertical slice
- `animus-data`: leaderless quorum write/read for a single tablet; routing via the
  control-plane tablet map; **epoch fencing** (reject ops bearing a stale epoch).
- **Acceptance:** a simulated 3-node cluster stores a key (W quorum), reads it back
  (R quorum, with R+W>N), and **survives one node kill without losing the
  acknowledged write** — reproducible from a seed.

### M5 — Elle-style history recording + checker
- `animus-test`: a `Recorder` logging `invoke`/`ok`/`fail`/`info` entries with
  **list-append** values and virtual-time stamps (indeterminate ops MUST be `info`,
  never `fail`). A basic in-process dependency-graph **cycle checker** for the
  transactional path, plus convergence + durability checks for the AP path. Export
  histories to EDN/JSON for real Elle/Jepsen later.
- **Acceptance:** the M4 workload runs through the recorder; the checker passes on
  correct runs and **flags an intentionally-introduced bug** (e.g., a dropped-write
  fault) with a minimal reproducing seed.

---

## 7. Explicitly OUT OF SCOPE for now

Do not start these until the M4 vertical slice proves the architecture:
CQL / DynamoDB adapters, Accord-style transactions, tablet split/merge, multi-tablet
routing, a custom LSM storage engine, and residency enforcement across hinted
handoff / repair / backup.

---

## 8. Working style

- One milestone per PR; keep diffs reviewable.
- Every distributed feature lands with a simulation test that injects faults.
- Never introduce nondeterminism into system code (no wall clock, no `tokio::spawn`
  outside `Env`, no `HashMap` iteration in logic — use `BTreeMap`).
- When a decision changes, update the relevant ADR in the same PR.
- Optimize for clarity and testability over completeness; this is foundational code.
