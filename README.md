# CustosDB

[![CI](https://github.com/custosdb/custosdb/actions/workflows/ci.yml/badge.svg)](https://github.com/custosdb/custosdb/actions/workflows/ci.yml)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)

> **Status: pre-alpha, foundational.** Not usable as a database yet. The work in
> progress is the distributed core and its deterministic test harness.

**CustosDB** is a masterless, linearly-scalable, open-source NoSQL database
written in Rust (no JVM). It descends from the Dynamo lineage shared by Cassandra
and DynamoDB: a wide-column / key-value model over a partitioned, sorted
*map-of-maps* storage primitive.

It pairs:

- a **leaderless AP data plane** with tunable consistency (quorum reads/writes),
- a small **strongly-consistent, Raft-backed transactional control plane** that
  owns cluster metadata,

shards data into **tablets** (splittable, movable key ranges), and is
**topology-aware** so data can be pinned to regions/jurisdictions for per-group
**data residency**.

Correctness is established by **deterministic simulation testing (DST)** from day
one: every distributed behavior ships with a fault-injecting simulation test that
is byte-for-byte reproducible from a single seed.

## Why two planes?

The **control plane** is consistent (Raft) and owns cluster metadata
(membership, the tablet map). The **data plane** is leaderless/AP and serves
reads and writes. A control-plane outage must **not** take down the data plane:
data nodes keep serving on cached metadata; only topology changes block. See
[ADR 0001](docs/adr/0001-two-plane-architecture.md).

## Design principles

1. **Determinism first.** All nondeterminism — time, randomness, network, disk,
   task scheduling — flows through a single `Env` seam. System code never calls
   the wall clock, spawns raw tasks, touches real sockets/disk, or iterates a
   `HashMap`. See [ADR 0003](docs/adr/0003-deterministic-simulation.md).
2. **Two planes, never blurred.** ([ADR 0001](docs/adr/0001-two-plane-architecture.md))
3. **Borrow storage, innovate on distribution.** Storage hides behind a trait
   backed by an in-memory implementation; the risk lives in the distributed
   layer. ([ADR 0008](docs/adr/0008-borrowed-storage-first.md))
4. **Vertical slices over horizontal layers.**
5. **Every distributed behavior ships with a fault-injecting simulation test.**

## Workspace layout

| Crate | Purpose |
|-------|---------|
| `custos-env` | `Env` traits + `ProdEnv` (real clock/net/disk/spawn) |
| `custos-sim` | Deterministic simulator: virtual clock, in-memory net, fake disk, scheduler, fault injection |
| `custos-storage` | `StorageEngine` trait + in-memory `BTreeMap` implementation |
| `custos-control` | Control-plane RSM: Raft, metadata model, epochs |
| `custos-data` | Leaderless data plane: quorum read/write, routing, epoch fencing |
| `custos-tablet` | Tablet model (key ranges, replica sets, epochs) |
| `custos-placement` | Placement groups + topology labels + residency *(later)* |
| `custos-consensus` | Accord-style transactional escalation *(later)* |
| `custos-cql` | CQL (Cassandra) wire adapter *(later)* |
| `custos-dynamo` | DynamoDB API adapter *(later)* |
| `custos-test` | Elle-style history recorder + checker |
| `custosd` | Node server binary |
| `custos-cli` | Operator CLI |

## Building

```sh
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

A reproducible simulation test that fails prints its seed; re-run it with
`CUSTOS_SEED=<seed> cargo test <name>` to replay the exact history.

## License & contributing

AGPL-3.0-only. Contributions require a DCO sign-off and CLA; see
[CONTRIBUTING.md](CONTRIBUTING.md) and [ADR 0007](docs/adr/0007-agpl-cla.md).
