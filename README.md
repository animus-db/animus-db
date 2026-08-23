# AnimusDB

[![CI](https://github.com/animusdb/animusdb/actions/workflows/ci.yml/badge.svg)](https://github.com/animusdb/animusdb/actions/workflows/ci.yml)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)

**[animusdb.io](https://animusdb.io)** — overview, documentation and install
instructions. The site's source is in [`website/`](website/).

> **Status: pre-alpha, foundational.** Not usable as a database yet. The work in
> progress is the distributed core and its deterministic test harness.

**AnimusDB** is a masterless, linearly-scalable, open-source NoSQL database
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
| `animus-env` | `Env` traits + `ProdEnv` (real clock/net/disk/spawn) |
| `animus-sim` | Deterministic simulator: virtual clock, in-memory net, fake disk, scheduler, fault injection |
| `animus-storage` | `StorageEngine` trait + in-memory `BTreeMap` impl + custom on-disk `LsmEngine` (WAL/SSTable/compaction over the `Env` seam) |
| `animus-control` | Control-plane RSM: Raft, metadata model, epochs |
| `animus-data` | Leaderless data plane: quorum read/write, routing, epoch fencing |
| `animus-tablet` | Tablet model (key ranges, replica sets, epochs) |
| `animus-placement` | Placement groups + topology labels + residency *(later)* |
| `animus-consensus` | Accord-style transactional escalation *(later)* |
| `animus-dynamo` | DynamoDB-style item API over the common core (wire protocol later) |
| `animus-test` | Elle-style history recorder + checker |
| `animusd` | Node server: assembles control + data + a client API over `ProdEnv` (runnable `--cluster` mode) |
| `animus-cli` | Operator/client CLI (`status` / `put` / `get`) |

## Running a cluster

In one process (dev convenience):

```sh
cargo run -p animusd --bin animusd -- --cluster 3   # prints each node's client address
cargo run -p animus-cli -- status 127.0.0.1:<port>
cargo run -p animus-cli -- put    127.0.0.1:<port> mykey myvalue
cargo run -p animus-cli -- get    127.0.0.1:<port> mykey
```

One process per node (real deployment) — generate a config once, then run a
process per node with a distinct `--node` index (on one host or many):

```sh
animusd gen-config --nodes 3 --host 10.0.0.1 > cluster.json
animusd --config cluster.json --node 0      # on each node, with a distinct --node
animus status 10.0.0.1:<node-0 client port>
```

## Building

```sh
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

A reproducible simulation test that fails prints its seed; re-run it with
`ANIMUS_SEED=<seed> cargo test <name>` to replay the exact history.

## License & contributing

AGPL-3.0-only. Contributions require a DCO sign-off and CLA; see
[CONTRIBUTING.md](CONTRIBUTING.md) and [ADR 0007](docs/adr/0007-agpl-cla.md).
