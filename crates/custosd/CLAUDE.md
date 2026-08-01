# CLAUDE.md — custosd

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The node server. A **lib + bin**: `lib.rs` assembles a runnable CustosDB node
over `ProdEnv` (the first real use of the production seam); `main.rs` is a thin
CLI wrapper. `custos-cli` depends on this crate for the client protocol types.

## Entry points

- `Node::bind` → `BoundNode::start` — two-phase construction (bind listeners,
  then install the peer address book and start protocols), so a cluster can use
  ephemeral ports and exchange addresses afterward.
- `bind_cluster` / `start_cluster` — spin up an in-process cluster (the binary's
  `--cluster N` mode and `tests/cluster.rs`).
- `ClientRequest` / `ClientResponse` + `read_frame` / `write_frame` — the
  length-prefixed JSON client protocol (reused by `custos-cli`).

## What's non-obvious

- A node runs **three internal `ProdEnv` roles on distinct ids/ports** — control
  (Raft), data (replica), coord (the `DataClient`) — because one inbox is
  single-consumer. The **client API is a plain request/reply TCP server**, *not*
  on the `Network`: coordination is server-side, so the coordinator is a static
  cluster member and replica replies route without knowing dynamic client
  addresses.
- Writes get a **quorum-derived version** (`DataClient::read_version` + 1), not a
  per-node counter — otherwise two coordinators assign the same version and the
  replica's monotonic-version check silently drops the later write. Global
  version assignment (HLC) is still future work.
- Client ops are serialized per node behind `coord_lock` so concurrent ops don't
  contend on the single coord inbox. Concurrency is future work.
- `--cluster N` runs the whole cluster in one process over loopback TCP;
  per-process deployment with a config file is future work.

## Tests / running

`cargo test -p custosd --test cluster` — a real-TCP 3-node cluster (uses real
time, so it polls with timeouts, not deterministic assertions).
Run it: `cargo run -p custosd --bin custosd -- --cluster 3` then
`cargo run -p custos-cli -- status <printed-addr>`.
