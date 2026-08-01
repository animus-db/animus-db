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
- `config::ClusterConfig` — the per-process deployment config (every node's four
  addresses + quorum sizes). Node ids follow a fixed convention from the index
  (control `i`, data `100+i`, coord `200+i`) so processes agree without listing
  ids. `run_node(config, index, dir)` binds *this* node and starts it.
- `bind_cluster` / `start_cluster` — spin up an in-process cluster (the binary's
  `--cluster N` mode and `tests/cluster.rs`).
- `ClientRequest` / `ClientResponse` + `read_frame` / `write_frame` — the
  length-prefixed JSON client protocol (reused by `custos-cli`).
- `dynamo` module — the **DynamoDB JSON-over-HTTP endpoint** (a fifth listener
  per node). A hand-rolled HTTP/1.1 server decodes `X-Amz-Target` +
  AttributeValue-JSON via `custos_dynamo::wire`, then routes through the **same
  `ClientCtx`** (coordinator + cached routing view) as the plain-TCP API.

## What's non-obvious

- A node runs **three internal `ProdEnv` roles on distinct ids/ports** — control
  (Raft), data (replica), coord (the `DataClient`) — because one inbox is
  single-consumer. The **client API is a plain request/reply TCP server**, *not*
  on the `Network`: coordination is server-side, so the coordinator is a static
  cluster member and replica replies route without knowing dynamic client
  addresses.
- Each node also serves a **fifth listener, the DynamoDB JSON/HTTP endpoint**
  (`RoleAddrs.dynamo`, `Node::dynamo_addr`). It is a *production-only I/O edge*
  (real tokio sockets + hand-rolled HTTP/1.1, like `ProdEnv`); below the edge it
  reuses the existing `DataClient`/`Env` paths, so determinism is unaffected.
  The data plane has no native delete, so DynamoDB `DeleteItem` writes a
  tombstone value that `GetItem` reads back as absent. No `CreateTable` yet — the
  edge uses a fixed `pk`/`sk` key-attribute convention.
- Writes get a **quorum-derived version** (`DataClient::read_version` + 1), not a
  per-node counter — otherwise two coordinators assign the same version and the
  replica's monotonic-version check silently drops the later write. Global
  version assignment (HLC) is still future work.
- Client ops are serialized per node behind `coord_lock` so concurrent ops don't
  contend on the single coord inbox. Concurrency is future work.
- Two run modes: `--cluster N` (whole cluster in one process, dev convenience)
  and `--config FILE --node I` (one node per process — real deployment). Both
  share `Node::bind`/`start`; only address/peer assembly differs.

## Tests / running

`cargo test -p custosd` — `tests/cluster.rs` (in-process cluster),
`tests/per_process.rs` (nodes started independently from a shared config), and
`tests/dynamo_wire.rs` (PutItem → GetItem → DeleteItem over the real DynamoDB
JSON/HTTP wire). All use real TCP/time, so they poll with timeouts, not
deterministic assertions.

Per-process run:
```sh
custosd gen-config --nodes 3 > cluster.json
custosd --config cluster.json --node 0   # one process per node, distinct --node
custos status <node-0 client addr>
# the node also prints its DynamoDB HTTP endpoint; talk to it with any
# DynamoDB JSON client, e.g.:
curl -s <dynamo addr>/ \
  -H 'X-Amz-Target: DynamoDB_20120810.PutItem' \
  -d '{"TableName":"t","Item":{"pk":{"S":"a"},"v":{"N":"1"}}}'
```
