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
- `config::ClusterConfig` — the per-process deployment config (every node's six
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
- `cql` module — the **CQL (Cassandra) v4 binary-protocol endpoint** (a sixth
  listener per node). A hand-rolled framed server does the `STARTUP → READY` /
  `OPTIONS → SUPPORTED` handshake and a tiny `QUERY` path (`INSERT`/`SELECT`)
  via the pure `custos_cql` crate, routing through the **same `ClientCtx`** as
  the other edges.

## What's non-obvious

- A node runs **three internal `ProdEnv` roles on distinct ids/ports** — control
  (Raft), data (replica), coord (the `DataClient`) — because one inbox is
  single-consumer. The **client API is a plain request/reply TCP server**, *not*
  on the `Network`: coordination is server-side, so the coordinator is a static
  cluster member and replica replies route without knowing dynamic client
  addresses.
- **The cluster's members are the DATA nodes, not the control ids** (this is what
  makes self-healing work end to end). The control ids `0..N` are only the Raft
  *consensus group*; `bootstrap` registers the **data ids** (`100+i`) as `Active`
  `Metadata` members, places the bootstrap tablet on the first
  `min(N, MAX_REPLICATION_FACTOR)` of them, and attaches a `PlacementPolicy`
  (`SetTabletPolicy`). So the failure detector (ADR 0012) and the placement
  reconciler (ADR 0005) — both of which operate over `Active` members — act on the
  nodes that actually hold data. Capping the RF at 3 leaves a **spare** in a
  larger cluster, which is what a detected `Down` can be re-placed onto.
- **The autonomous loops are wired here, over `ProdEnv` timers** (the mechanisms
  are sim-proven in `custos-control`/`custos-data`; this is the production
  assembly):
  - The **control-plane heartbeat + failure detection** are driven from the data
    nodes: each data replica's `start_replica` spawns `heartbeat_loop(data_env,
    control_ids)` (send-only on a clone of the data env, so it does not contend on
    the replica's single-consumer inbox), and `RaftNode::start` already runs the
    `detect_loop`/`reconcile_loop` on every control node (no-ops off the leader).
    A killed data node stops heartbeating → the leader marks its member `Down` →
    the reconciler moves the tablet off it.
  - **Anti-entropy** runs per data replica: `serve_anti_entropy(data_env, storage,
    TABLET, Epoch::INITIAL, peers, ANTI_ENTROPY_INTERVAL)`, also send-only on the
    data env (its `SyncPull` replies arrive back through the replica's inbox).
    **Gotcha:** the loop's epoch is fixed at `Epoch::INITIAL`, so it converges
    while the tablet is at its initial epoch (the steady state). A placement
    reconcile bumps the tablet epoch, after which the replica fences the
    stale-epoch anti-entropy traffic; a re-placed spare is then filled by
    **read-repair** on the first read that includes it, not by anti-entropy.
    Threading the live tablet epoch into the loop is deferred.
- Proven live in `tests/self_heal.rs`: a 4-node cluster (RF 3 + one spare) writes a
  key, kills a replica node, and the cluster autonomously marks it `Down`,
  re-places the tablet onto the spare (epoch bumps), and still serves the key from
  the survivors — observable self-healing in the assembled binary.
- **The data replica is durable by default**: `serve_replica` is backed by the
  on-disk `LsmEngine` opened over a *clone* of the node's **data** `ProdEnv`
  (`StorageBackend::Lsm`), so a value acked to a client survives a process
  restart (the LSM recovers from its WAL/SSTables/manifest on reopen) — like the
  control plane, which already persists its Raft WAL. The LSM does its disk I/O
  through the cloned data-env handle while the replica keeps the original handle
  for network `recv`; since the LSM only touches the disk, the single-consumer
  inbox is unaffected. The engine's files use a **flat filename prefix**
  (`LSM_PREFIX = "db-"`), *not* a subdirectory — `ProdEnv`'s disk opens files
  directly under the role's data dir and does not create intermediate
  directories, so a slash-bearing prefix (e.g. `"db/"`) would fail to create the
  files. `--ephemeral` (or `StorageBackend::Memory`) selects the volatile
  `MemoryEngine` instead, for dev runs that intentionally start empty.
  `start`/`start_cluster`/`run_node` default to the durable backend;
  `start_with`/`start_cluster_with`/`run_node_with` take an explicit
  `StorageBackend`. These are now **async + fallible** (opening the LSM is async
  and can fail), so the node-start entry points return `io::Result`.
- Each node also serves a **fifth listener, the DynamoDB JSON/HTTP endpoint**
  (`RoleAddrs.dynamo`, `Node::dynamo_addr`). It is a *production-only I/O edge*
  (real tokio sockets + hand-rolled HTTP/1.1, like `ProdEnv`); below the edge it
  reuses the existing `DataClient`/`Env` paths, so determinism is unaffected.
  The data plane has no native delete, so DynamoDB `DeleteItem` writes a
  tombstone value that `GetItem` reads back as absent. No `CreateTable` yet — the
  edge uses a fixed `pk`/`sk` key-attribute convention.
- And a **sixth listener, the CQL binary-protocol endpoint** (`RoleAddrs.cql`,
  `Node::cql_addr`). Same shape: a production-only I/O edge (real tokio sockets +
  hand-rolled CQL v4 framing in `cql.rs`; the pure protocol logic is in
  `custos-cql`), routed through the same `ClientCtx`. No schema catalog yet, so a
  row is the fixed `(pk, v)` convention keyed by the partition key (data-plane
  key `escape(table) || pk_bytes`); only `INSERT`/`SELECT` are recognized.
- Writes get a **quorum-derived version** (`DataClient::read_version` + 1), not a
  per-node counter — otherwise two coordinators assign the same version and the
  replica's monotonic-version check silently drops the later write. Global
  version assignment (HLC) is still future work.
- Client ops are serialized per node behind `coord_lock` so concurrent ops don't
  contend on the single coord inbox. Concurrency is future work.
- Two run modes: `--cluster N` (whole cluster in one process, dev convenience)
  and `--config FILE --node I` (one node per process — real deployment). Both
  share `Node::bind`/`start`; only address/peer assembly differs.
- **`Node::shutdown()` is a graceful teardown**: it aborts the node's
  client-facing listener tasks (client/dynamo/cql, on plain `tokio::spawn`) and
  calls `ProdEnv::shutdown()` on each of the three internal role envs, which
  aborts every task they own (the Raft driver, the replica serve loop, the
  internal accept loops). This frees all six listener ports so a replacement node
  can rebind the same addresses on the same data dir — the clean teardown a
  stopped OS process would provide. On-disk state is untouched (a value acked to
  a client was WAL-synced before the ack, so it survives). Wired to the Ctrl-C
  path in `main`. Dropping a `Node` without `shutdown()` still leaves its detached
  tasks running (they hold the ports), so call `shutdown()` to restart in-place.

## Tests / running

`cargo test -p custosd` — `tests/cluster.rs` (in-process cluster),
`tests/per_process.rs` (nodes started independently from a shared config),
`tests/dynamo_wire.rs` (PutItem → GetItem → DeleteItem over the real DynamoDB
JSON/HTTP wire), `tests/cql_wire.rs` (STARTUP handshake → INSERT → SELECT
over the real CQL binary wire), `tests/durable_restart.rs` (a key written
through the client API survives a node stop + restart on the **same dir +
addresses** with the LSM backend, and is lost with the `--ephemeral` memory
backend), and `tests/self_heal.rs` (**live self-healing**: a 4-node cluster
detects a killed replica node, marks it `Down`, re-places the tablet onto the
spare, and still serves the key from the survivors; plus a concurrent-client
smoke test that the assembled node does not deadlock under load). All use real
TCP/time, so they poll with timeouts, not deterministic assertions. The restart test runs both incarnations in the **same** runtime,
calling `Node::shutdown()` between them to abort the node's detached tasks and
free its listener ports (dropping a `Node` does not stop them), then rebinds the
same addresses and recovers — a clean teardown → rebind → recover cycle standing
in for an OS process restart.

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
