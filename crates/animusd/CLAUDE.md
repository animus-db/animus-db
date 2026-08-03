# CLAUDE.md — animusd

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The node server. A **lib + bin**: `lib.rs` assembles a runnable AnimusDB node
over `ProdEnv` (the first real use of the production seam); `main.rs` is a thin
CLI wrapper. `animus-cli` depends on this crate for the client protocol types.

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
  length-prefixed JSON client protocol (reused by `animus-cli`).
- `dynamo` module — the **DynamoDB JSON-over-HTTP endpoint** (a fifth listener
  per node). A hand-rolled HTTP/1.1 server decodes `X-Amz-Target` +
  AttributeValue-JSON via `animus_dynamo::wire`, then routes through the **same
  `ClientCtx`** (coordinator + cached routing view) as the plain-TCP API.
- `cql` module — the **CQL (Cassandra) v4 binary-protocol endpoint** (a sixth
  listener per node). A hand-rolled framed server does the `STARTUP → READY` /
  `OPTIONS → SUPPORTED` handshake and runs `QUERY`/`PREPARE`/`EXECUTE` via the
  pure `animus_cql` crate (a typed `CREATE KEYSPACE`/`USE`/`CREATE TABLE` schema
  catalog incl. **clustering/compound primary keys**, typed
  `INSERT`/`SELECT`/`UPDATE`/`DELETE` + prepared statements), routing through the
  **same `ClientCtx`** as the other edges and **honoring the requested
  consistency level** (it overrides the routing view's R/W per request). A
  *partition* is one data-plane value, so `INSERT`/`UPDATE`/`DELETE` are
  read-modify-write of that value under the coord lock, and a `DELETE` that
  empties the partition issues a data-plane delete/tombstone. The keyspace set +
  prepared-statement store are **per-cluster edge state** (see below).

## What's non-obvious

- A node runs **four internal `ProdEnv` roles on distinct ids/ports** — control
  (Raft, id `i`), data (AP replica, `100+i`), coord (the `DataClient`, `200+i`),
  and **raftkv** (the leaderful **CP** per-tablet Raft group, `300+i`, ADR 0017
  #3a) — because one inbox is single-consumer. `ClusterConfig` assigns seven
  consecutive ports per node (the four internal roles + client/dynamo/cql). The
  **client API is a plain request/reply TCP server**, *not* on the `Network`:
  coordination is server-side, so the coordinator is a static cluster member and
  replica replies route without knowing dynamic client addresses.
- **Per-table CP routing (ADR 0017 #3a, Stage 3a).** A table whose replicated
  schema is `ReplicationMode::Cp` (set via `MetaCommand::SetTableMode`; the interim
  admin path is `Node::propose_meta`) has its client reads/writes routed to a
  **leaderful per-tablet Raft group** (`animus-raftdata`) instead of the AP
  `DataClient`. The `client` API's `Put`/`Get` carry an optional `table`; the
  handler checks `Metadata::table_mode(table)` and, when `Cp`, calls
  `ClientCtx::cp_put`/`cp_get` → the group **leader** (found among the per-cluster
  `ClusterEdgeState::raftkv` handles, mirroring the control-handle registry). A CP
  write waits to read its value back before acking (durable-before-ack); a CP read
  is a linearizable ReadIndex read. Stage 3a hosts **one statically-placed CP
  group** spanning the first `min(N, MAX_REPLICATION_FACTOR)` nodes' `raftkv` ids,
  each backed by its own durable `LsmEngine` (type aliased `CpGroup`). **Scope:**
  CP routing works within a `--cluster N` process (shared edge state); cross-process
  routing, dynamic CP placement/split/reconfigure over `ProdEnv`, and the
  `Coresident`/`ProdEnv` pre-bound-listener-pool are **Stage 3b**. `tests/cp_plane.rs`
  proves the end-to-end round-trip (write via one node, read via another) + the AP
  plane staying untouched.
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
  are sim-proven in `animus-control`/`animus-data`; this is the production
  assembly):
  - The **control-plane heartbeat + failure detection** are driven from the data
    nodes: each data replica's `start_replica` spawns `heartbeat_loop(data_env,
    control_ids)` (send-only on a clone of the data env, so it does not contend on
    the replica's single-consumer inbox), and `RaftNode::start` already runs the
    `detect_loop`/`reconcile_loop` on every control node (no-ops off the leader).
    A killed data node stops heartbeating → the leader marks its member `Down` →
    the reconciler moves the tablet off it.
  - **Anti-entropy** runs per data replica: `serve_anti_entropy(data_env, handle,
    TABLET, peers, ANTI_ENTROPY_INTERVAL)`, also send-only on the data env (its
    `SyncPull` replies arrive back through the replica's inbox). It is given the
    replica `handle` and reads the tablet's **live** epoch from it each round, so
    after a placement reconcile bumps the tablet epoch — and the control plane
    advances this replica via `ReplicaHandle::set_epoch` — the digest round
    carries the bumped epoch and is **not** fenced: a re-placed spare converges in
    the **background**, not only via read-repair on its first read. (This closes
    the formerly-deferred fixed-`Epoch::INITIAL` gotcha; the live-epoch behavior
    is sim-proven in `animus-data/tests/repair.rs`.)
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
  tombstone value that `GetItem` reads back as absent. **`CreateTable` now
  proposes its key schema into the control plane's replicated catalog (ADR 0013)
  and waits for commit**, so a created table is durable + cluster-agreed (it
  survives a restart — `tests/dynamo_schema.rs`); the edge reaches the leader
  through the cluster's set of registered control handles (held in
  `ClusterEdgeState`, threaded via `ClientCtx::edge` — see below). A
  never-`CreateTable`d table falls back to the legacy `pk`/`sk` convention.
  **`CreateTable`'s GSI/LSI *definitions* are also replicated now** (ADR 0013):
  after the schema commits, `create_table` proposes one
  `MetaCommand::CreateTableIndex` per declared index (built via
  `animus_dynamo::schema::index_to_control`, passing the base partition key) and
  waits for each to replicate. The local registry is then reconciled to the
  replicated set via `mirror_catalog_schema` → `SchemaRegistry::sync_indexes`
  (called on the read/write paths too), so a freshly restarted node — or a follower
  that never saw the `CreateTable` — rebuilds its index machinery from
  `Metadata::table_indexes`, not process-local memory. Only the index *entry data*
  (the `escape(hash)||…||base_key` index) stays in-memory, rebuilt from observed
  `note_put`/`note_delete` writes (proven in `tests/dynamo_schema.rs`'s
  `create_table_index_replicates_to_second_node` / `…_survives_node_restart`).
  **Base-table `Query`/`Scan` now use the data plane's native quorum range scan**
  (`DataClient::scan`) over a contiguous data-plane key range (a partition prefix
  for `Query`, the whole-table prefix for `Scan`), decoding each live pair and
  dropping DynamoDB tombstone values — **no in-memory written-key tracking**
  (proven across a restart in `tests/dynamo_schema.rs`). The edge keeps only the
  **GSI/LSI index declarations** in-memory (for an *index* `Query`), held
  **per-cluster** in `ClusterEdgeState` (not a process `OnceLock`). The surface now
  also covers `UpdateItem`/`BatchWriteItem`/`TransactWriteItems` (the last
  condition-gated but not yet atomic), per-index projections, and document-path
  projections.
- And a **sixth listener, the CQL binary-protocol endpoint** (`RoleAddrs.cql`,
  `Node::cql_addr`). Same shape: a production-only I/O edge (real tokio sockets +
  hand-rolled CQL v4 framing in `cql.rs`; the pure protocol/type/catalog/planning
  logic is in `animus-cql`), routed through the same `ClientCtx`. It runs
  `QUERY`/`PREPARE`/`EXECUTE`: `CREATE TABLE` proposes a typed schema into the
  control plane's **replicated catalog** (ADR 0013) and `INSERT`/`SELECT` resolve
  columns from it (a typed row is one data-plane value keyed by `escape(table) ||
  pk_key_bytes`; the partition key is not stored in the value). `CREATE KEYSPACE`
  records the keyspace in the per-cluster `CqlState` (keyspaces are not yet
  replicated).
  - **The keyspace set + prepared-statement store (`CqlState`) are per-cluster
    edge state**, held in the cluster's `ClusterEdgeState` (threaded through
    `ClientCtx::edge`), **not** a process `OnceLock` — like the DynamoDB
    `SchemaRegistry`. They are shared across the cluster's CQL listeners (so
    `--cluster N` dev mode sees one node's `CREATE KEYSPACE` from another) but
    **isolated between two clusters in one process** (so a test harness can run
    several independent clusters without their edge state leaking — the fix for
    the former process-global `OnceLock` state-leak). They are still **not durable
    and not control-plane replicated**: lost on restart, and a one-process-per-node
    deployment has a per-process catalog (re-create schemas per process). Note
    table *schemas* are no longer here at all — they live in the control plane's
    replicated catalog (ADR 0013). Per-connection state (the `USE`d keyspace)
    lives in `Session`.
  - The **prepared-statement id is content-addressed** — a stable hash of the
    statement text (FNV-1a, no RNG so the edge stays deterministic) — so `PREPARE`
    on one connection and `EXECUTE` on another resolve to the same statement.
- **A `GET /metrics` admin route shares the DynamoDB HTTP listener** (ADR 0015) —
  no seventh port or `RoleAddrs` field. The DynamoDB edge's request parser now
  captures the request method + path; a `GET /metrics` is answered with the
  text-format snapshot as `text/plain` (everything else is the existing
  `POST /` + `X-Amz-Target` DynamoDB protocol). The body is **aggregated across the
  node's three role sinks** (control / data / coord) by `ClientCtx::metrics_text`:
  each role records into its **own** `ProdEnv` sink (`RaftNode::start` →
  `control_env.metrics()`; the replica + coordinator → their envs'), so the handler
  snapshots all three **at request time** (live, not cached), sums the counters,
  and takes the max leadership gauge. Both control- and data-plane counters surface
  from one endpoint; today only the control-plane counters move, and a data-plane
  counter surfaces automatically once recorded (no endpoint change). The two
  data/coord sinks are captured in `start_with` before their envs are moved and
  threaded into `ClientCtx`. The endpoint is on `Node::dynamo_addr()`
  (`curl -s <dynamo addr>/metrics`).
- Writes get a **quorum-derived version** (`DataClient::read_version` + 1), not a
  per-node counter — otherwise two coordinators assign the same version and the
  replica's monotonic-version check silently drops the later write. Global
  version assignment (HLC) is still future work.
- Client ops are serialized per node behind `coord_lock` so concurrent ops don't
  contend on the single coord inbox. Concurrency is future work.
- Two run modes: `--cluster N` (whole cluster in one process, dev convenience)
  and `--config FILE --node I` (one node per process — real deployment). Both
  share `Node::bind`/`start`; only address/peer assembly differs.
- **The wire edges' mutable state is `ClusterEdgeState`, scoped to one cluster**
  (not the whole process). It holds the set of control `RaftNode` handles a schema
  DDL proposal fans out to (so a follower-connected `CreateTable`/`CREATE TABLE`
  still reaches the leader), the DynamoDB `SchemaRegistry` (GSI/LSI index
  declarations — the base written-key index is gone, replaced by the native range
  scan), and the CQL `CqlState` (keyspaces + prepared statements). It is created
  once per cluster — in `start_cluster_with` (shared by every node of that
  cluster, so `--cluster N` dev mode agrees) and freshly in `run_node_with` (one
  per process) — and threaded into `start_with` → `ClientCtx::edge`. In
  `--cluster N` mode one process is one cluster, so this is equivalent to the old
  process-global; the point is that a **test harness running several independent
  clusters in one process gets a distinct, isolated edge-state set per cluster**,
  so two clusters never share a registry or a handle set. (This replaced the
  former `OnceLock` process statics, which leaked across tests in one binary —
  a later test's `CreateTable` fanned its proposal across every still-running
  cluster's leaders and timed out.) Schema DDL routes through
  `ClusterEdgeState::{leader_handle, propose_on_leaders}`; reads/writes resolve
  the table schema from this node's own replicated `Metadata`.
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

`cargo test -p animusd` — `tests/cluster.rs` (in-process cluster),
`tests/per_process.rs` (nodes started independently from a shared config),
`tests/dynamo_wire.rs` (PutItem → GetItem → DeleteItem over the real DynamoDB
JSON/HTTP wire), `tests/cql_wire.rs` (STARTUP → CREATE KEYSPACE/USE/CREATE
TABLE → PREPARE INSERT → EXECUTE with typed bound values → typed SELECT, columns
round-tripping, over the real CQL binary wire), `tests/cql_clustering.rs`
(compound primary key: INSERT rows out of clustering order → clustering-ordered
SELECT → single-row SELECT → UPDATE → single-row + whole-partition DELETE, at
QUORUM consistency), `tests/durable_restart.rs` (a key written
through the client API survives a node stop + restart on the **same dir +
addresses** with the LSM backend, and is lost with the `--ephemeral` memory
backend), `tests/metrics_endpoint.rs` (the admin `GET /metrics` HTTP route, ADR 0015: a
3-node cluster elects a leader, the scrape returns the `text/plain` `name value`
export with `control_elections_won >= 1` and `control_is_leader 1` on the leader /
`0` on a follower), and `tests/self_heal.rs` (**live self-healing**: a 4-node cluster
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
animusd gen-config --nodes 3 > cluster.json
animusd --config cluster.json --node 0   # one process per node, distinct --node
animus status <node-0 client addr>
# the node also prints its DynamoDB HTTP endpoint; talk to it with any
# DynamoDB JSON client, e.g.:
curl -s <dynamo addr>/ \
  -H 'X-Amz-Target: DynamoDB_20120810.PutItem' \
  -d '{"TableName":"t","Item":{"pk":{"S":"a"},"v":{"N":"1"}}}'
```
