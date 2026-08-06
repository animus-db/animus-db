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
  addresses). Node ids follow a fixed convention from the index (control `i`,
  raftkv `300+i`) so processes agree without listing ids. `run_node(config, index,
  dir)` binds *this* node and starts it.
- `bind_cluster` / `start_cluster` — spin up an in-process cluster (the binary's
  `--cluster N` mode and `tests/cluster.rs`).
- `ClientRequest` / `ClientResponse` + `read_frame` / `write_frame` — the
  length-prefixed JSON client protocol (reused by `animus-cli`).
- `dynamo` module — the **DynamoDB JSON-over-HTTP endpoint** (a fifth listener
  per node). A hand-rolled HTTP/1.1 server decodes `X-Amz-Target` +
  AttributeValue-JSON via `animus_dynamo::wire`, then routes through the **same
  `ClientCtx`** as the plain-TCP API. v1 (ADR 0019): reads/writes/scans go to the
  **CP plane** (`ClientCtx::cp_read`/`cp_write`/`cp_scan`), not the AP coordinator.
- `admin` module — the **admin / debug HTTP-JSON endpoint** (ADR 0020), a dedicated
  sixth listener (`RoleAddrs.admin`). Read-only introspection (config, status, both
  Raft layers, LSM/WAL debug, metrics, health) + gated operator actions
  (split/flush/compact/reconfigure/drain). `http` module — the shared hand-rolled
  HTTP/1.1 helpers (request parser + response writers) used by both `dynamo` and
  `admin`.
- `cql` module — the **CQL (Cassandra) v4 binary-protocol endpoint** (a
  listener per node). A hand-rolled framed server does the `STARTUP → READY` /
  `OPTIONS → SUPPORTED` handshake and runs `QUERY`/`PREPARE`/`EXECUTE` via the
  pure `animus_cql` crate (a typed `CREATE KEYSPACE`/`USE`/`CREATE TABLE` schema
  catalog incl. **clustering/compound primary keys**, typed
  `INSERT`/`SELECT`/`UPDATE`/`DELETE` + prepared statements), routing through the
  **same `ClientCtx`** as the other edges. v1 (ADR 0019): reads/writes go to the
  **CP plane** (`cp_read`/`cp_write`/`cp_delete`), which is linearizable — the
  requested **consistency level is accepted but moot** (CP is at least as strong as
  any level; it no longer sizes a quorum). A *partition* is one CP value, so
  `INSERT`/`UPDATE`/`DELETE` are read-modify-write of that value **under the coord
  lock** (which serializes a node's RMWs so the linearizable read + CP write are
  atomic per node; the Raft index is the MVCC version, so no client-assigned
  version), and a `DELETE` that empties the partition issues a CP tombstone
  (`cp_delete`). The keyspace set + prepared-statement store are **per-cluster edge
  state** (see below).

## What's non-obvious

- A node runs **two internal `ProdEnv` roles on distinct ids/ports** — control
  (Raft metadata, id `i`) and **raftkv** (the leaderful **CP** per-tablet Raft
  group, `300+i`, ADR 0017 #3a — the v1 data plane) — because one inbox is
  single-consumer. `ClusterConfig` assigns **six** consecutive ports per node (the two
  internal roles + client/dynamo/cql/**admin**, the admin port being ADR 0020). v1 (ADR 0019) is **CP-only**: the leaderless
  AP `data`/`coord` roles, `serve_replica`, anti-entropy, and hinted handoff are
  gone. The **client API is a plain request/reply TCP server**, *not* on the
  `Network`: a node that does not host the CP group leader **forwards** a data op to
  the leader's node over a fresh client connection (ADR 0017 #3b), so dynamic client
  addresses never touch the internal network.
- **CP routing (ADR 0017 #3a / v1 ADR 0019).** The data path is the **leaderful
  per-tablet Raft group** (`animus-cp-data`), reached through four `ClientCtx`
  primitives that all resolve the leader the same way (`cp_route`): `cp_read`
  (linearizable ReadIndex), `cp_write` / `cp_delete` (Raft-committed, waited to
  durable+applied — durable-before-ack), and `cp_scan` (linearizable range read).
  `cp_route` serves **locally** if this node hosts the leader, **forwards** to the
  leader's node if a local replica gives a leader hint + a `client_route` exists
  (ADR 0017 #3b cross-process, wrapped in `ClientRequest::Forwarded`, one hop), and
  otherwise **waits** for the local group to elect (it never forwards a CP op to a
  non-leader — including itself — during election). **Every data op — the wire edges
  (DynamoDB, CQL) and the plain-client `Put`/`Get`/`Scan`/`Delete` — routes through
  these.** The optional `table` no longer selects a plane (there is only the CP
  plane); the single CP group covers the whole keyspace. The edges create their
  tables in `ReplicationMode::Cp` (the mode is recorded for truthfulness, but
  routing no longer depends on it). A just-proposed write is confirmed via a **local**
  read on the leader (not a quorum barrier — the leader applies only after a quorum
  commit + WAL fsync, so a local read reflecting the value means it's durable; a
  per-write barrier would not scale under concurrent load). At node start a node
  hosts the **bootstrap CP group** spanning the first `min(N, MAX_REPLICATION_FACTOR)`
  nodes' `raftkv` ids; a **tablet split** (Phase 2.2/2.4) then stands up additional
  co-resident groups, each backed by its own `LsmEngine` or `MemoryEngine` per
  [`StorageBackend`] (an enum-wrapped `CpGroup`), keyed by `TabletId` in the edge
  registry (Phase 2.1 tablet-keyed routing). `tests/cp_plane.rs` (in-process
  round-trip) + `tests/cp_cross_process.rs` (forwarding) + the dynamo/cql wire +
  schema tests all exercise the CP path. **Split tablets are re-hosted on restart**
  (#2): each node durably records the split tablets it hosts in a **`cp-hosted`
  marker** (`load_hosted_cp`/`save_hosted_cp` on the `raftkv` env disk — genuinely
  *local* state: which co-resident `db-t{id}-` engines physically exist here, not
  derivable from the tablet map, which records placement in stable base ids). At
  start `start_with` reads the marker, pre-populates the per-node `minted` set from
  it (so the bootstrap group re-applying a committed `Split` on WAL recovery does
  **not** mint the sibling twice — split crash-idempotency), and spawns `cp_rehost`
  for each recorded tablet: mint its sibling, recover its `db-t{id}-` engine +
  `raftkv.wal` via `start_seeded` with an **empty** seed (the data is already on
  disk — the seed only ever carries a *fresh* split's handoff), register it, and
  re-publish the sibling's new address. `tests/cp_rehost.rs` (split a tablet, restart
  the cluster, the upper-range key survives on the re-hosted group). **Address
  publish is cross-process** now: the split-seed + re-host paths publish via
  `ClientCtx::register_cp_addr`, which relays the registration to the control leader
  through `client_route` (a follower can't propose directly) — so the `ctx` is built
  before the CP hosting block and threaded into the hook + re-host. **Hosting is
  driven by the tablet map too** (D1): a per-node **join-host loop** stands up an
  *empty* co-resident group for any tablet newly *placed* on this node by the
  reconciler (`epoch > INITIAL`, so it never starts a fresh split child empty), which
  the leader then catches up via `InstallSnapshot` — closing the auto-replace cascade.
  All hosting state is bundled in a **`CpHostCtx`** so every group (bootstrap, split
  child, re-hosted, joined) carries a split hook, enabling **deep splits** (D3: a
  split tablet can be split again); member ids derive **flatly** from the base id
  (`cp_member_id`) at any depth. **Each node can host at most `CP_SIBLING_POOL + 1`
  co-resident tablets** (the bootstrap group + one pre-bound listener-pool slot per
  split child); the pool is **64**, sized for bulk-seed→auto-split sharding tests.
  Exceeding it panics the split-hook task (`Coresident::sibling`), leaving the
  over-cap tablet **leaderless** (writes to its range hang) — so keep the pool ahead
  of the tablet count. (Found via bulk-seed: a pool of 4 left a 6th tablet leaderless;
  the `ProdEnv` "send to unknown peer" log of the missing split member is the
  downstream symptom, now at `debug`.)
- **The cluster's members are the CP `raftkv` nodes, not the control ids.** The
  control ids `0..N` are only the Raft *consensus group* for metadata; `bootstrap`
  (leader-only, idempotent) registers the **raftkv ids** (`300+i`) as `Active`
  `Metadata` members and records the single bootstrap **CP tablet** (whole keyspace)
  placed on the first `min(N, MAX_REPLICATION_FACTOR)` of them — the same set the CP
  group spans in `start_with`. This keeps `metadata().tablets`/`status` meaningful
  and gives dynamic CP reconfigure a hook (`tablets[t].replicas`). **Data-node
  failure detection is now wired over `ProdEnv`** (#3): every node spawns
  `heartbeat_loop` on its `raftkv` env, heartbeating the control group *as its
  `raftkv` member id*, so the control leader's `detect_loop` marks a crashed CP node
  `Down` (`tests/cp_reconfigure.rs::data_node_failure_is_detected`). And **each
  CP-hosting node runs `cp_reconfigure_loop`** (#3 / ADR 0017 Stage C): for every
  tablet whose group it leads, it pulls `tablets[t].replicas`, translates base ids to
  the group's member ids via `cp_members_for` (bootstrap tablet = base ids; a split
  tablet = `base + tablet * CP_SPLIT_ID_STRIDE`, matching `cp_split_seed` — so the
  replicated map can stay in base ids without reconciling to derived ids, #4), and
  takes one single-server `reconfigure_step` toward it
  (`tests/cp_reconfigure.rs::cp_group_follows_tablet_replica_set`: dropping a follower
  from the replica set reconfigures the group's voters down). **The full cascade is
  closed** (D1): `bootstrap` attaches a label-free RF `PlacementPolicy`, so on a `Down`
  replica the reconciler picks an Active spare, the spare's join-host loop stands up an
  empty group, and the leader adds + catches it up — auto-replacing the dead replica
  end to end (`tests/cp_reconfigure.rs::failure_auto_replaces_replica_onto_spare`). The
  v0 heartbeat/anti-entropy/hinted-handoff loops and the `serve_replica` data role are
  gone; the control-plane mechanisms (failure detection, placement) remain sim-proven
  in `animus-control`. **Small remainder:** new-group ids are derived, not
  control-plane-allocated (fine for realistic clusters).
- **The CP group is durable by default**: each hosting node's `RaftKvNode` is
  backed by the on-disk `LsmEngine` opened over its **raftkv** `ProdEnv`
  (`StorageBackend::Lsm`), so a value acked to a client (Raft-committed + WAL-fsynced
  before the ack) survives a process restart (the LSM + Raft WAL recover on reopen).
  The engine's files use a **flat filename prefix** (`LSM_PREFIX = "db-"`), *not* a
  subdirectory — `ProdEnv`'s disk opens files directly under the role's data dir and
  does not create intermediate directories, so a slash-bearing prefix (e.g. `"db/"`)
  would fail to create the files. `--ephemeral` (or `StorageBackend::Memory`) selects
  the volatile `MemoryEngine` instead (the `CpGroup` enum wraps either), for dev runs
  that intentionally start empty. `start`/`start_cluster`/`run_node` default to the
  durable backend; `start_with`/`start_cluster_with`/`run_node_with` take an explicit
  `StorageBackend`. These are **async + fallible** (opening the LSM is async and can
  fail), so the node-start entry points return `io::Result`. (`tests/durable_restart.rs`
  proves a client write survives a restart on the LSM backend and is lost on the
  memory backend; `tests/self_heal.rs` is now just a concurrent-load smoke test.)
- Each node also serves a **fifth listener, the DynamoDB JSON/HTTP endpoint**
  (`RoleAddrs.dynamo`, `Node::dynamo_addr`). It is a *production-only I/O edge*
  (real tokio sockets + hand-rolled HTTP/1.1, like `ProdEnv`); below the edge it
  routes through the CP primitives (`ClientCtx::cp_read`/`cp_write`/`cp_scan`).
  DynamoDB `DeleteItem` writes a sentinel tombstone *value* that `GetItem` reads
  back as absent (distinct from the CQL whole-partition `cp_delete`). **`CreateTable` now
  proposes its key schema into the control plane's replicated catalog (ADR 0013)
  and waits for commit**, so a created table is durable + cluster-agreed (it
  survives a restart — `tests/dynamo_schema.rs`); the edge reaches the leader
  through the cluster's set of registered control handles (held in
  `ClusterEdgeState`, threaded via `ClientCtx::edge` — see below). A
  never-`CreateTable`d table falls back to the legacy `pk`/`sk` convention.
  `CreateTable` now decodes `AttributeDefinitions` into `key_types` (carried on
  `Operation::CreateTable`) and passes them to `schema_bridge::to_control`, so the
  replicated catalog records each key column's declared **type** (`S`/`N`/`B` →
  `String`/`Number`/`Binary`) — previously the edge passed `&[]`, defaulting every
  key to `String`. The dashboard's key prefill reads these types.
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
  **Base-table `Query`/`Scan` use the CP plane's linearizable range scan**
  (`ClientCtx::cp_scan` → `RaftKvNode::linearizable_scan`) over a contiguous key
  range (a partition prefix for `Query`, the whole-table prefix for `Scan`),
  decoding each live pair and dropping DynamoDB tombstone values — **no in-memory
  written-key tracking** (proven across a restart in `tests/dynamo_schema.rs`). The edge keeps only the
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
- **A dedicated admin / debug HTTP-JSON endpoint** (`RoleAddrs.admin`,
  `Node::admin_addr`, ADR 0020) — a **sixth** per-node listener, isolated from the
  client/dynamo/cql data edges. A production-only I/O edge in `admin.rs` (real
  tokio sockets + the shared hand-rolled HTTP helpers extracted to `http.rs`, now
  shared with `dynamo.rs`). Read-only `GET` views — `/admin/{config,status,raft,
  raftkv,storage/lsm,storage/wal,storage/wal/segment,storage/key,storage/scan,metrics,health}`
  — plus gated `POST` actions — `/admin/{tablet/split,storage/flush,storage/compact,
  raftkv/reconfigure,drain}` and **data writes** — `/admin/data/{dynamo,cql,drop-table,seed}`
  (ADR 0021, the dashboard's write surface). Below the edge it only **reads** node state
  (control + CP Raft accessors, `LsmEngine` introspection: `sstable_views`/
  `wal_segment_*`/`memtable_*`, the `CpGroup` introspection passthroughs) aggregated
  live at request time, or drives an explicit action; node identity for `/admin/config`
  is captured into `ClientCtx.admin` (an `AdminInfo`). **No auth yet** — bind it to a
  trusted interface. The `animus admin <subcommand>` CLI consumes it.
  - **The web dashboard (ADR 0021)** is served from the same port: `GET /` (and the
    `/admin`, `/admin/ui` aliases) returns a self-contained vanilla-JS SPA embedded
    via `include_str!` (`dashboard.rs` → `dashboard.html`) — no bundler/npm, the
    build stays `cargo`-only. It is a pure **client** of the `/admin/*` JSON, so
    every `/admin/*` response now carries **CORS** (`http::CORS_HEADERS`, spliced via
    the new `http::write_response_with`; an `OPTIONS` preflight returns 204) because
    the page loaded from one node fans out in the browser to **every** node. The
    fan-out seed is **`GET /admin/peers`** → all nodes' admin addresses: each process
    already knows them (its `ClusterConfig` per-process, or the in-process bring-up),
    so they are threaded into `AdminInfo.admin_addrs` (via a new `start_with`
    param) rather than the browser guessing ports. Read-only for now (nodes/tablets/
    WAL/data panels); operator-action UI + the ADR 0018 transaction view are next.
    The data panel has both a single-key inspector (`/admin/storage/key`) and a
    **browse-keys** list (`/admin/storage/scan` → `CpGroup::local_scan`, first N live
    pairs `>= start`; click a key to send it to the inspector). The Storage tab's
    **node dropdown is filtered to nodes whose `/admin/raftkv` view lists the
    selected tablet** (the storage endpoints are node-local — `local_cp` — and 404
    on a non-hosting node; the tablet list is cluster-wide metadata, so the raw
    cross-product invited valid-looking 404 combos), annotating the leader; if no
    reachable node hosts the tablet yet (group still forming) it falls back to all
    nodes with a hint.
    `tests/dashboard_endpoint.rs` proves serve + CORS + preflight + peers.
  - **The Write tab (ADR 0021) writes through the admin port.** `POST
    /admin/data/dynamo {op, payload}` reuses the DynamoDB edge in-process
    (`dynamo::execute` — the factored decode+`run_operation`), returning the op's
    JSON. `POST /admin/data/cql {query, keyspace?}` runs CQL by driving **this
    node's own CQL port as a loopback client** (`cql_client` — STARTUP→QUERY per
    `;`-split statement, decoding the binary RESULT frame to JSON via
    `animus_cql::types`), so the 1000-line CQL edge is reused untouched rather than
    refactored to emit JSON. The browser can't speak the CQL binary protocol, so a
    server-side proxy is mandatory; Dynamo is proxied too for one origin / one CORS
    / one future-auth boundary. **This makes the admin port a data-write *and* DDL
    surface (still no auth)** — sharpening the bind-to-trusted-interface /
    auth-before-exposure follow-up. `tests/admin_endpoint.rs::admin_data_write_dynamo_and_cql`
    proves a Dynamo Put→Get round-trip + a CREATE/INSERT/SELECT CQL script.
    - **Dynamo table management + form/JSON sync.** The Dynamo panel lists tables
      from the replicated catalog (`/admin/status` `schemas.tables`, filtered to
      plain-named = Dynamo, vs CQL `ks.table`), creates via `CreateTable`, and drops
      via `POST /admin/data/drop-table` (`ctx.drop_table_schema`; the Dynamo wire has
      no `DeleteTable`, so this reuses the control-plane drop, schema-only). The op
      **table is a dropdown of existing tables** — ops are disabled when none exist, so
      you can't act on a non-existent table. Form and JSON share one request model
      (`dynModel`) with a Form/JSON **toggle** that syncs both ways (the JSON view is
      the full request; the form edits TableName + Item/Key, preserving extra fields
      like `UpdateExpression`). Selecting a table (or op) **prefills the request with
      that table's key attributes** — partition key + sort key, typed from the catalog
      (S/N/B) — into both views. `tests/admin_endpoint.rs::admin_table_management_create_and_drop`
      (also asserts a numeric sort key's type reaches the catalog).
    - **Bulk seed for sharding tests.** `POST /admin/data/seed {table, count, start?,
      key_prefix?, value_bytes?}` writes synthetic rows whose partition key is
      `key_prefix` + zero-padded index, stored under the edges' token-prefixed
      layout (`partition_token(escape(pk)) || escape(pk)`, `admin.rs::seed_key` —
      ADR 0022: seeding must hash like a real write, so sequential indices spread
      across the ring instead of piling into one tablet's tail)
      into an **existing** `table` (ADR 0023: seeding writes into a table, it
      does not create one — a non-existent table is a `404`, looked up in the
      replicated tablet map) via the normal durable `cp_write`, with bounded
      concurrency (`SEED_CONCURRENCY`) to amortize WAL group-commit; capped at
      `SEED_MAX_PER_REQUEST` per call. Each key is **retried** (`SEED_WRITE_ATTEMPTS`)
      so writes racing a tablet **split** — routed to the parent and truncated as
      the upper range moves to the new child — re-route to the elected child and
      land (idempotent per-key LWW), instead of surfacing "CP write did not commit
      in time". The dashboard's **Bulk seed** card chunks a
      larger total into requests, showing progress + refreshing the Tablets view so
      splits appear live. Combined with the binary's **`--cluster N --auto-split K`**
      flag (a CP-hosting node splits a tablet it leads once it exceeds K keys, Phase
      2.4, via `start_cluster_with_auto_split`), seeding past K auto-shards the
      keyspace — verified end to end (seed 12k keys, `--auto-split 4000` → 5 tablets).
      `tests/admin_endpoint.rs::admin_seed_writes_synthetic_keys`.
  - **Gotcha — `/admin/raftkv` is node-local, but in a single `--cluster N` process
    the shared `ClusterEdgeState` registers *every* node's CP group handle, so one
    node's view lists all replicas; a one-process-per-node deployment (separate edge
    each) shows just the local group.** A storage route resolves the tablet's *local*
    handle (`edge.local_cp`), so `--cluster` mode targets the first-registered
    replica's engine, not necessarily this node's — scrape per-process for true
    node-local storage debug (`tests/admin_endpoint.rs` uses `run_node` per node).
  - **Metrics are per-node sinks**: a follower's leader-only counters
    (`elections_won`, `append_entries_sent`) are legitimately 0, so `/admin/metrics`
    (and `/metrics`) is meaningful **per node** — scrape the control leader for the
    leader-only counters (the test asserts election counters only on the leader).
- **A `GET /metrics` admin route shares the DynamoDB HTTP listener** (ADR 0015) —
  the line-oriented metrics export stays on the dynamo port (the dedicated admin
  port above serves the richer JSON surface, incl. `/admin/metrics`). The DynamoDB edge's request parser now
  captures the request method + path; a `GET /metrics` is answered with the
  text-format snapshot as `text/plain` (everything else is the existing
  `POST /` + `X-Amz-Target` DynamoDB protocol). The body is **aggregated across the
  node's two role sinks** (control / raftkv) by `ClientCtx::metrics_text`: each role
  records into its **own** `ProdEnv` sink (`RaftNode::start` → `control_env.metrics()`;
  the CP group → the raftkv env's), so the handler snapshots both **at request time**
  (live, not cached), sums the counters, and takes the max leadership gauge. The
  raftkv sink is captured in `start_with` before its env is moved and threaded into
  `ClientCtx`. The endpoint is on `Node::dynamo_addr()` (`curl -s <dynamo addr>/metrics`).
- CP writes need **no client-assigned version**: the Raft log index *is* the MVCC
  version, so per-key LWW reproduces the agreed Raft order. (The v0 AP path derived
  a quorum version via `read_version`+1; that is gone with the AP plane.)
- A CQL/DynamoDB read-modify-write is serialized per node behind `rmw_lock` so the
  linearizable CP read + CP write are **atomic per node**. Cross-node atomicity (a
  CAS on the CP group) is later v1 work.
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
  client-facing listener tasks (client/dynamo/cql/admin, on plain `tokio::spawn`) and
  calls `ProdEnv::shutdown()` on each of the two internal role envs (control +
  raftkv), which aborts every task they own (the two Raft drivers + internal accept
  loops). This frees all six listener ports so a replacement node can rebind the
  same addresses on the same data dir — the clean teardown a stopped OS process
  would provide. On-disk state is untouched (a value acked to a client was Raft-
  committed + WAL-fsynced before the ack, so it survives). Wired to the Ctrl-C path
  in `main`. Dropping a `Node` without `shutdown()` still leaves its detached tasks
  running (they hold the ports), so call `shutdown()` to restart in-place.

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
`0` on a follower), `tests/cp_plane.rs` (CP round-trip: write via one node, read via
another — the CP group is the single source of truth), `tests/cp_cross_process.rs`
(cross-process CP forwarding to the leader's node — including the **derived-member-id**
regression: a *second* provisioned table's group speaks `cp_member_id`-derived ids, so
`cp_forward_target` must translate its leader hint back to a base id via `cp_base_id`
before the `client_route` lookup; the first table rides the bootstrap group where
member == base and can't catch this), `tests/admin_endpoint.rs` (the
admin / debug interface, ADR 0020: a per-process 3-node cluster, then the read-only
views config/status/raft/raftkv/storage·wal/metrics/health over the dedicated admin
port + the `storage/flush` action observed via `storage/lsm`; metrics asserted on
the control leader since sinks are per-node; bring-up wrapped in the port-TOCTOU
retry), `tests/dashboard_endpoint.rs` (the web dashboard, ADR 0021: `GET /` serves
the embedded SPA as `text/html`, `/admin/*` responses carry the CORS header, an
`OPTIONS` preflight returns 204, and `/admin/peers` lists all 3 nodes' admin
addresses — the fan-out seed), and `tests/self_heal.rs` (a
concurrent-client smoke test that the assembled node does not deadlock under load).
All use real TCP/time, so they poll with timeouts, not deterministic assertions. The restart test runs both incarnations in the **same** runtime,
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
# and an admin / debug endpoint (ADR 0020) — read-only introspection + actions:
curl -s <admin addr>/admin/status        # full cluster metadata
curl -s <admin addr>/admin/raftkv        # per-tablet CP group Raft state
curl -s '<admin addr>/admin/storage/wal/segment?tablet=1&seg=0'  # decoded WAL
animus admin status <admin addr>         # same, via the CLI
```
