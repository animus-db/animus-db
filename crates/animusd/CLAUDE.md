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
  dir)` binds *this* node and starts it. **ADR 0035 PR2 (config/identity
  decoupling)** adds `RoleAddrs.role: NodeRole` (`Control`/`Data`/`Both`,
  default `Both` — every JSON config before this field existed, and every
  entry point today, is `Both`): `control`/`raftkv` are now `Option<SocketAddr>`
  (`None` when that role isn't run), with a custom serde default
  (`Some(ephemeral)`, not the blanket `None` a bare `#[serde(default)]` would
  give — see the root `CLAUDE.md`'s entry on this) so an old config missing
  the field entirely still means combined mode. `ClusterConfig::control_ids`/
  `raftkv_ids`/`control_peer_book`/`raftkv_peer_book` are now role-filtered
  (identical output for an all-`Both` config); `BoundNode::start_with` takes
  an explicit `data_raftkv_ids` parameter (what `bootstrap` auto-registers as
  `Active` data members) instead of deriving it from `control_ids.len()`, so
  a caller can scope it to only the data-role nodes — every real entry point
  still passes the same set as before (combined mode is unchanged byte-for-
  byte). `Node::bind` (combined mode) still requires both addresses present
  (`Both`) — **ADR 0035 PR3** adds the control-only sibling,
  `Node::bind_control` → `BoundControlNode::start_control_with` (below), and
  **ADR 0035 PR4** adds the data-only sibling, `Node::bind_data` →
  `BoundDataNode::start_data_with` (below). `ClusterConfig::generate_split`
  is wired into `gen-config --control-nodes/--data-nodes` (PR3); both its
  control- and data-role entries are now runnable, via `animusd control` and
  `animusd data` respectively, targeting the *same* config.
- **`Node::bind_control` → `BoundControlNode::start_control_with` — the
  control-only counterpart of `Node::bind`/`BoundNode::start_with` (ADR 0035
  PR3, `animusd control`).** Binds only the control internal `ProdEnv` role
  plus the client + admin TCP listeners — no `raftkv` env, no dynamo/cql
  listeners, no CP storage engine. `run_node_control(config, index, dir)` is
  the `run_node`-shaped top-level entry point; CLI: `animusd control --config
  FILE --node I [--dir DIR]`. Both `BoundNode::start_with` and
  `BoundControlNode::start_control_with` build their `ClientCtx` and spawn
  the tasks every node shape needs (`route_sync_loop`/`metrics_sample_loop`/
  this node's own `register_node_addrs` self-registration/`serve_clients`/
  `admin::serve`) through one shared private helper, `spawn_common_tail` —
  everything role-specific (`bootstrap`/`peer_sync_loop`/the growth-node
  mirror/`heartbeat_loop`/the tablet-host reconciler/`auto_split_loop`/the
  dynamo+cql listeners) stays in `start_with` alone, appended after the
  shared tail returns. **No new rejection code was needed on the client
  dispatch side**: `Status`/`ProposeSchema`/`JoinInfo`/`SplitTablet`/
  `MergeTablets` only ever touch control `Metadata` (already correct on a
  control-only node, which has real local control Raft, just no data role);
  the data ops (`Put`/`Get`/`Scan`/`Delete`/`PutBatch`) degrade exactly like
  any other node that hosts zero local replicas — `resolve_cp_route`'s
  `ClientCtx.data == None` case falls straight into the existing "no local
  replica, forward via `client_route`" branch (see `ClientCtx::data`'s doc
  for the `Option<DataRole>` split this relies on, under "What's
  non-obvious"). `tests/control_only.rs` covers a bare control-only cluster
  (leader election + `/admin/status`/`/admin/health`/`/admin/config` +
  quiescence with zero data members), schema DDL direct-propose + follower
  relay against a control-only cluster, and a mixed cluster (a control-only
  trio plus one combined-mode data node reached via the ADR 0030 growth-node
  mirror — the mechanism PR4 below generalizes into a real data-only node —
  proving a `Put` issued against the CONTROL node's client port provisions +
  forwards to the data node and a schema command issued against the DATA
  node relays to the control leader).
- **`Node::bind_data` → `BoundDataNode::start_data_with` — the data-only
  counterpart of `Node::bind`/`BoundNode::start_with` (ADR 0035 PR4,
  `animusd data`).** Binds only the `raftkv` internal `ProdEnv` role plus the
  client/dynamo/cql/admin TCP listeners — **no control env, no local control
  `RaftCore` at all**. This node's `ClientCtx.control` is
  `ControlHandle::Remote(RemoteControlClient)` (`control_handle.rs`), not
  `Local`: it reaches the separately-deployed control plane exclusively over
  the network, via `control_seeds` (the control deployment's **client**-API
  addresses, a distinct axis from the internal `raftkv`-env peer book —
  see `run_node_data`'s doc for why this node's `raftkv` env peer book must
  still be `ClusterConfig::peer_book()`, the union with the control
  addresses, not `raftkv_peer_book()` alone: `heartbeat_loop` sends to
  `control_ids` over that very env). `run_node_data(config, index, dir,
  backend)` is the `run_node`/`run_node_control`-shaped top-level entry
  point; CLI: `animusd data --config FILE --node I [--dir DIR]
  [--ephemeral]`. Spawns exactly what `BoundNode::start_with` spawns for a
  data-role node minus everything control-plane-specific (`bootstrap`,
  `edge.register_control` — there is no local control handle to register):
  `spawn_common_tail`'s shared tail, `peer_sync_loop`, the (now-generalized,
  see "What's non-obvious") `remote_metadata_sync_loop`, a relayed
  `admin_add_member` self-registration, `heartbeat_loop`, the tablet-host
  reconciler, `auto_split_loop`, and the dynamo/cql listeners.
  `tests/data_only.rs` covers a genuine split cluster (3 control-only + 2
  data-only nodes, no combined-mode node anywhere): reads/writes across two
  data nodes (including a write via one and a read via the *other*, and
  polling `/admin/health`'s `hosts_cp` to converge — see its own doc on why
  that must be a poll, not a snapshot, right after a just-provisioned
  tablet's first write), schema DDL issued against a data node relaying to
  and committing on the control leader, a data node falling over to a
  remaining control seed when one control node goes down, and a data-node
  restart re-hosting its tablet from the surviving replica's Raft
  replication (not local state — the test uses the ephemeral memory backend
  specifically to prove this is real catch-up, not a local reopen).
  `run_node_data` (`--config`-based) covers only PR4's shape; ADR 0035 PR5
  adds the seed/join sibling, `run_node_data_join` (`animusd data --seed`) —
  see its own "What's non-obvious" entry below.
- `bind_cluster` / `start_cluster` — spin up an in-process cluster (the binary's
  `--cluster N` mode and `tests/cluster.rs`).
- **`start_split_cluster_with` — the in-process, single-command counterpart of
  a genuine split deployment (`animusd --cluster-control N --cluster-data M`,
  symmetric with `--cluster N`).** Binds `control_n` control-only nodes
  (`Node::bind_control`/`BoundControlNode::start_control_with`) followed by
  `data_n` data-only nodes (`Node::bind_data`/`BoundDataNode::start_data_with`)
  in one process — no combined-mode node anywhere — each with its own
  `ClusterEdgeState` (ADR 0031 PR2 doctrine: never shared), the same
  `dir/node-{index}` layout and control/raftkv id convention (`config::
  control_id`/`config::raftkv_id` over indexes `0..control_n` then
  `control_n..control_n+data_n`) `bind_cluster`/`ClusterConfig::generate_split`
  already establish. Every data node's `raftkv` env peer book is the union of
  the control and raftkv peer books (`ClusterConfig::control_peer_book`'s doc
  explains why `raftkv_peer_book()` alone isn't enough — `heartbeat_loop`
  targets the control ids over that same env); `backend` and both auto-split
  thresholds apply to the data nodes only. `tests/cluster_split.rs` is the
  in-process regression (`animusd control`/`animusd data` real-process split
  is `tests/split_cluster.rs`'s job instead). It originally flaked under
  `cargo test`'s parallel load on a write issued through a single **fixed**
  control-only node's client address, hitting the documented "zero-replica
  blind-forward" hazard (root `CLAUDE.md`): a control node forwards to *some*
  known replica of the tablet, not necessarily its leader. **That hazard is
  now closed at the source** (`ClientCtx::cp_forward` retries a "not the
  leader here" refusal at the refusing node's own embedded leader hint, then
  at the tablet's other replicas — see the root `CLAUDE.md` entry), so the
  test asserts through a single fixed control-only address deterministically;
  `fixed_control_node_write_read_is_deterministic` is the dedicated
  regression (20 keys through one fixed control node, no round-robin).
- `run_node_growth` — start a node as an ADR 0030 **growth member** from an
  operator-assembled *expanded* config (its control role is a permanent
  non-voter of the pre-growth control group; `start_with` detects this and
  spawns `remote_metadata_sync_loop`). **`run_node_join`** — the ADR 0032 PR2
  **seed/join** variant: the node starts knowing only its own `RoleAddrs` +
  a seed list (any existing nodes' **client** addresses), fetches
  `ClientRequest::JoinInfo` from a seed (the pre-growth `control_ids` + the
  answering node's peer book + its live `client_route` + admin addrs — any
  node answers from its own knowledge, no forwarding), runs a **collision
  guard** against a `Status` reply's `node_addrs` (an identical entry at my
  raftkv id = a rejoin, proceed; a different one = fail `AlreadyExists`
  before binding anything), then calls the exact same `start_with` shape
  `run_node_growth` does. **Every growth node self-registers its membership
  automatically**: `start_with`'s growth-node block (the
  `!control_ids.contains(&control_id)` branch) spawns a one-shot
  `admin_add_member` (idempotent `UpsertMember{Down}`, relayable) alongside
  `remote_metadata_sync_loop` — so neither growth entry point needs a
  separate `POST /admin/member/add` (still supported; now an idempotent
  no-op confirmation, which `tests/cluster_growth.rs` keeps exercising).
  CLI: `animusd join --seed ADDR[,ADDR...] --node I [--ip A] [--base-port P]
  [--dir D] [--ephemeral]` binds six consecutive ports from `--base-port`
  (default `7100 + 6*I`, mirroring `gen-config`'s stride/role order).
  `tests/seed_join.rs` covers happy path / collision / rejoin.
  **`run_node_data_join`** (ADR 0035 PR5, `animusd data --seed`) is the
  data-only sibling — see its own doc + the "What's non-obvious" entry
  below; it reuses this entry point's discovery/collision-guard logic via
  two factored-out helpers rather than duplicating it.
- `ClientRequest` / `ClientResponse` + `read_frame` / `write_frame` — the
  length-prefixed JSON client protocol (reused by `animus-cli`).
  `ClientRequest::JoinInfo` → `ClientResponse::JoinInfo` is the join
  discovery pair (ADR 0032 PR2, above).
- `dynamo` module — the **DynamoDB JSON-over-HTTP endpoint** (a fifth listener
  per node). A hand-rolled HTTP/1.1 server decodes `X-Amz-Target` +
  AttributeValue-JSON via `animus_dynamo::wire`, then routes through the **same
  `ClientCtx`** as the plain-TCP API. v1 (ADR 0019): reads/writes/scans go to the
  **CP plane** (`ClientCtx::cp_read`/`cp_write`/`cp_scan`), not the AP coordinator.
- `admin` module — the **admin / debug HTTP-JSON endpoint** (ADR 0020), a dedicated
  sixth listener (`RoleAddrs.admin`). Read-only introspection (config, status, both
  Raft layers, LSM/WAL debug, metrics, health) + gated operator actions
  (split/merge/flush/compact/reconfigure/drain). `http` module — the shared hand-rolled
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
  (`cp_delete`). The keyspace set + prepared-statement store are **per-node edge
  state** (see below).
- `otel` module — OpenTelemetry-compatible distributed tracing (ADR 0027).
  `init_tracing(instance_id)` (called once, from `main.rs`) installs the process
  subscriber: the existing stdout `fmt` layer plus, when `OTEL_EXPORTER_OTLP_ENDPOINT`
  is set, an OTLP/HTTP span exporter — opt-in, no-op by default, same doctrine as
  the ADR 0015 metrics seam. `current_traceparent`/`set_parent_traceparent` are the
  inject/extract primitives `cp_forward`/`handle_client` use to carry trace context
  across a forwarded cross-process hop (see below). `init_tracing_with_endpoint` is
  the test-facing seam (explicit endpoint, no process-env mutation — `set_var` is
  `unsafe` and this workspace forbids `unsafe_code`); see
  `tests/otel_tracing.rs`. **Scoped to this crate only** — no other crate depends on
  `opentelemetry*`.

## What's non-obvious

- **`ClientCtx.control` is a `ControlHandle`, not a bare `RaftNode<ProdEnv>`
  (ADR 0035 PR1, `Remote` added PR4)** — `control_handle.rs`. Reads split by
  freshness contract: `metadata_cached()` (staleness-tolerant;
  `ClientCtx::effective_metadata()` layers the ADR 0030 growth-node mirror on
  top of it) vs. `metadata_fresh()` (read-your-writes, never
  mirror-substituted — used by the schema commit-wait polls, the DynamoDB
  conditional-write existence gate, and — fixed alongside PR4, see below —
  `provision_tablet`'s initial replica-set read). Both are identical
  (`raft.metadata()`) for `Local`; `Remote` genuinely differs, per below.
  `ClusterEdgeState::leader_handle()` deliberately stays a concrete
  `RaftNode<ProdEnv>` registry — proposing is inherently local-Raft-log-only,
  so it never goes through `ControlHandle`.
- **`ControlHandle::Remote(RemoteControlClient)` (ADR 0035 PR4) is a
  data-only node's *entire* control-plane access — no local `RaftCore`, no
  local WAL, nothing to flush on shutdown.** `RemoteControlClient` holds:
  `seeds` (the control deployment's static **client**-API addresses — the
  discovery root), a polled `mirror: Arc<Mutex<Option<Metadata>>>`
  (`metadata_cached()` returns its last value, or default-empty until the
  first sync — `has_synced()` tells the two apart), and a `leader_hint:
  Arc<Mutex<Option<(NodeId, SocketAddr)>>>`. **`metadata_fresh()` is now
  `async`** (a real network round trip for `Remote`; a synchronous-in-
  substance passthrough for `Local`) — this rippled `propose_and_await`'s
  `committed` parameter from a plain `Fn() -> Option<T>` closure to `Fn() ->
  Fut where Fut: Future<Output = Option<T>>`; every call site's predicate
  became `|| async { ... }` (a *sync* closure returning an async block — no
  edition-2024 async-closure syntax needed, just the pre-existing "return a
  future from a plain closure" idiom), with zero behavior change for any
  existing (`Local`) node.
  - **The leader-hint lifecycle (ADR 0035 §1).** `ClientResponse::Status`
    changed from a bare tuple `Status(Metadata)` to a struct variant
    `Status { metadata, leader_hint: Option<(NodeId, SocketAddr)> }`
    (`#[serde(default)]` on the new field). The answering node fills
    `leader_hint` from its own `self.control.leader()` +
    `ClientCtx::route_addr(leader_id)` (`ClientCtx::control_leader_hint`) —
    the exact lookup `propose_schema`'s relay tier already did. Every
    `Status` consumer (the generalized `remote_metadata_sync_loop`, a live
    `metadata_fresh` fetch, `run_node_join`'s collision guard, the CLI, both
    combined-mode/per-process test files) now destructures the struct
    shape. `RemoteControlClient::metadata_fresh()` tries the current hint
    first, falls back to scanning every seed, and refreshes both the mirror
    and the hint from whichever reply lands — mirroring, not reusing (this
    handle has no `ClientCtx` to call through), `propose_schema`'s own
    hint-first-then-broadcast shape; it does **not** independently verify
    the responder self-reports as leader (documented as a known looseness,
    flagged for the PR5 staleness audit — a stale hint self-heals within a
    couple of hops since a non-leader's own reply carries *its* hint).
    `propose_schema` itself gained a new tier, `ControlHandle::
    leader_addr_hint()`, tried **before** the existing `leader()` +
    `route_addr()` lookup — the hint is strictly fresher for a data-only
    node (same `Status` reply that filled the mirror) and is `None`
    unconditionally for `Local`, so this changes nothing for any prior node
    shape. `ClientCtx::not_leader_error()` also uses the hint to give
    `admin_drain`/`admin_remove_member`'s "not the leader" refusal a
    concrete retry address when one is known.
  - **Every other `ControlHandle` method degrades to an honest, inert value
    for `Remote`**: `is_leader() → false`, `role() → Follower`, `term()`/
    `commit_index()`/`durable_index()`/`snapshot_index()`/`log_len()` → `0`,
    `config() → {}` (empty), `believes_alive() → false`, `metrics()` → a
    permanent `MetricsHandle::noop()`. `metadata_watch()` returns a fresh
    `MetadataWatch::default()` — since nothing ever calls `bump` on it,
    `changed()` never resolves, which is *exactly* the desired effect: no
    special-casing needed in `tablet_host_reconciler_loop`, whose `select!`
    then always falls through to its `RECONCILE_FALLBACK_INTERVAL` poll for
    a data-only node. `Node.raft` itself is now a `ControlHandle` (not a
    bare `RaftNode<ProdEnv>`); `is_control_leader`/`metadata`/`propose_meta`/
    `shutdown_graceful` each pattern-match or delegate accordingly
    (`propose_meta` returns `false` and `shutdown_graceful` skips the WAL
    flush for `Remote` — proposing/flushing are inherently local-Raft-log
    operations a `Remote` handle has none of).
  - **The tablet-host reconciler's pre-recovery guard gained a third OR-term,
    `ControlHandle::has_synced_metadata()`.** The guard used to be
    `last_applied() == 0 && ctx.remote_metadata.is_none()` (skip until *some*
    trustworthy view exists — real recovery, or the ADR 0030 growth mirror).
    A `Remote` handle's `last_applied()` is pinned at `0` **forever** (no
    local log to apply at all) and it never touches `ctx.remote_metadata`
    (that field is the ADR 0030-specific mirror; `Remote` keeps its own,
    read straight through `metadata_cached()`) — so without a third signal
    tied to `Remote`'s *own* readiness, the guard would never release a
    data-only node's reconciler, ever. `has_synced_metadata()` is `false`
    unconditionally for `Local` (its two existing signals already cover it)
    and mirrors `RemoteControlClient::has_synced()` for `Remote`.
  - **A real, pre-existing race this PR's own tests caught and fixed:
    `provision_tablet`'s "no tablet yet" branch picks the table's *initial,
    permanent* replica set from an Active-member scan.** `CreateTablet` only
    ever succeeds once per table (idempotent, first-committer wins) — so
    reading a stale `Metadata` view at exactly the wrong moment doesn't
    cause a transient hiccup a later retry heals, it silently and
    **permanently** under-replicates the tablet (nothing ever re-checks and
    grows a already-recorded RF policy). This was already a latent,
    vanishingly-rare hazard for `Local` (real control-Raft replication lag
    is sub-millisecond) — invisible until a data-only node's *routinely*
    poll-interval-stale mirror (ADR 0035 §5) made the window wide enough to
    hit reliably: `tests/data_only.rs`'s two-data-node split-cluster test
    flaked (RF pinned at 1 instead of 2) on almost every other run before the
    fix. Fixed by switching `provision_tablet`'s read from
    `metadata_cached()` to `metadata_fresh().await` — the same "the
    decision that becomes permanent must observe committed state, not a
    tolerant mirror" principle already applied to the schema commit-wait
    polls, just not yet to this one. **General lesson: when a routine
    (not just a rare crossover) staleness window opens up for the first
    time (ADR 0035 §5's own thesis), re-audit every `metadata_cached()`/
    `effective_metadata()` call site that feeds a *non-retried, permanent*
    decision — not just the ones already flagged as commit-wait polls.**
  - **The PR5 staleness audit this lesson called for found and fixed four
    more instances of the same shape** (all four had the identical
    signature: a `self.control.metadata_cached()` read feeding a decision
    with no re-check, permanently empty on a growth node, harmless on a
    `Remote` node since its `metadata_cached()` *is* the mirror already):
    `cp_scan`'s tablet-range computation (would silently return an empty
    result forever, not error or wait); `trigger_split`/`trigger_merge`'s
    `expected_epoch`/`new_id` precondition reads *and* their confirm-poll
    closures (would unconditionally return "no such tablet" before ever
    proposing, on every call — the epoch-CAS at apply time protects against
    a *concurrent* stale read racing another proposer, it cannot rescue a
    read that has nothing to see at all); `drop_table`'s `DropTableTablets`
    confirm poll (would report a **false success** on the very first poll,
    not merely time out, since "no tablets found" is indistinguishable from
    "already dropped" when the view is permanently empty); and
    `create_keyspace`'s pre-check + confirm poll via `has_keyspace` (would
    never resolve, so `CREATE KEYSPACE` always timed out even after
    genuinely committing — fixed by having `create_keyspace` go through
    `metadata_fresh()` directly, the same RYW split `create_table_schema`
    already used, and switching the now-unused `ClientCtx::has_keyspace`
    wrapper itself to `effective_metadata()` before removing it as dead
    code). Also hardened, a related but distinct class (not staleness, a
    *local-replica* freshness-ordering bug): `admin_drain` read
    `self.control.metadata_cached()` for its member lookup *before* checking
    `self.edge.leader_handle()`, unlike its sibling `admin_remove_member`
    (already fixed for exactly this — see the root `CLAUDE.md`'s
    "decommission" engineering-practices entry) — reordered to check
    leadership first. **Left deliberately as-is, with a comment recording
    why**: `/admin/raft`'s `raft_view` reads `metadata_cached()` directly
    (it's a diagnostic of *this replica's own* Raft view, not a cluster-wide
    summary — `effective_metadata()` would be the wrong contract there, not
    a missed fix). **General pattern to keep watching for**: grep every
    `self.control.metadata_cached()` (not `effective_metadata()`/
    `metadata_fresh()`) call site whenever a new consumer of `ControlHandle`
    is added — the type system can't catch this, `Remote` and a genuine
    `Local` voter both compile and both look identical in a single-node test
    that never exercises the mirror-substitution paths at all.
- **Long-poll metadata watch (ADR 0035 PR5): `ClientRequest::WatchMetadata {
  last_seen: u64 }`.** Replaces the ADR 0035 PR4 fixed-200ms
  `remote_metadata_sync_loop` poll for a `Remote` data node with a real
  wake-on-commit signal, reusing the ADR 0031 §trigger `MetadataWatch`
  primitive across the wire instead of inventing a new push mechanism.
  - **Server side** (`ClientCtx::watch_metadata`): only a genuine
    `ControlHandle::Local` replica serves it — it parks on
    `self.control.metadata_watch().changed(last_seen)` racing a
    `WATCH_METADATA_SERVER_TIMEOUT` (8s) sleep, then replies with the
    current `Metadata` either way (a timeout is a normal "nothing changed
    yet" outcome, not an error — the caller just retries with the same
    `last_seen`). A `Remote` node **rejects** the request outright
    (`ClientResponse::Error`) rather than degrading: its own
    `ControlHandle::metadata_watch()` is itself driven by replies to *this
    exact request*, so serving it would only let a misdirected watch (a
    stale `client_route` entry) degrade silently to an ~8s effective poll —
    worse than the pre-PR5 fixed-interval poll, not better. Rejecting fails
    fast instead.
  - **Reply shape**: `ClientResponse::Status` gained a `watermark: u64`
    field (`#[serde(default)]`, so it decodes to `0` from an older reply) —
    the answering node's own `metadata_watch().latest()` at reply time, which
    the caller passes back as the next call's `last_seen`. Every `Status`
    reply carries it now, not just a `WatchMetadata` one, since both are the
    same response type.
  - **Client side** (`remote_metadata_watch_loop`, replacing the `Remote`
    branch of `remote_metadata_sync_loop`): tries the current leader hint
    first (mirroring `RemoteControlClient::metadata_fresh`'s own candidate
    order — the leader is the node most likely to have just applied the
    change being waited for), then every seed; on a successful round trip
    (whether via a real change or the server's own timeout) it loops
    straight into the next long poll — the server-side bound is itself the
    throttle. Falls back to a plain `Status` poll + a short backoff only
    when *every* candidate fails at the transport level (never busy-loops).
  - **`RemoteControlClient` now owns its own driven `MetadataWatch`**
    (previously `ControlHandle::metadata_watch()` handed a `Remote` node a
    permanently-inert default — see the PR4 entry above, now superseded):
    `observe()` calls `watch.bump(watermark)` on every reply, so
    `tablet_host_reconciler_loop`'s `select!` on this now wakes on a real,
    network-relayed metadata change for a data-only node too, not only via
    its `RECONCILE_FALLBACK_INTERVAL` sleep arm. This required making
    `animus_control::MetadataWatch::bump` **`pub`** (it was crate-private,
    called only by the control driver's own loop) — a small, safe widening:
    the primitive's contract (fetch-max, wake-if-advanced) doesn't change,
    only who may call it from outside `animus-control`.
  - **Non-regression guard, found necessary while wiring this up**: *any*
    control node may answer a `Status`/`WatchMetadata` request, not
    necessarily the most caught-up one — the pre-PR5 poll already had this
    property (whichever seed answered first won, unconditionally), but PR5's
    watermark gave it, for the first time, something to check *against*.
    `RemoteControlClient::observe` now skips the mirror-overwrite (and the
    watch bump) unless `watermark >= watch.latest()`, so a reply from a
    replica lagging behind one this handle already saw can't regress the
    mirror's *content* even though the watch's watermark is itself
    monotonic (`fetch_max`) regardless — without the guard those two could
    silently disagree (the watch says "I've seen index 50," the mirror
    actually holds the state as of index 40). The leader hint is still taken
    unconditionally (it self-heals independently, see below) — only the
    metadata snapshot + watch pairing is guarded.
  - **Known, audited, and left as-is**: the leader-hint "trust whoever
    answers" looseness PR4 flagged. Verified every consumer (`propose_schema`,
    `RemoteControlClient::metadata_fresh`, the new watch loop) degrades to a
    seed-scan/broadcast tier on a transport failure, and confirmed *why* a
    stale hint self-heals in practice rather than compounding: a control
    node's own `leader()` is kept current by real Raft heartbeats/
    AppendEntries (not the ADR 0035 mirror-poll-interval class of
    staleness), and the periodic full-seed-list sync — not just the hint —
    refreshes it from whichever node answers, within one sync cycle even if
    the hinted address itself has gone unreachable. No code change needed;
    the existing design already degrades soundly.
- **`animusd data --seed` (ADR 0035 PR5, `run_node_data_join`) — the
  data-only counterpart of `animusd join`/`run_node_join`.** Reuses that
  entry point's `JoinInfo` discovery + `Status` collision guard **verbatim**
  via two factored-out free functions, `discover_join_info`/
  `check_join_collision` (both entry points previously duplicated this
  poll/match/error-format logic inline) — then constructs the `Remote` data
  assembly (`Node::bind_data` → `BoundDataNode::start_data_with`) instead of
  a combined-mode node with a local control `RaftCore`. `control_seeds` (the
  discovery root `RemoteControlClient::new` needs) is derived the same way
  `run_node_data` derives it from a static `ClusterConfig`, just from the
  discovery-built `client_route` map instead: filter `original_control_ids`
  through it. CLI: `animusd data --seed ADDR[,ADDR...] --node I [--dir D]
  [--ephemeral]`, mirroring `animusd join`'s port-derivation convention
  (`--base-port`, default `7100 + 6*index`) minus the control port a
  data-only `RoleAddrs` never binds. `tests/data_join.rs` covers a data-only
  node joining a running split cluster (3 control-only + 2 data-only), being
  promoted `Active` with zero operator admin calls, and gaining a real
  rebalanced tablet replica — seeded across several independent tables per
  the standing "the rebalancer needs an imbalanced starting point" lesson
  (root `CLAUDE.md`), since this test's whole point is exercising the same
  rebalance-onto-a-grown-node path `tests/seed_join.rs` already proves for
  combined-mode joins.
- **`ClientCtx.data: Option<DataRole>` (ADR 0035 PR3)** groups every field
  that only makes sense on a data-role node — `rmw_lock` (the per-node RMW
  serialization lock), `raftkv_metrics` (the raftkv env's metrics sink), and
  `base_id` (this node's own raftkv id, used by routing) — behind one
  `Option`, so "does this node have a data role" is a single type-level fact
  instead of three loose optionals that could disagree. `None` on a
  control-only node. Access via `ClientCtx::data()`, which **panics** if
  absent — safe only from a path that structurally cannot run on a
  control-only node (the dynamo/cql wire edges, whose listeners are never
  bound there, or `auto_split_loop`, only ever spawned for a data-capable
  node). **`resolve_cp_route` is the one call site that must never panic**
  (it sits on the client-request dispatch path a control-only node genuinely
  reaches for `Put`/`Get`/`Scan`/`Delete`/`PutBatch`) — it matches on
  `self.data.as_ref()` directly instead, so `has_local_replica`/`is_replica`
  come out `false` for a control-only node exactly as they would for any
  other node hosting zero local replicas of a tablet. `AdminInfo.raftkv_id`/
  `raftkv_addr`/`dynamo_addr`/`cql_addr` are the sibling `Option`s on the
  admin-view side (`None` on a control-only node; `/admin/config` renders
  them as JSON `null`), and `Node.dynamo_addr`/`cql_addr` are `Option`
  internally but keep their public accessors returning a bare `SocketAddr`
  (panicking if absent) so every existing combined-mode caller is
  unaffected — the same "expect() internally, don't change combined mode's
  public surface" pattern.
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
  per-tablet Raft group** (`animus-cp-data`), reached through five `ClientCtx`
  primitives that all resolve the leader the same way (`cp_route`): `cp_read`
  (linearizable ReadIndex), `cp_write` / `cp_delete` (Raft-committed, waited to
  durable+applied — durable-before-ack), `cp_scan` (linearizable range read), and
  **`cp_batch_write`** (bulk-write batching, ADR 0017): it **groups keys by tablet**
  and commits each group as **one `KvCommand::Batch` Raft entry** on that tablet's
  group leader (one consensus round for the whole group; forwarded via
  `ClientRequest::PutBatch` if this node isn't the leader), waited to durable+applied.
  Atomic **within** a tablet (one entry), non-atomic **across** tablets — matching
  DynamoDB `BatchWriteItem` semantics. The DynamoDB `BatchWriteItem` edge (a
  `Delete` is a tombstone-*value* write, so puts + deletes ride the same batch) and
  the admin bulk seeder (`SEED_BATCH_SIZE` keys per entry) both route through it.
  `cp_route` serves **locally** if this node hosts the leader, **forwards** to the
  leader's node if a local replica gives a leader hint + a `client_route` exists
  (ADR 0017 #3b cross-process, wrapped in `ClientRequest::Forwarded { request,
  traceparent }`, one hop — the `traceparent` field is ADR 0027: `handle_client`
  wraps every accepted request in a `client_request` span, `cp_forward` injects
  that span's W3C trace context onto the wire via `otel::current_traceparent`, and
  the receiving node's `handle_client` re-parents its own `client_request` span
  from it via `otel::set_parent_traceparent` *before* dispatching to
  `cp_serve_forwarded` — so a forwarded write is one joined distributed trace
  across both nodes when OTLP export is enabled, `None`/no-op otherwise), and
  otherwise **waits** for the local group to elect (it never forwards a CP op to a
  non-leader — including itself — during election). **Every data op — the wire edges
  (DynamoDB, CQL) and the plain-client `Put`/`Get`/`Scan`/`Delete` — routes through
  these.** The optional `table` no longer selects a plane (there is only the CP
  plane). The edges create their tables in `ReplicationMode::Cp` (the mode is
  recorded for truthfulness, but routing no longer depends on it). A just-proposed
  write is confirmed via a **local** read on the leader (not a quorum barrier —
  the leader applies only after a quorum commit + WAL fsync, so a local read
  reflecting the value means it's durable; a per-write barrier would not scale
  under concurrent load). The confirm loop polls at a **fine adaptive interval**
  (`CP_CONFIRM_POLL_INIT` ~200µs, doubling to a `CP_CONFIRM_POLL_MAX` 5ms
  ceiling), *not* the coarse 50ms `SCHEMA_POLL_INTERVAL` — paired with cp-data's
  wake-on-propose, a lone write returns in ~1ms instead of eating a fixed 50ms
  floor (`cp_plane.rs::single_write_latency_is_low`). Every table's tablet(s) are
  keyed by `TabletId` in the edge registry (`ClusterEdgeState`); a fresh table's
  first tablet, a split-minted sibling, and a reconciler-placed replacement all
  reach it the same way — the per-node **tablet-host reconciler** (ADR 0031 PR4, below). `tests/cp_plane.rs`
  (in-process round-trip) + `tests/cp_cross_process.rs` (forwarding) + the
  dynamo/cql wire + schema tests all exercise the CP path.
- **Tablet split (ADR 0028) is a single, atomic control-plane command — there
  is no data-plane half.** Since every tablet a node hosts shares one
  `raftkv` env (ADR 0026 Stage B, stream-addressed) and one shared storage
  engine confined per-tablet by `StorageScope` (ADR 0026/0028), `ClientCtx::
  trigger_split` just proposes `MetaCommand::SplitTablet` (epoch-CAS gated
  exactly like `CasTabletReplicas`) and waits for it to commit — the source
  tablet's range narrows and a new sibling tablet is minted covering the
  upper range, both immediately servable from the same already-populated
  engine. Commit of this one command *is* the whole operation: there is no
  second step to fail independently, so a metadata-only, leaderless orphan
  tablet is now **structurally impossible** — an entire class of bugs this
  file used to document at length (two-phase split failure, `Coresident`
  sibling-pool exhaustion, derived member ids, a `pending`-retry map, a
  cluster-wide auto-split claim, `DropOrphanTablet` cleanup) no longer has
  any mechanism left to apply to. That history is preserved in the root
  `CLAUDE.md` Engineering Practices section and ADR 0017's original text for
  archaeology. `animus-cp-data/CLAUDE.md` covers `StorageScope`/fencing/stream
  addressing; the per-node hosting mechanics are the tablet-host-reconciler entry
  below; `auto_split_loop`'s current (single-step) shape is documented at its
  definition in `lib.rs`.
- **Tablet merge (ADR 0033) is split's dual — also a single, atomic
  control-plane command, wired all the way through the reconciler.**
  `ClientCtx::trigger_merge` resolves both tablets' current epochs from one
  `Metadata` snapshot and proposes `MetaCommand::MergeTablets` (epoch-CAS
  gated on **both** tablets, and rejecting a cross-table merge — a hardening
  this feature added over the original, long-unwired command); commit widens
  `left`'s range to absorb `right`'s and removes `right`, recording it in the
  new `Metadata::merged_tablets` (a tiny, never-pruned marker — tablet ids are
  never reused, so an entry can never resurrect a wrong decision). Confirmed
  by polling for the exact pair of effects only this merge produces (`left`'s
  epoch advanced past what was read, **and** `right` is gone), robust against
  `right` vanishing for an unrelated reason mid-poll. Exposed as `POST
  /admin/tablet/merge {left, right}` and `ClientRequest::MergeTablets`
  (relayable, `is_relayable_command`, mirroring `SplitTablet`), plus `animus
  admin merge <admin-addr> <left> <right>` on the CLI. **The data-plane
  reaction is two new `animus_cp_data::host` planner actions** (ADR 0031's
  `plan`, extended): `HostAction::WidenScope` (the dual of `NarrowScope` — an
  already-hosted tablet whose metadata range *grew* widens its live
  `StorageScope` to match, via the new `RaftKvNode::widen_scope`) and
  `HostAction::Absorb` (the dual of `Reclaim` — a hosted-but-now-vanished
  tablet recorded in `Metadata::merged_tablets` is torn down **without
  erasing its data**, unlike `Reclaim`, since a sibling now serves that range
  on the very same node-shared engine). **Why a new replicated marker was
  needed instead of just inferring "merge" from the tablet map**: a
  hosted-but-absent tablet looks identical whether its whole table was
  dropped or it was merged into a neighbor, and a naive "does some other
  tablet's range now cover mine" check is unsound — two different tables'
  still-unsplit tablets can have byte-identical default
  `KeyRange::whole()` ranges, with no table identity left in scope to
  disambiguate once the tablet itself is gone from the map. See ADR 0033 for
  the full design and `tests/tablet_merge.rs` for the end-to-end proof (split
  → write both sides → merge → all data readable through the survivor,
  including via a *different* replica than the one the merge was triggered
  on → the absorbed tablet's WAL reclaimed and gone from `/admin/raftkv` on
  every replica → survives a restart with no resurrection).
  **Automatic (size-based) merge triggering is explicitly out of scope** —
  operator-driven only, matching `auto_split_loop`'s absence of a symmetric
  auto-merge counterpart for this increment.
- **Every client-facing CP read runs the read-side scope pre-check +
  served/absent disambiguation (`cp_get_local`/`cp_scan_local`, ADR 0033) —
  the read dual of the ADR 0028 write fence bullet below.** Found by
  `tests/tablet_merge.rs` flaking ~1-in-5 in isolation (a flaky `ProdEnv`
  test is a real bug): a linearizable get through the merge survivor
  answered a definitive `Value(None)` for an acked pre-merge write. Two
  distinct false-"absent" channels were closed on the read path (the third,
  primary fix — the absorb drain — lives in `animus-cp-data`, see its
  `CLAUDE.md`): (1) a get/scan resolving to a group whose live
  `scope_range()` does not contain the requested key/window (routing raced a
  merge's widen or a split's narrow) now errors retryably instead of
  serving — for scans this also closes a **silent truncation**, since
  `linearizable_scan` filters rows through the live scope and an un-widened
  survivor would return partial results with no error at all; (2) a
  ReadIndex barrier failure (deposed/mid-election leader) is no longer
  collapsed into "absent" — the forwarded `Get` arm used to do exactly that
  (`ClientResponse::Value(leader.linearizable_get(..))`) while the `Scan`
  arm already errored; both now go through the shared helpers, and the
  collapsed `linearizable_get` has **no `CpGroup` wrapper at all** so the
  unsafe shape can't be reached in this crate
  (`RaftKvNode::linearizable_get_served` is the disambiguated primitive).
  `cp_read`/`cp_scan_one` retry the `"; retry"`-class errors internally with
  re-resolved routing (bounded by `CLIENT_TIMEOUT`), so the client-visible
  contract is unchanged — a read during a split/merge crossover waits
  instead of erroring or lying. The in-crate `split_fence_tests` regression
  drives both duals (get + scan) directly against a narrowed parent's
  handle, mirroring the write-side test in the same module.
- **The auto-split trigger is byte-based, not just key-count-based (ADR
  0034).** `--auto-split K` (keys) still works exactly as before;
  `--auto-split-bytes B` adds an independent byte threshold
  (`start_cluster_with_auto_split_bytes`, `BoundNode::start_with`'s new
  `auto_split_bytes_threshold` parameter) — either, both, or neither may be
  set, and either exceeding its threshold fires a split. The cheap per-tick
  gate now checks `CpGroup::approx_key_count` (LSM-only, unchanged) **and**
  `CpGroup::approx_bytes` (either backend — `animus-cp-data`'s
  `RaftKvNode::approx_bytes` over `StorageEngine::approx_bytes_in_range`, a
  new additive trait method with an exact default and a cheap `LsmEngine`
  override; see that crate's doc). **The split point changes with the
  metric**: a byte-configured cluster splits at the **byte-weighted median**
  (`byte_weighted_median`, private to `lib.rs` — unit-tested via an in-crate
  `#[cfg(test)] mod auto_split_median_tests`, the same "private fn, in-crate
  test module" shape `split_fence_tests` already established), not the plain
  positional median a key-count-only cluster keeps using unchanged. Getting
  this right took a second pass: a naive "walk pairs accumulating a running
  byte total, cut at the first key where it reaches half" implementation
  looks plausible and passes a quick sanity check, but is subtly wrong
  whenever one key's own value is a large fraction of the total — it always
  cuts *right after* the key that pushes the running total over half, even
  when cutting *right before* that key would land closer to an even split
  (e.g. tiny keys summing to 100 bytes, then a first huge key of 10,000
  bytes with total 20,104 and half 10,052: the naive walk returns the *next*
  key after that huge one, giving a 100-byte / 20,004-byte split — the huge
  key's own bytes always land on the larger side no matter which of its two
  neighboring cut points is actually closer to half). The fix scans every
  achievable interior cut point (a key boundary — a key's bytes can never be
  divided) and picks whichever prefix sum is *closest* to half, which is the
  best any key-boundary split can do. General lesson: a "weighted median via
  a single accumulate-and-threshold pass" is only correct when no single
  item can dominate half the total; once one can, compare the two
  candidate cuts *around* it (before vs. after) rather than committing to
  whichever side the accumulator happens to cross the threshold on. **The
  dashboard's Tablets view surfaces both dimensions** (`admin::CpRaftView`
  gained `byte_size` — the exact, `StorageScope`-scoped total summed from the
  same `local_pairs` scan `key_count` already reads, no extra engine call —
  alongside the pre-existing `key_count`): a "Size" column sits next to
  "Keys", each with its **own** over-threshold pill, since a byte threshold
  and a key threshold are independent OR-gated triggers on the backend (either
  exceeding its threshold fires a split, `auto_split_loop`) — one dimension
  being under its threshold must never hide the other being over.
- **Every CP write path stamps + pre-checks the ADR 0028 write fence** (fixed
  2026-08-07 — the fences existed and were unit-tested in `animus-cp-data`
  since the split redesign, but had zero real callers here: `cp_put_local`/
  `cp_delete_local`/`cp_batch_propose` called the *unfenced* `RaftKvNode::
  put`/`delete`/`put_batch`, so `fence = KeyRange::whole()` on every real
  write and the apply-time check was a permanent no-op). Now each of those
  three helpers reads the target group's own live `RaftKvNode::scope_range()`
  and (1) **rejects the write before proposing** if any key falls outside it
  — returning an ordinary routing-failure error so the caller's retry
  re-resolves `cp_route` and reaches the correct child instead of the write
  being silently accepted — and (2) stamps that same range as the proposed
  entry's `fence` via `put_fenced`/`delete_fenced`/`put_batch_fenced`
  (`CpGroup`'s unfenced `put`/`delete`/`put_batch` wrappers are gone — there
  is no unfenced real write path left). The pre-check is load-bearing, not
  redundant with the fence: `cp_put_local`/`cp_delete_local` confirm success
  by reading the proposed value (or its absence) back from **local**
  storage, and a fenced-out entry still commits and applies as a no-op — so a
  confirm keyed on any coarser signal (e.g. `engine_applied_index()` alone,
  which a no-op still advances) would have **falsely acked** a write that
  never happened; the pre-check keeps the actual failure mode "clean error,
  client retries" instead of "silent success, silent data loss." A window
  still exists between the pre-check read and the entry's actual apply (the
  scope can narrow further in between) — the embedded fence covers exactly
  that sliver, dropping such a write as a safe no-op rather than mis-applying
  it. See `animusd/src/lib.rs`'s `split_fence_tests` module (an *in-crate*
  test — it needs the private `CpGroup`/`ClientCtx` handles to drive a write
  directly against a specific tablet's group, which nothing under `tests/`
  can reach) and ADR 0028 §3's update note.
- **The cluster's members are the CP `raftkv` nodes, not the control ids.** The
  control ids `0..N` are only the Raft *consensus group* for metadata; `bootstrap`
  (leader-only, idempotent) registers the **raftkv ids** (`300+i`) as `Active`
  `Metadata` members. This keeps `metadata().members`/`status` meaningful and
  gives dynamic CP reconfigure a hook (`tablets[t].replicas`). **Data-node
  failure detection is wired over `ProdEnv`**: every node spawns
  `heartbeat_loop` on its `raftkv` env, heartbeating the control group *as its
  `raftkv` member id*, so the control leader's `detect_loop` marks a crashed CP node
  `Down` (`tests/cp_reconfigure.rs::data_node_failure_is_detected`). And **each
  node's tablet-host reconciler (ADR 0031 PR4, below) reconfigures every tablet
  whose group this node leads**: each tick's `HostAction::Reconfigure` carries
  `tablets[t].replicas` — since ADR 0026 Stage B a tablet's group member id *is*
  simply the base `raftkv` id, so the replica set needs no translation — and
  takes one single-server `reconfigure_step` toward it
  (`tests/cp_reconfigure.rs::cp_group_follows_tablet_replica_set`: dropping a follower
  from the replica set reconfigures the group's voters down). **The reaction is
  event-driven now** (`RaftNode::metadata_watch()` + a 500ms fallback tick),
  so the old cadence race against the control plane's policy `reconcile_loop`
  — which the pre-ADR-0031 `cp_reconfigure_loop` mitigated by polling at 150ms,
  a third of `RECONCILE_INTERVAL`, plus jitter (see the root `CLAUDE.md`
  engineering-practices entry, now historical for this pair of loops) — is
  closed structurally: the reconciler observes a replica-set change on the
  commit that made it, not on the next arbitrarily-phased tick. **The full
  failure→placement→reconfigure cascade is closed**: `bootstrap` attaches a
  label-free RF `PlacementPolicy`, so on a `Down` replica the placement
  reconciler picks an Active spare, the spare's tablet-host reconciler stands
  up an empty group, and the leader adds + catches it up — auto-replacing the
  dead replica end to end
  (`tests/cp_reconfigure.rs::failure_auto_replaces_replica_onto_spare`).
- **Online cluster growth (ADR 0030) is data-plane only — the control group
  stays static.** `POST /admin/member/add {node, labels?}`
  (`ClientCtx::admin_add_member`) registers a new **raftkv** id `Down` via the
  existing relayable-proposal path (`UpsertMember{status: Down}` was added to
  `is_relayable_command`'s allowlist, scoped to `Down` only); its own
  `heartbeat_loop` promotes it to `Active` on first contact (ADR 0012's
  unmodified detector — verified, not changed). A grown node starts via the new
  `run_node_growth` (alongside `run_node`/`run_node_with`): it binds from an
  **expanded** `ClusterConfig` (lists every pre-growth node plus every node
  added so far) but passes the **pre-growth** control group as `control_ids` —
  making its own control role a permanent, structurally-safe **non-voter**
  (the same safety property an already-removed voter relies on: `is_voter()`
  gates campaigning cleanly) that can never receive real Raft replication for
  a group it was never added to. `BoundNode::start_with` detects this itself
  (`!control_ids.contains(&self.control_id)` — no new parameter) and spawns
  `remote_metadata_sync_loop`, mirroring the real cluster's `Metadata` via
  `ClientRequest::Status` polls against the pre-growth nodes' client addresses
  (resolvable through the now-complete `client_route`) into
  `ClientCtx::remote_metadata`. `ClientCtx::effective_metadata()` transparently
  prefers this mirror when populated (a no-op passthrough to
  `self.raft.metadata()` on every other node) — **every call site a growth
  node needs to actually function reads through it**: `tablet_for`,
  `resolve_cp_route`, `tablet_host_reconciler_loop` (the load-bearing one — how
  a growth node ever learns it was placed on a tablet at all; its 500ms
  fallback tick is what fires there, since a growth node's own control raft
  never advances and so never wakes `metadata_watch`),
  `peer_sync_loop`, `register_node_addrs`'s own commit confirmation, `cp_put`/
  `cp_get`'s `has_table_tablet` gate, and `/admin/status`. `propose_schema`
  (the shared "propose locally if leader, else relay to a *known* leader"
  primitive) also gained a last-resort fallback — broadcast to every other
  `client_route` address when there is **no** locally-known leader at all
  (true forever for a non-participating growth node, since it never receives
  a heartbeat/AppendEntries telling it who leads) — without this, a growth
  node's own address self-registration could never reach the real cluster.
  **The residual gap this bullet used to document — a pre-growth node's
  `client_route` being a static, process-start-only snapshot that could never
  forward to a tablet leader on a node grown in afterward — is closed by ADR
  0032 PR1** (see its own entry below): every node's `client_route` is now
  kept live by `route_sync_loop`. `tests/cluster_growth.rs`: 3→5 growth, no
  restart of the original 3, admin-add + promotion + rebalancing onto the new
  nodes + reads/writes throughout (including through an **original** node
  only, to a tablet leader that has since migrated onto a grown node — ADR
  0032 PR1's own regression) + a never-booted phantom staying `Down` + the
  admin peer list eventually including the grown nodes' admin addresses.
- **A replicated node address book (ADR 0032 PR1) closes the `client_route`/
  `/admin/peers` staleness ADR 0030 above documents, and is the foundation
  PR2 (`animusd join`) and PR3 (decommission) build on** — see
  `docs/adr/0032-seed-join-membership.md` for the full 3-PR design; all three
  PRs are implemented. `Metadata.node_addrs: BTreeMap<NodeId, NodeAddrs>`
  (`animus_control::meta::NodeAddrs { raftkv, client, admin }`) is every
  member's full address set, mutated by `MetaCommand::RegisterNodeAddrs`
  (idempotent, mirrors `RegisterCpAddr`'s own apply shape). Every node
  proposes it once at startup (`ClientCtx::register_node_addrs`, superseding
  the old `register_cp_addr` self-registration — `RegisterCpAddr`/
  `cp_member_addrs` are kept only for WAL back-compat and the internal
  `raftkv` peer book, never proposed by `animusd` anymore).
  `RegisterNodeAddrs` is in `is_relayable_command`'s allowlist (a
  follower-connected node must relay its own self-registration to the
  control leader — the same bimodal-failure shape every prior addition to
  this allowlist documents). Three consumers gained a **live** overlay on top
  of their previous static-only or `cp_member_addrs`-only view, all following
  the same "static seed ∪ replicated overlay, recomputed every tick" shape
  `peer_sync_loop` already established: (1) `peer_sync_loop` itself now also
  overlays `node_addrs[*].raftkv`; (2) `ClientCtx.client_route` is now
  `Arc<Mutex<BTreeMap<NodeId, SocketAddr>>>` (read via
  `ClientCtx::route_addr`/`route_snapshot`, never locked across an `.await`),
  kept live by the new **`route_sync_loop`** (a `peer_sync_loop` sibling, same
  `PEER_SYNC_INTERVAL` cadence) overlaying `node_addrs[*].client`; (3)
  `/admin/peers` (`admin.rs::peers_view`) now unions the static `admin_addrs`
  with `node_addrs[*].admin`, deduplicated and sorted. All three read through
  `ClientCtx::effective_metadata()`, so a control-plane-follower-less growth
  node (ADR 0030) syncs off its own remote mirror like every other
  `Metadata`-derived view it depends on.
- **Decommission (ADR 0032 PR3) is `drain` (existing) plus one new
  `MetaCommand::RemoveMember` proposal and a poll-to-convergence in between.**
  `GET /admin/member/drain-status?node=` (read-only, serves on any node via
  `effective_metadata()`) reports `{node, status, tablets_remaining}` —
  `Metadata::tablets_referencing(node)` and the member's own status, the
  drain-complete predicate. `POST /admin/member/remove {node}` →
  `ClientCtx::admin_remove_member`, **local-control-leader-only, deliberately
  not relayed** (symmetric with `admin_drain`, not with the `Down`
  add-member relay case — see `is_relayable_command`'s doc). Two admin-layer
  refusals before ever proposing (friendlier than a bare Raft rejection; the
  apply-time guard in `Metadata::apply` remains the actual authority): an
  original control-core member (`node`'s paired control id, `node -
  RAFTKV_ID_BASE`, is one of `ctx.admin.control_ids`) can never be
  decommissioned this way (the control group is static, ADR 0030, and
  `bootstrap` would just re-register it `Active` on the next tick anyway);
  and a not-yet-drained member (still `Active`/`Joining`, or still referenced
  by a tablet) is refused with the same drain-status counts. **Leadership is
  checked before either refusal**, not after — a follower's own `Metadata`
  replica can lag the leader's just-converged draining under load, so
  checking "am I the leader" first avoids a stale-replica false "still
  referenced" refusal masking the intended "retry on the leader" routing
  error (found via a `cargo test --workspace`-load flake in
  `tests/decommission.rs`; see the root `CLAUDE.md` engineering-practices
  entry). **Removal is not a fence**: a removed node's still-running process
  stays removed (self-registration is a startup one-shot), but restarting
  that process — or starting a fresh one at the same raftkv id — re-registers
  `Down` and rejoins exactly like a fresh join; the decommission flow's real
  last step is stopping the process. `animus admin decommission <admin-addr>
  <node-id>` automates drain → poll drain-status → remove as one CLI command
  (also exposed as separate `drain-status`/`remove` subcommands).
  `tests/decommission.rs` covers the full flow end to end (including id reuse
  on rejoin) plus all three refusal shapes.
- **The per-node tablet-host reconciler (ADR 0031 PR4) is the single owner of
  this node's tablet lifecycle** — it replaced the three loops this file used
  to document separately (`cp_join_host_loop`, `cp_gc_loop` +
  `cp_gc_release_phase`, `cp_reconfigure_loop`) and their per-node state
  (`minted`, `pending_release`, `CpHostCtx`). The machinery lives in
  `animus_cp_data::host` (read that crate's `CLAUDE.md` section first): the
  pure `plan` decides every action from **one** `MetadataView` snapshot per
  tick, and the `Reconciler` executor owns the hosted `RaftKvNode` map and
  executes the actions in `plan`'s fixed order (`NarrowScope` → `Host` →
  `Reconfigure` → `Release`/`Reclaim`). What stays in `animusd`
  (`tablet_host_reconciler_loop` + the `CpReconciler` backend enum in
  `lib.rs`):
  - **The trigger**: one spawned task per node racing
    `ctx.control.metadata_watch().changed(last_seen)` (ADR 0031 §trigger —
    event-driven, so a replica-set change is observed on the commit that made
    it, not on the next arbitrarily-phased poll tick) against a
    `RECONCILE_FALLBACK_INTERVAL` (500ms) sleep. The fallback is
    **load-bearing for ADR 0030 growth nodes** — their local control raft
    never advances, so `metadata_watch` never fires and only the fallback
    ticks them, reading `effective_metadata()`'s remote mirror. After either
    wake, coalesce to `watch.latest()` (a burst of commits under bulk load
    collapses into one tick, not one per entry).
  - **The pre-recovery guard**: skip a tick while `raft.last_applied() == 0`
    **and** the growth-node remote mirror is empty — pre-recovery `Metadata`
    is default-empty and would read as "everything dropped" to the
    reclaim/release phases. Gated on both signals so a growth node (whose
    local raft never leaves 0) still ticks off its mirror.
  - **The edge mirror**: `ClusterEdgeState`'s `raftkv` registry is now a
    **read-only mirror with exactly one writer** — the reconciler's
    `on_host`/`on_teardown` hooks call
    `register_raftkv`/`unregister_raftkv`; nothing else writes it. Routing
    (`resolve_cp_route`/`local_cp`/`cp_leader`) keeps reading it unchanged.
  - **Formation semantics are unchanged** (they moved, verbatim, into
    `host::plan`/`Reconciler::host`): a fresh table's first tablet, a
    split-minted sibling, and a placement-reconciler-placed replacement all
    form the same way — `Epoch::INITIAL` (or `StorageScope::has_data` on a
    restart, since WAL recovery alone doesn't restore voter status from a
    non-voter start) ⇒ full voter config; a bumped epoch ⇒ quiet non-voter
    until the leader adds it. Dedup is `LocalState::hosted` (the `minted`
    claim set's successor — reset on restart, which is fine: the reconciler
    re-discovers every tablet to host from replicated `Metadata` and re-forms
    each from the shared engine's durable data). A hosted tablet's
    `StorageScope` is re-narrowed via a planned `NarrowScope` action whenever
    its metadata range shrank (the split-source case — provably narrow-only),
    replacing the old per-tick unconditional re-narrow.
- **Drop-table GC (ADR 0024) is the reconciler's `Reclaim` action.** The real
  drop sink is `ClientCtx::drop_table` (CQL `DROP TABLE` + admin
  `/admin/data/drop-table`): `DropTableSchema` then `DropTableTablets`.
  **`drop_table_schema` stays schema-only** (the admin panel's schema-only
  drop). CQL `ALTER TABLE … ADD` no longer drops at all: it mutates the schema
  **in place, atomically** via `MetaCommand::ReplaceTableSchema`
  (`ClientCtx::replace_table_schema`) — the old drop-then-recreate could
  strand the table schema-less if a crash landed between the two commands —
  and an ALTER must never GC data. A tablet in `LocalState::hosted` that is
  absent from `Metadata.tablets` plans a `Reclaim`; the executor's teardown
  (in `animus-cp-data`, `Reconciler::teardown` — the old `cp_gc_tablet`'s
  exact shape) unregisters this node's handle via `on_teardown`, does
  `shutdown()` + wait `is_stopped()` (never touch data under a live driver;
  on timeout re-register via `on_host` and retry next tick — the planner
  re-emits the action until `confirm_torn_down`), then
  **`RaftKvNode::erase_scope()`** — tombstone every key in the tablet's own
  `StorageScope` out of the node's *shared* engine (never a file delete,
  since ADR 0026/0028 means the engine isn't this tablet's alone) — and
  deletes its own per-tablet WAL file (`animus_cp_data::wal_file(tablet)`).
  Confirm (`LocalState::confirm_torn_down`) last. **Drop + GC are convergent,
  not one-shot**: a restarted control replica re-applies its log through
  *historical* map states, so the reconciler may briefly re-host a dropped
  tablet's empty group — it reclaims it again once replay passes the drop
  (test the post-restart state with a poll, never a fixed sleep). **A new
  `MetaCommand` that must commit from a follower-connected node has to be
  added to `is_relayable_command`** — missing there is a *bimodal* failure:
  works when the connected node happens to be the control leader, silently
  times out ("did not commit") when it must relay (`tests/drop_table_gc.rs`
  caught exactly this for `DropTableTablets`).
- **Removed-replica GC (ADR 0029) is the reconciler's `Release` action — the
  release dual of `Reclaim` above.** When a tablet's replica set moves *off*
  this node while the tablet still **exists** (a manual `drain`, an automatic
  failure-repair swap, or a rebalance move — not the whole table being
  dropped), the same teardown runs; the only difference from reclaim is the
  predicate (`host::tablets_to_release`: present **and** `base_id ∉ replicas`
  vs. `tablets_to_reclaim`: absent — the two partition `LocalState::hosted`;
  a tablet is never both) plus **two guards, now enforced structurally inside
  `host::plan`** where the old loop enforced them by hand: (1) the
  **local-config gate** — release is only planned once this node's *own
  durable Raft log* voter config (`TabletFacts::config_excludes_me`, read
  from the hosted group's `config()`) already excludes `base_id`. That is the
  replay-independent anchor ADR 0029 PR1's `departing` mechanism guarantees a
  removed node reliably adopts — unlike replicated `Metadata.tablets`, which
  a restarting control replica replays through *historical* states, so the
  metadata signal alone can flicker; and (2) an **epoch-stability dampener**
  (`LocalState::pending_release`, `tablet → (epoch, consecutive ticks)`): the
  release condition must hold for `host::RELEASE_CONFIRM_TICKS` (3)
  consecutive plan calls *at an unchanged tablet epoch* before acting — any
  epoch change (a re-add's CAS bumps it) or condition flip resets the
  counter, so a replay transient can't trigger a release and a re-add cancels
  one in flight. A **joining spare is structurally never released**: it *is*
  in `replicas`, so `tablets_to_release` excludes it even during its brief
  non-voter formation window. **One accepted residual gap** (same shape the
  pre-ADR-0029 drain/repair paths already had — not a new risk): a node that
  crashes *before* it ever receives the removal config entry recovers a log
  whose config still lists itself, so the local-config gate never passes and
  that node leaks the group forever. `tests/cp_rebalance_gc.rs` (a
  `CasTabletReplicas` move-off → stop + erase; release converges across a
  restart-replay without resurrecting, while an unrelated still-hosted tablet
  is untouched; a repair-onto-spare join is never prematurely erased; a split
  immediately followed by a release does not corrupt the new sibling — see
  below), plus the `host::plan` unit tests in `animus-cp-data`.
- **The release erase is bounded by the tablet's CURRENT replicated range,
  never the group's in-memory `StorageScope` — the latter can be stale-wide
  for a just-split tablet.** This is *the* invariant ADR 0031 exists to make
  structural: `HostAction::Release` carries `erase_bound` — always the
  tablet's current `Metadata.tablets[t].range`, stamped by `plan` (which
  documents that it must never come from a `TabletFacts::scope_range` fact) —
  and `Reconciler::teardown` calls `narrow_scope(erase_bound)` immediately
  before `erase_scope()`. Pre-ADR-0031 history (why this matters): split T at
  M (T keeps `[start,M)`, new sibling C gets `[M,end)`, both on node X's one
  shared engine); if a rebalance/repair/drain dropped X from **T's** replica
  set before X's own join-host tick had re-narrowed T's scope to the
  post-split range, that scope froze stale-wide on X — and an unbounded
  `erase_scope()` at release would have tombstoned every key under T's
  *pre-split* wide range, including C's live keys (X is typically still a
  replica of C, since the split only touched T's replica set) — permanent,
  silent corruption of a tablet X was never even asked to release, at a
  version high enough to beat C's own fresh writes under per-key LWW. The old
  loops fixed this by *convention* (a `current_range` parameter threaded into
  `cp_gc_tablet`); the planner's fixed emission order + the `erase_bound`
  contract make the narrow-before-erase ordering a property of the one
  place that decides it. `narrow_scope` is documented narrow-only, and the
  current replicated range is always a subset of (or equal to) whatever the
  group's own scope already covers, so this is always a narrowing, never a
  widening. Regression tests: a deterministic primitive-level proof at
  `animus-cp-data/tests/
  narrow_scope.rs::narrow_then_erase_scope_spares_a_co_hosted_siblings_data`
  (build both halves of a whole-scope group, narrow, erase, assert the
  untouched half's value **and version** survive — no timing needed), the
  reconciler-level `animus-cp-data/tests/reconciler.rs` (the same invariant
  through `Reconciler::tick`'s own host → narrow → release flow), and an
  end-to-end `tests/cp_rebalance_gc.rs::
  split_then_immediate_release_spares_the_new_siblings_data` that proposes the
  split and the parent's replica-set CAS back-to-back on the control leader's
  own log (round-tripping the split through the wire protocol first gives the
  reconciler time to self-heal the scope before the drop lands, hiding the
  bug — empirically this made the difference between the E2E test catching
  the pre-fix bug 0/5 times vs. ~3/5 times) and confirms the child's data
  survives both cluster-wide and in the dropped node's own local storage.
  See the root `CLAUDE.md` engineering-practices entry for the general lesson.
- **`resolve_cp_route`'s `has_local_replica` gate must re-verify against this
  node's own current Raft config, not just "do I have a registered handle at
  all" — a lesson the release-GC's own grace window (above) creates.** Before
  ADR 0029, a registered local handle for a tablet was always a *current*
  replica (nothing ever removed a node from a still-existing tablet's replica
  set while leaving its handle registered), so `resolve_cp_route` trusted
  `local_cp(tablet).is_some()` as "wait for my own group to elect, don't
  bother forwarding." Once a healthy rebalance/repair move can leave a
  departed node's handle registered for up to `RELEASE_CONFIRM_TICKS` ticks
  (the release-GC's own grace window, by design), that node's client-facing
  requests for the tablet it just left hit exactly this branch and **wait
  forever** — `decide_cp_route`'s `!has_local_replica` guard skips computing
  `is_replica`/`fallback_forward` entirely whenever a local handle exists, so
  a stale handle can never fall through to "forward to whoever the metadata
  says actually replicates this now." Fixed by re-deriving `has_local_replica`
  as "a local handle exists **and** its own `CpGroup::config()` still lists
  this node" — the identical local, non-`Metadata` signal the release-GC gate
  already trusts (PR1's `departing` mechanism is what makes it durable). Found
  live via `tests/cp_rebalance.rs`: every client request to a just-rebalanced
  table hung until its own 10s `CLIENT_TIMEOUT`, on every node, not just the
  departed ones — because the *actual* current leader's own read barrier was
  separately broken (next entry) and looked identical from the client's side.
  **General check for any "cheap local check short-circuits a `Metadata`
  read" optimization: does staying valid for the life of the local handle
  actually depend on an invariant a *later* change (here, delayed release)
  quietly breaks?**
- **`RaftKvNode`'s read-barrier quorum (`majority` + which peers get probed)
  was keyed on `all_nodes` — the peer set a node happened to be *hosted with*,
  frozen at construction — never the group's live Raft `config()`.** Every
  membership change before ADR 0029 was a same-size, pre-known swap (a
  failure-repair spare was already listed in every replica's `all_nodes` from
  the moment the group formed, `membership.rs`'s and `reconfigure_trigger.rs`'s
  join tests all construct it that way), so `all_nodes` never actually
  diverged from the live config and this was invisible. A healthy rebalance
  move breaks that assumption outright: it can rotate a majority of a tablet's
  replicas onto nodes that were never in *any* surviving replica's `all_nodes`
  at all. The surviving leader's `read_barrier` then probes only its own stale
  peer set — which, after a full rotation, can intersect the *current* voter
  config in nothing but itself — so it can never collect the acks its own
  `majority()` (also computed from the same stale `all_nodes`) requires, and
  every `linearizable_get`/`linearizable_scan` on that tablet times out and
  reports the key **absent** (indistinguishable from genuine data loss from
  the outside) forever after. Fixed by computing both `majority()` and the
  probe fanout from `self.config()` (the live voter set) instead — `all_nodes`
  is now dead as a stored field (removed) and survives only as the one-time
  bootstrap value `RaftCore::new`/`recovered` seed their *initial* config from.
  Regression: `animus-cp-data/tests/read_index.rs`'s
  `linearizable_read_succeeds_after_a_full_membership_rotation` — rotates a
  3-node group `{0,1,2}` to `{2,3,4}` (two of three members replaced, mirroring
  the exact production shape) and stops the departed nodes outright (a
  still-live departed peer can accidentally still ack on term match alone,
  which masked an earlier, weaker version of this same test — a regression
  test for a "the sender doesn't respond" bug must actually make the sender
  not respond, not just remove it from the *current* config while it keeps
  running). **This is the cp-data-plane sibling of the exact bug class the
  root CLAUDE.md already documents for the control plane** ("a cached
  per-node handle derived from replicated state needs an explicit re-sync
  step for every way that state can change in place") — but it had never
  actually been triggered before ADR 0029 gave membership a shape (a full
  rotation) that could expose it. When adding new ways an existing invariant
  can change (here: "a group's peer set now evolves after hosting", where it
  used to be fixed), grep for every place that invariant's *original* form is
  cached, not just the mechanism that changes it.
- **The CP group is durable by default**: a node opens **one shared** `LsmEngine`
  over its **raftkv** `ProdEnv` at start (`StorageBackend::Lsm`), cloned into every
  tablet's `RaftKvNode` (ADR 0026/0028) — so a value acked to a client
  (Raft-committed + WAL-fsynced before the ack) survives a process restart (the
  engine + each tablet's own Raft WAL, `raftkv.wal.<tablet>`, recover on reopen).
  The engine's files use a **flat filename prefix** (`LSM_PREFIX = "db-"`), *not* a
  subdirectory — `ProdEnv`'s disk opens files directly under the role's data dir and
  does not create intermediate directories, so a slash-bearing prefix (e.g. `"db/"`)
  would fail to create the files. `--ephemeral` (or `StorageBackend::Memory`) selects
  the volatile `MemoryEngine` instead (the `SharedEngine`/`CpGroup` enums wrap
  either), for dev runs that intentionally start empty. `start`/`start_cluster`/
  `run_node` default to the durable backend; `start_with`/`start_cluster_with`/
  `run_node_with` take an explicit `StorageBackend`. These are **async + fallible**
  (opening the LSM is async and can fail), so the node-start entry points return
  `io::Result`. (`tests/durable_restart.rs` proves a client write survives a restart
  on the LSM backend and is lost on the memory backend; `tests/self_heal.rs` is now
  just a concurrent-load smoke test.)
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
  (the `escape(hash)||…||base_key` index) stays in-memory, maintained from observed
  `note_put`/`note_delete` writes (O(log n) per write via a base-key→entry reverse
  map) and **lazily backfilled on the first index query** against freshly-created
  index machinery (`dynamo.rs::backfill_index_if_needed`: one base-table scan
  replayed through `note_put`, then `mark_table_backfilled`) — so a GSI query
  returns pre-restart items without re-writing them (proven in
  `tests/dynamo_schema.rs`'s `create_table_index_replicates_to_second_node` /
  `…_survives_node_restart`).
  **Base-table `Query`/`Scan` use the CP plane's linearizable range scan**
  (`ClientCtx::cp_scan` → `RaftKvNode::linearizable_scan`) over a contiguous key
  range (a partition prefix for `Query`, the whole-table prefix for `Scan`),
  decoding each live pair and dropping DynamoDB tombstone values — **no in-memory
  written-key tracking** (proven across a restart in `tests/dynamo_schema.rs`). The edge keeps only the
  **GSI/LSI index declarations** in-memory (for an *index* `Query`), held
  **per-node** in `ClusterEdgeState` (not a process `OnceLock`; ADR 0031 PR2 —
  a node backfills its own entry data lazily on first query rather than
  relying on another node's observations). The surface now
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
  records the keyspace in the per-node `CqlState` (keyspaces are not yet
  replicated).
  - **The keyspace set + prepared-statement store (`CqlState`) are per-node
    edge state** (ADR 0031 PR2), held in the node's own `ClusterEdgeState`
    (threaded through `ClientCtx::edge`), **not** a process `OnceLock` — like
    the DynamoDB `SchemaRegistry`. They are shared across **connections to the
    same node** (so `PREPARE` on one connection and `EXECUTE` on another
    resolve to the same statement, as long as both connect to the same node)
    but **isolated between two nodes** — including two nodes of the same
    `--cluster N` cluster, matching a real one-process-per-node deployment's
    per-process catalog exactly, and between two clusters in one process (so a
    test harness can run several independent clusters, or several nodes,
    without their edge state leaking — the fix for the former process-global
    `OnceLock` state-leak, extended one level further by ADR 0031 PR2). They
    are still **not durable and not control-plane replicated**: lost on
    restart, and each process/node re-creates its own keyspaces/prepares. Note
    table *schemas* are no longer here at all — they live in the control
    plane's replicated catalog (ADR 0013), which every node sees the same way
    regardless. Per-connection state (the `USE`d keyspace) lives in `Session`.
  - The **prepared-statement id is content-addressed** — a stable hash of the
    statement text (FNV-1a, no RNG so the edge stays deterministic) — so `PREPARE`
    on one connection and `EXECUTE` on another resolve to the same statement,
    **provided both connections are to the same node** (see above).
- **A dedicated admin / debug HTTP-JSON endpoint** (`RoleAddrs.admin`,
  `Node::admin_addr`, ADR 0020) — a **sixth** per-node listener, isolated from the
  client/dynamo/cql data edges. A production-only I/O edge in `admin.rs` (real
  tokio sockets + the shared hand-rolled HTTP helpers extracted to `http.rs`, now
  shared with `dynamo.rs`). Read-only `GET` views — `/admin/{config,status,raft,
  raftkv,storage/lsm,storage/wal,storage/wal/segment,storage/key,storage/scan,metrics,health}`
  — plus gated `POST` actions — `/admin/{tablet/split,tablet/merge,storage/flush,storage/compact,
  raftkv/reconfigure,drain}` and **data writes** — `/admin/data/{dynamo,cql,drop-table,seed}`
  (ADR 0021, the dashboard's write surface). Below the edge it only **reads** node state
  (control + CP Raft accessors, `LsmEngine` introspection: `sstable_views`/
  `wal_segment_*`/`memtable_*`, the `CpGroup` introspection passthroughs) aggregated
  live at request time, or drives an explicit action; node identity for `/admin/config`
  is captured into `ClientCtx.admin` (an `AdminInfo`). **No auth yet** — bind it to a
  trusted interface. The `animus admin <subcommand>` CLI consumes it.
  - **The web dashboard (ADR 0021) is the "AnimusDB Console"** — a from-scratch
    visual/IA redesign (2026-08-06, implemented from a Claude Design mockup the
    user provided) replacing the earlier flat-tab debug dashboard. Still served
    from the same port: `GET /` (and the `/admin`, `/admin/ui` aliases, plus any
    `/admin/ui/<tab>`) returns a self-contained vanilla-JS SPA embedded via
    `include_str!` (`dashboard.rs` → `dashboard.html`) — no bundler/npm, the
    build stays `cargo`-only, and **no external fonts/CDN either** (ADR 0021 §1
    is firm on this; the console approximates the source design's Inter/IBM
    Plex Mono with system font stacks instead of a Google Fonts fetch). It is a
    pure **client** of the `/admin/*` JSON, so every `/admin/*` response
    carries **CORS** (`http::CORS_HEADERS`; an `OPTIONS` preflight returns 204)
    because the page loaded from one node fans out in the browser to **every**
    node. The fan-out seed is **`GET /admin/peers`**.
    **Shell: a sidebar, not a top tab row** (`dashboard.html`) — six views,
    `overview`/`placement`/`tablets`/`browser`/`storage`/`node` (the mutable
    `TABS` in `dashboard_core.js`, recomputed per this node's own role — see
    the ADR 0035 PR7 entry below), each with its own JS module
    (`dashboard_overview.js`/`dashboard_placement.js`/`dashboard_tablets.js`/
    `dashboard_browser.js`/`dashboard_storage.js`/`dashboard_node.js`, loaded
    after `dashboard_core.js` in that order — plain `<script src>` tags
    sharing one global scope, so later files call earlier ones' functions
    freely). **Each view keeps a real URL**
    (ADR 0021 follow-up 7): `/admin/ui/<tab>`, `admin.rs::is_ui_path` prefix-serving
    the SPA for any path under it (an unrecognized tab 200s and falls back to
    the default client-side, so a stale bookmark degrades gracefully); the page
    reads `location.pathname` on load (`tabFromPath`/`activateTab`) and uses
    `history.pushState`/`popstate`. The Storage tab's selected tablet/node ride
    along as `?tablet=&node=` (`gotoStorage`/`syncStorageUrl`/
    `applyPendingStorageParams` in `dashboard_core.js`) — the one piece of
    sub-tab URL state, reused by the Tablets view's "Open in Storage →" link and
    by Placement's per-node tablet rows.
    **Both a dark and a light theme** (`dashboard.css` CSS custom properties,
    the mockup's `oklch()` palette verbatim), toggled by a button in the top bar
    and persisted to `localStorage` (a UI preference, not data — no server
    round-trip). **Three things the design showed have zero backend support and
    are deliberately omitted, not faked**: per-node CPU/mem/disk % (nothing
    samples host resources anywhere in this workspace), an activity/event feed
    (no persisted/queryable event log exists — distinct from OTel tracing and
    the counter-snapshot `/admin/metrics/history` ring buffer), and a per-tablet
    election-history log (only current Raft state is tracked). Fabricating these
    would violate this admin tool's ground-truth-data ethos. The **Overview**
    view's "Tables" panel (a per-table tablet-count + status breakdown) is a
    real, honest substitute for the design's dropped "Recent activity" panel.
    **Tablets is one view with a `Lanes`/`Table`-shaped predecessor collapsed
    into a single filterable list + detail panel** (not the earlier
    lanes-vs-table toggle, which is superseded) — clicking a row opens a
    right-side panel with the raft group (from data already fetched) plus
    storage-engine stats fetched **on demand**, only for the selected tablet's
    leader, from `/admin/storage/lsm?tablet=` (`dashboard_tablets.js`'s
    `loadTabletDetailStorage`) — not for every row.
    **The Data Browser view replaces the old Write tab's Dynamo attribute-row
    form with a real item list + detail panel** (`dashboard_browser.js`):
    Scan/Query build real requests against `/admin/data/dynamo` (Query supports
    the exact sort-key grammar `animus_dynamo::wire` parses — `=`, `BETWEEN`,
    `begins_with` — see `buildQueryPayload`), decode the returned
    AttributeValue-map `Items` for display, and per-row Edit/Delete/Create use
    a dynamic attribute-row editor (key columns locked, arbitrary extra
    attributes addable/removable) because DynamoDB items are schemaless beyond
    their declared keys — a fixed-column form (as the source mockup's fake
    table had) can't represent that. **Each browser/write panel owns its own
    table selector** (`#br-dy-table` here, `#seed-table` on the folded-in Bulk
    seed tool — `dyTable`/`seedTable`), auto-picking the first valid table
    rather than requiring an explicit pick, and rather than one shared global
    header dropdown (an earlier revision had that; removed as redundant).
    `lastRenderedDyTable` gates when the Dynamo op panel's state is rebuilt
    (table actually changed) vs. left alone on a routine poll refresh,
    preserving in-progress edits.
    **The Storage view folds in the pre-redesign dashboard's debug tools**
    (`dashboard_storage.js`) — WAL segment/record inspection, LSM shape, a
    single-key inspector (`/admin/storage/key`), a **browse-keys** list
    (`/admin/storage/scan` → `CpGroup::local_scan`), and the Bulk seed tool —
    ported essentially unchanged, since the console design doesn't include this
    level of manual storage debugging at all and it would otherwise be lost.
    Its **node dropdown is filtered to nodes whose `/admin/raftkv` view lists the
    selected tablet** (the storage endpoints are node-local — `local_cp` — and
    404 on a non-hosting node); if no reachable node hosts the tablet yet (group
    still forming) the dropdown is empty with a hint (the Load/Browse/inspect
    handlers no-op on an empty node).
    `tests/dashboard_endpoint.rs` proves serve + CORS + preflight + peers; its
    "the shell contains X" assertions target the shell (`dashboard.html`) or the
    specific JS asset that actually carries the behavior being checked (e.g.
    the item form's key-lock indicator lives in `dashboard_browser.js`, not the
    shell) — a lesson from a **latent bug this redesign caught**: the pre-split
    single-file dashboard (before PR #48) had its whole JS inline, so asserting
    on `GET /`'s body for a JS-source string worked by accident; after the
    file split it silently stopped proving anything (the string had moved to a
    separately-served file `GET /` never returns), and nothing caught it until
    this rewrite touched the same test. When splitting a previously-inline asset
    into files, re-audit every test assertion that greps the *original*
    response body for content that may have moved.
    - **A split deployment (ADR 0035) needed one real bug fix and one
      additive field, not a new panel.** `/admin/config` gained a derived
      `role` string (`"control"`/`"data"`/`"combined"`, from the same
      `control_id`/`raftkv_id` `Option`s the JSON already carried — cheaper
      for every consumer than re-deriving the null-check itself). The
      Overview's node list now shows every CONTROL-only node too (previously
      it only ever iterated `Metadata.members`, which a control-only node is
      never a part of — see the crate's "the cluster's members are the CP
      raftkv nodes" entry above — so a control node was invisible anywhere
      in the dashboard), tagging each row with its role. Finding this
      surfaced a **real, pre-existing latent bug**: the "control leader"
      label (the health banner and the "Control plane" stat tile) resolved
      the leader's display id via `nodeRaftkvId`, which returns the node's
      `raftkv_id` — `null` for a control-only node — so a split deployment's
      Overview would have rendered "control leader node null" the first time
      its leader happened to be a control-only node (in combined mode this
      was invisible, because every node has a `raftkv_id`, so the bug had
      zero blast radius until a role that lacks one existed at all). Fixed
      with a new `nodeDisplayId` helper (`dashboard_core.js`) — prefers
      `raftkv_id`, falls back to `control_id` — used only at the two
      "identify an arbitrary node" call sites; every existing `nodeRaftkvId`
      call site is a raftkv-id-*keyed lookup* (matching a CP group's owning
      node, or a `Metadata.members` row), which structurally never receives
      a control-only node in the first place, so those were correctly left
      alone. **General lesson: a "get this node's id" helper that silently
      returns one specific role's id field breaks the moment a node without
      that field exists — audit every call site for "is this matching by
      that id" (safe) vs. "is this just labeling an arbitrary node for a
      human" (needs the fallback) before reusing it.** No other view needed
      changes: every node-local storage/raftkv query is either fetched
      tolerantly for every node during the fan-out (`/admin/raftkv`/
      `/admin/health` already answer a control-only node with an honest
      empty/false result, not an error — no `Remote`/control-only-specific
      code exists in `admin.rs` for them at all) or is only ever issued
      on-demand against a node already filtered to "hosts this tablet"
      (the Storage view's node dropdown, the Tablets detail panel's storage
      fetch) — a control-only node structurally never hosts a tablet, so it
      was already excluded by the existing filter, not by a new one. And
      `computeHealth()`'s only per-node read of control leadership
      (`n.raft.is_leader`) is a `.find()` across every node for "is *anyone*
      leader" — a single data node's `is_leader: false` was never a
      per-node degrade signal to begin with, so point (c) of the review
      ("don't read a data node's non-leadership as degraded") was already
      satisfied by the existing design; verified, not changed.
    - **Role-gated dashboards (ADR 0035 PR7): every node still serves the
      identical SPA shell/assets (`admin.rs::static_asset`/`is_ui_path` are
      unchanged), but which tabs a node's own page shows is gated on ITS OWN
      role, not a cluster-wide property.** PR6 made the dashboard *render* a
      split deployment correctly; PR7 makes each node's page *match* what it
      actually is, instead of every node — including a data-only node with
      no control-plane Raft state and, being a single node, nothing to
      place/balance — showing the same five cluster-wide tabs
      (Overview/Placement/Tablets/Storage) it can't usefully serve. A
      control-only or combined node's page is byte-for-byte unchanged
      (`ROLE_TABS.control`/`ROLE_TABS.combined` both start with the original
      five, in the original order, so the default tab stays "overview"); a
      data-only node gets a sixth view, **Node** (`dashboard_node.js`),
      instead: this node's own identity/health, control-plane mirror status,
      hosted tablets (its own `/admin/raftkv`), a storage-debug panel scoped
      to just this node (no node dropdown — there's only one node in scope,
      unlike the Storage tab's cluster-wide picker, so this is a trimmed
      local variant of `dashboard_storage.js`'s WAL/LSM/key/scan panels
      rather than a shared helper, since those functions are wired to
      specific `st-tablet`/`st-node` DOM ids a second concurrent instance
      can't reuse without ID collisions), and a link to a reachable
      control/combined node's Console. `ROLE_TABS.data = ["node",
      "browser"]` — Data Browser stays available (browsing your own data
      edge is node-dedicated UX), everything else is hidden. A combined node
      gets **all six**, Node appended last (it's also a data node).
      **Gating is entirely client-side** (`dashboard_core.js`'s
      `applyRoleGating`, hiding/showing `.sidebar button.navlink` elements
      and recomputing the mutable `TABS` list `tabFromPath`/`activateTab`
      already read) — the shell always contains every section; a role just
      controls which ones are reachable, so a role-inappropriate deep link
      (e.g. `/admin/ui/placement` loaded directly on a data-only node) falls
      back to that role's own default tab (`TABS[0]`) via the same
      unknown-tab fallback `tabFromPath` already had, rather than 404ing or
      going blank.
      **One backend addition**: `/admin/raft`'s `control_mirror` object
      (`watermark`, `leader_hint`, `has_synced`) — a data node has no
      cluster-wide state of its own to show, so "is my view of the control
      plane caught up, and who does it think leads" is the one genuinely
      new fact the Node view needed that no existing endpoint surfaced.
      Every field is a direct passthrough of a `ControlHandle` accessor
      that ADR 0035 PR4/PR5 already built for the `Remote` variant
      (`metadata_watch().latest()`, `leader_addr_hint()`,
      `has_synced_metadata()`) — grepping for the mechanism before adding
      anything found it was all already there, just never read by any
      client; the fix was three lines in `raft_view()`, not a new
      subsystem. For a `Local` handle (a control-only or combined node,
      which IS a control-plane voter) the same fields degrade to their
      honest values (`leader_hint: null`, `has_synced: false`) — there is no
      mirror for a voter to be "synced" against, its own Raft state above
      already is the ground truth, and the Node view's own copy says so
      explicitly rather than showing a misleading "not synced" pill.
      **The role probe is deliberately split from the cluster-wide
      fan-out**: `loadSelf()` fetches only this node's own `/admin/config`/
      `/admin/raft`/`/admin/raftkv`/`/admin/health` (never a peer), so
      resolving the role and gating the sidebar can never stall on a
      slow/unreachable OTHER node the way the existing `/admin/peers`-seeded
      fan-out in `loadAll()` can — `loadAll()` calls `loadSelf()` first,
      then proceeds to the slower cluster-wide fetch exactly as before. The
      "Open cluster console" link's discovery reuses that SAME cluster-wide
      fan-out's `STATE.nodes` (each entry already carries `config.role` from
      the existing per-node `/admin/config` fetch) to find a reachable
      control/combined node — **no second probe was written**, since the
      fan-out the dashboard already performs for every other view already
      contains the answer; see the root `CLAUDE.md`'s matching
      engineering-practices entry on checking existing fan-out data before
      adding a new discovery probe.
      `tests/dashboard_endpoint.rs::dashboard_role_gating_split_deployment`
      (reusing `tests/support::bring_up_split`) proves both roles serve the
      same shell + `dashboard_node.js`, asserts the gating markers against
      `dashboard_core.js`/`dashboard_node.js` (not the shell — this file's
      own documented "assert against the asset that carries the behavior"
      lesson), asserts `/admin/config`'s `role` differs across the split,
      and polls the data-only node's `/admin/raft` until `control_mirror.
      has_synced` goes true.
    - **Displayed keys show the partition token as unpadded base64url**
      (`admin.rs::key_display`): a wire-edge/seeder key is `token || escape(pk) ||
      rk` (ADR 0022), and the leading `TOKEN_BYTES` are a **binary** Murmur3 token
      that lossy UTF-8 would mangle — so a key with a non-printable prefix renders
      as `<11-char-base64url-token>:<readable pk/rk>` (e.g. `CCX7PfaR_cM:seed:0000…`).
      The encoding is base64url with no padding (RFC 4648 §5) because displayed
      keys are pasted back into `?key=`/`?start=` query params, where the standard
      alphabet's `+` decodes as a space (and `=` padding percent-encodes noisily);
      the codec is `animus_dynamo::wire::{base64url_encode,base64url_decode}` (the
      standard padded pair stays on the DynamoDB `B` wire). A *plain-client* `Put`
      stores its key verbatim (no token), so a fully-printable key is shown as text
      unchanged. **Values** keep lossy UTF-8 (`key_str`). `parse_key_display` is
      the inverse (the exactly-`TOKEN_BYTES` decode is strict — URL-safe alphabet
      only, canonical trailing bits — which keeps a plain `:`-bearing key from
      being mistaken for a token), so a browsed key round-trips back through the
      inspector (`/admin/storage/key`) and the scan `start` (paging). The
      dashboard's JS helpers (`b64url`/`bytes`/`tokenBound`) mirror the same
      encoding, so tablet range boundaries and SSTable key ranges are
      eyeball-comparable with browsed keys. Unit tests live in `admin.rs`; the
      `admin_endpoint` plain-`Put` `admin-key` guards the
      not-every-key-is-token-prefixed case.
  - **The Data Browser view (ADR 0021) writes through the admin port.** `POST
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
    - **Dynamo table management + Scan/Query/item CRUD.** The panel lists tables
      from the replicated catalog (`/admin/status` `schemas.tables`, filtered to
      plain-named = Dynamo, vs CQL `ks.table`), creates via `CreateTable`, and drops
      via `POST /admin/data/drop-table` (`ctx.drop_table_schema`; the Dynamo wire has
      no `DeleteTable`, so this reuses the control-plane drop, schema-only). The op
      **targets its own `#br-dy-table` selector** (see above) — disabled unless a
      Dynamo table exists, so you can't act on a non-existent or CQL-only table.
      Scan and Query build **real** requests (`dashboard_browser.js`'s
      `runDynamoOp`/`buildQueryPayload`) rather than the pre-redesign Write tab's
      Form/JSON editor over one fixed op; results decode the returned
      AttributeValue-map `Items` for a real item list, and per-row Edit/Delete
      plus "+ Create item" open a dynamic attribute-row editor (key columns
      locked, rows addable/removable) — not a fixed-column form, since items are
      schemaless beyond their declared keys. `tests/admin_endpoint.rs::admin_table_management_create_and_drop`
      (also asserts a numeric sort key's type reaches the catalog) still covers
      the underlying create/drop; the Scan/Query/CRUD paths reuse the same
      `/admin/data/dynamo` operations the old Write tab used, just orchestrated
      differently client-side, so no new server-side test was needed for them.
    - **Bulk seed for sharding tests.** `POST /admin/data/seed {table, count, start?,
      key_prefix?, value_bytes?}` writes synthetic rows whose partition key is
      `key_prefix` + zero-padded index, stored under the edges' token-prefixed
      layout (`partition_token(escape(pk)) || escape(pk)`, `admin.rs::seed_key` —
      ADR 0022: seeding must hash like a real write, so sequential indices spread
      across the ring instead of piling into one tablet's tail)
      into an **existing** `table` (ADR 0023: seeding writes into a table, it
      does not create one — a non-existent table is a `404`, looked up in the
      replicated tablet map), committed sequentially as `SEED_BATCH_SIZE`-key
      `cp_batch_write_patient` batches; capped at `SEED_MAX_PER_REQUEST` per
      call. Each batch is **retried** (`SEED_WRITE_ATTEMPTS`) so writes racing
      a tablet **split** — routed to the parent and truncated as the upper
      range moves to the new child — re-route to the elected child and land
      (idempotent per-key LWW), instead of surfacing "CP batch write did not
      commit in time". **The retry uses `ClientCtx::cp_batch_write_patient`,
      not a plain loop over `cp_batch_write`**: a bare confirm-timeout means
      the batch's `Batch` Raft entry was accepted onto the leader's log but
      not yet confirmed durable+applied — not that it's lost — so blindly
      resubmitting would append a second, fully duplicate entry for the same
      keys on top of one probably still committing, doubling replication/fsync
      load under exactly the slow/contended conditions that caused the
      timeout (root-caused via a live repro: `--auto-split 2000` under
      sustained bulk-seed looked like a leader-election storm but every Raft
      term, control plane and every CP group, stayed flat the whole time —
      `commit_index` kept climbing well past individual attempts already
      reported failed; the actual bottleneck was disk fsync latency, ~12-27ms
      measured on a WSL2 host vs. sub-ms on real NVMe). `cp_batch_write_patient`
      polls the *same* already-accepted entry for a second confirm window
      before falling back to a fresh propose, so only a genuine routing
      failure (leader moved, e.g. a split) triggers a real resubmission. The
      dashboard's **Bulk seed** card chunks a
      larger total into requests, showing progress + refreshing the Tablets view so
      splits appear live; it also **targets its own `#seed-table` selector**,
      disabled with a hint unless the selected table already has a tablet (from
      the tablet map in `/admin/status` — the exact set the endpoint's
      `has_table_tablet` check accepts, so Dynamo *and* CQL `ks.table` tables both
      qualify). Combined with the binary's **`--cluster N --auto-split K`**
      flag (a CP-hosting node splits a tablet it leads once it exceeds K keys, Phase
      2.4, via `start_cluster_with_auto_split`), seeding past K auto-shards the
      keyspace — verified end to end (seed 12k keys, `--auto-split 4000` → 5 tablets).
      `tests/admin_endpoint.rs::admin_seed_writes_synthetic_keys`. **Wrapped in an
      `admin_seed` span** (per-chunk `admin_seed_batch` children, ADR 0027): the
      seeder calls `cp_batch_write` directly rather than going through
      `handle_client`, so without its own span a batch forward's
      `otel::current_traceparent()` would have no active context to inject — the
      seed would write real data but be invisible in a trace backend no matter how
      much it wrote.
  - **`/admin/raftkv` (and every `edge.*`-backed admin view) is node-local
    everywhere, including `--cluster N` (ADR 0031 PR2 — `ClusterEdgeState` is
    always per-node now; see its doc in `lib.rs` for the historical shared-edge
    shape this replaced).** A storage route resolves the tablet's *local*
    handle (`edge.local_cp`), which is genuinely this node's own in both
    deployment modes — scrape any node for its own node-local storage debug
    (`tests/admin_endpoint.rs` uses `run_node` per node, matching real
    deployment; a `--cluster N` node's own admin port now behaves the same).
    **The dashboard still merges every node's `/admin/raftkv` into one
    cross-node view** (`dashboard_core.js::cpGroupsByTablet()`, via the
    `/admin/peers` fan-out, ADR 0021) — that aggregation is deliberate and
    happens at the HTTP-fan-out layer in the browser, not inside any one
    node's response. It relies on `CpRaftView::node` (`lib.rs::raft_view`)
    carrying each entry's real hosting node id explicitly
    (`self.env().node_id()`), because a merged response's origin server is not
    a reliable way to attribute a *particular* entry to a physical node once
    several nodes' responses are combined. **General lesson (still current):
    a debug/admin view whose response is merged across nodes by its consumer
    must carry each item's own identity in the payload** — the merging
    client cannot infer "whose state is this" from which server answered
    (`dashboard_core.js`'s `nodeByRaftkv(g.node)`, keyed on `tablet:node`).
    Before this fix, `/admin/raftkv` was *itself* cluster-wide in `--cluster
    N` (the shared `ClusterEdgeState` registered every node's CP group handle
    in one registry), which produced duplicate `{node, group}` entries
    mis-tagged with whichever admin port happened to answer and made every
    replica dot in a tablet's row resolve to the same (first) group's
    `is_leader` — a bug this file used to document at length. ADR 0031 PR2
    removed that root cause entirely (each node's `/admin/raftkv` now only
    ever lists its own groups), so `CpRaftView::node` is no longer covering
    for a per-response ambiguity, only for the dashboard's own deliberate
    cross-node merge.
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
  linearizable CP read + CP write are **atomic per node**. On the CQL edge that is
  every `INSERT`/`UPDATE`/`DELETE` (a partition RMW); on the DynamoDB edge it is
  the RMW ops only — conditional `PutItem`/`DeleteItem` (or `ReturnValues:
  ALL_OLD`), `UpdateItem`, and the whole of `TransactWriteItems` (one guard across
  all actions; the per-action helpers deliberately take no lock — the tokio Mutex
  is not reentrant). Unconditional puts/deletes and batch writes do no pre-read
  and take no lock. (The DynamoDB edge once took no lock at all — two concurrent
  `attribute_not_exists` puts on one node could both pass; regression in
  `tests/dynamo_extended.rs::concurrent_conditional_puts_one_wins`.) Cross-node
  atomicity (a CAS on the CP group) is later v1 work.
- **The wire edges snapshot the replicated `Metadata` once per request**
  (`dynamo.rs::run_operation` takes `let meta = &metadata(ctx)` and threads
  `&Metadata` through the helpers) — `RaftNode::metadata()` deep-clones under a
  lock, and a single request used to re-clone it 2+ times. Two rules keep the
  snapshot sound: (1) a path that must observe *fresh* state (the `CreateTable`
  commit-wait polls, the post-commit `mirror_catalog_schema`) reads live; and
  (2) **an existence gate that short-circuits a linearizable read must not
  conclude "absent" from the request-entry snapshot** — `quorum_read`'s
  "no tablet ⇒ no data" gate re-checks *live* on the snapshot-miss path, because
  a concurrent first write can provision the tablet after the request began, and
  under the `rmw_lock` a conditional writer's read must see it (two racing
  `attribute_not_exists` puts both succeeded when the gate trusted the snapshot —
  caught by `dynamo_extended.rs::concurrent_conditional_puts_one_wins`). Trust
  the snapshot on the hit path; re-verify on the miss path.
- Two combined-mode run modes: `--cluster N` (whole cluster in one process,
  dev convenience) and `--config FILE --node I` (one node per process — real
  deployment). Both share `Node::bind`/`start`; only address/peer assembly
  differs. **`animusd control --config FILE --node I` (ADR 0035 PR3)** is a
  third, control-only mode sharing the equivalent `Node::bind_control`/
  `BoundControlNode::start_control_with` pair, and **`animusd data --config
  FILE --node I` (ADR 0035 PR4)** is a fourth, data-only mode
  (`Node::bind_data`/`BoundDataNode::start_data_with`, `ControlHandle::
  Remote`) — see the entry-points section above for both. Running every
  control-role index of a `generate_split` config with `animusd control` and
  every data-role index with `animusd data` is the genuine split deployment
  ADR 0035 targets; a config can still mix in combined-mode indices (plain
  `--config FILE --node I` against a `Both`-role entry) for an incremental
  migration.
- **`--cluster N` without an explicit `--dir` defaults to ONE fixed path,
  `$TMPDIR/animusd` (`main.rs`), reused across every invocation on the
  machine — and `--ephemeral` does NOT make a run ephemeral with respect to
  that default dir.** `--ephemeral` only selects the CP-data group's
  `StorageBackend` (`Memory` vs `LsmEngine`, consumed later in
  `start_cluster_with`); `Node::bind` unconditionally opens the **control**
  role's `ProdEnv` at `dir/node-{i}/control` and the **raftkv** role's at
  `dir/node-{i}/raftkv` *before* that backend choice is ever consulted — so
  the replicated `Metadata` (tablet map, membership, schema catalog) and the
  raftkv role's own Raft WAL persist to disk across `--ephemeral` runs, and a
  "fresh" cluster silently inherits a previous run's tablet/split state
  (live-observed: a brand-new `--cluster 3 --ephemeral` already had a
  multiply-split tablet with a real range from an unrelated earlier run).
  Worse, **two `--cluster N` processes running concurrently without distinct
  `--dir`s will contend on the same on-disk control/raftkv WAL files** — a
  real correctness hazard for local dev (two agents/terminals each running
  `animusd --cluster 3` for a quick manual check), not just stale-state
  confusion. Always pass an explicit, freshly-created `--dir` for a
  throwaway manual run; don't rely on `--ephemeral` alone for a clean slate.
- **The wire edges' mutable state is `ClusterEdgeState`, scoped to one NODE**
  (ADR 0031 PR2 — not the whole process, and, since this change, not the whole
  in-process `--cluster N` cluster either). It holds this node's own control
  `RaftNode` handle (at most one — `propose_schema` proposes locally when this
  node is the control leader, else relays `ClientRequest::ProposeSchema` one
  hop to the leader's node via `client_route`, so a follower-connected
  `CreateTable`/`CREATE TABLE` still reaches the leader), this node's own
  hosted CP group handles (keyed by tablet), the DynamoDB `SchemaRegistry`
  (GSI/LSI index declarations — the base written-key index is gone, replaced
  by the native range scan), and the CQL `CqlState` (keyspaces + prepared
  statements). It is created **fresh per node** — once per node in
  `start_cluster_with`'s `--cluster N` bring-up loop (previously one instance
  shared by every node of the cluster; see the historical note in
  `ClusterEdgeState`'s own doc in `lib.rs`) and, as before, freshly in
  `run_node_with` (one per process) — and threaded into `start_with` →
  `ClientCtx::edge`. A **test harness running several independent clusters, or
  several nodes of the same cluster, in one process gets a distinct, isolated
  edge-state set per node**, so neither two clusters nor two nodes of one
  cluster ever share a registry or a handle set. (The per-*cluster* scoping
  originally replaced `OnceLock` process statics, which leaked across tests in
  one binary — a later test's `CreateTable` fanned its proposal across every
  still-running cluster's leaders and timed out; per-*node* scoping is the
  same fix taken one level further, closing the class of bugs the root
  `CLAUDE.md` documents under "the shared `--cluster N` edge masks per-node
  bugs.") Schema DDL routes through `ClusterEdgeState::leader_handle` +
  `ClientCtx::propose_schema`'s relay fallback; reads/writes resolve the table
  schema from this node's own replicated `Metadata`.
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
`tests/control_only.rs` (ADR 0035 PR3, `animusd control`: a bare control-only
cluster elects a leader and serves `/admin/status`/`/admin/health`/
`/admin/config` correctly with zero data members, quiescent over a bounded
polling window; schema DDL direct-propose + follower relay against a
control-only cluster; a mixed cluster — a control-only trio plus one
combined-mode data node reached via the ADR 0030 growth-node mirror — proving
a `Put` issued against the control node's client port provisions the table
and forwards to the data node, and a schema command issued against the data
node relays to the control leader), `tests/data_only.rs` (ADR 0035 PR4,
`animusd data`: a **genuine** split cluster — 3 control-only + 2 data-only
nodes, no combined-mode node anywhere — reads/writes across two data nodes
including a write via one and a read via the other, schema DDL issued
against a data node relaying to and committing on the control leader (with
every data node's own mirror converging on it too), a data node falling
over to a remaining control seed when one control node goes down, and a
data-node restart re-hosting its tablet from the surviving replica's real
Raft replication — the ephemeral memory backend is deliberate here, so
catch-up can only be real replication, never a local reopen),
`tests/data_join.rs` (ADR 0035 PR5, `animusd data --seed`: a data-only node
joins a running split cluster via `JoinInfo` discovery with no local control
`RaftCore` at all, is promoted `Active` with zero operator admin calls, gains
a real rebalanced tablet replica, and reads/writes round-trip through it both
ways), `tests/watch_metadata.rs` (ADR 0035 PR5, the long-poll
`ClientRequest::WatchMetadata` wire primitive: a genuine control replica
wakes a parked watch on the actual commit, well inside its server-side
timeout bound, and a data-only node rejects the request outright rather than
degrading), `tests/split_cluster.rs` (ADR 0035 PR6, scenarios spanning a
**genuine** split deployment — real `animusd control` + `animusd data`
processes, no combined-mode node anywhere — beyond PR3–PR5's own coverage:
control-LEADER failover under live data traffic with no lost acked write and
a post-failover DDL still committing; tablet split + merge triggered against
the data fleet's own admin port; a data-node failure detected and repaired
onto a spare; decommission of a data node gated to the control leader's
admin port, with the data node's own admin port refusing with a
leader-routing hint; and a full stop/restart of every process recovering
both control metadata and data from disk),
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
(cross-process CP forwarding to the leader's node, incl. a second provisioned
table's group — not just the first — forwarding correctly), `tests/admin_endpoint.rs` (the
admin / debug interface, ADR 0020: a per-process 3-node cluster, then the read-only
views config/status/raft/raftkv/storage·wal/metrics/health over the dedicated admin
port + the `storage/flush` action observed via `storage/lsm`; metrics asserted on
the control leader since sinks are per-node; bring-up wrapped in the port-TOCTOU
retry), `tests/dashboard_endpoint.rs` (the web dashboard, ADR 0021: `GET /` serves
the embedded SPA as `text/html`, every `/admin/ui/<tab>` deep link (incl. an
unrecognized tab name) also serves it, `/admin/*` responses carry the CORS header, an
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
