# CLAUDE.md — animusd

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The runnable AnimusDB node server — a **lib + bin**. `lib.rs` assembles a node
over `ProdEnv` (the first real use of the production seam): a control-plane Raft
(`animus-control`) for cluster metadata plus the CP data plane (`animus-cp-data`,
one leaderful Raft group per tablet) for linearizable reads/writes, fronted by
four wire edges (DynamoDB JSON/HTTP, CQL v4, a plain length-prefixed TCP client
protocol, and an admin/debug HTTP-JSON port with a web console). `main.rs` is a
thin CLI wrapper. `animus-cli` depends on this crate for the client protocol
types. v1 (ADR 0019) is **CP-only**; the leaderless AP `data`/`coord` roles are
gone.

**`lib.rs` is ~6800 lines** — grep for the symbol, don't scroll. It also holds
two in-crate `#[cfg(test)] mod`s that need private handles the `tests/` tree
can't reach: `split_fence_tests` (lib.rs:6452) and `auto_split_median_tests`
(lib.rs:6725).

## Module map (`src/`)

- **`lib.rs`** (~6800 lines) — the node assembly and everything routing/hosting.
  `Node`/`BoundNode`/`BoundControlNode`/`BoundDataNode` (bind → start pairs),
  `ClientCtx` (per-request context), `ClusterEdgeState` (per-node mutable edge
  state), all `run_node*` entry points, CP routing (`cp_route`/`cp_forward` +
  `FORWARD_ELECTION_BACKOFF`, `CLIENT_TIMEOUT`), `tablet_host_reconciler_loop`,
  `auto_split_loop`, `byte_weighted_median`, the `ClientRequest`/`ClientResponse`
  protocol types + `read_frame`/`write_frame`, and the two in-crate test mods
  above.
- **`main.rs`** — thin CLI wrapper; dispatches the invocation modes (below) and
  wires `otel::init_tracing` + the Ctrl-C graceful-shutdown path.
- **`config.rs`** — `ClusterConfig` (per-process deployment config), `RoleAddrs`
  (a node's six addresses + `role: NodeRole` = `Control`/`Data`/`Both`),
  role-filtered accessors (`control_ids`/`raftkv_ids`/`control_peer_book`/
  `raftkv_peer_book`/`peer_book`), `generate`/`generate_split`, and the
  **six-port stride** (`base_port + 6*i + {control,client,dynamo,cql,raftkv,
  admin}`; node ids: control `i`, raftkv `300+i`).
- **`control_handle.rs`** — the `ControlHandle` seam (ADR 0035 PR1):
  `Local(RaftNode<ProdEnv>)` for a node with real control Raft, vs.
  `Remote(RemoteControlClient)` for a data-only node reaching a separate control
  deployment over the network. `metadata_cached()` vs. `metadata_fresh()`
  freshness contract lives here.
- **`topology.rs`** — pure, side-effect-free routing decisions extracted from
  `lib.rs` for unit-testing: `decide_cp_route` (→ `RouteDecision`), `tablet_for_key`,
  and `format_not_leader_refusal`/`parse_not_leader_refusal` (the leader-hint
  string suffix `cp_forward` chases). All `pub(crate)`.
- **`dynamo.rs`** (~59 KB) — the DynamoDB JSON-over-HTTP edge; the `GET /metrics`
  route (ADR 0015) shares this listener.
- **`cql.rs`** (~42 KB) — the CQL (Cassandra) v4 binary-protocol edge.
- **`cql_client.rs`** — a minimal loopback CQL client the admin dashboard's CQL
  editor uses (`POST /admin/data/cql`) to drive this node's own CQL port.
- **`admin.rs`** (~58 KB) — the admin/debug HTTP-JSON endpoint (ADR 0020):
  read-only `GET` views + gated `POST` actions + the dashboard's data-write
  surface; also serves the SPA static assets.
- **`http.rs`** — shared hand-rolled HTTP/1.1 helpers (request parser + response
  writers) used by both `dynamo.rs` and `admin.rs`.
- **`otel.rs`** — OTLP/HTTP distributed-tracing seam (ADR 0027); opt-in, no-op
  unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Scoped to this crate only.
- **`dashboard.rs`** + **`dashboard.{html,css}`** + **`dashboard_{core,overview,
  placement,tablets,browser,storage,node}.js`** — the "AnimusDB Console" SPA
  (ADR 0021), `include_str!`'d and served as distinct static assets. Vanilla JS,
  no bundler/CDN. Tabs are role-gated client-side (ADR 0035 PR7).

## CLI reference

`main.rs` documents these invocation modes (durable LSM backend by default;
`--ephemeral` selects the volatile memory engine):

| Invocation | What it does |
|---|---|
| `gen-config --nodes N [--host H] [--base-port P]` | print a combined-mode cluster config (JSON) |
| `gen-config --control-nodes N --data-nodes M [--host H] [--base-port P]` | print a split-deployment config (ADR 0035) |
| `--config FILE --node I [--dir DIR] [--ephemeral]` | run node I of a config, combined mode (one process per node) |
| `--cluster N [--dir DIR] [--ip ADDR] [--ephemeral] [--auto-split K] [--auto-split-bytes B]` | run an N-node combined cluster in one process (dev) |
| `--cluster-control N --cluster-data M [--dir DIR] [--ip ADDR] [--ephemeral] [--auto-split K] [--auto-split-bytes B]` | run a whole split deployment in one process (dev, ADR 0035) |
| `join --seed ADDR[,ADDR...] [--node I] [--ip A] [--base-port P] [--dir D] [--ephemeral]` | combined-mode seed/join startup (ADR 0032 PR2; `--node` omitted → ADR 0036 cluster-allocated id) |
| `control --config FILE --node I [--dir DIR]` | run node I as a control-only node (ADR 0035 PR3) |
| `data --config FILE --node I [--dir DIR] [--ephemeral]` | run node I as a data-only node (ADR 0035 PR4) |
| `data --seed ADDR[,ADDR...] [--node I] [--ip A] [--base-port P] [--dir D] [--ephemeral]` | data-only seed/join (ADR 0035 PR5; `--node` omitted → ADR 0036 cluster-allocated id) |

`--auto-split K` (key count) and `--auto-split-bytes B` (byte size) are
independent OR-gated triggers — either, both, or neither. `join`/`data --seed`
derive six consecutive ports from `--base-port` (default `7100 + 6*I`) when
`--node I` is given. **`--node I` is optional on `join`/`data --seed`** (ADR
0036): omit it to have the control plane mint this node's id atomically from
its own `MetaCommand::AllocateNodeId` monotonic allocator instead of an
operator picking `I` — but then `--base-port` is **required** (an allocated
id is not a small index, so there's no `7100 + 6*I` to fall back to) and the
join is **ephemeral-identity**: a restart with a fresh dir gets a *new*
allocated id, and the old id's `Member` entry lingers `Down`/address-less
forever (never reused, prunable later via the existing `RemoveMember`/
decommission path). `--node I`'s durable, restart-stable identity is
unaffected — this is purely additive (`run_node_join_allocated`/
`run_node_data_join_allocated` in `lib.rs`, alongside the untouched
`run_node_join`/`run_node_data_join`).

## Deployment shapes (ADR 0035)

Three shapes, all built from the same role assemblies:

- **Combined** — every node runs both roles. `--cluster N` (one process) or
  `--config FILE --node I` (one process per node), against a `Both`-role config.
  `Node::bind` → `BoundNode::start_with`.
- **Control-only** — a small static metadata quorum, no storage engine, no data
  role. `animusd control --config FILE --node I`. `Node::bind_control` →
  `BoundControlNode::start_control_with`; has real local control Raft.
- **Data-only** — no local control `RaftCore` at all; `Metadata` comes from a
  polled/long-polled mirror of a separately-deployed control plane via
  `ControlHandle::Remote`. `animusd data --config FILE --node I` (or `data
  --seed ADDR`). `Node::bind_data` → `BoundDataNode::start_data_with`.

A config may **mix** combined-mode indices with control-only/data-only ones for
incremental migration. `--cluster-control N --cluster-data M` and
`start_split_cluster_with` are the in-process (dev) equivalent of a genuine
split deployment; each in-process node still gets its own `ClusterEdgeState` and
reaches others only through real forwarding/relay/mirror paths.

`BoundNode::start_with` and `BoundControlNode::start_control_with` share a
private `spawn_common_tail` helper (route/metrics/self-registration/serve/admin);
role-specific tasks (`bootstrap`, `peer_sync_loop`, the growth mirror,
`heartbeat_loop`, the reconciler, `auto_split_loop`, dynamo/cql listeners) are
appended by each `start_*` after it returns.

## Request routing (CP)

Five `ClientCtx` primitives resolve the tablet's group leader the same way via
`cp_route` (pure core: `topology::decide_cp_route`): `cp_read` (linearizable
ReadIndex), `cp_write`/`cp_delete` (Raft-committed, waited to durable+applied),
`cp_scan` (linearizable range read), and `cp_batch_write` (groups keys by tablet,
commits each group as one `KvCommand::Batch` entry — atomic within a tablet, not
across; backs DynamoDB `BatchWriteItem` and the admin seeder).

`cp_route` serves **locally** if this node hosts the leader; **forwards** one hop
(`ClientRequest::Forwarded { request, traceparent }`) to the leader's node if a
local replica gives a hint + a `client_route` exists; otherwise **waits** for the
local group to elect (never forwards to a non-leader, including itself, during
election). **One-hop invariant**: the receiver (`cp_serve_forwarded`) never
re-forwards.

**Hinted-retry forwarding** (`ClientCtx::cp_forward`, the single choke point for
every forward): a "not the leader here" refusal carries the refusing node's own
leader hint (`topology::format_not_leader_refusal`, a plain string suffix so old
and new binaries interoperate); `cp_forward` chases it — retry at the hint if
untried, else at another of the tablet's known replicas, bounded to one pass over
{hint} ∪ replicas within the overall `CLIENT_TIMEOUT`.

**Election-wait backoff (PR #106)**: when *every* candidate refuses with
`leader_hint=none` (the group is mid-election — a split-child/first-provision
formation window, or a crashed leader), one exhausted pass is not a failure.
`cp_forward` backs off `FORWARD_ELECTION_BACKOFF` (100ms, ≈ one election timeout,
lib.rs:470) and re-runs the pass, still hard-bounded by `CLIENT_TIMEOUT` — the
forwarded dual of the local path's `RouteDecision::Wait`. Gated on the tablet
being resolvable so an unmappable op still fails fast. Regression:
`tests/cluster_split.rs::single_shot_first_write_through_control_node_succeeds`.

**Write fences (ADR 0028)**: `cp_put_local`/`cp_delete_local`/`cp_batch_propose`
each (1) **pre-check** the target group's live `RaftKvNode::scope_range()` and
reject before proposing if any key falls outside it (returning a routing-failure
error so the caller re-resolves and reaches the correct child), and (2) **stamp**
that range as the proposed entry's `fence` (`put_fenced`/etc.). The pre-check is
load-bearing: a fenced-out entry still commits as a no-op, so a confirm keyed on a
coarser signal would falsely-ack; the embedded fence only covers the sliver
between pre-check and apply. `cp_get_local`/`cp_scan_local` run the read-side dual
(ADR 0033): a read resolving to a group whose live scope doesn't cover the
request errors retryably rather than serving a false "absent" (for scans, avoids a
silent truncation). See the in-crate `split_fence_tests`.

## Control-plane access

`ClientCtx.control` is a `ControlHandle`, not a bare `RaftNode`. Reads split by
freshness contract:

- `metadata_cached()` — staleness-tolerant. `effective_metadata()` layers the ADR
  0030 growth-node / data-only mirror on top.
- `metadata_fresh()` — read-your-writes, never mirror-substituted; **`async`** (a
  real round trip for `Remote`). Used by schema commit-wait polls, the DynamoDB
  conditional-write existence gate, and `provision_tablet`'s initial replica-set
  read.

For `Local` the two are identical (`raft.metadata()`); `Remote` genuinely differs
(mirror vs. network fetch). **Proposing is inherently local-Raft-log-only** —
`ClusterEdgeState::leader_handle()` stays a concrete `RaftNode` registry and never
goes through `ControlHandle`; `Remote` returns inert honest values for
`is_leader()`/`term()`/etc.

**`config()` returns `Option<BTreeSet<NodeId>>`, not a bare set (ADR 0037 PR2).**
`Local` is always `Some(raft.config())` — a genuine control-group replica reading
its own live `RaftCore` config. `Remote` has no local `RaftCore`, so it answers
the last control-voter set it has *observed on the wire* (`RemoteControlClient::
control_voters`, fed by `observe()` under the same freshness gate as the metadata
mirror) — `None` until the first `Status`/`WatchMetadata` reply lands. This is
deliberately an `Option`, not an always-populated `BTreeSet::new()` default as it
used to be: "never fetched yet" and "the control group genuinely has zero
voters" must stay distinguishable to any caller that cares (see the
engineering-lessons "handle has no local authority" entry) — most callers don't
and just `.unwrap_or_default()` it (`/admin/raft`'s `voters` field, the
`ClientResponse::Status::control_voters` wire field below).

**`ClientResponse::Status` carries `control_voters: BTreeSet<NodeId>`
(`#[serde(default)]`, ADR 0037 PR2)** — the answering node's own
`ctx.control.config().unwrap_or_default()` at reply time. This is the wire echo
of the *live* Raft config that actually governs control-plane quorum, distinct
from `Metadata.node_addrs`' `role: "control"` bookkeeping (a discovery hint: a
node can be registered with the control role and not currently be a live voter —
before its membership change lands, or after it's been removed). It rides the
same `Status`/`WatchMetadata` round trip `metadata_fresh()`/the mirror sync loop
already make, so a `Remote` node's own `RemoteControlClient` picks it up for
free — no new request type. A future control-plane membership-change admin
surface (later PR in the ADR 0037 stack) is the intended reader of this on a
`Remote`/CLI/dashboard caller that needs "who can I even try talking to."

**Discipline**: a read feeding a *non-retried, permanent* decision must use
`metadata_fresh()`, not `metadata_cached()`/`effective_metadata()` — a data-only
node's routinely-stale mirror makes that window wide. The type system can't catch
this (`Remote` and `Local` both compile). Grep every `metadata_cached()` call
site when adding a `ControlHandle` consumer. `provision_tablet` was fixed for
exactly this (RF silently pinned at 1); see the root `CLAUDE.md`
engineering-lessons log.

**`Remote` internals** (`RemoteControlClient`): `seeds` (the control deployment's
client-API addresses), a polled `mirror`, and a `leader_hint`. `metadata_fresh()`
tries the hint first, else scans every seed. `ClientResponse::Status` carries
`leader_hint` and a `watermark: u64`; the long-poll `ClientRequest::WatchMetadata
{ last_seen }` (ADR 0035 PR5) gives a `Remote` node a real wake-on-commit signal
via `remote_metadata_watch_loop` (a genuine `Local` replica serves it, parking on
`metadata_watch().changed(last_seen)` up to an 8s server bound; a `Remote` node
rejects it outright). `RemoteControlClient` owns its own driven `MetadataWatch`
(this required making `animus_control::MetadataWatch::bump` `pub`).

**The ADR 0030 growth-node branch of `remote_metadata_sync_loop` uses the same
long-poll mechanism**, not the original fixed-200ms `Status` poll — a growth
node's `ClientCtx.control` stays `ControlHandle::Local` (a real, permanently
non-voting control-group member, not `Remote`), so it constructs a standalone
`RemoteControlClient::with_mirror(seeds, ctx.remote_metadata.clone())` sharing
`ClientCtx.remote_metadata`'s existing `Arc<Mutex<Option<Metadata>>>` directly
as its mirror, then drives it through the same `remote_metadata_watch_loop`.
Pure latency improvement — the reconciler's own wake source is unaffected (a
growth node's local raft never advances, so its `metadata_watch()` still never
fires; `RECONCILE_FALLBACK_INTERVAL` still drives its ticks, just off a
fresher mirror). Regression: `tests/cluster_growth.rs::
growth_node_observes_metadata_promptly_via_watch`. **Gotcha surfaced by this
port**: a `WatchMetadata` request already in flight to a node at the instant
it's killed via `Node::shutdown()` doesn't fail over quickly — `shutdown()`
can't abort an already-spawned `serve_clients` per-connection handler task
(fire-and-forget, no tracked `JoinHandle`), so the zombie handler's
`select! { changed(..), sleep(8s) }` always falls through to the timeout arm
(its watch can never advance once the driver is dead) and replies with
stale-but-plausible cached data up to 8s late. A fixed-sleep assertion right
after a test's node-kill can be outrun by this; poll to convergence instead
(see the engineering-lessons log).

## Tablet lifecycle

**The per-node tablet-host reconciler (ADR 0031 PR4) is the single owner of this
node's tablet lifecycle** — it replaced three separate loops (`cp_join_host_loop`,
`cp_gc_loop`, `cp_reconfigure_loop`) and their state. The pure `plan` +
`Reconciler` executor live in `animus_cp_data::host` (read that crate's
`CLAUDE.md`); `plan` decides every action from one `MetadataView` snapshot per
tick and executes them in fixed order (`NarrowScope` → `Host` → `Reconfigure` →
`Release`/`Reclaim`; merge adds `WidenScope`/`Absorb`). What stays in `animusd`
(`tablet_host_reconciler_loop`):

- **Trigger**: one task per node racing `ctx.control.metadata_watch().changed(..)`
  (event-driven — observes a change on the commit that made it) against a
  `RECONCILE_FALLBACK_INTERVAL` (500ms) sleep. The fallback is **load-bearing for
  growth / data-only nodes** whose local control Raft never advances (their watch
  never fires; the mirror is read via `effective_metadata()`). Coalesce to
  `watch.latest()` after a wake so a commit burst collapses to one tick.
- **Pre-recovery guard**: skip while `raft.last_applied() == 0` **and** the remote
  mirror is empty (default-empty `Metadata` would read as "everything dropped").
  A data-only node needs the third signal `has_synced_metadata()`.
- **Edge mirror**: `ClusterEdgeState`'s `raftkv` registry is a read-only mirror
  with exactly one writer — the reconciler's `on_host`/`on_teardown` hooks.
- **Formation**: `Epoch::INITIAL` (or `StorageScope::has_data` on restart) ⇒ full
  voter config; a bumped epoch ⇒ quiet non-voter until the leader adds it. Dedup
  is `LocalState::hosted`.

**Auto-split (byte-based, ADR 0034)**: `auto_split_loop` gates per-tick on
`CpGroup::approx_key_count` (LSM-only) **and** `CpGroup::approx_bytes` (either
backend). The split point matches the metric: a byte-configured cluster splits at
`byte_weighted_median` (private to `lib.rs`, unit-tested in
`auto_split_median_tests`) — which scans every achievable key-boundary cut for the
one closest to half the bytes, not a single accumulate-and-threshold pass (subtly
wrong when one key dominates; see the root log). Key-count clusters keep the plain
positional median. Auto-merge triggering is out of scope — merge is
operator-driven.

**Split / merge** (ADR 0028 / 0033) are each a single atomic control-plane command
(`MetaCommand::SplitTablet`/`MergeTablets`, epoch-CAS gated) — there is no
data-plane half. Split narrows the source's range and mints a sibling on the same
shared engine; merge widens `left` to absorb `right`, recording `right` in the
never-pruned `Metadata::merged_tablets` marker (needed because a
hosted-but-vanished tablet looks identical whether merged or its table dropped).
The reconciler reacts with `WidenScope`/`Absorb` (absorb tears down **without
erasing** — a sibling now serves the range). `trigger_split`/`trigger_merge`
propose and poll for the exact effect. Exposed via `POST /admin/tablet/{split,
merge}` + `ClientRequest::{SplitTablet,MergeTablets}` (relayable).

**Drop-table GC** (ADR 0024) is the reconciler's `Reclaim` action;
**removed-replica GC** (ADR 0029) is its `Release` dual (moved off this node while
the tablet still exists — a drain/repair/rebalance). Both run
`shutdown()`+wait-`is_stopped()` then `erase_scope()` + delete the per-tablet WAL.
Release is gated on the **local durable Raft config already excluding `base_id`**
plus an epoch-stability dampener (`RELEASE_CONFIRM_TICKS`). The release erase is
bounded by the tablet's **current replicated range** (`HostAction::Release`'s
`erase_bound`), never a stale-wide in-memory scope — the invariant ADR 0031 makes
structural. Drop + GC are convergent (a restart replays through historical map
states) — test post-restart state with a poll, never a fixed sleep. A new
`MetaCommand` that must commit from a follower-connected node must be added to
`is_relayable_command` (missing there is a bimodal per-process flake).

## Wire edges

All edges are production-only I/O (real tokio sockets, hand-rolled framing) and
route below the edge through the same `ClientCtx` CP primitives.

- **DynamoDB** (`dynamo.rs`, `RoleAddrs.dynamo`) — decodes `X-Amz-Target` +
  AttributeValue-JSON via `animus_dynamo::wire`. `CreateTable` proposes its key
  schema **and** GSI/LSI *definitions* into the replicated catalog (ADR 0013) and
  waits for commit (survives restart); a node reconciles its local registry from
  `Metadata::table_indexes` via `mirror_catalog_schema`/`sync_indexes`. Index
  *entry data* stays in-memory, maintained from observed writes and **lazily
  backfilled** on first index query (`backfill_index_if_needed`). Base-table
  `Query`/`Scan` use `cp_scan` (no in-memory key tracking). Surface also covers
  `UpdateItem`/`BatchWriteItem`/`TransactWriteItems` (condition-gated, not yet
  atomic). `DeleteItem` writes a tombstone *value*.
- **CQL v4** (`cql.rs`, `RoleAddrs.cql`) — `STARTUP`/`OPTIONS` handshake +
  `QUERY`/`PREPARE`/`EXECUTE` via the pure `animus_cql` crate. `CREATE TABLE`
  proposes a typed schema into the replicated catalog (incl. clustering/compound
  keys). A partition is one CP value, so `INSERT`/`UPDATE`/`DELETE` are RMW under
  `rmw_lock`; the requested consistency level is accepted but moot (CP).
  Keyspaces are **replicated** (`CREATE KEYSPACE` proposes
  `MetaCommand::CreateKeyspace` into the control plane's `Metadata`, ADR 0013;
  `USE`/qualifier validation reads the replicated set via `keyspace_exists`,
  with a `ks.table`-prefix fallback). Only the **prepared-statement store**
  (`CqlState`) is per-node edge state (shared across connections *to the same
  node*, isolated between nodes, lost on restart); prepared ids are
  content-addressed (FNV-1a of the text).
- **Admin / debug** (`admin.rs`, `RoleAddrs.admin`, ADR 0020) — read-only `GET`
  views (`/admin/{config,status,peers,raft,raftkv,storage/*,metrics,metrics/
  history,member/drain-status,health,control/members}`) + gated `POST` actions
  (`/admin/{tablet/split,tablet/merge,storage/flush,storage/compact,raftkv/
  reconfigure,drain,member/add,member/remove,control/member/add,control/
  member/remove}`) + data writes (`/admin/data/{dynamo,cql,drop-table,
  seed}`). Below the edge it only reads node state (aggregated live per request) or
  drives a gated action. **No auth — bind to a trusted interface.** The `animus
  admin` CLI consumes it. The bulk seeder (`action_data_seed`) writes real
  **DynamoDB items** — key attributes resolved from the replicated catalog
  schema (ADR 0013), key/value bytes built exactly as the DynamoDB edge's
  `PutItem` would (`dynamo::item_key` + `wire::encode_stored_item`, ADR 0022),
  so seeded rows read back through `GetItem`/`Query`/`Scan` — in
  `cp_batch_write_patient` batches, wrapped in its own `admin_seed` span (it
  bypasses `handle_client`, so it needs one to emit any trace). `key_display`/`parse_key_display` render a binary partition
  token as unpadded base64url; a plain-client key is verbatim/printable.
  `/admin/peers`'s `peers: [{admin, role}, ...]` field (ADR 0035 residual
  follow-up, `admin.rs::peers_view`) carries each node's deployment role
  straight off replicated `Metadata.node_addrs[*].role` — closing the gap
  where role was only knowable by fetching that specific node's own
  `/admin/config` first; `admin_addrs` itself is unchanged.
- **Web console** (`dashboard.rs` + assets, ADR 0021) — a self-contained
  vanilla-JS SPA, a pure client of `/admin/*` JSON (so responses carry CORS). Six
  views seeded by a `/admin/peers` fan-out; tabs are **role-gated client-side**
  (`applyRoleGating`, ADR 0035 PR7) — a data-only node shows a dedicated **Node**
  view (`dashboard_node.js`) instead of the cluster-wide tabs. `loadSelf()`
  resolves this node's own role from a self-only fetch, kept separate from the
  slower cluster-wide fan-out. `/admin/config` carries a derived `role` string;
  `/admin/raft` carries a `control_mirror` object for the Node view. The
  Overview groups nodes as "Control plane" / "Data nodes" when any
  control-only node exists (a combined cluster keeps the flat list), and every
  reachable node's row — plus the Placement view's selected-node header —
  carries a `consoleLink()` (`dashboard_core.js`) to that node's OWN admin
  console, built from the origin the `/admin/peers` fan-out already resolved
  (empty for this page's own origin — a self-link is noise). **Cluster health
  means "is the data at risk," not "is anything in transition"** (ADR 0021 §7):
  `tabletStatus`'s ladder (`quorum-lost` → `under-replicated` → `healthy` →
  `forming`) only degrades on an actual redundancy/quorum loss; a split-child
  or freshly-provisioned tablet forming its Raft group with every assigned
  replica's node alive renders as a neutral `forming` pill, escalating to
  degraded only if stuck past 60s (`computeHealth`'s `overdueFormingCount`).
- **OTel** (`otel.rs`, ADR 0027) — `init_tracing(instance_id)` from `main.rs`;
  `current_traceparent`/`set_parent_traceparent` carry W3C trace context across a
  forwarded hop (`cp_forward` injects, the receiver's `handle_client`
  re-parents), so a forwarded write is one joined trace when export is enabled.
- **`GET /metrics`** (ADR 0015) shares the DynamoDB listener; `ClientCtx::
  metrics_text` aggregates both role sinks (control + raftkv) live at request time.

## Gotchas

- **A node runs two internal `ProdEnv` roles on distinct ids** — control (id `i`)
  and raftkv (id `300+i`) — because one inbox is single-consumer; never run two
  protocols on one node id. The client API is a plain TCP server, *not* on the
  `Network` — a non-leader forwards over a fresh client connection.
- **`ClusterEdgeState` is scoped to one NODE** (ADR 0031 PR2), created fresh per
  node — even in `--cluster N`, which previously shared one instance across the
  cluster and masked cross-process bugs. Holds this node's own control handle, its
  hosted CP group handles (keyed by tablet), the DynamoDB `SchemaRegistry`, and
  the CQL `CqlState`. No process-global (`OnceLock`) mutable state.
- **`ClientCtx.data: Option<DataRole>`** groups the data-role-only fields
  (`rmw_lock`, `raftkv_metrics`, `base_id`). `ClientCtx::data()` **panics** if
  absent — safe only from paths that structurally can't run on a control-only node
  (dynamo/cql edges, `auto_split_loop`). `resolve_cp_route` must never panic — it
  matches `self.data.as_ref()` directly (control-only node ⇒ zero local replicas).
- **`--cluster N` without `--dir` reuses ONE fixed path** (`$TMPDIR/animusd`), and
  `--ephemeral` does NOT make the control/raftkv WALs ephemeral (it only selects
  the CP-data `StorageBackend`). Two concurrent `--cluster N` runs contend on the
  same on-disk WALs — always pass a fresh explicit `--dir` for a throwaway run.
- **The cluster's members are the raftkv ids, not the control ids** — `bootstrap`
  (leader-only, idempotent) registers `300+i` as `Active`. Failure detection runs
  over `ProdEnv`: each node's `heartbeat_loop` heartbeats the control group *as its
  raftkv id*, so the control leader's `detect_loop` marks a crashed CP node `Down`.
- **Online growth (ADR 0030) is data-plane only** — the control group stays static;
  a grown node's control role is a permanent non-voter and mirrors `Metadata` via
  `remote_metadata_sync_loop` into `effective_metadata()` — long-polling
  `ClientRequest::WatchMetadata` (ADR 0035 PR5's mechanism, ported onto this
  branch too — see the `ControlHandle` section above), not a fixed-200ms
  `Status` poll. A replicated node
  address book (ADR 0032 PR1, `Metadata.node_addrs` + `route_sync_loop`) keeps
  `client_route`/`/admin/peers` live so forwarding reaches nodes grown in later.
- **A node's deployment role rides that same replicated address book**
  (`NodeAddrs.role: String`, ADR 0035 residual follow-up) — each of
  `BoundNode::start_with`/`BoundControlNode::start_control_with`/
  `BoundDataNode::start_data_with` stamps its own literal role
  (`"combined"`/`"control"`/`"data"`) at its `NodeAddrs` construction site, so
  `/admin/peers` can report every OTHER node's role straight from
  `Metadata.node_addrs` instead of the dashboard fanning out to each node's
  own `/admin/config` just to learn it. `#[serde(default = "combined")]` for
  WAL back-compat (every pre-ADR-0035 registration was combined-mode).
- **Decommission (ADR 0032 PR3)** = `drain` + `MetaCommand::RemoveMember`; check
  leadership *before* any metadata-dependent refusal (a follower's replica lags).
  Not a fence — a restarted process at the same raftkv id rejoins like a fresh
  join.
- **Cluster-allocated member ids (ADR 0036)** live in a disjoint id range
  (`animus_control::meta::ALLOC_ID_BASE = 1_000_000`, far above
  `config::RAFTKV_ID_BASE = 300`) so an allocated id can never collide with an
  operator-chosen `--node I` id — see `MetaCommand::AllocateNodeId`'s doc in
  `animus-control` for the allocator itself. `config::synthetic_control_id_for`
  (`raftkv_id | (1 << 63)`) derives a combined-mode allocated join's *local,
  non-replicated* placeholder control id from its freshly minted raftkv id
  (there's no small operator index to derive one from, unlike `control_id
  (index)`) — never written to `Metadata`, never dialed by another process,
  purely a structurally-safe permanent-non-voter placeholder exactly like a
  `--node`-indexed join's real control id serves. `is_relayable_command`
  (below) must allow `AllocateNodeId` — a joining process has no local
  control role at all yet, so it is that process's *only* way to reach the
  real leader.
- **Control-plane membership change (ADR 0037 PR3)**: `ClientCtx::
  admin_add_control_member`/`admin_remove_control_member` (`lib.rs`, near
  `admin_add_member`/`admin_remove_member`) grow/shrink the control group's
  *live* `RaftCore` config at runtime — local-control-leader-only, **not**
  relayed, **not** in `is_relayable_command` (the underlying primitive is
  `RaftNode::change_membership`, not a `MetaCommand` proposal, so there is no
  meaningful "relay" shape for it — only a genuine control-group voter's own
  in-process handle can call it). `POST /admin/control/member/{add,remove}` +
  `GET /admin/control/members` in `admin.rs`; `animus admin
  control-{add,remove,grow}` in `animus-cli`. Add takes an **operator-
  supplied** id below `animus_control::meta::ALLOC_ID_BASE` (the ADR 0036
  allocator is not wired into this path — a stopgap, flagged as future work)
  and the new voter's **internal control-Raft** address (distinct from its
  admin/client/raftkv ports — `animus admin control-add` resolves it from the
  new node's own `/admin/config` so the operator only ever deals in admin
  addresses). **Known scope limit**: making a freshly-added voter actually
  *reachable* needed `ProdEnv::merge_peer` (a new incremental peer-book
  update, since the control role never had `peer_sync_loop`'s per-tick
  static-∪-replicated overlay the `raftkv` role has) — called only on the
  **local leader's own** env, so a *later* leader (after a subsequent
  transfer or crash) has no path to independently rediscover that address;
  see `animus-env/CLAUDE.md`'s `merge_peer` entry and
  `docs/engineering-lessons.md`'s "Code patterns" entry on this gap.
  Remove's quorum-loss warning (down to 1 voter) is the only implemented
  trigger — the plan's second trigger ("every other voter believed Down")
  was dropped: `ControlHandle::believes_alive` is keyed to **raftkv** ids
  (see the "cluster's members are the raftkv ids, not the control ids"
  gotcha above), so calling it with a control id is always `false`, not
  "unknown" — see the engineering-lessons "id-space mismatch" entry.
  Removing the current leader's own slot arms a `transfer_leadership` and
  returns the same not-leader refusal every other case here uses (never a
  silent success) rather than trying to complete the removal itself once it
  has stepped down.
- **The CP group is durable by default** — one shared `LsmEngine` over the raftkv
  env, cloned into every tablet's `RaftKvNode`; acked writes survive restart. Files
  use a flat filename prefix (`LSM_PREFIX = "db-"`), not a subdirectory (`ProdEnv`'s
  disk doesn't create intermediate dirs). Node-start entry points are
  async+fallible (`io::Result`).
- **`Node::shutdown()` is a graceful teardown** — aborts the listener tasks and
  `ProdEnv::shutdown()`s both role envs, freeing all six ports so a replacement can
  rebind the same addresses/dir. Dropping a `Node` without it leaves tasks running.
  **It's fire-and-forget (`abort()` then return), not a guarantee those ports are
  free the instant it returns** — see `animus-env/CLAUDE.md`'s `ProdEnv::shutdown()`
  entry. A same-address restart needs **`Node::shutdown_and_wait()`** (aborts, then
  waits for every task to actually finish) or, more commonly, just
  `shutdown_graceful()` — which now ends in `shutdown_and_wait` rather than the
  plain `shutdown` — so every existing restart test got this fix for free without
  a test-file change. This was the actual root cause of the
  `full_split_cluster_restart_recovers_metadata_and_data` flake under `cargo test
  --workspace`; see `docs/engineering-lessons.md`'s "abort() is a request, not a
  guarantee" entry.
- **A merged-across-nodes admin view must carry each item's own identity** —
  `/admin/raftkv`'s `CpRaftView::node` carries the real hosting node id because the
  dashboard merges every node's response; the answering server isn't a reliable
  attribution once combined.
- **CP writes need no client-assigned version** — the Raft log index *is* the MVCC
  version, so per-key LWW reproduces the agreed order.
- Several gotchas here are instances of cross-cutting lessons — port-TOCTOU
  bring-up retries (`support::restart_same_addrs`), "a flaky `ProdEnv` test is a
  real bug", restart-test discipline (poll for catch-up, not leadership),
  converged-or-timeout polls for eventual properties, retry loops distinguishing
  never-accepted from accepted-unconfirmed. See the **engineering-lessons log
  (root `CLAUDE.md`)** for the general form of each.

## Tests

`cargo test -p animusd` — all tests are real-socket `ProdEnv` integration tests
that poll with timeouts, not deterministic assertions. The restart tests run both
incarnations in the same runtime, calling `Node::shutdown()` between them. Two
in-crate `#[cfg(test)] mod`s (`split_fence_tests`, `auto_split_median_tests`) live
in `lib.rs` because they need private handles.

Test-file map (`tests/`):

- `cluster.rs` / `per_process.rs` — in-process cluster / independently-started
  nodes from a shared config.
- `cluster_split.rs` — in-process split deployment (`start_split_cluster_with`):
  `in_process_split_cluster_serves_writes_and_reports_roles`,
  `fixed_control_node_write_read_is_deterministic` (20 keys through one fixed
  control node, no round-robin), `single_shot_first_write_through_control_node_
  succeeds` (the PR #106 election-wait regression).
- `control_only.rs` — bare control-only cluster + schema DDL relay + a mixed
  cluster (ADR 0035 PR3).
- `data_only.rs` — genuine split cluster, 3 control-only + 2 data-only (PR4).
- `data_join.rs` — `animusd data --seed` joining a split cluster (PR5).
- `watch_metadata.rs` — the `WatchMetadata` long-poll wire primitive (PR5).
- `split_cluster.rs` — genuine multi-process split deployment scenarios: control
  failover, split+merge, failure repair, decommission, full restart (PR6).
- `cluster_growth.rs` — 3→5 online growth without restarting the original 3.
- `seed_join.rs` — combined-mode seed/join (happy/collision/rejoin).
- `decommission.rs` — full drain → remove flow + refusal shapes.
- `control_membership_admin.rs` — control-plane membership-change admin API
  (ADR 0037 PR3): grow a control voter end to end (quiet non-voter →
  `POST /admin/control/member/add` → converges everywhere incl. a data-only
  node's `Remote` mirror); add collision refusals (existing voter is
  idempotent, existing member/`ALLOC_ID_BASE` are refused); remove's full
  refusal/warning matrix (idempotent unknown-node no-op, non-leader voter
  removes cleanly, leader self-removal arms a transfer and refuses rather
  than silently completing, down-to-1-voter warns, down-to-0 is refused);
  both mutating actions refuse on a follower (not relayable).
- `cp_plane.rs` — CP round-trip (write one node, read another) + write latency.
- `cp_cross_process.rs` — cross-process forwarding to the leader's node.
- `cp_reconfigure.rs` — failure detection, group-follows-replica-set, auto-repair.
- `cp_rebalance.rs` / `cp_rebalance_gc.rs` — healthy rebalance + removed-replica GC
  (release, erase-bound, split-then-release).
- `drop_table_gc.rs` — drop-table `Reclaim` (incl. the relay bimodal case).
- `tablet_merge.rs` — end-to-end split → merge → read through the survivor.
- `batch_write.rs` — `cp_batch_write` / `PutBatch` forwarding.
- `durable_restart.rs` — write survives restart on LSM, lost on `--ephemeral`.
- `self_heal.rs` — concurrent-load smoke test (no deadlock).
- `dynamo_wire.rs` / `dynamo_extended.rs` / `dynamo_documents.rs` /
  `dynamo_indexes.rs` / `dynamo_schema.rs` — the DynamoDB edge (wire round-trip,
  conditional writes, document paths, GSI/LSI, replicated+restart-surviving
  schema/index).
- `cql_wire.rs` / `cql_clustering.rs` / `cql_durable_schema.rs` — the CQL edge
  (typed round-trip, compound keys, durable replicated schema).
- `admin_endpoint.rs` — admin views + gated actions + data writes + bulk seed.
- `dashboard_endpoint.rs` — SPA serve + CORS + deep links + peers + role gating.
- `metrics_endpoint.rs` — `GET /metrics` (leader-only counters per node).
- `otel_tracing.rs` — OTLP span export (decodes the protobuf payload).
- `schema_ddl_relay.rs` — schema DDL relay through a follower-connected node.
- `frame_cap.rs` — client-protocol frame-size cap.
- `support/mod.rs` — shared bring-up helpers (`#![allow(dead_code)]`;
  `restart_same_addrs`, `bring_up_split`, port-TOCTOU retries).
