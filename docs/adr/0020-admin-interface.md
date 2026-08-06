# ADR 0020 — Admin / debug interface on a dedicated port

- **Status:** Accepted
- **Date:** 2026-08-04

## Context

A running `animusd` node today exposes almost no operational surface. The only
introspection a human can reach is:

- `animus status <client-addr>` — a single `ClientRequest::Status` over the
  plain-TCP client API returning the node's cached `Metadata` (members + tablet
  map). It is the *whole* topology query.
- `GET /metrics` on the DynamoDB HTTP listener (ADR 0015) — the aggregated
  text-format counter snapshot.

Everything else that an operator or a developer debugging a failing cluster
would want is locked inside the process:

- **Config** — the `ClusterConfig` / `RoleAddrs` (which ports a node bound, its
  control/raftkv ids, peer addresses) is a static `gen-config` file; a running
  node never serves it back.
- **Raft state** — for both the control plane (`RaftNode<ProdEnv>`) and each
  hosted CP group (`RaftKvNode<ProdEnv, _>`): role, term, leader, commit /
  applied / durable indices, log length, snapshot index, voter set, per-peer
  replication progress, failure-detector beliefs. All private to `RaftCore`,
  reachable only through a handful of accessors that the wire edges don't use.
- **Storage internals** — the on-disk `LsmEngine` already carries rich
  `#[doc(hidden)]` introspection used by tests (`sstable_count`,
  `level_table_counts`, `wal_segments`, `block_read_count`, the `SsTableMeta`
  per table: key range, version range, level, file size, bloom presence,
  `test_disk_versions_of(key)`), and the WAL `GroupCommit` exposes
  `live_segments`, `durable_seq`, `segment_file`, `rotation_count`. None of it
  is reachable from outside the process.
- **Operator actions** — only `ClientRequest::SplitTablet` exists, and the
  `animus` CLI doesn't even surface it. Force flush/compaction, CP membership
  reconfigure, and node drain have no entry point.

Three standing constraints shape any answer:

1. **Determinism (ADR 0003).** The admin surface is a *production-only I/O edge*,
   exactly like `/metrics` and the Dynamo/CQL listeners — it lives in `animusd`
   over `ProdEnv` and never runs under `SimEnv`. But the *accessors* it calls in
   the `<E: Env>` crates (`animus-control`, `animus-cp-data`, `animus-storage`)
   must stay determinism-clean: pure reads, no wall clock, no `HashMap`, snapshot
   into `BTreeMap`/`Vec`. The seam observes; it must never change the path it
   measures (ADR 0015).
2. **Single-consumer inbox.** A node runs two internal `ProdEnv` roles on
   distinct ids (control `i`, raftkv `300+i`); the admin surface reads their
   handles, it does **not** add a third protocol on a shared id.
3. **Multi-role aggregation (ADR 0015).** A node's observable state is spread
   across role handles (control `RaftNode`, the CP `CpGroup`s in
   `ClusterEdgeState.raftkv`, each role's metrics sink). The admin layer must
   aggregate at request time, live, the way `ClientCtx::metrics_text` already
   sums the two metrics sinks — never read one and call it the node.

We want a *complete* admin interface: list config and status, inspect both
Raft layers, debug the WAL and LSM, read metrics as structured data, and drive
operator actions (split, flush, compact, reconfigure, drain).

## Decision

**We will add a dedicated admin HTTP/JSON listener — a sixth per-node listener —
serving a read-only introspection surface plus a clearly separated set of
operator actions, all aggregated live at request time from the node's existing
role handles.**

### Why a dedicated port (not the Dynamo listener, not `ClientRequest`)

- **Isolation & policy.** Admin is the surface you firewall, bind to localhost
  or a management interface, and later put auth in front of. Co-tenanting it on
  the public Dynamo port (as `/metrics` does today) couples a debug/mutate
  surface to a data port. A dedicated `RoleAddrs.admin` lets a deployment expose
  ports 1–6 to clients and keep 7 internal.
- **JSON, not a typed enum.** Nested dumps (manifest, per-level SSTable lists,
  decoded WAL records) are natural as JSON and awkward as `ClientResponse`
  variants. HTTP/JSON is curl/browser/`jq`-friendly and matches the hand-rolled
  HTTP precedent already in `dynamo.rs`.
- **One surface, both verbs.** Read-only is `GET`; actions are `POST`. No split
  across two transports.

The cost is a known, mechanical ripple (see Consequences): the node packs roles
into consecutive ports, so adding a role bumps the stride 5→6 and touches every
`RoleAddrs` literal, `peer_book`, `Node::bind` arity, the `[ProdEnv; N]`
shutdown array, and the id convention — the compiler walks you through the
sites (a pattern the crate guide already documents for the raftkv role).

### Surface

A hand-rolled HTTP/1.1 server (reuse `dynamo.rs`'s request parser and
`text/plain`/JSON writers, extracted to a shared `http` helper module) bound on
`RoleAddrs.admin`, dispatching on `(method, path)`. Responses are JSON
(`application/json`); errors are a JSON `{"error": "..."}` with a 4xx/5xx code.

#### Phase 1 — read-only (`GET`)

| Route | Returns | Sourced from |
|-------|---------|--------------|
| `GET /admin/config` | This node's `ClusterConfig` view: node index, control id, raftkv id, the five (now six) `RoleAddrs`, the static peer list, and `cp_member_addrs` from `Metadata` | `ClientCtx` holds config/ids; `Metadata::cp_member_addrs` |
| `GET /admin/status` | The full replicated `Metadata` as JSON: members + `NodeStatus`, tablet map (range, epoch, replicas), schema catalog, table indexes, keyspaces | `ctx.raft.metadata()` |
| `GET /admin/raft` | Control-plane Raft: `role`, `term`, `leader`, `commit_index`, `last_applied`, `durable_index`, `snapshot_index`, `log_len`, `config` (voter set), and `believes_alive` per member | `RaftNode` accessors (exist) + new `log_len`/`config`/`last_log` pass-throughs |
| `GET /admin/raftkv` | Per hosted CP group: tablet id, `is_leader`, `term`, `leader`, indices, `config` (voters), applies-in-flight | iterate `ClusterEdgeState.raftkv`; new `RaftKvNode` accessors |
| `GET /admin/storage/lsm?tablet=N` | LSM debug for a tablet's engine: per-level table counts, every `SsTableMeta` (seq, level, key range, version range, file size, entry count, bloom), memtable byte size, flush/compaction/block-read counters, `levels_non_overlapping` | promote `LsmEngine` `#[doc(hidden)]` accessors to a real `Introspect` API |
| `GET /admin/storage/wal?tablet=N` | WAL debug: live segment numbers with byte size + max seq, `durable_seq`, `rotation_count`, `batch_sync_count` | `GroupCommit` accessors (exist) + `env.size` per segment |
| `GET /admin/storage/wal/segment?tablet=N&seg=S` | Decoded `WalRecord`s of one segment (paged) | `env.read(wal.segment_file(seg))` → parse newline-JSON |
| `GET /admin/storage/key?tablet=N&key=K` | Every on-disk `(version, is_tombstone)` for a key, plus the live value | `LsmEngine::test_disk_versions_of` (promoted) + `get` |
| `GET /admin/metrics` | The aggregated `MetricSnapshot` as JSON (counters + leader gauge) — the structured sibling of `/metrics` | `metrics_text`'s snapshot logic, emitted as JSON |
| `GET /admin/health` | Liveness/readiness: is the control node up, does it know a leader, is the local CP group past its first apply | derived |

Tablet-scoped storage routes target the `CpGroup`'s engine for that tablet; a
node that does not host the tablet returns 404 with a hint (the route is
node-local debug, not cluster-wide — you scrape each node, like `/metrics`).
The `MemoryEngine` backend (`--ephemeral`) has no WAL/SSTables, so storage
routes return a `{"backend":"memory"}` stub rather than erroring.

#### Phase 2 — operator actions (`POST`), each gated and idempotent

| Route | Body | Effect | Safety |
|-------|------|--------|--------|
| `POST /admin/tablet/split` | `{tablet, split_key}` | The existing `ClientRequest::SplitTablet` path | Already wired; idempotent on a re-split (records in control plane, proposes on leader) |
| `POST /admin/storage/flush` | `{tablet}` | Force a memtable flush on the tablet's engine | Idempotent (no-op if memtable empty); new `LsmEngine::flush_now` |
| `POST /admin/storage/compact` | `{tablet}` | Force a compaction pass | Idempotent; bounded work; new `LsmEngine::compact_now` |
| `POST /admin/raftkv/reconfigure` | `{tablet, voters}` | Drive `RaftKvNode::reconfigure_step` toward a target voter set (one single-server step per call, per the `change_membership` contract) | Leader-only; rejected if it would drop below quorum; converges over calls |
| `POST /admin/drain` | `{node}` | Mark a node `Leaving` and let the placement reconciler move its replicas off | Control-leader-only; reversible |

Actions return the action's observable result (e.g. the new tablet id for a
split, the post-step voter set for a reconfigure) so a caller can confirm
without a follow-up read. They route to the right node/leader using the same
resolution the data path uses (`cp_route` / the control-leader handle set),
forwarding when this node isn't the authority — the operator hits any node.

**Audit warning (2026-08-06 — open bugs behind two actions).** (1) The "Safety"
column above overstates `flush`/`compact`: `flush_now`/`compact_now` run on the
admin connection's task, **concurrent** with the tablet's single Raft apply
loop, and `LsmEngine` has no flush-in-progress guard — `flush()` snapshots the
memtable, builds the SSTable with the lock released, then unconditionally
`clear()`s the memtable, so a write applied (and acked) during the build window
is erased from visibility, and a *later* flush can GC its WAL segment, making
the loss permanent; two overlapping flushes can also double-allocate an SSTable
seq. Until the engine serializes flush-vs-apply and flush-vs-flush, forcing a
flush/compact on a tablet **under live write load** is an acked-write-loss
hazard (the normal write path is unaffected — it is single-task). (2)
`POST /admin/tablet/split` (and the auto-split loop) allocates the child tablet
id as `max(live ids)+1` instead of `next_free_tablet_id()`, which can **reuse a
dropped tablet's id** — see the ADR 0024 known-violation note.

Phase 2 is explicitly *separated*: it lands after Phase 1 is proven, behind the
same port, and each action ships with its own safety check + a `ProdEnv`
integration test (these are real-thread liveness paths — see the crate guide's
multi-threaded-test rule).

### Consumption: `animus admin <subcommand>`

Extend the `animus` CLI with an `admin` subcommand group that GETs/POSTs these
routes and pretty-prints them (`admin status|config|raft|raftkv|lsm|wal|metrics|
health`, and `admin split|flush|compact|reconfigure|drain`). The CLI stays a
thin client; the node is the source of truth. `<addr>` is the node's admin
address (printed at startup alongside client/dynamo/cql).

### Accessors to add (grounded; all pure reads)

- **`animus-control`** — `RaftNode` pass-throughs for `log_len()`, `config()`,
  `last_log_index()`/`last_log_term()` (the core already has these private; add
  thin wrappers). `believes_alive` already exists.
- **`animus-cp-data`** — `RaftKvNode` accessors mirroring the control ones
  (`term`, `commit_index`, `last_applied`, `durable_index`, `snapshot_index`,
  `log_len`, `config` already partly there via `is_leader`/`leader`/`config`).
- **`animus-storage`** — a small `LsmIntrospect` surface promoting the existing
  `#[doc(hidden)]` test accessors to documented methods (`sstable_meta() ->
  Vec<SsTableMeta>`, `level_table_counts`, `wal_segments`, `wal_durable_seq`,
  `memtable_bytes`, counters, `disk_versions_of`), plus `flush_now` /
  `compact_now` for Phase 2. WAL-segment-record decoding reads files through the
  `Env::Disk` seam (`read`/`size`) — no new disk capability needed.
- **`animusd`** — `ClientCtx` already holds the control handle, the CP groups
  (via `edge.raftkv`), and the metrics sinks; the admin handlers are pure
  readers over these, aggregating at request time. A `RoleAddrs.admin` field
  (`#[serde(default)]` so older configs still load) + the stride bump.

## Consequences

**Easier:**

- A single place to answer "what is this cluster doing right now" — config,
  membership, tablet placement, both Raft layers, storage shape — without
  attaching a debugger or reading WAL files by hand.
- Debugging a stuck cluster: compare `durable_index` vs `commit_index` vs
  `last_applied` across nodes, see which node holds a CP leader, dump the WAL
  tail of a tablet, list its SSTables and levels — all over `curl`/`jq`.
- Operator actions get a real entry point (split is already there; flush,
  compact, reconfigure, drain become reachable) instead of "future work".
- Structured `/admin/metrics` complements the Prometheus-style `/metrics`.

**Harder / costs knowingly accepted:**

- **A sixth listener.** The port stride goes 5→6; every `RoleAddrs` literal
  (config gen + test sites), `peer_book`, `Node::bind` arity, the `[ProdEnv; N]`
  shutdown array, and the conventional id base get touched in one mechanical
  pass. `#[serde(default)]` on the new addr field keeps older configs loading.
- **Wider accessor surface.** Promoting test-only introspection to a documented
  API is a small compatibility commitment; keep it read-only and snapshot-shaped
  so it can't perturb the measured path.
- **Phase 2 is real-thread liveness code.** Force-flush/compact and reconfigure
  hold locks and drive group commit under the multi-threaded `ProdEnv`; each
  needs a timeout-guarded `#[tokio::test(flavor = "multi_thread")]` (the
  determinism suite can't catch a deadlock here — crate-guide rule).
- **No auth yet.** Phase 1/2 assume the port is bound to a trusted interface;
  authn/authz is deferred and flagged as the obvious follow-up before this is
  exposed beyond localhost. The dedicated-port choice is what makes that
  follow-up clean.
- **Node-local storage debug.** `/admin/storage/*` reports the queried node's
  engine for a tablet; cluster-wide views are assembled client-side by scraping
  each replica (same model as `/metrics`).

### Follow-up work

- Auth in front of the admin port before any non-localhost exposure.
- A `--admin-bind` override so the admin port can bind a different interface
  than the data ports.
- Cluster-wide aggregation (a `GET /admin/cluster/...` that fans out to peers)
  once per-node debug is proven.
- A small web dashboard served on the admin port (static, self-contained) is a
  natural later addition now that the JSON exists.
