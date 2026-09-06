# ADR 0020 — Admin / debug interface on a dedicated port

- **Status:** Accepted. **Amended by [ADR 0053](
  0053-dynamodb-only-drop-cql.md) (2026-08-22):** the CQL listener this ADR
  references below (and the `POST /admin/data/cql` proxy a later follow-up,
  ADR 0021, added on top of this admin surface) are both removed — v1
  serves DynamoDB only.
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
   exactly like `/metrics` and the Dynamo/CQL listeners (CQL removed by
   [ADR 0053](0053-dynamodb-only-drop-cql.md); Dynamo is the only wire
   listener as of that ADR) — it lives in `animusd`
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

> **Amended (2026-08-22, ADR 0053):** `/admin/status` no longer emits
> `keyspaces` — the field was CQL's and left `Metadata` with the adapter.
> The row above is the original design record; the live payload omits it.

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

**Audit warning (2026-08-06 — both since fixed: (1) in PR #26, (2) in PR #21;
retained for the record).** (1) The "Safety"
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
address (printed at startup alongside client/dynamo/cql — the `cql` listener
was removed by [ADR 0053](0053-dynamodb-only-drop-cql.md)).

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

## Amendment (2026-08-19) — a polled observer must not materialize

`GET /admin/raftkv`'s `key_count`/`byte_size` were the tablet's **exact**
count/total, read by materializing every hosted tablet's rows per request
(`CpGroup::local_pairs`). That was written as a browse-grade debug surface
("the materialize-then-count cost is acceptable"), and it stopped being
true the moment ADR 0021's Console started polling it: the dashboard fetches
this route from **every node** on its auto-refresh interval — 5s by default —
so an open Tablets tab costs O(dataset) per node every 5s, forever.

Measured on a live 5-node cluster splitting a 20,000-row table: polling
this route every 3s inflated the split's own build from **4.5s to 41.8s**
(~9x), and the whole seed→8-tablet cascade from 23.3s to 57.4s. The
observer was changing what it observed by roughly an order of magnitude —
and an operator watching a slow split was, by watching, making it slower.

**`key_count`/`byte_size` are now the cheap `approx_key_count`/
`approx_bytes` estimates by default; `?exact=1` selects the old
materializing path.** Three consequences, all deliberate:

- The estimates are exactly what `auto_split_loop` gates on, so the
  Console's over-threshold pills now agree with the trigger that will
  actually fire — previously the pill compared an exact count against a
  threshold evaluated on an estimate.
- `approx_key_count` is `None` on the memory backend (no cheap counter), so
  an `--ephemeral` dev cluster renders "—" in the Keys column until asked
  for `?exact=1`. Accepted: the durable backend is the one that matters
  here, and the exact answer is one query parameter away.
- `approx_bytes` is base-scoped (ADR 0034) where the exact sum covers every
  kind in the tablet's engine, so the two differ in meaning as well as
  precision. Documented on the field rather than papered over.

The general rule this instance argues for: **an introspection surface
whose cost scales with the data it describes needs an explicit answer to
"what happens when something polls this every few seconds?"** — because a
dashboard eventually will. Regression:
`tests/admin_endpoint.rs::admin_raftkv_default_does_not_materialize_the_dataset`,
which meters the LSM's own `storage_sstable_block_reads` rather than wall
clock (10 default polls over 2,000 flushed rows: **0 block reads**; 10
`?exact=1` polls: 550).

## Amendment (2026-09-04, issue #595) — `/admin/health` needs hysteresis, not the raw pre-vote belief

`health()`'s readiness signal (the Kubernetes readiness probe, ADR 0060) used
to be exactly `RaftCore::leader().is_some()` — `leader_id`, the control
plane's own consensus-internal belief. ADR 0009's pre-vote mechanism
(`start_pre_vote`) clears that belief the instant a follower's own election
timer lapses, before any pre-vote round is even answered: correct for
consensus (a stale belief must never be trusted for granting a vote), but it
gives `/admin/health` a false-negative window on **every** transient
one-sided delay of one election timeout (150ms default) or more — a single
scheduling stall on a pod, a GC pause, one dropped heartbeat — even while the
real cluster leader is fully healthy and heartbeating every other replica
the whole time. A liveness/readiness signal must not inherit a safety
mechanism's hair-trigger semantics.

**Fix**: `animus-control::RaftCore` gains an observational
`last_leader_contact: Option<(NodeId, Nanos)>`, set only at a genuine leader
contact (a valid `AppendEntries`/`InstallSnapshot` from the current term's
leader, or this node itself becoming leader) and cleared only on a real
higher-term step-down (never by `start_pre_vote`/`start_election`'s own
local-suspicion clear of `leader_id`) — see that field's own doc comment in
`raft.rs` for the full reasoning. `RaftCore::leader_within(now, max_age)` /
`RaftNode::leader_within(max_age)` / `ControlHandle::leader_within(max_age)`
read it with a caller-supplied grace window; `leader()` itself is untouched
and still backs every consensus-facing consumer.

`admin::health()` now gates `200`/`503` on `leader_within(HEALTH_LEADER_
GRACE_ELECTION_TIMEOUTS × election_timeout())` (3 election timeouts, ~450ms
at the default base) instead of the raw `leader()`. The JSON body keeps
`control_leader_known` reporting the raw flag unchanged, and adds
`control_leader_recent` for the hysteresis-gated one — `ok` and the HTTP
status now track `control_leader_recent`. A genuinely leaderless node still
degrades to `503`, just after the grace rather than after the very first
missed heartbeat — this is hysteresis, not a permanent trust, and the
regression proves both halves (survives a one-sided delay inside the grace;
still degrades once the delay genuinely outlasts it).

**Deliberately unchanged**: `ClientCtx::propose_schema`'s leader-hop
decision. Its `leader()` read uses the full `CLIENT_TIMEOUT` (10s) for that
hop, while its broadcast fallback is capped per-hop at `FORWARD_HOP_TIMEOUT`
(2s, issue #585's fix) — handing it a possibly-stale `leader_within` belief
instead would risk spending the hop budget on a node that is not actually
reachable as leader, which is worse under `dynamo.rs`'s 5s
`SCHEMA_COMMIT_TIMEOUT` than falling through to the broadcast promptly. Only
the operational readiness probe gets the hysteresis; every leader-selection
consumer keeps reading the raw, immediately-corrected belief.

Regression: `animus-control/tests/leader_within_hysteresis.rs` (a one-sided
partition inside the grace, then held past it, across 12 seeds). See
`docs/engineering-lessons.md`'s matching entry for the general lesson.

## As-built (2026-09-05, roadmap U-05) — `POST /admin/control/transfer`

The roadmap's U-05 audit found no standalone leadership-transfer route:
`GET /admin/raft`'s `transfer_target` (ADR 0037's escape-valve field) is
read-only, and the only place this codebase ever armed a transfer was
buried inside `admin_remove_control_member`'s self-removal arm (ADR 0037) —
an operator who wanted to move leadership *without* also removing a voter
had no route to ask for it. Added `POST /admin/control/transfer {"to":
<node id>}`, beside the existing `control/member/add`/`control/member/remove`
pair — like those two, documented in ADR 0037 rather than duplicated in
this file's own design-time route table above, since all three are ADR
0037-shaped control-plane-membership actions.

Same discipline as every other `control/member/*` action: **local-control-
leader-only, not relayed** (`RaftCore::transfer_leadership` is a call on
this node's own in-process `RaftNode` handle, not a `MetaCommand`
proposal — a follower refuses with the ordinary not-the-leader error). The
handler (`ClientCtx::admin_transfer_control_leadership`) is idempotent if
`to` already names the current leader, refuses outright if `to` is not a
current voter, and otherwise arms the transfer and polls (bounded by the
same `CONTROL_TRANSFER_POLL_TIMEOUT` the self-removal arm already used) for
this node to step down — success means this node confirmed stepping down,
not a confirmed new leader elsewhere (the caller retries against the new
leader exactly as every other post-transfer caller in this codebase already
does). Wired through the full `AdminHost` stack
(`crates/animus-node/src/host.rs`'s trait method, `admin.rs`'s dispatch
match arm and `FakeHost` stub, `crates/animusd/src/admin.rs`'s handler) and
`animus admin control-transfer <admin-addr> <node-id>`. Regression:
`tests/admin_endpoint.rs::
admin_control_transfer_moves_leadership_to_the_named_node`/
`admin_control_transfer_on_a_follower_is_refused`.

## As-built (2026-09-06, roadmap U-05) — dashboard action buttons over existing routes

The Tablets tab's detail card gained four gated buttons — Split, Flush,
Compact, Reconfigure — over the four routes that already existed
(`POST /admin/tablet/split`, `POST /admin/storage/flush`, `POST
/admin/storage/compact`, `POST /admin/raftkv/reconfigure`, the Phase 2
route table above). No new admin route was added for this: the JS layer
(`crates/animusd/src/dashboard_tablets.js`) calls these exactly as the
`animus admin`/`animus-cli` forms already do, through the dashboard's one
mutation idiom (`postJSON`, a `window.confirm` guard per action).

**Worth stating plainly, since this is the second and third PR of a
three-PR series adding buttons of this shape (Node/control-member families
follow)**: a dashboard button is not a second gate on top of this ADR's
"no auth — bind to a trusted interface" posture, and was never meant to be
one. The **only** thing standing between a dashboard viewer and any of
these four actions is (a) `window.confirm`'s dialog, purely a
misclick guard, and (b) the fact that the Tablets tab — and this card in
particular — is only ever shown on a control-role node's console
(`ROLE_TABS`, ADR 0035 PR7 client-side gating). Neither is an
authorization boundary: **anyone who can reach the admin port at all can
already call every one of these routes directly**, with `curl` or the
`animus admin` CLI, button or no button — exactly the trusted-network
posture this ADR's Decision section states from the start ("No auth for
v1 ... The admin port is meant to be bound to a private/management
network"). Adding a button here changes nothing about who can act; it only
changes how conveniently an operator who is already inside that trusted
network can act. See `crates/animusd/CLAUDE.md`'s dashboard section and
ADR 0021's own "Actions" note for the same statement from the dashboard's
side.

## As-built (2026-09-06, roadmap U-05, continued) — the Node tab's own action buttons

The fourth PR of this series added the NODE family the note above deferred:
three gated buttons on the Node tab's new `#nd-actions` card — Drain
(`POST /admin/drain`), Remove (`POST /admin/member/remove` — the ADR 0032
drain→remove decommission flow's own second half), and Add member
(`POST /admin/member/add`, ADR 0030 online growth). No new admin route;
same `postJSON` + `window.confirm` idiom as the tablet family above, and
the identical statement applies unchanged: these buttons are not a second
gate on this ADR's "no auth — bind to a trusted interface" posture, only a
convenience for an operator already inside the trusted network — anyone
who can reach the admin port can already call `/admin/drain`/`/admin/
member/{add,remove}` directly. See `crates/animusd/CLAUDE.md`'s
`dashboard_node.js` entry for the full mechanism (including why this is
three buttons over three routes, not four — `/admin/member/remove` already
is both "finish decommissioning" and "remove a member," the same route
either way) and ADR 0021's matching amendment for the dashboard's own
side. The control-members panel's own add/remove/transfer buttons
(`/admin/control/member/{add,remove}`) remain the final PR of this series.

## As-built (2026-09-06, roadmap U-05, closing) — the control-members panel's own action buttons

The fifth and last PR of this series closes the one bullet the two notes
above deferred: the control-plane members panel itself
(`#nd-control-members`, landed read-only by an earlier PR in this series)
gained per-row **Transfer leadership here** and **Remove** buttons, plus a
sibling `#nd-control-actions` card for **Add**, over three routes that all
already existed — `POST /admin/control/transfer {to}` (this ADR's own
2026-09-05 entry above), `POST /admin/control/member/remove {node}`, and
`POST /admin/control/member/add {node?, addr}` (ADR 0037 PR3). No new
admin route. Same `postJSON` + `window.confirm` idiom, and the identical
statement applies unchanged a third time: these buttons are not a second
gate on this ADR's "no auth — bind to a trusted interface" posture, only a
convenience for an operator already inside the trusted network — anyone
who can reach the admin port can already call any of the three routes
directly. Transfer is hidden on a member's own row while it already leads
(nothing to transfer to itself); Remove surfaces the server's own
`warning`/refusal verbatim, including the quorum-loss guard ADR 0037 §2
describes, never auto-retrying with `force`. Add's body needs the new
voter's own internal control-Raft address, not just its id (`animus admin
control-add`'s own CLI form resolves that by fetching the new node's
`/admin/config` first — see `crates/animusd/CLAUDE.md`'s `dashboard_node.js`
entry for a bug found while confirming this and fixed 2026-09-06: that CLI
helper used to read a `cfg["control"]` field `/admin/config` hadn't served
under that name since the ADR 0040 PR1 identity merge, so the 3-argument
operator-supplied-id form of `control-add` always failed — now reads
`addrs.internal` instead), so this panel asks the operator for the
address directly rather than attempting a cross-origin fetch of another
node's admin port. There is **no separate `grow` route** to wire — `animus
admin control-grow` is a purely client-side loop of the same
`control/member/add` call (`animus-cli`'s own `run_control_grow`), not a
distinct server endpoint, so "Add," called as many times as needed, is
already the whole feature. **This closes docs/roadmap.md's whole U-05
section** — every bullet across all five PRs has now landed. See
`crates/animusd/CLAUDE.md`'s `dashboard_node.js` entry for the full
mechanism and `tests/dashboard_endpoint.rs::
dashboard_u05_control_member_actions` for the regression.

## As-built (2026-09-06, roadmap U-07) — `GET /admin/backup-store`

The first of U-07's four new observability routes, and its own named
template: `GET /admin/backup-store` (ADR 0059 §1/§3) surfaces this node's
own configured backup store (kind + a credential-safe `location`), a
bounded live scan of its local backup-object count/bytes, the on-demand
backup janitor's own live phase/counters (a new
`animus_node::backup_janitor::JanitorProgress`, published by
`backup_janitor_loop` through a new narrow capability trait,
`animus_node::host::BackupJanitorProgressHost`, into an
`Arc<std::sync::Mutex<JanitorProgress>>` on `ClientCtx` — the same "plain
`std::sync::Mutex`, short lock/mutate/drop, never held across an `.await`"
shape `metrics_history` already uses), and whether this node currently
believes it is the control-plane leader — the janitor only ever runs
there, so a follower's own `janitor` field simply stays `Idle` forever, an
honest answer rather than a gap.

**Object counts are a live, bounded scan, not a maintained counter**
(`BackupStoreHandle` tracks no running count/byte total today): `count` is
one cheap `SegmentStore::list` call over the whole `backup/` namespace;
`bytes` sums each object's length via a new `BackupStoreHandle::get_local`
(local-only — every id already came from this node's own `list_local`, so
there is no reason to fall through to the cluster-wide `get_any`), capped
at 200 objects with `"truncated": true` reported past the cap. This is the
identical "a polled observer must not materialize" judgment call this
file's own 2026-08-19 amendment made for `/admin/raftkv` — acceptable here
specifically because this route has no cheap alternative and nothing polls
it on a tight interval by default.

Wired through the full conventional stack: a match arm in
`crates/animus-node/src/admin.rs`'s dispatch table, the `AdminHost::
backup_store_view` method (`crates/animus-node/src/host.rs`) and its
`FakeHost` stub/dispatch test, a handler in `crates/animusd/src/admin.rs`,
`animus admin backup-store <admin-addr>` (`animus-cli`), and a new
read-only "Backup store" card on the Backups tab
(`dashboard_backups.js`/`dashboard.html`, fed from the tab's existing
`loadAll()` poll — no new timer). A small new helper,
`redact_store_location` (`crates/animusd/src/lib.rs`), strips a URI's
userinfo and query string before a store location ever reaches this JSON —
written now, once, so a future S3-backed store variant (and the next three
U-07 routes) inherit the same safety rather than each re-deriving their
own redaction; neither store variant this node can be configured with
today actually carries a credential, so this is defense-in-depth, not a
fix for an existing leak.

Regression: `tests/admin_endpoint.rs::
admin_backup_store_reports_reclaim_progress_and_leader_state` (a real
3-node cluster with an `fs:` backup store — create a table, take an
on-demand backup, delete it, and poll converged-or-timeout until the
control-plane leader's own route shows the janitor's object count back at
baseline with `objects_reclaimed > 0`; a follower reports `leader: false`
and stays `Idle`), `tests/dashboard_endpoint.rs::
dashboard_u07_backup_store_card`, and `animus_node::backup_janitor::tests`'
own progress-reporting assertions.

## As-built (2026-09-06, roadmap U-07) — `GET /admin/ttl`

The second of U-07's four observability routes, copying `/admin/backup-
store`'s own template (above) with one deliberate difference: the TTL
reaper (ADR 0051) runs on **every** node, self-gated per tablet
(`TtlScanHost::led_tablets`), never only on the control leader — so every
node's own `reaper` field is a genuinely live answer, never a stand-in for
"not the leader" the way a follower's `janitor` field on `/admin/backup-
store` is.

`GET /admin/ttl` returns `{reaper: TtlReaperProgress, tables: [{name,
attribute, enabled}...], leader_tablets: N}`. `reaper` is a new
`animus_node::ttl_reaper::TtlReaperProgress` — `phase`
(`idle`/`scanning`/`deleting`, mirroring the loop's real per-tablet control
flow), `last_tick_at_ms`, a `cursor` (the reaper's own driver-local resume
position: table + tablet id + a hex-encoded, `TTL_REAPER_CURSOR_KEY_CAP`
(24-byte)-truncated resume key — **never** the raw key bytes),
`deleted_last_tick`/`deleted_total`, `expired_seen_total`,
`tables_with_ttl`, and `last_error` — published at each phase transition
through a new capability trait mirroring `BackupJanitorProgressHost`
exactly, `animus_node::host::TtlReaperProgressHost`, into
`ClientCtx::ttl_reaper_progress: Arc<std::sync::Mutex<TtlReaperProgress>>`
(provisioned on every node shape, `client_ctx_host.rs`). `tables` is every
TTL-enabled table in the replicated catalog (a disabled table carries no
`TtlSpec` at all and so never appears — `enabled` is therefore always
`true` here, kept as a field for shape symmetry and future extension, not
because a `false` case exists today). `leader_tablets` counts this node's
own currently-hosted tablets that are (a) this node's own Raft leader and
(b) belong to a TTL-enabled table — the tablets this node's reaper is
*actually* reaping right now, a **per-tablet** fact distinct from the
control-plane leadership `/admin/backup-store`'s own `leader` field
reports.

Wired through the identical conventional stack `/admin/backup-store`
established: a match arm in `crates/animus-node/src/admin.rs`'s dispatch
table, the `AdminHost::ttl_view` method (`crates/animus-node/src/host.rs`)
and its `FakeHost` stub/dispatch test, a handler in
`crates/animusd/src/admin.rs`, and `animus admin ttl-reaper <admin-addr>`
(`animus-cli`) — named `ttl-reaper`, not the bare `ttl` its route would
suggest, since roadmap U-08(ii) plans a `ttl` *dynamo-proxy* wrapper
(`UpdateTimeToLive`/`DescribeTimeToLive` via `/admin/data/dynamo`) in the
same `admin_request` subcommand namespace and this GET arm must not claim
that name first. Renders on the **Storage** tab (`dashboard_storage.js`'s
new "TTL reaper" card, `#ttl-card`/`#ttl-body`) rather than Backups — this
route's per-node semantics (every node answers about its own reaper) don't
fit the Backups tab's existing "one shared answer" fetch shape, so
`dashboard_core.js`'s existing **per-node** `loadAll()` fan-out
(`STATE.nodes[*].ttl`) feeds it instead of a second single-fetch-against-
SEED call.

Regression: `tests/admin_endpoint.rs::
admin_ttl_reports_reaper_progress_and_ttl_tables` (a real 3-node cluster —
`UpdateTimeToLive` on a table, `PutItem` an already-expired item, and poll
converged-or-timeout until some node's own route shows `deleted_total >=
1`; every node's own `tables` list carries the TTL-enabled table since the
catalog replicates everywhere; a node leading none of that table's
tablets still answers its own honest counters rather than erroring),
`tests/dashboard_endpoint.rs::dashboard_u07_ttl_reaper_card`, and
`animus_node`'s `tests/ttl_reaper_sim.rs` (extended with progress
assertions on the existing `SimEnv`-driven reap/no-op scenarios).

### Follow-up work

- Auth in front of the admin port before any non-localhost exposure.
- A `--admin-bind` override so the admin port can bind a different interface
  than the data ports.
- Cluster-wide aggregation (a `GET /admin/cluster/...` that fans out to peers)
  once per-node debug is proven.
- A small web dashboard served on the admin port (static, self-contained) is a
  natural later addition now that the JSON exists.
