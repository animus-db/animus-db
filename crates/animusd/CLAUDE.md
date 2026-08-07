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
  `peer_sync_loop`, `register_cp_addr`'s own commit confirmation, `cp_put`/
  `cp_get`'s `has_table_tablet` gate, and `/admin/status`. `propose_schema`
  (the shared "propose locally if leader, else relay to a *known* leader"
  primitive) also gained a last-resort fallback — broadcast to every other
  `client_route` address when there is **no** locally-known leader at all
  (true forever for a non-participating growth node, since it never receives
  a heartbeat/AppendEntries telling it who leads) — without this, a growth
  node's own address self-registration could never reach the real cluster.
  **Known residual gap**: a *pre-growth* node's `client_route` is a static map
  built once at its own process start, so it cannot forward a client op to a
  tablet leader that has since moved onto a newly grown node — only a growth
  node's own `client_route` (built from the expanded config) is always
  complete. Route new client traffic through the grown nodes' own addresses
  until a replicated client-address map closes this (ADR 0030's documented
  follow-up). `tests/cluster_growth.rs`: 3→5 growth, no restart of the
  original 3, admin-add + promotion + rebalancing onto the new nodes +
  reads/writes throughout + a never-booted phantom staying `Down`.
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
    `ctx.raft.metadata_watch().changed(last_seen)` (ADR 0031 §trigger —
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
  — plus gated `POST` actions — `/admin/{tablet/split,storage/flush,storage/compact,
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
    **Shell: a sidebar, not a top tab row** (`dashboard.html`) — five views,
    `overview`/`placement`/`tablets`/`browser`/`storage` (`TABS` in
    `dashboard_core.js`), each with its own JS module
    (`dashboard_overview.js`/`dashboard_placement.js`/`dashboard_tablets.js`/
    `dashboard_browser.js`/`dashboard_storage.js`, loaded after `dashboard_core.js`
    in that order — plain `<script src>` tags sharing one global scope, so later
    files call earlier ones' functions freely). **Each view keeps a real URL**
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
- Two run modes: `--cluster N` (whole cluster in one process, dev convenience)
  and `--config FILE --node I` (one node per process — real deployment). Both
  share `Node::bind`/`start`; only address/peer assembly differs.
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
