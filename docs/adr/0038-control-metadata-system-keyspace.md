# ADR 0038 — Control-plane metadata backed by a per-node system-keyspace storage engine

- **Status:** Accepted — implemented across three PRs. PR1 (key encoding +
  reserved namespace), PR2 (shadow-mode engine mirror, dual-write, zero
  behavior change), **PR3 (this document): the cutover** — `Metadata` is
  `StateMachine::DRIVER_APPLIED = true`; `RaftCore` no longer applies
  `MetaCommand`s in-core; a new async apply task owns the only mutable
  `Metadata`, derives per-command system-keyspace writes, and publishes an
  `engine_applied`-gated cache every reader now reads. PR4 (`animusd`
  deployment-shape wiring polish + admin/dashboard storage surface) and PR5
  (incremental `WatchMetadata` deltas) are follow-ups, out of scope here.
- **Date:** 2026-08-10
- **Amends:** ADR 0009 (in-house Raft over `Env`), ADR 0013 (replicated schema
  catalog), ADR 0028 (shared per-node storage), ADR 0031 (tablet-host
  reconciler + `metadata_watch`), ADR 0035 (control-plane separate deployment).

## Context

Since ADR 0009, the control plane's `Metadata` (membership, the tablet map,
placement policies, the schema catalog, the node address book, and a few
monotonic id allocators) has been the in-house Raft `RaftCore`'s **in-core**
state machine: every committed command is applied synchronously, in-process,
against one `Metadata` struct held inside the core, and durability is a
`serde_json`-serialized snapshot of that *whole struct* embedded in the WAL
(`WalRecord::Snapshot { metadata, .. }`) and shipped wholesale — chunked, but
still O(state) to *build* — to a lagging follower via `InstallSnapshot`.

This was the right starting shape (ADR 0009's whole point was a tiny,
sync, deterministic core `SimEnv` could drive byte-for-byte), and it stayed
correct as the cluster grew. But it does not *scale* the way the per-tablet CP
data plane (ADR 0016/0017) already learned to: every single metadata mutation
— one member's heartbeat-driven status flip, one tablet's epoch-CAS bump, one
schema DDL — reprocesses and, at every compaction, re-serializes a blob sized
to the **entire cluster's** metadata, not to the size of what actually
changed. A cluster with thousands of tablets and a few hundred members turns
routine background churn (failure detection, auto-split, rebalancing) into an
increasingly expensive fixed tax on every node's WAL-compaction path, and a
freshly-joined or long-partitioned control voter's catch-up cost scales with
total cluster size, not with how far behind it fell.

ADR 0016/0017 already solved the identical shape of problem for the data
plane: a `DRIVER_APPLIED` state machine whose sync `RaftCore` only agrees
*order and durability*, handing committed-and-durable commands to a separate
async driver that applies them to a real `StorageEngine` — durability and
snapshot cost then scale with the *entities touched*, not the whole state.
This ADR is that same, already-proven mechanism, retargeted at `Metadata`.

### Options considered

| | **A: engine-backed control state machine (chosen)** | B: bootstrap "system tablet" | C: paginate/delta-compress `WatchMetadata` only |
|---|---|---|---|
| New consensus group? | No — same control `RaftCore`, new state-machine backing | Yes, conceptually (a tablet the control quorum hosts specially) | No |
| Bootstrap circularity | None — the system keyspace is the control node's own local storage | Real: something must still bootstrap *that* tablet's own placement before any tablet map exists | N/A |
| Fixes snapshot/`InstallSnapshot` O(cluster) reship | Yes — reuses the already-generic `DRIVER_APPLIED` lazy/chunked image path | Yes, eventually | No — only shrinks the wire payload |
| Fixes WAL-compaction O(cluster) reserialize | Yes | Yes | No |
| Effort for a pre-alpha codebase | Bounded — reuses `animus-cp-data`'s proven shapes | A second major consensus-integration project on par with ADR 0016/0017 | Cheapest, doesn't solve the stated problem |

Option B is the correct **eventual** end state if control-plane scale itself
ever becomes the bottleneck (tens of thousands of tablets on one quorum's own
Raft log/WAL) — but nothing in front of us requires it: control-plane
scalability today is bounded by *entity count* (tablets + members + schema
entries), which Option A already makes O(touched entities) per change, not
O(cluster). Revisit Option B only if profiling ever shows the control Raft
log/WAL itself — not the data plane — is the throughput ceiling. Option C is
rejected outright: it treats the symptom (wire payload size), not the cause
(every change still reprocesses/re-persists a whole-cluster-sized blob on the
durability path).

## Decision

**`Metadata::apply`'s decision logic does not change at all** — it stays pure,
synchronous, and computed against one authoritative in-memory struct. What
changes is *who* holds that struct and *when* it becomes durable.

1. `impl StateMachine<MetaCommand> for Metadata` sets `DRIVER_APPLIED = true`
   (PR3; PR1/PR2 left it `false`). `RaftCore` stops calling `Metadata::apply`
   itself; it buffers each committed-and-durable command as an effect
   (`drain_apply`) — the identical generic mechanism `animus-cp-data`'s
   `KvCommand`/`KvState` already uses, since `RaftCore<C, S>` was built generic
   over its state machine from ADR 0016 onward. No change to `raft.rs` was
   needed beyond flipping the flag and deleting the now-meaningless
   `RaftCore<MetaCommand, Metadata>::{metadata, members, placement_view}`
   inherent methods (they read `self.metadata`, which a `DRIVER_APPLIED` core
   never touches — mirroring `KvState`'s unit-placeholder shape, except
   `Metadata` itself doubles as the harmless, never-mutated generic `S`
   parameter, so no new placeholder type was needed).

2. `animus-control::node.rs` splits `RaftNode`'s single driver loop into a
   **consensus loop** (`drive` — recovers from the WAL, then only persists,
   steps the core, and ships messages; **no engine I/O**) and a separate async
   **apply task** (`meta_apply_loop`/`meta_apply_and_compact`, spawned by
   `drive` right after WAL recovery) that:
   - owns the *only* mutable `Metadata` (a private `shadow`, never shared with
     the core);
   - drains `RaftCore::drain_apply()`, applies each command via the real,
     unchanged `Metadata::apply` (through `mirror.rs`'s
     `apply_and_derive_mirror`, promoted from PR2's shadow-mirror role to the
     apply path's actual core), and derives the bounded set of
     [`syskv`](../../crates/animus-control/src/syskv.rs) key/value writes that
     command implies — one tablet, one member, one schema entry, never the
     whole map;
   - merge-batches those writes into a `StorageEngine` (a combined node's
     already-open shared CP-data engine, globally namespaced under
     `syskv::RESERVED_NAMESPACE`; a control-only node's own small dedicated
     engine — PR2's wiring, reused unchanged);
   - publishes the refreshed `Metadata` into an `Arc<Mutex<Metadata>>` cache
     gated by an `engine_applied: Arc<AtomicU64>` watermark (mirrors
     `animus-cp-data::RaftKvNode::engine_applied_index` exactly) and bumps
     `MetadataWatch` only *after* that publish — so a watcher never observes
     a change before it is both durable and visible in the cache.
3. Every existing synchronous reader — `RaftNode::metadata()`/`members()`/
   `placement_view()`, `reconcile_loop`, `detect_loop`, the ~30 `ctx.control`
   call sites in `animusd`, the dashboard — is **unchanged in signature**: it
   now reads the apply task's published cache instead of the core's own
   field. This is what keeps the blast radius bounded to `animus-control` plus
   a handful of `animusd` wiring call sites — no async rewrite ripples through
   `animusd`'s request-handling code.
4. **Snapshotting reuses the existing `DRIVER_APPLIED` lazy-image machinery
   verbatim.** The core raises `take_snapshot_needed()` only when its
   replication path actually needs to ship an `InstallSnapshot` chunk and has
   no image; the apply task services it by scanning the engine's system
   keyspace (`syskv_image`/`install_syskv_image`, the control-plane analogue of
   `animus-cp-data`'s `engine_image`/`install_engine_image`) instead of
   re-serializing `Metadata`. `InstallSnapshot`'s existing chunked,
   O(chunk)-not-O(state) shipping (ADR 0009/0017) is untouched.
5. **Crash recovery**: the apply task's prologue reads the engine's own
   `_applied_index` watermark key (**not** `core.last_applied()`, which after
   a WAL recovery only reflects the last *compacted* base and can understate
   what the engine already durably holds — compaction runs on a threshold,
   the engine watermark advances every apply pass) and rebuilds its `shadow`
   via `mirror::rebuild_metadata_from_engine`. It then replays the core's
   drained tail, **skipping any command whose index is already covered by that
   watermark** — a robust, index-based filter rather than relying on every
   individual `MetaCommand` happening to be idempotent under re-delivery.
   This is the mechanism that makes "rebuild from the engine, replay only the
   tail" true regardless of exactly where a crash landed relative to the last
   compaction.
6. **Bootstrap circularity is structurally avoided** (unchanged from the
   design PR2 already wired): this engine is the control node's own local
   storage, opened directly by the assembly layer, never a member of
   `Metadata.tablets` — nothing about the tablet map, hosting, or the
   per-node reconciler touches it. A control-only node
   (`BoundControlNode::start_control_with`) now **unconditionally** opens a
   dedicated engine (previously optional, shadow-mode-only, in PR2) — there is
   no more "no engine" control-plane deployment shape, since the engine is now
   the durable home of `Metadata` itself, not an optional mirror.

### Migration (breaking, by design)

Pre-alpha, no back-compat promise, matching ADR 0028's precedent ("full
replace, not a dual-mode shim"). **A control-plane WAL/snapshot written by a
pre-cutover binary cannot be read by a post-cutover one**: the WAL's
`Snapshot` record still carries a `metadata: S` field for wire-format
compatibility across every `DRIVER_APPLIED`/in-core state machine shape, but
for `Metadata` it is now always the meaningless `Metadata::default()` (the
real state lives in the engine) — a fresh cluster bring-up is required. This
is the same class of breaking change ADR 0028 made for tablet split, made
consciously here.

## Consequences

- **Durability/compaction cost is O(touched entities), not O(cluster)** — the
  actual scalability fix this ADR exists for.
- **Control-only nodes now provision a real, durable dedicated engine
  unconditionally** — a new failure mode (disk full/corrupt on a node that
  previously had none in this role). Crash-fault sim coverage for this lives
  in `animus-control/tests/{wal_compaction,apply_engine}.rs`;
  `animusd`-level restart coverage (both combined and control-only shapes)
  is the follow-up PR4's job, per the plan.
- **One extra async hop between "committed and durable" and "visible in
  `metadata()`"** — bounded by the apply task's idle-poll cadence
  (`APPLY_IDLE_POLL`, 5ms, identical to `animus-cp-data`'s), the same
  trade-off the data plane already made and is already proven live over real
  sockets/threads (`prod_liveness.rs`).
- **PR2's shadow-mode mirror machinery is retired**, not kept alongside the
  real path: `RaftCore::mirror_capture`/`mirror_log`/`enable_mirror_capture`/
  `drain_mirror_log` and `RaftNode::start_with_mirror` are removed — the
  generic `pending_apply`/`drain_apply` `DRIVER_APPLIED` machinery already did
  the identical job, so carrying both forward would have been two write
  paths for the same fact.
- **Non-goals (v1)**: no change to the tablet map's *authoritative* location
  (still `Metadata.tablets`, still control-plane-owned); no incremental
  `WatchMetadata` deltas (sketched as a candidate PR5, not committed here —
  the wire contract is unchanged, still a full `Metadata` clone per reply).

## See also

- `crates/animus-control/CLAUDE.md` — `node.rs`/`raft.rs`/`mirror.rs`/`syskv.rs`
  mechanics.
- `crates/animus-cp-data/CLAUDE.md` — the proven consensus-loop/apply-task
  split and `DRIVER_APPLIED` lazy-snapshot shape this ADR ports.
- `docs/engineering-lessons.md` — the election-storm bug class this split
  must not reintroduce, and the watermark-seeding lesson this PR added.
