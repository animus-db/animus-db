# ADR 0038 — Control-plane metadata backed by a per-node system-keyspace storage engine

- **Status:** Accepted — implemented across six PRs. PR1 (key encoding +
  reserved namespace), PR2 (shadow-mode engine mirror, dual-write, zero
  behavior change), **PR3: the cutover** — `Metadata` is
  `StateMachine::DRIVER_APPLIED = true`; `RaftCore` no longer applies
  `MetaCommand`s in-core; a new async apply task owns the only mutable
  `Metadata`, derives per-command system-keyspace writes, and publishes an
  `engine_applied`-gated cache every reader now reads. PR4 (`animusd`
  deployment-shape wiring polish + admin/dashboard storage surface). **PR5:
  "Phase 2" — incremental `WatchMetadata` deltas**, closing the one item
  PR3's Consequences section left as future work. **PR6 (this amendment): a
  read-only admin browse surface** (`GET /admin/system-table` + a dashboard
  section) over the system keyspace this ADR built (see below).
- **Date:** 2026-08-10 (PR3); amended 2026-08-10 (PR5); amended 2026-08-10 (PR6)
- **Amends:** ADR 0009 (in-house Raft over `Env`), ADR 0013 (replicated schema
  catalog), ADR 0028 (shared per-node storage), ADR 0031 (tablet-host
  reconciler + `metadata_watch`), ADR 0035 (control-plane separate deployment).
- **See also:** ADR 0039 works out this ADR's own Option B ("bootstrap system
  tablet," rejected below) in detail — design-only, not scheduled — and finds
  that its scaling payoff is gated on ADR 0018 (cross-tablet transactions),
  not on Option B alone.

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
  (still `Metadata.tablets`, still control-plane-owned). Incremental
  `WatchMetadata` deltas were sketched here as a candidate PR5 and are now
  shipped — see "Phase 2" below.

## Phase 2 (PR5): incremental `WatchMetadata` deltas

PR3 made the apply task derive each command's exact system-keyspace
writes/deletes (`mirror::apply_and_derive_mirror`'s `Vec<KeyWrite>`) as a
necessary step on the way to the engine — but `WatchMetadata`'s reply kept
shipping a **full `Metadata` clone** every time, unconditionally. This PR
makes the reply incremental, reusing that same per-command derivation instead
of duplicating it.

**Design** (unchanged from what this ADR's PR3 revision sketched, now
committed in full):

1. **Pre-diffed key/value deltas, never replayed commands.** A mirror installs
   received `KeyWrite`s verbatim into its cached `Metadata` via
   [`mirror::apply_key_write`](../../crates/animus-control/src/mirror.rs) — the
   same decode logic [`mirror::rebuild_metadata_from_engine`] already used for
   its bulk (`Put`-only) rebuild path, now shared by both. No control-plane
   business logic (no `MetaCommand` replay) runs on a mirror — the identical
   reasoning that already ruled out shipping raw commands over the wire.
2. **A bounded per-node in-memory delta ring**
   ([`delta_ring::DeltaRing`](../../crates/animus-control/src/delta_ring.rs)),
   fed by the apply task in the same pass that publishes `cache`/bumps
   `engine_applied`: one entry per drained command, keyed by its Raft log
   index, holding that command's derived `KeyWrite`s (possibly empty — a
   `NoOp`/rejected command still gets an entry, which is what keeps the ring's
   index space contiguous with no unexplained gaps). Bounded by **both** max
   entries and max total bytes (defaults 1024 / 4 MiB, `DeltaRing::
   with_bounds`/`RaftNode::start_with_ring_bounds` for a different bound) —
   oldest evicted first. Strictly per-node, best-effort, no cross-node
   coordination — nothing here is replicated or agreed on. **Reset to empty**
   whenever `cache` is rebuilt from a jump the ring itself didn't witness (a
   received `InstallSnapshot`, or the apply task's own startup/restart
   rebuild) — a mirror whose `last_seen` predates that jump then correctly
   falls back to a full fetch rather than the ring silently under-reporting.
3. **Serve path**: `WatchMetadata { last_seen }` answers with the ring's
   flattened `(last_seen, current]` writes
   (`RaftNode::watch_delta_since` → `animusd`'s
   `ClientResponse::MetadataDelta`) when the ring contiguously covers that
   range; otherwise falls back to the original full `ClientResponse::Status`
   reply — the same log-tail-vs-`InstallSnapshot` fallback shape this plane
   already has. The trivial "nothing changed since `last_seen`" case (a
   long-poll that resolved by timing out) is always a zero-length delta, ring
   or no ring — cheaper than a full clone even then. A plain
   `ClientRequest::Status` request is untouched: always the full reply,
   unconditionally — only `WatchMetadata` gained the incremental option.
4. **Both mirror consumers adopt deltas from one shared implementation site**:
   `animusd`'s `RemoteControlClient::observe_delta` (called from
   `remote_metadata_watch_loop`) is driven identically whether the caller is a
   genuine ADR 0035 PR4 data-only node (`ControlHandle::Remote`) or an ADR
   0030 growth node's standalone `RemoteControlClient::with_mirror` — there
   was only ever one call site to update, confirmed by reading the source
   before implementing (per this repo's "grep before reimplementing"
   practice). `observe_delta` only applies a delta when the mirror's current
   watermark exactly matches the delta's own `last_seen` basis (not merely
   `<=`) — a concurrent full `observe()` (e.g. a `metadata_fresh()` call
   racing the background watch loop, since `RemoteControlClient` is shared
   and both drive the same `Arc`-backed state) advancing the mirror in the
   meantime makes a delta's *sequential* application unsafe to apply blindly;
   detected and dropped rather than mis-applied, self-healing on the watch
   loop's next iteration.

No back-compat machinery was added (explicit call, matching this ADR's own
precedent): the `WatchMetadata`/`Status` wire types changed directly — a new
`ClientResponse::MetadataDelta` variant, no `V2` enum, no `#[serde(default)]`
compatibility shim — since this is pre-alpha with no live deployments to
interoperate with.

**Consequences of Phase 2**: a `WatchMetadata` round trip against a small,
recently-active control plane is now O(commands since `last_seen`), not
O(cluster) — the same fix PR3 already made for durability/compaction,
extended to this last full-`Metadata`-clone wire path. The ring is a pure
memory/latency trade-off with no safety implication: any coverage gap
(eviction, a snapshot install, a restart) degrades to exactly the pre-PR5
behavior (a full fetch), never to incorrect or stale data — see the
differential tests (`animus-control/tests/watch_deltas.rs`,
`animusd/tests/watch_metadata.rs`) for the byte-identical-to-a-full-fetch
proof this relies on.

## PR6: a read-only admin browse surface

Every prior PR here made the system keyspace faster to write and cheaper to
ship over the wire, but there was still no way for an operator to actually
*look at* what it holds — the only observability into it was the aggregate
`/admin/storage/control` LSM/WAL stats (PR4). This PR adds exactly that,
read-only, additive, no change to the write/apply path above:

- **`GET /admin/system-table?kind=&after=&limit=`** — browse this node's own
  reserved system keyspace directly, row by row. `{"available": false}` on a
  data-only node (no local `ctx.control_storage` engine at all, ADR 0035),
  the same honest-absence shape `/admin/storage/control` uses. Response:
  `available`, `applied_index` (a **dedicated point read** of the
  `_applied_index` watermark key — never derived from the scan window itself,
  which may be empty/kind-filtered/paginated), `kind_filter`, `count`,
  `limit`, `truncated`, `next_after`, `items: [{kind, id, version, value}]`.
- **Every [`syskv::EntityKind`] is browsable, including the internal/legacy
  ones** (`Counter`, `CpMemberAddr`, `NodeIdAlloc`) — a deliberate
  full-transparency call: hiding "boring" bookkeeping kinds would make "what
  does this node actually store" a lie by omission for the one surface whose
  whole purpose is answering that question. The dashboard labels them
  `(internal)`/`(legacy)` rather than hiding them.
- **Load-bearing implementation choice, worth restating even though
  [`syskv::reserved_scan_bounds`] and the endpoint's own doc comment already
  say it**: the whole reserved namespace is read via **one bounded
  `StorageEngine::scan` over `[start, end)`**, filtered by `kind` **in
  memory** afterward — never `StorageEngine::entries()`. On a combined node
  this engine is shared with the CP data plane (ADR 0028): `entries()` would
  scan **every user table's data on the node**, turning an O(system-keyspace)
  read into O(all-user-data-on-node). A future "simplification" that swaps
  the bounded scan for `entries()` would silently reintroduce exactly the
  scaling problem this whole ADR exists to fix, just moved from the write
  path to a read path — see `docs/engineering-lessons.md`.
- **`syskv.rs` additions**: [`EntityKind::as_str`]/[`EntityKind::from_segment`]
  are now `pub` (the endpoint parses/renders a `?kind=` filter through them
  directly, rather than re-deriving the segment table a third time);
  [`prefix_successor`] (a general byte-lexicographic successor helper,
  unit-tested including the trailing-`0xFF` edge case) and
  [`reserved_scan_bounds`] (the `[start, end)` pair covering the entire
  namespace, built from it) are the new pure primitives the scan bound
  above uses.
- **Cursor design**: `after`/`next_after` are the base64url
  (`animus_dynamo::wire`, **not** `key_display` — a system key isn't a
  data-plane key) of the raw engine key of the last item on a page. The next
  page's lower bound is that key's bytes with one `0x00` byte appended —
  exact and gap-free because `syskv`'s keys are provably prefix-free (the
  existing `no_two_distinct_entity_keys_prefix_one_another` test): no other
  key can start with `after`'s bytes, so no key can fall strictly between it
  and its `0x00`-appended successor.
- **Value decode mirrors `mirror::apply_put` exactly**: `Tablet`/`Member`/
  `Schema`/`Policy`/`NodeAddrs`/`CpMemberAddr` are `serde_json` passthrough;
  `Counter`/`NodeIdAlloc` are a raw big-endian `u64` rendered as a JSON
  number; `Keyspace`/`Merged` are presence-only (always `null`). A numeric
  kind's `id` (`Tablet`/`Member`/`Policy`/`NodeAddrs`/`Merged`/
  `CpMemberAddr`) renders as a decimal **string**, not a JSON number (a
  `u64` can exceed what JSON/JS represents exactly); every other kind's `id`
  is its UTF-8 name verbatim.
- **Dashboard**: the Storage tab's existing "Control system keyspace" card
  (PR4) grew a browse section directly inside it — the same control-role
  node selector, a kind filter (every kind, internal/legacy ones labeled),
  an "as of index N" watermark label, a table with expand-to-full-JSON per
  row, and a forward-only "Next page" pager. No new tab, no `ROLE_TABS`
  change — this rides the same role gating the existing card already has.

**Tests**: `animus-control`'s `syskv.rs` unit tests cover `prefix_successor`/
`reserved_scan_bounds` directly; `animusd/tests/system_table.rs` seeds every
`EntityKind` through the client protocol (a plain `Put` auto-provisions a
tablet, `ProposeSchema` reaches every other mirrored `MetaCommand`, a real
split+merge produces a `Merged` marker, `AllocateNodeId` produces the
`NodeIdAlloc` entry) and asserts every kind's exact value shape, the `kind`
filter, and gapless duplicate-free pagination against a differential oracle
(one unlimited scan); `control_only.rs`/`data_only.rs` cover the
available-`true`-with-rows / available-`false` shapes on genuine
control-only and data-only processes; `dashboard_endpoint.rs` asserts the
served assets carry the new markup/JS.

## See also

- `crates/animus-control/CLAUDE.md` — `node.rs`/`raft.rs`/`mirror.rs`/`syskv.rs`/
  `delta_ring.rs` mechanics.
- `crates/animusd/CLAUDE.md` — the `WatchMetadata` wire contract and
  `RemoteControlClient` mechanics, and (PR6) the `GET /admin/system-table`
  browse surface.
- `crates/animus-cp-data/CLAUDE.md` — the proven consensus-loop/apply-task
  split and `DRIVER_APPLIED` lazy-snapshot shape this ADR ports.
- `docs/engineering-lessons.md` — the election-storm bug class this split
  must not reintroduce, the watermark-seeding lesson PR3 added, the
  concurrent-mirror-update race PR5 added, and (PR6) the
  whole-namespace-scan-vs-`entries()` warning.
