# CLAUDE.md — animus-control

This file provides guidance to Claude Code (claude.ai/code) when working in this
crate.

## Purpose

The strongly-consistent control plane: an in-house Raft (ADR 0009, *not*
openraft, so `SimEnv` can drive it) replicating cluster metadata — membership
and the tablet map — with epoch compare-and-swap transactions. All consensus
logic is a synchronous, I/O-free state machine; a thin `Env` driver does the
I/O. The sync core is generic (ADR 0016) and is reused unchanged by the
per-tablet CP data plane (`animus-cp-data`).

## Entry points

- **`lib.rs`** — the public surface. Re-exports `SharedWal`, `RaftCore`,
  `RaftNode`, `Metadata`/`MetaCommand`, the schema types, `FailureDetector`,
  and `animus_placement::PlacementPolicy` (so a downstream assembler can
  `SetTabletPolicy` without a direct `animus-placement` dependency — the policy
  is part of this plane's public metadata surface).

- **`meta.rs`** — the `Metadata` state machine and its command enum.
  `Metadata::apply` is the deterministic state machine; `Metadata::reconcile`
  and `Metadata::rebalance` are the *pure* placement decisions (see Invariants).
  `Metadata` holds members, the tablet map, placement policies, the table-schema
  catalog, keyspaces, `node_addrs` (member id → full `NodeAddrs { raftkv,
  client, admin, role, control }`, ADR 0032 PR1) and the legacy
  `cp_member_addrs` (kept for WAL back-compat). `NodeAddrs.role: String` (ADR
  0035 residual follow-up, `#[serde(default = "combined")]` for WAL
  back-compat) is a member's own deployment role (`"control"`/`"data"`/
  `"combined"`) — a plain string, not an `animusd`-side enum, since this
  crate has no dependency on `animusd` and every other field here is already
  an opaque wire-format string this crate never interprets. A node only ever
  authoritatively knows its own role, so it is stamped once at
  self-registration time like the other three fields; `animusd`'s
  `/admin/peers` reads every *other* node's role straight off this field
  instead of fanning out to each node's own `/admin/config`.
  `NodeAddrs.control: Option<SocketAddr>` (ADR 0037 PR4, `#[serde(default)]`)
  is a genuinely-typed exception among these otherwise-opaque `String`
  fields — populated **only** for a control voter added at runtime via
  `RaftNode::change_membership` (`animusd`'s `admin_add_control_member`);
  `None` for every statically-configured voter, whose address comes from
  `ClusterConfig` at each node's own process start instead. Read by every
  control-role `animusd` node's own `control_peer_sync_loop` to keep that
  node's control env peer book current with runtime membership changes —
  see that crate's `CLAUDE.md` for the gap this closes.
  `PlacementView` is the narrow (members + tablets + policies, no
  schema) clone that `RaftCore::placement_view()` hands the driver loops so they
  evaluate off the core lock instead of cloning the whole `Metadata` every tick.
  `MetaCommand` variants (all applied in log order):
  - `NoOp` — the election no-op a fresh leader commits.
  - `UpsertMember` — insert/update a member (labels + status).
  - `CreateTablet` / `CasTabletReplicas` — create a tablet; CAS its replica set
    (applies only if the epoch matches, then bumps it).
  - `SplitTablet` (ADR 0028) — the *entire* split: epoch-CAS gated, narrows the
    source range and mints a new sibling over the same node-shared engine. No
    data-plane half, no possible orphan.
  - `MergeTablets` (ADR 0033) — split's dual: epoch-CAS gated on *both* sides,
    rejects a cross-table merge, widens `left` to absorb `right`, and records
    `right` in `merged_tablets`.
  - `SetTabletPolicy` — set/clear a tablet's placement policy.
  - `CreateTableSchema` / `DropTableSchema` / `ReplaceTableSchema` — register /
    drop / atomically replace a table's schema (ADR 0013; `ReplaceTableSchema`
    is the atomic `ALTER TABLE … ADD`, no drop-then-recreate window).
  - `DropTableTablets` (ADR 0024) — remove every tablet scoped to a table + its
    policies in one apply (the metadata half of drop-table GC; `NoOp` if none).
  - `CreateTableIndex` / `DropTableIndex` — create/drop a secondary-index
    *definition* (ADR 0013).
  - `SetTableMode` — set a table's serving mode.
  - `CreateKeyspace` / `DropKeyspace` — keyspace lifecycle.
  - `RegisterNodeAddrs` (ADR 0032 PR1) — idempotent register/overwrite of a
    member's full `NodeAddrs`; every node proposes this once at startup.
  - `RegisterCpAddr` — the predecessor of `RegisterNodeAddrs`, carrying an
    optional `tablet` association (ADR 0024 GC). **Kept for WAL back-compat
    only — no longer proposed by `animusd`'s startup path.**
  - `RemoveMember` (ADR 0032 PR3 decommission) — prune a member from `members`
    plus its address entries; gated at apply on the member being absent
    already (idempotent), `Leaving`/`Down` (never `Active`/`Joining`), and
    unreferenced by any tablet (`Metadata::tablets_referencing`, also the
    drain-complete predicate `/admin/member/drain-status` reports).
  - `AllocateNodeId` (ADR 0036) — atomically mint a fresh member id from the
    `ALLOC_ID_BASE`-disjoint monotonic allocator (`Metadata.next_alloc_id`)
    and register it `Down` with the given labels, no address yet. Idempotent
    on `nonce` (`Metadata.node_id_allocations: BTreeMap<String, NodeId>`, the
    idempotency ledger) — a proposer retry with the same nonce is a `NoOp`
    that returns the identical, already-minted id, never a second one. No
    epoch-CAS needed: uniqueness comes from the same monotonic-floor-plus-
    presence-check discipline `SplitTablet`'s allocator guard already uses
    for tablet ids. `Metadata::next_free_alloc_id` is this allocator's
    `next_free_tablet_id` analogue.

- **`raft.rs`** — `RaftCore<C, S>`: the synchronous, I/O-free Raft state
  machine, **generic over its command `C` and applied state-machine `S`**
  (defaults `MetaCommand` / `Metadata`, so existing references are unchanged).
  Time and randomness are parameters (`now`, `entropy`); it returns outbound
  messages and emits `WalRecord`s. The state machine is the `StateMachine<C>`
  trait (`apply` + `noop`), implemented by `Metadata` here and by a KV store in
  the data plane. Consensus (election / replication / commit / snapshot /
  `InstallSnapshot` / pre-vote / leadership transfer / durability) is identical
  for any `S`; only `apply` and the snapshot image type are `S`-specific. The
  `RaftNode` driver stays control-plane-specific (it bakes in reconcile +
  failure detection); the KV data plane gets its own driver.

- **`node.rs`** — `RaftNode<E>`: the `Env` driver wrapping the core. Runs
  `reconcile_loop` (the leader's automatic placement reconciler + rebalancer),
  `detect_loop` (the leader's failure detector, ADR 0012), and the
  `heartbeat_loop`/`send_heartbeat` helpers a member runs to heartbeat the
  control group. Records control-plane **metrics** (ADR 0015) via
  `record_outbound`/`record_transition`; `metrics()` exposes the handle and
  `start_with_metrics` lets a sim test supply the sink. Read-only state
  accessors (`role`/`term`/`leader`/`is_leader`/`commit_index`/`last_applied`/
  `durable_index`/`snapshot_index`/`log_len`/`last_log_index`/`config`) back the
  `animusd` admin interface (ADR 0020). **`change_membership`/
  `transfer_leadership`** (ADR 0037 PR1, `tests/control_membership.rs`) are
  thin wrappers over the identical-shaped `RaftCore` methods `animus-cp-data`
  already drives for a per-tablet group — the control plane's *own* Raft
  group can now grow/shrink/replace a voter one server at a time, recorded
  under their own `ControlReconfigureAccepted`/`Rejected` metric family
  (kept separate from cp-data's per-tablet `CpReconfigureAccepted`/
  `Rejected`). Unlike cp-data's `propose_and_wake`, there is no propose-side
  wake seam here — `RaftNode`'s plain `propose` has never had one either, so
  a control-plane proposal (including a membership change) is always
  serviced on the driver's next heartbeat tick; no seam was added to keep
  this PR a pure thin-wrapper addition. **PR3 adds the admin/CLI surface**
  (`animusd`'s `admin_add_control_member`/`admin_remove_control_member` +
  `POST /admin/control/member/{add,remove}` + `animus admin
  control-{add,remove,grow}` — see that crate's `CLAUDE.md`); this crate's
  own tests stay core-level only. **PR4 closes PR3's address-replication gap
  and the static-vs-live `control_ids` audit** — `NodeAddrs.control` (above)
  + `animusd`'s `control_peer_sync_loop`, and `admin_remove_member`'s
  control-voter refusal moving to `self.control.config()` (the live Raft
  config) instead of a static original-members list — see `animusd`'s
  `CLAUDE.md` for both. `metadata_watch() -> MetadataWatch` (ADR
  0031) is the executor-agnostic "applied index advanced" notification the
  per-node CP reconciler uses to react to a `Metadata` change without polling.

- **`schema.rs`** — the replicated **table-schema catalog** (ADR 0013), all
  plain data (no I/O/clock/RNG). `TableSchema` (partition key + ordered
  clustering keys + typed `ColumnDef`s + `indexes: Vec<IndexDef>`), `ColumnType`
  (union of CQL scalars + DynamoDB key families), `SchemaCatalog` (a
  `BTreeMap<TableName, TableSchema>` held in `Metadata`), and
  `IndexDef`/`IndexKind`/`IndexProjection` (the replicated GSI/LSI *shape*, not
  its entry data). `TableSchema::validate` is the pure malformed-schema check
  the state machine applies (unique index names; an LSI requires a sort
  attribute).

- **`persist.rs`** — `WalRecord`, `PersistedState` (durability/recovery; the
  write/compact/recover flow is diagrammed in `docs/wal.md`).
  `encode_snapshot_record_from_blob` encodes the WAL `Snapshot` line **reusing
  the core's cached serialized image** (`snapshot_blob`, via `serde_json`
  `RawValue`) so compaction serializes `Metadata` once, not twice —
  byte-identical to the plain encode, guarded by
  `snapshot_record_blob_reuse_round_trips`.

- **`detector.rs`** — `FailureDetector` (ADR 0012): a pure, unit-tested
  interval+timeout liveness detector (last-heartbeat instants + `now` +
  `timeout` decide alive/dead). No clock, no RNG.

- **`shared_wal.rs`** — `SharedWal` (ADR 0028 PR1): a multi-tenant WAL I/O
  coordinator that serializes concurrent tablet WAL writers into one file with
  coalesced `append`+`sync`. **Built and unit-tested but UNWIRED** — no
  `animusd`/`animus-cp-data` code constructs one; every tablet still writes its
  own WAL file. Wire-in-or-delete is an open decision (see ADR 0028).

- **`syskv.rs`** (ADR 0038 PR1) — the control plane's reserved **system
  keyspace** key encoding: pure functions, no I/O, **unwired in this PR** (no
  engine is threaded into `RaftNode`, no `StateMachine::DRIVER_APPLIED`
  change, no `node.rs` change — a later PR in the stack mirrors `Metadata`
  through it into a per-node `StorageEngine`). `RESERVED_NAMESPACE =
  "__animus_system"` is the top-level namespace no user table/keyspace may
  claim; `entity_key(EntityKind, id)` encodes `escape(RESERVED_NAMESPACE) ||
  escape(kind) || escape(id)` reusing `animus_tablet::escape` byte-for-byte
  (this crate already depends on `animus-tablet`, so no new dependency edge —
  unlike the wire adapters, which deliberately *duplicate* `escape` to stay
  dependency-light of this crate, there's no such constraint here). One
  `EntityKind` per `Metadata` collection (`Tablet`/`Member`/`Schema`/`Policy`/
  `NodeAddrs`/`Keyspace`/`Merged`), plus typed `tablet_key`/`member_key`/
  `schema_key`/`policy_key`/`node_addrs_key`/`keyspace_key`/`merged_key`
  helpers and a dedicated `applied_index_key()` watermark (a sibling of the
  entity-kind segment, not under one — mirrors `animus-cp-data`'s
  `engine_applied_index`). `decode_key` inverts `entity_key`/
  `applied_index_key` for a later PR's engine-scan path and this module's own
  round-trip tests. **`is_reserved_name` is the one piece wired in this PR**:
  called from `Metadata::apply`'s `CreateTableSchema`/`CreateKeyspace` arms
  (the state-machine-level, every-replica-agrees gate) and from both wire
  edges' `CreateTable`/`CREATE KEYSPACE`/`CREATE TABLE` paths (client-side,
  so a reserved-name collision surfaces as an immediate
  `ValidationException`/`ERR_INVALID` instead of an opaque commit-wait
  timeout) — same two-layer idiom the existing duplicate-table check already
  uses. Matching is a case-sensitive prefix test (exact match *or* merely
  prefixed, e.g. `__animus_system_backup`), since a later PR scopes this
  keyspace into a combined node's shared engine via a reserved
  `StorageScope` keyed on the exact namespace string, and a prefix match is
  the collision that scoping scheme cannot tell apart from a real system key.

## Key invariants

- **Config-in-log + current-term-commit gate (ADR 0017 C).** `LogEntry` may
  carry a `config: Option<voters>`; `RaftCore` keeps `peers`/`cluster_size` in
  sync with the latest log config (config rides snapshots + `InstallSnapshot`).
  `change_membership` appends a single-server config entry — one in flight, no
  leader self-removal, and **rejected until `commit_index >=
  first_term_index()`** (the index of the election no-op). This is the
  reconfiguration erratum guard; `first_term_index()` is also what the data
  plane's ReadIndex barrier gates on (Raft §6.4). The control plane itself never
  reconfigures, so its config stays `= initial_config`.

- **Election no-op is committed in `become_leader` itself.** After appending its
  no-op, `become_leader` advances commit, so a **single-node** group commits it
  immediately — which is what makes a restarted sole voter re-apply its
  recovered WAL tail instead of waiting for the next propose, and what stops any
  "current-term entry committed" gate from deadlocking a 1-of-1 group.

- **Commit advances only for current-term entries** via majority `matchIndex`
  (the Raft safety rule). Don't relax this.

- **Durable-before-visible, role-aware apply frontier (ADR 0009).** `apply`'s
  frontier is `min(commit_index, durable_index)` on the **leader** and
  `commit_index` on a **non-leader**. Only the leader's applied state is what a
  proposer acks on, so a command is leader-visible only after it is fsynced. A
  follower never acks a write (it only serves reads) and a committed entry
  already rests on a quorum of durable logs, so it applies on commit without
  waiting on its own fsync — gating there would only widen cross-node
  read-visibility lag. See "What's non-obvious" for the driver mechanics and
  hand-driven gotchas.

- **Epoch-CAS discipline on `Split`/`Merge`/`CasTabletReplicas`.** Every
  tablet-mutating command is a compare-and-swap on the tablet's epoch, evaluated
  identically on every replica, so accept/reject is consistent and racing
  proposers can't both commit. `MergeTablets` carries *two* expected epochs
  (it reads two tablets from one snapshot). Any new tablet-mutating command must
  adopt the same guard.

- **`SplitTablet`/`MergeTablets` also seed/bump `Tablet::version_floor`
  (cross-group LWW version-floor fix, confirmed real — full writeup in
  `docs/engineering-lessons.md`).** Every tablet a node hosts shares one
  physical `StorageEngine` (ADR 0026/0028), and `animus-cp-data` stamps a
  write's MVCC version from its **own** group's local Raft log index — which
  restarts low/independent for a fresh group, so a split sibling or a merge
  survivor could otherwise carry a version no higher than what a *different*
  group already stamped for the same key, and per-key LWW would silently drop
  the write. `SplitTablet` sets the new sibling's floor to
  `source.version_floor + 1` (the source's own floor is untouched — it never
  absorbs foreign data); `MergeTablets` bumps the surviving `left`'s floor to
  `max(left, right) + 1` (checked against **both** sides deliberately —
  `left`/`right` are chosen by key-range adjacency, not allocation order, so
  `right`'s id/floor is not always the smaller one). Both are pure functions
  of already-agreed `Metadata` state, computed once here so every data
  replica reads the identical value instead of deriving it locally.
  Regressions: `meta::tests::{split_tablet_seeds_the_new_siblings_version_
  floor_past_the_sources, merge_tablets_bumps_the_survivors_version_floor_
  past_both_sides}`.

- **`merged_tablets` is never pruned.** Tablet ids are never reused, so a
  recorded merge marker can never resurrect a wrong decision for a later id.
  It is the only sound way a per-node reconciler tells "this hosted tablet
  vanished because it was merged into a sibling" (data still served — never
  erase) from "vanished because its table was dropped" (erase) — range
  containment alone is unsound, since two tables' unsplit tablets share a
  byte-identical default `KeyRange::whole()`. See ADR 0033.

## What's non-obvious

- **The sync/driver split is deliberate.** All consensus logic is in the sync
  `RaftCore` (unit-testable, deterministic); the driver only does I/O. When
  changing protocol behavior, change the core and keep it I/O-free — don't reach
  for the env inside it. The driver races `env.recv()` against a timer via
  `futures::select`, drawing `entropy` every iteration for randomized election
  timeouts.

- **Two apply models (ADR 0017).** `Metadata` applies **in-core, synchronously**
  (`StateMachine::DRIVER_APPLIED = false`, the default). A data-plane KV store
  sets `DRIVER_APPLIED = true`: the core does *not* apply in-core — it buffers
  each committed-and-durable command as an effect for the async driver to apply
  to a real `StorageEngine` (drained via `drain_apply`, which only hands out
  fsynced commands, since engine I/O is async and the core is sync).

- **Pre-vote (ADR 0009).** An election timeout does **not** campaign directly —
  the node becomes a `PreCandidate` and runs a `PreVote`/`PreVoteResp` round
  *without bumping its term*; only a pre-vote majority triggers the real,
  term-incrementing `start_election`. Peers grant a pre-vote only with **no live
  leader** (lease = `leader_id.is_some() && now < election_deadline`, or `role ==
  Leader`), so a briefly-stalled node can't inflate the term and disrupt a
  healthy leader. Pre-vote messages **bypass** higher-term step-down — the sole
  exception is a *rejecting* `PreVoteResp` with a higher real term, which reverts
  the pre-candidate to a follower at that term. Tick semantics: a multi-node
  election now needs a `PreVoteResp` grant fed before the real
  `RequestVote`/`RequestVoteResp`; a single-node group still elects on one tick
  (self is a pre-vote majority). `set_election_timeout(base, now, entropy)` makes
  the default-150ms base configurable for a node doing real disk I/O.

- **Leadership transfer (`RaftCore::transfer_leadership`, ADR 0029).** The
  control plane never calls it — it's a per-tablet CP-data primitive living here
  because the sync core is shared. `change_membership` always rejects removing
  the current leader, so relocating a leader's own replica needs a Raft §3.10
  handoff: arm a transfer to a voter with `peer_match(target) >= commit_index()`
  (no config change in flight; records a one-election-timeout deadline), then
  **freeze** `propose`/`change_membership` (return `NotLeader` hinting the
  target) so the log stops growing, and send `TimeoutNow` only once the target
  **reaches `last_log_index()`** (re-sent every heartbeat until step-down). A
  target that never steps down by the deadline **aborts** (clears the arm,
  resumes proposals). Re-arming the same target is idempotent and does **not**
  push the deadline (else a perpetual retry starves the abort check). The selector
  and the arm gate must read the *same* threshold, and the return value ("did it
  arm") must never be discarded — see the engineering-lessons log (root
  `CLAUDE.md`) for the war story where they diverged.

- **Snapshot transfer is chunked and O(chunk), not O(state).** A follower behind
  the compacted prefix is caught up via a chunked `InstallSnapshot`: the leader
  ships offset-addressed chunks of `SNAPSHOT_CHUNK_BYTES` (one per round trip,
  resuming from the per-peer `snapshot_offset`); the follower reassembles into
  `incoming_snapshot` and installs atomically only on the final chunk. All in the
  sync core (deterministic). `snapshot_chunk_for` **slices `snapshot_blob` by
  reference — it does NOT re-serialize per chunk**; a naive per-chunk serialize
  on a multi-MB metadata pins the loop past the election timeout (a
  self-sustaining election storm, invisible to `SimEnv`'s virtual clock). Blob
  management differs by state-machine kind:
  - **In-core (`Metadata`):** the blob is kept **eagerly** — set in
    `snapshot_upto`, on *install* completion (retain received bytes), and in
    `recovered` — so the invariant `snapshot_index > 0 ⟹ blob.is_some()` holds
    and a chunk is never a 0-byte ship (regression:
    `install_snapshot.rs::caught_up_control_node_reships_non_empty`).
  - **`DRIVER_APPLIED` (data-plane KV):** the image is the *engine* bytes, built
    **lazily on demand** — the core raises `take_snapshot_needed`, the driver
    scans the engine and calls `snapshot_upto` then `set_snapshot_blob`, and the
    core **drops** the blob whenever it would go stale/idle, so no whole-tablet
    image is retained at rest (regression:
    `driver_applied_sm.rs::caught_up_node_reships_non_empty_snapshot`, driving
    both hops).
  - The compaction serialize reuses the cached blob for the WAL record too (via
    `encode_snapshot_record_from_blob`), so compaction serializes `Metadata`
    once. Moving the single remaining inline compaction serialize off the loop
    was **assessed and deferred** — a bounded, non-self-sustaining ~50-120ms
    stall, not worth coupling install→WAL-rewrite ordering onto a second task on
    the most safety-critical Raft. Liveness teeth:
    `install_snapshot.rs::large_snapshot_ships_in_o_chunk_time_not_o_state` +
    `tests/prod_liveness.rs`. Deferred: cross-leader resumption (an interrupted
    transfer restarts at offset 0) and chunk-stream flow control.

- **Durable-before-visible mechanics + hand-driven gotchas.** The driver
  advances the durable watermark via `mark_durable_through` in `flush_wal`,
  immediately after `env.sync(WAL)` (passing the drain-time `last_log_index`);
  `recovered()` sets it to the recovered `last_log_index`. The leader gate closed
  the acked-before-durable window that flaked `animusd`'s
  `create_table_survives_node_restart`. Gotchas: (1) a *leader* core driven by
  hand must simulate the fsync — drain, then
  `mark_durable_through(last_log_index())` — or its `metadata()` never reflects
  proposals (see `persistence.rs`); a hand-driven *follower* applies on commit
  with no fsync (see `follower_visibility.rs`). (2) A read on a follower right
  after a leader `CreateTable` must still wait for the definition to *replicate*
  there (`await_table_*` in the `animusd` tests) — a cross-node race independent
  of the local durable gate.

- **Automatic placement + rebalancing (ADR 0005, 0029).** Policies are
  replicated (`SetTabletPolicy` → `policies`). The decision is the pure
  `Metadata::reconcile` (repair: `animus_placement::replan` over `Active`
  members, emits a `CasTabletReplicas` only for policy-violating tablets) and its
  balance-driven complement `Metadata::rebalance` (`rebalance_step` picks a
  single balance-improving healthy-replica move, wrapped as a `CasTabletReplicas`
  at the current epoch — reusing the command, so no relay-allowlist change). The
  **leader** drives both in one `reconcile_loop`: repair first each tick, and
  rebalance only if repair proposed nothing *and* `tick % REBALANCE_EVERY_N_TICKS
  == 0`. That interval is pure churn control, not a safety invariant —
  correctness rests on the epoch-CAS and the data-plane catch-up gate. Keep the
  *timing* in the driver and the *decision* pure. A split child inherits the
  source's policy (else it is invisible to both repair and rebalance).

- **Automatic failure detection (ADR 0012).** Members heartbeat the control
  group (`heartbeat_loop` → `RaftMsg::Heartbeat`, a term-less message the driver
  **intercepts** in its `recv` arm and feeds to the pure `FailureDetector` — the
  core never sees it). The **leader** drives it: `detect_loop` proposes
  `UpsertMember{Active/Down}` for any tracked member whose liveness changed
  (`liveness_transitions`, idempotent — preserves labels, skips
  `Joining`/`Leaving`, and **only judges members that have heartbeated**). A
  committed `Down` cascades into re-placement. A freshly elected leader's
  detector is **cold**, so `detect_loop` applies a post-election grace
  (`LEADER_GRACE`, one `DETECT_TIMEOUT`, tracked via `Env`-time `leader_since`)
  during which it passes `allow_down = false`, so a new leader can't falsely mark
  live members `Down` before heartbeats repopulate the detector (recoveries are
  never suppressed). These loops are driven in production (`animusd`, proven over
  `ProdEnv`/TCP in `animusd/tests/self_heal.rs`). Detector state is per-node
  volatile; only transitions are replicated.

- **Replicated schema catalog (ADR 0013).** `Metadata.schemas` is mutated only by
  the `*TableSchema` commands, so it is Raft-replicated and recovered from the
  WAL/snapshot like all metadata (no `persist.rs`/`InstallSnapshot` change — the
  snapshot is a full `Metadata` image). Secondary-index *definitions* ride the
  same path (`TableSchema.indexes`, mutated by `Create/DropTableIndex`) — only
  the index *shape* is replicated; the index *entry data* stays at the wire edge,
  rebuilt from observed writes.

- **Observability metrics (ADR 0015).** All from `Env`-supplied or core-derived
  inputs (deterministic): election counters + `is_leader` gauge
  (`record_transition`); `append_entries_sent`/`_rejected` + `snapshot_installs`
  (off emitted messages, `record_outbound`); `failure_detector_down`/`_up` (the
  `Active`↔`Down` edges `detect_loop` proposes). `RaftNode::start` records into
  `env.metrics()`; use `start_with_metrics` to thread a handle a sim test can
  read (`SimEnv::metrics()` is the no-op default, so no `animus-sim` change).

- **`MetadataWatch` (ADR 0031).** The `AtomicWaker`-based wake-a-parked-task
  pattern (like `animus-cp-data`'s `ProposeSignal`), adapted to notify an
  *external* caller rather than the driver's own loop. Two points to remember if
  you touch or copy it: (1) it carries a **monotonic watermark** (`AtomicU64`,
  the observed `last_applied()`), not a one-shot consumed flag — `changed()`
  re-checks `current > last_seen` fresh every poll, so there is no
  wake-before-park race (a change that already happened resolves on the first
  poll). (2) It is bumped from the **driver loop** (`drive`), not the proposer,
  via `fetch_max` at exactly the points `last_applied` (gated by the same
  role-aware frontier `metadata()` uses) can have moved — so defensive calls on
  no-op iterations are free. It is **single-waiter** (one intended consumer: the
  per-node reconciler). Don't add a propose-side wake here the way
  `animus-cp-data` did — a metadata-watch caller only ever waits to learn when
  `metadata()` *could* reflect a change, and that visibility is bound by the
  driver's flush cadence anyway.

## Tests

`cargo test -p animus-control` (use `run_for`, never `run()` — perpetual
heartbeats). The 24 binaries:

- **`control_raft.rs`** — election/replication/leader-kill + multi-seed
  convergence.
- **`persistence.rs`** — durability/recovery; the hand-driven leader `persist`
  helper (drain + `mark_durable_through`).
- **`follower_visibility.rs`** — the role-aware apply gate (ADR 0009): a
  hand-driven follower applies a committed entry with `durable_index == 0`, a
  leader stays gated on its own proposal, a follower→leader transition keeps the
  applied entry and gates new proposals, and both `SimEnv` followers reflect the
  leader's committed command.
- **`membership_commit_gate.rs`** — core-level `change_membership`: the
  current-term-commit gate (`commit_index >= first_term_index`, leader-only) plus
  the single-server rules, driven at message granularity.
- **`generic_state_machine.rs`** — proves `RaftCore<C, S>` genericity (ADR 0016):
  drives the same core with a toy KV state machine (non-control-plane command +
  image) through propose → durable-apply → snapshot → WAL recovery.
- **`driver_applied_sm.rs`** — the `DRIVER_APPLIED` core mechanism (ADR 0017
  Stage A.1): effects are exactly the committed-durable commands in commit order,
  the in-core path is bypassed, durability still gates hand-out; plus the
  two-hop non-empty-reship regression.
- **`tablet_split_merge.rs`** — split and merge through Raft: racing splits at
  the same epoch (only the first applies), and merge rejecting a stale epoch
  racing a replica change / tablets from different tables.
- **`wal_compaction.rs`** — snapshot truncation.
- **`install_snapshot.rs`** — a partitioned follower catching up via
  `InstallSnapshot`, a far-behind follower via a *multi-chunk* one, non-empty
  reship, and the wall-clock-timed O(chunk)-not-O(state) liveness test.
- **`restart.rs`** — process restart-and-rejoin (via `Simulator::stop`).
- **`placement_reconcile.rs`** — caller-driven placement reconcile through Raft
  under a replica death + follower crash (drives `animus-placement`).
- **`placement_auto_reconcile.rs`** — leader-driven automatic reconcile from a
  replicated policy (no test-side `replan`/CAS).
- **`placement_rebalance.rs`** — leader-driven automatic load rebalancing (ADR
  0029, no test-side placement math): grow the cluster, only advance virtual
  time; spreads existing tablets to max−min ≤ 1 then goes quiet, repair defers to
  rebalance, residency + strict spread hold at every intermediate state, and it
  converges after killing the leader mid-flight; seed-swept.
- **`leadership_transfer.rs`** — core-level transfer (ADR 0029): arms only a
  caught-up current voter, retries `TimeoutNow` every heartbeat once the target
  reaches `last_log_index`, freezes `propose`/`change_membership` while armed,
  aborts and resumes if the target never catches up, an idempotent re-arm
  doesn't extend the deadline, a stale transfer doesn't survive a fresh election,
  `TimeoutNow` bypasses pre-vote, a departing peer keeps receiving the removal
  entry until it acks.
- **`failure_detection.rs`** — heartbeat-based failure detection end to end (ADR
  0012): a member crashes, the leader auto-commits `Down`, placement reconciles
  off it, the member restarts and returns to `Active`; plus detector +
  grace-gate unit tests.
- **`schema_catalog.rs`** — the replicated table-schema catalog end to end (ADR
  0013): propose, reject a duplicate + malformed on the state machine, kill the
  leader and assert survival + agreement, drop and see it replicate; plus
  `schema.rs` unit tests.
- **`schema_indexes.rs`** — replicated secondary-index definitions end to end
  (ADR 0013): create a GSI a second node sees, reject an index on a phantom
  table + a malformed LSI, restart and see the definition survive, drop
  cluster-wide; seed-reproducible.
- **`metrics.rs`** — control-plane metrics moving under known events (ADR 0015):
  a forced election bumps the election counters + leadership gauge, a crashed
  heartbeating member bumps `failure_detector_down` and its recovery `_up`; plus
  a same-seed byte-identical-snapshot reproducibility check.
- **`pre_vote.rs`** — pre-vote (ADR 0009): core-level (a live-leader lease
  rejects and never changes the term, an expired lease grants, a timeout makes a
  `PreCandidate` without bumping the term) + end-to-end (an isolated follower's
  pre-vote rounds don't move the stable leader's term and it rejoins on heal with
  no election; a genuine crash still elects at a higher term).
- **`metadata_watch.rs`** — the applied-index watch (ADR 0031): a watcher parked
  on `changed` wakes with the new index once a proposal commits and applies,
  stays parked across steady-state traffic with nothing proposed, and resolves
  on its first poll when the advance already happened (the wake-before-park case,
  safe by construction).
- **`prod_liveness.rs`** — a real-thread `ProdEnv` smoke test guarding the
  liveness properties `SimEnv`'s virtual clock can't see.
- **`node_id_allocation.rs`** — `MetaCommand::AllocateNodeId` (ADR 0036): the
  monotonic allocator mints distinct ids under concurrent proposals, a
  replayed nonce is idempotent, and the counter survives a restart.
- **`control_membership.rs`** — `RaftNode::change_membership`/
  `transfer_leadership` (ADR 0037 PR1): add/remove a voter and catch it up,
  reject a multi-server delta / leader self-removal / a second change while
  one is in flight, transfer-then-remove the leader, crash-mid-change
  converges either way (single-seed + a 200-seed sweep), byte-reproducible
  from a seed; plus two PR5 additions — a freshly-added voter's process
  restarting before it's caught up recovers from whatever WAL/snapshot it had
  and resumes, and removing a live voter while a different one is already
  dead is accepted (no core-level survivor-liveness guard) and demonstrably
  strands the group (a stranded 2-voter config with one dead never commits
  anything again) — the risk ADR 0037's Consequences section documents as
  knowingly accepted.
- **`control_membership_prod.rs`** (ADR 0037 PR5) — the real-thread `ProdEnv`
  liveness counterpart to `control_membership.rs`: grows a real 3-node
  control group to 5 (two sequential single-server `change_membership`
  calls, exactly as `animus admin control-grow`'s client-side loop
  sequences them) under real sockets/time/threads, asserting both prompt
  catch-up and that leadership stays bounded and settles to one stable
  leader afterward — no election storm from real-thread scheduling around a
  runtime membership change.
