# CLAUDE.md — animus-control

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The strongly-consistent control plane: an in-house Raft (ADR 0009, *not*
openraft) replicating cluster metadata — membership and the tablet map — with
epoch compare-and-swap transactions.

## Entry points

- `meta.rs` — `Metadata` (members + tablet map + placement policies + the
  table-schema catalog + keyspaces + **`cp_member_addrs`**: CP group member id →
  `raftkv` address, Phase 2 address distribution) and `MetaCommand` (`UpsertMember`,
  `CreateTablet`, `CasTabletReplicas`, **`SplitTablet`** (ADR 0028: the *entire*
  split operation — epoch-CAS gated like `CasTabletReplicas`; narrows the source
  tablet's range and mints a new sibling tablet over the same node-shared storage
  engine, no data-plane half, no possible orphan), `MergeTablets`,
  `SetTabletPolicy`, `CreateTableSchema`, `DropTableSchema`,
  **`DropTableTablets`** (ADR 0024: removes *every* tablet scoped to a table +
  its policies in one apply — the metadata half of drop-table GC, mirroring the
  `MergeTablets` cleanup; `NoOp` when the table has none), `CreateTableIndex`,
  `DropTableIndex`, `SetTableMode`, `CreateKeyspace`, `DropKeyspace`,
  **`RegisterCpAddr`** — now carrying an optional `tablet` association (ADR 0024
  GC): a tablet-scoped registration is rejected while the tablet is absent, and
  when a tablet leaves the map (`DropTableTablets`/`MergeTablets`) its members'
  addresses are pruned from `cp_member_addrs` + `cp_member_tablets`, keyed on
  *current absence* so a replayed historical state cannot resurrect them;
  legacy tablet-less entries are never pruned). `Metadata::apply` is the
  deterministic state machine; `Metadata::reconcile` is
  the pure placement decision (see below), whose shared body also backs
  **`PlacementView::reconcile`** — the narrow (members + tablets + policies,
  no schema catalog) clone `RaftCore::placement_view()`/`RaftCore::members()`
  hand the driver loops so they evaluate **off the core lock** instead of
  cloning the whole `Metadata` every tick (clone-churn fix);
  `table_schema`/`has_table_schema`/`table_schemas`/`table_indexes` read the
  catalog.
- `schema.rs` — the replicated **table-schema catalog** (ADR 0013): `TableSchema`
  (partition key + ordered clustering keys + typed `ColumnDef`s + `indexes:
  Vec<IndexDef>`), `ColumnType` (the union of CQL scalars + DynamoDB key families),
  and `SchemaCatalog` (a `BTreeMap<TableName, TableSchema>` held in `Metadata`).
  `IndexDef`/`IndexKind`/`IndexProjection` carry a **secondary-index definition**
  (GSI/LSI: name, kind, hash/sort attributes, projection) — the replicated index
  *shape*, not its entry data. `TableSchema::validate` is the pure
  malformed-schema check the state machine applies (incl. unique index names + an
  LSI requiring a sort attribute). All plain data — no I/O, no clock, no RNG.
- `raft.rs` — `RaftCore<C, S>`: a **synchronous, I/O-free** Raft state machine,
  **generic over its command `C` and applied state-machine `S`** (defaults:
  `MetaCommand` / `Metadata`, so existing references are unchanged). Time and
  randomness are parameters (`now`, `entropy`); it returns outbound messages and
  emits WAL records. The state machine is the `StateMachine<C>` trait (`apply` +
  `noop`), implemented by `Metadata` for the control plane and by a key-value store
  for the future per-tablet data plane (ADR 0016). The consensus logic
  (election/replication/commit/snapshot/`InstallSnapshot`/durability) is identical
  for any `S`; only `apply` and the snapshot image type are `S`-specific. **The
  driver `RaftNode` stays control-plane-specific** (it bakes in reconcile +
  failure-detection); the KV data plane gets its own driver. `RaftCore::metadata()`
  is the `S = Metadata` convenience over the generic `state()`. **Membership is
  config-in-log (ADR 0017 C):** `LogEntry` may carry a `config: Option<voters>`;
  `RaftCore` keeps `peers`/`cluster_size` in sync with the latest log config (the
  config rides snapshots + `InstallSnapshot`), and `change_membership` appends a
  single-server config entry (one-in-flight, no leader self-removal, **and gated
  on the leader having committed a current-term entry** — the reconfiguration
  erratum guard: rejected until `commit_index >= first_term_index()`, the index
  of the election no-op recorded in `become_leader`; that accessor is also what
  the data plane's ReadIndex barrier gates on, Raft §6.4). `become_leader` also
  advances commit after appending its no-op, so a **single-node** group commits
  it immediately — which is what makes a restarted sole voter re-apply its
  recovered WAL tail (per `recovered`'s contract) instead of waiting for the
  next propose. The control plane never reconfigures, so its config stays
  `= initial_config` and its behavior is unchanged. `applied()` is a **bounded
  window** (commands since the last snapshot — `snapshot_upto` drops the covered
  prefix, so it can't grow unboundedly in production); compare divergence before
  the snapshot threshold. **Pre-vote (ADR 0009):** an election-timeout no longer
  campaigns directly — the node becomes a `Role::PreCandidate` and runs a
  `RaftMsg::PreVote`/`PreVoteResp` round **without bumping its term**; only a
  pre-vote majority triggers the real, term-incrementing `start_election`. Peers
  grant a pre-vote only with **no live leader** (leader lease =
  `leader_id.is_some() && now < election_deadline`, or `role == Leader`), so a
  briefly-stalled/partitioned node can't inflate the cluster term and disrupt a
  healthy leader. Pre-vote messages **bypass** the higher-term step-down (the one
  exception: a *rejecting* `PreVoteResp` with a higher real term reverts the
  pre-candidate to a follower at that term). `set_election_timeout(base, now,
  entropy)` makes the (default-150ms) timeout base configurable so the assembly
  layer can widen it for a node doing real disk I/O. Both are additive on the
  shared wire, so `animus-cp-data` reuses them unchanged. **Two apply models
  (ADR 0017):** the control plane's `Metadata` applies **in-core, synchronously**
  (`StateMachine::DRIVER_APPLIED = false`, the default); a data-plane KV store sets
  `DRIVER_APPLIED = true`, so the core does *not* apply in-core — it buffers each
  committed-and-durable command as an effect for the **async driver** to apply to a
  real `StorageEngine` (drained via `RaftCore::drain_apply`, the `AccordCore`
  pattern, since engine I/O is async and the core is sync). Effects are still
  durable-gated, so `drain_apply` only hands out fsynced commands. (Stage A.1 done;
  engine-as-snapshot + streaming `InstallSnapshot` for the data plane is A.2.)
- `persist.rs` — `WalRecord`, `PersistedState` (durability/recovery). The WAL
  write/compact/recover flow is diagrammed in `docs/wal.md`.
  `PersistedState::encode_snapshot_record_from_blob` encodes the WAL `Snapshot`
  line **reusing the core's cached serialized image** (`snapshot_blob`) for the
  large `metadata` field (via `serde_json` `RawValue`), so compaction serializes
  `Metadata` **once**, not twice (once to ship, once for the WAL) — see the
  driver-liveness note below. Byte-identical to the plain encode; guarded by
  `snapshot_record_blob_reuse_round_trips`.
- `node.rs` — `RaftNode<E>`: the `Env` driver wrapping the core, plus
  `reconcile_loop` (the leader's automatic placement reconciler) and
  `detect_loop` (the leader's failure detector, ADR 0012). Also the
  `heartbeat_loop`/`send_heartbeat` helpers a member runs to heartbeat the
  control group. The driver records control-plane **metrics** (ADR 0015) via
  `record_outbound`/`record_transition` and the `detect_loop` propose path;
  `RaftNode::metrics()` exposes the handle and `start_with_metrics` lets a caller
  supply the sink. `RaftNode` also has read-only state accessors used by the
  `animusd` admin interface (ADR 0020): `role`/`term`/`leader`/`is_leader`/
  `commit_index`/`last_applied`/`durable_index`/`snapshot_index`/`log_len`/
  `last_log_index`/`config` (thin locks over the same-named `RaftCore` reads).
- `detector.rs` — `FailureDetector` (ADR 0012): a **pure**, unit-tested
  interval+timeout liveness detector — last-heartbeat instants + `now` + a
  `timeout` decide alive/dead. No clock, no RNG.
- `lib.rs` re-exports `animus_placement::PlacementPolicy`, so a downstream
  assembler (e.g. `animusd`) can `SetTabletPolicy` without taking a direct
  `animus-placement` dependency — the policy is part of this plane's public
  metadata surface (`Metadata::policies`).

## What's non-obvious

- The split is deliberate: **all consensus logic is in the sync `RaftCore`**
  (unit-testable, deterministic); the driver only does I/O. When changing
  protocol behavior, change the core and keep it I/O-free — don't reach for the
  env inside it.
- The driver races `env.recv()` against a timer via `futures::select`. It draws
  `entropy` every iteration (deterministic) and passes it in for randomized
  election timeouts.
- Durability: the core emits `WalRecord`s at log-append/truncate sites;
  `drain_persist` also folds in any hard-state (term/vote) change, so a granted
  vote is persisted before it's sent. The log is offset by a snapshot:
  `snapshot()` truncates the committed prefix it covers, and on a threshold the
  driver snapshots + rewrites the WAL to `wal_image()` (snapshot + hard + log
  tail) via atomic `Disk::replace` — bounding both. Recovery restores the
  snapshot and **re-applies the tail** (commit re-advances), so a CAS lands once.
  A follower behind the leader's compacted prefix is caught up via a **chunked**
  `InstallSnapshot`: the leader serializes `Metadata` and ships it in
  offset-addressed chunks of `SNAPSHOT_CHUNK_BYTES` (one per round trip, resuming
  from the per-peer `snapshot_offset`); the follower reassembles into a
  contiguous buffer (`incoming_snapshot`) and installs **atomically** only when
  the final chunk completes the buffer. `InstallSnapshotResp.next_offset` drives
  the next chunk; its `last_index` is non-zero only on completion. Chunking is
  all in the sync core (no I/O), so it stays deterministic. See `docs/wal.md`.
  **`snapshot_chunk_for` slices the `snapshot_blob` by reference — it does NOT
  re-serialize (or clone) per chunk** (the driver-liveness fix, see below). The
  two state-machine kinds manage the blob differently:
  - An **in-core** (`Metadata`) image is `serialize(metadata)`, kept **eagerly**:
    set by the core in `snapshot_upto` (local snapshot), on *install* completion
    (retain the received bytes), and in `recovered` (a recovered leader may ship
    before it re-compacts) — so the in-core invariant
    `snapshot_index > 0 ⟹ snapshot_blob.is_some()` holds and a chunk is never a
    0-byte ship (regression:
    `install_snapshot.rs::caught_up_control_node_reships_non_empty`).
  - A **`DRIVER_APPLIED`** (data-plane KV) image is the *engine* bytes, built
    **lazily on demand**: when replication needs a chunk and no image is
    materialized, the core sends nothing and raises `take_snapshot_needed`; the
    async driver scans the engine, calls `snapshot_upto(engine_applied)` *then*
    `set_snapshot_blob` (base and image must agree), and the next heartbeat
    ships. The core **drops** the blob whenever it would go stale or idle (base
    move in `snapshot_upto` — which also clears `snapshot_offset` so in-flight
    transfers restart at 0 against the new image — install completion on the
    receiver, and last-transfer completion on the sender), so no whole-tablet
    image is retained at rest and threshold compaction never builds one. The
    second-hop invariant is now *"any node with `snapshot_index > 0` can
    regenerate the image from its engine"* (regression:
    `driver_applied_sm.rs::caught_up_node_reships_non_empty_snapshot`, which
    drives both hops through the request→build→ship cycle).
- **Driver-liveness (deferred fix #5, the control-plane counterpart of the CP-data
  fix in ADR 0017).** The control driver applies `Metadata` **in-core,
  synchronously**, so — unlike CP-data — there is no slow async engine apply to move
  off the loop. The one O(state) hazard was `snapshot_chunk_for`
  **re-serializing the whole `Metadata` per 1KB chunk**: on a multi-MB metadata a
  follower catch-up shipped ~thousands of chunks, each an O(state) serialize (~50ms
  at ~1MB), pinning the consensus loop far past the 150ms election timeout — a
  self-sustaining election storm during any large-state catch-up. Fixed by
  **caching the serialized image in `snapshot_blob` and slicing it** (above), so
  chunk-serving is O(chunk). To avoid *doubling* the compaction serialize (the blob
  in `snapshot_upto` **plus** the WAL `Snapshot` record), `compact_wal` reuses the
  blob for the WAL via `RaftCore::encoded_wal_image` /
  `PersistedState::encode_snapshot_record_from_blob` (`RawValue`), so compaction
  still serializes `Metadata` exactly **once**. The remaining inline cost — a single
  compaction serialize on the loop — is a *bounded* stall (~50ms at ~1MB, ~120ms at
  ~3MB) that stays under the election timeout at realistic scale and is **not**
  self-sustaining (one stall per 64 applied entries, then the loop resumes and
  heartbeats); moving it fully off the loop was **assessed and deferred** (mirroring
  CP-data's apply-task split would couple the install→WAL-rewrite ordering into a
  second task on the most safety-critical Raft — real risk for a bounded, rare,
  extreme-scale stall). Liveness teeth: the wall-clock-timed
  `install_snapshot.rs::large_snapshot_ships_in_o_chunk_time_not_o_state` (fix: ~ms;
  regression: ~46s for a 1MB/1066-chunk snapshot) plus the real-thread `ProdEnv`
  smoke test `tests/prod_liveness.rs`.
- `CasTabletReplicas` applies only if the tablet's epoch matches, then bumps it
  — evaluated identically on every replica, so accept/reject is consistent.
  **`SplitTablet` is the same CAS shape, on an `expected_epoch` field — and,
  since ADR 0028, it is the *entire* split operation**, not just its metadata
  half: two proposers racing to split the same tablet at the same epoch with
  different keys must not both commit, and the epoch CAS is now the *only*
  guard needed (there is no data-plane half left that could independently
  fail or apply a conflicting second split) — the loser's proposal is
  rejected with `"epoch mismatch"` before recomputing the range split, exactly
  mirroring `CasTabletReplicas`; see
  `meta.rs::split_rejects_a_stale_epoch_racing_a_concurrent_split` and
  `tablet_split_merge.rs::racing_splits_at_the_same_epoch_only_one_applies`
  (the latter drives the actual race through Raft: two `SplitTablet`s
  proposed back-to-back at the same epoch, only the first applies). Because
  commit of this one command *is* the whole operation (the new sibling tablet
  is immediately servable — its `StorageScope` covers already-present data on
  the node's shared engine, ADR 0026/0028), a metadata-only, leaderless orphan
  tablet is now structurally impossible; the old `DropOrphanTablet` cleanup
  command this used to need is gone. See `animusd/CLAUDE.md`'s `trigger_split`
  notes for the calling side.
- **Automatic placement (ADR 0005).** Policies are replicated in `Metadata`
  (`SetTabletPolicy` → `policies` map). The decision lives in the pure
  `Metadata::reconcile` (runs `animus_placement::replan` over `Active` members,
  emits a `CasTabletReplicas` only for tablets whose set violates the policy —
  idempotent). The **leader** drives it: `node.rs`'s `reconcile_loop` ticks on a
  slow `env.sleep` timer and proposes the result. Keep the *timing* in the
  driver and the *decision* pure — don't put a clock or RNG in `reconcile`, and
  don't reconcile off-leader (a non-leader `propose` is dropped; a stale CAS is
  epoch-rejected). `animus-placement` is a **normal** dependency now (no cycle).
- **Automatic failure detection (ADR 0012).** Members heartbeat the control group
  (`heartbeat_loop` → `RaftMsg::Heartbeat`, a term-less message the driver
  **intercepts** in its `recv` arm and feeds to the shared `FailureDetector` —
  the core never sees it). The decision is the pure `FailureDetector` (in
  `detector.rs`); the **leader** drives it: `detect_loop` ticks on an `Env` timer
  and proposes `UpsertMember{Active/Down}` for any tracked member whose liveness
  changed (`liveness_transitions`, idempotent — preserves labels, skips
  `Joining`/`Leaving`, only judges members that have heartbeated). A committed
  `Down` is what the placement reconciler already reacts to, so a detected failure
  **cascades** into re-placement. Keep timing in the driver and the decision pure
  — don't put a clock/RNG in the detector. Detector state is per-node volatile (a
  new leader re-learns over one `timeout`); only the transitions are replicated.
  These loops are **now driven in production**: `animusd` spawns `heartbeat_loop`
  on each data node and relies on `RaftNode::start`'s `detect_loop`/`reconcile_loop`
  to mark a dead data member `Down` and re-place its tablet — proven live over
  `ProdEnv`/TCP in `animusd/tests/self_heal.rs` (the sim coverage here remains the
  deterministic source of truth). A freshly elected leader's detector is **cold**
  (per-node volatile state), so `detect_loop` applies a **post-election grace
  period** (`LEADER_GRACE`, one `DETECT_TIMEOUT`): it tracks `leader_since`
  (`Env` time, re-armed per term) and passes `allow_down = false` to
  `liveness_transitions` until the grace elapses, so a new leader can't falsely
  mark live members `Down` before heartbeats repopulate the detector. Recoveries
  are never suppressed. The gate is `Env`-time only (deterministic).
- **Replicated table-schema catalog (ADR 0013).** `Metadata.schemas`
  (`SchemaCatalog`, in `schema.rs`) holds each table's `TableSchema`, mutated only
  by `MetaCommand::{CreateTableSchema, DropTableSchema}` applied in
  `Metadata::apply`: create is rejected on a duplicate or a malformed schema
  (`TableSchema::validate`), drop is idempotent. Because it lives in `Metadata`,
  it is Raft-replicated and recovered from the WAL/snapshot like all metadata (no
  `persist.rs`/`InstallSnapshot` change — the snapshot is a full `Metadata`
  image). Read it via `Metadata::table_schema`/`has_table_schema`/`table_schemas`.
  The shape (partition key + clustering keys + typed columns) is the union of both
  wire adapters' needs; **the adapters consuming it is a deliberate follow-up.**
  **Secondary-index definitions** (GSI/LSI) ride the same path:
  `TableSchema.indexes` holds the `IndexDef`s, mutated by
  `MetaCommand::{CreateTableIndex, DropTableIndex}` (create rejects an unknown
  table or a malformed schema and replaces an index of the same name; drop is
  idempotent). Read via `Metadata::table_indexes(table)`. Only the index *shape* is
  replicated — the index *entry data* (the actual rows) stays at the wire edge
  (rebuilt from observed writes). End-to-end in `schema_indexes.rs`.
- **Observability metrics (ADR 0015).** The driver records, all from
  `Env`-supplied or core-derived inputs (deterministic): `elections_started`/
  `elections_won` + an `is_leader` gauge (from role/term transitions in
  `record_transition`); `append_entries_sent`/`append_entries_rejected` +
  `snapshot_installs` (read off the messages the core emits, in `record_outbound`
  — a rejection is an outbound `AppendEntriesResp { success: false }`); and
  `failure_detector_down`/`failure_detector_up` (the `Active`↔`Down` edges
  `detect_loop` proposes). `RaftNode::start` records into `env.metrics()`
  (`ProdEnv`'s real sink); use **`start_with_metrics`** to thread a recording
  handle a sim test can read — `SimEnv::metrics()` is the no-op default, so no
  `animus-sim` change is needed.
- **Durable-before-visible: `apply` is gated on `durable_index` — but only on the
  *leader*.** `apply`'s frontier is **role-aware** (ADR 0009): the **leader** uses
  `min(commit_index, durable_index)`, a **non-leader** uses `commit_index`.
  Rationale: only the leader's applied state is what a proposer *acks* on, so a
  command is leader-visible (`metadata()`/`applied()`) **only after it is fsynced**,
  never before — `RaftCore::propose` advances `commit_index` synchronously, but the
  leader won't apply past what's on disk. A **follower** never acks a write to a
  client (it only serves *reads*); a committed entry already rests on a quorum of
  durable logs (the driver flushes *before* sending outbound, so a follower fsyncs
  before its `AppendEntriesResp` and the leader before its `AppendEntries`), so a
  follower safely applies on **commit** without waiting on its *own* local fsync —
  gating it there would only widen cross-node read-visibility lag. `last_applied`
  only moves forward, so a follower that applied to commit then wins an election
  keeps those entries, while its *own future* proposals stay durability-gated
  (their index exceeds `durable_index` until it fsyncs). The driver advances the
  watermark via **`mark_durable_through`** in `flush_wal`, *immediately after*
  `env.sync(WAL)` (passing the drain-time `last_log_index`); `recovered()` sets it
  to the recovered `last_log_index`. The leader gate closed the acked-before-durable
  window that flaked `animusd`'s `create_table_survives_node_restart` (acute
  single-node, where commit is self-only). **Gotchas:** (1) a *leader* core driven
  by hand (not via `RaftNode`) must simulate the fsync — drain, then
  `mark_durable_through(last_log_index())` — or its `metadata()` never reflects
  proposals (see `persistence.rs`'s `persist` helper); a hand-driven *follower*
  applies on commit with no fsync (see `follower_visibility.rs`). (2) A read on a
  follower right after a leader `CreateTable` must still wait for the definition to
  *replicate* there (`await_table_*` in the `animusd` tests) — a cross-node race
  independent of the local durable gate. When you touch propose/commit/apply,
  preserve this ordering — don't apply past `durable_index` *on the leader*.
- Commit advances only for **current-term** entries via majority `matchIndex`
  (the Raft safety rule). Don't relax this.
- Snapshot transfer is **chunked** (see above). Deferred: cross-leader resumption
  (a transfer interrupted by a leader change restarts at offset 0) and chunk-stream
  flow-control.

## Tests

`cargo test -p animus-control` — election/replication/leader-kill +
multi-seed convergence (`control_raft.rs`), durability/recovery
(`persistence.rs`), the **role-aware apply gate** (`follower_visibility.rs`,
ADR 0009 — a hand-driven follower applies a committed entry with `durable_index ==
0`, a leader stays gated on its own proposal, a follower→leader transition keeps the
applied entry and gates new proposals, and both followers in a `SimEnv` cluster
reflect the leader's committed command), split/merge (`tablet_split_merge.rs`), snapshot truncation
(`wal_compaction.rs`), a partitioned follower catching up via `InstallSnapshot`
plus a far-behind follower catching up via a **multi-chunk** `InstallSnapshot`
(`install_snapshot.rs`), process restart-and-rejoin (`restart.rs`, using
`Simulator::stop`), caller-driven placement reconcile through Raft under a
replica death + follower crash (`placement_reconcile.rs`, driving
`animus-placement`), and **leader-driven automatic** reconcile from a replicated
policy (`placement_auto_reconcile.rs` — no test-side `replan`/CAS), and
**heartbeat-based failure detection** end to end (`failure_detection.rs`, ADR
0012 — a member crashes, the leader auto-commits `Down`, placement reconciles off
it, then the member restarts and returns to `Active`; plus detector unit tests in
`detector.rs` and grace-gate unit tests for `liveness_transitions` in `node.rs`),
and the **replicated table-schema catalog** end to end (`schema_catalog.rs`, ADR
0013 — propose schemas, reject a duplicate + a malformed one on the state machine,
kill the leader, assert the schemas survive + survivors agree, drop one and see it
replicate; plus `schema.rs` unit tests), the **replicated secondary-index
definitions** end to end (`schema_indexes.rs`, ADR 0013 — create a GSI so a second
node sees it, reject an index on a phantom table + a malformed LSI, restart a node
and see the definition survive from the catalog, drop it cluster-wide; reproducible
from a seed), and **control-plane metrics** moving
under known events (`metrics.rs`, ADR 0015 — a forced election bumps the election
counters + the leadership gauge; a crashed heartbeating member bumps
`failure_detector_down`, its recovery bumps `failure_detector_up`; plus a
same-seed byte-identical-snapshot reproducibility check), and **pre-vote**
(`pre_vote.rs`, ADR 0009 — core-level: a live-leader lease rejects a pre-vote and
never changes the term, an expired lease grants, a timeout makes a `PreCandidate`
without bumping the term; end-to-end under `SimEnv`: an isolated follower's
pre-vote rounds don't move the stable leader's term and it rejoins on heal with no
election, and a genuine leader crash still elects a new leader at a higher term).
Use `run_for`, never `run()` (perpetual heartbeats).
