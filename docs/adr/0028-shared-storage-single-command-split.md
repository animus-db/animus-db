# ADR 0028 — Shared per-node storage, control-plane-only tablet split

- **Status:** Accepted — implemented in `animus-control`, `animus-cp-data`,
  `animusd`. Supersedes ADR 0017 §4's tablet-split design and the "D" stage
  (D1–D3) described in ADR 0017's implementation log. **§3's write fences are
  now wired into every real CP write path** (`animusd`'s
  `cp_put_local`/`cp_delete_local`/`cp_batch_propose`, 2026-08-07) — see the
  note at the end of §3 below; they were merged additively but had zero
  production callers until this fix. **2026-08-10 correction to "What drop-table
  GC does instead of deleting files" and the "shares one engine's write path"
  consequence below**: "multiple independent version streams sharing one
  engine" is safe **only as long as a stream never starts serving a key
  another stream already versioned, without a floor** — a gap this ADR did
  not call out, confirmed real and fixed (`animus_tablet::Tablet::
  version_floor`, root `CLAUDE.md`'s cross-group-LWW entry). A split's fresh
  sibling group's own Raft log index restarts low/independent, so it could
  otherwise carry a version no higher than what the *source* group already
  stamped for a key now in the sibling's range, and per-key LWW would
  silently drop the overwrite. The two in-text notes below are otherwise
  unchanged (this is additive, not a retraction — `merge`/`merge_tombstone`
  still have no *engine-wide* monotonic floor, and still shouldn't).
- **2026-08-10 note:** ADR 0038 gives the **control plane** a per-node
  system-keyspace slice of "one shared engine" too (a combined node's control
  `Metadata` now durably lives in the same already-open shared engine this
  ADR describes, globally namespaced rather than `StorageScope`-confined per
  tenant) — the sharing *mechanism* this ADR established (one engine, many
  independently-versioned writers) is what makes that possible with no new
  storage-layer primitive.
- **Date:** 2026-08-07

## Context

Tablet split (ADR 0017 §4, Stage D) was a **two-phase** operation: the control
plane committed `MetaCommand::SplitTablet` (metadata only — mints a new tablet
id, narrows the source's range), and *separately* the source tablet's own
per-tablet Raft group had to agree a `KvCommand::Split` and physically hand off
the upper range's data to a **brand-new** group, minted via `Coresident::sibling`
(a fresh `NodeId`/env/directory/WAL per new tablet, `cp_member_id = base +
tablet * CP_SPLIT_ID_STRIDE`).

The non-atomicity between those two phases was not an edge case — it was the
direct cause of a long, still-growing list of documented bugs (see the root
`CLAUDE.md` Engineering Practices section, now marked superseded): orphaned
metadata-only tablets when step 2 failed independently of step 1, retry-storm
amplification in the step-2 confirm loop, an epoch-CAS race between two
proposers minting different children of the same tablet, a `pending`-retry map
and a cluster-wide auto-split claim to work around all of the above, a
`DropOrphanTablet` cleanup command, and a `Coresident`-minted sibling-pool
liveness cliff (a hard-coded `CP_SIBLING_POOL = 64` cap, panicking the split
hook on exhaustion, leaving the over-cap tablet permanently leaderless).
Nearly every "Code patterns" entry from the last dozen PRs on this codebase
traces back to this one seam.

Two prior, independent decisions made a fundamentally different design
possible:

- **ADR 0026 Stage A** gave `Network` a `(node, stream)` addressing axis, so a
  node can host an unbounded number of protocol instances on one inbox instead
  of minting a new `NodeId`/env per instance.
- The `StorageEngine` trait (ADR 0004/0008) already supports an arbitrary
  key-range **scan** and **`merge_tombstone`**, which is all that is needed to
  confine multiple tenants to disjoint physical key ranges within one engine.

Given those, the reason a data-plane split command ever existed — **physical
data movement**, handing bytes from one tablet's dedicated engine/WAL to a
brand-new one's — turns out to be self-inflicted: it exists only because each
tablet had its *own* engine and its *own* Raft group inbox. Remove both of
those constraints and a split has nothing left to move.

## Decision

We will:

1. **Give every node one shared `StorageEngine`** (an `LsmEngine` or
   `MemoryEngine`, matching the existing `StorageBackend` choice), opened once
   at node start, instead of one engine per tablet. Every tablet a node hosts —
   across every table — merges into this same engine.
2. **Confine each tablet's physical key access with a `StorageScope`**
   (`animus-cp-data`): a `prefix` (the owning table's identity, `escape(table_name)`
   — order-preserving and prefix-free, so tables can never collide even sharing
   one engine) plus a `range` (this tablet's own sub-portion of that table's
   keyspace). The range is **live-narrowable** (`Arc<Mutex<KeyRange>>` +
   `narrow()`), because a tablet's range shrinks when it is the source of a
   split while its physical data does not move.
3. **Fence every proposed write with its own range at propose time**
   (`fence: KeyRange` riding inside `Put`/`Delete`/`Cas`/`Batch`, checked at
   apply time against the fence *embedded in the log entry* — never a
   locally-polled value). This is what keeps the crossover window — a stale
   client still addressing the old, wider range right after a split narrows it
   — deterministic: every replica, regardless of how far it has independently
   progressed through observing the split's `Metadata`, makes the identical
   accept/reject decision for the same committed entry, because the decision
   travels with the entry rather than depending on when each replica happens
   to notice the split.

   **2026-08-07 update: wired into every real write path, plus a pre-propose
   check the original design under-specified.** The `*_fenced` proposers and
   `KeyRange`-embedded fence above landed additively (unit-tested in
   `animus-cp-data/tests/fenced_commands.rs`) but `animusd`'s actual CP write
   helpers (`cp_put_local`/`cp_delete_local`/`cp_batch_propose` — reached by
   every client write, including every `cp_serve_forwarded` counterpart) kept
   calling the *unfenced* `put`/`delete`/`put_batch` (`fence =
   KeyRange::whole()`), so the fence was a no-op in production: a
   stale-routed write during the crossover window could still land on and
   corrupt/shadow a split child's data. Fixed by adding an additive
   `RaftKvNode::scope_range()` accessor (and a `StorageScope::range()`
   getter) and stamping it as the fence on every real proposal. But the fence
   alone is not sufficient: those write helpers confirm success by reading
   back the proposed value (or its absence, for a delete) from **local**
   storage — a fenced-out entry still commits and applies as a no-op, so a
   confirm mechanism keyed on a coarser signal than exact value equality
   (e.g. a bare "has this index applied" watermark, which a no-op still
   advances) would **falsely ack** a write that never happened. The actual
   fix is a **pre-propose range check**: before proposing at all, the key(s)
   are checked against the group's own live `scope_range()`; a miss returns
   an ordinary routing-failure error (no propose), so the caller's retry
   re-resolves `cp_route` and reaches the correct child once this node's own
   view has caught up. The embedded fence still rides the entry regardless,
   covering the residual race between the pre-check and the entry's actual
   apply (the scope can narrow further in between) — a write landing in that
   sliver is dropped as a safe no-op, never mis-applied. See the root
   `CLAUDE.md` Engineering Practices entry for the general lesson.
4. **Adopt ADR 0026 Stage B**: migrate `RaftKvNode` fully onto `(node, stream)`
   addressing, `stream = tablet_id`, on the node's one `raftkv` env. A tablet's
   CP group member id is therefore simply the base `raftkv` id — not a derived
   `NodeId` — at any split depth. This retires `Coresident`/the `ProdEnv`
   sibling pool/`CP_SIBLING_POOL` and the whole
   `cp_member_id`/`cp_base_id`/`cp_members_for`/`CP_SPLIT_ID_STRIDE`
   translation seam for this crate pair.
5. **Make `MetaCommand::SplitTablet` (`animus-control`) the entire split
   operation.** It is epoch-CAS gated exactly like `CasTabletReplicas`
   (rejecting a stale-epoch racing proposer cleanly, at propose time). On
   commit, the source tablet's range narrows and a new sibling tablet is
   minted covering the handed-off range — both immediately servable, because
   the new sibling's `StorageScope` already covers live data on the same
   shared engine. There is no second, data-plane step to fail, retry, or leave
   half-done. The per-node **join-host loop** (`animusd`) then simply starts
   the new tablet's `RaftKvNode` the same way it starts any fresh tablet's —
   `topology::plan_join_host` no longer distinguishes "fresh split child" from
   "fresh whole-keyspace tablet" at all, because both start from data already
   present (or absent) in the shared engine, with nothing to seed.
6. **Full replace, not a dual-mode shim.** Pre-alpha, no migration concerns:
   `KvCommand::Split`, `propose_split`, `current_split_bound`/`SPLIT_BOUND_KEY`,
   the split hook (`SplitHook`, `start_with_split_hook`/`start_seeded_with_split_hook`),
   the `cp-hosted` durable marker (`load_hosted_cp`/`save_hosted_cp`), and
   `MetaCommand::DropOrphanTablet` are all **deleted**, not deprecated.

### What replaces the durable "which tablets does this node host" marker

The old design needed a durable per-node marker (`cp-hosted`) because "which
co-resident engines physically exist on this node" was local state not
derivable from the replicated tablet map. With one shared engine, that
question no longer needs answering at all — a restart just re-opens the one
engine (which recovers its own durable state) and the join-host loop
re-discovers every tablet to host by polling replicated `Metadata` fresh; if a
tablet was already resident, `StorageScope::has_data` (an async presence
check against the shared engine) tells the join-host loop "reform with the
full voter config," the same way a fresh-formation epoch does.

### What drop-table GC does instead of deleting files

Dropping a table's tablets (ADR 0024) can no longer delete "this tablet's
engine files," because the engine is shared. Instead, `RaftKvNode::erase_scope`
tombstones every key in the tablet's own `StorageScope` via
`StorageEngine::merge_tombstone` — never `delete_range`, which enforces an
engine-wide monotonic version floor that multiple independent per-tablet Raft
groups sharing one engine do not (and should not) share — at a version
(`last_applied() + 1`) guaranteed to exceed every version that specific group
ever wrote, since every merge it ever performed was stamped at most its own
applied index. Actual space reclaim happens later via the engine's existing
tombstone-GC compaction. Each tablet still gets its **own Raft WAL file**
(`raftkv.wal.<tablet>`) on the shared env, since `Disk` files are keyed by
name, not by stream (see "Deferred," below); GC deletes that one file
directly.

### The auto-split trigger simplifies to match

`animusd::auto_split_loop` no longer needs the `pending`-retry map (there is
no step 2 to retry), the cluster-wide `claim_auto_split`/`release_auto_split`
contention guard (a same-tick redundant `SplitTablet` from multiple nodes is
just a normal epoch-CAS race with one clean winner now — no orphan risk to
guard against), or any "already split once" exclusion (a tablet was never
actually limited to one split; splitting was always just a range-narrowing
command, so a regrown tablet is a legitimate candidate again with no special
case).

## A race this change newly exposed (and its fix)

`animusd::cp_reconfigure_loop` (steps a CP group's Raft voters toward a
tablet's replicated replica set) and `animus-control`'s policy `reconcile_loop`
(re-CASes a replica set back to satisfy its placement policy) are two
independent, un-jittered, fixed-500ms pollers. A manual (or higher-level)
replica-set change is a **one-shot race** between them — whichever observes it
first decides the outcome, since the loser's own next tick sees an
already-equal-to-desired state and never retries. Reordering this change's own
node-startup sequence (opening the shared engine before spawning the
control-plane's `RaftNode`, rather than after) shifted, but did not eliminate,
which side had first-mover advantage. The fix: `cp_reconfigure_loop` now polls
at a third of `reconcile_loop`'s period (150ms vs. 500ms, plus jitter), so an
operator-driven replica-set change reliably wins. See the root `CLAUDE.md`
Engineering Practices entry for the full diagnosis.

## Consequences

**Enabled:**

- Orphaned, leaderless, metadata-only tablets are now **structurally
  impossible** — there is no second step that can fail independently of the
  first, so there is nothing left to leave half-done.
- Deletes an entire class of previously-patched bugs at the root instead of
  continuing to patch them (retry-amplification, cluster-wide contention
  claims, epoch-CAS-only-catches-the-concurrent-case, orphan GC with an
  inherently ambiguous confirm signal).
- A node's storage footprint is no longer duplicated per tablet (one
  `LsmEngine`'s memtable/SSTable/compaction machinery per node, not one per
  tablet), and a split is instant from the storage engine's perspective — no
  data-copy latency, no handoff window.
- Removes a confirmed liveness cliff (`CP_SIBLING_POOL` exhaustion) entirely,
  not just raises its ceiling.

**Costs and risks knowingly accepted:**

- **Not yet a single physically-multiplexed WAL file per node.** Each tablet
  still gets its own Raft WAL file (`raftkv.wal.<tablet>`) on the shared env,
  because `Disk` files are keyed by name, not by stream. A prior increment
  built (but did not wire in) exactly the machinery for this — a
  `TaggedRecord`/`SharedWal` scheme in `animus-control` that multiplexes
  multiple tablets' WAL records into one physical file with per-tablet
  compaction and cross-tablet segment GC. Wiring it into `animus-cp-data`'s
  `drive`/`persist_wal`/`apply_and_compact` is deliberately **deferred** — it
  needs its own segment-GC design and fault-injection tests (crash
  mid-segment-roll, crash mid-per-tablet compaction, one tablet's compaction
  racing another's), which is exactly the kind of change that should not be
  bundled into an already-large integration PR.
- **A node's tablets now share one engine's write path, memtable, and
  compaction state.** A very hot tablet's write load or a large compaction now
  has *some* shared-resource interaction with every other tablet on the node
  (memtable flush thresholds, compaction scheduling), where before each
  tablet's engine was fully isolated. `merge`/`merge_tombstone`/`merge_batch`
  already tolerate multiple independent version streams sharing one engine (no
  engine-wide monotonic floor — only `put`/`delete`/`delete_range` enforce
  that, which is why GC uses `merge_tombstone`, not `delete_range`), so this is
  a resource-contention concern, not a correctness one; no regression was
  observed in the production wiring's own multi-tablet write-path tests, but a
  dedicated multi-thread `ProdEnv` load test analogous to
  `seed_load_does_not_storm_cp_elections` (proving N concurrent tablet apply
  loops on one shared engine don't stall each other into an election storm)
  is a natural follow-up, not yet written.
- **The `cp_reconfigure_loop`/`reconcile_loop` race (above) is mitigated, not
  eliminated.** A sufficiently large scheduling perturbation (e.g. heavy host
  contention) could still occasionally let the slower loop win; the fix
  reduces the failure probability by roughly polling-period-ratio, it does not
  make the race structurally impossible. An event-driven reconfiguration
  trigger (react to a `Metadata` change directly, rather than polling) would
  close this properly and is a candidate follow-up if it is ever observed to
  matter beyond test flakiness.
- **`animus-env`'s `Coresident` trait and its `SimEnv`/`ProdEnv`
  implementations are left in place**, unused by `animus-cp-data`/`animusd`
  now. Not removed, since it is a general `Env`-seam capability that might be
  needed again for an unrelated purpose; ADR 0026 tracks its status.

This ADR builds on ADR 0016/0017 (the per-tablet Raft data plane, whose §4
split design and Stage D this supersedes), ADR 0004/0008 (the `StorageEngine`
trait this depends on), and ADR 0026 (the stream-addressing seam this
completes Stage B of). The control plane's epoch-CAS discipline
(`CasTabletReplicas`, ADR 0005) is unchanged in shape — `SplitTablet` was
always the same shape, it simply now carries the *entire* operation instead of
one half of it.

## Amendment (2026-08-11, ADR 0018 PR2)

The fence (embedded per-entry, gating apply) closes the crossover window
*within* one source group's own log — a stale-routed write proposed before
the leader learned about the split still fails at apply if it falls outside
the fence. It does **not**, on its own, stop the source group from
*continuing to accept new writes* to the handed-off range indefinitely if its
own leader simply never re-checks its scope (the "wide fence, un-ticked
leader" case). ADR 0018 PR2's **range seal** closes that residual: once the
source proposes `KvCommand::Seal` for the handed-off range, every
later-ordered entry for a key inside it is rejected regardless of its own
fence — a second, independent gate stacked on top of the fence, not a
replacement for it. See ADR 0018's PR2 amendment for the full design.

## Amendment (2026-08-14, ADR 0042/0043)

The kind-scope set every tablet group owns (ADR 0041 §3's extension of this
ADR's "one engine, many independently-versioned writers" mechanism) grew by
one for the consumer-cursor rework: `KIND_CURSOR` (`0x04`, on a
GSI'd/streamed table's own base tablets) — five kinds total, snapshot codec
`VERSION` 13.

**Update (2026-08-14, round-3 rewrite): DynamoDB Streams adds no further
kind at all.** Round 2's design would have added `KIND_STREAM`/
`KIND_STREAM_META` (`0x05`/`0x06`) on a separate stream-shard tablet; round
3 replaces that whole tier with in-place sealing of a table's own existing
`KIND_CHANGE` scope (ADR 0043) — a stream's hot shard is literally the same
change log ADR 0041 already scoped here, and a *sealed* shard's bytes live
in an external `SegmentStore` (ADR 0043 §A7), never in this shared engine at
all. **The kind set therefore stays at five, and `VERSION` stays 13** —
nothing about this ADR's own mechanism needed to change a second time: a
kind is still a `StorageScope` sibling sharing the group's one live
`KeyRange`, `ALL_KINDS` is still the single registration point, and
`engine_image`/`erase_scope` still iterate it generically with no per-kind
special-casing required. `MergeTablets` was rejected on a streamed **base**
table (ADR 0042 §12's F1 stopgap) — an apply-time guard on an *ordinary*
tablet, never a new "structurally exempt" tablet class this ADR's own
"tablet is the unit of placement/hosting/snapshot" contract needed any
change to accommodate: nothing here assumed every tablet must eventually
merge, only that a tablet's range could *change* (narrow/widen) when one
did. **Update (2026-08-14, ADR 0044): tablet merge, `MergeTablets`, and
the F1 stopgap guarding it are all removed entirely — tablets are
split-only.** A tablet's range only ever narrows now; "widen" no longer
describes anything a tablet's range does.
