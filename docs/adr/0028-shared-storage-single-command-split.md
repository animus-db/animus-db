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
- **SUPERSEDED (2026-08-17) by [ADR
  0050](0050-per-tablet-storage-copy-based-splits.md), in full** — both
  halves: per-tablet physical engines replaced the shared per-node engine,
  and the copy-based background split workflow (build → freeze → cutover →
  retire) replaced the metadata-only zero-copy split. This ADR is
  historical record only; nothing below describes the tree as built.
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

## Amendment (2026-09-06, C-05 PR 1 benchmark)

The "Costs and risks knowingly accepted" section above deferred wiring
`SharedWal` (`animus-control::shared_wal`) into the per-tablet WAL path,
pending "its own segment-GC design and fault-injection tests." A later,
now-superseded roadmap pass briefly recommended deleting `SharedWal`
outright, reasoning from ADR 0048's "apply-poll term dominated" finding —
that finding is about **idle** cost, which quiescence (ADR 0044 phase 1)
already closes; it says nothing about **active-load** cross-group fsync
cost, which is `SharedWal`'s actual target and which quiescence does not
touch. `docs/roadmap.md`'s C-05 entry reversed that recommendation
(2026-09-02) on exactly this basis, once a `SimEnv` measurement (a
throwaway harness) confirmed the structural K-fsyncs-for-K-groups cost is
real and uncoalesced today.

This PR supplies the `ProdEnv` wall-clock benchmark the roadmap's own C-05
entry named as the prerequisite before committing to the wiring work:
`crates/animus-cp-data/benches/wal_fsync_bench.rs`
(`cargo bench -p animus-cp-data --bench wal_fsync_bench`), full method and
numbers in `docs/design/shared-wal-fsync-benchmark.md`. **Result: on this
host's real block-device-backed filesystem (ext4 on `/dev/vda`, not a
memory-backed `tmpfs`/`overlay` mount), concurrent fsyncs to K distinct
per-group WAL files are NOT already cheap at realistic tablet density** —
round latency scales with K (K=1 ~500us → K=128 ~10.5–11.2ms p50, ~25–43ms
p99), while routing the identical burst through the already-built,
unwired `SharedWal::append` API instead keeps latency nearly flat
regardless of K (~1.4–1.6ms p50 at K=128) and cuts the measured fsync
count from 128 to ~2 per round. Held consistently across three independent
runs.

**Recommendation: wire `SharedWal` (C-05 PR 2), then cut over (C-05 PR
3)** — see the design note for the full threshold, numbers, and the
single-group control that isolates this as a genuinely cross-group gap,
not a per-group group-commit gap (`persist_round.rs` already closes the
latter in production, independent of `SharedWal`). This benchmark's own
numbers are host-specific and not a media-independent claim — a
maintainer re-running it on `tmpfs`/`overlay` media should expect the gap
to shrink and should read the bench's own printed media line before
trusting a number gathered elsewhere.

## Amendment (2026-09-06, C-05 PR 2 — `SharedWal` wired behind `--shared-wal`)

`SharedWal` is now actually wired into `animus-cp-data`'s persist path,
gated behind `--shared-wal`/`cluster_settings.shared_wal`, **additive
default OFF** in this PR (byte-for-byte today's per-group-`wal_file`
behavior when omitted; PR 3 is the still-pending default-flip cutover, the
identical two-step shape `--heartbeat-batch`/C-02 used). This amendment
records the design decisions PR 2 made; see `crates/animus-cp-data/
CLAUDE.md` and `crates/animus-control/CLAUDE.md`'s `shared_wal.rs` entry
for the mechanism-level detail.

**`SharedWal` itself grew a second, group-aware API** (`animus-control::
shared_wal`) on top of the PR-1-era raw `append`/`compact` pair (kept
byte-for-byte unchanged, so `wal_fsync_bench.rs`'s own numbers stay
reproducing the identical code path): `append_tagged`/`compact_group`/
`forget`/`open`/`recovered_state`, backed by an in-memory `group_tails:
BTreeMap<TabletId, Vec<WalRecord<C, S>>>` cache — each tablet's own
currently-durable-on-the-shared-file record run. Every group-tails mutation
and its corresponding physical-write enqueue happen inside the SAME
critical section (the coordinator's pre-existing single-flight `inner`
lock), which is the load-bearing property the round/ack and GC arguments
below both rest on.

- **Round/ack semantics.** A persist round's whole record batch is
  submitted as ONE `append_tagged` call — one physical `Disk::append` +
  `Disk::sync`, coalesced with any other hosted group's own overlapping
  round (this is the mechanism PR 1's benchmark measured). The round's ack
  fires only once that physical write actually lands, identical durability
  semantics to the per-group file it replaces: a group's own entries are
  durable in index order, one group's fsync never exposes a byte of
  another's, and one group's truncation/compaction never blocks another's
  append (each mutates only its own `group_tails` entry).
- **Recovery indexing.** `SharedWal::open` is called exactly ONCE per node,
  before any tablet's own driver starts (`RaftKvNode::start_inner`'s
  `drive()` recovery step, gated on the constructor's `shared_wal: Option<
  Arc<SharedWal<KvCommand, KvState>>>` — `None` is the untouched per-group
  path). It reads the shared file once and demuxes it whole via
  `PersistedState::decode_tagged`, seeding `group_tails` for every tablet
  the file already holds; a tablet hosted later that was never in the file
  starts with an empty (absent) tail, correctly. Each group's own recovery
  is then `SharedWal::recovered_state(tablet)` — a `group_tails` lookup
  plus `PersistedState::replay` — the shared-WAL analogue of a per-group
  `env.read`+`decode`+`replay`, sourced from the one seeded read rather
  than each tablet re-reading the file independently.
- **GC policy and its bound.** There is no independent segment file to
  reclaim — the coordinator holds one physical file, atomically rewritten
  by `Disk::replace`. A tablet's own bytes are reclaimed the moment THAT
  tablet itself calls `compact_group` (`apply_and_compact`'s existing
  `COMPACT_THRESHOLD`/on-demand-image trigger, unchanged): its
  `group_tails` entry is replaced by its fresh, minimal `wal_image()`
  (snapshot + hard state + log tail), then the whole file is rewritten from
  the union of every tablet's current `group_tails` entry. A DIFFERENT
  tablet's compaction or append never touches another tablet's cached tail
  except to re-include it verbatim in the rewrite — so one tablet's GC can
  never reclaim (or lose) bytes another tablet still needs, and no tablet
  ever waits on another's compaction to reclaim its own. Bound: the shared
  file's size is the sum, over every hosted tablet, of that tablet's own
  bytes accumulated since ITS OWN last compaction — identical to the sum of
  today's per-group file sizes, just physically consolidated into one file
  instead of N. `forget` (called from `host::Reconciler::
  erase_tablet_files`'s shared-WAL branch, replacing the per-group
  `env.remove(wal_file(tablet))` call) is the teardown-time counterpart: a
  released/reclaimed tablet's bytes are dropped from `group_tails` and the
  file rewritten without them at once, rather than waiting on some other
  still-hosted tablet's next ordinary compaction.
- **Crash safety.** Proven directly (`animus-control::shared_wal`'s own
  unit tests, plus `animus-cp-data`'s `tests/sharedwal_fault_corpus.rs`
  under real `RaftKvNode` groups): a crash mid-append, mid-sync, or
  mid-rewrite yields, for every tablet, exactly its own last **durably
  written** tail. This rests on two properties that were already true and
  needed no change: `PersistedState::decode_tagged`'s per-record CRC32 +
  torn-tail tolerance (issue #495) already drops a torn/corrupted trailing
  record — and everything physically after it — rather than corrupting
  recovery; and a torn/corrupted region can only ever be the file's
  physical TAIL (append-only writes, `Disk::replace`'s own atomic swap), so
  "stop the whole file at the first bad line, not per-tablet" (`decode_
  tagged`'s own documented conservative choice) is safe by construction
  here — every record physically before the tear, for every tablet, is
  already fully valid. `decode_tagged`'s doc previously read "acceptable
  since this shared-WAL path is currently unwired" — that sentence is now
  stale and has been corrected in place; the residual it flagged was never
  a real hazard, for the reason just given, not something this PR had to
  close.
- **Flag shape.** `--shared-wal` (bare boolean CLI flag, no `--no-shared-
  wal` opt-out yet — meaningful only once PR 3 flips the default) /
  `cluster_settings.shared_wal: Option<bool>`, threaded through the
  identical `--config FILE --node I` and `--cluster N` entry points (and
  no others yet) `--heartbeat-batch` reaches, via `host::Reconciler::
  enable_shared_wal(Arc<SharedWal<..>>)` — mirroring `enable_heartbeat_
  batching`'s "opt in once, applies to every group hosted from here on"
  shape, except the `SharedWal::open` call itself (an `async` node-start
  step) has to happen one layer up, in `BoundNode::start_with_growth`,
  since a pure setter can't `.await`.
- **Layout-mismatch handling — a LOUD refusal, not a silent reset
  (corrected 2026-09-06).** The shared file (`animus_cp_data::SHARED_WAL =
  "raftkv.wal.shared"`) and the per-group files (`wal_file(stream) =
  "raftkv.wal.{stream}"`) live at disjoint names on the same data
  directory — a flag flip against an existing data dir written under the
  OTHER layout does not corrupt any *file*, since the newly-selected
  layout's own name simply hasn't been written before. But an early draft
  of this PR judged that a **silent** thing to allow, and it is not: the
  layout not selected is simply never read, so a node started with the
  flag flipped would recover every hosted tablet's `RaftCore` (log, term,
  `voted_for`) as if it had never persisted anything on this node before —
  a genuine **data-loss and Raft-safety hazard**, not a convenience, and
  one PR 3's default flip would have inflicted on every existing cluster
  the moment it landed. Fixed before this PR shipped: `animus_cp_data::
  host::check_wal_layout(env, shared_wal) -> io::Result<()>` runs once, at
  node start, **before** `SharedWal::open` and before any tablet's own
  `drive()` recovery — a directory listing only (`Env::list()`, never a
  file open) — and returns a hard `io::Error` naming both layouts and the
  flag whenever `shared_wal` disagrees with what's already on disk
  (per-group files present but `--shared-wal` is on; the shared file
  present but it's off). `BoundNode::start_with_growth` calls it and
  propagates the error with `?`, so it surfaces as an ordinary startup
  failure — `main.rs` prints it and exits non-zero, the identical path
  every other startup error already takes. A data directory holding
  NEITHER layout yet (a genuinely fresh `--dir`) always passes. See
  `animus-cp-data/CLAUDE.md`'s own `check_wal_layout` entry for the exact
  error text and the unit tests (`host::wal_layout_tests`, both
  directions) proving it, and `crates/animusd/tests/shared_wal_e2e.rs::
  a_restart_with_shared_wal_flipped_refuses_to_start` for the real-`ProdEnv`
  proof through the actual `run_node_with_cluster_settings` startup
  surface. This closes the gap the original text below left open — kept
  struck through rather than deleted, since the reasoning it gives for
  *why a flag flip can't corrupt a file* is still correct and still worth
  having on record: ~~No additional loud-failure check was added beyond
  this — the reset is silent-but-safe (no corruption, no crash, no mixed
  layout), which this PR judged sufficient for an internal, off-by-default
  tuning flag; a future PR could add an explicit marker file + startup
  check if the silent-reset behavior ever proves surprising in
  practice.~~ There is still no migration path between the two layouts
  (this repo's standing no-back-compat stance, root `CLAUDE.md`) — an
  operator who genuinely wants to switch layouts still needs a fresh
  cluster/data directory; the difference this fix makes is that doing so
  *by accident* now fails loudly at startup instead of silently discarding
  state.
- **What PR 3 flips.** Only the default: `shared_wal.unwrap_or(false)`
  becomes `shared_wal.unwrap_or(true)` at the same two resolution points
  `DEFAULT_HEARTBEAT_BATCH` lives at (`main.rs`), plus (per that cutover's
  own precedent) a `--no-shared-wal` opt-out flag and a real-thread
  `ProdEnv` liveness proof mirroring `heartbeat_batch_liveness.rs`. The
  mechanism itself (this PR) does not change.

**Corpus**: `crates/animus-cp-data/tests/sharedwal_fault_corpus.rs`, depth
knob `ANIMUS_SHAREDWAL_SEEDS` (default 1) — cross-group fsync coalescing
under a real burst (cell a), crash-mid-round durability/isolation with
`torn_tail_on_crash`/`corrupt_on_crash` armed (cell b), `forget`'s GC
reclaim without disturbing a sibling (cell c), and a quiet group's write
surviving a churning sibling's real `COMPACT_THRESHOLD`-crossing compaction
(cell d). See that file's own module doc for what it deliberately does NOT
cover (a shared-WAL-aware `raftkv_linearizable` `LeaderKill`/`FollowerKill`
harness — a larger, separately-scoped harness change) and
`crates/animusd/tests/shared_wal_e2e.rs` for the real-`ProdEnv`/real-disk
complement (two tables, a genuine process restart, `--shared-wal` on).

## Amendment (2026-09-06, C-05 PR 3 — cutover, as built)

`--shared-wal`/`cluster_settings.shared_wal` now defaults **ON** —
`main::DEFAULT_SHARED_WAL = true` at the same two resolution points
`DEFAULT_HEARTBEAT_BATCH` lives at (`main.rs`'s `run_in_process_cluster`
call and `run_single`), exactly the flip the PR 2 amendment's own "What PR
3 flips" paragraph predicted. `--no-shared-wal` is the new opt-out (a bare
boolean, mirroring `--heartbeat-batch`/`--no-heartbeat-batch`'s shape
exactly); `--shared-wal` is kept as a no-op restating the default, for
explicit/scripted invocations. **The mechanism itself (PR 2) is
unchanged** — this PR is the default flip plus its two required proofs,
the identical two-step shape C-02 (heartbeat batching) used.

- **Layout-mismatch messages now read the way round the default runs.**
  Both `animus_cp_data::host::check_wal_layout` error strings were
  rewritten (not just their surrounding doc comments) since "omit the
  flag" is no longer a valid fix once the flag defaults to `true`: the
  `shared_wal: true` (now-default) branch — hit by any pre-cutover
  per-group data directory on a plain upgrade with no flag passed at all —
  now says to pass `--no-shared-wal`; the `shared_wal: false`
  (`--no-shared-wal` passed) branch against an existing shared-layout
  directory now says to *omit* `--no-shared-wal`, never to pass
  `--shared-wal` (a no-op restating the value that's already the default).
  Both directions still carry "Refusing to start" and "persisted Raft
  state" verbatim — the substrings `host::wal_layout_tests` and
  `crates/animusd/tests/shared_wal_e2e.rs::
  a_restart_with_shared_wal_flipped_refuses_to_start` already asserted,
  both of which pass unmodified; each `wal_layout_tests` case additionally
  now asserts the exact new opt-out/omit phrasing.
- **`animusd data --config`'s gap, closed.** PR 2 wired only the two
  primary production entry points (`--config/--node` and `--cluster N`)
  all the way through, leaving `cluster_settings.shared_wal` silently
  ignored on a data-only node — a real gap, not the documented
  `--cluster-control`/`--cluster-data`/`join`/`data --seed` scope cut PR
  2's own amendment named (those stay hardcoded `false`, unaffected, the
  identical gap `--heartbeat-batch` has at the same call sites). Since
  `heartbeat_batch` already reached this path and PR 3 is the moment the
  default flips everywhere else, leaving a split deployment's data-only
  nodes permanently on the per-group layout with no way to opt in would
  have been a new, worse inconsistency at the exact moment this cutover
  ships — so `BoundDataNode::start_data_with_growth` gained the matching
  trailing `shared_wal: bool` parameter and its own
  `check_wal_layout`/`SharedWal::open`/`enable_shared_wal` call sequence,
  byte-identical in shape to `BoundNode::start_with_growth`'s combined-mode
  one; `run_node_data_with_cluster_settings` and `main.rs`'s
  `run_data_config` now thread `settings.shared_wal.unwrap_or
  (DEFAULT_SHARED_WAL)` through it. Every other documented gap
  (`--cluster-control`/`--cluster-data`, `join`/`data --seed`, and every
  narrower test/convenience wrapper) is unaffected — see `crates/animusd/
  CLAUDE.md`'s "Shared WAL" section for the current, complete enumeration.
- **Real-thread `ProdEnv` liveness proof under sustained load**:
  `crates/animusd/tests/shared_wal_liveness.rs`, mirroring
  `heartbeat_batch_liveness.rs`'s own role. A 3-node cluster hosting four
  tablet groups (one per table) with the shared WAL on by default (no flag
  passed) takes continuous concurrent writes across all four tables for a
  fixed wall interval — every acked write immediately verified readable
  over the linearizable `ConsistentRead`-equivalent path — long enough for
  each table to cross `COMPACT_THRESHOLD` (64) and force a real
  `SharedWal::compact_group` rewrite mid-load, not just append coalescing;
  `GET /admin/metrics`'s `cp_shared_wal_gc_rewrites` counter (summed across
  every node) is then polled to a nonzero value, direct proof the shared
  WAL's segment GC actually ran under real concurrent load without
  stalling the writer tasks' own timeouts. The busiest leader node (most
  groups led) is then killed mid-test and every group it led re-elects
  within a bounded budget, with reads/writes continuing via the survivors.
  Run 5x locally with no flake.
- **Full-gate soak**: `cargo fmt --all --check`, `cargo clippy --workspace
  --all-targets --all-features -- -D warnings`, `cargo build --workspace
  --all-targets` (this time including `animusd`, closing the one residual
  PR 2's own soak left — it had run with `--exclude animusd`), and `cargo
  test --workspace` — the whole point of the cutover: every existing test
  in the workspace now runs over the shared-WAL layout by default, so a
  green `cargo test --workspace` here is itself the strongest evidence
  that no other test anywhere silently assumed the per-group layout.
  `ANIMUS_SHAREDWAL_SEEDS=20` against `sharedwal_fault_corpus.rs`, plus
  `cargo deny check`, all green.
- **The raftkv nemesis corpus gap named by PR 2's own module doc is still
  open, deliberately** — `crates/animus-test/tests/raftkv_linearizable.rs`
  builds its groups via the plain `RaftKvNode::start` constructor (used at
  both its initial-bring-up and its crash-recovery-restart call sites),
  which has no `shared_wal` parameter at all — a structurally different,
  narrower constructor from the `start_hosted_*_with_shared_wal` family
  `sharedwal_fault_corpus.rs` itself uses. Reaching the shared-WAL path
  from that harness would mean threading one `Arc<SharedWal<..>>` through
  every simulated node's own group set, reworking its `LeaderKill`/
  `FollowerKill` nemesis handling and its crash-recovery restart path
  (`RaftKvNode::start` → `SharedWal::open`+`recovered_state`) — the same
  "larger, separately-scoped harness change" PR 2's own module doc already
  declined to do, unchanged by the cutover. `sharedwal_fault_corpus.rs`'s
  own cells (a)-(d) exercise the identical `persist_wal`/
  `apply_and_compact`/`host::Reconciler::forget` code paths that harness's
  groups would call if it were extended, and `shared_wal_liveness.rs`
  above proves the real-thread, real-leader-kill case a plain `SimEnv`
  corpus structurally cannot. This gap is not sized for a future PR here —
  named honestly rather than silently left implicit.

**C-05 is now complete** (all three PRs landed 2026-09-06: the `ProdEnv`
benchmark, the flag-gated wiring, and this cutover).

## Amendment (2026-09-07, issue #676 — `join`/`data --seed`/`--cluster-control`+`--cluster-data` reach)

C-05 PR 3's cutover flipped `--shared-wal`'s default ON for `--config`/
`--node`, `--cluster N`, and `animusd data --config`, but left `join`,
`data --seed`, and `--cluster-control`+`--cluster-data` hardcoded to the
per-group layout regardless of any flag — a real gap, not a scope cut PR 3
called out deliberately (its own module doc named it as unaffected, not as
intentionally out of scope). Since the default flipped ON everywhere else,
this meant a seed-joined node's own on-disk layout silently diverged from
what an operator would reasonably expect — the exact surprise issue #676
opened against.

Closed by threading `shared_wal: bool` through both real growth paths and
the split-deployment dev path:

- **`join`/`data --seed`**: `animusd::run_node_join`/`run_node_data_join`
  keep their own original arity (every existing caller — this crate's own
  test suite included — keeps compiling unchanged) and now default
  internally to `DEFAULT_SHARED_WAL` instead of hardcoding `false`; a new
  widened sibling, `run_node_join_with_settings`/`run_node_data_join_
  with_settings`, takes an explicit `shared_wal: bool` (plus
  `quiesce_after`/`heartbeat_batch`/`segment_store_config`/
  `backup_store_config`) for `main.rs`'s own `join`/`data --seed` CLI
  dispatch, which gained `--shared-wal`/`--no-shared-wal` (and the sibling
  flags) for the first time. Neither path has a config file to
  conflict-check a CLI flag against, so there is no "one way, not both"
  contract to add here — every flag simply sets its value directly, the
  same shape `--tls-*`/`--encryption-key` already have on these paths.
- **`--cluster-control`+`--cluster-data`**: `start_split_cluster_with_
  growth` gained a trailing `shared_wal: bool` (alongside `quiesce_after`/
  `heartbeat_batch`), threaded from `run_in_process_split_cluster`'s own
  CLI dispatch the identical `DEFAULT_SHARED_WAL`-when-omitted way. Its
  narrower sibling, `start_split_cluster_with_orphan_sweep_after` (used
  directly by `tests/cluster_split.rs`), keeps hardcoding the pre-#676
  off/per-group values — the same "narrower test wrapper stays at its own
  original semantics" convention this ADR's own PR 2/3 amendments already
  established for `start_with_streams` and friends.

Regression: `crates/animusd/tests/join_data_seed_settings_reach.rs`'s
`join_defaults_to_shared_wal_matching_a_bare_config_node` (the core proof —
a bare `animusd join`, no flags at all, now writes the SAME shared-WAL
layout a bare `--config`/`--node` does, verified the identical
restart-with-the-flag-flipped-is-refused technique `tests/shared_wal_e2e.rs`
uses) and `join_no_shared_wal_writes_the_per_group_layout` (the explicit
opt-out); `tests/split_cluster.rs::cluster_control_data_threads_quiesce_
after_to_admin_config` proves the identical wiring for `--quiesce-after` on
the split-deployment dev path (the cheapest observable for that knob;
`--shared-wal`/`--heartbeat-batch` have no `/admin/config` field to observe
directly there, unchanged by this amendment — see this ADR's own "No
`/admin/config` field yet" note).

`--segment-store`/`--backup-store` (+ `--s3-credentials`/
`--allow-insecure-s3`) also now reach `join`/`data --seed` as part of this
same change (S-04 PR 2's own gap on these two entry points) —
`--cluster-control`+`--cluster-data` and `data --config` remain a
documented gap for those two knobs specifically (no CLI/`cluster_settings`
route to either store on those two paths at all yet). `animusd control`'s
`--encryption-key` reach (ADR 0069, not this ADR's own knob) is covered by
that ADR's own 2026-09-07 amendment.

See `crates/animusd/CLAUDE.md`'s "Shared WAL" section for the current,
complete per-entry-point enumeration this amendment updates.
