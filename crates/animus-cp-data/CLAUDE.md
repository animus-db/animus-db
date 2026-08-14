# CLAUDE.md — animus-cp-data

This file provides guidance to Claude Code (claude.ai/code) when working in this
crate.

## Purpose

The **leaderful, strongly-consistent (CP) data plane** (ADR 0016, ADR 0017):
each tablet is its own Raft group with a single leader serving **linearizable**
single-tablet reads/writes, durable on a real `StorageEngine`. For v1 (ADR
0019) this is the *only* data plane — the original leaderless AP plane
(`animus-data`) is deferred and its crate deleted. The control plane
(`animus-control`) remains the metadata authority (tablet map, placement,
failure detection).

It instantiates `animus-control`'s generic, sync, I/O-free `RaftCore<C, S>`
(ADR 0009) with `C = KvCommand` and a **`DRIVER_APPLIED` state machine**
(`KvState`, a unit placeholder): the core agrees the *order* of commands but
does **not** apply them in-core (a `StorageEngine` apply is async I/O the sync
core can't do). The core buffers committed-and-durable commands as effects;
this crate's **async driver** drains them (`RaftCore::drain_apply`) and applies
to the engine — the same sync-core/async-driver split as `animus-consensus`'s
`AccordCore`.

## Entry points

- **`lib.rs`** — `RaftKvNode<E, S>` (the running tablet-group node) and its
  command/state types (`KvCommand`, `KvState`), `StorageScope`, the fenced
  commands, ReadIndex + CAS, the consensus-loop/apply-task split, and
  `ProposeSignal`. See the API bullets below.
- **`host.rs`** — the per-node tablet-host reconciler (ADR 0031): the pure
  `plan()` decision, `Reconciler` executor, `MetadataView`/`TabletFacts`/
  `LocalState`, and the `HostAction` set (incl. `Absorb`/`WidenScope`). 34
  unit tests. See "The host module".
- **`cluster_segment_store.rs`** (ADR 0043 §A7b, F5) — `ClusterSegmentStore<E,
  S>`: the **default** `SegmentStore` for the stream-shard subsystem — K-way
  replication of an immutable segment across `K` nodes' own local `S`-backed
  directories (each backed by `FsSegmentStore` in production,
  `SimSegmentStore` under sim), over `E`'s `Network` seam. `put_replicated`
  returns `Ok` only once every chosen target has durably written the object
  (all-or-error; a partial failure leaves harmless, idempotent-overwrite-
  safe orphans on the replicas that *did* succeed); `get_from`/`delete_from`
  take a catalog row's own recorded replica set (the load-bearing read/
  reclaim path a later PR's sealer/janitor will call); the trait's own
  `put`/`get`/`delete`/`list` are thinner, contract/testing-only paths (`get`/
  `delete` fall back to the *current* `PlacementView` candidates rather than
  a specific recorded set; `list` is local-only). One serving task per node
  (`ClusterSegmentStore::start`, spawned via `env.spawn_task`) is the single
  consumer of the reserved `SEGMENT_STREAM` (`u64::MAX`, ADR 0026 — chosen far
  outside any `TabletId`'s realistic range) — request and reply variants of
  its own `serde_json`'d `SegmentWire` enum share that one stream/inbox,
  correlated by a `req_id` a caller's `env.sleep`-based poll loop watches (see
  "What's non-obvious" and `docs/engineering-lessons.md`'s Testing section for
  why this isn't a `tokio::sync::oneshot`). `PlacementView` (implemented by
  `StaticPlacementView` for tests; `animusd`'s `Metadata`-mirror-backed
  wiring is a later PR) hands back the current candidate node set;
  `choose_targets` feeds it straight into `animus_placement::select_replicas`
  with a plain, label-blind `PlacementPolicy::simple` — `K = min(default_k,
  candidates.len())`, so a single-node cluster degrades to `K = 1` instead of
  refusing to serve. Not yet wired into `animusd` (that, plus the
  `SealStreamShard` catalog recording the chosen replica set, is a later PR);
  today it is tested standalone against `SimSegmentStore`.
- **`codec.rs`** — the crate's compact binary wire/image codec (ADR 0017 A.2):
  length-prefixed framing (like the storage manifest codec), magic/version
  checked. Carries `KvWire` messages and engine images; `serde_json`'s
  decimal-array `Vec<u8>` rendering cost ~3–4x. Decode failures are loud (a
  logged `Err` before the message is dropped). The Raft WAL keeps the shared
  control-plane serde_json `PersistedState` format.
- **`hlc.rs`** (ADR 0018 §2) — a pure, I/O-free Hybrid Logical Clock:
  `HlcTimestamp { wall_ms, logical }` and the per-node `Hlc` (`mint`/
  `witness`, both take the caller-sampled `Nanos` — `Hlc` never touches an
  `Env` or the wall clock itself). `pack`/`unpack` encode a timestamp as the
  storage-engine `u64` MVCC version directly (`(wall_ms << 20) | logical`,
  no node-id bits — settled over `animus-consensus`'s `(logical, node)`
  scheme because a string `NodeId` can't bit-pack); the 20-bit
  `LOGICAL_BITS` budget is hard-`assert!`-checked in `pack`, never
  `debug_assert!` (a silent overflow would silently collapse two distinct
  timestamps to one version). See the Key invariants section for how this
  is wired into the apply path and the witnessing chain.
- **`cursor.rs`** (ADR 0042/0043, `KIND_CURSOR = 0x04`) — consumer cursor
  rows: the per-tablet, per-consumer HLC watermark the DynamoDB Streams
  change-log lifecycle rework rests on (ADR 0042 §7/§8). `cursor_key(range_start,
  consumer) -> Vec<u8>` builds `token(8 bytes, this tablet's own live
  `range.start` truncated/zero-padded) || [0x00, CURSOR_TAG] ||
  consumer.as_bytes()` — the identical `token || [0x00, TAG] || id` shape
  `txn.rs`'s `record_key` already establishes, with `CURSOR_TAG = 0x03`
  taking the next tag value after `txn.rs`'s own `RECORD_TAG = 0x02`;
  `parse_cursor_key` is the fixed-offset dual, used by the min-over-rows
  scan (below). `encode_watermark`/`decode_watermark` pack/unpack a
  `HlcTimestamp` as the row's 8-byte-BE value, reusing `hlc::pack`/`unpack`.
  Unlike a txn record, a cursor row lives in its **own** kind scope, so it
  can never alias a real client key regardless of byte content — the
  module's escape-disjointness proof (mirroring `txn.rs`'s) is stated
  anyway, since it is what makes the *parser* unambiguous, not what keeps
  cursor rows out of client data. A documented residual gap (mirroring
  `txn.rs`'s own "split_key not token-aligned" note): the module doc spells
  out a pathological `Binary`-key edge case where the truncated-token key
  is not proven to stay strictly below a tablet's own `range.end`; left for
  a future corpus to stress. `token_of(range_start) -> [u8; TOKEN_BYTES]` is
  `cursor_key`'s own truncate/zero-pad step, split out (PR A2) so a caller
  that already has a *parsed* row's own token can compute *this* tablet's
  own token to compare against, without rebuilding a whole key — the
  `animusd` trim janitor's merge-residue cleanup (below) is exactly this
  caller. `RaftKvNode::cursor_watermark`/`cursor_rows`/`cursor_min_watermark`
  (in `lib.rs`, next to `local_get_kind`/`local_scan_kind`) are the read-side
  accessors — `cursor_min_watermark` implements ADR 0042 §7's min-over-rows
  rule directly (the minimum watermark across every row of a tag in this
  tablet's own, possibly merge-widened, `KIND_CURSOR` scope).
  `cursor_rows_with_token` (PR A2) is `cursor_rows`'s token-keeping sibling —
  `cursor_rows` itself is now a thin wrapper dropping the token, since none
  of *its* callers need it, but the trim janitor's merge-residue cleanup
  does (telling "this tablet's own row" from "a still-physically-present
  absorbed sibling's row" needs the token, not just the tag). Write-side is
  deliberately just `put_kind_batch(KIND_CURSOR, ..)` — no bespoke propose
  method — since the existing `KvCommand::KindBatch` primitive already
  covers it; the `animusd` GSI drain (`index_drain.rs`, cursor-based since PR
  A2 — see that crate's own `CLAUDE.md`) is what actually calls it in
  production today — the only production consumer. Round 3 has no separate
  stream copier or `"copier"` cursor row: the eventual sealer reads a
  table's own `KIND_CHANGE` change log directly (round-3 streams plan §A1).
- **`seal.rs`** (ADR 0018 §2 amendment) — the **range seal**: the structural
  replacement for the retired `version_floor` cross-group-LWW fix.
  `KvCommand::Seal { range, ts }` is proposed by a range-handoff source (a
  split's `NarrowScope`, or a merge's `Absorb`) through its own Raft log; a
  later-ordered mutating entry for a key inside a sealed range is rejected
  at apply, checked against a per-group in-memory set rebuilt at group
  start from a durable **engine marker key** (deliberately from the
  engine, not log replay, since compaction can truncate a `Seal` entry out
  of the log long before its rejection duty is done). The marker's key
  lives under `animus_control::syskv::RESERVED_NAMESPACE` — engine-global,
  outside every `StorageScope` — see the module's own doc for the
  key-disjointness proof.
- **`segment.rs`** (ADR 0042/0043) — the stream-shard **segment codec**: a
  sealed shard's `SegmentStore` object format, pure and I/O-free (magic +
  version header, `SegmentHeader` + `Vec<SegmentRecord>` body,
  length-prefixed framing mirroring `codec.rs`'s own style but a separate
  self-contained module). `encode`/`decode` — `encode` always derives the
  wire `count` from `records.len()` (never trusts a caller-supplied
  placeholder), `decode` validates magic/version (an unrecognized version
  is a loud, named `Err`), every length-prefixed field's framing, the
  stored `shard_id` against `shard_id(tablet, epoch)` (a mismatch is
  corruption), and that the body holds **exactly** the declared record
  count with no trailing bytes. `shard_id(tablet, epoch)` =
  `shardId-<tablet>-<epoch>` (ADR 0042 §2); `segment_id(table, label,
  tablet, epoch)` = `{table}/{label}/{tablet}/{epoch}` (ADR 0043 §A3/§A7,
  matching `FsSegmentStore`'s own path-mapping/`ClusterSegmentStore`'s id
  shape byte-for-byte). **The superset-slice rule (ADR 0042 §10)**:
  `slice_to_hlc_range(records, (start_exclusive, end_inclusive))` keeps
  exactly the records inside the catalog row's own committed range,
  dropping a deposed leader's late-`put` superset's extra tail (and,
  defensively, anything at or below the exclusive start); `decode_and_slice`
  composes decode-then-slice in one call so a reader (the `GetRecords`
  sealed-shard path) can't decode a segment and forget to slice it.
  `change_record` bytes are opaque to this crate throughout (ADR 0043's
  own layering rule) — only ever moved, never interpreted.
- **`ts_cache.rs`** (ADR 0018 §2) — the per-tablet **read-timestamp cache**
  (`TsCache`): leader-local, in-memory, best-effort write-conflict push. A
  two-generation rotating map; every served read bumps the span it read at
  its serve ts, and a propose-time write is pushed above any overlapping
  bump before it is embedded in a command. Losing this cache is always
  **safe** — see the Key invariants entry for why the real safety net is
  the logged read ceiling, not this cache.
- **`ceiling.rs`** (ADR 0018 §2) — the **logged read ceiling**'s durable
  marker: a single per-tablet engine-global key, same disjointness proof
  as `seal.rs`'s marker, always overwritten with the newest
  `KvCommand::ReadCeiling` value so `storage.latest_version()` durably
  reflects it — the group-start witness then re-derives a floor covering
  it on any future restart even after the log entry is compacted away. See
  the Key invariants entry for the full mechanism.
- **`txn.rs`** (ADR 0018 §2) — the transaction machinery: the 1-byte-tagged
  value **envelope** (`Envelope::Committed`/`Intent`) every apply-path
  write wraps its value in, and the transaction **record** (`TxnId`,
  `TxnStatus`, `TxnRecord`) that is the atomic commit point. A txn record
  is an ordinary **in-scope logical key** of the anchor tablet (unlike
  `seal.rs`/`ceiling.rs`'s engine-global markers, so it replicates/
  snapshots/splits like real data); `record_key` derives it from the
  anchor write's own partition token plus a lead-byte pair proved disjoint
  from every real key sharing that token — see the module's own doc for
  the full proof (and `docs/engineering-lessons.md`'s Code-patterns entry
  for the general technique). `Envelope::Intent::record_table` names the
  anchor's table (a record key alone doesn't identify which table's ring
  owns it, since tables' rings are independent). `TxnRecord::intent_spans:
  Vec<(String, KeyRange)>` names every key any participant ever staged,
  table name attached. See the Key invariants section for the full design.

### lib.rs API

`RaftKvNode<E, S>` is the running tablet-group node: `start`/`start_scoped`/
`start_hosted` (an explicit `StorageScope`); `put`/`put_batch`/`delete`
propose via Raft; `is_leader`; `linearizable_get` (ReadIndex); `local_get`
(a replica's raw, non-linearizable engine read, test/observability only).
`StorageScope` (ADR 0026/0028) confines a node's physical key access within
a possibly node-shared `StorageEngine`: a `prefix` (the owning table's
identity) plus a live-narrowable `range` (an `Arc<Mutex<KeyRange>>`, so a
split narrows it without restarting the group); `physical(key)` maps a
logical key to its on-engine key. `has_data(&storage)` lets `animusd` tell
"re-forming after a restart" from "brand-new spare joining"; `physical_bounds()`
computes a genuinely bounded physical upper bound via the prefix-upper-bound
trick, so a periodic byte estimate never degrades into a whole-engine scan.

**A group owns a scope *set*, not one scope (ADR 0041 §3).** `with_kind(kind)`
derives a sibling scope for one row kind — `KIND_BASE`/`KIND_LSI`/
`KIND_CHANGE`/`KIND_FOOTPRINT`/`KIND_CURSOR` (ADR 0042/0043 — consumer
cursor rows, see `cursor.rs` above; `KIND_STREAM`/`KIND_STREAM_META` land
with a later PR), enumerated by `ALL_KINDS` (five entries as of this PR,
codec `VERSION` 13) — over the *same*
`Arc<Mutex<KeyRange>>`, so one `narrow`/`widen` moves every kind at once.
**Why kinds are scopes, not a discriminator byte in the key**: a tablet is a
`[start, end)` range over *token* space, so a kind above the token would
break that contiguity; and `txn_stage` asserts a logical key leads with the
ADR 0022 token, deriving every `TxnRecord::intent_span` from it, so a kind
byte in the logical key would have forced a rewrite of every span/fence/
record-key/seal-marker in the ADR 0018 2PC machinery. `start_*` takes the
tablet's parent scope and derives `kind_scopes` from it; `self.scope` stays
bound to the base kind (why pre-existing read/write/fence/txn-record code
needed no edit, and why `approx_bytes` measures only base data). Only
genuinely whole-tablet operations iterate the set: `engine_image`/
`install_engine_image` (every `ImageEntry` gained a leading kind byte) and
`erase_scope` (drop-table GC must reclaim LSI rows/change log/footprints
too). **`StorageScope::whole()` is no longer an identity transform** — its
base-kind scope prefixes one `KIND_BASE` byte, so *any* group's physical key
is `prefix || kind || logical`. **Anything reading a group's bytes straight
off the engine must go through `RaftKvNode::physical_key(kind, key)` rather
than assembling `prefix || key` itself** — hard-coding the layout was
correct only while a group had exactly one scope, and four tests broke on
exactly that assumption.

`KvCommand::KindBatch` (codec tag 12) is the multi-kind atomic batch:
`put_kind_batch`/`put_kind_batch_fenced` commit `(kind, logical key,
Option<value>)` writes spanning several row-kind scopes as **one** Raft log
entry — the primitive materialized secondary indexes rest on (an LSI is
strongly consistent because its rows commit in the same entry as the base
row). One `fence` gates the whole entry. The optional `change_log:
Option<(prefix, record)>` completes its key at **apply** as `prefix ||
hlc::pack(ts)` — the proposer deliberately cannot supply that suffix, since
`ts` is minted inside `propose_ordered` and is the only timestamp that
agrees with the entry's log position. `local_get_kind`/`local_scan_kind` are
the read side (an LSI `Query`/`Scan`, the GSI drain's change-record sweep):
simpler than the base reads since a non-base scope only ever holds
**committed** values. **`end: Option<&[u8]>`** mirrors `local_scan`/
`linearizable_scan`'s unbounded-above handling for the base scope — **when
`end` is `None`, the bound is derived from this kind scope's own
`physical_bounds()`, never the caller's**, because no finite byte string can
bound an LSI row's keyspace in general (a trailing base-sort-key segment has
no length limit); the bound still comes from the kind scope's own prefix,
never `entries()`, so it can only ever read this one scope on this one
tablet.

Fenced commands (`put_fenced`/`delete_fenced`/`cas_fenced`/
`put_batch_fenced`) carry a `fence: KeyRange` *inside the proposed
command*, stamped by the leader from its own `StorageScope.range` (see Key
invariants for why this is load-bearing); `scope_range()` is the read-side
snapshot used both to reject a key before proposing and to stamp the
fence. `approx_bytes()` is the per-tablet cheap byte estimate
`animusd::auto_split_loop` gates on. Batch put (`KvCommand::Batch` +
`put_batch`) commits N keys as one Raft log entry. Linearizable CAS
(`cas`/`cas_result`/`compare_and_swap`) is decided at apply time (see Key
invariants); a current value covered by a `Pending`/unresolved intent
deterministically fails the swap.

**Transactions** (ADR 0018 §2, mechanics proven in Key invariants below):
`txn_stage`/`txn_decide`/`txn_write` are the single-Raft-group (anchor-only)
convenience; `txn_stage_anchor`/`txn_stage_participant` are the general
multi-participant entry points; `txn_commit_at_least`/`txn_abort`/
`txn_resolve` decide and resolve; `txn_status_local`/`txn_record_view`/
`txn_verify_staged`/`txn_abort_orphan` are the recovery primitives a pusher
composes; `linearizable_get_served_fast`/`resolve_intent_given_status` are
the foreign-intent read path; `pending_txns()`/`unresolved_decided()`
expose the per-group `TxnTracker` snapshot. `KvCommand::TxnStage`'s
`conditions` (own-key byte-level OCC, `Cas`-shaped) feeds `StageOutcome`
(`Staged`/`ConditionFailed`/`IntentBlocked`/`Fenced`) — this crate speaks
bytes only; a richer caller (`animusd::dynamo::run_transact`) compiles its
own expression down to this exact byte-equality shape.

Admin/debug accessors: `role`/`term`/`commit_index`/`last_applied`/
`durable_index`/`snapshot_index`/`log_len`/`storage()`.
**`engine_applied_index()`** is the confirm-by-index primitive linearizable
reads gate on, so a proposer confirms a specific `Accepted { index }`
applied instead of polling value equality. `read_at`/`scan_at` (MVCC
snapshot reads) take the same ReadIndex barrier then read at version `≤
hlc::pack(ts)`, refusing a `ts` not yet strictly below
`committed_ceiling()` (the write-push/ceiling invariant below). `KvWire`
wraps `RaftMsg` plus the ReadIndex read-barrier probes, driver-only.
Stream addressing (ADR 0026 Stage B): `start_hosted(.., stream)` addresses
a tablet's Raft traffic by `(node, stream)` instead of a distinct
`NodeId`/env per tablet, so every tablet a node hosts shares one env/port.

## Key invariants

State once here; cross-referenced from the sections below.

- **The packed HLC commit timestamp is the MVCC version (ADR 0018 §2
  amendment — replaces the retired `version_floor`-scaled Raft-index
  invariant).** Every mutating `KvCommand` variant carries a `ts:
  HlcTimestamp`, minted from the proposing leader's own per-group `Hlc` at
  propose time; apply stamps `hlc::pack(ts)` as the engine `version` at all
  four apply sites (`Put`/`Batch`/`Delete`/`Cas`) — never a Raft index — so
  per-key LWW reproduces cross-group HLC order and re-applying on recovery
  stays idempotent. This closes the cross-group-shared-engine hazard
  `version_floor` did (a fresh/widened group's own version sequence must
  never undercut a different group's), but by **witnessing** instead of a
  structural version-space separation:
  - **Witnessing** (`Hlc::witness`) folds a just-observed timestamp into a
    group's clock so its own future mints are guaranteed to exceed it, at
    four points: WAL recovery, every received `AppendEntries`
    (`witness_append_entries`, before `RaftCore::handle` — every entry,
    accepted or not, since a redundant witness is always safe), snapshot
    install, and group start (off the shared engine's own
    `latest_version()`, which alone covers a restart or a co-hosted
    sibling's already-present data).
  - **The range seal** (`seal.rs`) closes the one residual witnessing alone
    cannot: an in-flight write from a source-group leader that hasn't yet
    observed a split/merge, still in its own commit pipeline when the
    handoff happens. A source proposes `KvCommand::Seal { range, ts }`
    through its own log at handoff time (`host.rs`'s `Reconciler`, on a
    split's `NarrowScope` and a merge's `Absorb`); apply rejects any
    later-ordered mutating entry whose key falls in an already-sealed
    range, regardless of that entry's own `ts`, because within one group
    log order and HLC order coincide, so it is the **log position** that
    is authoritative. The reconciler gates a split child's `Host` / a
    merge survivor's `WidenScope` on observing the relevant seal marker
    locally first (`TabletFacts::parent_seal_observed`/`widen_seal_observed`,
    `Metadata::split_parents`/`absorbed_by` provenance) — see `host.rs`'s
    entry below and `seal.rs`'s module doc for the key-disjointness proof.
  - **Proposing (and, on the absorbed side, waiting out) the seal is a
    persistent condition, re-derived every tick — never a one-shot side
    effect of the tick that performs the local irreversible action it
    precedes.** A replica that narrows its scope while a follower must
    still propose the seal once later promoted to leader; an absorbed
    replica must wait for a locally-*committed* seal, never "nothing
    pending locally" alone (a quiescent replica satisfies that trivially
    before the seal has even been proposed, letting a fast follower tear
    down and strand the seal below quorum forever). `host.rs`'s
    `gather_facts` computes `TabletFacts::pending_seals` fresh every tick
    (via `seal_covers`), and `plan` turns each into `HostAction::
    ProposeSeal`, replanned until observed; `Reconciler::teardown`'s
    Absorb drain requires `seal_covers` locally before proceeding. This
    gate is self-supporting, not a deadlock: it keeps the quorum needed to
    commit the seal alive for as long as it takes; a genuinely quorum-dead
    group correctly stalls loudly instead of tearing down early. See ADR
    0018's §2 amendment and `docs/engineering-lessons.md` for the full
    story. Regression: `tests/reconciler_corpus.rs`'s
    `absorb_follower_waits_for_committed_seal_before_tearing_down`/
    `narrow_seal_survives_a_late_promotion_after_narrowing_as_a_follower`.
  - **A hard, non-`debug` assert** (`assert_ts_monotonic`, at apply) checks
    every applied entry's `ts` strictly exceeds the previous one this group
    applied — the load-bearing invariant the whole witnessing chain exists
    to guarantee; a failure means the chain itself is broken, not a
    recoverable condition. Regression: `tests/cross_group_lww.rs`
    (split/merge/seal-rejection/in-flight-race/clock-skew shapes) and
    `tests/witnessing.rs` (leader-change and restart-recovery
    monotonicity).
  - **`propose_ordered`: minting a proposal's `ts` and appending it to the
    Raft log must be one atomic step, not two** — found via `animusd`'s
    `self_heal.rs` panicking under real concurrent client load. Every
    mutating propose method now computes its `ts` **while holding the
    group's own `core` lock**, immediately followed by `core.propose(..)`
    in the same critical section; two proposers could otherwise mint
    monotonically (ts=A then ts=B) but race to append to the log in the
    *opposite* order, so apply would see a real decrease. **This is a
    `ProdEnv`-only bug, provably unreachable under `SimEnv`**: the original
    code had no `.await` point between minting and proposing, so only
    genuine OS-thread parallelism can interleave there. `propose_ordered`
    also floors every ts-producing path on `last_proposed_ts` (this
    leader's own last-*logged*, not just last-*applied*, ts, since the
    apply task can lag the consensus loop by design). A second, narrower
    bug: `next_ceiling_candidate`'s ratchet must never hand back
    `last_proposed_ts` *unmodified* as a candidate, only as a floor to
    strictly exceed, or a `ReadCeiling` can tie a write's exact ts. See
    `docs/engineering-lessons.md` for the diagnostic story. Regression:
    `tests/prod_concurrent_ts_monotonic.rs` — deliberately the one
    real-thread `ProdEnv` test in a crate whose other binaries are all
    `SimEnv`, since this race needs genuine thread parallelism to express.
- **CAS is decided at *apply* time, not propose time** — this is what makes it
  linearizable and contention-correct. `RaftCore` agrees only the order; `Cas`
  rides through as opaque data. Apply evaluates it in commit order against the
  key's *current committed* value and compares to `expected`; equal → merge
  at `index`, else no-op. Every replica applies the same order against the
  same state with no clock/RNG, so every replica makes the **identical**
  decision. Outcome is stashed in driver `CasResults` keyed by the log index.
- **`TxnStage`'s own-key conditions are decided at *apply* time too, the
  identical CAS-style discipline** — evaluated against each key's current
  committed value inside the same apply arm that decides the pre-existing
  fence/seal/foreign-intent gates, recording a `StageOutcome` per Raft log
  index (`StageOutcomes`, mirroring `CasResults`). **`wait_applied(index)
  .await == true` does NOT imply `stage_outcome(index)` is `Some`** — a
  snapshot install can advance `engine_applied` past `index` without this
  replica individually applying (hence recording an outcome for) that exact
  entry, since an install globs many commands together. `txn_stage_anchor`/
  `_participant` poll `stage_outcome` directly instead (`wait_stage_outcome`)
  — `None` on timeout, never a hard-`expect`ed fact that turns out not to be
  guaranteed. See `docs/engineering-lessons.md` for the general lesson.
- **The value envelope + transactions (`txn.rs`).** Every value the apply
  path merges into the engine is 1-byte-tagged: `0` = committed (raw value
  follows), `1` = an intent naming the staging `TxnId`, its record's
  logical key, and the staged value (`None` = a staged delete). Every read
  path unwraps it before a value reaches a caller: point reads resolve via
  `RaftKvNode::read_resolved` (a bounded retry while the covering
  transaction is `Pending` — `INTENT_WAIT_TIMEOUT`/`_POLL`); scans resolve
  via `resolve_scan_rows`, **non-blocking** — a still-`Pending` row is
  silently omitted. Resolution: `Committed{commit_ts}` at or before the
  read's own timestamp serves the staged value; `Aborted` (or a later
  `Committed`) serves the value the key held immediately before the
  intent, restored by rewinding to `get_at(key, intent_version - 1)` —
  **never** a tombstone, which would incorrectly shadow that older,
  still-live committed value.
  - **The txn record lives *inside* the anchor tablet's own `StorageScope`**
    — an ordinary in-scope logical key, not an engine-global marker like
    `seal.rs`/`ceiling.rs` — so it replicates/snapshots/splits like real
    data. Its key (`txn::record_key`) is `token(8 bytes) || [0x00, 0x02]
    || encode(txn_id)`; disjointness from every real key sharing that
    token is proved structurally from `animus_tablet::escape`'s own
    encoding rule, not a reserved-name convention — see `txn.rs`'s module
    doc. `txn::is_record_key` is what every scan path (and `has_data`)
    filters on so this internal key never leaks to a client.
  - **`erase_scope` deliberately does NOT go through `local_scan`** (which
    filters record keys and resolves values) — it uses `raw_scoped_keys`,
    since drop-table GC must physically erase everything this scope ever
    wrote (ordinary values, pending intents, and txn records alike).
  - **`TxnCommit`/`TxnAbort` carry no `fence`**, like `Seal`/`ReadCeiling`: a
    2PC decision must be durable and final regardless of any later range
    change, and never touches user data. A *conflicting* second decision
    on an already-decided record is a protocol-bug hard assert, mirroring
    `assert_ts_monotonic`'s doctrine.
  - **Writers push intents, never overwrite one**: `TxnStage`'s apply
    rejects (whole-or-nothing) any target key whose *current* value is an
    unresolved `Envelope::Intent` naming a **different** `txn_id`
    (same-txn re-staging is unaffected). This closes a durability hole: an
    overwritten-but-unresolved intent isn't erased (MVCC keeps every
    version), so if the overwriting transaction later aborts, its
    one-hop-back restore could land on the stale intent instead of a
    genuinely committed value — a chain a later correct resolve can never
    repair. Chasing the version chain back multiple hops on the read side
    was considered and rejected as unsound; rejecting the overwrite at
    apply time makes the corrupt chain structurally unrepresentable — see
    `KvCommand::TxnStage`'s own doc for why a plain `Put`/`Batch`/`Cas`
    over a foreign intent stays legal (analyzed safe: it serializes
    strictly after the intent's own transaction). **The proposer side
    matters just as much**: a stage call returning `Some(ts)` only ever
    means "this entry applied," never "my content landed" — so
    `animusd::ClientCtx::txn_prepare_pushing` verifies every staged key
    via `txn_verify_staged` after each attempt, retrying before reporting
    a client-facing conflict. Regression: `tests/txn_recovery.rs`'s
    `stage_over_a_foreign_pending_intent_no_ops_then_a_pushed_retry_succeeds`
    and `abort_restore_never_meets_another_transactions_intent`.
  - **A residual, documented gap**: a tablet split's `split_key` is an
    arbitrary existing row's key, not token-aligned, so a split racing an
    in-flight transaction could in principle separate a token's rows (and
    its txn record) across two sibling tablets. Deferred, per `txn.rs`'s
    module doc.
  - **Multi-participant 2PC**: a non-anchor participant's stage merges
    intents only (`record_key`/`record_table` name the **anchor's**
    record, a different tablet's — possibly a different table's —
    keyspace entirely, never checked against or written into this
    group's own fence/engine). `KvCommand::TxnResolve` carries an explicit
    `outcome: TxnOutcome` field — the decision travels with the command
    rather than being re-derived locally, since a non-anchor participant's
    tablet never holds a local copy to read. A reader that can't resolve
    an intent locally (`ResolveStep::Foreign`) is a distinct outcome from
    `Pending`; `linearizable_get_served_fast` hands routing info to
    `animusd`'s cross-tablet `TxnStatus` query. Regression:
    `tests/txn_multi.rs`.
  - **In-doubt recovery + decision semantics.** Recovery makes a **second,
    independent decider** on an already-decided record legal — a
    still-live coordinator's commit can race a recovery pusher's abort, or
    vice versa. `apply_and_compact`'s `TxnCommit`/`TxnAbort` arms let the
    **first**-applied decision win (log position is the ballot); any later
    conflicting proposal is a logged no-op, never a panic — including two
    `Committed` decisions at two genuinely **different** `commit_ts`
    values (`mint_at_least` mints a fresh ts every call, so a live
    coordinator and an independent post-grace recovery push can each
    legitimately conclude "commit" with different timestamps — reachable
    under nothing more exotic than an ordinary leader election). The
    *only* remaining hard assert is two genuinely **conflicting**
    decisions racing the same log position. `RaftKvNode::txn_abort` (the
    abort-only dual of `txn_commit_at_least`) lets a caller decide without
    resolving — every decider must re-read the record's actual status
    afterward, never assume its own proposal won.
  - **`TxnRecord::intent_spans`/`KvCommand::TxnStage.spans` is
    `Vec<(String, KeyRange)>`**, not a bare `Vec<KeyRange>` — a non-anchor
    stage never populated it at all in an earlier shape, leaving a record
    with zero visibility into other participants. `animusd`'s coordinator
    now hands the anchor's stage the complete cross-participant `(table,
    span)` list up front (`RaftKvNode::txn_stage_anchor`, the general
    entry point; `txn_stage` is a thin single-participant wrapper).
  - **Orphan records + the resurrection guard**: prepare stages every
    participant concurrently, so a participant's stage can succeed while
    the *anchor's* own `TxnStage` — which would create the record — never
    lands (the same fence/seal-miss gap a participant's stage already has).
    `KvCommand::TxnAbort`'s `orphan_created_ts: Option<HlcTimestamp>` means
    "no record exists at all; synthesize a fresh `Aborted` tombstone
    directly" (`RaftKvNode::txn_abort_orphan`) — an absent record can only
    ever decide abort (committing needs a participant list only the
    record would have provided). `apply_and_compact`'s `TxnStage` arm also
    checks, for `is_anchor: true` only, whether a **decided** record for
    this exact `txn_id` already exists; if so the entry no-ops, never
    resurrecting it to `Pending`. Regression: the in-crate
    `pr5_orphan_and_resurrection_tests` module (`lib.rs`) — needs
    `pub(crate)` access the public API can't express.
  - **`TxnTracker`** (per-group): `pending` (records this group anchors,
    still `Pending`) and `unresolved_decided` (decided but not yet locally
    resolved — deliberately approximate but still safe: a resolver that
    stops tracking slightly early never loses correctness, since a
    straggling remote intent is resolved on demand the moment any reader
    hits it). Rebuilt at group start via a bounded scope scan for
    `txn::is_record_key` markers, deliberately not log replay (mirroring
    `sealed`/`committed_ceiling`'s engine-marker-survives-compaction
    reasoning).
  - **Read-path push, scoped to the foreign-intent path** — `animusd`'s
    `cp_get_local_resolving` calls `ClientCtx::txn_recover` on a
    still-`Pending`/failed `TxnStatus` query instead of immediately
    reporting "retry." The **locally**-`Pending` case (this crate's own
    bounded `read_resolved` retry) and scans stay unchanged — the resolver
    loop eventually pushes those. Regression: `tests/txn_recovery.rs`;
    `animusd/tests/cp_txn.rs`'s coordinator-crash pair.
  Regression (whole txn suite): `tests/txn_single.rs` (commit/abort paths,
  tombstones, a pending read blocking then serving, crash/restart
  idempotency, a sealed-range stage rejected wholesale);
  `tests/snapshot_catchup.rs`'s `snapshot_catchup_carries_txn_records_and_intents`;
  `tests/prod_concurrent_ts_monotonic.rs`'s
  `concurrent_txn_writes_and_reads_never_violate_ts_monotonicity`.
- **`engine_applied` vs `last_applied`.** The two-task split (below) means the
  core's `last_applied` (a buffer cursor the consensus loop advances) *leads*
  the engine. Linearizable reads therefore gate on the separate
  **`engine_applied`** atomic the apply task advances after each merge —
  **never** `last_applied` (else a read could observe past the engine).
- **Durable-before-visible** (ADR 0009): effects are only drained for fsynced
  entries, and the engine write follows the WAL `fsync`.
- **Write-conflict push + the logged read ceiling — the serializability half
  of the MVCC design.** A write must never commit at a `ts ≤` a `ts` at
  which its keys were already served to a reader. Two layers, deliberately
  separate:
  - **`ts_cache.rs`'s `TsCache`** is leader-local, in-memory, best-effort —
    every served read bumps the span it read at its serve `ts`; every
    mutating propose (`mint_pushed`) checks its minted `ts` against the
    highest overlapping bump (plus the committed ceiling) and, if not
    strictly above, witnesses that floor and re-mints (one retry always
    suffices). Losing this cache is always **safe**: over-conservative
    pushes are still correct writes, just marginally later-timestamped.
  - **The logged read ceiling** (`ceiling.rs`, `KvCommand::ReadCeiling`) is
    the actual safety net a leader-local cache alone can't be, across a
    leader change: a leader may only serve a read at a `ts` strictly below
    the highest `ReadCeiling` its group has **committed and applied**, and
    proposes a fresh one (`Hlc::uncertainty_upper(serve_ts)`, amortizing to
    roughly one per `HLC_MAX_OFFSET`) when serving above the current one.
    Safety: a live leader change's new leader already witnessed the prior
    ceiling's `ts` via ordinary `AppendEntries` receipt **before it could
    ever campaign** (Raft leader completeness), so its own future mints —
    and every write it proposes — strictly exceed it. A durable **engine
    marker** closes the residual a purely in-memory design would leave: a
    read-only workload can compact a `ReadCeiling` entry out of the log
    with no interleaved write to raise `storage.latest_version()`, so the
    marker's own merge does that job directly. **Never disambiguate a
    ceiling candidate via `Hlc::witness`** — it would drag the proposing
    leader's own clock forward to a value deliberately `HLC_MAX_OFFSET` in
    the future, poisoning every ordinary mint right after and turning the
    intended O(1) amortized proposal rate into O(N) (a real regression a
    seed-driven test caught); `next_ceiling_candidate` is a **separate**
    CAS ratchet for exactly this reason. Regression: `tests/ts_cache.rs`,
    `tests/snapshot_reads.rs`.
- **Uncertainty-interval read restarts.** `RaftKvNode::read_at` restarts
  **once** at `Hlc::uncertainty_upper(ts)` when it observes no value at
  `ts` but a version exists in `(ts, uncertainty_upper(ts)]` — a bounded
  *liveness* cost (`Metric::CpUncertaintyRestarts`), never a correctness
  one: the restart only ever moves the serve timestamp later, so it can
  only pick up more committed data, never lose any. Not wired into
  `linearizable_get_served` (serves at "latest") or scans.
- **Fences are per-entry, decided at apply, and backed by a pre-propose
  check.** Every replica's apply checks a command's key(s) against the fence
  **embedded in the log entry**, never a locally-polled value — so two
  replicas at different points in observing a split's `Metadata` make the
  identical accept/reject decision. The embedded fence only covers the
  residual race between a caller's pre-propose `scope_range()` check and
  the entry's actual apply; the pre-propose reject is load-bearing, not
  redundant (see `animusd/CLAUDE.md` and the root `CLAUDE.md` entry on a
  safety mechanism with zero production callers).
- **An `Absorb` teardown DRAINS the committed log into the engine BEFORE
  halting, and the survivor's `WidenScope` is deferred until the absorb
  confirms.** The apply task exits on `shutdown()` at its next loop-top
  check **without** draining committed-but-unapplied entries, and teardown
  then deletes the group's Raft WAL — the only local copy. Harmless for
  `Release`/`Reclaim` (they erase the data anyway) but fatal for `Absorb`:
  the absorbed range is about to be served from this same engine through
  the survivor's widened scope, so an acked write still in the commit
  pipeline would silently never reach the engine. The drain
  (`ABSORB_DRAIN_TIMEOUT`) waits — while the driver is live — for commit to
  cover the full local log, engine-applied to cover that commit, **and**
  for this replica's own engine to locally observe a *committed*
  range-seal covering this tablet's scope — never proceeding on "nothing
  pending locally" alone, which a quiescent replica satisfies trivially
  before the seal has even been proposed. On timeout, the stuck-apply
  escape hatch (proceed with a loud warning) fires only when the seal is
  already locally observed and it's purely the engine-merge watermark
  lagging; a stuck seal *commit* retries next tick instead, logging
  loudly so a genuinely quorum-dead absorbed group is visible to
  operators rather than silently torn down. `plan`'s `absorbing` gate
  sequences drain-before-widen. ADR 0033 post-merge hardening — a 1-in-5
  `ProdEnv` flake in `animusd`'s `tablet_merge.rs` was a real, permanent
  false-"absent," caught only once a genuine multi-process split
  deployment exposed it. The read-side halves live in this crate's
  `RaftKvNode` + `animusd` — see the root `CLAUDE.md` and ADR 0033.

## The host module

**Wired into production (ADR 0031).** `animusd` used to scatter "which
tablets does this node host, and what should it do about each" across
several independent `ProdEnv` loops, each re-deriving its own slice of
`Metadata` and its own bookkeeping. `host::plan` unifies the **decision**
into one pure, synchronous function (mirroring this crate's own
sync-core/async-driver split — unit-tested directly); `host::Reconciler<E,
S>` is the **execute** half, also in this crate so it owns the whole
lifecycle's invariants and is directly `SimEnv`-testable.

- **`plan(view: &MetadataView, facts: &BTreeMap<TabletId, TabletFacts>,
  state: &LocalState, base_id: NodeId) -> (Vec<HostAction>, LocalState)`**.
  Pure and synchronous — no `Env`, clock, RNG, or I/O. `MetadataView` is a
  small owned projection (`tablets`, `down`, `merged`, `split_parent`/
  `absorbed_by`), deliberately *not* the whole `animus_control::Metadata`,
  keeping the crate decoupled from the control plane's state shape.
  `TabletFacts` bundles the impure per-tablet inputs the caller gathers
  first (`hosted`, `is_leader`, `config_excludes_me`, `scope_range`,
  `has_data`, `parent_seal_observed`/`widen_seal_observed`). `LocalState`
  is the pure mirror of `animusd`'s `minted` claim set + `pending_release`
  epoch dampener, threaded call to call.
- **`plan` never removes a tablet from `LocalState::hosted` on its own**
  when emitting a fallible teardown (`Reclaim`/`Release`) — real teardown
  is async and can time out. The caller calls
  `LocalState::confirm_torn_down` once its own teardown actually
  completes; until then the next `plan` re-plans the same action.
- **`Reconciler::tick(&mut self, view: &MetadataView)` is the whole
  per-tick contract**: gather `TabletFacts` from its own hosted nodes
  (`gather_facts`), call `plan` exactly once, then execute the returned
  actions **in the order `plan` emits**. The reconciler owns the hosted
  map, making it the single writer of "does this node host tablet T."
  `on_host`/`on_teardown` hooks let `animusd` mirror hosting changes into
  its `ClusterEdgeState` routing registry as a **read-only reaction**,
  never a second writer.
- **The caller still owns the trigger and the pre-recovery guard.**
  Deciding *when* to call `tick` (an event-driven `metadata_watch` wake +
  a periodic fallback) and the `last_applied() == 0` pre-recovery guard (a
  live control-plane `RaftNode` read this crate has no business taking)
  both stay in `animusd::tablet_host_reconciler_loop`.

### HostAction

**Emitted in this fixed order: `ProposeSeal` → `NarrowScope`/`WidenScope` →
`Host` → `Reconfigure` → `Release`/`Reclaim`/`Absorb`.** Briefly: `ProposeSeal`
(re-)proposes a still-owed range-seal (persistent, re-derived every tick, a
no-op unless leading); `NarrowScope`/`WidenScope` move an already-hosted
tablet's scope to match its current metadata range (provably one-directional
each — `is_subrange`/its widen dual); `Host` stands up a fresh/joining/
restarting tablet, deferred for a split child until its parent's range-seal
is locally observed; `Reconfigure` is one `reconfigure_step` toward the
desired replica set; `Release`/`Reclaim`/`Absorb` tear down a tablet moved
off, dropped, or merged away respectively.

**`Reclaim` vs `Absorb` cannot be told apart from `tablets` alone.** A hosted
tablet vanishing looks identical whether its table was dropped or it was
merged; inferring "merge" from "some other tablet's range now covers mine" is
unsound (two tables' still-unsplit tablets can have byte-identical
`KeyRange::whole()` ranges, with no table identity in scope). `merged` is the
explicit signal (a tiny, never-pruned marker — tablet ids are never reused).
`Reclaim` erases; `Absorb` never erases (the survivor now owns the range on
the same node-shared engine) and drains before halting (see Key invariants).
- **`Reconciler` teardown** (`Release`/`Reclaim`): call `on_teardown`
  (unregister from routing *before* touching the driver), `shutdown()`, poll
  `is_stopped()` bounded by `RECLAIM_STOP_TIMEOUT` (10s, via `env.sleep` — no
  tokio-only primitive), re-register via `on_host` and leave `LocalState`
  untouched on timeout (so `plan` re-emits the same action next tick), else
  narrow to `erase_bound` (Release only), `erase_scope()`, delete the WAL, and
  only then `confirm_torn_down`. `Absorb`'s teardown (`TeardownKind::Absorb`)
  skips both the narrow and the `erase_scope()` — only the driver stops and its
  WAL is removed. Timeouts: `ABSORB_DRAIN_TIMEOUT`, `RELEASE_CONFIRM_TICKS`,
  `RECLAIM_STOP_TIMEOUT`.

## What's non-obvious

- **The driver is split into a consensus loop + an apply task** — the
  driver-liveness fix (ADR 0017). Engine apply + compaction are slow
  (~180–300ms for a batch of LSM merges + a compaction rewrite on real disk)
  and used to run *inline* on the loop servicing Raft messages, so under write
  load the driver blocked past the 150ms election timeout → followers
  campaigned → a **leader-election storm** that truncated in-flight writes and
  collapsed throughput to ~15/s. Now:
  - **Consensus loop** (`drive`): recover from WAL, spawn the apply task, then
    loop `persist_wal` (drain records → append + `fsync` → `mark_durable_
    through`, under `wal_lock`) → `select(recv, timer, propose-wake)` → step
    the core → `persist_wal` again → send. It does **no** engine apply, so it
    always heartbeats/acks within the election timeout.
  - **Apply task** (`apply_loop` → `apply_and_compact`): install received
    snapshots, `drain_apply` → `merge`/`merge_tombstone` in commit order, and
    compact — all off the consensus loop. Backs off (`APPLY_IDLE_POLL`) only
    when idle.
  - The WAL is written by both tasks (append vs. compaction rewrite),
    serialized by the async `wal_lock`; compaction snapshots only up to
    `engine_applied` via `snapshot_upto` (not `last_applied`, which the engine
    hasn't merged) and **discards the consensus loop's pending records** in the
    same locked block (`replay` is push-based → re-appending would duplicate).
    Compaction is skipped while `halted`. `is_stopped()` requires *both* tasks
    stopped (`stopped && apply_stopped`) before the GC deletes artifacts.
  - This is also where `engine_applied` vs `last_applied` (Key invariants)
    comes from.
- **Wake-on-propose cuts single-write latency.** `put`/`delete`/`cas`/
  `change_membership` route through `propose_and_wake`: after the core appends,
  the proposer raises a `ProposeSignal` (`AtomicBool` +
  `futures::task::AtomicWaker`) that the consensus loop races as a third
  `select` arm, then calls `RaftCore::replicate_now` (broadcast immediately,
  resetting the heartbeat deadline) instead of leaving the entry parked until
  the next ~50ms heartbeat. `AtomicWaker` is deliberately **executor-agnostic**
  — synchronous `wake()` under `SimEnv`'s `ArcWake` executor (deterministic, no
  wall clock), resolves the register/wake race under tokio's `ProdEnv`; no
  tokio-only primitive, so determinism holds. The `ProposePending` future
  registers the waker *before* checking the flag (against a lost wakeup) and
  consumes it (`swap(false)`) on resolve, so it never busy-spins. A `NotLeader`
  propose appends nothing, so it doesn't wake. Verified over `ProdEnv` in
  `animusd/tests/cp_plane.rs::single_write_latency_is_low` (median ~52ms →
  ~11ms).
- **Unbounded scans must not fall through to `entries()`.** `local_scan`'s
  `end: None` branch (used by `/admin/raftkv`'s `raft_view`, by `erase_scope()`
  teardown, and transparently by `linearizable_scan` — the real DynamoDB
  `Scan`/CQL full-table path) derives a bounded upper bound from
  `physical_bounds` instead. `entries()` scans the **whole shared engine** (ADR
  0028), so on a node hosting several tablets every unbounded scan cost
  O(hosted tablets × whole node engine) — live-observed as `/admin/raftkv`
  hanging for 20s+ on a grown, auto-split cluster. `entries()` remains the
  fallback only for `StorageScope::whole()` (no finite bound).
- Distinct WAL file (`raftkv.wal`) from the control plane's `raft.wal`, so a
  node can host both planes. The name is exported (`animus_cp_data::WAL`) so
  the drop-table GC (ADR 0024) can delete a stopped group's WAL.
- **`shutdown()` is a graceful driver halt, not a kill** (ADR 0024): it latches
  a flag the driver observes at the top of its loop — *between* full
  persist+apply passes and within one wake — so WAL and engine are never left
  mid-write. Poll `is_stopped()` before touching the group's files. A halted
  node's accessors still answer from the **frozen** core (a halted leader keeps
  reporting `is_leader() == true`), so never route to a handle after
  unregistering it; a halted node must not be reused — restarting the tablet
  means a fresh `start`.
- **Test gotcha (membership):** pre-start a to-be-added node knowing only the
  *current* voters, NOT itself — a node started inside its own initial config
  is a voter that can campaign, win, and inject itself before the real add
  (`start_election` gates on `is_voter`). A `start` whose `all_nodes` excludes
  its own id is a quiet non-voter until the leader adds it. (Caught by the
  `reconfigure_trigger` seed sweep — a single seed hid it.)
- The ADR 0029 reconfigure/leadership-transfer follow-up fix (the two-layer
  transfer-gate threshold mismatch, the proposal-freeze while a transfer is
  armed, and the down-extra search fix) is a cross-cutting lesson — see the
  root `CLAUDE.md` engineering-practices log and `animus-control/CLAUDE.md`'s
  "Leadership transfer" entry for the core mechanics; the regressions are
  `tests/leader_transfer_reconfigure.rs` and
  `tests/reconfigure_down_extra_priority.rs`.
- **`ClusterSegmentStore`'s request/reply correlation (ADR 0043 §A7b) is a
  shared `Mutex<BTreeMap<req_id, Option<Reply>>>` polled via `env.sleep`,
  never a `tokio::sync::oneshot`** — the identical shape `RaftKvNode`'s own
  `ReadProbe`/`ReadProbeAck` read-barrier confirmation already uses, for the
  same reason: a `SimEnv` caller has no tokio runtime present to drive a
  oneshot's waker. See `docs/engineering-lessons.md`'s Testing section for
  the general rule (does the primitive come from `std`/`futures`, or a
  specific async runtime crate). A request and its reply are two variants of
  one `serde_json`'d wire enum sharing **one** stream/inbox
  ([`SEGMENT_STREAM`] `= u64::MAX`, deliberately outside any `TabletId`'s
  realistic range) — the same "one dedicated stream, one single-consumer
  serving task" shape the per-tablet driver loop uses on its own `stream`,
  generalized to a cluster-wide (not per-tablet) responsibility.
- **A `put_replicated`/`delete_from` failure can leave harmless orphans on
  whichever targets *did* succeed** — never cataloged (a later PR's sealer
  only commits `SealStreamShard` after `put_replicated` itself returns `Ok`),
  and `SegmentStore::put`/`delete` are idempotent overwrite/delete by
  contract, so a retry to the same deterministic id always converges. Don't
  "fix" a partial failure by trying to roll back the targets that already
  succeeded — that would add a second distributed failure mode (the rollback
  itself can partially fail) to clean up a case that is already safe to leave
  alone.

## Tests

`cargo test -p animus-cp-data`. All but one of the 26 test binaries drive
`SimEnv` — use `run_for`/`run_until`, never `run()` (the driver has perpetual
heartbeat/election timers). Linearizable reads are async (a read-barrier probe
round), so drive them as spawned tasks + `run_for`, and never `block_on` a
`tick()` whose planned action tears a group down (`Reconciler::teardown` polls
`env.sleep()` internally). The one exception is `prod_concurrent_ts_monotonic.rs`
(below) — a real-thread `ProdEnv` test, deliberately, because the race it
guards is provably unreachable under `SimEnv`'s single-threaded scheduler.
There is also one **in-crate** `#[cfg(test)] mod` at the bottom of `lib.rs`
itself (`pr5_orphan_and_resurrection_tests`, ADR 0018 §2/PR5's §2b) —
`cargo test -p animus-cp-data --lib` runs it; it needs `pub(crate)` access
(`txn::record_key`, a direct `KvCommand::TxnStage` construction,
`propose_ordered_aux`/`mint_pushed`) no external `tests/` file can reach, to
build a "late `TxnStage` for an already-known `txn_id`" scenario the public
API (which always mints a *fresh* id) cannot express.

One binary per behavior; the file names describe them (`ls
crates/animus-cp-data/tests/`) — covering single-tablet Raft mechanics
(election/replication/leader-kill, ReadIndex, CAS, batch, membership,
snapshot catch-up), automatic reconfiguration and leadership-transfer
cascades, the ADR 0026 stream-addressing/shared-engine primitives, the ADR
0041 `KindBatch`/scope-set mechanics, the ADR 0042/0043 `KIND_CURSOR`
scope-isolation and min-over-rows suite (`cursor_scope.rs`), `segment.rs`'s
own in-module `#[cfg(test)]` unit tests (`cargo test -p animus-cp-data --lib
segment::` — round-trip, decode rejections, and the superset-slice rule),
the ADR 0043 `§A7b` `ClusterSegmentStore` replication/fault suite
(`cluster_segment_store.rs`, a 3-node cluster over `SimSegmentStore`), the
ADR 0018 HLC/MVCC/range-seal/transaction suites (single- and
multi-participant, in-doubt recovery, write-key conditions, snapshot
reads, the read-timestamp cache), the `host.rs` reconciler end to end, and
the real-thread `ProdEnv` regression noted above.

### Reconciler lifecycle corpus (`tests/reconciler_corpus.rs`)

The 34 `host.rs` unit tests prove `plan` correct as a pure function; the entry
above proves ONE end-to-end sequence; this corpus is the first
**seed-reproducible fault-injection** suite for the whole tablet lifecycle. It
follows the house corpus doctrine (ADR 0014): a frozen, name-seeded scenario
list (`scenario_cells()`), a depth knob, and coverage/seed-expansion guards.

- **Harness**: each scenario builds a small `Cluster` of real
  `Reconciler<SimEnv, MemoryEngine>`s and drives it via `tick(node, &view)`
  with hand-scripted `MetadataView`s standing in for the control plane (no
  live control-plane `RaftNode` needed); real `RaftKvNode` groups form/
  elect/replicate underneath. Every scenario runs as a **spawned task**
  driven by `Simulator::run_for`, which can call `&self` fault methods
  (`stop`/`crash`/`partition_pair`/`heal`) since `Simulator` derives
  `Clone` (cloning just hands out another reference to the same
  `Arc`-backed world).
- **~20 frozen scenarios** (`ANIMUS_RECONCILER_SEEDS`, default 1 =
  byte-identical), covering the tablet lifecycle end to end: fresh-host,
  split-narrows-source, rebalance-off-releases (sparing a sibling),
  drop-table-reclaims, spare-join-then-promoted, reconfigure-removes-a-
  down-replica-first, merge-widens-survivor-and-absorbs-sibling-unerased
  (the deterministic absorb-drain regression, zero sim time before the
  merge view ticks), crash-restart shapes (single-replica upgrade via
  `has_data`, follower rejoin), a deliberate **contract-boundary** test
  proving *why* the `last_applied == 0` guard is load-bearing (bypassing
  it genuinely erases-then-rehosts empty), partition/release interplay,
  and two scenarios (`absorb_follower_waits_for_committed_seal_before_
  tearing_down`/`narrow_seal_survives_a_late_promotion_after_narrowing_
  as_a_follower`) that make the range-seal livelock fix (see Key
  invariants) deterministic under `SimEnv` by controlling per-node tick
  order and a real Raft membership change by hand.
- **Invariant checks, generic across scenarios**: (a) hosting convergence
  (`assert_hosted_converged`); (b) data safety (`assert_present`/
  `assert_absent`, raw physical-key reads — survivors readable, released/
  reclaimed erased, a co-hosted sibling never touched); (c) no zombie groups
  (`assert_all_stopped`); (d) idempotence (`assert_idempotent`) — meaning the
  observable *state* doesn't drift (hosted set, hook call counts, live scope
  ranges, Raft configs), **not** "the second tick emits zero actions"
  (`Reconfigure` is replanned every tick a node leads a group).
- **Depth found a test-robustness gap, not a reconciler bug**: two
  hand-rolled "force a real membership removal" helpers could hit a
  `NotLeader` right after confirming `is_leader()` — the documented
  proposal-freeze-while-transfer-armed behavior a single-shot assert can't
  tell from a real failure. Fixed by retrying the whole sequence each poll
  tick. Held green through `ANIMUS_RECONCILER_SEEDS=300` (5,400 runs).
- **To add a scenario**: write `fn scenario_my_thing(seed: u64)` in the
  existing shape (`run(seed, |sim| async move { .. })`), add a
  `scenario!("my_thing_name", scenario_my_thing)` to `scenario_cells()`, and
  run it under `ANIMUS_RECONCILER_SEEDS=100` (or higher) with a `timeout` the
  first time (a hang means a same-instant unbounded-work loop, not slowness —
  see the root `CLAUDE.md`).
- **Run at depth**: `ANIMUS_RECONCILER_SEEDS=K cargo test -p animus-cp-data
  --test reconciler_corpus reconciler_corpus_runs_every_scenario` (default
  `K=1`; held green through `K=300` in ~52s).
