//! A deterministic-safe observability seam: monotonic counters and a small
//! gauge, recorded behind a cheap-to-clone handle (ADR 0015).
//!
//! This is intentionally a *minimal* sink — atomic counters keyed by a closed
//! [`Metric`] enum plus one leadership gauge — chosen so it never introduces
//! nondeterminism (the load-bearing constraint, ADR 0003). Specifically:
//!
//! - **No wall clock.** Recording a counter is a pure atomic add; nothing here
//!   reads time. A caller that wants a timestamped metric passes one derived
//!   from [`Clock::now`](crate::Clock::now), never `std::time`.
//! - **No `HashMap`/`HashSet`.** The recording side is a fixed array of atomics
//!   ([`MetricSink`]) so recording is lock-free and allocation-free; a snapshot
//!   collects into a [`BTreeMap`], whose iteration order is deterministic.
//! - **No unseeded randomness, no I/O.** Recording touches only atomics; export
//!   ([`MetricSink::snapshot`] / [`MetricSnapshot::to_text`]) is a pure read.
//!
//! The seam is **additive**: [`Env::metrics`](crate::Env::metrics) has a default
//! implementation returning a shared no-op handle, so every component generic
//! over `E: Env` keeps compiling and behaving identically whether or not it (or
//! its env) cares about metrics. [`ProdEnv`](crate::ProdEnv) overrides it with a
//! real recording handle; under simulation a test constructs a recording
//! [`MetricsHandle`] and threads it into the component it wants to observe (no
//! change to `animus-sim` required).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// The metrics this seam knows how to record (ADR 0015) — control-plane Raft and
/// leaderless-AP data-plane counters. Each variant maps to one slot in the
/// fixed-size [`MetricSink`] array, so recording is a single atomic op with no
/// map lookup and no allocation.
///
/// The set is deliberately closed and small: a closed enum keeps recording
/// O(1)/lock-free and makes the exported names a stable, reviewable surface.
/// Add a variant (and a row to [`Metric::ALL`]) to instrument something new —
/// **append** new variants after the existing ones so their array slots and the
/// text-export order stay stable (the snapshot is byte-reproducible).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Metric {
    /// A leader election was started (a follower/candidate timed out and bumped
    /// its term).
    ElectionsStarted,
    /// An election was won (this node became leader).
    ElectionsWon,
    /// An `AppendEntries` was sent to a peer (replication or heartbeat).
    AppendEntriesSent,
    /// An `AppendEntries` was rejected by a follower (consistency-check failure
    /// or a stale term), driving the leader to back up `next_index`.
    AppendEntriesRejected,
    /// A chunked `InstallSnapshot` transfer completed and was installed on this
    /// node (follower side).
    SnapshotInstalls,
    /// A member's failure-detector verdict transitioned `Active`->`Down` (a
    /// `Down` was proposed by the leader, ADR 0012).
    FailureDetectorDown,
    /// A member's failure-detector verdict transitioned `Down`->`Active` (a
    /// recovery was proposed by the leader, ADR 0012).
    FailureDetectorUp,

    // --- Data plane (ADR 0001/0010/0005, ADR 0015 data-plane extension) ---
    // Added after the control-plane variants; their array slots follow, so the
    // existing variants/order/slots stay stable and the export is byte-reproducible.
    /// A quorum write was attempted by the coordinator (a `Write` broadcast to a
    /// tablet's replicas).
    DataQuorumWritesAttempted,
    /// A quorum write committed: `W` replicas durably acked it.
    DataQuorumWritesSucceeded,
    /// A quorum write failed: fewer than `W` replicas acked within the timeout
    /// (sub-quorum / fenced / down replicas).
    DataQuorumWritesFailed,
    /// A quorum read was attempted by the coordinator (a `Read` broadcast).
    DataQuorumReadsAttempted,
    /// A quorum read succeeded: `R` replicas responded.
    DataQuorumReadsSucceeded,
    /// A quorum read failed: a read quorum could not be reached (sub-quorum /
    /// fenced / down replicas).
    DataQuorumReadsFailed,
    /// A read observed divergent responders and triggered read-repair (one push
    /// per divergent quorum read).
    DataReadRepairTriggered,
    /// Keys pushed back by read-repair (the winning `(value, version)` re-sent to
    /// the read's participants). One per repaired key.
    DataReadRepairKeysRepaired,
    /// A hint was stored for a replica that missed a committed write/delete
    /// (hinted handoff, ADR 0010). One per `(target, key)` buffered.
    DataHintsStored,
    /// A buffered hint batch was replayed to a returning target (a `Sync` of the
    /// missed entries sent on the handoff/replay path).
    DataHintsDelivered,
    /// A background anti-entropy round fired and emitted a segment digest to its
    /// peers (one per non-empty round, segment-digest exchange).
    DataAntiEntropyRounds,

    // --- Storage engine (LSM, ADR 0004/0008, ADR 0015 storage extension) ---
    // Appended after the data-plane variants; their array slots follow, so every
    // earlier variant's slot and the text-export order stay stable and the
    // snapshot remains byte-reproducible. All recorded at the real LSM site that
    // knows the outcome (a flush/compaction actually performed, a block fetched
    // from disk), all observe-only — they change no engine behavior — and all
    // deterministic (counters only, no wall clock).
    /// A memtable was flushed to a fresh SSTable (one per completed flush, counted
    /// after the manifest swap commits the new table).
    StorageFlushes,
    /// A leveled compaction was performed (one per compaction whose manifest swap
    /// committed — a compaction *actually run*, not merely scheduled).
    StorageCompactions,
    /// Input SSTables consumed by compactions (the "segments compacted": the source
    /// + overlapping target tables merged away). Summed across compactions.
    StorageCompactionTablesMerged,
    /// Input SSTable bytes consumed by compactions (the on-disk `file_size` of every
    /// merged input table). Summed across compactions — the read-side I/O a
    /// compaction folded.
    StorageCompactionBytesMerged,
    /// An SSTable data block was fetched from disk (one per `read_at` of a block;
    /// the read-amplification counter). A point read that the key-range / Bloom
    /// gates reject reads no block, so this stays flat for a proven-absent key.
    StorageSstableBlockReads,
    /// A per-table Bloom filter answered "may contain" for a point lookup whose key
    /// was inside the table's key range (a candidate that will read a block).
    StorageBloomHits,
    /// A per-table Bloom filter ruled a key absent (key inside the table's range but
    /// the Bloom said no), saving a block read. Only counted for tables that
    /// actually carry a Bloom (`has_bloom`).
    StorageBloomMisses,
    /// A WAL segment was rotated: the active segment crossed its byte budget and a
    /// fresh segment file was opened (one per rotation, at the group-commit site).
    StorageWalSegmentRotations,
    /// Tombstone-GC records reclaimed during compaction (a tombstone or a version it
    /// shadowed, physically dropped below the GC floor). Summed across compactions.
    StorageTombstonesReclaimed,

    // --- CP data plane (ADR 0016/0017, ADR 0015 CP-plane extension) ---
    // Appended after the storage-engine variants; every earlier variant's slot and
    // the text-export order stay stable, so the snapshot remains byte-reproducible.
    // Recorded by `animus-cp-data`'s `RaftKvNode` (the per-tablet Raft-group driver
    // + its public propose API) at the sites that know the real *outcome* — an
    // actually-accepted/rejected propose, an actual commit advance, an effect
    // actually drained and applied to the engine, a barrier that actually
    // confirmed/didn't — never on every attempt or every driver-loop tick.
    /// A client propose (`put`/`delete`/`cas`/`propose_split`) was accepted by this
    /// group's leader (appended to its log).
    CpProposalsAccepted,
    /// A client propose was rejected because this node is not the group's leader.
    CpProposalsRejectedNotLeader,
    /// Log entries newly committed (an actual `maybe_advance_commit` advance,
    /// summed by how far the commit index moved — not incremented on every timer
    /// tick or message that leaves it unchanged).
    CpCommits,
    /// Committed-and-durable commands actually drained (`RaftCore::drain_apply`)
    /// and applied to the engine, summed across apply passes.
    CpApplies,
    /// A run of accumulated `Put`/`Delete` effects was flushed as one
    /// `StorageEngine::merge_batch` call (one WAL `fsync` for the whole run). Pair
    /// with [`CpApplyBatchSizeSum`] to derive the average batch size.
    CpApplyBatchRuns,
    /// Effects included in a [`CpApplyBatchRuns`] batch, summed across batches.
    CpApplyBatchSizeSum,
    /// A ReadIndex read barrier (linearizable `get`/`scan`) confirmed leadership by
    /// quorum and was served.
    CpReadBarriersServed,
    /// A ReadIndex read barrier did not confirm before its deadline (a step-down,
    /// term change, or the wait timed out) and returned no value.
    CpReadBarriersTimedOut,
    /// An **eventually-consistent** read (ADR 0055, `ConsistentRead: false`)
    /// was served from this node's **own** replica — no network hop, no
    /// consensus work. The cheapest outcome, and the one the feature exists
    /// to produce.
    CpEventualReadsLocal,
    /// An eventually-consistent read was served by **one forwarded hop** to
    /// another replica, because this node holds none it may serve from.
    CpEventualReadsForwarded,
    /// An eventually-consistent read could **not** be served cheaply at all
    /// and fell back to the linearizable ReadIndex path (no serveable
    /// replica here, no reachable one elsewhere, a scope/routing race, or the
    /// freshness gate refusing a catching-up replica). Always correct, never
    /// cheap — a rate that stays high means the cheap path is not actually
    /// being taken, which no client-visible symptom would reveal.
    CpEventualReadsFellBack,
    /// The threshold-triggered compaction step advanced the snapshot base and
    /// truncated the Raft log prefix. Decoupled from
    /// [`CpSnapshotImageBuilds`] (PR #29's lazy-image design): a trigger does not
    /// by itself build a shippable image.
    CpSnapshotTriggers,
    /// The engine was actually scanned into a snapshot image and installed
    /// (`RaftCore::set_snapshot_blob`) because a replication attempt needed to ship
    /// one and found none materialized (`RaftCore::take_snapshot_needed`) — the
    /// on-demand half of the lazy-image design.
    CpSnapshotImageBuilds,
    /// One `InstallSnapshot` chunk was actually sent to a peer.
    CpSnapshotShips,
    /// A peer's `InstallSnapshot` transfer completed and it installed the snapshot
    /// (an outbound `InstallSnapshotResp` with a non-zero `last_index`, observed on
    /// the follower that just finished).
    CpSnapshotInstalls,
    /// A single-server `change_membership` step (direct call or the automatic
    /// `reconfigure_step`) was accepted by this group's leader.
    CpReconfigureAccepted,
    /// A single-server `change_membership` step was rejected (not leader, an
    /// in-flight change, a multi-server delta, or a leader self-removal).
    CpReconfigureRejected,

    // --- Control plane's own membership change (ADR 0009/0017 C reused for
    // the control group itself, ADR 0015 control-plane extension) ---
    // Appended after the CP-data-plane variants; every earlier variant's slot
    // and the text-export order stay stable, so the snapshot remains
    // byte-reproducible. Recorded by `animus-control::RaftNode::change_
    // membership` (thin wrapper over `RaftCore::change_membership`) — kept as
    // its own counter family, distinct from `CpReconfigureAccepted`/
    // `CpReconfigureRejected` (the per-tablet data-plane counters), so
    // control-*group* reconfiguration churn (growing/shrinking/replacing a
    // control voter) is separately observable from per-tablet churn.
    /// A single-server `change_membership` step on the control group itself
    /// was accepted by the control leader.
    ControlReconfigureAccepted,
    /// A single-server `change_membership` step on the control group itself
    /// was rejected (not leader, an in-flight change, a multi-server delta, or
    /// a leader self-removal).
    ControlReconfigureRejected,

    // --- Orphan-member sweep (ADR 0040 PR6) ---
    // Appended after the control-group-membership-change variants; every
    // earlier variant's slot and the text-export order stay stable, so the
    // snapshot remains byte-reproducible. Recorded by
    // `animus_control::node`'s `orphan_sweep_loop` at the one site that knows
    // the real outcome — a `RemoveMember` actually *proposed* for a
    // never-activated claim once its volatile leader-side timer exceeds
    // `orphan_sweep_after` (never on every tick, and never for a claim the
    // leader is merely still watching).
    /// The leader proposed `RemoveMember` for a claim (a `node_addrs` entry,
    /// with or without a `members` row) its own volatile timer judged
    /// sweep-eligible for at least `orphan_sweep_after` — never-activated,
    /// not a control voter, not referenced by any tablet, not
    /// decommissioning. Counts the *proposal*, not a confirmed commit (the
    /// existing `RemoveMember` apply-time guard is the safety net — a
    /// genuine late activation racing the proposal still shows up here even
    /// if the proposal itself is rejected at apply).
    OrphanMembersSwept,

    // --- Read-timestamp cache / logged read ceiling (ADR 0018 §2, PR2b) ---
    // Appended after the orphan-sweep variant; every earlier variant's slot
    // and the text-export order stay stable, so the snapshot remains
    // byte-reproducible. Recorded by `animus-cp-data`'s `RaftKvNode` at the
    // one site that knows the real outcome — a `ReadCeiling` command actually
    // *committed* (not merely proposed, and never once per read: the whole
    // point of the ceiling design is that it amortizes to roughly one
    // proposal per `HLC_MAX_OFFSET` of wall time under continuous reads).
    /// A `KvCommand::ReadCeiling` was accepted by this group's leader
    /// (appended to its log) to raise the committed read ceiling above a
    /// read this leader is about to serve.
    CpReadCeilingProposals,

    // --- Multi-participant transactions (ADR 0018 §2, PR4) ---
    // Appended after the read-ceiling variant; every earlier variant's slot
    // and the text-export order stay stable, so the snapshot remains
    // byte-reproducible. Recorded by `animus-cp-data`'s `RaftKvNode` at the
    // one site that knows the real outcome — a read that observed no value
    // at `ts` but a version within `(ts, uncertainty_upper(ts)]`, and
    // therefore restarted once at the higher timestamp (ADR 0018 §2: the
    // uncertainty-interval mechanism, never a false negative — the restart
    // is what proves it, not a client-visible error).
    /// A linearizable/snapshot read restarted once at a higher timestamp
    /// because a version existed within its clock-uncertainty window.
    CpUncertaintyRestarts,

    // --- In-doubt transaction recovery + the resolver loop (ADR 0018 §2,
    // PR5) --- Appended after the uncertainty-restarts variant; every
    // earlier variant's slot and the text-export order stay stable, so the
    // snapshot remains byte-reproducible. Recorded by `animusd`'s
    // `ClientCtx::txn_recover`/`txn_resolver_loop`.
    /// A recovery push (`ClientCtx::txn_recover`) drove a stale `Pending`
    /// record to `Committed` (every participant verified staged).
    CpTxnRecoveredCommitted,
    /// A recovery push drove a stale `Pending` record to `Aborted` (at
    /// least one participant's intent was missing).
    CpTxnRecoveredAborted,
    /// One `txn_resolver_loop` tick ran to completion on this node (over
    /// however many locally-led tablet groups it walked).
    CpTxnResolverRuns,

    // --- Dynamo transactional surface (ADR 0018 §2, PR7) --- Appended after
    // the resolver-runs variant; every earlier variant's slot and the
    // text-export order stay stable, so the snapshot remains
    // byte-reproducible. Recorded by `animusd::dynamo` at the one site that
    // knows the real outcome of each request.
    /// A `TransactWriteItems` request committed atomically (`ClientCtx::cp_txn`
    /// returned `Ok`).
    DynamoTransactWritesCommitted,
    /// A `TransactWriteItems` request was cancelled — a condition failure or a
    /// lost 2PC race (`ClientCtx::cp_txn` returned `Err`, or a condition
    /// evaluated false before staging began).
    DynamoTransactWritesCanceled,
    /// A `TransactGetItems` request returned a quiescence-confirmed consistent
    /// snapshot on its **first** confirming round (no retry needed).
    DynamoTransactGetsOk,
    /// A `TransactGetItems` request needed at least one extra round beyond the
    /// first confirming pair before its snapshot quiesced (still returned
    /// `Ok`), or exhausted its bounded retries without quiescing (returned a
    /// retryable `TransactionCanceledException`) — both are contention
    /// signals, hence one counter (see `run_transact_get`'s doc for why the
    /// two aren't split further).
    DynamoTransactGetsRetried,

    // --- DynamoDB Streams sealer (ADR 0042/0043, round-3 sealer PR) ---
    // Appended after the transact-gets-retried variant; every earlier
    // variant's slot and the text-export order stay stable, so the snapshot
    // remains byte-reproducible. Recorded by `animusd::index_drain`'s
    // `change_consumer_loop` seal arm and hot-trim arm — a mix of genuine
    // counters (`StreamSealsTotal`/`StreamSealFailuresTotal`) and **level**
    // gauges written via `MetricsHandle::set` rather than `MetricsHandle::
    // incr` (`StreamHotBytes`/`StreamSealBacklogMs`/`ChangeLogTrimBlocked`)
    // — a plain counter slot re-purposed as a last-write-wins level, the
    // same shape `MetricSink::is_leader` already uses for a boolean level,
    // generalized here to an arbitrary `u64` level without a second array.
    /// The sealing leader's own current `KIND_CHANGE` scope size (bytes,
    /// `CpGroup::approx_bytes`) for the tablet its most recent seal-arm tick
    /// evaluated — a level, not a count: each tick overwrites it via
    /// `MetricsHandle::set`.
    StreamHotBytes,
    /// **Semantics changed, ADR 0042 fork G (2026-08-16).** Used to be the
    /// age (milliseconds) of the *oldest unsealed record*, computed by
    /// scanning `KIND_CHANGE` every tick. It is now the age (milliseconds,
    /// the loop's own `env` clock, never a raw OS clock) of the tablet's own
    /// **last seal**, measured only while unsealed bytes exist
    /// (`approx_bytes_kind(KIND_CHANGE) > 0`) — `0` whenever the hot tail is
    /// empty. Read straight off the `stream_shards` catalog
    /// (`Metadata::last_seal_wall_ms`) for a tablet that has sealed before;
    /// a tablet that never has falls back to a one-time-memoized real scan
    /// of the true oldest pending record's own HLC, cached per tablet after
    /// its first observation so no *further* scan is ever needed for that
    /// tablet again (see `animusd::index_drain::seal_tick`'s own doc for the
    /// full design and why a cheaper driver-local timestamp guess doesn't
    /// work). The old and new values agree whenever the hot tail is one
    /// contiguous burst (the common case) and can differ for a slow,
    /// trickling backlog — traded deliberately for never *repeatedly*
    /// scanning `KIND_CHANGE` just to keep this level current.
    /// A level, overwritten via `MetricsHandle::set`.
    StreamSealBacklogMs,
    /// A stream shard seal committed (the segment `put` succeeded on every
    /// replica and `MetaCommand::SealStreamShard` was confirmed in the
    /// replicated catalog) — counts the confirmed commit, not the attempt
    /// (a crash-retried re-seal of the same `(tablet, epoch)` counts once,
    /// at whichever attempt's confirmation actually lands).
    StreamSealsTotal,
    /// A seal attempt failed (the segment store `put` errored, or the
    /// `SealStreamShard` proposal never confirmed within its timeout) — the
    /// next tick simply retries, per ADR 0043 §A3's recovery discipline.
    StreamSealFailuresTotal,
    /// Whether the most recent hot-trim arm tick found at least one
    /// streamed-or-indexed tablet whose trim was blocked by a missing
    /// expected watermark (1) or not (0) — a level (this tick's outcome
    /// **OR**ed across every tablet this node leads), overwritten via
    /// `MetricsHandle::set`; the consequence is real (an unhealed store or a
    /// stream that has never sealed grows its hot scope unboundedly) but
    /// never itself an error.
    ChangeLogTrimBlocked,

    // --- DynamoDB Streams segment janitor (ADR 0042/0043, round-3 PR7) ---
    // Appended after the sealer/hot-trim variants above; every earlier
    // variant's slot and the text-export order stay stable. Recorded by
    // `animusd::segment_janitor`'s own control-plane-leader-only background
    // loop — a mix of genuine counters (`StreamSegmentsExpiredTotal`/
    // `StreamRepairsTotal`) and **level** gauges written via
    // `MetricsHandle::set` (`StreamSegmentsLive`/`StreamRepairBacklog`),
    // the identical "counter slot re-purposed as a last-write-wins level"
    // shape `StreamHotBytes`/`StreamSealBacklogMs` already use above.
    /// The number of currently-unexpired stream-shard catalog rows this
    /// tick's snapshot counted, across every table/label — a level,
    /// overwritten via `MetricsHandle::set` each janitor tick (only ever
    /// recorded by whichever node currently believes it is the
    /// control-plane leader; every other node's sink simply stays at its
    /// last-observed value from when it last led, which is an accepted
    /// staleness for a metric that only means anything on the leader).
    StreamSegmentsLive,
    /// A stream-shard catalog row completed the janitor's two-phase reclaim
    /// (its segment object was deleted at every still-membership-present
    /// recorded replica, or confirmed unreachable-because-removed, and the
    /// row itself was physically removed) — counts the confirmed removal,
    /// not the mark (a row can sit marked-but-not-yet-removed for several
    /// ticks while a slow/unreachable replica's delete is retried).
    StreamSegmentsExpiredTotal,
    /// A live (unexpired) row's replica set was successfully updated to
    /// replace a replica lost to the cluster's own membership (a `Down` or
    /// removed node) with a freshly-chosen one — one count per row whose
    /// catalog update committed, not per replica copied (a single repair
    /// tick backfilling two lost replicas of the same row still counts
    /// once, since it is one catalog-row decision).
    StreamRepairsTotal,
    /// The number of live rows this tick's snapshot found under-replicated
    /// (at least one recorded replica not currently an `Active` member) —
    /// a level, overwritten via `MetricsHandle::set` each tick; the
    /// convergent backlog a healthy cluster keeps at (or promptly returns
    /// to) zero, and the signal an operator watches to tell "membership
    /// churn the repair sweep is still catching up on" from "stuck."
    StreamRepairBacklog,

    // --- Apply-time merge write-loss seatbelt (ADR 0018 §2 write-loss
    // amendment) --- Appended after the stream-repair-backlog variant;
    // every earlier variant's slot and the text-export order stay stable,
    // so the snapshot remains byte-reproducible. Recorded by
    // `animus-cp-data`'s apply arms that already treat a `storage.merge`/
    // `merge_tombstone` outcome as "landed" before this fix started
    // checking it (`TxnStage`'s intent write, `TxnResolve`'s commit/
    // abort-restore writes, `Cas`'s swap) — see `surface_suspicious_merge_
    // noop`'s doc for the replay-vs-fresh-apply distinction these two
    // metrics exist to separate.
    /// A `merge`/`merge_tombstone` call at one of the three audited
    /// apply-arm sites returned `Ok(false)` ("did not take effect")
    /// where the caller's own control flow had already treated the write
    /// as landed — recorded unconditionally, including the common benign
    /// case (an ordinary post-crash WAL replay re-applying an entry the
    /// engine already durably reflects from before this process started).
    CpMergeTookNoEffect,
    /// The strict subset of [`CpMergeTookNoEffect`] this process can
    /// **prove** is not explainable by replay (the entry's own version
    /// strictly exceeds the engine-durable watermark recovered at this
    /// apply task's own start) — a genuine, live invariant violation, not
    /// a startup artifact. Every increment here pairs with a capped
    /// `tracing::warn!`. **Deliberately not a hard assert (or even a
    /// `debug_assert!`)**: an earlier draft tried one and it fired on
    /// legitimate, already-tested scenarios this replay-vs-fresh
    /// distinguisher doesn't yet account for (e.g. an application-level
    /// retry landing an identical entry a second time within one process
    /// lifetime, not a restart at all) — see `surface_suspicious_merge_
    /// noop`'s doc for the open FIXME. Metric + log only until a
    /// same-value-idempotent-reapply check exists; this is exactly the
    /// signal that would have caught the write-loss bug (ADR 0018 §2's
    /// amendment) had it existed then.
    CpMergeTookNoEffectUnexplained,

    // --- F11 token-alignment choke point (ADR 0042 §14, growth PR2) ---
    // Appended after the merge-write-loss variants above; every earlier
    // variant's slot and the text-export order stay stable, so the
    // snapshot remains byte-reproducible. Recorded by `animusd::
    // ClientCtx::trigger_split` — the single choke point every split
    // proposer (auto-split, `POST /admin/tablet/split`,
    // `ClientRequest::SplitTablet`) funnels through.
    /// A streamed table's split key rounded down (F11) onto the target
    /// tablet's own `range.start` — a single very-hot partition token that
    /// owns the tablet's entire range, which can never legally split
    /// without breaking the per-token affinity F11 exists to protect (ADR
    /// 0042 §14 Fork E, the accepted single-token hot-partition limit).
    /// Counts the skip, not an error: `trigger_split` returns immediately
    /// (no propose attempt) and `auto_split_loop` matches this specific
    /// outcome to skip its own "split did not commit" warning, which would
    /// otherwise fire every cooldown, forever, for a tablet that
    /// structurally cannot split.
    StreamSplitSingleTokenSkipped,

    // --- Quiescence (ADR 0044 phase-1 PR3) --- Appended after the F11 variant;
    // every earlier variant's slot and the text-export order stay stable, so
    // the snapshot remains byte-reproducible. Recorded by `animus-cp-data`'s
    // consensus-loop send site (`record_kv_outbound`) — the per-tablet
    // counterpart to the control plane's `AppendEntriesSent`, kept as its own
    // variant (not reused) since it is recorded off the CP-plane's `KvWire`
    // outbound list rather than `RaftNode::record_outbound`'s `Out<MetaCommand>`
    // list. This is what an idle/quiesced tablet group's own heartbeat traffic
    // going flat is measured against — the ADR 0044 idle-cost win phase 1
    // targets.
    /// A per-tablet CP-data `AppendEntries` (replication or heartbeat) was
    /// sent to a peer.
    CpAppendEntriesSent,

    // --- Quiescence observability (ADR 0044 phase-1 PR7) --- Appended
    // after the append-entries-sent variant above; every earlier variant's
    // slot and the text-export order stay stable, so the snapshot remains
    // byte-reproducible. Recorded by `animus-cp-data`'s consensus loop
    // (`CpQuiesces`/`CpUnquiesces`, incremented on every genuine
    // quiesced/ticking transition it observes — one per group, all sharing
    // this node's one sink) and `animusd`'s `metrics_sample_loop`
    // (`CpGroupsQuiesced`, a level: how many of this node's *currently
    // hosted* groups this sample found quiesced — see that loop's own doc
    // for why a per-group increment/decrement can't safely stand in for a
    // periodic re-count across a shared sink).
    /// A per-tablet CP-data group transitioned into quiescence (`RaftCore::
    /// quiesced` flipped `false -> true`) — counts the transition, not a
    /// duration.
    CpQuiesces,
    /// A per-tablet CP-data group transitioned out of quiescence (`RaftCore::
    /// quiesced` flipped `true -> false`) — any inbound message, local
    /// propose, or explicit wake.
    CpUnquiesces,
    /// The number of this node's currently-hosted CP-data groups this
    /// node's most recent metrics sample found quiesced — a level,
    /// overwritten via `MetricsHandle::set` (never `incr`), the identical
    /// "counter slot re-purposed as a last-write-wins level" shape
    /// `StreamHotBytes`/`StreamSegmentsLive` already use above. Read-only:
    /// sampling never itself wakes a group (fork F — admin/dashboard reads
    /// must never disturb the fleet-wide idle-cost win quiescence exists
    /// for).
    CpGroupsQuiesced,

    // --- Universal kind-write path (ADR 0049, Train A rung 4) --- Appended
    // after the quiescence-observability variants above; every earlier
    // variant's slot and the text-export order stay stable, so the snapshot
    // remains byte-reproducible. Recorded by `animusd::index_drain`'s
    // hot-trim arm — the one deleter of hot change records, which under ADR
    // 0049 covers *every* table (a never-streamed, never-indexed table's
    // image-less marker records are transient by the zero-expected-terms
    // trim rule).
    /// Change-log records this node's hot-trim arm deleted, cumulative —
    /// a genuine counter. On a plain (no-stream, no-GSI) table this is the
    /// marker churn the every-table trim keeps transient; on a
    /// streamed/indexed table it counts ordinary post-seal/post-drain
    /// trimming exactly as before. Also the trim-safe half of the
    /// marker-emission regression tests' accounting: an emitted record is
    /// either still pending or was counted here — a union a racing trim
    /// tick cannot erase.
    ChangeLogTrimmedTotal,

    // --- In-doubt recovery verify inconclusiveness (issue #298 shape B fix)
    // --- Appended after the kind-write-path variant above; every earlier
    // variant's slot and the text-export order stay stable, so the snapshot
    // remains byte-reproducible. Recorded by `animusd`'s `ClientCtx::
    // txn_recover`/`txn_resolver_loop`.
    /// A recovery push (`ClientCtx::txn_recover`) declined to decide because
    /// at least one participant's `txn_verify` returned `Err` (could not
    /// verify, e.g. a transient routing failure mid-fork/cutover) rather
    /// than an affirmative `Ok(true)`/`Ok(false)` — an `Err` is never
    /// evidence of "not staged," so this call proposes nothing and returns
    /// `Pending` for the next sweep to retry.
    CpTxnRecoveryVerifyInconclusive,
    /// A transaction has stayed `Pending` — via repeated
    /// [`Self::CpTxnRecoveryVerifyInconclusive`] declines — past
    /// `txn_resolver_loop`'s own stuck-recovery grace window, without ever
    /// reaching a decision. Metered once per stuck episode (mirroring
    /// `unresolved_decided`'s own lookup-failure grace tracker): this is a
    /// liveness signal for an operator, never a correctness concern — the
    /// transaction's own intents stay safely `Pending` (never wrongly
    /// decided) and resolve the moment `txn_verify` can actually confirm
    /// their state again.
    CpTxnRecoveryStuckInconclusive,
    // --- `unresolved_decided` background-resolution gap (issue #298
    // residuals) --- Appended after the trim-total variant; every earlier
    // variant's slot and the text-export order stay stable, so the snapshot
    // remains byte-reproducible. Recorded by `animusd`'s `txn_resolver_loop`.
    /// An `unresolved_decided` entry's `txn_record_view` lookup kept failing
    /// for at least `animus_cp_data::RECOVERY_GRACE` — this loop has given up
    /// background-resolving it *for now* (there is no `intent_spans` to act
    /// on without a readable record). Correctness is unaffected: a straggling
    /// unresolved remote intent is still resolved on demand the moment any
    /// reader hits it (the foreign-intent read-path push, ADR 0018 §2/PR5
    /// §3) — this counter only signals reduced background promptness for
    /// that one transaction, e.g. because its record's tablet retired
    /// mid-recovery.
    CpTxnUnresolvedDecidedStuck,
    // --- `ClientRequestToken` idempotency, issue #298 "deep shape A"
    // amendment --- Appended after the unresolved-decided-stuck variant;
    // every earlier variant's slot and the text-export order stay stable, so
    // the snapshot remains byte-reproducible. Recorded by
    // `animusd::dynamo::run_transact`.
    /// A `TransactWriteItems` request's own `cp_txn` call stayed ambiguous
    /// (`TxnAbortReason::is_ambiguous`) through every internal retry within
    /// `CLIENT_TIMEOUT` — this coordinator could not confirm whether the
    /// transaction committed or not. Deliberately **not** counted as
    /// [`Self::DynamoTransactWritesCanceled`]: a `ClientRequestToken`'s
    /// idempotency record is left `PENDING` rather than falsely marked
    /// `CANCELLED` (see `run_transact`'s own doc) — this counter is purely a
    /// liveness signal for an operator, since correctness never depends on
    /// it firing.
    DynamoTransactWritesAmbiguous,

    // --- Control-plane leadership transfer observability (issue #313)
    // --- Appended after the ambiguous-transact-writes variant above; every
    // earlier variant's slot and the text-export order stay stable, so the
    // snapshot remains byte-reproducible. Recorded by
    // `animus_control::node`'s driver loop, the one place that observes a
    // leader-still-Leader `transfer_target` clear (never inside the pure
    // `RaftCore`, which has no metrics handle — see ADR 0003's sync/driver
    // split).
    /// An armed leadership transfer (`RaftCore::transfer_leadership`) missed
    /// its deadline while this node was still leader — the target never
    /// stepped down in time (crashed after arming, fell behind, or a
    /// dropped `TimeoutNow`/election round). Distinct from an ordinary
    /// transfer *completing*, which clears `transfer_target` via this same
    /// node stepping down to a higher term instead — that path never
    /// increments this counter. A nonzero rate means the single
    /// un-randomized `election_base` budget `transfer_leadership` arms is
    /// too tight for this deployment's real round-trip latency; see
    /// `animus-control/CLAUDE.md`'s "Leadership transfer" entry.
    ControlTransferAborted,

    // --- CP data-plane needs-snapshot state (issue #554) --- Appended after
    // the leadership-transfer-observability variant above; every earlier
    // variant's slot and the text-export order stay stable, so the snapshot
    // remains byte-reproducible. Recorded by `animus-cp-data`'s `drive`.
    /// This replica's own engine, at `drive()` start, was found behind its
    /// own recovered `RaftCore::snapshot_index` — the log's own compacted
    /// prefix is gone from the engine too (a wiped/rebuilt engine reopened
    /// fresh, or any other way the two fell out of sync). The replica
    /// refuses reads and campaigning until a fresh `InstallSnapshot` closes
    /// the gap (`RaftCore::state_machine_behind`). A nonzero rate is
    /// expected whenever the reconciler's engine-loss recovery fires past
    /// the compaction threshold; see `animus-cp-data/CLAUDE.md`'s
    /// "needs-snapshot state" entry.
    CpEngineNeedsSnapshot,

    // --- Tablet-host reconciler engine-loss recovery (issue #554) ---
    // Appended after the needs-snapshot-state variant above; every earlier
    // variant's slot and the text-export order stay stable, so
    // the snapshot remains byte-reproducible. Recorded by `animus-cp-data`'s
    // `host::Reconciler::ensure_engine`/`materialize_split_child` (the G4
    // re-open branch) — see `EngineFactory::destroy`'s call sites for the
    // full mechanism.
    /// This node's own `EngineFactory::open` for a hosted (or split-child)
    /// tablet failed — treated as a corrupt/lost local engine, not a
    /// transient fault. Counts every occurrence, including one immediately
    /// followed by a successful destroy-and-reopen.
    CpEngineOpenFailed,
    /// Following a [`Self::CpEngineOpenFailed`], `EngineFactory::destroy`
    /// plus a fresh `EngineFactory::open` succeeded — the tablet's engine
    /// was rebuilt from nothing and Raft (log replay plus, for a replica
    /// whose own log has been compacted since the loss, the leader's
    /// on-demand `InstallSnapshot`) repopulates it. The reconciler hosts the
    /// tablet normally from here; no operator action needed, though a
    /// nonzero rate is worth watching (see `animus-cp-data/CLAUDE.md`'s
    /// "engine-loss recovery" entry).
    CpEngineRebuilt,
    /// The fresh `EngineFactory::open` after a [`Self::CpEngineOpenFailed`]
    /// `destroy` **also** failed — this node's disk itself is unhealthy
    /// (not just one tablet's files), not a one-off corruption. The
    /// reconciler does not retry the destroy a second time in the same
    /// action; it falls back to the pre-existing warn-and-skip behavior, and
    /// `plan` re-emits the action next tick.
    CpEngineRebuildFailed,

    // --- Client-connection cancellation (issue #596) --- Appended after the
    // engine-rebuild variant above; every earlier variant's slot and the
    // text-export order stay stable, so the snapshot remains
    // byte-reproducible. Recorded by `animusd`'s `handle_connection` (the
    // per-connection request loop shared by the client and intra listeners,
    // ADR 0047) at the one site that knows the real outcome — the peer's
    // socket was observed closed (EOF or a read error) while a request was
    // still in flight, and the in-flight request future was dropped rather
    // than driven to completion. See `crates/animusd/CLAUDE.md`'s
    // fire-and-forget-connection-handler entry for the mechanism this closes
    // and `docs/engineering-lessons.md`'s matching entry (issue #585/#586)
    // for why an abandoned forwarded RPC's full confirm-wait budget running
    // with nobody listening is the amplifier this counter is meant to catch.
    /// A client (or a forwarding peer) closed its connection while this node
    /// was still handling that connection's in-flight request — the request
    /// future was dropped instead of run to completion. A sustained nonzero
    /// rate under a hint-chasing forward means abandoned server-side work is
    /// piling up, not necessarily a client bug (see the issue #596 lesson).
    ClientRequestsAbandoned,
}

impl Metric {
    /// Every metric, in a fixed order. The array index of a metric in `ALL` is
    /// its slot in the [`MetricSink`]; keep this in sync with the enum.
    pub const ALL: [Metric; 83] = [
        Metric::ElectionsStarted,
        Metric::ElectionsWon,
        Metric::AppendEntriesSent,
        Metric::AppendEntriesRejected,
        Metric::SnapshotInstalls,
        Metric::FailureDetectorDown,
        Metric::FailureDetectorUp,
        Metric::DataQuorumWritesAttempted,
        Metric::DataQuorumWritesSucceeded,
        Metric::DataQuorumWritesFailed,
        Metric::DataQuorumReadsAttempted,
        Metric::DataQuorumReadsSucceeded,
        Metric::DataQuorumReadsFailed,
        Metric::DataReadRepairTriggered,
        Metric::DataReadRepairKeysRepaired,
        Metric::DataHintsStored,
        Metric::DataHintsDelivered,
        Metric::DataAntiEntropyRounds,
        Metric::StorageFlushes,
        Metric::StorageCompactions,
        Metric::StorageCompactionTablesMerged,
        Metric::StorageCompactionBytesMerged,
        Metric::StorageSstableBlockReads,
        Metric::StorageBloomHits,
        Metric::StorageBloomMisses,
        Metric::StorageWalSegmentRotations,
        Metric::StorageTombstonesReclaimed,
        Metric::CpProposalsAccepted,
        Metric::CpProposalsRejectedNotLeader,
        Metric::CpCommits,
        Metric::CpApplies,
        Metric::CpApplyBatchRuns,
        Metric::CpApplyBatchSizeSum,
        Metric::CpReadBarriersServed,
        Metric::CpReadBarriersTimedOut,
        Metric::CpEventualReadsLocal,
        Metric::CpEventualReadsForwarded,
        Metric::CpEventualReadsFellBack,
        Metric::CpSnapshotTriggers,
        Metric::CpSnapshotImageBuilds,
        Metric::CpSnapshotShips,
        Metric::CpSnapshotInstalls,
        Metric::CpReconfigureAccepted,
        Metric::CpReconfigureRejected,
        Metric::ControlReconfigureAccepted,
        Metric::ControlReconfigureRejected,
        Metric::OrphanMembersSwept,
        Metric::CpReadCeilingProposals,
        Metric::CpUncertaintyRestarts,
        Metric::CpTxnRecoveredCommitted,
        Metric::CpTxnRecoveredAborted,
        Metric::CpTxnResolverRuns,
        Metric::DynamoTransactWritesCommitted,
        Metric::DynamoTransactWritesCanceled,
        Metric::DynamoTransactGetsOk,
        Metric::DynamoTransactGetsRetried,
        Metric::StreamHotBytes,
        Metric::StreamSealBacklogMs,
        Metric::StreamSealsTotal,
        Metric::StreamSealFailuresTotal,
        Metric::ChangeLogTrimBlocked,
        Metric::StreamSegmentsLive,
        Metric::StreamSegmentsExpiredTotal,
        Metric::StreamRepairsTotal,
        Metric::StreamRepairBacklog,
        Metric::CpMergeTookNoEffect,
        Metric::CpMergeTookNoEffectUnexplained,
        Metric::StreamSplitSingleTokenSkipped,
        Metric::CpAppendEntriesSent,
        Metric::CpQuiesces,
        Metric::CpUnquiesces,
        Metric::CpGroupsQuiesced,
        Metric::ChangeLogTrimmedTotal,
        Metric::CpTxnRecoveryVerifyInconclusive,
        Metric::CpTxnRecoveryStuckInconclusive,
        Metric::CpTxnUnresolvedDecidedStuck,
        Metric::DynamoTransactWritesAmbiguous,
        Metric::ControlTransferAborted,
        Metric::CpEngineNeedsSnapshot,
        Metric::CpEngineOpenFailed,
        Metric::CpEngineRebuilt,
        Metric::CpEngineRebuildFailed,
        Metric::ClientRequestsAbandoned,
    ];

    /// The stable exported name of this metric (snake_case, used as the text
    /// export key). Stable across versions — treat as part of the export surface.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Metric::ElectionsStarted => "control_elections_started",
            Metric::ElectionsWon => "control_elections_won",
            Metric::AppendEntriesSent => "control_append_entries_sent",
            Metric::AppendEntriesRejected => "control_append_entries_rejected",
            Metric::SnapshotInstalls => "control_snapshot_installs",
            Metric::FailureDetectorDown => "control_failure_detector_down",
            Metric::FailureDetectorUp => "control_failure_detector_up",
            Metric::DataQuorumWritesAttempted => "data_quorum_writes_attempted",
            Metric::DataQuorumWritesSucceeded => "data_quorum_writes_succeeded",
            Metric::DataQuorumWritesFailed => "data_quorum_writes_failed",
            Metric::DataQuorumReadsAttempted => "data_quorum_reads_attempted",
            Metric::DataQuorumReadsSucceeded => "data_quorum_reads_succeeded",
            Metric::DataQuorumReadsFailed => "data_quorum_reads_failed",
            Metric::DataReadRepairTriggered => "data_read_repair_triggered",
            Metric::DataReadRepairKeysRepaired => "data_read_repair_keys_repaired",
            Metric::DataHintsStored => "data_hints_stored",
            Metric::DataHintsDelivered => "data_hints_delivered",
            Metric::DataAntiEntropyRounds => "data_anti_entropy_rounds",
            Metric::StorageFlushes => "storage_flushes",
            Metric::StorageCompactions => "storage_compactions",
            Metric::StorageCompactionTablesMerged => "storage_compaction_tables_merged",
            Metric::StorageCompactionBytesMerged => "storage_compaction_bytes_merged",
            Metric::StorageSstableBlockReads => "storage_sstable_block_reads",
            Metric::StorageBloomHits => "storage_bloom_hits",
            Metric::StorageBloomMisses => "storage_bloom_misses",
            Metric::StorageWalSegmentRotations => "storage_wal_segment_rotations",
            Metric::StorageTombstonesReclaimed => "storage_tombstones_reclaimed",
            Metric::CpProposalsAccepted => "cp_proposals_accepted",
            Metric::CpProposalsRejectedNotLeader => "cp_proposals_rejected_not_leader",
            Metric::CpCommits => "cp_commits",
            Metric::CpApplies => "cp_applies",
            Metric::CpApplyBatchRuns => "cp_apply_batch_runs",
            Metric::CpApplyBatchSizeSum => "cp_apply_batch_size_sum",
            Metric::CpReadBarriersServed => "cp_read_barriers_served",
            Metric::CpReadBarriersTimedOut => "cp_read_barriers_timed_out",
            Metric::CpEventualReadsLocal => "cp_eventual_reads_local",
            Metric::CpEventualReadsForwarded => "cp_eventual_reads_forwarded",
            Metric::CpEventualReadsFellBack => "cp_eventual_reads_fell_back",
            Metric::CpSnapshotTriggers => "cp_snapshot_triggers",
            Metric::CpSnapshotImageBuilds => "cp_snapshot_image_builds",
            Metric::CpSnapshotShips => "cp_snapshot_ships",
            Metric::CpSnapshotInstalls => "cp_snapshot_installs",
            Metric::CpReconfigureAccepted => "cp_reconfigure_accepted",
            Metric::CpReconfigureRejected => "cp_reconfigure_rejected",
            Metric::ControlReconfigureAccepted => "control_reconfigure_accepted",
            Metric::ControlReconfigureRejected => "control_reconfigure_rejected",
            Metric::OrphanMembersSwept => "control_orphan_members_swept",
            Metric::CpReadCeilingProposals => "cp_read_ceiling_proposals",
            Metric::CpUncertaintyRestarts => "cp_uncertainty_restarts",
            Metric::CpTxnRecoveredCommitted => "cp_txn_recovered_committed",
            Metric::CpTxnRecoveredAborted => "cp_txn_recovered_aborted",
            Metric::CpTxnResolverRuns => "cp_txn_resolver_runs",
            Metric::DynamoTransactWritesCommitted => "dynamo_transact_writes_committed",
            Metric::DynamoTransactWritesCanceled => "dynamo_transact_writes_canceled",
            Metric::DynamoTransactGetsOk => "dynamo_transact_gets_ok",
            Metric::DynamoTransactGetsRetried => "dynamo_transact_gets_retried",
            Metric::StreamHotBytes => "stream_hot_bytes",
            Metric::StreamSealBacklogMs => "stream_seal_backlog_ms",
            Metric::StreamSealsTotal => "stream_seals_total",
            Metric::StreamSealFailuresTotal => "stream_seal_failures_total",
            Metric::ChangeLogTrimBlocked => "change_log_trim_blocked",
            Metric::StreamSegmentsLive => "stream_segments_live",
            Metric::StreamSegmentsExpiredTotal => "stream_segments_expired_total",
            Metric::StreamRepairsTotal => "stream_repairs_total",
            Metric::StreamRepairBacklog => "stream_repair_backlog",
            Metric::CpMergeTookNoEffect => "cp_merge_took_no_effect",
            Metric::CpMergeTookNoEffectUnexplained => "cp_merge_took_no_effect_unexplained",
            Metric::StreamSplitSingleTokenSkipped => "stream_split_single_token_skipped",
            Metric::CpAppendEntriesSent => "cp_append_entries_sent",
            Metric::CpQuiesces => "cp_quiesces",
            Metric::CpUnquiesces => "cp_unquiesces",
            Metric::CpGroupsQuiesced => "cp_groups_quiesced",
            Metric::ChangeLogTrimmedTotal => "change_log_trimmed_total",
            Metric::CpTxnRecoveryVerifyInconclusive => "cp_txn_recovery_verify_inconclusive",
            Metric::CpTxnRecoveryStuckInconclusive => "cp_txn_recovery_stuck_inconclusive",
            Metric::CpTxnUnresolvedDecidedStuck => "cp_txn_unresolved_decided_stuck",
            Metric::DynamoTransactWritesAmbiguous => "dynamo_transact_writes_ambiguous",
            Metric::ControlTransferAborted => "control_transfer_aborted",
            Metric::CpEngineNeedsSnapshot => "cp_engine_needs_snapshot",
            Metric::CpEngineOpenFailed => "cp_engine_open_failed",
            Metric::CpEngineRebuilt => "cp_engine_rebuilt",
            Metric::CpEngineRebuildFailed => "cp_engine_rebuild_failed",
            Metric::ClientRequestsAbandoned => "client_requests_abandoned",
        }
    }

    /// This metric's fixed slot index in the [`MetricSink`] array.
    const fn slot(self) -> usize {
        self as usize
    }
}

/// The fixed-size, lock-free recording sink behind a [`MetricsHandle`]. Counters
/// are monotonic `u64`s; the single gauge is an `i64` (whether this node
/// believes it is leader). Recording is a relaxed atomic op, so it is cheap
/// enough to call on the hot path and introduces no ordering dependency that
/// could perturb determinism.
#[derive(Debug)]
pub struct MetricSink {
    counters: [AtomicU64; Metric::ALL.len()],
    /// A single gauge: whether this node currently believes it is leader (1) or
    /// not (0). A gauge (settable both ways) rather than a counter because
    /// leadership is a *level*, not an event count.
    is_leader: AtomicI64,
}

impl Default for MetricSink {
    fn default() -> Self {
        Self {
            // `AtomicU64` is not `Copy`, so build the array element-wise.
            counters: std::array::from_fn(|_| AtomicU64::new(0)),
            is_leader: AtomicI64::new(0),
        }
    }
}

impl MetricSink {
    /// Add `n` to `metric`'s counter (relaxed; recording needs no ordering).
    pub fn incr_by(&self, metric: Metric, n: u64) {
        self.counters[metric.slot()].fetch_add(n, Ordering::Relaxed);
    }

    /// Read `metric`'s current counter value.
    #[must_use]
    pub fn get(&self, metric: Metric) -> u64 {
        self.counters[metric.slot()].load(Ordering::Relaxed)
    }

    /// Overwrite `metric`'s slot with `value` (relaxed) — for a **level**
    /// metric re-using a counter slot (see e.g. `Metric::StreamHotBytes`'s
    /// doc) rather than a monotonic count. Never mix `incr_by`/`set` calls
    /// on the same variant; each metric's own doc says which it is.
    pub fn set(&self, metric: Metric, value: u64) {
        self.counters[metric.slot()].store(value, Ordering::Relaxed);
    }

    /// Set the leadership gauge (1 = this node believes it is leader, 0 = not).
    pub fn set_leader(&self, leader: bool) {
        self.is_leader.store(i64::from(leader), Ordering::Relaxed);
    }

    /// Read the leadership gauge.
    #[must_use]
    pub fn leader_gauge(&self) -> i64 {
        self.is_leader.load(Ordering::Relaxed)
    }

    /// Take a point-in-time snapshot of every counter and the gauge. A pure read
    /// of the atomics into a deterministically-ordered structure — safe to call
    /// concurrently with recording (each load is independent; the snapshot is not
    /// claimed to be a single atomic instant across metrics, which observability
    /// does not require).
    #[must_use]
    pub fn snapshot(&self) -> MetricSnapshot {
        let mut counters = BTreeMap::new();
        for m in Metric::ALL {
            counters.insert(m, self.get(m));
        }
        MetricSnapshot {
            counters,
            is_leader: self.leader_gauge(),
        }
    }
}

/// A cheap-to-clone handle onto a [`MetricSink`]. Components hold one and call
/// [`incr`](MetricsHandle::incr) / [`set_leader`](MetricsHandle::set_leader);
/// the owner (a `ProdEnv`, or a test) keeps the same handle to read it back. A
/// handle obtained from [`MetricsHandle::noop`] records into a shared throwaway
/// sink — recording is still valid (and free), it is simply never read.
#[derive(Clone, Debug)]
pub struct MetricsHandle {
    sink: Arc<MetricSink>,
}

impl Default for MetricsHandle {
    fn default() -> Self {
        Self::recording()
    }
}

impl MetricsHandle {
    /// A handle onto a fresh recording sink. The caller keeps the handle (clones
    /// share the one sink) and reads it back via [`snapshot`](Self::snapshot).
    #[must_use]
    pub fn recording() -> Self {
        Self {
            sink: Arc::new(MetricSink::default()),
        }
    }

    /// The process-wide no-op handle: a single shared sink that is recorded into
    /// but never read. This is what [`Env::metrics`](crate::Env::metrics)'s
    /// default returns, so an env that does not care about metrics costs nothing
    /// and changes no behavior. (It is a real sink, not a branch on every
    /// record, so the hot path has no `if metrics.is_some()` check.)
    #[must_use]
    pub fn noop() -> Self {
        use std::sync::OnceLock;
        static NOOP: OnceLock<MetricsHandle> = OnceLock::new();
        NOOP.get_or_init(MetricsHandle::recording).clone()
    }

    /// Increment `metric` by one.
    pub fn incr(&self, metric: Metric) {
        self.sink.incr_by(metric, 1);
    }

    /// Increment `metric` by `n`.
    pub fn incr_by(&self, metric: Metric, n: u64) {
        self.sink.incr_by(metric, n);
    }

    /// Overwrite a **level** metric's slot — see [`MetricSink::set`]'s doc.
    pub fn set(&self, metric: Metric, value: u64) {
        self.sink.set(metric, value);
    }

    /// Set the leadership gauge.
    pub fn set_leader(&self, leader: bool) {
        self.sink.set_leader(leader);
    }

    /// Read one counter (mostly for tests).
    #[must_use]
    pub fn get(&self, metric: Metric) -> u64 {
        self.sink.get(metric)
    }

    /// Snapshot every metric for export or assertion.
    #[must_use]
    pub fn snapshot(&self) -> MetricSnapshot {
        self.sink.snapshot()
    }

    /// Whether `self` and `other` record into the **same** underlying sink
    /// (`Arc::ptr_eq`) — i.e. two handles obtained from the same `Env`
    /// (possibly via distinct clones/roles). ADR 0040 PR1 merged a combined
    /// node's two internal `ProdEnv` roles into one, so a caller that used to
    /// aggregate "the control-role sink" and "the raftkv-role sink" as two
    /// distinct handles must first check they aren't now the same handle
    /// (summing a snapshot with itself would double-count every counter).
    #[must_use]
    pub fn is_same_sink(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.sink, &other.sink)
    }
}

/// An exported point-in-time view of all metrics, in deterministic order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricSnapshot {
    /// Every counter, keyed by [`Metric`] (a `BTreeMap`, so iteration order is
    /// stable).
    pub counters: BTreeMap<Metric, u64>,
    /// The leadership gauge (1 = leader, 0 = not).
    pub is_leader: i64,
}

impl MetricSnapshot {
    /// Render the snapshot as stable, line-oriented text — one `name value` pair
    /// per line, in `Metric::ALL` order, followed by the gauge. No timestamp (the
    /// export is timeless; a scrape adds its own), so the output is a pure
    /// function of the counter values and is byte-identical for equal snapshots.
    #[must_use]
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        for m in Metric::ALL {
            let v = self.counters.get(&m).copied().unwrap_or(0);
            out.push_str(m.name());
            out.push(' ');
            out.push_str(&v.to_string());
            out.push('\n');
        }
        out.push_str("control_is_leader ");
        out.push_str(&self.is_leader.to_string());
        out.push('\n');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incr_and_snapshot_round_trip() {
        let h = MetricsHandle::recording();
        h.incr(Metric::ElectionsStarted);
        h.incr(Metric::ElectionsStarted);
        h.incr_by(Metric::AppendEntriesSent, 5);
        h.set_leader(true);

        let snap = h.snapshot();
        assert_eq!(snap.counters[&Metric::ElectionsStarted], 2);
        assert_eq!(snap.counters[&Metric::AppendEntriesSent], 5);
        assert_eq!(snap.counters[&Metric::ElectionsWon], 0);
        assert_eq!(snap.is_leader, 1);
    }

    #[test]
    fn clones_share_one_sink() {
        let a = MetricsHandle::recording();
        let b = a.clone();
        a.incr(Metric::ElectionsWon);
        b.incr(Metric::ElectionsWon);
        assert_eq!(a.get(Metric::ElectionsWon), 2);
    }

    #[test]
    fn text_export_is_stable_and_ordered() {
        let h = MetricsHandle::recording();
        h.incr_by(Metric::SnapshotInstalls, 3);
        let text = h.snapshot().to_text();
        assert!(text.starts_with("control_elections_started 0\n"));
        assert!(text.contains("control_snapshot_installs 3\n"));
        assert!(text.ends_with("control_is_leader 0\n"));
        assert_eq!(text, h.snapshot().to_text());
    }

    #[test]
    fn slot_indices_match_all_order() {
        for (i, m) in Metric::ALL.iter().enumerate() {
            assert_eq!(m.slot(), i, "slot for {m:?} must match ALL index");
        }
    }
}
