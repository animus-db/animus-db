//! The DynamoDB Streams **segment janitor** (ADR 0043 §A9, round-3 PR7): a
//! distinct, control-plane-**leader**-only background loop — not an arm of
//! the per-tablet `index_drain::change_consumer_loop` — that reclaims
//! already-*sealed* segments past retention and repairs replicas the
//! cluster's own membership has lost. Retention and repair are cluster-wide
//! catalog concerns over the whole `stream_shards` map, not any one source
//! tablet's own hot-log housekeeping (that job is the sealer + hot-trim arm,
//! `index_drain.rs`).
//!
//! ## Who runs this, and the control-only-leader gap
//!
//! Spawned unconditionally on every node shape that can ever become the
//! control-plane leader (`BoundNode::start_with_streams` — combined — and
//! `BoundControlNode::start_control_with` — control-only, ADR 0035) — the
//! same "run everywhere, self-gate on `ctx.edge.leader_handle()`" pattern
//! `auto_split_loop`/`txn_resolver_loop`/the tablet-host reconciler already
//! use for their own per-tablet leadership check, generalized here to a
//! whole-process "am I the control leader at all" question. **Never**
//! spawned on a data-only node (`BoundDataNode`, ADR 0035 PR4) — it never
//! registers a local control `RaftNode` into its own `ClusterEdgeState` at
//! all, so `leader_handle()` there is permanently `None`; spawning the loop
//! would be a harmless but pointless extra tick forever.
//!
//! **Documented scope gap.** Phases 2/3 below (object deletion, replica
//! repair) need a [`crate::SegmentStoreHandle`] — which only exists on a
//! node with a data role (`crate::DataRole`) — while phase 1's own *mark*
//! step and the drop-table retention-zero rule need only `Metadata`. On a
//! **combined** node this is moot (every combined node has both roles); on
//! a **control-only** leader (a genuine split deployment, ADR 0035) phases
//! 2/3 are skipped for as long as that leadership stint lasts — rows still
//! get marked (and so stop being newly discoverable via `dynamo_streams.rs`'s
//! own `expired`-row filtering) but their objects/rows are never physically
//! reclaimed until a data-role node becomes the control leader instead. A
//! **pure** split deployment (control-only nodes are the *only* control
//! voters) therefore never runs phases 2/3 at all today — a real, deliberate
//! deferral: extending `SegmentStoreHandle` provisioning to a control-only
//! node is its own follow-up scope, not attempted here. Every end-to-end
//! test for this module runs against a combined-mode cluster, where the
//! control leader always also has a data role. See
//! `docs/engineering-lessons.md` for the full note.
//!
//! ## The phases, one snapshot per tick
//!
//! Every phase below reads from the **same** `Metadata` snapshot
//! (`leader.metadata()`, taken once at the top of the tick) — not because a
//! moving target would be unsafe (every command here is idempotent and
//! re-derived fresh next tick regardless), but so one tick's own decisions
//! are internally consistent with each other.
//!
//! 1. **Retention expiry, two-phase (ADR 0043 §A9)**: mark every unexpired
//!    row past `retention` — measured from its own `seal_wall_ms` against
//!    this loop's `env` clock — *or* whose table has been dropped entirely
//!    (`Metadata::table_schema` no longer names it: the drop-table cascade's
//!    own retention-zero rule, see the module-level note below) via
//!    `MetaCommand::ExpireStreamShards { remove: false }`. Then, for every
//!    row already marked `expired` (from this tick or an earlier one),
//!    delete the object at every recorded replica still present in the
//!    cluster's own membership — a replica **removed** from membership
//!    entirely is treated as confirmed-absent (its files, if it has any
//!    left, are unreachable and no longer this cluster's concern; only a
//!    **removed**, not merely `Down`, member counts — a `Down` member might
//!    come back, so its copy is still owed a genuine delete attempt) — and,
//!    once every present replica's delete has actually succeeded (or there
//!    was nothing left to delete), physically remove the row via
//!    `MetaCommand::ExpireStreamShards { remove: true }`. Every step here is
//!    idempotent and safe to retry on the very next tick; a crash anywhere
//!    (including between marking and deleting, or between deleting and
//!    removing) just resumes.
//! 2. **Replica repair (ADR 0043 §A9/§A7b, F5's own durability mandate)**:
//!    for every **live** (unexpired) row with a non-empty `replicas` (a
//!    `ClusterSegmentStore`-backed row — the single-directory `FsSegmentStore`
//!    opt-in always records an empty list, ADR 0043 §A7b's own convention,
//!    and has no per-node replica concept to repair at all), verify each
//!    recorded replica is a current `Active` member; for however many are
//!    not, fetch a live copy from whichever recorded replicas *are* `Active`
//!    and push it to enough freshly-chosen targets to restore the row's own
//!    original replica count (`ClusterSegmentStore::repair`), then commit
//!    the updated `replicas` set via the content-preserving `SealStreamShard`
//!    update `animus-control`'s apply arm now supports (round-3 PR7
//!    amendment — see that command's own doc for the decision). Never
//!    touches an expired row (it is about to be physically reclaimed by
//!    phase 1 anyway — repairing it first is harmless, just wasted work, so
//!    this is a plain skip, not a correctness guard).
//! 3. **Orphan reap (ADR 0042 §10/ADR 0043 §A3 as-built amendment,
//!    [`reap_orphans`]).** The ledger-named-object fix makes every seal
//!    attempt write at its own unique id, never the bare deterministic
//!    `segment::segment_id` — a losing/abandoned attempt's own object is
//!    therefore never overwritten away by a later attempt (the write-once
//!    store forbids it) and becomes a permanent orphan unless something
//!    reaps it. Two sub-cases, one proven-safe and immediate, one
//!    age-gated: see [`reap_orphans`]'s own doc. **Local-only** —
//!    `SegmentStoreHandle::list_local`'s own documented scope — so one tick
//!    only discovers this node's own local copies; a K-way replicated
//!    orphan's other copies are swept as control leadership later rotates
//!    to whichever node holds them, an accepted eventual-convergence
//!    property rather than an immediate one.
//!
//! ## Why `ClientCtx::drop_table` does nothing new (the convergent design)
//!
//! `MetaCommand::ExpireStreamShards` is deliberately **not relayable**
//! (`animus-control`'s own doc on the command, and `lib.rs`'s
//! `is_relayable_command` doc) — its only sanctioned caller is a
//! control-plane-leader-only background loop that already holds a live
//! `RaftNode` handle, the same restriction `RemoveMember` gets. `drop_table`
//! runs on **whichever node the client happened to connect to**, essentially
//! never guaranteed to be the control leader — so it structurally *cannot*
//! propose `ExpireStreamShards` itself except in the lucky case it already
//! is the leader, and even then duplicating this loop's own two-phase logic
//! inline would be a second, harder-to-keep-consistent copy of the identical
//! decision. The chosen design instead makes a dropped table's un-reaped
//! rows *look like* ordinary, already-overdue retention work to this loop —
//! no new command, no new code path in `drop_table` at all, and correctness
//! holds regardless of timing or which node ran the drop. See this module's
//! own test suite (`tests/stream_janitor.rs`) for the convergence proof,
//! including the "drop mid disable-grace, two labels" variant.

use std::time::Duration;

use animus_control::RaftNode;
use animus_cp_data::segment;
use animus_env::{Clock, Env, Metric, NodeId, ProdEnv};
use animus_tablet::TabletId;

use crate::ClientCtx;
use crate::{MetaCommand, Metadata, NodeStatus};

/// How often this loop wakes to re-derive its whole decision from a fresh
/// `Metadata` snapshot — matches `index_drain.rs`'s own
/// `INDEX_DRAIN_INTERVAL` cadence: cheap per-tick work, and this codebase's
/// own testing discipline (tiny `--stream-retention` values, never the
/// production default) needs a fast tick so a converged-or-timeout test
/// doesn't itself become the slow part of the corpus.
const SEGMENT_JANITOR_INTERVAL: Duration = Duration::from_millis(200);

/// How long an **unsealed** epoch's own orphan candidate must sit before
/// [`reap_orphans`]'s open-epoch sub-case considers it abandoned rather than
/// still legitimately in flight — generous relative to `index_drain::
/// SEAL_COMMIT_TIMEOUT` (10s): every legitimate attempt either commits or
/// gives up well inside this window, so anything still unclaimed past it is
/// genuinely abandoned, never a false positive against a slow-but-live
/// attempt.
const ORPHAN_GRACE: Duration = Duration::from_secs(5 * 60);

/// The control-plane-leader-only background loop (ADR 0043 §A9) — see the
/// module doc for who spawns this, why it self-gates every tick rather than
/// being spawned only on whichever node happens to lead right now, and the
/// documented control-only-leader scope gap.
pub(crate) async fn segment_janitor_loop(ctx: ClientCtx, retention: Duration) {
    loop {
        tokio::time::sleep(SEGMENT_JANITOR_INTERVAL).await;
        let Some(leader) = ctx.edge.leader_handle() else {
            continue;
        };
        segment_janitor_tick(&ctx, &leader, retention).await;
    }
}

/// One tick's whole decision — see the module doc's "The phases" section.
async fn segment_janitor_tick(ctx: &ClientCtx, leader: &RaftNode<ProdEnv>, retention: Duration) {
    let meta = leader.metadata();
    let now_ms = ctx.env.now().0 / 1_000_000;
    let retention_ms = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
    let metrics = ctx.env.metrics();

    // --- Phase 1a: mark every due row (age past retention, or its whole
    // table has been dropped — the drop-table retention-zero rule) --------
    let mut to_mark: Vec<(TabletId, u64)> = Vec::new();
    let mut live_count: u64 = 0;
    let mut under_replicated: u64 = 0;
    for ((tablet, epoch), row) in meta.stream_shards.iter() {
        if row.expired {
            continue;
        }
        live_count += 1;
        let table_dropped = meta.table_schema(&row.table).is_none();
        let due = table_dropped || now_ms.saturating_sub(row.seal_wall_ms) >= retention_ms;
        if due {
            to_mark.push((*tablet, *epoch));
        } else if !row.replicas.is_empty() {
            let alive = row.replicas.iter().filter(|r| is_active(&meta, r)).count();
            if alive < row.replicas.len() {
                under_replicated += 1;
            }
        }
    }
    metrics.set(Metric::StreamSegmentsLive, live_count);
    metrics.set(Metric::StreamRepairBacklog, under_replicated);
    if !to_mark.is_empty() {
        let _ = leader.propose(MetaCommand::ExpireStreamShards {
            rows: to_mark,
            remove: false,
        });
    }

    // --- Phase 1b: delete objects for every already-marked row, then
    // physically remove whichever ones are now fully reclaimed -------------
    let mut removed: Vec<(TabletId, u64)> = Vec::new();
    for ((tablet, epoch), row) in meta.stream_shards.iter() {
        if !row.expired {
            continue;
        }
        // **Epoch-derivation guard**: `index_drain::seal_now`'s own `next_
        // epoch` (and `dynamo_streams::current_open_epoch`) derive a
        // tablet's next/current epoch from its own chain's highest-numbered
        // *existing* row — "chain length," not an independent counter (ADR
        // 0042 §2's own design: "epoch = the chain length"). Physically
        // removing a tablet's own highest-epoch row while that tablet could
        // still seal again would make a future seal recompute the SAME
        // epoch number for genuinely new data — a silent identity collision
        // this loop must never cause. Only the object may be deleted; the
        // row itself stays (still correctly `expired`, so already invisible
        // to `DescribeStream`'s enumeration and to phase 2's repair) until
        // either the tablet seals past it (no longer the max) or the tablet
        // itself is gone (dropped — nothing will ever derive an epoch for
        // it again).
        let is_tablet_max = meta
            .stream_shards
            .range((*tablet, epoch + 1)..=(*tablet, u64::MAX))
            .next()
            .is_none();
        let may_remove_row = !is_tablet_max || !meta.tablets.contains_key(tablet);

        // Ledger-named-object amendment: resolve the object id from the
        // row itself, never recompute `segment_id` — the row's `object_id`
        // is the *only* id this shard's winning bytes ever actually lived
        // at (see `StreamShardRow::object_id`'s own doc).
        let seg_id = row.object_id.as_str();
        if row.replicas.is_empty() {
            // The single-directory `FsSegmentStore` opt-in's own convention
            // (ADR 0043 §A7b): no per-replica list, but a real object still
            // needs a real (local) delete — never treat "empty list" as
            // "nothing to delete."
            let Some(data) = ctx.data_opt() else {
                continue; // control-only leader — see the module doc's gap
            };
            match data.segment_store.delete_sealed(&[], seg_id).await {
                Ok(()) if may_remove_row => removed.push((*tablet, *epoch)),
                Ok(()) => {}
                Err(e) => tracing::warn!(
                    tablet = tablet.0,
                    epoch,
                    error = %e,
                    "segment janitor: fs-mode object delete failed, retrying next tick"
                ),
            }
            continue;
        }
        let still_present: Vec<NodeId> = row
            .replicas
            .iter()
            .filter(|r| meta.members.contains_key(*r))
            .cloned()
            .collect();
        if still_present.is_empty() {
            // Every recorded replica has been removed from the cluster's
            // own membership entirely — confirmed-absent, nothing left to
            // delete (see the module doc's dead-replica rule).
            if may_remove_row {
                removed.push((*tablet, *epoch));
            }
            continue;
        }
        let Some(data) = ctx.data_opt() else {
            continue; // control-only leader — see the module doc's gap
        };
        match data
            .segment_store
            .delete_sealed(&still_present, seg_id)
            .await
        {
            Ok(()) if may_remove_row => removed.push((*tablet, *epoch)),
            Ok(()) => {}
            Err(e) => tracing::warn!(
                tablet = tablet.0,
                epoch,
                error = %e,
                "segment janitor: object delete failed, retrying next tick"
            ),
        }
    }
    if !removed.is_empty() {
        metrics.incr_by(Metric::StreamSegmentsExpiredTotal, removed.len() as u64);
        let _ = leader.propose(MetaCommand::ExpireStreamShards {
            rows: removed,
            remove: true,
        });
    }

    // --- Phase 2: replica repair for every live, cluster-replicated row --
    let Some(data) = ctx.data_opt() else {
        return; // control-only leader — nothing further this tick
    };
    for ((tablet, epoch), row) in meta.stream_shards.iter() {
        if row.expired || row.replicas.is_empty() {
            continue;
        }
        let alive: Vec<NodeId> = row
            .replicas
            .iter()
            .filter(|r| is_active(&meta, r))
            .cloned()
            .collect();
        if alive.len() == row.replicas.len() {
            continue; // fully healthy
        }
        if alive.is_empty() {
            tracing::warn!(
                tablet = tablet.0,
                epoch,
                "segment janitor: no live replica left to repair this shard from"
            );
            continue;
        }
        // Ledger-named-object amendment: resolve from the row, never
        // recompute `segment_id`.
        let seg_id = row.object_id.as_str();
        let bytes = match data.segment_store.get_sealed(&alive, seg_id).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                tracing::error!(
                    tablet = tablet.0,
                    epoch,
                    "segment janitor: a live row's object is unexpectedly gone from every \
                     alive replica"
                );
                continue;
            }
            Err(e) => {
                tracing::warn!(
                    tablet = tablet.0,
                    epoch,
                    error = %e,
                    "segment janitor: repair fetch failed, retrying next tick"
                );
                continue;
            }
        };
        let target_k = row.replicas.len();
        let new_replicas = match data
            .segment_store
            .repair_replicas(seg_id, &bytes, &alive, target_k)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    tablet = tablet.0,
                    epoch,
                    error = %e,
                    "segment janitor: repair push failed, retrying next tick"
                );
                continue;
            }
        };
        let mut current_sorted = row.replicas.clone();
        current_sorted.sort();
        let mut new_sorted = new_replicas;
        new_sorted.sort();
        if new_sorted != current_sorted {
            metrics.incr(Metric::StreamRepairsTotal);
            let _ = leader.propose(MetaCommand::SealStreamShard {
                table: row.table.clone(),
                label: row.label.clone(),
                tablet: *tablet,
                epoch: *epoch,
                view_type: row.view_type,
                hlc_range: row.hlc_range,
                count: row.count,
                seal_wall_ms: row.seal_wall_ms,
                replicas: new_sorted,
                // Repair moves bytes, never re-derives them — the object id
                // stays exactly the row's own already-committed one (the
                // ledger-named-object amendment: this proposal's `object_id`
                // must match the existing row's for `Metadata::apply`'s
                // content check to recognize this as the legitimate
                // replicas-only-update shape, not a content conflict).
                object_id: row.object_id.clone(),
            });
        }
    }

    // --- Phase 3: orphan reap (ledger-named-object amendment) ------------
    reap_orphans(&data.segment_store, &meta, now_ms).await;
}

/// Phase 3: orphan reap (ADR 0042 §10/ADR 0043 §A3 as-built amendment) — a
/// losing or abandoned seal attempt's own segment object, written at its own
/// unique id ([`segment::segment_object_id`]) but never referenced by any
/// catalog row. The old shared-deterministic-id scheme never needed this: a
/// later attempt's `put` simply overwrote an earlier one at the same id (the
/// exact bug this amendment fixes). With write-once, unique-per-attempt ids,
/// a losing attempt's object survives forever unless something reaps it.
///
/// Two sub-cases:
/// - **(a) the epoch IS sealed** — a row exists for `(tablet, epoch)`:
///   every OTHER object at that shard's own deterministic prefix
///   ([`segment::segment_id`]) is a **proven** orphan.
///   `MetaCommand::SealStreamShard`'s first-committer-wins apply arm
///   guarantees no future proposal can ever commit a *different* row for
///   this exact `(tablet, epoch)` again, so reaping it is always safe —
///   no age check needed, and it runs every tick for as long as the row
///   exists (including once marked `expired`, right up until phase 1
///   physically removes it), so in practice a dueling-seal race's own
///   orphan is swept within one `SEGMENT_JANITOR_INTERVAL` of the winner
///   committing.
/// - **(b) the epoch is NOT yet sealed** — a live, streamed tablet's own
///   *current open* epoch has no row yet: an orphan here might be
///   genuinely abandoned (the attempt that wrote it crashed and nothing
///   ever retried) or might still be legitimately in flight — content
///   alone can't tell them apart, so this sweep decodes each candidate's
///   own header (`seal_wall_ms`, ADR 0003 Env-seamed) and only deletes it
///   once its age exceeds [`ORPHAN_GRACE`].
///
/// **Local-only** (`SegmentStoreHandle::list_local`'s own doc): a single
/// call only discovers this node's own local segment directory. See the
/// module doc's own note on why this converges eventually rather than
/// immediately.
async fn reap_orphans(store: &crate::SegmentStoreHandle, meta: &Metadata, now_ms: u64) {
    // (a) Sealed epochs: every non-winning object at the shard's own
    // prefix is a proven orphan.
    for ((tablet, epoch), row) in meta.stream_shards.iter() {
        let prefix = segment::segment_id(&row.table, &row.label, tablet.0, *epoch);
        let ids = match store.list_local(&format!("{prefix}/")).await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(
                    tablet = tablet.0,
                    epoch,
                    error = %e,
                    "segment janitor: orphan sweep list failed (sealed epoch), retrying next tick"
                );
                continue;
            }
        };
        for id in ids {
            if id == row.object_id {
                continue; // the winning object itself
            }
            if let Err(e) = store.delete_local(&id).await {
                tracing::warn!(
                    id = %id,
                    error = %e,
                    "segment janitor: sealed-epoch orphan delete failed, retrying next tick"
                );
            }
        }
    }

    // (b) Open epochs on a live, streamed tablet: age-gated.
    for (tablet, t) in meta.tablets.iter() {
        let Some(table) = t.table.as_deref() else {
            continue;
        };
        let Some(spec) = meta.table_stream(table) else {
            continue; // not currently streamed — nothing to sweep here
        };
        let open_epoch = meta
            .stream_shards
            .range((*tablet, 0)..=(*tablet, u64::MAX))
            .next_back()
            .map_or(0, |((_, e), _)| e + 1);
        let prefix = segment::segment_id(table, &spec.label, tablet.0, open_epoch);
        let ids = match store.list_local(&format!("{prefix}/")).await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(
                    tablet = tablet.0,
                    epoch = open_epoch,
                    error = %e,
                    "segment janitor: orphan sweep list failed (open epoch), retrying next tick"
                );
                continue;
            }
        };
        for id in ids {
            let bytes = match store.get_local(&id).await {
                Ok(Some(bytes)) => bytes,
                Ok(None) => continue, // raced its own delete elsewhere; fine
                Err(e) => {
                    tracing::warn!(id = %id, error = %e, "segment janitor: orphan fetch failed, retrying next tick");
                    continue;
                }
            };
            let Ok(decoded) = segment::decode(&bytes) else {
                // Not a well-formed segment at all — leave it alone rather
                // than guess; this sweep only ever reaps what it can prove
                // is one of this subsystem's own abandoned attempts.
                continue;
            };
            let age_ms = now_ms.saturating_sub(decoded.header.seal_wall_ms);
            if age_ms >= u64::try_from(ORPHAN_GRACE.as_millis()).unwrap_or(u64::MAX)
                && let Err(e) = store.delete_local(&id).await
            {
                tracing::warn!(id = %id, error = %e, "segment janitor: open-epoch orphan delete failed, retrying next tick");
            }
        }
    }
}

/// `node` is a current, `Active` cluster member.
fn is_active(meta: &Metadata, node: &NodeId) -> bool {
    meta.members.get(node).map(|m| m.status) == Some(NodeStatus::Active)
}

/// The orphan-reap phase's own regression suite (ADR 0042 §10/ADR 0043 §A3
/// as-built amendment, point 6 of the delivery spec: "orphan reaping and
/// grace window"). In-crate (not `tests/`) because it needs `reap_orphans`
/// and `SegmentStoreHandle`'s local-only accessors, both `pub(crate)` —
/// the same reason `index_drain.rs`'s own `stream_sealer_tests` lives
/// beside its implementation. Built directly over a bare `FsSegmentStore`
/// plus a hand-`.apply()`-driven `Metadata` (no live control `RaftNode`
/// needed at all, mirroring `animus-test`'s own reconciler/lineage-corpus
/// technique) rather than a full node bring-up — `reap_orphans` only
/// depends on a `&SegmentStoreHandle` and a `&Metadata` snapshot, so this
/// is the lightest harness that actually exercises it.
#[cfg(test)]
mod orphan_reap_tests {
    use super::*;
    use animus_control::{ApplyOutcome, ColumnType, StreamSpec, StreamViewType, TableSchema};
    use animus_env::FsSegmentStore;
    use animus_tablet::KeyRange;

    fn tmp_store() -> (tempfile::TempDir, crate::SegmentStoreHandle) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FsSegmentStore::new(dir.path());
        (dir, crate::SegmentStoreHandle::Fs(store))
    }

    /// A schema with streaming enabled and one live tablet — the minimum
    /// `Metadata` shape [`reap_orphans`]'s open-epoch sub-case needs to
    /// even consider a tablet worth sweeping.
    fn base_meta_with_stream(table: &str, label: &str, tablet: TabletId) -> Metadata {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: table.to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied,
            "test setup: create schema"
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableStream {
                table: table.to_owned(),
                spec: Some(StreamSpec {
                    view_type: StreamViewType::NewAndOldImages,
                    label: label.to_owned(),
                }),
            }),
            ApplyOutcome::Applied,
            "test setup: enable stream"
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet,
                table: Some(table.to_owned()),
                range: KeyRange::whole(),
                replicas: Vec::new(),
            }),
            ApplyOutcome::Applied,
            "test setup: create tablet"
        );
        m
    }

    /// (a) Sealed-epoch sub-case: once a row exists, every OTHER object at
    /// that shard's own prefix is a proven orphan — reaped immediately,
    /// with no age check at all (the winning object survives untouched
    /// regardless of `now_ms`).
    #[tokio::test]
    async fn sealed_epoch_orphan_is_reaped_immediately_no_age_check() {
        let (_dir, store) = tmp_store();
        let table = "orders";
        let label = "L1";
        let tablet = TabletId(1);
        let mut meta = base_meta_with_stream(table, label, tablet);

        let prefix = segment::segment_id(table, label, tablet.0, 0);
        let winner_id = format!("{prefix}/winner");
        let orphan_id = format!("{prefix}/orphan");
        store
            .put_sealed(&winner_id, b"winner-bytes")
            .await
            .expect("put winner");
        store
            .put_sealed(&orphan_id, b"orphan-bytes")
            .await
            .expect("put orphan");

        assert_eq!(
            meta.apply(&MetaCommand::SealStreamShard {
                table: table.to_owned(),
                label: label.to_owned(),
                tablet,
                epoch: 0,
                view_type: StreamViewType::NewAndOldImages,
                hlc_range: (0, 100),
                count: 1,
                seal_wall_ms: 1_000,
                replicas: Vec::new(),
                object_id: winner_id.clone(),
            }),
            ApplyOutcome::Applied,
            "test setup: seal epoch 0 with the winning object"
        );

        // `now_ms` is deliberately tiny (younger than any real grace
        // window) — the sealed-epoch sub-case must reap regardless.
        reap_orphans(&store, &meta, 1_500).await;

        assert_eq!(
            store.get_local(&winner_id).await.expect("get winner"),
            Some(b"winner-bytes".to_vec()),
            "the catalog's own winning object must never be touched"
        );
        assert_eq!(
            store.get_local(&orphan_id).await.expect("get orphan"),
            None,
            "a sealed epoch's own non-winning object must be reaped immediately"
        );
    }

    /// (b) Open-epoch sub-case: with no row yet, an orphan candidate's own
    /// age (from its encoded `seal_wall_ms`) gates reaping — young enough
    /// survives, past [`ORPHAN_GRACE`] is reaped.
    #[tokio::test]
    async fn open_epoch_orphan_respects_the_grace_window() {
        let (_dir, store) = tmp_store();
        let table = "orders";
        let label = "L1";
        let tablet = TabletId(1);
        let meta = base_meta_with_stream(table, label, tablet);
        // No row sealed yet anywhere — epoch 0 is still open.

        let prefix = segment::segment_id(table, label, tablet.0, 0);
        let young_id = format!("{prefix}/young-attempt");
        let old_id = format!("{prefix}/old-attempt");

        let now_ms = 10_000_000u64;
        let grace_ms = u64::try_from(ORPHAN_GRACE.as_millis()).expect("grace fits u64");

        let young_header = segment::SegmentHeader {
            table: table.to_owned(),
            label: label.to_owned(),
            shard_id: segment::shard_id(tablet.0, 0),
            tablet: tablet.0,
            epoch: 0,
            parent_shard_id: None,
            hlc_range: (0, 100),
            count: 0,
            seal_wall_ms: now_ms - grace_ms / 2, // well within the grace window
        };
        let young_bytes = segment::encode(&young_header, &[]);
        let mut old_header = young_header;
        old_header.seal_wall_ms = now_ms - grace_ms - 1_000; // past the grace window
        let old_bytes = segment::encode(&old_header, &[]);

        store
            .put_sealed(&young_id, &young_bytes)
            .await
            .expect("put young");
        store
            .put_sealed(&old_id, &old_bytes)
            .await
            .expect("put old");

        reap_orphans(&store, &meta, now_ms).await;

        assert_eq!(
            store.get_local(&young_id).await.expect("get young"),
            Some(young_bytes),
            "an in-flight-plausible orphan must survive the grace window"
        );
        assert_eq!(
            store.get_local(&old_id).await.expect("get old"),
            None,
            "an orphan past the grace window must be reaped"
        );
    }
}
