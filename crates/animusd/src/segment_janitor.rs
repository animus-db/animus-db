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

        let seg_id = segment::segment_id(&row.table, &row.label, tablet.0, *epoch);
        if row.replicas.is_empty() {
            // The single-directory `FsSegmentStore` opt-in's own convention
            // (ADR 0043 §A7b): no per-replica list, but a real object still
            // needs a real (local) delete — never treat "empty list" as
            // "nothing to delete."
            let Some(data) = ctx.data_opt() else {
                continue; // control-only leader — see the module doc's gap
            };
            match data.segment_store.delete_sealed(&[], &seg_id).await {
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
            .delete_sealed(&still_present, &seg_id)
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
        let seg_id = segment::segment_id(&row.table, &row.label, tablet.0, *epoch);
        let bytes = match data.segment_store.get_sealed(&alive, &seg_id).await {
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
            .repair_replicas(&seg_id, &bytes, &alive, target_k)
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
            });
        }
    }
}

/// `node` is a current, `Active` cluster member.
fn is_active(meta: &Metadata, node: &NodeId) -> bool {
    meta.members.get(node).map(|m| m.status) == Some(NodeStatus::Active)
}
