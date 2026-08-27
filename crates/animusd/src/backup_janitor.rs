//! The on-demand backup **janitor** (ADR 0059 §3, Train 1 PR④): a distinct,
//! control-plane-**leader**-only background loop — mirroring
//! [`crate::segment_janitor`]'s own shape exactly — that reclaims a backup's
//! store objects once it has been marked for deletion (an operator's
//! `DeleteBackup` wire call, `animusd::dynamo::delete_backup`, which proposes
//! [`MetaCommand::MarkBackupDeleted`]) or has failed (`MetaCommand::
//! FailBackup`, proposed by the completion aggregator, `backup_completion.rs`,
//! past a stuck-`Creating` timeout), then removes the catalog row itself.
//!
//! ## Who runs this, and the control-only-leader gap
//!
//! Spawned unconditionally on every node shape that can ever become the
//! control-plane leader (`BoundNode::start_with_streams` — combined — and
//! `BoundControlNode::start_control_with` — control-only, ADR 0035) — the
//! same "run everywhere, self-gate on `ctx.edge.leader_handle()`" pattern
//! `segment_janitor_loop`/`backup_completion_loop` already use. **Never**
//! spawned on a data-only node (`BoundDataNode`), which never registers a
//! local control `RaftNode` at all, so `leader_handle()` there is
//! permanently `None`.
//!
//! **Documented scope gap, identical shape to `segment_janitor.rs`'s own.**
//! Object reclaim needs a [`crate::BackupStoreHandle`], which only exists on
//! a node with a data role (`crate::DataRole`); a **control-only** leader (a
//! genuine ADR 0035 split deployment) skips this loop's whole reclaim step
//! every tick it leads — a `DeleteBackup`-marked or `Failed` row simply sits
//! un-reclaimed until a data-role node becomes the control leader instead.
//! Marking itself (`MarkBackupDeleted`) needs only `Metadata` and is not this
//! loop's job at all — it is proposed directly by the wire edge
//! (`animusd::dynamo::delete_backup`) or the completion aggregator
//! (`backup_completion.rs`'s own `FailBackup`), neither of which is
//! control-plane-leader-only, exactly like `ExpireStreamShards`'s own
//! mark-vs-reclaim split (`segment_janitor.rs`'s module doc).
//!
//! ## Reclaim is local-only — a deliberate Train 1 simplification
//!
//! Unlike the segment janitor's own cataloged-row reclaim (`StreamShardRow::
//! replicas`, a durable record of exactly which nodes hold a copy, pushed at
//! seal time), **no backup object carries a recorded replica list** —
//! `backup_capture.rs`'s chunk `put`s and `backup_completion.rs`'s manifest
//! `put` both discard [`crate::BackupStoreHandle::put`]'s own returned
//! replica set (ADR 0059 §1's `ClusterSegmentStore` chooses a fresh `K`-node
//! target set per `put`, so even recording *one* such set per backup would
//! not necessarily cover every object's own target set if cluster membership
//! shifted mid-capture). Reconstructing which objects exist at all is
//! likewise not possible from the catalog alone: a tablet's own completion
//! record ([`animus_control::BackupTabletProgress`]) carries total bytes,
//! not a chunk count, so there is no way to enumerate `backup/{backup_id}/
//! {tablet}/{chunk}` object ids without asking the store.
//!
//! This janitor's reclaim step therefore does what ADR 0059 §3 explicitly
//! licenses for exactly this situation: **`SegmentStore::list()` as a
//! debug/sweep tool** ([`crate::BackupStoreHandle::list_local`]), scoped to
//! this backup's own `backup/{backup_id}/` prefix, deleting every id found
//! on **this node's own local** backup directory
//! ([`crate::BackupStoreHandle::delete_local`]) — the identical "local-only,
//! converges as leadership rotates across nodes holding copies" shape
//! `segment_janitor.rs`'s own orphan sweep already uses, generalized here
//! from "extra, uncataloged objects" to "this backup's objects" outright,
//! since there is no stronger cataloged-replica alternative to reach for.
//!
//! **Named residual, not silently accepted**: for a `Cluster`-backed store
//! whose control-plane leader never happens to be one of the `K` nodes that
//! actually hold a given backup's objects, this loop's local sweep finds
//! nothing on that leader and finalizes the row (removes it from `Metadata`)
//! the very first tick it observes the mark — before any node that *does*
//! hold a copy ever gets a chance to sweep its own. Those copies then become
//! permanent, uncataloged orphans (no row is left to name their prefix for a
//! future sweep to rediscover). This is a real gap on a cluster larger than
//! `ClusterSegmentStore::DEFAULT_K` (3) — on a cluster at or below that size
//! every node is always a target, so the gap does not manifest in practice —
//! and is left here, explicitly, as Train 1's own accepted simplification:
//! closing it needs either a per-object `replicas` list (mirroring
//! `StreamShardRow::replicas`) or a cluster-wide list primitive for
//! `ClusterSegmentStore` (neither exists today), both out of this PR's scope.
//! See `docs/engineering-lessons.md` for the fuller note.
//!
//! ## On-demand backups never auto-expire
//!
//! This loop has **no retention clock** — an `Available` backup is reclaimed
//! only by an explicit `DeleteBackup` (which lands here as `Expired`, the
//! mark phase's own terminal state, ADR 0043 §A9's mold reused verbatim by
//! ADR 0059 §3) or a completion-aggregator `FailBackup`. Continuous backups'
//! own retention window (PITR, ADR 0059 §9/§10) is Train 3's concern, a
//! wholly separate consumer of the same underlying mechanism.

use std::time::Duration;

use animus_control::{BackupStatus, RaftNode};
use animus_cp_data::backup as backup_codec;
use animus_env::ProdEnv;

use crate::ClientCtx;
use crate::MetaCommand;

/// This loop's tick cadence — matches `segment_janitor.rs`/`backup_completion.rs`'s
/// own `_INTERVAL` constants: cheap per-tick work, and a fast tick keeps this
/// crate's own converged-or-timeout tests from becoming the slow part of the
/// corpus.
const BACKUP_JANITOR_INTERVAL: Duration = Duration::from_millis(200);

/// The control-plane-leader-only background loop (ADR 0059 §3) — see the
/// module doc for who spawns this, why it self-gates every tick rather than
/// running only on whichever node happens to lead right now, and the
/// documented control-only-leader / local-only-reclaim scope gaps.
pub(crate) async fn backup_janitor_loop(ctx: ClientCtx) {
    loop {
        tokio::time::sleep(BACKUP_JANITOR_INTERVAL).await;
        let Some(leader) = ctx.edge.leader_handle() else {
            continue;
        };
        backup_janitor_tick(&ctx, &leader).await;
    }
}

/// One tick's whole decision — see the module doc's "Reclaim is local-only"
/// section for why this is a local sweep rather than a cataloged-replica
/// delete.
async fn backup_janitor_tick(ctx: &ClientCtx, leader: &RaftNode<ProdEnv>) {
    let meta = leader.metadata();
    let to_reclaim: Vec<String> = meta
        .backups
        .iter()
        .filter(|(_, row)| {
            matches!(
                row.status,
                BackupStatus::Expired | BackupStatus::Failed { .. }
            )
        })
        .map(|(id, _)| id.clone())
        .collect();
    if to_reclaim.is_empty() {
        return;
    }
    let Some(data) = ctx.data_opt() else {
        return; // control-only leader — see the module doc's gap
    };
    for backup_id in to_reclaim {
        let prefix = backup_codec::backup_prefix(&backup_id);
        let ids = match data.backup_store.list_local(&prefix).await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(
                    backup_id,
                    error = %e,
                    "backup janitor: local object list failed, retrying next tick"
                );
                continue;
            }
        };
        let mut all_deleted = true;
        for id in &ids {
            if let Err(e) = data.backup_store.delete_local(id).await {
                tracing::warn!(
                    backup_id,
                    id,
                    error = %e,
                    "backup janitor: local object delete failed, retrying next tick"
                );
                all_deleted = false;
            }
        }
        if !all_deleted {
            continue; // leave the row for the next tick's retry
        }
        // Every object this node's own local store held for this backup is
        // gone (possibly none at all — see the module doc's local-only
        // caveat) — finalize by removing the catalog row itself, the
        // existing, unmodified `DeleteBackup` command (ADR 0059 §3's own
        // two-phase mold: mark, then reclaim objects, then remove the row).
        let _ = leader.propose(MetaCommand::DeleteBackup { backup_id });
    }
}

#[cfg(test)]
mod tests {
    use animus_control::{ApplyOutcome, ColumnType, Metadata, TableSchema};
    use animus_env::{FsSegmentStore, SegmentStore};
    use animus_tablet::{KeyRange, TabletId};

    use super::*;

    /// A minimal `Metadata` with one table, one tablet, and a completed
    /// backup — the setup every scenario below starts from.
    fn base_meta_with_backup(backup_id: &str) -> Metadata {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("orders".to_owned()),
                range: KeyRange::whole(),
                replicas: Vec::new(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: backup_id.to_owned(),
                table: "orders".to_owned(),
                created_wall_ms: 1_000,
                backup_name: "nightly".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: backup_id.to_owned(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CompleteBackup {
                backup_id: backup_id.to_owned(),
            }),
            ApplyOutcome::Applied
        );
        m
    }

    /// The pure per-backup decision this tick applies, factored out of
    /// [`backup_janitor_tick`] (which needs a live `RaftNode`/`ClientCtx`) so
    /// the local-sweep-then-finalize logic is unit-testable directly against
    /// a bare [`FsSegmentStore`] and a hand-`.apply()`-driven [`Metadata`] —
    /// the same lightweight-harness technique `segment_janitor.rs`'s own
    /// `orphan_reap_tests` uses.
    async fn reclaim_one(store: &FsSegmentStore, backup_id: &str) -> bool {
        let prefix = backup_codec::backup_prefix(backup_id);
        let ids = store.list(&prefix).await.expect("list");
        let mut all_deleted = true;
        for id in &ids {
            if store.delete(id).await.is_err() {
                all_deleted = false;
            }
        }
        all_deleted
    }

    /// A `DeleteBackup`-marked (`Expired`) row's local objects are all
    /// reclaimed, and the sweep reports success (the janitor's own signal to
    /// finalize by removing the row).
    #[tokio::test]
    async fn expired_backups_own_local_objects_are_all_reclaimed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FsSegmentStore::new(dir.path());
        let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/abc";
        let mut meta = base_meta_with_backup(backup_id);
        assert_eq!(
            meta.apply(&MetaCommand::MarkBackupDeleted {
                backup_id: backup_id.to_owned(),
            }),
            ApplyOutcome::Applied
        );

        let manifest_id = backup_codec::backup_manifest_object_id(backup_id);
        let chunk_id = backup_codec::backup_data_object_id(backup_id, 1, 0);
        store
            .put(&manifest_id, b"manifest-bytes")
            .await
            .expect("put manifest");
        store
            .put(&chunk_id, b"chunk-bytes")
            .await
            .expect("put chunk");

        let all_deleted = reclaim_one(&store, backup_id).await;
        assert!(all_deleted);
        assert_eq!(store.get(&manifest_id).await.expect("get"), None);
        assert_eq!(store.get(&chunk_id).await.expect("get"), None);
    }

    /// An orphan object under a **different** backup's own prefix is never
    /// touched by this backup's own sweep — the fixed `backup/{id}/`
    /// namespace ([`backup_codec::backup_prefix`]) is what keeps one
    /// backup's reclaim from ever reaching another's objects.
    #[tokio::test]
    async fn reclaim_never_touches_a_different_backups_objects() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FsSegmentStore::new(dir.path());
        let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/abc";
        let other_id = "arn:aws:dynamodb:animus:0:table/orders/backup/def";
        let mut meta = base_meta_with_backup(backup_id);
        assert_eq!(
            meta.apply(&MetaCommand::MarkBackupDeleted {
                backup_id: backup_id.to_owned(),
            }),
            ApplyOutcome::Applied
        );

        let this_manifest = backup_codec::backup_manifest_object_id(backup_id);
        let other_manifest = backup_codec::backup_manifest_object_id(other_id);
        store.put(&this_manifest, b"mine").await.expect("put");
        store.put(&other_manifest, b"not mine").await.expect("put");

        let all_deleted = reclaim_one(&store, backup_id).await;
        assert!(all_deleted);
        assert_eq!(store.get(&this_manifest).await.expect("get"), None);
        assert_eq!(
            store.get(&other_manifest).await.expect("get"),
            Some(b"not mine".to_vec()),
            "a different backup's own object must survive"
        );
    }

    /// A backup with no objects ever written locally (this node never held a
    /// copy — the module doc's own named residual) sweeps as an immediate,
    /// vacuous success: `all_deleted` is `true` on an empty candidate list,
    /// which is exactly the local-only-reclaim design's documented
    /// consequence, pinned here as a regression against accidentally
    /// "fixing" this into a false negative (which would instead wedge every
    /// backup delete on a leader that never happens to hold a copy).
    #[tokio::test]
    async fn a_backup_with_no_local_objects_still_reports_reclaimed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FsSegmentStore::new(dir.path());
        let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/abc";
        let mut meta = base_meta_with_backup(backup_id);
        assert_eq!(
            meta.apply(&MetaCommand::MarkBackupDeleted {
                backup_id: backup_id.to_owned(),
            }),
            ApplyOutcome::Applied
        );

        let all_deleted = reclaim_one(&store, backup_id).await;
        assert!(all_deleted);
    }

    /// A `Failed` backup's own objects (a partial capture that never
    /// completed) are reclaimed exactly like an `Expired` one — the janitor
    /// treats both statuses identically (`backup_janitor_tick`'s own filter).
    #[tokio::test]
    async fn failed_backups_own_partial_objects_are_reclaimed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FsSegmentStore::new(dir.path());
        let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/abc";
        let mut meta = Metadata::default();
        assert_eq!(
            meta.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            meta.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("orders".to_owned()),
                range: KeyRange::whole(),
                replicas: Vec::new(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            meta.apply(&MetaCommand::BeginBackup {
                backup_id: backup_id.to_owned(),
                table: "orders".to_owned(),
                created_wall_ms: 1_000,
                backup_name: "nightly".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            meta.apply(&MetaCommand::FailBackup {
                backup_id: backup_id.to_owned(),
                reason: "stuck".to_owned(),
            }),
            ApplyOutcome::Applied
        );

        let partial_chunk = backup_codec::backup_data_object_id(backup_id, 1, 0);
        store
            .put(&partial_chunk, b"partial")
            .await
            .expect("put partial chunk");

        let all_deleted = reclaim_one(&store, backup_id).await;
        assert!(all_deleted);
        assert_eq!(store.get(&partial_chunk).await.expect("get"), None);
    }

    /// **Crash mid-sweep resumes idempotently.** A first sweep reclaims one
    /// of two chunk objects (simulating a crash right after that delete but
    /// before the row was ever finalized — the row is deliberately left in
    /// place here, exactly as [`backup_janitor_tick`] leaves it when
    /// `all_deleted` comes back `false`); a second, independent sweep over
    /// the SAME still-`Expired` row finishes the job and converges to fully
    /// reclaimed — `SegmentStore::delete`'s own idempotent-on-absence
    /// contract is what makes re-sweeping an already-partially-reclaimed
    /// backup safe to retry from scratch rather than needing its own resume
    /// cursor.
    #[tokio::test]
    async fn crash_mid_sweep_resumes_idempotently_on_the_next_tick() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FsSegmentStore::new(dir.path());
        let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/abc";
        let mut meta = base_meta_with_backup(backup_id);
        assert_eq!(
            meta.apply(&MetaCommand::MarkBackupDeleted {
                backup_id: backup_id.to_owned(),
            }),
            ApplyOutcome::Applied
        );

        let manifest_id = backup_codec::backup_manifest_object_id(backup_id);
        let chunk_id = backup_codec::backup_data_object_id(backup_id, 1, 0);
        store
            .put(&manifest_id, b"manifest")
            .await
            .expect("put manifest");
        store.put(&chunk_id, b"chunk").await.expect("put chunk");

        // "Crash" mid-sweep: only the manifest object is reclaimed this
        // tick (the chunk delete never ran — the driver died first). The
        // row stays `Expired`; nothing finalizes it yet.
        store.delete(&manifest_id).await.expect("partial reclaim");
        assert_eq!(store.get(&manifest_id).await.expect("get"), None);
        assert_eq!(
            store.get(&chunk_id).await.expect("get"),
            Some(b"chunk".to_vec()),
            "the chunk survives the simulated crash, exactly as a real \
             mid-sweep death would leave it"
        );
        assert_eq!(
            meta.backup(backup_id).unwrap().status,
            BackupStatus::Expired,
            "still marked, not yet finalized"
        );

        // The next tick's sweep re-lists from scratch (no resume cursor
        // needed) and finishes the job.
        let all_deleted = reclaim_one(&store, backup_id).await;
        assert!(all_deleted);
        assert_eq!(store.get(&chunk_id).await.expect("get"), None);

        // A third sweep over an already-fully-reclaimed backup is a
        // harmless no-op (nothing left to list), the same convergence
        // property idempotency at the object layer already gives.
        let all_deleted_again = reclaim_one(&store, backup_id).await;
        assert!(all_deleted_again);
    }
}
