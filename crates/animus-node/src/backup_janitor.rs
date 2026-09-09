//! The on-demand backup **janitor** (ADR 0059 §3, Train 1 PR④), moved here
//! by ADR 0061 rung C2 — a control-plane-**leader**-only background loop,
//! mirroring `backup_completion`'s own shape, that reclaims a backup's
//! store objects once it has been marked for deletion or has failed, then
//! removes the catalog row itself. See `crate::host::{ControlLeaderHost,
//! BackupObjectStore}` for the two capabilities this loop needs.
//!
//! ## Reclaim is local-only — a deliberate Train 1 simplification
//!
//! No backup object carries a recorded replica list, so this janitor's
//! reclaim step does what ADR 0059 §3 explicitly licenses for exactly this
//! situation: [`BackupObjectStore::backup_list_local`] as a debug/sweep
//! tool, scoped to this backup's own `backup/{backup_id}/` prefix, deleting
//! every id found via [`BackupObjectStore::backup_delete_local`] — local-
//! only, converging as control leadership rotates across nodes holding
//! copies. See `animusd/CLAUDE.md`'s `backup_janitor.rs` entry for the full
//! named residual this accepts (unchanged by this move).
//!
//! ## Who runs this, and the control-only-leader gap
//!
//! Every method on [`BackupObjectStore`] answers `None` on a control-only
//! leader (no data role provisions a backup-store handle there) — this
//! loop's whole reclaim step is then simply skipped that tick, exactly the
//! documented gap `backup_completion`/`segment_janitor` (not moved) share.
//!
//! ## On-demand backups never auto-expire
//!
//! This loop has **no retention clock** — an `Available` backup is
//! reclaimed only by an explicit `DeleteBackup` (landing here `Expired`) or
//! a completion-aggregator `FailBackup`.
//!
//! ## Progress reporting (roadmap U-07)
//!
//! [`JanitorProgress`] is a small, `Env`-free snapshot of this loop's own
//! state — phase, last tick, and cumulative counters — published through
//! [`crate::host::BackupJanitorProgressHost`] at each phase transition
//! below. `animusd::ClientCtx` backs it with an `Arc<Mutex<JanitorProgress>>`
//! (`std::sync::Mutex`, matching every other short lock/mutate/drop shared
//! diagnostic state in that crate — never held across an `.await`), read by
//! `GET /admin/backup-store`. Every timestamp is [`animus_env::Env::now`]-
//! derived, never a wall clock (ADR 0003) — this loop, and its progress
//! snapshot, stay meaningful under `SimEnv` too, even though nothing reads
//! it there today.

use std::time::Duration;

use animus_control::BackupStatus;
use animus_cp_data::backup as backup_codec;
use animus_env::Env;

use crate::host::{BackupJanitorProgressHost, BackupObjectStore, ControlLeaderHost};

/// This loop's production tick cadence — every real spawn site
/// (`animusd::backup_janitor::backup_janitor_loop`'s two `spawn_common_tail`
/// callers) passes this explicitly. [`backup_janitor_loop`] itself takes
/// `interval` as a parameter (ADR 0061 rung I C-09 PR 3's follow-on,
/// 2026-09-09 — mirrors [`ttl_reaper_loop`](crate::ttl_reaper::ttl_reaper_loop)'s
/// own shape) so a fixture with its own cadence needs (a `SimCluster`-shaped
/// harness driving many nodes over compressed virtual time) can pass a
/// coarser one without this constant's own production value ever changing.
pub const BACKUP_JANITOR_INTERVAL: Duration = Duration::from_millis(200);

/// One phase of the backup janitor's own tick (roadmap U-07) — rendered on
/// `GET /admin/backup-store`. See the module doc's "Reclaim is local-only"
/// section for what each non-idle phase actually does.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JanitorPhase {
    /// Not currently the control leader, or nothing needs reclaiming as of
    /// the last tick.
    #[default]
    Idle,
    /// Listing/deleting a backup's own local objects.
    Reclaiming,
    /// Every local object for a backup is gone; proposing `DeleteBackup` to
    /// finalize (remove) its catalog row.
    RemovingRow,
}

/// A snapshot of the backup janitor's own progress (roadmap U-07) — see the
/// "Progress reporting" section of the module doc for how this is
/// published and consumed. Every counter is cumulative (never reset) for
/// as long as the host process lives; `Default` is the correct initial
/// state for a node that has never (yet) been the control leader.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JanitorProgress {
    /// This tick's phase — see [`JanitorPhase`].
    pub phase: JanitorPhase,
    /// `env.now()` at the start of this loop's most recently observed
    /// tick, in milliseconds — `None` before the first tick has ever run
    /// (or on a node that has never been the control leader).
    pub last_tick_at_ms: Option<u64>,
    /// Cumulative count of backup rows this janitor has looked at across
    /// every tick since this node became control leader.
    pub backups_seen: u64,
    /// Cumulative count of local objects reclaimed (deleted) across every
    /// tick since this node became control leader.
    pub objects_reclaimed: u64,
    /// The most recent list/delete error observed, if any — cleared once a
    /// full tick completes with no error.
    pub last_error: Option<String>,
    /// The backup id this janitor is (or was, as of the last tick) actively
    /// reclaiming — `None` when idle.
    pub current_backup_id: Option<String>,
}

/// The control-plane-leader-only background loop (ADR 0059 §3) — see the
/// module doc for the documented control-only-leader / local-only-reclaim
/// scope gaps.
///
/// `interval` is this loop's own tick cadence — every real production spawn
/// site passes [`BACKUP_JANITOR_INTERVAL`] explicitly (ADR 0061 rung I C-09
/// PR 3's follow-on, 2026-09-09); a fixture with its own cadence needs may
/// pass something else.
pub async fn backup_janitor_loop<E, H>(env: E, host: H, interval: Duration)
where
    E: Env,
    H: ControlLeaderHost<E> + BackupObjectStore + BackupJanitorProgressHost,
{
    loop {
        env.sleep(interval).await;
        let now_ms = env.now().0 / 1_000_000;
        let Some(leader) = host.control_leader() else {
            // Not (or no longer) the control leader — report idle rather
            // than leaving a stale, possibly-mid-reclaim phase behind.
            host.update_backup_janitor_progress(&mut |p| {
                p.phase = JanitorPhase::Idle;
                p.last_tick_at_ms = Some(now_ms);
                p.current_backup_id = None;
            });
            continue;
        };
        backup_janitor_tick(&host, &leader, now_ms).await;
    }
}

/// One tick's whole decision — see the module doc's "Reclaim is local-only"
/// section.
async fn backup_janitor_tick<E, H>(host: &H, leader: &animus_control::RaftNode<E>, now_ms: u64)
where
    E: Env,
    H: BackupObjectStore + BackupJanitorProgressHost,
{
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
        host.update_backup_janitor_progress(&mut |p| {
            p.phase = JanitorPhase::Idle;
            p.last_tick_at_ms = Some(now_ms);
            p.current_backup_id = None;
            p.last_error = None;
        });
        return;
    }
    host.update_backup_janitor_progress(&mut |p| {
        p.last_tick_at_ms = Some(now_ms);
        p.backups_seen = p.backups_seen.saturating_add(to_reclaim.len() as u64);
    });
    let mut tick_error: Option<String> = None;
    for backup_id in to_reclaim {
        host.update_backup_janitor_progress(&mut |p| {
            p.phase = JanitorPhase::Reclaiming;
            p.current_backup_id = Some(backup_id.clone());
        });
        let prefix = backup_codec::backup_prefix(&backup_id);
        let ids = match host.backup_list_local(&prefix).await {
            None => return, // control-only leader — see the module doc's gap
            Some(Ok(ids)) => ids,
            Some(Err(e)) => {
                tracing::warn!(
                    backup_id,
                    error = %e,
                    "backup janitor: local object list failed, retrying next tick"
                );
                tick_error = Some(e.to_string());
                continue;
            }
        };
        let mut all_deleted = true;
        for id in &ids {
            match host.backup_delete_local(id).await {
                None => return, // control-only leader
                Some(Ok(())) => {
                    host.update_backup_janitor_progress(&mut |p| {
                        p.objects_reclaimed = p.objects_reclaimed.saturating_add(1);
                    });
                }
                Some(Err(e)) => {
                    tracing::warn!(
                        backup_id,
                        id,
                        error = %e,
                        "backup janitor: local object delete failed, retrying next tick"
                    );
                    tick_error = Some(e.to_string());
                    all_deleted = false;
                }
            }
        }
        if !all_deleted {
            continue; // leave the row for the next tick's retry
        }
        host.update_backup_janitor_progress(&mut |p| {
            p.phase = JanitorPhase::RemovingRow;
        });
        let _ = leader.propose(animus_control::MetaCommand::DeleteBackup { backup_id });
    }
    host.update_backup_janitor_progress(&mut |p| {
        p.phase = JanitorPhase::Idle;
        p.current_backup_id = None;
        p.last_error = tick_error.clone();
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use animus_control::raft::ProposeResult;
    use animus_control::{ColumnType, MetaCommand, RaftNode, TableSchema};
    use animus_env::{EnvExt, NodeId, nid};
    use animus_sim::{SimEnv, Simulator};
    use animus_storage::MemoryEngine;
    use animus_tablet::{KeyRange, TabletId};
    use async_trait::async_trait;

    use super::*;
    use crate::host::{BackupJanitorProgressHost, BackupObjectStore};

    /// A synthetic [`BackupObjectStore`] — a plain in-memory object map, no
    /// real filesystem. `control_only` toggles the `None`-everywhere gap
    /// this loop documents for a control-only leader. Also backs
    /// [`BackupJanitorProgressHost`] with its own `JanitorProgress` slot, so
    /// this double can drive and observe the progress-reporting mechanism
    /// too (roadmap U-07).
    #[derive(Clone, Default)]
    struct FakeBackupStore(
        Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
        Arc<Mutex<JanitorProgress>>,
    );

    impl FakeBackupStore {
        fn put_raw(&self, id: &str, bytes: &[u8]) {
            self.0.lock().unwrap().insert(id.to_owned(), bytes.to_vec());
        }
        fn has(&self, id: &str) -> bool {
            self.0.lock().unwrap().contains_key(id)
        }
        fn progress(&self) -> JanitorProgress {
            self.1.lock().unwrap().clone()
        }
    }

    impl BackupJanitorProgressHost for FakeBackupStore {
        fn update_backup_janitor_progress(&self, update: &mut dyn FnMut(&mut JanitorProgress)) {
            update(&mut self.1.lock().unwrap());
        }
    }

    #[async_trait]
    impl BackupObjectStore for FakeBackupStore {
        async fn backup_put(&self, id: &str, bytes: &[u8]) -> Option<std::io::Result<Vec<NodeId>>> {
            self.put_raw(id, bytes);
            Some(Ok(Vec::new()))
        }
        async fn backup_list_local(&self, prefix: &str) -> Option<std::io::Result<Vec<String>>> {
            Some(Ok(self
                .0
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect()))
        }
        async fn backup_delete_local(&self, id: &str) -> Option<std::io::Result<()>> {
            self.0.lock().unwrap().remove(id);
            Some(Ok(()))
        }
        async fn backup_delete_at(
            &self,
            _replicas: &[NodeId],
            id: &str,
        ) -> Option<std::io::Result<()>> {
            self.0.lock().unwrap().remove(id);
            Some(Ok(()))
        }
    }

    /// A control-only leader: every [`BackupObjectStore`] method answers
    /// `None`, mirroring `ClientCtx::data_opt()` returning `None` in
    /// `animusd`.
    #[derive(Clone, Default)]
    struct ControlOnlyStore;

    #[async_trait]
    impl BackupObjectStore for ControlOnlyStore {
        async fn backup_put(
            &self,
            _id: &str,
            _bytes: &[u8],
        ) -> Option<std::io::Result<Vec<NodeId>>> {
            None
        }
        async fn backup_list_local(&self, _prefix: &str) -> Option<std::io::Result<Vec<String>>> {
            None
        }
        async fn backup_delete_local(&self, _id: &str) -> Option<std::io::Result<()>> {
            None
        }
        async fn backup_delete_at(
            &self,
            _replicas: &[NodeId],
            _id: &str,
        ) -> Option<std::io::Result<()>> {
            None
        }
    }

    impl BackupJanitorProgressHost for ControlOnlyStore {
        fn update_backup_janitor_progress(&self, _update: &mut dyn FnMut(&mut JanitorProgress)) {
            // A control-only leader has nowhere to publish progress in
            // production either (no `ClientCtx` field is reachable from a
            // pure `RaftNode` context) — a no-op here, matching every other
            // method on this test double.
        }
    }

    fn single_voter(seed: u64) -> (Simulator, RaftNode<SimEnv>) {
        let sim = Simulator::new(seed);
        let node = RaftNode::start(sim.env(nid(0)), vec![nid(0)], MemoryEngine::new());
        (sim, node)
    }

    fn accepted(r: ProposeResult) -> bool {
        matches!(r, ProposeResult::Accepted { .. })
    }

    /// Drives a backup all the way to `Available` (one table, one tablet)
    /// — mirrors `animus-control`'s own `base_meta_with_backup` fixture
    /// shape.
    fn complete_a_backup(node: &RaftNode<SimEnv>, sim: &mut Simulator, backup_id: &str) {
        assert!(accepted(node.propose(MetaCommand::CreateTableSchema {
            table: "orders".to_owned(),
            schema: TableSchema::simple("id", ColumnType::String),
        })));
        assert!(accepted(node.propose(MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(0)],
        })));
        assert!(accepted(node.propose(MetaCommand::BeginBackup {
            backup_id: backup_id.to_owned(),
            table: "orders".to_owned(),
            created_wall_ms: 1_000,
            backup_name: "nightly".to_owned(),
            pitr_base: false,
        })));
        assert!(accepted(node.propose(
            MetaCommand::RecordBackupTabletComplete {
                backup_id: backup_id.to_owned(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            }
        )));
        assert!(accepted(node.propose(MetaCommand::CompleteBackup {
            backup_id: backup_id.to_owned(),
        })));
        sim.run_for(Duration::from_millis(50));
    }

    /// A `MarkBackupDeleted`-marked (`Expired`) backup's local objects are
    /// all reclaimed and the row is finalized (removed) — the end-to-end
    /// property `animusd`'s own `dynamo_backup.rs` e2e test also covers
    /// through a real cluster; this proves the same convergence
    /// deterministically, one tick at a time, against a synthetic store.
    #[test]
    fn expired_backups_own_local_objects_are_reclaimed_and_the_row_is_removed() {
        let seed = 0xBACF_0001;
        let (mut sim, node) = single_voter(seed);
        sim.run_for(Duration::from_millis(500));
        assert!(node.is_leader(), "seed={seed}");

        let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/abc";
        complete_a_backup(&node, &mut sim, backup_id);
        assert!(accepted(node.propose(MetaCommand::MarkBackupDeleted {
            backup_id: backup_id.to_owned(),
        })));
        sim.run_for(Duration::from_millis(50));

        let store = FakeBackupStore::default();
        let manifest_id = backup_codec::backup_manifest_object_id(backup_id);
        let chunk_id = backup_codec::backup_data_object_id(backup_id, 1, 0);
        store.put_raw(&manifest_id, b"manifest-bytes");
        store.put_raw(&chunk_id, b"chunk-bytes");

        let env = node.env().clone();
        let (host, leader) = (store.clone(), node.clone());
        env.spawn_task(async move { backup_janitor_tick(&host, &leader, 0).await });
        sim.run_for(Duration::from_millis(50));

        assert!(!store.has(&manifest_id), "seed={seed}");
        assert!(!store.has(&chunk_id), "seed={seed}");
        assert!(
            node.metadata().backup(backup_id).is_none(),
            "the row must be finalized (removed) once every local object is gone (seed={seed})"
        );

        // Roadmap U-07: the progress snapshot reflects the whole tick —
        // ended idle, both local objects counted as reclaimed, one backup
        // seen, no error, and the just-finished backup's own id cleared.
        let progress = store.progress();
        assert_eq!(progress.phase, JanitorPhase::Idle, "seed={seed}");
        assert_eq!(progress.backups_seen, 1, "seed={seed}");
        assert_eq!(progress.objects_reclaimed, 2, "seed={seed}");
        assert_eq!(progress.last_error, None, "seed={seed}");
        assert_eq!(progress.current_backup_id, None, "seed={seed}");
        assert_eq!(progress.last_tick_at_ms, Some(0), "seed={seed}");
    }

    /// A backup with no locally-held objects at all (this node never held a
    /// copy — the local-only-reclaim design's documented residual) still
    /// converges immediately: an empty candidate list is a vacuous success,
    /// not a stuck row.
    #[test]
    fn a_backup_with_no_local_objects_still_finalizes() {
        let seed = 0xBACF_0002;
        let (mut sim, node) = single_voter(seed);
        sim.run_for(Duration::from_millis(500));
        assert!(node.is_leader(), "seed={seed}");

        let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/abc";
        complete_a_backup(&node, &mut sim, backup_id);
        assert!(accepted(node.propose(MetaCommand::MarkBackupDeleted {
            backup_id: backup_id.to_owned(),
        })));
        sim.run_for(Duration::from_millis(50));

        let store = FakeBackupStore::default(); // deliberately empty
        let env = node.env().clone();
        let (host, leader) = (store, node.clone());
        env.spawn_task(async move { backup_janitor_tick(&host, &leader, 0).await });
        sim.run_for(Duration::from_millis(50));

        assert!(
            node.metadata().backup(backup_id).is_none(),
            "an empty local sweep must still finalize the row (seed={seed})"
        );
    }

    /// A sweep never touches a *different* backup's own objects — the fixed
    /// `backup/{id}/` namespace is what keeps one backup's reclaim scoped.
    #[test]
    fn reclaim_never_touches_a_different_backups_objects() {
        let seed = 0xBACF_0003;
        let (mut sim, node) = single_voter(seed);
        sim.run_for(Duration::from_millis(500));
        assert!(node.is_leader(), "seed={seed}");

        let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/abc";
        let other_id = "arn:aws:dynamodb:animus:0:table/orders/backup/def";
        complete_a_backup(&node, &mut sim, backup_id);
        assert!(accepted(node.propose(MetaCommand::MarkBackupDeleted {
            backup_id: backup_id.to_owned(),
        })));
        sim.run_for(Duration::from_millis(50));

        let store = FakeBackupStore::default();
        let this_manifest = backup_codec::backup_manifest_object_id(backup_id);
        let other_manifest = backup_codec::backup_manifest_object_id(other_id);
        store.put_raw(&this_manifest, b"mine");
        store.put_raw(&other_manifest, b"not mine");

        let env = node.env().clone();
        let (host, leader) = (store.clone(), node.clone());
        env.spawn_task(async move { backup_janitor_tick(&host, &leader, 0).await });
        sim.run_for(Duration::from_millis(50));

        assert!(!store.has(&this_manifest), "seed={seed}");
        assert!(
            store.has(&other_manifest),
            "a different backup's own object must survive (seed={seed})"
        );
    }

    /// A control-only leader (every [`BackupObjectStore`] method answers
    /// `None`) skips reclaim entirely — the row stays `Expired`, never
    /// finalized, for as long as this loop only ever sees that host. This
    /// is the documented control-only-leader scope gap, pinned as a
    /// regression rather than left to drift silently.
    #[test]
    fn a_control_only_leader_never_finalizes_the_row() {
        let seed = 0xBACF_0004;
        let (mut sim, node) = single_voter(seed);
        sim.run_for(Duration::from_millis(500));
        assert!(node.is_leader(), "seed={seed}");

        let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/abc";
        complete_a_backup(&node, &mut sim, backup_id);
        assert!(accepted(node.propose(MetaCommand::MarkBackupDeleted {
            backup_id: backup_id.to_owned(),
        })));
        sim.run_for(Duration::from_millis(50));

        let env = node.env().clone();
        let leader = node.clone();
        env.spawn_task(async move {
            backup_janitor_tick(&ControlOnlyStore, &leader, 0).await;
        });
        sim.run_for(Duration::from_millis(50));

        assert_eq!(
            node.metadata().backup(backup_id).map(|r| r.status.clone()),
            Some(animus_control::BackupStatus::Expired),
            "a control-only leader must never finalize the row (seed={seed})"
        );
    }
}
