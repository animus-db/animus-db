//! On-demand backup **completion aggregator** (ADR 0059 §3/§4, Train 1
//! PR③), moved here by ADR 0061 rung C2 — a control-plane-**leader**-only
//! background loop driving every `Creating` backup to a terminal state.
//! See `crate::host::{ControlLeaderHost, BackupObjectStore}` for the two
//! capabilities this loop needs: a control-plane leader handle (for
//! `metadata()`/`propose()`, both already `E`-generic on `RaftNode<E>`
//! itself) and a durable `put` against the backup object store.
//!
//! ## The decision, each tick
//!
//! For every backup row currently `Creating`:
//!
//! - **Ready** (every one of its pinned tablets' own current live capture
//!   frontier has reported — [`animus_control::Metadata::
//!   backup_ready_to_complete`]): assemble the manifest object from
//!   [`animus_control::Metadata::backup_manifest_tablet_progress`], `put`
//!   it to the backup store, and only THEN propose
//!   [`animus_control::MetaCommand::CompleteBackup`] —
//!   **durable-before-visible** (ADR 0059 §4): a backup reaches `AVAILABLE`
//!   only once its manifest object is durably stored. A crash between the
//!   `put` and the proposal simply leaves the row `Creating`; the next tick
//!   re-`put`s the identical manifest bytes at the identical id and
//!   retries — the store's own write-once contract makes the re-put a safe
//!   no-op.
//! - **Not ready, and stuck past [`STUCK_CREATING_TIMEOUT`]** with no
//!   observed progress in that window: propose
//!   [`animus_control::MetaCommand::FailBackup`], so a wedged capture
//!   doesn't leave the row `Creating` forever. The timer resets on **any**
//!   growth in the number of tablets that have reported.
//!
//! ## The control-only-leader gap (documented, not fixed here)
//!
//! **Failing** a stuck backup needs only `Metadata`, cheap on any
//! control-plane leader. **Completing** one needs [`BackupObjectStore`] to
//! durably `put` the manifest, which [`BackupObjectStore::backup_put`]
//! answers `None` for on a control-only leader (no data role provisions a
//! backup-store handle there) — a backup can sit fully captured-but-
//! `Creating` for as long as a control-only node leads.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{BackupId, BackupStatus, Metadata};
use animus_cp_data::backup as backup_codec;
use animus_env::{Env, Nanos};

use crate::host::{BackupObjectStore, ControlLeaderHost};

/// This loop's tick cadence.
pub const BACKUP_COMPLETION_INTERVAL: Duration = Duration::from_millis(200);

/// How long a `Creating` backup may go with **zero** newly-reported tablets
/// before this loop fails it outright — see the module doc.
pub const STUCK_CREATING_TIMEOUT: Duration = Duration::from_secs(600);

/// Per-backup stuck-progress tracking (driver-local, owned by this one
/// loop). `reported` is the tablet-report count last observed for this
/// backup; `since` is the virtual instant that count was last seen to grow.
struct StuckTracker {
    since: Nanos,
    reported: usize,
}

/// The control-plane-leader-only background loop (ADR 0059 §3/§4) — see
/// the module doc for the decision and the documented control-only-leader
/// scope gap.
pub async fn backup_completion_loop<E, H>(env: E, host: H)
where
    E: Env,
    H: ControlLeaderHost<E> + BackupObjectStore,
{
    let mut stuck: BTreeMap<BackupId, StuckTracker> = BTreeMap::new();
    loop {
        env.sleep(BACKUP_COMPLETION_INTERVAL).await;
        let Some(leader) = host.control_leader() else {
            continue;
        };
        backup_completion_tick(&env, &host, &leader, &mut stuck).await;
    }
}

/// One tick's whole decision — see the module doc's "The decision" section.
async fn backup_completion_tick<E, H>(
    env: &E,
    host: &H,
    leader: &animus_control::RaftNode<E>,
    stuck: &mut BTreeMap<BackupId, StuckTracker>,
) where
    E: Env,
    H: BackupObjectStore,
{
    let meta = leader.metadata();
    // Bound the tracker to backups still genuinely `Creating`.
    stuck.retain(|id, _| {
        meta.backup(id)
            .is_some_and(|row| matches!(row.status, BackupStatus::Creating))
    });

    let creating: Vec<(&BackupId, usize)> = meta
        .backups
        .iter()
        .filter(|(_, row)| matches!(row.status, BackupStatus::Creating))
        .map(|(id, _)| {
            let reported = meta
                .backup_manifest_tablet_progress(id)
                .into_iter()
                .filter(|(_, progress)| progress.is_some())
                .count();
            (id, reported)
        })
        .collect();

    for (backup_id, reported) in creating {
        if meta.backup_ready_to_complete(backup_id) {
            stuck.remove(backup_id);
            complete_backup(host, leader, &meta, backup_id).await;
            continue;
        }
        let now = env.now();
        let entry = stuck.entry(backup_id.clone()).or_insert(StuckTracker {
            since: now,
            reported,
        });
        if reported > entry.reported {
            entry.reported = reported;
            entry.since = now;
            continue;
        }
        if now.duration_since(entry.since) >= STUCK_CREATING_TIMEOUT {
            let _ = leader.propose(animus_control::MetaCommand::FailBackup {
                backup_id: backup_id.clone(),
                reason: format!(
                    "capture made no progress for {STUCK_CREATING_TIMEOUT:?} (stuck-Creating timeout)"
                ),
            });
        }
    }
}

/// Write the manifest object (durable-before-visible) then propose
/// `CompleteBackup` — see the module doc for the ordering argument and the
/// control-only-leader gap this gates on.
async fn complete_backup<E, H>(
    host: &H,
    leader: &animus_control::RaftNode<E>,
    meta: &Metadata,
    backup_id: &str,
) where
    E: Env,
    H: BackupObjectStore,
{
    let Some(row) = meta.backup(backup_id) else {
        return;
    };
    let tablet_progress = meta
        .backup_manifest_tablet_progress(backup_id)
        .into_iter()
        .filter_map(|(tablet, progress)| {
            progress.map(|p| backup_codec::BackupManifestTabletEntry {
                tablet,
                progress: p,
            })
        })
        .collect();
    let object = backup_codec::BackupManifestObject {
        manifest: row.manifest.clone(),
        tablet_progress,
    };
    let bytes = backup_codec::encode_manifest_object(&object);
    let object_id = backup_codec::backup_manifest_object_id(backup_id);
    match host.backup_put(&object_id, &bytes).await {
        None => return, // control-only leader — see the module doc's gap
        Some(Err(err)) => {
            tracing::warn!(
                backup_id,
                error = %err,
                "backup completion: manifest put failed, retrying next tick"
            );
            return;
        }
        Some(Ok(_replicas)) => {}
    }
    let _ = leader.propose(animus_control::MetaCommand::CompleteBackup {
        backup_id: backup_id.to_owned(),
    });
}
