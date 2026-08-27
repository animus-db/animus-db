//! On-demand backup **completion aggregator** (ADR 0059 §3/§4, Train 1
//! PR③): a control-plane-**leader**-only background loop, the identical
//! self-gating shape as `index_backfill_loop`/`segment_janitor_loop`
//! (`segment_janitor.rs`'s own doc), driving every `Creating` backup to a
//! terminal state.
//!
//! ## The decision, each tick
//!
//! For every backup row currently `Creating`:
//!
//! - **Ready** (every one of its pinned tablets' own current live capture
//!   frontier has reported —
//!   [`animus_control::Metadata::backup_ready_to_complete`], the §6
//!   re-planning-aware readiness check shared with [`crate::backup_capture`]
//!   and the `ANIMUS_BACKUP_SEEDS` corpus): assemble the manifest object
//!   from [`animus_control::Metadata::backup_manifest_tablet_progress`]
//!   (never a blanket scan of every `backup_tablet_progress` row — that
//!   accessor's own doc explains why a split-superseded stale report must
//!   never double-count into the manifest), `put` it to the backup store,
//!   and only THEN propose [`MetaCommand::CompleteBackup`] —
//!   **durable-before-visible** (ADR 0059 §4): a backup reaches
//!   `AVAILABLE`, DynamoDB's own terminal on-demand status, only once its
//!   manifest object is durably stored, so `CompleteBackup` is never
//!   proposed first and patched up after. A crash between the `put` and
//!   the proposal simply leaves the row `Creating`; the next tick re-`put`s
//!   the identical manifest bytes (a pure, deterministic function of
//!   already-committed `Metadata`) at the identical id and retries —
//!   `SegmentStore::put`'s own write-once contract makes the re-put a safe
//!   no-op.
//! - **Not ready, and stuck past [`STUCK_CREATING_TIMEOUT`]** with no
//!   observed progress in that window (see [`StuckTracker`]): propose
//!   [`MetaCommand::FailBackup`], so a wedged capture (a permanently
//!   unreachable tablet, a store outage outlasting every retry) doesn't
//!   leave the row `Creating` forever. The timer resets on **any** growth
//!   in the number of tablets that have reported — a slowly-but-genuinely
//!   progressing capture is never failed out from under itself, only one
//!   that has made zero progress for the whole window.
//!
//! ## The control-only-leader gap (documented, not fixed here)
//!
//! Exactly the same residual `segment_janitor.rs` already carries, restated
//! for this loop: **failing** a stuck backup needs only `Metadata`, cheap
//! on any control-plane leader (including a pure control-only node, ADR
//! 0035 split deployment). **Completing** one needs a
//! [`crate::BackupStoreHandle`] to durably `put` the manifest, which today
//! exists only on a node with a data role
//! ([`ClientCtx::data_opt`] answers `None` on a control-only leader). A
//! backup can therefore sit fully captured-but-`Creating` for as long as a
//! control-only node leads, until a data-capable node takes the lead
//! instead — the identical shape as the segment janitor's own phases 2/3
//! gap, and closed by the identical follow-up (extending
//! [`crate::BackupStoreHandle`] provisioning to a control-only node).

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{BackupId, BackupStatus, Metadata, RaftNode};
use animus_cp_data::backup as backup_codec;
use animus_env::ProdEnv;

use crate::{ClientCtx, MetaCommand};

/// This loop's tick cadence — matches `index_backfill.rs`/
/// `segment_janitor.rs`'s own `_INTERVAL` constants.
const BACKUP_COMPLETION_INTERVAL: Duration = Duration::from_millis(200);

/// How long a `Creating` backup may go with **zero** newly-reported
/// tablets before this loop fails it outright (ADR 0059 §3's "waiting past
/// a bounded timeout" mark-phase rule). No CLI/config knob exists yet —
/// mirrors `DEFAULT_STREAM_RETENTION`/`segment_janitor.rs`'s own
/// documented "no split-deployment knob yet" precedent — so this is always
/// the production default. Ten minutes is deliberately generous: on-demand
/// backup capture has no inherent time bound (a very large table's own
/// sweep may legitimately take a while), so this exists to catch a
/// genuinely wedged capture (a permanently unreachable tablet, an
/// exhausted-retries store outage), not to bound ordinary progress.
const STUCK_CREATING_TIMEOUT: Duration = Duration::from_secs(600);

/// Per-backup stuck-progress tracking (driver-local, owned by this one
/// loop — mirrors `ChangeRateTracker`'s/`change_consumer_loop`'s
/// `first_hot_seen` ownership discipline: no lock needed, one writer/
/// reader). `reported` is the tablet-report count last observed for this
/// backup; `since` is when that count was last seen to grow (reset to
/// "now" every time it does).
struct StuckTracker {
    since: tokio::time::Instant,
    reported: usize,
}

/// The control-plane-leader-only background loop (ADR 0059 §3/§4) — see
/// the module doc for who spawns this and the documented control-only-
/// leader scope gap.
pub(crate) async fn backup_completion_loop(ctx: ClientCtx) {
    let mut stuck: BTreeMap<BackupId, StuckTracker> = BTreeMap::new();
    loop {
        tokio::time::sleep(BACKUP_COMPLETION_INTERVAL).await;
        let Some(leader) = ctx.edge.leader_handle() else {
            continue;
        };
        backup_completion_tick(&ctx, &leader, &mut stuck).await;
    }
}

/// One tick's whole decision — see the module doc's "The decision" section.
async fn backup_completion_tick(
    ctx: &ClientCtx,
    leader: &RaftNode<ProdEnv>,
    stuck: &mut BTreeMap<BackupId, StuckTracker>,
) {
    let meta = leader.metadata();
    // Bound the tracker to backups still genuinely `Creating` — a cheap
    // `BTreeMap` retain, never a scan, mirroring every other driver-local
    // memo in this crate.
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
            complete_backup(ctx, leader, &meta, backup_id).await;
            continue;
        }
        let now = tokio::time::Instant::now();
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
            let _ = leader.propose(MetaCommand::FailBackup {
                backup_id: backup_id.clone(),
                reason: format!(
                    "capture made no progress for {:?} (stuck-Creating timeout)",
                    STUCK_CREATING_TIMEOUT
                ),
            });
        }
    }
}

/// Write the manifest object (durable-before-visible) then propose
/// [`MetaCommand::CompleteBackup`] — see the module doc for the ordering
/// argument and the control-only-leader gap this gates on.
async fn complete_backup(
    ctx: &ClientCtx,
    leader: &RaftNode<ProdEnv>,
    meta: &Metadata,
    backup_id: &str,
) {
    let Some(data) = ctx.data_opt() else {
        return; // control-only leader — see the module doc's documented gap
    };
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
    if let Err(err) = data.backup_store.put(&object_id, &bytes).await {
        tracing::warn!(
            backup_id,
            error = %err,
            "backup completion: manifest put failed, retrying next tick"
        );
        return;
    }
    let _ = leader.propose(MetaCommand::CompleteBackup {
        backup_id: backup_id.to_owned(),
    });
}
