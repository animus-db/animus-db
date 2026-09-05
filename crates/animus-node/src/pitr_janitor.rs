//! Point-in-time recovery (PITR, ADR 0059 §9, Train 3) background work,
//! moved here by ADR 0061 rung C2 — two distinct, control-plane-**leader**-
//! only loops mirroring `backup_completion`/`backup_janitor`'s own shape.
//! See `crate::host::{ControlLeaderHost, BackupObjectStore}` for the two
//! capabilities both loops need (only [`BackupObjectStore::
//! backup_delete_at`], not `put`/`list_local`/`delete_local`, since a PITR
//! segment's replica set is already known from its own catalog row —
//! unlike an on-demand backup object, which carries none).
//!
//! ## [`pitr_snapshot_loop`] — periodic base snapshots
//!
//! For every table with PITR currently enabled, proposes an internally-
//! triggered `MetaCommand::BeginBackup { pitr_base: true, .. }` on a
//! schedule once its most recent PITR base snapshot has aged past
//! `snapshot_cadence` — reusing Train 1's capture driver/aggregator
//! completely unmodified. **The `pitr_base` flag tags the row in the same
//! apply that mints it (issue #593)** — there used to be a separate
//! `MetaCommand::MarkBackupPitrBase` proposed only once this loop observed
//! its own `BeginBackup` row exist, which left a real committed window (the
//! instant between the two commits) where the row was an ordinary,
//! untagged `Creating` backup — visible under `ListBackups`' default `USER`
//! filter and the console's per-table backups projection. Folding the tag
//! into `BeginBackup` itself closes that window structurally: every replica
//! that ever observes the row observes it already tagged, and there is no
//! longer a self-healing sweep to run (there is nothing left for one to
//! heal — a `BeginBackup` either commits fully tagged or doesn't commit at
//! all). See `Metadata::pitr_base_backups`'s own doc (`animus-control`) and
//! `docs/adr/0059-backup-restore.md` §9's 2026-09-04 as-built amendment for
//! the full incident.
//!
//! ## [`pitr_janitor_loop`] — retention
//!
//! Two independent two-phase retention sweeps sharing one tick and one
//! `retention` window: PITR segments (mark/delete/remove, subject to the
//! same epoch-derivation guard `segment_janitor.rs` established — that
//! loop has **not** moved in this rung, see its own doc for why) and PITR
//! base snapshots (mark an aged-out one `Expired` once a newer base already
//! covers the retention floor — the "never remove a table's own current
//! replay anchor" guard). Base-snapshot reclaim itself rides the
//! **existing, unmoved-in-this-rung** `backup_janitor_loop`, which already
//! reclaims every `Expired`/`Failed` row regardless of a PITR tag.
//!
//! ## No CLI-configurable retention/cadence knob
//!
//! A documented, deliberate simplification — see `animusd/CLAUDE.md`.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{BackupId, BackupRow, BackupStatus, MetaCommand};
use animus_env::{Env, NodeId};
use animus_tablet::TabletId;

use crate::host::{BackupObjectStore, ControlLeaderHost};

/// Both loops' tick cadence.
pub const PITR_TICK_INTERVAL: Duration = Duration::from_millis(200);

/// The PITR segment/base-snapshot retention window's own production
/// default (ADR 0059 §9: 35 days).
pub const DEFAULT_PITR_RETENTION: Duration = Duration::from_secs(35 * 24 * 60 * 60);

/// How often a PITR-enabled table gets a fresh internally-triggered base
/// snapshot — the production default.
pub const DEFAULT_PITR_SNAPSHOT_CADENCE: Duration = Duration::from_secs(6 * 60 * 60);

/// The `BackupName` marker every internally-triggered PITR base snapshot
/// carries (`{prefix}{table}`) — purely a human-readable label for
/// `DescribeBackup`/`ListBackups`/the console; never the source of truth
/// (`Metadata::pitr_base_backups`, set atomically by `BeginBackup`'s own
/// `pitr_base` flag, is — see the module doc).
const PITR_SNAPSHOT_BACKUP_NAME_PREFIX: &str = "__pitr_base__";

fn pitr_snapshot_backup_name(table: &str) -> String {
    format!("{PITR_SNAPSHOT_BACKUP_NAME_PREFIX}{table}")
}

/// The periodic base-snapshot loop — see the module doc.
pub async fn pitr_snapshot_loop<E, H>(env: E, host: H, snapshot_cadence: Duration)
where
    E: Env,
    H: ControlLeaderHost<E>,
{
    loop {
        env.sleep(PITR_TICK_INTERVAL).await;
        let Some(leader) = host.control_leader() else {
            continue;
        };
        pitr_snapshot_tick(&env, &leader, snapshot_cadence);
    }
}

/// One tick's whole decision — see the module doc's "periodic base
/// snapshots" section.
fn pitr_snapshot_tick<E: Env>(
    env: &E,
    leader: &animus_control::RaftNode<E>,
    snapshot_cadence: Duration,
) {
    let meta = leader.metadata();
    let now_ms = env.now().0 / 1_000_000;
    let cadence_ms = u64::try_from(snapshot_cadence.as_millis()).unwrap_or(u64::MAX);

    for (table, schema) in meta.schemas.iter() {
        if schema.pitr.is_none() {
            continue;
        }
        let has_recent_or_inflight =
            meta.pitr_base_backups_for_table(table)
                .any(|(_, row)| match &row.status {
                    BackupStatus::Creating => true,
                    BackupStatus::Available => {
                        now_ms.saturating_sub(row.manifest.created_wall_ms) < cadence_ms
                    }
                    BackupStatus::Failed { .. } | BackupStatus::Expired => false,
                });
        if has_recent_or_inflight {
            continue;
        }
        let backup_id = animus_dynamo::wire::backup_arn(table, &format!("{:016x}", env.next_u64()));
        let created_wall_ms = env.wall_now().0;
        let _ = leader.propose(MetaCommand::BeginBackup {
            backup_id,
            table: table.clone(),
            created_wall_ms,
            backup_name: pitr_snapshot_backup_name(table),
            pitr_base: true,
        });
        // One fresh snapshot proposal per table per tick is enough — the
        // next tick's own re-derived `has_recent_or_inflight` (once the
        // `Creating` row is visible) prevents a pile-up even if this
        // propose's own confirm is never observed by this loop.
    }
}

/// The retention loop — see the module doc.
pub async fn pitr_janitor_loop<E, H>(env: E, host: H, retention: Duration)
where
    E: Env,
    H: ControlLeaderHost<E> + BackupObjectStore,
{
    loop {
        env.sleep(PITR_TICK_INTERVAL).await;
        let Some(leader) = host.control_leader() else {
            continue;
        };
        pitr_janitor_tick(&env, &host, &leader, retention).await;
    }
}

/// One tick's whole decision — see the module doc's "retention" section.
async fn pitr_janitor_tick<E, H>(
    env: &E,
    host: &H,
    leader: &animus_control::RaftNode<E>,
    retention: Duration,
) where
    E: Env,
    H: BackupObjectStore,
{
    let meta = leader.metadata();
    let now_ms = env.now().0 / 1_000_000;
    let retention_ms = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);

    // --- PITR segments: phase 1a (mark) ------------------------------
    let mut to_mark: Vec<(TabletId, u64)> = Vec::new();
    for ((tablet, epoch), row) in meta.pitr_segments.iter() {
        if row.expired {
            continue;
        }
        if now_ms.saturating_sub(row.seal_wall_ms) >= retention_ms {
            to_mark.push((*tablet, *epoch));
        }
    }
    if !to_mark.is_empty() {
        let _ = leader.propose(MetaCommand::ExpirePitrSegments {
            rows: to_mark,
            remove: false,
        });
    }

    // --- PITR segments: phase 1b (delete objects, then remove rows) --
    let mut removed: Vec<(TabletId, u64)> = Vec::new();
    for ((tablet, epoch), row) in meta.pitr_segments.iter() {
        if !row.expired {
            continue;
        }
        // Epoch-derivation guard — identical argument to
        // `segment_janitor.rs`'s own (see that module's doc, and
        // `animusd/CLAUDE.md`): never physically remove a tablet's own
        // current highest-epoch row while the tablet still exists.
        let is_tablet_max = meta
            .pitr_segments
            .range((*tablet, epoch + 1)..=(*tablet, u64::MAX))
            .next()
            .is_none();
        let may_remove_row = !is_tablet_max || !meta.tablets.contains_key(tablet);

        let still_present: Vec<NodeId> = row
            .replicas
            .iter()
            .filter(|r| meta.members.contains_key(*r))
            .cloned()
            .collect();
        if !row.replicas.is_empty() && still_present.is_empty() {
            // Every recorded replica has left the cluster's own membership
            // entirely — confirmed-absent, nothing left to delete.
            if may_remove_row {
                removed.push((*tablet, *epoch));
            }
            continue;
        }
        match host.backup_delete_at(&still_present, &row.object_id).await {
            None => continue, // control-only leader — see the module doc's gap
            Some(Ok(())) if may_remove_row => removed.push((*tablet, *epoch)),
            Some(Ok(())) => {}
            Some(Err(e)) => tracing::warn!(
                tablet = tablet.0,
                epoch,
                error = %e,
                "PITR janitor: segment object delete failed, retrying next tick"
            ),
        }
    }
    if !removed.is_empty() {
        let _ = leader.propose(MetaCommand::ExpirePitrSegments {
            rows: removed,
            remove: true,
        });
    }

    // --- PITR base snapshots: mark old-enough ones past the keep-anchor
    // guard (module doc) — the existing `backup_janitor_loop` reclaims
    // everything from here. ------------------------------------------
    let floor_ms = now_ms.saturating_sub(retention_ms);
    let mut by_table: BTreeMap<&str, Vec<(&BackupId, &BackupRow)>> = BTreeMap::new();
    for id in &meta.pitr_base_backups {
        if let Some(row) = meta.backups.get(id)
            && matches!(row.status, BackupStatus::Available)
        {
            by_table
                .entry(row.table.as_str())
                .or_default()
                .push((id, row));
        }
    }
    for bases in by_table.values_mut() {
        bases.sort_by_key(|(_, row)| row.manifest.created_wall_ms);
        // The keep anchor: the newest base at or before the retention
        // floor — every segment sealed after it and still within retention
        // needs it as its replay base, so it must survive regardless of its
        // own age. Every OLDER base is superseded and safe to mark.
        let keep_anchor_wall_ms = bases
            .iter()
            .rev()
            .find(|(_, row)| row.manifest.created_wall_ms <= floor_ms)
            .map(|(_, row)| row.manifest.created_wall_ms);
        let Some(anchor) = keep_anchor_wall_ms else {
            continue; // no base is old enough yet to have a successor to mark
        };
        for (id, row) in bases.iter() {
            if row.manifest.created_wall_ms < anchor {
                let _ = leader.propose(MetaCommand::MarkBackupDeleted {
                    backup_id: (*id).clone(),
                });
            }
        }
    }
}
