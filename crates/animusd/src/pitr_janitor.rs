//! Point-in-time recovery (PITR, ADR 0059 §9, Train 3) background work that
//! is **not** an arm of the per-tablet `index_drain::change_consumer_loop`
//! (the fifth consumer arm — sealing — lives there instead, alongside the
//! stream sealer): two distinct, control-plane-**leader**-only loops,
//! mirroring `segment_janitor.rs`/`backup_janitor.rs`'s own shape exactly.
//!
//! ## [`pitr_snapshot_loop`] — periodic base snapshots
//!
//! For every table with PITR currently enabled, proposes an
//! internally-triggered `MetaCommand::BeginBackup` on a schedule (ADR 0059
//! §9's "reusing the Train 1 capture path unchanged") once its most recent
//! PITR base snapshot has aged past [`snapshot_cadence`](pitr_snapshot_loop).
//! **This reuses Train 1's capture driver/aggregator (`backup_capture.rs`/
//! `backup_completion.rs`) completely unmodified** — both already operate
//! generically over every `Creating` row in `Metadata::backups` regardless
//! of who proposed `BeginBackup`, so an internally-triggered snapshot is
//! indistinguishable, from their point of view, from an ordinary
//! `CreateBackup` wire call. This loop's only extra job is tagging the
//! resulting row via `MetaCommand::MarkBackupPitrBase` once it exists, so
//! the retention janitor below (and `DescribeContinuousBackups`) can tell it
//! apart from a genuine on-demand backup.
//!
//! **Self-healing tag** (a named, accepted residual, not a defended
//! two-phase-commit property): `BeginBackup` and `MarkBackupPitrBase` are
//! two independent proposals — a crash or a dropped ack between them would
//! otherwise leave a `BeginBackup` row sitting around forever, untagged, and
//! (since an ordinary on-demand backup never auto-expires,
//! `backup_janitor.rs`'s own documented rule) permanently leaked. Every
//! tick, in addition to deciding whether a *fresh* snapshot is due, this
//! loop also re-proposes `MarkBackupPitrBase` for every row whose
//! `backup_name` carries [`PITR_SNAPSHOT_BACKUP_NAME_PREFIX`] but isn't yet
//! in `Metadata::pitr_base_backups` — closing the gap on the very next tick
//! rather than never. A real on-demand `CreateBackup` call that happens to
//! choose the identical name is a narrow, accepted false-positive-tag edge
//! case, the same class of "an unlikely literal collision is a documented
//! caveat, not a defended invariant" callout `animus_cp_data::backup`'s own
//! module doc already makes for a table literally named `backup`.
//!
//! ## [`pitr_janitor_loop`] — retention
//!
//! Two independent two-phase (ADR 0043 §A9 mold) retention sweeps sharing
//! one tick and one `retention` window:
//!
//! - **PITR segments** (`Metadata::pitr_segments`): mark every row whose own
//!   `seal_wall_ms` has aged past `retention`, delete its backup-store
//!   object (via [`crate::BackupStoreHandle::delete`], unused until this
//!   loop — see that method's own doc), then physically remove the row —
//!   subject to the identical **epoch-derivation guard**
//!   `segment_janitor.rs` already established for stream shards (never
//!   remove a tablet's own current highest-epoch row while the tablet still
//!   exists, since `SealPitrSegment`'s epoch derivation is "this tablet's
//!   own chain length"). **Deliberately no drop-table retention-zero rule**
//!   — unlike a stream shard, a PITR segment's retention is governed by age
//!   alone regardless of whether its source table's schema still exists
//!   (ADR 0059 §9/§10's explicit override of the streams rule: "deleted
//!   table restore within the retention window works"). **No replica-repair
//!   phase** — a deliberate Train 3 simplification (mirroring Train 1 PR④'s
//!   own "reclaim is local-only" acceptance for on-demand backups): PITR
//!   segments are churned by ordinary retention on a bounded, predictable
//!   schedule and repair is not this train's own scope; a future train can
//!   add it by copying `segment_janitor.rs`'s own phase 2 verbatim.
//! - **PITR base snapshots** (a `BackupRow` named in
//!   `Metadata::pitr_base_backups`): mark an `Available` one `Expired` (via
//!   the ordinary `MetaCommand::MarkBackupDeleted`, the identical command
//!   the `DeleteBackup` wire operation uses) once it has aged past
//!   `retention` **and** a newer base snapshot already covers the retention
//!   floor — the "never remove a table's own current replay anchor while
//!   segments after it still need it" guard ADR 0059 §9 calls for, applied
//!   per table. **This loop does nothing further** — the *existing*,
//!   unmodified `backup_janitor_loop` already reclaims every `Expired`/
//!   `Failed` row in `Metadata::backups` regardless of whether it carries a
//!   PITR tag, so marking here is the entire job; the object/row reclaim
//!   this produces rides that pre-existing loop for free.
//!
//! ## Who runs this, and the control-only-leader gap
//!
//! Spawned unconditionally on every node shape that can ever become the
//! control-plane leader (combined and control-only, ADR 0035) — the
//! identical "run everywhere, self-gate on `ctx.edge.leader_handle()`"
//! pattern `segment_janitor_loop`/`backup_janitor_loop` already use. Never
//! spawned on a data-only node (`BoundDataNode`), which never registers a
//! local control `RaftNode` at all. **The identical control-only-leader
//! scope gap `segment_janitor.rs`/`backup_janitor.rs` already document**:
//! marking (`MetaCommand::ExpirePitrSegments`/`MarkBackupDeleted`) needs
//! only `Metadata`, cheap on any control-plane leader; object deletion needs
//! a [`crate::BackupStoreHandle`], which only exists on a node with a data
//! role — a control-only leader marks correctly but cannot physically
//! reclaim PITR segment objects for as long as it leads.
//!
//! **No CLI-configurable retention/cadence knob yet** — a documented,
//! deliberate Train 3 simplification, the same shape
//! `ttl_reaper.rs`'s own sweep interval already has ("no CLI flag exists
//! yet ... always the production default"): [`DEFAULT_PITR_RETENTION`]/
//! [`DEFAULT_PITR_SNAPSHOT_CADENCE`] are used at every production spawn
//! site; both loops' own `Duration` parameters exist so a test can pass a
//! tiny value directly (this codebase's house testing discipline).

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{BackupId, BackupRow, BackupStatus, RaftNode};
use animus_env::{Clock, NodeId, ProdEnv, Rng};
use animus_tablet::TabletId;

use crate::ClientCtx;
use crate::MetaCommand;

/// Both loops' tick cadence — matches `segment_janitor.rs`/
/// `backup_janitor.rs`'s own `_INTERVAL` constants.
const PITR_TICK_INTERVAL: Duration = Duration::from_millis(200);

/// The PITR segment/base-snapshot retention window's own production default
/// (ADR 0059 §9: 35 days) — see the module doc's "no CLI knob yet" note.
pub const DEFAULT_PITR_RETENTION: Duration = Duration::from_secs(35 * 24 * 60 * 60);

/// How often a PITR-enabled table gets a fresh internally-triggered base
/// snapshot (ADR 0059 §9) — the production default. Bounds PITR's own
/// replay length: a restore never has to walk further back than the
/// nearest preceding base snapshot. See the module doc's "no CLI knob yet"
/// note.
pub const DEFAULT_PITR_SNAPSHOT_CADENCE: Duration = Duration::from_secs(6 * 60 * 60);

/// The `BackupName` marker every internally-triggered PITR base snapshot
/// carries (`{prefix}{table}`) — used only as a **self-healing hint** (see
/// the module doc's own caveat), never as the source of truth
/// (`Metadata::pitr_base_backups` is).
const PITR_SNAPSHOT_BACKUP_NAME_PREFIX: &str = "__pitr_base__";

fn pitr_snapshot_backup_name(table: &str) -> String {
    format!("{PITR_SNAPSHOT_BACKUP_NAME_PREFIX}{table}")
}

/// The periodic base-snapshot loop — see the module doc.
pub(crate) async fn pitr_snapshot_loop(ctx: ClientCtx, snapshot_cadence: Duration) {
    loop {
        tokio::time::sleep(PITR_TICK_INTERVAL).await;
        let Some(leader) = ctx.edge.leader_handle() else {
            continue;
        };
        pitr_snapshot_tick(&ctx, &leader, snapshot_cadence).await;
    }
}

/// One tick's whole decision — see the module doc's "periodic base
/// snapshots" section.
async fn pitr_snapshot_tick(
    ctx: &ClientCtx,
    leader: &RaftNode<ProdEnv>,
    snapshot_cadence: Duration,
) {
    let meta = leader.metadata();
    let now_ms = ctx.env.now().0 / 1_000_000;
    let cadence_ms = u64::try_from(snapshot_cadence.as_millis()).unwrap_or(u64::MAX);

    // Self-healing tag sweep (module doc): closes the gap left by a
    // `BeginBackup` whose own `MarkBackupPitrBase` never landed.
    for (backup_id, row) in meta.backups.iter() {
        if row
            .backup_name
            .starts_with(PITR_SNAPSHOT_BACKUP_NAME_PREFIX)
            && !meta.pitr_base_backups.contains(backup_id)
        {
            let _ = leader.propose(MetaCommand::MarkBackupPitrBase {
                backup_id: backup_id.clone(),
            });
        }
    }

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
        let backup_id =
            animus_dynamo::wire::backup_arn(table, &format!("{:016x}", ctx.env.next_u64()));
        let created_wall_ms = ctx.env.wall_now().0;
        let _ = leader.propose(MetaCommand::BeginBackup {
            backup_id,
            table: table.clone(),
            created_wall_ms,
            backup_name: pitr_snapshot_backup_name(table),
        });
        // One fresh snapshot proposal per table per tick is enough — the
        // next tick's own re-derived `has_recent_or_inflight` (once the
        // `Creating` row is visible) prevents a pile-up even if this
        // propose's own confirm is never observed by this loop.
    }
}

/// The retention loop — see the module doc.
pub(crate) async fn pitr_janitor_loop(ctx: ClientCtx, retention: Duration) {
    loop {
        tokio::time::sleep(PITR_TICK_INTERVAL).await;
        let Some(leader) = ctx.edge.leader_handle() else {
            continue;
        };
        pitr_janitor_tick(&ctx, &leader, retention).await;
    }
}

/// One tick's whole decision — see the module doc's "retention" section.
async fn pitr_janitor_tick(ctx: &ClientCtx, leader: &RaftNode<ProdEnv>, retention: Duration) {
    let meta = leader.metadata();
    let now_ms = ctx.env.now().0 / 1_000_000;
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
        // `segment_janitor.rs`'s own (see that module's doc): never
        // physically remove a tablet's own current highest-epoch row while
        // the tablet still exists, since `SealPitrSegment`'s epoch is "this
        // tablet's own chain length."
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
        let Some(data) = ctx.data_opt() else {
            continue; // control-only leader — see the module doc's gap
        };
        match data
            .backup_store
            .delete(&still_present, &row.object_id)
            .await
        {
            Ok(()) if may_remove_row => removed.push((*tablet, *epoch)),
            Ok(()) => {}
            Err(e) => tracing::warn!(
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

#[cfg(test)]
mod tests {
    use animus_control::{ApplyOutcome, ColumnType, Metadata, PitrSpec, TableSchema};
    use animus_tablet::KeyRange;

    use super::*;

    fn base_meta_with_pitr(table: &str) -> Metadata {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: table.to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some(table.to_owned()),
                range: KeyRange::whole(),
                replicas: Vec::new(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::UpdateContinuousBackups {
                table: table.to_owned(),
                enabled: true,
                wall_ms: 0,
            }),
            ApplyOutcome::Applied
        );
        m
    }

    /// The pure keep-anchor predicate this tick's PITR-base-snapshot phase
    /// applies, factored out for a lightweight, `RaftNode`-free unit test —
    /// mirrors the shape of the tick's own inline logic exactly (see that
    /// function for the production call site).
    fn due_for_mark(bases_created_wall_ms: &[u64], floor_ms: u64) -> Vec<u64> {
        let mut sorted = bases_created_wall_ms.to_vec();
        sorted.sort_unstable();
        let keep_anchor = sorted.iter().rev().find(|&&ms| ms <= floor_ms).copied();
        let Some(anchor) = keep_anchor else {
            return Vec::new();
        };
        sorted.into_iter().filter(|&ms| ms < anchor).collect()
    }

    /// Never marks the single newest base at or before the floor — it is
    /// every later segment's own replay anchor.
    #[test]
    fn keep_anchor_never_marks_the_newest_base_at_or_before_the_floor() {
        assert_eq!(due_for_mark(&[1_000], 5_000), Vec::<u64>::new());
        assert_eq!(due_for_mark(&[1_000, 2_000], 5_000), vec![1_000]);
    }

    /// A base newer than the floor is never marked, regardless of older
    /// siblings.
    #[test]
    fn keep_anchor_never_marks_a_base_newer_than_the_floor() {
        assert_eq!(due_for_mark(&[1_000, 2_000, 9_000], 5_000), vec![1_000]);
    }

    /// No base at or before the floor at all: nothing is due yet (there is
    /// no successor to hand replay duty to).
    #[test]
    fn keep_anchor_marks_nothing_when_every_base_is_within_the_window() {
        assert_eq!(due_for_mark(&[9_000, 9_500], 5_000), Vec::<u64>::new());
    }

    /// `pitr_snapshot_backup_name`'s own prefix round-trips (the self-healing
    /// sweep's detection rule).
    #[test]
    fn pitr_snapshot_backup_name_carries_the_marker_prefix() {
        let name = pitr_snapshot_backup_name("orders");
        assert!(name.starts_with(PITR_SNAPSHOT_BACKUP_NAME_PREFIX));
    }

    /// Sanity: `base_meta_with_pitr` produces a table with PITR enabled at
    /// generation 1, ready for the tick-level tests in
    /// `tests/pitr_fault_corpus.rs`/`animusd`'s own e2e suite.
    #[test]
    fn base_meta_with_pitr_enables_pitr() {
        let m = base_meta_with_pitr("orders");
        assert_eq!(m.table_pitr("orders").unwrap().generation, 1);
        let _: Option<&PitrSpec> = m.table_pitr("orders");
    }
}
