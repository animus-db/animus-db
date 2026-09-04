//! `RestoreTableFromBackup`'s **restore driver** (ADR 0059 §7, Train 2): a
//! per-tablet, leader-side, event-driven loop that seeds a `Seeding`
//! restore's single destination tablet from its backup's data objects, then
//! activates it. See `crate::dynamo::restore_table_from_backup` for the wire
//! op that mints a `Seeding` [`RestoreRow`] via `MetaCommand::BeginRestore`,
//! and `docs/adr/0059-backup-restore.md`'s Train 2 as-built note for the
//! design decision this module implements.
//!
//! ## Pinned-tablets-vs-fresh-layout (the ADR's own open question)
//!
//! `MetaCommand::BeginRestore` mints exactly **one** `Building` tablet over
//! the whole ring for the restored table — never one per the backup's own
//! (possibly many) pinned/reporting tablets. This is the ADR's own
//! explicitly-licensed "mint a fresh layout... e.g. one tablet per the
//! placement engine's own preference" branch, taken at its simplest: a
//! single destination tablet needs **no per-row key routing** at all — every
//! data object across every one of the backup's `tablet_progress` entries
//! (however many physical tablets originally captured them, including any
//! split-during-capture re-planning, ADR 0059 §6) seeds into the SAME
//! tablet, verbatim, regardless of which physical tablet's range a row
//! originally came from. This sidesteps the open question entirely (no
//! `range` field ever needed on `BackupManifestTabletEntry`, since nothing
//! here consults per-reporting-tablet ranges) and matches ordinary
//! `CreateTable`'s own "one tablet over the whole ring" provisioning
//! convention exactly (`animusd::ClientCtx::provision_tablet`). The ordinary
//! auto-split machinery reshapes the restored table's tablet count
//! afterward, exactly as it would for any freshly-populated table — see the
//! ADR's as-built note for the full reasoning and the acknowledged tradeoff
//! (every one of a backup's original tablets funnels through one Raft group
//! during the seed phase).
//!
//! ## Discovery
//!
//! Identical shape to [`crate::backup_capture`]'s own capture driver: each
//! tick, for every `Seeding` restore and every tablet this node currently
//! leads, seed this restore's own tablet if it matches.
//!
//! ## Resumability — deliberately no durable cursor
//!
//! Unlike backup capture's [`crate::backup_capture::CaptureCursor`] (a
//! durable `KIND_CURSOR` row, needed there because a re-derived chunk must
//! be byte-identical across a leader change to respect the backup store's
//! write-once contract), restore's own destination objects are the backup's
//! own **immutable, already-committed** data objects — nothing this driver
//! writes to the backup store, and `propose_seed_batch`'s merge-at-carried-
//! version semantics make re-seeding an already-applied chunk a safe no-op
//! regardless of how many times it repeats (the exact same idempotency the
//! split-build driver's own seeding already rests on, ADR 0050 rung 4). A
//! driver-local (non-durable) resume cursor, mirroring `ttl_reaper.rs`'s own
//! "no durable row, an interrupted sweep simply resumes-or-restarts safely"
//! discipline, is therefore enough: a leader change or crash simply loses
//! this node's own in-memory progress and the new leader's driver restarts
//! the whole backup sweep from its first chunk — safe, if potentially
//! wasteful for a very large backup restarted repeatedly under a flapping
//! leader (a named, accepted Train 2 simplification, not a correctness
//! gap).
//!
//! ## Bounded liveness — a wedged restore fails, never half-serves
//!
//! Mirrors [`crate::backup_completion`]'s own stuck-`Creating` aggregator
//! shape, but embedded directly in this per-tablet driver rather than a
//! separate control-plane-leader aggregator: since a restore has exactly one
//! destination tablet, the tablet's own leader is already the sole authority
//! on that restore's progress, so no cross-tablet aggregation is needed.
//! [`RESTORE_STUCK_TIMEOUT`] past the last observed forward progress (rows
//! seeded, or a leader change that starts this driver's own local tracking
//! fresh) proposes `MetaCommand::FailRestore` — the target table's schema
//! and its permanently-`Building` tablet are left in place but never
//! routable (see [`RestoreStatus::Failed`]'s own doc), so a wedged restore
//! never half-serves; an operator/caller can simply `DeleteTable` it.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use animus_control::schema::IndexDef;
use animus_control::{BackupStatus, MetaCommand, ProposeResult, RestoreId, RestoreStatus};
use animus_cp_data::backup as backup_codec;
use tokio::sync::Mutex;

use crate::{ClientCtx, CpGroup};

/// This loop's tick cadence — matches [`crate::backup_capture::
/// BACKUP_CAPTURE_INTERVAL`] and every other per-tablet consumer loop in
/// this crate.
pub(crate) const RESTORE_TICK_INTERVAL: Duration = Duration::from_millis(200);

/// How long a `Seeding` restore may go with no observed forward progress
/// (a chunk seeded, or a completion proposed) before this driver gives up
/// and proposes `FailRestore` — mirrors [`crate::backup_completion::
/// STUCK_CREATING_TIMEOUT`]'s own bound and rationale (a genuinely stuck
/// backup store, or a source backup that vanished mid-restore, must not
/// leave a restore `Seeding` forever).
pub(crate) const RESTORE_STUCK_TIMEOUT: Duration = Duration::from_secs(600);

/// How long [`propose_local`]'s own confirm wait allows before giving up for
/// this tick — the next tick simply retries (every proposal here is
/// idempotent or safe to repeat).
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);

/// This driver's own per-restore liveness tracking — deliberately
/// **in-memory only** (see the module doc's "Resumability" section): reset
/// whenever this node starts observing a restore fresh (first tick, or a
/// leader change that hands the tablet to this node), advanced on any
/// observed forward progress.
struct RestoreProgress {
    last_progress: Instant,
}

/// Every led tablet's own restore-seeding step, once per
/// [`RESTORE_TICK_INTERVAL`] tick.
pub(crate) async fn backup_restore_loop(ctx: ClientCtx) {
    let tracking: Mutex<BTreeMap<RestoreId, RestoreProgress>> = Mutex::new(BTreeMap::new());
    loop {
        tokio::time::sleep(RESTORE_TICK_INTERVAL).await;
        let meta = ctx.effective_metadata();
        if meta.restores.is_empty() {
            continue;
        }
        let seeding: Vec<(RestoreId, animus_control::RestoreRow)> = meta
            .restores
            .iter()
            .filter(|(_, row)| matches!(row.status, RestoreStatus::Seeding))
            .map(|(id, row)| (id.clone(), row.clone()))
            .collect();
        if seeding.is_empty() {
            continue;
        }
        let hosted = ctx.edge.hosted_groups();
        for (restore_id, row) in seeding {
            let Some(group) = hosted
                .iter()
                .find(|(t, _)| *t == row.tablet)
                .map(|(_, g)| g.clone())
            else {
                continue; // not (or not yet) hosted here
            };
            if !group.is_leader() {
                continue;
            }
            // The source backup must still be readable. `Expired`/`Failed`
            // mid-restore (a `DeleteBackup` racing an in-flight restore, ADR
            // 0059's Train 2 as-built note on this narrow, accepted race) is
            // fatal — never half-serve a target table seeded from a source
            // that has vanished out from under it.
            match meta.backup(&row.backup_id).map(|b| &b.status) {
                Some(BackupStatus::Available) => {}
                Some(BackupStatus::Creating) => continue, // shouldn't happen; retry
                _ => {
                    fail_restore(&ctx, &restore_id, "source backup is no longer available").await;
                    continue;
                }
            }
            let now = Instant::now();
            let stuck = {
                let mut guard = tracking.lock().await;
                let entry = guard
                    .entry(restore_id.clone())
                    .or_insert_with(|| RestoreProgress { last_progress: now });
                now.duration_since(entry.last_progress) > RESTORE_STUCK_TIMEOUT
            };
            if stuck {
                fail_restore(&ctx, &restore_id, "restore made no progress in time").await;
                tracking.lock().await.remove(&restore_id);
                continue;
            }
            match restore_tick(&ctx, &group, &restore_id, &row).await {
                RestoreTickOutcome::Progressed | RestoreTickOutcome::Completed => {
                    tracking
                        .lock()
                        .await
                        .insert(restore_id.clone(), RestoreProgress { last_progress: now });
                }
                RestoreTickOutcome::NoProgress => {}
            }
        }
    }
}

/// What one [`restore_tick`] call accomplished — purely so the caller knows
/// whether to reset this restore's own stuck-timeout clock.
enum RestoreTickOutcome {
    /// At least one chunk seeded.
    Progressed,
    /// The restore reached its last chunk and completion was proposed (or
    /// already observed done).
    Completed,
    /// Nothing happened (a store-read fault, a rejected/timed-out propose).
    NoProgress,
}

/// One restore step for `(restore_id, backup_id)`, seeding **this node's own
/// leader handle** of `group` from every one of the backup's data objects —
/// see the module doc for why a single destination tablet needs no
/// per-object filtering at all. Reads the manifest object fresh every tick
/// (cheap — one small object) rather than caching it, so a store fault on a
/// prior tick simply retries cleanly on the next.
async fn restore_tick(
    ctx: &ClientCtx,
    group: &CpGroup,
    restore_id: &str,
    row: &animus_control::RestoreRow,
) -> RestoreTickOutcome {
    let backup_id = row.backup_id.as_str();
    let manifest_id = backup_codec::backup_manifest_object_id(backup_id);
    let manifest_bytes = match ctx.backup_store.get_any(&manifest_id).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            tracing::debug!(
                backup_id,
                "restore: manifest object not found yet, retrying"
            );
            return RestoreTickOutcome::NoProgress;
        }
        Err(err) => {
            tracing::debug!(backup_id, %err, "restore: manifest object read failed, retrying");
            return RestoreTickOutcome::NoProgress;
        }
    };
    let manifest = match backup_codec::decode_manifest_object(&manifest_bytes) {
        Ok(m) => m,
        Err(err) => {
            tracing::warn!(backup_id, %err, "restore: manifest object is corrupt");
            return RestoreTickOutcome::NoProgress;
        }
    };

    // Sweep every reporting tablet's own full chunk sequence, in manifest
    // order, seeding each one sequentially — deliberately the WHOLE backup
    // in one tick call (not one chunk per external tick the way the capture
    // driver's own per-tablet sweep paces itself), since this driver keeps
    // no durable position to resume a partial sweep from (see the module
    // doc's "Resumability" section): a fault partway through simply leaves
    // every already-applied prefix chunk to re-merge as a safe no-op on the
    // very next full retry, rather than needing to remember where it left
    // off. `SeedRow`s already carry the tablet's own logical key/value/
    // version verbatim — no re-encoding, no key rewriting, and no per-row
    // routing (see the module doc's own layout-decision section for why).
    let mut progressed = false;
    for entry in &manifest.tablet_progress {
        let mut chunk = 0u64;
        loop {
            let object_id = backup_codec::backup_data_object_id(backup_id, entry.tablet.0, chunk);
            let bytes = match ctx.backup_store.get_any(&object_id).await {
                Ok(Some(bytes)) => bytes,
                Ok(None) => break, // this reporting tablet's own chunks are exhausted
                Err(err) => {
                    tracing::debug!(
                        backup_id,
                        tablet = entry.tablet.0,
                        chunk,
                        %err,
                        "restore: data chunk read failed, retrying"
                    );
                    return progress_outcome(progressed);
                }
            };
            let rows = match backup_codec::decode_data_chunk(&bytes) {
                Ok(rows) => rows,
                Err(err) => {
                    tracing::warn!(
                        backup_id,
                        tablet = entry.tablet.0,
                        chunk,
                        %err,
                        "restore: data chunk is corrupt"
                    );
                    return progress_outcome(progressed);
                }
            };
            // Re-wrap each captured (already-resolved, envelope-less) value
            // in a fresh committed envelope before merging via `SeedBatch` —
            // see `backup_codec::encode_restored_value`'s own doc for why
            // this is load-bearing, not cosmetic.
            let rows: Vec<animus_cp_data::SeedRow> = rows
                .into_iter()
                .map(|(kind, key, value, version)| {
                    (
                        kind,
                        key,
                        value.map(|v| backup_codec::encode_restored_value(&v)),
                        version,
                    )
                })
                .collect();
            if !propose_local(group, rows).await {
                return progress_outcome(progressed);
            }
            progressed = true;
            chunk += 1;
        }
    }

    // ADR 0059 §10 (Train 3 PR②): a `RestoreTableToPointInTime` restore's
    // own segment-replay plan, if any — swept the same "whole plan, every
    // tick, no durable position" way as the base chunk sweep just above,
    // for the identical reason (see the module doc's "Resumability"
    // section): every seed row this produces is idempotent under
    // `SeedBatch`'s merge-at-carried-version, so a fault partway through
    // just leaves an already-applied prefix to safely re-merge on retry.
    if let Some(plan) = &row.pitr {
        match replay_pitr_segments(ctx, group, backup_id, &row.target_table, plan).await {
            // Either way this tick's overall progress is already settled by
            // the base sweep above — `restore_tick` reaches its own
            // terminal `Completed` return right after this block either
            // way, so there is no later read of `progressed` left for
            // `Progressed` to feed.
            ReplayOutcome::Progressed | ReplayOutcome::Complete => {}
            ReplayOutcome::NoProgress => return progress_outcome(progressed),
        }
    }

    // Every reporting tablet's every chunk has been seeded (and, for a PITR
    // restore, every planned segment replayed) — activate, then (ADR 0059
    // §8) declare this restore's own resolved GSI plan on the now-`Active`
    // target table, exactly the moment `RestoreRow::gsi_defs`'s own doc
    // says is safe: the backfill seeder's very next tick sweeps this
    // tablet's real, fully-seeded `KIND_BASE` rows, never an empty one.
    complete_restore(ctx, restore_id, &row.target_table, &row.gsi_defs).await;
    RestoreTickOutcome::Completed
}

/// The outcome of one full [`replay_pitr_segments`] sweep — see
/// [`progress_outcome`]'s own doc for why this distinction matters to the
/// caller's stuck-timeout clock.
enum ReplayOutcome {
    /// At least one segment's records were seeded.
    Progressed,
    /// Every planned segment was already fully swept (a repeat call after
    /// this restore's own prior tick already finished the plan) — not
    /// itself forward progress, but not a fault either.
    Complete,
    /// A store fault or corrupt segment stopped the sweep before it reached
    /// its own end.
    NoProgress,
}

/// Replay every [`animus_control::PitrReplaySegmentRef`] in `plan.segments`
/// (ADR 0059 §10, Train 3 PR②) into `group`'s own destination tablet:
/// fetch each segment object, slice it to the plan's own already-resolved
/// `replay_range` (`segment::decode_and_slice` — the identical superset-
/// slice discipline the Streams read path already trusts), decode each
/// record's opaque `change_record` bytes back into an
/// [`animus_dynamo::ChangeRecord`] (this crate's own layer — PITR's
/// segment codec, like the stream sealer's, treats it as opaque, ADR
/// 0043's layering rule), and re-derive the identical physical
/// `KIND_BASE`/`KIND_LSI` writes a **live** write of that same historical
/// mutation would have produced, via [`crate::dynamo::kind_writes_for_item`]
/// — the same pure function `kind_write_item_at_leader` calls for an
/// ordinary write, so a replayed row's LSI add/remove bookkeeping is
/// derived identically rather than a second, independently-maintained
/// copy of that logic.
///
/// **`KIND_FOOTPRINT` is deliberately never reconstructed here** — a
/// restored table's GSIs are rebuilt from scratch by the backfill seeder
/// (ADR 0059 §8) once this restore activates, which derives every
/// footprint fresh from the table's own **final** `KIND_BASE` content
/// regardless of how that content was assembled (base snapshot seeding,
/// replay, or an ordinary live write) — seeding a footprint here would
/// only ever be redundant with, never a substitute for, that sweep.
///
/// **`KIND_CHANGE` is deliberately never reproduced either** — the derived
/// `change_log` half of `kind_writes_for_item`'s return is discarded on
/// every iteration: a restored table's own change log starts empty,
/// exactly like a split child's (ADR 0050) and an on-demand-restored
/// table's (ADR 0059 §7/Train 2's own copy-kinds rule).
///
/// **`encode_restored_value` re-wraps every derived value**, for the
/// identical reason Train 2's own base-snapshot sweep above does: both a
/// change record's own `new_image` and `kind_writes_for_item`'s derived
/// LSI values are plain, envelope-less bytes (`wire::encode_stored_item`'s
/// own output), and `SeedBatch`'s merge is a raw envelope-tag-included
/// byte passthrough — this is Train 2's own as-built amendment's "very
/// likely yes" prediction about PITR replay, confirmed while building it.
///
/// A record with `ChangeRecord::consumer_hidden()` true (a GSI backfill's
/// own synthetic marker, an ADR 0049 §1 image-less marker, or an ADR
/// 0049 §3 stage marker) is skipped outright — none of the three carries
/// row content a restore could replay, and (for the marker case) PITR
/// forces every write on a PITR-enabled table through the image-carrying
/// evaluate-at-leader path regardless (`table_change_records_carry_images`),
/// so a genuine marker should never appear in a PITR segment in the first
/// place; skipped rather than treated as fatal purely as defense in depth.
async fn replay_pitr_segments(
    ctx: &ClientCtx,
    group: &CpGroup,
    backup_id: &str,
    target_table: &str,
    plan: &animus_control::PitrRestorePlan,
) -> ReplayOutcome {
    let mut progressed = false;
    for seg in &plan.segments {
        let bytes = match ctx.backup_store.get_any(&seg.object_id).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                tracing::debug!(
                    backup_id,
                    object_id = %seg.object_id,
                    "restore: PITR segment object not found yet, retrying"
                );
                return ReplayOutcome::NoProgress;
            }
            Err(err) => {
                tracing::debug!(
                    backup_id,
                    object_id = %seg.object_id,
                    %err,
                    "restore: PITR segment read failed, retrying"
                );
                return ReplayOutcome::NoProgress;
            }
        };
        let (_header, records) =
            match animus_cp_data::segment::decode_and_slice(&bytes, seg.replay_range) {
                Ok(decoded) => decoded,
                Err(err) => {
                    tracing::warn!(
                        backup_id,
                        object_id = %seg.object_id,
                        %err,
                        "restore: PITR segment object is corrupt"
                    );
                    return progress_outcome_replay(progressed);
                }
            };
        if records.is_empty() {
            progressed = true;
            continue;
        }

        // Read fresh each segment (cheap — the target's own schema/LSIs
        // never change mid-restore in practice, and re-reading avoids
        // caching a snapshot across what can be many `.await` points).
        let meta = ctx.effective_metadata();
        let mut seed_rows: Vec<animus_cp_data::SeedRow> = Vec::new();
        for rec in &records {
            let Some(change) = animus_dynamo::ChangeRecord::decode(&rec.change_record) else {
                tracing::warn!(
                    backup_id,
                    object_id = %seg.object_id,
                    "restore: PITR change record is corrupt, skipping"
                );
                continue;
            };
            if change.consumer_hidden() {
                continue;
            }
            let Some(item) = change.new_image.as_ref().or(change.old_image.as_ref()) else {
                continue; // both images absent — nothing to replay
            };
            let schema = crate::dynamo::schema_for(&meta, target_table);
            let Some(pk) = item.get(&schema.partition_key).cloned() else {
                tracing::warn!(
                    backup_id,
                    "restore: PITR change record is missing its own partition key attribute"
                );
                continue;
            };
            let sk = schema
                .sort_key
                .as_ref()
                .and_then(|name| item.get(name))
                .cloned();
            let base_key = crate::dynamo::item_key(&pk, sk.as_ref());
            let base_value = match &change.new_image {
                Some(item) => animus_dynamo::wire::encode_stored_item(item),
                None => animus_dynamo::wire::encode_tombstone(),
            };
            let (writes, _change_log) = crate::dynamo::kind_writes_for_item(
                &meta,
                target_table,
                &pk,
                sk.as_ref(),
                &base_key,
                base_value,
                change.old_image.as_ref(),
                change.new_image.as_ref(),
                change.ttl_expired,
            );
            for (kind, key, value) in writes {
                seed_rows.push((
                    kind,
                    key,
                    value.map(|v| backup_codec::encode_restored_value(&v)),
                    rec.packed_hlc,
                ));
            }
        }
        if !propose_local(group, seed_rows).await {
            return progress_outcome_replay(progressed);
        }
        progressed = true;
    }
    if progressed {
        ReplayOutcome::Progressed
    } else {
        ReplayOutcome::Complete
    }
}

/// [`ReplayOutcome::Progressed`] if this call's sweep got at least one
/// segment further than where it started, else [`ReplayOutcome::
/// NoProgress`] — the [`ReplayOutcome`] twin of [`progress_outcome`].
fn progress_outcome_replay(progressed: bool) -> ReplayOutcome {
    if progressed {
        ReplayOutcome::Progressed
    } else {
        ReplayOutcome::NoProgress
    }
}

/// [`RestoreTickOutcome::Progressed`] if this call's sweep got at least one
/// chunk further than where it started, else [`RestoreTickOutcome::
/// NoProgress`] — the stuck-timeout clock (`backup_restore_loop`) only
/// resets on genuine forward movement, never on a call that immediately
/// faulted with nothing seeded.
fn progress_outcome(progressed: bool) -> RestoreTickOutcome {
    if progressed {
        RestoreTickOutcome::Progressed
    } else {
        RestoreTickOutcome::NoProgress
    }
}

/// Propose `rows` as this restore's own destination tablet's `SeedBatch` on
/// a **known-leader** local handle, confirming by applied index — the
/// identical shape `animusd::ClientCtx::seed_rows_local` uses for the
/// split-build driver's own seeding (`propose_seed_batch`'s merge-at-
/// carried-version semantics make this idempotent under retry).
async fn propose_local(group: &CpGroup, rows: Vec<animus_cp_data::SeedRow>) -> bool {
    if rows.is_empty() {
        return true;
    }
    let index = match group.propose_seed_batch(rows) {
        ProposeResult::Accepted { index, .. } => index,
        other => {
            tracing::debug!(?other, "restore: seed batch not accepted");
            return false;
        }
    };
    let deadline = tokio::time::Instant::now() + CONFIRM_TIMEOUT;
    while tokio::time::Instant::now() < deadline {
        if group.engine_applied_index() >= index {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

/// Propose `MetaCommand::CompleteRestore`, relayed via
/// [`ClientCtx::propose_schema`] (on the `is_relayable_command` allowlist)
/// since this tablet's own leader need not be the control-plane leader.
/// Idempotent from the caller's point of view: a repeat once already `Done`
/// is simply rejected by the state machine, harmlessly. Then (ADR 0059 §8)
/// declares every one of this restore's resolved GSI definitions via
/// `MetaCommand::CreateTableIndex` (already on the relay allowlist, the
/// identical schema-catalog-class reasoning `SetTableStream`/
/// `SetTableTtl` share) — fire-and-forget, not commit-waited: the backfill
/// seeder + completion aggregator (ADR 0045, unmodified) converge each one
/// to `Active` from here entirely on their own, exactly as they do for an
/// `UpdateTable`-added GSI on any other populated table. Safe to call on
/// every tick this restore is observed `Done` on (a repeat `CreateTableIndex`
/// of an unchanged definition is a harmless idempotent `upsert_index`, per
/// that command's own doc) — this driver doesn't track "did I already
/// declare these" separately, mirroring its own "no durable position"
/// stance elsewhere in this module.
async fn complete_restore(
    ctx: &ClientCtx,
    restore_id: &str,
    target_table: &str,
    gsi_defs: &[IndexDef],
) {
    let _ = ctx
        .propose_schema(&MetaCommand::CompleteRestore {
            restore_id: restore_id.to_owned(),
        })
        .await;
    for def in gsi_defs {
        let _ = ctx
            .propose_schema(&MetaCommand::CreateTableIndex {
                table: target_table.to_owned(),
                index: def.clone(),
            })
            .await;
    }
}

/// Propose `MetaCommand::FailRestore` — see [`complete_restore`]'s doc for
/// the relay reasoning, identical here.
async fn fail_restore(ctx: &ClientCtx, restore_id: &str, reason: &str) {
    let _ = ctx
        .propose_schema(&MetaCommand::FailRestore {
            restore_id: restore_id.to_owned(),
            reason: reason.to_owned(),
        })
        .await;
}
