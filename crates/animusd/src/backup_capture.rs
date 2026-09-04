//! On-demand backup **capture driver** (ADR 0059 §4/§5/§6, Train 1 PR③): a
//! per-tablet, leader-side, event-driven loop that sweeps a `Creating`
//! backup's `KIND_BASE`/`KIND_LSI`/`KIND_FOOTPRINT` rows into chunked
//! backup-store objects, then reports completion through the control-plane
//! catalog. See [`crate::backup_completion`] for the control-plane-leader
//! aggregator this driver's reports feed.
//!
//! ## Discovery
//!
//! Not polling, and not the metadata-watch-driven wake `index_drain.rs`'s
//! module doc describes for the split-build driver — this loop shares that
//! driver's *tick cadence* (a fixed interval, mirroring every per-tablet
//! consumer loop in this crate) but its *trigger* is simpler: each tick,
//! for every `Creating` backup and every tablet this node currently leads,
//! [`animus_control::Metadata::backup_capture_target`] (the ONE predicate
//! shared with the completion aggregator and the `ANIMUS_BACKUP_SEEDS`
//! corpus) decides whether this tablet owes that backup a capture —
//! directly pinned, or a live `split_lineage` descendant of a retired
//! pinned tablet (§6, re-planning). A tablet that already has its own
//! progress row (`Metadata::backup_tablet_progress`) is skipped outright —
//! no redundant cursor read, no wake.
//!
//! ## Object identity (write-once discipline)
//!
//! A tablet's own share of one backup writes objects at
//! `backup/{backup_id}/{tablet}/{chunk}`
//! ([`animus_cp_data::backup::backup_data_object_id`]), where `chunk` is
//! this tablet's own **durable, monotonic** per-backup counter carried
//! inside [`CaptureCursor`] — never re-derived, never guessed. Correctness
//! rests on one invariant: **the exact bytes written under a given
//! `(backup_id, tablet, chunk)` triple are a pure function of the
//! durably-committed [`CaptureCursor`] state that preceded that chunk.**
//! Every new cursor row commits only immediately after the store `put` it
//! accounts for, in the same tick, reading whatever cursor state is
//! durably on file at tick start:
//!
//! - **A crash between the store `put` and the cursor's own commit** (a
//!   real leader-change hazard: the old leader's `put` succeeded but its
//!   `KindBatch` proposal never applied) is safe by construction: the new
//!   leader reads the SAME last-committed cursor, so it re-derives the
//!   identical `(kind, start_key, cut_version)` triple, scans the identical
//!   snapshot-pinned rows (every correctly-caught-up replica applies the
//!   same Raft log, so "state as of version V" is the same logical
//!   snapshot everywhere — [`animus_cp_data::RaftKvNode::
//!   local_scan_kind_snapshot`]'s own doc), and re-`put`s at the SAME
//!   `chunk` index with byte-identical content.
//!   [`animus_env::SegmentStore::put`]'s own write-once contract tolerates
//!   exactly this: an identical-content re-put at an already-written id is
//!   a safe no-op.
//! - **`cut_version` is pinned once, at a tablet's first tick for this
//!   backup, and never re-derived.** If it were re-read from
//!   [`CpGroup::engine_latest_version`] on every tick, a leader change
//!   mid-capture could observe a HIGHER watermark (more rows committed
//!   since) and produce a WIDER slice for the same `next_key` — different
//!   content at an already-used `chunk` index, the exact violation
//!   write-once exists to catch. Pinning it into the cursor row itself,
//!   durably, the first time a tablet starts capturing this backup is what
//!   makes every later tick — on any replica — replay the identical
//!   snapshot.
//! - **A split re-planning a tablet's own share (§6) never reuses a
//!   `chunk` index across tablet identities**: each live tablet (the
//!   originally-pinned one, or a re-planned descendant) has its own
//!   `(backup_id, tablet)`-scoped object namespace and its own
//!   independently-numbered `chunk` counter (a fresh [`CaptureCursor`],
//!   since a split child's `KIND_CURSOR` scope is born empty — the
//!   identical `SplitPolicy::RestartFromScratch` the GSI drain/backfill
//!   seeder already rely on, `animus_cp_data::cursor`'s own doc), so two
//!   different tablet ids can never collide on the same object id
//!   regardless of timing.
//!
//! ## The cursor
//!
//! A durable, resumable `KIND_CURSOR` row (the backfill-seeder shape,
//! `index_drain.rs`'s own per-index cursor convention), tag
//! [`backup_cursor_tag`] — see `animus_cp_data::cursor`'s own "Two value
//! conventions" doc for why this is a THIRD, bespoke value shape rather
//! than either existing convention: [`CaptureCursor`] carries more than a
//! bare watermark or a bare key (the pinned `cut_version`, the current
//! phase, the resume key, the running chunk counter and byte total).
//!
//! ## Quiescence
//!
//! Deliberately minimal: unlike the split-build driver (which holds a
//! standing quiesce veto for its whole multi-phase workflow), this driver
//! only calls [`CpGroup::wake`] immediately before a tick that actually
//! proposes something (a chunk's cursor advance, or the terminal completion
//! report) — mirroring the TTL reaper's identical "read for free, wake only
//! to write" discipline (`ttl_reaper.rs`'s own doc: a quiesced,
//! nothing-to-do tablet costs one idle local scan per tick and stays
//! quiesced). A tablet with no `Creating` backup pinning it is never woken
//! by this driver at all; one that IS a target wakes for exactly as many
//! ticks as it has chunks left to write, then goes quiet again once it
//! reports completion — "don't defeat quiescence for idle groups beyond
//! what a capture in progress genuinely needs" (ADR 0059 §4).
//!
//! ## Reads never block writes
//!
//! Every read here ([`CpGroup::local_get_kind`]/
//! [`CpGroup::local_scan_kind_snapshot`]) is a local, non-blocking engine
//! read at a fixed, already-committed snapshot version — no read barrier,
//! no lock across a concurrent write, no proposed freeze. The only writes
//! this driver ever makes are its own bookkeeping (the cursor row, and —
//! once — a control-plane completion report); it never touches, blocks, or
//! delays an ordinary client write to the tablet it is capturing.

use std::time::Duration;

use animus_control::{BackupStatus, ProposeResult};
use animus_cp_data::backup as backup_codec;
use animus_cp_data::cursor;
use animus_cp_data::{KIND_BASE, KIND_CURSOR, KIND_FOOTPRINT, KIND_LSI, SeedRow};
use animus_tablet::TabletId;

use crate::{ClientCtx, CpGroup, MetaCommand};

/// This loop's tick cadence — matches `index_backfill.rs`/
/// `segment_janitor.rs`'s own `_INTERVAL` constants: cheap per-tick work,
/// and a fast tick keeps this crate's own converged-or-timeout tests from
/// becoming the slow part of the corpus.
pub(crate) const BACKUP_CAPTURE_INTERVAL: Duration = Duration::from_millis(200);

/// How long [`propose_cursor`]/the completion report wait for their own
/// proposal to apply locally before giving up for this tick (the next tick
/// simply retries — every proposal here is idempotent or write-once-safe to
/// repeat).
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);

/// A deliberate Train 1 simplification against the split-build driver's own
/// [`SEED_CHUNK_BYTES`](crate::index_drain)-style **byte** budget (that
/// constant is private to `index_drain.rs`, whose own module is not this
/// one — Rust module privacy, not a deliberate divergence — so it isn't
/// literally reusable here): each data-chunk object holds up to this many
/// rows rather than a byte budget. A modest row count keeps an individual
/// object well under any real DynamoDB item's own ~400 KB bound in
/// practice; matching the split driver's byte-budgeted chunking exactly is
/// a named follow-up, not a correctness requirement — capture's own
/// [`animus_cp_data::backup::encode_data_chunk`] format has no size limit
/// of its own either way.
const CHUNK_ROWS: usize = 200;

/// The three row kinds a backup ever captures (ADR 0059 §2), in sweep
/// order — never `KIND_CHANGE` (a restored table's own change log starts
/// empty, exactly like a split child's) or `KIND_CURSOR` (per-tablet-
/// identity bookkeeping, meaningless on a freshly restored tablet id), the
/// identical copy-kinds rule the split-build driver already enforces for
/// `SeedBatch`.
const CAPTURE_KINDS: [u8; 3] = [KIND_BASE, KIND_LSI, KIND_FOOTPRINT];

/// A tablet's own per-backup capture progress — the exact bytes this
/// module's `KIND_CURSOR` row (tag [`backup_cursor_tag`]) holds. See the
/// module doc's "Object identity" section for why every field here is
/// exactly what write-once safety across a crash/leader-change needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CaptureCursor {
    /// The snapshot version this tablet's whole capture is pinned to —
    /// read ONCE, at this cursor's first write, from
    /// [`CpGroup::engine_latest_version`]. Never re-derived on a later
    /// tick.
    pub(crate) cut_version: u64,
    /// Index into [`CAPTURE_KINDS`] currently being swept, or
    /// `CAPTURE_KINDS.len()` once every kind is exhausted (capture done).
    pub(crate) phase: usize,
    /// The current phase's own resume key
    /// ([`CpGroup::local_scan_kind_snapshot`]'s own cursor convention) —
    /// empty at the start of a phase.
    pub(crate) next_key: Vec<u8>,
    /// The next chunk index this tablet will write for this backup — the
    /// object-identity counter (see the module doc).
    pub(crate) next_chunk: u64,
    /// Running total bytes of every chunk object written so far — reported
    /// verbatim as `RecordBackupTabletComplete`'s own `bytes` once done.
    pub(crate) bytes_so_far: u64,
}

impl CaptureCursor {
    fn fresh(cut_version: u64) -> Self {
        CaptureCursor {
            cut_version,
            phase: 0,
            next_key: Vec::new(),
            next_chunk: 0,
            bytes_so_far: 0,
        }
    }

    fn done(&self) -> bool {
        self.phase >= CAPTURE_KINDS.len()
    }
}

/// This backup's own `KIND_CURSOR` tag on one tablet — registered in
/// `animus_cp_data::cursor::classify_tag` (`SplitPolicy::
/// RestartFromScratch`: see this module's own "Object identity" doc for
/// why a re-planned split descendant simply restarting its own share from
/// scratch is exactly the right behavior, not a gap).
pub(crate) fn backup_cursor_tag(backup_id: &str) -> String {
    format!("backup:{backup_id}")
}

/// [`CaptureCursor`]'s wire version byte — bumped on any incompatible
/// layout change (pre-alpha, no back-compat need, but a version byte keeps
/// a future change a loud, deliberate decision rather than a silent
/// misread, mirroring every other hand-rolled codec in this codebase).
const CURSOR_CODEC_VERSION: u8 = 1;

/// `[version, phase, cut_version(8), next_chunk(8), bytes_so_far(8),
/// next_key_len(4), next_key]`.
pub(crate) fn encode_capture_cursor(c: &CaptureCursor) -> Vec<u8> {
    let mut out = Vec::with_capacity(30 + c.next_key.len());
    out.push(CURSOR_CODEC_VERSION);
    out.push(u8::try_from(c.phase).expect("CAPTURE_KINDS.len() fits a byte"));
    out.extend_from_slice(&c.cut_version.to_be_bytes());
    out.extend_from_slice(&c.next_chunk.to_be_bytes());
    out.extend_from_slice(&c.bytes_so_far.to_be_bytes());
    out.extend_from_slice(
        &(u32::try_from(c.next_key.len()).expect("a real key fits a u32 length")).to_be_bytes(),
    );
    out.extend_from_slice(&c.next_key);
    out
}

/// The dual of [`encode_capture_cursor`]. `None` on anything malformed —
/// this crate only ever reads back what it itself wrote (mirroring
/// `seal.rs`/`ceiling.rs`'s "an internal marker should never be malformed"
/// doctrine elsewhere in this codebase), so a caller sees this as a
/// defensive read, not an expected case; a `None` here is treated
/// identically to "no cursor yet" by every caller, which is always safe
/// (worst case: the tablet's own capture restarts this backup from
/// scratch, re-`put`ting chunk 0 onward — safe only because a fresh
/// `cut_version` pin at that point would DIFFER from any already-written
/// chunk's own basis, so in practice this path should never be exercised
/// outside a genuine codec bug; left defensive rather than a hard panic for
/// the same reason `decode_backfill_cursor`'s sibling conventions are).
pub(crate) fn decode_capture_cursor(bytes: &[u8]) -> Option<CaptureCursor> {
    const HEADER: usize = 1 + 1 + 8 + 8 + 8 + 4;
    if bytes.len() < HEADER || bytes[0] != CURSOR_CODEC_VERSION {
        return None;
    }
    let phase = bytes[1] as usize;
    let cut_version = u64::from_be_bytes(bytes[2..10].try_into().ok()?);
    let next_chunk = u64::from_be_bytes(bytes[10..18].try_into().ok()?);
    let bytes_so_far = u64::from_be_bytes(bytes[18..26].try_into().ok()?);
    let key_len = u32::from_be_bytes(bytes[26..30].try_into().ok()?) as usize;
    if bytes.len() != HEADER + key_len {
        return None;
    }
    let next_key = bytes[HEADER..].to_vec();
    Some(CaptureCursor {
        cut_version,
        phase,
        next_key,
        next_chunk,
        bytes_so_far,
    })
}

/// Every led tablet's own capture step, once per [`BACKUP_CAPTURE_INTERVAL`]
/// tick — see the module doc's "Discovery" section for the per-`(backup,
/// tablet)` targeting decision.
pub(crate) async fn backup_capture_loop(ctx: ClientCtx) {
    loop {
        tokio::time::sleep(BACKUP_CAPTURE_INTERVAL).await;
        let meta = ctx.effective_metadata();
        if meta.backups.is_empty() {
            continue;
        }
        let creating: Vec<&String> = meta
            .backups
            .iter()
            .filter(|(_, row)| matches!(row.status, BackupStatus::Creating))
            .map(|(id, _)| id)
            .collect();
        if creating.is_empty() {
            continue;
        }
        for (tablet, group) in ctx.edge.hosted_groups() {
            if !group.is_leader() {
                continue;
            }
            for backup_id in &creating {
                if meta
                    .backup_tablet_progress
                    .contains_key(&((*backup_id).clone(), tablet))
                {
                    continue; // already reported — nothing left to do
                }
                if !meta.backup_capture_target(backup_id, tablet) {
                    continue; // not (or no longer) this tablet's backup
                }
                backup_capture_tick(&ctx, &group, tablet, backup_id).await;
            }
        }
    }
}

/// One capture step for one `(backup_id, tablet)` pair this node leads
/// (ADR 0059 §4). Reads the durable cursor (pinning a fresh one at this
/// tablet's own current watermark if none exists yet), does at most ONE
/// chunk's worth of work, and — once every kind is exhausted — proposes
/// [`MetaCommand::RecordBackupTabletComplete`]. Returns once this tick's
/// single step is done; [`backup_capture_loop`] re-invokes on the next
/// tick until the tablet's own share reports complete — no internal
/// loop-to-completion, mirroring every other per-tick driver in this
/// crate (`backfill_seed_tick`, `seal_tick`).
async fn backup_capture_tick(ctx: &ClientCtx, group: &CpGroup, tablet: TabletId, backup_id: &str) {
    let tag = backup_cursor_tag(backup_id);
    let cursor_key = cursor::cursor_key(&group.scope_range().start, &tag);
    let mut cur = group
        .local_get_kind(KIND_CURSOR, &cursor_key)
        .await
        .and_then(|b| decode_capture_cursor(&b))
        .unwrap_or_else(|| CaptureCursor::fresh(group.engine_latest_version()));

    if cur.done() {
        report_capture_complete(ctx, backup_id, tablet, &cur).await;
        return;
    }

    let kind = CAPTURE_KINDS[cur.phase];
    let (rows, next) = group
        .local_scan_kind_snapshot(kind, &cur.next_key, cur.cut_version, CHUNK_ROWS)
        .await;

    if rows.is_empty() {
        // This phase's own kind scope is exhausted at this cut_version —
        // advance to the next kind (or Done). No store write is needed:
        // nothing here is data, just bookkeeping.
        cur.phase += 1;
        cur.next_key = Vec::new();
        group.wake();
        if propose_cursor(group, &cursor_key, &cur).await && cur.done() {
            report_capture_complete(ctx, backup_id, tablet, &cur).await;
        }
        return;
    }

    let seed_rows: Vec<SeedRow> = rows
        .iter()
        .map(|(k, v, ver)| (kind, k.clone(), Some(v.clone()), *ver))
        .collect();
    let object_bytes = backup_codec::encode_data_chunk(&seed_rows);
    let object_id = backup_codec::backup_data_object_id(backup_id, tablet.0, cur.next_chunk);

    // Write-once: an identical-content re-put at an already-written id
    // (this exact chunk, re-derived after a crash between this `put` and
    // the cursor-advance below ever committing) is a safe no-op. A genuine
    // store fault (including an injected ack-lost put — the object landed,
    // the ack didn't) is tolerated by simply not advancing the cursor: the
    // NEXT tick re-derives the identical bytes from the same durable
    // cursor state and retries the identical `put`.
    if let Err(err) = ctx.backup_store.put(&object_id, &object_bytes).await {
        tracing::debug!(
            backup_id,
            tablet = tablet.0,
            chunk = cur.next_chunk,
            error = %err,
            "backup capture: chunk put failed, will retry next tick"
        );
        return;
    }

    cur.next_chunk += 1;
    cur.bytes_so_far += object_bytes.len() as u64;
    match next {
        Some(key) => cur.next_key = key,
        None => {
            cur.phase += 1;
            cur.next_key = Vec::new();
        }
    }
    group.wake();
    if propose_cursor(group, &cursor_key, &cur).await && cur.done() {
        report_capture_complete(ctx, backup_id, tablet, &cur).await;
    }
}

/// Commit `cur` as this tablet's own new `KIND_CURSOR` row, waiting
/// (bounded by [`CONFIRM_TIMEOUT`]) for it to apply locally before
/// returning `true` — mirrors `index_drain::advance_backfill_cursor`'s
/// identical confirm discipline exactly, so the *next* tick's own
/// [`CpGroup::local_get_kind`] read reliably observes this tick's advance
/// rather than racing it. `false` on a rejected propose or a timed-out
/// confirm; the caller simply leaves the completion report for a later
/// tick either way (a spurious re-derivation of the same chunk is always
/// safe — see the module doc).
async fn propose_cursor(group: &CpGroup, cursor_key: &[u8], cur: &CaptureCursor) -> bool {
    let index = match group.put_kind_batch_conditioned(
        vec![(
            KIND_CURSOR,
            cursor_key.to_vec(),
            Some(encode_capture_cursor(cur)),
        )],
        Vec::new(),
        Vec::new(),
    ) {
        ProposeResult::Accepted { index, .. } => index,
        other => {
            tracing::debug!(?other, "backup capture: cursor advance not accepted");
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

/// Propose [`MetaCommand::RecordBackupTabletComplete`] for a tablet whose
/// [`CaptureCursor`] has reached [`CaptureCursor::done`] — idempotent
/// (the catalog's own apply arm no-ops an identical repeat), so this is
/// safe to call on every tick a `Done` cursor is observed; skips the
/// proposal entirely once this node's own metadata mirror already shows
/// the report landed, so a long-lived `Creating` backup with a slow
/// aggregator doesn't spam the control plane forever. Relayed via
/// [`ClientCtx::propose_schema`] (`MetaCommand::RecordBackupTabletComplete`
/// is on the `is_relayable_command` allowlist) since this tablet's own
/// leader need not be — and on a split deployment, may not even be
/// control-connected to — the control-plane leader.
async fn report_capture_complete(
    ctx: &ClientCtx,
    backup_id: &str,
    tablet: TabletId,
    cur: &CaptureCursor,
) {
    if ctx
        .effective_metadata()
        .backup_tablet_progress
        .contains_key(&(backup_id.to_owned(), tablet))
    {
        return;
    }
    let _ = ctx
        .propose_schema(&MetaCommand::RecordBackupTabletComplete {
            backup_id: backup_id.to_owned(),
            tablet,
            cut_version: cur.cut_version,
            bytes: cur.bytes_so_far,
        })
        .await;
}

#[cfg(test)]
mod cursor_codec_tests {
    use super::*;

    #[test]
    fn round_trips_empty_and_populated_keys() {
        for cur in [
            CaptureCursor::fresh(0),
            CaptureCursor::fresh(12_345),
            CaptureCursor {
                cut_version: 999,
                phase: 2,
                next_key: b"some-real-row-key-bytes".to_vec(),
                next_chunk: 7,
                bytes_so_far: 123_456,
            },
        ] {
            let bytes = encode_capture_cursor(&cur);
            assert_eq!(decode_capture_cursor(&bytes), Some(cur));
        }
    }

    #[test]
    fn done_is_exactly_phase_past_the_last_kind() {
        let mut cur = CaptureCursor::fresh(0);
        assert!(!cur.done());
        cur.phase = CAPTURE_KINDS.len() - 1;
        assert!(!cur.done());
        cur.phase = CAPTURE_KINDS.len();
        assert!(cur.done());
    }

    #[test]
    fn decode_rejects_a_truncated_buffer_and_a_bad_version() {
        let cur = CaptureCursor::fresh(1);
        let mut bytes = encode_capture_cursor(&cur);
        assert!(decode_capture_cursor(&bytes[..bytes.len() - 1]).is_none());
        bytes[0] = CURSOR_CODEC_VERSION + 1;
        assert!(decode_capture_cursor(&bytes).is_none());
    }

    #[test]
    fn backup_cursor_tag_is_stable() {
        assert_eq!(backup_cursor_tag("bkp-1"), "backup:bkp-1");
    }
}
