//! The DynamoDB-style **TTL reaper** (ADR 0051), moved here by ADR 0061
//! rung C2 — see `crate::host::TtlScanHost` for the narrow capability this
//! loop needs from its host (a `Metadata` read, which tablets this node
//! leads, a pure local non-waking scan, and one conditional-delete write)
//! and why those four operations are enough to carry the *entire* control
//! flow below out of `animusd`, even though the write itself still
//! delegates to `animusd`'s own ADR 0049 kind-write machinery.
//!
//! A per-node background loop that deletes items whose declared TTL
//! attribute has passed, on every tablet this node currently **leads** of a
//! TTL-enabled table.
//!
//! ## Why this is small
//!
//! ADR 0051 §3 keeps every read path AWS-faithful: an expired item stays
//! visible until something actually deletes it. This loop is that
//! something. It is deliberately the only place TTL expiry is *decided* —
//! [`TtlScanHost::ttl_delete_if_attribute_equals`] rides the identical
//! `DeleteItem` primitive an ordinary client delete uses, so GSI rows, LSI
//! rows, the change-log record, and the stream image all fall out of that
//! shared write path for free. This module owns only the scan and the
//! per-item expiry decision.
//!
//! ## Wall-clock time (ADR 0051 §1)
//!
//! Every expiry decision compares a stored item's declared expiry against
//! **`env.wall_now()`**, never `env.now()` — see [`animus_env::Clock::
//! wall_now`]'s own doc for why: `now()` is monotonic-since-start and
//! carries no calendar meaning, so it cannot interpret a client-supplied
//! absolute epoch second.
//!
//! ## Quiescence: read without waking, wake only to delete (ADR 0051 §6)
//!
//! [`TtlScanHost::scan_base_capped`]'s own contract requires it to be a
//! **pure, non-waking** local read (ADR 0048) — a tablet with nothing
//! expired therefore costs this node exactly one idle scan per sweep
//! interval and leaves quiescence completely undisturbed.
//! [`TtlScanHost::ttl_delete_if_attribute_equals`] wakes the group itself,
//! immediately before proposing — see that method's own doc.
//!
//! ## Bounding one tick's work
//!
//! A led tablet's own [`TTL_SCAN_BATCH`] rows are read per sweep, resuming
//! from a **driver-local** cursor (`BTreeMap<TabletId, Vec<u8>>`) — no
//! durable cursor row, since an interrupted sweep simply resumes (or, on a
//! crash/leader change, restarts from scratch, which is always safe:
//! `is_expired` is a pure function of the item and the current wall clock).
//!
//! ## The conditional delete (ADR 0051 §4)
//!
//! Every delete is conditional on the exact `AttributeValue` this sweep
//! observed for the TTL attribute — a client's concurrent TTL refresh/
//! removal makes the delete a no-op (routine, not an error) instead of
//! racing it. See [`TtlScanHost::ttl_delete_if_attribute_equals`]'s own doc.
//!
//! ## Progress reporting (roadmap U-07)
//!
//! [`TtlReaperProgress`] is a small, `Env`-free snapshot of this loop's own
//! state — phase, last tick, resume cursor, and cumulative counters —
//! published through [`crate::host::TtlReaperProgressHost`] at each phase
//! transition below, mirroring `backup_janitor`'s own `JanitorProgress`
//! precedent exactly. Unlike the backup janitor (control-plane-leader-only,
//! so only ONE node's progress is ever meaningful), the TTL reaper runs on
//! **every** node, self-gated per tablet — so every node's own
//! `TtlReaperProgress` is a genuine, independently meaningful answer, not
//! just an honestly-idle placeholder. `animusd::ClientCtx` backs it with an
//! `Arc<Mutex<TtlReaperProgress>>` (`std::sync::Mutex`, never held across an
//! `.await`), read by `GET /admin/ttl`. Every timestamp is
//! [`animus_env::Env::now`]-derived, never a wall clock (ADR 0003) — this
//! loop's progress snapshot stays meaningful under `SimEnv` too. The
//! resume cursor is rendered hex-encoded and truncated
//! ([`TTL_REAPER_CURSOR_KEY_CAP`]) — an admin route must never become a way
//! to reconstruct arbitrary item key bytes.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::Metadata;
use animus_dynamo::{is_expired, wire};
use animus_env::Env;
use animus_tablet::{TabletId, TabletState};
use serde::{Deserialize, Serialize};

use crate::host::{TtlReaperProgressHost, TtlScanHost};

/// How often each node sweeps the tablet groups it leads for expired TTL
/// items — the production default. Minutes, not a sub-second interval: see
/// `animusd/CLAUDE.md`'s `ttl_reaper.rs` entry for the full "why a minute"
/// reasoning (unchanged by this move). A small value in tests.
pub const DEFAULT_TTL_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// How many base rows one tick scans **per led, TTL-enabled tablet** —
/// bounds one sweep's own scan (and, transitively, its worst-case propose
/// count: at most one delete per scanned row).
const TTL_SCAN_BATCH: usize = 500;

/// How many bytes of a tablet's own driver-local resume key
/// [`TtlReaperCursor::key_hex`] renders (roadmap U-07), before
/// hex-encoding — never the whole key. See the module doc's "Progress
/// reporting" section.
const TTL_REAPER_CURSOR_KEY_CAP: usize = 24;

/// One phase of the TTL reaper's own tick (roadmap U-07) — rendered on
/// `GET /admin/ttl`. Mirrors the loop's real per-tablet control flow: a
/// tick with nothing to do (no led tablets this sweep) is `Idle`; scanning
/// a led tablet's own base rows is `Scanning`; proposing the conditional
/// delete for an expired row is `Deleting`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TtlReaperPhase {
    /// This node currently leads no tablet of any TTL-enabled table (or no
    /// tick has run yet).
    #[default]
    Idle,
    /// Scanning a led tablet's own base rows for expired items.
    Scanning,
    /// Proposing the conditional delete for an item this sweep found
    /// expired.
    Deleting,
}

/// This loop's own driver-local resume position for one tablet, rendered
/// on `GET /admin/ttl` (roadmap U-07) — a JSON-safe projection of the
/// `BTreeMap<TabletId, Vec<u8>>` cursor the loop actually drives itself
/// from, never the raw key bytes (see [`TTL_REAPER_CURSOR_KEY_CAP`]).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtlReaperCursor {
    /// The table this cursor's tablet belongs to.
    pub table: String,
    /// The tablet id this cursor resumes scanning from.
    pub tablet_id: u64,
    /// Hex-encoded resume key, truncated to [`TTL_REAPER_CURSOR_KEY_CAP`]
    /// bytes of the raw key before encoding — never the full key, and
    /// never raw bytes.
    pub key_hex: String,
}

/// A snapshot of the TTL reaper's own progress (roadmap U-07) — see the
/// "Progress reporting" section of the module doc for how this is
/// published and consumed. `deleted_total`/`expired_seen_total`/
/// `tables_with_ttl` are cumulative (never reset) for as long as the host
/// process lives; `deleted_last_tick` resets to 0 at the start of every
/// tick. `Default` is the correct initial state for a node that has never
/// (yet) run a sweep.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TtlReaperProgress {
    /// This tick's phase — see [`TtlReaperPhase`].
    pub phase: TtlReaperPhase,
    /// `env.now()` at the start of this loop's most recently observed
    /// tick, in milliseconds — `None` before the first tick has ever run.
    pub last_tick_at_ms: Option<u64>,
    /// The most recently active tablet's own resume position, as of the
    /// start of its most recent scan — `None` before any tablet has ever
    /// been swept.
    pub cursor: Option<TtlReaperCursor>,
    /// Items actually deleted (`Ok(true)`) during the most recently
    /// completed tick — reset to 0 at the start of every tick.
    pub deleted_last_tick: u64,
    /// Cumulative count of items actually deleted across every tick since
    /// this node started.
    pub deleted_total: u64,
    /// Cumulative count of items this loop has observed as expired
    /// (`is_expired` true), regardless of whether the delete that followed
    /// actually landed (a concurrent TTL refresh/removal can still make it
    /// a no-op) — since this node started.
    pub expired_seen_total: u64,
    /// How many tables in the current catalog have TTL enabled, as of the
    /// most recent tick.
    pub tables_with_ttl: u64,
    /// The most recent delete error observed, if any — cleared once a full
    /// tick completes with no error.
    pub last_error: Option<String>,
}

/// Hex-encode `bytes`, capped at [`TTL_REAPER_CURSOR_KEY_CAP`] bytes
/// (roadmap U-07) — see [`TtlReaperCursor::key_hex`]'s own doc.
fn hex_encode_capped(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(TTL_REAPER_CURSOR_KEY_CAP)
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The **TTL reaper** background loop (ADR 0051 §4/§6) — see the module doc
/// for the full design. One instance per node; self-gates every tick on
/// which tablets [`TtlScanHost::led_tablets`] reports.
pub async fn ttl_reaper_loop<E, H>(env: E, host: H, interval: Duration)
where
    E: Env,
    H: TtlScanHost + TtlReaperProgressHost,
{
    // Driver-local resume cursor per led tablet — see the module doc's
    // "Bounding one tick's work" section.
    let mut cursors: BTreeMap<TabletId, Vec<u8>> = BTreeMap::new();
    loop {
        env.sleep(interval).await;
        let now_ms = env.now().0 / 1_000_000;
        // One `Metadata` snapshot for the whole tick: every tablet this
        // tick visits is judged against the identical view of the catalog,
        // even though every decision here is idempotent and re-derived
        // fresh next tick regardless.
        let meta = host.ttl_metadata();
        cursors.retain(|t, _| meta.tablets.contains_key(t));
        let tables_with_ttl = meta.schemas.iter().filter(|(_, s)| s.ttl.is_some()).count() as u64;
        let led = host.led_tablets();
        host.update_ttl_reaper_progress(&mut |p| {
            p.last_tick_at_ms = Some(now_ms);
            p.tables_with_ttl = tables_with_ttl;
            p.deleted_last_tick = 0;
            // An honest `Idle` when there is nothing this tick could do —
            // overwritten to `Scanning`/`Deleting` by the per-tablet sweep
            // below, and restored to `Idle` once the whole tick completes.
            p.phase = TtlReaperPhase::Idle;
        });
        let mut tick_error: Option<String> = None;
        for tablet in led {
            ttl_sweep_one_tablet(&env, &host, &meta, tablet, &mut cursors, &mut tick_error).await;
        }
        host.update_ttl_reaper_progress(&mut |p| {
            p.phase = TtlReaperPhase::Idle;
            p.last_error = tick_error.clone();
        });
    }
}

/// One led tablet's own sweep — factored out of the loop body so the
/// per-tablet control flow is a plain function, easier to read and to step
/// through in a debugger than an inline loop body. `tick_error` collects
/// the tick's own last observed delete error (see the module doc's
/// "Progress reporting" section) — mirrored on the caller's loop after
/// every tablet in the tick has been visited.
///
/// **`pub` since ADR 0061 rung I (C-09 PR 2)** — previously private to
/// this module. `animusd::SimCluster::drive_ttl_sweep` (a test-only
/// convenience for a scenario that needs to assert an intermediate,
/// pre-cadence reaper state without waiting out the always-on loop's own
/// [`DEFAULT_TTL_SWEEP_INTERVAL`]-scale cadence) calls this directly,
/// looping it to exhaustion over its own driver-local cursor, rather than
/// reimplementing the per-tablet scan/expire/delete control flow a second
/// time — the same "drive the real per-op primitive on demand" shape
/// `index_drain::seal_now`/`drain_tablet` already have for
/// `SimCluster::drive_stream_seal`/`drain_gsi`. No behavior change: this
/// is a pure visibility widening, the exact function every tick of
/// [`ttl_reaper_loop`] already calls.
pub async fn ttl_sweep_one_tablet<E, H>(
    env: &E,
    host: &H,
    meta: &Metadata,
    tablet: TabletId,
    cursors: &mut BTreeMap<TabletId, Vec<u8>>,
    tick_error: &mut Option<String>,
) where
    E: Env,
    H: TtlScanHost + TtlReaperProgressHost,
{
    let Some(tab) = meta.tablets.get(&tablet) else {
        return; // stale view — gone by the time we got here
    };
    // A `Building` split child is unroutable and serves nothing yet (ADR
    // 0050 rung 5) — nothing meaningful to scan. A `Splitting` *parent* is
    // still fully served, so it stays visited.
    if tab.state == TabletState::Building {
        return;
    }
    let Some(table) = tab.table.clone() else {
        return; // legacy whole-keyspace tablet, or a stale view
    };
    // A hidden GSI index table has no schema entry of its own (bookkeeping-
    // only), so `table_ttl` naturally reads `None` for one and this loop
    // skips it without any extra check.
    let Some(ttl) = meta.table_ttl(&table).cloned() else {
        cursors.remove(&tablet);
        return;
    };
    let Some(schema) = meta.table_schema(&table) else {
        return; // schema vanished between the two reads this tick
    };
    let partition_key = schema.partition_key.clone();
    let sort_key = schema.clustering_keys.first().cloned();

    // The one genuinely non-waking read this loop performs — see the
    // module doc's "Quiescence" section.
    let start = cursors.get(&tablet).cloned().unwrap_or_default();
    host.update_ttl_reaper_progress(&mut |p| {
        p.phase = TtlReaperPhase::Scanning;
        p.cursor = Some(TtlReaperCursor {
            table: table.clone(),
            tablet_id: tablet.0,
            key_hex: hex_encode_capped(&start),
        });
    });
    let rows = host.scan_base_capped(tablet, &start, TTL_SCAN_BATCH).await;
    if rows.is_empty() {
        // Either a genuinely empty tablet, or this sweep reached the end of
        // it — either way, wrap around to the beginning next tick.
        cursors.remove(&tablet);
        return;
    }
    let hit_cap = rows.len() == TTL_SCAN_BATCH;
    if hit_cap {
        let mut next_start = rows.last().expect("rows non-empty").0.clone();
        next_start.push(0);
        tracing::debug!(
            tablet = tablet.0,
            table = %table,
            scanned = rows.len(),
            "ttl reaper: hit the per-tick scan cap for this tablet, resuming next tick"
        );
        cursors.insert(tablet, next_start);
    } else {
        // This pass reached the tablet's own end — wrap next tick.
        cursors.remove(&tablet);
    }

    let now = env.wall_now().as_secs();
    for (_key, value) in rows {
        let Ok(Some(item)) = wire::decode_stored_item(&value) else {
            continue; // tombstone, or (shouldn't happen) corrupt bytes
        };
        if !is_expired(&item, &ttl.attribute_name, now) {
            continue;
        }
        host.update_ttl_reaper_progress(&mut |p| {
            p.expired_seen_total = p.expired_seen_total.saturating_add(1);
        });
        let Some(ttl_value) = item.get(&ttl.attribute_name).cloned() else {
            continue; // is_expired implies presence; defensive only
        };
        let Some(pk) = item.get(&partition_key).cloned() else {
            tracing::debug!(
                tablet = tablet.0,
                table = %table,
                "ttl reaper: expired item missing its own partition key attribute, skipping"
            );
            continue;
        };
        let sk = sort_key.as_ref().and_then(|name| item.get(name).cloned());
        host.update_ttl_reaper_progress(&mut |p| {
            p.phase = TtlReaperPhase::Deleting;
        });
        // Both `Ok(true)` (deleted) and `Ok(false)` (condition failed — the
        // item's TTL was refreshed/cleared since this scan observed it,
        // routine and expected) need no further action here beyond the
        // progress counters.
        match host
            .ttl_delete_if_attribute_equals(
                tablet,
                &table,
                &pk,
                sk.as_ref(),
                &ttl.attribute_name,
                ttl_value,
            )
            .await
        {
            Ok(true) => {
                host.update_ttl_reaper_progress(&mut |p| {
                    p.deleted_total = p.deleted_total.saturating_add(1);
                    p.deleted_last_tick = p.deleted_last_tick.saturating_add(1);
                });
            }
            Ok(false) => {}
            Err(err) => {
                tracing::debug!(
                    tablet = tablet.0,
                    table = %table,
                    error = %err,
                    "ttl reaper: delete of an expired item failed"
                );
                *tick_error = Some(err);
            }
        }
    }
}
