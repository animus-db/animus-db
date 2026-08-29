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

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::Metadata;
use animus_dynamo::{is_expired, wire};
use animus_env::Env;
use animus_tablet::{TabletId, TabletState};

use crate::host::TtlScanHost;

/// How often each node sweeps the tablet groups it leads for expired TTL
/// items — the production default. Minutes, not a sub-second interval: see
/// `animusd/CLAUDE.md`'s `ttl_reaper.rs` entry for the full "why a minute"
/// reasoning (unchanged by this move). A small value in tests.
pub const DEFAULT_TTL_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// How many base rows one tick scans **per led, TTL-enabled tablet** —
/// bounds one sweep's own scan (and, transitively, its worst-case propose
/// count: at most one delete per scanned row).
const TTL_SCAN_BATCH: usize = 500;

/// The **TTL reaper** background loop (ADR 0051 §4/§6) — see the module doc
/// for the full design. One instance per node; self-gates every tick on
/// which tablets [`TtlScanHost::led_tablets`] reports.
pub async fn ttl_reaper_loop<E, H>(env: E, host: H, interval: Duration)
where
    E: Env,
    H: TtlScanHost,
{
    // Driver-local resume cursor per led tablet — see the module doc's
    // "Bounding one tick's work" section.
    let mut cursors: BTreeMap<TabletId, Vec<u8>> = BTreeMap::new();
    loop {
        env.sleep(interval).await;
        // One `Metadata` snapshot for the whole tick: every tablet this
        // tick visits is judged against the identical view of the catalog,
        // even though every decision here is idempotent and re-derived
        // fresh next tick regardless.
        let meta = host.ttl_metadata();
        cursors.retain(|t, _| meta.tablets.contains_key(t));
        for tablet in host.led_tablets() {
            ttl_sweep_one_tablet(&env, &host, &meta, tablet, &mut cursors).await;
        }
    }
}

/// One led tablet's own sweep — factored out of the loop body so the
/// per-tablet control flow is a plain function, easier to read and to step
/// through in a debugger than an inline loop body.
async fn ttl_sweep_one_tablet<E, H>(
    env: &E,
    host: &H,
    meta: &Metadata,
    tablet: TabletId,
    cursors: &mut BTreeMap<TabletId, Vec<u8>>,
) where
    E: Env,
    H: TtlScanHost,
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
        // Both `Ok(true)` (deleted) and `Ok(false)` (condition failed — the
        // item's TTL was refreshed/cleared since this scan observed it,
        // routine and expected) need no further action here.
        if let Err(err) = host
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
            tracing::debug!(
                tablet = tablet.0,
                table = %table,
                error = %err,
                "ttl reaper: delete of an expired item failed"
            );
        }
    }
}
