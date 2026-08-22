//! The DynamoDB-style **TTL reaper** (ADR 0051): a per-node background loop
//! that deletes items whose declared TTL attribute has passed, on every
//! tablet this node currently **leads** of a TTL-enabled table.
//!
//! ## Why this exists, and why it is small
//!
//! ADR 0051 §3 keeps every read path AWS-faithful: an expired item stays
//! visible until something actually deletes it. This loop is that
//! something. It is deliberately the *only* new code this feature needed
//! at the write path — see §4: a delete here rides the identical
//! [`crate::dynamo::kind_write_item_at_leader`] primitive `DeleteItem`
//! itself uses (`KindWriteOp::Delete`), so GSI rows, LSI rows, the
//! change-log record, and the stream image a consumer reads all fall out
//! of the ADR 0049 universal kind-write path for free. This module owns
//! only the *scan* — deciding *which* items are expired and *when* to look
//! — never a second implementation of what a delete commits.
//!
//! ## Wall-clock time (ADR 0051 §1)
//!
//! `expires_at`/`is_expired` (`animus_dynamo::ttl`) compare a stored item's
//! declared expiry against **`ctx.env.wall_now().as_secs()`** —
//! [`animus_env::Clock::wall_now`], never [`animus_env::Clock::now`]. This
//! is the one place in `animusd` outside `wall_now`'s own doc that this
//! bears repeating: `now()` is monotonic-since-start and carries no
//! calendar meaning, so it cannot interpret a client-supplied absolute
//! epoch second. Under `SimEnv`, `wall_now()` is a pure function of virtual
//! time and the run's seed (`SIM_WALL_EPOCH_MS` + elapsed virtual time), so
//! a sweep's decisions replay exactly like anything else in this codebase.
//!
//! ## Quiescence: read without waking, wake only to delete (ADR 0051 §6)
//!
//! ADR 0048 established that a read must never wake a quiesced group. This
//! loop's own scan honors that by construction, not by convention: it reads
//! through [`crate::CpGroup`]'s `local_scan_kind_capped` (verified against
//! `animus-cp-data`'s source — a **pure local engine read**,
//! `self.storage.scan(..)` straight off the tablet's own `StorageEngine`,
//! with no Raft round, no message, and no call anywhere near
//! `RaftKvNode::wake`/`WakeSignal`). A tablet with nothing expired therefore
//! costs this node exactly one idle LSM read per sweep interval and leaves
//! quiescence completely undisturbed — the group's own idle-activity clock
//! (tracked entirely inside `RaftCore`, off Raft-level events) never even
//! observes the scan. Only once the scan actually finds an expired item does
//! this loop call [`crate::CpGroup`]'s `wake()` — idempotent and cheap on
//! every state (`animusd/CLAUDE.md`'s "wake-on-demand" entry) — immediately
//! before proposing the delete, so the group is genuinely awake for the one
//! operation (a Raft proposal) that structurally requires it. No discrepancy
//! from the ADR's stated contract was found while building this: reading
//! truly never wakes here.
//!
//! ## Bounding one tick's work
//!
//! A led tablet's own [`TTL_SCAN_BATCH`] rows are read per sweep, resuming
//! from a **driver-local** cursor (`BTreeMap<TabletId, Vec<u8>>`, the same
//! ownership discipline `index_drain::change_consumer_loop`'s
//! `first_hot_seen`/`marker_bytes_seen` memos use — this loop is the sole
//! writer/reader, so no lock). A durable cursor row would be needless
//! machinery here: an unfinished sweep simply resumes next tick, and a
//! crash/leader-change restart just re-scans from the beginning, which is
//! always safe (idempotent — `is_expired` is a pure function of the item
//! and the current wall clock) and merely a bounded delay in how fast a
//! very large table's tail gets its first pass. Hitting the cap without
//! reaching a tablet's own end is `tracing::debug!`-logged rather than
//! silently truncated. Every delete this loop proposes is itself bounded by
//! that same per-tick scan cap (at most one delete per scanned row), so no
//! separate delete cap is needed.
//!
//! ## The conditional delete (ADR 0051 §4)
//!
//! Every delete is conditional on the **exact** `AttributeValue` this
//! sweep observed for the TTL attribute
//! (`ConditionExpression::Compare(attribute, Comparator::Eq, observed_value)`), evaluated at
//! the leader under the same `rmw_lock` (and apply-time OCC seatbelt)
//! ordinary conditional writes get. If a client refreshes or removes the
//! item's TTL between this scan and the delete actually landing, the
//! condition fails and the item survives —
//! [`crate::dynamo::KindWriteOutcome::ConditionFailed`] is therefore a
//! routine, expected outcome here, not an error, and is neither logged as
//! one nor retried this tick (the item is no longer known-expired; the next
//! sweep re-evaluates it fresh).

use std::collections::BTreeMap;
use std::time::Duration;

use animus_cp_data::KIND_BASE;
use animus_dynamo::{Comparator, ConditionExpression, is_expired, wire};
use animus_env::Clock;
use animus_tablet::{TabletId, TabletState};

use crate::dynamo::{self, KindWriteOutcome};
use crate::{ClientCtx, KindWriteOp};

/// How often each node sweeps the tablet groups it leads for expired TTL
/// items.
///
/// Minutes, not `index_drain::INDEX_DRAIN_INTERVAL`'s 200ms: unlike GSI/
/// stream maintenance, which must keep a consumer-visible lag budget small,
/// TTL deletion carries no such promise anywhere — real DynamoDB itself
/// documents only "typically within 48 hours." Every led, TTL-enabled
/// tablet this loop visits costs one genuine local LSM scan per tick
/// regardless of whether anything has expired (the quiescence contract
/// above requires that scan to be unconditional, not gated behind some
/// cheaper pre-check), so a sub-minute interval would burn real, ongoing
/// CPU across every TTL-enabled table's every tablet for no externally
/// visible benefit. A minute keeps that footprint negligible while still
/// closing the "expired but not yet reaped" visibility window (ADR 0051
/// §3) two orders of magnitude faster than DynamoDB's own worst case.
pub(crate) const DEFAULT_TTL_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// How many base rows one tick scans **per led, TTL-enabled tablet** — bounds
/// one sweep's own LSM read (and, transitively, its worst-case propose
/// count: at most one delete per scanned row) so one huge table's tablet
/// cannot monopolize a tick. Matches the order of magnitude
/// `index_drain.rs`'s own `TRIM_BATCH`/`BACKFILL_SEED_BATCH` bounded-batch
/// discipline uses for the identical reason.
const TTL_SCAN_BATCH: usize = 500;

/// The **TTL reaper** background loop (ADR 0051 §4/§6) — see the module doc
/// for the full design. One instance per node, spawned unconditionally
/// wherever `index_drain::change_consumer_loop` is (both node shapes that
/// can host a CP group leader): self-gates every tick on `group.is_leader()`
/// per tablet, so a node leading nothing does one cheap `Metadata` read and
/// nothing else.
pub(crate) async fn ttl_reaper_loop(ctx: ClientCtx, interval: Duration) {
    // Driver-local resume cursor per led tablet — see the module doc's
    // "Bounding one tick's work" section for why this is memory-only rather
    // than a durable `KIND_CURSOR` row. `None`/absent means "start this
    // tablet's scan from its own beginning."
    let mut cursors: BTreeMap<TabletId, Vec<u8>> = BTreeMap::new();
    loop {
        tokio::time::sleep(interval).await;
        // One `Metadata` snapshot for the whole tick (the segment janitor's
        // documented convention, `segment_janitor.rs`'s own module doc):
        // every tablet this tick visits is judged against the identical
        // view of the catalog, even though every decision here is
        // idempotent and re-derived fresh next tick regardless.
        let meta = ctx.effective_metadata();
        cursors.retain(|t, _| meta.tablets.contains_key(t));
        for (tablet, group) in ctx.edge.hosted_groups() {
            if !group.is_leader() {
                continue;
            }
            let Some(tab) = meta.tablets.get(&tablet) else {
                continue; // stale view — gone by the time we got here
            };
            // A `Building` split child is unroutable and serves nothing yet
            // (ADR 0050 rung 5) — nothing meaningful to scan, and touching
            // it early is the same class of hazard every other consumer
            // loop avoids for the same reason (see `change_consumer_loop`'s
            // own gate). A `Splitting` *parent* is still fully served, so
            // it stays visited.
            if tab.state == TabletState::Building {
                continue;
            }
            let Some(table) = tab.table.clone() else {
                continue; // legacy whole-keyspace tablet, or a stale view
            };
            // A hidden GSI index table has no schema entry of its own (it
            // is bookkeeping-only — `animusd/CLAUDE.md`'s dashboard entry),
            // so `table_ttl` naturally reads `None` for one and this loop
            // skips it without any extra check.
            let Some(ttl) = meta.table_ttl(&table).cloned() else {
                cursors.remove(&tablet);
                continue;
            };
            let Some(schema) = meta.table_schema(&table) else {
                continue; // schema vanished between the two reads this tick
            };
            let partition_key = schema.partition_key.clone();
            let sort_key = schema.clustering_keys.first().cloned();

            // The one genuinely non-waking read this loop performs — see
            // the module doc's "Quiescence" section.
            let start = cursors.get(&tablet).cloned().unwrap_or_default();
            let rows = group
                .local_scan_kind_capped(KIND_BASE, &start, None, TTL_SCAN_BATCH)
                .await;
            if rows.is_empty() {
                // Either a genuinely empty tablet, or this sweep reached the
                // end of it — either way, wrap around to the beginning next
                // tick for full eventual coverage.
                cursors.remove(&tablet);
                continue;
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

            let now = ctx.env.wall_now().as_secs();
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
                // ADR 0051 §6: wake — and only now — because there is
                // genuinely a delete to propose.
                group.wake();
                let condition = ConditionExpression::Compare(
                    ttl.attribute_name.clone(),
                    Comparator::Eq,
                    ttl_value,
                );
                match dynamo::kind_write_item_at_leader(
                    &ctx,
                    &group,
                    &meta,
                    &table,
                    &pk,
                    sk.as_ref(),
                    KindWriteOp::Delete,
                    Some(&condition),
                    // ADR 0051 §7: this delete is the TTL reaper's own, so
                    // its change record carries the service `userIdentity`.
                    true,
                )
                .await
                {
                    Ok(KindWriteOutcome::Ok { .. }) => {}
                    // Normal, expected outcome (ADR 0051 §4) — the item's
                    // TTL was refreshed or cleared between this scan and
                    // the delete landing. Not an error.
                    Ok(KindWriteOutcome::ConditionFailed) => {}
                    Err(e) => {
                        tracing::debug!(
                            tablet = tablet.0,
                            table = %table,
                            error = %e.message,
                            "ttl reaper: delete of an expired item failed"
                        );
                    }
                }
            }
        }
    }
}
