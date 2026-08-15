//! The secondary-index **backfill-completion aggregator** (ADR 0045 §4): a
//! distinct, control-plane-**leader**-only background loop — not an arm of
//! the per-tablet `index_drain::change_consumer_loop` — that watches
//! `Metadata::index_backfill` (the per-tablet "I finished seeding this
//! index" catalog the backfill seeder populates, a later PR) and flips a
//! table's index from `Creating` to `Active` once every one of the table's
//! *currently live* tablets has reported in.
//!
//! ## Who runs this
//!
//! Spawned unconditionally on every node shape that can ever become the
//! control-plane leader — `BoundNode::start_with_streams` (combined) and
//! `BoundControlNode::start_control_with` (control-only) — the same "run
//! everywhere, self-gate on `ctx.edge.leader_handle()`" pattern
//! `segment_janitor_loop`/`detect_loop`/`orphan_sweep_loop` already use.
//! **Never** spawned on a data-only node (`BoundDataNode`), which never
//! registers a local control `RaftNode` into its own `ClusterEdgeState` at
//! all, so `leader_handle()` there is permanently `None`.
//!
//! Unlike the segment janitor, this loop touches **only** replicated
//! `Metadata` (a live tablet-map read + a schema-catalog read, then a
//! `MetaCommand` proposal) — no `SegmentStoreHandle`, no data role of any
//! kind — so it has **no** control-only-leader scope gap the way that
//! loop's phases 2/3 do: a control-only leader drives the flip exactly like
//! a combined-node leader does. Regression:
//! `tests/index_backfill.rs::control_only_leader_drives_the_flip`.
//!
//! ## The decision, one snapshot per tick
//!
//! Each tick, for every table with at least one index currently `Creating`:
//! if the table currently has **at least one** tablet, and **every** tablet
//! currently in `Metadata::tablets_for_table(table)` (a fresh read this same
//! tick, never a cached/stale set — a tablet that appears after some others
//! have already reported must still block the flip until it reports too)
//! has a matching row in `Metadata::index_backfill`, propose
//! `MetaCommand::SetIndexStatus { status: Active, .. }`. A table with **zero**
//! tablets never flips (a vacuously-true "every tablet reported" over an
//! empty set would be a false positive here, not a real completion) — it
//! simply waits for the table's first tablet to exist, the same way a
//! genuinely incomplete backfill waits for its last straggler.

use std::time::Duration;

use animus_control::RaftNode;
use animus_env::ProdEnv;

use crate::ClientCtx;
use crate::{IndexStatus, MetaCommand};

/// How often this loop wakes to re-derive its whole decision from a fresh
/// `Metadata` snapshot — matches `segment_janitor.rs`'s own
/// `SEGMENT_JANITOR_INTERVAL` cadence: cheap per-tick work, and this
/// codebase's own testing discipline needs a fast tick so a
/// converged-or-timeout test doesn't itself become the slow part of the
/// corpus.
const INDEX_BACKFILL_LOOP_INTERVAL: Duration = Duration::from_millis(200);

/// The control-plane-leader-only background loop (ADR 0045 §4) — see the
/// module doc for who spawns this, why it self-gates every tick rather than
/// being spawned only on whichever node happens to lead right now, and why
/// (unlike the segment janitor) it has no control-only-leader scope gap.
pub(crate) async fn index_backfill_loop(ctx: ClientCtx) {
    loop {
        tokio::time::sleep(INDEX_BACKFILL_LOOP_INTERVAL).await;
        let Some(leader) = ctx.edge.leader_handle() else {
            continue;
        };
        index_backfill_tick(&leader);
    }
}

/// One tick's whole decision — see the module doc's "The decision" section.
/// Pure I/O-wise beyond the one `propose` call (a local Raft log append, not
/// a network round trip), so this is a plain sync function, unlike the
/// segment janitor's tick (which does real segment-store I/O).
fn index_backfill_tick(leader: &RaftNode<ProdEnv>) {
    let meta = leader.metadata();
    for (table, schema) in meta.schemas.iter() {
        for index in &schema.indexes {
            if index.status != IndexStatus::Creating {
                continue;
            }
            let mut has_any_tablet = false;
            let all_reported = meta.tablets_for_table(table).all(|(&tablet, _)| {
                has_any_tablet = true;
                meta.index_backfill
                    .contains_key(&(tablet, index.name.clone()))
            });
            if !has_any_tablet || !all_reported {
                continue;
            }
            let _ = leader.propose(MetaCommand::SetIndexStatus {
                table: table.clone(),
                index: index.name.clone(),
                status: IndexStatus::Active,
            });
        }
    }
}
