//! The secondary-index **backfill-completion aggregator** (ADR 0045 §4),
//! moved here by ADR 0061 rung C2 — see `crate::host::ControlLeaderHost`
//! for the one capability this loop needs and why a whole trait wasn't
//! necessary for just `metadata()`/`propose()` (both live on
//! [`animus_control::RaftNode`] directly, already `E`-generic).
//!
//! A distinct, control-plane-**leader**-only background loop — not an arm
//! of the per-tablet change-consumer loop — that watches
//! `Metadata::index_backfill` (the per-tablet "I finished seeding this
//! index" catalog a per-tablet backfill seeder populates) and flips a
//! table's index from `Creating` to `Active` once every one of the table's
//! *currently live* tablets has reported in.
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
//!
//! Touches only replicated `Metadata` (a live tablet-map read + a
//! schema-catalog read, then a `MetaCommand` proposal) — no data-plane I/O
//! of any kind — so it has **no** control-only-leader scope gap the way
//! some of this rung's other loops do: a pure control-only leader drives
//! the flip exactly like a combined-node leader does.

use animus_control::{IndexStatus, MetaCommand};
use animus_env::Env;

use crate::host::ControlLeaderHost;

/// How often this loop wakes to re-derive its whole decision from a fresh
/// `Metadata` snapshot: cheap per-tick work, and this codebase's own
/// testing discipline needs a fast tick so a converged-or-timeout test
/// doesn't itself become the slow part of the corpus.
pub const INDEX_BACKFILL_LOOP_INTERVAL_MS: u64 = 200;

/// The control-plane-leader-only background loop (ADR 0045 §4) — see the
/// module doc for who spawns this (via `animusd`'s own wrapper), why it
/// self-gates every tick rather than being spawned only on whichever node
/// happens to lead right now, and why it has no control-only-leader scope
/// gap. `interval_ms` is the production `INDEX_BACKFILL_LOOP_INTERVAL_MS`
/// at every real call site, and a small value in tests.
pub async fn index_backfill_loop<E, H>(env: E, host: H, interval_ms: u64)
where
    E: Env,
    H: ControlLeaderHost<E>,
{
    let interval = std::time::Duration::from_millis(interval_ms);
    loop {
        env.sleep(interval).await;
        let Some(leader) = host.control_leader() else {
            continue;
        };
        index_backfill_tick(&leader);
    }
}

/// One tick's whole decision — see the module doc's "The decision" section.
/// Pure I/O-wise beyond the one `propose` call (a local Raft log append, not
/// a network round trip), so this is a plain sync function.
fn index_backfill_tick<E: Env>(leader: &animus_control::RaftNode<E>) {
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

#[cfg(test)]
mod tests {
    use animus_control::schema::{IndexKind, IndexProjection};
    use animus_control::{ApplyOutcome, ColumnType, IndexDef, Metadata, TableSchema};
    use animus_env::nid;
    use animus_tablet::{KeyRange, TabletId};

    use super::*;

    /// A fixture mirroring `animus-control`'s own `table_with_index_and_
    /// tablet` (`meta.rs`'s `MarkIndexBackfilled` test suite): one table,
    /// one `Creating` GSI, one tablet scoped to it.
    fn table_with_index_and_tablet(table: &str, index: &str, tablet: TabletId) -> Metadata {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: table.to_owned(),
                schema: TableSchema::simple("id", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTableIndex {
                table: table.to_owned(),
                index: IndexDef {
                    name: index.to_owned(),
                    kind: IndexKind::Global,
                    hash_attribute: "email".to_owned(),
                    sort_attribute: None,
                    projection: IndexProjection::All,
                    status: IndexStatus::Creating,
                    hash_attribute_type: None,
                    sort_attribute_type: None,
                },
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet,
                table: Some(table.to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            }),
            ApplyOutcome::Applied
        );
        m
    }

    /// A table with no tablets at all never reports "every tablet has
    /// reported" — the vacuous-true trap `index_backfill_tick`'s own
    /// `has_any_tablet` guard exists to close.
    #[test]
    fn zero_tablets_never_flips() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("id", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTableIndex {
                table: "orders".to_owned(),
                index: IndexDef {
                    name: "gsi1".to_owned(),
                    kind: IndexKind::Global,
                    hash_attribute: "email".to_owned(),
                    sort_attribute: None,
                    projection: IndexProjection::All,
                    status: IndexStatus::Creating,
                    hash_attribute_type: None,
                    sort_attribute_type: None,
                },
            }),
            ApplyOutcome::Applied
        );
        let mut has_any_tablet = false;
        let all_reported = m.tablets_for_table("orders").all(|(&tablet, _)| {
            has_any_tablet = true;
            m.index_backfill.contains_key(&(tablet, "gsi1".to_owned()))
        });
        assert!(!has_any_tablet);
        assert!(
            all_reported,
            "vacuous true over an empty set — the tick's own guard must reject this, not this predicate alone"
        );
    }

    /// A straggler tablet blocks "every tablet reported" until it reports —
    /// the same fresh-read-every-tick property `index_backfill_tick`
    /// upholds by never caching `tablets_for_table`.
    #[test]
    fn a_straggler_tablet_blocks_until_it_reports() {
        let mut m = table_with_index_and_tablet("orders", "gsi1", TabletId(1));
        let reported = |m: &Metadata| {
            m.tablets_for_table("orders")
                .all(|(&tablet, _)| m.index_backfill.contains_key(&(tablet, "gsi1".to_owned())))
        };
        assert!(!reported(&m), "the one tablet hasn't reported yet");

        assert_eq!(
            m.apply(&MetaCommand::MarkIndexBackfilled {
                table: "orders".to_owned(),
                index: "gsi1".to_owned(),
                tablet: TabletId(1),
            }),
            ApplyOutcome::Applied
        );
        assert!(reported(&m), "the only tablet has now reported");
    }
}
