//! The repair-settled report loop (ADR 0079 step 6, `Gate::NodeCapacity`).
//!
//! The control leader's repair pacer frees a repair's in-flight slot when the
//! repair is observably complete. Completion is data-plane-local (a learner is
//! added, caught up and promoted inside the tablet's own Raft group), so the
//! tablet's leader reports it: once its live group's voters equal `Metadata`'s
//! replica set for the tablet's current epoch and no learner remains, it
//! proposes `MetaCommand::RepairSettled { tablet, epoch }` (relayed to the
//! control leader; on the `is_relayable_command` allowlist). The command is
//! epoch-CAS'd and idempotent, so a retried, duplicated or stale report is
//! harmless; a lost report only means the pacer's `REPAIR_INFLIGHT_TTL` frees
//! the slot instead.
//!
//! Only a tablet whose epoch has moved past `INITIAL` (a placement change has
//! happened) is reported, at most [`REPAIR_SETTLED_PER_TICK`] per tick, and
//! never while the gate is closed (older voters cannot decode the variant).
//! Reads are the cached local ones; the log is touched only to report.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use animus_env::Nanos;
use animus_tablet::{Epoch, TabletId};

use crate::{ClientCtx, MetaCommand, RelayClient};
use animus_env::Env;

/// Tick cadence of the loop.
#[cfg(feature = "server")]
pub(crate) const REPAIR_SETTLED_INTERVAL: Duration = Duration::from_millis(200);
/// Reports proposed per tick (bounds the one-time burst after the gate opens).
pub(crate) const REPAIR_SETTLED_PER_TICK: usize = 16;
/// How long an unconfirmed report suppresses a re-propose of the same epoch.
const REPAIR_SETTLED_RETRY: Duration = Duration::from_secs(2);

/// Run forever on every node; self-gates per tablet on leadership.
#[cfg(feature = "server")]
pub(crate) async fn repair_settled_loop(ctx: ClientCtx) {
    let mut proposed: BTreeMap<TabletId, (Epoch, Nanos)> = BTreeMap::new();
    loop {
        tokio::time::sleep(REPAIR_SETTLED_INTERVAL).await;
        repair_settled_tick(&ctx, &mut proposed).await;
    }
}

/// One tick (callable directly from tests).
pub(crate) async fn repair_settled_tick<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    proposed: &mut BTreeMap<TabletId, (Epoch, Nanos)>,
) {
    let meta = ctx.effective_metadata();
    if !meta.capacity_gate_open() {
        proposed.clear();
        return;
    }
    let now = ctx.env.now();
    proposed.retain(|t, (e, at)| {
        meta.tablets
            .get(t)
            .is_some_and(|tab| tab.epoch == *e && tab.settled_epoch != Some(*e))
            && now.duration_since(*at) < REPAIR_SETTLED_RETRY
    });
    let mut sent = 0usize;
    for (tablet, group) in ctx.edge.hosted_groups() {
        if sent >= REPAIR_SETTLED_PER_TICK {
            break;
        }
        if !group.is_leader() {
            continue;
        }
        let Some(t) = meta.tablets.get(&tablet) else {
            continue;
        };
        if t.epoch == Epoch::INITIAL
            || t.settled_epoch == Some(t.epoch)
            || proposed.contains_key(&tablet)
        {
            continue;
        }
        let desired: BTreeSet<_> = t.replicas.iter().cloned().collect();
        if group.config() != desired || !group.learners().is_empty() {
            continue;
        }
        proposed.insert(tablet, (t.epoch, now));
        sent += 1;
        let _ = ctx
            .propose_schema(&MetaCommand::RepairSettled {
                tablet,
                epoch: t.epoch,
            })
            .await;
    }
}
