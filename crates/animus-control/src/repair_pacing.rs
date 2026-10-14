//! Repair-proposal pacing for the control leader (ADR 0079 section 3, step 6).
//!
//! `Metadata::reconcile` returns every violating tablet's repair
//! (`CasTabletReplicas`); proposing them all in one tick starts a learner and a
//! snapshot stream per tablet on the surviving nodes with no cap. The
//! [`RepairPacer`] sits between that pure decision and the propose loop:
//!
//! * at most `R` ([`RepairPacing::per_tick`]) **new** repairs are proposed per
//!   tick, in ascending `TabletId` order (the order `reconcile` returns them
//!   in, re-sorted here so the rule does not lean on it);
//! * a new repair is not started while `R_inflight`
//!   ([`RepairPacing::inflight_max`]) repairs are in flight.
//!
//! **In flight** is leader-local and time-bounded. A repair becomes in flight
//! the moment its `CasTabletReplicas` is accepted by the leader's core, and
//! stops being in flight at the first of: (1) [`REPAIR_INFLIGHT_TTL`] elapsed
//! (the control plane has no replicated "learner caught up" signal: the
//! learner phase is data-plane-local, so completion is not observable here and
//! a time bound is the only one that cannot be wedged by a repair that never
//! completes); (2) the tablet leaves the tablet map; (3) the leader loses
//! leadership (the whole tracker is dropped). A proposal whose CAS has not yet
//! applied (the view's epoch still equals the tracked `expected_epoch`) is a
//! *retry* of the same repair, not a new one: it passes through free, which
//! preserves the pre-pacing behaviour of re-proposing every tick until the
//! entry applies.
//!
//! Class L (ADR 0073): leader-local computation over replicated state; every
//! proposal is an existing command and no replica applies anything new.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use animus_env::Nanos;
use animus_tablet::{Epoch, Tablet, TabletId};

use crate::meta::MetaCommand;

/// Default `R`: new repairs proposed per reconcile tick (ADR 0079 Q4: a fixed
/// cadence, not adaptive).
pub const DEFAULT_REPAIR_PROPOSALS_PER_TICK: usize = 8;
/// Default `R_inflight`: repairs in flight before a new one is deferred.
pub const DEFAULT_REPAIR_INFLIGHT_MAX: usize = 16;
/// How long an accepted repair holds an in-flight slot if nothing ends it
/// sooner. Derived from the existing [`crate::node::REPAIR_DWELL`], the
/// control plane's own "long enough to treat a member as gone" bound.
pub const REPAIR_INFLIGHT_TTL: Duration = crate::node::REPAIR_DWELL;

/// The two pacing knobs. `0` turns the matching limit off (today's behaviour).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RepairPacing {
    /// `R`: new repairs proposed per tick; `0` = unlimited.
    pub per_tick: usize,
    /// `R_inflight`: repairs in flight at once; `0` = unlimited.
    pub inflight_max: usize,
}

impl Default for RepairPacing {
    fn default() -> Self {
        Self {
            per_tick: DEFAULT_REPAIR_PROPOSALS_PER_TICK,
            inflight_max: DEFAULT_REPAIR_INFLIGHT_MAX,
        }
    }
}

impl RepairPacing {
    /// Pacing fully off (the pre-ADR-0079 behaviour).
    pub const OFF: Self = Self {
        per_tick: 0,
        inflight_max: 0,
    };
}

/// A live, settable [`RepairPacing`] shared between a [`crate::RaftNode`] and
/// its reconcile loop, so the embedder can set it after `start`.
#[derive(Clone, Debug)]
pub struct RepairPacingHandle {
    inner: Arc<(AtomicUsize, AtomicUsize)>,
}

impl Default for RepairPacingHandle {
    fn default() -> Self {
        Self::new(RepairPacing::default())
    }
}

impl RepairPacingHandle {
    /// A handle holding `p`.
    #[must_use]
    pub fn new(p: RepairPacing) -> Self {
        Self {
            inner: Arc::new((
                AtomicUsize::new(p.per_tick),
                AtomicUsize::new(p.inflight_max),
            )),
        }
    }

    /// Replace the pacing.
    pub fn set(&self, p: RepairPacing) {
        self.inner.0.store(p.per_tick, Ordering::Relaxed);
        self.inner.1.store(p.inflight_max, Ordering::Relaxed);
    }

    /// The current pacing.
    #[must_use]
    pub fn get(&self) -> RepairPacing {
        RepairPacing {
            per_tick: self.inner.0.load(Ordering::Relaxed),
            inflight_max: self.inner.1.load(Ordering::Relaxed),
        }
    }
}

/// One tick's pacing outcome.
#[derive(Debug, Default)]
pub struct Paced {
    /// The proposals to make now (retries first, then new repairs ascending).
    pub propose: Vec<MetaCommand>,
    /// How many were deferred to a later tick.
    pub deferred: usize,
}

/// The leader's in-flight tracker and selector. Volatile, per-node,
/// `env.now()`-keyed, dropped on losing leadership (like the other reconcile
/// loop trackers).
#[derive(Debug, Default)]
pub struct RepairPacer {
    /// tablet -> (the `expected_epoch` the repair was proposed at, accepted at).
    inflight: BTreeMap<TabletId, (Epoch, Nanos)>,
}

impl RepairPacer {
    /// An empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Repairs in flight now.
    #[must_use]
    pub fn inflight(&self) -> usize {
        self.inflight.len()
    }

    /// Forget everything (leadership lost).
    pub fn clear(&mut self) {
        self.inflight.clear();
    }

    /// Drop entries that timed out or whose tablet is gone.
    pub fn expire(&mut self, now: Nanos, tablets: &BTreeMap<TabletId, Tablet>) {
        self.inflight.retain(|t, (_, since)| {
            tablets.contains_key(t) && now.duration_since(*since) < REPAIR_INFLIGHT_TTL
        });
    }

    /// Choose which of `proposals` (the pure `reconcile` output) to propose
    /// this tick. Call [`expire`](Self::expire) first and
    /// [`accepted`](Self::accepted) for every returned command the core
    /// accepted.
    pub fn select(
        &self,
        pacing: RepairPacing,
        tablets: &BTreeMap<TabletId, Tablet>,
        proposals: Vec<MetaCommand>,
    ) -> Paced {
        let mut retries = Vec::new();
        let mut fresh: Vec<(TabletId, MetaCommand)> = Vec::new();
        for cmd in proposals {
            let MetaCommand::CasTabletReplicas {
                tablet,
                expected_epoch,
                ..
            } = &cmd
            else {
                // Not a repair CAS: never paced.
                retries.push(cmd);
                continue;
            };
            let unapplied_retry = self.inflight.get(tablet).is_some_and(|(e, _)| {
                *e == *expected_epoch && tablets.get(tablet).is_some_and(|t| t.epoch == *e)
            });
            if unapplied_retry {
                retries.push(cmd);
            } else {
                fresh.push((*tablet, cmd));
            }
        }
        fresh.sort_by_key(|(t, _)| *t);
        let mut out = Paced {
            propose: retries,
            deferred: 0,
        };
        let mut started = 0usize;
        let mut slots = self.inflight.len();
        for (tablet, cmd) in fresh {
            let adds = !self.inflight.contains_key(&tablet);
            let over_tick = pacing.per_tick > 0 && started >= pacing.per_tick;
            let over_inflight = pacing.inflight_max > 0 && adds && slots >= pacing.inflight_max;
            if over_tick || over_inflight {
                out.deferred += 1;
                continue;
            }
            started += 1;
            if adds {
                slots += 1;
            }
            out.propose.push(cmd);
        }
        out
    }

    /// Record that the core accepted `cmd` at `now`. A retry of an already
    /// tracked unapplied repair keeps its original start time (so a repair
    /// that never applies still ages out).
    pub fn accepted(&mut self, now: Nanos, cmd: &MetaCommand) {
        if let MetaCommand::CasTabletReplicas {
            tablet,
            expected_epoch,
            ..
        } = cmd
        {
            match self.inflight.get(tablet) {
                Some((e, _)) if *e == *expected_epoch => {}
                _ => {
                    self.inflight.insert(*tablet, (*expected_epoch, now));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cas(t: u64, epoch: u64) -> MetaCommand {
        MetaCommand::CasTabletReplicas {
            tablet: TabletId(t),
            expected_epoch: Epoch(epoch),
            replicas: vec![],
        }
    }

    fn tablets(ids: &[u64]) -> BTreeMap<TabletId, Tablet> {
        ids.iter()
            .map(|&i| {
                (
                    TabletId(i),
                    Tablet::new(
                        TabletId(i),
                        animus_tablet::KeyRange {
                            start: vec![],
                            end: None,
                        },
                        vec![],
                    ),
                )
            })
            .collect()
    }

    fn ids(p: &Paced) -> Vec<u64> {
        p.propose
            .iter()
            .filter_map(|c| match c {
                MetaCommand::CasTabletReplicas { tablet, .. } => Some(tablet.0),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn at_most_r_per_tick_ascending_and_all_eventually() {
        let ts = tablets(&(1..=20).collect::<Vec<_>>());
        let mut pacer = RepairPacer::new();
        let pacing = RepairPacing {
            per_tick: 8,
            inflight_max: 0,
        };
        // Shuffled input; applied tablets drop out of the violating set.
        let mut pending: Vec<u64> = (1..=20).rev().collect();
        let mut seen = Vec::new();
        let mut ticks = 0;
        while !pending.is_empty() {
            ticks += 1;
            let props: Vec<_> = pending.iter().map(|&t| cas(t, 1)).collect();
            let p = pacer.select(pacing, &ts, props);
            assert!(ids(&p).len() <= 8);
            assert_eq!(p.deferred, pending.len() - ids(&p).len());
            let mut sorted = ids(&p);
            sorted.sort_unstable();
            assert_eq!(ids(&p), sorted, "ascending TabletId");
            for c in &p.propose {
                pacer.accepted(Nanos(0), c);
            }
            for t in ids(&p) {
                pending.retain(|&x| x != t);
                seen.push(t);
            }
        }
        assert_eq!(ticks, 3, "ceil(20/8)");
        assert_eq!(seen.len(), 20);
    }

    #[test]
    fn inflight_cap_defers_until_ttl_expiry() {
        let ts = tablets(&(1..=10).collect::<Vec<_>>());
        let mut pacer = RepairPacer::new();
        let pacing = RepairPacing {
            per_tick: 8,
            inflight_max: 4,
        };
        let props = |from: u64| (from..=10).map(|t| cas(t, 1)).collect::<Vec<_>>();
        let p = pacer.select(pacing, &ts, props(1));
        assert_eq!(ids(&p), vec![1, 2, 3, 4]);
        for c in &p.propose {
            pacer.accepted(Nanos(0), c);
        }
        // Applied (epoch moved on) but still in flight: a new tick starts none.
        let mut applied = ts.clone();
        for t in 1..=4 {
            applied.get_mut(&TabletId(t)).unwrap().epoch = Epoch(2);
        }
        let p = pacer.select(pacing, &applied, props(5));
        assert!(p.propose.is_empty());
        assert_eq!(p.deferred, 6);
        // A repair that never completes does not hold its slot forever.
        let late = Nanos(0).saturating_add(REPAIR_INFLIGHT_TTL);
        pacer.expire(late, &applied);
        assert_eq!(pacer.inflight(), 0);
        let p = pacer.select(pacing, &applied, props(5));
        assert_eq!(ids(&p), vec![5, 6, 7, 8]);
    }

    #[test]
    fn unapplied_retry_is_free_and_keeps_its_age() {
        let ts = tablets(&[1, 2]);
        let mut pacer = RepairPacer::new();
        let pacing = RepairPacing {
            per_tick: 1,
            inflight_max: 1,
        };
        let p = pacer.select(pacing, &ts, vec![cas(1, 1), cas(2, 1)]);
        assert_eq!(ids(&p), vec![1]);
        pacer.accepted(Nanos(0), &p.propose[0]);
        // Tablet 1's CAS has not applied (epoch still 1): re-proposed, free.
        let p = pacer.select(pacing, &ts, vec![cas(1, 1), cas(2, 1)]);
        assert_eq!(ids(&p), vec![1]);
        assert_eq!(p.deferred, 1);
        pacer.accepted(Nanos(1), &p.propose[0]);
        pacer.expire(Nanos(0).saturating_add(REPAIR_INFLIGHT_TTL), &ts);
        assert_eq!(pacer.inflight(), 0, "retry did not extend the age");
    }

    #[test]
    fn off_is_unpaced() {
        let ts = tablets(&(1..=50).collect::<Vec<_>>());
        let pacer = RepairPacer::new();
        let p = pacer.select(
            RepairPacing::OFF,
            &ts,
            (1..=50).map(|t| cas(t, 1)).collect(),
        );
        assert_eq!(p.propose.len(), 50);
        assert_eq!(p.deferred, 0);
    }
}
