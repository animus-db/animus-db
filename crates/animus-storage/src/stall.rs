//! ADR 0081: the LSM **write stall** policy and the node-shared stall ledger.
//!
//! Everything in this module is node-local, in-memory timing (ADR 0073 class
//! L): no persisted form, no wire form. [`StallPolicy`] is a pure function from
//! an engine's state (L0 table count, memtable bytes) to a [`StallDecision`];
//! the engine applies it *before* the WAL append, outside every lock, and a
//! decision can only make the writer wait or sleep, never fail it (decision 2).
//!
//! [`StallLedger`] is the optional node-wide aggregate (engines slowed /
//! stopped, highest L0 count) the engines of one node share so the node's
//! `/admin/health` can show it; it carries no policy.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Metric, MetricsHandle};

/// Default L0 slowdown level, as a multiple of `compaction_trigger` (`3T`).
pub const DEFAULT_L0_SLOWDOWN_FACTOR: usize = 3;
/// Default L0 stop level, as a multiple of `compaction_trigger` (`5T`).
pub const DEFAULT_L0_STOP_FACTOR: usize = 5;
/// Write rate at the slowdown level (bytes/s); the ramp starts here.
pub const DEFAULT_L0_SLOWDOWN_WRITE_RATE: u64 = 16 << 20;
/// Write rate at the last level below `stop` (bytes/s); the ramp ends here.
pub const DEFAULT_L0_MIN_WRITE_RATE: u64 = 256 << 10;
/// Consecutive non-ENOSPC maintenance failures that turn a stall into a
/// fail-stop `Err(Backend)` (decision 2 rule 3). Counted in failures, never
/// in seconds.
pub const DEFAULT_MAINTENANCE_FAILURE_LIMIT: u32 = 8;

/// Why a write is being held back.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum StallCause {
    /// The memtable is over its hard cap (4x the flush threshold).
    Memtable,
    /// L0 holds at least `slowdown` tables: writes are delayed, not stopped.
    L0Slowdown,
    /// L0 holds at least `stop` tables: writes wait for it to fall.
    L0Stop,
}

impl StallCause {
    /// A stable lower-case name (metrics, admin).
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            StallCause::Memtable => "memtable",
            StallCause::L0Slowdown => "l0_slowdown",
            StallCause::L0Stop => "l0_stop",
        }
    }
}

/// The state of an engine's stall, coarsest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum StallLevel {
    /// Writes proceed.
    None,
    /// Writes are delayed per byte (`L0Slowdown`).
    Slowed,
    /// Writes wait (`L0Stop` or an over-cap memtable).
    Stopped,
}

/// What the stall decides for one write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StallDecision {
    /// Proceed now.
    Go,
    /// Sleep `delay` once, then proceed (the linear slowdown ramp).
    Delay {
        /// The cause (always [`StallCause::L0Slowdown`]).
        cause: StallCause,
        /// How long to sleep.
        delay: Duration,
    },
    /// Wait and re-evaluate: the writer may not proceed until the state clears.
    Wait {
        /// The cause.
        cause: StallCause,
    },
}

impl StallDecision {
    /// The level this decision puts the engine at.
    #[must_use]
    pub const fn level(self) -> StallLevel {
        match self {
            StallDecision::Go => StallLevel::None,
            StallDecision::Delay { .. } => StallLevel::Slowed,
            StallDecision::Wait { .. } => StallLevel::Stopped,
        }
    }
}

/// The pure stall policy of one engine, resolved from its `LsmOptions`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StallPolicy {
    /// L0 table count at which writes are delayed (`0` = the L0 stall is off).
    pub slowdown_tables: usize,
    /// L0 table count at which writes wait (`0` = the L0 stall is off).
    pub stop_tables: usize,
    /// Memtable bytes above which writes wait.
    pub memtable_cap: usize,
    /// Write rate (bytes/s) at `slowdown_tables`.
    pub slowdown_rate: u64,
    /// Write rate (bytes/s) at `stop_tables - 1`.
    pub min_rate: u64,
}

impl StallPolicy {
    /// Decide for a write of `write_bytes` given the engine's current state.
    /// Pure and monotone in `l0_tables` and `memtable_bytes`.
    #[must_use]
    pub fn decide(
        &self,
        l0_tables: usize,
        memtable_bytes: usize,
        write_bytes: u64,
    ) -> StallDecision {
        if memtable_bytes > self.memtable_cap {
            return StallDecision::Wait {
                cause: StallCause::Memtable,
            };
        }
        if self.stop_tables == 0 {
            return StallDecision::Go;
        }
        if l0_tables >= self.stop_tables {
            return StallDecision::Wait {
                cause: StallCause::L0Stop,
            };
        }
        if l0_tables >= self.slowdown_tables {
            let rate = self.rate_at(l0_tables).max(1);
            let nanos = u128::from(write_bytes) * 1_000_000_000 / u128::from(rate);
            return StallDecision::Delay {
                cause: StallCause::L0Slowdown,
                delay: Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX)),
            };
        }
        StallDecision::Go
    }

    /// The write rate (bytes/s) at `l0_tables`, falling linearly from
    /// `slowdown_rate` at `slowdown_tables` to `min_rate` at `stop_tables - 1`.
    /// Outside `[slowdown, stop)` it clamps to the nearest end.
    #[must_use]
    pub fn rate_at(&self, l0_tables: usize) -> u64 {
        if self.stop_tables == 0 || l0_tables <= self.slowdown_tables {
            return self.slowdown_rate;
        }
        let last = self.stop_tables.saturating_sub(1);
        if l0_tables >= last || last <= self.slowdown_tables {
            return self.min_rate;
        }
        let span = (last - self.slowdown_tables) as u128;
        let into = (l0_tables - self.slowdown_tables) as u128;
        let drop = u128::from(self.slowdown_rate.saturating_sub(self.min_rate)) * into / span;
        self.slowdown_rate - u64::try_from(drop).unwrap_or(u64::MAX)
    }
}

/// One engine's registration with the ledger.
struct EngineSlot {
    level: StallLevel,
    l0: usize,
    streak: u32,
}

struct LedgerState {
    next_id: u64,
    engines: BTreeMap<u64, EngineSlot>,
}

/// The node-wide aggregate of every engine's stall level and L0 count, behind
/// the `storage_engines_stalled_*` and `storage_l0_tables_*` gauges. One per
/// node, shared by `Arc` with each engine ([`crate::LsmEngine::set_stall_ledger`]).
/// Purely observational: it never changes what an engine does.
#[derive(Clone)]
pub struct StallLedger {
    inner: Arc<LedgerInner>,
}

struct LedgerInner {
    state: Mutex<LedgerState>,
    metrics: MetricsHandle,
}

/// A point-in-time copy of the ledger (admin JSON, tests).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StallSnapshot {
    /// Engines whose writes are being delayed now.
    pub engines_slowed: usize,
    /// Engines whose writes are waiting now.
    pub engines_stopped: usize,
    /// The highest L0 table count of any registered engine now.
    pub l0_max: usize,
    /// The longest current run of consecutive maintenance failures of any engine.
    pub failure_streak: u32,
}

impl StallLedger {
    /// A ledger publishing to `metrics`.
    #[must_use]
    pub fn new(metrics: MetricsHandle) -> Self {
        Self {
            inner: Arc::new(LedgerInner {
                state: Mutex::new(LedgerState {
                    next_id: 0,
                    engines: BTreeMap::new(),
                }),
                metrics,
            }),
        }
    }

    /// Register an engine; the returned id is its handle for [`update`](Self::update).
    #[must_use]
    pub fn register(&self) -> u64 {
        let mut st = self.inner.state.lock().expect("stall ledger poisoned");
        let id = st.next_id;
        st.next_id += 1;
        st.engines.insert(
            id,
            EngineSlot {
                level: StallLevel::None,
                l0: 0,
                streak: 0,
            },
        );
        id
    }

    /// Remove an engine (it was dropped); its contribution leaves the gauges.
    pub fn unregister(&self, id: u64) {
        let mut st = self.inner.state.lock().expect("stall ledger poisoned");
        st.engines.remove(&id);
        self.publish(&st);
    }

    /// Record an engine's current level and L0 count.
    pub fn update(&self, id: u64, level: StallLevel, l0: usize, streak: u32) {
        let mut st = self.inner.state.lock().expect("stall ledger poisoned");
        if let Some(slot) = st.engines.get_mut(&id) {
            slot.level = level;
            slot.l0 = l0;
            slot.streak = streak;
        }
        self.publish(&st);
    }

    /// The current aggregate.
    #[must_use]
    pub fn snapshot(&self) -> StallSnapshot {
        let st = self.inner.state.lock().expect("stall ledger poisoned");
        Self::fold(&st)
    }

    fn fold(st: &LedgerState) -> StallSnapshot {
        let mut s = StallSnapshot::default();
        for slot in st.engines.values() {
            match slot.level {
                StallLevel::None => {}
                StallLevel::Slowed => s.engines_slowed += 1,
                StallLevel::Stopped => s.engines_stopped += 1,
            }
            s.l0_max = s.l0_max.max(slot.l0);
            s.failure_streak = s.failure_streak.max(slot.streak);
        }
        s
    }

    fn publish(&self, st: &LedgerState) {
        let s = Self::fold(st);
        let m = &self.inner.metrics;
        m.set(Metric::StorageEnginesSlowed, s.engines_slowed as u64);
        m.set(Metric::StorageEnginesStopped, s.engines_stopped as u64);
        m.set(Metric::StorageL0TablesMax, s.l0_max as u64);
        m.set_max(Metric::StorageL0TablesHwm, s.l0_max as u64);
        m.set(
            Metric::StorageMaintenanceFailureStreak,
            u64::from(s.failure_streak),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> StallPolicy {
        StallPolicy {
            slowdown_tables: 12,
            stop_tables: 20,
            memtable_cap: 256 * 1024,
            slowdown_rate: 16 << 20,
            min_rate: 256 << 10,
        }
    }

    #[test]
    fn go_below_slowdown_delay_in_band_wait_at_stop() {
        let p = policy();
        assert_eq!(p.decide(0, 0, 4096), StallDecision::Go);
        assert_eq!(p.decide(11, 0, 4096), StallDecision::Go);
        assert!(matches!(
            p.decide(12, 0, 4096),
            StallDecision::Delay {
                cause: StallCause::L0Slowdown,
                ..
            }
        ));
        assert!(matches!(p.decide(19, 0, 4096), StallDecision::Delay { .. }));
        assert_eq!(
            p.decide(20, 0, 4096),
            StallDecision::Wait {
                cause: StallCause::L0Stop
            }
        );
        assert_eq!(
            p.decide(500, 0, 4096),
            StallDecision::Wait {
                cause: StallCause::L0Stop
            }
        );
    }

    #[test]
    fn memtable_over_cap_waits_even_with_the_l0_stall_off() {
        let mut p = policy();
        p.slowdown_tables = 0;
        p.stop_tables = 0;
        assert_eq!(p.decide(1000, 256 * 1024, 1), StallDecision::Go);
        assert_eq!(
            p.decide(1000, 256 * 1024 + 1, 1),
            StallDecision::Wait {
                cause: StallCause::Memtable
            }
        );
    }

    #[test]
    fn the_ramp_is_linear_and_monotone_decreasing() {
        let p = policy();
        assert_eq!(p.rate_at(12), 16 << 20);
        assert_eq!(p.rate_at(19), 256 << 10);
        let mut prev = u64::MAX;
        for l0 in 12..20 {
            let r = p.rate_at(l0);
            assert!(r <= prev, "rate rose at l0={l0}");
            prev = r;
        }
        // Midpoint of a 7-step ramp: 12 + 3.5 steps; step 3 and 4 straddle it.
        let step = ((16u64 << 20) - (256 << 10)) / 7;
        assert_eq!(p.rate_at(13), (16 << 20) - step);
    }

    #[test]
    fn delay_is_bytes_over_rate_and_monotone_in_l0() {
        let p = policy();
        let d = |l0| match p.decide(l0, 0, 1 << 20) {
            StallDecision::Delay { delay, .. } => delay,
            other => panic!("{other:?}"),
        };
        // 1 MiB at 16 MiB/s is 62.5 ms.
        assert_eq!(d(12), Duration::from_micros(62_500));
        assert!(d(19) > d(15) && d(15) > d(12));
        // 1 MiB at 256 KiB/s is 4 s.
        assert_eq!(d(19), Duration::from_secs(4));
    }

    #[test]
    fn a_degenerate_single_level_band_uses_the_min_rate() {
        let p = StallPolicy {
            slowdown_tables: 9,
            stop_tables: 10,
            ..policy()
        };
        assert_eq!(p.rate_at(9), 16 << 20);
        assert!(matches!(p.decide(9, 0, 1), StallDecision::Delay { .. }));
    }

    #[test]
    fn decisions_are_deterministic_for_equal_inputs() {
        let p = policy();
        for l0 in 0..30 {
            assert_eq!(p.decide(l0, 7, 999), p.decide(l0, 7, 999));
        }
    }

    #[test]
    fn ledger_folds_levels_and_l0_max() {
        let l = StallLedger::new(MetricsHandle::recording());
        let a = l.register();
        let b = l.register();
        l.update(a, StallLevel::Slowed, 13, 0);
        l.update(b, StallLevel::Stopped, 21, 3);
        assert_eq!(
            l.snapshot(),
            StallSnapshot {
                engines_slowed: 1,
                engines_stopped: 1,
                l0_max: 21,
                failure_streak: 3
            }
        );
        l.update(b, StallLevel::None, 2, 0);
        l.unregister(a);
        assert_eq!(l.snapshot(), StallSnapshot::default().with_l0(2));
    }

    impl StallSnapshot {
        fn with_l0(mut self, l0: usize) -> Self {
            self.l0_max = l0;
            self
        }
    }
}
