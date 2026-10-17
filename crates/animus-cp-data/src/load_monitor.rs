//! The per-node **load monitor** (ADR 0078 mechanism 2, lag-stretched election
//! timeouts): one instance per physical node turns *scheduling lateness* into
//! a number, and the number into (a) a stretch the driver adds to a follower's
//! election timeout and (b) a reduction of the campaign governor's `K`.
//!
//! One task per node (`env.spawn_task`, like the [`HeartbeatBatcher`]'s) loops
//! `env.sleep(PROBE)`; after each wake `sample = (now - slept_from) - PROBE`,
//! floored at zero. That is exactly what hurts a Raft timer: how late the
//! runtime runs a timer that was due. Under `SimEnv` it is zero unless a fault
//! (the CPU-starvation model) makes it otherwise, so an unloaded sim is
//! untouched.
//!
//! Everything numeric here is integer arithmetic on nanoseconds and pure, so
//! the properties (monotone, bounded, dead band) are unit-tested directly:
//!
//! - estimate: `lag = max(sample, lag - lag / 32)`, instant attack and slow
//!   decay (half-life about 450 ms at the 20 ms probe);
//! - dead band: a lag below [`DEAD_BAND`] reads as zero, so a healthy node
//!   (scheduling jitter of a few ms) is bit-for-bit the pre-0078 node;
//! - stretch: `S = min(4 * lag, S_max)`, `S_max = lag_stretch_max_x * base`;
//! - `K_eff`: `K/2` at lag >= 30 ms, `K/4` (minimum 1) at lag >= 100 ms.
//!
//! The monitor is advisory: `/admin/health` shows its readings, nothing routes
//! on them.
//!
//! [`HeartbeatBatcher`]: crate::heartbeat_batch::HeartbeatBatcher

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use animus_env::{Env, EnvExt, Metric, MetricsHandle};

use crate::awake_admission::AwakeAdmission;
use crate::campaign_governor::CampaignGovernor;

/// The probe period: how long the monitor task sleeps between samples.
pub const PROBE: Duration = Duration::from_millis(20);

/// A smoothed lag below this reads as zero.
pub const DEAD_BAND: Duration = Duration::from_millis(10);

/// The stretch is `STRETCH_C` probe-lags (ADR 0078: a follower's wait for a
/// heartbeat sums the leader's lateness to send and its own to observe).
pub const STRETCH_C: u64 = 4;

/// Default `lag_stretch_max_x`: `S_max` as a multiple of the election base.
pub const DEFAULT_LAG_STRETCH_MAX_X: u64 = 4;

/// Lag at which `K_eff` halves.
pub const K_HALVE_AT: Duration = Duration::from_millis(30);

/// Lag at which `K_eff` quarters.
pub const K_QUARTER_AT: Duration = Duration::from_millis(100);

/// One estimator step: `max(sample, lag - lag / 32)` (nanoseconds).
#[must_use]
pub fn next_estimate(lag: u64, sample: u64) -> u64 {
    sample.max(lag - lag / 32)
}

/// The dead-banded reading of an estimate: zero below [`DEAD_BAND`].
#[must_use]
pub fn reading(lag: u64) -> u64 {
    if u128::from(lag) < DEAD_BAND.as_nanos() {
        0
    } else {
        lag
    }
}

/// The stretch for an estimate: `min(STRETCH_C * reading(lag), max_x * base)`.
/// `max_x == 0` disables it.
#[must_use]
pub fn stretch_nanos(lag: u64, base: Duration, max_x: u64) -> u64 {
    stretch_nanos_with_gain(lag, base, max_x, STRETCH_C)
}

/// [`stretch_nanos`] with an explicit gain `c` (the calibration sweep's knob;
/// production always uses [`STRETCH_C`]).
#[must_use]
pub fn stretch_nanos_with_gain(lag: u64, base: Duration, max_x: u64, c: u64) -> u64 {
    let cap = u64::try_from(base.as_nanos())
        .unwrap_or(u64::MAX)
        .saturating_mul(max_x);
    reading(lag).saturating_mul(c).min(cap)
}

/// The shared **lag-to-factor** rule (ADR 0079 decision Q9): the divisor a
/// node-local capacity is reduced by at a given lag estimate. `1` below
/// [`K_HALVE_AT`], `2` from it, `4` from [`K_QUARTER_AT`] (dead-banded through
/// [`reading`]). One function, so every capacity that follows the node's
/// scheduling lag (the campaign governor's `K_eff`, the awake cap's `A_eff`,
/// later the background pools) reduces by the same thresholds.
#[must_use]
pub fn lag_divisor(lag: u64) -> usize {
    let r = u128::from(reading(lag));
    if r >= K_QUARTER_AT.as_nanos() {
        4
    } else if r >= K_HALVE_AT.as_nanos() {
        2
    } else {
        1
    }
}

/// `base` reduced by [`lag_divisor`]; never below 1.
#[must_use]
pub fn scale_by_lag(base: usize, lag: u64) -> usize {
    (base.max(1) / lag_divisor(lag)).max(1)
}

/// The effective campaign cap for an estimate: `K`, `K/2` at >= 30 ms, `K/4`
/// (minimum 1) at >= 100 ms; never below 1, never above `k`.
#[must_use]
pub fn k_eff(k: usize, lag: u64) -> usize {
    scale_by_lag(k, lag)
}

/// The effective awake cap for an estimate (ADR 0079 layer 2): `A` reduced by
/// the same rule as [`k_eff`]. `a` is the already-resolved cap (at least 1).
#[must_use]
pub fn a_eff(a: usize, lag: u64) -> usize {
    scale_by_lag(a, lag)
}

struct Inner {
    /// The raw smoothed estimate, nanoseconds (before the dead band).
    lag: AtomicU64,
    max_x: u64,
    /// The stretch gain `c` ([`STRETCH_C`] unless a calibration run overrides it).
    gain: u64,
    /// The node's default election base, for the published stretch metric.
    base: Duration,
    metrics: MetricsHandle,
    governor: Option<CampaignGovernor>,
    /// The node's awake-group admission (ADR 0079), whose `A_eff` follows the
    /// same lag through the shared rule; attached once after construction.
    awake: OnceLock<AwakeAdmission>,
    /// The node's background-work budget (ADR 0079 layer 3), whose pool sizes
    /// follow the same lag; attached once after construction.
    budget: OnceLock<crate::background_budget::BackgroundBudget>,
    /// The node's propose admission (ADR 0080), whose per-group backlog bounds
    /// follow the same lag; attached once after construction.
    admission: OnceLock<crate::admission::NodeAdmission>,
}

/// The per-node monitor handle. Cloning is an `Arc` bump; drivers hold a clone
/// and read [`stretch`](Self::stretch) once per loop iteration.
#[derive(Clone)]
pub struct LoadMonitor {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for LoadMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadMonitor")
            .field("lag", &self.lag())
            .field("max_x", &self.inner.max_x)
            .finish()
    }
}

impl LoadMonitor {
    /// Start the monitor: spawns its one probe task on `env`. `max_x` is
    /// `lag_stretch_max_x` (`animusd` does not construct a monitor for 0),
    /// `base` the node's default election base (for the published stretch
    /// metric), `governor` the node's governor whose `K_eff` follows the lag.
    pub fn new<E: Env>(
        env: &E,
        max_x: u64,
        base: Duration,
        metrics: MetricsHandle,
        governor: Option<CampaignGovernor>,
    ) -> Self {
        let monitor = Self::detached(max_x, base, metrics, governor);
        env.spawn_task(probe_loop(env.clone(), monitor.clone()));
        monitor
    }

    /// A monitor with no probe task; the caller feeds it with
    /// [`observe`](Self::observe). For unit tests.
    #[must_use]
    pub fn detached(
        max_x: u64,
        base: Duration,
        metrics: MetricsHandle,
        governor: Option<CampaignGovernor>,
    ) -> Self {
        Self::detached_with_gain(STRETCH_C, max_x, base, metrics, governor)
    }

    /// [`detached`](Self::detached) with an explicit stretch gain `c`, for the
    /// ADR 0078 step 6 calibration sweep only (production uses [`STRETCH_C`]).
    #[must_use]
    pub fn detached_with_gain(
        gain: u64,
        max_x: u64,
        base: Duration,
        metrics: MetricsHandle,
        governor: Option<CampaignGovernor>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                lag: AtomicU64::new(0),
                gain,
                max_x,
                base,
                metrics,
                governor,
                awake: OnceLock::new(),
                budget: OnceLock::new(),
                admission: OnceLock::new(),
            }),
        }
    }

    /// Attach the node's awake-group admission (ADR 0079): from now on its
    /// `A_eff` follows this monitor's lag. At most once; a second attach is
    /// ignored.
    pub fn attach_awake(&self, awake: AwakeAdmission) {
        let _ = self.inner.awake.set(awake);
    }

    /// Attach the node's background-work budget (ADR 0079 layer 3): from now
    /// on its pool sizes follow this monitor's lag. At most once.
    pub fn attach_budget(&self, budget: crate::background_budget::BackgroundBudget) {
        let _ = self.inner.budget.set(budget);
    }

    /// Attach the node's propose admission (ADR 0080): from now on its
    /// per-group bounds follow this monitor's lag. At most once.
    pub fn attach_admission(&self, admission: crate::admission::NodeAdmission) {
        let _ = self.inner.admission.set(admission);
    }

    /// Fold one probe sample (nanoseconds late) into the estimate and publish.
    pub fn observe(&self, sample_nanos: u64) {
        let lag = next_estimate(self.inner.lag.load(Ordering::Relaxed), sample_nanos);
        self.inner.lag.store(lag, Ordering::Relaxed);
        self.inner
            .metrics
            .set(Metric::CpElectionLagNanos, reading(lag));
        self.inner.metrics.set(
            Metric::CpElectionStretchNanos,
            stretch_nanos_with_gain(lag, self.inner.base, self.inner.max_x, self.inner.gain),
        );
        if let Some(g) = &self.inner.governor {
            g.set_lag(lag);
        }
        if let Some(a) = self.inner.awake.get() {
            a.set_lag(lag);
        }
        if let Some(b) = self.inner.budget.get() {
            b.set_lag(lag);
        }
        if let Some(a) = self.inner.admission.get() {
            a.set_lag(lag);
        }
    }

    /// The dead-banded smoothed lag.
    #[must_use]
    pub fn lag(&self) -> Duration {
        Duration::from_nanos(reading(self.inner.lag.load(Ordering::Relaxed)))
    }

    /// The stretch to install on a group whose election base is `base`.
    #[must_use]
    pub fn stretch(&self, base: Duration) -> Duration {
        Duration::from_nanos(stretch_nanos_with_gain(
            self.inner.lag.load(Ordering::Relaxed),
            base,
            self.inner.max_x,
            self.inner.gain,
        ))
    }

    /// The stretch for the node's default base (what `/admin/health` shows).
    #[must_use]
    pub fn stretch_default(&self) -> Duration {
        self.stretch(self.inner.base)
    }

    /// The configured `lag_stretch_max_x`.
    #[must_use]
    pub fn max_x(&self) -> u64 {
        self.inner.max_x
    }
}

async fn probe_loop<E: Env>(env: E, monitor: LoadMonitor) {
    let probe = u64::try_from(PROBE.as_nanos()).unwrap_or(u64::MAX);
    loop {
        let from = env.now();
        env.sleep(PROBE).await;
        let took = env.now().0.saturating_sub(from.0);
        monitor.observe(took.saturating_sub(probe));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;

    #[test]
    fn estimate_attacks_instantly_and_decays_slowly() {
        let mut lag = 0;
        lag = next_estimate(lag, 80 * MS);
        assert_eq!(lag, 80 * MS, "instant attack");
        let after = next_estimate(lag, 0);
        assert!(after < lag && after > lag - lag / 16, "slow decay: {after}");
        // Half-life about 22 probes (32 * ln 2) at 1/32 per step.
        let mut l = 80 * MS;
        for _ in 0..22 {
            l = next_estimate(l, 0);
        }
        assert!(l < 45 * MS && l > 35 * MS, "{l}");
    }

    #[test]
    fn estimate_is_monotone_in_both_arguments_and_bounded() {
        let samples = [0, 1, 7 * MS, 12 * MS, 30 * MS, 99 * MS, 400 * MS];
        for &a in &samples {
            for &b in &samples {
                // Monotone in the previous estimate and in the sample.
                if a <= b {
                    for &s in &samples {
                        assert!(next_estimate(a, s) <= next_estimate(b, s));
                        assert!(next_estimate(s, a) <= next_estimate(s, b));
                    }
                }
                // Bounded by the larger of the two, never negative.
                assert!(next_estimate(a, b) <= a.max(b));
                // Never below the sample.
                assert!(next_estimate(a, b) >= b);
            }
        }
        // A sustained run of one sample converges to at most that sample.
        let mut lag = 0;
        for _ in 0..1000 {
            lag = next_estimate(lag, 50 * MS);
            assert!(lag <= 50 * MS);
        }
        assert_eq!(lag, 50 * MS);
        // Decay terminates (integer floor) at a tiny residue the dead band hides.
        let mut l = 400 * MS;
        for _ in 0..5000 {
            l = next_estimate(l, 0);
        }
        assert!(l < 32, "{l}");
        assert_eq!(reading(l), 0);
    }

    #[test]
    fn dead_band_reads_zero_below_ten_ms() {
        assert_eq!(reading(0), 0);
        assert_eq!(reading(10 * MS - 1), 0);
        assert_eq!(reading(10 * MS), 10 * MS);
        assert_eq!(reading(25 * MS), 25 * MS);
        assert_eq!(stretch_nanos(9 * MS, Duration::from_millis(150), 4), 0);
        assert_eq!(k_eff(32, 9 * MS), 32);
    }

    #[test]
    fn stretch_is_four_lags_capped_and_off_at_zero() {
        let base = Duration::from_millis(150);
        assert_eq!(stretch_nanos(10 * MS, base, 4), 40 * MS);
        assert_eq!(stretch_nanos(50 * MS, base, 4), 200 * MS);
        // Cap: 4 * base = 600 ms.
        assert_eq!(stretch_nanos(150 * MS, base, 4), 600 * MS);
        assert_eq!(stretch_nanos(10_000 * MS, base, 4), 600 * MS);
        assert_eq!(stretch_nanos(10_000 * MS, base, 1), 150 * MS);
        // Disabled.
        assert_eq!(stretch_nanos(10_000 * MS, base, 0), 0);
        // Monotone in lag.
        let mut prev = 0;
        for l in (0..400).map(|i| i * MS) {
            let s = stretch_nanos(l, base, 4);
            assert!(s >= prev && s <= 600 * MS);
            prev = s;
        }
        // Scales with the group's own base (the WAN profile).
        assert_eq!(
            stretch_nanos(10_000 * MS, Duration::from_millis(1000), 4),
            4000 * MS
        );
    }

    #[test]
    fn k_eff_halves_at_30ms_and_quarters_at_100ms() {
        assert_eq!(k_eff(32, 0), 32);
        assert_eq!(k_eff(32, 29 * MS), 32);
        assert_eq!(k_eff(32, 30 * MS), 16);
        assert_eq!(k_eff(32, 99 * MS), 16);
        assert_eq!(k_eff(32, 100 * MS), 8);
        assert_eq!(k_eff(32, 10_000 * MS), 8);
        // Minimum 1.
        assert_eq!(k_eff(1, 100 * MS), 1);
        assert_eq!(k_eff(3, 100 * MS), 1);
        assert_eq!(k_eff(2, 30 * MS), 1);
        // Monotone non-increasing in lag.
        let mut prev = usize::MAX;
        for l in (0..300).map(|i| i * MS) {
            let k = k_eff(32, l);
            assert!(k <= prev && k >= 1);
            prev = k;
        }
    }

    /// ADR 0079 decision Q9: `k_eff` was refactored onto the shared helper.
    /// The pre-refactor body, verbatim, is the oracle over a dense grid.
    #[test]
    fn k_eff_is_behaviour_identical_to_the_pre_shared_helper_body() {
        fn old_k_eff(k: usize, lag: u64) -> usize {
            let r = u128::from(reading(lag));
            let k = k.max(1);
            if r >= K_QUARTER_AT.as_nanos() {
                (k / 4).max(1)
            } else if r >= K_HALVE_AT.as_nanos() {
                (k / 2).max(1)
            } else {
                k
            }
        }
        let lags = (0..4000u64).map(|i| i * MS / 8).chain([
            u64::MAX,
            29_999_999,
            30_000_000,
            99_999_999,
            100_000_000,
        ]);
        for lag in lags {
            for k in (0..=70usize).chain([1000, 1500, 6000, usize::MAX]) {
                assert_eq!(k_eff(k, lag), old_k_eff(k, lag), "k={k} lag={lag}");
            }
        }
    }

    #[test]
    fn a_eff_follows_the_shared_rule() {
        assert_eq!(a_eff(1500, 0), 1500);
        assert_eq!(a_eff(1500, 30 * MS), 750);
        assert_eq!(a_eff(1500, 100 * MS), 375);
        assert_eq!(a_eff(1, 100 * MS), 1);
        for lag in [0, 9 * MS, 30 * MS, 99 * MS, 100 * MS, 5000 * MS] {
            assert_eq!(a_eff(32, lag), k_eff(32, lag));
        }
    }

    #[test]
    fn detached_monitor_publishes_readings() {
        let metrics = animus_env::MetricsHandle::default();
        let m = LoadMonitor::detached(4, Duration::from_millis(150), metrics, None);
        assert_eq!(m.lag(), Duration::ZERO);
        m.observe(5 * MS);
        assert_eq!(m.lag(), Duration::ZERO, "dead band");
        assert_eq!(m.stretch(Duration::from_millis(150)), Duration::ZERO);
        m.observe(50 * MS);
        assert_eq!(m.lag(), Duration::from_millis(50));
        assert_eq!(m.stretch_default(), Duration::from_millis(200));
        let off = LoadMonitor::detached(
            0,
            Duration::from_millis(150),
            animus_env::MetricsHandle::default(),
            None,
        );
        off.observe(500 * MS);
        assert_eq!(off.stretch_default(), Duration::ZERO);
    }
}
