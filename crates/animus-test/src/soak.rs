//! Soak-run support (R-01 sub-track (a), `docs/soak.md`): a pure, deterministic
//! resource-trend detector and the duration-knob parser.
//!
//! The soak harness (`animusd`'s `tests/soak.rs`) samples RSS, open fds,
//! threads, data-dir/WAL/SSTable bytes and a few backlog gauges per node. This
//! module decides, from one series of `(seconds, value)` samples, whether the
//! series shows **monotone growth** after a warm-up. It does no I/O and reads
//! no clock, so it is a pure function of its input and unit-tested here with
//! synthetic series (growth, plateau, compaction sawtooth, step-then-flat).
//!
//! # The rule
//!
//! Samples before `first_t + warmup_secs` are dropped. The rest is split into
//! `windows` equal-duration windows and each window is reduced to its
//! **median** (robust to a compaction/GC sawtooth whose period is shorter than
//! a window, and to single outlier samples). With tolerance
//! `tol = max(abs_tol, rel_tol * |first window median|)`, a series is
//! [`Verdict::Growing`] when either:
//!
//! - **(A) new high**: the last window's median exceeds the *maximum* of every
//!   earlier window's median by more than `tol` (a step that is still rising
//!   or has not been followed by a plateau), or
//! - **(B) steady climb**: last minus first window median exceeds `tol`, at
//!   least `min_rising_fraction` of the window-to-window steps are rising, and
//!   the least-squares slope over the raw post-warm-up samples, projected over
//!   the span, also exceeds `tol`.
//!
//! (A) catches a leak that starts late or is large; (B) a slow leak whose
//! per-window increment is below `tol` but whose total is not. A flat series
//! with noise, a sawtooth, a one-off step up that then plateaus, and a
//! decreasing series are [`Verdict::Bounded`]. Too few samples (a window with
//! fewer than `min_samples_per_window`) is [`Verdict::Insufficient`], which is
//! never a failure: the caller prints it and the run is simply unproven for
//! that series.

use std::time::Duration;

/// Parameters of [`evaluate`].
#[derive(Clone, Debug)]
pub struct TrendConfig {
    /// Seconds after the first sample that are ignored (caches, tablet
    /// splits, the first compactions, allocator high-water marks).
    pub warmup_secs: f64,
    /// Number of equal-duration windows the post-warm-up span is cut into.
    pub windows: usize,
    /// A window with fewer samples makes the verdict `Insufficient`.
    pub min_samples_per_window: usize,
    /// Relative tolerance, as a fraction of the first window's median.
    pub rel_tol: f64,
    /// Absolute tolerance floor, in the series' own unit.
    pub abs_tol: f64,
    /// Fraction of window-to-window steps that must rise for rule (B).
    pub min_rising_fraction: f64,
}

impl TrendConfig {
    /// A config with the harness defaults for the given warm-up.
    #[must_use]
    pub fn new(warmup_secs: f64, rel_tol: f64, abs_tol: f64) -> Self {
        Self {
            warmup_secs,
            windows: 6,
            min_samples_per_window: 3,
            rel_tol,
            abs_tol,
            min_rising_fraction: 0.8,
        }
    }
}

/// The outcome for one series.
#[derive(Clone, Debug, PartialEq)]
pub enum Verdict {
    /// Not enough post-warm-up data to judge.
    Insufficient(String),
    /// No monotone growth beyond tolerance.
    Bounded,
    /// Monotone growth: the reason names the rule that fired.
    Growing(String),
}

/// A verdict plus the numbers behind it, for the run summary.
#[derive(Clone, Debug)]
pub struct TrendReport {
    /// The decision.
    pub verdict: Verdict,
    /// Per-window medians (empty when `Insufficient` for lack of span).
    pub window_medians: Vec<f64>,
    /// Least-squares slope per second over the post-warm-up samples.
    pub slope_per_sec: f64,
    /// Tolerance that applied.
    pub tol: f64,
    /// Maximum post-warm-up sample.
    pub max: f64,
}

impl TrendReport {
    /// `true` only for [`Verdict::Growing`].
    #[must_use]
    pub fn is_growing(&self) -> bool {
        matches!(self.verdict, Verdict::Growing(_))
    }
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.total_cmp(b));
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

fn slope(points: &[(f64, f64)]) -> f64 {
    let n = points.len() as f64;
    if points.len() < 2 {
        return 0.0;
    }
    let mt = points.iter().map(|p| p.0).sum::<f64>() / n;
    let mv = points.iter().map(|p| p.1).sum::<f64>() / n;
    let (mut num, mut den) = (0.0, 0.0);
    for (t, v) in points {
        num += (t - mt) * (v - mv);
        den += (t - mt) * (t - mt);
    }
    if den == 0.0 { 0.0 } else { num / den }
}

/// Decide whether `samples` (`(seconds, value)`, ascending in time) show
/// monotone growth after the warm-up. Pure and deterministic.
#[must_use]
pub fn evaluate(samples: &[(f64, f64)], cfg: &TrendConfig) -> TrendReport {
    let insufficient = |why: String| TrendReport {
        verdict: Verdict::Insufficient(why),
        window_medians: Vec::new(),
        slope_per_sec: 0.0,
        tol: cfg.abs_tol,
        max: f64::NAN,
    };
    let Some(first) = samples.first() else {
        return insufficient("no samples".into());
    };
    let t0 = first.0 + cfg.warmup_secs;
    let warm: Vec<(f64, f64)> = samples.iter().copied().filter(|p| p.0 >= t0).collect();
    let (Some(lo), Some(hi)) = (warm.first(), warm.last()) else {
        return insufficient(format!(
            "all samples inside the {}s warm-up",
            cfg.warmup_secs
        ));
    };
    let span = hi.0 - lo.0;
    if span <= 0.0 || cfg.windows < 2 {
        return insufficient("post-warm-up span is empty".into());
    }
    let width = span / cfg.windows as f64;
    let mut buckets: Vec<Vec<f64>> = vec![Vec::new(); cfg.windows];
    for (t, v) in &warm {
        let i = (((t - lo.0) / width) as usize).min(cfg.windows - 1);
        buckets[i].push(*v);
    }
    if let Some((i, b)) = buckets
        .iter()
        .enumerate()
        .find(|(_, b)| b.len() < cfg.min_samples_per_window)
    {
        return insufficient(format!(
            "window {i} has {} sample(s), need {}",
            b.len(),
            cfg.min_samples_per_window
        ));
    }
    let medians: Vec<f64> = buckets.iter_mut().map(|b| median(b)).collect();
    let max = warm.iter().map(|p| p.1).fold(f64::NEG_INFINITY, f64::max);
    let sl = slope(&warm);
    let tol = cfg.abs_tol.max(cfg.rel_tol * medians[0].abs());
    let last = medians[cfg.windows - 1];
    let first_m = medians[0];
    let prior_max = medians[..cfg.windows - 1]
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);

    let steps = cfg.windows - 1;
    let rising = medians.windows(2).filter(|w| w[1] > w[0]).count();
    let verdict = if last - prior_max > tol {
        Verdict::Growing(format!(
            "(A) last window median {last:.1} is a new high, {:.1} above every earlier window (max {prior_max:.1}); tolerance {tol:.1}",
            last - prior_max
        ))
    } else if last - first_m > tol
        && rising as f64 >= cfg.min_rising_fraction * steps as f64
        && sl * span > tol
    {
        Verdict::Growing(format!(
            "(B) steady climb: median {first_m:.1} -> {last:.1} (+{:.1}), {rising}/{steps} windows rising, slope {:.4}/s; tolerance {tol:.1}",
            last - first_m,
            sl
        ))
    } else {
        Verdict::Bounded
    };
    TrendReport {
        verdict,
        window_medians: medians,
        slope_per_sec: sl,
        tol,
        max,
    }
}

/// Parse a soak duration: a bare number of seconds, or a number with an
/// `s`/`m`/`h`/`d` suffix (`90`, `15m`, `2h`, `7d`). `None` if malformed or
/// zero.
#[must_use]
pub fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    let (num, mult) = match s.chars().last()? {
        's' => (&s[..s.len() - 1], 1),
        'm' => (&s[..s.len() - 1], 60),
        'h' => (&s[..s.len() - 1], 3600),
        'd' => (&s[..s.len() - 1], 86_400),
        c if c.is_ascii_digit() => (s, 1),
        _ => return None,
    };
    let n: u64 = num.trim().parse().ok()?;
    (n > 0).then(|| Duration::from_secs(n.saturating_mul(mult)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic noise in `[-1, 1)` (a fixed LCG; no clock, no rng crate).
    fn noise(i: u64) -> f64 {
        let x = i
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((x >> 33) as f64 / (1u64 << 31) as f64) * 2.0 - 1.0
    }

    /// One sample every 15 s for `secs`, value from `f(t, i)`.
    fn series(secs: u64, f: impl Fn(f64, u64) -> f64) -> Vec<(f64, f64)> {
        (0..secs / 15)
            .map(|i| {
                let t = (i * 15) as f64;
                (t, f(t, i))
            })
            .collect()
    }

    fn cfg() -> TrendConfig {
        // 10% relative, 1000-unit floor, 1 h warm-up.
        TrendConfig::new(3600.0, 0.10, 1000.0)
    }

    const DAY: u64 = 86_400;

    #[test]
    fn linear_growth_is_flagged() {
        // +12 units/min on a 100_000 baseline: +17_280/day, above the 10% tol.
        let s = series(DAY, |t, i| 100_000.0 + t / 5.0 + 50.0 * noise(i));
        let r = evaluate(&s, &cfg());
        assert!(r.is_growing(), "{r:?}");
    }

    #[test]
    fn plateau_with_noise_is_bounded() {
        let s = series(DAY, |_, i| 100_000.0 + 500.0 * noise(i));
        let r = evaluate(&s, &cfg());
        assert_eq!(r.verdict, Verdict::Bounded, "{r:?}");
    }

    #[test]
    fn compaction_sawtooth_is_bounded() {
        // Ramp 0..8000 over 20 min, then drops back: period shorter than a
        // window, amplitude below the 10% tolerance of a 100_000 baseline.
        let s = series(DAY, |t, i| {
            100_000.0 + (t % 1200.0) / 1200.0 * 8000.0 + 100.0 * noise(i)
        });
        let r = evaluate(&s, &cfg());
        assert_eq!(r.verdict, Verdict::Bounded, "{r:?}");
    }

    #[test]
    fn sawtooth_on_a_leak_is_still_flagged() {
        let s = series(DAY, |t, i| {
            100_000.0 + t / 5.0 + (t % 1200.0) / 1200.0 * 8000.0 + 100.0 * noise(i)
        });
        let r = evaluate(&s, &cfg());
        assert!(r.is_growing(), "{r:?}");
    }

    #[test]
    fn step_up_then_flat_is_bounded() {
        // A one-off +30% step six hours in (a tablet split, a cache), then flat.
        let s = series(DAY, |t, i| {
            let base = if t > 6.0 * 3600.0 {
                130_000.0
            } else {
                100_000.0
            };
            base + 300.0 * noise(i)
        });
        let r = evaluate(&s, &cfg());
        assert_eq!(r.verdict, Verdict::Bounded, "{r:?}");
    }

    #[test]
    fn growth_only_inside_warmup_is_ignored() {
        // Ramps 0 -> 50_000 in the first 40 minutes, flat afterwards.
        let s = series(DAY, |t, i| {
            20_000.0 + (t.min(2400.0) / 2400.0) * 50_000.0 + 200.0 * noise(i)
        });
        let r = evaluate(&s, &cfg());
        assert_eq!(r.verdict, Verdict::Bounded, "{r:?}");
    }

    #[test]
    fn late_onset_leak_is_flagged_by_the_new_high_rule() {
        // Flat for 18 h, then a steep climb: only the last windows rise.
        let s = series(DAY, |t, i| {
            let leak = (t - 18.0 * 3600.0).max(0.0);
            100_000.0 + leak + 200.0 * noise(i)
        });
        let r = evaluate(&s, &cfg());
        assert!(r.is_growing(), "{r:?}");
        assert!(
            matches!(&r.verdict, Verdict::Growing(m) if m.starts_with("(A)")),
            "{r:?}"
        );
    }

    #[test]
    fn slow_fd_leak_is_flagged_by_the_steady_climb_rule() {
        // fds: 1 per 15 min on a base of 200 (tolerance floor 16): +96/day.
        let c = TrendConfig::new(3600.0, 0.05, 16.0);
        let s = series(DAY, |t, _| 200.0 + (t / 900.0).floor());
        let r = evaluate(&s, &c);
        assert!(r.is_growing(), "{r:?}");
    }

    #[test]
    fn decreasing_series_is_bounded() {
        let s = series(DAY, |t, i| 200_000.0 - t / 10.0 + 100.0 * noise(i));
        let r = evaluate(&s, &cfg());
        assert_eq!(r.verdict, Verdict::Bounded, "{r:?}");
    }

    #[test]
    fn too_little_data_is_insufficient_not_a_failure() {
        let c = TrendConfig::new(600.0, 0.1, 10.0);
        assert!(matches!(
            evaluate(&[], &c).verdict,
            Verdict::Insufficient(_)
        ));
        // Everything inside the warm-up.
        let s = series(500, |_, _| 1.0);
        assert!(matches!(evaluate(&s, &c).verdict, Verdict::Insufficient(_)));
        // Past the warm-up but only a handful of samples per window.
        let s = series(700, |_, _| 1.0);
        assert!(matches!(evaluate(&s, &c).verdict, Verdict::Insufficient(_)));
    }

    #[test]
    fn evaluation_is_deterministic() {
        let s = series(DAY, |t, i| 100_000.0 + t / 40.0 + 300.0 * noise(i));
        let a = evaluate(&s, &cfg());
        let b = evaluate(&s, &cfg());
        assert_eq!(a.verdict, b.verdict);
        assert_eq!(a.window_medians, b.window_medians);
    }

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration("90"), Some(Duration::from_secs(90)));
        assert_eq!(parse_duration("15m"), Some(Duration::from_secs(900)));
        assert_eq!(parse_duration(" 2h "), Some(Duration::from_secs(7200)));
        assert_eq!(parse_duration("7d"), Some(Duration::from_secs(604_800)));
        assert_eq!(parse_duration("0"), None);
        assert_eq!(parse_duration("x"), None);
        assert_eq!(parse_duration("5w"), None);
        assert_eq!(parse_duration(""), None);
    }
}
