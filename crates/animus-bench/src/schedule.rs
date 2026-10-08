//! The open-loop arrival schedule — pure, clock-free.
//!
//! An open-loop generator decides *when each request should have been sent*
//! independently of how the server is doing. [`Schedule`] is that decision:
//! operation `i` has the **intended send time** `start + i / rate`. The I/O
//! shell sleeps until the intended time (or sends immediately if it is
//! already late — it never skips or delays an arrival), and latency is
//! measured from the intended time, not the actual send (see
//! [`crate::recorder`]). That is the whole coordinated-omission correction.

/// Nanoseconds in a second.
pub const NS_PER_SEC: u64 = 1_000_000_000;

/// A fixed-rate arrival schedule. Intended times are computed from the index
/// (never accumulated), so there is no drift however long the run is.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Schedule {
    start_ns: u64,
    rate_per_sec: f64,
}

impl Schedule {
    /// A schedule that sends operation 0 at `start_ns` and then `rate_per_sec`
    /// operations per second.
    ///
    /// # Panics
    /// If `rate_per_sec` is not finite and strictly positive (a configuration
    /// error the CLI rejects before it gets here).
    #[must_use]
    pub fn new(start_ns: u64, rate_per_sec: f64) -> Self {
        assert!(
            rate_per_sec.is_finite() && rate_per_sec > 0.0,
            "arrival rate must be finite and > 0, got {rate_per_sec}"
        );
        Self {
            start_ns,
            rate_per_sec,
        }
    }

    /// The configured arrival rate (ops/sec).
    #[must_use]
    pub fn rate(&self) -> f64 {
        self.rate_per_sec
    }

    /// The intended send time of operation `index`, in run-time nanoseconds.
    #[must_use]
    pub fn intended_ns(&self, index: u64) -> u64 {
        #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
        let offset = (index as f64 * NS_PER_SEC as f64 / self.rate_per_sec) as u64;
        self.start_ns.saturating_add(offset)
    }

    /// How many operations arrive in a window of `duration_ns` starting at
    /// the schedule's start (`floor(rate * duration)`).
    #[must_use]
    pub fn count_in(&self, duration_ns: u64) -> u64 {
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let n = (self.rate_per_sec * duration_ns as f64 / NS_PER_SEC as f64) as u64;
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intended_times_are_evenly_spaced_from_start() {
        let s = Schedule::new(5_000_000_000, 100.0);
        assert_eq!(s.intended_ns(0), 5_000_000_000);
        assert_eq!(s.intended_ns(1), 5_010_000_000);
        assert_eq!(s.intended_ns(100), 6_000_000_000);
        assert_eq!(s.intended_ns(12_345), 5_000_000_000 + 123_450_000_000);
    }

    #[test]
    fn fractional_rates_do_not_drift() {
        // 3 ops/sec: 1/3 s spacing, but index 3_000_000 must land exactly on
        // 1_000_000 s — computed from the index, not accumulated.
        let s = Schedule::new(0, 3.0);
        assert_eq!(s.intended_ns(3_000_000), 1_000_000 * NS_PER_SEC);
        assert!(s.intended_ns(1) > s.intended_ns(0));
    }

    #[test]
    fn count_in_window_is_rate_times_duration() {
        let s = Schedule::new(0, 250.0);
        assert_eq!(s.count_in(2 * NS_PER_SEC), 500);
        assert_eq!(s.count_in(NS_PER_SEC / 2), 125);
        // The last intended time of a window is strictly inside it.
        assert!(s.intended_ns(s.count_in(2 * NS_PER_SEC) - 1) < 2 * NS_PER_SEC);
    }

    #[test]
    #[should_panic(expected = "arrival rate")]
    fn zero_rate_is_rejected() {
        let _ = Schedule::new(0, 0.0);
    }
}
