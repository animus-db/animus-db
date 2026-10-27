//! The node-wide **compaction write-bytes budget** (ADR 0081 decision 3): a
//! token bucket with debt.
//!
//! Pure and `Env`-seam clean like [`crate::background_budget`]: no timers, no
//! tasks, no entropy; the current time is passed in and the refill is computed
//! from it at each request, so the bucket is deterministic under `SimEnv` and
//! testable as a plain function. `BTreeMap`-free (it is a single counter).
//!
//! # Shape
//!
//! * `rate` bytes/s refill, capacity (burst) = one second of the effective
//!   rate. The balance may go **negative** (debt).
//! * A non-urgent request of `n` bytes is admitted when the balance is
//!   positive and then subtracts `n`; it is never refused for being larger
//!   than the burst (a compaction is monolithic, a limiter that could never
//!   admit it would be a livelock). The long-run average is bounded by `rate`
//!   plus one compaction.
//! * An **urgent** request (the engine's L0 reached `2 x compaction_trigger`)
//!   is always admitted and charged into debt: the debt delays only the
//!   non-urgent engines, which is the right priority and the no-deadlock rule.
//! * A flush is charged into debt and never delayed.
//! * The effective rate follows the node's scheduling lag through the shared
//!   [`crate::load_monitor::scale_by_lag`] rule, with a floor
//!   ([`RATE_FLOOR_BYTES_PER_SEC`], or the configured rate when that is
//!   lower) so lag can never turn the budget into a starvation.
//!
//! The balance is kept in byte-nanoseconds (`bytes * 1e9`) so a refill of a
//! few nanoseconds loses nothing to integer truncation.

use animus_env::Nanos;

/// Floor of the lag-scaled rate: 4 MiB/s (ADR 0081 decision 3).
pub const RATE_FLOOR_BYTES_PER_SEC: u64 = 4 << 20;

const NANOS: i128 = 1_000_000_000;

/// A token bucket with debt. `rate == 0` means **off**: every request is
/// admitted and nothing is tracked.
#[derive(Clone, Debug)]
pub struct IoBucket {
    rate: u64,
    /// Balance in byte-nanoseconds; negative is debt.
    balance: i128,
    last: Option<Nanos>,
    /// Admissions of non-urgent requests / urgent bypasses / refusals.
    pub admitted: u64,
    /// Urgent requests admitted over an empty or negative balance.
    pub urgent: u64,
    /// Non-urgent requests refused for want of credit.
    pub refused: u64,
    /// Total bytes charged (admissions, flush charges and settle deltas).
    pub charged_bytes: u128,
}

impl IoBucket {
    /// A bucket refilling at `rate_bytes_per_sec` (`0` = off), starting full.
    #[must_use]
    pub fn new(rate_bytes_per_sec: u64) -> Self {
        Self {
            rate: rate_bytes_per_sec,
            balance: i128::from(rate_bytes_per_sec) * NANOS,
            last: None,
            admitted: 0,
            urgent: 0,
            refused: 0,
            charged_bytes: 0,
        }
    }

    /// Whether the budget is on.
    #[must_use]
    pub fn is_on(&self) -> bool {
        self.rate > 0
    }

    /// The configured rate (bytes/s).
    #[must_use]
    pub fn rate(&self) -> u64 {
        self.rate
    }

    /// The effective rate at node scheduling lag `lag_nanos`: the configured
    /// rate reduced by the shared lag rule, never below
    /// `min(rate, RATE_FLOOR_BYTES_PER_SEC)`.
    #[must_use]
    pub fn effective_rate(&self, lag_nanos: u64) -> u64 {
        if self.rate == 0 {
            return 0;
        }
        let scaled = crate::load_monitor::scale_by_lag(
            usize::try_from(self.rate).unwrap_or(usize::MAX),
            lag_nanos,
        ) as u64;
        scaled.max(self.rate.min(RATE_FLOOR_BYTES_PER_SEC))
    }

    /// Refill up to `now` at `rate_eff`, capped at one second of it. Time that
    /// goes backwards is ignored.
    fn refill(&mut self, now: Nanos, rate_eff: u64) {
        let cap = i128::from(rate_eff) * NANOS;
        if let Some(last) = self.last
            && now.0 > last.0
        {
            let add = i128::from(now.0 - last.0) * i128::from(rate_eff);
            self.balance = (self.balance + add).min(cap);
        }
        // A balance above the (lag-lowered) cap is clamped down too.
        self.balance = self.balance.min(cap);
        self.last = Some(now);
    }

    /// Ask to spend `bytes`. `urgent` requests are always admitted and may
    /// overdraw; others are admitted only with a positive balance. Returns
    /// whether the caller may run now.
    pub fn try_admit(&mut self, now: Nanos, lag_nanos: u64, bytes: u64, urgent: bool) -> bool {
        if self.rate == 0 {
            return true;
        }
        let eff = self.effective_rate(lag_nanos);
        self.refill(now, eff);
        if urgent {
            if self.balance <= 0 {
                self.urgent += 1;
            }
            self.debit(bytes);
            return true;
        }
        if self.balance > 0 {
            self.admitted += 1;
            self.debit(bytes);
            true
        } else {
            self.refused += 1;
            false
        }
    }

    /// Correct an earlier charge of `estimated` with the `actual` bytes
    /// written: a refund when smaller, more debt when larger.
    pub fn settle(&mut self, estimated: u64, actual: u64) {
        if self.rate == 0 {
            return;
        }
        self.balance += (i128::from(estimated) - i128::from(actual)) * NANOS;
        self.charged_bytes = self
            .charged_bytes
            .saturating_add(u128::from(actual))
            .saturating_sub(u128::from(estimated));
    }

    /// Charge bytes that must never be delayed (a flush's output) into debt.
    pub fn charge(&mut self, now: Nanos, lag_nanos: u64, bytes: u64) {
        if self.rate == 0 {
            return;
        }
        let eff = self.effective_rate(lag_nanos);
        self.refill(now, eff);
        self.debit(bytes);
    }

    fn debit(&mut self, bytes: u64) {
        self.balance -= i128::from(bytes) * NANOS;
        self.charged_bytes = self.charged_bytes.saturating_add(u128::from(bytes));
    }

    /// Bytes the bucket is overdrawn by (0 when in credit).
    #[must_use]
    pub fn debt_bytes(&self) -> u64 {
        if self.balance < 0 {
            u64::try_from(-self.balance / NANOS).unwrap_or(u64::MAX)
        } else {
            0
        }
    }

    /// Whole bytes of credit (0 when in debt).
    #[must_use]
    pub fn credit_bytes(&self) -> u64 {
        if self.balance > 0 {
            u64::try_from(self.balance / NANOS).unwrap_or(u64::MAX)
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;
    fn t(ms: u64) -> Nanos {
        Nanos(ms * 1_000_000)
    }

    #[test]
    fn off_admits_everything_and_tracks_nothing() {
        let mut b = IoBucket::new(0);
        assert!(!b.is_on());
        assert!(b.try_admit(t(0), 0, u64::MAX, false));
        b.charge(t(0), 0, 1 << 40);
        assert_eq!(b.debt_bytes(), 0);
    }

    #[test]
    fn starts_full_admits_a_request_larger_than_the_burst_then_refuses() {
        let mut b = IoBucket::new(8 * MIB);
        // A 100 MiB compaction is admitted against the 8 MiB burst: never a livelock.
        assert!(b.try_admit(t(0), 0, 100 * MIB, false));
        assert_eq!(b.debt_bytes(), 92 * MIB);
        assert!(!b.try_admit(t(10), 0, MIB, false));
        assert_eq!(b.refused, 1);
    }

    #[test]
    fn refills_from_the_passed_in_time_and_caps_at_one_second() {
        let mut b = IoBucket::new(8 * MIB);
        assert!(b.try_admit(t(0), 0, 8 * MIB + 8 * MIB, false)); // balance -8 MiB
        // 1 s later the debt is repaid exactly: balance 0, not yet positive.
        assert!(!b.try_admit(t(1000), 0, 1, false));
        // A little more time and it is positive again.
        assert!(b.try_admit(t(1001), 0, 1, false));
        // A very long idle caps at one second of credit.
        let mut b = IoBucket::new(8 * MIB);
        b.charge(t(0), 0, 0);
        assert!(b.try_admit(t(3_600_000), 0, 0, false));
        assert_eq!(b.credit_bytes(), 8 * MIB);
    }

    #[test]
    fn long_run_rate_is_bounded_by_rate_plus_one_compaction() {
        // Closed loop: ask every 10 ms for a 3 MiB compaction at 8 MiB/s.
        let mut b = IoBucket::new(8 * MIB);
        let mut written = 0u64;
        for ms in (0..60_000).step_by(10) {
            if b.try_admit(t(ms), 0, 3 * MIB, false) {
                written += 3 * MIB;
            }
        }
        let bound = 8 * MIB * 60 + 8 * MIB + 3 * MIB;
        assert!(written <= bound, "{written} > {bound}");
        assert!(written >= 8 * MIB * 55, "starved: {written}");
    }

    #[test]
    fn urgent_is_always_admitted_into_debt_and_delays_the_non_urgent() {
        let mut b = IoBucket::new(4 * MIB);
        assert!(b.try_admit(t(0), 0, 8 * MIB, false));
        assert!(b.try_admit(t(1), 0, 50 * MIB, true));
        assert_eq!(b.urgent, 1);
        assert!(b.debt_bytes() > 40 * MIB);
        assert!(!b.try_admit(t(2), 0, 1, false));
        // Debt of ~54 MiB at 4 MiB/s repays in ~13.5 s.
        assert!(!b.try_admit(t(10_000), 0, 1, false));
        assert!(b.try_admit(t(15_000), 0, 1, false));
    }

    #[test]
    fn settle_refunds_or_adds_debt() {
        let mut b = IoBucket::new(4 * MIB);
        assert!(b.try_admit(t(0), 0, 10 * MIB, false));
        let before = b.debt_bytes();
        b.settle(10 * MIB, 4 * MIB);
        assert_eq!(b.debt_bytes(), before - 6 * MIB);
        b.settle(4 * MIB, 9 * MIB);
        assert_eq!(b.debt_bytes(), before - 6 * MIB + 5 * MIB);
    }

    #[test]
    fn flush_charges_never_refuse_but_do_make_debt() {
        let mut b = IoBucket::new(MIB);
        b.charge(t(0), 0, 5 * MIB);
        assert_eq!(b.debt_bytes(), 4 * MIB);
        assert!(!b.try_admit(t(1), 0, 1, false));
    }

    #[test]
    fn lag_scales_the_rate_down_with_a_floor() {
        let b = IoBucket::new(64 * MIB);
        assert_eq!(b.effective_rate(0), 64 * MIB);
        assert_eq!(b.effective_rate(30_000_000), 32 * MIB);
        assert_eq!(b.effective_rate(100_000_000), 16 * MIB);
        // 8 MiB/s quartered would be 2 MiB/s: held at the 4 MiB/s floor.
        let b = IoBucket::new(8 * MIB);
        assert_eq!(b.effective_rate(100_000_000), 4 * MIB);
        // A configured rate below the floor is never raised above itself.
        let b = IoBucket::new(MIB);
        assert_eq!(b.effective_rate(500_000_000), MIB);
    }

    #[test]
    fn deterministic_for_equal_inputs_and_tolerant_of_a_clock_that_stalls() {
        let run = || {
            let mut b = IoBucket::new(2 * MIB);
            let mut out = Vec::new();
            for ms in [0, 0, 5, 5, 400, 399, 2000, 2001] {
                out.push(b.try_admit(t(ms), 0, MIB, false));
            }
            (out, b.debt_bytes())
        };
        assert_eq!(run(), run());
    }
}
