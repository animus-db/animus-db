//! The node-wide **`QueuedBytes` budget** (ADR 0080 section 4): one shared
//! counter that every bounded queue on a node charges what it holds against,
//! so "every queue is bounded" (ADR 0074) also has a *sum* — the memory the
//! node's queues can hold is `B` plus a stated allowance, not
//! `queues x per-queue cap`.
//!
//! Pure and lock-free: an `Arc` of atomics, no `Env`, no clock, no task. A
//! charge is an RAII [`Reservation`]; dropping it (including on future
//! cancellation or a panic unwind) returns the bytes, so there is no leak
//! path that needs an explicit release.
//!
//! Two kinds of charge:
//!
//! - [`try_reserve`](QueuedBytes::try_reserve): refused when the budget is at
//!   100 %. For client-class work, which can be turned away with a retryable
//!   error.
//! - [`force_reserve`](QueuedBytes::force_reserve): always succeeds and is
//!   counted. For `Internal` and `Liveness` work, which must never be
//!   refused (a refused 2PC resolve behind a refused stage is a deadlock), so
//!   the real bound is `B` plus what those classes hold, which is itself
//!   bounded by the propose bound's 2x allowance.
//!
//! A budget of `0` is *unlimited*: charges are still counted (the gauge and
//! high-water mark stay honest) but never refused.
//!
//! The **soft watermark** ([`SOFT_PERCENT`], 70 %) is a read-only predicate,
//! [`over_soft`](QueuedBytes::over_soft): callers shrink their bounds by one
//! lag step while it is true (ADR 0080 section 4).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// The soft watermark, percent of the budget. At or above it the node gate and
/// the group bounds shrink by one lag step.
pub const SOFT_PERCENT: u64 = 70;

/// A hot group may reserve at most `budget / PER_GROUP_DIVISOR` of the budget.
pub const PER_GROUP_DIVISOR: u64 = 8;

#[derive(Debug)]
struct Inner {
    /// `0` = unlimited.
    budget: AtomicU64,
    used: AtomicU64,
    hwm: AtomicU64,
    /// Reservations refused because the budget was at 100 %.
    refused: AtomicU64,
}

/// The shared budget handle. Cloning is an `Arc` bump; every clone charges the
/// same counter.
#[derive(Clone, Debug)]
pub struct QueuedBytes {
    inner: Arc<Inner>,
}

/// An RAII charge against a [`QueuedBytes`] budget; the bytes return on drop.
#[derive(Debug)]
pub struct Reservation {
    inner: Arc<Inner>,
    n: u64,
}

impl Reservation {
    /// The bytes this reservation holds.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.n
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.inner.used.fetch_sub(self.n, Ordering::AcqRel);
    }
}

impl QueuedBytes {
    /// A budget of `budget` bytes; `0` is unlimited (counted, never refused).
    #[must_use]
    pub fn new(budget: u64) -> Self {
        Self {
            inner: Arc::new(Inner {
                budget: AtomicU64::new(budget),
                used: AtomicU64::new(0),
                hwm: AtomicU64::new(0),
                refused: AtomicU64::new(0),
            }),
        }
    }

    /// An unlimited budget (counts only).
    #[must_use]
    pub fn unlimited() -> Self {
        Self::new(0)
    }

    /// The configured budget (`0` = unlimited).
    #[must_use]
    pub fn budget(&self) -> u64 {
        self.inner.budget.load(Ordering::Relaxed)
    }

    /// Bytes charged now.
    #[must_use]
    pub fn used(&self) -> u64 {
        self.inner.used.load(Ordering::Acquire)
    }

    /// The most bytes ever charged at once.
    #[must_use]
    pub fn hwm(&self) -> u64 {
        self.inner.hwm.load(Ordering::Relaxed)
    }

    /// Reservations refused so far.
    #[must_use]
    pub fn refused(&self) -> u64 {
        self.inner.refused.load(Ordering::Relaxed)
    }

    /// Charge `n` bytes unless that would take the budget past 100 %.
    /// `None` means refuse the work (client class).
    #[must_use]
    pub fn try_reserve(&self, n: u64) -> Option<Reservation> {
        let budget = self.budget();
        let mut cur = self.inner.used.load(Ordering::Acquire);
        loop {
            // A single charge larger than the whole budget is admitted when
            // nothing else is queued, so an oversized item is not refused
            // forever; the per-queue bounds cap what one charge can be.
            if budget != 0 && cur.saturating_add(n) > budget && cur != 0 {
                self.inner.refused.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            match self.inner.used.compare_exchange_weak(
                cur,
                cur + n,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.inner.hwm.fetch_max(cur + n, Ordering::Relaxed);
                    return Some(Reservation {
                        inner: Arc::clone(&self.inner),
                        n,
                    });
                }
                Err(seen) => cur = seen,
            }
        }
    }

    /// Charge `n` bytes unconditionally (`Internal`/`Liveness` classes).
    #[must_use]
    pub fn force_reserve(&self, n: u64) -> Reservation {
        let now = self.inner.used.fetch_add(n, Ordering::AcqRel) + n;
        self.inner.hwm.fetch_max(now, Ordering::Relaxed);
        Reservation {
            inner: Arc::clone(&self.inner),
            n,
        }
    }

    /// Whether the budget is at or past the soft watermark (70 %). Always
    /// `false` for an unlimited budget.
    #[must_use]
    pub fn over_soft(&self) -> bool {
        let b = self.budget();
        b != 0 && self.used().saturating_mul(100) >= b.saturating_mul(SOFT_PERCENT)
    }

    /// Whether the budget is at or past 100 %. Always `false` when unlimited.
    #[must_use]
    pub fn over_hard(&self) -> bool {
        let b = self.budget();
        b != 0 && self.used() >= b
    }

    /// Publish the level and high-water mark to `metrics`
    /// (`queued_bytes`, `queued_bytes_hwm`).
    pub fn publish(&self, metrics: &crate::MetricsHandle) {
        metrics.set(crate::Metric::QueuedBytes, self.used());
        metrics.set_max(crate::Metric::QueuedBytesHwm, self.hwm());
    }

    /// The most one group may hold: `min(configured, budget / 8)`; just
    /// `configured` when the budget is unlimited. `configured == 0` (off)
    /// stays `0`.
    #[must_use]
    pub fn per_group_cap(&self, configured: u64) -> u64 {
        let b = self.budget();
        if configured == 0 || b == 0 {
            configured
        } else {
            configured.min((b / PER_GROUP_DIVISOR).max(1))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_and_release_are_exact() {
        let q = QueuedBytes::new(100);
        let a = q.try_reserve(60).expect("fits");
        assert_eq!(q.used(), 60);
        assert!(q.try_reserve(41).is_none(), "past 100 %");
        assert_eq!(q.refused(), 1);
        let b = q.try_reserve(40).expect("exactly the budget");
        assert_eq!((q.used(), q.hwm()), (100, 100));
        drop(a);
        assert_eq!(q.used(), 40);
        drop(b);
        assert_eq!(q.used(), 0);
        assert_eq!(q.hwm(), 100, "the high-water mark is never lowered");
    }

    #[test]
    fn force_reserve_can_exceed_the_budget_and_is_counted() {
        let q = QueuedBytes::new(100);
        let _a = q.try_reserve(100).expect("fills");
        let f = q.force_reserve(30);
        assert_eq!(q.used(), 130);
        assert!(q.over_hard());
        assert!(q.try_reserve(1).is_none(), "client class is refused");
        drop(f);
        assert_eq!(q.used(), 100);
    }

    #[test]
    fn a_dropped_reservation_never_leaks_across_cancellation() {
        let q = QueuedBytes::new(1 << 20);
        for i in 0..1000u64 {
            let r = q.try_reserve(i + 1).expect("fits");
            // A moved-into-closure reservation dropped without ever being
            // used (the shape of a cancelled future).
            let f = move || drop(r);
            if i % 3 == 0 {
                f();
            }
        }
        assert_eq!(q.used(), 0);
    }

    #[test]
    fn soft_watermark_is_seventy_percent() {
        let q = QueuedBytes::new(1000);
        let _a = q.try_reserve(699).expect("fits");
        assert!(!q.over_soft());
        let _b = q.try_reserve(1).expect("fits");
        assert!(q.over_soft());
        assert!(!q.over_hard());
    }

    #[test]
    fn unlimited_counts_but_never_refuses() {
        let q = QueuedBytes::unlimited();
        let r = q.try_reserve(u64::MAX / 4).expect("unlimited");
        assert!(!q.over_soft() && !q.over_hard());
        assert_eq!(q.used(), u64::MAX / 4);
        drop(r);
        assert_eq!(q.per_group_cap(4 << 20), 4 << 20);
    }

    #[test]
    fn an_oversized_single_charge_is_admitted_when_the_budget_is_empty() {
        let q = QueuedBytes::new(100);
        let big = q.try_reserve(500).expect("alone");
        assert!(q.try_reserve(1).is_none());
        drop(big);
        assert!(q.try_reserve(1).is_some());
    }

    #[test]
    fn per_group_cap_is_min_of_configured_and_an_eighth() {
        let q = QueuedBytes::new(256 << 20);
        assert_eq!(q.per_group_cap(4 << 20), 4 << 20);
        let small = QueuedBytes::new(8 << 20);
        assert_eq!(small.per_group_cap(4 << 20), 1 << 20);
        assert_eq!(small.per_group_cap(0), 0, "off stays off");
    }

    #[test]
    fn contention_never_overshoots_the_budget() {
        let q = QueuedBytes::new(64);
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let q = q.clone();
                std::thread::spawn(move || {
                    for _ in 0..20_000 {
                        if let Some(r) = q.try_reserve(8) {
                            assert!(q.used() <= 64);
                            drop(r);
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().expect("worker panicked");
        }
        assert_eq!(q.used(), 0);
        assert!(q.hwm() <= 64);
    }
}
