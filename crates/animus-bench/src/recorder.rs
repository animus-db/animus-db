//! Latency recording with coordinated-omission correction — pure, clock-free.
//!
//! Every completed operation has three timestamps (run-time nanoseconds):
//! `intended` (when the schedule said it should be sent), `started` (when a
//! worker actually put it on the wire) and `completed`.
//!
//! - **Corrected latency** = `completed - intended`. If the server (or the
//!   client's own queue) stalls, every arrival scheduled during the stall is
//!   charged the time it *would have* waited had it been sent on time. This is
//!   the number reported as the headline.
//! - **Service time** = `completed - started`: what a closed-loop client
//!   would measure (it only sends when the previous call returned, so a
//!   stall silently removes the arrivals that would have suffered). Recorded
//!   alongside so a report shows both and a reader can see how much the
//!   correction matters.
//!
//! Histograms are HdrHistogram, microsecond resolution, 3 significant figures,
//! range 1 µs .. 1 h (values outside are saturated, never dropped).

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

/// Largest recordable latency (1 hour), in microseconds.
pub const MAX_LATENCY_US: u64 = 3_600_000_000;
const SIG_FIGS: u8 = 3;

fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, MAX_LATENCY_US, SIG_FIGS).expect("static histogram bounds")
}

fn ns_to_us(ns: u64) -> u64 {
    // Round up and floor at 1 µs so a sub-microsecond op still registers.
    ns.div_ceil(1_000).max(1)
}

/// Percentile summary of one histogram, all latencies in microseconds.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LatencySummary {
    pub count: u64,
    pub min_us: u64,
    pub mean_us: f64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub p99_9_us: u64,
    pub p99_99_us: u64,
    pub max_us: u64,
}

impl LatencySummary {
    fn of(h: &Histogram<u64>) -> Self {
        if h.is_empty() {
            return Self::default();
        }
        Self {
            count: h.len(),
            min_us: h.min(),
            mean_us: h.mean(),
            p50_us: h.value_at_quantile(0.50),
            p90_us: h.value_at_quantile(0.90),
            p99_us: h.value_at_quantile(0.99),
            p99_9_us: h.value_at_quantile(0.999),
            p99_99_us: h.value_at_quantile(0.9999),
            max_us: h.max(),
        }
    }
}

/// Both views of one operation class's latency.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ClassResult {
    /// Operations recorded (completed, whether or not the result was empty).
    pub count: u64,
    /// Latency from the **intended** send time (coordinated-omission
    /// corrected) — the headline numbers.
    pub corrected: LatencySummary,
    /// Latency from the actual send (what a closed-loop client sees).
    pub service: LatencySummary,
}

/// Recorder for one operation class.
#[derive(Clone, Debug)]
pub struct OpRecorder {
    corrected: Histogram<u64>,
    service: Histogram<u64>,
}

impl Default for OpRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl OpRecorder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            corrected: new_hist(),
            service: new_hist(),
        }
    }

    /// Record one completed operation. Timestamps are run-time nanoseconds;
    /// a `started`/`completed` earlier than `intended` saturates to zero.
    pub fn record(&mut self, intended_ns: u64, started_ns: u64, completed_ns: u64) {
        self.corrected
            .saturating_record(ns_to_us(completed_ns.saturating_sub(intended_ns)));
        self.service
            .saturating_record(ns_to_us(completed_ns.saturating_sub(started_ns)));
    }

    /// Fold another recorder (e.g. another worker's) into this one.
    pub fn merge(&mut self, other: &Self) {
        self.corrected
            .add(&other.corrected)
            .expect("same-bounds histograms always merge");
        self.service
            .add(&other.service)
            .expect("same-bounds histograms always merge");
    }

    /// Operations recorded so far.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.corrected.len()
    }

    /// Percentile summaries of both views.
    #[must_use]
    pub fn summary(&self) -> ClassResult {
        ClassResult {
            count: self.count(),
            corrected: LatencySummary::of(&self.corrected),
            service: LatencySummary::of(&self.service),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::{NS_PER_SEC, Schedule};

    const MS: u64 = 1_000_000;

    /// A single-connection FIFO server on a fake clock: request `k` cannot
    /// start before the previous one finished, and takes `service_ns(k)`.
    /// Returns `(intended, started, completed)` per request. No real time.
    fn simulate_fifo_server(
        schedule: &Schedule,
        n: u64,
        service_ns: impl Fn(u64) -> u64,
    ) -> Vec<(u64, u64, u64)> {
        let mut prev_done = 0u64;
        (0..n)
            .map(|k| {
                let intended = schedule.intended_ns(k);
                let started = intended.max(prev_done);
                let completed = started + service_ns(k);
                prev_done = completed;
                (intended, started, completed)
            })
            .collect()
    }

    fn record_all(samples: &[(u64, u64, u64)]) -> OpRecorder {
        let mut r = OpRecorder::new();
        for &(i, s, c) in samples {
            r.record(i, s, c);
        }
        r
    }

    /// THE coordinated-omission property. 100 req/s for 10 s; the server
    /// stalls for 2 s on request 100 (at t = 1 s) and is otherwise 1 ms.
    /// The ~220 arrivals scheduled during/after the stall queue behind it:
    /// the corrected view must show a ~2 s tail for them, while the
    /// service-time (closed-loop) view hides it — exactly one 2 s sample in
    /// a thousand, invisible below p99.9.
    #[test]
    fn stalled_server_shows_a_tail_only_in_the_corrected_view() {
        let stall_ns = 2 * NS_PER_SEC;
        let schedule = Schedule::new(0, 100.0);
        let samples =
            simulate_fifo_server(&schedule, 1_000, |k| if k == 100 { stall_ns } else { MS });
        let rec = record_all(&samples);
        let s = rec.summary();
        assert_eq!(s.count, 1_000);

        // Corrected: the queued requests inherit the stall.
        let two_s_us = stall_ns / 1_000;
        assert!(
            s.corrected.max_us >= two_s_us && s.corrected.max_us <= two_s_us + 10_000,
            "corrected max {} should be ~stall {}",
            s.corrected.max_us,
            two_s_us
        );
        assert!(
            s.corrected.p99_us >= 1_800_000,
            "corrected p99 {}us must reflect the ~2s stall",
            s.corrected.p99_us
        );
        assert!(
            s.corrected.p90_us >= 500_000,
            "corrected p90 {}us: ~22% of arrivals were delayed",
            s.corrected.p90_us
        );
        // The quiet majority is still fast (a median is not dragged up).
        assert!(s.corrected.p50_us <= 5_000, "p50 {}", s.corrected.p50_us);

        // Service time (what closed-loop would report): only the one stalled
        // request is slow; p99 and below are ~1 ms.
        assert!(
            s.service.p99_us <= 2_000,
            "service p99 {}us hides the stall",
            s.service.p99_us
        );
        assert!(s.service.p90_us <= 2_000);
        assert!(s.service.max_us >= two_s_us);

        // And the closed-loop view literally IS the service-time list: a
        // closed-loop client issues the next request only after the last
        // returned, so it never schedules the queued arrivals at all.
        let mut closed = OpRecorder::new();
        let mut t = 0u64;
        for k in 0..1_000u64 {
            let svc = if k == 100 { stall_ns } else { MS };
            closed.record(t, t, t + svc);
            t += svc;
        }
        let c = closed.summary();
        assert!(c.corrected.p99_us <= 2_000, "closed-loop p99 hides it too");
    }

    #[test]
    fn without_a_stall_both_views_agree() {
        let schedule = Schedule::new(0, 100.0);
        let samples = simulate_fifo_server(&schedule, 500, |_| MS);
        let s = record_all(&samples).summary();
        assert_eq!(s.corrected.p99_us, s.service.p99_us);
        assert_eq!(s.corrected.max_us, s.service.max_us);
    }

    #[test]
    fn client_side_queueing_is_charged_to_the_corrected_view() {
        // Offered load above capacity: arrivals every 1 ms, service 2 ms.
        // The backlog grows without bound; service time stays 2 ms while the
        // corrected latency of the last request is ~ n/2 ms.
        let schedule = Schedule::new(0, 1_000.0);
        let n = 1_000;
        let samples = simulate_fifo_server(&schedule, n, |_| 2 * MS);
        let s = record_all(&samples).summary();
        assert!(s.service.max_us <= 2_100, "service {}", s.service.max_us);
        assert!(
            s.corrected.max_us >= 490_000,
            "corrected max {}us should show the unbounded backlog",
            s.corrected.max_us
        );
    }

    #[test]
    fn merge_combines_counts_and_extremes() {
        let mut a = OpRecorder::new();
        let mut b = OpRecorder::new();
        a.record(0, 0, MS);
        b.record(0, 0, 50 * MS);
        a.merge(&b);
        let s = a.summary();
        assert_eq!(s.count, 2);
        assert!(s.corrected.max_us >= 50_000);
        assert!(s.corrected.min_us <= 1_001);
    }

    #[test]
    fn empty_summary_is_all_zero() {
        assert_eq!(OpRecorder::new().summary(), ClassResult::default());
    }
}
