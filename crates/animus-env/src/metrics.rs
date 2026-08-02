//! A deterministic-safe observability seam: monotonic counters and a small
//! gauge, recorded behind a cheap-to-clone handle (ADR 0015).
//!
//! This is intentionally a *minimal* sink — atomic counters keyed by a closed
//! [`Metric`] enum plus one leadership gauge — chosen so it never introduces
//! nondeterminism (the load-bearing constraint, ADR 0003). Specifically:
//!
//! - **No wall clock.** Recording a counter is a pure atomic add; nothing here
//!   reads time. A caller that wants a timestamped metric passes one derived
//!   from [`Clock::now`](crate::Clock::now), never `std::time`.
//! - **No `HashMap`/`HashSet`.** The recording side is a fixed array of atomics
//!   ([`MetricSink`]) so recording is lock-free and allocation-free; a snapshot
//!   collects into a [`BTreeMap`], whose iteration order is deterministic.
//! - **No unseeded randomness, no I/O.** Recording touches only atomics; export
//!   ([`MetricSink::snapshot`] / [`MetricSnapshot::to_text`]) is a pure read.
//!
//! The seam is **additive**: [`Env::metrics`](crate::Env::metrics) has a default
//! implementation returning a shared no-op handle, so every component generic
//! over `E: Env` keeps compiling and behaving identically whether or not it (or
//! its env) cares about metrics. [`ProdEnv`](crate::ProdEnv) overrides it with a
//! real recording handle; under simulation a test constructs a recording
//! [`MetricsHandle`] and threads it into the component it wants to observe (no
//! change to `animus-sim` required).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// The metrics this seam knows how to record (ADR 0015) — control-plane Raft and
/// leaderless-AP data-plane counters. Each variant maps to one slot in the
/// fixed-size [`MetricSink`] array, so recording is a single atomic op with no
/// map lookup and no allocation.
///
/// The set is deliberately closed and small: a closed enum keeps recording
/// O(1)/lock-free and makes the exported names a stable, reviewable surface.
/// Add a variant (and a row to [`Metric::ALL`]) to instrument something new —
/// **append** new variants after the existing ones so their array slots and the
/// text-export order stay stable (the snapshot is byte-reproducible).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Metric {
    /// A leader election was started (a follower/candidate timed out and bumped
    /// its term).
    ElectionsStarted,
    /// An election was won (this node became leader).
    ElectionsWon,
    /// An `AppendEntries` was sent to a peer (replication or heartbeat).
    AppendEntriesSent,
    /// An `AppendEntries` was rejected by a follower (consistency-check failure
    /// or a stale term), driving the leader to back up `next_index`.
    AppendEntriesRejected,
    /// A chunked `InstallSnapshot` transfer completed and was installed on this
    /// node (follower side).
    SnapshotInstalls,
    /// A member's failure-detector verdict transitioned `Active`->`Down` (a
    /// `Down` was proposed by the leader, ADR 0012).
    FailureDetectorDown,
    /// A member's failure-detector verdict transitioned `Down`->`Active` (a
    /// recovery was proposed by the leader, ADR 0012).
    FailureDetectorUp,

    // --- Data plane (ADR 0001/0010/0005, ADR 0015 data-plane extension) ---
    // Added after the control-plane variants; their array slots follow, so the
    // existing variants/order/slots stay stable and the export is byte-reproducible.
    /// A quorum write was attempted by the coordinator (a `Write` broadcast to a
    /// tablet's replicas).
    DataQuorumWritesAttempted,
    /// A quorum write committed: `W` replicas durably acked it.
    DataQuorumWritesSucceeded,
    /// A quorum write failed: fewer than `W` replicas acked within the timeout
    /// (sub-quorum / fenced / down replicas).
    DataQuorumWritesFailed,
    /// A quorum read was attempted by the coordinator (a `Read` broadcast).
    DataQuorumReadsAttempted,
    /// A quorum read succeeded: `R` replicas responded.
    DataQuorumReadsSucceeded,
    /// A quorum read failed: a read quorum could not be reached (sub-quorum /
    /// fenced / down replicas).
    DataQuorumReadsFailed,
    /// A read observed divergent responders and triggered read-repair (one push
    /// per divergent quorum read).
    DataReadRepairTriggered,
    /// Keys pushed back by read-repair (the winning `(value, version)` re-sent to
    /// the read's participants). One per repaired key.
    DataReadRepairKeysRepaired,
    /// A hint was stored for a replica that missed a committed write/delete
    /// (hinted handoff, ADR 0010). One per `(target, key)` buffered.
    DataHintsStored,
    /// A buffered hint batch was replayed to a returning target (a `Sync` of the
    /// missed entries sent on the handoff/replay path).
    DataHintsDelivered,
    /// A background anti-entropy round fired and emitted a segment digest to its
    /// peers (one per non-empty round, segment-digest exchange).
    DataAntiEntropyRounds,
}

impl Metric {
    /// Every metric, in a fixed order. The array index of a metric in `ALL` is
    /// its slot in the [`MetricSink`]; keep this in sync with the enum.
    pub const ALL: [Metric; 18] = [
        Metric::ElectionsStarted,
        Metric::ElectionsWon,
        Metric::AppendEntriesSent,
        Metric::AppendEntriesRejected,
        Metric::SnapshotInstalls,
        Metric::FailureDetectorDown,
        Metric::FailureDetectorUp,
        Metric::DataQuorumWritesAttempted,
        Metric::DataQuorumWritesSucceeded,
        Metric::DataQuorumWritesFailed,
        Metric::DataQuorumReadsAttempted,
        Metric::DataQuorumReadsSucceeded,
        Metric::DataQuorumReadsFailed,
        Metric::DataReadRepairTriggered,
        Metric::DataReadRepairKeysRepaired,
        Metric::DataHintsStored,
        Metric::DataHintsDelivered,
        Metric::DataAntiEntropyRounds,
    ];

    /// The stable exported name of this metric (snake_case, used as the text
    /// export key). Stable across versions — treat as part of the export surface.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Metric::ElectionsStarted => "control_elections_started",
            Metric::ElectionsWon => "control_elections_won",
            Metric::AppendEntriesSent => "control_append_entries_sent",
            Metric::AppendEntriesRejected => "control_append_entries_rejected",
            Metric::SnapshotInstalls => "control_snapshot_installs",
            Metric::FailureDetectorDown => "control_failure_detector_down",
            Metric::FailureDetectorUp => "control_failure_detector_up",
            Metric::DataQuorumWritesAttempted => "data_quorum_writes_attempted",
            Metric::DataQuorumWritesSucceeded => "data_quorum_writes_succeeded",
            Metric::DataQuorumWritesFailed => "data_quorum_writes_failed",
            Metric::DataQuorumReadsAttempted => "data_quorum_reads_attempted",
            Metric::DataQuorumReadsSucceeded => "data_quorum_reads_succeeded",
            Metric::DataQuorumReadsFailed => "data_quorum_reads_failed",
            Metric::DataReadRepairTriggered => "data_read_repair_triggered",
            Metric::DataReadRepairKeysRepaired => "data_read_repair_keys_repaired",
            Metric::DataHintsStored => "data_hints_stored",
            Metric::DataHintsDelivered => "data_hints_delivered",
            Metric::DataAntiEntropyRounds => "data_anti_entropy_rounds",
        }
    }

    /// This metric's fixed slot index in the [`MetricSink`] array.
    const fn slot(self) -> usize {
        self as usize
    }
}

/// The fixed-size, lock-free recording sink behind a [`MetricsHandle`]. Counters
/// are monotonic `u64`s; the single gauge is an `i64` (whether this node
/// believes it is leader). Recording is a relaxed atomic op, so it is cheap
/// enough to call on the hot path and introduces no ordering dependency that
/// could perturb determinism.
#[derive(Debug)]
pub struct MetricSink {
    counters: [AtomicU64; Metric::ALL.len()],
    /// A single gauge: whether this node currently believes it is leader (1) or
    /// not (0). A gauge (settable both ways) rather than a counter because
    /// leadership is a *level*, not an event count.
    is_leader: AtomicI64,
}

impl Default for MetricSink {
    fn default() -> Self {
        Self {
            // `AtomicU64` is not `Copy`, so build the array element-wise.
            counters: std::array::from_fn(|_| AtomicU64::new(0)),
            is_leader: AtomicI64::new(0),
        }
    }
}

impl MetricSink {
    /// Add `n` to `metric`'s counter (relaxed; recording needs no ordering).
    pub fn incr_by(&self, metric: Metric, n: u64) {
        self.counters[metric.slot()].fetch_add(n, Ordering::Relaxed);
    }

    /// Read `metric`'s current counter value.
    #[must_use]
    pub fn get(&self, metric: Metric) -> u64 {
        self.counters[metric.slot()].load(Ordering::Relaxed)
    }

    /// Set the leadership gauge (1 = this node believes it is leader, 0 = not).
    pub fn set_leader(&self, leader: bool) {
        self.is_leader.store(i64::from(leader), Ordering::Relaxed);
    }

    /// Read the leadership gauge.
    #[must_use]
    pub fn leader_gauge(&self) -> i64 {
        self.is_leader.load(Ordering::Relaxed)
    }

    /// Take a point-in-time snapshot of every counter and the gauge. A pure read
    /// of the atomics into a deterministically-ordered structure — safe to call
    /// concurrently with recording (each load is independent; the snapshot is not
    /// claimed to be a single atomic instant across metrics, which observability
    /// does not require).
    #[must_use]
    pub fn snapshot(&self) -> MetricSnapshot {
        let mut counters = BTreeMap::new();
        for m in Metric::ALL {
            counters.insert(m, self.get(m));
        }
        MetricSnapshot {
            counters,
            is_leader: self.leader_gauge(),
        }
    }
}

/// A cheap-to-clone handle onto a [`MetricSink`]. Components hold one and call
/// [`incr`](MetricsHandle::incr) / [`set_leader`](MetricsHandle::set_leader);
/// the owner (a `ProdEnv`, or a test) keeps the same handle to read it back. A
/// handle obtained from [`MetricsHandle::noop`] records into a shared throwaway
/// sink — recording is still valid (and free), it is simply never read.
#[derive(Clone, Debug)]
pub struct MetricsHandle {
    sink: Arc<MetricSink>,
}

impl Default for MetricsHandle {
    fn default() -> Self {
        Self::recording()
    }
}

impl MetricsHandle {
    /// A handle onto a fresh recording sink. The caller keeps the handle (clones
    /// share the one sink) and reads it back via [`snapshot`](Self::snapshot).
    #[must_use]
    pub fn recording() -> Self {
        Self {
            sink: Arc::new(MetricSink::default()),
        }
    }

    /// The process-wide no-op handle: a single shared sink that is recorded into
    /// but never read. This is what [`Env::metrics`](crate::Env::metrics)'s
    /// default returns, so an env that does not care about metrics costs nothing
    /// and changes no behavior. (It is a real sink, not a branch on every
    /// record, so the hot path has no `if metrics.is_some()` check.)
    #[must_use]
    pub fn noop() -> Self {
        use std::sync::OnceLock;
        static NOOP: OnceLock<MetricsHandle> = OnceLock::new();
        NOOP.get_or_init(MetricsHandle::recording).clone()
    }

    /// Increment `metric` by one.
    pub fn incr(&self, metric: Metric) {
        self.sink.incr_by(metric, 1);
    }

    /// Increment `metric` by `n`.
    pub fn incr_by(&self, metric: Metric, n: u64) {
        self.sink.incr_by(metric, n);
    }

    /// Set the leadership gauge.
    pub fn set_leader(&self, leader: bool) {
        self.sink.set_leader(leader);
    }

    /// Read one counter (mostly for tests).
    #[must_use]
    pub fn get(&self, metric: Metric) -> u64 {
        self.sink.get(metric)
    }

    /// Snapshot every metric for export or assertion.
    #[must_use]
    pub fn snapshot(&self) -> MetricSnapshot {
        self.sink.snapshot()
    }
}

/// An exported point-in-time view of all metrics, in deterministic order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricSnapshot {
    /// Every counter, keyed by [`Metric`] (a `BTreeMap`, so iteration order is
    /// stable).
    pub counters: BTreeMap<Metric, u64>,
    /// The leadership gauge (1 = leader, 0 = not).
    pub is_leader: i64,
}

impl MetricSnapshot {
    /// Render the snapshot as stable, line-oriented text — one `name value` pair
    /// per line, in `Metric::ALL` order, followed by the gauge. No timestamp (the
    /// export is timeless; a scrape adds its own), so the output is a pure
    /// function of the counter values and is byte-identical for equal snapshots.
    #[must_use]
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        for m in Metric::ALL {
            let v = self.counters.get(&m).copied().unwrap_or(0);
            out.push_str(m.name());
            out.push(' ');
            out.push_str(&v.to_string());
            out.push('\n');
        }
        out.push_str("control_is_leader ");
        out.push_str(&self.is_leader.to_string());
        out.push('\n');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incr_and_snapshot_round_trip() {
        let h = MetricsHandle::recording();
        h.incr(Metric::ElectionsStarted);
        h.incr(Metric::ElectionsStarted);
        h.incr_by(Metric::AppendEntriesSent, 5);
        h.set_leader(true);

        let snap = h.snapshot();
        assert_eq!(snap.counters[&Metric::ElectionsStarted], 2);
        assert_eq!(snap.counters[&Metric::AppendEntriesSent], 5);
        assert_eq!(snap.counters[&Metric::ElectionsWon], 0);
        assert_eq!(snap.is_leader, 1);
    }

    #[test]
    fn clones_share_one_sink() {
        let a = MetricsHandle::recording();
        let b = a.clone();
        a.incr(Metric::ElectionsWon);
        b.incr(Metric::ElectionsWon);
        assert_eq!(a.get(Metric::ElectionsWon), 2);
    }

    #[test]
    fn text_export_is_stable_and_ordered() {
        let h = MetricsHandle::recording();
        h.incr_by(Metric::SnapshotInstalls, 3);
        let text = h.snapshot().to_text();
        assert!(text.starts_with("control_elections_started 0\n"));
        assert!(text.contains("control_snapshot_installs 3\n"));
        assert!(text.ends_with("control_is_leader 0\n"));
        assert_eq!(text, h.snapshot().to_text());
    }

    #[test]
    fn slot_indices_match_all_order() {
        for (i, m) in Metric::ALL.iter().enumerate() {
            assert_eq!(m.slot(), i, "slot for {m:?} must match ALL index");
        }
    }
}
