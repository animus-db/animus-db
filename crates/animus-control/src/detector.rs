//! Heartbeat-based failure detection (ADR 0012).
//!
//! Member nodes emit periodic heartbeats; the control-plane leader observes them
//! and, when a member falls silent for longer than a timeout, marks it `Down` —
//! and `Active` again when its heartbeats resume. This is the automatic detection
//! that turns the existing placement auto-reconciliation (which already *reacts*
//! to `Down`, ADR 0005) into something that fires on real failures with no
//! operator.
//!
//! The **decision** is kept here as a pure, deterministic function of:
//! - each tracked member's last-heartbeat instant,
//! - the current instant `now`, and
//! - a fixed `timeout` threshold.
//!
//! A member is *alive* if a heartbeat arrived within the last `timeout`; once it
//! has been silent for longer it is *dead*. This is a plain interval+timeout
//! detector — simple and fully deterministic (no φ-accrual estimation, no
//! wall-clock, no randomness). The `RaftNode` driver (see [`crate::node`]) feeds
//! it `now` and observed heartbeats over the `Env` seam and, **when leader**,
//! proposes the resulting `UpsertMember{status}` transitions through Raft.
//!
//! Keeping the math here (and out of the consensus `RaftCore`) mirrors the
//! placement split (ADR 0005/0009): the decision is pure and unit-testable, while
//! the driver supplies only timing/IO.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_env::{Nanos, NodeId};

/// A pure, deterministic heartbeat failure detector.
///
/// Records the last instant a heartbeat was seen from each tracked member and,
/// given `now`, classifies each as alive (heartbeat within `timeout`) or dead.
/// It holds no clock and draws no randomness — every method is a pure function of
/// its arguments and recorded state, so the leader's verdict is reproducible.
#[derive(Clone, Debug)]
pub struct FailureDetector {
    /// Most recent heartbeat instant observed per member.
    last_seen: BTreeMap<NodeId, Nanos>,
    /// A member silent for longer than this is considered dead.
    timeout: Duration,
}

/// The detector's verdict for a single tracked member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Liveness {
    /// The member the verdict is about.
    pub node: NodeId,
    /// `true` if a heartbeat arrived within `timeout` of `now`.
    pub alive: bool,
}

impl FailureDetector {
    /// Create a detector with the given silence `timeout`. A member is judged
    /// dead once `now - last_heartbeat > timeout`.
    #[must_use]
    pub fn new(timeout: Duration) -> Self {
        Self {
            last_seen: BTreeMap::new(),
            timeout,
        }
    }

    /// Record a heartbeat from `node` observed at `now`. Starts tracking the
    /// member if it was not already tracked. Only ever moves a member's
    /// last-seen instant forward (a delayed/reordered heartbeat can't rewind it).
    pub fn observe(&mut self, node: NodeId, now: Nanos) {
        let slot = self.last_seen.entry(node).or_insert(now);
        if now.0 > slot.0 {
            *slot = now;
        }
    }

    /// Stop tracking `node` (e.g. it was removed from membership). Idempotent.
    pub fn forget(&mut self, node: NodeId) {
        self.last_seen.remove(&node);
    }

    /// Whether `node` is currently being tracked (has ever heartbeated).
    #[must_use]
    pub fn tracks(&self, node: NodeId) -> bool {
        self.last_seen.contains_key(&node)
    }

    /// The liveness verdict for `node` at `now`: alive iff a heartbeat was seen
    /// within `timeout`. An untracked member is reported dead (no evidence of
    /// life).
    #[must_use]
    pub fn is_alive(&self, node: NodeId, now: Nanos) -> bool {
        match self.last_seen.get(&node) {
            Some(last) => now.duration_since(*last) <= self.timeout,
            None => false,
        }
    }

    /// Classify every tracked member at `now`. Returned in ascending node-id
    /// order (the `BTreeMap` iteration order), so the sequence — and any
    /// proposals derived from it — is a deterministic function of the recorded
    /// heartbeats and `now`.
    #[must_use]
    pub fn evaluate(&self, now: Nanos) -> Vec<Liveness> {
        self.last_seen
            .iter()
            .map(|(&node, &last)| Liveness {
                node,
                alive: now.duration_since(last) <= self.timeout,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: Duration = Duration::from_millis(300);

    #[test]
    fn fresh_heartbeat_is_alive_and_stale_is_dead() {
        let mut d = FailureDetector::new(T);
        d.observe(10, Nanos(1_000));
        // Within the timeout window: alive.
        assert!(d.is_alive(10, Nanos(1_000 + 200_000_000)));
        // Exactly at the timeout boundary: still alive (<=).
        assert!(d.is_alive(10, Nanos(1_000 + 300_000_000)));
        // Past the timeout: dead.
        assert!(!d.is_alive(10, Nanos(1_000 + 300_000_001)));
    }

    #[test]
    fn untracked_member_is_dead() {
        let d = FailureDetector::new(T);
        assert!(!d.is_alive(99, Nanos(0)));
        assert!(!d.tracks(99));
    }

    #[test]
    fn observe_only_moves_forward() {
        let mut d = FailureDetector::new(T);
        d.observe(10, Nanos(5_000));
        // A reordered older heartbeat must not rewind last-seen.
        d.observe(10, Nanos(1_000));
        assert!(d.is_alive(10, Nanos(5_000 + 300_000_000)));
    }

    #[test]
    fn evaluate_is_sorted_and_classifies_each() {
        let mut d = FailureDetector::new(T);
        d.observe(12, Nanos(1_000));
        d.observe(10, Nanos(1_000));
        d.observe(11, Nanos(1_000_000_000)); // far in the future relative to others
        let now = Nanos(1_000 + 400_000_000);
        let v = d.evaluate(now);
        assert_eq!(
            v.iter().map(|l| l.node).collect::<Vec<_>>(),
            vec![10, 11, 12]
        );
        assert!(!v[0].alive); // 10: stale
        assert!(v[1].alive); // 11: fresh
        assert!(!v[2].alive); // 12: stale
    }

    #[test]
    fn forget_stops_tracking() {
        let mut d = FailureDetector::new(T);
        d.observe(10, Nanos(1_000));
        assert!(d.tracks(10));
        d.forget(10);
        assert!(!d.tracks(10));
    }
}
