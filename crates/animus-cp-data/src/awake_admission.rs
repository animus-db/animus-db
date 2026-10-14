//! The per-node **awake-group admission** (ADR 0079 layer 2): one instance per
//! physical node, shared by every [`crate::RaftKvNode`] the node hosts, that
//! bounds how many of the node's data-plane Raft groups are **awake** (not
//! quiesced, ADR 0048) at once, by deferring *bulk* wakes.
//!
//! A quiesced group costs memory only (about 0.011 core per 1000 replicas
//! measured); an awake one costs heartbeat and timer CPU (about 0.5 core per
//! 1000). A mass wake (the reconciler's proactive wake of every group touching
//! a newly `Down` node, ADR 0048 fork H) turns the cheap population into the
//! expensive one in a single tick, which is the ADR 0078 / #1297 overload.
//!
//! # Shape
//!
//! Synchronous and `Env`-seam clean, like [`crate::campaign_governor`]: no
//! tasks, time passed in as [`Nanos`], `BTreeMap`/`BTreeSet` throughout, no
//! entropy. The cap `A` is **soft**:
//!
//! - **Bulk wake** ([`AwakeAdmission::try_admit_wake`], the reconciler's
//!   `RaftKvNode::wake_bulk`): admitted iff `awake < A_eff` and the group is
//!   within the first `free` entries of the FIFO queue; otherwise queued under
//!   `(first_deferred_at, stream id)` and left **quiesced** (it keeps its
//!   leader belief; reads and writes route as before). When a slot frees (any
//!   group quiesces or leaves, or `A_eff` rises as lag falls) the first `free`
//!   waiters are woken through the callback they registered, so deferral costs
//!   no polling. The reconciler re-wakes a still-`Down` group every tick, which
//!   is the lost-wake backstop.
//! - **Everything else is admitted over the cap**: a client write, a peer
//!   message, a proactive edge wake, a leadership transfer or `campaign_now`
//!   campaign. They are one group per request, bounded by client admission
//!   (#1296), and refusing them would make a quiesced tablet unwritable on a
//!   busy node. The driver reports every quiesced-to-awake transition it
//!   observes ([`AwakeAdmission::report`]); one that was not an admitted bulk
//!   wake and lands the node past `A_eff` counts as `overcap`.
//! - **Hosting is exempt** ([`AwakeAdmission::register`], decision Q5): a
//!   freshly hosted group starts awake and counts toward `awake`, but is never
//!   refused (ADR 0078's staggered hosting bounds the ramp).
//!
//! `A_eff` is `A` reduced by the node's scheduling lag through the shared
//! [`crate::load_monitor::lag_divisor`] rule (decision Q9), so it is the same
//! reduction as the campaign governor's `K_eff`. Reducing `A_eff` below the
//! current awake count only stops admission; nothing is put to sleep.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use animus_env::{Metric, MetricsHandle, Nanos};

use crate::campaign_governor::CampaignWake;

/// Default `A` per configured core (ADR 0079 section 2, calibrated in step 1).
pub const DEFAULT_AWAKE_MAX_PER_CORE: usize = 1500;

/// The resolved awake cap `A` for a node: `awake_max_per_core * cores`, or
/// `None` (the cap is off) when `cores` is unset/zero or the per-core figure is
/// zero. `cores` is a *configured* number, never autodetected (ADR 0078
/// decision 2).
#[must_use]
pub fn resolve_awake_max(cores: Option<usize>, awake_max_per_core: usize) -> Option<usize> {
    let cores = cores?;
    let a = cores.saturating_mul(awake_max_per_core);
    (a > 0).then_some(a)
}

/// The outcome of [`AwakeAdmission::try_admit_wake`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum WakeAdmission {
    /// The group may un-quiesce now (its slot is reserved).
    Admitted,
    /// The group is queued and stays quiesced; its callback fires when a slot
    /// frees.
    Deferred,
}

/// Deliberate bugs for the Tier 1 negative controls (ADR 0079 controls (b)
/// and (c)). `None` in every real build; only the corpus constructs another
/// value.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum AwakeFault {
    /// The real, soft cap.
    #[default]
    None,
    /// A **hard** cap: a quiesced group with no slot refuses (drops) inbound
    /// peer messages and its own proposals' replication kick instead of
    /// waking over the cap. Acted on by the driver, see
    /// [`AwakeAdmission::hard_refuses`].
    HardCap,
    /// `A_eff` ignores the observed lag (no reduction).
    IgnoreLag,
    /// The lost-leader bypass is off: a group whose leader is on a `Down` node
    /// is deferred like any other bulk wake (negative control (d)).
    NoLostLeaderBypass,
}

struct State {
    /// `A` (at least 1).
    a: usize,
    /// The load monitor's smoothed lag (nanoseconds).
    lag: u64,
    fault: AwakeFault,
    /// Streams currently counted awake (reserved slots included).
    awake: BTreeSet<u64>,
    /// FIFO of deferred groups, keyed `(first_deferred_at, stream id)`.
    queue: BTreeMap<(Nanos, u64), CampaignWake>,
    /// `stream -> first_deferred_at`, the index into `queue`.
    queued: BTreeMap<u64, Nanos>,
    admitted: u64,
    deferred: u64,
    overcap: u64,
    awake_hwm: usize,
    queue_hwm: usize,
    /// Highest awake count right after a bulk admission (never above the
    /// `A_eff` of that moment: bulk wakes alone cannot pass the cap).
    admit_awake_hwm: usize,
}

impl State {
    fn a_eff(&self) -> usize {
        if self.fault == AwakeFault::IgnoreLag {
            self.a
        } else {
            crate::load_monitor::a_eff(self.a, self.lag)
        }
    }

    fn free(&self) -> usize {
        self.a_eff().saturating_sub(self.awake.len())
    }

    fn note_hwm(&mut self) {
        self.awake_hwm = self.awake_hwm.max(self.awake.len());
        self.queue_hwm = self.queue_hwm.max(self.queue.len());
    }

    /// Callbacks of the first `free` waiters (they are admitted if they ask
    /// now).
    fn wakeups(&self) -> Vec<CampaignWake> {
        self.queue
            .values()
            .take(self.free())
            .map(Arc::clone)
            .collect()
    }

    fn unqueue(&mut self, stream: u64) {
        if let Some(first) = self.queued.remove(&stream) {
            self.queue.remove(&(first, stream));
        }
    }
}

struct Inner {
    metrics: MetricsHandle,
    state: Mutex<State>,
}

impl Inner {
    fn publish(&self, st: &State) {
        self.metrics
            .set(Metric::CpAwakeReplicas, st.awake.len() as u64);
        self.metrics
            .set(Metric::CpAwakeMaxEffective, st.a_eff() as u64);
        self.metrics
            .set(Metric::CpWakeQueueDepth, st.queue.len() as u64);
    }
}

/// Counters and levels, for tests and `/admin`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AwakeStats {
    /// Groups counted awake now.
    pub awake: usize,
    /// Highest `awake` seen.
    pub awake_hwm: usize,
    /// `A`.
    pub awake_max: usize,
    /// `A_eff` now.
    pub awake_max_effective: usize,
    /// Groups queued now.
    pub queue_depth: usize,
    /// Highest queue depth seen.
    pub queue_hwm: usize,
    /// Bulk wakes admitted.
    pub admitted: u64,
    /// Highest awake count right after a bulk admission: at most `A_eff` of
    /// that moment, whatever else woke over the cap.
    pub admit_awake_hwm: usize,
    /// Bulk wakes deferred (counted once per group, at the first refusal).
    pub deferred: u64,
    /// Groups that left quiescence over the cap.
    pub overcap: u64,
}

/// RAII membership of one group in the node's awake population (see
/// [`AwakeAdmission::membership`]).
pub struct AwakeMembership {
    admission: AwakeAdmission,
    stream: u64,
}

impl Drop for AwakeMembership {
    fn drop(&mut self) {
        self.admission.deregister(self.stream);
    }
}

/// The per-node awake-group admission. Cloning is a cheap `Arc` bump; every
/// clone shares one population.
#[derive(Clone)]
pub struct AwakeAdmission {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for AwakeAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwakeAdmission")
            .field("stats", &self.stats())
            .finish()
    }
}

impl AwakeAdmission {
    /// An admission with cap `awake_max` (`A`, at least 1); records into
    /// `metrics`. Callers do not construct one when the cap is off
    /// ([`resolve_awake_max`] returned `None`).
    #[must_use]
    pub fn new(awake_max: usize, metrics: MetricsHandle) -> Self {
        Self::with_fault(awake_max, metrics, AwakeFault::None)
    }

    /// [`new`](Self::new) with a deliberate bug, for the negative controls
    /// only (see [`AwakeFault`]).
    #[doc(hidden)]
    #[must_use]
    pub fn with_fault(awake_max: usize, metrics: MetricsHandle, fault: AwakeFault) -> Self {
        Self {
            inner: Arc::new(Inner {
                metrics,
                state: Mutex::new(State {
                    a: awake_max.max(1),
                    lag: 0,
                    fault,
                    awake: BTreeSet::new(),
                    queue: BTreeMap::new(),
                    queued: BTreeMap::new(),
                    admitted: 0,
                    deferred: 0,
                    overcap: 0,
                    awake_hwm: 0,
                    queue_hwm: 0,
                    admit_awake_hwm: 0,
                }),
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner.state.lock().expect("awake admission poisoned")
    }

    /// Feed the load monitor's smoothed lag (nanoseconds): `A_eff` follows the
    /// shared lag rule. When it rises and slots open, the head of the queue is
    /// woken. Groups already awake are never put to sleep.
    pub fn set_lag(&self, lag_nanos: u64) {
        let wakes = {
            let mut st = self.lock();
            let before = st.a_eff();
            st.lag = lag_nanos;
            self.inner.publish(&st);
            if st.a_eff() > before {
                st.wakeups()
            } else {
                Vec::new()
            }
        };
        for w in wakes {
            w();
        }
    }

    /// `A_eff` now.
    #[must_use]
    pub fn a_eff(&self) -> usize {
        self.lock().a_eff()
    }

    /// Register a hosted group (starts awake, **exempt**: never refused and not
    /// counted as overcap). Idempotent.
    pub fn register(&self, stream: u64) {
        let mut st = self.lock();
        st.awake.insert(stream);
        st.note_hwm();
        self.inner.publish(&st);
    }

    /// [`register`](Self::register) the group and return a guard that
    /// [`deregister`](Self::deregister)s it on drop, so every exit path of the
    /// driver (halt, a dropped task) returns the slot.
    pub fn membership(&self, stream: u64) -> AwakeMembership {
        self.register(stream);
        AwakeMembership {
            admission: self.clone(),
            stream,
        }
    }

    /// The group is gone (shutdown, release): drop it from the awake set and
    /// the queue, and pass its slot to the head of the queue.
    pub fn deregister(&self, stream: u64) {
        let wakes = {
            let mut st = self.lock();
            let was_awake = st.awake.remove(&stream);
            st.unqueue(stream);
            self.inner.publish(&st);
            if was_awake { st.wakeups() } else { Vec::new() }
        };
        for w in wakes {
            w();
        }
    }

    /// The driver's observation of group `stream`'s quiescence after a step.
    /// A transition to quiesced frees a slot (waking the queue head); a
    /// transition to awake that was not an admitted bulk wake is a soft-cap
    /// wake: it is counted as `overcap` when it leaves the node past `A_eff`.
    /// Idempotent for an unchanged state.
    pub fn report(&self, stream: u64, quiesced: bool) {
        let wakes = {
            let mut st = self.lock();
            if quiesced {
                if !st.awake.remove(&stream) {
                    return;
                }
                self.inner.publish(&st);
                st.wakeups()
            } else {
                if st.awake.contains(&stream) {
                    return;
                }
                if st.fault == AwakeFault::HardCap && st.free() == 0 {
                    // Negative control (b): the hard cap never lets a group
                    // that woke over the cap count as awake, so it keeps
                    // refusing it (see `hard_refuses`).
                    return;
                }
                st.awake.insert(stream);
                st.unqueue(stream);
                st.note_hwm();
                if st.awake.len() > st.a_eff() {
                    st.overcap += 1;
                    self.inner.metrics.incr(Metric::CpWakeOvercap);
                }
                self.inner.publish(&st);
                Vec::new()
            }
        };
        for w in wakes {
            w();
        }
    }

    /// A bulk wake (the reconciler's proactive `Down` wake) of quiesced group
    /// `stream` at `now`.
    ///
    /// Admits iff a slot is free **and** the group is within the first `free`
    /// entries of the FIFO queue (a group not yet queued is behind everyone
    /// who is), reserving the slot at once so a herd of concurrent asks cannot
    /// overshoot. Otherwise queues the group under `(now, stream)` on its first
    /// refusal and keeps its original ticket on later asks; `wake` is called
    /// when a slot frees while the group is near the head.
    pub fn try_admit_wake(&self, stream: u64, now: Nanos, wake: &CampaignWake) -> WakeAdmission {
        {
            let mut st = self.lock();
            if st.awake.contains(&stream) {
                return WakeAdmission::Admitted;
            }
            let free = st.free();
            let admit = match st.queued.get(&stream) {
                Some(&first) => st
                    .queue
                    .keys()
                    .take(free)
                    .any(|&(t, s)| t == first && s == stream),
                None => st.queue.len() < free,
            };
            if admit {
                st.unqueue(stream);
                st.awake.insert(stream);
                st.admitted += 1;
                st.admit_awake_hwm = st.admit_awake_hwm.max(st.awake.len());
                st.note_hwm();
                self.inner.publish(&st);
                WakeAdmission::Admitted
            } else {
                match st.queued.get(&stream).copied() {
                    Some(first) => {
                        st.queue.insert((first, stream), Arc::clone(wake));
                    }
                    None => {
                        st.queued.insert(stream, now);
                        st.queue.insert((now, stream), Arc::clone(wake));
                        st.deferred += 1;
                        st.note_hwm();
                        self.inner.metrics.incr(Metric::CpWakeDeferred);
                    }
                }
                self.inner.publish(&st);
                WakeAdmission::Deferred
            }
        }
    }

    /// The `HardCap` negative control only: whether a hard cap would refuse a
    /// quiesced group's inbound work now (it holds no slot and none is free).
    /// Always `false` for the real, soft cap.
    #[doc(hidden)]
    #[must_use]
    pub fn hard_refuses(&self, stream: u64) -> bool {
        let st = self.lock();
        st.fault == AwakeFault::HardCap && !st.awake.contains(&stream) && st.free() == 0
    }

    /// Whether a bulk wake of a group with a `Down` leader bypasses the cap
    /// (true except under the `NoLostLeaderBypass` negative control).
    #[must_use]
    pub fn lost_leader_bypass(&self) -> bool {
        self.lock().fault != AwakeFault::NoLostLeaderBypass
    }

    /// Whether this admission is running the `HardCap` negative control.
    #[doc(hidden)]
    #[must_use]
    pub fn is_hard_cap(&self) -> bool {
        self.lock().fault == AwakeFault::HardCap
    }

    /// Counters and levels now.
    #[must_use]
    pub fn stats(&self) -> AwakeStats {
        let st = self.lock();
        AwakeStats {
            awake: st.awake.len(),
            awake_hwm: st.awake_hwm,
            awake_max: st.a,
            awake_max_effective: st.a_eff(),
            queue_depth: st.queue.len(),
            queue_hwm: st.queue_hwm,
            admitted: st.admitted,
            admit_awake_hwm: st.admit_awake_hwm,
            deferred: st.deferred,
            overcap: st.overcap,
        }
    }

    /// Reset the high-water marks to the current levels (a corpus cell that
    /// measures the burst after its set-up).
    #[doc(hidden)]
    pub fn reset_hwm(&self) {
        let mut st = self.lock();
        st.awake_hwm = st.awake.len();
        st.queue_hwm = st.queue.len();
        st.admit_awake_hwm = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn adm(a: usize) -> AwakeAdmission {
        AwakeAdmission::new(a, MetricsHandle::recording())
    }

    fn counter_wake() -> (CampaignWake, Arc<AtomicU64>) {
        let n = Arc::new(AtomicU64::new(0));
        let n2 = Arc::clone(&n);
        (
            Arc::new(move || {
                n2.fetch_add(1, Ordering::SeqCst);
            }),
            n,
        )
    }

    #[test]
    fn resolve_needs_configured_cores_and_nonzero_per_core() {
        assert_eq!(resolve_awake_max(None, 1500), None);
        assert_eq!(resolve_awake_max(Some(0), 1500), None);
        assert_eq!(resolve_awake_max(Some(4), 0), None);
        assert_eq!(resolve_awake_max(Some(4), 1500), Some(6000));
    }

    #[test]
    fn admits_until_the_cap_then_defers_fifo() {
        let a = adm(2);
        let (w, n) = counter_wake();
        assert_eq!(a.try_admit_wake(1, Nanos(1), &w), WakeAdmission::Admitted);
        assert_eq!(a.try_admit_wake(2, Nanos(2), &w), WakeAdmission::Admitted);
        assert_eq!(a.try_admit_wake(3, Nanos(3), &w), WakeAdmission::Deferred);
        assert_eq!(a.try_admit_wake(4, Nanos(4), &w), WakeAdmission::Deferred);
        let s = a.stats();
        assert_eq!((s.awake, s.queue_depth, s.deferred), (2, 2, 2));
        assert_eq!(s.admit_awake_hwm, 2);
        // A re-ask does not re-count and keeps its ticket.
        assert_eq!(a.try_admit_wake(3, Nanos(9), &w), WakeAdmission::Deferred);
        assert_eq!(a.stats().deferred, 2);
        // A slot frees: exactly the head is woken, and only it is admitted.
        a.report(1, true);
        assert_eq!(n.load(Ordering::SeqCst), 1);
        assert_eq!(a.try_admit_wake(4, Nanos(10), &w), WakeAdmission::Deferred);
        assert_eq!(a.try_admit_wake(3, Nanos(11), &w), WakeAdmission::Admitted);
        assert_eq!(a.stats().queue_depth, 1);
    }

    #[test]
    fn a_new_ask_cannot_jump_the_queue() {
        let a = adm(1);
        let (w, _) = counter_wake();
        a.register(1);
        assert_eq!(a.try_admit_wake(2, Nanos(1), &w), WakeAdmission::Deferred);
        a.report(1, true);
        // Slot is free but group 2 holds the head ticket.
        assert_eq!(a.try_admit_wake(3, Nanos(2), &w), WakeAdmission::Deferred);
        assert_eq!(a.try_admit_wake(2, Nanos(3), &w), WakeAdmission::Admitted);
    }

    #[test]
    fn hosting_and_writes_exceed_the_cap_and_only_writes_count() {
        let m = MetricsHandle::recording();
        let a = AwakeAdmission::new(1, m.clone());
        a.register(1);
        a.register(2);
        assert_eq!(a.stats().awake, 2);
        assert_eq!(a.stats().overcap, 0, "hosting is exempt");
        a.report(1, true);
        a.report(2, true);
        // A write wakes a group with no slot taken from the queue.
        a.report(3, false);
        assert_eq!(a.stats().overcap, 0, "within the cap");
        a.report(4, false);
        assert_eq!(a.stats().overcap, 1);
        assert_eq!(m.get(Metric::CpWakeOvercap), 1);
        // An admitted bulk wake is never overcap even when it fills the cap.
        a.report(3, true);
        a.report(4, true);
        let (w, _) = counter_wake();
        assert_eq!(a.try_admit_wake(5, Nanos(1), &w), WakeAdmission::Admitted);
        a.report(5, false);
        assert_eq!(a.stats().overcap, 1);
    }

    #[test]
    fn a_woken_group_leaves_the_queue() {
        let a = adm(1);
        let (w, _) = counter_wake();
        a.register(1);
        assert_eq!(a.try_admit_wake(2, Nanos(1), &w), WakeAdmission::Deferred);
        // The queued group is woken by a client write instead.
        a.report(2, false);
        assert_eq!(a.stats().queue_depth, 0);
        a.deregister(2);
        a.deregister(1);
        assert_eq!(a.stats().awake, 0);
    }

    #[test]
    fn lag_reduces_a_eff_and_recovery_wakes_the_head() {
        let m = MetricsHandle::recording();
        let a = AwakeAdmission::new(8, m.clone());
        let (w, n) = counter_wake();
        for s in 0..4 {
            a.register(s);
        }
        a.set_lag(120_000_000);
        assert_eq!(a.a_eff(), 2);
        assert_eq!(m.get(Metric::CpAwakeMaxEffective), 2);
        assert_eq!(a.try_admit_wake(10, Nanos(1), &w), WakeAdmission::Deferred);
        assert_eq!(n.load(Ordering::SeqCst), 0);
        a.set_lag(0);
        assert_eq!(a.a_eff(), 8);
        assert_eq!(n.load(Ordering::SeqCst), 1, "the queue head is woken");
        assert_eq!(a.try_admit_wake(10, Nanos(2), &w), WakeAdmission::Admitted);
    }

    #[test]
    fn ignore_lag_fault_keeps_the_full_cap() {
        let a = AwakeAdmission::with_fault(8, MetricsHandle::recording(), AwakeFault::IgnoreLag);
        a.set_lag(500_000_000);
        assert_eq!(a.a_eff(), 8);
    }

    #[test]
    fn the_queue_is_woken_in_fifo_order_for_exactly_the_free_slots() {
        let a = adm(3);
        let order = Arc::new(Mutex::new(Vec::new()));
        let mk = |id: u64| -> CampaignWake {
            let o = Arc::clone(&order);
            Arc::new(move || o.lock().expect("order").push(id))
        };
        for s in 0..3 {
            a.register(s);
        }
        for (i, s) in [10u64, 11, 12, 13].into_iter().enumerate() {
            let t = Nanos(i as u64 + 1);
            assert_eq!(a.try_admit_wake(s, t, &mk(s)), WakeAdmission::Deferred);
        }
        a.report(0, true);
        a.report(1, true);
        // Each quiesce wakes the first `free` waiters: 1, then 2.
        assert_eq!(*order.lock().expect("order"), vec![10, 10, 11]);
    }
}
