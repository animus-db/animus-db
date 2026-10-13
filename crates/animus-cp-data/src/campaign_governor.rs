//! The per-node **campaign governor** (ADR 0078 mechanism 1, election
//! backpressure): one instance per physical node, shared by every
//! [`crate::RaftKvNode`] the node hosts, that caps how many of the node's Raft
//! groups may be in a **campaign attempt** (pre-vote through election) at once.
//!
//! A node hosts one Raft group per tablet replica, and every group decides alone
//! that its leader is gone. When the node is late (after a restart, a mass wake,
//! or plain overload) many timers lapse together; the campaigns they start cost
//! more than the heartbeats they replace, which makes the node later (issue
//! #1199). A concurrency cap self-scales with how long campaigns actually take
//! (Little's law), which is exactly the signal that was missing.
//!
//! # Shape
//!
//! Synchronous and `Env`-seam clean: no tasks of its own, time passed in as
//! [`Nanos`], `BTreeMap` throughout, no entropy. The driver
//! (`RaftKvNode`'s consensus loop) asks [`CampaignGovernor::try_admit`] when a
//! group's election timer has lapsed ([`RaftCore::campaign_due`]) and holds the
//! returned [`CampaignPermit`] for the life of the attempt; dropping it returns
//! the slot (RAII, so every exit path releases: leader, stepped down, shutdown,
//! the group's task dropped).
//!
//! - **FIFO.** A refused group is queued under `(first_deferred_at, stream id)`.
//!   A free slot goes to the head of the queue; a permit returned wakes the
//!   first `free` waiters through the callback they registered (the group's
//!   own `WakeSignal`), so deferral costs no polling. As a backstop against a
//!   lost wake the core re-checks at its own `election_base`
//!   ([`RaftCore::tick_gated`]'s `campaign_hold`).
//! - **A failed attempt re-queues at the back.** The driver drops the permit
//!   when the attempt's own election deadline lapses and asks again; the ask
//!   gets a fresh ticket, so a group that cannot win does not pin a permit
//!   ahead of groups that have not had a turn.
//! - **Leak backstop.** A permit older than its `ttl` (the caller passes
//!   `2 * election_base_max` of the holder's profile) is expired lazily on the
//!   next `try_admit`, so a lost drop cannot shrink the pool forever. A queued
//!   waiter that has not re-checked within its `ttl` is expired the same way (a
//!   group whose task vanished).
//! - **Exempt campaigns** ([`CampaignGovernor::acquire_exempt`]): `TimeoutNow`
//!   (leadership transfer, the preferred-leader step) and `campaign_now` (split
//!   child) take a permit if one is free and **proceed over the cap if not**;
//!   either way they count in the in-flight gauge. They are one per group and
//!   driven by a bounded decision, so they cannot form a storm.
//!
//! The governor only ever delays *when* a node may start a campaign. It never
//! changes who may win an election, what a vote means or what a term is; the
//! responder side (granting pre-votes and votes) is deliberately not gated, see
//! `RaftCore::tick_gated` for the deadlock that would cause.
//!
//! [`RaftCore::campaign_due`]: animus_control::raft::RaftCore::campaign_due
//! [`RaftCore::tick_gated`]: animus_control::raft::RaftCore::tick_gated

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Metric, MetricsHandle, Nanos};

use crate::load_monitor::LoadMonitor;

/// Default `K`, the in-flight cap (ADR 0078 "Maintainer decisions": 32).
pub const DEFAULT_CAMPAIGN_MAX_INFLIGHT: usize = 32;

/// A wake callback the governor invokes (outside its lock) when a queued group
/// may now be admitted. The driver passes a closure raising its group's
/// `WakeSignal`.
pub type CampaignWake = Arc<dyn Fn() + Send + Sync>;

/// The outcome of [`CampaignGovernor::try_admit`].
#[must_use]
pub enum Admission {
    /// The group may campaign; hold the permit for the life of the attempt.
    Admitted(CampaignPermit),
    /// The group is queued; it is woken through its callback when a slot frees
    /// (and re-checks on its own `election_base` regardless).
    Deferred,
}

/// Deliberate bugs for the Tier 1 negative controls (ADR 0078 controls (c) and
/// (d)). `None` in every real build; only the corpus constructs another value
/// (through [`CampaignGovernor::with_fault`]).
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum GovernorFault {
    /// The real governor.
    #[default]
    None,
    /// No RAII release: a dropped permit is only reclaimed by the lazy expiry.
    LeakPermits,
    /// The queue is served newest-first (a failed attempt that re-asks jumps
    /// every older waiter).
    Lifo,
    /// The driver re-arms a deferred group's election timer, so the group stops
    /// granting pre-votes to admitted campaigners (admission gating the
    /// responder side too). Acted on by the driver, see `gated_tick`.
    RearmOnDefer,
}

struct PermitRec {
    stream: u64,
    exempt: bool,
    expires_at: Nanos,
}

struct Waiter {
    wake: CampaignWake,
    /// Last time the group asked (a fresh `try_admit`); a waiter silent for
    /// `ttl` is presumed gone.
    last_seen: Nanos,
    ttl: Duration,
}

struct State {
    /// `K`; the effective cap is [`State::k_eff`].
    k: usize,
    /// The load monitor's smoothed lag (nanoseconds); drives `K_eff`.
    lag: u64,
    fault: GovernorFault,
    next_id: u64,
    permits: BTreeMap<u64, PermitRec>,
    /// FIFO of waiting groups, keyed `(first_deferred_at, stream id)`.
    queue: BTreeMap<(Nanos, u64), Waiter>,
    /// `stream -> first_deferred_at`, the index into `queue`.
    queued: BTreeMap<u64, Nanos>,
    admitted: u64,
    deferred: u64,
    expired_permits: u64,
    hwm: usize,
    nonexempt_hwm: usize,
}

impl State {
    fn k_eff(&self) -> usize {
        crate::load_monitor::k_eff(self.k, self.lag)
    }

    fn free(&self) -> usize {
        self.k_eff().saturating_sub(self.permits.len())
    }

    fn nonexempt(&self) -> usize {
        self.permits.values().filter(|p| !p.exempt).count()
    }

    /// Drop permits and waiters whose backstop has passed.
    fn expire(&mut self, now: Nanos, keep_stream: u64) {
        let before = self.permits.len();
        self.permits.retain(|_, p| p.expires_at > now);
        self.expired_permits += (before - self.permits.len()) as u64;
        let gone: Vec<(Nanos, u64)> = self
            .queue
            .iter()
            .filter(|((_, stream), w)| {
                *stream != keep_stream && w.last_seen.saturating_add(w.ttl) <= now
            })
            .map(|(k, _)| *k)
            .collect();
        for k in gone {
            self.queue.remove(&k);
            self.queued.remove(&k.1);
        }
    }

    /// Wake callbacks of the first `free` waiters (they will be admitted if
    /// they ask now).
    fn wakeups(&self) -> Vec<CampaignWake> {
        let free = self.free();
        if self.fault == GovernorFault::Lifo {
            return self
                .queue
                .values()
                .rev()
                .take(free)
                .map(|w| Arc::clone(&w.wake))
                .collect();
        }
        self.queue
            .values()
            .take(free)
            .map(|w| Arc::clone(&w.wake))
            .collect()
    }

    /// Whether `(first, stream)` is among the groups the next `free` slots
    /// serve.
    fn within_head(&self, first: Nanos, stream: u64, free: usize) -> bool {
        if self.fault == GovernorFault::Lifo {
            return self
                .queue
                .keys()
                .rev()
                .take(free)
                .any(|&(t, s)| t == first && s == stream);
        }
        self.queue
            .keys()
            .take(free)
            .any(|&(t, s)| t == first && s == stream)
    }

    fn note_hwm(&mut self) {
        self.hwm = self.hwm.max(self.permits.len());
        self.nonexempt_hwm = self.nonexempt_hwm.max(self.nonexempt());
    }
}

struct Inner {
    metrics: MetricsHandle,
    state: Mutex<State>,
}

impl Inner {
    fn publish(&self, st: &State) {
        self.metrics
            .set(Metric::CpCampaignInflight, st.permits.len() as u64);
        self.metrics
            .set(Metric::CpCampaignInflightHwm, st.hwm as u64);
        self.metrics
            .set(Metric::CpCampaignQueueDepth, st.queue.len() as u64);
    }

    fn release(&self, id: u64) {
        let wakes = {
            let mut st = self.state.lock().expect("campaign governor poisoned");
            if st.fault == GovernorFault::LeakPermits {
                // Negative control: the drop does not return the slot.
                return;
            }
            if st.permits.remove(&id).is_none() {
                // Already expired by the leak backstop; nothing to return.
                return;
            }
            self.publish(&st);
            st.wakeups()
        };
        for w in wakes {
            w();
        }
    }
}

/// Counters and levels, for tests and `/admin`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GovernorStats {
    /// Permits outstanding now (exempt included).
    pub inflight: usize,
    /// Groups queued now.
    pub queue_depth: usize,
    /// Highest `inflight` seen.
    pub inflight_hwm: usize,
    /// Highest number of **non-exempt** permits seen at once; never above `K`.
    pub nonexempt_hwm: usize,
    /// Attempts admitted (immediately or from the queue).
    pub admitted: u64,
    /// Attempts deferred (counted at the first refusal).
    pub deferred: u64,
    /// Permits reclaimed by the leak backstop.
    pub expired_permits: u64,
}

/// The per-node campaign governor. Cloning is a cheap `Arc` bump; every clone
/// shares one pool.
#[derive(Clone)]
pub struct CampaignGovernor {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for CampaignGovernor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CampaignGovernor")
            .field("stats", &self.stats())
            .finish()
    }
}

impl CampaignGovernor {
    /// A governor admitting at most `max_inflight` (`K`, at least 1) timer-
    /// driven campaigns at once; records into `metrics`. `K == 0` means
    /// "disabled" and callers do not construct one (`animusd` passes `None`).
    #[must_use]
    pub fn new(max_inflight: usize, metrics: MetricsHandle) -> Self {
        Self::with_fault(max_inflight, metrics, GovernorFault::None)
    }

    /// [`new`](Self::new) with a deliberate bug, for the negative controls
    /// only (see [`GovernorFault`]).
    #[doc(hidden)]
    #[must_use]
    pub fn with_fault(max_inflight: usize, metrics: MetricsHandle, fault: GovernorFault) -> Self {
        Self {
            inner: Arc::new(Inner {
                metrics,
                state: Mutex::new(State {
                    k: max_inflight.max(1),
                    lag: 0,
                    fault,
                    next_id: 1,
                    permits: BTreeMap::new(),
                    queue: BTreeMap::new(),
                    queued: BTreeMap::new(),
                    admitted: 0,
                    deferred: 0,
                    expired_permits: 0,
                    hwm: 0,
                    nonexempt_hwm: 0,
                }),
            }),
        }
    }

    /// Feed the load monitor's smoothed lag (nanoseconds, ADR 0078 step 4):
    /// `K_eff` is `K` below 30 ms, `K/2` from 30 ms and `K/4` from 100 ms. When
    /// the lag falls and slots open, the head of the queue is woken. Permits
    /// already held are never revoked.
    pub fn set_lag(&self, lag_nanos: u64) {
        let wakes = {
            let mut st = self.inner.state.lock().expect("campaign governor poisoned");
            let before = st.k_eff();
            st.lag = lag_nanos;
            if st.k_eff() > before {
                st.wakeups()
            } else {
                Vec::new()
            }
        };
        for w in wakes {
            w();
        }
    }

    /// The effective cap now (`K` reduced by the observed lag).
    #[must_use]
    pub fn k_eff(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("campaign governor poisoned")
            .k_eff()
    }

    /// Ask for a campaign permit for group `stream` at `now`.
    ///
    /// Admits iff a slot is free **and** the group is within the first `free`
    /// entries of the FIFO queue (a group not yet queued is behind everyone
    /// who is). Otherwise queues the group under `(now, stream)` on its first
    /// refusal and keeps its original ticket on later asks. `ttl` is the
    /// holder's leak backstop (`2 * election_base_max`); `wake` is called when
    /// a slot frees while the group is near the head.
    pub fn try_admit(
        &self,
        stream: u64,
        now: Nanos,
        ttl: Duration,
        wake: &CampaignWake,
    ) -> Admission {
        let (outcome, wakes) = {
            let mut st = self.inner.state.lock().expect("campaign governor poisoned");
            st.expire(now, stream);
            let free = st.free();
            let admit = match st.queued.get(&stream) {
                Some(&first) => st.within_head(first, stream, free),
                None => st.queue.len() < free,
            };
            let outcome = if admit {
                if let Some(first) = st.queued.remove(&stream) {
                    st.queue.remove(&(first, stream));
                }
                let id = st.next_id;
                st.next_id += 1;
                st.permits.insert(
                    id,
                    PermitRec {
                        stream,
                        exempt: false,
                        expires_at: now.saturating_add(ttl),
                    },
                );
                st.admitted += 1;
                st.note_hwm();
                self.inner.metrics.incr(Metric::CpCampaignAdmitted);
                Some(id)
            } else {
                match st.queued.get(&stream).copied() {
                    Some(first) => {
                        if let Some(w) = st.queue.get_mut(&(first, stream)) {
                            w.last_seen = now;
                            w.ttl = ttl;
                            w.wake = Arc::clone(wake);
                        }
                    }
                    None => {
                        st.queued.insert(stream, now);
                        st.queue.insert(
                            (now, stream),
                            Waiter {
                                wake: Arc::clone(wake),
                                last_seen: now,
                                ttl,
                            },
                        );
                        st.deferred += 1;
                        self.inner.metrics.incr(Metric::CpCampaignDeferred);
                    }
                }
                None
            };
            self.inner.publish(&st);
            // Expiry may have freed slots for waiters other than the caller.
            let wakes = if outcome.is_some() {
                Vec::new()
            } else {
                st.wakeups()
            };
            (outcome, wakes)
        };
        for w in wakes {
            w();
        }
        match outcome {
            Some(id) => Admission::Admitted(CampaignPermit {
                inner: Arc::clone(&self.inner),
                id,
            }),
            None => Admission::Deferred,
        }
    }

    /// A permit for an **exempt** campaign (`TimeoutNow`, `campaign_now`): always
    /// granted, over the cap if need be, and counted in the in-flight gauge.
    /// Also removes `stream` from the queue (it no longer waits).
    pub fn acquire_exempt(&self, stream: u64, now: Nanos, ttl: Duration) -> CampaignPermit {
        let id = {
            let mut st = self.inner.state.lock().expect("campaign governor poisoned");
            st.expire(now, stream);
            if let Some(first) = st.queued.remove(&stream) {
                st.queue.remove(&(first, stream));
            }
            let id = st.next_id;
            st.next_id += 1;
            st.permits.insert(
                id,
                PermitRec {
                    stream,
                    exempt: true,
                    expires_at: now.saturating_add(ttl),
                },
            );
            st.note_hwm();
            self.inner.publish(&st);
            id
        };
        CampaignPermit {
            inner: Arc::clone(&self.inner),
            id,
        }
    }

    /// `stream` no longer needs a permit (it heard a leader): drop its queue
    /// entry so it cannot block the head. Returns whether it was queued.
    pub fn cancel(&self, stream: u64) -> bool {
        let wakes = {
            let mut st = self.inner.state.lock().expect("campaign governor poisoned");
            let Some(first) = st.queued.remove(&stream) else {
                return false;
            };
            st.queue.remove(&(first, stream));
            self.inner.publish(&st);
            st.wakeups()
        };
        for w in wakes {
            w();
        }
        true
    }

    /// Counters and levels.
    #[must_use]
    pub fn stats(&self) -> GovernorStats {
        let st = self.inner.state.lock().expect("campaign governor poisoned");
        GovernorStats {
            inflight: st.permits.len(),
            queue_depth: st.queue.len(),
            inflight_hwm: st.hwm,
            nonexempt_hwm: st.nonexempt_hwm,
            admitted: st.admitted,
            deferred: st.deferred,
            expired_permits: st.expired_permits,
        }
    }

    /// The deliberate bug this governor was built with (`None` in production).
    #[doc(hidden)]
    #[must_use]
    pub fn fault(&self) -> GovernorFault {
        self.inner
            .state
            .lock()
            .expect("campaign governor poisoned")
            .fault
    }

    /// The configured cap `K`.
    #[must_use]
    pub fn max_inflight(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("campaign governor poisoned")
            .k
    }

    /// Streams currently holding a permit, ascending (diagnostics).
    #[must_use]
    pub fn holders(&self) -> Vec<u64> {
        let st = self.inner.state.lock().expect("campaign governor poisoned");
        let mut v: Vec<u64> = st.permits.values().map(|p| p.stream).collect();
        v.sort_unstable();
        v
    }
}

/// An admitted campaign attempt's slot. Dropping it returns the slot to the
/// governor and wakes the next waiters; this is the only release path, so
/// every exit of the holder (win, step down, shutdown, task drop) is covered.
#[must_use]
pub struct CampaignPermit {
    inner: Arc<Inner>,
    id: u64,
}

impl Drop for CampaignPermit {
    fn drop(&mut self) {
        self.inner.release(self.id);
    }
}

/// The per-node election-backpressure handles (ADR 0078), bundled so the
/// start-up plumbing carries one value. `Default` is "everything off", which
/// is byte-for-byte the pre-ADR behavior.
#[derive(Clone, Default)]
pub struct ElectionBackpressure {
    /// Campaign admission (mechanism 1); `None` = disabled.
    pub governor: Option<CampaignGovernor>,
    /// Scheduling-lag monitor (mechanism 2): stretches follower election
    /// timeouts and reduces the governor's `K_eff`; `None` = disabled.
    pub monitor: Option<LoadMonitor>,
    /// Awake-group admission (ADR 0079 layer 2): the node's soft cap on awake
    /// replicas and its wake queue; `None` = disabled (`--cores` unset).
    /// Lives here because it rides the same per-node plumbing and the same
    /// `LoadMonitor` lag.
    pub awake: Option<crate::awake_admission::AwakeAdmission>,
    /// Background-work budget (ADR 0079 layer 3): the node's permit pools for
    /// snapshot streams, learner catch-up, splits and compactions; `None` =
    /// every pool off. Its effective sizes follow the same `LoadMonitor` lag.
    pub budget: Option<crate::background_budget::BackgroundBudget>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    const TTL: Duration = Duration::from_secs(1);

    fn gov(k: usize) -> CampaignGovernor {
        CampaignGovernor::new(k, MetricsHandle::recording())
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

    fn admitted(a: Admission) -> CampaignPermit {
        match a {
            Admission::Admitted(p) => p,
            Admission::Deferred => panic!("expected admission"),
        }
    }

    #[test]
    fn admits_up_to_the_cap_and_defers_the_rest() {
        let g = gov(2);
        let (w, _) = counter_wake();
        let a = admitted(g.try_admit(1, Nanos(10), TTL, &w));
        let _b = admitted(g.try_admit(2, Nanos(11), TTL, &w));
        assert!(matches!(
            g.try_admit(3, Nanos(12), TTL, &w),
            Admission::Deferred
        ));
        assert_eq!(g.stats().inflight, 2);
        assert_eq!(g.stats().queue_depth, 1);
        drop(a);
        assert_eq!(g.stats().inflight, 1);
    }

    #[test]
    fn dropping_a_permit_returns_the_slot_and_wakes_the_head() {
        let g = gov(1);
        let (w1, n1) = counter_wake();
        let (w2, n2) = counter_wake();
        let p = admitted(g.try_admit(1, Nanos(1), TTL, &w1));
        assert!(matches!(
            g.try_admit(2, Nanos(2), TTL, &w2),
            Admission::Deferred
        ));
        assert_eq!(n2.load(Ordering::SeqCst), 0);
        drop(p);
        assert_eq!(n2.load(Ordering::SeqCst), 1, "the head is woken");
        assert_eq!(n1.load(Ordering::SeqCst), 0);
        let _q = admitted(g.try_admit(2, Nanos(3), TTL, &w2));
        assert_eq!(g.stats().queue_depth, 0);
    }

    #[test]
    fn a_newcomer_does_not_jump_a_queued_group() {
        let g = gov(1);
        let (w, _) = counter_wake();
        let p = admitted(g.try_admit(1, Nanos(1), TTL, &w));
        assert!(matches!(
            g.try_admit(2, Nanos(2), TTL, &w),
            Admission::Deferred
        ));
        drop(p);
        // Slot is free, group 2 is queued ahead: group 3 must wait.
        assert!(matches!(
            g.try_admit(3, Nanos(4), TTL, &w),
            Admission::Deferred
        ));
        let _p2 = admitted(g.try_admit(2, Nanos(5), TTL, &w));
    }

    #[test]
    fn a_failed_attempt_requeues_at_the_back() {
        let g = gov(1);
        let (w, _) = counter_wake();
        let p = admitted(g.try_admit(1, Nanos(1), TTL, &w));
        assert!(matches!(
            g.try_admit(2, Nanos(2), TTL, &w),
            Admission::Deferred
        ));
        // Group 1's attempt fails: it drops the permit and asks again.
        drop(p);
        assert!(
            matches!(g.try_admit(1, Nanos(3), TTL, &w), Admission::Deferred),
            "group 2 was waiting first"
        );
        let p2 = admitted(g.try_admit(2, Nanos(4), TTL, &w));
        drop(p2);
        let _p1 = admitted(g.try_admit(1, Nanos(5), TTL, &w));
    }

    #[test]
    fn a_leaked_permit_expires_lazily() {
        let g = gov(1);
        let (w, _) = counter_wake();
        let leaked = admitted(g.try_admit(1, Nanos(0), TTL, &w));
        std::mem::forget(leaked);
        assert!(matches!(
            g.try_admit(2, Nanos(1), TTL, &w),
            Admission::Deferred
        ));
        let later = Nanos(0)
            .saturating_add(TTL)
            .saturating_add(Duration::from_nanos(1));
        let _p = admitted(g.try_admit(2, later, TTL, &w));
        assert_eq!(g.stats().expired_permits, 1);
    }

    #[test]
    fn a_silent_waiter_expires_and_does_not_block_the_head() {
        let g = gov(1);
        let (w, _) = counter_wake();
        let p = admitted(g.try_admit(1, Nanos(0), TTL, &w));
        assert!(matches!(
            g.try_admit(2, Nanos(1), TTL, &w),
            Admission::Deferred
        ));
        drop(p);
        // Group 2 never comes back; group 3 asks after the waiter's ttl.
        let later = Nanos(0)
            .saturating_add(TTL)
            .saturating_add(Duration::from_nanos(2));
        let _p3 = admitted(g.try_admit(3, later, TTL, &w));
        assert_eq!(g.stats().queue_depth, 0);
    }

    #[test]
    fn cancel_removes_a_waiter_and_wakes_the_next() {
        let g = gov(1);
        let (w, _) = counter_wake();
        let (w3, n3) = counter_wake();
        let _p = admitted(g.try_admit(1, Nanos(1), TTL, &w));
        assert!(matches!(
            g.try_admit(2, Nanos(2), TTL, &w),
            Admission::Deferred
        ));
        assert!(matches!(
            g.try_admit(3, Nanos(3), TTL, &w3),
            Admission::Deferred
        ));
        assert!(g.cancel(2));
        assert!(!g.cancel(2));
        assert_eq!(g.stats().queue_depth, 1);
        // No free slot: the cancel wakes nobody.
        assert_eq!(n3.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn exempt_campaigns_exceed_the_cap_and_count() {
        let g = gov(1);
        let (w, _) = counter_wake();
        let a = admitted(g.try_admit(1, Nanos(1), TTL, &w));
        let e = g.acquire_exempt(9, Nanos(2), TTL);
        let s = g.stats();
        assert_eq!(s.inflight, 2, "over the cap, counted");
        assert_eq!(s.inflight_hwm, 2);
        assert_eq!(s.nonexempt_hwm, 1);
        assert!(matches!(
            g.try_admit(2, Nanos(3), TTL, &w),
            Admission::Deferred
        ));
        drop(a);
        // Exempt permit still occupies the only slot.
        assert!(matches!(
            g.try_admit(2, Nanos(4), TTL, &w),
            Admission::Deferred
        ));
        drop(e);
        let _p = admitted(g.try_admit(2, Nanos(5), TTL, &w));
    }

    #[test]
    fn metrics_follow_the_state() {
        let m = MetricsHandle::recording();
        let g = CampaignGovernor::new(1, m.clone());
        let (w, _) = counter_wake();
        let p = admitted(g.try_admit(1, Nanos(1), TTL, &w));
        assert!(matches!(
            g.try_admit(2, Nanos(2), TTL, &w),
            Admission::Deferred
        ));
        assert_eq!(m.get(Metric::CpCampaignAdmitted), 1);
        assert_eq!(m.get(Metric::CpCampaignDeferred), 1);
        assert_eq!(m.get(Metric::CpCampaignInflight), 1);
        assert_eq!(m.get(Metric::CpCampaignQueueDepth), 1);
        // Re-asking while queued is not a second deferral.
        let _ = g.try_admit(2, Nanos(3), TTL, &w);
        assert_eq!(m.get(Metric::CpCampaignDeferred), 1);
        drop(p);
        assert_eq!(m.get(Metric::CpCampaignInflight), 0);
        assert_eq!(m.get(Metric::CpCampaignInflightHwm), 1);
    }

    /// Property (seeded, deterministic): `n` groups repeatedly want a permit and
    /// hold it for a random time. The cap is never exceeded, every group is
    /// admitted at least once over a bounded horizon (no starvation) and the
    /// queue is served strictly first-come first-served among the groups that
    /// keep asking.
    #[test]
    fn fifo_fairness_and_no_starvation_over_random_hold_times() {
        for seed in 1..=40u64 {
            let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut next = move || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };
            let k = 1 + (next() % 4) as usize;
            let n = 8 + (next() % 24) as usize;
            let g = gov(k);
            let (w, _) = counter_wake();
            let mut holding: Vec<Option<(CampaignPermit, u64)>> = (0..n).map(|_| None).collect();
            let mut asked_at: Vec<Option<u64>> = vec![None; n];
            let mut admitted_count = vec![0u32; n];
            let mut now = 0u64;
            for _ in 0..1500 {
                now += 1 + next() % 5;
                // Release permits whose hold has elapsed.
                for h in holding.iter_mut() {
                    if matches!(h, Some((_, until)) if *until <= now) {
                        *h = None;
                    }
                }
                // Every group not holding asks (in a rotating order).
                let start = (next() % n as u64) as usize;
                for off in 0..n {
                    let i = (start + off) % n;
                    if holding[i].is_some() {
                        continue;
                    }
                    asked_at[i].get_or_insert(now);
                    match g.try_admit(i as u64, Nanos(now), Duration::from_secs(3600), &w) {
                        Admission::Admitted(p) => {
                            let hold = 1 + next() % 30;
                            holding[i] = Some((p, now + hold));
                            admitted_count[i] += 1;
                            asked_at[i] = None;
                        }
                        Admission::Deferred => {}
                    }
                }
                let s = g.stats();
                assert!(
                    s.inflight <= k,
                    "seed {seed}: cap {k} exceeded: {}",
                    s.inflight
                );
            }
            assert!(
                admitted_count.iter().all(|&c| c > 0),
                "seed {seed}: a group starved: {admitted_count:?}"
            );
            // Bounded wait: with `n` groups, `k` slots and holds under 31 ticks
            // of at most 5 time units each, every group is served within a
            // few queue turns; a group admitted fewer times than a tenth of
            // the fair share would mean the queue is not first-come
            // first-served (the strict order is asserted by
            // `queued_groups_are_admitted_in_deferral_order`).
            let min = *admitted_count.iter().min().unwrap();
            let max = *admitted_count.iter().max().unwrap();
            assert!(
                u64::from(max) <= u64::from(min) * 4 + 4,
                "seed {seed}: unfair service {admitted_count:?}"
            );
            assert!(g.stats().nonexempt_hwm <= k);
        }
    }

    /// NEGATIVE CONTROL (ADR 0078 (d)): a LIFO queue starves the oldest waiter
    /// when a group that cannot win keeps re-asking, which the same workload
    /// does not do to the real FIFO queue.
    #[test]
    fn a_lifo_queue_starves_the_oldest_waiter() {
        fn run(fault: GovernorFault) -> bool {
            let g = CampaignGovernor::with_fault(1, MetricsHandle::recording(), fault);
            let (w, _) = counter_wake();
            // Group 100 queues first; groups 1 and 2 are "failing campaigners":
            // each is admitted, holds, drops and re-asks (as a failed attempt does).
            let mut holder = Some(admitted(g.try_admit(1, Nanos(0), TTL, &w)));
            assert!(matches!(
                g.try_admit(100, Nanos(1), TTL, &w),
                Admission::Deferred
            ));
            assert!(matches!(
                g.try_admit(2, Nanos(2), TTL, &w),
                Admission::Deferred
            ));
            let mut turn = 1u64;
            for now in 10..200u64 {
                drop(holder.take());
                // The failed campaigner re-asks; so does the old waiter.
                for id in [turn, 100, 3 - turn] {
                    if let Admission::Admitted(p) = g.try_admit(id, Nanos(now * 10), TTL, &w) {
                        if id == 100 {
                            return true;
                        }
                        holder = Some(p);
                        turn = id;
                        break;
                    }
                }
            }
            false
        }
        assert!(run(GovernorFault::None), "FIFO serves the old waiter");
        assert!(!run(GovernorFault::Lifo), "LIFO starves it");
    }

    /// The queue itself is strictly FIFO: groups deferred in order are admitted
    /// in that order when permits turn over one at a time.
    #[test]
    fn queued_groups_are_admitted_in_deferral_order() {
        let g = gov(1);
        let (w, _) = counter_wake();
        let first = admitted(g.try_admit(0, Nanos(0), TTL, &w));
        let ids: Vec<u64> = vec![7, 3, 9, 1, 5];
        for (t, id) in ids.iter().enumerate() {
            assert!(matches!(
                g.try_admit(*id, Nanos(10 + t as u64), TTL, &w),
                Admission::Deferred
            ));
        }
        drop(first);
        let mut served = Vec::new();
        let mut now = 100;
        while served.len() < ids.len() {
            let mut got = None;
            for id in &ids {
                if served.contains(id) {
                    continue;
                }
                now += 1;
                if let Admission::Admitted(p) = g.try_admit(*id, Nanos(now), TTL, &w) {
                    got = Some((*id, p));
                    break;
                }
            }
            let (id, p) = got.expect("someone is admitted");
            served.push(id);
            drop(p);
        }
        assert_eq!(served, ids, "FIFO by first deferral");
    }
}
