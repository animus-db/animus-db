//! The per-node **background-work budget** (ADR 0079 layer 3): permit pools that
//! bound how much *background* work a node runs at once, so a repair burst, a
//! split storm or a compaction backlog cannot saturate a node the way an
//! unbounded one does.
//!
//! # Shape
//!
//! Synchronous and `Env`-seam clean, like [`crate::campaign_governor`] and
//! [`crate::awake_admission`]: no tasks, time passed in as [`Nanos`],
//! `BTreeMap` throughout, no entropy. One [`BackgroundBudget`] per physical
//! node, shared by `Arc` with every group the node hosts and with the node's
//! engines.
//!
//! | Pool | A permit is held by | Default |
//! |---|---|---|
//! | [`BgPool::SnapshotSend`] | a leader, per peer, from its first `InstallSnapshot` chunk to the install ack | 2 |
//! | [`BgPool::SnapshotRecv`] | a replica, from the first chunk it accepts to its install ack | 2 |
//! | [`BgPool::Catchup`] | a hosted **learner**, from the first leader message it accepts until it is promoted | 4 |
//! | [`BgPool::Split`] | the parent's leader node, from `BeginSplitInPlace` until both children's Placing is done | 1 |
//! | [`BgPool::Compaction`] | one LSM compaction run on any engine of the node | `max(1, cores/2)`, off without `--cores` |
//!
//! A pool of size `0`/`None` is **off** (unbounded, today's behaviour). A
//! pool's effective size follows the node's scheduling lag through the shared
//! [`crate::load_monitor::scale_by_lag`] rule (decision Q9) and is **never
//! below 1** (a pool at 0 would stall background work forever; the
//! [`BudgetFault::PoolMinZero`] control shows what that costs).
//!
//! # Permits are RAII, and can expire
//!
//! A [`BgPermit`] returns its slot on `Drop`, so an error, a cancelled future
//! or a dropped driver all release it. A permit may also carry an **idle TTL**
//! ([`BgPermit::touch`] renews it): a pool that finds itself full first reaps
//! the permits whose holder has gone silent for longer than their TTL. That is
//! the bound that keeps a transfer to a dead or refusing peer from holding a
//! slot forever (see [`BgFlow`]). The TTL reaping is lazy (at the next
//! acquire), so no timer is needed.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::RaftMsg;
use animus_env::{Metric, MetricsHandle, Nanos, NodeId};

use crate::{KvCommand, KvWire};

/// Default snapshot streams per direction (send, and separately receive).
pub const DEFAULT_BG_SNAPSHOT_STREAMS: usize = 2;
/// Default learners being caught up at once on this node.
pub const DEFAULT_BG_CATCHUP_LEARNERS: usize = 4;
/// Default concurrent in-place splits led by this node.
pub const DEFAULT_BG_SPLITS: usize = 1;

/// A permit pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum BgPool {
    /// Outgoing `InstallSnapshot` transfers.
    SnapshotSend,
    /// Incoming `InstallSnapshot` transfers.
    SnapshotRecv,
    /// Learners being caught up on this node.
    Catchup,
    /// In-place splits this node leads.
    Split,
    /// LSM compaction runs, across all engines on the node.
    Compaction,
}

impl BgPool {
    /// Every pool, in a fixed order.
    pub const ALL: [BgPool; 5] = [
        BgPool::SnapshotSend,
        BgPool::SnapshotRecv,
        BgPool::Catchup,
        BgPool::Split,
        BgPool::Compaction,
    ];

    /// The stable name used in the admin JSON.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            BgPool::SnapshotSend => "snapshot_send",
            BgPool::SnapshotRecv => "snapshot_recv",
            BgPool::Catchup => "catchup",
            BgPool::Split => "split",
            BgPool::Compaction => "compaction",
        }
    }

    const fn inflight_metric(self) -> Metric {
        match self {
            BgPool::SnapshotSend => Metric::BgPermitInflightSnapshotSend,
            BgPool::SnapshotRecv => Metric::BgPermitInflightSnapshotRecv,
            BgPool::Catchup => Metric::BgPermitInflightCatchup,
            BgPool::Split => Metric::BgPermitInflightSplit,
            BgPool::Compaction => Metric::BgPermitInflightCompaction,
        }
    }

    const fn hwm_metric(self) -> Metric {
        match self {
            BgPool::SnapshotSend => Metric::BgPermitInflightHwmSnapshotSend,
            BgPool::SnapshotRecv => Metric::BgPermitInflightHwmSnapshotRecv,
            BgPool::Catchup => Metric::BgPermitInflightHwmCatchup,
            BgPool::Split => Metric::BgPermitInflightHwmSplit,
            BgPool::Compaction => Metric::BgPermitInflightHwmCompaction,
        }
    }

    const fn deferred_metric(self) -> Metric {
        match self {
            BgPool::SnapshotSend => Metric::BgPermitDeferredSnapshotSend,
            BgPool::SnapshotRecv => Metric::BgPermitDeferredSnapshotRecv,
            BgPool::Catchup => Metric::BgPermitDeferredCatchup,
            BgPool::Split => Metric::BgPermitDeferredSplit,
            BgPool::Compaction => Metric::BgPermitDeferredCompaction,
        }
    }
}

/// The resolved pool sizes; `None` = the pool is off (unbounded).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BudgetSizes {
    /// [`BgPool::SnapshotSend`].
    pub snapshot_send: Option<usize>,
    /// [`BgPool::SnapshotRecv`].
    pub snapshot_recv: Option<usize>,
    /// [`BgPool::Catchup`].
    pub catchup: Option<usize>,
    /// [`BgPool::Split`].
    pub splits: Option<usize>,
    /// [`BgPool::Compaction`].
    pub compactions: Option<usize>,
    /// ADR 0081 decision 3: the node-wide compaction write budget in bytes/s;
    /// `0` is **off** (the default), never derived from CPU or disk.
    pub compaction_io_bytes_per_sec: u64,
}

impl BudgetSizes {
    /// Resolve the flags (`--bg-snapshot-streams`, `--bg-catchup-learners`,
    /// `--bg-splits`, `--bg-compactions`, `--cores`). An unset flag takes the
    /// default, `0` turns the pool off. The compaction pool is **off unless**
    /// `--bg-compactions` or `--cores` is set (`max(1, cores / 2)`): the core
    /// count is never autodetected, so a node without it compacts as before.
    #[must_use]
    pub fn resolve(
        snapshot_streams: Option<usize>,
        catchup_learners: Option<usize>,
        splits: Option<usize>,
        compactions: Option<usize>,
        cores: Option<usize>,
    ) -> Self {
        let on = |v: Option<usize>, default: usize| {
            let n = v.unwrap_or(default);
            (n > 0).then_some(n)
        };
        let snap = on(snapshot_streams, DEFAULT_BG_SNAPSHOT_STREAMS);
        Self {
            snapshot_send: snap,
            snapshot_recv: snap,
            catchup: on(catchup_learners, DEFAULT_BG_CATCHUP_LEARNERS),
            splits: on(splits, DEFAULT_BG_SPLITS),
            compactions: match compactions {
                Some(n) => (n > 0).then_some(n),
                None => cores.map(|c| (c / 2).max(1)),
            },
            compaction_io_bytes_per_sec: 0,
        }
    }

    /// Set the compaction write budget (`--compaction-io-bytes-per-sec`,
    /// ADR 0081 decision 3); `0` leaves it off.
    #[must_use]
    pub const fn with_compaction_io(mut self, bytes_per_sec: u64) -> Self {
        self.compaction_io_bytes_per_sec = bytes_per_sec;
        self
    }

    /// Every pool off: byte-for-byte the pre-ADR behaviour.
    #[must_use]
    pub const fn off() -> Self {
        Self {
            snapshot_send: None,
            snapshot_recv: None,
            catchup: None,
            splits: None,
            compactions: None,
            compaction_io_bytes_per_sec: 0,
        }
    }

    fn size(&self, pool: BgPool) -> Option<usize> {
        match pool {
            BgPool::SnapshotSend => self.snapshot_send,
            BgPool::SnapshotRecv => self.snapshot_recv,
            BgPool::Catchup => self.catchup,
            BgPool::Split => self.splits,
            BgPool::Compaction => self.compactions,
        }
    }
}

/// Deliberate bugs for the Tier 1 negative controls (ADR 0079 controls (d)
/// and (e)). `None` in every real build; only the corpus constructs another
/// value.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BudgetFault {
    /// The real budget.
    #[default]
    None,
    /// Permits are never returned (no RAII release, no TTL reaping): the pool
    /// leaks until it is stuck (control (d)).
    LeakPermits,
    /// A pool's effective size may reach 0 under lag (control (e)): background
    /// work stalls, so L0 only ever drains through the safety valve.
    PoolMinZero,
    /// Permits are returned normally but never reaped for idling: a transfer
    /// to a dead or refusing peer holds its slot forever (the
    /// unacknowledged-transfer control).
    NoIdleBound,
}

struct Held {
    last_touch: Nanos,
    ttl: Option<Duration>,
}

impl Held {
    fn expired(&self, now: Nanos) -> bool {
        self.ttl
            .is_some_and(|t| now.duration_since(self.last_touch) >= t)
    }
}

#[derive(Default)]
struct PoolState {
    /// `None` = off.
    size: Option<usize>,
    held: BTreeMap<u64, Held>,
    hwm: usize,
    /// Highest `held` count right after a *within-cap* admission (never above
    /// the `size_eff` of that moment; bypasses cannot raise it).
    admit_hwm: usize,
    admitted: u64,
    deferred: u64,
    bypassed: u64,
    expired: u64,
}

struct State {
    lag: u64,
    fault: BudgetFault,
    next_id: u64,
    pools: BTreeMap<BgPool, PoolState>,
    /// ADR 0081 decision 3: the node's compaction write-bytes bucket.
    io: crate::compaction_io::IoBucket,
}

impl State {
    fn size_eff(&self, pool: BgPool) -> Option<usize> {
        let size = self.pools.get(&pool)?.size?;
        Some(if self.fault == BudgetFault::PoolMinZero {
            size / crate::load_monitor::lag_divisor(self.lag)
        } else {
            crate::load_monitor::scale_by_lag(size, self.lag)
        })
    }
}

struct Shared {
    state: Mutex<State>,
    metrics: MetricsHandle,
}

impl Shared {
    fn publish(&self, st: &State, pool: BgPool) {
        let p = &st.pools[&pool];
        if p.size.is_some() {
            self.metrics
                .set(pool.inflight_metric(), p.held.len() as u64);
            self.metrics.set(pool.hwm_metric(), p.hwm as u64);
        }
    }

    fn publish_io(&self, st: &State) {
        if st.io.is_on() {
            self.metrics
                .set(Metric::StorageCompactionIoRate, st.io.effective_rate(st.lag));
            self.metrics
                .set(Metric::StorageCompactionIoDebtBytes, st.io.debt_bytes());
        }
    }

    fn release(&self, pool: BgPool, id: u64) {
        let mut st = self.state.lock().expect("budget poisoned");
        if st.fault == BudgetFault::LeakPermits {
            return;
        }
        if let Some(p) = st.pools.get_mut(&pool) {
            p.held.remove(&id);
        }
        self.publish(&st, pool);
    }
}

/// The compaction write budget's counters (ADR 0081).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionIoStats {
    /// Configured rate, bytes/s (`0` = off).
    pub rate: u64,
    /// The rate after lag scaling.
    pub rate_eff: u64,
    /// Bytes overdrawn now.
    pub debt_bytes: u64,
    /// Bytes of credit now.
    pub credit_bytes: u64,
    /// Non-urgent compactions admitted.
    pub admitted: u64,
    /// Urgent compactions admitted over an empty bucket.
    pub urgent: u64,
    /// Non-urgent compactions refused for want of credit.
    pub refused: u64,
    /// Total bytes charged.
    pub charged_bytes: u128,
}

/// One pool's counters (for the admin interface and the tests).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolStats {
    /// Configured size (`None` = off).
    pub size: Option<usize>,
    /// The size after lag reduction (`None` = off).
    pub size_eff: Option<usize>,
    /// Permits held now (bypasses included).
    pub inflight: usize,
    /// Most permits ever held at once.
    pub hwm: usize,
    /// Highest count right after a within-cap admission.
    pub admit_hwm: usize,
    /// Within-cap admissions.
    pub admitted: u64,
    /// Requests refused for want of a permit.
    pub deferred: u64,
    /// Over-cap grants (the L0 safety valve).
    pub bypassed: u64,
    /// Permits reaped for idling past their TTL.
    pub expired: u64,
}

/// The node's background-work budget. Cloning is an `Arc` bump.
#[derive(Clone)]
pub struct BackgroundBudget {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for BackgroundBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BackgroundBudget")
    }
}

impl BackgroundBudget {
    /// A budget with the given pool sizes.
    #[must_use]
    pub fn new(sizes: BudgetSizes, metrics: MetricsHandle) -> Self {
        Self::with_fault(sizes, metrics, BudgetFault::None)
    }

    /// [`new`](Self::new) with a deliberate bug, for the negative controls.
    #[doc(hidden)]
    #[must_use]
    pub fn with_fault(sizes: BudgetSizes, metrics: MetricsHandle, fault: BudgetFault) -> Self {
        let pools = BgPool::ALL
            .into_iter()
            .map(|p| {
                (
                    p,
                    PoolState {
                        size: sizes.size(p),
                        ..PoolState::default()
                    },
                )
            })
            .collect();
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    lag: 0,
                    fault,
                    next_id: 1,
                    pools,
                    io: crate::compaction_io::IoBucket::new(sizes.compaction_io_bytes_per_sec),
                }),
                metrics,
            }),
        }
    }

    /// Feed the node's smoothed scheduling lag (nanoseconds), from the
    /// [`crate::load_monitor::LoadMonitor`]. The effective sizes follow it.
    pub fn set_lag(&self, lag: u64) {
        self.shared.state.lock().expect("budget poisoned").lag = lag;
    }

    /// Turn on the node-wide compaction write budget (ADR 0081 decision 3) at
    /// `bytes_per_sec` (`0` = off, today's behaviour). Call before the budget
    /// is shared with any engine; the bucket starts with one second of credit.
    pub fn set_compaction_io_rate(&self, bytes_per_sec: u64) {
        let mut st = self.shared.state.lock().expect("budget poisoned");
        st.io = crate::compaction_io::IoBucket::new(bytes_per_sec);
        self.shared.publish_io(&st);
    }

    /// Whether the compaction write budget is on.
    #[must_use]
    pub fn compaction_io_on(&self) -> bool {
        self.shared
            .state
            .lock()
            .expect("budget poisoned")
            .io
            .is_on()
    }

    /// The compaction write budget's counters (admin, tests).
    #[must_use]
    pub fn compaction_io_stats(&self) -> CompactionIoStats {
        let st = self.shared.state.lock().expect("budget poisoned");
        CompactionIoStats {
            rate: st.io.rate(),
            rate_eff: st.io.effective_rate(st.lag),
            debt_bytes: st.io.debt_bytes(),
            credit_bytes: st.io.credit_bytes(),
            admitted: st.io.admitted,
            urgent: st.io.urgent,
            refused: st.io.refused,
            charged_bytes: st.io.charged_bytes,
        }
    }

    /// Ask to spend `bytes` of the compaction write budget at `now`. `urgent`
    /// requests are always admitted (into debt).
    #[must_use]
    pub fn compaction_io_admit(&self, now: Nanos, bytes: u64, urgent: bool) -> bool {
        let mut st = self.shared.state.lock().expect("budget poisoned");
        let lag = st.lag;
        let ok = st.io.try_admit(now, lag, bytes, urgent);
        self.shared.publish_io(&st);
        ok
    }

    /// Correct a charge with the bytes actually written.
    pub fn compaction_io_settle(&self, estimated: u64, actual: u64) {
        let mut st = self.shared.state.lock().expect("budget poisoned");
        st.io.settle(estimated, actual);
        self.shared.publish_io(&st);
    }

    /// Charge never-delayed bytes (a flush) into debt.
    pub fn compaction_io_charge(&self, now: Nanos, bytes: u64) {
        let mut st = self.shared.state.lock().expect("budget poisoned");
        let lag = st.lag;
        st.io.charge(now, lag, bytes);
        self.shared.publish_io(&st);
    }

    /// Whether `pool` is on (bounded).
    #[must_use]
    pub fn is_on(&self, pool: BgPool) -> bool {
        self.shared.state.lock().expect("budget poisoned").pools[&pool]
            .size
            .is_some()
    }

    /// Ask for a permit. `ttl` makes it expire after that long without a
    /// [`BgPermit::touch`]; permits already past their TTL are reaped first.
    /// `None` = no permit free (**not** counted as a deferral: the caller
    /// decides when a refusal counts, see [`note_deferred`](Self::note_deferred)).
    /// An off pool always grants an untracked permit.
    #[must_use]
    pub fn try_acquire(&self, pool: BgPool, now: Nanos, ttl: Option<Duration>) -> Option<BgPermit> {
        let mut st = self.shared.state.lock().expect("budget poisoned");
        let no_reap = matches!(
            st.fault,
            BudgetFault::LeakPermits | BudgetFault::NoIdleBound
        );
        let eff = st.size_eff(pool);
        let id = st.next_id;
        let p = st.pools.get_mut(&pool).expect("every pool exists");
        let Some(eff) = eff else {
            return Some(BgPermit::untracked());
        };
        if !no_reap {
            let before = p.held.len();
            p.held.retain(|_, h| !h.expired(now));
            p.expired += (before - p.held.len()) as u64;
        }
        if p.held.len() >= eff {
            return None;
        }
        p.held.insert(
            id,
            Held {
                last_touch: now,
                ttl,
            },
        );
        p.admitted += 1;
        p.hwm = p.hwm.max(p.held.len());
        p.admit_hwm = p.admit_hwm.max(p.held.len());
        st.next_id += 1;
        self.shared.publish(&st, pool);
        Some(BgPermit {
            inner: Some((Arc::clone(&self.shared), pool, id)),
        })
    }

    /// A permit granted **over the cap** (the compaction pool's L0 safety
    /// valve): always granted, counted as a bypass.
    pub fn acquire_bypass(&self, pool: BgPool) -> BgPermit {
        let mut st = self.shared.state.lock().expect("budget poisoned");
        let id = st.next_id;
        let p = st.pools.get_mut(&pool).expect("every pool exists");
        if p.size.is_none() {
            return BgPermit::untracked();
        }
        p.held.insert(
            id,
            Held {
                last_touch: Nanos(0),
                ttl: None,
            },
        );
        p.bypassed += 1;
        p.hwm = p.hwm.max(p.held.len());
        st.next_id += 1;
        if pool == BgPool::Compaction {
            self.shared.metrics.incr(Metric::BgCompactionOvercap);
        }
        self.shared.publish(&st, pool);
        BgPermit {
            inner: Some((Arc::clone(&self.shared), pool, id)),
        }
    }

    /// Count one deferred request on `pool` (the caller passes it once per
    /// requester, not once per retry).
    pub fn note_deferred(&self, pool: BgPool) {
        let mut st = self.shared.state.lock().expect("budget poisoned");
        let p = st.pools.get_mut(&pool).expect("every pool exists");
        if p.size.is_some() {
            p.deferred += 1;
            self.shared.metrics.incr(pool.deferred_metric());
        }
    }

    /// One pool's counters.
    #[must_use]
    pub fn stats(&self, pool: BgPool) -> PoolStats {
        let st = self.shared.state.lock().expect("budget poisoned");
        let p = &st.pools[&pool];
        PoolStats {
            size: p.size,
            size_eff: st.size_eff(pool),
            inflight: p.held.len(),
            hwm: p.hwm,
            admit_hwm: p.admit_hwm,
            admitted: p.admitted,
            deferred: p.deferred,
            bypassed: p.bypassed,
            expired: p.expired,
        }
    }

    /// A compaction gate for one engine ([`animus_storage::CompactionGate`]):
    /// takes a [`BgPool::Compaction`] permit per run, a bypass permit when the
    /// engine's L0 safety valve says so, and counts one deferral per engine
    /// until it is next admitted. `None` when the compaction pool is off (the
    /// engine then compacts exactly as before).
    #[must_use]
    pub fn compaction_gate(
        &self,
        env_now: Arc<dyn Fn() -> Nanos + Send + Sync>,
    ) -> Option<Arc<dyn animus_storage::CompactionGate>> {
        (self.is_on(BgPool::Compaction) || self.compaction_io_on()).then(|| {
            Arc::new(EngineCompactionGate {
                budget: self.clone(),
                now: env_now,
                deferral_marked: std::sync::atomic::AtomicBool::new(false),
            }) as Arc<dyn animus_storage::CompactionGate>
        })
    }
}

struct EngineCompactionGate {
    budget: BackgroundBudget,
    now: Arc<dyn Fn() -> Nanos + Send + Sync>,
    deferral_marked: std::sync::atomic::AtomicBool,
}

impl animus_storage::CompactionGate for EngineCompactionGate {
    fn acquire(&self, bypass: bool) -> Option<animus_storage::CompactionPermit> {
        use std::sync::atomic::Ordering;
        if bypass {
            return Some(Box::new(self.budget.acquire_bypass(BgPool::Compaction)));
        }
        match self
            .budget
            .try_acquire(BgPool::Compaction, (self.now)(), None)
        {
            Some(p) => {
                self.deferral_marked.store(false, Ordering::Relaxed);
                Some(Box::new(p))
            }
            None => {
                if !self.deferral_marked.swap(true, Ordering::Relaxed) {
                    self.budget.note_deferred(BgPool::Compaction);
                }
                None
            }
        }
    }
    fn io_admit(&self, estimated_bytes: u64, urgent: bool) -> animus_storage::IoAdmission {
        if self
            .budget
            .compaction_io_admit((self.now)(), estimated_bytes, urgent)
        {
            animus_storage::IoAdmission::Admitted
        } else {
            animus_storage::IoAdmission::Defer
        }
    }

    fn io_settle(&self, estimated_bytes: u64, actual_bytes: u64) {
        self.budget
            .compaction_io_settle(estimated_bytes, actual_bytes);
    }

    fn io_charge(&self, bytes: u64) {
        self.budget.compaction_io_charge((self.now)(), bytes);
    }

}

/// A held slot; dropping it returns the slot.
#[must_use]
pub struct BgPermit {
    inner: Option<(Arc<Shared>, BgPool, u64)>,
}

impl BgPermit {
    fn untracked() -> Self {
        Self { inner: None }
    }

    /// Renew the idle TTL (progress was made).
    pub fn touch(&self, now: Nanos) {
        if let Some((shared, pool, id)) = &self.inner {
            let mut st = shared.state.lock().expect("budget poisoned");
            if let Some(h) = st.pools.get_mut(pool).and_then(|p| p.held.get_mut(id)) {
                h.last_touch = now;
            }
        }
    }

    /// Whether the pool still holds this permit at `now` (a permit that idled
    /// past its TTL is reaped here, and is then no longer live). An off pool's
    /// untracked permit is always live.
    #[must_use]
    pub fn alive_at(&self, now: Nanos) -> bool {
        let Some((shared, pool, id)) = &self.inner else {
            return true;
        };
        let mut st = shared.state.lock().expect("budget poisoned");
        let no_reap = matches!(
            st.fault,
            BudgetFault::LeakPermits | BudgetFault::NoIdleBound
        );
        let Some(p) = st.pools.get_mut(pool) else {
            return false;
        };
        match p.held.get(id) {
            None => false,
            Some(h) if !no_reap && h.expired(now) => {
                p.held.remove(id);
                p.expired += 1;
                shared.publish(&st, *pool);
                false
            }
            Some(_) => true,
        }
    }
}

impl Drop for BgPermit {
    fn drop(&mut self) {
        if let Some((shared, pool, id)) = self.inner.take() {
            shared.release(pool, id);
        }
    }
}

// ---------------------------------------------------------------------------
// The split pool
// ---------------------------------------------------------------------------

/// How long a split may hold its pool permit before the pool reclaims it
/// regardless: 10 minutes, 40 x animusd's `AUTO_SPLIT_COOLDOWN` (15 s) and far
/// beyond a healthy split's fork plus directed Placing, so only a stuck one is
/// ever cut off.
pub const SPLIT_PERMIT_MAX_HOLD: Duration = Duration::from_secs(600);

/// The outcome of asking the split pool.
#[must_use]
pub enum SplitAdmission {
    /// Go ahead; `Some` is the permit to [`hold`](SplitPermits::hold) until the
    /// split finishes (`None` = the pool is off, or the tablet already holds
    /// one or is already splitting).
    Granted(Option<BgPermit>),
    /// The pool is full: refuse with a retryable error.
    Busy,
}

/// The splits this node started, each holding a [`BgPool::Split`] permit from
/// `BeginSplitInPlace` until it is **finished** in the replicated `Metadata`:
/// the parent is retired (or back to `Active` after an abort) and every child
/// recorded for it has its directed Placing done (ADR 0062 section 2). The
/// release is a pure function of `Metadata`, so it is the same on every node
/// and needs no extra message.
#[derive(Clone)]
pub struct SplitPermits {
    budget: BackgroundBudget,
    held: Arc<Mutex<BTreeMap<animus_tablet::TabletId, BgPermit>>>,
    /// Parents whose deferral is already counted (once per requester).
    deferred: Arc<Mutex<BTreeSet<animus_tablet::TabletId>>>,
    /// Manual splits waiting for a permit, oldest first ([`admit_wait`](Self::admit_wait)).
    waiters: Arc<Mutex<SplitWaiters>>,
}

#[derive(Default)]
struct SplitWaiters {
    next: u64,
    queue: std::collections::VecDeque<u64>,
}

/// A place in the split pool's FIFO of waiting manual splits. Dropping it
/// (the wait finished, timed out, or its future was cancelled) leaves the
/// queue, so an abandoned waiter can never block the ones behind it.
struct SplitTicket {
    id: u64,
    waiters: Arc<Mutex<SplitWaiters>>,
}

impl Drop for SplitTicket {
    fn drop(&mut self) {
        let mut w = self.waiters.lock().expect("split waiters poisoned");
        w.queue.retain(|&t| t != self.id);
    }
}

impl SplitPermits {
    /// A registry over `budget`'s split pool.
    #[must_use]
    pub fn new(budget: BackgroundBudget) -> Self {
        Self {
            budget,
            held: Arc::default(),
            deferred: Arc::default(),
            waiters: Arc::default(),
        }
    }

    /// Whether the split pool is on.
    #[must_use]
    pub fn is_on(&self) -> bool {
        self.budget.is_on(BgPool::Split)
    }

    /// Splits currently holding a permit (test aid).
    #[must_use]
    pub fn held(&self) -> usize {
        self.held.lock().expect("split permits poisoned").len()
    }

    /// Return the permit of every finished split (see the type docs), and of
    /// any the pool has already reaped for exceeding [`SPLIT_PERMIT_MAX_HOLD`].
    pub fn release_finished(&self, meta: &animus_control::Metadata, now: Nanos) {
        use animus_tablet::TabletState;
        let mut held = self.held.lock().expect("split permits poisoned");
        held.retain(|&parent, permit| {
            if !permit.alive_at(now) {
                return false;
            }
            match meta.tablets.get(&parent) {
                Some(t) if t.state == TabletState::Splitting => return true,
                // Back to Active (aborted), or never began: done.
                Some(_) => return false,
                None => {}
            }
            // Retired: keep the permit while any child's Placing is unfinished.
            meta.split_lineage
                .iter()
                .filter(|(_, l)| l.parent == parent)
                .any(|(child, _)| meta.split_placing.get(child).is_some_and(|p| !p.done))
        });
    }

    /// Whether a new split would be refused now (the auto-split sweeps skip a
    /// tablet instead of materializing it). Counts one deferral per tablet
    /// until it is next admitted.
    pub fn pool_full(
        &self,
        tablet: animus_tablet::TabletId,
        meta: &animus_control::Metadata,
        now: Nanos,
    ) -> bool {
        if !self.is_on() {
            return false;
        }
        self.release_finished(meta, now);
        let st = self.budget.stats(BgPool::Split);
        let full = st.size_eff.is_some_and(|eff| st.inflight >= eff);
        if full && self.deferred.lock().expect("poisoned").insert(tablet) {
            self.budget.note_deferred(BgPool::Split);
        }
        full
    }

    /// Ask the split pool for `tablet` (see [`SplitAdmission`]).
    pub fn admit(
        &self,
        tablet: animus_tablet::TabletId,
        meta: &animus_control::Metadata,
        now: Nanos,
    ) -> SplitAdmission {
        if !self.is_on() {
            return SplitAdmission::Granted(None);
        }
        self.release_finished(meta, now);
        // Idempotent kickoff of a split that is already running: no new permit.
        let already = self.held.lock().expect("poisoned").contains_key(&tablet)
            || meta
                .tablets
                .get(&tablet)
                .is_some_and(|t| t.state == animus_tablet::TabletState::Splitting);
        if already {
            return SplitAdmission::Granted(None);
        }
        match self
            .budget
            .try_acquire(BgPool::Split, now, Some(SPLIT_PERMIT_MAX_HOLD))
        {
            Some(p) => {
                self.deferred.lock().expect("poisoned").remove(&tablet);
                SplitAdmission::Granted(Some(p))
            }
            None => {
                if self.deferred.lock().expect("poisoned").insert(tablet) {
                    self.budget.note_deferred(BgPool::Split);
                }
                SplitAdmission::Busy
            }
        }
    }

    /// [`admit`](Self::admit), but **wait** for a permit (ADR 0079 step 5): an
    /// explicit, operator-requested split queues behind the pool instead of
    /// being refused. Waiters are served strictly first come, first served;
    /// the permit frees as `meta()` (re-read on every poll) shows an earlier
    /// split finished, so the wait polls every `poll` of `env` time. Returns
    /// [`SplitAdmission::Busy`] only once `bound` has passed. Cancellation-safe:
    /// a dropped future leaves the queue (see `SplitTicket`).
    pub async fn admit_wait<E: animus_env::Env>(
        &self,
        env: &E,
        tablet: animus_tablet::TabletId,
        meta: impl Fn() -> animus_control::Metadata,
        bound: Duration,
        poll: Duration,
    ) -> SplitAdmission {
        let deadline = env.now().saturating_add(bound);
        let ticket = {
            let mut w = self.waiters.lock().expect("split waiters poisoned");
            let id = w.next;
            w.next += 1;
            w.queue.push_back(id);
            SplitTicket {
                id,
                waiters: self.waiters.clone(),
            }
        };
        loop {
            let at_head = self
                .waiters
                .lock()
                .expect("split waiters poisoned")
                .queue
                .front()
                == Some(&ticket.id);
            if at_head && let SplitAdmission::Granted(p) = self.admit(tablet, &meta(), env.now()) {
                return SplitAdmission::Granted(p);
            }
            if env.now() >= deadline {
                return SplitAdmission::Busy;
            }
            env.sleep(poll).await;
        }
    }

    /// Keep `permit` until `tablet`'s split finishes.
    pub fn hold(&self, tablet: animus_tablet::TabletId, permit: BgPermit) {
        self.held
            .lock()
            .expect("split permits poisoned")
            .insert(tablet, permit);
    }
}

// ---------------------------------------------------------------------------
// The per-group snapshot / catch-up flow
// ---------------------------------------------------------------------------

/// How many heartbeat ticks without an acknowledgement end a snapshot
/// transfer's claim on a *send* slot: twice the steady-state gap of the
/// sender's resend schedule ([`animus_control::SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS`],
/// 32 ticks, about 1.6 s at the 50 ms heartbeat). The schedule resends at
/// doubling gaps (1, 2, 4, ... 32 ticks) and then every 32nd tick, so by 64
/// ticks a peer that is alive and has room has been offered the first chunk
/// about seven times; silence for that long means a dead, partitioned or
/// refusing receiver (a receiver with a full pool refuses by not
/// acknowledging, ADR 0079 step 3), and holding the slot any longer only
/// starves transfers that could make progress.
pub const SNAPSHOT_UNACKED_BOUND_TICKS: u32 =
    2 * animus_control::SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS;

/// The sender's unacknowledged-transfer bound at `heartbeat`.
#[must_use]
pub fn snapshot_unacked_bound(heartbeat: Duration) -> Duration {
    heartbeat.saturating_mul(SNAPSHOT_UNACKED_BOUND_TICKS)
}

/// The receiver's idle bound: a receive slot (or a learner's catch-up slot)
/// whose leader has been silent this long is reaped. Twice the sender's bound,
/// so the sender has always given up (and its cooldown has run) before the
/// receiver's slot is taken from a transfer that may still resume.
#[must_use]
pub fn snapshot_recv_idle_bound(heartbeat: Duration) -> Duration {
    snapshot_unacked_bound(heartbeat).saturating_mul(2)
}

/// The snapshot and learner catch-up permits of **one hosted group** on this
/// node, driven by the group's consensus loop. It filters the Raft messages
/// the group receives and sends; the core never sees the budget, so Raft
/// safety rules are untouched: a refused chunk is indistinguishable from a
/// slow follower (ADR 0079 step 3, class L).
///
/// - **Receive.** A replica admits an `InstallSnapshot` chunk only while it
///   holds a [`BgPool::SnapshotRecv`] permit (taken at the first chunk it
///   admits, released by its install/redundancy ack or after the idle bound).
///   Without one the chunk is **dropped unacknowledged**; the sender's
///   existing backoff resends it. A **learner** additionally needs a
///   [`BgPool::Catchup`] permit for any leader message, held until it is
///   promoted (or removed, or the leader goes silent past the idle bound).
/// - **Send.** A leader ships a chunk to a peer only while it holds a
///   [`BgPool::SnapshotSend`] permit for that peer. The permit is released by
///   the peer's install ack, or **by the unacknowledged bound**
///   ([`snapshot_unacked_bound`]): a transfer that makes no progress is
///   dropped, and for one bound afterwards that peer may not re-ask, so a
///   dead receiver cannot monopolise a slot.
pub(crate) struct BgFlow {
    budget: BackgroundBudget,
    send: BTreeMap<NodeId, BgPermit>,
    /// Peers that lost a send slot to the unacknowledged bound: no re-ask
    /// before this instant.
    cooldown: BTreeMap<NodeId, Nanos>,
    send_deferred: BTreeSet<NodeId>,
    recv: Option<BgPermit>,
    recv_deferred: bool,
    catchup: Option<BgPermit>,
    catchup_deferred: bool,
}

impl BgFlow {
    /// A fresh flow for one group.
    #[must_use]
    pub(crate) fn new(budget: BackgroundBudget) -> Self {
        Self {
            budget,
            send: BTreeMap::new(),
            cooldown: BTreeMap::new(),
            send_deferred: BTreeSet::new(),
            recv: None,
            recv_deferred: false,
            catchup: None,
            catchup_deferred: false,
        }
    }

    /// Whether the group's snapshot send permits currently held (test aid).
    #[cfg(test)]
    #[must_use]
    pub(crate) fn sends_held(&self) -> usize {
        self.send.len()
    }

    /// Decide whether an inbound Raft message is handed to the core. `false`
    /// = drop it unacknowledged. `heartbeat` is the group's current heartbeat
    /// interval, `is_learner` whether the group's core says this node is a
    /// learner.
    pub(crate) fn admit_inbound(
        &mut self,
        from: &NodeId,
        msg: &RaftMsg<KvCommand>,
        now: Nanos,
        heartbeat: Duration,
        is_learner: bool,
    ) -> bool {
        let idle = snapshot_recv_idle_bound(heartbeat);
        match msg {
            RaftMsg::InstallSnapshotResp {
                last_index,
                next_offset,
                ..
            } => {
                // A sender's view of its transfer to `from`.
                if let Some(p) = self.send.get(from) {
                    if *last_index > 0 || *next_offset == 0 {
                        // Installed, or "already holds this state": done.
                        self.send.remove(from);
                    } else {
                        p.touch(now);
                    }
                }
                true
            }
            RaftMsg::InstallSnapshot { .. } | RaftMsg::AppendEntries { .. } => {
                if is_learner && !self.hold_catchup(now, idle) {
                    return false;
                }
                if matches!(msg, RaftMsg::InstallSnapshot { .. }) {
                    return self.hold_recv(now, idle);
                }
                true
            }
            _ => true,
        }
    }

    fn hold_catchup(&mut self, now: Nanos, idle: Duration) -> bool {
        if let Some(p) = &self.catchup {
            if p.alive_at(now) {
                p.touch(now);
                return true;
            }
            self.catchup = None;
        }
        match self.budget.try_acquire(BgPool::Catchup, now, Some(idle)) {
            Some(p) => {
                self.catchup = Some(p);
                self.catchup_deferred = false;
                true
            }
            None => {
                if !self.catchup_deferred {
                    self.catchup_deferred = true;
                    self.budget.note_deferred(BgPool::Catchup);
                }
                false
            }
        }
    }

    fn hold_recv(&mut self, now: Nanos, idle: Duration) -> bool {
        if let Some(p) = &self.recv {
            if p.alive_at(now) {
                p.touch(now);
                return true;
            }
            self.recv = None;
        }
        match self
            .budget
            .try_acquire(BgPool::SnapshotRecv, now, Some(idle))
        {
            Some(p) => {
                self.recv = Some(p);
                self.recv_deferred = false;
                true
            }
            None => {
                if !self.recv_deferred {
                    self.recv_deferred = true;
                    self.budget.note_deferred(BgPool::SnapshotRecv);
                }
                false
            }
        }
    }

    /// After the core has stepped: a promoted (or removed) learner returns its
    /// catch-up slot.
    pub(crate) fn after_step(&mut self, is_learner: bool) {
        if !is_learner {
            self.catchup = None;
            self.catchup_deferred = false;
        }
    }

    /// Filter the messages the group is about to send. A leader's snapshot
    /// chunk to a peer goes out only under a send permit; our own install /
    /// redundancy acknowledgement returns the receive slot. `is_leader` false
    /// returns every send slot (a deposed leader's transfers are dead).
    pub(crate) fn filter_outbound(
        &mut self,
        outs: Vec<(NodeId, KvWire)>,
        now: Nanos,
        heartbeat: Duration,
        is_leader: bool,
    ) -> Vec<(NodeId, KvWire)> {
        if !is_leader {
            self.send.clear();
        }
        let bound = snapshot_unacked_bound(heartbeat);
        // A permit the pool reaped (idle past its bound) is a release by the
        // unacknowledged bound: the peer sits out one bound.
        let reaped: Vec<NodeId> = self
            .send
            .iter()
            .filter(|(_, p)| !p.alive_at(now))
            .map(|(n, _)| n.clone())
            .collect();
        for n in reaped {
            self.send.remove(&n);
            self.cooldown.insert(n, now.saturating_add(bound));
        }
        self.cooldown.retain(|_, until| *until > now);
        let mut out = Vec::with_capacity(outs.len());
        for (to, wire) in outs {
            match &wire {
                KvWire::Raft(RaftMsg::InstallSnapshot { .. }) => {
                    if self.send.contains_key(&to) {
                        out.push((to, wire));
                    } else if self.cooldown.contains_key(&to) {
                        // Sitting out: not counted again as a deferral.
                    } else if let Some(p) =
                        self.budget
                            .try_acquire(BgPool::SnapshotSend, now, Some(bound))
                    {
                        self.send_deferred.remove(&to);
                        self.send.insert(to.clone(), p);
                        out.push((to, wire));
                    } else if self.send_deferred.insert(to) {
                        self.budget.note_deferred(BgPool::SnapshotSend);
                    }
                }
                KvWire::Raft(RaftMsg::InstallSnapshotResp {
                    last_index,
                    next_offset,
                    ..
                }) => {
                    if *last_index > 0 || *next_offset == 0 {
                        self.recv = None;
                        self.recv_deferred = false;
                    }
                    out.push((to, wire));
                }
                _ => out.push((to, wire)),
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(sizes: BudgetSizes) -> BackgroundBudget {
        BackgroundBudget::new(sizes, MetricsHandle::recording())
    }

    fn one(pool: BgPool, n: usize) -> BudgetSizes {
        let mut s = BudgetSizes::off();
        match pool {
            BgPool::SnapshotSend => s.snapshot_send = Some(n),
            BgPool::SnapshotRecv => s.snapshot_recv = Some(n),
            BgPool::Catchup => s.catchup = Some(n),
            BgPool::Split => s.splits = Some(n),
            BgPool::Compaction => s.compactions = Some(n),
        }
        s
    }

    #[test]
    fn defaults_follow_the_adr_and_compactions_need_cores() {
        let s = BudgetSizes::resolve(None, None, None, None, None);
        assert_eq!(s.snapshot_send, Some(2));
        assert_eq!(s.snapshot_recv, Some(2));
        assert_eq!(s.catchup, Some(4));
        assert_eq!(s.splits, Some(1));
        assert_eq!(s.compactions, None, "no --cores: today's compaction");
        assert_eq!(
            BudgetSizes::resolve(None, None, None, None, Some(1)).compactions,
            Some(1)
        );
        assert_eq!(
            BudgetSizes::resolve(None, None, None, None, Some(8)).compactions,
            Some(4)
        );
        // An explicit flag wins over cores; 0 turns a pool off.
        let s = BudgetSizes::resolve(Some(0), Some(0), Some(0), Some(0), Some(8));
        assert_eq!(s, BudgetSizes::off());
        assert_eq!(
            BudgetSizes::resolve(None, None, None, Some(3), Some(8)).compactions,
            Some(3)
        );
    }

    #[test]
    fn permits_are_bounded_and_returned_on_drop() {
        let b = budget(one(BgPool::Split, 2));
        let p1 = b.try_acquire(BgPool::Split, Nanos(0), None).unwrap();
        let p2 = b.try_acquire(BgPool::Split, Nanos(0), None).unwrap();
        assert!(b.try_acquire(BgPool::Split, Nanos(0), None).is_none());
        assert_eq!(b.stats(BgPool::Split).inflight, 2);
        drop(p1);
        let p3 = b.try_acquire(BgPool::Split, Nanos(0), None).unwrap();
        assert_eq!(b.stats(BgPool::Split).hwm, 2);
        drop((p2, p3));
        assert_eq!(b.stats(BgPool::Split).inflight, 0, "no leak");
    }

    #[test]
    fn an_off_pool_is_unbounded() {
        let b = budget(BudgetSizes::off());
        let ps: Vec<_> = (0..50)
            .map(|_| b.try_acquire(BgPool::Catchup, Nanos(0), None).unwrap())
            .collect();
        assert_eq!(b.stats(BgPool::Catchup).inflight, 0);
        drop(ps);
    }

    #[test]
    fn size_follows_lag_and_never_reaches_zero() {
        let b = budget(one(BgPool::Compaction, 4));
        assert_eq!(b.stats(BgPool::Compaction).size_eff, Some(4));
        b.set_lag(40_000_000);
        assert_eq!(b.stats(BgPool::Compaction).size_eff, Some(2));
        b.set_lag(500_000_000);
        assert_eq!(b.stats(BgPool::Compaction).size_eff, Some(1));
        let b = budget(one(BgPool::Compaction, 1));
        b.set_lag(500_000_000);
        assert_eq!(b.stats(BgPool::Compaction).size_eff, Some(1), "minimum 1");
        let z = BackgroundBudget::with_fault(
            one(BgPool::Compaction, 1),
            MetricsHandle::recording(),
            BudgetFault::PoolMinZero,
        );
        z.set_lag(500_000_000);
        assert_eq!(z.stats(BgPool::Compaction).size_eff, Some(0));
    }

    #[test]
    fn an_idle_permit_expires_and_a_touched_one_does_not() {
        let b = budget(one(BgPool::SnapshotSend, 1));
        let ttl = Some(Duration::from_secs(1));
        let p = b.try_acquire(BgPool::SnapshotSend, Nanos(0), ttl).unwrap();
        assert!(
            b.try_acquire(BgPool::SnapshotSend, Nanos(500_000_000), ttl)
                .is_none()
        );
        p.touch(Nanos(900_000_000));
        assert!(
            b.try_acquire(BgPool::SnapshotSend, Nanos(1_500_000_000), ttl)
                .is_none()
        );
        // Idle since 0.9 s: gone by 2 s, and the holder sees it.
        let q = b.try_acquire(BgPool::SnapshotSend, Nanos(2_000_000_000), ttl);
        assert!(q.is_some());
        assert!(!p.alive_at(Nanos(2_000_000_000)));
        assert_eq!(b.stats(BgPool::SnapshotSend).expired, 1);
        drop(p); // a late drop of a reaped permit must not free the new one
        assert_eq!(b.stats(BgPool::SnapshotSend).inflight, 1);
    }

    #[test]
    fn a_bypass_is_granted_over_the_cap_and_counted() {
        let m = MetricsHandle::recording();
        let b = BackgroundBudget::new(one(BgPool::Compaction, 1), m.clone());
        let _p = b.try_acquire(BgPool::Compaction, Nanos(0), None).unwrap();
        let _q = b.acquire_bypass(BgPool::Compaction);
        let s = b.stats(BgPool::Compaction);
        assert_eq!((s.inflight, s.hwm, s.admit_hwm, s.bypassed), (2, 2, 1, 1));
        assert_eq!(m.get(Metric::BgCompactionOvercap), 1);
    }

    #[test]
    fn the_leak_fault_never_returns_a_slot() {
        let b = BackgroundBudget::with_fault(
            one(BgPool::Split, 1),
            MetricsHandle::recording(),
            BudgetFault::LeakPermits,
        );
        drop(b.try_acquire(BgPool::Split, Nanos(0), None));
        assert!(
            b.try_acquire(BgPool::Split, Nanos(u64::MAX / 2), None)
                .is_none()
        );
    }

    // --- BgFlow --------------------------------------------------------

    use animus_env::nid;

    fn snap(offset: u64) -> RaftMsg<KvCommand> {
        RaftMsg::InstallSnapshot {
            term: 1,
            leader: nid(0),
            last_index: 10,
            last_term: 1,
            offset,
            data: vec![0; 4],
            total: 8,
            done: false,
            config: None,
            learners: None,
        }
    }

    fn resp(last_index: u64, next_offset: u64) -> RaftMsg<KvCommand> {
        RaftMsg::InstallSnapshotResp {
            term: 1,
            last_index,
            next_offset,
        }
    }

    fn out(to: u64, m: RaftMsg<KvCommand>) -> Vec<(NodeId, KvWire)> {
        vec![(nid(to), KvWire::Raft(m))]
    }

    const HB: Duration = Duration::from_millis(50);

    fn at(ms: u64) -> Nanos {
        Nanos(ms * 1_000_000)
    }

    #[test]
    fn the_unacked_bound_is_two_steady_state_resend_gaps() {
        assert_eq!(SNAPSHOT_UNACKED_BOUND_TICKS, 64);
        assert_eq!(snapshot_unacked_bound(HB), Duration::from_millis(3_200));
        assert_eq!(snapshot_recv_idle_bound(HB), Duration::from_millis(6_400));
    }

    #[test]
    fn a_sender_releases_a_transfer_nobody_acknowledges_and_does_not_starve_the_next() {
        let b = budget(one(BgPool::SnapshotSend, 1));
        let mut f = BgFlow::new(b.clone());
        let bound = snapshot_unacked_bound(HB).as_millis() as u64;
        // t=0: peer 1 takes the only slot, peer 2 is refused (and counted once).
        assert_eq!(f.filter_outbound(out(1, snap(0)), at(0), HB, true).len(), 1);
        assert!(
            f.filter_outbound(out(2, snap(0)), at(10), HB, true)
                .is_empty()
        );
        assert!(
            f.filter_outbound(out(2, snap(0)), at(20), HB, true)
                .is_empty()
        );
        assert_eq!(b.stats(BgPool::SnapshotSend).deferred, 1);
        // Peer 1 resends keep flowing while its slot is live (no ack yet).
        assert_eq!(
            f.filter_outbound(out(1, snap(0)), at(bound - 1), HB, true)
                .len(),
            1,
            "still under the bound"
        );
        // ... but a resend is not progress: it does not renew the claim.
        assert!(
            f.filter_outbound(out(2, snap(0)), at(bound - 1), HB, true)
                .is_empty(),
            "peer 2 still waits just under the bound"
        );
        // At the bound peer 1's slot is released, peer 2 gets it.
        let sent = f.filter_outbound(
            [out(1, snap(0)), out(2, snap(0))].concat(),
            at(bound),
            HB,
            true,
        );
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, nid(2), "the waiting peer got the slot");
        assert_eq!(b.stats(BgPool::SnapshotSend).expired, 1);
        // Peer 1 sits out one more bound even if the slot frees.
        assert!(
            f.filter_outbound(
                out(2, resp(10, 8))
                    .into_iter()
                    .chain(out(1, snap(0)))
                    .collect(),
                at(bound + 5),
                HB,
                true
            )
            .iter()
            .all(|(to, _)| *to != nid(1))
        );
        // The ack of peer 2 frees its slot.
        assert!(f.admit_inbound(&nid(2), &resp(10, 8), at(bound + 10), HB, false));
        assert_eq!(f.sends_held(), 0);
        assert_eq!(b.stats(BgPool::SnapshotSend).inflight, 0);
        // After its cooldown peer 1 may ask again.
        assert_eq!(
            f.filter_outbound(out(1, snap(0)), at(2 * bound + 20), HB, true)
                .len(),
            1
        );
    }

    #[test]
    fn an_acknowledged_chunk_renews_the_claim() {
        let b = budget(one(BgPool::SnapshotSend, 1));
        let mut f = BgFlow::new(b.clone());
        let bound = snapshot_unacked_bound(HB).as_millis() as u64;
        assert_eq!(f.filter_outbound(out(1, snap(0)), at(0), HB, true).len(), 1);
        // A mid-transfer ack (next_offset > 0, nothing installed) every 3 s
        // keeps the slot for far longer than one bound.
        for k in 1..=5 {
            let t = k * (bound - 200);
            assert!(f.admit_inbound(&nid(1), &resp(0, 4 * k), at(t), HB, false));
            assert!(
                f.filter_outbound(out(2, snap(0)), at(t + 1), HB, true)
                    .is_empty(),
                "peer 2 must not displace a transfer that is making progress (k={k})"
            );
        }
        assert_eq!(b.stats(BgPool::SnapshotSend).expired, 0);
    }

    #[test]
    fn a_deposed_leader_returns_every_send_slot() {
        let b = budget(one(BgPool::SnapshotSend, 2));
        let mut f = BgFlow::new(b.clone());
        f.filter_outbound(out(1, snap(0)), at(0), HB, true);
        f.filter_outbound(out(2, snap(0)), at(0), HB, true);
        assert_eq!(b.stats(BgPool::SnapshotSend).inflight, 2);
        f.filter_outbound(Vec::new(), at(1), HB, false);
        assert_eq!(b.stats(BgPool::SnapshotSend).inflight, 0);
    }

    #[test]
    fn a_full_receive_pool_refuses_the_chunk_silently_until_an_install_ack_frees_it() {
        let b = budget(one(BgPool::SnapshotRecv, 2));
        let mut fs: Vec<BgFlow> = (0..3).map(|_| BgFlow::new(b.clone())).collect();
        let m = snap(0);
        assert!(fs[0].admit_inbound(&nid(0), &m, at(0), HB, false));
        assert!(fs[1].admit_inbound(&nid(0), &m, at(0), HB, false));
        assert!(
            !fs[2].admit_inbound(&nid(0), &m, at(1), HB, false),
            "refused"
        );
        assert!(
            !fs[2].admit_inbound(&nid(0), &m, at(2), HB, false),
            "still refused"
        );
        assert_eq!(b.stats(BgPool::SnapshotRecv).deferred, 1, "counted once");
        // A later chunk of an admitted transfer is not re-admitted, just served.
        assert!(fs[0].admit_inbound(&nid(0), &snap(4), at(3), HB, false));
        assert_eq!(b.stats(BgPool::SnapshotRecv).inflight, 2);
        // Group 0 finishes: its install ack goes out and returns the slot.
        fs[0].filter_outbound(out(0, resp(10, 8)), at(4), HB, false);
        assert!(fs[2].admit_inbound(&nid(0), &m, at(5), HB, false));
        // A receiver whose leader went silent loses its slot at the idle bound.
        let idle = snapshot_recv_idle_bound(HB).as_millis() as u64;
        let mut late = BgFlow::new(b.clone());
        assert!(!late.admit_inbound(&nid(0), &m, at(6), HB, false));
        assert!(late.admit_inbound(&nid(0), &m, at(6 + idle + 5), HB, false));
        assert!(b.stats(BgPool::SnapshotRecv).expired >= 1);
    }

    #[test]
    fn a_learner_needs_a_catchup_slot_for_any_leader_message_until_promoted() {
        let b = budget(one(BgPool::Catchup, 1));
        let mut a = BgFlow::new(b.clone());
        let mut c = BgFlow::new(b.clone());
        let ae = RaftMsg::AppendEntries {
            term: 1,
            leader: nid(0),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };
        assert!(a.admit_inbound(&nid(0), &ae, at(0), HB, true));
        assert!(
            !c.admit_inbound(&nid(0), &ae, at(1), HB, true),
            "second learner waits"
        );
        // A voter is never gated.
        assert!(c.admit_inbound(&nid(0), &ae, at(2), HB, false));
        // Promotion returns the slot.
        a.after_step(false);
        assert!(c.admit_inbound(&nid(0), &ae, at(3), HB, true));
    }

    #[test]
    fn dropping_a_flow_returns_everything_it_held() {
        let b = BackgroundBudget::new(
            BudgetSizes::resolve(None, None, None, None, None),
            MetricsHandle::recording(),
        );
        {
            let mut f = BgFlow::new(b.clone());
            f.filter_outbound(out(1, snap(0)), at(0), HB, true);
            f.admit_inbound(&nid(0), &snap(0), at(0), HB, true);
            assert!(
                b.stats(BgPool::SnapshotSend).inflight
                    + b.stats(BgPool::SnapshotRecv).inflight
                    + b.stats(BgPool::Catchup).inflight
                    >= 3
            );
        }
        for p in BgPool::ALL {
            assert_eq!(b.stats(p).inflight, 0, "{p:?} leaked");
        }
    }
}
