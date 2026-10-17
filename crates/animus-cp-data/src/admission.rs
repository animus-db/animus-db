//! The **leader propose bound** (ADR 0080 decision 1): per-group admission at
//! the data plane's one propose choke point, [`RaftKvNode::gated_propose`].
//!
//! A group's *backlog* is `last_log_index - engine_applied_index` (entries
//! appended but not yet merged into the engine: uncommitted plus
//! committed-but-unapplied) and the payload bytes of those entries (a small
//! ring of `(index, bytes)` trimmed by the apply watermark). When a
//! **client-origin** command would take either past its bound, the propose is
//! refused with `ProposeResult::Busy` *before* anything is appended, so a
//! `Busy` can never have taken effect.
//!
//! Three classes, an exhaustive match with no `_` arm
//! ([`KvCommand::admission_class`], the `required_gate` pattern: a new
//! variant does not compile until it is classified):
//!
//! - [`Client`](AdmissionClass::Client): bounded at the limit;
//! - [`Internal`](AdmissionClass::Internal) (TTL reaper deletes, index drain,
//!   MREC apply, change-log trim): bounded at **2x**, so housekeeping yields
//!   to clients;
//! - [`Liveness`](AdmissionClass::Liveness) (2PC decisions, `ReadCeiling`,
//!   `SeedBatch`, `Freeze`/`SplitTablet`, `NoOp`): **never refused**, because
//!   they are what drains the backlog. Refusing a resolve behind a refused
//!   stage is a deadlock.
//!
//! The effective limits follow the node's scheduling lag through the shared
//! [`scale_by_lag`](crate::load_monitor::scale_by_lag) rule (half at 30 ms, a
//! quarter at 100 ms, floors 8 entries / 256 KiB) and shrink by one more lag
//! step at the `QueuedBytes` soft watermark. A hot group may hold at most
//! `min(max_bytes, budget / 8)` of the budget.
//!
//! This module is pure (no `Env`, no clock): the node-level handle
//! [`NodeAdmission`] is a bundle of atomics fed by the `LoadMonitor`.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use animus_env::{MetricsHandle, QueuedBytes, Reservation};

use crate::load_monitor::{K_HALVE_AT, K_QUARTER_AT, reading, scale_by_lag};
use crate::{KindEvalEntry, KindEvalOp, KvCommand, txn};

/// Default `propose_backlog_max_entries` (ADR 0080 decision 1).
pub const DEFAULT_BACKLOG_MAX_ENTRIES: u64 = 512;
/// Default `propose_backlog_max_bytes`: 4 MiB per group.
pub const DEFAULT_BACKLOG_MAX_BYTES: u64 = 4 << 20;
/// Lag-scaled floor for the entry bound.
pub const MIN_BACKLOG_ENTRIES: u64 = 8;
/// Lag-scaled floor for the byte bound.
pub const MIN_BACKLOG_BYTES: u64 = 256 << 10;
/// The `Internal` class is bounded at this multiple of the limit.
pub const INTERNAL_MULTIPLIER: u64 = 2;
/// The lag-scaled node gate's floor (ADR 0080 section 2a).
pub const NODE_GATE_FLOOR: usize = 256;

/// How a command is admitted at the propose bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AdmissionClass {
    /// Client-origin work: refused at the limit.
    Client,
    /// Housekeeping: refused at 2x the limit.
    Internal,
    /// What drains the backlog: never refused.
    Liveness,
}

impl KvCommand {
    /// This command's [`AdmissionClass`]. **Exhaustive, no `_` arm.**
    ///
    /// `KindEval`/`KindEvalBatch` are content-dependent: a TTL-reaper delete
    /// (`ttl_expired`) or an MREC apply (`KindEvalOp::Replicate`) is
    /// `Internal`; any other evaluated write is `Client`. `KindBatch` is
    /// `Internal` (its remaining producers are the restore driver, index
    /// backfill and the TTL/backup-cursor bookkeeping writers; client writes
    /// are `KindEval`/`KindEvalBatch`/`TxnStage` since ADR 0054). The raw
    /// `Put`/`Batch`/`Delete`/`Cas` are `Client` (the conservative choice: a
    /// refusal only means "retry later").
    #[must_use]
    pub fn admission_class(&self) -> AdmissionClass {
        match self {
            KvCommand::Put { .. }
            | KvCommand::Batch { .. }
            | KvCommand::Delete { .. }
            | KvCommand::Cas { .. }
            | KvCommand::TxnStage { .. } => AdmissionClass::Client,
            KvCommand::KindEval {
                op, ttl_expired, ..
            } => eval_class(op, *ttl_expired),
            KvCommand::KindEvalBatch { entries, .. } => entries
                .iter()
                .map(entry_class)
                .min()
                .unwrap_or(AdmissionClass::Internal),
            KvCommand::KindBatch { .. } => AdmissionClass::Internal,
            KvCommand::SeedBatch { .. }
            | KvCommand::Freeze { .. }
            | KvCommand::SplitTablet { .. }
            | KvCommand::ReadCeiling { .. }
            | KvCommand::TxnCommit { .. }
            | KvCommand::TxnAbort { .. }
            | KvCommand::TxnCommitSealChecked { .. }
            | KvCommand::TxnAbortSealChecked { .. }
            | KvCommand::TxnResolve { .. }
            | KvCommand::NoOp => AdmissionClass::Liveness,
        }
    }

    /// A cheap estimate of the entry's payload bytes (key and value lengths
    /// plus a fixed overhead per evaluated write), used for the backlog byte
    /// bound. Never serializes. Exhaustive, no `_` arm.
    #[must_use]
    pub fn approx_bytes(&self) -> u64 {
        const OVERHEAD: u64 = 64;
        const EVAL_OVERHEAD: u64 = 256;
        let n: usize = match self {
            KvCommand::Put { key, value, .. } => key.len() + value.len(),
            KvCommand::Batch { puts, .. } => puts.iter().map(|(k, v)| k.len() + v.len()).sum(),
            KvCommand::KindBatch {
                writes, change_log, ..
            } => {
                kind_writes_len(writes)
                    + change_log
                        .iter()
                        .map(|(k, v)| k.len() + v.len())
                        .sum::<usize>()
            }
            KvCommand::KindEval { op, .. } => EVAL_OVERHEAD as usize + op_len(op),
            KvCommand::KindEvalBatch { entries, .. } => entries
                .iter()
                .map(|e| EVAL_OVERHEAD as usize + op_len(&e.op))
                .sum(),
            KvCommand::SeedBatch { rows, .. } => rows
                .iter()
                .map(|(_, k, v, _)| k.len() + v.as_ref().map_or(0, Vec::len) + 9)
                .sum(),
            KvCommand::Delete { key, .. } => key.len(),
            KvCommand::Cas {
                key,
                expected,
                value,
                ..
            } => key.len() + expected.as_ref().map_or(0, Vec::len) + value.len(),
            KvCommand::SplitTablet { split_key, .. } => split_key.len() + 256,
            KvCommand::TxnStage {
                record_key,
                writes,
                spans,
                conditions,
                ..
            } => {
                record_key.len()
                    + spans.iter().map(|(t, _)| t.len() + 64).sum::<usize>()
                    + conditions
                        .iter()
                        .map(|(k, v)| k.len() + v.as_ref().map_or(0, Vec::len))
                        .sum::<usize>()
                    + writes.iter().map(txn_write_len).sum::<usize>()
            }
            KvCommand::TxnResolve {
                record_key, keys, ..
            } => record_key.len() + keys.iter().map(Vec::len).sum::<usize>(),
            KvCommand::TxnCommit { record_key, .. }
            | KvCommand::TxnAbort { record_key, .. }
            | KvCommand::TxnCommitSealChecked { record_key, .. }
            | KvCommand::TxnAbortSealChecked { record_key, .. } => record_key.len(),
            KvCommand::Freeze { .. } | KvCommand::ReadCeiling { .. } | KvCommand::NoOp => 0,
        };
        n as u64 + OVERHEAD
    }
}

fn eval_class(op: &KindEvalOp, ttl_expired: bool) -> AdmissionClass {
    if ttl_expired || matches!(op, KindEvalOp::Replicate { .. }) {
        AdmissionClass::Internal
    } else {
        AdmissionClass::Client
    }
}

fn entry_class(e: &KindEvalEntry) -> AdmissionClass {
    eval_class(&e.op, e.ttl_expired)
}

fn kind_writes_len(writes: &[crate::KindWrite]) -> usize {
    writes
        .iter()
        .map(|(_, k, v)| k.len() + v.as_ref().map_or(0, Vec::len) + 1)
        .sum()
}

fn op_len(op: &KindEvalOp) -> usize {
    match op {
        KindEvalOp::Put(item) => animus_item::item_size(item),
        KindEvalOp::Delete => 0,
        KindEvalOp::Update { key_item, actions } => {
            animus_item::item_size(key_item) + 64 * actions.len()
        }
        KindEvalOp::Replicate { item, .. } => item.as_ref().map_or(0, animus_item::item_size),
    }
}

fn txn_write_len(w: &txn::TxnWrite) -> usize {
    w.key.len()
        + w.value.as_ref().map_or(0, Vec::len)
        + kind_writes_len(&w.kind_writes)
        + w.change_log
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>()
        + w.stage_marker
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>()
        + w.pending.as_ref().map_or(0, |p| 256 + op_len(&p.op))
}

/// The configured propose-bound limits. `0` disables the matching bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BacklogLimits {
    pub max_entries: u64,
    pub max_bytes: u64,
}

impl Default for BacklogLimits {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_BACKLOG_MAX_ENTRIES,
            max_bytes: DEFAULT_BACKLOG_MAX_BYTES,
        }
    }
}

impl BacklogLimits {
    /// Both bounds off (the pre-ADR-0080 behaviour).
    #[must_use]
    pub const fn off() -> Self {
        Self {
            max_entries: 0,
            max_bytes: 0,
        }
    }

    /// Whether any bound is on.
    #[must_use]
    pub const fn is_on(&self) -> bool {
        self.max_entries != 0 || self.max_bytes != 0
    }
}

/// Why a client reservation or admission was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BusyReason {
    /// The group's backlog is at its entry or byte bound.
    Backlog,
    /// The node `QueuedBytes` budget is at 100 %.
    Budget,
}

/// The node-level propose-admission handle: the configured limits, the
/// lag the `LoadMonitor` feeds, and the node `QueuedBytes` budget. Cloning is
/// an `Arc` bump.
#[derive(Clone)]
pub struct NodeAdmission {
    inner: Arc<Inner>,
}

struct Inner {
    limits: BacklogLimits,
    /// The raw smoothed lag (nanoseconds), fed by the `LoadMonitor`.
    lag: AtomicU64,
    budget: QueuedBytes,
    metrics: MetricsHandle,
}

impl std::fmt::Debug for NodeAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeAdmission")
            .field("limits", &self.inner.limits)
            .field("lag", &self.lag())
            .finish()
    }
}

impl NodeAdmission {
    #[must_use]
    pub fn new(limits: BacklogLimits, budget: QueuedBytes, metrics: MetricsHandle) -> Self {
        Self {
            inner: Arc::new(Inner {
                limits,
                lag: AtomicU64::new(0),
                budget,
                metrics,
            }),
        }
    }

    /// Fed by the `LoadMonitor` after every sample (raw, nanoseconds).
    pub fn set_lag(&self, lag: u64) {
        self.inner.lag.store(lag, Ordering::Relaxed);
    }

    /// The raw smoothed lag the monitor last published (nanoseconds).
    #[must_use]
    pub fn lag(&self) -> u64 {
        self.inner.lag.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn budget(&self) -> &QueuedBytes {
        &self.inner.budget
    }

    #[must_use]
    pub fn limits(&self) -> BacklogLimits {
        self.inner.limits
    }

    #[must_use]
    pub fn metrics(&self) -> &MetricsHandle {
        &self.inner.metrics
    }

    /// The lag the caps use: the monitor's lag, advanced by **one lag step**
    /// when the `QueuedBytes` budget is past its soft watermark (ADR 0080
    /// section 4: "as if lag were 30 ms").
    #[must_use]
    pub fn effective_lag(&self) -> u64 {
        let lag = self.lag();
        if self.inner.budget.over_soft() {
            step_lag(lag)
        } else {
            lag
        }
    }

    /// The effective limits now: configured, lag-scaled with floors, and
    /// the byte bound capped at `budget / 8`.
    #[must_use]
    pub fn effective(&self) -> BacklogLimits {
        let lag = self.effective_lag();
        let l = self.inner.limits;
        let entries = if l.max_entries == 0 {
            0
        } else {
            (scale_by_lag(l.max_entries as usize, lag) as u64)
                .max(MIN_BACKLOG_ENTRIES.min(l.max_entries))
        };
        let bytes = if l.max_bytes == 0 {
            0
        } else {
            let capped = self.inner.budget.per_group_cap(l.max_bytes);
            (scale_by_lag(capped as usize, lag) as u64).max(MIN_BACKLOG_BYTES.min(capped))
        };
        BacklogLimits {
            max_entries: entries,
            max_bytes: bytes,
        }
    }

    /// The node gate's effective size (ADR 0080 section 2a): `configured`
    /// scaled by the same lag rule as `K_eff`/`A_eff` (half at 30 ms, a quarter
    /// at 100 ms) and never below [`NODE_GATE_FLOOR`] (or `configured` itself
    /// when that is smaller). `0` stays `0` (gate off is not a thing here: the
    /// caller never passes it).
    #[must_use]
    pub fn node_gate(&self, configured: usize) -> usize {
        if configured == 0 {
            return 0;
        }
        scale_by_lag(configured, self.effective_lag()).max(NODE_GATE_FLOOR.min(configured))
    }

    /// Decide whether a command of `class` and `new_bytes` may be appended to
    /// a group whose backlog is `entries` / `bytes`. Pure.
    pub fn verdict(
        &self,
        class: AdmissionClass,
        entries: u64,
        bytes: u64,
        new_bytes: u64,
    ) -> Result<(), BusyReason> {
        let mult = match class {
            AdmissionClass::Liveness => return Ok(()),
            AdmissionClass::Internal => INTERNAL_MULTIPLIER,
            AdmissionClass::Client => 1,
        };
        let e = self.effective();
        if e.max_entries != 0 && entries >= e.max_entries.saturating_mul(mult) {
            return Err(BusyReason::Backlog);
        }
        // A command alone larger than the byte bound is admitted into an empty
        // group, so an oversized item is not refused forever.
        if e.max_bytes != 0
            && bytes > 0
            && bytes.saturating_add(new_bytes) > e.max_bytes.saturating_mul(mult)
        {
            return Err(BusyReason::Backlog);
        }
        Ok(())
    }

    /// Charge `bytes` to the node budget for a command of `class`: a client
    /// class is refused at 100 %; `Internal` and `Liveness` are always charged.
    pub fn reserve(&self, class: AdmissionClass, bytes: u64) -> Result<Reservation, BusyReason> {
        match class {
            AdmissionClass::Client => self
                .inner
                .budget
                .try_reserve(bytes)
                .ok_or(BusyReason::Budget),
            AdmissionClass::Internal | AdmissionClass::Liveness => {
                Ok(self.inner.budget.force_reserve(bytes))
            }
        }
    }
}

/// One lag step worse than `lag`: below 30 ms reads as 30 ms, from 30 ms
/// reads as 100 ms, from 100 ms unchanged (the shared rule's last step).
#[must_use]
pub fn step_lag(lag: u64) -> u64 {
    let r = u128::from(reading(lag));
    if r >= K_QUARTER_AT.as_nanos() {
        lag
    } else if r >= K_HALVE_AT.as_nanos() {
        u64::try_from(K_QUARTER_AT.as_nanos()).unwrap_or(u64::MAX)
    } else {
        u64::try_from(K_HALVE_AT.as_nanos()).unwrap_or(u64::MAX)
    }
}

/// One entry of the ring: the log `index`, its estimated bytes, and the
/// budget charge held until the apply watermark passes `index`.
struct Slot {
    index: u64,
    bytes: u64,
    _charge: Reservation,
}

/// The leader's **uncommitted-plus-unapplied byte ring** for one group: a
/// small queue of `(index, bytes)` trimmed by the apply watermark. Entry
/// count needs no ring (`last_log_index - applied`); bytes do.
#[derive(Default)]
pub struct ProposeBacklog {
    ring: VecDeque<Slot>,
    bytes: u64,
    entries_hwm: u64,
    bytes_hwm: u64,
}

impl std::fmt::Debug for ProposeBacklog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProposeBacklog")
            .field("slots", &self.ring.len())
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl ProposeBacklog {
    /// Drop every slot at or below the apply watermark (releasing its charge).
    pub fn trim(&mut self, applied: u64) {
        while let Some(front) = self.ring.front() {
            if front.index > applied {
                break;
            }
            self.bytes -= front.bytes;
            self.ring.pop_front();
        }
    }

    /// The tracked payload bytes not yet applied.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Slots in the ring (test visibility).
    #[must_use]
    pub fn slots(&self) -> usize {
        self.ring.len()
    }

    /// Record an accepted entry. `index` must exceed every recorded index
    /// (a leader appends monotonically); a lower one (the log was truncated
    /// by a leadership change in between) first discards the stale tail.
    pub fn record(&mut self, index: u64, bytes: u64, charge: Reservation) {
        while let Some(back) = self.ring.back() {
            if back.index < index {
                break;
            }
            self.bytes -= back.bytes;
            self.ring.pop_back();
        }
        self.bytes += bytes;
        self.ring.push_back(Slot {
            index,
            bytes,
            _charge: charge,
        });
    }

    /// Fold `entries` / `bytes` into this group's high-water marks and
    /// return the marks `(entries_hwm, bytes_hwm)`.
    pub fn note_hwm(&mut self, entries: u64, bytes: u64) -> (u64, u64) {
        self.entries_hwm = self.entries_hwm.max(entries);
        self.bytes_hwm = self.bytes_hwm.max(bytes);
        (self.entries_hwm, self.bytes_hwm)
    }

    /// Drop everything (this node stopped leading and the tail is moot).
    pub fn clear(&mut self) {
        self.ring.clear();
        self.bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_env::QueuedBytes;

    const MS: u64 = 1_000_000;

    fn adm(limits: BacklogLimits, budget: u64) -> NodeAdmission {
        NodeAdmission::new(limits, QueuedBytes::new(budget), MetricsHandle::recording())
    }

    fn charge(a: &NodeAdmission, n: u64) -> Reservation {
        a.reserve(AdmissionClass::Client, n).expect("budget")
    }

    #[test]
    fn the_node_gate_follows_the_lag_steps_and_keeps_its_floor() {
        let a = adm(BacklogLimits::default(), 0);
        assert_eq!(a.node_gate(2048), 2048);
        a.set_lag(30 * MS);
        assert_eq!(a.node_gate(2048), 1024);
        a.set_lag(100 * MS);
        assert_eq!(a.node_gate(2048), 512);
        a.set_lag(10_000 * MS);
        assert!(a.node_gate(2048) >= NODE_GATE_FLOOR);
        // A configured gate below the floor is not raised above itself.
        assert_eq!(a.node_gate(100), 100);
    }

    #[test]
    fn the_budget_soft_watermark_shrinks_the_node_gate_one_step() {
        let a = adm(BacklogLimits::default(), 1000);
        let _hold = a.budget().force_reserve(700);
        assert_eq!(a.node_gate(2048), 1024, "as if lag were 30 ms");
    }

    #[test]
    fn client_is_refused_at_the_entry_bound_internal_at_double_liveness_never() {
        let a = adm(
            BacklogLimits {
                max_entries: 10,
                max_bytes: 0,
            },
            0,
        );
        assert!(a.verdict(AdmissionClass::Client, 9, 0, 1).is_ok());
        assert_eq!(
            a.verdict(AdmissionClass::Client, 10, 0, 1),
            Err(BusyReason::Backlog)
        );
        assert!(a.verdict(AdmissionClass::Internal, 19, 0, 1).is_ok());
        assert!(a.verdict(AdmissionClass::Internal, 20, 0, 1).is_err());
        for n in [0, 10, 20, 10_000_000] {
            assert!(
                a.verdict(AdmissionClass::Liveness, n, u64::MAX / 2, 1 << 30)
                    .is_ok(),
                "a liveness command is never refused"
            );
        }
    }

    #[test]
    fn the_byte_bound_refuses_but_admits_an_oversized_command_into_an_empty_group() {
        let a = adm(
            BacklogLimits {
                max_entries: 0,
                max_bytes: 1 << 20,
            },
            0,
        );
        assert!(a.verdict(AdmissionClass::Client, 0, 0, 5 << 20).is_ok());
        assert!(a.verdict(AdmissionClass::Client, 1, 1, 5 << 20).is_err());
        assert!(
            a.verdict(AdmissionClass::Client, 1, 600 << 10, 400 << 10)
                .is_ok()
        );
        assert!(
            a.verdict(AdmissionClass::Client, 1, 600 << 10, 500 << 10)
                .is_err()
        );
    }

    #[test]
    fn disabled_bounds_admit_everything() {
        let a = adm(BacklogLimits::off(), 0);
        assert!(!a.limits().is_on());
        assert!(
            a.verdict(AdmissionClass::Client, u64::MAX / 2, u64::MAX / 2, 1)
                .is_ok()
        );
    }

    #[test]
    fn limits_follow_the_shared_lag_rule_with_floors() {
        let a = adm(BacklogLimits::default(), 0);
        assert_eq!(a.effective().max_entries, 512);
        a.set_lag(30 * MS);
        assert_eq!(a.effective().max_entries, 256);
        assert_eq!(a.effective().max_bytes, 2 << 20);
        a.set_lag(100 * MS);
        assert_eq!(a.effective().max_entries, 128);
        assert_eq!(a.effective().max_bytes, 1 << 20);
        // Floors.
        let small = adm(
            BacklogLimits {
                max_entries: 16,
                max_bytes: 512 << 10,
            },
            0,
        );
        small.set_lag(500 * MS);
        assert_eq!(small.effective().max_entries, 8);
        assert_eq!(small.effective().max_bytes, 256 << 10);
        // A limit configured below its floor is not raised.
        let tiny = adm(
            BacklogLimits {
                max_entries: 4,
                max_bytes: 1000,
            },
            0,
        );
        tiny.set_lag(500 * MS);
        assert_eq!(tiny.effective().max_entries, 4);
        assert_eq!(tiny.effective().max_bytes, 1000);
        // Monotone non-increasing in lag.
        let mut prev = u64::MAX;
        for l in (0..300).map(|i| i * MS) {
            a.set_lag(l);
            let e = a.effective().max_entries;
            assert!(e <= prev);
            prev = e;
        }
    }

    #[test]
    fn the_soft_watermark_shrinks_by_one_lag_step() {
        let a = adm(BacklogLimits::default(), 1000);
        assert_eq!(a.effective().max_entries, 512);
        let _hold = a.budget().force_reserve(700);
        assert!(a.budget().over_soft());
        assert_eq!(a.effective().max_entries, 256, "as if lag were 30 ms");
        a.set_lag(30 * MS);
        assert_eq!(a.effective().max_entries, 128, "30 ms steps to 100 ms");
        a.set_lag(200 * MS);
        assert_eq!(a.effective().max_entries, 128, "100 ms is the last step");
    }

    #[test]
    fn a_hot_group_holds_at_most_an_eighth_of_the_budget() {
        let a = adm(BacklogLimits::default(), 8 << 20);
        assert_eq!(a.effective().max_bytes, 1 << 20, "min(4 MiB, B/8)");
    }

    #[test]
    fn the_budget_refuses_client_class_only() {
        let a = adm(BacklogLimits::default(), 100);
        let _full = a.reserve(AdmissionClass::Client, 100).expect("fills");
        assert_eq!(
            a.reserve(AdmissionClass::Client, 1).err(),
            Some(BusyReason::Budget)
        );
        let _i = a.reserve(AdmissionClass::Internal, 50).expect("internal");
        let _l = a.reserve(AdmissionClass::Liveness, 50).expect("liveness");
        assert_eq!(a.budget().used(), 200);
    }

    #[test]
    fn the_ring_trims_by_the_apply_watermark_and_releases_its_charge() {
        let a = adm(BacklogLimits::default(), 1 << 20);
        let mut r = ProposeBacklog::default();
        for i in 1..=10u64 {
            r.record(i, 100, charge(&a, 100));
        }
        assert_eq!((r.slots(), r.bytes(), a.budget().used()), (10, 1000, 1000));
        r.trim(4);
        assert_eq!((r.slots(), r.bytes(), a.budget().used()), (6, 600, 600));
        r.trim(4);
        assert_eq!(r.slots(), 6, "idempotent");
        r.trim(100);
        assert_eq!((r.slots(), r.bytes(), a.budget().used()), (0, 0, 0));
    }

    #[test]
    fn a_lower_index_discards_the_stale_truncated_tail() {
        let a = adm(BacklogLimits::default(), 1 << 20);
        let mut r = ProposeBacklog::default();
        for i in 5..=9u64 {
            r.record(i, 10, charge(&a, 10));
        }
        // A new leader's log reused 7..: the old 7, 8, 9 were truncated.
        r.record(7, 25, charge(&a, 25));
        assert_eq!((r.slots(), r.bytes()), (3, 20 + 25));
        assert_eq!(a.budget().used(), 45);
    }

    #[test]
    fn busy_refusal_leaves_no_trace_in_the_ring() {
        // The gated_propose contract: verdict first, reserve second, record
        // only on Accepted. A refused verdict touches neither.
        let a = adm(
            BacklogLimits {
                max_entries: 2,
                max_bytes: 0,
            },
            1 << 20,
        );
        let r = ProposeBacklog::default();
        assert!(a.verdict(AdmissionClass::Client, 2, r.bytes(), 10).is_err());
        assert_eq!((r.slots(), a.budget().used()), (0, 0));
    }

    #[test]
    fn step_lag_walks_the_three_regimes() {
        assert_eq!(step_lag(0), 30 * MS);
        assert_eq!(step_lag(5 * MS), 30 * MS, "dead band reads zero");
        assert_eq!(step_lag(40 * MS), 100 * MS);
        assert_eq!(step_lag(150 * MS), 150 * MS);
    }
}
