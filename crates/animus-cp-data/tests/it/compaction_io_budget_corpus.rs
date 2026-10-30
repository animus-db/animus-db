//! ADR 0081 Tier 1 (decisions 3 and 4): the node-wide **compaction write-bytes
//! budget** over real `LsmEngine<SimEnv>` engines sharing one
//! [`BackgroundBudget`]. Knobs: `ANIMUS_LSM_STALL_SEEDS=K` (default 1; shared
//! with `animus-storage`'s `lsm_write_stall`), `ANIMUS_LSM_STALL_CELL=<substring>`,
//! `ANIMUS_SEED=<seed>`.
//!
//! Cells: 7 N engines share one budget (the aggregate non-urgent bytes stay
//! within `rate x T + burst + one compaction`, an urgent engine is never held
//! back by other engines' debt); 8 the flush-first `MaintenanceLock` order
//! (unit-tested beside the lock in `animus-storage`) and, here, flush wait
//! bounded under a throttled budget; 9 a throttled compaction defers and ends
//! the maintenance task (flushes keep running while it waits); 10 lag scaling
//! (the pure bucket tests plus the budget-level one at the end of this file).
//!
//! Negative controls (each asserts its oracle FAILS): (d) no urgent bypass,
//! (e) the pool permit held while waiting for credit, (f) a flush gated by the
//! bucket, (g) the wait inside the compaction, holding the maintenance lock.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::background_budget::{BackgroundBudget, BudgetSizes};
use animus_env::{Clock, EnvExt, MetricsHandle, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{
    CompactionGate, CompactionPermit, IoAdmission, LsmEngine, LsmOptions, StallFault, StorageEngine,
};
use futures::executor::block_on;

const TRIGGER: usize = 2;
/// Urgent level (`2 x trigger`) and the slack a still-writing engine adds.
const URGENT_L0: usize = 2 * TRIGGER;

fn opts(fault: StallFault) -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 256,
        compaction_trigger: TRIGGER,
        target_table_bytes: 1024,
        level_fanout: 2,
        wal_segment_bytes: 1 << 20,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: true,
        background_maintenance: true,
        l0_slowdown_factor: 3,
        l0_stop_factor: 5,
        stall_fault: fault,
        ..LsmOptions::default()
    }
}

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
    {
        return vec![s];
    }
    let k: u64 = std::env::var("ANIMUS_LSM_STALL_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    animus_test::corpus::shard_retain((0..k.max(1)).map(|i| 0x1081 + i).collect())
}

fn cell_enabled(name: &str) -> bool {
    std::env::var("ANIMUS_LSM_STALL_CELL").map_or(true, |f| name.contains(&f))
}

/// Counts what the engines hand the gate, and tracks permits held against
/// compactions actually running (for control (e)).
struct Meter {
    inner: Arc<dyn CompactionGate>,
    /// Estimated bytes admitted to non-urgent / urgent requests.
    nonurgent_est: AtomicU64,
    urgent_est: AtomicU64,
    max_est: AtomicU64,
    urgent_deferred: AtomicU64,
    deferred: AtomicU64,
    /// Permits outstanding and compactions between `io_admit` and `io_settle`.
    held: Arc<AtomicU64>,
    running: AtomicU64,
    /// Largest `held - running` observed by a sampler (violations of (e)).
    held_idle_max: AtomicU64,
}

struct CountedPermit {
    held: Arc<AtomicU64>,
    _inner: Option<CompactionPermit>,
}
impl Drop for CountedPermit {
    fn drop(&mut self) {
        self.held.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Meter {
    fn sample(&self) {
        let held = self.held.load(Ordering::Acquire);
        let running = self.running.load(Ordering::Acquire);
        if held > running {
            self.held_idle_max
                .fetch_max(held - running, Ordering::Relaxed);
        }
    }
}

impl CompactionGate for Meter {
    fn acquire(&self, bypass: bool) -> Option<CompactionPermit> {
        let p = self.inner.acquire(bypass)?;
        self.held.fetch_add(1, Ordering::AcqRel);
        Some(Box::new(CountedPermit {
            held: self.held.clone(),
            _inner: Some(p),
        }))
    }
    fn io_admit(&self, est: u64, urgent: bool) -> IoAdmission {
        let r = self.inner.io_admit(est, urgent);
        match (r, urgent) {
            (IoAdmission::Admitted, true) => {
                self.urgent_est.fetch_add(est, Ordering::Relaxed);
                self.running.fetch_add(1, Ordering::AcqRel);
            }
            (IoAdmission::Admitted, false) => {
                self.nonurgent_est.fetch_add(est, Ordering::Relaxed);
                self.max_est.fetch_max(est, Ordering::Relaxed);
                self.running.fetch_add(1, Ordering::AcqRel);
            }
            (IoAdmission::Defer, u) => {
                self.deferred.fetch_add(1, Ordering::Relaxed);
                if u {
                    self.urgent_deferred.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        r
    }
    fn io_settle(&self, est: u64, actual: u64) {
        self.running.fetch_sub(1, Ordering::AcqRel);
        self.inner.io_settle(est, actual);
    }
    fn io_charge(&self, bytes: u64) {
        self.inner.io_charge(bytes);
    }
}

struct Rig {
    sim: Simulator,
    engines: Vec<LsmEngine<SimEnv>>,
    meter: Arc<Meter>,
    #[allow(dead_code)]
    budget: BackgroundBudget,
    acked: Arc<AtomicU64>,
    errs: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
}

struct RigCfg {
    engines: usize,
    pool: Option<usize>,
    rate: u64,
    sync_ms: u64,
    val_len: usize,
    fault: StallFault,
}

fn rig(seed: u64, c: &RigCfg) -> Rig {
    let sim = Simulator::new(seed);
    let env0 = sim.env(nid(0));
    let mut sizes = BudgetSizes::off();
    sizes.compactions = c.pool;
    sizes.compaction_io_bytes_per_sec = c.rate;
    let budget = BackgroundBudget::new(sizes, MetricsHandle::recording());
    let clk = env0.clone();
    let inner = budget
        .compaction_gate(Arc::new(move || clk.now()))
        .expect("pool or budget on");
    let meter = Arc::new(Meter {
        inner,
        nonurgent_est: AtomicU64::new(0),
        urgent_est: AtomicU64::new(0),
        max_est: AtomicU64::new(0),
        urgent_deferred: AtomicU64::new(0),
        deferred: AtomicU64::new(0),
        held: Arc::new(AtomicU64::new(0)),
        running: AtomicU64::new(0),
        held_idle_max: AtomicU64::new(0),
    });
    let engines: Vec<LsmEngine<SimEnv>> = (0..c.engines)
        .map(|i| {
            let e = block_on(LsmEngine::open_with(
                env0.clone(),
                &format!("t{i}/"),
                opts(c.fault),
            ))
            .expect("open");
            e.set_compaction_gate(meter.clone());
            e
        })
        .collect();
    let mut cfg = DiskConfig::default();
    cfg.set_sync_delay(Duration::from_millis(c.sync_ms));
    sim.set_disk_config(cfg);
    let ver = Arc::new(AtomicU64::new(0));
    let acked = Arc::new(AtomicU64::new(0));
    let errs = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    for (i, e) in engines.iter().enumerate() {
        let (e, ver, acked, errs, stop) = (
            e.clone(),
            ver.clone(),
            acked.clone(),
            errs.clone(),
            stop.clone(),
        );
        let env = sim.env(nid(0));
        let vl = c.val_len;
        env.clone().spawn_task(async move {
            let mut n = 0u64;
            while !stop.load(Ordering::Acquire) {
                let v = ver.fetch_add(1, Ordering::AcqRel) + 1;
                match e
                    .merge(format!("e{i}-{n:07}").as_bytes(), &vec![5u8; vl], v)
                    .await
                {
                    Ok(_) => {
                        acked.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(x) => {
                        errs.lock().unwrap().push(x.to_string());
                        return;
                    }
                }
                n += 1;
                env.sleep(Duration::from_millis(1)).await;
            }
        });
    }
    Rig {
        sim,
        engines,
        meter,
        budget,
        acked,
        errs,
        stop,
    }
}

struct Observed {
    l0_hwm: usize,
}

impl Rig {
    fn run(&mut self, secs: u64) -> Observed {
        let mut l0_hwm = 0;
        for _ in 0..(secs * 100) {
            self.sim.run_for(Duration::from_millis(10));
            self.meter.sample();
            for e in &self.engines {
                l0_hwm = l0_hwm.max(e.l0_table_count());
            }
        }
        Observed { l0_hwm }
    }
}

const KIB: u64 = 1024;

// ---------------------------------------------------------------------------
// Cell 7: N engines share one budget.
// ---------------------------------------------------------------------------

fn cell7(seed: u64, fault: StallFault) -> (Result<(), String>, Rig, u64) {
    let rate = 2 * KIB; // far below what the engines want to compact
    let secs = 25;
    let mut r = rig(
        seed,
        &RigCfg {
            engines: 8,
            pool: Some(2),
            rate,
            sync_ms: 3,
            val_len: 100,
            fault,
        },
    );
    let obs = r.run(secs);
    let verdict = (|| {
        if let Some(e) = r.errs.lock().unwrap().first() {
            return Err(format!("a write returned Err: {e}"));
        }
        // The aggregate (non-urgent) compaction write rate stays within
        // rate x T + burst (one second of rate) + one compaction.
        let bound = rate * secs + rate + r.meter.max_est.load(Ordering::Relaxed);
        let got = r.meter.nonurgent_est.load(Ordering::Relaxed);
        if got > bound {
            return Err(format!(
                "non-urgent bytes {got} exceed the budget bound {bound}"
            ));
        }
        // An urgent engine is never delayed by the debt of the others: no
        // urgent request was ever deferred, and L0 never climbed past the
        // urgent level plus the in-flight slack.
        let deferred_urgent = r.meter.urgent_deferred.load(Ordering::Relaxed);
        if deferred_urgent > 0 {
            return Err(format!(
                "{deferred_urgent} urgent compactions were deferred"
            ));
        }
        let slack = URGENT_L0 + 2;
        if obs.l0_hwm > slack {
            return Err(format!(
                "an engine's L0 reached {} > urgent level {URGENT_L0} + slack",
                obs.l0_hwm
            ));
        }
        // The mechanism bit: non-urgent engines were actually held back.
        if r.meter.deferred.load(Ordering::Relaxed) == 0 {
            return Err("no compaction was ever deferred by the budget".into());
        }
        Ok(())
    })();
    (verdict, r, bound_for(rate, secs))
}

fn bound_for(rate: u64, secs: u64) -> u64 {
    rate * secs + rate
}

#[test]
fn cell7_engines_share_one_budget_and_urgent_is_never_held_back() {
    if !cell_enabled("cell7") {
        return;
    }
    for seed in seeds() {
        let (v, r, _) = cell7(seed, StallFault::None);
        assert!(v.is_ok(), "seed {seed}: {}", v.unwrap_err());
        assert!(
            r.acked.load(Ordering::Relaxed) > 100,
            "seed {seed}: writers made no progress"
        );
        // Everything acked is still readable.
        for (i, e) in r.engines.iter().enumerate() {
            assert!(
                block_on(e.get(format!("e{i}-0000000").as_bytes()))
                    .unwrap()
                    .is_some(),
                "seed {seed}: first write of engine {i} lost"
            );
        }
        r.stop.store(true, Ordering::Release);
    }
}

/// Control (d): the bucket applies to every compaction (no urgent bypass), so
/// at a rate far below the write rate L0 climbs past the urgent level and the
/// "an urgent engine is never held back" oracle FAILS.
#[test]
fn control_d_no_urgent_bypass_lets_l0_run_past_the_urgent_level() {
    if !cell_enabled("control_d") {
        return;
    }
    for seed in seeds() {
        let (v, _r, _) = cell7(seed, StallFault::NoUrgentBypass);
        let why = v.expect_err(&format!("seed {seed}: control (d) must fail the oracle"));
        assert!(
            why.contains("L0 reached") || why.contains("urgent compactions were deferred"),
            "seed {seed}: failed for the wrong reason: {why}"
        );
    }
}

// ---------------------------------------------------------------------------
// Control (e): the pool permit held while waiting for credit.
// ---------------------------------------------------------------------------

fn permit_discipline(seed: u64, fault: StallFault) -> Result<(), String> {
    let mut r = rig(
        seed,
        &RigCfg {
            engines: 6,
            pool: Some(2),
            rate: KIB,
            sync_ms: 3,
            val_len: 100,
            fault,
        },
    );
    r.run(20);
    r.stop.store(true, Ordering::Release);
    let worst = r.meter.held_idle_max.load(Ordering::Relaxed);
    if worst > 0 {
        return Err(format!(
            "{worst} pool permits were held by engines with no compaction running"
        ));
    }
    if r.meter.deferred.load(Ordering::Relaxed) == 0 {
        return Err("the budget never refused: the cell does not exercise the order".into());
    }
    Ok(())
}

/// Permit first, bucket second, and the permit is dropped before the deferral:
/// no permit is ever held by an engine that is only waiting for credit.
#[test]
fn permit_then_bucket_and_the_permit_is_dropped_before_deferring() {
    if !cell_enabled("permit_order") {
        return;
    }
    for seed in seeds() {
        let v = permit_discipline(seed, StallFault::None);
        assert!(v.is_ok(), "seed {seed}: {}", v.unwrap_err());
    }
}

#[test]
fn control_e_a_permit_held_while_waiting_for_credit_starves_the_pool() {
    if !cell_enabled("control_e") {
        return;
    }
    for seed in seeds() {
        let v = permit_discipline(seed, StallFault::PermitHeldWhileThrottled);
        let why = v.expect_err(&format!("seed {seed}: control (e) must fail the oracle"));
        assert!(
            why.contains("pool permits were held"),
            "seed {seed}: failed for the wrong reason: {why}"
        );
    }
}

// ---------------------------------------------------------------------------
// Cell 9 + controls (f), (g): defer-and-end; flush never gated, never behind a wait.
// ---------------------------------------------------------------------------

struct Flushy {
    flushes: u64,
    flush_blocked: Duration,
    memtable_stall: Duration,
    deferrals: u64,
}

fn flush_discipline(seed: u64, fault: StallFault) -> (Result<(), String>, Flushy) {
    let secs = 20;
    let mut r = rig(
        seed,
        &RigCfg {
            engines: 4,
            pool: None,
            rate: 1, // one byte per second: every non-urgent compaction waits
            sync_ms: 3,
            val_len: 100,
            fault,
        },
    );
    for _ in 0..(secs * 100) {
        r.sim.run_for(Duration::from_millis(10));
    }
    r.stop.store(true, Ordering::Release);
    let mut f = Flushy {
        flushes: 0,
        flush_blocked: Duration::ZERO,
        memtable_stall: Duration::ZERO,
        deferrals: 0,
    };
    for e in &r.engines {
        f.flushes += e.flush_count();
        let s = e.stall_stats();
        f.flush_blocked += Duration::from_nanos(s.flush_blocked_nanos);
        f.memtable_stall += Duration::from_nanos(s.nanos[0]);
        f.deferrals += s.io_deferrals;
    }
    let acked = r.acked.load(Ordering::Relaxed);
    let verdict = (|| {
        if let Some(e) = r.errs.lock().unwrap().first() {
            return Err(format!("a write returned Err: {e}"));
        }
        // Flushes kept up with the writes (one per ~256-byte memtable), so a
        // flush that became due during a compaction's wait did run.
        let min_flushes = acked * 120 / 256 / 2;
        if f.flushes < min_flushes {
            return Err(format!(
                "flushes fell behind the writes: {} flushes for {acked} acked writes (min {min_flushes})",
                f.flushes
            ));
        }
        // The flush never waits behind a throttled compaction (the wait is not
        // inside the maintenance lock) ...
        if f.flush_blocked > Duration::from_secs(2) {
            return Err(format!(
                "flushes waited {:?} on the maintenance lock",
                f.flush_blocked
            ));
        }
        // ... and the memtable gate did not engage for want of a flush.
        if f.memtable_stall > Duration::from_secs(2) {
            return Err(format!(
                "writers waited {:?} on a full memtable",
                f.memtable_stall
            ));
        }
        if f.deferrals == 0 {
            return Err("the budget never deferred a compaction".into());
        }
        Ok(())
    })();
    (verdict, f)
}

#[test]
fn cell9_a_throttled_compaction_defers_and_ends_the_task_so_flushes_run() {
    if !cell_enabled("cell9") {
        return;
    }
    for seed in seeds() {
        let (v, f) = flush_discipline(seed, StallFault::None);
        assert!(v.is_ok(), "seed {seed}: {}", v.unwrap_err());
        let _ = f;
    }
}

/// Control (f): a flush gated by the bucket like a compaction: with the budget
/// at one byte a second the flush waits for credit that never comes, the
/// memtable overshoots and writers stall.
#[test]
fn control_f_a_gated_flush_lets_the_memtable_overshoot() {
    if !cell_enabled("control_f") {
        return;
    }
    for seed in seeds() {
        let (v, _) = flush_discipline(seed, StallFault::FlushGated);
        let why = v.expect_err(&format!(
            "seed {seed}: control (f) must fail the flush-never-gated oracle"
        ));
        assert!(
            why.contains("flushes fell behind") || why.contains("full memtable"),
            "seed {seed}: failed for the wrong reason: {why}"
        );
    }
}

/// Control (g): the IO wait inside the compaction, holding the maintenance
/// lock: a due flush is blocked for the throttled duration.
#[test]
fn control_g_waiting_inside_the_compaction_blocks_the_flush() {
    if !cell_enabled("control_g") {
        return;
    }
    for seed in seeds() {
        let (v, f) = flush_discipline(seed, StallFault::WaitInsideCompaction);
        let why = v.expect_err(&format!(
            "seed {seed}: control (g) must fail the flush-never-blocked oracle (blocked {:?})",
            f.flush_blocked
        ));
        assert!(
            why.contains("on the maintenance lock") || why.contains("flushes fell behind"),
            "seed {seed}: failed for the wrong reason: {why}"
        );
    }
}

// ---------------------------------------------------------------------------
// Cell 10: lag scaling at the budget (the pure rule is in `compaction_io`).
// ---------------------------------------------------------------------------

#[test]
fn cell10_the_effective_rate_follows_lag_and_never_drops_below_the_floor() {
    let mut sizes = BudgetSizes::off();
    sizes.compaction_io_bytes_per_sec = 64 << 20;
    let b = BackgroundBudget::new(sizes, MetricsHandle::recording());
    assert_eq!(b.compaction_io_stats().rate_eff, 64 << 20);
    b.set_lag(40_000_000);
    assert_eq!(b.compaction_io_stats().rate_eff, 32 << 20);
    b.set_lag(150_000_000);
    assert_eq!(b.compaction_io_stats().rate_eff, 16 << 20);
    b.set_lag(0);
    assert_eq!(b.compaction_io_stats().rate_eff, 64 << 20, "recovers");
    // A small budget is clamped at the 4 MiB/s floor, not driven to zero.
    let mut sizes = BudgetSizes::off();
    sizes.compaction_io_bytes_per_sec = 8 << 20;
    let b = BackgroundBudget::new(sizes, MetricsHandle::recording());
    b.set_lag(500_000_000);
    assert_eq!(b.compaction_io_stats().rate_eff, 4 << 20);
}

/// The budget off (rate 0) is exactly today's behaviour: no gate unless the
/// pool is on, every request admitted.
#[test]
fn a_zero_rate_is_off_and_installs_no_gate_without_the_pool() {
    let b = BackgroundBudget::new(BudgetSizes::off(), MetricsHandle::recording());
    assert!(!b.compaction_io_on());
    assert!(
        b.compaction_gate(Arc::new(|| animus_env::Nanos(0)))
            .is_none()
    );
    assert!(b.compaction_io_admit(animus_env::Nanos(0), u64::MAX, false));
}
