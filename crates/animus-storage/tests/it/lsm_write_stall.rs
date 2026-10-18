//! ADR 0081 Tier 1: the LSM write stall (decisions 1 and 2) over
//! `LsmEngine<SimEnv>`. Issue #1338 (a stall below Raft may delay a committed
//! entry but must never fail it) is the headline oracle of every cell.
//!
//! The stall sits **before** the WAL append, so any `Err` it returns left the
//! engine unchanged. Knobs: `ANIMUS_LSM_STALL_SEEDS=K` (default 1) seeds per
//! cell, `ANIMUS_LSM_STALL_CELL=<substring>` narrows to matching cells,
//! `ANIMUS_SEED=<seed>` replays one.
//!
//! Common oracles (`check_common`): no write returns `Err` from a stall; every
//! acked write is readable once the stall clears (and after a crash and
//! restart); L0 never exceeds `stop` plus the in-flight slack. Each negative
//! control asserts the relevant oracle FAILS.
//!
//! Why a "wedged" compaction gate drives the L0 climb: with one maintenance task
//! per engine, a flush and a compaction alternate on one lock and a compaction
//! consumes every L0 table present, so in healthy operation L0 stays at about
//! `compaction_trigger + 1` whatever the disk speed (measured; see
//! `docs/density-measurements.md`). L0 only climbs past `2 x trigger` if
//! compaction is *withheld* (a pool or limiter that does not honour the urgent
//! bypass), which is what the out-of-contract `Wedge` gate stands in for.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Clock, EnvExt, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{
    CompactionGate, CompactionPermit, LsmEngine, LsmOptions, StallFault, StorageEngine,
};
use futures::executor::block_on;

const PREFIX: &str = "db/";
/// Trigger 2 => bypass 4, slowdown 6, stop 10 tables; memtable cap 1024 bytes.
const TRIGGER: usize = 2;
const STOP_TABLES: usize = 5 * TRIGGER;

fn opts(fault: StallFault, l0_stall: bool) -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 256,
        compaction_trigger: TRIGGER,
        target_table_bytes: 1024,
        level_fanout: 2,
        wal_segment_bytes: 1 << 20,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: true,
        background_maintenance: true,
        l0_slowdown_factor: if l0_stall { 3 } else { 0 },
        l0_stop_factor: if l0_stall { 5 } else { 0 },
        // Slow enough that the ramp is visible at ~100-byte writes.
        l0_slowdown_write_rate: 64 << 10,
        l0_min_write_rate: 8 << 10,
        stall_fault: fault,
        ..LsmOptions::default()
    }
}

fn open_with(sim: &Simulator, o: LsmOptions) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, o)).expect("open")
}

fn slow(d: Duration) -> DiskConfig {
    let mut c = DiskConfig::default();
    c.set_sync_delay(d);
    c
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
    (0..k.max(1)).map(|i| 0x1081 + i).collect()
}

/// Whether the cell named `name` is selected by `ANIMUS_LSM_STALL_CELL`.
fn cell_enabled(name: &str) -> bool {
    std::env::var("ANIMUS_LSM_STALL_CELL").map_or(true, |f| name.contains(&f))
}

/// splitmix64: a tiny seeded stream for per-seed workload variety (the sim's
/// own RNG is reserved for the engine's environment).
struct Mix(u64);
impl Mix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }
}

/// A compaction gate that withholds every compaction (even the urgent
/// bypass, which a real gate must grant) while wedged: the stand-in for
/// anything that keeps compaction from running.
struct Wedge(Arc<AtomicBool>);
impl CompactionGate for Wedge {
    fn acquire(&self, _bypass: bool) -> Option<CompactionPermit> {
        if self.0.load(Ordering::Acquire) {
            None
        } else {
            Some(Box::new(()))
        }
    }
}

#[derive(Default)]
struct Log {
    acked: Vec<(Vec<u8>, Vec<u8>)>,
    errs: Vec<(Duration, String)>,
}

struct Writers {
    log: Arc<Mutex<Log>>,
    stop: Arc<AtomicBool>,
}

impl Writers {
    fn acked(&self) -> usize {
        self.log.lock().unwrap().acked.len()
    }
    fn errs(&self) -> Vec<(Duration, String)> {
        self.log.lock().unwrap().errs.clone()
    }
    fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }
}

/// `n` closed-loop writers (1 ms think time, so a writer never runs out of
/// work and never spins at one instant) of unique keys through `merge`.
fn start_writers(
    sim: &Simulator,
    e: &LsmEngine<SimEnv>,
    n: usize,
    val_len: usize,
    ver: &Arc<AtomicU64>,
    tag: &str,
) -> Writers {
    let log = Arc::new(Mutex::new(Log::default()));
    let stop = Arc::new(AtomicBool::new(false));
    for w in 0..n {
        let (e, log, stop, ver) = (e.clone(), log.clone(), stop.clone(), ver.clone());
        let env = sim.env(nid(0));
        let tag = tag.to_string();
        env.clone().spawn_task(async move {
            let mut i = 0u64;
            while !stop.load(Ordering::Acquire) {
                let v = ver.fetch_add(1, Ordering::AcqRel) + 1;
                let key = format!("{tag}w{w}-{i:07}").into_bytes();
                let val = vec![b'a' + (w as u8 % 26); val_len];
                match e.merge(&key, &val, v).await {
                    Ok(_) => log.lock().unwrap().acked.push((key, val)),
                    Err(err) => {
                        let at = Duration::from_nanos(env.now().0);
                        log.lock().unwrap().errs.push((at, err.to_string()));
                        return;
                    }
                }
                i += 1;
                env.sleep(Duration::from_millis(1)).await;
            }
        });
    }
    Writers { log, stop }
}

/// Run `d` of virtual time in 10 ms slices, sampling the engine's L0 count.
fn run_sampling(sim: &mut Simulator, e: &LsmEngine<SimEnv>, d: Duration) -> usize {
    let mut hwm = 0;
    let slices = d.as_millis() / 10;
    for _ in 0..slices {
        sim.run_for(Duration::from_millis(10));
        hwm = hwm.max(e.l0_table_count());
    }
    hwm
}

/// The oracles every cell shares; `Err(why)` when one fails.
fn check_common(
    e: &LsmEngine<SimEnv>,
    w: &Writers,
    l0_hwm: usize,
    n_writers: usize,
) -> Result<(), String> {
    let errs = w.errs();
    if !errs.is_empty() {
        return Err(format!("a stalled write returned Err: {errs:?}"));
    }
    // Writers that already passed the check keep flushing: slack = one table
    // per in-flight writer plus the table being flushed.
    let bound = STOP_TABLES + n_writers + 2;
    if l0_hwm > bound {
        return Err(format!(
            "L0 reached {l0_hwm} > stop {STOP_TABLES} + slack (bound {bound})"
        ));
    }
    for (k, v) in &w.log.lock().unwrap().acked {
        match block_on(e.get(k)) {
            Ok(Some(got)) if &got.value == v => {}
            other => {
                return Err(format!(
                    "acked key {:?} unreadable: {other:?}",
                    String::from_utf8_lossy(k)
                ));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Cell 1: steady overload, compaction withheld: slowdown, then stop, then clears.
// ---------------------------------------------------------------------------

struct Cell1 {
    slowdown_writes: u64,
    stop_writes: u64,
    l0_hwm_wedged: usize,
    acked_wedged_end: usize,
    acked_after: usize,
    l0_final: usize,
}

fn cell1(seed: u64, l0_stall: bool) -> (Result<(), String>, Cell1) {
    let mut mix = Mix(seed);
    let n = mix.range(3, 6) as usize;
    let val_len = mix.range(60, 140) as usize;
    let sync_ms = mix.range(2, 6);
    let wedge_s = mix.range(10, 16);
    let mut sim = Simulator::new(seed);
    let e = open_with(&sim, opts(StallFault::None, l0_stall));
    let wedged = Arc::new(AtomicBool::new(true));
    e.set_compaction_gate(Arc::new(Wedge(wedged.clone())));
    sim.set_disk_config(slow(Duration::from_millis(sync_ms)));
    let ver = Arc::new(AtomicU64::new(0));
    let w = start_writers(&sim, &e, n, val_len, &ver, "c1");

    let hwm_wedged = run_sampling(&mut sim, &e, Duration::from_secs(wedge_s));
    let acked_wedged_end = w.acked();
    let stats_wedged = e.stall_stats();
    wedged.store(false, Ordering::Release);
    let hwm_after = run_sampling(&mut sim, &e, Duration::from_secs(20));
    let acked_after = w.acked() - acked_wedged_end;
    w.stop();
    sim.run_for(Duration::from_secs(2));
    sim.set_disk_config(DiskConfig::default());
    sim.run_until_quiescent(2_000_000);
    let l0_final = e.l0_table_count();
    let hwm = hwm_wedged.max(hwm_after);
    let mut r = check_common(&e, &w, hwm, n);
    if r.is_ok() && l0_stall && l0_final > STOP_TABLES {
        r = Err(format!(
            "the stop never cleared: L0 {l0_final} after the wedge lifted"
        ));
    }
    (
        r,
        Cell1 {
            slowdown_writes: stats_wedged.writes[1],
            stop_writes: stats_wedged.writes[2],
            l0_hwm_wedged: hwm_wedged,
            acked_wedged_end,
            acked_after,
            l0_final,
        },
    )
}

#[test]
fn cell1_l0_climbs_slows_stops_then_clears() {
    if !cell_enabled("cell1") {
        return;
    }
    for seed in seeds() {
        let (r, c) = cell1(seed, true);
        assert!(r.is_ok(), "seed {seed}: {}", r.unwrap_err());
        assert!(
            c.slowdown_writes > 0,
            "seed {seed}: no write was ever delayed"
        );
        assert!(
            c.stop_writes > 0,
            "seed {seed}: L0 never reached the stop level"
        );
        assert!(
            c.l0_hwm_wedged >= STOP_TABLES,
            "seed {seed}: the workload never reached stop (hwm {})",
            c.l0_hwm_wedged
        );
        assert!(
            c.acked_after > c.acked_wedged_end / 4,
            "seed {seed}: writers did not resume after the wedge lifted ({} then {})",
            c.acked_wedged_end,
            c.acked_after
        );
        assert!(
            c.l0_final <= TRIGGER + 2,
            "seed {seed}: L0 {} did not clear",
            c.l0_final
        );
    }
}

/// Control (a): the L0 stall off. Under the same workload L0 grows past the
/// stop level, so the bounded-L0 oracle FAILS.
#[test]
fn control_a_stall_off_lets_l0_grow_without_bound() {
    if !cell_enabled("control_a") {
        return;
    }
    for seed in seeds() {
        let (r, c) = cell1(seed, false);
        let why = r.expect_err(&format!(
            "seed {seed}: control (a) must fail the oracle (hwm {})",
            c.l0_hwm_wedged
        ));
        assert!(
            why.contains("L0 reached"),
            "seed {seed}: failed for the wrong reason: {why}"
        );
    }
}

// ---------------------------------------------------------------------------
// Cell 2: memtable over cap behind a long maintenance run; run past 10 s.
// ---------------------------------------------------------------------------

fn big() -> Vec<u8> {
    vec![b'x'; 2048]
}

type PutLog = Arc<Mutex<Vec<(Duration, Result<(), String>)>>>;

/// Two sequential puts on one task: the first fills the memtable past the
/// 1 KiB cap, so the second lands in the stall.
fn spawn_two_puts(sim: &Simulator, e: &LsmEngine<SimEnv>, base: u64) -> PutLog {
    let done: PutLog = Arc::new(Mutex::new(Vec::new()));
    let (d, e2) = (done.clone(), e.clone());
    let env = sim.env(nid(0));
    env.clone().spawn_task(async move {
        for (i, k) in [b"big1".as_slice(), b"big2".as_slice()]
            .into_iter()
            .enumerate()
        {
            let r = e2
                .put(k, &big(), base + i as u64)
                .await
                .map_err(|x| x.to_string());
            let failed = r.is_err();
            d.lock()
                .unwrap()
                .push((Duration::from_nanos(env.now().0), r));
            if failed {
                return;
            }
        }
    });
    done
}

fn cell2(seed: u64, fault: StallFault) -> (Result<(), String>, Duration) {
    let mut mix = Mix(seed);
    let sync_s = mix.range(20, 40);
    let mut sim = Simulator::new(seed);
    let e = open_with(&sim, opts(fault, true));
    sim.set_disk_config(slow(Duration::from_secs(sync_s)));
    let done = spawn_two_puts(&sim, &e, 1);
    sim.run_for(Duration::from_secs(sync_s * 40));
    let res = done.lock().unwrap().clone();
    let finished = res.last().map_or(Duration::ZERO, |(t, _)| *t);
    let verdict = if res.len() == 2 && res.iter().all(|(_, r)| r.is_ok()) {
        Ok(())
    } else {
        Err(format!("the stalled put did not simply succeed: {res:?}"))
    };
    if verdict.is_ok() {
        sim.set_disk_config(DiskConfig::default());
        sim.run_until_quiescent(1_000_000);
        for k in [b"big1".as_slice(), b"big2".as_slice()] {
            let got = block_on(e.get(k)).unwrap().unwrap();
            assert_eq!(got.value, big(), "seed {seed}");
        }
    }
    (verdict, finished)
}

/// A maintenance run that is merely slow (tens of seconds per fsync) keeps the
/// memtable over its cap for far longer than the old 10,000 x 1 ms poll
/// bound. The write waits and then succeeds, never `Err`.
#[test]
fn cell2_memtable_over_cap_behind_slow_maintenance_waits_past_ten_seconds() {
    if !cell_enabled("cell2") {
        return;
    }
    for seed in seeds() {
        let (r, finished) = cell2(seed, StallFault::None);
        assert!(r.is_ok(), "seed {seed}: {}", r.unwrap_err());
        assert!(
            finished > Duration::from_secs(60),
            "seed {seed}: finished at {finished:?}"
        );
    }
}

/// Control (b): the pre-ADR gate (a 10,000-poll bound that ends in `Err`,
/// evaluated after the append) fails the same workload.
#[test]
fn control_b_the_old_bounded_gate_errors_after_ten_seconds() {
    if !cell_enabled("control_b") {
        return;
    }
    for seed in seeds() {
        let (r, _) = cell2(seed, StallFault::OldGate);
        assert!(
            r.is_err(),
            "seed {seed}: control (b) must fail the no-Err oracle"
        );
    }
}

/// Control (h): a fail-stop limit counted in seconds instead of failures trips
/// on slow-but-working maintenance.
#[test]
fn control_h_a_limit_counted_in_seconds_trips_on_slow_maintenance() {
    if !cell_enabled("control_h") {
        return;
    }
    for seed in seeds() {
        let (r, _) = cell2(seed, StallFault::LimitInSeconds);
        assert!(
            r.is_err(),
            "seed {seed}: control (h) must fail the no-Err oracle"
        );
    }
}

// ---------------------------------------------------------------------------
// Cell 3: crash mid-stall, restart, resume.  Cell 6: with torn/corrupt tails.
// ---------------------------------------------------------------------------

fn crash_mid_stall(seed: u64, torn: bool) -> Result<(), String> {
    let mut mix = Mix(seed);
    let n = mix.range(3, 5) as usize;
    let val_len = mix.range(60, 120) as usize;
    let sync_ms = mix.range(2, 5);
    let mut sim = Simulator::new(seed);
    let e = open_with(&sim, opts(StallFault::None, true));
    let wedged = Arc::new(AtomicBool::new(true));
    e.set_compaction_gate(Arc::new(Wedge(wedged)));
    let mut cfg = slow(Duration::from_millis(sync_ms));
    cfg.torn_tail_on_crash = torn;
    cfg.corrupt_on_crash = torn;
    sim.set_disk_config(cfg);
    let ver = Arc::new(AtomicU64::new(0));
    let w = start_writers(&sim, &e, n, val_len, &ver, "pre");
    // Run until the stop level is reached, then a bit more so writers are
    // parked in the stall.
    let mut stalled = false;
    for _ in 0..3000 {
        sim.run_for(Duration::from_millis(10));
        if e.stall_stats().writes[2] > 0 {
            stalled = true;
            break;
        }
    }
    if !stalled {
        return Err("the workload never reached the L0 stop level".into());
    }
    sim.run_for(Duration::from_millis(200));
    // Power loss with writers parked in the stall.
    let acked: Vec<(Vec<u8>, Vec<u8>)> = w.log.lock().unwrap().acked.clone();
    drop(e);
    sim.stop(nid(0));
    sim.crash(nid(0));
    sim.restart(nid(0));
    sim.set_disk_config(DiskConfig::default());
    let e2 = open_with(&sim, opts(StallFault::None, true));
    for (k, v) in &acked {
        match block_on(e2.get(k)) {
            Ok(Some(got)) if &got.value == v => {}
            other => {
                return Err(format!(
                    "acked key {:?} lost across the crash: {other:?}",
                    String::from_utf8_lossy(k)
                ));
            }
        }
    }
    // Resume: the restarted engine (no gate => compaction runs) accepts and
    // serves new writes with no Err.
    let w2 = start_writers(&sim, &e2, 2, val_len, &ver, "post");
    let hwm = run_sampling(&mut sim, &e2, Duration::from_secs(10));
    w2.stop();
    sim.run_until_quiescent(2_000_000);
    if w2.acked() == 0 {
        return Err("the restarted engine never acked a write".into());
    }
    check_common(&e2, &w2, hwm, 2)
}

#[test]
fn cell3_crash_mid_stall_restart_resume() {
    if !cell_enabled("cell3") {
        return;
    }
    for seed in seeds() {
        let r = crash_mid_stall(seed, false);
        assert!(r.is_ok(), "seed {seed}: {}", r.unwrap_err());
    }
}

#[test]
fn cell6_torn_and_corrupt_tails_armed_while_stalled() {
    if !cell_enabled("cell6") {
        return;
    }
    for seed in seeds() {
        let r = crash_mid_stall(seed, true);
        assert!(r.is_ok(), "seed {seed}: {}", r.unwrap_err());
    }
}

// ---------------------------------------------------------------------------
// Cell 4: ENOSPC while stalled.  Cell 5: a repeated non-ENOSPC fault.
// ---------------------------------------------------------------------------

type Outcome = Arc<Mutex<Option<Result<(), String>>>>;

fn spawn_put(sim: &Simulator, e: &LsmEngine<SimEnv>, key: &'static [u8], version: u64) -> Outcome {
    let out: Outcome = Arc::new(Mutex::new(None));
    let (o, e2) = (out.clone(), e.clone());
    sim.env(nid(0)).spawn_task(async move {
        let r = e2
            .put(key, &big(), version)
            .await
            .map_err(|x| x.to_string());
        *o.lock().unwrap() = Some(r);
    });
    out
}

struct FaultRun {
    sim: Simulator,
    e: LsmEngine<SimEnv>,
    first: Option<Result<(), String>>,
    second: Option<Result<(), String>>,
}

/// Put one value past the memtable cap, let its maintenance start, then break
/// the disk with `cfg` and put a second: it lands in the stall.
fn fault_during_stall(seed: u64, fault: StallFault, cfg: DiskConfig) -> FaultRun {
    let mut sim = Simulator::new(seed);
    let e = open_with(&sim, opts(fault, true));
    sim.set_disk_config(slow(Duration::from_millis(100)));
    let first = spawn_put(&sim, &e, b"big1", 1);
    for _ in 0..1000 {
        sim.run_for(Duration::from_millis(10));
        if e.background_maintenance_in_flight() {
            break;
        }
    }
    sim.set_disk_config(cfg);
    let second = spawn_put(&sim, &e, b"big2", 2);
    sim.run_for(Duration::from_secs(60));
    let (first, second) = (first.lock().unwrap().take(), second.lock().unwrap().take());
    FaultRun {
        sim,
        e,
        first,
        second,
    }
}

/// ENOSPC: the stalled write fails as `StorageFull` and, because the stall is
/// before the append, left no trace; a retry after space returns succeeds.
#[test]
fn cell4_enospc_during_a_stall_is_storage_full_and_leaves_no_trace() {
    if !cell_enabled("cell4") {
        return;
    }
    for seed in seeds() {
        let mut full = DiskConfig::default();
        full.set_enospc_prob(1.0);
        let mut run = fault_during_stall(seed, StallFault::None, full);
        assert!(
            matches!(run.first, Some(Ok(()))),
            "seed {seed}: first put {:?}",
            run.first
        );
        let err = run
            .second
            .take()
            .unwrap_or_else(|| panic!("seed {seed}: the stalled put never returned"))
            .expect_err("a stalled put on a full disk must fail");
        assert!(
            err.contains("storage full"),
            "seed {seed}: expected StorageFull, got {err}"
        );
        assert_eq!(
            run.e.maintenance_consecutive_failures(),
            0,
            "seed {seed}: ENOSPC is not counted"
        );
        assert!(
            block_on(run.e.get(b"big2")).unwrap().is_none(),
            "seed {seed}: a failed stalled write left a trace"
        );
        // Space returns: the identical call goes through.
        run.sim.set_disk_config(DiskConfig::default());
        let retry = spawn_put(&run.sim, &run.e, b"big2", 2);
        run.sim.run_for(Duration::from_secs(60));
        let r = retry.lock().unwrap().take();
        assert!(matches!(r, Some(Ok(()))), "seed {seed}: retry got {r:?}");
    }
}

/// Control (c): the stall evaluated after the append (the old position). The
/// same ENOSPC now fails a write that took effect, so the oracle "a put that
/// returned Err left no trace" FAILS.
#[test]
fn control_c_stall_after_the_append_fails_a_write_that_took_effect() {
    if !cell_enabled("control_c") {
        return;
    }
    for seed in seeds() {
        let mut full = DiskConfig::default();
        full.set_enospc_prob(1.0);
        let run = fault_during_stall(seed, StallFault::AfterAppend, full);
        let failed_but_applied = [
            (&run.first, b"big1".as_slice()),
            (&run.second, b"big2".as_slice()),
        ]
        .into_iter()
        .any(|(r, k)| matches!(r, Some(Err(_))) && block_on(run.e.get(k)).unwrap().is_some());
        assert!(
            failed_but_applied,
            "seed {seed}: control (c) must fail the failed => absent oracle \
             (first {:?}, second {:?})",
            run.first, run.second
        );
    }
}

/// A persistently failing disk (non-ENOSPC) fail-stops the stalled write after
/// `maintenance_failure_limit` consecutive failures, not after elapsed time
/// and not as an unbounded wait; again nothing was appended.
#[test]
fn cell5_repeated_non_enospc_faults_fail_stop_after_n_failures() {
    if !cell_enabled("cell5") {
        return;
    }
    for seed in seeds() {
        let mut dead = DiskConfig::default();
        dead.set_error_prob(1.0);
        let mut run = fault_during_stall(seed, StallFault::None, dead);
        let err = run
            .second
            .take()
            .unwrap_or_else(|| panic!("seed {seed}: the stalled put never returned"))
            .expect_err("a persistently failing disk must fail-stop the stalled write");
        assert!(
            err.contains("backend error"),
            "seed {seed}: expected Backend, got {err}"
        );
        assert!(
            run.e.maintenance_consecutive_failures() >= 8,
            "seed {seed}: failed after only {} consecutive failures",
            run.e.maintenance_consecutive_failures()
        );
        assert!(
            run.e.background_maintenance_error().is_some(),
            "seed {seed}"
        );
        run.sim.set_disk_config(DiskConfig::default());
        assert!(
            block_on(run.e.get(b"big2")).unwrap().is_none(),
            "seed {seed}: a fail-stopped write left a trace"
        );
    }
}

/// A fault run shorter than the limit is waited out: maintenance recovers, the
/// streak resets, the stalled write succeeds.
#[test]
fn transient_failures_below_the_limit_do_not_fail_the_write() {
    if !cell_enabled("transient") {
        return;
    }
    for seed in seeds() {
        let mut sim = Simulator::new(seed);
        let e = open_with(&sim, opts(StallFault::None, true));
        sim.set_disk_config(slow(Duration::from_millis(100)));
        let first = spawn_put(&sim, &e, b"big1", 1);
        for _ in 0..1000 {
            sim.run_for(Duration::from_millis(10));
            if e.background_maintenance_in_flight() {
                break;
            }
        }
        let mut dead = DiskConfig::default();
        dead.set_error_prob(1.0);
        sim.set_disk_config(dead);
        let second = spawn_put(&sim, &e, b"big2", 2);
        // Heal after a few ms: fewer than 8 consecutive failures.
        sim.run_for(Duration::from_millis(3));
        sim.set_disk_config(DiskConfig::default());
        sim.run_for(Duration::from_secs(60));
        let (a, b) = (first.lock().unwrap().take(), second.lock().unwrap().take());
        assert!(
            matches!(a, Some(Ok(()))) && matches!(b, Some(Ok(()))),
            "seed {seed}: a brief fault must not fail a write, got {a:?} {b:?}"
        );
        assert_eq!(e.maintenance_consecutive_failures(), 0, "seed {seed}");
    }
}

// ---------------------------------------------------------------------------
// Validation (Tier 0, on the options).
// ---------------------------------------------------------------------------

#[test]
fn validate_orders_the_l0_factors_or_requires_both_zero() {
    let ok = |slow, stop| {
        LsmOptions {
            l0_slowdown_factor: slow,
            l0_stop_factor: stop,
            ..LsmOptions::default()
        }
        .validate()
        .is_ok()
    };
    assert!(ok(0, 0), "both zero disables the stall");
    assert!(ok(3, 5));
    assert!(ok(3, 4));
    assert!(!ok(2, 5), "slowdown must exceed the bypass factor (2)");
    assert!(!ok(3, 3), "stop must exceed slowdown");
    assert!(!ok(5, 3));
    assert!(!ok(0, 5), "a half-enabled stall is refused");
    assert!(!ok(3, 0));
    let bad_rates = LsmOptions {
        l0_slowdown_factor: 3,
        l0_stop_factor: 5,
        l0_min_write_rate: 0,
        ..LsmOptions::default()
    };
    assert!(bad_rates.validate().is_err());
    let no_limit = LsmOptions {
        maintenance_failure_limit: 0,
        ..LsmOptions::default()
    };
    assert!(no_limit.validate().is_err());
    assert!(LsmOptions::production().validate().is_ok());
    assert!(LsmOptions::production_raw_versions().validate().is_ok());
}

#[test]
fn production_turns_the_l0_stall_on_and_the_control_keyspace_keeps_it_off() {
    let p = LsmOptions::production();
    assert_eq!((p.l0_slowdown_factor, p.l0_stop_factor), (3, 5));
    let c = LsmOptions::production_raw_versions();
    assert_eq!((c.l0_slowdown_factor, c.l0_stop_factor), (0, 0));
    let d = LsmOptions::default();
    assert_eq!((d.l0_slowdown_factor, d.l0_stop_factor), (0, 0));
}
