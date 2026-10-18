//! ADR 0081 implementation step 1: the **characterisation / measurement** rig.
//!
//! Not a pass/fail test: an `#[ignore]`d sweep that prints one row per
//! configuration, run by hand to fill the as-built amendment of ADR 0081 and
//! `docs/density-measurements.md` section I:
//!
//! ```text
//! cargo test -p animus-cp-data --test it lsm_stall_measure -- --ignored --nocapture
//! ```
//!
//! Everything runs on `LsmEngine<SimEnv>` with the node-wide `BackgroundBudget`
//! (compaction pool of 2, as `--cores 4` gives) installed on every engine. The
//! disk is slowed only by `DiskConfig::set_sync_delay` (ADR 0079 decision 7:
//! no disk-cost model), so what is measured is *scheduling and queueing*
//! (L0 counts, lock waits, stalls, refusal), not bandwidth; bandwidth is the
//! `ProdEnv` run (`animus-storage/tests/lsm_stall_prod.rs`) and the `dd`
//! baseline recorded beside the numbers.
//!
//! Modes, one mechanism at a time:
//! * `off`: the pre-ADR engine -- the memtable gate after the append
//!   (`StallFault::AfterAppend`), no L0 stall, no IO budget (a counting meter
//!   only, which admits everything).
//! * `stall`: the ADR 0081 stall (gate before the append, L0 3T/5T), no IO budget.
//! * `io{pct}`: stall + the IO budget at `pct` percent of the sustained
//!   compaction write rate that `stall` measured on the same cell.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::background_budget::{BackgroundBudget, BudgetSizes};
use animus_env::{Clock, EnvExt, MetricsHandle, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{
    CompactionGate, CompactionPermit, IoAdmission, LsmEngine, LsmOptions, StallFault, StorageEngine,
};
use futures::executor::block_on;

/// Wraps the real gate to count what the engines hand it (the bytes a
/// compaction writes and a flush writes, deferrals), whatever the mode.
struct Meter {
    inner: Arc<dyn CompactionGate>,
    compaction_bytes: AtomicU64,
    compactions: AtomicU64,
    flush_bytes: AtomicU64,
    io_deferred: AtomicU64,
    urgent: AtomicU64,
}

impl CompactionGate for Meter {
    fn acquire(&self, bypass: bool) -> Option<CompactionPermit> {
        if bypass {
            self.urgent.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.acquire(bypass)
    }
    fn io_admit(&self, est: u64, urgent: bool) -> IoAdmission {
        let r = self.inner.io_admit(est, urgent);
        if r == IoAdmission::Defer {
            self.io_deferred.fetch_add(1, Ordering::Relaxed);
        }
        r
    }
    fn io_settle(&self, est: u64, actual: u64) {
        self.compaction_bytes.fetch_add(actual, Ordering::Relaxed);
        self.compactions.fetch_add(1, Ordering::Relaxed);
        self.inner.io_settle(est, actual);
    }
    fn io_charge(&self, bytes: u64) {
        self.flush_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.inner.io_charge(bytes);
    }
}

#[derive(Clone, Copy, Debug)]
struct Cell {
    engines: usize,
    writers_per_engine: usize,
    value_bytes: usize,
    sync_ms: u64,
    virtual_secs: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Off,
    Stall,
    Io(u64), // percent of the sustained rate, resolved by the caller into bytes/s
}

#[derive(Default, Debug)]
struct Row {
    acked: u64,
    p50_ms: f64,
    p99_ms: f64,
    l0_hwm: usize,
    l0_hwm_sampled: usize,
    mem_hwm: usize,
    compactions: u64,
    comp_mib_s: f64,
    flush_mib_s: f64,
    flush_blocked_ms: f64,
    stalled_ms: [f64; 3],
    stalled_writes: [u64; 3],
    io_deferred: u64,
    urgent: u64,
    errs: u64,
    final_sstables: usize,
}

fn engine_opts(mode: Mode) -> LsmOptions {
    let mut o = LsmOptions::production();
    if mode == Mode::Off {
        o.l0_slowdown_factor = 0;
        o.l0_stop_factor = 0;
        o.stall_fault = StallFault::AfterAppend;
    }
    o
}

fn run_cell(seed: u64, cell: Cell, mode: Mode, io_rate: u64) -> Row {
    let mut sim = Simulator::new(seed);
    let env0 = sim.env(nid(0));
    let mut sizes = BudgetSizes::off();
    sizes.compactions = Some(2);
    let budget = BackgroundBudget::new(sizes, MetricsHandle::recording());
    if matches!(mode, Mode::Io(_)) {
        budget.set_compaction_io_rate(io_rate);
    } else {
        // A bucket that never refuses, so the engines' gate still reports.
        budget.set_compaction_io_rate(1 << 50);
    }
    let env_for_clock = env0.clone();
    let inner = budget
        .compaction_gate(Arc::new(move || env_for_clock.now()))
        .expect("gate");
    let meter = Arc::new(Meter {
        inner,
        compaction_bytes: AtomicU64::new(0),
        compactions: AtomicU64::new(0),
        flush_bytes: AtomicU64::new(0),
        io_deferred: AtomicU64::new(0),
        urgent: AtomicU64::new(0),
    });
    let engines: Vec<LsmEngine<SimEnv>> = (0..cell.engines)
        .map(|i| {
            let e = block_on(LsmEngine::open_with(
                env0.clone(),
                &format!("t{i}/"),
                engine_opts(mode),
            ))
            .expect("open");
            e.set_compaction_gate(meter.clone());
            e
        })
        .collect();
    let mut cfg = DiskConfig::default();
    cfg.set_sync_delay(Duration::from_millis(cell.sync_ms));
    sim.set_disk_config(cfg);

    let ver = Arc::new(AtomicU64::new(0));
    let lat: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let errs = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    for (ei, e) in engines.iter().enumerate() {
        for w in 0..cell.writers_per_engine {
            let (e, ver, lat, errs, stop) = (
                e.clone(),
                ver.clone(),
                lat.clone(),
                errs.clone(),
                stop.clone(),
            );
            let env = sim.env(nid(0));
            let vb = cell.value_bytes;
            env.clone().spawn_task(async move {
                let mut i = 0u64;
                while !stop.load(Ordering::Acquire) {
                    let v = ver.fetch_add(1, Ordering::AcqRel) + 1;
                    let key = format!("e{ei}w{w}-{i:08}").into_bytes();
                    let t0 = env.now();
                    match e.merge(&key, &vec![7u8; vb], v).await {
                        Ok(_) => {
                            let d = env.now().duration_since(t0);
                            lat.lock().unwrap().push(d.as_micros() as u64);
                        }
                        Err(_) => {
                            errs.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    }
                    i += 1;
                    env.sleep(Duration::from_millis(1)).await;
                }
            });
        }
    }

    let mut l0_sampled = 0usize;
    let mut mem_hwm = 0usize;
    for _ in 0..(cell.virtual_secs * 100) {
        sim.run_for(Duration::from_millis(10));
        for e in &engines {
            l0_sampled = l0_sampled.max(e.l0_table_count());
            mem_hwm = mem_hwm.max(e.memtable_bytes());
        }
    }
    stop.store(true, Ordering::Release);
    let secs = cell.virtual_secs as f64;
    let mut l = lat.lock().unwrap().clone();
    l.sort_unstable();
    let pct = |p: f64| {
        if l.is_empty() {
            0.0
        } else {
            l[((l.len() as f64 - 1.0) * p) as usize] as f64 / 1000.0
        }
    };
    let mut row = Row {
        acked: l.len() as u64,
        p50_ms: pct(0.50),
        p99_ms: pct(0.99),
        l0_hwm_sampled: l0_sampled,
        mem_hwm,
        compactions: meter.compactions.load(Ordering::Relaxed),
        comp_mib_s: meter.compaction_bytes.load(Ordering::Relaxed) as f64 / secs / (1 << 20) as f64,
        flush_mib_s: meter.flush_bytes.load(Ordering::Relaxed) as f64 / secs / (1 << 20) as f64,
        io_deferred: meter.io_deferred.load(Ordering::Relaxed),
        urgent: meter.urgent.load(Ordering::Relaxed),
        errs: errs.load(Ordering::Relaxed),
        ..Row::default()
    };
    for e in &engines {
        let s = e.stall_stats();
        row.l0_hwm = row.l0_hwm.max(s.l0_hwm as usize);
        row.flush_blocked_ms += s.flush_blocked_nanos as f64 / 1e6;
        for c in 0..3 {
            row.stalled_ms[c] += s.nanos[c] as f64 / 1e6;
            row.stalled_writes[c] += s.writes[c];
        }
        row.final_sstables += e.sstable_count();
    }
    row
}

fn print_header() {
    println!(
        "{:<26} {:>7} {:>7} {:>8} {:>5} {:>5} {:>6} {:>6} {:>8} {:>8} {:>10} {:>16} {:>16} {:>6} {:>6} {:>4}",
        "cell/mode",
        "acked/s",
        "p50ms",
        "p99ms",
        "l0hwm",
        "l0smp",
        "memKiB",
        "comps",
        "compMiB/s",
        "flshMiB/s",
        "flushBlkMs",
        "stallMs mem/slow/stop",
        "stallN mem/slow/stop",
        "ioDef",
        "urg",
        "errs"
    );
}

fn print_row(name: &str, cell: Cell, r: &Row) {
    println!(
        "{:<26} {:>7.0} {:>7.1} {:>8.1} {:>5} {:>5} {:>6} {:>6} {:>8.3} {:>8.3} {:>10.0} {:>16} {:>16} {:>6} {:>6} {:>4}",
        name,
        r.acked as f64 / cell.virtual_secs as f64,
        r.p50_ms,
        r.p99_ms,
        r.l0_hwm,
        r.l0_hwm_sampled,
        r.mem_hwm / 1024,
        r.compactions,
        r.comp_mib_s,
        r.flush_mib_s,
        r.flush_blocked_ms,
        format!(
            "{:.0}/{:.0}/{:.0}",
            r.stalled_ms[0], r.stalled_ms[1], r.stalled_ms[2]
        ),
        format!(
            "{}/{}/{}",
            r.stalled_writes[0], r.stalled_writes[1], r.stalled_writes[2]
        ),
        r.io_deferred,
        r.urgent,
        r.errs
    );
}

/// The characterisation grid. `MEASURE_SECS` (default 40) sets the virtual
/// length of each run.
#[test]
#[ignore = "measurement rig: run by hand, prints a table (ADR 0081 step 1)"]
fn lsm_stall_characterise() {
    let secs: u64 = std::env::var("MEASURE_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40);
    let only = std::env::var("MEASURE_CELL").ok();
    // One writer per engine: the production topology (a tablet's apply task is
    // the only writer of its private engine). Several writers per engine used
    // to starve the flush through `applies_in_flight` (issue #1341, fixed by
    // the flush-pending admission gate).
    let cells = [
        // (name, engines, writers per engine, sync delay in ms)
        ("e1_w1_5ms", 1, 1, 5),
        ("e1_w1_30ms", 1, 1, 30),
        ("e8_w1_5ms", 8, 1, 5),
        ("e8_w1_30ms", 8, 1, 30),
        ("e32_w1_5ms", 32, 1, 5),
        ("e32_w1_30ms", 32, 1, 30),
        ("e8_w8_5ms", 8, 8, 5),
        // Issue #1341: several writers on ONE engine (flush admission).
        ("e1_w8_5ms", 1, 8, 5),
        ("e1_w8_30ms", 1, 8, 30),
    ];
    print_header();
    for (name, engines, writers_per_engine, sync_ms) in cells {
        if only.as_ref().is_some_and(|o| !name.contains(o.as_str())) {
            continue;
        }
        let cell = Cell {
            engines,
            writers_per_engine,
            value_bytes: 1024,
            sync_ms,
            virtual_secs: secs,
        };
        let off = run_cell(7, cell, Mode::Off, 0);
        print_row(&format!("{name} off"), cell, &off);
        let stall = run_cell(7, cell, Mode::Stall, 0);
        print_row(&format!("{name} stall"), cell, &stall);
        let sustained = (stall.comp_mib_s * (1 << 20) as f64) as u64;
        if sustained > 0 {
            for pct in [100u64, 50, 25] {
                let rate = (sustained * pct / 100).max(1);
                let io = run_cell(7, cell, Mode::Io(pct), rate);
                print_row(&format!("{name} io{pct}"), cell, &io);
            }
        }
    }
}
