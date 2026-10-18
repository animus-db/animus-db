//! Issue #1341: with several writers overlapping on ONE engine over a slow
//! disk, `applies_in_flight` is almost never zero, so a flush that merely skips
//! while a write is in flight starves, the memtable pins at its cap, and
//! goodput collapses. The flush now closes an entry gate and drains the
//! in-flight applies instead (`Inner::flush_pending`).
//!
//! Oracles per seed: (1) flushes keep happening (at least one per two
//! thresholds of data acked), (2) the memtable high-water stays within cap plus
//! the in-flight slack, (3) every acked write is readable. Crash-safety of the
//! drain (no partially applied batch persisted) is the `lsm_crash*` corpora's
//! job. Verified to FAIL against the pre-fix engine (flush skipped while any
//! apply was in flight).
//!
//! Knobs: `ANIMUS_FLUSH_STARVATION_SEEDS=K` (default 1), `ANIMUS_SEED=<seed>`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Clock, EnvExt, nid};
use animus_sim::{DiskConfig, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";
const WRITERS: usize = 8;
const VALUE: usize = 128;
const FLUSH_BYTES: usize = 4096;
type Acked = Arc<Mutex<Vec<(Vec<u8>, u64)>>>;

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
    {
        return vec![s];
    }
    let k: u64 = std::env::var("ANIMUS_FLUSH_STARVATION_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    (0..k.max(1)).map(|i| 0x1341 + i).collect()
}

fn opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: FLUSH_BYTES,
        compaction_trigger: 4,
        target_table_bytes: 16 << 10,
        level_fanout: 4,
        wal_segment_bytes: 1 << 20,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: true,
        background_maintenance: true,
        l0_slowdown_factor: 0,
        l0_stop_factor: 0,
        ..LsmOptions::default()
    }
}

fn run(seed: u64, sync_ms: u64) {
    let mut sim = Simulator::new(seed);
    let env0 = sim.env(nid(0));
    let engine = block_on(LsmEngine::open_with(env0.clone(), PREFIX, opts())).expect("open");
    let mut cfg = DiskConfig::default();
    cfg.set_sync_delay(Duration::from_millis(sync_ms));
    sim.set_disk_config(cfg);

    let acked: Acked = Arc::new(Mutex::new(Vec::new()));
    let ver = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    for w in 0..WRITERS {
        let (e, acked, ver, stop) = (engine.clone(), acked.clone(), ver.clone(), stop.clone());
        let env = sim.env(nid(0));
        env.clone().spawn_task(async move {
            let mut i = 0u64;
            while !stop.load(Ordering::Acquire) {
                let v = ver.fetch_add(1, Ordering::AcqRel) + 1;
                let key = format!("w{w}-{i:06}").into_bytes();
                if e.merge(&key, &[w as u8; VALUE], v).await.is_ok() {
                    acked.lock().unwrap().push((key, v));
                }
                i += 1;
                env.sleep(Duration::from_micros(200)).await;
            }
        });
    }
    let mut mem_hwm = 0usize;
    for _ in 0..500 {
        sim.run_for(Duration::from_millis(10));
        mem_hwm = mem_hwm.max(engine.memtable_bytes());
    }
    stop.store(true, Ordering::Release);
    sim.run_for(Duration::from_secs(2));

    let acked = acked.lock().unwrap().clone();
    let bytes = acked.len() * (VALUE + 24);
    let flushes = engine.flush_count();
    let want = (bytes / (2 * FLUSH_BYTES)) as u64;
    eprintln!(
        "seed={seed:#x} sync={sync_ms}ms acked={} flushes={flushes} (want>={want}) mem_hwm={mem_hwm}",
        acked.len()
    );
    assert!(
        flushes >= want.max(1),
        "seed {seed:#x}: flush starved: {flushes} flushes for {bytes} acked bytes (want >= {want}); \
         replay: ANIMUS_SEED={seed:#x}"
    );
    // cap = 4 x threshold; allow the in-flight writers' slack.
    assert!(
        mem_hwm <= 4 * FLUSH_BYTES + WRITERS * (VALUE + 64) * 2,
        "seed {seed:#x}: memtable pinned at {mem_hwm}; replay: ANIMUS_SEED={seed:#x}"
    );
    for (k, v) in &acked {
        let got = block_on(engine.get(k)).expect("get");
        assert!(got.is_some(), "seed {seed:#x}: acked {k:?}@{v} lost");
    }
}

#[test]
fn flush_is_not_starved_by_overlapping_writers() {
    for seed in seeds() {
        run(seed, 5);
        run(seed, 30);
    }
}
