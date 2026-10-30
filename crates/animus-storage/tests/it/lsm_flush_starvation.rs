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
    animus_test::corpus::shard_retain((0..k.max(1)).map(|i| 0x1341 + i).collect())
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

/// Issue #1341, second half: a flush must not wait for a compaction that is
/// already running. The disk is slow, a compaction with many small output
/// tables is in progress, and a memtable flush started meanwhile has to land
/// (and be readable, with the compaction's inputs intact) before the
/// compaction ends. Before the flush/compaction lanes were split this flush
/// queued behind the whole compaction.
#[test]
fn flush_completes_while_a_compaction_is_running() {
    for seed in seeds() {
        let mut sim = Simulator::new(seed);
        let env0 = sim.env(nid(0));
        let o = LsmOptions {
            flush_threshold_bytes: 1 << 20,
            compaction_trigger: 3,
            target_table_bytes: 256,
            level_fanout: 4,
            wal_segment_bytes: 1 << 20,
            tombstone_grace_versions: 1 << 20,
            trust_monotonic_versions: true,
            // Background maintenance, so a write never runs the due compaction
            // inline (it would then queue on the compaction lane itself).
            background_maintenance: true,
            l0_slowdown_factor: 0,
            l0_stop_factor: 0,
            ..LsmOptions::default()
        };
        let engine = block_on(LsmEngine::open_with(env0.clone(), PREFIX, o)).expect("open");
        let mut ver = 0u64;
        // Two flushes: L0 = 2, below the trigger of 3.
        for round in 0..2u8 {
            for k in 0..30u32 {
                ver += 1;
                block_on(engine.merge(format!("k{k:04}").as_bytes(), &[round; 100], ver))
                    .expect("merge");
            }
            block_on(engine.flush_now()).expect("flush");
        }
        let mut cfg = DiskConfig::default();
        cfg.set_sync_delay(Duration::from_millis(200));
        sim.set_disk_config(cfg);

        // Task A: the third flush makes L0 = 3, then a long compaction runs.
        let compaction_done = Arc::new(AtomicBool::new(false));
        {
            let (e, done) = (engine.clone(), compaction_done.clone());
            let base = ver;
            env0.clone().spawn_task(async move {
                for k in 0..30u32 {
                    e.merge(
                        format!("k{k:04}").as_bytes(),
                        &[2u8; 100],
                        base + 1 + u64::from(k),
                    )
                    .await
                    .expect("merge");
                }
                e.flush_now().await.expect("flush a");
                done.store(true, Ordering::Release);
            });
        }
        // Run until the third flush has landed (compaction now in progress).
        let mut guard = 0;
        while engine.flush_count() < 3 && guard < 2000 {
            sim.run_for(Duration::from_millis(50));
            guard += 1;
        }
        assert_eq!(engine.flush_count(), 3, "seed {seed:#x}: third flush");
        assert_eq!(
            engine.compaction_count(),
            0,
            "seed {seed:#x}: compaction already committed; test needs a longer one"
        );
        // Task B: a fresh write and a flush while the compaction runs.
        {
            let e = engine.clone();
            let v = ver + 100;
            env0.clone().spawn_task(async move {
                e.merge(b"zz-during", &[9u8; 100], v).await.expect("merge");
                e.flush_now().await.expect("flush b");
            });
        }
        let mut guard = 0;
        while engine.flush_count() < 4 && guard < 4000 {
            sim.run_for(Duration::from_millis(50));
            guard += 1;
        }
        assert_eq!(engine.flush_count(), 4, "seed {seed:#x}: fourth flush");
        assert_eq!(
            engine.compaction_count(),
            0,
            "seed {seed:#x}: the flush waited for the running compaction to commit; \
             replay: ANIMUS_SEED={seed:#x}"
        );
        sim.run_for(Duration::from_secs(120));
        assert!(compaction_done.load(Ordering::Acquire), "compaction ends");
        // Nothing lost across the interleaved installs.
        let got = block_on(engine.get(b"zz-during")).expect("get");
        assert!(got.is_some(), "seed {seed:#x}: flushed-during write lost");
        for k in 0..30u32 {
            let got = block_on(engine.get(format!("k{k:04}").as_bytes()))
                .expect("get")
                .expect("present");
            assert_eq!(got.value, vec![2u8; 100], "seed {seed:#x}: k{k}");
        }
    }
}
