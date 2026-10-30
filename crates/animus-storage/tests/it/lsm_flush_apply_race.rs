//! PR #26 under `SimEnv`: `LsmEngine::flush` builds its SSTable outside the
//! engine lock, so a write applied during that window sits in the memtable but
//! not in the flushed snapshot. The old unconditional `memtable.clear()` erased
//! such an acked, WAL-durable write, and the flush's WAL-segment GC then made
//! the loss permanent. Overlapping maintenance (flush vs flush vs compaction)
//! also used to allocate duplicate SSTable seqs and clobber manifests.
//!
//! `tests/lsm_concurrent.rs` pins this with real threads. `SimEnv` is
//! single-threaded but tasks do interleave at every await, and a
//! `DiskConfig::sync_delay` stretches both the writers' WAL syncs and the
//! flush's SSTable build/sync across virtual time, so the same interleavings are
//! reachable deterministically. Each seeded run:
//!
//! * opens an engine with tiny thresholds (frequent flushes, WAL rotation, and
//!   leveled compaction), in foreground- or background-maintenance mode,
//! * runs 2-5 closed-loop writer tasks (`env.spawn_task`, unique keys through
//!   `merge`) alongside 1-3 maintenance tasks that loop `flush_now` /
//!   `compact_now` on seeded periods, all under a seeded `sync_delay`,
//! * power-cuts the node at a seeded virtual time (optionally with torn-tail
//!   and corruption armed) and reopens strictly, three cycles in a row so a
//!   clobbered manifest from one cycle is read back by the next.
//!
//! Oracles: no maintenance call or writer returns `Err` (no faults are
//! injected); at each cycle's end, before the crash, every acked write is
//! readable from the live engine at its acked value (catches an erased
//! memtable slot); after every crash+reopen every write acked in any earlier
//! cycle is still readable (catches a lost WAL segment or manifest clobber); the
//! level invariant holds. Replay with `ANIMUS_SEED=<seed> cargo test -p
//! animus-storage --test it lsm_flush_apply_race::`; depth knob
//! `ANIMUS_LSM_FLUSH_RACE_SEEDS=K` (default 12).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Clock, EnvExt, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";
const CYCLES: u64 = 3;

fn opts(background: bool) -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 384,
        compaction_trigger: 2,
        target_table_bytes: 1024,
        level_fanout: 2,
        wal_segment_bytes: 1024,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: true,
        background_maintenance: background,
        ..LsmOptions::default()
    }
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

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
    {
        return vec![s];
    }
    let k: u64 = std::env::var("ANIMUS_LSM_FLUSH_RACE_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12);
    animus_test::corpus::shard_retain((0..k.max(1)).map(|i| 0x26_0000 + i * 0x9E37).collect())
}

#[derive(Default)]
struct Log {
    acked: Vec<(Vec<u8>, Vec<u8>)>,
    errs: Vec<String>,
}

fn open(sim: &Simulator, background: bool) -> Result<LsmEngine<SimEnv>, String> {
    block_on(LsmEngine::open_with(
        sim.env(nid(0)),
        PREFIX,
        opts(background),
    ))
    .map_err(|e| e.to_string())
}

/// Every write in `acked` must read back at its acked value.
fn check_readable(
    e: &LsmEngine<SimEnv>,
    acked: &[(Vec<u8>, Vec<u8>)],
    when: &str,
) -> Result<(), String> {
    for (k, v) in acked {
        match block_on(e.get(k)) {
            Ok(Some(got)) if &got.value == v => {}
            other => {
                return Err(format!(
                    "{when}: acked key {:?} unreadable: {other:?}",
                    String::from_utf8_lossy(k)
                ));
            }
        }
    }
    Ok(())
}

fn run_one(seed: u64) -> Result<(), String> {
    let mut mix = Mix(seed);
    let background = mix.range(0, 1) == 1;
    let writers = mix.range(2, 4) as usize;
    let flushers = mix.range(1, 3) as usize;
    let val_len = mix.range(40, 120) as usize;
    let sync_us = mix.range(200, 1500);
    let torn = mix.range(0, 1) == 1;

    let mut sim = Simulator::new(seed);
    let mut cfg = DiskConfig::default();
    cfg.set_sync_delay(Duration::from_micros(sync_us));
    sim.set_disk_config(cfg);

    let ver = Arc::new(AtomicU64::new(0));
    let mut all_acked: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut total_flushes = 0u64;
    for cycle in 0..CYCLES {
        let e = open(&sim, background).map_err(|m| format!("cycle {cycle}: open failed: {m}"))?;
        check_readable(&e, &all_acked, &format!("cycle {cycle} after reopen"))?;
        if !e.levels_non_overlapping() {
            return Err(format!("cycle {cycle}: levels overlap after reopen"));
        }

        let log = Arc::new(Mutex::new(Log::default()));
        let stop = Arc::new(AtomicBool::new(false));
        for w in 0..writers {
            let (e, log, stop, ver) = (e.clone(), log.clone(), stop.clone(), ver.clone());
            let env = sim.env(nid(0));
            let think = Duration::from_micros(mix.range(1500, 6000));
            env.clone().spawn_task(async move {
                let mut i = 0u64;
                while !stop.load(Ordering::Acquire) {
                    let v = ver.fetch_add(1, Ordering::AcqRel) + 1;
                    let key = format!("c{cycle}w{w}-{i:06}").into_bytes();
                    let val = vec![b'a' + (w as u8 % 26); val_len];
                    match e.merge(&key, &val, v).await {
                        Ok(_) => log.lock().unwrap().acked.push((key, val)),
                        Err(err) => {
                            log.lock().unwrap().errs.push(format!("writer {w}: {err}"));
                            return;
                        }
                    }
                    i += 1;
                    env.sleep(think).await;
                }
            });
        }
        for f in 0..flushers {
            let (e, log, stop) = (e.clone(), log.clone(), stop.clone());
            let env = sim.env(nid(0));
            let period = Duration::from_micros(mix.range(300, 3000));
            let full = f % 2 == 0;
            env.clone().spawn_task(async move {
                while !stop.load(Ordering::Acquire) {
                    env.sleep(period).await;
                    let r = if full {
                        e.flush_now().await
                    } else {
                        e.compact_now().await
                    };
                    if let Err(err) = r {
                        log.lock()
                            .unwrap()
                            .errs
                            .push(format!("maintainer {f}: {err}"));
                        return;
                    }
                }
            });
        }

        // Run to a seeded virtual time, in slices so the oracle can stop early.
        let run_ms = mix.range(120, 360);
        for _ in 0..run_ms / 4 {
            sim.run_for(Duration::from_millis(4));
        }
        let (acked, errs) = {
            let l = log.lock().unwrap();
            (l.acked.clone(), l.errs.clone())
        };
        if !errs.is_empty() {
            return Err(format!("cycle {cycle}: unexpected errors {errs:?}"));
        }
        if acked.is_empty() {
            return Err(format!("cycle {cycle}: no write was ever acked"));
        }
        total_flushes += e.flush_count();
        all_acked.extend(acked);
        // Live check, mid-flight (writers and maintainers still running): every
        // acked write is visible right now.
        check_readable(&e, &all_acked, &format!("cycle {cycle} live"))?;

        // Power cut with everything still running.
        stop.store(true, Ordering::Release);
        drop(e);
        sim.stop(nid(0));
        let mut crash_cfg = DiskConfig::default();
        crash_cfg.set_sync_delay(Duration::from_micros(sync_us));
        crash_cfg.torn_tail_on_crash = torn;
        crash_cfg.corrupt_on_crash = torn;
        sim.set_disk_config(crash_cfg);
        sim.crash(nid(0));
        sim.restart(nid(0));
        let mut cfg = DiskConfig::default();
        cfg.set_sync_delay(Duration::from_micros(sync_us));
        sim.set_disk_config(cfg);
    }
    // The race is only exercised if flushes really ran against the writers.
    if total_flushes < 3 {
        return Err(format!(
            "only {total_flushes} flushes ran (bg={background} writers={writers} \
             flushers={flushers} sync_us={sync_us}): the workload does not exercise the race"
        ));
    }
    let e = open(&sim, background).map_err(|m| format!("final open failed: {m}"))?;
    check_readable(&e, &all_acked, "after the final reopen")
}

#[test]
fn concurrent_writers_flush_and_compaction_lose_no_acked_write_across_crashes() {
    for seed in seeds() {
        if let Err(why) = run_one(seed) {
            panic!("seed={seed}: {why}");
        }
    }
}
