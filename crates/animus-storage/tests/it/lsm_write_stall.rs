//! Issue #1338: the `background_maintenance` memtable gate
//! (`await_backpressure`) must never fail a write merely because time passed.
//! A stall below Raft may delay a committed entry but must not fail it (the
//! raftkv apply task panics on a non-`StorageFull` engine error). A genuine,
//! persistent maintenance failure still fails, but only after N consecutive
//! failures; ENOSPC stays the recoverable `StorageFull` class.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{EnvExt, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine, StorageError};
use futures::executor::block_on;

const PREFIX: &str = "db/";

fn opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 256,
        compaction_trigger: 4,
        target_table_bytes: 1024,
        level_fanout: 2,
        wal_segment_bytes: 1 << 20,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: true,
    }
}

fn open(sim: &Simulator) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, opts())).expect("open")
}

/// One value bigger than the 1 KiB hard cap (4 x 256), so the writer lands in
/// the gate with the memtable over the cap.
fn big() -> Vec<u8> {
    vec![b'x'; 2048]
}

type Outcome = Arc<Mutex<Option<Result<(), StorageError>>>>;

fn spawn_big_put(sim: &Simulator, e: &LsmEngine<SimEnv>, version: u64) -> Outcome {
    let out: Outcome = Arc::new(Mutex::new(None));
    let (o, w) = (Arc::clone(&out), e.clone());
    sim.env(nid(0)).spawn_task(async move {
        let r = w.put(b"big", &big(), version).await;
        *o.lock().unwrap() = Some(r);
    });
    out
}

/// The sync_delay config used to make maintenance slow: every fsync takes
/// `d` of virtual time, so a flush (several syncs) outlasts the old 10 s gate.
fn slow(d: Duration) -> DiskConfig {
    let mut c = DiskConfig::default();
    c.set_sync_delay(d);
    c
}

/// Step virtual time until the write has applied (its post-write trigger put a
/// maintenance task in flight), so a fault injected next hits maintenance and
/// not the write's own WAL commit.
fn run_until_applied(sim: &mut Simulator, e: &LsmEngine<SimEnv>) {
    for _ in 0..1000 {
        if e.background_maintenance_in_flight() {
            return;
        }
        sim.run_for(Duration::from_millis(10));
    }
    panic!("the write never applied");
}

fn seeds() -> impl Iterator<Item = u64> {
    let k: u64 = std::env::var("ANIMUS_WRITE_STALL_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    (0..k.max(1)).map(|i| 1338 + i)
}

/// A maintenance run that is merely slow (30 s per fsync, so a flush takes
/// over a minute of virtual time) keeps the memtable over the cap for far
/// longer than the old 10 000 x 1 ms poll bound. The write must wait and then
/// succeed, never `Err` (on origin/main it returned `StorageError::Backend`).
#[test]
fn slow_maintenance_delays_but_never_fails_the_write() {
    for seed in seeds() {
        let mut sim = Simulator::new(seed);
        let e = open(&sim);
        sim.set_disk_config(slow(Duration::from_secs(30)));
        let out = spawn_big_put(&sim, &e, 1);
        sim.run_for(Duration::from_secs(1800));
        let r = out.lock().unwrap().take();
        assert!(
            matches!(r, Some(Ok(()))),
            "seed {seed}: a slow (not failing) maintenance run must delay the write, got {r:?}"
        );
        assert_eq!(e.background_maintenance_error(), None, "seed {seed}");
        sim.set_disk_config(DiskConfig::default());
        sim.run_until_quiescent(1_000_000);
        let got = block_on(e.get(b"big")).unwrap().unwrap();
        assert_eq!(got.value, big(), "seed {seed}");
    }
}

/// A genuinely failing disk (non-ENOSPC, persistent) still fails the write,
/// but only after N consecutive maintenance failures, not after elapsed time:
/// the error is `Backend`, and the engine counted at least 8 failures.
#[test]
fn persistent_non_enospc_failure_errors_after_n_consecutive_failures() {
    for seed in seeds() {
        let mut sim = Simulator::new(seed);
        let e = open(&sim);
        // The WAL sync (100 ms) lets the write apply; the disk then dies while
        // the first flush is in flight.
        sim.set_disk_config(slow(Duration::from_millis(100)));
        let out = spawn_big_put(&sim, &e, 1);
        run_until_applied(&mut sim, &e);
        let mut dead = DiskConfig::default();
        dead.set_error_prob(1.0);
        sim.set_disk_config(dead);
        sim.run_for(Duration::from_secs(60));
        let r = out.lock().unwrap().take();
        assert!(
            matches!(r, Some(Err(StorageError::Backend(_)))),
            "seed {seed}: a persistently failing disk must fail the write, got {r:?}"
        );
        assert!(
            e.maintenance_consecutive_failures() >= 8,
            "seed {seed}: failed after only {} consecutive failures; {r:?}; {:?}",
            e.maintenance_consecutive_failures(),
            e.maintenance_consecutive_failures()
        );
        assert!(e.background_maintenance_error().is_some(), "seed {seed}");
    }
}

/// A transient failure run shorter than N does not fail the write: maintenance
/// recovers, the counter resets, and the write succeeds.
#[test]
fn transient_failures_below_n_do_not_fail_the_write() {
    for seed in seeds() {
        let mut sim = Simulator::new(seed);
        let e = open(&sim);
        sim.set_disk_config(slow(Duration::from_millis(100)));
        let out = spawn_big_put(&sim, &e, 1);
        run_until_applied(&mut sim, &e);
        let mut dead = DiskConfig::default();
        dead.set_error_prob(1.0);
        sim.set_disk_config(dead);
        // Heal after a few ms: fewer than 8 polls (1 ms each) have elapsed.
        sim.run_for(Duration::from_millis(3));
        sim.set_disk_config(DiskConfig::default());
        sim.run_for(Duration::from_secs(60));
        let r = out.lock().unwrap().take();
        assert!(
            matches!(r, Some(Ok(()))),
            "seed {seed}: a brief fault must not fail the write, got {r:?}"
        );
        assert_eq!(e.maintenance_consecutive_failures(), 0, "seed {seed}");
    }
}

/// ENOSPC stays the recoverable `StorageFull` class (never `Backend`, never
/// counted toward the fail-stop), and a retry after space returns succeeds.
#[test]
fn enospc_still_surfaces_as_storage_full_and_recovers() {
    for seed in seeds() {
        let mut sim = Simulator::new(seed);
        let e = open(&sim);
        sim.set_disk_config(slow(Duration::from_millis(100)));
        let out = spawn_big_put(&sim, &e, 1);
        run_until_applied(&mut sim, &e);
        let mut full = DiskConfig::default();
        full.set_enospc_prob(1.0);
        sim.set_disk_config(full);
        sim.run_for(Duration::from_secs(60));
        let r = out.lock().unwrap().take();
        assert!(
            matches!(r, Some(Err(StorageError::StorageFull(_)))),
            "seed {seed}: ENOSPC must surface as StorageFull, got {r:?}"
        );
        assert_eq!(e.maintenance_consecutive_failures(), 0, "seed {seed}");

        // Space is back: a newer-versioned write goes through (the original is already applied).
        sim.set_disk_config(DiskConfig::default());
        let out = spawn_big_put(&sim, &e, 2);
        sim.run_for(Duration::from_secs(60));
        let r = out.lock().unwrap().take();
        assert!(matches!(r, Some(Ok(()))), "seed {seed}: retry got {r:?}");
    }
}
