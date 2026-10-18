//! ADR 0081 Tier 3: real-thread, real-filesystem liveness of the L0 write
//! stall over `ProdEnv`. `SimEnv` proves the stall's logic and ordering; only a
//! real-thread run proves the wait loop, the maintenance-lock waiters and the
//! wakers make progress when compaction returns, with no `Err` and no deadlock.
//!
//! Timeout-guarded (a regression fails loudly instead of hanging; mirrors
//! `lsm_concurrent.rs`). Also carries an `#[ignore]`d measurement: the volume's
//! sequential fsync'd write bandwidth, the number an operator sizes
//! `compactionIoMiBPerSec` against (`-- --ignored --nocapture`).

// ADR 0003 / ADR 0061 Decision 4 (rung B5): a real-thread ProdEnv liveness test
// (see the module doc), so real tokio::time::timeout is the point here.
#![allow(
    clippy::disallowed_methods,
    reason = "real-thread ProdEnv liveness test (module doc); ADR 0061 Decision 4"
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use animus_env::{ProdEnv, nid};
use animus_storage::{CompactionGate, CompactionPermit, LsmEngine, LsmOptions, StorageEngine};

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

fn opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 4096,
        compaction_trigger: 4,
        target_table_bytes: 16 << 10,
        level_fanout: 4,
        wal_segment_bytes: 1 << 20,
        background_maintenance: true,
        l0_slowdown_factor: 3,
        l0_stop_factor: 5,
        l0_slowdown_write_rate: 1 << 20,
        l0_min_write_rate: 64 << 10,
        ..LsmOptions::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_writer_waits_without_err_and_resumes_when_compaction_returns() {
    let dir = std::env::temp_dir().join(format!("animus-lsm-stall-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = "127.0.0.1:0".parse().unwrap();
    let (env, _bound) = ProdEnv::bind(nid(0), addr, &dir).await.expect("bind");

    let work = async {
        let e = LsmEngine::open_with(env.clone(), "stall-", opts())
            .await
            .expect("open");
        let wedged = Arc::new(AtomicBool::new(true));
        e.set_compaction_gate(Arc::new(Wedge(wedged.clone())));

        const TOTAL: usize = 4000;
        let acked = Arc::new(AtomicUsize::new(0));
        let writer = {
            let e = e.clone();
            let acked = acked.clone();
            tokio::spawn(async move {
                for i in 0..TOTAL {
                    let v = vec![b'v'; 200];
                    e.put(format!("k{i:06}").as_bytes(), &v, (i + 1) as u64)
                        .await
                        .map_err(|err| format!("put {i} returned Err: {err}"))?;
                    acked.store(i + 1, Ordering::SeqCst);
                }
                Ok::<(), String>(())
            })
        };

        // The writer must stall (stop level) while compaction is withheld: its
        // progress stops, L0 stays bounded near the stop level, and nothing errs.
        let mut last = 0;
        let mut still_for = 0;
        let deadline = Instant::now() + Duration::from_secs(30);
        while still_for < 8 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let now = acked.load(Ordering::SeqCst);
            if now == last && now > 0 {
                still_for += 1;
            } else {
                still_for = 0;
                last = now;
            }
            assert!(
                Instant::now() < deadline,
                "the writer never stalled ({now} acked)"
            );
            assert!(
                !writer.is_finished(),
                "the writer finished while compaction was withheld"
            );
        }
        assert!(
            e.l0_table_count() <= 5 * 4 + 6,
            "L0 ran away past the stop level: {}",
            e.l0_table_count()
        );
        let st = e.stall_stats();
        assert!(
            st.writes.iter().sum::<u64>() > 0,
            "the engine reports no stalled writes: {st:?}"
        );

        // Compaction returns: the writer resumes and completes.
        wedged.store(false, Ordering::Release);
        let r = writer.await.expect("writer task");
        assert!(r.is_ok(), "{r:?}");
        assert_eq!(acked.load(Ordering::SeqCst), TOTAL);
        for i in (0..TOTAL).step_by(97) {
            let got = e.get(format!("k{i:06}").as_bytes()).await.unwrap();
            assert!(got.is_some(), "acked key {i} is missing");
        }
    };
    tokio::time::timeout(Duration::from_secs(120), work)
        .await
        .expect("the stalled writer deadlocked (timeout)");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Measurement, not an assertion: the sequential write+fsync bandwidth of the
/// volume the test directory lives on (the `dd oflag=dsync`-style baseline the
/// ADR 0081 sizing note refers to).
#[test]
#[ignore = "measurement"]
fn volume_sequential_fsync_write_baseline() {
    use std::io::Write;
    let dir = std::env::temp_dir().join(format!("animus-lsm-dd-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut f = std::fs::File::create(dir.join("dd")).unwrap();
    let chunk = vec![0xa5u8; 1 << 20];
    let t = Instant::now();
    for _ in 0..128 {
        f.write_all(&chunk).unwrap();
        f.sync_all().unwrap();
    }
    let s = t.elapsed().as_secs_f64();
    eprintln!(
        "DD 128 MiB with an fsync per MiB in {s:.2} s = {:.1} MiB/s",
        128.0 / s
    );
    let _ = std::fs::remove_dir_all(&dir);
}
