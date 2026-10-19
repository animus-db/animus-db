//! Issue #1341 follow-up: real-filesystem throughput of a follower-style apply
//! stream -- one sequential task applying single-entry `merge_batch`es of
//! ~5 KiB values (a Raft apply task catching up) -- under production
//! (`background_maintenance`) options. Measures entries/s, per-apply latency
//! and flush count, so the flush lane's cost to the writer can be compared
//! across trees. `-- --ignored --nocapture`.

#![allow(
    clippy::disallowed_methods,
    reason = "real-thread ProdEnv measurement; ADR 0061 Decision 4"
)]

use std::time::Instant;

use animus_env::{ProdEnv, nid};
use animus_storage::{LsmEngine, LsmOptions, MergeOp, StorageEngine};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn follower_apply_catchup_rate() {
    let dir = std::env::temp_dir().join(format!("animus-lsm-catchup-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = "127.0.0.1:0".parse().unwrap();
    let (env, _bound) = ProdEnv::bind(nid(0), addr, &dir).await.expect("bind");
    let e = LsmEngine::open_with(
        env.clone(),
        "catchup-",
        LsmOptions::production_raw_versions(),
    )
    .await
    .expect("open");
    let n: usize = std::env::var("CATCHUP_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1500);
    let value = vec![b'v'; 5000];
    // Optional load (env): CATCHUP_BG=K sibling engines (other tablets on the same
    // node/disk), each a sequential writer; CATCHUP_W2=1 a second concurrent
    // writer on the measured engine itself.
    let bg: usize = std::env::var("CATCHUP_BG")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let w2 = std::env::var("CATCHUP_W2").is_ok();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut bg_tasks = Vec::new();
    for b in 0..bg {
        let eb = LsmEngine::open_with(
            env.clone(),
            &format!("bg{b}-"),
            LsmOptions::production_raw_versions(),
        )
        .await
        .expect("open bg");
        let stop = stop.clone();
        let v = value.clone();
        bg_tasks.push(tokio::spawn(async move {
            let mut i = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                eb.merge_batch(vec![MergeOp::put(
                    format!("b{:06}", i % 400).into_bytes(),
                    v.clone(),
                    i + 1,
                )])
                .await
                .expect("bg apply");
                i += 1;
            }
        }));
    }
    if w2 {
        let e2 = e.clone();
        let stop = stop.clone();
        let v = value.clone();
        bg_tasks.push(tokio::spawn(async move {
            let mut i = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                e2.merge_batch(vec![MergeOp::put(
                    format!("w{:06}", i % 400).into_bytes(),
                    v.clone(),
                    1_000_000 + i,
                )])
                .await
                .expect("w2 apply");
                i += 1;
            }
        }));
    }
    let mut lat = Vec::with_capacity(n);
    let t0 = Instant::now();
    for i in 0..n {
        let t = Instant::now();
        e.merge_batch(vec![MergeOp::put(
            format!("k{:06}", i % 400).into_bytes(),
            value.clone(),
            (i + 1) as u64,
        )])
        .await
        .expect("apply");
        lat.push(t.elapsed());
    }
    let total = t0.elapsed();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for t in bg_tasks {
        let _ = t.await;
    }
    lat.sort();
    eprintln!(
        "catchup: {n} applies in {total:?} = {:.1}/s; p50 {:?} p99 {:?} max {:?}; flushes {}",
        n as f64 / total.as_secs_f64(),
        lat[n / 2],
        lat[n * 99 / 100],
        lat[n - 1],
        e.flush_count()
    );
    let _ = std::fs::remove_dir_all(&dir);
}
