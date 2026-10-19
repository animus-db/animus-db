//! Issue #1349: real-filesystem cost of an apply-style loop on hot keys.
//!
//! Per entry: a point `get` plus a tombstone-aware newest-version probe of a
//! hot key (many retained versions, spread over flushed tables), then a
//! `merge_batch`. Run in a DEBUG build, where LZ4 + crc32 per block dominate:
//! `cargo test -p animus-storage --test lsm_apply_read_amp_prod -- --ignored --nocapture`.
//! `RAMP_PROBE=scan` uses the old one-key range-scan probe, default is the
//! point probe; `RAMP_N` / `RAMP_KEYS` size the run. Prints entries/s and
//! block reads per entry. `SimEnv` bounds block reads deterministically in
//! `tests/it/lsm_hot_key_read_amp.rs`; this is the wall-clock companion.

#![allow(
    clippy::disallowed_methods,
    reason = "real-thread ProdEnv measurement; ADR 0061 Decision 4"
)]

use std::time::Instant;

use animus_env::{ProdEnv, nid};
use animus_storage::{LsmEngine, LsmOptions, MergeOp, StorageEngine};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn apply_path_read_amplification() {
    let dir = std::env::temp_dir().join(format!("animus-lsm-ramp-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = "127.0.0.1:0".parse().unwrap();
    let (env, _bound) = ProdEnv::bind(nid(0), addr, &dir).await.expect("bind");
    let e = LsmEngine::open_with(env.clone(), "ramp-", LsmOptions::production_raw_versions())
        .await
        .expect("open");
    let env_usize = |name: &str, default: usize| {
        std::env::var(name)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    };
    let n = env_usize("RAMP_N", 1500);
    let keys = env_usize("RAMP_KEYS", 20);
    let scan_probe = std::env::var("RAMP_PROBE").is_ok_and(|p| p == "scan");
    let value = vec![b'v'; 700];
    e.reset_block_reads();
    let mut read_time = std::time::Duration::ZERO;
    let t0 = Instant::now();
    for i in 0..n {
        let key = format!("k{:06}", i % keys).into_bytes();
        let t = Instant::now();
        let _ = e.get(&key).await.expect("get");
        if scan_probe {
            let mut end = key.clone();
            end.push(0);
            let _ = e.scan_with_tombstones(&key, &end).await.expect("scan");
        } else {
            let _ = StorageEngine::latest_version_incl_tombstone(&e, &key)
                .await
                .expect("probe");
        }
        read_time += t.elapsed();
        e.merge_batch(vec![MergeOp::put(key, value.clone(), (i + 1) as u64)])
            .await
            .expect("apply");
    }
    eprintln!(
        "ramp[{}]: {n} entries {:?} ({:.1}/s), read time {:?} ({:?}/entry), block reads {} ({:.2}/entry), flushes {}",
        if scan_probe { "scan" } else { "point" },
        t0.elapsed(),
        n as f64 / t0.elapsed().as_secs_f64(),
        read_time,
        read_time / n as u32,
        e.block_read_count(),
        e.block_read_count() as f64 / n as f64,
        e.flush_count()
    );
    let _ = std::fs::remove_dir_all(&dir);
}
