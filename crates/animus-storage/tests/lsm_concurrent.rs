//! Real-multithreading regression for WAL group commit.
//!
//! The deterministic single-threaded `SimEnv` cannot exercise a preemptive
//! interleaving where a writer enqueues its WAL record *while the current group-
//! commit leader is mid-`fsync`*. That interleaving stranded such a writer (it
//! parked waiting for a `sync` that never covered its record and never re-led),
//! deadlocking under the real multi-threaded `ProdEnv` (surfaced by the
//! benchmark's concurrent phase). This runs many concurrent writers on a real
//! tokio multi-thread runtime + real disk, guarded by a timeout so a regression
//! fails loudly instead of hanging.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use animus_env::ProdEnv;
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_do_not_deadlock() {
    let dir = std::env::temp_dir().join(format!("animus-gc-deadlock-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = "127.0.0.1:0".parse().unwrap();
    let (env, _bound) = ProdEnv::bind(0, addr, &dir).await.expect("bind ProdEnv");
    let lsm = LsmEngine::open(env, "db-").await.expect("open lsm");

    // Many rounds of many concurrent writers to disjoint keys. `merge` (per-key
    // LWW) avoids the global monotonic-version contract so concurrent writers
    // don't collide on versions; it still routes through the group-commit WAL.
    let work = async {
        for round in 0..50u64 {
            let mut handles = Vec::new();
            for w in 0..16u64 {
                let lsm = lsm.clone();
                handles.push(tokio::spawn(async move {
                    let key = format!("r{round}-w{w}");
                    let version = round * 100 + w + 1;
                    lsm.merge(key.as_bytes(), b"v", version).await.unwrap();
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(30), work)
        .await
        .expect("group commit deadlocked: a writer parked on a sync that never covered its record");

    // Every acked write is durably readable.
    assert_eq!(lsm.get(b"r0-w0").await.unwrap().unwrap().value, b"v");
    assert_eq!(lsm.get(b"r49-w15").await.unwrap().unwrap().value, b"v");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A scan must not fail (or silently truncate) when a **concurrent compaction**
/// removes an SSTable file it referenced. Reads snapshot the reader set then fetch
/// blocks lock-free; a compaction swaps the readers and `remove`s the superseded
/// files, so a lock-free read of a just-removed file used to get an empty (short)
/// read → `Backend("short sstable block read")`, which the data plane `.expect()`ed
/// → a panicked worker (observed under bulk-seed → auto-split). The engine now
/// re-snapshots and retries on a raced compaction. This drives the race on a real
/// runtime + disk: a writer storm with tiny flush/compaction thresholds (so
/// compactions fire continuously) while scanners loop `entries()` over the whole
/// table; every scan must succeed, and the final scan must see *all* keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scans_survive_concurrent_compaction() {
    let dir = std::env::temp_dir().join(format!("animus-read-compact-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = "127.0.0.1:0".parse().unwrap();
    let (env, _bound) = ProdEnv::bind(0, addr, &dir).await.expect("bind ProdEnv");
    // Tiny thresholds: flush almost every few writes, compact every 2 L0 tables —
    // so flushes + compactions (which remove old files) run continuously under the
    // scanners.
    let opts = LsmOptions {
        flush_threshold_bytes: 256,
        compaction_trigger: 2,
        target_table_bytes: 1024,
        ..LsmOptions::default()
    };
    let lsm = LsmEngine::open_with(env, "db-", opts)
        .await
        .expect("open lsm");

    const N: u64 = 3000;
    let done = Arc::new(AtomicBool::new(false));

    // Writer: many distinct keys, forcing repeated flush + compaction.
    let writer = {
        let lsm = lsm.clone();
        let done = done.clone();
        tokio::spawn(async move {
            for i in 0..N {
                lsm.merge(format!("k{i:08}").as_bytes(), b"value", i + 1)
                    .await
                    .unwrap();
            }
            done.store(true, Ordering::SeqCst);
        })
    };

    // Scanners: full-table scans in a tight loop while the writer runs. Each scan
    // must return `Ok` (the regression: it used to error/panic mid-compaction) and
    // its keys must be a monotonic superset over time (never lose a key already
    // observed — which a compaction-removed table would cause).
    let scanner = || {
        let lsm = lsm.clone();
        let done = done.clone();
        tokio::spawn(async move {
            let mut max_seen = 0usize;
            loop {
                let entries = lsm
                    .entries()
                    .await
                    .expect("scan errored under concurrent compaction");
                assert!(
                    entries.len() >= max_seen,
                    "scan lost keys under compaction: {} < {}",
                    entries.len(),
                    max_seen
                );
                max_seen = entries.len();
                if done.load(Ordering::SeqCst) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
    };
    let scanners: Vec<_> = (0..3).map(|_| scanner()).collect();

    let run = async {
        writer.await.unwrap();
        for s in scanners {
            s.await.unwrap();
        }
    };
    tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("writer + scanners did not finish");

    // The final scan sees every written key.
    let entries = lsm.entries().await.unwrap();
    assert_eq!(entries.len() as u64, N, "final scan must see all keys");
    let _ = std::fs::remove_dir_all(&dir);
}
