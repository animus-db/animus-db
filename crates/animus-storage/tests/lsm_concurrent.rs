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
//!
//! Also the regressions for **flush/compaction concurrency**: flush-vs-apply
//! (a write applied during the SSTable-build window used to be erased by the
//! flush's blanket `memtable.clear()`, then its WAL segment GC'd — acked-write
//! loss) and flush-vs-flush / flush-vs-compaction (overlapping maintenance used
//! to allocate duplicate SSTable seqs and clobber manifests). See the
//! maintenance-lock + surgical-clear fix in `lsm.rs::flush`.

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

/// Assert every `(key, value)` in `expected` reads back from `lsm`, via one full
/// scan (`entries` merges memtable + SSTables).
async fn assert_all_present(
    lsm: &LsmEngine<ProdEnv>,
    expected: &std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
    context: &str,
) {
    let entries = lsm.entries().await.expect("scan");
    let got: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
        entries.into_iter().map(|(k, vv)| (k, vv.value)).collect();
    for (k, v) in expected {
        match got.get(k) {
            Some(g) if g == v => {}
            Some(g) => panic!(
                "{context}: key {:?} has wrong value {:?} (expected {:?})",
                String::from_utf8_lossy(k),
                String::from_utf8_lossy(g),
                String::from_utf8_lossy(v)
            ),
            None => panic!(
                "{context}: acked write lost: key {:?} missing",
                String::from_utf8_lossy(k)
            ),
        }
    }
}

/// Concurrent writers **with flushes actually occurring** must lose no acked
/// write — before or after a restart/recovery.
///
/// Regression for the flush-vs-apply race: `flush()` snapshotted the memtable,
/// released the lock across the SSTable build, then did an unconditional
/// `memtable.clear()` — erasing any write applied (acked, WAL-durable) by a
/// concurrent task during the build window; a *later* flush then advanced the
/// WAL watermark past that write's seq and GC'd its segment — permanent loss.
/// The pre-existing `concurrent_writers_do_not_deadlock` never crosses the
/// flush threshold, which is exactly why this went unseen: here tiny thresholds
/// make writer-driven flushes (and compactions) fire continuously under the
/// concurrent writers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_with_flushes_lose_no_acked_write() {
    let dir = std::env::temp_dir().join(format!("animus-flush-race-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = "127.0.0.1:0".parse().unwrap();
    let (env, _bound) = ProdEnv::bind(0, addr, &dir).await.expect("bind ProdEnv");
    let opts = LsmOptions {
        flush_threshold_bytes: 1024,
        compaction_trigger: 2,
        target_table_bytes: 4096,
        wal_segment_bytes: 1024,
        ..LsmOptions::default()
    };
    let lsm = LsmEngine::open_with(env, "db-", opts)
        .await
        .expect("open lsm");

    const WRITERS: u64 = 8;
    const PER_WRITER: u64 = 300;
    let work = async {
        let mut handles = Vec::new();
        for w in 0..WRITERS {
            let lsm = lsm.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..PER_WRITER {
                    let key = format!("w{w:02}-k{i:05}");
                    // Value derived from the key so cross-contamination shows.
                    let value = format!("val-{key}");
                    let version = w * 100_000 + i + 1;
                    assert!(
                        lsm.merge(key.as_bytes(), value.as_bytes(), version)
                            .await
                            .expect("merge acked"),
                        "fresh key merge must apply"
                    );
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    };
    tokio::time::timeout(Duration::from_secs(120), work)
        .await
        .expect("concurrent writers with flushes did not finish (deadlock?)");

    // The scenario has teeth only if flushes really happened.
    assert!(
        lsm.flush_count() >= 1,
        "test invalid: no flush occurred (threshold not crossed)"
    );

    let expected: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = (0..WRITERS)
        .flat_map(|w| {
            (0..PER_WRITER).map(move |i| {
                let key = format!("w{w:02}-k{i:05}");
                (key.clone().into_bytes(), format!("val-{key}").into_bytes())
            })
        })
        .collect();

    // Every acked write reads back live...
    assert_all_present(&lsm, &expected, "live").await;

    // ...and after a restart (fresh env + engine over the same directory), so a
    // write surviving only until its WAL segment was wrongly GC'd is caught.
    drop(lsm);
    let (env2, _bound2) = ProdEnv::bind(0, addr, &dir).await.expect("rebind ProdEnv");
    let reopened = LsmEngine::open_with(env2, "db-", opts)
        .await
        .expect("reopen lsm after restart");
    assert_all_present(&reopened, &expected, "after restart").await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// `flush_now`/`compact_now` driven from a **separate task while writes stream**
/// (the `POST /admin/storage/flush|compact` path) must lose no acked write and
/// must leave the manifest valid. This is the real-world trigger for the
/// flush-vs-apply and flush-vs-flush races: the v1 client path is a single Raft
/// apply task, but the admin actions run concurrently with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forced_flush_under_live_load_loses_no_acked_write() {
    let dir = std::env::temp_dir().join(format!("animus-admin-flush-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = "127.0.0.1:0".parse().unwrap();
    let (env, _bound) = ProdEnv::bind(0, addr, &dir).await.expect("bind ProdEnv");
    let opts = LsmOptions {
        flush_threshold_bytes: 2048,
        compaction_trigger: 2,
        target_table_bytes: 4096,
        wal_segment_bytes: 1024,
        ..LsmOptions::default()
    };
    let lsm = LsmEngine::open_with(env, "db-", opts)
        .await
        .expect("open lsm");

    const N: u64 = 1500;
    let done = Arc::new(AtomicBool::new(false));

    // Writer stream (the "apply loop"): sequential merges, crossing the flush
    // threshold repeatedly so writer-driven flushes race the admin actions.
    let writer = {
        let lsm = lsm.clone();
        let done = done.clone();
        tokio::spawn(async move {
            for i in 0..N {
                let key = format!("k{i:08}");
                let value = format!("val-{key}");
                lsm.merge(key.as_bytes(), value.as_bytes(), i + 1)
                    .await
                    .expect("merge acked");
            }
            done.store(true, Ordering::SeqCst);
        })
    };

    // Admin task: hammer forced flushes + compactions the whole time.
    let admin = {
        let lsm = lsm.clone();
        let done = done.clone();
        tokio::spawn(async move {
            let mut forced = 0u64;
            while !done.load(Ordering::SeqCst) {
                lsm.flush_now().await.expect("forced flush failed");
                lsm.compact_now().await.expect("forced compaction failed");
                forced += 1;
                tokio::task::yield_now().await;
            }
            forced
        })
    };

    let run = async {
        writer.await.unwrap();
        admin.await.unwrap()
    };
    let forced = tokio::time::timeout(Duration::from_secs(120), run)
        .await
        .expect("writer + admin flusher did not finish (deadlock?)");
    assert!(forced >= 1, "test invalid: no forced flush ran");

    let expected: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = (0..N)
        .map(|i| {
            let key = format!("k{i:08}");
            (key.clone().into_bytes(), format!("val-{key}").into_bytes())
        })
        .collect();
    assert_all_present(&lsm, &expected, "live").await;

    // Manifest stays valid: unique SSTable seqs, non-overlapping leveled runs.
    let views = lsm.sstable_views();
    let seqs: Vec<u64> = views.iter().map(|v| v.seq).collect();
    let mut deduped = seqs.clone();
    deduped.dedup();
    assert_eq!(seqs, deduped, "duplicate SSTable seq in manifest: {seqs:?}");
    assert!(lsm.levels_non_overlapping(), "L1+ runs overlap");

    // And it recovers: a reopen re-validates the manifest + replays the WAL.
    drop(lsm);
    let (env2, _bound2) = ProdEnv::bind(0, addr, &dir).await.expect("rebind ProdEnv");
    let reopened = LsmEngine::open_with(env2, "db-", opts)
        .await
        .expect("reopen lsm after forced-flush load");
    assert_all_present(&reopened, &expected, "after restart").await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Overlapping `flush_now` calls must not corrupt the engine: no duplicate
/// SSTable seq (two overlapping flushes used to both allocate `next_seq + 1`,
/// since `next_seq` only advances at the final swap), and every call either
/// completes a flush or no-ops. Writes stream concurrently to widen the build
/// window the calls overlap in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_flush_now_calls_do_not_corrupt() {
    let dir = std::env::temp_dir().join(format!("animus-double-flush-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = "127.0.0.1:0".parse().unwrap();
    let (env, _bound) = ProdEnv::bind(0, addr, &dir).await.expect("bind ProdEnv");
    // Large threshold: only the forced flushes flush, so they are the ones racing.
    let opts = LsmOptions {
        flush_threshold_bytes: 10 * 1024 * 1024,
        ..LsmOptions::default()
    };
    let lsm = LsmEngine::open_with(env, "db-", opts)
        .await
        .expect("open lsm");

    const ROUNDS: u64 = 20;
    const PER_ROUND: u64 = 25;
    let work = async {
        let mut written = 0u64;
        for round in 0..ROUNDS {
            // Populate the memtable...
            for i in 0..PER_ROUND {
                let n = round * PER_ROUND + i;
                let key = format!("k{n:06}");
                let value = format!("val-{key}");
                lsm.merge(key.as_bytes(), value.as_bytes(), n + 1)
                    .await
                    .expect("merge acked");
                written += 1;
            }
            // ...then race a burst of concurrent forced flushes against a
            // concurrent writer (so applies land mid-flush too).
            let mut handles = Vec::new();
            for _ in 0..8 {
                let lsm = lsm.clone();
                handles.push(tokio::spawn(async move {
                    lsm.flush_now().await.expect("flush_now failed");
                }));
            }
            {
                let lsm = lsm.clone();
                let base = 1_000_000 + round * PER_ROUND;
                handles.push(tokio::spawn(async move {
                    for i in 0..PER_ROUND {
                        let n = base + i;
                        let key = format!("k{n:06}");
                        let value = format!("val-{key}");
                        lsm.merge(key.as_bytes(), value.as_bytes(), n + 1)
                            .await
                            .expect("merge acked");
                    }
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
            written += PER_ROUND;
        }
        written
    };
    let written = tokio::time::timeout(Duration::from_secs(120), work)
        .await
        .expect("overlapping flush_now burst did not finish (deadlock?)");

    assert!(
        lsm.flush_count() >= 1,
        "test invalid: no forced flush actually flushed"
    );

    // No duplicate SSTable seq (manifest corruption from a double flush).
    let views = lsm.sstable_views();
    let seqs: Vec<u64> = views.iter().map(|v| v.seq).collect();
    let mut deduped = seqs.clone();
    deduped.dedup();
    assert_eq!(seqs, deduped, "duplicate SSTable seq in manifest: {seqs:?}");

    // Every acked write reads back, live and after a restart.
    let expected: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = (0..ROUNDS)
        .flat_map(|round| {
            (0..PER_ROUND).flat_map(move |i| {
                let a = round * PER_ROUND + i;
                let b = 1_000_000 + round * PER_ROUND + i;
                [a, b].map(|n| {
                    let key = format!("k{n:06}");
                    (key.clone().into_bytes(), format!("val-{key}").into_bytes())
                })
            })
        })
        .collect();
    assert_eq!(expected.len() as u64, written, "test bookkeeping");
    assert_all_present(&lsm, &expected, "live").await;

    drop(lsm);
    let (env2, _bound2) = ProdEnv::bind(0, addr, &dir).await.expect("rebind ProdEnv");
    let reopened = LsmEngine::open_with(env2, "db-", opts)
        .await
        .expect("reopen lsm after overlapping flushes");
    assert_all_present(&reopened, &expected, "after restart").await;
    let _ = std::fs::remove_dir_all(&dir);
}
