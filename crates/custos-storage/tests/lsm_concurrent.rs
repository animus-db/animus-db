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

use std::time::Duration;

use custos_env::ProdEnv;
use custos_storage::{LsmEngine, StorageEngine};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_do_not_deadlock() {
    let dir = std::env::temp_dir().join(format!("custos-gc-deadlock-{}", std::process::id()));
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
