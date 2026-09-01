//! Real-multithreading regression for `LsmEngine::clone_to_filtered` racing a
//! concurrent writer (mirrors `lsm_clone_concurrent.rs`'s own `clone_to`
//! regression for the identical reason — see that file's module doc).
//!
//! `clone_to_filtered` calls the same internal, unconditionally-draining
//! `flush()` `clone_to` does before taking its point-in-time snapshot, so a
//! genuinely leftover (still-memtable-resident) row at snapshot time can only
//! arise from a *concurrent* writer landing between that flush and the
//! snapshot lock acquisition — a window the deterministic, single-threaded
//! `SimEnv` cannot reproduce (its disk ops resolve without yielding, so a
//! `flush()` call always lands strictly before or after a writer's own
//! `log_and_apply`, never *during* it). `lsm_clone_filtered.rs`'s own
//! `clone_filter_tests` module (in `src/lsm.rs`) unit-tests the extracted
//! `key_in_keep`/`table_overlaps_keep` predicates directly and exhaustively;
//! this file proves the end-to-end leftover-memtable path under the one
//! scheduling model that can actually reach it — real threads, real races.
//!
//! Two single-writer scenarios, deliberately NOT interleaving keep-range and
//! dropped-range keys within the same writer (interleaving them would make
//! nearly every flushed table a `keep`-boundary-straddling one by
//! construction — a real and correctly-handled case already covered by
//! `lsm_clone_filtered.rs`'s `a_boundary_straddling_table_is_linked_whole`,
//! but not what this file is after): one writer stays entirely inside
//! `keep`, one entirely outside it, so a clean pass/fail signal survives
//! whatever the flush timing happens to be.

#![allow(
    clippy::disallowed_methods,
    reason = "real-thread ProdEnv liveness test (SimEnv cannot reproduce this race, see module doc); ADR 0061 Decision 4"
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use animus_env::{ProdEnv, nid};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};

fn opts() -> LsmOptions {
    // A high flush threshold so most acked writes stay resident in the
    // memtable for a while, widening the window a racing clone must cross
    // rather than relying on the writer's own threshold-triggered flushes to
    // do the draining for it.
    LsmOptions {
        flush_threshold_bytes: 1 << 20,
        compaction_trigger: 4,
        target_table_bytes: 1 << 20,
        wal_segment_bytes: 1 << 16,
        ..LsmOptions::default()
    }
}

async fn open(dir: &std::path::Path) -> LsmEngine<ProdEnv> {
    let addr = "127.0.0.1:0".parse().unwrap();
    let (env, _bound) = ProdEnv::bind(nid(0), addr, dir)
        .await
        .expect("bind ProdEnv");
    LsmEngine::open_with(env, "db-", opts())
        .await
        .expect("open lsm")
}

/// A writer entirely inside `keep = [a, b)` races `clone_to_filtered`: every
/// key acked before a given round's clone call must be present in that
/// round's result, exactly like plain `clone_to`'s own equivalent
/// regression — proving the filtered entry point loses no acked in-range
/// write, whether it landed via a linked SSTable or the leftover-memtable
/// path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clone_to_filtered_under_live_load_never_drops_an_acked_in_range_write() {
    let dir = std::env::temp_dir().join(format!(
        "animus-clone-filtered-inrange-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let lsm = open(&dir).await;

    const N: u64 = 2000;
    let acked = Arc::new(AtomicU64::new(0));

    let writer = {
        let lsm = lsm.clone();
        let acked = acked.clone();
        tokio::spawn(async move {
            for i in 0..N {
                let key = format!("a{i:08}");
                lsm.merge(key.as_bytes(), format!("val-{key}").as_bytes(), i + 1)
                    .await
                    .expect("merge acked");
                acked.store(i + 1, Ordering::SeqCst);
            }
        })
    };

    let keep: [(Vec<u8>, Option<Vec<u8>>); 1] = [(b"a".to_vec(), Some(b"b".to_vec()))];
    let racer = {
        let lsm = lsm.clone();
        let acked = acked.clone();
        tokio::spawn(async move {
            let mut round = 0u32;
            loop {
                let confirmed = acked.load(Ordering::SeqCst);
                let target = format!("db-clone-inrange-{round}-");
                let clone = lsm
                    .clone_to_filtered(target.clone(), &keep)
                    .await
                    .expect("clone_to_filtered failed");
                for i in 0..confirmed {
                    let key = format!("a{i:08}");
                    let expected = format!("val-{key}");
                    let got = clone
                        .get(key.as_bytes())
                        .await
                        .expect("clone read")
                        .map(|vv| vv.value);
                    assert_eq!(
                        got.as_deref(),
                        Some(expected.as_bytes()),
                        "clone_to_filtered round {round} (confirmed={confirmed}) is \
                         missing acked-before-clone in-range key {key}"
                    );
                }
                drop(clone);
                round += 1;
                if confirmed >= N {
                    break round;
                }
                tokio::task::yield_now().await;
            }
        })
    };

    let run = async {
        writer.await.unwrap();
        racer.await.unwrap()
    };
    let rounds = tokio::time::timeout(Duration::from_secs(120), run)
        .await
        .expect("writer + filtered-clone racer did not finish (deadlock?)");
    assert!(rounds >= 1, "test invalid: clone_to_filtered never ran");

    let _ = std::fs::remove_dir_all(&dir);
}

/// The dual: a writer entirely OUTSIDE `keep = [a, b)` (a `z*` key space)
/// races `clone_to_filtered`. Every single round's clone — whatever the
/// flush timing happened to be relative to the racing writer — must come
/// back completely empty: zero SSTables (whole-file assignment excludes
/// every flushed table, since none of them can ever overlap `keep`) and zero
/// rows (the leftover-memtable snapshot, filtered by the identical
/// `key_in_keep` predicate, has nothing left to write out). This is the one
/// property that specifically proves the leftover-memtable filtering path —
/// a row landing in the memtable strictly between `clone_to_filtered`'s own
/// internal `flush()` and its snapshot lock acquisition (unreachable under
/// `SimEnv`, see the module doc) is still correctly dropped, not merely
/// "usually" dropped by whole-file assignment alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clone_to_filtered_under_live_load_never_leaks_a_dropped_range_write() {
    let dir = std::env::temp_dir().join(format!(
        "animus-clone-filtered-outrange-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let lsm = open(&dir).await;

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let lsm = lsm.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut i: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("z{i:08}");
                lsm.merge(key.as_bytes(), format!("val-{key}").as_bytes(), i + 1)
                    .await
                    .expect("merge acked");
                i += 1;
            }
            i
        })
    };

    let keep: [(Vec<u8>, Option<Vec<u8>>); 1] = [(b"a".to_vec(), Some(b"b".to_vec()))];
    let rounds_done = Arc::new(AtomicU64::new(0));
    let racer = {
        let lsm = lsm.clone();
        let rounds_done = rounds_done.clone();
        tokio::spawn(async move {
            for round in 0..200u32 {
                let target = format!("db-clone-outrange-{round}-");
                let clone = lsm
                    .clone_to_filtered(target.clone(), &keep)
                    .await
                    .expect("clone_to_filtered failed");
                assert_eq!(
                    clone.sstable_count(),
                    0,
                    "round {round}: a dropped-range-only source must never link \
                     any table into a `keep`-filtered clone"
                );
                let rows = clone.entries().await.expect("clone entries");
                assert!(
                    rows.is_empty(),
                    "round {round}: a dropped-range-only source must never leave \
                     a leftover-memtable row in a `keep`-filtered clone: {rows:?}"
                );
                drop(clone);
                rounds_done.store(u64::from(round) + 1, Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
        })
    };

    let racer_res = tokio::time::timeout(Duration::from_secs(120), racer)
        .await
        .expect("filtered-clone racer did not finish (deadlock?)");
    racer_res.unwrap();
    stop.store(true, Ordering::Relaxed);
    let writes = writer.await.unwrap();
    assert!(
        writes > 0,
        "test invalid: the writer never got to run concurrently with the racer"
    );
    assert!(
        rounds_done.load(Ordering::SeqCst) >= 1,
        "test invalid: clone_to_filtered never ran"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
