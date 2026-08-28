//! Real-multithreading regression for `LsmEngine::clone_to` racing a
//! concurrent writer (issue #298).
//!
//! `clone_to` used to call `flush()` exactly once and trust its `Ok(())`
//! return — but `flush()` silently no-ops (flushes nothing, still returns
//! `Ok(())`) whenever `applies_in_flight > 0`: a concurrent writer is
//! between its own WAL fsync and memtable apply. A row that lands in the
//! memtable and is never independently flushed to its own SSTable before
//! `clone_to` happens to race that exact window is then permanently absent
//! from the clone — it was never linked in (the clone is SSTables-only,
//! `wal_segments: Vec::new()`) and the memtable it lived in is left behind.
//!
//! This is the real in-place-split materialization shape: a tablet-host
//! reconciler task calls `clone_to` on a *different* task from the one
//! applying the tablet's own committed writes, and (per this crate's own
//! `carries_user_data` rule) a frozen split parent still accepts
//! consumer-bookkeeping writes from the GSI drain/backfill seeder right up
//! to cutover — so the two are never structurally serialized against each
//! other. The deterministic single-threaded `SimEnv` cannot reproduce the
//! race (disk ops resolve without yielding, so a `flush()` call always
//! lands strictly before or after a writer's own `log_and_apply`, never
//! *during* it) — this needs a real multi-thread `ProdEnv`, mirroring
//! `lsm_concurrent.rs`'s own flush-vs-apply regression for the identical
//! reason.

// ADR 0003 / ADR 0061 Decision 4 (rung B5): a real-thread ProdEnv liveness
// regression (see the module doc above) — SimEnv structurally cannot exercise
// this race, so real tokio::spawn/sleep/timeout are the point, not a hole.
#![allow(
    clippy::disallowed_methods,
    reason = "real-thread ProdEnv liveness test (SimEnv cannot reproduce this race, see module doc); ADR 0061 Decision 4"
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use animus_env::{ProdEnv, nid};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clone_to_under_live_load_never_drops_an_acked_write() {
    let dir = std::env::temp_dir().join(format!("animus-clone-concurrent-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = "127.0.0.1:0".parse().unwrap();
    let (env, _bound) = ProdEnv::bind(nid(0), addr, &dir)
        .await
        .expect("bind ProdEnv");
    // A high flush threshold so most acked writes stay resident in the
    // memtable for a while — widening the window `clone_to` must race
    // rather than relying on the writer's own threshold-triggered flushes
    // to do `clone_to`'s job for it.
    let opts = LsmOptions {
        flush_threshold_bytes: 1 << 20,
        compaction_trigger: 4,
        target_table_bytes: 1 << 20,
        wal_segment_bytes: 1 << 16,
        ..LsmOptions::default()
    };
    let lsm = LsmEngine::open_with(env, "db-", opts)
        .await
        .expect("open lsm");

    const N: u64 = 2000;
    // The highest index the writer has confirmed acked so far (updated
    // *after* each merge returns `Ok`, so a value observed here was
    // genuinely durable+applied at that moment — never a write still
    // in flight).
    let acked = Arc::new(AtomicU64::new(0));

    let writer = {
        let lsm = lsm.clone();
        let acked = acked.clone();
        tokio::spawn(async move {
            for i in 0..N {
                let key = format!("k{i:08}");
                let value = format!("val-{key}");
                lsm.merge(key.as_bytes(), value.as_bytes(), i + 1)
                    .await
                    .expect("merge acked");
                acked.store(i + 1, Ordering::SeqCst);
            }
        })
    };

    // Clone racer: repeatedly clone the live engine to a fresh throwaway
    // prefix while the writer streams, checking every acked-before-this-
    // clone key actually landed. Captures `acked` *before* `clone_to`
    // starts — anything acked *during* the clone is fine to miss (that's
    // a legitimate "clone ran before that write" ordering, not a bug); the
    // bug this guards is a row acked-and-confirmed-before landing nowhere
    // in the result.
    let racer = {
        let lsm = lsm.clone();
        let acked = acked.clone();
        tokio::spawn(async move {
            let mut round = 0u32;
            loop {
                let confirmed = acked.load(Ordering::SeqCst);
                let target = format!("db-clone-{round}-");
                let clone = lsm.clone_to(target.clone()).await.expect("clone_to failed");
                for i in 0..confirmed {
                    let key = format!("k{i:08}");
                    let expected = format!("val-{key}");
                    let got = clone
                        .get(key.as_bytes())
                        .await
                        .expect("clone read")
                        .map(|vv| vv.value);
                    assert_eq!(
                        got.as_deref(),
                        Some(expected.as_bytes()),
                        "clone_to round {round} (confirmed={confirmed}) is missing \
                         acked-before-clone key {key} — clone_to raced flush's own \
                         applies_in_flight no-op and lost it"
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
        .expect("writer + clone racer did not finish (deadlock?)");
    assert!(rounds >= 1, "test invalid: clone_to never ran");

    let _ = std::fs::remove_dir_all(&dir);
}

/// **Liveness regression (2026-08-26 flush-retry-starvation fix).** Pins the
/// property the no-lost-write test above does not: `clone_to` must
/// **complete** — promptly, without erroring out — even against a writer
/// that never pauses for the whole duration of the call.
///
/// `clone_to` used to retry `flush()` in a bounded loop until the memtable
/// read empty, sleeping between attempts. That loop has no liveness
/// guarantee against a *persistent* concurrent writer: the writer can refill
/// the memtable faster than any bounded number of flushes drains it, so
/// after exhausting its retry budget the call returned a loud
/// `StorageError::Backend` instead of a clone — reported from CI as
/// `clone_to_under_live_load_never_drops_an_acked_write` failing with
/// "memtable still non-empty after 1000 flush retries" on a busier runner,
/// even though the identical code passed on the same seed locally (a
/// starvation flake by construction, not an infra fluke — see
/// `docs/engineering-lessons.md`). The fix replaced "retry until empty"
/// with a single point-in-time snapshot of whatever the memtable holds,
/// written into the clone's own namespace — bounded, one-shot work
/// independent of how long or how fast a writer keeps writing.
///
/// This test keeps a writer running in as tight a loop as the runtime
/// allows for the ENTIRE test (never idling, never pausing — no `sleep`,
/// no bounded op count that could race the writer finishing before
/// `clone_to` is even called) and asserts `clone_to` still returns `Ok`
/// well inside a generous timeout. Against the old bounded-retry
/// implementation this reproduces the CI failure directly under a real
/// multi-thread `ProdEnv` (the deterministic `SimEnv` cannot: see the
/// module doc above for why).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clone_to_completes_under_a_writer_that_never_pauses() {
    let dir = std::env::temp_dir().join(format!("animus-clone-liveness-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = "127.0.0.1:0".parse().unwrap();
    let (env, _bound) = ProdEnv::bind(nid(0), addr, &dir)
        .await
        .expect("bind ProdEnv");
    // A high flush threshold so the writer's own threshold-triggered flushes
    // don't incidentally do `clone_to`'s draining job for it — the memtable
    // stays genuinely, persistently non-empty across the whole test.
    let opts = LsmOptions {
        flush_threshold_bytes: 1 << 20,
        compaction_trigger: 4,
        target_table_bytes: 1 << 20,
        wal_segment_bytes: 1 << 16,
        ..LsmOptions::default()
    };
    let lsm = LsmEngine::open_with(env, "db-", opts)
        .await
        .expect("open lsm");

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let lsm = lsm.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut i: u64 = 0;
            // No sleeps, no yields deliberately awaited — as fast and as
            // persistent a writer as this thread can drive, for as long as
            // the test lets it run.
            while !stop.load(Ordering::Relaxed) {
                let key = format!("k{i:08}");
                let value = format!("val-{key}");
                lsm.merge(key.as_bytes(), value.as_bytes(), i + 1)
                    .await
                    .expect("merge acked");
                i += 1;
            }
            i
        })
    };

    // Let the writer get genuinely underway (memtable non-empty, mid-stream)
    // before racing `clone_to` against it.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The property under test: this returns `Ok` promptly — bounded by one
    // flush attempt plus writing out a single memtable snapshot, never by
    // how long the writer keeps writing. The old implementation could spend
    // its whole retry budget (~1000 attempts) racing the writer here and
    // then return `Err`; the generous timeout only guards against a genuine
    // hang, not against the old code's slow-then-error failure mode, which
    // the inner `expect` below catches directly.
    let clone_result =
        tokio::time::timeout(Duration::from_secs(20), lsm.clone_to("db-liveness-clone-"))
            .await
            .expect("clone_to did not return within 20s against a persistent writer (hang)")
            .expect(
                "clone_to must succeed under a writer that never pauses, not error out \
         after exhausting a bounded retry budget (the flush-retry starvation bug)",
            );

    stop.store(true, Ordering::Relaxed);
    let writes = writer.await.unwrap();
    assert!(
        writes > 0,
        "test invalid: the writer never got to run concurrently with clone_to"
    );

    // Sanity: the returned handle is a real, independently usable engine.
    clone_result
        .put(b"post-clone-marker", b"1", u64::MAX - 1)
        .await
        .expect("clone is independently writable");

    let _ = std::fs::remove_dir_all(&dir);
}
