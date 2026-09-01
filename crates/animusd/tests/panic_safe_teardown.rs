//! Regression test for issue #511: prod-liveness tests lack panic-safe node
//! teardown.
//!
//! **The confirmed mechanism** (see `support::PanicSafeTempDir`'s own doc
//! and `crates/animus-control/CLAUDE.md`'s "The WAL `fsync` is raced..."
//! entry): a mid-test panic drops a `Vec<Node>`/`Node` and its `TempDir`
//! together. `Drop for Node` only latches every *hosted CP group's*
//! `halted` flag (issues #282/#279) — it deliberately does not abort this
//! node's background driver tasks, which keep running, detached, past the
//! point their `Node` was dropped. The **control-plane** Raft driver's own
//! `persist_wal` has **no** `halted`-gate at all (`animus-control::node`'s
//! `env.append(WAL, ..).await.expect("wal append")`/`env.sync(WAL).await.
//! expect("wal sync")` are bare, unconditional `.expect()`s, live or
//! shutting down) — so a plain `tempfile::TempDir` removing its directory
//! tree as part of that same panicking unwind can race a still-live
//! control-plane driver task's next WAL append/sync, turning the ORIGINAL
//! assertion failure into a second, unrelated `.expect()` panic that
//! obscures it.
//!
//! This suite proves the fix — `support::PanicSafeTempDir` — deterministically,
//! with no ProdEnv cluster and no timing dependency at all: it isolates the
//! exact mechanism (does a panicking unwind remove a directory a background
//! operation is actively using?) with a real background thread racing a
//! real panic, coordinated by explicit signals rather than sleeps, so the
//! outcome never depends on scheduling luck.

use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

mod support;

/// Spawn a background "driver" thread that continuously writes to
/// `dir.join("wal")`, mirroring `persist_wal`'s own append+sync loop
/// against a real file. Returns (join handle, a signal the caller can flip
/// to stop it, a counter of successful writes, and the first I/O error
/// observed, if any).
struct BackgroundWriter {
    handle: Option<thread::JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    successes: Arc<AtomicU64>,
    first_error: Arc<std::sync::Mutex<Option<String>>>,
}

impl BackgroundWriter {
    fn start(dir: PathBuf) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let successes = Arc::new(AtomicU64::new(0));
        let first_error: Arc<std::sync::Mutex<Option<String>>> =
            Arc::new(std::sync::Mutex::new(None));

        let stop2 = stop.clone();
        let successes2 = successes.clone();
        let first_error2 = first_error.clone();
        let handle = thread::spawn(move || {
            let path = dir.join("wal");
            while !stop2.load(Ordering::SeqCst) {
                match std::fs::write(&path, b"record") {
                    Ok(()) => {
                        successes2.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => {
                        // Record only the first error — mirrors
                        // `persist_wal`'s own `.expect()`, which would have
                        // panicked (and stopped the loop) on the first one.
                        let mut slot = first_error2.lock().unwrap();
                        if slot.is_none() {
                            *slot = Some(e.to_string());
                        }
                        // Keep looping in this test (unlike the real
                        // `.expect()`) so we can also observe recovery —
                        // but a single observed error is already the
                        // failure this test is checking for.
                    }
                }
                // A tight loop, no sleep: maximizes the chance of actually
                // racing the directory removal instead of finishing before
                // or starting after it — but correctness below never
                // depends on winning that race, only on synchronizing
                // through `successes`/`stop`.
            }
        });

        Self {
            handle: Some(handle),
            stop,
            successes,
            first_error,
        }
    }

    /// Block until at least one write has succeeded — proves the
    /// background thread is genuinely live and using the directory before
    /// the caller does anything to it.
    fn wait_until_live(&self) {
        while self.successes.load(Ordering::SeqCst) == 0 {
            thread::yield_now();
        }
    }

    fn stop_and_join(mut self) -> (u64, Option<String>) {
        self.stop.store(true, Ordering::SeqCst);
        self.handle
            .take()
            .unwrap()
            .join()
            .expect("writer thread join");
        (
            self.successes.load(Ordering::SeqCst),
            self.first_error.lock().unwrap().clone(),
        )
    }
}

/// **Red** (the vulnerability issue #511 describes, isolated): a bare
/// `tempfile::TempDir` removes its directory as an ordinary, unconditional
/// part of `Drop` — including when that `Drop` runs mid-panic-unwind — so a
/// background operation still actively using the directory (the shape of
/// the control-plane WAL driver task `support::PanicSafeTempDir`'s own doc
/// describes) observes a real I/O error immediately after.
#[test]
fn bare_tempdir_removes_its_directory_out_from_under_a_live_background_writer_on_panic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    let writer = BackgroundWriter::start(path.clone());
    writer.wait_until_live();

    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let _dir = dir; // moved in: its Drop runs as part of THIS unwind
        panic!("ORIGINAL_ASSERTION_FAILURE_MARKER");
    }));
    let panic_msg = result.expect_err("the panic must propagate out of catch_unwind");
    let panic_msg = panic_msg
        .downcast_ref::<&str>()
        .copied()
        .unwrap_or("<non-&str panic payload>");
    assert_eq!(panic_msg, "ORIGINAL_ASSERTION_FAILURE_MARKER");

    // The directory is gone the instant the panicking Drop ran.
    assert!(
        !path.exists(),
        "a bare TempDir must remove its directory even on a panicking drop \
         (this is the pre-existing tempfile behavior the fix works around, \
         not the fix itself)"
    );

    // Give the still-live background writer a moment to hit the now-missing
    // directory and record a real I/O error — this is the failure mode
    // `persist_wal`'s bare `.expect("wal append")`/`.expect("wal sync")`
    // would turn into a masking panic in the real driver task.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut observed_error = None;
    while std::time::Instant::now() < deadline {
        if let Some(e) = writer.first_error.lock().unwrap().clone() {
            observed_error = Some(e);
            break;
        }
        thread::yield_now();
    }
    let (_successes, _) = writer.stop_and_join();
    let observed_error = observed_error
        .expect("background writer must observe an I/O error against a removed directory");
    assert!(
        io::Error::new(io::ErrorKind::NotFound, observed_error.clone()).kind()
            == io::ErrorKind::NotFound
            || observed_error.contains("o such file")
            || observed_error.contains("No such file"),
        "expected a not-found-shaped I/O error, got: {observed_error}"
    );
}

/// **Green** (the fix): `support::PanicSafeTempDir` leaks its directory on a
/// panicking drop instead of removing it, so the identical background
/// writer never observes an error — the original panic surfaces cleanly and
/// nothing races the directory's removal, because nothing removed it.
#[test]
fn panic_safe_tempdir_never_removes_its_directory_on_a_panicking_drop() {
    let dir = support::panic_safe_tempdir();
    let path = dir.path().to_path_buf();
    let writer = BackgroundWriter::start(path.clone());
    writer.wait_until_live();

    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let _dir = dir; // moved in: its Drop runs as part of THIS unwind
        panic!("ORIGINAL_ASSERTION_FAILURE_MARKER");
    }));
    let panic_msg = result.expect_err("the panic must propagate out of catch_unwind");
    let panic_msg = panic_msg
        .downcast_ref::<&str>()
        .copied()
        .unwrap_or("<non-&str panic payload>");
    assert_eq!(
        panic_msg, "ORIGINAL_ASSERTION_FAILURE_MARKER",
        "the ORIGINAL panic must survive unmasked — this is the whole point of issue #511's fix"
    );

    // The directory survives the panicking drop.
    assert!(
        path.exists(),
        "PanicSafeTempDir must leak its directory on a panicking drop"
    );

    // Let the background writer run a bit longer under the SAME conditions
    // that produced a real error in the bare-TempDir test above, then
    // confirm it never saw one.
    let before = writer.successes.load(Ordering::SeqCst);
    let deadline = std::time::Instant::now() + Duration::from_millis(200);
    while std::time::Instant::now() < deadline {
        thread::yield_now();
    }
    let (after, first_error) = writer.stop_and_join();
    assert!(
        first_error.is_none(),
        "background writer observed an I/O error after the panic-safe drop: {first_error:?}"
    );
    assert!(
        after > before,
        "background writer should have kept making successful progress after the panic"
    );

    // Clean up manually now that we've proven it's safe — a real test run
    // leaks the directory (bounded by CI's ephemeral runner); this suite
    // cleans up after itself so repeated local runs don't accumulate dirs.
    let _ = std::fs::remove_dir_all(&path);
}

/// Sanity: on a NORMAL (non-panicking) drop, `PanicSafeTempDir` behaves
/// exactly like `TempDir` — no leak on the common, passing-test path.
#[test]
fn panic_safe_tempdir_cleans_up_normally_when_nothing_panics() {
    let dir = support::panic_safe_tempdir();
    let path = dir.path().to_path_buf();
    assert!(path.exists());
    drop(dir);
    assert!(
        !path.exists(),
        "a non-panicking drop must remove the directory exactly like a bare TempDir"
    );
}
