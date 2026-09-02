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
//! This suite proves the fix — `support::PanicSafeTempDir` — with no ProdEnv
//! cluster: it isolates the exact mechanism (does a panicking unwind remove
//! a directory a background operation is actively using?) with a real
//! background thread racing a real panic, coordinated by explicit signals
//! (wait for the writer's first successful write) rather than sleeps
//! wherever the property allows it.
//!
//! **A caveat on "no timing dependency": the `bare_tempdir_*` (red) test
//! deliberately races a live writer thread against `std::fs::
//! remove_dir_all` itself — that race IS the vulnerability being
//! isolated — so which side wins is genuine, unpredictable OS scheduling.**
//! `remove_dir_all` can finish cleanly (directory gone), or its final
//! `rmdir` can lose to the writer recreating `wal` in the unlink-vs-rmdir
//! window and fail "directory not empty" — an error `TempDir::drop`
//! silently swallows, leaving the directory behind. An earlier version of
//! this test asserted `!path.exists()` alone, i.e. only the first outcome;
//! under load the second is real and flaked CI (issue #555), and a naive
//! "or the writer observed an I/O error" fix still isn't enough — the
//! writer can win that specific recreate race *without* ever seeing an
//! error either. What actually IS deterministic, independent of which way
//! that race lands: `remove_dir_all`'s directory listing is taken strictly
//! after the writer has already reported itself live, so it is guaranteed
//! to unlink the exact file (inode) the writer was using at that point —
//! whatever exists at that path afterwards, if anything, can only be a
//! *different* inode. The red test asserts on that inode identity instead
//! of on timing, so its verdict is decided the instant the panicking drop
//! returns, with no polling required; it separately gives the writer a
//! brief, bounded window purely to enrich the proof with an observed
//! not-found error when the race happens to land that way, but the
//! pass/fail verdict never depends on that window. The **green** test has
//! a much smaller timing dependency of its own (it waits briefly to
//! observe continued successful progress), but nothing in it races a
//! directory removal, since `PanicSafeTempDir` never calls one on a
//! panicking drop.

use std::os::unix::fs::MetadataExt;
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
                        // Record the first error and stop — mirrors
                        // `persist_wal`'s own `.expect()`, which would have
                        // panicked (and so also stopped) on this first one.
                        // Stopping here, rather than looping to retry, keeps
                        // `first_error` a stable, final signal once set and
                        // keeps this thread from re-creating `wal` forever
                        // against a directory a panicking `TempDir::drop`
                        // may still be racing to remove.
                        let mut slot = first_error2.lock().unwrap();
                        if slot.is_none() {
                            *slot = Some(e.to_string());
                        }
                        break;
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
/// describes) gets torn out from under it. See the module doc for why the
/// assertion below is decided by the file's identity (its inode), not by
/// which side of the removal-vs-recreate race happens to win (issue #555).
#[test]
fn bare_tempdir_removes_its_directory_out_from_under_a_live_background_writer_on_panic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    let wal_path = path.join("wal");
    let writer = BackgroundWriter::start(path.clone());
    writer.wait_until_live();

    // The inode the writer is actively using right before the drop. This
    // is what makes the assertion below deterministic: `remove_dir_all`'s
    // directory listing is taken strictly after `wait_until_live` returned
    // (the panic, and so the drop, only happens below), so it is guaranteed
    // to see — and unlink — precisely this inode. Whatever exists at
    // `wal_path` afterwards can only be this same inode if `remove_dir_all`
    // never got around to unlinking it at all, which cannot happen.
    let original_ino = std::fs::metadata(&wal_path)
        .expect("wal file must exist once the writer has reported itself live")
        .ino();

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

    // `catch_unwind` does not return until the unwind — and so the
    // panicking `Drop for TempDir`, and its `remove_dir_all` — has run to
    // completion, so there is nothing left to wait for on that front: the
    // outcome is already decided. Read it off the filesystem now:
    //  - the directory may be gone entirely (`!path.exists()`) — removal
    //    won outright;
    //  - or it may persist with a `wal` entry whose inode differs from
    //    `original_ino` — the writer won the *name*, by recreating `wal`
    //    in the window between `remove_dir_all` unlinking it and the final
    //    `rmdir` (which then fails "directory not empty" and is swallowed
    //    by `TempDir::drop`), but the *original* file is still gone;
    //  - `stat`ing `wal_path` may itself fail (`NotFound`) if it is
    //    observed in the instant between that unlink and the writer's next
    //    recreate — also proof the original is gone.
    // Any of the three is proof this specific file was torn out from under
    // the writer; there is no fourth outcome, so nothing here is racy.
    let current_ino = std::fs::metadata(&wal_path).ok().map(|m| m.ino());
    let torn_out_by_inode = !path.exists() || current_ino != Some(original_ino);

    // Also give the writer a brief, bounded window to report a real I/O
    // error against the now-missing directory — the failure mode
    // `persist_wal`'s bare `.expect("wal append")`/`.expect("wal sync")`
    // would turn into a masking panic in the real driver task. This is
    // purely to enrich the proof above with the error-kind check below
    // when the race happens to land that way; the pass/fail verdict never
    // depends on whether this window catches it.
    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    let mut observed_error = writer.first_error.lock().unwrap().clone();
    while observed_error.is_none() && std::time::Instant::now() < deadline {
        observed_error = writer.first_error.lock().unwrap().clone();
        thread::yield_now();
    }
    let (_successes, first_error_at_join) = writer.stop_and_join();
    let observed_error = observed_error.or(first_error_at_join);

    assert!(
        torn_out_by_inode || observed_error.is_some(),
        "a bare TempDir's panicking drop must tear the directory out from under a live \
         writer: expected the directory gone, the `wal` file's inode to have changed, or \
         the writer to have observed an I/O error, but `wal_path` still resolves to its \
         original inode ({original_ino}) at {path:?} and the writer never errored"
    );
    if let Some(observed_error) = observed_error {
        assert!(
            observed_error.contains("o such file") || observed_error.contains("No such file"),
            "expected a not-found-shaped I/O error, got: {observed_error}"
        );
    }

    // Clean up manually: the inode-mismatch outcome above means the
    // directory can persist (with a writer-recreated `wal` inside) even
    // though the ORIGINAL file was torn out from under it — don't leak
    // that leftover across repeated local runs.
    let _ = std::fs::remove_dir_all(&path);
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
