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
//! error either.
//!
//! **A second fix (issue #555) closed that gap with an inode NUMBER
//! identity check, and that fix was itself unsound — corrected by issue
//! #1003.** The idea was: capture `wal`'s inode number right before the
//! panic, and treat a post-drop `stat` returning a *different* number (or
//! no file at all) as proof the original was torn out. That reasoning
//! silently assumes an inode number is a stable identity across an
//! unlink — true on tmpfs, which never recycles a freed number, false on
//! ext4, which recycles one eagerly (a freed inode number becomes the
//! lowest free bit in its flex group's own bitmap, and the very next
//! `O_CREAT` in that group claims it). When the writer wins the
//! unlink-vs-rmdir recreate race, the recreated `wal` can land on the
//! SAME inode number the original had — measured directly on this repo's
//! own ext4 sandbox root under CPU load (~1/6000 iterations of a
//! standalone mimic of this test's own loop); a loop-mounted ext4 filled
//! to near-ENOSPC hit it more often (~3/3000). Near-full disk is not the
//! cause — `unlink`/`rmdir`/`O_CREAT` all still succeed at 100% full on
//! ext4, measured — it only shifts the race's own scheduling odds. On
//! ext4, the inode-number check's "different inode" disjunct can read
//! `false` on a run where the directory persists, the writer won the
//! name, and no error was ever observed: the fourth outcome the old doc
//! claimed didn't exist.
//!
//! **The fix that actually holds regardless of filesystem: pin the
//! original `wal`'s identity with an open file handle held across the
//! panicking drop, and read that handle's own link count afterward.**
//! Holding an open `File` on the original `wal` does two things a bare
//! inode-number snapshot cannot: it makes it impossible for the
//! filesystem to recycle that inode's number while the test still holds
//! it open (the kernel will not reuse an inode with a nonzero reference
//! count), and it gives a direct, `fstat`-visible observable of the
//! unlink itself — the handle's own `nlink`, which `unlink`/`unlinkat`
//! decrements the instant the last directory entry naming that inode is
//! removed, independent of whether anything later reuses the freed
//! number for a *different* file. `remove_dir_all`'s directory listing is
//! taken strictly after the writer has already reported itself live, so
//! it is guaranteed to call `unlinkat` on exactly the entry the writer
//! was using at that point — the pinned handle's `nlink` reaching 0 is
//! therefore proof this specific file was unlinked, decided the instant
//! the panicking drop returns, with no polling and no filesystem-specific
//! assumption. (The one documented exception: NFS's silly-rename can
//! leave an open-but-unlinked file's directory entry renamed rather than
//! removed outright — not a concern for this repo's CI or sandbox
//! targets, all local filesystems.) The red test's pass/fail verdict is
//! that link count alone now; the old three-way inode/error
//! classification survives purely as enrichment for the panic message
//! when the assertion fails. The **green** test has a much smaller timing
//! dependency of its own (it waits briefly to observe continued
//! successful progress), but nothing in it races a directory removal,
//! since `PanicSafeTempDir` never calls one on a panicking drop.

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
    ///
    /// Bounds what used to be an unconditional spin: if the writer's very
    /// first write fails (e.g. the filesystem is at true ENOSPC) before
    /// any success is ever recorded, `successes` stays 0 forever and a
    /// naive spin here would hang the test indefinitely. This is not a
    /// timeout — it is a correctness check that stops spinning the
    /// instant the writer thread itself has already given up, surfacing
    /// its `first_error` in the panic message instead of hanging.
    fn wait_until_live(&self) {
        loop {
            if self.successes.load(Ordering::SeqCst) > 0 {
                return;
            }
            if let Some(err) = self.first_error.lock().unwrap().clone() {
                panic!(
                    "background writer's very first write failed before it ever reported \
                     itself live (e.g. the filesystem is at true ENOSPC), so this test can \
                     never proceed: {err}"
                );
            }
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
/// describes) gets torn out from under it.
///
/// The verdict is decided by the pinned original `wal` handle's own link
/// count, not by an inode NUMBER comparison (issue #555's own fix) and not
/// by which side of the removal-vs-recreate race happens to win. An inode
/// number is not a stable identity across an unlink on every filesystem —
/// ext4 reuses a freed one eagerly, which defeated #555's fix outright (see
/// the module doc for the full account, and issue #1003 for the corrected
/// fix below).
#[test]
fn bare_tempdir_removes_its_directory_out_from_under_a_live_background_writer_on_panic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    let wal_path = path.join("wal");
    let writer = BackgroundWriter::start(path.clone());
    writer.wait_until_live();

    // Pin the original `wal` inode by holding it open across the panicking
    // drop below. This is the fix issue #1003 corrects #555's own with:
    // (a) an open handle makes it impossible for the filesystem to recycle
    // this inode's number while the test still holds it — so whatever
    // `remove_dir_all` does can never be confused, after the fact, with a
    // *different* file that happened to reuse the same number (ext4's own
    // eager reuse is exactly what broke the inode-number check); and (b) it
    // gives a direct, `fstat`-visible observable of the unlink itself: this
    // handle's own `nlink`, which the kernel decrements the instant the
    // last directory entry naming this inode is removed, regardless of
    // whether anything later reuses the freed number.
    let pinned = std::fs::File::open(&wal_path)
        .expect("wal file must exist once the writer has reported itself live");
    // Kept only for the diagnostic message below, never for the verdict —
    // an inode NUMBER is not what this test now trusts (see above).
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
    // completion. `remove_dir_all`'s directory listing is taken strictly
    // after `wait_until_live` returned above, so it is guaranteed to have
    // called `unlinkat` on exactly the entry the writer was using at that
    // point — the pinned handle's `nlink` dropping to 0 is race-free and
    // filesystem-agnostic proof of that unlink (any Linux fs; NFS
    // silly-rename is the one documented exception, and not a CI target
    // here — see the module doc).
    let pinned_nlink = pinned
        .metadata()
        .expect("fstat the pinned original wal handle")
        .nlink();

    // Enrichment only, never the verdict: the old three-way inode/error
    // classification, kept purely to make a failure message legible.
    let current_ino = std::fs::metadata(&wal_path).ok().map(|m| m.ino());
    let torn_out_by_inode = !path.exists() || current_ino != Some(original_ino);
    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    let mut observed_error = writer.first_error.lock().unwrap().clone();
    while observed_error.is_none() && std::time::Instant::now() < deadline {
        observed_error = writer.first_error.lock().unwrap().clone();
        thread::yield_now();
    }
    let (_successes, first_error_at_join) = writer.stop_and_join();
    let observed_error = observed_error.or(first_error_at_join);
    if let Some(observed_error) = &observed_error {
        assert!(
            observed_error.contains("o such file") || observed_error.contains("No such file"),
            "expected a not-found-shaped I/O error, got: {observed_error}"
        );
    }

    if pinned_nlink != 0 {
        // A filesystem that genuinely refuses to unlink a directory entry
        // out from under a live background writer is a different failure
        // than the one this test isolates — surface it, don't guess at it.
        // Do NOT make the test pass on this path: attempting our own
        // cleanup here is purely to enrich the panic message with whatever
        // `remove_dir_all` itself reports, since a swallowed error is
        // exactly the class of thing #1003's own hypothesis (a genuinely
        // refused removal) would look like if it were ever real.
        let cleanup_result = std::fs::remove_dir_all(&path);
        panic!(
            "the pinned original `wal` handle's link count never reached 0 after the \
             panicking drop — expected the panicking `TempDir::drop`'s `remove_dir_all` to \
             have unlinked it, but `nlink` is still {pinned_nlink} at {wal_path:?} \
             (original inode {original_ino}, torn_out_by_inode={torn_out_by_inode}, \
             observed_error={observed_error:?}); a manual `remove_dir_all(&path)` attempted \
             here as a diagnostic returned: {cleanup_result:?}"
        );
    }

    drop(pinned);

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
