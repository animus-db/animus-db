# A real-subprocess test must poll `try_wait` to a bounded deadline before its own `Drop` re-kills the child (2026-09-13, animusd `--ephemeral` cleanup follow-up)

Writing `cluster_ephemeral_default_dir.rs`'s SIGTERM-driven shutdown tests
(proving an `--ephemeral` in-process cluster removes its own auto-generated
data directory on clean shutdown) surfaced a real hazard in the file's
pre-existing `Run` fixture, worth naming generally: `Run`'s `Drop` impl
calls `self.child.kill()` then `self.child.wait()` unconditionally, as a
best-effort teardown for every test in the file. A test that itself signals
the child (SIGTERM here, to exercise `wait_for_ctrl_c`'s graceful path) and
then only checks the exit status via a single `try_wait()` call — or worse,
assumes the process is gone the instant the signal is sent — leaves a
window where `Drop`'s own `kill()` targets a pid the OS may have already
reused for an unrelated process, since a signal is asynchronous and the
process is not actually reaped until something calls `wait`/`try_wait` and
observes an exit status.

The fix is to poll `try_wait()` in a bounded loop (never a fixed sleep)
until it returns `Ok(Some(status))`, and only then proceed with
assertions/teardown. This isn't just cleaner control flow: `std::process::
Child` caches the observed exit status internally the first time `wait`/
`try_wait` sees it, and both `kill()` and a later `wait()` become no-ops
against a cached status rather than re-signaling/re-reaping a live OS pid —
so polling to observe the exit status first is what makes a subsequent
`Drop`-driven `kill()`+`wait()` provably safe, not merely usually fine.
Any real-subprocess test in this crate that signals a child itself (rather
than only killing it in `Drop`) should poll to the same discipline before
relying on the fixture's own teardown.

A related, smaller design point from the same task: prefer an explicit
call at the natural end of a function over a `Drop` guard for "run this
exactly on a *clean* exit" semantics — a `Drop` impl also fires on a
panic-unwind path, which is the wrong trigger when the intent is
specifically "only after everything shut down without incident" (here:
removing an `--ephemeral` cluster's own auto-generated data directory only
on a clean `Ctrl-C`/SIGTERM shutdown, leaving it in place for a post-mortem
on anything else, exactly like a durable run's directory always is).
