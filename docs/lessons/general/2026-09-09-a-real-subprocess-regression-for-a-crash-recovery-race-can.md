# A real-subprocess regression for a crash-recovery race can be reproducible-by-hand yet still be the wrong thing to gate CI on — assert the deterministic root cause instead of the racy symptom (same 2026-09-09 fix)

While writing the real-binary regression test for the `--cluster N`
default-`--dir` fix above, the literal "second cluster comes up stuck and
never elects" symptom the bug report described turned out to be
**timing-sensitive**: a `--cluster 3` run followed by a reused-directory,
different-sized `--cluster 5` run left 2 of 5 nodes permanently unelected
in 3 of 4 manual trials, but occasionally (the 4th) re-converged within a
second instead — a genuine race in the crash-recovery path, not a test
bug. Per this repo's own standing rule (`CLAUDE.md`'s "Session operating
mode" item 4 — a flaky test is a bug, never something to retry or widen a
timeout around), a test that gates on that election outcome would
sometimes give a false "healthy" reading against the very code it exists
to catch. The fix: assert the fully deterministic mechanism that *causes*
the race instead of the race itself — "the second back-to-back run must
create its own brand-new directory under `$TMPDIR`, never reuse whatever
the first run already created there" (`animusd_temp_entries()`'s
before/after directory-listing diff, see `crates/animusd/tests/
cluster_ephemeral_default_dir.rs`). That assertion is both necessary and
sufficient to prevent the downstream stall and never flakes, because it
depends on nothing but which directories exist on disk. Two smaller traps
hit along the way, worth naming for the next real-subprocess test: (1) a
bare `std::process::Child` does **not** kill its process on drop (only an
explicit `Drop` impl does) — wrapping the spawned child in an RAII guard
*after* some fallible/panicking setup work leaks an orphaned real
`animusd` process on that panic path, which then silently shares/corrupts
a later run's reused directory and confounds the very investigation that
spawned it; construct the guard immediately after `spawn()`, before
anything else can panic. (2) `timeout <secs> cargo test ...` from the
shell kills the `cargo test` process on expiry but does not reach its
already-spawned grandchildren, which likewise survive as orphans holding
stale on-disk state — prefer a timeout expressed inside the test itself
(`tokio::time::timeout` wrapping a body whose `Run::drop` reaps its own
children) over an external `timeout` wrapper for any test that spawns real
subprocesses.
