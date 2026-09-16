# A test that deliberately races a real thread against `remove_dir_all` to *demonstrate* the race must assert on the race's *consequence*, not on which side won — and CI's per-job retry is exactly the kind of thing that hides a test defect shaped like this for months (2026-09-02, issue #555).

**A test that deliberately races a real thread against `remove_dir_all`
to *demonstrate* the race must assert on the race's *consequence*, not
on which side won — and CI's per-job retry is exactly the kind of thing
that hides a test defect shaped like this for months (2026-09-02, issue
#555).** The issue #511 regression suite above
(`crates/animusd/tests/panic_safe_teardown.rs`) has a **red** test that
starts a real background thread recreating `dir/wal` in a tight loop,
then drops a bare `tempfile::TempDir` mid-panic and asserted
`!path.exists()` — "the removal won" is only one of the ways
`TempDir::drop`'s `remove_dir_all` can resolve against a live writer.
`remove_dir_all` unlinks `wal` then `rmdir`s the directory; if the
writer recreates `wal` in that window, the `rmdir` fails "directory not
empty" and `TempDir::drop` — which ignores its `Result` — swallows the
error, leaving the directory behind. The `!path.exists()` assertion then
fails on a run that still demonstrates the exact vulnerability being
tested, just via the other outcome; this was invisible in ordinary CI
because `.github/workflows/ci.yml`'s `prod-liveness` job retries the
whole `animusd` suite once, so a first-attempt failure silently reran
and passed — the run still reported green (see Session operating mode
item 4: a flake absorbed by a retry is still a flake, still a bug).
Reproduced locally by running the binary under CPU load (`yes >
/dev/null &` × 3-4 on a 4-core box, `for i in $(seq 100); do cargo test
... || break; done`): ~4/100 failures on the pre-fix assertion. **The
obvious-looking fix is not enough on its own**: asserting
`writer.first_error.is_some() || !path.exists()` — accept either
"writer saw an I/O error" or "directory gone" — is the right *shape*,
but empirically got *worse* (10/100 under the same load) because there
is a third outcome neither disjunct covers: the writer can win the
recreate race *without ever observing an error at all* (`File::create`
against the still-present parent directory just succeeds, silently,
handing back a brand-new inode). Any assertion built only from "did an
error happen" and "does the path exist" is incomplete — it needs to
enumerate every outcome the race can actually produce, and this one
can't be enumerated by polling for a *signal*, only by checking an
*identity*. **The fix that actually closed it**: capture the `wal`
file's own inode (`std::os::unix::fs::MetadataExt::ino`) right before
the panic. `remove_dir_all`'s directory listing is taken strictly after
the writer has already reported itself live, so it is guaranteed to
unlink that exact inode — whatever exists at that path afterward, if
anything, can only be a *different* inode (or nothing). That turns the
assertion into one decided the instant the panicking drop returns, with
no polling and no timing dependency at all, closing the gap the
disjunction-only fix left open (verified: 0/200 under the same load,
including a heavier 4-`yes` variant). **General lesson**: a real-thread
race test that exists to *demonstrate* a race is only as sound as its
enumeration of every outcome that race can produce; when a first fix
"looks" like it removes the ambiguity, it can just relocate the
ambiguity to a case the new assertion still doesn't cover — reproduce
under load both *before and after* the fix (not just before) to catch
that, since the fix's own soundness is exactly as empirical a claim as
the original bug's existence.
