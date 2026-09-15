# A shared bring-up helper must borrow the caller's `TempDir` (or return its guard), never own-and-drop one internally

**A shared bring-up helper must borrow the caller's `TempDir` (or return
its guard), never own-and-drop one internally** (issue #273).
`dynamo_index_scan.rs::setup()` created `tempfile::tempdir()` locally and
returned only `(nodes, addr)` — the guard dropped at the end of `setup()`,
`remove_dir_all`-ing the data tree out from under the still-running 3-node
LSM cluster the caller kept using, so a background WAL write later panicked
`"wal group-commit sync failed"`. Every other bring-up helper in the suite
already took `dir: &Path` from a caller-held guard; this was the one
offender, found by a full sweep, not a pattern to assume is safe elsewhere.
**The issue #278 halted-gated WAL tolerance does NOT retroactively make
this safe**: that tolerance only applies while a driver loop's own
`halted` latch is set (a deliberate shutdown in progress), and this
cluster is never shut down — `halted` stays false the whole time, so the
fault surfaces as the loud, unconditional durability panic, not the
tolerated teardown case. Fixed by returning `(TempDir, Vec<Node>, SocketAddr)`
and binding the guard at every call site (`let (_dir, nodes, addr) =
setup().await;`). (`animusd/tests/dynamo_index_scan.rs`.)
