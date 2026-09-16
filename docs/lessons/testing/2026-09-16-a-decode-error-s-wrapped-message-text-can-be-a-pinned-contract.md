# A decode error's wrapped message text can be a pinned contract elsewhere in the crate

When replacing one layer of a decode path's error handling (e.g. swapping
`serde_json::from_slice` for a hand-rolled binary decoder inside
`SsTableReader::open`, issue #839), it's tempting to simplify the call site by
dropping a `.map_err(|e| StorageError::Backend(format!("some prefix: {e}")))`
wrapper that looks redundant once the inner decoder already returns the same
error variant. But a **regression test elsewhere in the crate** can assert on
the literal wrapped message text via `err.to_string().contains("some
prefix")` — here, `lsm_crash.rs`'s `fsync_lie_flush_survives_as_a_clean_open_
error` pins `"corrupt sstable index"` specifically, a substring only the
now-removed wrapper produced (the inner decoder's own errors say `"truncated
sstable index"`, `"sstable index crc mismatch"`, etc., never that phrase).
Dropping the wrapper compiled fine and every test in the file being edited
still passed — the break only showed up running the *other* integration test
binary that happens to pattern-match the wrapped string.

**The generalizable check:** before changing what a decode function's error
message says (not just its variant/type), `grep -rn` the literal string
fragment across the whole crate's `tests/` — not just the module you're
editing — for `.contains(...)`/`.to_string()` assertions on it. A `cargo test
-p <crate>` run across every test binary (not just the one whose source you
touched) is the other net that catches this, which is exactly why the gates
run the whole crate, not a `--test` filter, before reporting done.
