# Adding a new AWS-documented request-shape limit needs a workspace-wide sweep for pre-existing test helpers that already build a request past it, not just a decode-level unit test (2026-08-23, DynamoDB batch/transaction caps).

**Adding a new AWS-documented request-shape limit needs a workspace-wide
sweep for pre-existing test helpers that already build a request past it,
not just a decode-level unit test (2026-08-23, DynamoDB batch/transaction
caps).** Enforcing `BatchWriteItem`'s real 25-item-per-call cap
immediately broke three already-green `animusd` integration tests that
predate the cap and built genuinely oversized single-call batches to
drive load gently: `batch_write.rs`'s throughput comparison (200 items in
one call), `backfill_seeder.rs`'s 300-row populate (chunked at 100), and
`update_table_drop_index.rs`'s `batch_put_items` helper (also chunked at
100). None of them were wrong when written — nothing enforced the cap
yet, so "one big batch" was a legitimate way to minimize consensus
rounds. `grep -rn 'chunks(100)\|chunks(200)\|...'`-style searches across
every crate's `tests/` tree found and fixed those three — **but missed a
fourth, `animusd::dynamo::stream_write_path_tests::
batch_write_on_a_marker_table_commits_one_entry_per_tablet`, an in-crate
`#[cfg(test)] mod` living inside `src/dynamo.rs` itself (40 items in one
call), caught only by CI actually running `cargo test -p animusd --lib`**
(see this crate's own `CLAUDE.md` "Tests" section for the list of
in-crate test modules — `lib.rs`, `index_drain.rs`, and `dynamo.rs` each
hide one or more, for the same reason: they need a private handle no
external `tests/` file can reach). The lesson from that miss: a sweep for
this class of regression must cover `src/**` `#[cfg(test)]` modules
**as well as** the `tests/` integration-test tree in every crate the
change touches — grepping only `tests/*.rs` silently skips exactly the
in-crate tests a crate's own `CLAUDE.md` flags as needing special
handling in the first place. The crate's own decode-level unit tests,
however thorough (at-cap accepted, one-over rejected), only prove the new
check rejects a synthetic oversized request — they can't prove the rest
of the workspace (either tree) doesn't already build one for an unrelated
reason. **General form**: any new hard cap on a previously-uncapped wire
shape (item size, batch/transaction item count, index count, key count,
…) needs a workspace-wide sweep of BOTH `tests/*.rs` and every `src/**
#[cfg(test)]` module before the change is considered validated, since a
test helper written before the cap existed has no reason to already
respect it — and running the actual `cargo test -p <crate> --lib`/
`cargo test -p <crate>` targets, not just a source grep, is what catches
what the grep pattern didn't anticipate. Fix the test helpers to chunk
(the same technique a real client SDK uses), not the new cap.
(`crates/animusd/tests/batch_write.rs`, `backfill_seeder.rs`,
`update_table_drop_index.rs`, `crates/animusd/src/dynamo.rs`.)
