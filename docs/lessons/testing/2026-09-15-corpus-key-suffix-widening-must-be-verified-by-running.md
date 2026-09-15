# A static audit of a self-contained corpus's key-parsing is not a substitute for running it

When a production key/wire format widens (issue #852: a DynamoDB Streams
change-log key's trailing suffix grew from a bare 8-byte `packed_hlc` to a
12-byte `(packed_hlc: u64, ordinal: u32)` pair), every consumer that
independently re-derives the key layout — not just the production readers —
has to move in lockstep. This repo's self-contained fault-injection corpora
(`animus-test/tests/stream_lineage_corpus.rs`, `pitr_fault_corpus.rs`,
`backfill_fault_corpus.rs`) deliberately reimplement the production sealing/
draining algorithms directly rather than importing `animusd` (a crate they
cannot depend on), which means each one has its own copy of the "strip the
trailing N-byte suffix off a change-log key" logic.

A static, read-the-source audit of these files concluded `backfill_fault_
corpus.rs` was unaffected, reasoning that its own `KIND_CURSOR` cursor row is
keyed by a raw base-key prefix, not by HLC, so ordinal-widening the change-log
key format couldn't touch it. That reasoning was correct about the cursor row
and *wrong* about the file as a whole: `assert_full_coverage`'s own
`partitions_with_change_marker`/`decoded_change_records` helpers read the raw
`KIND_CHANGE` key directly (`k.len().checked_sub(8)`) to derive each change
record's own partition prefix, entirely independent of the cursor mechanism
the audit reasoned about. Once the production key widened to 12 bytes, those
two helpers silently mis-sliced every key, corrupting the derived partition
set and producing false "partition has a base row but no dirty marker"
failures the moment the fix landed — 11 of 12 scenarios in that file went red
immediately.

**The lesson**: a per-file static audit of "does this file's own mechanism
touch the changed format" is not sufficient for a corpus that reimplements
production logic byte-for-byte — the touched surface can be a different
function in the same file than the one the audit reasoned about (a
coverage-diffing assertion helper vs. the cursor persistence the audit
focused on). After any key/wire-format change, actually build and run every
self-contained corpus that constructs or parses that format
(`grep -rn "checked_sub(N)\|len() - N"` for the old width across the whole
`animus-test` and `animusd` trees, not just the crates the change's own
description names), rather than trusting a reasoning-only pass to have found
every call site. The same investigation also found three in-crate
`animusd::dynamo::stream_write_path_tests` tests doing the identical raw
8-byte suffix slice directly on a `group.pending_changes()` key — a second,
independent instance of the same class of miss, only caught because the
task's own gate list required a full `cargo test -p animusd --lib` run
before declaring the change complete. A key-format change's "done" checklist
must include actually running every test binary that can reach the changed
key shape, not just the ones an initial grep for the changed function names
turns up.
