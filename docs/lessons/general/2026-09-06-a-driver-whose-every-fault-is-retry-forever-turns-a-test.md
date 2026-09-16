# A driver whose every fault is "retry forever" turns a test-fixture mismatch into a silent hang, not a clear failure (ADR 0068 §6, S-05 PR 2 import driver)

Building `dynamo_import.rs`'s real-item test cases (a full export→import
round trip; a hand-written data file with two malformed items), both
initially failed with nothing more informative than "import did not reach
a terminal state in 30s" — no error body, no rejected request, just a row
stuck `IN_PROGRESS` forever. The instinct at that point is to suspect the
production code (the new `import.rs` driver, freshly written and never
proven against a real multi-item payload). It wasn't: the bug was in the
*test*'s own `import_table()` helper, which built an `ImportTable` request
with no `InputCompressionType` field at all — decoding to this adapter's
own default, `NONE` — while the paired fixture helper
(`write_hand_export`/a real `ExportTableToPointInTime` call) always wrote
`GZIP`-compressed data. The driver received real gzip bytes, tried to
treat them as plain UTF-8 text (per the request's own, wrongly-defaulted
compression), got binary garbage, logged `"data file is not utf8"` at
`WARN`, and — correctly, by this driver's own design (see the module doc:
every I/O/content-shape fault here is deliberately *retryable*, since a
customer bucket can be transiently unreachable or still finishing a write)
— just tried again next tick, forever, with no forward progress and no
terminal state to report. The fix was two lines in the test file (default
the shared helper to `GZIP`, matching what its own paired fixture always
produces); nothing in `import.rs` itself was wrong.

**Diagnosis, not just the fix**: the failure looked identical to a real
production bug from the outside (a hung poll, a generic timeout panic) —
distinguishing "test fixture mismatch" from "driver bug" needed actually
reading the driver's own log output, which required a temporary
`tracing_subscriber::fmt().with_env_filter("debug").try_init()` at the top
of the *specific failing test* (this workspace's test binaries carry no
default subscriber) and re-running that one test alone with `--nocapture`
— the moment the same `WARN` line printed once per tick, the root cause
was obvious. **General form**: (1) a background driver built on the
"every fault is retryable, only a bounded few are terminal" philosophy
(the same shape `backup_restore.rs`'s restore driver, and now
`import.rs`, both use) makes a stuck `IN_PROGRESS`/`Seeding`/`Creating`
row the *symptom* for an entire class of distinct root causes — content
mismatch, a wrong prefix, a transient store fault, a genuine driver bug —
so "it's stuck" alone is never enough signal to start editing production
code; reach for the driver's own `tracing` output on the *one* failing
test first. (2) When a test brings up a fixture through one helper and
issues the request that consumes it through a second, sibling helper, the
two must agree on every field that changes interpretation (here:
compression) — a shared default in one helper is only safe if every
caller of the *other* helper is guaranteed to match it; consider a single
helper that builds both, or an explicit parameter, once more than one
fixture shape (`GZIP` vs `NONE`) is in play.
