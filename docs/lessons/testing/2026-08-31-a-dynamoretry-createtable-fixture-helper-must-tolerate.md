# A `dynamo_retry`+`CreateTable` fixture helper must tolerate `ResourceInUseException` on the retry, not just retry 500s (2026-08-31, issue #461).

**A `dynamo_retry`+`CreateTable` fixture helper must tolerate
`ResourceInUseException` on the retry, not just retry 500s (2026-08-31,
issue #461).** `create_table` (`crates/animusd/src/schema.rs`) calls the
blocking `await_table_serveable` before it acks 200 — a real wait that can
itself time out server-side and surface as a retryable 500, *after* the
schema commit it was waiting on already landed durably. A test's
`dynamo_retry` helper that retries only on `status == 500` then replays
the identical `CreateTable` into the duplicate-name check ahead of that
wait, which correctly answers `400 ResourceInUseException` — a legitimate
outcome of retrying a non-idempotent-from-the-client's-view operation, not
a test failure. `dynamo_expression_surface.rs`'s `setup()` hard-asserted
200 and had no such tolerance, so it flaked under exactly this timing.
Fixed by treating a `ResourceInUseException` `CreateTable` reply as
success **and** re-probing serveability explicitly in that branch (a
`ConsistentRead` `GetItem` run through the same `dynamo_retry` helper) —
the 400 path skips the server's own `await_table_serveable` wait, so
"the table exists" and "the table is serveable" are not the same fact
once you take that branch. A repo-wide grep at fix time
(`ResourceInUseException` in `crates/animusd/tests/`) found no existing
shared helper doing this — every other hit was a test deliberately
asserting the *rejection* case — so there was nothing to reuse; if a
second `setup()` trips the same race, factor the tolerant-CreateTable +
readiness-reprobe pair into `tests/support/mod.rs` rather than hand-rolling
a third copy.
