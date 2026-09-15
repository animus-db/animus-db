# A mechanical `Instant::now().elapsed()` → `Env` conversion must preserve WHAT is measured, not just the API — an absolute timestamp compared against a stored wall time is not an elapsed duration (issue #737, closing the finding above)

The bug the previous entry traced to its source line is now fixed —
worth its own entry because the *general* lesson is about the conversion
technique itself, not just this one call site, and generalizes past this
one rung.

**What went wrong.** ADR 0061 rung C5 step 3b mechanically converted every
`tokio::time::Instant::now()`/`.elapsed()` call in five `animusd` modules
to the `Env` seam. Most of those calls fit a clean pattern —
`let deadline = Instant::now() + TIMEOUT; ... while Instant::now() <
deadline { .. }` — and converted cleanly to `env.now().saturating_add(..)`
/`env.now() < deadline`, because in every one of those the *quantity*
being measured (elapsed wall time toward a locally-scoped deadline) has an
exact `Env`-seam equivalent. Two sites in `ClientCtx::txn_recover`
(`crates/animusd/src/txn_coordinator.rs`) did not fit that pattern at all:
they used `tokio::time::Instant::now().elapsed()` to produce a **near-zero
duration, immediately discarded except for a comparison** — a
`now_ms`-shaped local the surrounding code then compared against a
**stored, previously-minted absolute timestamp** (`created_ts.wall_ms`,
an `HlcTimestamp`'s own wall-clock component) plus a grace window. The
conversion rung noticed the shape didn't fit cleanly (`Nanos` has no
`elapsed()`), correctly diagnosed that the *pre-existing* code was
computing a near-zero elapsed gap rather than anything meaningful for the
comparison it fed, and — deliberately, and documented as such at the time
— reproduced that identical near-zero value with two back-to-back
`env.now()` reads plus `duration_since`, rather than "fixing" what looked
like a pre-existing latent bug during an unrelated testability rung. That
call was defensible in isolation (an incidental bug does get its own PR,
not a drive-by fix bundled into a different rung's scope) — but the
underlying defect it preserved was real, not cosmetic: it made
`ClientCtx::txn_recover`'s non-local grace check compare a manufactured
near-zero value against `wall_ms + RECOVERY_GRACE` forever, so recovery of
a foreign in-doubt intent declined permanently instead of proceeding once
`RECOVERY_GRACE` had genuinely elapsed — invisible in every `ProdEnv`
integration test (which also exercises `txn_resolver_loop`'s background
sweep, which always runs on the record's own anchor leader and so always
took the *correct*, unaffected `CpRoute::Local` branch) and unreachable
under `SimEnv` until an unrelated fix (issue #731, the entry above) first
cleared a deadlock that had been intercepting every recovery attempt
before `txn_recover` was ever actually called.

**The fix**: give the grace check its own single, absolute-timestamp-
producing helper (`recovery_grace_now_ms`, shared by both `txn_recover`
call sites so they cannot diverge on this again) that reads `env.now()`
directly and converts nanoseconds to milliseconds the same way
`animus_cp_data::hlc::Hlc::mint` does when it produces `wall_ms` in the
first place — never a duration between two adjacent reads.

**The general lesson**: a mechanical `Instant::now().elapsed()` → `Env`
seam conversion must ask, at every call site, not just "does this compile
against the new API" but "what quantity did the old code actually
produce, and does the new code produce the *comparable* quantity." Two
shapes look superficially identical (`instant.elapsed()`, `Nanos::
duration_since`) but answer completely different questions:

- **An elapsed-duration measurement** ("how long did this operation take,"
  "has this deadline passed") wants exactly what `tokio::time::Instant::
  elapsed()`/the deadline-comparison pattern already gives — a genuinely
  fresh reading compared against an earlier reading or a computed
  deadline, both from the *same* clock, both meaningful as a *duration*.
- **An absolute-timestamp comparison** ("is `now` past `stored_value +
  grace`") wants the raw clock reading itself, at the moment of
  comparison, compared against another absolute reading (however it was
  produced) — never a duration between two reads of the *same* moment,
  which by construction is always near-zero regardless of how much real
  time has passed since the *stored* value was minted.

Reproducing a pre-existing computation's literal shape during a mechanical
conversion is the right call when the shape is sound (preserves
behavior, defers a genuinely separate concern to its own fix) — but it is
only actually behavior-preserving when the *quantity* the old shape
produced was correct for what the surrounding logic needed in the first
place. When a conversion rung's own investigation flags a call site as
"doesn't fit the clean pattern" (as this one explicitly did, in both the
commit and the crate's own `CLAUDE.md`), that flag is exactly the signal
to ask the harder question — what is this value actually being compared
against, and in what units/clock — rather than only asking "does this
byte-for-byte reproduce what the old code computed." The old code's own
computation can itself be the bug the conversion is inadvertently
preserving. **Audit every converted clock read for the quantity it
produces, not just the API it now compiles against** — the check that
would have caught this one directly: for every `env.now()` read feeding a
comparison, ask what the *other* side of that comparison is (a locally-
scoped deadline built the same tick? sound as a duration. A value stored
elsewhere, minted at a different time, by a possibly different clock read?
needs an absolute reading, not a duration) — see `crates/animusd/CLAUDE.md`
's txn_coordinator.rs entry and `docs/adr/0018-cross-tablet-transactions.
md`'s matching 2026-09-07 amendment (which states the grace check's own
clock requirement explicitly, for exactly this reason) for the concrete
instance this generalizes from.
