# A driver stamped a wall-clock field from the monotonic clock — the two `Env` time sources look interchangeable until a value crosses a wire boundary (ADR 0059 §9/§10, Train 3 PR②)

`pitr_seal_now`/`pitr_tick` (`animusd::index_drain`) stamped
`PitrSegmentRow::seal_wall_ms` from `ctx.env.now()` — `Nanos`,
monotonic-since-process-start, the ordinary timer/timeout/backoff seam —
instead of `ctx.env.wall_now()`, ADR 0051's one real-calendar-time seam.
Both compile, both return a plausible-looking integer, and every existing
test passed: nothing *inside* the sealing path ever compares
`seal_wall_ms` to a real timestamp, so the bug was invisible until a
consumer that does — `PitrSpec::enabled_wall_ms` (genuinely wall-clock,
set from a wire-facing `UpdateContinuousBackups` call) compared against
this field to derive `LatestRestorableDateTime` — silently collapsed the
whole PITR restore window to zero width, forever, the instant any tablet
ever sealed. It shipped in Train 3 PR① and sat undetected through that
PR's own full corpus and review, because PR①'s own scope never *read*
`seal_wall_ms` against a real timestamp; PR②, the first consumer to do so
in earnest, hit it on its very first end-to-end run.

**The generalizable rule**: when a field's own doc or name says "wall
clock" / "real time" / anything that will eventually be compared against a
value carried in from *outside* the simulation seam (a wire timestamp, an
external system's clock, ADR 0051's `wall_now()` in this codebase), audit
every site that *writes* it as carefully as the sites that read it —
`env.now()` and `env.wall_now()` return the same Rust type from the same
trait and both "just work" in isolation, so a writer-side mixup produces
no type error, no panic, and no test failure until something finally
diffs the written value against a genuinely wall-clock one. A codebase
with two clock seams needs a grep sweep of every write site for a
wall-clock-typed field whenever that field gains its first real consumer,
not just a review of the consumer's own new code.
