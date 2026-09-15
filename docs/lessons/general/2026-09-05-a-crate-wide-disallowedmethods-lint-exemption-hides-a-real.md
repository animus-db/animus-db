# A crate-wide `disallowed_methods` lint exemption hides a real determinism violation from every enforcement layer, not just from review (W-09, ADR 0034 amendment)

Building `RequestRateTracker` (the W-09 write-rate auto-split signal)
alongside its existing sibling `ChangeRateTracker` (`crates/animusd/src/
lib.rs`) surfaced that the sibling had been calling `tokio::time::
Instant::now()` directly since it was added (ADR 0042 §14, growth PR3) —
a plain violation of the root `CLAUDE.md`'s "no wall clock, use `env.now()`"
determinism rule (ADR 0003). Nothing caught it: `animusd`'s package-level
`[lints.clippy]` override turns off `disallowed_methods` for the whole
crate (`crates/animusd/CLAUDE.md`'s own documented ADR 0061 rung B5
carve-out, covering `lib.rs` itself among the files still under the
package-level allow), so `cargo clippy --workspace -- -D warnings` — the
gate this repo actually runs — was silent about it. The only two things
that could have caught it were a human specifically checking this one call
site during original review, or the narrower `#[deny(clippy::
disallowed_methods)]` ADR 0061 Phase C's closing rung put on five
`E: Env`-generic client-path modules (`schema`/`read_path`/`write_path`/
`txn_coordinator`/`forwarding`) — and `ChangeRateTracker` lives in `lib.rs`
itself, outside that narrower deny's scope.

The violation was latent, not inert: `ChangeRateTracker::observe` read the
wall clock to compute an EWMA smoothing interval, which under `SimEnv`
(any future test that tried to drive it deterministically) would have
produced a real, non-reproducible time reading spliced into an otherwise
seed-pure computation — exactly the failure mode the determinism rule
exists to prevent, just never yet exercised because nothing had tried to
unit-test this tracker under `SimEnv` before this task needed to.

**Fixed as a pure refactor** (`RateSample`/`RateSample::advance`, the
shared EWMA core both trackers now use): `observe` takes the caller's own
`env.now(): Nanos` reading as an explicit parameter instead of reading a
clock internally. Every real call site already had a `ClientCtx`/`CpGroup`
handle in scope to read `now` from (`index_drain.rs`'s `seal_tick`,
`dynamo.rs`'s `kind_write_item_at_leader`), so this needed no new
plumbing — only the tracker's own two-line clock read moved to its caller.
Both trackers then got a `SimEnv`-driven unit test (`rate_tracker_tests`,
`lib.rs`) proving the EWMA converges toward a sustained rate and decays
once observations slow or stop, entirely over virtual time.

**General form**: a crate-wide (or even five-module-wide) lint exemption is
a documented, deliberate debt — not a blanket license to stop looking. A
type living inside the exempted surface that reads a raw clock/spawns a raw
task/etc. is invisible to `cargo clippy -D warnings` by construction, so
the only remaining check is a human noticing during review, or — as
happened here — a later change that needs the same shape and, in building
it correctly, finds the older sibling wasn't. When extending or mirroring
an existing type inside a lint-exempted module, always check whether the
existing one already violates the rule the exemption is hiding, and fix
both together if the fix is a pure refactor with no behavior change — the
same "in scope because the two must share one shape anyway" reasoning that
applies to any other shared-code touch.
