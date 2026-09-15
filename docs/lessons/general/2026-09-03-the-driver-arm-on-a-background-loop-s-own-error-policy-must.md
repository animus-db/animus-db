# The driver arm on a background loop's own error policy must match its sibling routes' (2026-09-03, issue #580, extends #572's lesson)

`inplace_split_driver_tick`'s two exhaustion loops (`crates/animusd/src/
index_drain.rs`, the streams and PITR final-seal loops immediately inside
the `Splitting` arm) called `seal_now`/`pitr_seal_now` in a `while ...?
.is_some() {}` shape — any `Err`, transient or permanent, propagated
straight out via `?` and aborted the whole tick, including every veto
check and the `CutoverSplit` propose after it. But a losing dueling-seal
race against the ordinary per-tick `seal_tick`/`pitr_tick` arm (same
`INDEX_DRAIN_INTERVAL` tick, same `(tablet, next_epoch)` slot — the
identical race issue #572 already named for `force_seal_tablet`/
`force_pitr_seal_tablet`'s `Local` route) returns a `"; retry"`-suffixed
transient error (`index_drain::is_retryable_elsewhere`), not a permanent
one — and this loop had no way to tell the two apart, so a transient loss
here cost the driver a full extra `INDEX_DRAIN_INTERVAL` (200ms) tick
before it tried again, on top of whatever this tick's own commit-wait
budget (`SEAL_COMMIT_TIMEOUT`) had already spent.

Fixed to mirror #572's own resolution exactly: a retryable loss now
`continue`s the exhaustion loop immediately (re-reading fresh state and
retrying within the *same* tick, the same treatment the periodic arm's own
next call already gets structurally for free), and only a non-retryable
`Err` propagates via `?` as before. Same classifier
(`index_drain::is_retryable_elsewhere`), reused, not reimplemented — one
retryability rule for this whole error family, now applied identically on
every route that can hit it: the periodic per-tick arm, `force_seal_tablet`/
`force_pitr_seal_tablet`'s local route (#572), and the split driver's own
endgame loop (this fix). See #572's own lesson entry above for the general
form ("a routed operation's error policy must be identical on the local
and forwarded routes") — this is the same shape one route further out: not
local-vs-forwarded, but ordinary-tick-arm-vs-split-driver-arm, both
converging on the identical underlying primitive and its identical
transient-failure mode.

No `SimEnv`-driven regression test was added for this specific change: the
loop's shape (`while primitive(..)?.is_some() {}` around a fallible
primitive) is exercised for the ordinary per-tick arm by `index_drain.rs`'s
own `stream_sealer_tests` unit-test module (per issue #570's precedent,
referenced from that section) — there is no equivalent PITR-arm unit-test
module yet — but reproducing
*this* specific race — the split driver's own exhaustion loop racing the
periodic arm for the same `(tablet, epoch)` slot inside one real
`INDEX_DRAIN_INTERVAL` tick, during an actual in-place split — needs two
real concurrent driver ticks against a live `CpGroup`, which is exactly the
shape `crates/animusd/CLAUDE.md`'s own `ClientCtx` `SimEnv` harness doesn't
reach (it drives `ClientCtx` request handling, not this crate's real-thread
background loops racing each other) and a `ProdEnv` `#[tokio::test(multi_
thread)]` integration test would be needed to force deterministically —
timing-dependent by construction, and this repo's own stated policy (see
"`SimEnv` proves logic and ordering, not real-thread liveness" in
`CLAUDE.md`) is not to paper over that gap with a test that would itself be
flaky. Correctness here rests on the change being a direct, minimal mirror
of #572's already-reviewed fix to the identical predicate on a materially
identical race, not on a new test proving it.
