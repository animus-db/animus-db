# A "claim now, confirm later" state machine needs a release on *every* failure exit of the executor, not just the one the original design happened to handle (2026-08-19).

**A "claim now, confirm later" state machine needs a release on *every*
failure exit of the executor, not just the one the original design happened
to handle (2026-08-19).** `animus-cp-data`'s tablet-host reconciler splits a
pure `plan()` from an async executor: `plan` inserts a tablet into
`LocalState::hosted` the instant it decides to emit `HostAction::Host`, and
`tick` commits that state *before* running the actions. The teardown half of
the discipline was built correctly and documented at length — a
`Reclaim`/`Release` claim survives until the executor calls
`confirm_torn_down`, so a timed-out driver shutdown is simply re-planned next
tick. The host half had no such release: `host()`'s two early returns (the
tablet gone from `Metadata`, or `EngineFactory::open` failing on real disk
I/O) established no live handle and undid no claim, so `plan`'s own
idempotence gate (`!next.hosted.contains(&tablet)`) then swallowed the
tablet **permanently** — a phantom replica, degraded RF, no operator signal
beyond one `warn!`, recoverable only by restarting the process. The doc
comment two lines above the failure asserted the opposite ("`plan` re-emits
it next tick"), which is the tell: prose describing a recovery path is not
evidence the path exists. It survived because the sim-only `EngineFactory`
always returned `Ok`, so no test could reach the branch at all — a fallible
seam whose test double cannot fail is an untested seam. The mirror hole sat
in `teardown()`, which returned early when no live handle existed without
confirming, so a zombie claim re-planned its teardown forever. **The general
check**: for any optimistic claim a pure planner takes ahead of an async
executor, enumerate *every* way the executor can fail to land the action and
confirm each one either completes the claim or releases it — and give the
fault a test double that can actually inject the failure.
(`crates/animus-cp-data/src/host.rs::{plan, Reconciler::host,
Reconciler::teardown, LocalState::release_unconfirmed_host}`,
`tests/reconciler.rs::reconciler_recovers_a_tablet_after_a_transient_engine_open_failure`.)
