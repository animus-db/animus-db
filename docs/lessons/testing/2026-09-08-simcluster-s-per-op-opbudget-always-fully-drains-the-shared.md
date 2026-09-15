# `SimCluster`'s per-op `OP_BUDGET` always fully drains the shared virtual clock, so a "not yet happened" assertion against a fast always-on background loop cannot be expressed the way it can under a real, slow interval (2026-09-08, ADR 0061 rung I C-09 PR 3).

**`SimCluster`'s per-op `OP_BUDGET` always fully drains the shared virtual
clock, so a "not yet happened" assertion against a fast always-on
background loop cannot be expressed the way it can under a real, slow
interval (2026-09-08, ADR 0061 rung I C-09 PR 3).** `SimCluster::dynamo`/
`put`/etc. all go through `spawn_and_capture`, which calls
`self.sim.run_for(OP_BUDGET)` (12s) unconditionally; `animus_sim::
Simulator::run_until` always drains every scheduled event up to that
deadline before returning, regardless of how quickly the awaited future
itself resolved. Any background loop with a period well under `OP_BUDGET`
(the always-on TTL reaper's 200ms `SIM_TTL_SWEEP_INTERVAL` is 60x
shorter) therefore gets dozens of ticks *inside* a single ordinary wire
call, not just between separate calls. That makes "write a
should-already-be-actionable state, then immediately check it hasn't
been acted on yet" scenarios structurally unreachable through the wire
entry point — the original real-socket test this pattern converts from
(`dynamo_ttl.rs::expired_item_is_still_readable_immediately`) relies on
a *slow* production-scale interval specifically to keep that window
open, and `SimCluster` has no "hold a background loop back for N calls"
primitive to substitute (a `drive_*` helper only ever forces extra
ticks, never suppresses the always-on ones). Before assuming a
`SimCluster` conversion of a "still in the pre-action state" test is
just a matter of picking the right helper, check whether the assertion
needs the always-on loop to have had *zero* chances, not just to have
had no reason to act — if so, it may be a genuine, well-reasoned residual
to leave on `ProdEnv`, not a gap in fixture coverage. A "never acts on
this input" assertion (the opposite: many ticks are fine because none of
them should do anything) has no such problem and converts cleanly.
