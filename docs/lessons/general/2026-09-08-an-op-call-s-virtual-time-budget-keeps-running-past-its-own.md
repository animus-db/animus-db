# An op call's virtual-time budget keeps running past its own future's completion — an "X still exists right after this call" assertion is unsound against a background reclaim loop unless the retention window comfortably clears the whole budget (ADR 0061 rung G, C-07 PR 5, 2026-09-08)

`SimCluster::spawn_and_capture` (the helper every op call — `dynamo`/
`put`/`drive_stream_seal`/`drive_inplace_split_cutover`/… — goes through)
always advances the simulator's virtual clock by the *full* `OP_BUDGET`
(12s) before returning, via an unconditional `self.sim.run_for(OP_BUDGET)`
— regardless of how quickly the call's own spawned future actually
resolves. An earlier lesson (this file's "sizing a per-op cost against the
per-call refill" entry) already covers what this means for a rate-based
throttle assertion; this is a different, second consequence of the
identical mechanism.

`two_phase_expiry_removes_the_row_and_every_replicas_object`
(`sim_cluster_stream_janitor.rs`) asserts a just-sealed segment object
still exists **immediately** after `drive_stream_seal` returns. With the
scenario's original `retention = 2s`, the seal itself typically lands
early inside `drive_stream_seal`'s own 12-second window — but the call
does not return the instant the seal commits, it keeps running the
simulator for the *rest* of that 12-second budget regardless, and the
segment janitor's 200ms tick has ample room in that leftover window to
mark-and-physically-delete the just-sealed object (phase 1b deletes the
object as soon as it's marked, independent of any later-epoch pin — see
`segment_janitor.rs`'s own doc) well before the assertion ever runs. The
result was a reproducible failure at every seed (`the segment object must
exist right after its own seal: []`), not a flake — this fixture is fully
deterministic, so a wrong retention/`OP_BUDGET` relationship fails every
single time, the same way a wrong assertion would.

**General form**: any scenario asserting "a thing this call just produced
is still present/absent right after the call returns" is implicitly
assuming nothing else advanced virtual time between the producing action
and the observation — but a `SimCluster` op call's own fixed-budget
`run_for` is exactly such an advance, hidden inside the call the scenario
already trusted. Where a background reclaim/expiry mechanism is also
running (this fixture spawns every production background loop
unconditionally on every node), a retention/timeout window shorter than
`OP_BUDGET` cannot be trusted to still be "not yet due" by the time
control returns to the test — size it to comfortably clear `OP_BUDGET`
whenever the scenario's own assertion depends on that immediacy, exactly
as the mid-sweep-catching scenarios in this same module already do
deliberately. A scenario that only needs *eventual* convergence (poll to
convergence afterward) has no such constraint and can keep a short
retention — the two existing strategies documented in `sim_cluster_
stream_janitor.rs`'s own module doc are the reusable shape.
