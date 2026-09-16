# `SimCluster::admin`/`SimCluster::dynamo`-style helpers unconditionally burn a full `OP_BUDGET` of virtual time per call — a scenario that needs to control elapsed virtual time precisely between two mutating calls must bypass them

`SimCluster::spawn_and_capture` (the shared plumbing under `SimCluster::admin`,
`::dynamo`, `::put`/`::get`/etc.) always `run_for(OP_BUDGET)` (12s) after
spawning the request future, then takes whatever landed in the result slot —
by design, not a bug: at least one other scenario relies on every call
unconditionally advancing the clock by the full budget regardless of how fast
the request resolved (a `ThrottleBucket` needing to have genuinely refilled by
the next retry with no explicit sleep of its own). But it means a scenario
that issues two mutating admin calls back to back and expects only a SMALL,
precisely-controlled amount of virtual time to have elapsed between them (a
narrow post-election timing window, a lease about to expire, anything sized
against a specific constant) will silently see 12+ seconds elapsed after the
FIRST call alone, even though that call's own decision was made — and could be
observed as already resolved — almost instantly. Symptom: a guard/timeout that
should read as "just barely inside its window" reads as "long since expired,"
and the failure looks exactly like a product bug in the guard, not a test
authoring mistake, because the guard's own decision really was made using a
now-elapsed value the test never intended to produce. `SimCluster::admin_timed`
(early-stopping, reports real elapsed) or a direct call on the underlying
`RaftNode`/`Simulator` handle (bypassing the admin/HTTP layer entirely) are the
ways out; either lets a test advance virtual time in exactly the increments it
chooses.

A second, compounding trap in the same family: `SimCluster::control_leader_index`
(and helpers shaped like it) has its own **hidden retry** — it silently
`run_for`s in its own increments (up to a multi-second budget) if no leader is
found on the very first check, so a scenario polling leadership status in a
tight per-step loop of its own can have a chunk of that loop's steps
invisibly consumed by the helper's own catch-up during a momentary election
gap, again destroying precise elapsed-time control. Prefer a direct,
non-retrying single-shot leadership read for that shape of scenario, adding a
new thin one if none exists, and keep the retrying helper for everything that
only cares "did we eventually get a leader," not "how much time did that
take."

(`crates/animusd/src/sim_cluster.rs`'s `spawn_and_capture`/`admin_timed`/
`control_leader_index`/`is_control_leader`; the shape that surfaced this was
`sim_cluster_control_membership_admin.rs`'s issue #923 regression, which needs
elapsed virtual time between a leadership transfer and a follow-up removal
call to land inside a specific ~2s grace window.)
