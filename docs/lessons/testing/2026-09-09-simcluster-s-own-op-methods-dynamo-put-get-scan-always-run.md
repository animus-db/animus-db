# `SimCluster`'s own op methods (`dynamo`/`put`/`get`/`scan`/...) always run a request to completion in one synchronous call, so there is no window from a test's own code to interleave a second action mid-flight the way a real-socket test's `tokio::join!`/fire-and-forget-then-sleep can

**`SimCluster`'s own op methods (`dynamo`/`put`/`get`/`scan`/...) always
run a request to completion in one synchronous call, so there is no
window from a test's own code to interleave a second action mid-flight
the way a real-socket test's `tokio::join!`/fire-and-forget-then-sleep
can** (ADR 0061 rung J, C-10 PR 3). Every one of these methods is
`spawn_and_capture`: spawn the future, then `self.sim.run_for(OP_BUDGET)`
drains the simulator to completion (or timeout) before returning — there
is no intermediate point a caller can observe or act on. A real-socket
test that races a background poll against a live request, or fires a
request and abandons it after a brief sleep to simulate a crash
mid-cascade, has no literal equivalent under this fixture. The fixture's
own established workaround, used consistently across every "crash during
X" scenario (`sim_cluster_dynamo_drop_table.rs::run_scenario_4_a_node_
crashed_during_the_drop_and_restarted_reclaims_its_engine` and this PR's
own `a_crash_and_retry_mid_cascade_still_converges`): spawn the request
by hand on the target node's own `SimEnv` (`SimCluster::handle().env(
node)`, the identical primitive the D1-step-3 corpus's own concurrent
client tasks use), optionally drive the simulator a little via
`SimCluster::run_for` to let it make partial progress, then interrupt
with `SimCluster::crash`/`restart` before ever calling `dynamo`/`put`/etc.
(which would run it to completion instead). A scenario that needs the
interleaving itself (not just "abandon and check the end state
converges") — e.g. a live poll racing a live write to prove a property
never transiently holds — has no fixture-level equivalent at all; the
honest move is a deterministic sequenced substitute that proves the same
property a different way (documented inline, per this crate's own "note
where a scenario doesn't literally reproduce concurrency" convention),
not a strained attempt to force real concurrency out of a
single-threaded discrete-event simulator.
