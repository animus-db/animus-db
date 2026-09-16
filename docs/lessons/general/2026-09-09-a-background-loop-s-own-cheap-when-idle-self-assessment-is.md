# A background loop's own "cheap when idle" self-assessment is per-loop, not per-fixture — and a plausible root-cause story still needs a direct measurement before the fix is trusted (2026-09-09, ADR 0061 rung I C-09 PR 3's follow-on, #772)

Five separate rungs (D4 PR 1's real `Reconciler` cutover + `heartbeat_
loop`, D4 PR 5's backup janitor, C-07 PR 5's segment janitor, C-09's TTL
reaper, C-10's index backfill), each landing weeks apart, each added one
more always-on per-node background loop to `SimCluster::new`/`restart` —
and each one's own PR reasonably concluded its own 200ms tick was cheap,
because it was validated against this crate's own depth-1 `sim_cluster`
lib tier (a handful of short scenarios) and looked negligible there. None
of them was ever run against `ANIMUS_DYNAMO_WIRE_SEEDS=25` — the nightly
`corpus-deep` tier, 200 scenarios, 30-minute CI budget — because nothing
about any one PR's own scope pointed at that tier as the place its cost
would compound. It took a sixth rung's own always-on loop (the TTL
reaper, then a seventh, index backfill) before the accumulated tick
overhead crossed the 30-minute wall and the nightly finally went red, at
scenario 181/200. **The general lesson: a background loop's own
"cheap-when-idle" self-assessment is scoped to the tier it was measured
against, never to every tier that will ever spawn it** — the tier to
measure a NEW always-on `SimCluster` loop's added cost against is
whichever tier multiplies node-count times op-count the hardest
(`ANIMUS_DYNAMO_WIRE_SEEDS=25` for this fixture specifically, not the
depth-1 default), and a fix belongs in a single shared, auditable
constant (`SIM_FALLBACK_TICK`, `sim_cluster.rs`) rather than five
independently-tuned 200ms ones that each looked reasonable in isolation.

**A second, sharper lesson from the same investigation: the plausible
root-cause story the first pass wrote down was only partly right, and a
direct A/B measurement caught the gap before the fix shipped on faith
alone.** The framing going in — "200ms times up to 7 nodes times ~12s
virtual per op is a ~3.3x compounded overhead" — was arithmetically
clean and matched the timeline (the regression from a documented
~601s/200-scenario baseline to a 30-minute timeout correlated with
exactly these five rungs' own landing dates). It was still not the
dominant cause: a direct `animus_sim::Simulator::stats()` measurement (a
new, purely additive `task_polls`/`timer_fires` counter, built for
exactly this) on the corpus's cheapest cell showed the five loops' own
tick rate moved total executor cost only ~5.6% across a 300x range of
tick coarseness (200ms to 1s to 60s). The real dominant cost was
something the fix's own scope never touched: real per-tablet/
control-plane Raft consensus traffic (heartbeats, `AppendEntries`,
confirm-poll retries) accumulating across the large cumulative virtual
time `SimCluster`'s own `spawn_and_capture` design burns — every
synchronous op call drives a full, unconditional `Simulator::
run_for(OP_BUDGET)` (12s) regardless of how quickly it resolves, and the
corpus's own probe calls (`run_transact_probe` especially, added by a
later, unrelated rung) issue many such calls per scenario. The fix still
shipped — it closes the acute symptom (the corpus now completes at
`ANIMUS_DYNAMO_WIRE_SEEDS=25` instead of timing out) and is a real,
worthwhile simplification (one auditable constant instead of five) — but
it does **not** restore the corpus to anywhere near its stale ~601s
baseline, and the honest report says so rather than declaring victory
against the softer, unverified target. **The rule this leaves behind**:
when a plausible, timeline-correlated root-cause story is handed to you
as already-established, measure it directly before trusting it to size
the fix — correlation in time between "these rungs landed" and "the tier
went red" does not by itself prove causal magnitude, only that a
defensible fix in the region has to exist; a cheap, purely additive
counter (here, two `Relaxed` atomics on the simulator's own drain loop)
is usually enough to tell the difference between "the whole story" and
"the timeline-plausible half of it," and is worth building even when the
task brief already names a fix to implement.
