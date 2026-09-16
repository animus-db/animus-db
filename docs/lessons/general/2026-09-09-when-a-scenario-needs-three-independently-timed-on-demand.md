# When a scenario needs three independently-timed on-demand primitives to race correctly, and you can't run it, use the license — don't guess (ADR 0061 rung J, C-10 PR 4)

Converting `tests/backfill_seeder.rs`'s `split_during_backfill_converges_
with_correct_final_gsi` to `SimCluster` would have meant hand-interleaving
three separately-timed on-demand primitives every round —
`drive_backfill_seed`/`drain_gsi` (the pre-cutover vetoes `index_drain::
inplace_split_driver_tick` checks) and `drive_inplace_split_cutover`
itself (the cutover propose) — across every node, in a fixture that
spawns no `index_drain::change_consumer_loop` at all (every per-tick arm
that loop would normally run is instead something a test drives by hand).
The session converting it had no `cargo` access in its worktree, so
nothing about the resulting round-loop's actual convergence — whether the
always-on completion aggregator can flip the index `Active` while the
parent is still un-cut-over (it watches "every tablet *currently* in the
table's live map has reported," which the still-`Splitting` parent alone
can already satisfy, independent of whether cutover ever committed), or
whether the post-cutover Fork-A per-child resweep converges within any
round budget picked blind — could be checked before landing it.

The ADR's own rung-opener text had already licensed exactly this case
("licensed to stay `ProdEnv` if it does not converge cleanly under the
fixture"), anticipating that a scenario combining several of a rung's own
newly-built on-demand primitives at once is a materially different
verification problem than any one of them alone, each already proven
individually by a sibling scenario. **The lesson generalizes past this one
test**: when a conversion's own correctness rests on the *interaction* of
several independently-timed driver calls rather than on any single call's
already-proven behavior, and the session doing the conversion cannot
compile or run what it writes, use the license to keep the scenario on
its original substrate rather than ship an unverified sequencing that
might hang, flake, or silently pass for the wrong reason. A named,
reasoned residual with a one-line "why" is worth more than an unverified
conversion — the next session with build access can attempt it with a
real pass/fail signal instead of guessing twice.

**A second, smaller finding from the same investigation, fixed in a later
pass of this same PR**: `SimCluster::restart` respawned `heartbeat_loop`/
`backup_janitor_loop`/`segment_janitor_loop`/`ttl_reaper_loop`/
(conditionally) `auto_split_loop` — every perpetual background task
`Simulator::stop` drops for the restarted node — but not `index_backfill::
index_backfill_loop` (added to `SimCluster::new` by this rung's own PR 2).
Not a correctness bug on its own: the completion aggregator is
control-plane-**leader**-only and self-gated, and every *other* node's own
instance (spawned once at `SimCluster::new`, never touched by a different
node's restart) keeps running regardless — so a scenario that
crashes/restarts a tablet leader who happens to have also been the control
leader still converges once some survivor takes over control leadership,
since that survivor's own aggregator instance was never stopped. It only
mattered for a scenario that would restart *every* node in sequence, or
that inspects the restarted node's own `index_backfill_loop` liveness
directly. **Fixed**: `SimCluster::restart` now respawns `index_backfill::
index_backfill_loop` unconditionally, in the same place and the same shape
as the `ttl_reaper_loop` respawn immediately above it — a plain
mirror-the-pattern fix, no new mechanism.
