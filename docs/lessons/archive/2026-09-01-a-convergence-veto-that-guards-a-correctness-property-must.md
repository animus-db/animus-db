# A convergence veto that guards a correctness property must be accelerated, never bounded or bypassed — the bound belongs on the *load* that's slow to drain, not on the gate itself (issue #288).

**A convergence veto that guards a correctness property must be
accelerated, never bounded or bypassed — the bound belongs on the *load*
that's slow to drain, not on the gate itself (issue #288).** The
split-cutover GSI-drain veto (`index_drain.rs::split_driver_tick`
stage 3a) blocks `CutoverSplit` until the parent's `"gsi"` cursor reaches
the highest pending change record — because cutover retires the parent and
the reconciler reclaims its engine outright (no drain-before-halt exists
post-ADR-0044, see `animus-cp-data/CLAUDE.md`'s "Superseded by ADR 0044"
entry), so firing
cutover past an un-drained cursor would silently lose GSI updates forever
(children are born with empty change logs by design). An unthrottled write
flood racing the split made this veto converge too slowly (several
10s-of-seconds retries under load, see the "unthrottled continuous write
flood" entry above) — but the correct fix was never to loosen the veto
(e.g. force cutover after N stalled ticks, mirroring `SPLIT_MAX_TAIL_
PASSES`'s bounded chase for the *build* phase). `SPLIT_MAX_TAIL_PASSES`
is safe to bound because its own correctness never depended on the lag
being zero (the post-freeze final drain + final image still transfer
everything regardless of the bound); the GSI-drain veto's correctness
*is* exactly "the lag is zero" — there is no compensating post-cutover
mechanism, so a bound here would be a straightforward data-loss bug, not
a liveness relaxation. The sound fix exploits a fact the *build* phase
doesn't have: once the parent is frozen (Freeze rejects every later user
write), the backlog this veto watches is fixed, not growing — so driving
the drain to exhaustion in a tight loop, right there in the frozen
endgame, has zero fairness cost and only removes the artificial one-tick
(`INDEX_DRAIN_INTERVAL`, 200ms) lag between "a drain pass makes progress"
and "the veto notices," including surviving a transient propose failure
under load without waiting a full extra tick to retry it. **General rule**:
before touching a gate that's "too slow to satisfy," classify it — is the
gate a correctness invariant (something bad happens if you proceed before
it holds) or a liveness heuristic (nothing unsafe happens, it's just an
imperfect proxy for "caught up")? Only the second kind may ever grow a
bounded-chase escape hatch; the first kind's only legal fix is making the
thing it's waiting on happen faster, exploiting whatever makes the wait
bounded now (here: the parent going static at freeze) rather than relaxing
what "caught up" means.
(`crates/animusd/src/index_drain.rs::split_driver_tick`,
`FROZEN_ENDGAME_GSI_DRAIN_MAX_PASSES`.)
