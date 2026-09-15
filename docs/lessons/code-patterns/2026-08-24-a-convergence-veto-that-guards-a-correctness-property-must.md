# A convergence veto that guards a correctness property must be accelerated, never bounded or bypassed — the bound belongs on the *load* that's slow to drain, not on the gate itself (issue #288).

**A convergence veto that guards a correctness property must be
accelerated, never bounded or bypassed — the bound belongs on the *load*
that's slow to drain, not on the gate itself (issue #288).** The
frozen-endgame GSI-drain veto blocks `CutoverSplit` until the parent's
`"gsi"` cursor reaches the highest pending change record — because
cutover retires the parent and the reconciler reclaims its engine
outright (no drain-before-halt exists post-ADR-0044, see
`animus-cp-data/CLAUDE.md`'s "Superseded by ADR 0044" entry), so firing
cutover past an un-drained cursor would silently lose GSI updates forever
(children are born with empty change logs by design). An unthrottled write
flood racing the split made this veto converge too slowly (several
10s-of-seconds retries under load, see the "unthrottled continuous write
flood" entry above) — but the correct fix was never to loosen the veto
(e.g. force cutover after N stalled ticks, the shape the now-deleted
copy-based split-build driver's own bounded tail-pass chase used for the
*build* phase, safe there only because that phase's correctness never
depended on the lag being zero). The GSI-drain veto's correctness *is*
exactly "the lag is zero" — there is no compensating post-cutover
mechanism, so a bound here would be a straightforward data-loss bug, not
a liveness relaxation. The sound fix exploits a fact the copy-based
build phase didn't have: once the parent is frozen (in-place: once its
own single-entry fork has committed and both children are already fully
formed), the backlog this veto watches is fixed, not growing — so
driving the drain to exhaustion in a tight loop, right there in the
frozen endgame, has zero fairness cost and only removes the artificial
one-tick (`INDEX_DRAIN_INTERVAL`, 200ms) lag between "a drain pass makes
progress" and "the veto notices," including surviving a transient
propose failure under load without waiting a full extra tick to retry
it. **General rule**: before touching a gate that's "too slow to
satisfy," classify it — is the gate a correctness invariant (something
bad happens if you proceed before it holds) or a liveness heuristic
(nothing unsafe happens, it's just an imperfect proxy for "caught up")?
Only the second kind may ever grow a bounded-chase escape hatch; the
first kind's only legal fix is making the thing it's waiting on happen
faster, exploiting whatever makes the wait bounded now, rather than
relaxing what "caught up" means. **Still live, still following this
rule**: the acceleration mechanism itself (`gsi_caught_up`/
`FROZEN_ENDGAME_GSI_DRAIN_MAX_PASSES`) survived the copy-based driver's
own deletion — the in-place split's own frozen-endgame driver
(`index_drain.rs::inplace_split_driver_tick`) is now its sole caller,
applying the identical acceleration to the identical veto ahead of its
own `CutoverSplit` propose. Only the copy-based build-phase counterexample
(`SPLIT_MAX_TAIL_PASSES`, the bounded chase this entry originally
contrasted against) is gone; see
`docs/engineering-lessons-archive.md`'s "The copy-based split-build
driver" section for that original text verbatim.
