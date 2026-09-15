# A health/status rollup that gates on a *proxy* signal (a member's `Down` status) rather than the actual risk that signal stands in for (a tablet under-replicated/leaderless) can diverge from reality forever, because the two clear on different triggers.

**A health/status rollup that gates on a *proxy* signal (a member's `Down`
status) rather than the actual risk that signal stands in for (a tablet
under-replicated/leaderless) can diverge from reality forever, because the
two clear on different triggers.** The dashboard's `computeHealth()`
(ADR 0021) treated any `Down` member as itself "degraded" — but a `Down`
member only clears on manual decommission (ADR 0032 PR3) or the node
rejoining, while the actual data-loss risk it represents is cleared much
sooner, automatically, once the placement reconciler repairs every tablet
the dead node used to replicate onto a spare (`failure_auto_replaces_
replica_onto_spare`). So a cluster whose data was fully re-replicated
within seconds could show "Degraded" indefinitely, until someone
remembered to decommission the long-dead node. Fixed by keying "degraded"
on the tablets' own derived status (`leaderlessCount`/`underReplicatedCount`,
already computed per-tablet for the "Under-replicated" stat tile) instead
of the member roster; `downCount` is kept as informational context in the
banner/tiles, not a health-gating input. **General check for any rollup
built from "X is down/unhealthy ⇒ overall is unhealthy": does the thing
being protected (data replication, request-serving capacity) actually
recover on a faster/different path than the raw signal does — and if so,
gate on the protected property, not the signal.**
