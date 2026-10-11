# A pure planner that defers work must leave the deferred items unclaimed and make the executor re-tick itself

**Context.** ADR 0078 step 5 (staggered hosting): `host::plan` now takes a remaining budget and emits at
most that many fresh `Host` actions, lowest `TabletId` first.

**Lessons.**
- Defer by *not claiming*. `plan` records a tablet in `LocalState::hosted` the instant it emits `Host`
  (the optimistic claim). A deferred tablet is simply left out of that set, so the next call re-derives
  it from the same inputs with no separate backlog structure to keep consistent. The only extra state is
  an output counter (`deferred_hosts`) for the caller's wake and gauges. The property tests then fall out
  directly: sorted output, `len <= budget`, deferred tablets absent from `next.hosted`, repeated calls
  drain everything exactly once.
- Cap only the path that creates the load. Split-child materialization, `Reconfigure`, `Release` and
  `Reclaim` stay exempt, and a test with a zero budget pins that they all still fire.
- A planner that defers makes nothing wake the loop to ask again: groups settling raises no
  metadata-watch event, so the loop would fall back to its 500 ms poll and a 10-wave ramp would take 5 s
  of pure idle. The executor must expose a `next_wake()` (here `min(settle/4, 50 ms)` while anything is
  deferred) and every loop that drives it, production and `SimCluster` alike, must race its sleep against
  it. Easy to forget on the second loop.
- When a budget input is derived from live state (unsettled = hosted groups without a leader), compute it
  once per tick before `plan`, from the same clock the rest of the tick uses (`env.now()`), and give the
  "give up waiting" backstop a timeout so a group that can never elect cannot hold a slot forever.
- A new default-on throttle changes every existing test that creates more than `H` of the thing. Run the
  whole tier with it on and look at each change; a SimCluster test that hosts few tablets is unaffected,
  a bulk one now sees waves.
