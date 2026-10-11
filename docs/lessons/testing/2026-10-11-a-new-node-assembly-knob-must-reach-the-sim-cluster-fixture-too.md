# A new node-assembly knob must reach the `SimCluster` fixture too

**Context.** ADR 0078 Tier 2: the C-17 density cells in `animusd`'s
`sim_cluster_scale.rs` were extended to print the election-backpressure metrics
(`campaign_inflight_hwm`, `campaign_deferred_total`). The first run printed
zeros everywhere, including on cells whose groups demonstrably re-elected.

**What happened.** Two independent gaps, either of which alone gives zeros:

1. Steps 3 and 4 of the ADR wired the campaign governor and the load monitor
   into `animusd`'s production node assembly (`node_election_backpressure`,
   called next to `set_host_stagger`/`set_ramp_gauge`), but `SimCluster`
   builds its reconcilers in its own `build_reconciler`, which "mirrors
   production's node assembly exactly" by hand. Nobody added the new setter
   there, so every `SimCluster` test ran with the mechanisms off while
   production runs with them on. Nothing failed: off is the old behaviour.
2. Even with the mechanisms on, `SimCluster::metric` reads the node's
   `node_metrics` sink (what `/admin/metrics` aggregates), while a hosted
   `RaftKvNode` and the governor record into `SimEnv::metrics()`, a different
   per-node sink (documented next to `node_metrics`, "harmless today only
   because nothing reads it"). The new reader was the first to need it.

**Lessons.**

- A hand-maintained mirror of a production assembly ("exactly as production
  does") is a second call site. When a change adds a setter to the production
  assembly, grep for the mirror (`build_reconciler`, `SimNode`, test rigs) in
  the same change; otherwise the cluster tier silently tests a configuration
  production no longer runs.
- A metric that reads zero in a new harness is a wiring question before it is
  a result. Add one sanity counter that must be non-zero when the mechanism is
  engaged (here `campaign_admitted_total`) and look at it before trusting the
  others.
- In `SimCluster`, read per-group and per-mechanism counters with
  `SimCluster::env_metric` (the `SimEnv::metrics()` sink); `SimCluster::metric`
  only sees what the node-level handle records.
