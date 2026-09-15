# A level-gauge `Metric` sampled across every owner sharing ONE `MetricsHandle` sink cannot be maintained by per-owner increment/decrement — it needs a single periodic re-count.

**A level-gauge `Metric` sampled across every owner sharing ONE
`MetricsHandle` sink cannot be maintained by per-owner increment/decrement
— it needs a single periodic re-count.** Adding `Metric::CpGroupsQuiesced`
(ADR 0044 phase-1 PR7, "how many of this node's hosted CP groups are
quiesced right now"): every tablet group on a node shares the *same*
`MetricsHandle` (one per-node env sink, ADR 0026), so a naive "each
group's own consensus loop increments on quiesce, decrements on wake"
would have every group blindly mutating one shared counter with no
coordination — correct only if every transition from every group is
captured exactly once, which is unverifiable from any single group's own
vantage point. Counters that record a *transition* (`CpQuiesces`/
`CpUnquiesces`) are fine as per-owner increments (each transition really
is independent and additive); a gauge that claims to reflect *current
aggregate state across owners* is not, and needs one periodic sampler
with a view across every owner (here, `metrics_sample_loop` walking
`ClusterEdgeState::hosted_groups()` and calling `MetricsHandle::set`
once) rather than N independent mutators each guessing at the whole.
**General rule**: before wiring a level gauge via per-event increment/
decrement, check whether every event source shares one sink — if so, a
single periodic re-aggregation is both simpler and the only version
that's actually correct.
