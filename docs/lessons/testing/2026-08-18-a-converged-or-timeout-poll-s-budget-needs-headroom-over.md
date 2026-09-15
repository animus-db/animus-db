# A converged-or-timeout poll's budget needs headroom over the worst-case duration of a SINGLE retry attempt when that attempt rides a network-hop timeout, not just headroom over healthy wall-clock

**A converged-or-timeout poll's budget needs headroom over the worst-case
duration of a SINGLE retry attempt when that attempt rides a network-hop
timeout, not just headroom over healthy wall-clock** (issue #281).
`data_only.rs`'s `schema_ddl_via_a_data_node_relays_and_commits` retried
`ProposeSchema` every 100ms inside a 20s poll — but a freshly-started
data-only node has no leader hint yet, so a single attempt can
legitimately fall all the way to broadcast and stall for the full 10s
`CLIENT_TIMEOUT` before the next iteration even starts. A 20s budget is
barely 2x one such worst-case attempt, not a real margin, and a starved
CI runner routinely needs more than one. Same runner-aware-budget idiom
as `split_cluster.rs`'s 30s→90s split-completion polls and
`backfill_seeder.rs`'s 60s→180s `CONVERGE_BUDGET` (both issue #278 item
9): size the multiplier against the attempt's own worst-case bound, not
against how long the property takes to converge when nothing is
contended. (`animusd/tests/data_only.rs`.)
