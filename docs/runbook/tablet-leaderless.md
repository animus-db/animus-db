# Tablet group leaderless / bouncing

Alert entry point (rules in `deploy/observability/animus-alerts.yml`, R-01(f)). Conventions (`<admin-addr>`, the control leader, `GET /metrics` on the DynamoDB port) are in [README.md](README.md).

Alerts: `AnimusTabletGroupLeaderless`
(`sum(rate(cp_route_fanout_exhausted[5m])) > 0` for 3m: a request found no usable
leader for some tablet after asking several replicas),
`AnimusWritesBouncingOffNonLeaders` (more than 20% of proposals refused as
not-leader for 10m), `AnimusLinearizableReadsTimingOut` (more than 5% of
`ConsistentRead` barriers failing for 5m).

Meaning: some tablet Raft group has no leader, is electing repeatedly, or its
leader cannot reach a quorum. Clients see `ServiceUnavailable` (503) or
`InternalServerError` (500) for keys in that tablet, and eventually-consistent
reads may still work.

First checks: follow [tablet-unavailable.md](tablet-unavailable.md) (find the
tablet on the dashboard Tablets tab or in `GET /admin/status` + `/admin/raftkv`,
check whether a majority of its replicas' nodes are `Active`). If a node is down,
[node-down.md](node-down.md). Note that a recent split or rebalance also
produces a short burst of these signals.

## Maturity

Derived from the alert expression and the metric documentation in `crates/animus-env/src/metrics.rs`; not exercised against a real incident.
