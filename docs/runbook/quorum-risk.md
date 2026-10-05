# Quorum tolerance exhausted

Alert entry point (rules in `deploy/observability/animus-alerts.yml`, R-01(f)). Conventions (`<admin-addr>`, the control leader, `GET /metrics` on the DynamoDB port) are in [README.md](README.md).

Alert: `AnimusQuorumToleranceExhausted` fires when
`count(up == 0) >= floor((count(up) - 1) / 2)` for 2 minutes, i.e. the number
of unscrapable nodes has reached what a cluster of that size can lose while
every node is a control voter. It is deliberately conservative (data-only nodes
inflate the count). It means one more failure may cost the control plane, or
a tablet, its majority.

First checks:

1. Which nodes are down? `up{job="animusd"} == 0`, then [node-down.md](node-down.md) for each.
2. Is there still a control leader? `curl -s http://<admin-addr>/admin/health` (200 means yes) and `/admin/raft`.
   If not, go to [control-plane-quorum-loss.md](control-plane-quorum-loss.md).
3. Which tablets have lost redundancy? [tablet-unavailable.md](tablet-unavailable.md): the dashboard shows `under-replicated` / `quorum-lost` per tablet.

Do not start a rolling restart, a scale-down or any membership change while this
alert is firing (the control-voter removal guard only counts reachable voters
at the instant you ask). Restore the down nodes first.

## Maturity

Derived from the alert expression and the metric documentation in `crates/animus-env/src/metrics.rs`; not exercised against a real incident.
