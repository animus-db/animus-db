# Throttling

Alert entry point (rules in `deploy/observability/animus-alerts.yml`, R-01(f)). Conventions (`<admin-addr>`, the control leader, `GET /metrics` on the DynamoDB port) are in [README.md](README.md).

Alert: `AnimusThrottlingHigh` (more than 5% of requests throttled for 15m).

`ProvisionedThroughputExceededException` (HTTP 400) is the per-table token
bucket of ADR 0065, working as designed: the table's provisioned capacity (or
the cluster default) is exhausted. First checks: `throttled_reads` /
`throttled_writes` on `GET /metrics`, and the per-tablet `throttle` array in
`GET /admin/metrics` (tokens left, rates, counts) to see whether one hot tablet
or the whole table is short. Then follow [overload-and-throttling.md](overload-and-throttling.md):
raise the table's provisioned throughput, spread the key space, or fix the
client retry behaviour.

## Maturity

Derived from the alert expression and the metric documentation in `crates/animus-env/src/metrics.rs`; not exercised against a real incident.
