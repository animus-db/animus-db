# 5xx errors and abandoned requests

Alert entry point (rules in `deploy/observability/animus-alerts.yml`, R-01(f)). Conventions (`<admin-addr>`, the control leader, `GET /metrics` on the DynamoDB port) are in [README.md](README.md).

Alerts: `AnimusHighServerErrorRate`, `AnimusErrorBudgetFastBurn`,
`AnimusErrorBudgetSlowBurn`, `AnimusClientRequestsAbandoned`
(`rate(client_requests_abandoned[5m]) > 1`).

What produces 5xx on the DynamoDB port (`animusd/src/dynamo.rs`):
`ServiceUnavailable` (503): the server's own retry budget ran out on a
**transient** refusal (a split cutover freeze, a leadership chase); every AWS
SDK retries it. `InternalServerError` (500): an internal failure such as no
quorum for the tablet or corrupt stored bytes. Throttling is a **400**, not a
5xx ([throttling.md](throttling.md)).

First checks: which operation and tablet? A burst right after a split or node
restart is expected. Sustained: [tablet-leaderless.md](tablet-leaderless.md) and
[tablet-unavailable.md](tablet-unavailable.md); also
[control-plane-leader.md](control-plane-leader.md) (DDL needs the control plane:
`CreateTable` returns a 500 "did not commit to the control plane in time" when
no leader is reachable). `client_requests_abandoned` means clients disconnect
while the server is still working (client timeout shorter than server latency)
and amplifies load under retry; see [overload-and-throttling.md](overload-and-throttling.md).

## Maturity

Derived from the alert expression and the metric documentation in `crates/animus-env/src/metrics.rs`; not exercised against a real incident.
