# Control-plane leader problems

Alert entry point (rules in `deploy/observability/animus-alerts.yml`, R-01(f)). Conventions (`<admin-addr>`, the control leader, `GET /metrics` on the DynamoDB port) are in [README.md](README.md).

Alerts: `AnimusControlPlaneLeaderless` (`sum(control_is_leader) == 0` for
1 minute), `AnimusControlPlaneMultipleLeaders` (`sum(control_is_leader) > 1`
for 5 minutes), `AnimusControlPlaneElectionChurn`
(`sum(increase(control_elections_started[10m])) > 3`).

**Leaderless.** No control voter believes it leads. Check each combined/control
node: `curl -s http://<admin-addr>/admin/raft` (`leader`, `term`, `role`) and
`/admin/health`. If a majority of control voters is down or partitioned this is
a quorum loss: [control-plane-quorum-loss.md](control-plane-quorum-loss.md). If a
majority is up, look for a network partition between them, a certificate or
handshake problem ([network.md](network.md), [cert-rotation.md](cert-rotation.md)),
or a wiped voter that refuses to vote (see [node-replace.md](node-replace.md)).
While leaderless, the data plane keeps serving existing tablets. `/admin/health`
goes 503 after three election timeouts, but the Kubernetes readiness probe
(`/admin/ready`, issue #1274) does not, so the client Service keeps its endpoints
(see [control-plane-quorum-loss.md](control-plane-quorum-loss.md)).

**Multiple leaders.** Each node reports its own belief. A short overlap right
after a leadership change is possible (an old leader has not yet seen the higher
term). Sustained, compare `term` on every `/admin/raft`: the highest term wins;
the stale one should step down on contact. A persistent split means the nodes
cannot reach each other: treat as [network.md](network.md).

**Election churn.** More than three elections in ten minutes: flapping
leadership from a slow or lossy network, an overloaded or I/O-starved
node (check [disk-full.md](disk-full.md)), or a node restarting in a loop
([node-down.md](node-down.md)). `animus admin control-transfer` (CLI is
currently broken, see README; curl: `POST /admin/control/transfer
{"to":"<node-id>"}` on the leader) moves leadership to a healthy voter.

## Maturity

Derived from the alert expression and the metric documentation in `crates/animus-env/src/metrics.rs`; not exercised against a real incident.
