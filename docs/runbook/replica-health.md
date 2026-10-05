# Replica health

Alert entry point (rules in `deploy/observability/animus-alerts.yml`, R-01(f)). Conventions (`<admin-addr>`, the control leader, `GET /metrics` on the DynamoDB port) are in [README.md](README.md).

Alerts: `AnimusReplicaRefusedAsVoter`
(`cp_groups_refused_as_voter > 0` for 10m on an instance),
`AnimusReconcilerStopTimeout` (`increase(cp_reconciler_stop_timeout[15m]) > 0`).

**Refused as voter.** A tablet replica on that node was started from empty
persisted state and the boot-time cluster check (ADR 0009 amendment, issue #667)
decided it might be a wiped former voter: it replicates but never votes or
campaigns, so its group runs one voter short. `GET /admin/raftkv` on the node
shows `refused_as_voter: true` for the tablet(s). Usual cause: a data volume was
wiped or replaced without the node being removed and re-added. Fix: treat the
node as replaced ([node-replace.md](node-replace.md)). The metric is a level and
only clears when the replica is re-admitted through the learner path or removed.

**Reconciler stop timeout.** The host reconciler gave up waiting for one
tablet's driver to halt (a stuck or very slow engine). Other tablets are
unaffected. Check that node's disk ([disk-full.md](disk-full.md)) and logs for
`panicked` or storage errors; restart the node if it persists
([node-down.md](node-down.md)).

## Maturity

Derived from the alert expression and the metric documentation in `crates/animus-env/src/metrics.rs`; not exercised against a real incident.
