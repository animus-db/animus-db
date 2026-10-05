# Cross-tablet transaction recovery

Alert entry point (rules in `deploy/observability/animus-alerts.yml`, R-01(f)). Conventions (`<admin-addr>`, the control leader, `GET /metrics` on the DynamoDB port) are in [README.md](README.md).

Alert: `AnimusTxnRecoveryStuck`
(`cp_txn_recovery_stuck_inconclusive` or `cp_txn_unresolved_decided_stuck`
increased in 15m).

Meaning (metric documentation): the background resolver of a cross-tablet
(2PC) transaction could not reach a decision, or has a decided transaction it
cannot resolve (for example its record's tablet retired). **Correctness is
unaffected**: undecided intents stay `Pending` and are never wrongly decided;
a straggling intent is resolved on demand when any reader hits it. The signal
is reduced background promptness, and reads or writes touching those keys may
be slower or briefly refused.

First checks: is a tablet involved unavailable? [tablet-unavailable.md](tablet-unavailable.md)
(`GET /admin/txns` lists per-group transaction-tracker state). If every tablet is
healthy and the counter keeps rising, capture `GET /admin/txns` from all
nodes and the logs and escalate; no operator action resolves a transaction.

## Maturity

Derived from the alert expression and the metric documentation in `crates/animus-env/src/metrics.rs`; not exercised against a real incident.
