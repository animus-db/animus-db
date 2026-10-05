# Tablet engine recovery

Alert entry point (rules in `deploy/observability/animus-alerts.yml`, R-01(f)). Conventions (`<admin-addr>`, the control leader, `GET /metrics` on the DynamoDB port) are in [README.md](README.md).

Alerts: `AnimusEngineOpenFailed` (`cp_engine_open_failed` increased in 15m),
`AnimusEngineRebuildFailed` (`cp_engine_rebuild_failed` increased in 15m),
`AnimusReplicaNeedsSnapshotRepeated` (`cp_engine_needs_snapshot` increased
more than 3 times in 1h).

Meaning (from the metric documentation): a tablet's storage engine failed to
open (corrupt or missing files); the reconciler destroys it and reopens a
fresh one, then the replica catches up from its peers by Raft log or
`InstallSnapshot` (`cp_engine_rebuilt` counts successes). Open-failed alone is
recovery in progress. **Rebuild-failed means the re-open after the destroy also
failed: the node's disk itself is unhealthy**, not one tablet's files, and
the reconciler keeps retrying.

First checks on the named instance: free space and I/O errors
([disk-full.md](disk-full.md)); the node log for the tablet id; `GET
/admin/raftkv` for the tablet's `commit_index` vs `engine_applied_index`.
Make sure the other replicas of that tablet are healthy before touching the node
([tablet-unavailable.md](tablet-unavailable.md)). If the disk is bad, replace
the node ([node-replace.md](node-replace.md)). Repeated needs-snapshot with
no disk fault points at repeated engine loss: capture logs and escalate; there
is no further tool.

## Maturity

Derived from the alert expression and the metric documentation in `crates/animus-env/src/metrics.rs`; not exercised against a real incident.
