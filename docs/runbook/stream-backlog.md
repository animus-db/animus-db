# DynamoDB Streams / change-log backlog

Alert entry point (rules in `deploy/observability/animus-alerts.yml`, R-01(f)). Conventions (`<admin-addr>`, the control leader, `GET /metrics` on the DynamoDB port) are in [README.md](README.md).

Alerts: `AnimusStreamSealBacklog` (`max(stream_seal_backlog_ms) > 900000`),
`AnimusStreamSealFailures` (`increase(stream_seal_failures_total[15m]) > 3`),
`AnimusChangeLogTrimBlocked` (`max(change_log_trim_blocked) == 1` for 30m),
`AnimusStreamRepairBacklog` (`max(stream_repair_backlog) > 0` for 30m).

Streams (and PITR, which shares the seal triggers) seal the per-tablet change log
into objects in the segment store. Sealing triggers on size/age
(`--stream-seal-bytes`, default 4 MiB, `--stream-seal-age`, default 4 h), so a
backlog metric is only meaningful against those settings. Failures mean the
segment store is unreachable or rejecting writes: check `GET /admin/segment-store`
(store kind, object counts) and, for `s3://`, endpoint reachability and
credentials; for `fs:`/`dir:` the path's free space and permissions
([disk-full.md](disk-full.md)). `change_log_trim_blocked = 1` means a hot change
log cannot be trimmed (an unhealed store or a stream that never sealed) and
grows without bound. `stream_repair_backlog > 0` means segments have a replica
on a non-`Active` node; it should return to zero once the node returns or is
replaced ([node-down.md](node-down.md)). `GET /admin/gc` shows the segment
janitor's phase.

## Maturity

Derived from the alert expression and the metric documentation in `crates/animus-env/src/metrics.rs`; not exercised against a real incident.
