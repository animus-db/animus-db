# Data volume filling up

Alert entry point (rules in `deploy/observability/animus-alerts.yml`, R-01(f)). Conventions (`<admin-addr>`, the control leader, `GET /metrics` on the DynamoDB port) are in [README.md](README.md).

Alerts: `AnimusDataVolumeFillingUp` (under 15% free for 15m),
`AnimusDataVolumeAlmostFull` (under 5% free for 5m), on PVCs named `data-*`.

Act on the first alert; the second means the node may already be failing.
Behaviour of a node that actually runs out of space is untested and may leave a
live but wedged process: read [disk-full.md](disk-full.md) now. Immediate
checks: which node and what is using the space (`GET /admin/storage/lsm`,
`/admin/storage/wal` per tablet, plus `<dir>/segments` and `<dir>/backups`);
whether a compaction or snapshot is in flight; whether the sealing/backup store
is local (`dir:`/`fs:` or the default `cluster` store keeps objects under the
node's own directory). Remedies are in [disk-full.md](disk-full.md): grow the
volume, or move load off the node (drain: [node-decommission.md](node-decommission.md),
or replace it with a bigger one: [node-replace.md](node-replace.md)).

## Maturity

Derived from the alert expression and the metric documentation in `crates/animus-env/src/metrics.rs`; not exercised against a real incident.
