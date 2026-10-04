# Backup, restore and point-in-time recovery drill

Mechanism: ADR 0059 (backup/restore/PITR), ADR 0068 (S3 export/import), ADR 0069
(encryption). Conventions are in [README.md](README.md). The admin proxy used
below, `POST /admin/data/dynamo {"op":..., "payload":...}`, runs the DynamoDB
operations without SigV4 (the admin port is trusted-network only); the same
operations are available signed on the DynamoDB port, for example
`aws dynamodb create-backup --endpoint-url http://<dyn>`.

> The `animus admin backup-create|restore|pitr-enable|...` subcommands wrap the
> same calls but currently fail against the admin port (see
> [node-replace.md](node-replace.md)); use `curl`.

## Decide where backups live (`--backup-store`)

| Value | Meaning | Survives loss of the whole cluster? |
|---|---|---|
| (omitted) or `cluster` | objects replicated across this cluster's own nodes, under each node's `<dir>/backups` | **No.** Protects against operator and application mistakes only. |
| `fs:/absolute/path` | one directory (mount a replicated or separately backed-up volume at the same path on every node) | only if that storage survives |
| `s3://<bucket>[/<prefix>]?endpoint=<https://host[:port]>[&region=<r>][&path_style=true]` | an S3-compatible bucket; TLS is required unless `insecure_http=true` on a loopback endpoint, or `--allow-insecure-s3` is also given | **Yes**, the real disaster-recovery configuration |

S3 credentials: `--s3-credentials PATH` (a JSON file
`{"access_key_id":"...","secret_access_key_file":"/path"}` or `"secret_access_key_env":"VAR"`)
or the environment variables `ANIMUS_S3_ACCESS_KEY_ID` and
`ANIMUS_S3_SECRET_ACCESS_KEY`. Only combined and control-capable invocations take
`--backup-store` (`--config/--node`, `join`, `control`, `data`, `--cluster`; not all
flags on every mode: see `animusd` usage). On Kubernetes use `spec.s3.backupStore`
(+ `spec.s3.credentialsSecretName`, `spec.s3.egressCidrs`) or `spec.backupStore:
"cluster"|"fs:<path under /var/lib/animus>"`; set at most one. Inspect:
`curl -s http://<admin>/admin/backup-store` (kind, location redacted, object count/bytes,
janitor phase). With `--encryption-key` the store is sealed under the cluster key
([encryption-key-rotation.md](encryption-key-rotation.md)). A backup remains readable by any
later post-baseline version (ADR 0073).

PITR retention is a fixed 35 days (not configurable). Base snapshots are taken every
6 hours. **PITR's change log is sealed on the same triggers as DynamoDB Streams: 4 MiB
or 4 hours per tablet by default** (`--stream-seal-bytes`, `--stream-seal-age`; also
`cluster_settings.stream_seal_bytes`/`stream_seal_age_secs`), so
`LatestRestorableDateTime` can lag real time by up to hours, not seconds (ADR 0059
says "apply/seal lag"). Observed on a dev cluster with defaults: it did not advance for
40 s of continuous writes. If you need a tight RPO, lower `--stream-seal-age` (this also
changes Streams sealing) and measure `LatestRestorableDateTime` yourself.

## The drill (all steps run by the author on a local dev cluster, 2026-10-04)

Pick any node's admin address `A`. Replace `T`, names and ARNs.

```sh
dyn() { curl -s -X POST http://$A/admin/data/dynamo -d "$1"; echo; }

# 0. Create a table and some data (or use a staging copy of a real table).
dyn '{"op":"CreateTable","payload":{"TableName":"orders","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"}}'
dyn '{"op":"PutItem","payload":{"TableName":"orders","Item":{"id":{"S":"k1"},"v":{"S":"before"}}}}'

# 1. On-demand backup. Returns BackupArn, BackupStatus CREATING.
dyn '{"op":"CreateBackup","payload":{"TableName":"orders","BackupName":"drill1"}}'
# poll until BackupStatus is AVAILABLE
dyn '{"op":"DescribeBackup","payload":{"BackupArn":"<arn>"}}'
curl -s http://$A/admin/backups        # replicated catalog with per-tablet progress

# 2. Change the source after the backup.
dyn '{"op":"PutItem","payload":{"TableName":"orders","Item":{"id":{"S":"k4"},"v":{"S":"after-backup"}}}}'

# 3. Restore. Always to a NEW table name; fails if it exists. TTL and streams are not re-enabled.
dyn '{"op":"RestoreTableFromBackup","payload":{"TargetTableName":"orders_restored","BackupArn":"<arn>"}}'
# poll DescribeTable until TableStatus ACTIVE (CREATING for the whole restore)
dyn '{"op":"DescribeTable","payload":{"TableName":"orders_restored"}}'
curl -s http://$A/admin/restores       # restore catalog
dyn '{"op":"Scan","payload":{"TableName":"orders_restored"}}'   # must show k1..k3 and NOT k4

# 4. PITR.
dyn '{"op":"UpdateContinuousBackups","payload":{"TableName":"orders","PointInTimeRecoverySpecification":{"PointInTimeRecoveryEnabled":true}}}'
dyn '{"op":"DescribeContinuousBackups","payload":{"TableName":"orders"}}'   # Earliest/LatestRestorableDateTime
dyn '{"op":"RestoreTableToPointInTime","payload":{"SourceTableName":"orders","TargetTableName":"orders_pitr","UseLatestRestorableTime":true}}'
# or "RestoreDateTime": <epoch seconds> within [Earliest, Latest]; outside it: InvalidRestoreTimeException
```

Observed results on 2026-10-04 (3 items, `animusd --cluster-control 1 --cluster-data 2
--ephemeral`, default stores): backup `AVAILABLE` on the first poll; restore `CREATING`
then `ACTIVE` within 2 s with exactly the 3 pre-backup items, the post-backup write absent
from the restored table and present in the source; PITR enable returned `ENABLED`;
restore with `UseLatestRestorableTime` returned the items written up to the (stale)
latest-restorable point and not a later write. **This was a tiny single-process dev
cluster on in-memory engines, not a real drill**: it proves the wire path and the
documentation, not durability, sizes, timing, S3 or a restore after node loss.

Cleanup: `{"op":"DeleteBackup","payload":{"BackupArn":"<arn>"}}`, `DeleteTable` for the
restored tables, `UpdateContinuousBackups` with `false` (disable then re-enable resets the
retention window). A backup outlives its source table and stays describable after a
`DeleteTable`; a dropped table stays PITR-restorable for the retention window.

## A real drill (E-3 requires one on real nodes)

Repeat on a staging cluster that has the production shape: real `--backup-store s3://...`
(or `fs:` on a mounted volume), TLS and encryption on if production has them, a table
with GSIs (rebuilt on restore, not copied), enough data for sizes and timings to mean
something. Record: backup duration and bytes, restore time, items and index counts equal to
the source at the backup point, `GET /admin/backup-store` object counts, and the PITR
`Latest` lag. Then rehearse the worst case: destroy the cluster, build a new one with
the same `--backup-store`, and see whether the restore is possible. **The restore path reads
the backup catalog from replicated `Metadata`, which a new cluster does not have; no shipped
tool imports an existing store's manifests.** Expect this to fail and
treat `ExportTableToPointInTime` (ADR 0068, below) as the cross-cluster copy until
proven otherwise.

## S3 export and import (cross-cluster copy)

```sh
dyn '{"op":"ExportTableToPointInTime","payload":{"TableArn":"<table-arn>","S3Bucket":"<bucket>","S3Prefix":"<prefix>"}}'
dyn '{"op":"DescribeExport","payload":{"ExportArn":"<arn>"}}'
```

(CLI forms `export-create|export-describe|export-list|import-create|import-describe|import-list`
exist; see `animus` usage.) Node flags `--export-s3-endpoint` / `--export-s3-region` give the
connection parameters (config-file mode only). Export objects are plain DynamoDB JSON in a
customer bucket; no server-side encryption is applied by AnimusDB (configure it on the bucket).
`ImportTable` creates a new table; the CLI wrapper builds only a minimal single-hash-key
(plus optional sort key) definition with no GSI/throughput flags. These two operations
were not run in this drill.

## Maturity

The backup, restore and PITR corpora (`ANIMUS_BACKUP_SEEDS`, `ANIMUS_PITR_SEEDS`,
`ANIMUS_EXPORT_IMPORT_SEEDS`) run in simulation. The `curl` sequence above was executed once
on a dev cluster as described. Not executed: S3 backends, real disks, a restore into a fresh
cluster, restore under encryption, timing at size.
