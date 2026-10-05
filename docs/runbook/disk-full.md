# Disk full and disk sizing

Conventions are in [README.md](README.md). Alert entry point:
[disk-space.md](disk-space.md).

## Status: handled in simulation, not yet proven on a real full disk

Disk-full (`ENOSPC`, `ErrorKind::StorageFull`) on a WAL write is handled
(roadmap R-01(d), issue #1185; design in `docs/resource-bounds.md` section 3). It is
proven by a seeded `SimEnv` corpus (`ANIMUS_DISK_FULL_SEEDS`), **not** yet on a real
size-limited filesystem. What a node does:

- A group (a tablet's Raft group, or the control group) whose WAL write hits ENOSPC
  marks that WAL **suspect**: it is never appended to or `fsync`ed again. Nothing the
  failed round covered is applied or acknowledged.
- Writes to that group are **refused** with HTTP 503 `ServiceUnavailable`, message
  `StorageFull: ...; retry`, and `overload_storage_full` increments on `/metrics`.
  Reads of already-applied state keep working. The process does not die.
- `GET /admin/health` shows `storage_full: true` (with `storage_full_control` and the
  list `storage_full_tablets`); its status code is unchanged on purpose, so alert on
  the field or on `overload_storage_full`, not on readiness. `/admin/raftkv` shows
  `storage_full` per group.
- The node **probes for free space by itself** (backoff 50 ms to 2 s) and, once a
  fresh WAL file can be written, rewrites the WAL from the in-memory log and resumes
  with no restart. Space returning is therefore enough; you do not need to restart.

Known gaps (file or check issues before relying on them):

- **The LSM engine is not covered.** A flush, compaction or apply-time engine write
  that hits ENOSPC is still a hard `panic!` inside a background task; `ProdEnv` catches
  it per task, logs it and keeps the process up, with that tablet's apply task dead
  (`/admin/live` stays 200 and the panic count is not exported). If you see `panicked`
  or `No space left` in the log without a `StorageFull` refusal, **restart the node**
  after freeing space.
- **No leader step-down.** A leader whose own disk is full keeps leadership and
  refuses writes, so its tablets are unavailable for writes until space returns or
  you move leadership/load away.
- The LSM engine also needs free space to make progress (write backpressure fails
  loudly after `BACKPRESSURE_MAX_POLLS` rather than queueing without bound).

## If a disk is full or nearly full now

1. Confirm: `df -h` on the data volume (Kubernetes: `kubectl exec <pod> -- df -h
   /var/lib/animus`), `storage_full` on `/admin/health`, node log for `StorageFull`,
   `panicked`, `No space left`, `sync failed`.
2. Do not delete files under `--dir` by hand. WAL segments, SSTables and the manifest
   are one consistent set; removing any of them corrupts the replica.
3. Free space the safe way, in this order:
   - Grow the volume (Kubernetes: PVC expansion if the StorageClass allows it, then
     restart the pod for the filesystem to be seen; bare metal: extend the filesystem).
     This is the only fix that needs no data movement.
   - Move load away: [node-decommission.md](node-decommission.md) (drain), which
     re-homes tablets onto other nodes. Draining itself writes to the *receiving* nodes
     and needs temporary extra space for catch-up and `InstallSnapshot`.
   - Remove data you own: drop a table you no longer need
     (`aws dynamodb delete-table`, or `POST /admin/data/dynamo
     {"op":"DeleteTable",...}`); the convergent GC reclaims it on each node (ADR 0024).
   - Delete old backups (`DeleteBackup`) if the backup store is the default `cluster`
     store or a local `fs:` path on that volume ([backup-restore-pitr.md](backup-restore-pitr.md)).
4. Once space is back, a WAL-suspect group recovers on its own (watch `storage_full`
   clear on `/admin/health`). Restart the node only if the log shows an LSM-engine
   `panicked`/`No space left` with no `StorageFull` refusal, then verify with
   [node-down.md](node-down.md) step 5.
5. If the disk was full while the node was a leader, check the other replicas of its
   tablets did not diverge: all `commit_index` values converge on `/admin/raftkv`.

## Where the space goes (sizing guidance from the code)

Everything a node owns is under its `--dir`: `internal/` (the control-plane
WAL and system keyspace, the per-tablet LSM engines `db-t<N>-*`, per-tablet WAL, and the
shared WAL), `segments/` (this node's share of DynamoDB Streams objects with the
default store), `backups/` (this node's share of backup/PITR objects with the default
`cluster` store; with `fs:`/`dir:`/`s3://` stores that lives elsewhere).

- **Replication.** Every tablet is stored RF times (RF = `min(nodes, 3)`), once per
  replica node. Raw data size x RF is the floor.
- **LSM write/space amplification.** The engine is leveled: a memtable flushes to L0,
  4 L0 tables trigger an L0 to L1 compaction, level n holds up to `4 x 4^(n-1)` tables,
  target SSTable ~2 MiB (`DEFAULT_*` in `animus-storage/src/lsm.rs`). A compaction writes
  its outputs **before** removing its inputs (the inputs stay named by the manifest until
  the swap commits), so a node needs transient headroom of at least the size of the data
  being compacted. `animusd` opens every engine with these defaults (it has no LSM tuning
  flag; the defaults are documented in source as "sized for tests"), notably a **64 KiB
  memtable flush threshold**, which makes flushes and compactions frequent; the
  effect on write amplification at scale is unmeasured (C-17).
- **History is retained.** Every `(key, version)` record is preserved; tombstones and
  overwritten versions are reclaimed only once they are older than the grace window
  (default `1 << 20` versions, "sized generously so GC is a no-op under ordinary use").
  Deletes and overwrites may therefore not return disk space promptly. How long that is
  in wall time depends on the version scale and has not been measured; do not assume
  deleting data frees space soon.
- **WAL.** Each tablet has a Raft log plus the engine's own WAL. The control-plane and
  shared WALs are rewritten to stay near the live tail (snapshot then atomic replace,
  `docs/wal.md`); the rewrite stages a copy, so budget transient space equal to the
  live WAL size. Segments are 64 KiB by default in the engine.
- **Snapshots.** A lagging or new replica receives an `InstallSnapshot` streamed from
  the leader's engine; the receiver needs space for the incoming image in addition to
  its existing data until it is installed. A rebalance or replacement moves whole tablets
  this way.
- **Streams, PITR and the change log.** A stream or PITR-enabled table keeps its
  change log on the tablet until it is sealed to the segment store (default seal at
  4 MiB or 4 h, `--stream-seal-bytes`/`--stream-seal-age`) and retained (streams 24 h by
  default, `--stream-retention`; PITR 35 days, fixed). With the default `cluster` stores
  those objects are replicated across nodes inside `segments/` and `backups/`.
- **Backups** with the default `cluster` store live on the same nodes' disks as the
  data they protect; see [backup-restore-pitr.md](backup-restore-pitr.md).
- **Encryption** adds a small per-frame overhead (ADR 0069); not measured.

Rule of thumb for planning, until C-17 measures it: provision each node's volume for
`(raw data / nodes) x RF` (steady state), then at least 2x that so compaction, a
rebalance and an `InstallSnapshot` never run the disk to zero, and alert at 70-80%,
not at the 85% and 95% the shipped alerts use as a last line. This is a conservative
engineering estimate, **not a measured number**.

## Maturity

The WAL path is sim-tested only; nothing here has been reproduced on a real full disk. The sizing text is derived from constants and comments in
`animus-storage`, `animus-cp-data` and `animusd`; no measurement backs the 2x rule.
