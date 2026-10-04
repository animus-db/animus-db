# Disk full and disk sizing

Conventions are in [README.md](README.md). Alert entry point:
[disk-space.md](disk-space.md).

## Honest status: real-node behaviour on a full disk is UNTESTED and UNDEFINED

Disk-full (`ENOSPC`, `ErrorKind::StorageFull`) is injected only in simulation
(`animus-sim` `DiskConfig`). No code in the node, storage or data-plane crates
branches on it: there is no read-only mode, no named "disk full" error, no
low-space guard that refuses writes early. Roadmap R-01(d) will define this
behaviour; until then, what follows is what the code would do, by reading it, and
none of it has been reproduced on a real full disk.

By code reading (`animus-cp-data/CLAUDE.md`, `ProdEnv::spawn_task`):

- A failed WAL append or sync on a live tablet group, or a failed engine write during
  apply, is treated as a durability fault. It is a hard `panic!` inside a background
  task, deliberately "crash-stop before ack": the write is not acknowledged, so no
  acked write is lost.
- But `ProdEnv` catches that panic per task, logs it at `error` level, counts it
  internally (the counter is not exported as a metric) and lets the process keep
  running. There is no process-level fail-stop. A node can therefore stay up with
  that tablet's driver or apply task dead: `/admin/live` stays 200, and `/admin/health`
  stays 200 while the control plane is fine. Clients see timeouts or 5xx for those
  tablets. A past occurrence under disk pressure is documented in `ProdEnv`'s own
  comments (issue #939: "wal group-commit sync failed").
- The LSM engine applies write backpressure when maintenance falls behind and fails
  loudly after a bounded wait (`BACKPRESSURE_MAX_POLLS`) rather than queueing
  without bound, but flush and compaction need free space to make progress.
- Recovery after space returns is the ordinary restart path (WAL replay, torn-tail
  repair); with the zombie behaviour above, **restart the node** rather than waiting
  for it to heal.

## If a disk is full or nearly full now

1. Confirm: `df -h` on the data volume (Kubernetes: `kubectl exec <pod> -- df -h
   /var/lib/animus`), node log for `panicked`, `No space left`, `sync failed`.
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
4. Restart the affected node and verify with [node-down.md](node-down.md) step 5.
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

Untested on a real full disk. The sizing text is derived from constants and comments in
`animus-storage`, `animus-cp-data` and `animusd`; no measurement backs the 2x rule.
