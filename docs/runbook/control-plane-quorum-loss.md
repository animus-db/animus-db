# Control-plane quorum loss

The control plane is a small Raft group (the control voters: every combined
node with ordinal below `controlNodes`, or the `animusd control` nodes) that
owns `Metadata`: membership, the tablet map and replica sets, the schema
catalog, the backup catalog, credentials. Losing a majority of its voters
stops all metadata change. Conventions are in [README.md](README.md).

## Is there really no quorum?

```sh
curl -s http://<admin>/admin/control/members          # "voters": the configured voter ids (any node answers)
curl -s http://<admin>/admin/raft                     # per node: role, term, leader, commit_index, peers believes_alive
curl -s -o /dev/null -w '%{http_code}\n' http://<admin>/admin/health    # 503 on every node = no recent leader
curl -s http://<dyn>/metrics | grep -E '^control_(is_leader|elections_started|failure_detector_down) '
```

Quorum is `floor(voters/2) + 1` of the **configured** voters (not of those
currently up). With 3 voters you need 2, with 5 you need 3. If a majority of
voters is reachable but no leader emerges, suspect the network or TLS
([network.md](network.md), [cert-rotation.md](cert-rotation.md)) or a wiped voter
refusing to vote ([node-replace.md](node-replace.md) B) before concluding the
quorum is gone.

## What keeps working, what stops

Verified from code and ADR 0037 (not by an outage drill):

- **Keeps working**: reads and writes to existing tablets whose own Raft groups
  still have a majority; they do not need the control leader.
- **Stops**: `CreateTable`/`DeleteTable`/`UpdateTable`/index and stream DDL, splits,
  failure detection and replica repair (the leader drives them), drain, remove,
  control-group membership changes, credential changes, new backups and restores.
  Anything needing a metadata commit fails ("did not commit to the control plane
  in time (no leader reachable?)", HTTP 500). Nodes serve their last mirrored
  `Metadata` for routing.
- **Kubernetes caveat (by code reading, not drilled).** The pod readiness probe is
  `/admin/health`, which is 503 when no control leader was heard from for three
  election timeouts. The client-facing `<name>-dynamo` Service does not publish
  not-ready addresses (only the headless internal Service does). So a control quorum
  loss makes every pod `NotReady` and removes all endpoints from the client Service
  even though tablet groups could still serve. Port-forward or address a pod
  directly to reach the data plane meanwhile.

## Recoverable: voters come back with their data

If enough voters can be restarted with their data directories intact, nothing else
is needed: the control Raft recovers from its WAL and a leader is elected.
Restart them ([node-down.md](node-down.md)), same `--dir`, same identity. Do
this before anything else; it is the only fully supported recovery.

## Recoverable by design: a minority is lost, a majority is alive

Replace the lost voters one at a time ([node-replace.md](node-replace.md) B).
Never remove a second voter while a third is suspected dead.

## Not recoverable with any shipped tool: a majority of voters is permanently gone

There is **no unsafe-recovery tool**. Checked by searching the source, ADRs and
CLI for any forced membership reset, "force new cluster", unsafe or disaster
recovery path: none exists.

- `control-remove --force` (`POST /admin/control/member/remove {"force":true}`) only
  bypasses the reachability guard. It still proposes a configuration change through
  the Raft log, which needs a leader and a majority of the *old* configuration, so
  it cannot help without quorum. It never removes the last voter either.
- The tablet-level `reconfigure` (`POST /admin/raftkv/reconfigure`) is leader-only
  per tablet and likewise needs a majority of that tablet's group; it changes data
  plane replica sets, not the control plane.
- Nothing rebuilds `Metadata` from the data nodes. Hand-editing WALs or snapshots
  is not a procedure this project supports; do not attempt it on the only copy.

Consequences, stated plainly: the cluster can no longer change metadata, ever. The
data plane may keep serving existing tablets for as long as their groups keep a
majority, but with no failure detection or repair, any further node loss
degrades tablets permanently. The data is still on the data nodes' disks and
in your backups. The honest recovery is **rebuild**:

1. Stop client traffic. Keep all disks. If any control voter's directory survives,
   copy it aside first.
2. Create a new cluster (new `gen-config`/new `AnimusCluster`).
3. Restore data. `RestoreTableFromBackup` needs the backup *catalog*, which lives in
   the lost `Metadata`; the objects in an `fs:`/`s3://` backup store are not
   re-importable into a new cluster by any shipped tool. **Verify this on a
   throwaway cluster before relying on it** ([backup-restore-pitr.md](backup-restore-pitr.md)).
   Data exported with ExportTableToPointInTime (ADR 0068, plain DynamoDB-JSON in a
   customer bucket) can be loaded with `ImportTable`
   (`animus admin import-create`, or `POST /admin/data/dynamo`); that is the one path that
   does not depend on the lost catalog.

So: **schedule recurring exports to an object store as a disaster-recovery copy**
in addition to backups, until a recovery tool exists.

## Prevention

- At least 3 control voters on separate failure domains; an odd number.
- Durable storage for voters. Never ephemeral: the operator rejects `storage.ephemeral`
  with more than one voter, because a wiped existing voter is permanently refused
  (issue #667) and enough of them cost the group its quorum for good.
- Do not run control-group changes during an outage of another voter
  ([node-decommission.md](node-decommission.md), [quorum-risk.md](quorum-risk.md)).
- The operator's PodDisruptionBudget blocks voluntary evictions that would cost quorum.

## Recommendation (for the maintainers)

File a follow-up to design an explicit, loud, offline "force new configuration"
recovery tool (the etcd `--force-new-cluster` / TiKV `unsafe-recover` shape): started
on a surviving voter's data directory with an explicit acknowledgement flag, it
rewrites the persisted Raft configuration to that voter alone (term bumped, never
auto-run), after which `control-add` regrows the group and a re-registration
pass reconciles tablet replicas. It needs its own ADR (it can discard committed
metadata) and a simulation test. Without it, beta has no answer to permanent loss of
two of three voters other than rebuild-from-export.

## Maturity

Diagnosis endpoints and the refusals were checked in code, and the "single
voter cannot be removed" refusal was run on a dev cluster. The behaviour during
an actual loss, including the Kubernetes readiness effect, was not drilled. The
rebuild path is untested.
