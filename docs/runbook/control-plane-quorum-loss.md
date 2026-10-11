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

## A majority of voters is permanently gone: force a new configuration

**Operator-managed clusters (issue #1277).** The operator re-adds any control
ordinal in `0..controlNodes` missing from the live voter set on every reconcile,
which would undo the removals and wipes above. Before step 1, pause it:
`kubectl annotate animuscluster NAME animusdb.io/pause-control-voter-reconcile=true`
(status shows a `ControlVoterReconcilePaused` condition; children are still
applied). When the group is regrown as you want (or you want the operator to do
the regrowth), remove it: `kubectl annotate animuscluster NAME
animusdb.io/pause-control-voter-reconcile-`.

When a majority of control voters cannot come back (disks destroyed), no in-band
change can commit: `control-remove --force` and the tablet-level `reconfigure` both
need a majority of the old configuration. The supported way out is the **offline,
data-losing** `animusd recover-control` (ADR 0077): it rewrites one surviving
voter's own WAL so that it is the sole control voter. Use it only after the
"recoverable" sections above are ruled out.

What it keeps and loses. It keeps everything in the survivor's WAL and system
keyspace, **including entries it holds but never saw committed** (they become
committed). It loses any write the old group acknowledged that the survivor never
received. The tool cannot tell which those are.

1. **Stop client traffic and every control process.** Keep all disks. Copy every
   surviving control voter's data directory aside.
2. **Pick the survivor with the most complete log.** For each candidate, run the
   tool without the ack flag (it prints the plan and writes nothing) and compare
   "last index"; prefer the highest.
   ```sh
   animusd recover-control --config cluster.json --node <I> [--dir <DIR>] [--encryption-key <PATH>]
   ```
3. **Wipe the data directory of every other old control voter** (not just stop
   them). A voter that restarts on its old disk keeps state the recovered group
   does not have. The term jump and a removal notice stop it disrupting the
   survivor, but only a wiped node may be re-admitted.
4. **Run it for real on the chosen survivor** (process stopped; it refuses if a
   node is bound to the node's internal address, if the node is not a voter in its
   own WAL, or if the WAL is missing or empty):
   ```sh
   animusd recover-control --config cluster.json --node <I> --acknowledge-data-loss
   ```
   It backs the WAL up as `internal/raft.wal.pre-force-new-config.<term>` first.
   Re-running is a no-op.
5. **Start the survivor.** It elects itself (a one-voter group) and serves
   `Metadata` as of its log. Check `/admin/control/members` shows one voter and
   `/admin/health` is 200.
6. **Regrow the group** one voter at a time with `control-add`
   ([node-replace.md](node-replace.md) B) using the wiped nodes or new ones. Never
   start a wiped old voter before the survivor is serving.
7. **Reconcile `Metadata`.** It still lists the dead nodes; the failure detector
   marks them down and repair re-replicates tablets from surviving data nodes.
   Decommission nodes that are never coming back ([node-decommission.md](node-decommission.md)).
   Backups in the catalog survive in the survivor's `Metadata`; those taken in
   the lost tail are not catalogued.

Deployed under the Kubernetes operator, scale the `StatefulSet` to zero (or
otherwise stop the pods), run the tool in a debug pod mounting the survivor's
volume, and delete the other control voters' PVCs before scaling back up.

The data-plane equivalent for a tablet group that lost its majority is not built
(ADR 0077 phase 2); restore from backup/PITR.

If **no** control voter's directory survives, there is nothing to recover from:
rebuild a new cluster and load data from your exports with `ImportTable`
(`animus admin import-create`, or `POST /admin/data/dynamo`). The backup catalog
lives in the lost `Metadata`, so keep recurring exports to an object store as a
disaster-recovery copy.

## Prevention

- At least 3 control voters on separate failure domains; an odd number.
- Durable storage for voters. Never ephemeral: the operator rejects `storage.ephemeral`
  with more than one voter, because a wiped existing voter is permanently refused
  (issue #667) and enough of them cost the group its quorum for good.
- Do not run control-group changes during an outage of another voter
  ([node-decommission.md](node-decommission.md), [quorum-risk.md](quorum-risk.md)).
- The operator's PodDisruptionBudget blocks voluntary evictions that would cost quorum.

## Maturity

Diagnosis endpoints and the refusals were checked in code, and the "single
voter cannot be removed" refusal was run on a dev cluster. The behaviour during
an actual loss, including the Kubernetes readiness effect, was not drilled. The
rebuild path is untested. `recover-control` is proven in simulation (the ADR 0077 corpus) and by unit tests of its refusals; it has not been drilled on a real cluster.
