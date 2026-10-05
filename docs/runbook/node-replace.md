# Replace a failed or retired node

Use after [node-down.md](node-down.md) decided the node is not coming back with
its data (disk lost, host gone, volume wiped), or to move a node to new hardware.
Mechanism: ADR 0030 (online growth), ADR 0032 (join, drain, remove), ADR 0037
(control-group membership), ADR 0009's 2026-09-15 amendment (wiped voter).
Conventions are in [README.md](README.md).

> **Use the `curl` forms.** On `main` at `e8c037da` every `animus admin ...`
> subcommand fails against a real admin port with `animus: client handshake
> with <addr> failed: TimedOut` (the CLI runs the client-protocol preamble before
> speaking HTTP; the admin listener does not answer it). Reproduced on a
> running `animusd --cluster-control 1 --cluster-data 2`. The HTTP endpoints
> themselves work. CLI forms are shown for the day this is fixed.

## Principles

- **Replace, do not resurrect.** A node whose persisted state is gone must not be
  restarted under its old identity into a quorum it belonged to: a wiped
  control voter refuses to vote (and a wiped tablet replica refuses as a voter,
  metric `cp_groups_refused_as_voter`), by design.
- **Keep majorities.** Do the control-plane step only while a control majority is
  alive. Never remove a second voter while a third is suspected dead: the removal
  guard only counts voters reachable at that instant, and a mistake can wedge
  all further membership changes forever (ADR 0037).
- Replace one node at a time; wait for convergence between steps.
- `remove` is refused while the member is `Active`/`Joining` or any tablet
  still references it. A `Down` node loses its replicas to the repair pass 5 s after
  being marked `Down`, but only onto `Active` nodes: in a small cluster (for
  example 3 nodes, RF 3) the repair has nowhere to put them until the replacement
  has joined. **Bring the replacement up before expecting `tablets_remaining`
  to reach 0.**

## A. Data-only or non-voter node

1. Make sure the old process is stopped and will not return (stop the unit, or on
   Kubernetes delete its pod and PVC; see K below).
2. Start the replacement. Bare metal, new identity, any live node's
   **intra** address as seed (not the client address):

   ```sh
   animusd join --seed <host:intra-port>[,...] --base-port <P> --dir /var/lib/animus [--id NAME] [--advertise-host NAME] \
     [--tls-cert ... --tls-key ... --tls-ca ...] [--encryption-key PATH]   # plus the same store/TLS/key flags as the rest of the cluster
   # data-only equivalent: animusd data --seed <addr> ... (no local control role)
   ```

   Without `--id` the node mints its own id (an *ephemeral* identity: a restart
   with a fresh `--dir` mints a new one). `--id NAME` is durable. It registers
   as `Down` and is promoted to `Active` by its first heartbeat; placement then
   moves replicas onto it.
3. Find the control leader and drain/remove the dead member (the dead node is
   `Down`; drain marks it `Leaving` so placement moves anything left):

   ```sh
   curl -s -X POST http://<leader-admin>/admin/drain -d '{"node":"<dead-id>"}'
   curl -s "http://<leader-admin>/admin/member/drain-status?node=<dead-id>"   # wait: tablets_remaining 0
   curl -s -X POST http://<leader-admin>/admin/member/remove -d '{"node":"<dead-id>"}'
   curl -s "http://<leader-admin>/admin/member/drain-status?node=<dead-id>"   # status "absent" once committed
   ```

   (`remove` answering `ok` means the proposal was accepted; poll for `absent`.)
   CLI: `animus admin decommission <leader-admin> <dead-id>` does the same flow.
4. Verify ([node-down.md](node-down.md) step 5): replacement `Active`, no
   tablet `under-replicated`, `drain-status` of the dead id `absent`.

Removal is not a fence: if the old process is ever started again it
re-registers as a fresh join. Make sure it is gone.

## B. Combined node that is a control-plane voter

Check first: `curl -s http://<any>/admin/control/members` (`voters`).

1. Quorum alive? If not, stop: [control-plane-quorum-loss.md](control-plane-quorum-loss.md).
2. Remove the dead voter from the control group, on the leader. No force is
   needed to remove the dead voter itself; the guard refuses only if a *different*
   remaining voter is unreachable:

   ```sh
   curl -s -X POST http://<leader-admin>/admin/control/member/remove -d '{"node":"<dead-id>"}'
   ```
3. Start the replacement process (bare metal: a new `animusd control` or combined
   node whose `--config` lists only the current voters as its control peer book,
   per ADR 0037), then add it as a voter, one at a time:

   ```sh
   curl -s -X POST http://<leader-admin>/admin/control/member/add -d '{"node":"<new-id>","addr":"<new-node-INTERNAL-raft-addr host:port>"}'
   curl -s http://<new-node-admin>/admin/control/members   # wait until voters include <new-id>
   ```

   Omit `"node"` to have the leader mint the id (the response carries it); then start the
   process with `--id <minted-id>`. The `addr` is the node's `internal` address, not its
   admin address (`GET /admin/config` on the new node shows it; the CLI's 3-arg
   `control-add` fetches it for you).
4. Then the data-plane part exactly as in section A (drain, remove).
5. Verify `GET /admin/control/members`, `/admin/raft` (a leader, expected voter
   count), `/admin/health` 200 everywhere.

## K. Kubernetes (operator-managed)

Pods are `<name>-<ordinal>` with a stable identity and one PVC each. A node
"replacement" is: the same ordinal comes back with an empty volume.

- **Do not** count on `kubectl delete pod` alone for a lost volume: the pod
  returns under its old id with an empty directory (refused as above).
- Sequence for a **data ordinal** (>= `spec.controlNodes`): run A.3 first (drain,
  poll, remove the id `<name>-<ordinal>` on the control leader), then delete the PVC
  and the pod (`kubectl delete pvc data-<name>-<ordinal> -n <ns>` completes once the pod
  is gone, `kubectl delete pod <name>-<ordinal> -n <ns>`). The StatefulSet recreates
  the pod with a fresh volume and it rejoins as a new member at the same id (ADR 0032:
  removal followed by a restart is a fresh rejoin). If the pod already came back with
  an empty volume while still registered, its replicas will show as refused as
  voters; completing A.3 afterwards still works (drain moves replicas off, remove after
  `tablets_remaining` is 0), then delete the pod once more so it rejoins cleanly.
- **Control ordinal** (< `spec.controlNodes`): `control-remove` the id (section B.2),
  delete the PVC and pod. **The operator re-adds any ordinal in `0..controlNodes`
  that is missing from the live voter set on every reconcile (about every 30 s,
  `advance_control_growth`),** so once the new pod is up and reports the combined
  role, the operator performs the `control/member/add` itself. Do not also
  run B.3 by hand unless the operator is stopped. This is derived from the code
  and has not been run end to end.
- `spec.nodes` cannot go below `spec.controlNodes`, and `spec.controlNodes`
  cannot be reduced.

## Maturity

Join, drain, remove, id reuse after removal and control-voter replacement are
covered by `animusd` integration tests and simulation corpora
(`tests/decommission.rs`, `control_membership*.rs`). The `curl` endpoints in
steps A.3 were run by the author against a local dev cluster (drain and remove
of a data node, refusal of removing an `Active` node, refusal of removing the
only control voter). Everything else here, in particular the Kubernetes
sequence and the operator's automatic voter re-add, is derived from code and ADRs
and has not been executed on a real cluster.
