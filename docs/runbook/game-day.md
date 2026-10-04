# Game-day drill checklist (kind)

**STATUS: NOT EXECUTED.** This checklist was written in a sandbox that cannot run `kind`
(no `CAP_SYS_RESOURCE`, see `crates/animus-operator/CLAUDE.md`'s e2e section). No item
below has been run on `kind`. **Executing it once is an outstanding criterion (E-9)**, and
every "expected" result is a prediction from code, not an observation. Record the first
execution (date, commit, image digest, result per item, deviations) at the bottom of this
page and file a bug for every deviation.

Conventions are in [README.md](README.md). Each drill points at the page whose procedure it
tests.

## Substrate

`scripts/e2e-kind.sh` is the supported way to get a `kind` cluster running the operator and
a 3-node `AnimusCluster` (`name: e2e`, namespace `animus-e2e`, `nodes: 3`, `controlNodes: 3`,
base port 14000, so admin `14003`, DynamoDB `14002`):

```sh
docker build -t animusd:e2e .
E2E_TLS=1 E2E_ENCRYPTION=1 scripts/e2e-kind.sh      # optional legs: E2E_TLS, E2E_S3, E2E_ENCRYPTION, E2E_WEBHOOK; needs docker, kind, kubectl, curl, jq
```

**The script tears its cluster down on exit** (`cleanup` trap) and has no keep-alive option,
so a drill cannot reuse the cluster it creates. Either (a) copy its bring-up phases by hand
(`kind create cluster`, `kind load docker-image`, `kubectl apply -f deploy/operator/crd.yaml`,
build and run `animus-operator run` out of cluster with `RUST_LOG=info` or apply
`deploy/operator/{rbac,deployment}.yaml`, then `kubectl apply` an `AnimusCluster` like the
manifest in the script's "apply AnimusCluster" phase), or (b) add a keep-alive flag to the
script (a code change this page does not make; report it as a follow-up). Reach pods with
`kubectl port-forward pod/e2e-<n> 18101:14003 18100:14002` (the script does this itself).

Use a larger shape for the drills that need it: `nodes: 5`, `controlNodes: 3`, so that data-only
ordinals 3 and 4 exist.

## Preflight (every drill)

- [ ] `GET /admin/health` 200 on all pods, all members `Active`, dashboard `healthy`.
- [ ] A test table with 10k+ items and a GSI, PITR enabled, and a loop writing and
      reading with `ConsistentRead: true`, recording every error and any lost acknowledged
      write (the oracle: every acked write must be readable afterwards).
- [ ] Alerts loaded if the observability PR is deployed; note which fire.

## Drills

| # | Drill | Do | Expect (prediction) | Page |
|---|---|---|---|---|
| 1 | Kill a data pod | `kubectl delete pod e2e-3` | pod returns with its PVC; no acked write lost; a brief 5xx burst; if back within about 5.5 s no data movement, else replicas re-planned; `AnimusMemberDeclaredDown` fires | [node-down.md](node-down.md) |
| 2 | Kill the control leader's pod | find leader via `/admin/raft`, delete the pod | new leader within seconds; DDL (`CreateTable`) works after; no data loss | [control-plane-leader.md](control-plane-leader.md) |
| 3 | Lose control quorum | delete 2 of 3 voter pods and hold them down (cordon their node or scale the node pool) | all pods `NotReady` after 3 election timeouts, client Service loses endpoints (prediction from code); data plane reachable by port-forward; recover by bringing the pods back | [control-plane-quorum-loss.md](control-plane-quorum-loss.md) |
| 4 | Lose a volume | delete pod `e2e-4` and its PVC | follow the Kubernetes section; confirm the replacement rejoins and the tablet statuses return to `healthy` | [node-replace.md](node-replace.md) |
| 5 | Lose a control voter's volume | as 4 on `e2e-2` | verify the operator re-adds the voter by itself; check #667 behaviour | [node-replace.md](node-replace.md) |
| 6 | Scale down | `kubectl patch animuscluster e2e --type merge -p '{"spec":{"nodes":4}}'` | **tests the suspected scale-down defect** (drain posted to a data-only pod returns 409): expect `DrainFailed` | [node-decommission.md](node-decommission.md) |
| 7 | Network partition | `NetworkPolicy` or `tc netem` isolating one pod, then two | minority side: no leadership, majority continues; heal and compare `commit_index` | [tablet-unavailable.md](tablet-unavailable.md) |
| 8 | Disk full | fill a pod's data volume (`dd` into a temp file on the same filesystem under `/var/lib/animus`) | **unknown** (the point of the drill): record the log, whether the process stays up, `/admin/live`, client errors, recovery after freeing space and restart | [disk-full.md](disk-full.md) |
| 9 | Slow disk / CPU pressure | cgroup limits or a throttled device (`IOChaos` if installed) | elections stay stable? record churn alert and tail latency | [overload-and-throttling.md](overload-and-throttling.md) |
| 10 | Overload | drive writes above a table's provisioned throughput | 400 `ProvisionedThroughputExceededException`, `throttled_writes` rising; no node instability; record behaviour with no throttle configured and saturating load (no admission control exists) | [overload-and-throttling.md](overload-and-throttling.md) |
| 11 | Backup and restore | the drill in the page, with `E2E_S3=1` (RustFS) | restore equals the backup point; PITR `Latest` lag measured | [backup-restore-pitr.md](backup-restore-pitr.md) |
| 12 | Cert rotation | with `E2E_TLS=1`: reissue the `<name>-tls` Certificate, then pod-by-pod restart | no handshake-refused growth; old cert gone from `openssl s_client` | [cert-rotation.md](cert-rotation.md) |
| 13 | Encryption key | `E2E_ENCRYPTION=1`; change the Secret content and restart one pod | pod refuses to start with the named wrong-key error; restore the key | [encryption-key-rotation.md](encryption-key-rotation.md) |
| 14 | Whole-cluster upgrade | recreate the `AnimusCluster` with a new image | data intact, as in the page; confirm that editing `spec.image` in place produces the mixed-version roll we say not to do (do it only on a throwaway cluster) | [upgrade.md](upgrade.md) |

After each drill: the preflight checks again, the write/read oracle reports no lost
acknowledged write, and any alert that should have fired did (and linked to the right page).

## Execution record

| Date | Commit / image | Operator | Drills run | Result and deviations |
|---|---|---|---|---|
| (never executed) | | | | |

## Related, executed on a dev cluster (not kind) on 2026-10-04

The author ran, against a local `animusd --cluster-control 1 --cluster-data 2 --ephemeral`
(one process, in-memory engines): the backup, restore and PITR sequence
([backup-restore-pitr.md](backup-restore-pitr.md)), the throttle check
([overload-and-throttling.md](overload-and-throttling.md)), drain and remove of a data node
and the refusal paths ([node-decommission.md](node-decommission.md)), and the admin-port
`animus` CLI failure noted in [node-replace.md](node-replace.md). This does not satisfy E-9.
