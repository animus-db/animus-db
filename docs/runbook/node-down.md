# A node is down or unreachable

Use this when a node's `/metrics` scrape has disappeared, a Kubernetes pod is
`NotReady` or crash-looping, a dashboard shows a member `Down`, or clients
report errors that name one node. Conventions (`<admin-addr>`, the control
leader, ports) are in [README.md](README.md).

## What the cluster does by itself

- Every node heartbeats every 100 ms. The control-plane leader marks a member
  `Down` after 500 ms of silence and back to `Active` on its next heartbeat.
- A `Down` member's tablet replicas are **not** moved at once. After it has
  stayed `Down` for a further 5 s (`REPAIR_DWELL`) the control leader
  re-plans the affected tablets onto `Active` nodes
  (`replan_repair`, which only grows a replica set and never shrinks one at
  capacity: with no spare `Active` node the dead replica stays assigned until
  one appears).
- Tablet Raft groups elect new leaders on their own; a tablet with a majority
  of replicas alive keeps serving. A control-plane majority alive keeps the
  control plane serving. There is no maintenance mode and no way to ask the
  cluster to tolerate a longer outage without re-planning.

Consequence: a node that is back inside about 5.5 s causes no data movement.
A node that is down for longer (any real pod reschedule) causes its replicas
to be rebuilt elsewhere; that is safe (epoch-CAS and catch-up gating) but
costs I/O. Plan restarts accordingly ([cert-rotation.md](cert-rotation.md),
[upgrade.md](upgrade.md)).

## 1. Is the process up?

```sh
curl -s -o /dev/null -w '%{http_code}\n' http://<admin-addr>/admin/live   # 200 = admin server answers
curl -s -o /dev/null -w '%{http_code}\n' http://<admin-addr>/admin/health # 200 = recent control leader heard
```

(Use `https://` and `--cacert` when TLS is on.)

| `live` | `health` | Meaning | Go to |
|---|---|---|---|
| no answer | no answer | process dead, host down, or network/firewall | step 2 |
| 200 | 503 | process up, but it has not heard a control leader for 3 election timeouts; usually the *control plane* is the problem, not this node | [control-plane-quorum-loss.md](control-plane-quorum-loss.md) if several nodes show it |
| 200 | 200 | node healthy at this level; the problem is a specific tablet or the client path | [tablet-unavailable.md](tablet-unavailable.md), [overload-and-throttling.md](overload-and-throttling.md) |

`health` is a control-plane signal only. A node can answer `health` 200 while
one of its tablet groups is dead (see the panic note in step 3).

## 2. Process dead or unreachable

Kubernetes (operator-managed cluster `<name>` in `<ns>`):

```sh
kubectl -n <ns> get pods -l app.kubernetes.io/instance=<name> -o wide
kubectl -n <ns> describe pod <name>-<ordinal>        # events, probe failures, OOMKilled
kubectl -n <ns> logs <name>-<ordinal> --previous     # why the last run exited
kubectl -n <ns> get animuscluster <name> -o yaml     # status.phase, status.conditions
```

Pod identity and storage are stable (StatefulSet, one PVC per pod): a deleted
or evicted pod comes back with the same ordinal, hostname and data volume.
`kubectl delete pod <name>-<ordinal>` is the supported "restart".

Bare metal: restart the process with exactly the command and **the same
`--dir`** it had before, for example
`animusd --config cluster.json --node I --dir /var/lib/animus` (or the
`animusd join ...`, `animusd control ...`, `animusd data ...` form it was
started with). Recovery from a crash with an intact data directory is the
normal restart path and is covered by the simulation corpora (WAL replay, LSM
manifest recovery, torn-tail repair).

## 3. It restarts but will not become healthy

Read the node's log (`RUST_LOG=debug` for more). Known named refusals at boot
(they are deliberate; do not work around them):

| Symptom in the log | Cause | Action |
|---|---|---|
| Encryption marker refusal: "wrong key", "encrypted directory, no key", or "key against a plaintext directory" (ADR 0069) | the `--encryption-key` file does not match what the data directory was created with | restore the right key; see [encryption-key-rotation.md](encryption-key-rotation.md) |
| TLS material cannot be read or parsed (`TlsConfig::load`) | missing/invalid PEM at the configured paths | fix the files; see [cert-rotation.md](cert-rotation.md) |
| A named format/version error naming an unsupported version | the binary is older than the data (downgrade) | run the newer binary; rollback is restore from backup ([upgrade.md](upgrade.md)) |
| A control voter whose data directory was wiped never votes or campaigns (no metric names this; `GET /admin/raft` shows no leader/votes from it) | the boot-time wiped-voter check (ADR 0009, issue #667): a voter with empty persisted state must not vote until it is re-admitted | [node-replace.md](node-replace.md), "wiped control voter" |
| Data tablets on a restarted node replicate but never vote: metric `cp_groups_refused_as_voter` > 0, and `/admin/raftkv` shows `refused_as_voter` per group | the same check applied to a tablet replica whose persisted state was empty | [node-replace.md](node-replace.md); expect to need the learner/rejoin path |
| Hostname does not resolve / TLS name mismatch at join | advertise host or certificate SAN wrong | check `--advertise-host` / `spec` and the certificate SANs |

**A hung or half-dead process.** `ProdEnv` wraps every spawned background task
so a panic in it is logged at `error` level and counted, and then the task
simply stops; the process keeps running. Nothing exports that counter as a
metric. A node whose per-tablet driver or apply task has panicked (the
documented example is a failed WAL group-commit sync under disk pressure,
issue #939) can therefore keep answering `/admin/live` and `/admin/health`
while serving nothing for that tablet. If logs contain `panicked`, treat the
node as failed: capture the log, then restart it
(`kubectl delete pod`, or stop and start the process). Check
[disk-full.md](disk-full.md) first, because the usual trigger is the disk.

## 4. Decide: restart, wait, or replace

| Situation | Decision |
|---|---|
| Process crashed or was OOM-killed, disk intact | restart it, same `--dir`. Watch it return to `Active` (step 5). |
| Host or pod unreachable for a short time, disk intact | wait; it rejoins on its own. Its replicas begin moving after about 5.5 s of absence, which is harmless. |
| Disk lost, volume deleted, or `--dir` wiped | **replace**: [node-replace.md](node-replace.md). Do not start an empty directory under the old id on a control voter. |
| Host permanently gone | **replace**, then remove the dead member: [node-replace.md](node-replace.md). |
| Node is healthy but you want it gone | [node-decommission.md](node-decommission.md) (drain first). |

Do not run `animus admin remove` against a `Down` node to "clean it up": it
refuses while the node is `Active`/`Joining` or still referenced by any
tablet, and a `Down` node is still referenced until the repair pass has moved
its replicas away.

## 5. Confirm recovery

```sh
animus admin health <admin-addr>                                    # 200 body: ok true
curl -s http://<any-admin-addr>/admin/status | jq '.members'        # the node's "status" is "Active"
animus admin raftkv <admin-addr>      # per hosted tablet: role, leader, commit_index, engine_applied_index
curl -s http://<any-dynamo-addr>/metrics | grep -E '^(control_is_leader|cp_proposals_rejected_not_leader|cp_groups_refused_as_voter|cp_engine_open_failed) '
```

The dashboard (`GET /` on any admin port) shows the same information; its
Overview tab summarises "is data at risk" (`degraded` when a replica's node is
`Down`, `critical` when a tablet has lost quorum or there is no control
leader).

## Maturity

Heartbeat/detector timings, `REPAIR_DWELL`, the readiness/liveness split and
crash recovery of an intact data directory are covered by simulation or unit
tests. The `kubectl`/bare-metal procedure above has not been executed by its
author. The "zombie after a background-task panic" behaviour is by code
reading (`ProdEnv::spawn_task`'s catch-unwind wrapper and the absence of any
process-level fail-stop); it has not been reproduced on a real disk-full node.
