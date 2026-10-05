# deploy/chaos: the Kubernetes (Chaos Mesh) chaos leg

**Status: designed, pinned, NOT RUN.** Nothing in this directory has been
applied to a real cluster by anyone. The development sandbox this was written
in cannot run `kind` (no `CAP_SYS_RESOURCE`), so none of these manifests, the
selectors, or the pinned versions have been validated. Treat every file as a
reviewed draft until an `e2e-kind`-style job has run it green. See
[`docs/chaos.md`](../../docs/chaos.md) for the design, what each fault is for,
and what the bare multi-process harness (`crates/animusd/tests/chaos.rs`)
already covers without Kubernetes.

The bare multi-process harness is the leg that runs today. This directory is
the Kubernetes counterpart for what loopback processes cannot express: real
CNI partitions, real kernel-level IO faults, real clock offsets (`TimeChaos`),
and pod lifecycle through the operator (ADR 0060).

## Pinned version

| Component | Pin |
|---|---|
| Chaos Mesh | helm chart / app `2.7.2` (assumed current at the time of writing; re-check before first run) |
| Runtime | containerd (kind default), `chaosDaemon.runtime=containerd`, socket `/run/containerd/containerd.sock` |

```sh
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm install chaos-mesh chaos-mesh/chaos-mesh \
  --namespace chaos-mesh --create-namespace --version 2.7.2 \
  --set chaosDaemon.runtime=containerd \
  --set chaosDaemon.socketPath=/run/containerd/containerd.sock \
  --set dashboard.create=false
```

## Target cluster

Every manifest selects pods of an `AnimusCluster` named `chaos` in namespace
`animus-chaos` (the operator labels each child
`app.kubernetes.io/name=animusdb`, `app.kubernetes.io/instance=<cluster name>`,
`crates/animus-operator/src/desired/mod.rs`; StatefulSet pods are
`chaos-0 .. chaos-N`, data at `/var/lib/animus`). Create it from
`deploy/operator/example.yaml` with `metadata.name: chaos`, `nodes: 3`,
`storage.ephemeral: false`.

## Files

| File | Chaos kind | Fault | Maps to the bare harness |
|---|---|---|---|
| `pod-kill.yaml` | `PodChaos` | `pod-kill` one pod (grace 0 = SIGKILL, restarted by the StatefulSet on the same PVC) | `Fault::Kill` |
| `network-partition.yaml` | `NetworkChaos` | `partition` one pod from the rest, both directions | `Fault::Isolate` / `Split` |
| `network-delay.yaml` | `NetworkChaos` | `delay` 300 ms +/- 100 ms between all pods | `Fault::Delay` |
| `network-loss.yaml` | `NetworkChaos` | 20 % packet `loss` on one pod | (new: real packet loss) |
| `io-latency.yaml` | `IOChaos` | 100 ms latency on every IO under `/var/lib/animus` | slow disk (pending in the bare harness) |
| `time-skew.yaml` | `TimeChaos` | clock offset on one pod | clock skew (k8s-only) |
| `io-disk-full.yaml` | `IOChaos` | `ENOSPC` on writes | **DO NOT RUN**: blocked on #1185 (see the file) |

There is no SIGSTOP equivalent among the Chaos Mesh kinds; the stall fault is
`kubectl exec <pod> -- kill -STOP 1` then `-CONT` (the bare harness does the
same with the process pid). `StressChaos` (CPU/memory pressure) is out of this
leg's scope.

## Running one (once validated)

```sh
kubectl apply -f deploy/chaos/network-partition.yaml
# ... drive the workload and the oracles against the client Service ...
kubectl delete -f deploy/chaos/network-partition.yaml   # heal
```

The recorded workload and oracle check are the existing `animus-test` +
harness code; the k8s leg would reuse `tests/chaos_support/workload.rs`
pointed at a port-forwarded client Service instead of loopback ports. That
wiring does not exist yet (see `docs/chaos.md`, "Kubernetes leg: what is
missing").
