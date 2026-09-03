# AnimusDB Kubernetes operator — deploy manifests

Manifests for running the `animus-operator` controller (`crates/animus-operator`)
and provisioning `AnimusCluster` custom resources against it. See that crate's
own `CLAUDE.md` for the controller's design.

## Apply order

```sh
kubectl apply -f deploy/operator/crd.yaml       # AnimusCluster CRD
kubectl apply -f deploy/operator/rbac.yaml       # namespace + ServiceAccount + ClusterRole(Binding)
kubectl apply -f deploy/operator/deployment.yaml # the operator Deployment
```

`crd.yaml` is generated output — regenerate it with `animus-operator crd`
(after any change to `crates/animus-operator/src/crd.rs`) rather than
hand-editing it:

```sh
cargo run -p animus-operator --bin animus-operator -- crd > deploy/operator/crd.yaml
```

Once the operator's own `Deployment` is `Ready`, create an `AnimusCluster`:

```sh
kubectl apply -f deploy/operator/example.yaml
kubectl get animuscluster example -o wide
```

## Example manifest

See [`example.yaml`](example.yaml) — a 3-node combined-role cluster (every
pod runs both the control and data role, `controlNodes` defaulting to
`min(3, nodes)`), durable 10Gi storage, and the default `ClusterIP` client
service.

A larger cluster with a real control/data split — say 3 control-role pods
plus 7 data-only pods — sets `nodes: 10` and `controlNodes: 3`: ordinals
`0..3` run combined, `3..10` run data-only (`animusd data`). `controlNodes`
is **immutable** after creation; a later change is rejected (a status
condition, `ImmutableFieldChanged`, is set) rather than applied, since v1
ships no admission webhook to reject the write itself.

## Testing

`cargo test -p animus-operator` is the pure `desired`-builder unit suite —
no cluster needed. `scripts/e2e-kind.sh` (`.github/workflows/e2e-kind.yml`,
CI-gated) is the cluster-driven end-to-end complement: a real `kind`
cluster through create → bootstrap → scale → delete, with the DynamoDB
wire exercised throughout — see `crates/animus-operator/CLAUDE.md`'s own
e2e section for what it does and does not prove.

## What the operator does not do (yet)

- **No finalizer** — deleting an `AnimusCluster` relies on Kubernetes
  garbage collection following the owner references every child object
  (`ConfigMap`/`Service`/`StatefulSet`/`NetworkPolicy`) carries. There is
  nothing else to clean up (no external backup store, no DNS record) so
  this is a deliberate v1 scope cut, not a known gap.
- ~~The operator's own container image is not yet built/published~~ —
  closed 2026-09-02 (S-07a). The root `Dockerfile`'s `runtime-operator`
  stage builds it, and `.github/workflows/image.yml`'s `animus-operator`
  matrix entry publishes `ghcr.io/animus-db/animus-operator` on the same
  tag/push rules as `animusd` — `deployment.yaml`'s image reference is real,
  not a placeholder.
- **`spec.autoSplitBytes` is accepted but not yet wired to a flag** —
  `animusd`'s `--config FILE --node I`/`animusd data --config FILE --node I`
  invocations (what every pod in this deployment shape runs) don't accept
  `--auto-split-bytes` today; only the dev-only `--cluster N` in-process
  mode does. See `crates/animus-operator/src/desired/cluster_config.rs`'s
  `entrypoint_script` doc.
