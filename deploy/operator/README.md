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

## TLS (ADR 0064 commit 3)

`spec.tls` turns on TLS across the cluster's own ports (mutual on
`internal`/`intra`, server-only on `client`/`dynamo`/`admin`/`console` — ADR
0064 Decision 1/2), exactly one of two shapes:

```yaml
spec:
  tls:
    secretName: my-preexisting-tls   # a Secret you issued and placed yourself
# or:
spec:
  tls:
    certManager:
      issuerRef:
        name: my-cluster-issuer
        kind: ClusterIssuer           # or Issuer (namespace-scoped)
      # duration: "2160h"             # optional, cert-manager default otherwise
      # renewBefore: "360h"
```

Setting both, or neither field inside `tls`, is rejected: the controller
sets a `TlsSpecInvalid` status condition and reconciles the rest of the
spec as if `tls` were absent, rather than getting stuck.

The `certManager` shape only *references* an `Issuer`/`ClusterIssuer` — it
must already exist (cert-manager itself, plus that issuer, are prerequisites
this operator does not install or create) — and the controller creates a
`cert-manager.io/v1` `Certificate` (owned by the `AnimusCluster`, named
`{cluster}-tls`) whose `dnsNames` cover every pod's stable per-ordinal FQDN
plus both Services (headless internal + client-facing `dynamo`), so the
cert-manager-issued cert (in `Secret` `{cluster}-tls`) verifies against
however a peer dials it. The `secretName` shape skips the `Certificate`
entirely — you own that `Secret`'s lifecycle (issuance and rotation).

Either way the resolved `Secret` (`kubernetes.io/tls` shape:
`tls.crt`/`tls.key`/`ca.crt`) is mounted read-only at `/etc/animus/tls` on
every pod, and every generated node's `cluster.json` gets a `tls` section
pointing at those three files — the *same* cert/key on every pod, not a
distinct one per ordinal (see `crd::TlsSpec`'s own doc for why).

**(2026-09-05)** The pod's own readiness/liveness probes (`GET
/admin/health`) switch to `scheme: HTTPS` once `spec.tls` is set — admin is
server-only TLS, so a plaintext kubelet probe against a TLS-only listener
fails the handshake on the server side, and without this every pod stays
NotReady and gets restart-looped by the kubelet. The kubelet's HTTPS probe
does not verify the server certificate, so no CA needs plumbing into it.

The scale-down drain sequence's admin-port calls (`crate::admin_client`)
switch to TLS automatically once `spec.tls` is set: the controller reads
the resolved `Secret`'s `ca.crt` through the same `kube::Api` the rest of
the controller already uses (RBAC `secrets: get/list/watch`, added by
`rbac.yaml`) — not a mounted file on the operator's own pod — which is what
lets this work identically whether the operator runs in-cluster
(`deployment.yaml`) or out-of-cluster via `cargo run -p animus-operator --
run` against a local kubeconfig (the `scripts/e2e-kind.sh` shape): both
paths reach the API server, neither needs a filesystem mount of its own.

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
