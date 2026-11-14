# animus-operator Helm chart

Installs the AnimusDB Kubernetes operator (the `AnimusCluster` controller), its
CRD, RBAC, and optionally the validating admission webhook. Create clusters with
the sibling [`animus-cluster`](../animus-cluster/README.md) chart or by applying
an `AnimusCluster` yourself.

The chart never creates the namespace and sends nothing anywhere (AnimusDB has no
telemetry). Rendering and linting are checked in CI; **a real install has not been
tested** (no Kubernetes cluster is available to the authors' CI sandbox, and the
`kind` e2e in `scripts/e2e-kind.sh` still deploys the raw manifests).

## Install

From a checkout:

```sh
helm install animus-operator deploy/helm/animus-operator \
  --namespace animus-operator --create-namespace
```

From the OCI registry (published on release tags):

```sh
helm install animus-operator oci://ghcr.io/animus-db/charts/animus-operator \
  --version 0.1.0-alpha.0 --namespace animus-operator --create-namespace
```

**Pre-release note.** The chart's default image tag is its `appVersion`, which only
exists in the registry once the first release tag has been cut. Until then install
the main-branch build:

```sh
helm install animus-operator deploy/helm/animus-operator \
  --namespace animus-operator --create-namespace --set image.tag=latest
```

Any namespace works with the default `adminAccess: proxy`. Only `adminAccess: direct`
needs the namespace `animus-operator` (the operator's generated NetworkPolicy admits
that namespace by name).

## Upgrade

```sh
helm upgrade animus-operator deploy/helm/animus-operator --namespace animus-operator
```

**Helm never upgrades or deletes CRDs in `crds/`.** After a chart upgrade that
changes the CRD, apply it yourself first:

```sh
kubectl apply -f deploy/helm/animus-operator/crds/animusclusters.animusdb.io.yaml
```

Uninstalling the chart
leaves the CRD, and therefore your `AnimusCluster` objects, in place.

Upgrading the operator does not roll your database pods unless a template change
requires it; roll the database itself by editing `spec.image` (see
`docs/runbook/upgrade.md`).

## Admission webhook (optional)

`webhook.enabled=true` adds the Service and `ValidatingWebhookConfiguration`
(`failurePolicy: Fail`: while the operator pod is down, `AnimusCluster` writes are
refused). It needs a certificate, from one of two sources:

```sh
# cert-manager and an existing Issuer/ClusterIssuer (the chart renders the Certificate)
helm upgrade --install animus-operator deploy/helm/animus-operator -n animus-operator \
  --set webhook.enabled=true --set webhook.certManager.enabled=true \
  --set webhook.certManager.issuerRef.name=my-cluster-issuer

# or your own kubernetes.io/tls Secret in the release namespace + its CA
helm upgrade --install animus-operator deploy/helm/animus-operator -n animus-operator \
  --set webhook.enabled=true --set webhook.tlsSecretName=my-webhook-tls \
  --set webhook.caBundle="$(base64 -w0 ca.crt)"
```

Rendering with the webhook enabled and neither source fails with an explanatory error.

## Values

See [`values.yaml`](values.yaml); every key is commented. Notable choices:

- `replicas` is not a value: the operator has no leader election, so the Deployment is
  one replica with a `Recreate` strategy.
- `securityContext` defaults: non-root (uid 1000, the image's user), no privilege
  escalation, all capabilities dropped, `readOnlyRootFilesystem: true` (the operator
  only talks to the API server; a code search found no filesystem writes outside tests,
  but this has not been run on a cluster; set it to `false` if your environment
  disagrees), `seccompProfile: RuntimeDefault`.
- The pod label `app.kubernetes.io/name: animus-operator` is fixed and does not follow
  `nameOverride`.
- The ClusterRole rules equal `deploy/operator/rbac.yaml`; the CRD is a byte-identical
  copy of `deploy/operator/crd.yaml`. Both are enforced by `scripts/check-helm-charts.sh`
  and the `crd_manifest_pinned` test.

## Regenerating the CRD

After a change to `crates/animus-operator/src/crd.rs`:

```sh
cargo run -p animus-operator -- crd > deploy/operator/crd.yaml
cp deploy/operator/crd.yaml deploy/helm/animus-operator/crds/animusclusters.animusdb.io.yaml
```
