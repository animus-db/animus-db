# Helm charts

The primary way to run AnimusDB on Kubernetes (ADR 0060, 2026-11-14 amendment).

| Chart | What it installs |
|-------|------------------|
| [`animus-operator`](animus-operator/README.md) | CRD, RBAC, the operator Deployment, optional validating webhook |
| [`animus-cluster`](animus-cluster/README.md) | One `AnimusCluster` (the database) |

```sh
helm install animus-operator deploy/helm/animus-operator -n animus-operator --create-namespace
helm install my-db deploy/helm/animus-cluster -n default
```

Until the first release tag exists add `--set image.tag=latest` to the first command and
`--set spec.image=ghcr.io/animus-db/animusd:latest` to the second. Published as OCI artifacts
(`oci://ghcr.io/animus-db/charts/<chart>`) by `.github/workflows/release.yml` on `v*` tags.

The raw manifests in [`../operator/`](../operator/) remain supported and are kept in step with
the charts by `scripts/check-helm-charts.sh` (CI job `helm`): identical RBAC rules, a
byte-identical CRD, and the chart `appVersion` equals the workspace version. Static checks only:
no chart has been installed on a cluster by the authors' CI.
