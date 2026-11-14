# animus-cluster Helm chart

Renders one `AnimusCluster` custom resource. The
[`animus-operator`](../animus-operator/README.md) chart (which installs the CRD) must be
installed first; this chart installs nothing but the one object, in the release namespace.
The operator turns it into the StatefulSet, Services, NetworkPolicy and PodDisruptionBudget.

## Install

```sh
helm install my-db deploy/helm/animus-cluster --namespace default
# or: helm install my-db oci://ghcr.io/animus-db/charts/animus-cluster --version 0.1.0-alpha.0
kubectl get animuscluster my-db -o wide
```

The object is named after the release (`my-db`); `fullnameOverride` / `nameOverride`
change it. **Pre-release note:** `spec.image` defaults to
`ghcr.io/animus-db/animusd:<appVersion>`, which exists only after the first release tag.
Until then add `--set spec.image=ghcr.io/animus-db/animusd:latest`.

## Configuration

`values.yaml` has one block, `spec:`, passed to the CRD verbatim. Its defaults mirror
`deploy/operator/example.yaml` (3 nodes, 3 control nodes, 10Gi durable storage,
`ClusterIP` client service). Any field the CRD accepts works, so the chart does not drift
from it: TLS, S3 stores, encryption, rolling-upgrade policy and the rest are documented in
[`deploy/operator/README.md`](../../operator/README.md).

```sh
helm install my-db deploy/helm/animus-cluster \
  --set spec.nodes=5 --set spec.storage.size=100Gi --set spec.storage.storageClassName=fast-ssd
```

`spec.controlNodes` can only grow, `spec.storage.ephemeral: true` is rejected with more
than one control node, and editing `spec.image` performs a gated rolling upgrade. `helm
upgrade` applies such edits like any other spec change.

## Connect

The DynamoDB endpoint is the Service `<name>-dynamo` on port `basePort + 2` (14002 by
default). From your workstation:

```sh
kubectl port-forward svc/my-db-dynamo 8000:14002 &
export AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local AWS_REGION=us-east-1
aws --endpoint-url http://localhost:8000 dynamodb create-table --table-name t \
  --attribute-definitions AttributeName=id,AttributeType=S \
  --key-schema AttributeName=id,KeyType=HASH --billing-mode PAY_PER_REQUEST
aws --endpoint-url http://localhost:8000 dynamodb list-tables
```

Credentials are not checked unless `spec.dynamoAuthSecretName` is set. In-cluster clients
use `http://my-db-dynamo.<namespace>.svc:14002`.

## Delete

`helm uninstall my-db` deletes the `AnimusCluster`; Kubernetes garbage collection removes
the children. PersistentVolumeClaims from the StatefulSet are kept by Kubernetes defaults.
