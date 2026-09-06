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
is **grow-only** since S-07d — see the dedicated section below.

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
`rbac.yaml`) — not a mounted file on the operator's own pod. That CA is
consulted only in `--admin-access direct` mode (below); the default
`proxy` mode dials no TLS of its own and ignores it (the Kubernetes API
server verifies nothing about the pod's serving certificate either — see
the next section).

## Admin access (`--admin-access {proxy,direct}`, ADR 0060's dated amendment)

Every admin-port call this controller makes (today: the scale-down drain
sequence, `crate::admin_client::drain_and_remove_node`) reaches its target
pod one of two ways, chosen by `animus-operator run`'s own
`--admin-access` flag:

- **`proxy` (the default)** routes the request through the Kubernetes API
  server's pod-proxy subresource — `GET`/`POST
  /api/v1/namespaces/{ns}/pods/{scheme}:{pod}:{port}/proxy{path}` — so the
  only address this process ever dials for an admin call is the API
  server itself, which it already talks to for everything else. This is
  what makes admin-port calls reachable at all when the operator runs
  **out-of-cluster** against a local kubeconfig (`cargo run -p
  animus-operator -- run`, the shape `scripts/e2e-kind.sh` and any other
  local-iteration workflow use): from outside the cluster network,
  neither a pod's headless-`Service` DNS name
  (`<pod>.<svc>.<ns>.svc.cluster.local`) nor its `10.244.x.x` pod IP is
  routable — only the API server's own (already-reachable) address is.
  `proxy` also works in-cluster, at the cost of one extra hop through the
  API server; admin calls are rare (a handful of requests across a whole
  scale-down), so that hop is not a concern. **No CA plumbing needed**:
  the API server itself dials TLS to the pod for a `https:` proxy target
  and does not verify the pod's serving certificate — this is
  Kubernetes' own pod-proxy behavior, not a choice made here.
- **`direct`** dials the pod's admin port itself — plain HTTP, or
  server-only TLS trusting `spec.tls`'s resolved CA, verified end-to-end
  — exactly what this controller did before `proxy` existed. Only
  reachable when the operator runs **in-cluster**
  (`deployment.yaml`); out-of-cluster, every direct-mode admin call fails
  (both the pod DNS name and its IP are unroutable from outside the
  cluster network) rather than hanging — every admin request, in both
  modes, is bounded by `crate::admin_client::ADMIN_REQUEST_TIMEOUT` (a
  few seconds), so an unroutable target fails a reconcile step fast.

`deployment.yaml` passes no `--admin-access` flag, so the in-cluster
deployment also defaults to `proxy` — deliberately: one access mode
covers both deployment shapes, and there's no operational reason to
special-case in-cluster onto `direct`. Pass `--admin-access direct`
explicitly if you want the old direct-dial behavior in-cluster.

**RBAC**: `proxy` mode needs `pods/proxy` (`get` for the drain-status
poll, `create` for the drain/remove POSTs) in addition to the pre-existing
`pods: get/list/watch` — both are already granted by `rbac.yaml`
regardless of which mode you actually run with, since the flag is a
per-process runtime choice RBAC can't see.

## S3 backup/segment stores (S-04 PR 3)

`spec.s3` wires the cluster's backup and/or stream-segment store onto a
real S3-compatible bucket, closing `docs/roadmap.md`'s S-04 item:

```yaml
spec:
  s3:
    backupStore: "s3://my-backups-bucket?endpoint=https://s3.us-east-1.amazonaws.com&region=us-east-1"
    # segmentStore: "s3://my-streams-bucket?endpoint=https://s3.us-east-1.amazonaws.com&region=us-east-1"
    credentialsSecretName: my-s3-credentials   # keys: access_key_id, secret_access_key
    allowInsecureHttp: false                   # true only for a plain-http:// dev endpoint (MinIO/localstack)
    egressCidrs: ["0.0.0.0/0"]                 # NARROW to your object store's real CIDR range
```

At least one of `backupStore`/`segmentStore` must be set, and the URI
shape is the identical `s3://<bucket>[/<prefix>]?endpoint=<scheme://
host[:port]>&region=<region>[&insecure_http=true]` `animusd`'s own
`--backup-store`/`--segment-store` flags accept (see ADR 0059's own S-04
amendment). `credentialsSecretName` names a **pre-existing** `Secret`
(same namespace, keys `access_key_id`/`secret_access_key`) this operator
only ever mounts read-only at `/etc/animus/s3` — it never creates or
writes one, mirroring `spec.tls`'s own `secretName` precedent. The secret
value never reaches the `ConfigMap`/`cluster.json`: a combined-role pod's
own generated `entrypoint.sh` reads both files at container-start time and
writes a scratch `--s3-credentials` JSON file naming only the *path* to
`secret_access_key`. **Combined-role pods only** — `animusd data --config`
accepts no S3-store flags today (a pre-existing `animusd` gap, not
introduced here; see `crates/animus-operator/CLAUDE.md`'s CLI-flag-support
table).

Setting `spec.s3` also adds an `Egress` section to the generated
`NetworkPolicy` (every cluster now gets one, `spec.s3` or not — see below):
a third rule, scoped to `egressCidrs`, opens the configured store URIs' own
`endpoint=` port(s). **`NetworkPolicy` cannot express a hostname
allowlist** — only IP blocks — so this operator has no way to resolve an
endpoint's hostname into the right CIDR for you; `egressCidrs` defaults to
`["0.0.0.0/0"]` (open to any destination on that port) and **should be
narrowed to your object store's real address range** in any environment
where that egress must be restricted.

Invalid specs (neither store set, an empty `credentialsSecretName`, a
malformed store URI, or `insecure_http=true` without `allowInsecureHttp`)
are rejected: the controller sets an `S3SpecInvalid` status condition and
reconciles the rest of the spec as if `s3` were absent, the same posture
`TlsSpecInvalid` uses above.

**Every cluster's `NetworkPolicy` egress, S3 or not**: before this PR the
generated policy set no `Egress` in `policyTypes` at all, which leaves
Kubernetes egress **completely unrestricted by omission** regardless of
anything else the policy says — `docs/roadmap.md`'s S-04 item named this
exactly. Every cluster now gets an explicit `Egress` section with two
baseline rules (intra-cluster on the `internal`/`intra` ports, and DNS to
`kube-system`'s `kube-dns`/CoreDNS pods) whether or not `spec.s3` is set.

## Non-S3 backup/segment stores (S-07b)

`spec.backupStore`/`spec.segmentStore` are the CRD surface for the
*non-S3* forms `spec.s3` above doesn't cover — pinning `--backup-store
cluster|fs:<path>` or `--segment-store dir:<path>` from the spec instead of
configuring it by hand, closing `docs/roadmap.md`'s S-07 item b:

```yaml
spec:
  backupStore: "fs:/var/lib/animus/backups"   # or "cluster" (the default, spelled out)
  segmentStore: "dir:/var/lib/animus/segments" # --segment-store has no "cluster" keyword
```

Each field accepts exactly the literal forms named above — a malformed
value, a `segmentStore: "cluster"` (rejected: `--segment-store` has no such
keyword at all; omit the field to select its default instead), or an
`s3://...` URI (rejected, pointing at `spec.s3` — only that section
supplies the credentials an S3 store needs) sets a `StoreSpecInvalid`
status condition and reconciles the rest of the spec with both fields
stripped, the same posture `TlsSpecInvalid`/`S3SpecInvalid` use above.
Setting the same store in both `spec.s3` and the matching top-level field
(e.g. both `spec.s3.backupStore` and `spec.backupStore`) is also rejected,
naming both fields.

**The `fs:`/`dir:` path must live under the pod's own data volume**
(`/var/lib/animus` by default — a `PersistentVolumeClaim`, or an
`emptyDir` when `spec.storage.ephemeral` is set) — a path elsewhere on the
container filesystem is never a sensible place to point a store, and a
path equal to that root itself is rejected too (that's where `animusd
--dir` puts its own on-disk files). Unlike `spec.s3`, **no new volume or
`Secret` is mounted for this** — `cluster`/`fs:`/`dir:` need no
credentials, so the pod's already-mounted data volume is all that's
involved. Reaches only **combined-role pods**, the same pre-existing
`animusd` gap `spec.s3` documents above.

## PodDisruptionBudget (S-07c)

Every `AnimusCluster` gets a `{name}-pdb` `PodDisruptionBudget` selecting
the cluster's own pods, with no `spec`-level opt-out or override —
`maxUnavailable` is always computed from `spec.nodes`/`spec.controlNodes`,
never a constant and never user-supplied:

```
maxUnavailable = min(
  floor((controlNodes - 1) / 2),                       # control-plane quorum
  floor((min(nodes, 3) - 1) / 2),                       # data-plane tablet RF (fixed at 3 today)
)
```

For the operator's own default 3-node/3-`controlNodes` shape this is `1`;
it stays `1` for any larger `nodes` count too, as long as `controlNodes`
doesn't also grow, since the data-plane replication factor is capped — a
scale-up/down within that range never needs the budget recomputed. (While
a `controlNodes` growth, S-07d, is still catching up, the budget is
computed from the live-confirmed voter count, never the full target — see
this file's own "Control-voter growth" section below.) A cluster
smaller than its replication factor (`nodes < 3`), or with a single
control voter (`controlNodes == 1`), computes `0` — **this correctly
blocks every voluntary eviction**, since such a cluster cannot survive
losing its one and only copy of a control-plane or data-plane majority.
See `crates/animus-operator/src/desired/poddisruptionbudget.rs`'s own
module doc for the full reasoning, including why there is no CRD field to
loosen or disable it.

## Control-voter growth (S-07d)

`spec.controlNodes` is **grow-only**: an increase is honored — the
operator drives ADR 0037's `control/member/add` against the
newly-promoted ordinals, one voter at a time, until the control group
itself confirms each one (`GET /admin/control/members`), surfacing
progress as a `ControlNodesGrowing` status condition while it's in
flight. A **decrease** is still rejected (a status condition,
`ControlNodesShrinkRejected`, is set) rather than applied, since v1 ships
no admission webhook to reject the write itself and control voters can
only be removed one at a time through their own careful quorum-loss
checks (ADR 0037 §2), never inferred from a bare spec edit. See ADR
0060's own "Control-voter growth (S-07d, 2026-09-06)" section for the
full design (the live-truth-driven sequence, why role-promotion needs a
restart, the `SocketAddr` gap this works around, and how a controller
restart resumes).

**Growing `controlNodes` restarts every pod, not just the promoted
one.** Regenerating the config that drives the role split requires a
config-affecting spec change to actually reach an already-running pod,
which now happens via a pod-template config-hash annotation
(`desired::statefulset::CONFIG_HASH_ANNOTATION`) that triggers a normal
`StatefulSet` rolling restart, highest ordinal first, one at a time. This
applies to *any* config-affecting field, not just `controlNodes` —
`spec.tls`/`spec.s3`/`spec.backupStore`/`spec.segmentStore`/
`spec.dynamoAuthSecretName`/`spec.quiesceAfterSecs`/`spec.autoSplitBytes`
all now trigger a rolling restart when changed too, where they previously
sat unapplied on already-running pods until an unrelated restart.

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
  (`ConfigMap`/`Service`/`StatefulSet`/`NetworkPolicy`/
  `PodDisruptionBudget`) carries. There is nothing else to clean up (no
  external backup store, no DNS record) so this is a deliberate v1 scope
  cut, not a known gap.
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
