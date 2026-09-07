# ADR 0070 — Operator admission webhook

- **Status:** Accepted — implemented (S-07e). **2026-09-07 amendment**: the
  `E2E_WEBHOOK=1` leg's first real CI run (PR #703) did find a bug, but in
  the leg's own script, not the in-cluster deployment shape this ADR
  anticipated as the more likely source (see the Consequences bullet
  below) — the rejection/acceptance assertions fired before the webhook
  Service's Endpoints were populated and kube-proxy had programmed its
  ClusterIP, so the API server's admission call got `connection refused`,
  which `failurePolicy: Fail` turned into an `InternalError` the script
  misread as "the webhook ran and didn't reject". Fixed in
  `scripts/e2e-kind.sh` (issue #704): a converged-or-timeout wait on the
  Service's own Endpoints before the `ValidatingWebhookConfiguration` is
  registered, plus a bounded retry on both assertion probes scoped to that
  same dial-failure error shape — see
  `crates/animus-operator/CLAUDE.md`'s e2e section and
  `docs/engineering-lessons.md`'s issue #704 entry for the full account.
- **Date:** 2026-09-07
- **Origin:** `docs/roadmap.md`'s S-07 item e ("Admission webhook validating
  the CRD"), the last item of ADR 0060's own deferred list.
- **Depends on:** [ADR 0060](0060-kubernetes-operator.md) (the operator
  crate, `AnimusCluster` CRD, and the reconciler's own pre-existing
  spec-shape validation this webhook shares logic with), [ADR 0064](
  0064-tls-on-every-port.md) (the cert-manager `Certificate` builder and
  `spec.tls.certManager` CRD shape this webhook's own cert issuance
  reuses/generalizes).

## Context

`crates/animus-operator`'s reconciler has, since ADR 0060, validated a
handful of `AnimusCluster` spec rules purely from the spec itself — `spec
.tls` sets exactly one of `secretName`/`certManager`, `spec.s3` is
internally consistent, `spec.backupStore`/`spec.segmentStore` are
syntactically valid and non-conflicting, and (S-07d) `spec.controlNodes`
only ever grows. Every one of these checks has run at **reconcile** time,
not at **write** time: an invalid write is admitted by the API server,
persisted, and only then caught by the controller, which sets a status
condition (`TlsSpecInvalid`, `S3SpecInvalid`, `StoreSpecInvalid`,
`ControlNodesShrinkRejected`) and reconciles the rest of the spec with the
bad field stripped or the prior value restored. Every one of those
conditions' own doc comments has, until this ADR, carried some form of the
same sentence: *"no admission webhook in v1 to reject the write itself."*

That gap has three real costs:

1. **A bad write looks like it succeeded.** `kubectl apply` returns
   success; the rejection only shows up later, in `status.conditions`, if
   the operator thinks to look.
2. **The bad value is still persisted**, momentarily or (for a field the
   reconciler can't safely strip, like `spec.controlNodes`'s shrink) for as
   long as the operator chooses to keep it — a GitOps tool reconciling
   against the live object's own spec can get confused when the live
   `controlNodes` doesn't match what was applied.
3. Two independent gaps just closed this making a real webhook cheap to
   add: **TLS on every port shipped** (ADR 0064), including a cert-manager
   `Certificate` builder and `spec.tls.certManager` CRD shape a webhook's
   own cert issuance can reuse directly (the roadmap's own stated
   prerequisite); and the reconciler's ad hoc validation had grown to four
   separate call sites (`spec.tls`, `spec.s3`, the store fields,
   `controlNodes`) with near-identical "no admission webhook, so..."
   framing repeated at each one — a single shared validator was overdue
   regardless of whether a webhook ever consumed it.

Kubernetes' own answer to "reject a bad write before it's persisted" is a
`ValidatingWebhookConfiguration`: the API server POSTs an `AdmissionReview`
to an HTTPS endpoint before committing a CREATE/UPDATE/DELETE, and the
endpoint's `AdmissionResponse` decides allowed/denied. This ADR builds
that endpoint into the same `animus-operator` binary, opt-in.

## Decision

### 1. A pure, shared validator (`crate::validate`)

Every rule the reconciler already checked purely from the spec — plus two
new ones the CRD's own doc comments claimed were true but nothing
enforced (`spec.nodes >= 1`; `spec.controlNodes` resolved `>= 1`) — moves
into one function:

```rust
pub fn validate_spec(
    old: Option<&AnimusClusterSpec>,
    new: &AnimusClusterSpec,
) -> Result<(), Vec<Violation>>
```

`Violation { field: &'static str, message: String }` names which part of
the spec is wrong and why; `validate_spec` collects **every** violation in
one pass rather than stopping at the first, so a caller sees everything
wrong with a write in one round trip. It does not reimplement
`TlsSpec::validate`/`S3StoreSpec::validate`/`AnimusClusterSpec::
validate_store_spec` (which stay exactly where they were, in `crd.rs`) —
it calls them, so the reconciler and the webhook, calling the identical
function, structurally cannot drift apart on what those three checks mean.
The two `spec.controlNodes` rules that used to be inline arithmetic in
`controller.rs` (`nodes < controlNodes` refusal; the shrink-rejection
check) moved out into their own small pure functions
(`validate_control_nodes_within_nodes`, `control_nodes_regression`) that
both the reconciler and `validate_spec` now call, for the same reason.

**Deliberately excluded**: whether a referenced `Secret` (`spec.tls.
secretName`, `spec.s3.credentialsSecretName`, `spec.encryptionKeySecretName`,
`spec.dynamoAuthSecretName`) actually exists. That's the one check in this
crate that genuinely needs a live Kubernetes API call
(`crate::controller::validate_encryption_key_secret` is the existing
example), and a webhook must stay **fast and side-effect free** — see
Decision 2 below for why. Live checks stay reconciler-only, with their own
condition-based fallback, unchanged by this ADR.

The reconciler keeps every one of its own status conditions
(`TlsSpecInvalid`/`S3SpecInvalid`/`StoreSpecInvalid`/
`ControlNodesShrinkRejected`, plus a new, purely informational
`NodesSpecInvalid` for the `spec.nodes >= 1` rule, which has no sane
fallback value to strip/substitute) — this is the fallback for a cluster
running without the webhook, or a write that predates it. A cluster with
the webhook installed should never actually observe most of these
conditions in practice, since the write is rejected before it's persisted;
they remain load-bearing for every cluster that hasn't installed one.

### 2. The webhook server (`crate::webhook`), opt-in

`animus-operator run --webhook-addr ADDR --webhook-cert PATH --webhook-key
PATH` (all three or none — a partial set is a startup error) starts a
second HTTPS listener in the same process, alongside the reconcile loop.
It decodes a `POST /validate` request body as `kube::core::admission::
AdmissionReview<AnimusCluster>` (the `admission` Cargo feature on the
`kube` dependency this crate already has, no new HTTP or TLS framework
crate), runs `validate_spec` — `old` from `AdmissionRequest::old_object`
on an UPDATE, `None` on a CREATE — and answers allowed/denied with every
violation's `field: message` joined. Kubernetes' own webhook contract puts
the verdict in the response **body**, not the HTTP status; every reachable
outcome (a valid write, an invalid one, a malformed body, an unexpected
resource kind) answers HTTP `200` with a JSON `AdmissionReview`, matching
`kube::core::admission`'s own documented usage — the process never treats
a bad *request* as a server error, only a bad *spec* as a denial.

**Fast and side-effect free is a hard requirement, not a nicety.** A
`ValidatingWebhookConfiguration` with `failurePolicy: Fail` (this one,
Decision 3) makes every `AnimusCluster` write in the cluster synchronously
depend on this endpoint answering promptly. The handler therefore never
calls the Kubernetes API, never talks to a pod's admin port, and does
nothing beyond decode a request body, run a pure function, and encode a
response — the entire interesting logic (`handle_review`) is exposed as a
standalone function over an already-decoded request precisely so it needs
no socket to test, and so nothing async beyond body-reading and
TLS/TCP I/O sits between "request in" and "response out."

**Server shape**: a `tokio_rustls::TlsAcceptor` (server-only — no client
certificate; the API server verifies this webhook via `caBundle`, this
webhook verifies nobody, matching the admin port's own no-auth posture,
ADR 0020) wrapping `hyper::server::conn::http1` + `hyper_util::rt::TokioIo`
— the identical shape `crates/animusd/src/admin.rs::serve`'s own accept
loop already uses, one real Kubernetes API server call away instead of a
kubelet probe or a `kubectl` client. One task per connection; a failed TLS
handshake or connection error is logged and the loop keeps serving every
other connection.

**Without the three flags, nothing listens** — `cargo run -p
animus-operator -- run` (what `scripts/e2e-kind.sh`'s plain leg uses)
behaves byte-for-byte as before this ADR.

### 3. Cert issuance: two ways, and a static `ValidatingWebhookConfiguration`

Mirroring `spec.tls`'s own two-shape precedent (ADR 0064):

- **cert-manager** (the default `deploy/operator/webhook.yaml` assumes):
  `crate::desired::certificate`'s cert-manager `Certificate` builder,
  which already existed for an `AnimusCluster`'s own TLS cert, is
  generalized — the `Certificate.spec`-building core moved into a shared
  private helper, and a new `build_standalone(name, ns, secret_name,
  dns_names, issuer_ref, duration, renew_before) -> DynamicObject` (no
  owner reference — nothing in this crate owns the webhook's own objects)
  sits alongside the existing `build(cluster, spec)`. A new subcommand,
  `animus-operator webhook-cert --namespace NS --service NAME --issuer-name
  NAME [--issuer-kind Issuer|ClusterIssuer] [--secret-name NAME] [--duration
  D] [--renew-before D]`, prints the resulting `Certificate` YAML to
  stdout — the identical "print YAML, pipe into `kubectl apply -f -`"
  shape the pre-existing `animus-operator crd` subcommand already
  established, chosen over having the operator apply this object itself
  (see the RBAC/blast-radius reasoning in Decision 4). `webhook.yaml`'s
  `ValidatingWebhookConfiguration` carries `cert-manager.io/
  inject-ca-from`, so `cainjector` fills `caBundle` once the `Certificate`
  is `Ready` — no manual CA plumbing.
- **A pre-existing `Secret`**: a hand-issued `kubernetes.io/tls` `Secret`,
  the same shape `spec.tls.secretName` already accepts for an
  `AnimusCluster`'s own cert, with `caBundle` filled in by hand
  (`deploy/operator/README.md`'s own "Admission webhook" section has the
  exact `kubectl` commands for both paths).

### 4. Static manifest, not operator-managed

`deploy/operator/webhook.yaml` (a `Service` for the operator pod's own new
webhook port, plus the `ValidatingWebhookConfiguration` itself, scoped to
`animusdb.io`/`animusclusters` CREATE/UPDATE, `failurePolicy: Fail`,
`sideEffects: None`, `admissionReviewVersions: [v1]`, `timeoutSeconds: 5`)
is a **static, hand-applied manifest** — the operator's own reconcile loop
never creates, updates, or watches it, matching `deployment.yaml`/
`rbac.yaml`'s own precedent (this repo ships no Kustomize/Helm layer; every
manifest here is already applied by hand or by a deployment pipeline
outside the operator's own control).

The alternative — the operator self-registering its own
`ValidatingWebhookConfiguration` at startup, the same way it applies an
`AnimusCluster`'s own children — was considered and rejected:

- It would need new RBAC on a **cluster-scoped**
  `admissionregistration.k8s.io` resource. Every RBAC grant this
  operator's `ClusterRole` carries today is scoped to objects it already
  owns or reads (an `AnimusCluster`'s own children, or read-only
  `Secret`/`pods`) — a webhook configuration is different in kind: a bug
  or a compromised operator process with write access to it could rewrite
  *which writes get admitted at all*, cluster-wide, not just corrupt this
  operator's own child objects. That is a strictly larger blast radius for
  a one-time, rarely-changing object to justify.
- There is no natural reconcile cadence for it the way there is for an
  `AnimusCluster`'s children (which genuinely drift — a `StatefulSet`'s
  replica count, a `ConfigMap`'s content — and need re-converging every
  tick). The webhook configuration changes only when this file's own
  contents change; applying it once, by hand or by a deployment pipeline,
  is simpler and more auditable than teaching the reconciler a second,
  unrelated apply loop for a singleton cluster-scoped object.

Static also matches the answer already given for the `Certificate`
generation above (a one-shot `kubectl apply -f -`, not something the
operator itself creates) and for `deployment.yaml`/`rbac.yaml` themselves.

## Consequences

- **A cluster running without the webhook is unaffected.** No CRD field
  changed shape (the `Violation` messages surfaced through the pre-existing
  status conditions are unchanged in spirit, mostly unchanged in exact
  wording); no existing reconciler behavior changed except that two blocks
  of inline arithmetic became calls into `crate::validate`, producing
  identical decisions. `cargo run -p animus-operator -- run` with no
  `--webhook-*` flags is byte-for-byte the pre-ADR-0070 process.
- **A cluster with the webhook installed gets synchronous rejection** for
  every rule `validate_spec` covers — the reconciler's own condition-based
  fallback becomes, in practice, unreachable for those rules (it stays in
  place for a cluster that hasn't installed the webhook, or a write that
  raced its installation).
- **`failurePolicy: Fail` means a broken/unreachable webhook blocks every
  `AnimusCluster` write**, deliberately — a spec this validator would
  reject is worse to silently admit than to have writes briefly
  unavailable while the webhook itself is down. This is exactly why
  Decision 2's "fast and side-effect free" requirement is load-bearing,
  not a style preference.
- **One new transitive dependency**: `httpdate` (via `hyper`'s newly
  enabled `"server"` Cargo feature, for HTTP `Date` response headers) —
  already MIT/Apache-2.0-licensed and already on `cargo deny`'s allow-list
  shape; no new top-level crate was added to reach any part of this
  design (`kube`'s `admission` feature reuses types already in the
  dependency graph; the server reuses the `hyper`/`hyper-util`/
  `tokio-rustls` stack `admin_client.rs` already pulled in).
- **The e2e-kind smoke needs the operator in-cluster for this leg
  specifically** (`E2E_WEBHOOK=1`) — the plain leg's out-of-cluster
  `cargo run` shape can't be dialed by the API server the way an in-cluster
  `Service` can. See `crates/animus-operator/CLAUDE.md`'s e2e section for
  exactly what that leg deploys and asserts, and that it is **unverified in
  this repository's sandboxed dev environment** for the same structural
  reason (no `CAP_SYS_RESOURCE`) every other `kind`-driven leg already is.

## Delivery plan

Shipped as a single PR (S-07e): the shared validator, the webhook server,
the two cert paths (`webhook-cert` subcommand + `deploy/operator/
webhook.yaml`), `deployment.yaml`'s commented opt-in block, the e2e-kind
`E2E_WEBHOOK=1` leg, and this ADR plus its ADR 0060 pointer amendment. No
follow-up PR is anticipated unless the `E2E_WEBHOOK=1` leg's first real CI
run finds a bug in the in-cluster deployment shape (the same posture every
other `kind`-driven leg in this crate already carries, per each one's own
"UNVERIFIED in this sandbox" note).
