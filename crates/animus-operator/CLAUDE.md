# CLAUDE.md — animus-operator

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

A Kubernetes operator for AnimusDB, built on `kube-rs`: the `AnimusCluster`
custom resource (group `animusdb.io`, version `v1alpha1`) plus a controller
that reconciles it into a running per-process AnimusDB deployment — a
`ConfigMap` (the `animusd::config::ClusterConfig` JSON every pod loads, plus
a small dispatch script), a headless internal `Service` for node-to-node
traffic, a client-facing `dynamo` `Service`, a `NetworkPolicy`, and a
`StatefulSet`. This is the "deployment target" the root `CLAUDE.md`'s
architecture map calls out: seed/intra node-to-node traffic stays
cluster-internal, only the DynamoDB wire edge is exposed.

**This crate does not depend on `animusd`, `animus-env`, or any other
workspace crate.** It only has to *emit* JSON `animusd` can parse and a
shell script that execs the right `animusd` invocation — a hand-written
mirror of `animusd::config::ClusterConfig`/`RoleAddrs`'s serde shape avoids
pulling the whole node-server dependency tree into a Kubernetes-controller
binary for a build-time-only JSON shape. **Keeping that mirror in sync with
`animusd`'s real shape is a manual invariant, not a compiler-enforced one**
— see the gotcha below.

## Entry points

- `src/crd.rs` — the `AnimusCluster` type (`kube::CustomResource` derive):
  `AnimusClusterSpec`/`AnimusClusterStatus`/`StorageSpec`/
  `ClientServiceSpec`/`ClusterCondition`/`ClusterPhase`/`TlsSpec`/
  `S3StoreSpec` (S-04 PR 3, see this file's own S3 section below), and
  the non-S3 `backup_store`/`segment_store` fields (S-07b, see this
  file's own Non-S3 stores section below). Pure
  data + a handful of `_or_default()`/`validate()` helpers; no k8s API
  calls, no logic that needs a live object beyond its own fields.
- `src/s3_uri.rs` — a small, deliberately minimal syntax-only check of an
  `s3://...` store URI (S-04 PR 3): bucket present, `endpoint=` query key
  present, `http://`/`https://` scheme. **Not** a reimplementation of
  `animusd::main::parse_s3_uri` (this crate has no dependency on
  `animusd` — see this file's own note above); see this module's own doc
  for exactly what it does and does not re-verify. Used by
  `crd::S3StoreSpec::validate` and `desired::networkpolicy`'s port
  extraction.
- `src/desired/` — **pure builder functions**, `(name, ns, spec) -> a typed
  k8s-openapi object`, no cluster access. This is where almost all of this
  crate's tests live:
  - `cluster_config.rs` — the `animusd::config::ClusterConfig`/`RoleAddrs`
    JSON mirror (see the gotcha below) and `entrypoint_script` (the POSIX
    `sh` dispatch script every pod runs). Also `TlsSection`/`tls_section`/
    `TLS_MOUNT_DIR` (ADR 0064 commit 3) — the `RoleAddrs.tls` mirror every
    node gets, identical across every pod, when `spec.tls` is set.
  - `certificate.rs` (ADR 0064 commit 3) — the cert-manager `Certificate`
    builder, only invoked when `spec.tls.certManager` is set. Produces a
    `kube::core::DynamicObject`, not a typed struct — `cert-manager.io/v1`
    isn't a `k8s-openapi` type — via `certificate::api_resource()`
    (`ApiResource::from_gvk`), which `cluster_api.rs` also uses to address
    it through `Api::namespaced_with`. `dns_names` computes the SAN list:
    every pod's own stable per-ordinal FQDN plus both Services (headless
    internal + client-facing `dynamo`), short and fully-qualified.
  - `poddisruptionbudget.rs` (S-07c) — the `{name}-pdb`
    `PodDisruptionBudget` builder: `maxUnavailable` is derived from
    `spec.nodes`/`spec.controlNodes` and a fixed data-plane replication
    factor mirror, never a constant and never CRD-overridable — see this
    file's own "PodDisruptionBudget" section below.
  - `configmap.rs`/`services.rs`/`statefulset.rs`/`networkpolicy.rs` — one
    builder module per child kind. `statefulset.rs` mounts `spec.tls`'s
    resolved `Secret` (ADR 0064 commit 3) read-only at `/etc/animus/tls`,
    the same mount for either `TlsSpec` shape, (S-04 PR 3)
    `spec.s3.credentialsSecretName`'s `Secret` read-only at `/etc/animus/s3`,
    and (ADR 0069 S-03 PR 3)
    `spec.encryptionKeySecretName`'s `Secret` read-only, `defaultMode`
    restricted, at `ENCRYPTION_KEY_MOUNT_DIR` (`/etc/animus/encryption`),
    all on every pod; `networkpolicy.rs` is unaffected by TLS (its own module
    doc explains why: TLS is a mode a port's listener can be configured
    into, not a change to which pods may reach which port) but **is**
    affected by `spec.s3` (S-04 PR 3) — see this file's own S3 section
    below for the egress rules it now always adds.
  - `mod.rs` — shared label/name helpers (`common_labels`/
    `selector_labels`/`owner_reference`/`pod_fqdn`/the `*_name` functions)
    every builder module uses, so every child's naming/labeling convention
    lives in one place.
  - `test_support.rs` (`#[cfg(test)]` only) — `test_cluster(name, ns, nodes,
    control_nodes)`, the one fixture every builder test file shares.
- `src/admin_client.rs` — a minimal HTTP(S) JSON client (`hyper`/
  `hyper-util`, reusing `kube`'s own already-pulled HTTP stack rather than
  adding `reqwest`) for `animusd`'s admin/debug interface (ADR 0020) — used
  only by the scale-down drain sequence below. Plain HTTP by default;
  server-only TLS (ADR 0064 commit 3, no client cert — this crate never
  joins the cluster) whenever the target cluster's `spec.tls` is set,
  since `animusd` then serves `admin` over TLS too. Own small
  `tower_service::Service<Uri>` connector (`AdminConnector`/`MaybeTlsIo`)
  rather than a `hyper-rustls` dependency or reuse of `animus-env`'s
  `MaybeTlsStream`/`animus-cli`'s connector — this crate depends on neither
  crate (see this file's own "does not depend on `animusd`/`animus-env`"
  note above) and `hyper-util`'s legacy `Client` needs a connector shaped
  as a `tower_service::Service<Uri>`, not the `AsyncRead+AsyncWrite`
  wrapper those two build for a different call shape. `AdminOps::
  post_json`/`get_json` both take `ca_pem: Option<&[u8]>` (ADR 0061 rung E1
  seam, extended for TLS) — `Some` dials TLS trusting those CA bytes,
  `None` plain TCP; `crate::controller::reconcile` reads the bytes out of
  `spec.tls`'s resolved `Secret` via `ClusterApi::get_secret` (the
  Kubernetes API, not a mounted file — see the TLS section below for why).
  This is `AdminClient`, one of two `AdminOps` implementors, selected by
  `--admin-access direct`; it only works when the operator runs
  in-cluster. `ProxyAdminClient`, in the same file, is the **default**
  (`--admin-access proxy`) — see ADR 0060's own dated amendment ("operator
  admin access through the API server pod proxy") for the full rationale:
  it parses `admin_base_url`'s own URL shape back apart
  (`parse_admin_url`) into `(namespace, pod, port, scheme)` and issues the
  same GET/POST as a Kubernetes API request against the pod-proxy
  subresource (`/api/v1/namespaces/{ns}/pods/{scheme}:{pod}:{port}/proxy
  {path}`) instead of dialing the pod directly — the one address this
  works from is the API server, reachable in every deployment shape
  including out-of-cluster (`scripts/e2e-kind.sh`'s own shape), unlike a
  pod's headless-`Service` DNS name or pod IP. `RealAdminClient` (an enum,
  `Direct`/`Proxy`) is what `run()` actually constructs from
  `AdminAccessMode`, keeping `Context<C, A>` monomorphized against one
  concrete `A: AdminOps` type regardless of which mode was chosen at
  startup. Both implementors bound every request with
  `ADMIN_REQUEST_TIMEOUT` — an unroutable pod fails a reconcile step fast,
  never hangs it.
- `src/cluster_api.rs` — `ClusterApi`, the test seam over the `kube::Api`
  calls `controller.rs` performs, plus `RealClusterApi`, its production
  implementor (ADR 0061 rung E1 — see Tests).
- `src/controller.rs` — the thin imperative shell: `reconcile` builds every
  desired child via `desired::*`, server-side-applies each
  (`PatchParams::apply(FIELD_MANAGER).force()`, field manager
  `"animus-operator"`), reads the applied `StatefulSet`'s own
  `status.readyReplicas` to compute `AnimusClusterStatus`, and requeues
  (~30s on success, 15s on error). The genuinely stateful pieces that can't
  be pure functions live here too: the scale-down member-drain sequence
  (`drain_and_remove_node`, talks to a real pod's admin port), the
  `controlNodes`-shrink-rejection check (`previous_applied_control_nodes`,
  reads the previously-applied `ConfigMap` back to recover what was
  actually applied last time — see its own doc for why that, not a status
  annotation, is the source of truth), and, since S-07d, the growth
  machinery that drives an *increase* forward (`advance_control_growth`
  and its helpers — see this file's own S-07d section below).
- `src/main.rs` — two subcommands: `run` (the controller, `kube::Client::
  try_default()` — in-cluster service-account config when running as a pod,
  or the local kubeconfig otherwise) and `crd` (prints the
  `CustomResourceDefinition` as real YAML via `serde_yaml` to stdout — see
  `deploy/operator/README.md` for regenerating the committed
  `deploy/operator/crd.yaml`).

## What's non-obvious

- **The operator's own container image is published (2026-09-02, S-07a).**
  `ghcr.io/animus-db/animus-operator` is built from the root `Dockerfile`'s
  `runtime-operator` stage (`docker build --target runtime-operator .`) and
  pushed by `.github/workflows/image.yml`'s `animus-operator` matrix entry
  on the same tag/push rules as `animusd`; `deploy/operator/deployment.yaml`
  references it as a real image, not a placeholder. This doesn't change how
  `scripts/e2e-kind.sh` runs the controller (still out-of-cluster via a
  `cargo`-built binary, deliberately — see the e2e section below) or how
  local development works.
- **BTreeMap-only, same as every other crate (ADR 0003's determinism rule,
  lint-enforced via `clippy.toml`)** — even though this crate has no `Env`
  seam and nothing here is sim-tested (see the next bullet), the workspace
  lint still applies, and there is a real reason to keep it beyond
  uniformity: every builder function is meant to be a **pure, deterministic**
  `(name, ns, spec) -> object` map — a `HashMap`'s nondeterministic
  iteration order leaking into a generated `ConfigMap`'s JSON (or a label
  map's serialized key order) would make an otherwise-identical reconcile
  produce spurious diffs against the API server on every run.
- **No `Env` seam here — this crate is entirely outside the `animus-env`/
  `animus-sim` determinism story.** It is production-only wiring (real
  `tokio`, a real `kube::Client` talking to a real API server) in the same
  sense `animus-env::ProdEnv` is — nothing here runs under `SimEnv`, and
  nothing here needs to: the *interesting* logic (what a cluster's children
  should look like) is factored into the pure `desired` builders precisely
  so it can be tested without a fault-injecting simulator or a real
  cluster — ordinary `#[test]`s on plain data in, plain data out.
- **The `desired::cluster_config` mirror must be kept in sync with
  `animusd::config::ClusterConfig`/`RoleAddrs` by hand.** There is no shared
  type and no compile-time check tying them together. Whenever
  `crates/animusd/src/config.rs` or `RoleAddrs` (`crates/animusd/src/lib.rs`)
  gains, renames, or removes a JSON field, this crate's `desired::
  cluster_config::{ClusterConfig, RoleAddrs, NodeRole}` needs the matching
  edit — nothing here will fail to compile if it drifts, only the generated
  `ConfigMap` will fail to parse (or silently mean something different) at
  container start. Grep `crates/animusd/CLAUDE.md`'s config.rs entry before
  touching either side.
- **No per-pod port striding, unlike `animusd::config::ClusterConfig::
  generate`.** `animusd`'s own bare-metal/dev generator stripes ports
  across nodes (`base_port + 6*i + offset`) because several node processes
  can share one host IP there. In Kubernetes every pod is its own network
  namespace with its own stable DNS name (`{name}-{ordinal}.{name}-
  internal.{ns}.svc.cluster.local}`, via the headless `Service`), so
  **every pod binds the identical six ports** and `RoleAddrs::
  advertise_host` (not the port) is what makes each entry distinct — see
  `desired::cluster_config`'s own module doc. This is *why* a `Service` can
  give every pod behind it the same numeric `targetPort`; if a future change
  reintroduces per-node port striding here, every `Service` port builder
  breaks with it.
- **`entrypoint_script` only emits a flag the target `animusd` subcommand
  actually parses — checked against `crates/animusd/src/main.rs`'s real CLI
  parser, not its usage-string doc comment**, which can drift (see that
  crate's own module doc for a documented instance of exactly this drift).
  Current support table (re-verify against `main.rs` if either side
  changes):

  | flag | combined (`animusd --config --node`) | data (`animusd data --config --node`) |
  |---|---|---|
  | `--dir` | yes | yes |
  | `--ephemeral` | yes | yes |
  | `--dynamo-auth` | yes | yes |
  | `--backup-store`/`--segment-store`/`--s3-credentials`/`--allow-insecure-s3` (S-04 PR 3, `spec.s3`) | yes | **no** |
  | `--backup-store`/`--segment-store` (S-07b, `spec.backupStore`/`spec.segmentStore` — the non-S3 `cluster`/`fs:`/`dir:` forms) | yes | **no** |

  **The S-04 PR 3 row's "no" on the data branch is a pre-existing
  `animusd` gap, not introduced here**: `run_data_config` (`main.rs`)
  accepts none of those four flags — see that function's own "same
  documented gap as `--backup-store`" comment. A data-only pod still
  mounts `spec.s3.credentialsSecretName`'s `Secret` at `/etc/animus/s3`
  like every other pod (`desired::statefulset::build` doesn't condition
  the mount on role), it just never reads it. **S-07b's own row is the
  identical gap** — `spec.backupStore`/`spec.segmentStore` reach only the
  combined branch for the same reason, but need no `Secret`/volume at all:
  a `fs:`/`dir:` path is required to live under `DATA_DIR`, which every
  pod already has mounted.

  **`spec.autoSplitBytes`/`spec.quiesceAfterSecs` are never emitted as CLI
  flags on either branch (S-06)** — both now reach `animusd` through
  `build_cluster_config`'s own `cluster_settings` section of the generated
  `cluster.json` instead (`desired::cluster_config::ClusterSettings`, a
  mirror of `animusd::config::ClusterSettings`), which every pod reads
  regardless of which `animusd` subcommand it execs. This is strictly
  better than a CLI flag for both fields: `quiesceAfterSecs` used to reach
  only the combined branch (data-role pods had no route to quiescence at
  all, pre-S-06 on the `animusd` side); `autoSplitBytes` never reached
  either branch as a flag (only `--cluster N`'s dev-only in-process mode
  ever accepted `--auto-split-bytes`). Emitting `--quiesce-after` here
  **on top of** the config section would in fact be a hard `animusd`
  startup error on the combined branch — its CLI flag and the config
  file's own section setting the same field is refused, not silently
  reconciled (`resolve_cluster_settings` in `animusd`'s own `main.rs`); do
  not reintroduce it as a flag without removing it from the emitted
  section, or vice versa.
  **`desired::cluster_config::ClusterSettings` also mirrors
  `throttle_read_units`/`throttle_write_units` (ADR 0065 §5(a), W-08 step
  4) — shape parity only, the same precedent `auto_split_ops_rate` (W-09)
  already set**: no `AnimusClusterSpec` field exposes either yet, so
  `build_cluster_config` never populates them (`cluster_settings_throttle_
  fields_are_never_populated_by_this_crate` pins this), and there is no
  `spec.throttle*` CRD surface to add here until a future ADR/PR wires one
  up.
  **`--split-mode` no longer exists (fixed, #590)**: `crates/animusd/src/
  main.rs`'s own module doc states the flag and the copy-based split
  workflow it selected were deleted outright (2026-09-01, ADR 0058's rung 4
  layer) — `main.rs`'s CLI parser rejects `--split-mode` as unknown on
  every subcommand. `entrypoint_script` used to emit it unconditionally on
  the combined branch whenever `AnimusClusterSpec.split_mode` was set,
  which made any cluster spec setting `splitMode` fail at pod startup;
  `split_mode` has been removed from `AnimusClusterSpec` entirely (there is
  no back-compat promise in this repo, ADR 0060/root `CLAUDE.md`), so
  there is no flag left to conditionally emit. See
  `entrypoint_flags_are_all_accepted_by_animusd` in `desired::
  cluster_config`'s tests for the regression coverage (every `--flag`
  token the script emits is checked against an explicit allowlist of what
  `main.rs` actually accepts).
- **The `StatefulSet` pod template carries a config-hash restart annotation
  (`desired::statefulset::CONFIG_HASH_ANNOTATION`, S-07d groundwork,
  2026-09-06) — hashed from a *restart-relevant projection*, never the raw
  generated `ConfigMap`.** A mounted `ConfigMap` volume's content updates in
  place on the kubelet's own sync period, but nothing makes an
  already-running `animusd` process re-read it — `cluster.json` is a
  startup-time config file, not hot-reloaded — so a config-affecting spec
  change (`spec.tls`, `spec.s3`/`backupStore`/`segmentStore`,
  `spec.dynamoAuthSecretName`, `spec.quiesceAfterSecs`/`autoSplitBytes`, or
  a `spec.controlNodes` role-split change) used to sit unapplied on running
  pods until they happened to restart for an unrelated reason. Baking a
  hash into the pod template turns such a change into a `spec.template`
  change, which the `StatefulSet` controller rolls exactly like an image
  bump.
  **The rule for what goes into the hash: hash exactly what a running pod
  read once at boot and cannot pick up live — never the node list, its
  length, or any per-node address/id/`advertise_host`.** The first cut of
  this (this same commit's original version) hashed the entire generated
  `ConfigMap` `data` map, which put `cluster.json`'s whole `nodes` array —
  including every *existing* node's unchanged entry — into the hash; a
  plain `spec.nodes` scale-up/down (which only appends/removes a trailing
  `RoleAddrs` entry, per `scale_up_config_append_preserves_existing_
  entries_byte_for_byte`) therefore rolled every already-running pod for no
  reason, even though a running `animusd` never rereads that array — it
  learns of new/changed peers through replicated `Metadata` (ADR 0030
  self-registration) only. This broke `e2e-kind-tls`: the scale phase's
  3 → 4 node growth rolled `e2e-0`/`e2e-1`/`e2e-2` out from under the
  script's own `kubectl port-forward`, failing the post-scale `GetItem`.
  Fixed by hashing `desired::statefulset::restart_relevant_projection`
  instead — a small typed struct built straight from `AnimusClusterSpec`
  (not by string-munging the generated JSON): the `control_nodes`
  role-split threshold and the full `entrypoint.sh` text (both already
  independent of `spec.nodes` — `entrypoint_script` takes only `spec`),
  `cluster_settings` (`cluster_settings_or_none`, the same "empty means
  absent" rule `build_cluster_config` uses), and whether TLS is wired at
  all (`spec.tls.is_some()` plus the fixed `tls_section()` mount paths).
  **A `spec.controlNodes` increase still changes this hash and rolls every
  pod** — role is purely `ordinal < control_nodes`, so raising the
  threshold can flip an *existing* ordinal's role even though no node
  address changed, and that pod needs a restart to pick up its new
  subcommand/flags; this is exactly the case S-07d's growth flow expects to
  restart pods for. Hashed with FNV-1a 64 (inline, no new dependency) over
  the projection's JSON encoding — deliberately **not**
  `std::collections::hash_map::DefaultHasher`, which carries no
  cross-Rust-release stability guarantee and would risk rolling every
  deployed cluster's pods on a routine operator toolchain bump. See
  `desired::statefulset`'s own module doc and
  `restart_relevant_projection`'s doc for the full field-by-field in/out
  list, and `docs/engineering-lessons.md`'s S-07d entries for the general
  lesson ("a shared `StatefulSet` pod-template annotation restarts every
  pod — hash only what a running pod cannot pick up live, never the thing
  that changes on every routine scale").
- **`control_nodes_changed` reads the *previous* `ConfigMap`'s own applied
  `cluster.json` back to detect an immutable-field change**, rather than a
  status annotation the controller would have to remember to write and keep
  in sync — the applied `ConfigMap` is already server-side-apply's own
  durable record of what was actually generated last reconcile, so there is
  nothing separate to keep consistent. It infers the *previous*
  `controlNodes` value from a prefix count of `role: "both"` entries in that
  JSON (role is assigned strictly by `ordinal < control_nodes`, so the count
  of leading `"both"` entries **is** the previous `controlNodes` value) —
  see `desired::cluster_config::build_cluster_config`'s own doc for why that
  invariant holds.
- **`previous_applied_control_nodes` (S-07d, renamed from
  `control_nodes_changed`) reads the *previous* `ConfigMap`'s own applied
  `cluster.json` back**, rather than a status annotation the controller
  would have to remember to write and keep in sync — the applied
  `ConfigMap` is already server-side-apply's own durable record of what was
  actually generated last reconcile, so there is nothing separate to keep
  consistent. It infers the *previous* `controlNodes` value from a prefix
  count of `role: "both"` entries in that JSON (role is assigned strictly by
  `ordinal < control_nodes`, so the count of leading `"both"` entries **is**
  the previous `controlNodes` value) — see `desired::cluster_config::
  build_cluster_config`'s own doc for why that invariant holds. Unlike its
  pre-S-07d self, it no longer compares against the *desired* value at all
  (hence the rename) — it always reports the prior applied value, changed
  or not, since both the shrink-rejection check and the growth machinery
  need it, and growth needs to keep observing it every reconcile while a
  growth is in flight, not just on the one reconcile where the spec edit
  first lands.
- **Scale-down drains highest-ordinal-first, one pod fully removed before
  the next starts**, and stops the whole sequence (setting the
  `DrainFailed` condition, leaving the `StatefulSet`'s replica count
  untouched) on the first failure — never a partial multi-pod drain in
  flight, and never scales the `StatefulSet` down past a pod that hasn't
  finished draining. Talks to a pod's admin port through the headless
  internal `Service`'s own per-pod DNS name (`{name}-{ordinal}.{name}-
  internal.{ns}.svc.cluster.local:{admin_port}`), the same address
  `RoleAddrs::advertise_host` carries — reusing it here rather than reading
  a pod IP keeps the drain sequence correct across a pod restart mid-drain
  (the DNS name is stable; a pod IP is not).
- **No finalizer in v1** (`controller.rs`'s own module doc): deletion relies
  entirely on Kubernetes GC following the `controller: true` owner
  references every child carries. There is nothing external an
  `AnimusCluster` owns (no backup store, no DNS record) for a finalizer to
  clean up, so this is a deliberate scope cut, not a documented gap to close
  later.
- **`kube`'s `rustls-tls` feature (no OpenSSL, matching the workspace's
  crypto posture elsewhere — ADR 0057's SigV4 RustCrypto choice) pulls
  `rustls-platform-verifier` -> `webpki-root-certs`, a bundled-root-CA data
  crate under CDLA-Permissive-2.0** — not on `deny.toml`'s original
  allow-list (every other license there is a code license; this is a data
  license). Added with an explicit comment rather than silently passing;
  see that file if `cargo deny check` ever flags it again after a `kube`
  version bump changes its dependency shape.

## TLS (ADR 0064 commit 3)

`AnimusClusterSpec.tls: Option<TlsSpec>` (`crd.rs`), two mutually exclusive
shapes validated by `TlsSpec::validate` — called from `crate::validate::
validate_spec`, shared by `crate::controller::reconcile`'s own condition-
based fallback and the validating webhook (`crate::webhook`, S-07e/ADR
0070, which rejects the write itself at write time when installed — see
this file's own "Admission webhook" section below): `secretName` (a
pre-existing `kubernetes.io/tls` `Secret` an operator user issued and
placed by hand) or `certManager` (`issuerRef` + optional
`duration`/`renewBefore`, referencing an already-existing `Issuer`/
`ClusterIssuer` this operator never creates). Both or neither set is
rejected: a `TlsSpecInvalid` status condition, and the rest of that
reconcile proceeds with TLS stripped (see `reconcile`'s own early check)
rather than getting stuck on one bad field.

Either shape resolves to the same `Secret` name
(`TlsSpec::secret_name_or_default`) and the same downstream wiring:

- `desired::certificate::build` creates a `Certificate` (a sixth
  `apply_children` child, applied only for the `certManager` shape) whose
  SAN list (`dns_names`) covers every pod's own FQDN plus both Services.
- `desired::statefulset::build` mounts the resolved `Secret` read-only at
  `/etc/animus/tls` on every pod — identical mount for either shape.
- **2026-09-05**: `desired::statefulset::build` also switches the
  readiness/liveness probes' `GET /admin/health` to `scheme: HTTPS` when
  `spec.tls` is set (admin is server-only TLS, so a plaintext kubelet probe
  against a TLS-only listener fails the handshake server-side every probe
  period and the pod never goes Ready — the bug the `e2e-kind-tls` job's
  first real CI run caught, run 33963360763). The kubelet's HTTPS probe
  scheme does not verify the server certificate, so this needs no CA
  plumbed into the kubelet itself.
- `desired::cluster_config::build_cluster_config` gives every node's
  `RoleAddrs` the identical `TlsSection` (`tls_section()`), pointing at
  `/etc/animus/tls/{tls.crt,tls.key,ca.crt}` — baked into the generated
  `cluster.json`, not per-pod `--tls-*` flags (`animusd`'s own
  `--config`/`--node` entry point is what this operator always generates —
  see the flag-support table above — and `ClusterConfig::validate_tls`'s
  whole-*file* check, not commit 2's own per-process gap, is what this
  shape sidesteps by construction).
- `crate::controller::reconcile`'s scale-down drain sequence reads the
  resolved `Secret`'s `ca.crt` via `ClusterApi::get_secret` (the Kubernetes
  API — RBAC `secrets: get/list/watch`, `deploy/operator/rbac.yaml`) and
  passes it to `AdminOps::post_json`/`get_json` as `ca_pem`, switching
  `admin_base_url`'s scheme to `https`. Reading through the API rather than
  mounting a file into the *operator's own* pod is deliberate: it works
  identically whether the operator runs in-cluster
  (`deploy/operator/deployment.yaml`) or out-of-cluster via `cargo run -p
  animus-operator -- run` against a local kubeconfig (what
  `scripts/e2e-kind.sh` does) — both reach the API server, neither needs a
  filesystem mount of its own.

**`scripts/e2e-kind.sh --tls` path (`E2E_TLS=1`) is UNVERIFIED in this
sandbox** — `kind` cannot come up here at all (see the e2e section's own
`CAP_SYS_RESOURCE` note), so the TLS-specific script additions (cert-manager
install, a self-signed `ClusterIssuer`, `spec.tls.certManager` on the
manifest, waiting on the `Certificate`'s own `Ready` condition, and
`curl --cacert --resolve` against the dynamo Service's own SAN) have been
written carefully and `bash -n`-checked, but never run end to end. Treat a
first real CI failure on the `e2e-kind-tls` job as this path finding its
first real bug, not as this note being wrong.

## S3 (S-04 PR 3, closes `docs/roadmap.md`'s S-04)

`AnimusClusterSpec.s3: Option<S3StoreSpec>` (`crd.rs`) mirrors `TlsSpec`'s
own precedent — a CRD section that only *references* a pre-existing
`Secret`, never one this operator creates or writes. `backupStore`/
`segmentStore` are literal `s3://...` URI values (at least one must be
set); `credentialsSecretName` names a `Secret` holding `access_key_id`/
`secret_access_key`; `allowInsecureHttp` (default `false`) must be `true`
for either store URI to set `insecure_http=true`; `egressCidrs` (default
`["0.0.0.0/0"]`) scopes the generated `NetworkPolicy`'s S3 egress rule.
`S3StoreSpec::validate` (called from `crate::validate::validate_spec`, same
shared-validator posture as `TlsSpec::validate` above) rejects:
neither store set, an empty `credentialsSecretName`, a URI `crate::
s3_uri::parse` can't make sense of, or `insecure_http=true` without
`allowInsecureHttp` — a `S3SpecInvalid` status condition, `spec.s3`
stripped for the rest of that reconcile, same as an invalid `spec.tls`.

**`crate::s3_uri`, not `animusd`'s own `parse_s3_uri`.** This crate
doesn't depend on `animusd` (this file's own "does not depend on
`animusd`" note above), so `S3StoreSpec::validate` and `desired::
networkpolicy`'s port-extraction both go through a small, deliberately
narrower syntactic check (`crate::s3_uri::parse`: bucket present,
`endpoint=` query key present, `http://`/`https://` scheme) — see that
module's own doc for exactly what it does and does not re-verify. The
real credential/region/loopback-vs-`--allow-insecure-s3` logic is
`animusd`'s own, at node startup, unchanged.

Downstream wiring:

- `desired::statefulset::build` mounts `credentialsSecretName`'s `Secret`
  read-only at `/etc/animus/s3` on **every** pod, combined or data-role
  alike (the same "one shared Secret, every pod" shape `dynamo-auth`/`tls`
  already use) — even though only a combined-role pod's own
  `entrypoint.sh` branch reads it (see the flag-support table above for
  why: `animusd data --config` accepts no S3-store flags today).
- `desired::cluster_config::entrypoint_script` adds, **only on the
  combined branch**: a preamble (before the `exec`) that reads the
  mounted `Secret`'s two files at container-start time and writes
  `/tmp/animus-s3-credentials.json` — `{"access_key_id": "<read from the
  mount>", "secret_access_key_file": "/etc/animus/s3/secret_access_key"}`
  — followed by `--s3-credentials /tmp/animus-s3-credentials.json`,
  `--allow-insecure-s3` (when `allowInsecureHttp`), and `--backup-store`/
  `--segment-store` (each single-quoted via `shell_single_quote` — an
  `s3://...` URI's own query string contains `&`/`?`, shell-special
  characters that would otherwise be misinterpreted, `&` in particular
  backgrounding the `exec`). **The `Secret`'s value never appears in the
  generated `ConfigMap`** — only a shell command that reads it at
  container-start time, inside the pod, and a *path* to
  `secret_access_key`; `access_key_id` gets the identical treatment even
  though it's the less sensitive of the two.
- `desired::networkpolicy::build` — see the egress paragraph below.

**Egress, closing the roadmap's own "egress unrestricted by omission"
line.** The generated `NetworkPolicy` now sets `policyTypes: [Ingress,
Egress]` unconditionally, with two baseline `Egress` rules on *every*
cluster (`spec.s3` or not): intra-cluster (this cluster's own pods, the
`internal`+`intra` ports only) and DNS to `kube-system`'s `kube-dns`/
CoreDNS pods (UDP+TCP 53 — without this, in-cluster name resolution
itself breaks the moment egress stops being wide open). A third rule
is added only when `spec.s3` is set: the configured store URIs'
`endpoint=` port(s) (deduplicated via `desired::networkpolicy::
s3_endpoint_ports`; 443/80 default by scheme absent an explicit port),
scoped to `spec.s3.egressCidrs`. **`NetworkPolicy` cannot express a
hostname allowlist** — this operator has no way to resolve an endpoint's
hostname into the right CIDR itself, which is exactly why `egressCidrs`
exists and defaults open (`["0.0.0.0/0"]`): narrow it to your object
store's real address range — `deploy/operator/example.yaml`'s commented
`s3:` section says so inline.

## Non-S3 stores (S-07b, closes `docs/roadmap.md`'s S-07 item b)

`AnimusClusterSpec.backup_store`/`.segment_store: Option<String>` (`crd.rs`)
are the CRD surface for the *non-S3* forms `spec.s3` above doesn't cover —
carrying the literal `--backup-store`/`--segment-store` flag value
verbatim (`"cluster"`, `"fs:<path>"`, `"dir:<path>"`), unlike `S3StoreSpec`
which is a sub-object. `AnimusClusterSpec::validate_store_spec` (called
from `crate::validate::validate_spec`, same shared-validator posture as
`TlsSpec`/`S3StoreSpec`'s own `validate`) accepts exactly
`"cluster"` or `"fs:<path>"` for `backupStore`, and only `"dir:<path>"`
for `segmentStore` — **`animusd`'s own `--segment-store` has no `"cluster"`
keyword at all** (`parse_segment_store`'s own doc: omitting the flag is
the *only* way to select its default), so a literal `"cluster"` there is
rejected rather than silently remapped to "omit the flag." An `s3://...`
value in either field is rejected pointing at `spec.s3` instead (only that
section supplies the credentials an S3 store needs), and setting the same
store in both `spec.s3` and the matching top-level field is rejected as a
conflict naming both — a `StoreSpecInvalid` status condition either way,
`backupStore`/`segmentStore` stripped for the rest of that reconcile.

**Every `fs:`/`dir:` path must live strictly under
`desired::cluster_config::DATA_DIR`** (`/var/lib/animus`) — the one
directory every pod already has mounted (a `PersistentVolumeClaim`, or an
`emptyDir` when `spec.storage.ephemeral` is set), and never `DATA_DIR`
itself (that's where `animusd --dir` puts the storage engine's own
on-disk files; a store sharing that exact root would mix its own objects
in among them). **No new volume or `Secret` for this** — unlike `spec.s3`,
`cluster`/`fs:`/`dir:` need no credentials, so `desired::statefulset`
needed no change at all: the pod's already-mounted data volume is
sufficient.

`desired::cluster_config::entrypoint_script` emits `--backup-store`/
`--segment-store` from whichever of `spec.s3.{backup,segment}Store` (the
`s3://...` form, credentials wired as described above) or the plain
top-level `spec.{backup,segment}Store` is set — the two are mutually
exclusive per store (enforced by `validate_store_spec` before this
function ever runs on an invalid combination), so the builder itself just
takes the first `Some` of the two, `.or()`-chained. Both values are
`shell_single_quote`d like every other operator-controlled string
interpolated into the generated `sh` script (`fs:`/`dir:` paths happen not
to contain shell metacharacters today, but nothing about the type says
they can't). Reaches only **combined-role pods**, the identical
pre-existing `animusd` gap `spec.s3` documents in the flag-support table
above.

**`scripts/e2e-kind.sh`'s `E2E_S3=1` leg is UNVERIFIED in this sandbox**
— same `CAP_SYS_RESOURCE` reason `E2E_TLS`'s own leg is (see the e2e
section below): it deploys a single-pod MinIO + Service, creates the
bucket via a throwaway `minio/mc` pod and the credentials `Secret`,
applies `spec.s3.backupStore` pointing at `http://minio.<ns>.svc:9000`
with `allowInsecureHttp: true`, then exercises `CreateBackup`/
`DescribeBackup` over the DynamoDB wire and checks `GET
/admin/backup-store` reports `"kind":"s3"`. Written carefully and
`bash -n`-checked, never run end to end anywhere — treat a first real CI
failure on the `e2e-kind-s3` job as this leg finding its first real bug.

## Encryption at rest (ADR 0069, S-03 PR 3, closes `docs/roadmap.md`'s
S-03 — S-03 is now complete)

`AnimusClusterSpec.encryption_key_secret_name: Option<String>` (`crd.rs`)
mirrors `dynamo_auth_secret_name`'s own flat-field shape (not `TlsSpec`'s
nested two-shape one — there is no second way to *obtain* this key the
way cert-manager offers a second way to *issue* a TLS cert, so a nested
object would only add ceremony): names a pre-existing `Secret` (same
namespace) this operator only ever mounts, **never** generates, inspects,
or stores. The `Secret` must carry the raw key under one well-known data
key, `desired::cluster_config::ENCRYPTION_KEY_SECRET_DATA_KEY` (`"key"`)
— the 64-hex-character format `animus_env::EncryptionKey::
load_from_file` parses.

Downstream wiring, all mirroring `spec.tls`'s own precedent:

- `desired::statefulset::build` mounts the `Secret` read-only at
  `desired::cluster_config::ENCRYPTION_KEY_MOUNT_DIR`
  (`/etc/animus/encryption`) on **every** pod regardless of role, with
  `defaultMode: 0o444` (`desired::statefulset::
  ENCRYPTION_KEY_SECRET_DEFAULT_MODE`) — world-readable, no write bit for
  anyone. Tighter than `0o440`/`0o400` deliberately: this pod spec sets no
  `securityContext.fsGroup`, and a `Secret` volume's files are owned
  `root:root` by default, so dropping the "other" read bit would make the
  key unreadable by the non-root `animus` user the `animusd` image runs
  as (`Dockerfile`'s `USER animus:animus`).
- `desired::cluster_config::build_cluster_config` gives every node's
  `RoleAddrs` the identical `encryption_key_path`
  (`desired::cluster_config::encryption_key_mount_path()` =
  `/etc/animus/encryption/key`) when the field is set — the exact
  `tls_section()` precedent (a fixed, mount-path-only value, identical
  across every node by construction). **Never a `--encryption-key` CLI
  flag** — every pod this operator generates already runs `--config`/
  `data --config` against a `cluster.json` whose own node entry can carry
  the field directly (ADR 0069 PR 1's own config-field hook), so the
  config-file route was already complete for this operator's deployment
  shape; emitting the flag on top would in fact be a hard `animusd`
  startup error (the config file's own section and the flag both setting
  it is refused, not silently reconciled — the same "one way, not both"
  contract `--dynamo-auth`/`--quiesce-after` already document).
- `desired::statefulset::RestartRelevantConfig` gained an
  `encryption_key_path: Option<String>` field, `#[serde(skip_
  serializing_if = "Option::is_none")]` — deliberately **not** `tls`'s own
  always-serialize-as-`null` shape, so a spec with the field unset
  (every cluster that predates this PR) serializes byte-identically to
  before the field existed and the pinned config-hash fixture test needed
  no literal update. Only the field's *presence* participates in the
  hash (mapped to the one fixed mount path, never the `Secret`'s own
  name) — adding/removing it rolls every pod (a running `animusd` reads
  `RoleAddrs::encryption_key_path` once at boot, never live); renaming the
  referenced `Secret` under an unchanged presence state rolls pods too,
  but through the volume's own `secretName` diff, not through this hash
  (mirrors `spec.tls`'s own secret-name-excluded reasoning exactly);
  rotating the `Secret`'s own *content* under an unchanged name never
  rolls anything, at either layer — correct, since ADR 0069 has no
  in-place re-encryption mechanism to roll a pod *into*.

**Validated live, unlike every other `*SpecInvalid` check in this
crate — and deliberately NOT stripped on failure.**
`crate::controller::validate_encryption_key_secret` reads the named
`Secret` back through `ClusterApi::get_secret` on every reconcile (a
`Secret` *reference*'s only checkable property is whether it actually
exists, which the spec alone can never say — unlike `TlsSpec::validate`/
`S3StoreSpec::validate`, pure functions with no cluster access) and sets
`crd::CONDITION_ENCRYPTION_KEY_SECRET_INVALID` naming exactly what's
wrong (missing entirely, or present without the `"key"` data key). Unlike
`TlsSpecInvalid`/`S3SpecInvalid`/`StoreSpecInvalid` — each of which
strips its own field and reconciles the rest of the spec as if it were
unset — this check leaves `spec.encryptionKeySecretName` in place either
way: falling back to "as if unset" would regenerate a plaintext
`cluster.json` for a cluster whose data directory may already be
encrypted, which (via the config-hash annotation) would roll every pod
straight into ADR 0069's own loud "no key against an encrypted
directory" startup refusal — a `CrashLoopBackOff` this operator would
have actively caused, not merely failed to prevent. Leaving the spec's
own still-referencing desired state in place instead means a genuinely
missing `Secret` just leaves the pod `ContainerCreating` (harmless,
self-healing once the `Secret` exists) with the condition explaining why.
**Removing the field from an already-encrypted cluster is the symmetric,
equally uncovered case** — the operator applies the edit as given (volume
gone, `cluster.json` field gone, pods roll), and each restarted `animusd`
hits PR 1's identical refusal on its own, at its own startup, the only
place that mismatch can correctly be caught; the operator does not, and
structurally cannot, second-guess an operator-authored spec edit here.

**A documentation/code gap found, not fixed, while landing this PR**: ADR
0069 states `/admin/config` reports `encryption_key_path` as a path
string. Tracing `animusd::admin::config_view`/`AdminInfo` while grounding
this PR found that field was never actually added there — `/admin/config`
reports nothing about encryption at all today (the safer of the two
possible drifts, but still a stale claim, not a security bug). Not fixed
here (out of an operator-only PR's scope — see ADR 0069's own "As-built:
PR 3" amendment for the full note); `scripts/e2e-kind.sh`'s own
`E2E_ENCRYPTION=1` leg checks the actual invariant instead (the raw
`/admin/config` response body never contains the key hex material),
which holds regardless of whether a future PR adds that field.

**`scripts/e2e-kind.sh`'s `E2E_ENCRYPTION=1` leg is UNVERIFIED in this
sandbox** — same `CAP_SYS_RESOURCE` reason `E2E_TLS`/`E2E_S3`'s own legs
are (see the e2e section below): creates the `Secret` (a freshly
generated key via `openssl rand -hex 32`, never logged), sets
`spec.encryptionKeySecretName`, then — after the ordinary PutItem/GetItem
round trip — `kubectl exec`s into the serving pod and `grep -r`s its own
data directory for the plaintext item value (must be absent) and checks
`GET /admin/config`'s raw response body never contains the key hex.
Written carefully and `bash -n`-checked, never run end to end anywhere —
treat a first real CI failure on the `e2e-kind-encryption` job as this
leg finding its first real bug.

**No CRD field or code touches the default replicated `cluster` segment/
backup store** — that gap (issue #680) sits entirely in `animus-cp-data`,
outside anything this operator's mount could influence either way, and
`issue #676` (several per-node flags, `--encryption-key` among them, not
threaded through `animusd join`/`data --seed`/`--cluster-control`+
`--cluster-data`) is likewise irrelevant to this operator, which never
generates those invocations.

## PodDisruptionBudget (S-07c, closes `docs/roadmap.md`'s S-07 item c and
this crate's own ADR 0060 deferred-list bullet)

`crate::desired::poddisruptionbudget::build` produces a `{name}-pdb`
`PodDisruptionBudget` for every `AnimusCluster`, owner-referenced and
selecting `selector_labels(name)` like every other child, applied
unconditionally in `apply_children` (after the `NetworkPolicy`, before the
`StatefulSet`) — a **required** child, not an optional one the way
`spec.tls.certManager`'s `Certificate` is.

`maxUnavailable` is computed, never a constant and never
CRD-overridable:

```
maxUnavailable = min(
  floor((controlNodes - 1) / 2),                         # control-plane quorum
  floor((min(nodes, MAX_REPLICATION_FACTOR) - 1) / 2),    # data-plane tablet RF
)
```

`MAX_REPLICATION_FACTOR = 3` mirrors `animusd::MAX_REPLICATION_FACTOR`
(`crates/animusd/src/lib.rs`) by hand as `desired::poddisruptionbudget::
DATA_PLANE_MAX_REPLICATION_FACTOR` — this crate has no dependency on
`animusd` (this file's own note above), and there is no `spec`-level
replication-factor override today, so this is the same manual-sync
posture `desired::cluster_config`'s `ClusterConfig`/`RoleAddrs` JSON
mirror already established (grep `crates/animusd/src/lib.rs`'s own
`MAX_REPLICATION_FACTOR` before touching either side). `nodes`/
`control_nodes` are each clamped to at least `1` first, so `safe_max_
unavailable` is a total function that never panics or goes negative even
on a not-yet-valid spec.

**Degenerate shapes all compute `0` — correctly, not as a bug**:
`nodes == 1`, `controlNodes == 1`, and `nodes < MAX_REPLICATION_FACTOR`
each block every voluntary eviction outright, because such a cluster
cannot survive losing its one live copy of a control-plane or data-plane
majority. Once `nodes` and `controlNodes` each reach `3` (this operator's
own default), the value plateaus at `1` for any larger `nodes` — this is
*why* the builder uses `maxUnavailable` rather than `minAvailable`
(mutually exclusive on a `PodDisruptionBudgetSpec`): `minAvailable` would
have to be re-derived as `nodes - maxUnavailable` on every scale event,
while `maxUnavailable` is scale-invariant across the entire range that
matters in practice. `apply_children` still re-derives and re-applies it
every reconcile regardless (the same unconditional-re-apply posture every
other required child has), always from the **desired** spec (`spec.
nodes`/`spec.controlNodes`), never the `StatefulSet`'s live replica
count — a scale-down transition test (`controller::tests`) pins that a
cluster previously scaled to 5 replicas immediately gets the *new*,
stricter budget on a reconcile to a smaller `nodes`/`controlNodes`, not
the stale prior shape's looser one. **One exception since S-07d**: while a
`controlNodes` growth is still catching up, `apply_children` builds the
`PodDisruptionBudget` from the live-confirmed *achieved* count instead of
the full target — see this file's own S-07d section below for why.

**No CRD field was added.** The safe value is already a pure, fully
determined function of two existing spec fields; an override could only
loosen it (unsafe, must be rejected), tighten it (already achievable via
`spec.nodes`/`spec.controlNodes` directly), or disable it outright, which
would make this the first required child this operator ever stops
applying once a spec says so — there is no finalizer or deletion path
(`crate::controller`'s own "no finalizer in v1" doc) for a child that
used to be desired and no longer is. See ADR 0060's own 2026-09-06
amendment for the full reasoning, including what a future override would
need to answer first (its own deletion story) before being added.

`deploy/operator/rbac.yaml` grants the `policy` API group's
`poddisruptionbudgets` the same full verb set as every other owned kind;
`run()` watches it via `.owns(Api::<PodDisruptionBudget>::all(..))` like
the other four typed child kinds.

## Control-voter growth (S-07d, closes `docs/roadmap.md`'s S-07 item d and
this crate's own ADR 0060 deferred-list bullet)

`spec.controlNodes` is **grow-only**: a decrease is still rejected outright
(unchanged behavior, renamed condition —
`crd::CONDITION_CONTROL_NODES_SHRINK_REJECTED`, was `ImmutableFieldChanged`
via `control_nodes_changed`); an increase is driven forward by the
controller itself, one voter at a time, automating exactly the ADR 0037
admin path (`animus admin control-add`) a human operator used to run by
hand. See ADR 0060's own "Control-voter growth (S-07d, 2026-09-06)"
amendment for the full design write-up (the live-truth-driven sequence,
why role-promotion needs a restart, why growth doesn't reopen genesis's
own "sequential join" rejection, the `SocketAddr` gap and its workaround,
"retry on the leader" without a leader address hint, the PDB interaction,
and how a controller restart resumes) — this section is the crate-local
pointer + the gotchas worth knowing before touching this code.

**The pure decision core**: `next_growth_ordinal(cluster_name, target,
voters)`/`achieved_control_nodes(cluster_name, target, voters)` in
`controller.rs` are plain functions of `(name, target, voters)` — no I/O,
fully unit-tested — turning the control group's own live voter-id set
(`GET /admin/control/members`'s `"voters"` field, parsed by
`parse_voters`) into "which ordinal is missing next" / "how many are
already confirmed". Everything async around them
(`fetch_control_members`/`discover_control_voters`/
`ordinal_reports_role_both`/`resolve_control_dial_addr`/
`add_control_voter`/`advance_control_growth`) is a thin orchestration
layer exercised through `FakeAdminClient`/`FakeClusterApi` — see Tests
below.

**Config-hash restart annotation, a separate, independently-reviewable
groundwork step (its own first commit)**: `desired::statefulset::
CONFIG_HASH_ANNOTATION` bakes a content hash of the generated config's
*restart-relevant projection* (everything a pod reads at boot and cannot
learn live — never the node list, so a `nodes`-only scale never rolls
pods) into the pod template, turning any config-affecting spec change into a
`StatefulSet.spec.template` change — the only way to make an already-
running pod actually notice `cluster.json` changed, since `animusd` only
reads it at container start. **This restarts every pod on any
ConfigMap-affecting change, not just a `controlNodes` growth** — `spec.tls`,
`spec.s3`, `spec.backupStore`/`spec.segmentStore`,
`spec.dynamoAuthSecretName`, `spec.quiesceAfterSecs`/`spec.autoSplitBytes`
all silently sat unapplied on already-running pods before this landed;
now every one of them triggers a real rolling restart too. Not gated
behind `controlNodes` specifically — there was no clean way to restart
*only* the newly-promoted ordinals anyway (the pod template is shared
across every ordinal), so this is the simplest correct mechanism, not a
narrowly-scoped one.

**`ClusterApi::get_pod_ip` is this crate's first real consumer of the
`pods: get/list/watch` RBAC grant** `deploy/operator/rbac.yaml` already
carried (pre-provisioned for "the controller reads pod status/conditions"
in general, never actually exercised before S-07d) — no RBAC change was
needed. It reads a promoted ordinal's live `status.podIP` via the
Kubernetes API, **not a DNS lookup** — seeded via `FakeClusterApi::
seed_pod_ip` in tests, unlike a raw `tokio::net::lookup_host` call, which
would bypass the seam entirely and make this untestable without a real
cluster. See ADR 0060's own "The `SocketAddr` gap" subsection for why this
lookup exists at all (a real, pre-existing `animusd` admin-API limitation
this crate works around rather than fixes).

**`FakeAdminClient` (S-07d additions, `fakes.rs`)**: `seed_control_voters`/
`control_voters()` back `GET /admin/control/members` with a plain
`BTreeSet<String>` a test can seed and later inspect (grown in place by a
successful fake `POST .../member/add`, unlike the drain-status queue
above, which is consumed); `mark_ordinal_ready_both(ordinal)` makes
`GET /admin/config` report `role: "combined"` for that ordinal
specifically — `animusd`'s own real `AdminInfo.role` literal (never
`"both"`, which is `desired::cluster_config::NodeRole::Both`'s unrelated
spelling on the generated `cluster.json`; a 2026-09-06 fix corrected both
this fake and `controller::ordinal_reports_role_both`'s own comparison
after the mismatch made growth wait forever in CI, see this file's own
S-07d section and ADR 0060's own "2026-09-06 correction" amendment) —
parsed out of the request URL's own `{name}-{ordinal}.` host prefix via
`ordinal_from_url`, since every admin call here is already addressed
per-ordinal that way — and `"data"` for every other; `fail_control_members`
makes `GET /admin/control/members` fail for every ordinal, exercising
`advance_control_growth`'s "can't observe live truth" stall-visibility
path; `fail_add_control_
member_for_ordinal(ordinal)` makes `POST .../member/add` refuse when
dialed against that one ordinal's own admin port specifically — the
retry-a-different-voter test needs a *per-ordinal* failure, not the
blanket `fail_drain`/`fail_remove` shape the pre-existing scale-down tests
use.

## Admission webhook (S-07e, closes `docs/roadmap.md`'s S-07 item e and
ADR 0060's own deferred list — S-07 is now fully closed; ADR 0070)

Two new modules, both `pub` at the crate root (`lib.rs`):

- **`src/validate.rs`** — `validate_spec(old: Option<&AnimusClusterSpec>,
  new: &AnimusClusterSpec) -> Result<(), Vec<Violation>>`, a pure function
  (no cluster access, no `async`) checking every CRD-shape rule this crate
  enforces purely from the spec: `spec.nodes >= 1` and `spec.controlNodes`
  (resolved) `>= 1`/`<= spec.nodes` (both new — the CRD's own doc comment
  claimed `nodes >= 1` but nothing enforced it before this PR),
  `spec.controlNodes` never decreasing from `old`'s own resolved value
  (`old` is `None` on a CREATE review, or when the reconciler has no
  previously-applied `ConfigMap` yet), and `TlsSpec::validate`/
  `S3StoreSpec::validate`/`AnimusClusterSpec::validate_store_spec` — the
  same three methods `crd.rs` already defined, called from here rather than
  reimplemented, so this crate has exactly one place any of these six
  rules can be checked. Collects every violation in one pass (never stops
  at the first). **This is the one function both `crate::controller::
  reconcile` and `crate::webhook::handle_review` call** — see this file's
  TLS/S3/Non-S3-stores sections above, each updated to point here instead
  of repeating "no admission webhook in v1." `crate::controller::
  control_nodes_regression`/`validate_control_nodes_within_nodes`
  (moved out of `reconcile`'s own inline arithmetic into these two small
  pure functions, still in `controller.rs` since they're reconciler-shaped
  — reused by `validate_spec` too) are the two rules that didn't already
  live on a `crd.rs` type. **Deliberately excludes** any *live* check (does
  a referenced `Secret` exist) — those stay reconciler-only
  (`validate_encryption_key_secret`'s own doc explains why a webhook must
  never make one); a new, purely informational `CONDITION_NODES_SPEC_
  INVALID` condition (`crd.rs`) surfaces the new `nodes >= 1` rule on a
  cluster running without the webhook, with no fallback value to
  strip/substitute (unlike `spec.tls`/`spec.s3`, which can safely
  reconcile "as if unset").
- **`src/webhook.rs`** — the HTTPS server itself: `load_tls_acceptor(cert_
  path, key_path) -> Result<TlsAcceptor, WebhookError>` (server-only, no
  client cert — the server-side mirror of `admin_client.rs::
  build_tls_connector`, reading PEM bytes itself and parsing via
  `rustls_pki_types`' `*_slice_iter`/`from_pem_slice`, not the `*_file`
  helpers, since this workspace's dependency graph enables `rustls-
  pki-types`'s `alloc` feature but not `std` — same choice `admin_client.rs`
  already made), `handle_review(AdmissionReview<AnimusCluster>) ->
  AdmissionReview<DynamicObject>` (the entire interesting logic, exposed
  standalone over an *already-decoded* request so the handler tests need
  no socket — decodes to an `AdmissionRequest`, checks `kind.kind ==
  "AnimusCluster"` defense-in-depth, calls `validate::validate_spec`, and
  builds the allowed/denied `AdmissionResponse`, joining every `Violation`'s
  `field: message` with `"; "` on a denial), `run(addr, acceptor)` (the
  accept loop — one task per connection, `hyper::server::conn::http1` +
  `hyper_util::rt::TokioIo`, the identical shape `crates/animusd/src/
  admin.rs::serve` uses, one real Kubernetes API server call away instead
  of a kubelet probe). Uses `kube`'s `admission` Cargo feature (already
  enabled — `Cargo.toml`) for `AdmissionReview`/`AdmissionRequest`/
  `AdmissionResponse`; no new HTTP or TLS framework crate — `hyper` gained
  the `"server"` feature (alongside its pre-existing `"client"`/`"http1"`)
  for `hyper::server::conn::http1`, which pulled in exactly one new
  transitive dependency, `httpdate` (the `Date` response header).

`main.rs` gained `--webhook-addr ADDR --webhook-cert PATH --webhook-key
PATH` on `run` (all three or none — `parse_webhook_config`; a partial set
is a startup error) and a third subcommand, `animus-operator webhook-cert
--namespace NS --service NAME --issuer-name NAME [--issuer-kind
Issuer|ClusterIssuer] [--secret-name NAME] [--duration D] [--renew-before
D]`, printing a standalone cert-manager `Certificate` YAML to stdout — the
identical "print YAML, pipe into `kubectl apply -f -`" shape `crd` already
established. When `run` is given the three webhook flags, `main.rs::run`
loads the `TlsAcceptor` and `tokio::spawn`s `webhook::run` *before*
awaiting the reconcile loop (which never returns on success) — a TLS-
material or bind failure exits the process; a cluster whose deployment
named these flags expects the webhook to actually be up, since
`failurePolicy: Fail` (below) makes every `AnimusCluster` write depend on
it. Without the three flags, nothing listens beyond the reconcile loop —
`cargo run -p animus-operator -- run` (`scripts/e2e-kind.sh`'s plain leg)
is byte-for-byte unchanged.

**Cert issuance, two paths, mirroring `spec.tls`'s own precedent** —
`crate::desired::certificate` gained `build_standalone(name, ns,
secret_name, dns_names, issuer_ref, duration, renew_before) ->
DynamicObject` (no owner reference — nothing in this crate owns the
webhook's own cluster-independent objects) and `webhook_dns_names(service,
ns) -> Vec<String>`, both generalized out of the existing `build`/
`dns_names` (an `AnimusCluster`'s own TLS cert) via a new private `cert_
spec` helper the two share — `webhook-cert` above is `build_standalone`'s
one caller. `deploy/operator/webhook.yaml` ships a working example
`Certificate` object (the cert-manager path) alongside the `Service`/
`ValidatingWebhookConfiguration`; `deploy/operator/README.md`'s "Admission
webhook" section has the exact commands for both the cert-manager path and
the hand-issued-`Secret` alternative.

**Static manifest, not operator-managed** — `deploy/operator/webhook.yaml`
(`Service` + `ValidatingWebhookConfiguration`, `failurePolicy: Fail`,
`sideEffects: None`, `admissionReviewVersions: [v1]`, `timeoutSeconds: 5`,
scoped to `animusdb.io`/`animusclusters` CREATE/UPDATE) is a static,
hand-applied manifest — the reconciler never creates, updates, or watches
it, and no RBAC was added (the alternative, the operator self-registering
a cluster-scoped `admissionregistration.k8s.io` object, was rejected — see
ADR 0070's own Decision 4 for the full blast-radius/no-reconcile-cadence
reasoning). `deployment.yaml` ships the webhook's own `args`/`ports`/
`volumeMounts`/`volumes` as a commented-out block (mirroring `example.yaml`'s
own commented-optional-section style) rather than active by default.

## Tests

`cargo test -p animus-operator` — every `desired::*` builder module has its
own `#[cfg(test)] mod tests` (258 lib unit tests as of S-07e, up from 234
at S-03 PR 3 — the new `validate`/`webhook` modules and the `certificate`/
`controller` additions this PR made, see this file's own "Admission
webhook" section above):
golden-JSON assertions for the `ClusterConfig`/`entrypoint.sh`
`ConfigMap` contents (including the no-port-striding invariant, a
scale-up byte-for-byte-preserves-existing-entries regression, since
S-04 PR 3 the `--s3-credentials`-file-writing preamble/flags, since
S-07b the non-S3 `backupStore`/`segmentStore` flag emission and its
"whichever of `spec.s3` or the top-level field is set" precedence, and
since S-03 PR 3 the `encryption_key_path` mount-path presence/identical-
across-nodes/never-a-CLI-flag cases),
`Service` port sets, `StatefulSet` probe paths/ports and ephemeral-vs-durable
storage shape (plus the `spec.s3` and, since S-03 PR 3, `spec.
encryptionKeySecretName` `Secret` mounts — the latter's `defaultMode`
restriction included), `NetworkPolicy`
selector/ingress/egress rule structure (including the S-04 PR 3 egress
additions: baseline intra+DNS on every cluster, an S3 rule only when
`spec.s3` is set), and, since S-07c, `PodDisruptionBudget`
`maxUnavailable` arithmetic covering every degenerate shape (`nodes == 1`,
`controlNodes == 1`, `nodes < MAX_REPLICATION_FACTOR`, a larger
`controlNodes` capped by the data-plane term and vice versa, scale
invariance once past the replication factor) plus a golden-JSON shape
test. **No cluster is needed** — every test constructs an
`AnimusCluster` via `test_support::test_cluster` and asserts on the
returned typed object or its JSON, never against a live API server.
`s3_uri::tests` covers the standalone URI parser (`src/s3_uri.rs`)
directly.

- **`src/controller.rs` has its own fake-kube-client harness now** (ADR
  0061 rung E1, `crate::fakes`, `#[cfg(test)]` only). `controller.rs`'s two
  live-cluster boundaries — the `kube::Api` calls and the `AdminClient`
  admin-port HTTP calls — are each behind a small `#[async_trait]` trait
  (`cluster_api::ClusterApi`, `admin_client::AdminOps`); `Context`,
  `reconcile`, `apply_children`, `previous_applied_control_nodes`,
  `drain_and_remove_node`, and, since S-07d, `advance_control_growth`/
  `add_control_voter` are all generic over both. Production (`run()`)
  wires the real implementors (`RealClusterApi`, `AdminClient`); tests wire
  `fakes::{FakeClusterApi, FakeAdminClient}`, small in-memory
  record-and-serve stores (see their own doc for exactly what they do and
  do not model — no resourceVersion/admission/watch semantics, a
  same-process store rather than `kube`'s own wire protocol). This is a
  hand-written trait rather than `kube`'s own `tower_test`-backed mock
  `Client` — see ADR 0061's 2026-09-04 amendment note for the trade-off
  (in short: there are *two* live-cluster boundaries here, not one, since
  the admin-port client is deliberately not built on `kube::Client`, so a
  `kube`-specific mock would only ever cover half of it).
  `controller::tests` (in `src/controller.rs`) covers: a fresh cluster's
  reconcile applying all six children in the right order; that an
  unchanged cluster's reconcile still re-applies every child (pinned as the
  actual, deliberate behavior — `apply_children` never diffs against
  previously-applied state, so this is an idempotent re-apply, not a
  no-op); `previous_applied_control_nodes` reading the prior applied value
  (changed or not) and "no prior `ConfigMap` yet"; `drain_and_remove_node`'s
  sequence on both the immediate-success path and the **bounded**
  never-completes path (`#[tokio::test(start_paused = true)]`'s virtual
  clock resolves the 120 x 5s poll budget without real wall-clock wait);
  reconcile-level scale-down sequencing, both the highest-ordinal-first
  happy path and stop-on-first-drain-failure; the `controlNodes`-decrease
  refusal end to end; since S-07d, growth: the pure `next_growth_ordinal`/
  `achieved_control_nodes`/`parse_voters` functions directly, plus
  reconcile-level coverage of the config regenerating to the full target
  immediately, waiting for the promoted pod's `role: "combined"` before
  adding it (and the PDB using the achieved count meanwhile), a successful
  add plus the retry-a-different-voter-ordinal path
  (`fail_add_control_member_for_ordinal`), a stalled-discovery reconcile
  still recording `ControlNodesGrowing` (`fail_control_members`), growth
  completion clearing the condition, resuming from live truth after a
  simulated controller restart
  (the `ConfigMap`-already-matches-target case the condition exists for),
  and `controlNodes` above `nodes` still rejected; since ADR 0064 commit 3, `spec.tls`: a
  `Certificate` applied as a seventh child for the `certManager` shape and
  none for `secretName`; both/neither shapes set rejected with
  `TlsSpecInvalid`; and the scale-down drain sequence reading a seeded
  `Secret`'s `ca.crt` and dialing `https://` once `spec.tls` is set; and,
  since S-04 PR 3, `spec.s3`: a valid spec applies the same six children
  (no seventh child, unlike `spec.tls.certManager`) with the `Secret` mount/
  entrypoint flags/egress rule all present (`FakeClusterApi::
  networkpolicy`, a new accessor this PR added alongside the pre-existing
  `configmap`/`get_statefulset`); each `S3StoreSpec::validate` rejection
  (neither store set, empty `credentialsSecretName`, malformed URI,
  `insecure_http` without `allowInsecureHttp`) surfaces `S3SpecInvalid`
  and strips `spec.s3` for that reconcile; and a cluster with no `spec.s3`
  still gets the new baseline egress (intra + DNS) with no `s3` volume;
  and, since S-07b, `spec.backupStore`/`spec.segmentStore`: a valid
  non-S3 spec applies the same six children with the entrypoint flag
  present and no S3 credentials wiring; `validate_store_spec` rejections
  (`segmentStore: "cluster"`, a path outside `DATA_DIR`, a conflict with
  `spec.s3`'s own store field) surface `StoreSpecInvalid` and strip both
  fields for that reconcile; and a cluster with neither field set emits
  no `--backup-store`/`--segment-store` flag at all; and, since S-07c,
  `PodDisruptionBudget`: applied once per reconcile with an owner
  reference and a selector matching the *actual* `statefulset::build`
  output's own pod-template labels (`FakeClusterApi::poddisruptionbudget`,
  a new accessor this PR added), and a scale-down transition
  (`nodes: 5` → `nodes: 2, controlNodes: 2`) recomputing `maxUnavailable`
  from the new desired spec rather than inheriting the prior shape's
  looser value; and, since S-03 PR 3, `spec.encryptionKeySecretName`: a
  valid `Secret` (present, with the `"key"` data key) wires the mount and
  `cluster.json` field with no `EncryptionKeySecretInvalid` condition; a
  missing `Secret`, and one present but missing the data key, each
  surface that condition naming the problem WITHOUT stripping the field
  (`parsed_cluster_config`, a new test helper parsing the applied
  `ConfigMap`'s `cluster.json` back into `desired::cluster_config::
  ClusterConfig`, asserts every node still carries `encryption_key_path`
  either way); the condition clears once the `Secret` is fixed on a later
  reconcile; and a cluster with the field unset touches neither the
  mount, the `cluster.json` field, nor the condition.
  **What this harness does not prove**: real
  `kube::Api` wire behavior against an actual API server (conflicts,
  admission, watch-driven requeue, real server-side-apply field-ownership
  semantics) or real-thread liveness of the `Controller::run` watch loop —
  that gap is still the e2e smoke's to close, unchanged by this harness.

## e2e

`scripts/e2e-kind.sh` (`.github/workflows/e2e-kind.yml`, CI-gated on every
push/PR touching `crates/animus-operator/**`, `deploy/**`, `Dockerfile`, or
the script/workflow itself) is the `kind`-cluster-driven end-to-end
complement `src/controller.rs`'s own unit-test gap above calls for: it
creates a real `kind` cluster, loads a locally built `animusd` image into
it, applies the CRD and an `AnimusCluster`, runs the controller **out of
cluster** — `cargo build -p animus-operator` synchronously, then the
already-compiled binary is `exec`ed directly (`animus-operator run`
against the kind kubeconfig, not backgrounded via `cargo run` — see
"Building the operator binary before backgrounding it" below) — in-cluster
deployment of the operator's own image, per
`deploy/operator/deployment.yaml`, is exercised in production, not by this
smoke), waits for the `StatefulSet` to reach 3/3 ready, resolves which specific
pod `svc/{name}-dynamo` currently routes to (via that Service's own
`Endpoints`) and port-forwards that POD directly on both its dynamo and
admin ports (issue #595 — see below), waits for that same pod's own `GET
/admin/health` to report `200`, then exercises the real DynamoDB wire
(`CreateTable`/`PutItem`/`GetItem`, asserting the item round-trips), scales
to 4 nodes and confirms the item still reads back, then (S-07d) grows
`spec.controlNodes` 3 → 4 (promoting the pod that scale-up just added into
a real control voter), polls `GET /admin/control/members` for the new
voter count, re-checks the PDB, re-resolves/re-forwards the serving pod
(the config-hash-triggered rolling restart may have recycled it — see this
file's own S-07d section), confirms the item still reads back once more,
then deletes the `AnimusCluster` and confirms every owned child is
garbage-collected. Local
invocation (mirrors the script's own header comment):

```sh
docker build -t animusd:e2e --build-arg BASE_REGISTRY=mirror.gcr.io/library \
  --secret id=ccrca,src=/root/.ccr/ca-bundle.crt .
KIND_NODE_IMAGE=mirror.gcr.io/kindest/node:v1.34.0 ANIMUSD_IMAGE=animusd:e2e \
  bash scripts/e2e-kind.sh
```

The `--build-arg`/`--secret` pair is only for a sandboxed dev host behind a
TLS-intercepting egress proxy that can't reach Docker Hub's blob CDN (see
the Dockerfile's own header) — CI and an ordinary developer machine just
run `docker build -t animusd:e2e .` with `KIND_NODE_IMAGE` unset (kind
picks its own pinned default).

**Building the operator binary before backgrounding it (issue #661,
S-07d).** The "run operator out-of-cluster" phase used to background
`exec cargo run -p animus-operator -- run` directly, with nothing earlier
in the script warming the build cache — `animus-operator` is the *only*
`cargo` invocation in the whole script, so `kube-rs`'s dependency tree
(`rustls`/`hyper`/`k8s-openapi`/`kube-runtime`) compiled from cold right
there, easily a minute or more. Every line landing in `$OPERATOR_LOG`
during that window was `cargo`'s own `Compiling ...` chatter — a process
still compiling is indistinguishable, from the log alone, from one that's
stuck, which is exactly what made a real bug (see this file's own S-07d
growth entries and `docs/engineering-lessons.md`'s issue #661 entry on
`ProdEnv::send_stream`) hard to diagnose from the log alone. The phase now
runs `cargo build -p animus-operator --bin animus-operator` synchronously
first (its own output goes straight to the terminal), resolves the
compiled binary's path the same way cargo itself would
(`${CARGO_TARGET_DIR:-$REPO_ROOT/target}/debug/animus-operator`), and
*then* backgrounds an `exec` of that binary directly. Every line that can
land in `$OPERATOR_LOG` from this point on is genuine runtime tracing —
confirmed `EnvFilter::from_default_env()` with `RUST_LOG=info` set does
correctly enable `animus-operator::controller`'s own `info!` lines (e.g.
`"reconciling AnimusCluster"`); the missing-tracing-lines symptom was
never an `EnvFilter`/`fmt::init()` semantics bug. Side benefit: `exec`ing
the binary directly means `$OPERATOR_PID` (from `$!`) is the real process,
not a `cargo run` supervisor — `cleanup()`'s pre-existing belt-and-
suspenders `pkill -f "animus-operator run"` is now clearly redundant
(kept anyway, harmless).

**The S-07d `controlNodes` growth wait is a converge-or-STALL wait, not a
flat deadline (issue #705, also closes the #703 e2e-kind-encryption
finding).** The flat `300s wait_for "control group reports 4 voters"` this
used to be ran into two independent problems on two separate real CI runs,
both of which land on the same wait: (1) run 34085241118 — the
newly-promoted ordinal-3 pod exited cleanly (`Reason: Completed, Exit Code:
0`) three times over ~4 minutes before stabilizing. Tracing
`crates/animusd/src/main.rs` confirms `wait_for_ctrl_c` (SIGINT/SIGTERM,
`node.shutdown_graceful().await`, then `Ok(())`) is the **only** code path
by which a running `animusd` process — combined or data role — returns
`Ok(())`; every startup/runtime error instead takes the `Err` path to a
nonzero `ExitCode::FAILURE`. So a repeated clean exit is necessarily
SIGTERM-driven, never an application-level crash — the exact SIGTERM
source (a livenessProbe kill vs. something else) was **not** pinned down
in this investigation, since the diagnostics dump at the time didn't
capture `kubectl logs --previous` for the crashed instances (now fixed,
see below) — but each restart genuinely delayed
`advance_control_growth`'s own `/admin/config` polling (`REQUEUE_OK=30s`
cadence in `controller.rs`), landing the first possible `member/add` call
2s after the old flat deadline even though the growth mechanism itself was
healthy throughout. A stale-ConfigMap-volume read (the kubelet's own
periodic resync of a mounted `ConfigMap`) was investigated and **ruled
out** as the trigger: `entrypoint.sh` and `cluster.json` are two keys of
the same `ConfigMap` object, so a pod can never mount one stale and the
other fresh, and even a wholesale-stale mount would just re-run ordinal 3
in its own prior, already-healthy `data` role — it doesn't explain a
restart storm either way. (2) PR #703's e2e-kind-encryption run
(34100368447) hit the identical wait for a different reason: it polls
`GET /admin/control/members` through the port-forward established
*before* `controlNodes` was even patched, and the S-07d rolling restart
recycled that pinned pod (e2e-2) mid-wait — `kubectl port-forward
pod/...` dies silently the moment its target pod is deleted/recreated,
and the pre-#705 `control_voters_count` folded every subsequent
connection failure into a bare `0`, so the wait spun out its entire
remaining budget reading a false "0 voters" instead of noticing it had
lost its connection.

`scripts/e2e-kind.sh`'s own `wait_for_progress` (a second wait primitive
alongside the pre-existing `wait_for`, same DESC/interval style) fixes
both: it keeps waiting as long as *any* of a small set of progress signals
is still changing — the promoted ordinal's own restart count/ready
condition/phase (read straight off the API server via `kubectl get pod`,
so immune to problem (2)) and the live voter count once a reading is
actually obtainable (`control_voters_reading`, which now self-heals the
port-forward — `resolve_and_forward_dynamo_pod`, extracted from what used
to be two near-duplicate inline blocks — on a connection failure rather
than ever reporting a false `0`, closing problem (2) directly) — and fails
only once `STALL_SECS` pass with every one of those signals unchanged, or
a `600s` hard ceiling is hit, whichever comes first. Deliberately **not**
applied to the earlier `nodes`/`readyReplicas` waits, or to any shrink
path (this script has none — `controlNodes` decrease rejection is
unit-tested in `controller.rs` and, since S-07e, checked live by the
webhook e2e leg via a rejected `kubectl patch`, never a `wait_for` loop):
those poll purely via `kubectl` (not the pinned port-forward, so problem
(2) doesn't apply) and a plain `spec.nodes` scale-up only ever creates a
brand-new pod rather than restarting an existing one (so problem (1)'s
restart-storm risk doesn't apply either — see this file's own S-07d
config-hash section for why only a `ConfigMap`-affecting change, like a
`controlNodes` role-split flip, rolls already-running pods).

`dump_diagnostics` also gained, per pod/per container: its current
restart count and `lastState`, and — whenever that count is `> 0` —
`kubectl logs --previous --tail=200` for that container (issue #705). The
pre-existing `--tail=100` current-instance log capture only ever shows
whichever instance is running *right now*; once a container has
restarted, the crashed instance's own output is gone from that stream
entirely, which is exactly what made this issue's own root cause
unconfirmable from the original job log. A recurrence now carries the
crashed instance's own stdout/stderr in the diagnostics dump.

**Two script-side hardenings for a flake this smoke hit twice with the
identical signature (issue #595)**, on top of the actual root-cause fix
(ADR 0020's 2026-09-04 amendment; `animus-control`/`animusd::admin` — a
follower's `/admin/health` used to read the raw, pre-vote-hair-triggered
`leader_id` belief instead of a hysteresis-gated one): (1) the script no
longer trusts the `StatefulSet`'s aggregate `3/3 ready` count as proof that
the ONE pod it is about to port-forward is itself, right now, ready —
it resolves that specific pod off `svc/{name}-dynamo`'s own `Endpoints`
and polls that pod's own `/admin/health` before issuing any DynamoDB call
against it; (2) the first `CreateTable` (the call this issue's two failing
runs both died on) is now a bounded converged-or-timeout retry scoped
narrowly to the one transient 500 ("did not commit to the control plane in
time") this issue is about — `CreateTable` is idempotent server-side, so a
retry that lands on an already-committed table is treated as success
(`ResourceInUseException`), and every other error class still fails the
run immediately, on the first attempt, unchanged. See the script's own
header comment for the two failing run links and the full reasoning.

**`E2E_TLS=1` (ADR 0064 commit 3, CI's own `e2e-kind-tls` job) runs the same
smoke over TLS**: installs cert-manager (pinned version), creates a
self-signed `ClusterIssuer`, sets `spec.tls.certManager` on the
`AnimusCluster` manifest, waits for the resulting `Certificate`'s own
`Ready` condition, then drives the DynamoDB wire with `curl --cacert
--resolve` (the dynamo Service's cluster-DNS name — one of the
`Certificate`'s own SANs, `desired::certificate::dns_names` — resolved to
the port-forward's `127.0.0.1`, so hostname verification passes against the
issued cert) instead of plain HTTP; the plain-TCP path (`E2E_TLS` unset) is
byte-for-byte unchanged. **UNVERIFIED in this repository's sandboxed dev
environment** — this environment cannot bring up `kind` at all (see this
section's own `CAP_SYS_RESOURCE` note below), so the TLS additions have
been written carefully and `bash -n`-checked but have not been run end to
end anywhere; the first real `e2e-kind-tls` CI run is this path's first
real test.

**`E2E_S3=1` (S-04 PR 3, CI's own `e2e-kind-s3` job) runs the same smoke
plus a `spec.s3.backupStore` leg**: deploys a single-pod MinIO + Service
into the kind cluster (the well-known `minio/minio` image), creates its
bucket via a throwaway `minio/mc` pod, creates the `access_key_id`/
`secret_access_key` credentials `Secret`, applies the `AnimusCluster` with
`spec.s3.backupStore` pointing at `http://minio.<ns>.svc:9000`
(`allowInsecureHttp: true` — a loopback-to-the-cluster dev target, never a
real deployment shape), then exercises `CreateBackup`/`DescribeBackup`
over the DynamoDB wire and checks `GET /admin/backup-store` reports
`"kind":"s3"`; the plain-TCP path (`E2E_S3` unset) is byte-for-byte
unchanged. Independent of `E2E_TLS` — either, both, or neither may be set.
**UNVERIFIED in this repository's sandboxed dev environment**, same
`CAP_SYS_RESOURCE` reason as `E2E_TLS` above — written carefully and
`bash -n`-checked but never run end to end anywhere; the first real
`e2e-kind-s3` CI run is this leg's first real test.

**`E2E_ENCRYPTION=1` (ADR 0069 S-03 PR 3, CI's own `e2e-kind-encryption`
job) runs the same smoke plus a `spec.encryptionKeySecretName` leg**: no
extra in-cluster dependency (unlike MinIO for `E2E_S3`) — creates the
`Secret` (a freshly generated 64-hex-character key via `openssl rand
-hex 32`, never logged anywhere in the script), sets `spec.
encryptionKeySecretName` on the manifest, then — right after the ordinary
`PutItem`/`GetItem` round trip, before the scale-up — `kubectl exec`s
into the serving pod and `grep -r`s its own data directory for the
plaintext item value written by `PutItem` (must be absent: `LsmEngine`
applies no block compression, so a plaintext write would land those
exact bytes in an SSTable/WAL file somewhere under `DATA_MOUNT_DIR`) and
checks `GET /admin/config`'s raw response body never contains the key
hex; the plain-TCP path (`E2E_ENCRYPTION` unset) is byte-for-byte
unchanged. Independent of `E2E_TLS`/`E2E_S3` — any combination may be
set. **UNVERIFIED in this repository's sandboxed dev environment**, same
`CAP_SYS_RESOURCE` reason as `E2E_TLS`/`E2E_S3` above — written carefully
and `bash -n`-checked but never run end to end anywhere; the first real
`e2e-kind-encryption` CI run is this leg's first real test.

**`E2E_WEBHOOK=1` (S-07e, ADR 0070, CI's own `e2e-kind-webhook` job) runs
the same smoke plus an in-cluster validating-admission-webhook leg** — the
one leg here that needs the operator running **in-cluster**, since the API
server must be able to dial the webhook, which an out-of-cluster `cargo
run` process (what every leg, this one included, still uses for the
ordinary reconcile loop) structurally cannot serve. Rather than moving the
*whole* operator in-cluster (real RBAC/ServiceAccount wiring against a
live cluster, and a second controller racing the existing out-of-cluster
one over the same objects — a materially larger change than this leg
needs), it builds the `animus-operator` image (`docker build --target
runtime-operator`, BuildKit-cache-shared with the `ANIMUSD_IMAGE` build so
this is a fast cache hit, not a second from-scratch compile — see the
Dockerfile's own "single cache-mounted compile" comment) and deploys a
**second, minimal** Deployment running `--webhook-only` (`main.rs`'s own
opt-in mode — no reconcile loop, no Kubernetes client ever built at all,
since `validate_spec` is pure and the webhook itself never touches the
API), a hand-issued self-signed `Secret` via `openssl` (the "Without
cert-manager" path `deploy/operator/README.md` documents — no cert-manager
dependency for this leg, independent of whatever `E2E_TLS` did), a
`Service`, and a `ValidatingWebhookConfiguration` scoped to this leg's own
namespace via `namespaceSelector` (a webhook outage here can't affect
anything outside this smoke's own objects, even under `failurePolicy:
Fail`). Then asserts the one property no `cargo test -p animus-operator`
run can prove: an invalid `spec.controlNodes` decrease (3 → 1, the
identical grow-only rule `crate::validate::validate_spec` enforces) is
rejected by the API server **itself** — `kubectl patch` fails outright,
naming `spec.controlNodes` in its own error, not merely surfaced as a
status condition on a persisted object — and a valid edit
(`quiesceAfterSecs`) is still admitted and persisted. Independent of
`E2E_TLS`/`E2E_S3`/`E2E_ENCRYPTION` — any combination may be set; the
plain-TCP path (`E2E_WEBHOOK` unset) is byte-for-byte unchanged.
**UNVERIFIED in this repository's sandboxed dev environment**, same
`CAP_SYS_RESOURCE` reason as every leg above — written carefully and
`bash -n`-checked but never run end to end anywhere; the first real
`e2e-kind-webhook` CI run is this leg's first real test.

**A sandboxed dev/build host can be structurally unable to run this at
all — not a bug in this script or the operator.** `kind`'s own control
plane (`etcd`/`kube-apiserver`/`kube-scheduler`/`kube-controller-manager`,
run as static pods) gets a **negative** `oom_score_adj` from kubelet
unconditionally, for every one of them, regardless of the pod's own
resources — the standard Kubernetes "protect the critical pods from the
OOM killer first" behavior, not something a kind config or a pod spec can
opt out of. Applying a negative value requires `CAP_SYS_RESOURCE` in the
container's own namespace at container-create time (`runc`'s `nsexec`
calls it while still in the parent's privilege domain, so the capability
has to be present all the way up the chain — the container's declared
capabilities can never regrant one the host/daemon never had). A host
whose outermost capability set already excludes `CAP_SYS_RESOURCE` (`docker
run --cap-add SYS_RESOURCE` there is flatly rejected as "not supported by
your kernel or not available in the current environment," not merely
denied at use) can never bring up `kind`'s control plane, independent of
the node image, the containerd version, or the cgroup driver (`systemd`
vs. `cgroupfs`, both tried) — every one of those was ruled out by direct
`runc create --debug` reproduction against a hand-built bundle before
landing on the real, single-line cause: `nsexec: failed to update
/proc/self/oom_score_adj: Permission denied`, laundered by containerd into
the far more generic `can't get final child's PID from pipe: EOF` that
actually reaches `kubectl`/the kubelet log. A normal CI runner or dev
machine (full capability set) is unaffected; this is specific to a host
that has deliberately dropped `CAP_SYS_RESOURCE` for its own sandboxing
reasons. If `scripts/e2e-kind.sh` fails at the `kind create cluster` phase
with this exact `runc`/`EOF` signature in the diagnostics dump, this is
almost certainly it — check `docker run --cap-add SYS_RESOURCE ... echo ok`
first before debugging anything else.
