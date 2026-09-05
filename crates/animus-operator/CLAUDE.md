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
  `ClientServiceSpec`/`ClusterCondition`/`ClusterPhase`. Pure data + a
  handful of `_or_default()` helpers; no k8s API calls, no logic that needs
  a live object beyond its own fields.
- `src/desired/` — **pure builder functions**, `(name, ns, spec) -> a typed
  k8s-openapi object`, no cluster access. This is where almost all of this
  crate's tests live:
  - `cluster_config.rs` — the `animusd::config::ClusterConfig`/`RoleAddrs`
    JSON mirror (see the gotcha below) and `entrypoint_script` (the POSIX
    `sh` dispatch script every pod runs).
  - `configmap.rs`/`services.rs`/`statefulset.rs`/`networkpolicy.rs` — one
    builder module per child kind.
  - `mod.rs` — shared label/name helpers (`common_labels`/
    `selector_labels`/`owner_reference`/`pod_fqdn`/the `*_name` functions)
    every builder module uses, so every child's naming/labeling convention
    lives in one place.
  - `test_support.rs` (`#[cfg(test)]` only) — `test_cluster(name, ns, nodes,
    control_nodes)`, the one fixture every builder test file shares.
- `src/admin_client.rs` — a minimal plain-HTTP JSON client (`hyper`/
  `hyper-util`, reusing `kube`'s own already-pulled HTTP stack rather than
  adding `reqwest`) for `animusd`'s admin/debug interface (ADR 0020) — used
  only by the scale-down drain sequence below. Also carries `AdminOps`, the
  test seam over that client's two calls (ADR 0061 rung E1 — see Tests).
- `src/cluster_api.rs` — `ClusterApi`, the test seam over the `kube::Api`
  calls `controller.rs` performs, plus `RealClusterApi`, its production
  implementor (ADR 0061 rung E1 — see Tests).
- `src/controller.rs` — the thin imperative shell: `reconcile` builds every
  desired child via `desired::*`, server-side-applies each
  (`PatchParams::apply(FIELD_MANAGER).force()`, field manager
  `"animus-operator"`), reads the applied `StatefulSet`'s own
  `status.readyReplicas` to compute `AnimusClusterStatus`, and requeues
  (~30s on success, 15s on error). The two genuinely stateful pieces that
  can't be pure functions live here too: the scale-down member-drain
  sequence (`drain_and_remove_node`, talks to a real pod's admin port) and
  the `controlNodes`-immutability check (`control_nodes_changed`, reads the
  previously-applied `ConfigMap` back to recover what was actually applied
  last time — see its own doc for why that, not a status annotation, is the
  source of truth).
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
  `scripts/e2e-kind.sh` runs the controller (still out-of-cluster via
  `cargo run`, deliberately — see the e2e section below) or how local
  development works.
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

## Tests

`cargo test -p animus-operator` — every `desired::*` builder module has its
own `#[cfg(test)] mod tests` (51 tests total as of ADR 0061 rung E1's
landing): golden-JSON assertions for the `ClusterConfig`/`entrypoint.sh`
`ConfigMap` contents (including the no-port-striding invariant and a
scale-up byte-for-byte-preserves-existing-entries regression), `Service`
port sets, `StatefulSet` probe paths/ports and ephemeral-vs-durable storage
shape, and `NetworkPolicy` selector/rule structure. **No cluster is
needed** — every test constructs an `AnimusCluster` via `test_support::
test_cluster` and asserts on the returned typed object or its JSON, never
against a live API server.

- **`src/controller.rs` has its own fake-kube-client harness now** (ADR
  0061 rung E1, `crate::fakes`, `#[cfg(test)]` only). `controller.rs`'s two
  live-cluster boundaries — the `kube::Api` calls and the `AdminClient`
  admin-port HTTP calls — are each behind a small `#[async_trait]` trait
  (`cluster_api::ClusterApi`, `admin_client::AdminOps`); `Context`,
  `reconcile`, `apply_children`, `control_nodes_changed`, and
  `drain_and_remove_node` are all generic over both. Production (`run()`)
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
  reconcile applying all five children in the right order; that an
  unchanged cluster's reconcile still re-applies every child (pinned as the
  actual, deliberate behavior — `apply_children` never diffs against
  previously-applied state, so this is an idempotent re-apply, not a
  no-op); `control_nodes_changed` detecting a real change, no change, and
  "no prior `ConfigMap` yet"; `drain_and_remove_node`'s sequence on both
  the immediate-success path and the **bounded** never-completes path
  (`#[tokio::test(start_paused = true)]`'s virtual clock resolves the 120
  x 5s poll budget without real wall-clock wait); reconcile-level
  scale-down sequencing, both the highest-ordinal-first happy path and
  stop-on-first-drain-failure; and the immutable-`controlNodes`-change
  refusal end to end. **What this harness does not prove**: real
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
cluster** (`cargo run -p animus-operator -- run` against the kind
kubeconfig — in-cluster deployment of the operator's own image, per
`deploy/operator/deployment.yaml`, is exercised in production, not by this
smoke), waits for the `StatefulSet` to reach 3/3 ready, resolves which specific
pod `svc/{name}-dynamo` currently routes to (via that Service's own
`Endpoints`) and port-forwards that POD directly on both its dynamo and
admin ports (issue #595 — see below), waits for that same pod's own `GET
/admin/health` to report `200`, then exercises the real DynamoDB wire
(`CreateTable`/`PutItem`/`GetItem`, asserting the item round-trips), scales
to 4 nodes and confirms the item still reads back, then deletes the
`AnimusCluster` and confirms every owned child is garbage-collected. Local
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
