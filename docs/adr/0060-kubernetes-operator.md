# ADR 0060 — The Kubernetes operator: hostname-native addressing + `animus-operator`

- **Status:** Accepted — the animusd groundwork (SIGTERM, hostname seeds,
  the string-keyed peer/route books, `advertise_host`), the container
  image, and the `animus-operator` MVP land in the same stacked series as
  this ADR; the deferred list below (TLS, control-voter growth, …) remains
  future work. **2026-09-02:** operator image publishing and `Cargo.lock`
  are no longer on that list — see Part 2's and the deferred list's own
  notes below (S-07a).
- **Date:** 2026-08-27
- **Amends:** [ADR 0047](0047-intra-node-port.md) (this ADR is the design
  this repo's own `CLAUDE.md` and ADR 0047's Context section have promised
  since the intra/client port split shipped — "Animus's target deployment
  is Kubernetes via an operator"; this document is that operator, plus the
  `animusd` groundwork its deployment shape needs that ADR 0047 alone
  didn't require), [ADR 0032](0032-seed-join-membership.md) (`--seed`'s
  parsing and the replicated node address book are where hostname-native
  addressing actually lands), [ADR 0035](0035-control-plane-separate-deployment.md)
  (the operator's `controlNodes`/`nodes` split is a thin CRD-level face on
  this ADR's combined/data-only role assemblies, not a new deployment
  shape), [ADR 0040](0040-self-minted-string-node-ids.md) (`RegisterNode`'s
  registration CAS is the exact mechanism this ADR's Part 1 repairs the
  stale-address failure mode of), [ADR 0057](0057-sigv4-client-auth.md)
  (`dynamoAuthSecretName` mounts the credential file `--dynamo-auth`
  already accepts).
- **Depends on:** ADR 0003 (the `Env` seam — Part 1's DNS resolution stays
  entirely inside `ProdEnv`/`animusd`, so this touches neither
  `animus-sim` nor determinism), ADR 0020 (the admin/debug HTTP surface —
  readiness/liveness probes, and the trusted-network posture this ADR's
  NetworkPolicy stands in for), ADR 0030 (online growth — how the operator
  scales a running cluster up), ADR 0055 (routing/read-path behavior is
  unaffected by any of this — noted only because a reviewer's first
  instinct is to ask).

## Context

Root `CLAUDE.md` already states the target: *"a K8s operator runs the
cluster with seed/intra node-to-node traffic kept cluster-internal … only
the client-facing wire edge (DynamoDB) is exposed outside the cluster."*
ADR 0047 built the port-level mechanism that premise needs (a dedicated
`intra` port, refused on `client`) and named the deployment target
explicitly in its own Context section. ADR 0059 went further and named a
*specific* operational consequence of that target (the S3 `SegmentStore`
egress exception) without ever designing the operator itself. This ADR is
that design: a Kubernetes operator (`animus-operator`, a new workspace
crate) that runs an `AnimusCluster` custom resource, plus the `animusd`
groundwork the operator's own deployment shape requires that no prior ADR
happened to need.

### The groundwork gap: everything is numeric `SocketAddr`

A StatefulSet pod keeps its **identity** across a restart — `--id` and its
PVC both survive — but its **IP address does not**. `animusd` was built
entirely around `SocketAddr`, top to bottom:

- `RoleAddrs`/`NodeAddrs` (`animusd/src/lib.rs`, `animus-control/src/meta.rs`)
  carry `internal`/`client`/`intra`/`admin`/`dynamo`/`console` as
  `SocketAddr` (config) and `String` (replicated `NodeAddrs` — but every
  producer of that `String` today is simply `SocketAddr::to_string()`, so
  the value is numeric in practice even though the field's own type
  already tolerates a hostname).
- `--seed ADDR` (`main.rs`) is parsed with `.parse::<SocketAddr>()` at the
  CLI boundary, twice (the `join`/`data --seed` parsers) — a DNS name is a
  hard parse error before the process ever tries to dial it.
- `ProdEnv`'s dial-side peer book (`animus-env/src/prod.rs`) is
  `peers: Arc<StdMutex<BTreeMap<NodeId, SocketAddr>>>` — every send target
  is a bare, pre-resolved `SocketAddr`.
- `animusd`'s own route books — `client_route`/`intra_route`
  (`ClientCtx`), the `route_addr`/`intra_addr` accessors, the
  `route_snapshot`/`intra_route_snapshot` machine-relay caches, and the
  direct dial sites in `join_request`/`relay_request_with_timeout` — are
  all `SocketAddr`-typed and populated by `.parse::<SocketAddr>()`ing
  `Metadata.node_addrs[*].{client,intra}` inside `peer_sync_loop`/
  `route_sync_loop`/`intra_route_sync_loop`, even though the replicated
  field feeding them is already a plain `String`.

The result, concretely: a StatefulSet pod restarts, gets a new IP from the
CNI, and re-runs its own self-registration
(`ctx.register_node(node, addrs, ..)`, `lib.rs` — the unconditional
self-registration every combined/data-only start path performs). Its
`NodeAddrs` now differs from what's on file at the identical node id.
`MetaCommand::RegisterNode`'s CAS (`animus-control/src/meta.rs`) is keyed
on `node_addrs` alone (ADR 0040 Decision C: an id absent from `node_addrs`
claims it; a byte-identical re-registration is `NoOp`; **a different
`NodeAddrs` already on file is `Rejected`** — this is deliberate, load-
bearing collision detection, not a bug) — so the restart's own
re-registration is rejected outright. And that rejection is silently
discarded: `lib.rs`'s startup self-registration task reads
`let _ = ctx.register_node(node, addrs, BTreeMap::new()).await;` — the
`Result` is dropped on the floor. Every peer that already resolved this
node's old address keeps dialing it forever; nothing re-resolves, nothing
repairs, and nothing even logs that the claim was refused.

Compounding this: only `SIGINT` is handled. `main.rs`'s
`wait_for_ctrl_c` wraps `tokio::signal::ctrl_c().await` exclusively, at
every one of its seven call sites across the CLI's run modes — there is no
`SIGTERM` handler anywhere in the binary. Kubernetes sends `SIGTERM` to
end a pod, not `SIGINT`; today that signal is simply ignored by the
runtime (default disposition, an immediate uncaught-signal kill under most
container init setups, or at minimum no chance for the process to run
`shutdown_graceful()` inside the grace period the operator explicitly
means to give it) — the pod is killed the hard way every time, including
during an entirely ordinary rolling update.

Neither of these is a data-plane correctness gap — a killed leader
fails over via the ordinary Raft election path regardless — but together
they mean a StatefulSet-managed AnimusDB cluster degrades on every single
pod restart: stale peer addresses accumulate, and every restart is an
unclean kill instead of a graceful handoff. An operator built without
fixing this would be building the deployment target's central case on top
of a target the software doesn't actually support yet.

## Decision

Three parts: (1) `animusd` groundwork — hostname-native addressing and
SIGTERM handling, a staged `gh-stack` train; (2) a container image + CI
pipeline; (3) the operator itself, a new crate `animus-operator`.

### Part 1 — `animusd` groundwork ("Path A": advertise a stable name, don't chase a moving one)

**Four staged changes, landing as one `gh-stack` train** (root
`CLAUDE.md`'s stacked-PR default — groundwork, then the mechanism, then
the wiring, then the split is exactly the "more than one reviewable
logical step" shape that rule calls out):

**1. SIGTERM parity + hostname-capable `--seed`.**
`wait_for_ctrl_c` becomes `wait_for_shutdown_signal`, racing
`tokio::signal::ctrl_c()` against `tokio::signal::unix::signal(SignalKind::terminate())`
— either fires the identical `shutdown_graceful()` path every existing
call site already runs. All seven call sites in `main.rs` switch over
uniformly; no behavioral change on a `ctrl_c`-only environment (a
developer's own terminal still Ctrl-C's the same way).
`--seed ADDR` stops requiring `ADDR` to parse as a `SocketAddr` — it
resolves via `tokio::net::lookup_host(addr)` at the point each parser
currently calls `.parse::<SocketAddr>()`, picking the first resolved
address (bootstrap-only traffic: this is the one-shot discovery dial
`join_request`/`poll_seeds_for` make before any replicated address book
exists, never a steady-state hot path). This alone makes a Kubernetes
Service DNS name (`animus-seed.default.svc.cluster.local:14004`, say) a
valid `--seed` value — fulfilling the join-semantics text ADR 0047 §"Join
semantics" already wrote ("the operator supplies the seed's intra address
directly … the Kubernetes operator wires it from the same Service
DNS/IP it always used") for a value that, until this change, could not
actually be a DNS name.

**2. `ProdEnv`'s peer book becomes hostname-capable.**
`peers: Arc<StdMutex<BTreeMap<NodeId, SocketAddr>>>` becomes
`BTreeMap<NodeId, String>`. Dialing resolves at connect time — `tokio::net::TcpStream::connect`
already accepts `&str`/`impl ToSocketAddrs` and resolves internally, so
the send path needs no new resolution step of its own, only the type
change at the boundary. The resolved address a successful connect
produces is cached as the connection-pool key (unchanged behavior for a
numeric peer — resolving a literal IP is a no-op), and that cache entry is
invalidated on the **existing** drop-stale-stream-on-write-error path
(`ProdEnv`'s connection pool already tears down and re-dials on a failed
send; this is the same trigger, doing one more thing on the same
occasion). A moved pod therefore recovers on its own next send: no new
liveness signal, no new repair protocol, just "the next dial re-resolves
because the last one's cached connection just failed" — the identical
shape a numeric-address deployment already relies on when a peer process
restarts on the same IP.

**3. `animusd`'s own route books drop their `SocketAddr` parse step.**
`client_route`/`intra_route`, `route_addr`/`intra_addr`, the
`route_snapshot`/`intra_route_snapshot` caches, and the direct
`TcpStream::connect` call sites (`join_request`,
`relay_request_with_timeout`) all become `String`-typed. The
`.parse::<SocketAddr>()` calls inside `peer_sync_loop`/`route_sync_loop`/
`intra_route_sync_loop` are deleted outright — not replaced with a
resolve step, because there is nothing to resolve at that layer:
`Metadata.node_addrs[*]`'s `NodeAddrs` fields are **already** `String`
(`animus-control/src/meta.rs`; every field's own doc comment already
calls this out as "a plain wire-format string this crate never
interprets"). The only reason today's sync loops parse them into
`SocketAddr` at all is that every producer of that string, until this
change, happens to write `SocketAddr::to_string()` — a self-imposed
round trip through a type the replicated field was never actually
declared as. Deleting the parse step is a strict simplification with no
new resolution logic anywhere in this layer.

**4. The advertise/dial split.**
`RoleAddrs` gains `advertise_host: Option<String>` (`#[serde(default)]` —
absent means today's behavior exactly: every existing config and every
existing test is byte-identical). A new `--advertise-host NAME` flag joins
every self-registering entry point (`--config/--node`, `--cluster N`,
`join`, `data --seed`, `control`, `data --config`). Where present, every
`NodeAddrs` construction site advertises `advertise_host:port` for each
of the six ports instead of the address the listener actually bound to.
**Listeners keep binding literal `SocketAddr`s** — `--ip` stays a real
bind address (the pod's own IP, supplied via the Kubernetes Downward API,
`status.podIP`) — this field only changes what a node **tells the rest of
the cluster to dial**, never what it itself listens on. **One shared
advertise host for all six ports**, not a per-role override: this matches
the existing base-port-stride model (one `SocketAddr`, six port offsets)
exactly, and a per-role advertise override has no motivating deployment
shape yet — rejected as speculative generality until one exists.

### Why this closes the stale-address problem structurally

With a stable per-pod DNS name advertised (a StatefulSet pod's own
`<pod-name>.<governing-headless-service>.<namespace>.svc.cluster.local`),
a pod restart re-registers **the byte-identical `NodeAddrs`** it always
has — the same hostname, the same six ports, unchanged by whatever IP the
CNI handed the new pod. `RegisterNode`'s CAS therefore takes its existing
**same-identity-rejoin path**: a byte-identical re-registration is
`NoOp` (ADR 0032's rejoin idempotency, already implemented, already
tested). The `Rejected`-on-address-mismatch branch this ADR's Context
describes simply never fires for a pod restart, because there is no
longer an address mismatch to detect. **No new repair protocol is
introduced anywhere in this design** — the fix is entirely "stop making
the address volatile," not "detect and repair volatility after the
fact."

### Rejected alternative: "Path B" — keep numeric IPs, add a self-repair protocol

The alternative considered and rejected: keep `NodeAddrs` numeric, and
generalize the existing (currently update-only, ADR 0040 PR4-tightened)
`MetaCommand::RegisterNodeAddrs` into a genuine self-repair path — let a
node whose IP changed propose a correction, racing whatever CAS rule
governs who's allowed to update whose entry. Rejected for two reasons:

1. **It builds a new race-prone protocol around a value that churns on
   every single restart.** Every pod restart would need this repair path
   to run correctly, immediately, with no window in which a stale
   address is dialed — turning "restart hygiene" into an ongoing
   distributed-systems problem instead of a one-time addressing decision.
   Path A needs the repair protocol to fire **zero** times, ever, because
   there is nothing to repair.
2. **`RegisterNodeAddrs` is relayable over the intra port with no binding
   between the proposer's own identity and the node id it claims to
   update.** Widening its callers to cover this case would mean any node
   able to reach the intra port could propose a plausible-looking address
   update for *any* node id — today this is a narrow, update-only,
   already-registered-identity-required path (ADR 0040 PR4); generalizing
   it into "any node can correct any other node's address, triggered by
   IP churn" turns the intra network's existing trusted-network posture
   (below) into a genuine routing-hijack surface, not merely an
   unauthenticated one. The cost of closing that gap properly (binding a
   proposer to the identity it may update) is strictly greater than the
   cost of not needing the protocol at all.

### Restating the posture, explicitly

**Nothing in this ADR adds authentication to the intra, admin, join, or
any other internal port.** ADR 0020 and ADR 0047 already establish that
posture deliberately — the intra/admin surfaces are unauthenticated by
design, and the Kubernetes operator's own NetworkPolicy/Service topology
(Part 3, below) *is* the security boundary those ADRs assumed would
eventually exist. This ADR is what makes that assumption concrete; it
does not change the trust model itself. An operator that got the
NetworkPolicy wrong would expose every internal port cluster-wide (or,
worse, outside the cluster) with zero authentication behind it — the
NetworkPolicy is load-bearing, not a defense-in-depth nicety.

### Why none of this touches `animus-sim` or determinism

Every piece of Part 1 — `tokio::net::lookup_host`, `TcpStream::connect`'s
internal resolution, the peer-book/route-book type changes — lives
entirely inside `ProdEnv` and `animusd`. The `Env` seam (ADR 0003) is
`NodeId`-keyed, not address-keyed: `SimEnv`'s `Network` implementation
never resolves a hostname because it never had a real socket to dial in
the first place (sends are routed by `NodeId` through the simulator's own
in-memory delivery, unaffected by whether `ProdEnv`'s peer book happens to
hold a `String` or a `SocketAddr`). No simulation test, corpus, or
determinism guarantee is affected by this train.

## Decision, continued

### Part 2 — container image

A multi-stage `Dockerfile` at the repo root: a `rust:1.96` builder stage
producing the `animusd` and `animus` (`animus-cli`) binaries, copied into a
`debian:bookworm-slim` runtime stage running as a **non-root** user, with
`VOLUME /var/lib/animus` for the LSM data directory. `Cargo.lock` was
**gitignored** in this repository at the time this ADR was accepted — the
image build minted its own lockfile at build time rather than reusing a
committed one, noted here explicitly rather than silently discovered as an
irreproducible-build surprise later. A CI workflow builds the image on
every PR (build-only, no push — a normal compile-and-smoke gate) and
pushes `ghcr.io/animus-db/animusd` on `main` and on tags. **2026-09-02
(S-07a):** the operator's own controller binary (`animus-operator`, Part 3)
now has a second stage in the same `Dockerfile` (`runtime-operator`,
selected with `--target`) and a matching second image published by the
same workflow, `ghcr.io/animus-db/animus-operator`, on the same tag/push
rules — `deploy/operator/deployment.yaml`'s image reference is real, not a
placeholder.

**2026-09-02 (S-07a):** `Cargo.lock` is now committed — `.gitignore`'s
`Cargo.lock` line is gone, `Dockerfile` `COPY`s it alongside `Cargo.toml`,
and both the image build and CI's cargo build/test/clippy invocations pass
`--locked` so the committed lock is authoritative (a manifest/lock drift
now fails the build loudly instead of silently re-resolving). The
supply-chain concession this Part's Consequences bullet named is closed.

### Part 3 — the operator (`animus-operator`)

A new workspace crate, Rust, built on `kube-rs`, running a standard
reconciler loop over one CRD.

**CRD: `AnimusCluster`**, group `animusdb.io`, version `v1alpha1`,
namespaced.

Spec (initial surface):

| Field | Meaning |
|---|---|
| `image` | The `animusd` image to run. |
| `nodes` | Total pod/replica count. |
| `controlNodes` | Control-voter count (default `3`); **immutable after creation**. Pods `0..controlNodes-1` run role `Both` (ADR 0035); the rest run role `Data`. |
| `storage.size`, `storage.storageClassName?`, `storage.ephemeral?` | Per-pod PVC sizing/class, or an ephemeral (no-PVC) mode for throwaway clusters. |
| `resources?` | Pod resource requests/limits, passed through verbatim. |
| `basePort` | Port-stride base (default `14000`, matching `animusd`'s own default). |
| `clientService.type` | `ClusterIP` \| `LoadBalancer` \| `NodePort` — how the DynamoDB-only client Service is exposed. |
| `quiesceAfterSecs?`, `autoSplitBytes?` | Passthrough tuning, mapped straight onto the matching `animusd` flags. |
| `dynamoAuthSecretName?` | A `Secret` name mounted into every pod and wired to `--dynamo-auth` (ADR 0057). |

Status: `observedGeneration`, `readyNodes`, `phase`, `conditions` — the
conventional Kubernetes controller status shape, nothing bespoke.

**Reconciled children, per cluster:**

- A **ConfigMap** holding the generated `ClusterConfig` JSON — every
  pod's `RoleAddrs`, each advertising its own stable per-pod DNS name
  (`<pod>.<headless-svc>.<namespace>.svc.cluster.local`, Part 1's
  `advertise_host`) and binding `$POD_IP` (Downward API `status.podIP`,
  Part 1's unchanged literal-`SocketAddr`-bind contract).
- A **governing headless Service** (`clusterIP: None`,
  `publishNotReadyAddresses: true` — a forming pod's DNS must resolve
  before it passes its own readiness probe, or the cluster can't bootstrap
  at all) carrying `internal`/`intra`/`admin`/`console` — cluster-internal
  only, never intended to leave the cluster's own network.
- A **client Service** exposing **only** the `dynamo` port — the ADR 0047
  exposure model (client-facing wire edge only) realized as an actual
  Kubernetes Service boundary, type per `clientService.type`.
- A **StatefulSet**: `podManagementPolicy: Parallel` (control-plane
  bootstrap needs a quorum of pods up together, not a strict one-at-a-time
  rollout — see Bootstrap, below), `volumeClaimTemplates` for
  `/var/lib/animus`, readiness **and** liveness probes both on
  `GET /admin/health` (ADR 0020), and
  `terminationGracePeriodSeconds: 90` — `shutdown_graceful()` can take
  tens of seconds on a group with in-flight work, and Part 1's SIGTERM fix
  is only useful if Kubernetes actually waits long enough for it to
  finish.
- A **NetworkPolicy** restricting every non-`dynamo` port to traffic from
  the cluster's own pods (plus the operator itself, for its own admin-API
  reconciliation reads) — the concrete enforcement of the trusted-network
  posture Part 1 restates rather than changes.

### Bootstrap: static generated config, not imperative sequencing

The control-plane Raft group's genesis voter set must be known **up
front** — `control_ids` is fixed at process construction (ADR 0009/0035),
and a quorum of that fixed set is what makes the cluster live. The
operator therefore generates the **whole** `ClusterConfig` — every pod's
identity, role, and address — as one static document at CR-apply time,
mounted identically into every pod via the ConfigMap above. The cluster
becomes live the moment pods `0..controlNodes-1` are up and can form
quorum; no further operator action is needed for the control plane to
elect.

**Rejected alternative: pod-0 solo bootstrap, then sequential join, then
admin-driven control-group growth per pod.** Considered and rejected: this
shape makes the operator responsible for a sequence of imperative,
partially-idempotent admin actions (each pod's `join`, each control-voter
add via ADR 0037's admin path) that must complete in order, retried
individually on failure, with a real failure mode if a pod restarts
mid-sequence before its own join or its own promotion to voter has
committed. The static-config approach needs none of that: every pod's
final address and role are decided once, before any pod starts, and every
pod's own startup path (`--config FILE --node I`, entirely unchanged by
this ADR) does the rest. Growing the control voter set **after** creation
is a materially different, rarer operation (below) and keeps its own,
separate answer.

### Scale-up and scale-down

**Scale-up**: the operator appends new `Data`-role entries to the
ConfigMap and bumps the StatefulSet's replica count. Each new pod starts
on the identical `--config FILE --node I` path every existing pod already
uses — no special-cased "growth pod" bootstrap logic in the operator
itself, since ADR 0030's online-growth machinery already handles a new
data-role node joining a live cluster.

**Scale-down**: the operator drives the existing ADR 0032 sequence
directly — drain, poll drain-status to convergence, then remove via the
admin API — **before** deleting the corresponding pod and its PVC.
Scaling below `controlNodes` is rejected outright by the operator's own
validation (a `controlNodes`-immutable invariant with fewer live pods than
control voters is not a state this design supports).

**Growing the control voter set post-creation is explicitly out of
scope** for this ADR — `controlNodes` is immutable after `AnimusCluster`
creation. ADR 0037's admin API/CLI already exists for an operator (human,
this time) to grow the control group manually if a cluster genuinely
outgrows its original control-voter count; wiring that into
`animus-operator` itself is a named follow-up, not part of this design.

### Reconciler style

The reconciler is a thin `kube-rs` controller loop wrapping **pure
"desired child objects" builder functions** — deterministic, unit-tested,
no cluster access inside them (`spec -> {ConfigMap, Service, Service,
StatefulSet, NetworkPolicy}`, testable exactly like `animus-placement`'s
policy engine is tested against `animus-control`). Reconciliation itself
uses server-side apply against those desired objects, the conventional
`kube-rs` idiom, rather than hand-rolled diffing. **This crate holds no
`Env`-seam code** — it is production wiring outside `SimEnv`'s scope,
exactly like `animusd`'s own `main.rs`/deployment assembly — but follows
the same testable-core/imperative-shell discipline every other crate in
this workspace does: the "what should exist" decision is a pure function,
even though "make it exist" is an ordinary imperative Kubernetes client
call. **`BTreeMap` only** in any of the pure builder logic, per the
workspace-wide clippy lint (root `CLAUDE.md`) — this crate is not exempt
just because it sits outside the `Env` seam.

### Upgrades

**None, by design** — restating this repository's own no-back-compat
rule (root `CLAUDE.md`): there is no rolling-upgrade story, no
wire/WAL/on-disk-format compatibility guarantee across an image change.
Changing `spec.image` or any topology field beyond plain scale-up/down is
either rejected by the operator's own validation or requires recreating
the `AnimusCluster` from scratch. This is a deliberate consequence of the
repository's pre-alpha posture, not an oversight this ADR defers fixing.

### End-to-end testing

A `kind`-based smoke test in CI: create a `kind` cluster, install the CRD
and operator, apply an `AnimusCluster`, wait for `phase: Ready`, run a
`PutItem`/`GetItem` round trip through the client Service, scale `nodes`
up by one and wait for it to re-converge to `Ready`, then delete the CR
and confirm cleanup. This is a smoke test, not a substitute for the
existing sim/`ProdEnv` corpora — it proves the operator's own plumbing,
not `animusd`'s distributed correctness (which stays the sim corpus's
job).

### Not in v1 (explicitly deferred)

- **TLS** — on any port. No milestone in this codebase has added TLS
  anywhere yet; this ADR doesn't start.
- **Multi-cluster** — one `AnimusCluster` resource governs one AnimusDB
  cluster; no cross-cluster/federation concept.
- **Backups wiring** — `--backup-store` (ADR 0059) has no CRD-level
  surface yet; an operator user configures it by hand today via a manual
  Deployment/config; no `spec`-level passthrough is provided in this
  initial surface.
- **PITR** — no CRD surface (depends on the backups wiring above).
- **Control-voter growth** — `controlNodes` is immutable; ADR 0037's admin
  path remains the manual escape hatch.
- **`PodDisruptionBudget` tuning** — the StatefulSet ships with none
  beyond Kubernetes defaults; a deliberately-tuned PDB is a follow-up.
- **S3 `SegmentStore` backend** — ADR 0059 §1 already scoped this as its
  own follow-up trait-swap; this ADR doesn't touch it.
- **Admission/conversion webhooks** — the CRD ships with no webhook of any
  kind; `v1alpha1` has no prior version to convert from.

**A known, pre-existing flag-plumbing gap, noted for the operator's
benefit rather than fixed here**: `animusd`'s own `--quiesce-after`/
`--split-mode`/`--auto-split*` flags are documented (`animusd/CLAUDE.md`)
as **not yet wired** into the standalone `control`/`data` subcommands or
the `--cluster-control`/`--cluster-data` split-deployment dev path — only
`--config FILE --node I` and `--cluster N` carry them today. The operator
generates `--config FILE --node I` invocations exclusively (Bootstrap,
above), so this gap does not block v1 of the operator; it would need
closing first if a future operator version ever needed those knobs on a
`control`/`data`-subcommand-launched pod specifically.

**(2026-09-04 as-built note, #590)**: `--split-mode` itself no longer
exists — it and the copy-based split workflow it selected were deleted
outright from `animusd` (2026-09-01, ADR 0058's rung 4 layer), so the
paragraph above's mention of it is historical only (it was accurate when
this ADR was written, 2026-08-27, before that deletion). The CRD's
`AnimusClusterSpec.splitMode`/`spec.splitMode` field described in this
ADR's own spec table above has been removed to match — it had no
surviving flag to plumb into, and its presence made every `entrypoint.sh`
invocation with `splitMode` set a live pod-startup failure (`animusd`
rejects the unknown flag). `quiesceAfterSecs`/`autoSplitBytes` are
unaffected by this note; their own flag-vs-config-section story is
covered by S-06 (`crates/animus-operator/CLAUDE.md`'s CLI-flag-support
table), not restated here.

## Consequences

- A StatefulSet-managed cluster tolerates ordinary pod restarts (rolling
  update, eviction, node drain) without address rot — the central
  operational case the operator exists to support.
- Every pod restart now gets a real grace window to run
  `shutdown_graceful()` before Kubernetes escalates to a hard kill.
- `--seed`/every route book now carries a `String`, not a `SocketAddr` —
  a small but real widening of what a malformed config file can express
  (a genuinely unresolvable hostname fails at connect time rather than at
  parse time); acceptable, since a connect-time failure is already the
  ordinary "peer unreachable" case every existing retry loop already
  handles.
- **No new authentication anywhere** — an operator misconfiguration (a
  NetworkPolicy that's too permissive, or a client Service accidentally
  exposing an internal port) is a real, unmitigated exposure risk. This is
  named, not hidden: the operator's NetworkPolicy is the *entire* security
  boundary for every internal port, exactly as ADR 0020/0047 already
  assumed it eventually would be.
- The `animus-operator` crate is a new, independently-versioned surface
  with its own release cadence relative to `animusd` — a CRD schema change
  is itself subject to this repository's no-back-compat stance (a
  `v1alpha1` bump, or a clean recreate, not a migration).
- ~~`Cargo.lock` being gitignored means the container image's dependency
  versions are whatever `cargo build` resolves at image-build time, not a
  pinned, reviewed set~~ — closed 2026-09-02 (S-07a, see Part 2's own note
  above): `Cargo.lock` is committed and every image build is `--locked`.

## Delivery plan

Staged as `gh-stack` series per part (root `CLAUDE.md`'s stacked-PR
default):

- **Train 1 — `animusd` groundwork (Part 1).** Four PRs in the order
  listed above: SIGTERM + hostname `--seed`; `ProdEnv` peer-book
  hostname support; `animusd` route-book `String` migration; the
  advertise/dial split (`advertise_host` + `--advertise-host`). Each is
  independently reviewable and, per the described `#[serde(default)]`/
  additive-flag shape, individually a no-op for every existing
  deployment and test until the last PR's flag is actually set.
- **Train 2 — container image + CI (Part 2).** The `Dockerfile`, the
  build-on-PR workflow, and the push-on-main/tag workflow.
- **Train 3 — `animus-operator` (Part 3).** The CRD types, the pure
  builder functions (unit-tested first), the `kube-rs` controller loop,
  bootstrap, scale-up/down, and the `kind`-based e2e smoke test — likely
  its own multi-PR stack given its size, left to that train's own
  scoping pass rather than pre-divided here.
