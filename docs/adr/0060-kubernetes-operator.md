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
- A **PodDisruptionBudget** (S-07c, see this ADR's own 2026-09-06
  amendment below) bounding simultaneous voluntary evictions to whatever
  the cluster's own control-plane and data-plane quorum math allows.

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
- ~~**`PodDisruptionBudget` tuning** — the StatefulSet ships with none
  beyond Kubernetes defaults; a deliberately-tuned PDB is a follow-up.~~ —
  closed 2026-09-06 (S-07c, see this ADR's own amendment below).
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

## Amendment (2026-09-05, ADR 0064)

The "Not in v1 (explicitly deferred)" list above named TLS outright: "on
any port. No milestone in this codebase has added TLS anywhere yet; this
ADR doesn't start." [ADR 0064](0064-tls-on-every-port.md) was that
milestone, now landed in full (commit 3, 2026-09-05): `AnimusClusterSpec.
tls` (a pre-existing `Secret` or a cert-manager `Certificate`/`Issuer`),
the `StatefulSet` mount + `ClusterConfig` `tls` section this ADR deferred,
and a TLS-capable admin client for the scale-down drain sequence — see
ADR 0064's own commit-3 and closing amendment notes for the full as-built
account. The Consequences section's "no new authentication anywhere"
bullet below is accordingly narrowed the same way ADR 0047's own amendment
narrows its "intra stays unauthenticated" line: TLS is opt-in and
config-gated (a cluster with no `spec.tls` is unchanged), but a cluster
that turns it on gets mutual cluster-membership authentication on the
internal/intra ports and server-only TLS elsewhere — not a NetworkPolicy
replacement, a second, independent layer underneath it.

## Amendment (2026-09-06): S-04 PR 3 — `spec.s3`, egress policy

`docs/roadmap.md`'s S-04 item ("operator egress/credential-secret work")
is closed by this amendment, landing `AnimusClusterSpec.s3: Option
<S3StoreSpec>` (`crd.rs`) — the CRD-level surface the "Not in v1" list
above deferred under "Backups wiring," narrowed to exactly the S3 case ADR
0059's own S-04 design amendment scoped: `fs:`/`cluster`/`dir:` stores
still have no CRD surface (S-07b, unaffected by this amendment).

**`spec.s3` mirrors `spec.tls`'s own precedent** — a CRD section that only
*references* a pre-existing `Secret` (`credentialsSecretName`, keys
`access_key_id`/`secret_access_key`), never one this operator creates or
writes. `backupStore`/`segmentStore` are the literal `s3://...` URI values
`animusd`'s own `--backup-store`/`--segment-store` flags accept (ADR
0059's As-built PR 2 note); at least one must be set. Since this crate
does not depend on `animusd` (this file's own "does not depend on
`animusd`" note, restated in `crates/animus-operator/CLAUDE.md`), `crate::
s3_uri::parse` re-checks only a minimal syntactic subset of `animusd`'s
own `parse_s3_uri` (bucket present, `endpoint=` query key present, scheme
`http://`/`https://`) — enough to catch an obviously malformed URI at
reconcile time (`S3StoreSpec::validate`, same "no admission webhook in v1"
posture as `TlsSpec::validate`) rather than letting it reach a pod and
crash-loop; the real credential/region/loopback-vs-`--allow-insecure-s3`
logic stays exactly where it already lived, node-side in `animusd`.

**The credential never leaves the `Secret` mount.** Every pod mounts
`credentialsSecretName` read-only at `/etc/animus/s3` (the same idiom as
`/etc/animus/dynamo-auth`/`/etc/animus/tls`); a combined-role pod's own
generated `entrypoint.sh` reads both files (`access_key_id`/
`secret_access_key`) **at container-start time** and writes a scratch
`--s3-credentials` JSON file (`/tmp/animus-s3-credentials.json`) naming
only the *path* to `secret_access_key` — the secret value itself never
appears in the `ConfigMap`, `cluster.json`, a log line, or a CR status.
**Combined-role only**: `animusd data --config` accepts none of
`--backup-store`/`--segment-store`/`--s3-credentials`/
`--allow-insecure-s3` today (a **pre-existing** `animusd` gap, not
introduced here — see `crates/animusd/src/main.rs::run_data_config`'s own
"same documented gap as `--backup-store`" comment) — a data-only pod
mounts the `Secret` like every other pod but never reads it.

**Egress, closing the roadmap's own "egress unrestricted by omission"
line.** `desired::networkpolicy::build` now sets `policyTypes: [Ingress,
Egress]` unconditionally and adds two baseline `Egress` rules to *every*
cluster, `spec.s3` or not: intra-cluster (this cluster's own pods, the
`internal`+`intra` ports node-to-node Raft/RPC actually uses) and DNS to
`kube-system`'s `kube-dns`/CoreDNS pods (UDP+TCP 53) — without the DNS
rule, in-cluster name resolution itself (including an S3 endpoint's own
hostname) would break the moment egress stopped being wide-open by
omission. A third rule is added **only when `spec.s3` is set**: the
configured store URIs' own `endpoint=` port(s) (deduplicated; 443/80
default by scheme when the URI names no explicit port), scoped to
`spec.s3.egressCidrs` (`["0.0.0.0/0"]` by default). `NetworkPolicy` cannot
express a hostname allowlist — only IP blocks — so this operator has no
way to resolve an endpoint's hostname into the right CIDR itself;
`egressCidrs` exists precisely so an operator user can narrow it to their
object store's real address range, and both the CRD field's own doc and
`deploy/operator/example.yaml`'s commented `s3:` section say so plainly.
A cluster with no `spec.s3` gets exactly the two baseline rules, nothing
S3-specific.

**e2e**: `scripts/e2e-kind.sh --E2E_S3=1` (`.github/workflows/
e2e-kind.yml`'s `e2e-kind-s3` job) deploys a single-pod MinIO + Service
into the kind cluster, creates its bucket via a throwaway `minio/mc` pod,
creates the credentials `Secret`, applies an `AnimusCluster` with
`spec.s3.backupStore` pointing at `http://minio.<ns>.svc:9000`
(`allowInsecureHttp: true` — a loopback-to-the-cluster MinIO dev target,
never a real deployment shape), then exercises `CreateBackup`/
`DescribeBackup` over the DynamoDB wire and checks `GET
/admin/backup-store` reports `"kind":"s3"`. **UNVERIFIED in this
sandbox** — same `CAP_SYS_RESOURCE` reason `E2E_TLS`'s own leg is (see
`crates/animus-operator/CLAUDE.md`'s e2e section): written carefully and
`bash -n`-checked, never run end to end anywhere. Treat a first real CI
failure on `e2e-kind-s3` as this leg finding its first real bug.

## Amendment (2026-09-06): S-07b — non-S3 store CRD surface

`docs/roadmap.md`'s S-07 item b is closed by this amendment: the previous
S-04 PR 3 amendment above left `fs:`/`cluster`/`dir:` stores with no CRD
surface at all — an operator user wanting a plain filesystem-backed backup
store (no S3 bucket, no credentials) had to configure it by hand outside
the CRD. This amendment adds exactly that surface, alongside `spec.s3`,
never replacing it.

**Shape**: two plain top-level fields, not a sub-object —
`AnimusClusterSpec.backup_store: Option<String>` and `.segment_store:
Option<String>` (`crd.rs`) — carrying the literal `--backup-store`/
`--segment-store` flag value verbatim: `"cluster"` or `"fs:<path>"` for
`backupStore`, `"dir:<path>"` only for `segmentStore`. A plain `String`
was chosen over a `StoreSpec` enum (`Cluster`/`Fs(String)`) because the
value *is* the CLI flag's own text, unparsed, the same posture `spec.s3`'s
own `backupStore`/`segmentStore` fields already established for the
`s3://...` form — inventing a typed enum here would just be a second
representation of the identical three-way choice `animusd`'s own parser
already makes, for a section with no sub-fields (no credentials, no CIDR
list) to justify a struct the way `S3StoreSpec` earns one.

**Validation** (`AnimusClusterSpec::validate_store_spec`, called from
`crate::controller::reconcile` right after `spec.s3`'s own validation —
same "no admission webhook in v1" posture as `TlsSpec`/`S3StoreSpec`):

- `backupStore` accepts exactly `"cluster"` or `"fs:<path>"`.
- `segmentStore` accepts only `"dir:<path>"` — **`animusd`'s own
  `--segment-store` has no `"cluster"` keyword at all**
  (`crates/animusd/src/main.rs::parse_segment_store`'s own doc comment:
  omitting the flag is the *only* way to select its default). A literal
  `"cluster"` in `segmentStore` is rejected rather than silently
  remapped to "omit the flag" — this is the one place the two fields'
  grammars are *not* symmetric, and getting it wrong would otherwise
  surface as a live pod-startup failure (`animusd` rejecting an unknown
  value) instead of a reconcile-time status condition.
- `<path>` must be an absolute path strictly under
  `desired::cluster_config::DATA_DIR` (`/var/lib/animus`) — **the pod's
  own data volume, and the only directory this operator can vouch is
  actually mounted**: a `PersistentVolumeClaim`, or an `emptyDir` when
  `spec.storage.ephemeral` is set. Never `DATA_DIR` itself (that root is
  where `animusd --dir` puts the storage engine's own on-disk files — a
  store sharing it exactly would mix its own objects in among them; a
  subdirectory is required). A path outside `DATA_DIR` is rejected
  outright rather than silently accepted and left to fail at pod startup
  with a permissions or missing-directory error — the same "catch it at
  reconcile time, not at container start" motivation `S3StoreSpec::
  validate` already has for a malformed URI.
- An `s3://...` value in either field is rejected, pointing at `spec.s3`
  instead: only that section supplies the credentials an S3 store needs,
  so an `s3://...` string in the non-S3 field is always a copy-paste
  error, never a valid configuration.
- Setting the same store in both `spec.s3` and the matching top-level
  field (e.g. both `spec.s3.backupStore` and `spec.backupStore`) is
  rejected as a conflict naming both — there is no sensible "last one
  wins" semantics here, and picking one silently would make the
  generated `entrypoint.sh` depend on write order in a way nothing in
  the spec exposes.

Any rejection sets a `StoreSpecInvalid` status condition and reconciles
the rest of the spec with both fields stripped (never getting stuck
entirely on one bad field) — the identical posture `TlsSpecInvalid`/
`S3SpecInvalid` already use.

**No new volume, no new `Secret`, no `desired::statefulset` change at
all** — this is the detail that makes this surface materially smaller
than `spec.s3`'s: `cluster`/`fs:`/`dir:` need no credentials, so the
pod's already-mounted data volume is the only thing an `fs:`/`dir:` path
can point at, and the `DATA_DIR`-prefix validation rule above is what
makes that true by construction rather than by convention. `desired::
cluster_config::entrypoint_script` emits `--backup-store`/
`--segment-store` from whichever of `spec.s3.{backup,segment}Store` (the
`s3://...` form, its own credentials preamble unchanged from the S-04 PR
3 amendment) or the new top-level `spec.{backup,segment}Store` is set —
mutually exclusive per store by the validation above, so the builder
itself is a plain `.or()` between the two `Option<&str>`s. Both single-
quoted via the pre-existing `shell_single_quote`, same as every other
operator-controlled string this generator interpolates into the `sh`
script. Reaches only **combined-role pods** — the identical pre-existing
`animusd` gap (`animusd data --config` accepts none of these flags) the
S-04 PR 3 amendment already documents; a data-role pod's data volume is
mounted the same way, it just has no `entrypoint.sh` branch that would
ever read these fields.

**Deliverable**: `crd.rs` (fields + `validate_store_spec` + unit tests),
`desired::cluster_config` (flag emission + golden tests),
`controller.rs` (the validation call + fake-harness tests),
`deploy/operator/crd.yaml` regenerated (`crd_manifest_pinned` green),
`deploy/operator/example.yaml`/`README.md` updated, this ADR, this
crate's own `CLAUDE.md`, and `docs/roadmap.md`'s S-07 item b removed.
`scripts/e2e-kind.sh`'s plain-TCP leg additionally sets
`spec.segmentStore: dir:<data-mount>/segments` unconditionally (chosen
over `backupStore` specifically so it composes with the pre-existing
`E2E_S3=1` leg, which already sets `spec.s3.backupStore` — using
`segmentStore` here avoids a same-reconcile conflict between the two
legs when both are enabled) and checks `GET /admin/segment-store` reports
`"kind":"fs"`.

## Amendment (2026-09-06): S-07c — PodDisruptionBudget

`docs/roadmap.md`'s S-07 item c is closed by this amendment, and it also
closes this ADR's own "Not in v1" bullet on the topic: every
`AnimusCluster` now gets a `{name}-pdb` `PodDisruptionBudget`
(`crate::desired::poddisruptionbudget`), applied unconditionally alongside
the other five children (`ConfigMap`/`Service` x2/`NetworkPolicy`/
`StatefulSet`), never a sixth *optional* child the way `spec.tls.
certManager`'s `Certificate` is.

**The budget is derived from the cluster's own quorum math, never a
constant.** Two independent things must each keep a majority alive under
a voluntary eviction:

- **The control-plane Raft group** — exactly `spec.controlNodes` pods
  (ordinals `0..controlNodes-1`) are voters (this ADR's own spec table
  above), tolerating `floor((controlNodes - 1) / 2)` simultaneous losses.
- **Every data-plane tablet group** — `animusd` places each tablet on the
  first `min(N, MAX_REPLICATION_FACTOR)` `Active` members it sees, where
  `MAX_REPLICATION_FACTOR = 3` (`crates/animusd/src/lib.rs`) is a fixed
  constant today, not a `spec`-level knob. This crate has no dependency on
  `animusd` (this crate's own `CLAUDE.md`), so the constant is mirrored by
  hand (`desired::poddisruptionbudget::DATA_PLANE_MAX_REPLICATION_FACTOR`)
  — the identical manual-sync posture `desired::cluster_config`'s
  `ClusterConfig`/`RoleAddrs` JSON mirror already established. The
  operator cannot see *which* pods actually hold any given tablet's
  replicas (placement is the data plane's own runtime decision), so the
  safe assumption is the worst case: any of the cluster's `nodes` pods
  could be asked to host one, capped at the target replication factor —
  effective RF `= min(nodes, MAX_REPLICATION_FACTOR)`, tolerating
  `floor((rf - 1) / 2)` simultaneous replica losses.

`maxUnavailable` is the smaller of the two:

```
maxUnavailable = min(
  floor((controlNodes - 1) / 2),
  floor((min(nodes, MAX_REPLICATION_FACTOR) - 1) / 2),
)
```

A single global `PodDisruptionBudget` selecting every one of the
cluster's own pods (`selector_labels`) caps *simultaneous* voluntary
evictions cluster-wide regardless of which specific pods a scheduler
picks, which is exactly what bounds both risks at once with one budget —
no need for two separate PDBs scoped to disjoint pod subsets.

**Degenerate shapes, handled explicitly (with tests)**: `nodes == 1`,
`controlNodes == 1`, and `nodes < MAX_REPLICATION_FACTOR` (2, since RF is
fixed at 3) all compute `maxUnavailable = 0` — **blocking every voluntary
eviction outright, which is the correct, intended outcome, not a bug**: a
single-voter control plane or a tablet group with only one live replica
cannot survive losing its one copy, voluntarily or otherwise. A larger
`controlNodes` than the data-plane budget allows is capped by the
data-plane term (e.g. `nodes=10, controlNodes=7` still computes `1`, from
the RF-capped data term, not the control term's own `3`); a larger data
capacity than `controlNodes` allows is capped the other way
(`nodes=10, controlNodes=1` computes `0`). Every input is clamped to at
least `1` before the arithmetic runs, so the function never panics or
returns a negative budget even on a not-yet-valid or momentarily
inconsistent spec (`spec.nodes >= 1`/`controlNodes <= nodes` are each
pre-existing, and separately enforced/documented, invariants this
builder does not re-validate).

**Expressed as `maxUnavailable`, never `minAvailable`.** The two are
mutually exclusive on a `PodDisruptionBudgetSpec` and can express the
identical constraint against a *known* total pod count, but
`minAvailable` would be `nodes - maxUnavailable` — recomputed on every
`spec.nodes` change. `maxUnavailable` itself is scale-invariant across
the range that matters in practice: once `nodes` and `controlNodes` each
reach `3` (this operator's own default), the value stays `1` for any
larger `nodes`, since `controlNodes` is immutable after creation and the
effective replication factor plateaus at `MAX_REPLICATION_FACTOR`. A
scale-up/down within that range needs no PDB change at all — though
`apply_children` re-derives and re-applies it every reconcile regardless
(the same unconditional-re-apply posture every other required child
already has), always from the *desired* spec (`spec.nodes`/
`spec.controlNodes`), **never** the `StatefulSet`'s live/current replica
count, which would make the budget momentarily wrong mid-scale. A
dedicated controller-level test drives this through `FakeClusterApi`: a
cluster previously scaled to 5 replicas (RF-plateaued budget of `1`)
reconciled down to `nodes: 2, controlNodes: 2` immediately gets the
stricter `0`, not the stale 5-node shape's `1`.

**No CRD field for this.** An override could only ever be asked to (a)
loosen the computed value, which is unsafe by construction and would have
to be rejected anyway, (b) tighten it, which a smaller `spec.nodes`/
`spec.controlNodes` already achieves directly, or (c) disable the budget
outright — which would make this the *first* required child this
operator ever stops applying once a spec says so; every other required
child is applied unconditionally forever, and there is no finalizer or
deletion path (this ADR's own "no finalizer in v1" decision) for a child
that used to be desired and no longer is. Since the safe value is already
a pure, fully-determined function of two existing spec fields, there is
nothing left for a CRD field to usefully express today. If a real need
for an override surfaces later, add it then, with its own deletion story
(what happens to a previously-applied PDB when a later spec disables it)
worked out at the same time — don't reach for delete-on-toggle without
that answered first.

**RBAC**: `deploy/operator/rbac.yaml` grants the `policy` API group's
`poddisruptionbudgets` the same full verb set (`get/list/watch/create/
update/patch/delete`) as every other owned kind.

**Deliverable**: `crate::desired::poddisruptionbudget` (builder + golden
JSON test + arithmetic unit tests covering every degenerate shape),
`crate::desired::mod`'s `pod_disruption_budget_name` helper,
`crate::cluster_api::ClusterApi::apply_poddisruptionbudget` (+
`RealClusterApi`/`FakeClusterApi` implementors), `crate::controller::
apply_children` (ordering) and `run()`'s `.owns(Api::<PodDisruptionBudget>
::all(..))` watch wiring, controller-level tests through
`FakeClusterApi` (applied once per reconcile, owner reference set,
selector matching the *actual* `statefulset::build` output's pod-template
labels, and the scale-down-recomputes-from-desired-spec transition),
`deploy/operator/rbac.yaml`, `deploy/operator/README.md`'s owned-resources
list and a new "PodDisruptionBudget" section, this ADR, this crate's own
`CLAUDE.md`, and `docs/roadmap.md`'s S-07 item c removed. No CRD field was
added, so `deploy/operator/crd.yaml` is unchanged and `crd_manifest_
pinned` needed no regeneration. `scripts/e2e-kind.sh`'s plain-TCP leg
additionally asserts `kubectl get pdb` reports `maxUnavailable: 1` for the
smoke's own 3-node/3-`controlNodes` shape, both right after the initial
3/3-ready wait and again after the scale-up to 4 nodes (pinning the
scale-invariance claim above against a real cluster, not just the unit
corpus).

## Amendment (2026-09-06): operator admin access through the API server pod proxy

**Groundwork for S-07d and for `drain_and_remove_node`'s own reachability.**
`scripts/e2e-kind.sh` runs the operator **out-of-cluster**
(`cargo run -p animus-operator -- run` against the runner's local
kubeconfig) — the documented local-iteration shape, and the shape every
`e2e-kind` CI leg uses. From there, neither a pod's headless-`Service` DNS
name (`<pod>.<svc>.<ns>.svc.cluster.local`) nor its `10.244.x.x` pod IP is
routable: both live on the cluster's own pod network, which a process
outside the cluster (a laptop, a CI runner) simply has no route to. Every
admin-port call this controller makes — the scale-down drain sequence
(`drain_and_remove_node`, this ADR's own "Scale-up and scale-down"
section) today, and S-07d's `spec.controlNodes` growth sequence once it
lands — therefore could never succeed out-of-cluster, a gap the e2e smoke
never caught because it has never yet forced a scale-down (a real
`kind`-cluster reproduction of this exact failure mode is documented in
`docs/engineering-lessons.md`).

**Fix: reach the pod through the Kubernetes API server's pod-proxy
subresource instead of dialing it directly** — `GET`/`POST
/api/v1/namespaces/{ns}/pods/{scheme}:{pod}:{port}/proxy{path}`, `scheme`
being `http` or `https`. The API server is the one address this
controller reaches from *every* deployment shape (it is what `kube::
Client::try_default()` connects to, whether that resolves to an in-cluster
service account or a local kubeconfig), so proxying an admin request
through it — rather than dialing the pod's own address — closes the
reachability gap structurally, the same way this ADR's Part 1 (a stable
pod DNS name instead of a moving pod IP) closed the *stale-address*
problem structurally rather than adding a repair loop. **No CA plumbing
is needed on this path**: the API server itself dials TLS to the pod for
an `https:` proxy target and does not verify the pod's serving
certificate (Kubernetes' own pod-proxy behavior, not a choice made here)
— this is genuinely simpler than the direct-dial TLS client
(`crate::admin_client`'s `AdminConnector`/`build_tls_connector`, ADR 0064
commit 3), which still exists and still verifies the cluster CA, for the
other mode below.

**`animus-operator run --admin-access {proxy,direct}`**, defaulting to
`proxy`:

- **`proxy`** (default): every admin call goes through the pod-proxy
  subresource above. Works in-cluster and out-of-cluster identically —
  one mode, one behavior, regardless of `deployment.yaml` vs. `cargo run
  -p animus-operator -- run`. The extra API-server hop this adds is a
  non-concern: an admin-port call is rare (a handful of requests across a
  whole scale-down or a one-voter growth step), never a hot path.
- **`direct`**: dials the pod's admin port itself, exactly as before this
  amendment (plain HTTP, or server-only TLS trusting `spec.tls`'s
  resolved CA). Kept because it is strictly simpler when it does apply —
  no extra hop, no proxy-subresource RBAC — and because a future
  operator-hardening item might want to reason about it independently;
  it only works in-cluster.

`deployment.yaml` (the in-cluster shape) passes no `--admin-access` flag
either, so it also gets `proxy` by default — deliberately: this ADR's
whole point in Part 3 was one controller binary working identically
in-cluster and out-of-cluster (`animusd`'s own combined/control-only/
data-only role split precedent), and special-casing the in-cluster
deployment onto a different admin-access mode than local iteration uses
would reintroduce exactly the "works here, not there" asymmetry this
amendment closes. An operator that wants the old direct-dial behavior
in-cluster can still ask for it explicitly.

**Every admin request, in both modes, is bounded by a real timeout**
(`crate::admin_client::ADMIN_REQUEST_TIMEOUT`, a few seconds) — connect
through response for `direct`, the whole API-server round trip for
`proxy`. An unroutable pod IP or DNS name used to have no such bound (a
`direct`-mode `TcpStream::connect` against a dead address can hang far
longer than any reconcile loop should tolerate); now it fails a reconcile
step fast, surfacing as a real, retried `DrainFailed`/growth-failure
condition rather than a stuck reconcile indistinguishable from a hung
controller process.

**`AdminOps`'s signature is unchanged.** `crate::admin_client::AdminOps::
get_json`/`post_json` still take the same `url: &str` +
`ca_pem: Option<&[u8]>` shape `AdminClient` (the pre-existing direct
implementor) always has — `crate::admin_client::ProxyAdminClient` parses
`admin_base_url`'s own URL shape (`{scheme}://{pod}.{internal-svc}.
{ns}.svc.cluster.local:{port}{path}`, six dot-separated host labels, pod
name first and namespace third) back apart into the `(namespace, pod,
port, scheme)` the pod-proxy path needs, rather than threading a second,
structured target type through the trait. This crate owns both ends of
that URL contract (`admin_base_url` is its only producer), so parsing it
back apart is safe and keeps every existing `AdminOps` call site —
`drain_and_remove_node` today, S-07d's growth sequence once it lands —
unchanged; `ca_pem` is accepted by `ProxyAdminClient` only to keep the
signature identical, and ignored (see above). `crate::fakes::
FakeAdminClient` (the test seam) is likewise untouched: it already records
the same logical `(method, url)` pair regardless of which real
implementor would have served it.

**RBAC**: `deploy/operator/rbac.yaml` adds `pods/proxy` (`get`/`create`)
alongside the pre-existing `pods: get/list/watch` — granted unconditionally
(RBAC has no notion of "only when `--admin-access proxy`"), documented in
`deploy/operator/README.md`'s new "Admin access" section.

**Deliverable**: `crate::admin_client::{ProxyAdminClient, AdminAccessMode,
RealAdminClient, ADMIN_REQUEST_TIMEOUT}` (+ unit tests for proxy-path
construction, URL round-tripping, error mapping, and the timeout/mode
defaults), `crate::controller::run`'s new `admin_access` parameter and
`admin_base_url`'s `pub(crate)` visibility bump (so `ProxyAdminClient`'s
own tests can build one), `crate::main`'s `--admin-access` argv parsing,
`deploy/operator/rbac.yaml`, `deploy/operator/README.md`, this ADR, and
`docs/engineering-lessons.md`. No CRD field, no `deploy/operator/crd.yaml`
regeneration. **What only a real `kind` cluster can prove**: that the API
server's pod-proxy subresource actually forwards a request to a live
pod's admin port end to end (method, headers, body, and the response
verbatim) — this crate's own unit corpus proves path construction and
error-mapping only, never a live proxied round trip; `scripts/e2e-kind.sh`
already runs entirely out-of-cluster (this amendment's default `proxy`
mode is exercised on every leg by construction, not as a special case),
and closes the S-07d CI failure this amendment's own title names.
