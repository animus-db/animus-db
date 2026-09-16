# ADR 0064 — TLS on every port

- **Status:** Accepted — all four commits of `S-01` landed: mutual TLS on
  the intra-node wire inside `ProdEnv` (commit 1); TLS on every `animusd`
  listener and dialer — client, intra-`ClientRequest`, admin, console —
  plus `animus-cli`'s client-protocol and admin dials (commit 2); the
  Kubernetes operator's cert-manager `Certificate`/`Secret` wiring plus its
  admin client's TLS connector (commit 3); and the closing website/roadmap
  update (commit 4, this amendment) — all config-gated, default off. See
  Decision 6 for what remains explicitly out of scope (cert rotation
  without a restart, SNI multi-tenancy, client certs on the client port, a
  managed/rotated CA) — those are real, named follow-ups, not part of this
  milestone.
- **Date:** 2026-09-05
- **Amends:** [ADR 0047](0047-intra-node-port.md) (port classes — TLS is
  orthogonal to the internal/intra/client/admin/console split that ADR
  makes; see its own amendment note below), [ADR 0057](
  0057-sigv4-client-auth.md) (named TLS "a separate concern" for the client
  port; this ADR is that concern, now taken up), [ADR 0060](
  0060-kubernetes-operator.md) ("TLS — on any port. No milestone in this
  codebase has added TLS anywhere yet; this ADR doesn't start" — this ADR
  does)
- **Origin:** `docs/roadmap.md`'s S-01 ("TLS on every port")

## Context

Every port this codebase serves is plaintext today. `docs/roadmap.md`
names the gap plainly: "none anywhere: client, intra-node, admin,
console." Three ADRs already flagged TLS as a deferred concern rather than
an oversight — ADR 0047 split node-to-node traffic onto its own `intra`
port purely on trust/reachability grounds (an operator keeps it off any
externally-reachable Service) without touching confidentiality or peer
authentication; ADR 0057 added SigV4 request signing to the client port
and explicitly noted "TLS (a separate concern; SigV4 does not protect
confidentiality)"; ADR 0060's Kubernetes operator design deferred TLS
outright ("no milestone in this codebase has added TLS anywhere yet; this
ADR doesn't start"). This ADR is that milestone.

The gap has a sharper edge on the **internal** ports than the client-facing
ones: `internal` (the raw Raft wire) and, since ADR 0047, `intra`
(`ClientRequest` relays — `Forwarded`, `ProposeSchema`, `WatchMetadata`,
`JoinInfo`, every internal-only forwarding payload) carry no authentication
of *any* kind today, not even SigV4's static-secret approximation. Any host
that can reach a node's internal port can inject Raft frames or intra
relays as if it were a cluster member — the operator's `NetworkPolicy`
(ADR 0060) narrows *reachability* but is not the same guarantee as the
wire itself refusing a non-member. This ADR closes that gap at the
transport layer: **mutual TLS on the internal ports authenticates cluster
membership**, not just narrows network reachability.

`rustls` (0.23, `ring` crypto provider), `tokio-rustls` (0.26), and
`rustls-pki-types` are already in the workspace dependency graph —
`animus-operator`'s `kube` dependency pulls them in via its `rustls-tls`
feature (`deny.toml` already allow-lists the license chain this drags in,
including the `webpki-root-certs` CDLA-Permissive-2.0 data crate). This ADR
is the first place the workspace's *own* code drives `rustls` directly,
rather than consuming it transitively through `kube`.

## Decision

### 1. Mutual TLS on the intra-node ports

The internal Raft wire (this commit) and, in commit 2, the intra
`ClientRequest` port both speak **mutual** TLS when configured: every node
presents a certificate signed by a per-cluster CA, and verifies its peer's
certificate against that same CA before any frame is read or written. This
is what closes the membership-authentication gap named above — a host
without a cert the cluster's CA signed cannot complete a handshake at all,
so it can never reach the point of injecting a Raft frame or an intra
relay. Client-facing SigV4 (ADR 0057) is unrelated and unaffected: it
authenticates a DynamoDB *caller's* identity against a static secret, not a
cluster *member's* identity against a CA.

### 2. Server-only TLS on the client, admin, and console ports

The DynamoDB client port, the admin/debug HTTP-JSON interface (ADR 0020),
and the Data Console (ADR 0052) get **server-only** TLS when configured: a
client verifies the node it's talking to, but the node does not require or
verify a client certificate. SigV4 (ADR 0057) remains the client-side
identity story on the DynamoDB port exactly as that ADR designed it — TLS
here buys confidentiality and server authenticity, not caller identity.
**Client-cert auth on the client port is a possible follow-up**, not part
of this decision: it would let a deployment skip SigV4 entirely for
mTLS-only client authentication, but that is a materially different trust
model (per-client certs issued and rotated, vs. a static secret map) and is
left for a future ADR if a deployment actually wants it.

### 3. Config shape

```rust
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: Option<PathBuf>,
}
```

File-based, like `--dynamo-auth` (ADR 0057) — no inline PEM in config or
CLI args. `ca_path` is **required** wherever mutual TLS is the mode (the
internal/intra ports) and **optional** everywhere TLS is server-only (the
client/admin/console ports, where there is no peer client cert to verify
against a CA at all). Default is off across every port: a node with no
`TlsConfig` at all behaves byte-for-byte as it always has — plain TCP,
unauthenticated at the transport, exactly today's posture.

**A cluster is either all-TLS or all-plain on the internal wire.** A
Raft group cannot usefully have some members dialing in plaintext and
others requiring a handshake — a mixed cluster would either silently drop
half its peers or accept unauthenticated connections from the other half,
neither of which is a real security posture. Commit 2's config validation
enforces this as a hard startup error (checked once, at cluster-config
load, across every node's `internal`/`intra` TLS setting) rather than
leaving it to be discovered at the first failed handshake. The
client/admin/console ports have no such constraint — each port's TLS mode
is independent, since a mismatched client-port setting only affects that
port's own callers, never cluster membership.

### 4. The transport wrapper shape

```rust
pub enum MaybeTlsStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::TlsStream<TcpStream>>),
}
```

implementing `AsyncRead + AsyncWrite` by delegating to whichever variant is
live. Two alternatives were rejected:

- **`Box<dyn AsyncRead + AsyncWrite + Unpin + Send>`** — this sits on the
  hot path every frame moves through, including every Raft heartbeat; a
  vtable indirection and a heap allocation per stream (rather than per
  connection lifetime, since the trait object would need re-boxing at
  every layer that constructs one) is real, avoidable cost for no benefit
  the enum doesn't already give — `tokio_rustls::TlsStream<T>` itself
  already unifies the client- and server-initiated cases, so the enum's
  only two variants are exactly "plain" and "TLS," nothing finer-grained
  is needed.
- **A Cargo feature (`tls`)** — rejected because it would bifurcate this
  crate's own *build*: two separately-compiled variants of `animus-env`
  (and everything downstream that names `MaybeTlsStream`) to maintain and
  test, for a decision that is a **runtime** config choice (a node either
  has a `TlsConfig` or it doesn't), not a compile-time capability
  question. The existing `prod` feature (ADR 0061 rung C0) already answers
  the compile-time question this crate needed answered — "can this crate
  reach real sockets at all" — and TLS support rides inside that same
  gate rather than adding a second axis.

`MaybeTlsStream` lives in `animus-env` (behind the `prod` feature,
alongside `ProdEnv` itself) specifically so `animusd`'s own listeners
(commit 2) can reuse the identical type for the client/intra/admin/console
ports instead of reimplementing the wrapper.

### 5. Crypto provider: `ring`

`rustls` 0.23 requires picking a `CryptoProvider` explicitly (no more
implicit default). This codebase pins `ring` — not `aws-lc-rs`, rustls's
own default — specifically because `ring` is **already** in the dependency
graph via `kube`'s `rustls-tls` feature (`animus-operator`). Picking the
same provider means the workspace carries one crypto backend, not two;
`aws-lc-rs` requires a C/assembly toolchain at build time (`aws-lc-sys`)
that `ring`'s pure-Rust-plus-select-asm build does not, so standardizing on
`ring` also keeps the build story uniform across every crate that ends up
touching TLS.

### 6. Explicitly out of scope

- **Cert rotation without a restart.** `TlsConfig::load()` reads PEM files
  once at bind time; a rotated cert on disk has no effect until the
  process restarts. A live-reload mechanism (a `ServerConfig`/
  `ClientConfig` swap behind an `ArcSwap` or similar, triggered by a
  filesystem watch or a signal) is a real, valuable follow-up but not part
  of this decision — every port's TLS material is as static as its
  listen address is today.
- **SNI-based multi-tenancy.** Nothing here uses TLS Server Name
  Indication to route one listener to different certificate material per
  virtual host; every port has exactly one `TlsConfig` for its whole
  lifetime.
- **Client certs on the client port.** Noted above (Decision 2) as a
  possible, materially different follow-up, not attempted here.
- **A managed/rotated CA.** This ADR assumes an operator (human, or
  cert-manager per commit 3) hands every node its cert/key/CA files
  already issued; no CA-issuance logic lives in this codebase.

### 7. Certificate SAN requirement

A node's certificate must carry a Subject Alternative Name for every
string another node's peer book might dial it by: its bind address's IP,
and — if it is ever registered by hostname (a Kubernetes pod's stable DNS
name, ADR 0060's advertise/dial split) — that DNS name too. The TLS
handshake's hostname verification runs against whichever string the peer
book actually holds for that peer (`ProdEnv`'s peer book is `host:port`,
numeric or a hostname, per ADR 0060), so a certificate missing the SAN a
caller dials by fails the handshake outright. This is a deployment/
cert-issuance concern this ADR records rather than papers over: commit 3's
cert-manager `Certificate` resource must issue with SANs covering both the
pod's internal Service DNS name and, if numeric peer addresses are ever
used, the pod IP.

## Testing expectation

- **This commit**: `prod::tests` loopback tests using an `rcgen`-generated
  self-signed CA and per-node leaf certs (dev-dependency only, never a
  production one) — frames flow both ways under TLS, a peer presenting a
  cert from a different CA is refused with no delivery and no panic, a
  plain-TCP dial into a TLS listener fails cleanly while the listener keeps
  serving genuine TLS peers, and a `send`/reconnect survives a TLS peer
  restart exactly like the existing plain-TCP reconnect test. The existing
  plain-TCP `prod::tests` are unmodified and stay green — `ProdEnv::bind`
  keeps its exact signature and behavior; TLS is reached only through the
  new `ProdEnv::bind_with_tls` constructor.
- **Commit 2**: a TLS variant of each real-listener `animusd` integration
  test (`prod`-feature-gated, real-thread) for the client/intra/admin/
  console listeners, plus the all-TLS-or-all-plain config validation
  (a startup error, tested directly).
- **Commit 3**: the `kind` e2e smoke (`scripts/e2e-kind.sh`) exercised with
  cert-manager-issued certificates end to end, proving the operator's
  `Certificate`/`Issuer` wiring actually produces material the nodes
  accept.

## Consequences

- **The internal wire's membership-authentication gap is closed** once a
  cluster turns TLS on: a host without a cluster-CA-signed cert cannot
  complete a handshake, so it cannot inject Raft frames or intra relays —
  a strictly stronger guarantee than the operator's `NetworkPolicy`
  reachability narrowing alone (ADR 0060), which stays in place as
  defense in depth, not a replacement.
- **Still opt-in.** Every existing deployment, test, and quick-start keeps
  working unchanged — `TlsConfig` is `None` by default at every layer this
  commit touches, and `ProdEnv::bind` (every existing call site, ~25+
  across the workspace) is untouched.
- **A new, small crypto dependency surface** inside `animus-env` itself
  (previously only `animus-dynamo`, for SigV4, touched crypto code) —
  `rustls`, `tokio-rustls`, `rustls-pemfile`, `rustls-pki-types`
  unconditionally under the `prod` feature, `rcgen` dev-only. All
  Apache-2.0/MIT/ISC, already covered by `deny.toml`'s existing allow-list
  (added there for `kube`'s own `rustls-tls` chain).
- **Cert issuance and rotation are now an operational concern** for any
  deployment that turns TLS on — commit 3's cert-manager wiring automates
  issuance for the Kubernetes operator target; a bare-metal/manual
  deployment must issue and place PEM files itself, and rotate them with a
  restart (see Decision 6).

## Alternatives considered

- **A Cargo feature for TLS support, mirroring the `prod` feature's
  shape.** Rejected — see Decision 4: this is a runtime config choice, not
  a compile-time capability question, and a second feature axis would
  double the build/test matrix for no correctness benefit.
- **`native-tls` (OpenSSL/Schannel/Security.framework bindings) instead of
  `rustls`.** Rejected: `rustls` is already in the graph via `kube`, is
  pure Rust (no C/FFI, matching this workspace's `unsafe_code = "forbid"`
  posture for its own crates and avoiding a new build-toolchain
  dependency), and `tokio-rustls` is the natural fit for the `tokio`-based
  transport `ProdEnv` already uses.
- **`aws-lc-rs` as the crypto provider instead of `ring`.** Rejected: see
  Decision 5 — `ring` is already resolved in the graph via `kube`, and
  picking it avoids a second crypto backend plus `aws-lc-rs`'s heavier
  C/assembly build requirement.
- **Skip mutual TLS on the internal wire; server-only everywhere,
  matching the client port.** Rejected: the internal wire's whole point is
  cluster-membership trust — a non-member should never get past the
  transport handshake at all. Server-only TLS would encrypt the wire but
  do nothing for the actual gap this ADR opens with (anyone reachable on
  the port can inject frames); only mutual TLS closes it.

## Amendment note (ADR 0047)

This ADR is the "TLS is orthogonal to the internal/intra/client/admin/
console classification" instance ADR 0047's port-class split anticipated
without building: `intra`'s separation from `client` was about audience
and reachability, not confidentiality or peer authentication. This ADR
adds TLS as an independent axis over that same classification — every
port keeps its existing class and purpose; TLS is a mode each port can be
configured into, not a new class.

## Amendment note (ADR 0057)

ADR 0057's "TLS (a separate concern; SigV4 does not protect
confidentiality)" is the concern this ADR takes up. SigV4 and TLS remain
independent and complementary on the client port exactly as ADR 0057
anticipated: SigV4 authenticates the caller's identity against a static
secret; server-only TLS (Decision 2) adds confidentiality and server
authenticity underneath it. Neither depends on or subsumes the other.

## Amendment note (ADR 0060)

ADR 0060's "Not in v1 (explicitly deferred)" list named TLS outright: "on
any port. No milestone in this codebase has added TLS anywhere yet; this
ADR doesn't start." This ADR is that milestone; commit 3 of this series
adds the operator's cert-manager `Certificate`/`Issuer` wiring + volume
mounts + `ClusterConfig` cert-path fields ADR 0060 deferred, once commits 1
and 2 give the operator something to configure.

## Amendment note (commit 2 landed, 2026-09-05)

Commit 2 (S-01 step 2) lands Decision 1/2's actual mechanism in `animusd`
and `animus-cli`, on top of commit 1's `animus-env` primitives, with no
further design change — every decision above stands as written. As-built
specifics worth recording:

- **`TlsMaterial` grew a second acceptor.** `animus-env`'s `TlsMaterial`
  (commit 1) now carries `server_acceptor: tokio_rustls::TlsAcceptor`
  alongside the original `acceptor` (renamed in spirit, not in name — it
  stays mutual) and `connector`: `TlsConfig::load()` builds both
  `ServerConfig`s from the same cert/key (`with_client_cert_verifier` for
  `acceptor`, `with_no_client_auth()` for `server_acceptor`), so a node's
  own single `TlsConfig` never needs loading twice for the two modes.
  `animus_env::tls::server_name_for` — `pub(crate)` in commit 1 — is now
  `pub`, since `animusd`'s own relay dialers need the identical
  `ServerName` derivation the internal wire already used.
- **Config shape**: `RoleAddrs` (not `ClusterConfig`) gained `tls:
  Option<config::TlsSection>` — **per-node**, unlike `dynamo_auth`
  (cluster-wide), because TLS material is inherently per-node (each node
  presents its own cert; only `ca_path` is conventionally shared).
  `TlsSection` mirrors `animus_env::TlsConfig`'s three fields exactly and
  converts to it via `to_tls_config()`. `ClusterConfig::validate_tls`
  (called from `from_json`) enforces Decision 3's all-or-none rule across
  every node's own `tls` presence — the check is necessarily whole-*file*,
  not whole-*deployment*: a real multi-process deployment where each
  process supplies its own `--tls-*` CLI flags (rather than baking every
  node's section into one shared config file) is invisible to any single
  process's own load-time check, since each process only ever sees its own
  flag. That gap is documented, not closed, in `main.rs`'s own module doc
  — the config-file route (every node's `tls` section baked in up front,
  the shape a Kubernetes ConfigMap naturally wants for commit 3) sidesteps
  it entirely by construction.
- **CLI flags**: `--tls-cert PATH --tls-key PATH --tls-ca PATH`, all three
  or none, on `--config`/`--node` (combined) and `data --config`/`data
  --seed` — the same subset of entry points `--dynamo-auth` reaches on
  purpose (not `join`/`control`, mirroring that flag's own non-acceptance
  there; not `--cluster N`/`--cluster-control`/`--cluster-data`, which
  hard-error on the flag instead of silently ignoring it — a deliberate
  departure from the silent-gap precedent those dev-only paths otherwise
  use for knobs like `--advertise-host`, since silently starting a
  plaintext cluster an operator asked for TLS on is a materially worse
  failure mode than an unsupported-combination error). `apply_tls_flag`
  mirrors `apply_advertise_host_flag`'s per-node-entry shape (not
  `apply_dynamo_auth_flag`'s cluster-wide one) for the same per-node-cert
  reason as the config shape above.
- **Per-port TLS mode, as built** (Decision 1/2, unchanged from the
  original decision — recorded here as the concrete table):

  | Port | Mode | Acceptor / dialer |
  |------|------|--------------------|
  | `internal` (raw Raft wire) | mutual | `ProdEnv::bind_with_tls` (commit 1) |
  | `intra` (`ClientRequest` relay) | mutual | `TlsMaterial::acceptor` / `.connector` |
  | `client` (`ClientRequest`, external) | server-only | `TlsMaterial::server_acceptor` |
  | `dynamo` | server-only | `TlsMaterial::server_acceptor` |
  | `admin` | server-only | `TlsMaterial::server_acceptor` |
  | `console` | server-only | `TlsMaterial::server_acceptor` |

- **One generic stream, not a fork.** `http.rs`'s response/request
  helpers, `admin.rs`/`dynamo.rs`/`console.rs`'s `handle_conn`, and
  `lib.rs`'s `handle_connection` are all generic over `S: AsyncRead +
  AsyncWrite + Unpin` (or an `impl Trait` argument, for `write_frame`/
  `read_frame` specifically — see their own doc for why a named type
  parameter there would have broken every pre-existing `read_frame::
  <SomeType>(..)` turbofish call site across the test suite: Rust does not
  infer an unspecified *trailing* explicit type parameter, so `S` had to
  be an anonymous `impl Trait` argument, not a second named parameter,
  regardless of ordering). Each accept loop wraps a plain `TcpStream` in
  `animus_env::MaybeTlsStream::Plain` when TLS is off and runs it through
  the right acceptor when on; a failed handshake is logged at `warn` with
  the peer's address and the connection dropped, mirroring
  `animus_env::prod::spawn_accept`'s own contract — the listener keeps
  serving.
- **Dialers**: `ClientCtx` and `AnimusdRelayClient` (the latter no longer
  zero-sized) each carry `Option<TlsMaterial>`; `relay_request`/
  `relay_request_with_timeout` take it as a parameter and dial the `intra`
  port through `TlsMaterial::connector` (always mutual — every relay this
  crate makes targets `intra`, never `client`). `RemoteControlClient`
  (`animus-node`) grew a `relay()` accessor so `animusd`'s
  `remote_metadata_watch_loop` — which drives its own `WatchMetadata`/
  `Status` round trips outside `metadata_fresh` — reaches the identical
  relay path (and its TLS material) instead of re-dialing by hand.
  `animus-cli` never joins the cluster and so never needs a client
  certificate at all: `--tls-ca PATH` builds a server-only `rustls`
  `ClientConfig` (no `with_client_auth_cert`), reused for both the
  client-protocol dial and every `http_call` (admin) dial.
- **What stays plain**: `cluster_bench` (the wire benchmark) is untouched
  — a deliberate scope cut, not an oversight; benchmarking the TLS
  handshake/record-layer cost is a follow-up if ever needed.
  `animus-operator`'s admin client was commit 3's job (see that
  amendment note below).
- **Tests**: `crates/animus-env/src/prod.rs`'s
  `server_only_acceptor_accepts_a_client_with_no_certificate` (commit 1's
  file, since `TlsMaterial::server_acceptor` is that crate's own type);
  `crates/animusd/tests/support/mod.rs::tls_pki`/`bring_up_deadline_tls`
  (a small independent copy of `animus-env`'s own `#[cfg(test)]`-private
  PKI helper — see that function's own doc for why it isn't reused
  directly); `crates/animusd/tests/tls_e2e.rs` (a real 3-node TLS cluster:
  `CreateTable`/`PutItem`/`GetItem` across nodes over server-only TLS,
  admin/console GET over TLS, a plain-TCP dial refused while the port
  keeps serving, a different-CA client refused on the intra port, and the
  mixed-config validation error); config round-trip + `validate_tls` unit
  tests in `config.rs`; flag-parsing/conflict unit tests in `main.rs`; and
  parser + connector-construction unit tests in `animus-cli`.

## Amendment note (commit 3 landed, 2026-09-05)

Commit 3 (S-01 step 3) gives `animus-operator` (ADR 0060) something to
point at commits 1–2's TLS-capable `animusd`: `AnimusClusterSpec.tls`, a
`cert-manager.io/v1` `Certificate` builder, the `StatefulSet`/
`ClusterConfig` mirror wiring, and a TLS-capable admin client for the
scale-down drain sequence — no further design change to Decisions 1–7
above. As-built specifics worth recording:

- **CRD shape**: `AnimusClusterSpec.tls: Option<TlsSpec>`
  (`crates/animus-operator/src/crd.rs`), two mutually exclusive shapes —
  `secretName: String` (a pre-existing `kubernetes.io/tls` `Secret`) or
  `certManager: { issuerRef: { name, kind, group? }, duration?,
  renewBefore? }` — validated by `TlsSpec::validate` at reconcile time (no
  admission webhook in v1, same posture as `controlNodes`' immutability
  check): both or neither set is rejected with a `TlsSpecInvalid` status
  condition, and that reconcile proceeds with TLS stripped rather than
  getting stuck. Both shapes resolve to the same `Secret` name
  (`TlsSpec::secret_name_or_default` — the explicit `secretName`, or
  `{cluster}-tls` for `certManager`).
- **One shared cert, not per-pod.** Every pod mounts the identical
  resolved `Secret` at `/etc/animus/tls` (`desired::statefulset::build`,
  mirroring the pre-existing `dynamo_auth` mount pattern) and every node's
  generated `cluster.json` gets the identical `RoleAddrs.tls`
  (`desired::cluster_config::{TlsSection, tls_section}`), pointing at
  `/etc/animus/tls/{tls.crt,tls.key,ca.crt}` — baked into the config file,
  not per-pod `--tls-*` flags, which is what makes `ClusterConfig::
  validate_tls`'s whole-*file* check (commit 2's own as-built note) apply
  cleanly here: this operator always generates one `--config FILE --node
  I` config carrying every node's own `tls` section up front. This
  departs from commit 2's "TLS material is inherently per-node" framing in
  the letter but not the substance: nothing stops every pod legitimately
  presenting the *same* certificate (its SAN list already has to cover
  every ordinal for cross-node dialing to work at all, so a per-pod split
  would shrink no SAN list, only multiply objects to manage) — see
  `crd::TlsSpec`'s own doc.
- **(2026-09-05 fix) The kubelet probes follow `spec.tls` too.** Admin is
  server-only TLS (Decision 2), and the `StatefulSet`'s readiness/liveness
  probes hit that same port — a plaintext `GET /admin/health` against a
  TLS-only listener fails the handshake on the server side every probe
  period, so every pod stayed `NotReady` and was restart-looped by the
  kubelet (the first real CI run of the `e2e-kind-tls` job below caught
  this: `admin TLS handshake failed ... InvalidContentType`).
  `desired::statefulset::admin_probe` now sets `HTTPGetAction.scheme:
  HTTPS` whenever `spec.tls` is set, else leaves it unset (plain HTTP).
  The kubelet's HTTPS probe scheme does not verify the server certificate,
  so this needed no CA plumbed into the kubelet itself — no other change
  to Decisions 1–7.
- **The `Certificate`, only for `certManager`.** `desired::certificate::
  build` returns `None` for the `secretName` shape (nothing to create —
  the operator only *reads* that `Secret`) and for no TLS at all; for
  `certManager` it builds a `cert-manager.io/v1` `Certificate` (a `kube::
  core::DynamicObject`, since that API group isn't in `k8s-openapi`) named
  `{cluster}-tls`, `secretName: {cluster}-tls`, `usages: [server auth,
  client auth]`, `isCA: false`, and a `dnsNames` list
  (`desired::certificate::dns_names`) covering every pod's own stable
  per-ordinal FQDN plus both the headless internal `Service` and the
  client-facing `dynamo` `Service` (short and fully-qualified forms of
  each) — satisfying Decision 7's SAN requirement for every string a peer
  (or a client) might dial by. Applied as a sixth `apply_children` child,
  before the `StatefulSet`; the referenced `Issuer`/`ClusterIssuer` is
  never created by this operator, matching Decision 6's "no CA-issuance
  logic lives in this codebase."
- **The admin client's TLS connector is a small independent one, not a
  shared crate.** `animus-operator` depends on neither `animus-env` nor
  `animus-cli` (a standing constraint, see that crate's own `CLAUDE.md`),
  and `hyper-util`'s legacy `Client` needs a connector shaped as a
  `tower_service::Service<Uri>` — a different shape than either
  `MaybeTlsStream` (an `AsyncRead+AsyncWrite` enum) or `animus-cli`'s own
  connector build. `admin_client.rs` grew `AdminConnector`/`MaybeTlsIo`
  (the same plain-or-TLS-stream shape, implemented against `hyper-util`'s
  own `Connect`/`Connection` traits instead) and `AdminOps::post_json`/
  `get_json` grew a `ca_pem: Option<&[u8]>` parameter — `Some` dials TLS
  trusting those CA bytes (server-only, no client cert — this crate never
  joins the cluster), `None` plain TCP. `crate::controller::reconcile`
  reads the resolved `Secret`'s `ca.crt` via a new `ClusterApi::
  get_secret` (RBAC `secrets: get/list/watch`,
  `deploy/operator/rbac.yaml`) — the Kubernetes API, not a file mounted
  into the *operator's own* pod, which is what makes this work identically
  whether the operator runs in-cluster
  (`deploy/operator/deployment.yaml`) or out-of-cluster via `cargo run -p
  animus-operator -- run` against a local kubeconfig (what
  `scripts/e2e-kind.sh` does — see ADR 0060's own "no `Env` seam" framing
  for why this crate is production-only wiring either way).
- **e2e**: `scripts/e2e-kind.sh` gained an `E2E_TLS=1` path (cert-manager
  install, a self-signed `ClusterIssuer`, `spec.tls.certManager` on the
  manifest, waiting on the `Certificate`'s own `Ready` condition, then
  `curl --cacert --resolve` against the dynamo Service's own SAN instead
  of plain HTTP) and `.github/workflows/e2e-kind.yml` gained a second job,
  `e2e-kind-tls`, running it. **Unverified in this repository's sandboxed
  dev environment** — `kind` cannot come up here at all regardless of
  anything TLS-specific (see `crates/animus-operator/CLAUDE.md`'s e2e
  section, the `CAP_SYS_RESOURCE` note) — so this path is new, carefully
  written, `bash -n`-checked code that has not been run end to end
  anywhere yet; the first real CI run of `e2e-kind-tls` is this path's
  first real test, per the Testing expectation section above.
- **Tests**: `crd::tests` (both/neither-shape rejection, secret-name
  resolution); `desired::certificate::tests` (SAN list, GVK/name,
  issuerRef/usages/isCA, duration/renewBefore pass-through, owner
  reference); `desired::cluster_config::tests`/`desired::statefulset::
  tests` (the `tls` section/mount present and byte-identical across every
  node, absent when unset); `controller::tests` (a `Certificate` applied
  as a sixth child for `certManager` and none for `secretName`; both/
  neither rejected with `TlsSpecInvalid`; the scale-down drain sequence
  reading a seeded `Secret`'s `ca.crt` and dialing `https://`).

## Amendment note (commit 4 landed, closing, 2026-09-05)

Commit 4 (S-01 step 4) is documentation-only — no further code, no design
change to Decisions 1–7. It closes S-01 out: `docs/roadmap.md`'s S-01
section is deleted (its own maintenance rule: a landed item is removed from
the roadmap, not marked done in place — the decision record lives here and
in the crate guides instead), `website/`'s three "no TLS"/"every port
assumes a trusted network" statements (`index.html`, `architecture.html`,
`how-it-works.html`, `docs.html`, `install.html`) are replaced with the
accurate, as-built posture — TLS available and config-gated, off by
default, mutual on the internal/intra ports and server-only on client/
admin/console, SigV4 unaffected and still the only client-port caller-
identity story — and `index.html`'s single "TLS + auth-beyond-SigV4"
Planned pill is split: TLS moves to "Works today," a narrower
"authentication beyond SigV4" pill stays Planned (that's S-02's own scope,
untouched by this ADR — see its Amendment note above).

**Summary of what the four commits actually built, end to end:**

1. `animus-env`: `TlsConfig`/`TlsMaterial` (mutual `acceptor` + server-only
   `server_acceptor` + `connector`, one `TlsConfig::load()`),
   `MaybeTlsStream`, `tls::server_name_for` — mutual TLS on the raw
   internal Raft wire inside `ProdEnv`, behind the `prod` feature.
2. `animusd`/`animus-cli`: per-node `RoleAddrs.tls: Option<TlsSection>`,
   `ClusterConfig::validate_tls`'s all-or-none rule, `--tls-cert/-key/-ca`
   flags on the entry points that accept them, every listener
   (`internal`/`intra` mutual, `client`/`dynamo`/`admin`/`console`
   server-only) and every dialer (the intra relay client, `animus-cli`'s
   client-protocol and admin dials) wired through `MaybeTlsStream`/
   `TlsMaterial`.
3. `animus-operator`: `AnimusClusterSpec.tls` (a pre-existing `Secret` or a
   cert-manager `Certificate`, validated at reconcile time), the
   `Certificate`/`StatefulSet`/`ClusterConfig` builders, and a TLS-capable
   admin client for the scale-down drain sequence — the operator target
   ADR 0060 deferred TLS to, closed.
4. This documentation pass.

**What is still, deliberately, not done** (Decision 6, restated for anyone
landing on this ADR looking for a gap list): cert rotation without a
process restart; SNI-based multi-tenancy; client certificates on the
client port (mTLS-only client auth as a SigV4 alternative); a managed or
auto-rotated CA (an operator — human or cert-manager — hands every node
its already-issued cert/key/CA files; no CA-issuance logic lives in this
codebase). Any of these is a new ADR's job, not a reopening of this one.
`scripts/e2e-kind.sh`'s `E2E_TLS=1` path (commit 3) also remains
unverified in any sandbox that cannot run `kind` at all — its first real
verification is whenever CI's `e2e-kind-tls` job first runs green (or
doesn't).

## Amendment (2026-09-16, issue #913) — stable SANs and a CA hierarchy requirement

`e2e-kind-tls`'s first real run to reach a `spec.nodes`/`spec.controlNodes`
scale-up (found investigating issue #864's own growth stall, and filed
separately as #913 since the mechanism is TLS-specific) hit a persistent
mTLS failure: after `spec.nodes` 3→4 and `spec.controlNodes` 3→4, the
recreated pod `e2e-3` logged, continuously, for the whole ~5-minute window
before the rollout wait timed out:

```
WARN animus_env::prod: TLS handshake failed (dropping connection) err=Custom { kind: InvalidData, error: AlertReceived(BadCertificate) } peer_addr=10.244.0.12:44804
WARN animus_env::prod: TLS handshake failed (dropping connection) err=Custom { kind: InvalidData, error: AlertReceived(BadCertificate) } peer_addr=10.244.0.11:48712
WARN animus_env::prod: TLS handshake failed (dropping connection) err=Custom { kind: InvalidData, error: AlertReceived(BadCertificate) } peer_addr=10.244.0.13:55980
```

against the pod IPs of `e2e-0`/`e2e-1`/`e2e-2` — the three pre-existing,
never-restarted control voters. `e2e-3` never sustained
`control_leader_recent`, so `/admin/health` never returned 200, so the
`StatefulSet`'s own `OrderedReady`-equivalent readiness gate on a recreated
pod never let the rollout proceed. Timing (run 35036379078, job
104606305854): `spec.nodes` patched at 23:44:17, `spec.nodes` StatefulSet
converged (4/4 ready — the pre-growth SAN list at that point still covered
3 pods, but pod 3 hadn't been TLS-recreated by the growth step yet) at
23:44:22, `spec.controlNodes` patched at 23:44:22, control group converged
to 4 voters at 23:44:39, and the wait for the `controlNodes` config-hash
roll to finish timed out at 23:49:39 with `e2e-3` (`Start Time: 23:44:25`,
`Restart Count: 0`, continuously `Running`, never `Ready`) still failing
its readiness probe.

### Mechanism

- **Which Secret keys `animusd` reads, and what `ca_path` is** (`crates/
  animus-operator/src/desired/cluster_config.rs:305,327-333`): every pod
  mounts one `Secret` at `/etc/animus/tls`, and every node's `cluster.json`
  points `cert_path`/`key_path`/`ca_path` at `tls.crt`/`tls.key`/`ca.crt`
  inside it — `ca_path` is **whatever the mounted Secret's `ca.crt` key
  holds**, not necessarily a distinct root: for a bare `selfSigned`
  `ClusterIssuer` (what the e2e used), cert-manager's output Secret has
  `ca.crt` equal to the leaf certificate itself (a self-signed cert is its
  own issuer), so the "CA" every pod trusts against **is** the one shared
  leaf this cluster presents (ADR 0064 commit 3's own "one shared cert, not
  per-pod" design, `deploy/operator/README.md`'s TLS section).
- **What changes in the Certificate spec on a scale-up, and why that
  matters**: `desired::certificate::build` (before this fix,
  `crates/animus-operator/src/desired/certificate.rs:121` as it stood
  investigating this issue) recomputed `dns_names(name, ns, spec.nodes)` —
  one per-ordinal FQDN per node — on every reconcile. `spec.controlNodes`
  changes alone never touched this list (only `spec.nodes` does); this
  e2e's phase order happens to change both nearly at once, but only the
  `spec.nodes` edit is what mutates the `Certificate`. cert-manager treats
  any change to a `Certificate`'s `spec` (its `dnsNames` included) as a
  reissuance request, updating the same `{cluster}-tls` `Secret` **in
  place** — `tls.crt`/`tls.key`/`ca.crt` all get overwritten.
- **`animusd` never hot-reloads TLS material** (ADR 0064 Decision 6,
  unchanged by this amendment): `animus_env::tls::TlsConfig::load` (
  `crates/animus-env/src/tls.rs:187`) reads the PEM files once, at listener
  bind time; there is no file watcher or reload path anywhere in
  `crates/animusd` or `crates/animus-env`. A pod's in-memory `TlsMaterial`
  is exactly whatever was on disk the instant it booted, for its entire
  lifetime.
- **Net effect, for a bare self-signed `ClusterIssuer` used directly as the
  leaf issuer**: since `ca.crt` *is* the leaf, a reissuance replaces the
  trust anchor and the presented identity together, atomically, for every
  *future* pod — but every already-running pod's in-memory copy of the old
  leaf-as-CA is untouched. `e2e-0`/`e2e-1`/`e2e-2` booted at 23:44:0x,
  before the `spec.nodes` 3→4 patch (23:44:17) reached cert-manager and it
  finished reissuing; they hold the **pre-scale-up** cert/CA. `e2e-3`
  (`Start Time: 23:44:25`, created by the *separate* `spec.controlNodes`
  growth step's config-hash roll, not by the `spec.nodes` StatefulSet
  scale-up itself — S-07d's `restart_relevant_projection` rolls every pod
  on a `controlNodes` change) mounted the Secret **after** cert-manager
  reissued it for the wider SAN list, so it holds the **post-scale-up**
  cert/CA. Two mutually-incompatible root certificates now coexist across
  the running pods, permanently (nothing ever re-reads the file): `e2e-3`
  presents a leaf `e2e-0`/`e2e-1`/`e2e-2` cannot validate against their old
  `ca.crt`, and it cannot validate *their* leaf against its own new
  `ca.crt` either — `AlertReceived(BadCertificate)` on every handshake
  attempt, in both directions, exactly as the log shows.

This is not a rotation-policy detail to pin down further: **whether
cert-manager happens to reuse the private key across that reissuance is
irrelevant to the fix** — a bare self-signed issuer's output Secret making
`ca.crt` and `tls.crt` the same value means *any* leaf change is also a
trust-anchor change, for a deployment that was never supposed to need one
during ordinary scaling.

### Fix

1. **A SAN set that is stable across `spec.nodes`/`spec.controlNodes`**
   (`crates/animus-operator/src/desired/certificate.rs`): `dns_names` no
   longer takes a node count. It now covers the headless internal
   `Service`'s own name (short + FQDN, unchanged) plus two **wildcard**
   SANs — `*.<internal-svc>.<ns>.svc` and
   `*.<internal-svc>.<ns>.svc.cluster.local` — replacing the per-ordinal
   FQDN list entirely. A Kubernetes headless `Service`'s pod DNS name
   (`RoleAddrs::advertise_host`, `desired::pod_fqdn`) is always exactly one
   label before the service name — `{cluster}-{ordinal}.{internal-svc}...`
   — so a single-label wildcard covers every ordinal a cluster will ever
   have, present or future, with **no reissue ever triggered by a
   node-count change again**. The client-facing `dynamo` `Service`'s SANs
   are unchanged (they never depended on node count to begin with). Both
   cert-manager's ACME issuers and its `ca`/`selfSigned` issuer types
   accept wildcard `dnsNames` — no ACME DNS-01 requirement applies to the
   non-ACME issuer types this operator supports (`Issuer`/`ClusterIssuer`
   referencing a `ca`- or `selfSigned`-backed signer; this codebase has no
   ACME support to begin with). A new test
   (`certificate_spec_is_byte_identical_across_a_nodes_scale_up`) pins the
   regression directly: the built `Certificate.spec` is byte-identical for
   a `nodes=3/controlNodes=3` cluster and a `nodes=4/controlNodes=4` one.
2. **A stable trust anchor for the e2e's own TLS leg**
   (`scripts/e2e-kind.sh`): the bare `selfSigned: {}` `ClusterIssuer` is no
   longer handed to the `AnimusCluster`'s own
   `spec.tls.certManager.issuerRef`. It now mints exactly one CA
   `Certificate` (`isCA: true`), once, at cluster bring-up; a second
   `ClusterIssuer` (`spec.ca.secretName`) signs the cluster's actual leaf
   off that CA's key. This is a structural improvement beyond just
   surviving *this* issue: for a `ca`-backed (non-self-signed-leaf) issuer,
   cert-manager's output Secret sets `ca.crt` to the **issuing CA's own
   certificate**, distinct from `tls.crt`/`tls.key` — so `ca.crt` now stays
   byte-identical across *any* future leaf reissuance (a
   `duration`/`renewBefore` rollover, not just a scale event), and mutual
   TLS peer validation keeps working across such a reissuance even without
   hot-reloading `tls.crt`/`tls.key` on already-running pods. This is also
   the shape a real production deployment should use with a self-signed
   root: `deploy/operator/README.md`'s own TLS section and
   `crates/animus-operator/CLAUDE.md` now say so explicitly, since a bare
   `selfSigned` issuer handed directly to a multi-pod mTLS deployment's
   `issuerRef` was never a safe pattern, only one that happened not to be
   exercised (`spec.nodes` never changed) until this issue's own scale-up
   step first ran in CI.
3. **Hot-reloading TLS material in `animusd` was not needed for this
   fix** and is not built here: fix 1 removes the only reissue trigger a
   routine scale-up can hit (a `Certificate.spec` change) and fix 2 makes
   the one remaining trigger (a time-based renewal) safe without a reload,
   since the CA half of the trust store — the actual thing mutual TLS
   validates a peer's leaf against — no longer changes on a leaf renewal.
   Cert rotation without a process restart remains exactly the Decision-6
   gap this ADR already named, unchanged; nothing here narrows or widens
   it.

### Consequences

- A `spec.nodes` scale-up (with TLS on) no longer reissues the cluster's
  shared leaf certificate at all — the `Certificate` object's `spec` is
  identical before and after.
- Any deployment (this e2e included) using a self-signed root for
  `spec.tls.certManager` must put a `ca`-type `ClusterIssuer` between that
  root and the `AnimusCluster`'s own `issuerRef` — a bare `selfSigned`
  issuer used directly is now a documented anti-pattern for this
  operator's mTLS shape, not a silent trap.
- Decision 7's SAN requirement ("every string a peer's peer book might
  dial it by") is now satisfied by a wildcard rather than an enumerated
  list — still exact-matching every pod's real dial string, just without
  needing to be regenerated as the cluster grows.

### Tests

`crates/animus-operator/src/desired/certificate.rs`:
`dns_names_cover_the_internal_service_wildcard_plus_both_services_short_and_fqdn`,
`dns_names_do_not_depend_on_node_count`,
`certificate_spec_is_byte_identical_across_a_nodes_scale_up`, and the
pre-existing `cert_manager_shape_builds_a_certificate_with_the_right_gvk_and_name`/
`issuer_ref_and_usages_and_is_ca`/`duration_and_renew_before_*`/
`owner_reference_present` suite, updated where the SAN list itself was
asserted. `scripts/e2e-kind.sh`'s new/changed heredocs were rendered with
their variables set and parsed as YAML (not just `bash -n`'d) — a comment
carrying a backtick inside an unquoted heredoc executes as shell rather
than staying inert YAML, the same class of bug the concurrent issue #864
investigation (PR #909) hit and is recording as its own lessons entry; the
comments this PR adds around the new CA-hierarchy heredocs are placed
*above* them, never inside. The `E2E_TLS=1` leg's actual scale-up-then-growth sequence remains
unverified in any sandbox that cannot run `kind` at all (the same
standing limitation ADR 0064 commit 3 and ADR 0060 already note) — its
first real verification is the next green `e2e-kind-tls` CI run.

## Amendment (2026-09-16, issue #913 round 2) — the fresh-head e2e run still failed; three hypotheses ruled out, one open

The above fix's first real validation — stacking it under PR #909's own
`e2e-kind-tls` leg (which adds `kubectl rollout status statefulset/e2e
--timeout=300s` right after growth converges, so the run can no longer
declare success before pod 3 is actually `Ready`) and running it on a
**fresh** cluster (so every pod, including `e2e-0..2`, was created from
the wildcard-SAN Certificate and the CA-backed issuer from the start, not
migrated from an earlier state) — still failed the same way (run
35040977782, job 104620543323): `spec.nodes` 3→4 converged in 10s,
`spec.controlNodes` 3→4 converged (4 voters) in 10s, then the rollout wait
timed out after 300s with pod `e2e-3` continuously `Running`/never
`Ready`, its log full of the identical
`AlertReceived(BadCertificate)` lines against `e2e-0`/`e2e-1`/`e2e-2`'s
pod IPs. Confirmed from that run's own diagnostics: `spec.tls.certManager.
issuerRef.name` is `e2e-ca-issuer` (the CA-backed issuer, not the bootstrap
`selfSigned` one) — the round-1 fix's own wiring is in place and being
used.

**Direction of the alert, established from the code, not assumed**:
`AlertReceived(BadCertificate)` is logged only in `spawn_accept`
(`crates/animus-env/src/prod.rs:749-753`), the **inbound**-accept path —
`peer_addr` is `listener.accept()`'s own remote-socket address, so pod 3
is the **server** here, and the three logged addresses are `e2e-0`/`e2e-1`/
`e2e-2` **dialing in** to it. `rustls::Error::AlertReceived` means this
side *received* the alert, i.e. **the peer sent it** — and in a TLS 1.3
mutual handshake the server sends its own certificate (and requests the
client's) *before* the client sends its client certificate, so a client
that rejects the server's presented certificate aborts and sends
`bad_certificate` back without ever completing its own certificate flight.
Read together: **`e2e-0`/`e2e-1`/`e2e-2`, dialing pod 3 as TLS clients,
rejected pod 3's presented server certificate.**

**Three of the four standing hypotheses are now ruled out with direct
evidence, not just review:**

- **(a) Wildcard-SAN/hostname mismatch — ruled out by a new, decisive
  test.** `crates/animus-env/src/prod.rs`'s
  `tls_wildcard_san_matches_a_per_ordinal_pod_hostname` runs a real
  loopback handshake through the exact same `TlsMaterial::acceptor`/
  `connector` and `server_name_for` derivation `spawn_accept`/
  `connect_maybe_tls` use in production, with the server's leaf SAN set to
  `*.e2e-internal.animus-e2e.svc.cluster.local` (this fix's own shape) and
  the client dialing `e2e-3.e2e-internal.animus-e2e.svc.cluster.local` (a
  real per-ordinal pod hostname, `desired::pod_fqdn`'s own shape) — it
  passes. The wildcard construction is not the bug.
- **(b) Client-certificate hostname check — ruled out by TLS semantics.**
  Nothing in `crates/animus-env/src/tls.rs`'s acceptor construction (a
  standard `rustls` client-cert verifier built from the CA root store)
  performs any hostname/SAN check against an inbound client certificate —
  only chain-of-trust validation. There is no mechanism here that could
  reject a client based on what hostname it dialed by.
- **(c) The operator's own re-apply reissuing the Certificate — ruled out
  by code review.** `apply_certificate` (`crates/animus-operator/src/
  controller.rs:103-105`, `cluster_api.rs`) is a server-side-apply
  `PatchParams::apply(FIELD_MANAGER).force()`, run unconditionally on
  every reconcile with **exactly the same** `Certificate.spec` regardless
  of `spec.nodes`/`spec.controlNodes` (pinned by
  `certificate_spec_is_byte_identical_across_a_nodes_scale_up`, added in
  this issue's first fix) — an idempotent re-apply of byte-identical
  content gives cert-manager nothing to react to. Neither `cert_spec`
  (this crate) nor the e2e's own CA `Certificate` sets
  `privateKey.rotationPolicy`, so both default to cert-manager's own
  `Never` — no forced key rotation on any resync either.

**What remains open**: hypothesis (d), a genuine timing/propagation issue
around the recreated pod's Secret mount, is neither confirmed nor ruled
out — the evidence to settle it (the `Certificate`/`Secret`'s own
`resourceVersion` history, cert-manager's `CertificateRequest` objects and
events across the whole scale-up-then-growth window) was not captured by
this run's diagnostics. `dump_diagnostics` (`scripts/e2e-kind.sh`) now
captures it whenever `E2E_TLS=1`: `kubectl get certificate,secret`,
per-`Certificate` `resourceVersion`/`generation`/`status`, the `Secret`'s
own `resourceVersion`/`creationTimestamp`, `openssl x509`-derived
fingerprint/serial/validity for `tls.crt` and `ca.crt` specifically (never
the private key or raw cert bytes), and every `Certificate`/
`CertificateRequest` event plus the `CertificateRequest` objects
themselves. The next `e2e-kind-tls` run — now also gated by the same
`kubectl rollout status` wait on this branch, closing the gap that let an
earlier run of this same fix "pass" without ever waiting for pod 3 to
become `Ready` — is what actually settles (d): if `e2e-3`'s `tls.crt`/
`ca.crt` fingerprints differ from `e2e-0`'s, a reissue is confirmed and
its `CertificateRequest` timeline pinpoints when and why; if they're
identical, the divergence is somewhere this investigation has not yet
looked, and the round-1 fix's own architecture (wildcard SAN, CA
hierarchy) needs to be revisited rather than assumed sufficient.

## Amendment (2026-09-16, issue #913 round 3) — two more code-level candidates checked and ruled out; no code change

Given (1) `e2e-0`/`e2e-1`/`e2e-2` reject pod 3's *server* certificate (the
round-2 amendment's own directional finding), (2) the `Certificate` spec
is provably byte-identical across the scale-up, and (3) `e2e-0`/`e2e-1`/
`e2e-2` handshake with each other fine, two further code-level candidates
specific to pod 3's *second incarnation* were checked — both ruled out,
with no live cluster needed:

**(A) The hostname peers actually dial for node 3, versus the wildcard's
own shape.** Traced end to end through the real address-construction
code, not the ADR's own descriptive prose:

- `desired::cluster_config::build_cluster_config`
  (`crates/animus-operator/src/desired/cluster_config.rs:193-232`) writes,
  for **every** ordinal (`0..spec.nodes`, role-independent): `internal`/
  `client`/`intra`/`dynamo`/`admin`/`console` as the identical bind
  placeholder `0.0.0.0:{port}` (every pod is its own network namespace in
  Kubernetes, so there is nothing node-specific to put here — see this
  module's own doc, lines 13-20), and `advertise_host: Some(super::
  pod_fqdn(name, ns, i))` — the **one and only** per-node differentiator,
  always the full FQDN form `{name}-{i}.{internal-svc}.{ns}.svc.cluster.
  local` (`desired::pod_fqdn`, `crates/animus-operator/src/desired/mod.rs:
  129-134`). No bare `<pod>.<svc>` short form, no IP, is ever written —
  `pod_fqdn` has exactly one hardcoded shape.
- On the `animusd` side, every dial address — for both ports this ADR
  makes mutual TLS (`internal` **and** `intra`) — ultimately traces back
  to the same helper, `advertised_addr(advertise_host, bind_addr) ->
  format!("{host}:{}", bind_addr.port())` (`crates/animusd/src/lib.rs:
  2342-2347`). Two route sources exist and both trace back to it:
  `ClusterConfig::peer_book` (`internal`, `crates/animusd/src/config.rs:
  511-521`) and the static `client_route`/`intra_route` builders
  (`crates/animusd/src/lib.rs:14707,14919,15133,15151,15268,15478,15819`)
  read every node's `advertise_host` straight out of the shared
  `cluster.json` directly through `advertised_addr`; `intra_route_
  sync_loop` (`crates/animusd/src/lib.rs:11400-11404`) instead **layers a
  live, gossip-replicated `NodeAddrs.intra` value over that static route**
  for any node that has self-registered — but that live value was itself
  set via the identical `advertised_addr(self.advertise_host.as_deref(),
  ...)` call at that node's own registration (e.g. `BoundControlNode`'s
  own `NodeAddrs` construction, `crates/animusd/src/lib.rs:6644-6647`),
  so it carries the same hostname either way; the dynamic path changes
  *when* a route updates (on registration/rejoin), never *what hostname
  shape* it uses. **Reading every other node's own `advertise_host`
  directly**, not a separately-discovered address, is what both paths
  share. Both ports therefore dial the
  identical hostname `advertised_addr` produces, differing only in port
  number, which `server_name_for` strips before deriving the `ServerName`
  (`crates/animus-env/src/tls.rs:329`) — so testing one port's hostname
  form (as `tls_wildcard_san_matches_a_per_ordinal_pod_hostname` already
  does) is dispositive for both; there is no second, port-dependent
  hostname shape to separately test.
- **Whether the old per-ordinal list could have covered a form the new
  wildcard misses**: no. The old `dns_names(name, ns, nodes)` enumerated
  `(0..nodes).map(pod_fqdn)` — literally the same `pod_fqdn` function,
  same shape, and (this is the point the round-1 fix exists to close)
  **it would not have listed ordinal 3 at all before the scale-up**
  (`nodes` was still 3). The wildcard is a strict superset of anything
  the old per-ordinal list could ever have provided for an FQDN dial —
  round 1 did not trade a covered form for an uncovered one.

**Conclusion: (A) is ruled out.** Every internal/intra dial for pod 3,
from any peer, targets exactly the hostname
`tls_wildcard_san_matches_a_per_ordinal_pod_hostname` already proves the
wildcard SAN matches.

**(B) Which certificate pod 3 presents on its intra port as a
combined-role pod versus as the data-only pod it was before growth.**
Traced through the same config-generation and load path:

- `build_cluster_config` computes `tls: spec.tls.as_ref().map(|_|
  tls_section())` **once**, outside the per-ordinal `.map(|i| RoleAddrs
  {...})` closure (`crates/animus-operator/src/desired/cluster_config.rs:
  203,229`), and assigns the identical cloned value to every node
  regardless of `role: NodeRole::{Both,Data}` (line 229). `tls_section()`
  itself (`crates/animus-operator/src/desired/cluster_config.rs:327-333`)
  returns fixed, mount-path-only fields — `/etc/animus/tls/{tls.crt,tls.
  key,ca.crt}` — with no role or port parameter at all.
- On the `animusd` side there is exactly one `TlsConfig`/`TlsMaterial`
  per node (loaded once at startup, `TlsConfig::load`,
  `crates/animus-env/src/tls.rs:187`), and `TlsMaterial` carries three
  pre-built handshake objects derived from that **same** cert/key/ca —
  `acceptor` (mutual, for `internal`/`intra`), `server_acceptor`
  (server-only, for `client`/`dynamo`/`admin`/`console`), and `connector`
  (outbound) — ADR 0064 commit 2's own as-built note above
  ("`TlsMaterial` grew a second acceptor... built from the same cert/key
  as `acceptor`"). Nothing in this construction branches on node role at
  all; a data-only node binds and TLS-wraps its `internal` port
  identically to a combined node (every role needs the internal env —
  control Raft, per-tablet Raft, and heartbeats all ride it,
  `crates/animusd/src/config.rs:502-509`'s own doc).
- Since ordinal 3's `advertise_host`/`internal`/`intra`/`tls` fields are
  byte-identical between its `Data`-role generation of `cluster.json` and
  its `Both`-role regeneration after `controlNodes` growth (only `role`
  itself, and the `entrypoint.sh` dispatch it drives, differ — the config
  generator's own `NodeRole` branch, lines 217-221, touches nothing else),
  there is no per-role cert/key/ca *selection* anywhere for this fix to
  have broken, and no "dynamo Service's certificate" distinct from the
  internal wire's own — every port shares the one `Secret`
  (`deploy/operator/README.md`'s TLS section, "the *same* cert/key on
  every pod, not a distinct one per ordinal").

**Conclusion: (B) is ruled out.** There is no role- or
incarnation-dependent certificate selection in this codebase for either
the config that's generated or the material `animusd` loads from it.

**Diagnostics hardened further, no live cluster needed for this part
either**: `dump_diagnostics` now also dumps each pod's **first** 40 log
lines (`kubectl logs --tail=-1 | head -n 40`), alongside the pre-existing
tail — so the next `e2e-kind-tls` run shows whether the `BadCertificate`
storm on the recreated pod starts at its very first handshake attempt
(consistent with a boot-time race, hypothesis (d)) or only after some
clean activity (which would point somewhere this investigation has not
yet considered).

**Status**: (A) and (B) — the two candidates specific to pod 3's second
incarnation that code review alone could settle — are ruled out. (a),
(b), (c) from round 2 remain ruled out. (d), a live timing/propagation
question, is the only standing hypothesis, and the next `e2e-kind-tls`
run (with round 2's certificate/secret/event diagnostics and this round's
first-lines log capture) is what will settle it. No code change was made
in this round — round 1's wildcard-SAN and CA-hierarchy fix remains in
place unmodified, since neither (A) nor (B) found a gap in it.
