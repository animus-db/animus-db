# ADR 0064 — TLS on every port

- **Status:** Proposed — commits 1–3 of 4 (`S-01`) implemented: mutual
  TLS on the intra-node wire inside `ProdEnv` (commit 1); TLS on every
  `animusd` listener and dialer — client, intra-`ClientRequest`, admin,
  console — plus `animus-cli`'s client-protocol and admin dials (commit
  2); and the Kubernetes operator's cert-manager `Certificate`/`Secret`
  wiring plus its admin client's TLS connector (commit 3) — all
  config-gated, default off. Commit 4 (website/closing notes) is not yet
  built.
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
