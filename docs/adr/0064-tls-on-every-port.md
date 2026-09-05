# ADR 0064 — TLS on every port

- **Status:** Proposed — commit 1 of 4 (`S-01`) implemented: mutual TLS on
  the intra-node wire inside `ProdEnv`, config-gated, default off. Commits
  2–4 (client/intra-`ClientRequest`/admin/console listeners in `animusd`,
  operator cert-manager wiring, website/closing notes) are not yet built.
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
