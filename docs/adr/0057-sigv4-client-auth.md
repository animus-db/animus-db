# ADR 0057 — SigV4 authentication on the DynamoDB client edge

- **Status:** Accepted
- **Date:** 2026-08-23
- **Amends:** [ADR 0047](0047-intra-node-port.md) (closes its "later
  milestone" auth pointer for the client port), [ADR 0052](0052-data-console-port.md)
  (narrows its "no auth anywhere" statement)
- **Depends on:** [ADR 0047](0047-intra-node-port.md) (port segmentation),
  [ADR 0051](0051-dynamodb-ttl.md) (`Clock::wall_now()`/`UnixMillis` seam),
  [ADR 0053](0053-dynamodb-only-drop-cql.md) (v1 is DynamoDB-only)

## Context

AnimusDB's DynamoDB edge has **no authentication of any kind** — no SigV4
verification, no credential store. That is an explicitly deferred milestone,
not an oversight: ADR 0047 and ADR 0052 both flag it ("no authentication on
either port yet — a later milestone"), and no ADR stakes out an actual auth
design. This ADR picks that milestone up.

Two facts scope it:

1. **The deployment shape already does the heavy lifting.** Under the
   intended Kubernetes-operator topology (root `CLAUDE.md`), only the
   client-facing DynamoDB edge is exposed outside the cluster; intra/seed
   traffic never reaches an externally-reachable Service, and the admin and
   console ports carry an operator-network trust posture (ADR 0020,
   ADR 0052). So on the one segment that faces the outside world, SigV4's
   primary value is **AWS SDK compatibility** — SDKs sign every request by
   default, and some will not send unsigned requests without extra
   configuration — more than it is access control.

2. **The wire is DynamoDB-only (ADR 0053)**, and DynamoDB's protocol
   defines exactly what an auth failure looks like on the wire: HTTP 400
   with a `com.amazon.coral.service#…` `__type`. Fidelity to that shape is
   what makes real SDKs work unmodified.

Accordingly, this milestone is **signature verification against static
credentials** — proof that the caller holds a configured secret — and
deliberately **not** an IAM-equivalent authorization engine. There are no
policies, no per-operation authorization, no identities beyond the access
key id.

## Decision

We will verify AWS Signature Version 4 on the **client DynamoDB port only**,
against a static credential map from node configuration, default-off.

### Credential store: static map in the node config

- A new optional `dynamo_auth` section in the cluster config
  (`ClusterConfig`), holding a `credentials` map of
  `access_key_id → secret_access_key` (a `BTreeMap`, per ADR 0003
  determinism rules). Absent section ⇒ auth disabled ⇒ today's behavior;
  every existing config, test, and deployment is untouched.
- Startup modes that take no config file (`animusd data --seed …`, the
  in-process `--cluster N` dev shapes) accept `--dynamo-auth <path>`
  naming a JSON file of the same section shape. Supplying both the config
  section and the flag is a hard startup error — no silent precedence.
- **Explicitly out of scope:** rotation, replication of credentials through
  `Metadata`, any dynamic credential API, and secret-at-rest protection.
  The secret sits in plaintext in a config file that is already trusted
  operator input; a secrets-manager story is a later milestone.

### Which ports

- **The client DynamoDB port only.** When `dynamo_auth` is configured,
  every request on that listener — item API and Streams API alike — must
  carry a valid SigV4 signature before it reaches dispatch.
- **The intra port stays unauthenticated by design** — ADR 0047's explicit
  stance stands: intra is the more-trusted, cluster-internal segment, kept
  off any externally-reachable Service by the operator topology.
- **Admin and console ports keep their ADR 0020 / ADR 0052 posture**
  (trusted operator network, auth a flagged prerequisite before wider
  exposure). In particular the dashboard's `POST /admin/data/dynamo` proxy,
  which funnels into the same `execute_routed` dispatch fork, deliberately
  does **not** require SigV4: the gate is a property of the *client
  listener*, so it lives at the DynamoDB HTTP edge (`animusd::dynamo`),
  ahead of dispatch — not inside `execute_routed`, which would silently
  re-gate the admin surface and conflate two trust postures. (This is a
  considered exception to the "gate at the shared fork" lesson in
  `docs/engineering-lessons.md`: the fork stays the single dispatch point;
  auth is per-listener, not per-operation.)
- `GET /metrics` on the client port stays unauthenticated: Prometheus-style
  scrapers cannot SigV4-sign, the surface is read-only diagnostics, and
  operators who expose the client port externally can (and should) firewall
  the metrics path. An accepted, revisitable cost.

### Where the verification logic lives

A new **pure `sigv4` module in `animus-dynamo`**: canonical-request
construction, the HMAC-SHA256 signing-key chain, and constant-shape
comparison — bytes and strings in, verdict out. This is a **deliberate,
narrow widening of the crate's charter** (today: decode/encode only). It
stays inside the crate's purity rules — no `Env`, no I/O, no clock: the
"current wall time" used for skew checking is **passed in as a parameter**
(mirroring how `ttl.rs` takes `now: u64`), and the caller in `animusd`
reads it via `env.wall_now()` (ADR 0051 discipline — never
`SystemTime::now()`; `wall_now` is exactly the seam for interpreting an
externally-supplied absolute timestamp, which `X-Amz-Date` is).
`animusd` keeps only thin middleware wiring: capture headers, read the
clock, call the verifier, map the verdict to a wire error.

This adds the workspace's **first crypto dependencies**: RustCrypto's
`sha2` and `hmac` (MIT OR Apache-2.0 — clears `deny.toml`'s allow-list;
no `unsafe` in our own code, per the workspace `forbid(unsafe_code)`
lint). Hex encoding is hand-rolled rather than a third dependency.

The HTTP layer (`animusd::http::read_http_request`) starts retaining a
full lowercased header map instead of discarding everything but
`x-amz-target`/`content-length`/`connection` — required because
`SignedHeaders` may name any header and verification must reconstruct the
canonical form of each one.

### Verification semantics

- Parse `Authorization: AWS4-HMAC-SHA256 Credential=…/SignedHeaders=…/
  Signature=…` plus `X-Amz-Date` (the form every AWS SDK sends; the
  `Date`-header fallback is not supported). `SignedHeaders` must include
  `host` and `x-amz-date`.
- Recompute the canonical request per the SigV4 spec — including general
  path/query canonicalization even though DynamoDB traffic is `POST /`
  with an empty query — and the payload hash from the actual body bytes.
- The **credential scope is taken from the client verbatim**: AnimusDB has
  no region concept, so the claimed region and service strings are used
  as-is in key derivation and never pinned (a wrong secret still cannot
  forge a signature). The scope's terminal component must be
  `aws4_request`, and the scope date must equal `X-Amz-Date`'s date
  portion, as AWS enforces.
- **Clock skew:** `X-Amz-Date` must be within **±5 minutes** of
  `env.wall_now()` (AWS's window). The wall-clock read happens in
  `animusd` and is passed into the pure verifier.
- Not supported (out of scope, matching "static credentials only"):
  presigned/query-string auth, chunked/streaming payload signing,
  `UNSIGNED-PAYLOAD`, and session tokens — `X-Amz-Security-Token`, if a
  client signs one, participates as an ordinary signed header but is not
  validated, and an SDK configured with static credentials never sends
  one.

### Error mapping (AWS-faithful)

All auth failures are HTTP 400 with a
`com.amazon.coral.service#…` `__type` — the namespace real AWS uses for
the auth layer, distinct from the
`com.amazonaws.dynamodb.v20120810#…` namespace of service errors — so
these responses are rendered at the `animusd` edge rather than through
`WireError::to_json`'s DynamoDB-namespace prefix:

| Failure | `__type` (short) | Message |
|---|---|---|
| Missing/malformed `Authorization` header | `MissingAuthenticationTokenException` | `Request is missing Authentication Token` |
| Unknown access key id | `UnrecognizedClientException` | `The security token included in the request is invalid.` |
| Signature mismatch | `InvalidSignatureException` | `The request signature we calculated does not match the signature you provided. Check your AWS Secret Access Key and signing method. Consult the service documentation for details.` |
| `X-Amz-Date` outside ±5 min | `InvalidSignatureException` | `Signature expired: <X-Amz-Date> is now earlier/later than <window bound>` (AWS's shape) |

### Testing

- The pure verifier is validated against **AWS's published SigV4
  test-vector suite** (the `aws-sig-v4-test-suite` corpus), vendored as
  deterministic unit-test data in `animus-dynamo`. A pure function needs
  no sim story (ADR 0003 applies to nondeterminism, and there is none
  here).
- `ProdEnv` end-to-end tests in `crates/animusd/tests/`: a
  correctly-signed request round-trips; unsigned, mis-signed,
  unknown-key, and clock-skewed requests are rejected with exactly the
  codes above; auth-disabled clusters behave exactly as before. The tests
  sign with a **hand-rolled test signer** (same algorithm, exercised
  against the same vendored vectors).
- **Deliberate deviation:** no e2e test drives a real `aws-sdk-dynamodb`
  client. The SDK's crypto backends (`ring`/`aws-lc-rs`) carry
  OpenSSL-derived license terms that are not in `deny.toml`'s allow-list,
  so adding it even as a dev-dependency fails the cargo-deny gate. The
  vendored official vectors are the compatibility oracle instead; a real
  SDK smoke test could later live in a non-gating optional CI job if the
  maintainer wants one.

## Non-goals (this ADR)

- IAM policies, per-operation authorization, condition keys — any
  authorization semantics beyond "the signature verifies against a
  configured secret".
- Multi-tenancy or per-key scoping of tables.
- Credential rotation, dynamic credential APIs, replicating credentials
  through `Metadata`.
- TLS (a separate concern; SigV4 does not protect confidentiality).
- Auth on the intra, admin, or console ports (postures unchanged, see
  above).

## Alternatives considered

- **Verifier in `animusd`** — rejected: the canonicalization/HMAC chain is
  exactly the kind of pure wire semantics `animus-dynamo` exists to own
  and unit-test; `animusd` would bury it in transport code.
- **The `aws-sigv4` crate** — rejected: it is signer-oriented, drags in
  `aws-smithy-*` tree surface, and the verify side needs our own
  canonical-request reconstruction anyway; the hand-rolled core is small
  and vector-validated.
- **Default-on** — rejected: it would break every existing test,
  deployment, and quick-start with no credential in hand. Revisit before
  anything GA-shaped; default-off is the pre-alpha-honest choice.
- **Credentials in replicated `Metadata`** — rejected for this milestone:
  it buys rotation/consistency machinery this ADR explicitly defers, and
  makes the control plane a secret store before there is any
  secret-at-rest story.

## Consequences

- AWS SDKs and standard DynamoDB tooling work out of the box against an
  auth-enabled cluster by configuring static credentials — no
  "send unsigned requests" contortions.
- The client edge gains a real (if minimal) barrier: a request without a
  configured secret cannot execute operations. This is still **not**
  authorization, and the ADR's own framing should temper any impulse to
  grow it into IAM piecemeal.
- `animus-dynamo`'s charter widens from "decode/encode" to "decode/encode
  + client-edge auth verification" — flagged here so the crate guide can
  say so and reviewers hold the line on it staying pure.
- First crypto dependencies (`sha2`, `hmac`) enter the tree.
- `HttpRequest` retains all request headers (lowercased map) instead of
  three hardcoded ones — negligible cost, and it removes a standing trap
  for any future header-reading feature.
- Plaintext secrets in config files are an accepted interim posture,
  consistent with "no back-compat until further notice" pre-alpha status.

## Amendment (2026-09-05, ADR 0064)

The Non-goals section above named TLS explicitly: "a separate concern;
SigV4 does not protect confidentiality." [ADR 0064](
0064-tls-on-every-port.md) takes up that concern: server-only TLS on the
client port (confidentiality + server authenticity) layers underneath
SigV4 (caller identity via a static secret) with neither depending on the
other — exactly the independence this ADR anticipated. The intra/admin/
console posture note above ("auth on the intra, admin, or console ports
… postures unchanged") is about *authentication*, not TLS, and is
unaffected: ADR 0064 adds mutual TLS on the intra port and server-only TLS
on the admin/console ports, none of which is an authentication scheme in
the SigV4 sense.
