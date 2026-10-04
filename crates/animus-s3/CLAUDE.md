# CLAUDE.md — animus-s3

This file provides guidance to Claude Code (claude.ai/code) when working in
this crate.

## Purpose

An S3 client for AnimusDB's own use — the client half of S-04's three-PR
plan (`docs/roadmap.md` §2; design amendment in
`docs/adr/0059-backup-restore.md`, "S-04: S3 `SegmentStore` backend —
design," 2026-09-06, plus its "As-built: PR 2" amendment). This PR (PR 1)
is a self-contained, independently testable building block: a pure AWS
Signature Version 4 request signer, and a minimal `put`/`get`/`delete`/
`head`/`list_objects_v2` client generic over an explicit transport seam —
it ships no `SegmentStore` and no `s3:` URI on any CLI flag itself. **PR 2
is done**: `animus_env::S3SegmentStore<T: client::Transport>`
(`crates/animus-env/src/s3_store.rs`) wraps `client::S3Client<T>` behind
the `SegmentStore` trait, and `animusd`'s `main.rs` wires `s3://...` onto
both `--segment-store`/`--backup-store` — see that crate's own `CLAUDE.md`
entry and `crates/animusd/CLAUDE.md`'s S-04 entry for the full design.
`animus-env` depends on this crate as a plain (non-`fake`, non-`prod`)
optional dependency for `client::S3Client`/`Transport`/`sigv4::Credentials`
alone, and — the first real downstream consumer of the `fake` feature
described below — takes it as a `[dev-dependencies]` feature for its own
`S3SegmentStore` contract test. PR 3 (landed) did the
Kubernetes-operator egress/credential-secret side (`spec.s3`,
`animus-operator`'s `S3StoreSpec`). S-08 M1 (non-static credentials +
virtual-hosted addressing) has landed — see "Credential providers" and
"Addressing" below; S-08 M2 (multipart upload + ranged GET) has
landed too — see "Multipart and ranged GET" below; the rest of S-08 is in
`docs/roadmap.md`.

## Entry points

- `sigv4` — the pure signer. [`sigv4::sign_request`] takes a
  [`sigv4::Credentials`], a [`sigv4::SigningScope`] (region/service), and a
  [`sigv4::RequestToSign`] (method/uri/host/query/headers/payload-hash/
  timestamp) and returns the three headers a caller must send
  (`Authorization`/`X-Amz-Date`/`X-Amz-Content-Sha256`). No I/O, no clock —
  "now" (`RequestToSign::timestamp`) is always a caller-supplied parameter,
  the same "now passed in, never read" convention `animus_dynamo::ttl`/
  `animus_dynamo::sigv4` already use; see [`sigv4::format_amz_date`] for
  turning an epoch-seconds value into the `X-Amz-Date` shape. Also exposes
  the verification-support half ([`sigv4::parse_authorization`]/
  [`sigv4::verify_signature`]) `crate::fake` uses to check every request's
  signature end to end.
- `client` — [`client::S3Client<T: client::Transport>`]: `put_object`/
  `get_object`/`delete_object`/`head_object`/`list_objects_v2` (prefix +
  continuation-token pagination), all taking a `now_epoch_ms: u64` for the
  same reason `sigv4` does. [`client::Transport`] is the seam: a minimal
  `async fn send(HttpRequest) -> Result<HttpResponse, TransportError>` over
  owned bytes — no socket type anywhere in the trait, so the client is
  testable without one. [`client::S3Error`] distinguishes `NotFound`/
  `AccessDenied`/`Service{code,message,status}`/`Transport(..)`;
  `TransportError` is `Connect`/`Io`/`Timeout`. **No retries in the client**
  — `animus_env::S3SegmentStore` owns retry policy (S-08 M3: Env-seamed
  exponential backoff with full jitter), so it is seed-testable.
- `creds` — **pure** credential sourcing (S-08 M1): the async
  [`creds::CredentialProvider`] trait (`credentials(now_epoch_ms)` +
  `refresh(stale, now)`; never reads a clock), [`creds::StaticProvider`],
  [`creds::CachingProvider`] (refresh 5 min before expiry, single flight),
  and the `Transport`-driven sources [`creds::StsWebIdentityProvider`]
  (unsigned `AssumeRoleWithWebIdentity`), [`creds::ContainerProvider`]
  (ECS / EKS Pod Identity) and [`creds::ImdsV2Provider`]. Tokens (web
  identity JWT, container auth) arrive as injected [`creds::TokenSource`]
  closures — no `std::fs`/`std::env` here. `creds_prod` (`prod`-gated) holds
  the env-var provider and file/env token sources.
- `xml` — a small, tolerant tag-scanning extractor for the four things this
  crate's responses need (`Contents/Key`, `Contents/Size`, `IsTruncated`,
  `NextContinuationToken`, and an `Error/Code`+`Message` body). **No XML
  crate was added** — see "Why no XML dependency" below.
- `fake` (`#[cfg(any(test, feature = "fake"))]`) — [`fake::FakeS3`], an
  in-memory `Transport` implementor with real SigV4 signature verification
  (see its own doc). Available to a downstream crate's tests too, via the
  `fake` feature.
- `fake::FaultyTransport<T>` (S-08 M3) — wraps any transport with a scripted
  `FaultPlan` (`FnMut(request index, &HttpRequest) -> Fault`); faults: `Pass`,
  `Status{status, code}` (S3 error body, not applied), `TransportError`,
  `Timeout`, `ApplyThenError` (applied to the inner transport, response
  replaced by a transport error — the lost-ack case). Used by
  `animus-test`'s `s3_fault_corpus`.
- `prod` (`#[cfg(feature = "prod")]`) — [`prod::HyperRustlsTransport`], the
  one real-socket/TLS `Transport`. One connection per request, no pooling.
  **Timeouts (S-08 M3)**: `TransportTimeouts` (default 10 s connect = TCP +
  TLS handshake, 60 s whole request — multipart parts are 16 MiB; set with
  `with_timeouts`) via `tokio::time::timeout`, surfacing as the retryable
  `TransportError::Timeout`. Real-socket test: `tests/transport_timeout.rs`
  (a listener that accepts and never answers).

## Feature flags

- **`prod`** (default off) — mirrors `animus-env`'s own `prod` feature (ADR
  0061 rung C0) exactly: gates the crate's only real I/O
  (`prod::HyperRustlsTransport` — real TCP, optional rustls TLS,
  `tokio::spawn` to drive the `hyper` connection future). A consumer
  depending on this crate with `default-features = false` cannot name
  `HyperRustlsTransport` at all — it fails to compile, not just goes
  unused. Pulls in `tokio`/`hyper`/`hyper-util`/`http-body-util`/`bytes`/
  `rustls`/`tokio-rustls`/`rustls-pki-types`/`rustls-native-certs`/
  `tracing`, all `optional = true` and otherwise absent from the build.
- **`fake`** (default off) — makes `crate::fake` available outside
  `#[cfg(test)]`. **Consumed since PR 2** by `animus-env`'s own
  `[dev-dependencies]` (`S3SegmentStore`'s contract test, `s3_store.rs`) —
  the downstream contract test this bullet used to describe as a future
  possibility. Adds no dependency: everything `fake.rs` needs
  (`async-trait`, `std::sync::Mutex`, `BTreeMap`) is already unconditional.
- Neither feature is required to use `sigv4`/`client`/`xml` — those three
  modules, and this crate's own default build, need nothing beyond
  `async-trait`/`thiserror`/`sha2`/`hmac`.

## Determinism posture

This crate is **not** `animus-env`-seamed (no `Env`) — deliberate: `sigv4`
and `client::S3Client` are pure functions of their inputs (including
`now_epoch_ms`, always a parameter), `creds` never reads a clock, and the
only real time/sockets are in `prod::HyperRustlsTransport` (module-level
justified allow; its timeouts are real-time by nature). The *seam* is one
layer up: `animus_env::S3SegmentStore<T, E: Clock + Rng>` supplies
`env.wall_now()`, `env.sleep()` and jitter, so retry behaviour is
seed-reproducible under `SimEnv` (`FaultyTransport` + `s3_fault_corpus`).
Don't add an `animus-env` dependency here "to save a parameter".

## Why no XML dependency

`docs/roadmap.md`'s S-04 plan explicitly names the trade-off: no XML crate
was in the workspace `Cargo.lock` before this PR, and `ListObjectsV2`/error
bodies need exactly four tag shapes. `xml.rs`'s `between`/`xml_unescape`
(~60 lines, fully unit-tested against a real multi-page `ListBucketResult`
body, an empty bucket, an entity-escaped key, and a `NoSuchKey` error body)
cover that surface without a new dependency to license/advisory-vet. See
that module's own doc for exactly what it does *not* handle (nested
same-named tags, comments, CDATA, attributes) — none of which the four
tags this crate reads ever need.

## Sharing the SigV4 signing-key chain with `animus_dynamo::sigv4` (ADR 0057)

**Decision: copy the ~20-line HMAC chain, don't depend on `animus-dynamo`.**
See `sigv4.rs`'s own module doc for the full reasoning (short version:
`animus-dynamo` is a DynamoDB-specific wire adapter — depending on it here
would be the wrong layering direction, since S-04's own plan has
`animus-cp-data`/`animusd` depend on `animus-s3` directly, never through
`animus-dynamo`). `tests/sigv4_chain_matches_dynamo.rs` is the
dev-dependency-only proof the two chains agree byte-for-byte on the same
inputs — the **only** place this crate names `animus-dynamo` at all (not in
any `[dependencies]` entry, only `[dev-dependencies]`).

## S3 vs. generic-service SigV4 canonicalization

`sigv4::canonical_uri_s3` deliberately does **not** resolve `.`/`..`
path segments the way `animus_dynamo::sigv4::canonical_uri` does for the
generic AWS SigV4 test suite — an S3 object key may legitimately contain a
literal `.`/`..`/empty segment as part of its name, and resolving them away
before signing would silently mis-sign (or worse, mis-address) such a key.
See `sigv4.rs`'s module doc for the full comparison.

**Encode exactly once, from the same raw input, for each purpose — never
chain the two.** The request path is threaded through this crate as a
**raw** (unescaped) string from construction (`client::S3Client`'s own
methods) all the way to [`sigv4::RequestToSign::uri`]; the wire URI and the
signed canonical form are each derived by calling [`sigv4::canonical_uri_s3`]
**once**, independently, on that same raw string — never by re-encoding the
other's already-encoded output (which would double-percent-encode, e.g.
turning a key containing a space into `%20` for one purpose and `%2520` for
the other, breaking the signature). `crate::fake`'s verification path is the
mirror image: it receives an already-canonical wire URI and must
[`sigv4::percent_decode`] it back to raw **once** before re-deriving the
canonical form for comparison — see `client.rs`'s and `fake.rs`'s own doc
comments at the exact call sites for the worked-through reasoning; this was
a real bug caught and fixed while building this PR (an earlier draft
percent-encoded an object key at both `client::S3Client`'s call site *and*
inside `canonical_uri_s3`, silently breaking any request for a key
containing a character needing escaping).

**The query string follows the same "encode exactly once" rule, but as
typed pairs, never a joined string** (fixed by issue #855 — see
[`sigv4::RequestToSign::query`]'s own doc for the full account). An earlier
version of this crate joined raw `(key, value)` pairs with `&` into a single
string, threaded *that* through to `RequestToSign`, and separately
re-derived the wire query by splitting the same joined string back apart —
so a value containing its own literal `&` (S3 permits `&` in an object key,
and ADR 0068's `ImportTable` `S3KeyPrefix` is an unrestricted customer
string that reaches `list_objects_v2`'s `prefix` this way) was
indistinguishable from a second key/value pair once joined, silently
corrupting both the wire request and its own signature identically. Fixed
by never introducing the joined-string representation at all:
[`sigv4::canonical_query_string`] takes `&[(&str, &str)]` directly (raw,
unescaped pairs — percent-encodes and sorts them once, itself), and
[`sigv4::RequestToSign::query`] is typed pairs for the same reason.
`crate::fake`'s verification path recovers pairs from a wire query string
with [`sigv4::parse_wire_query_pairs`], which splits on `&`/`=` **before**
percent-decoding (safe by construction, since an already-encoded value's own
`&`/`=` would already read as `%26`/`%3D`) — never by decoding the whole
string first and then splitting the result, which would reopen the
identical bug on the verification side.

## Addressing

Default is **path-style** (`scheme://host/{bucket}/{key}`) — what makes an
explicit `--endpoint` config work against MinIO/RustFS/localstack.
`S3Client::with_addressing(Addressing::VirtualHosted)` switches to
`bucket.host/key` (the `Host` header and the SigV4-signed host both become
`bucket.host`; the path is just the key; list is `GET /`). It is a client
builder, **not** an `S3Config` field, so every existing `S3Config { .. }`
struct literal keeps compiling. `validate_virtual_hosted` rejects an IP
endpoint (no `bucket.1.2.3.4`) and a non-DNS-compatible bucket (3-63 chars
`[a-z0-9-]`, alnum ends; dots rejected too — they break the wildcard TLS
cert) with `S3Error::InvalidConfig`. `FakeS3` recovers the bucket from the
first label of the `Host` header when it equals its bucket.

## Credential providers (S-08 M1)

- `S3Client` holds an `Arc<dyn CredentialProvider>`: `new(transport,
  S3Config)` wraps `config.credentials` in a `StaticProvider`;
  `with_provider(transport, S3Target, provider)` is the non-static entry.
- **Wrap fetching providers in `CachingProvider`.** `Sts…`/`Container…`/
  `Imds…` fetch on every call by design; the cache supplies refresh-at-
  expiry-5min and single flight (a tiny hand-rolled async gate, so the pure
  crate needs no runtime). A failed refresh inside the 5-minute window
  serves the still-unexpired cached credentials.
- **Expired-token retry**: an S3 error body coded `ExpiredToken`/
  `ExpiredTokenException`/`InvalidToken`/`TokenRefreshRequired` makes the
  client call `provider.refresh(&stale, now)` and retry the identical
  request **once**; a second rejection is `S3Error::CredentialsExpired`.
  `HEAD` errors carry no body, so an expired token on `HEAD` is not
  detected (it surfaces as a plain status error) — known limitation.
- Temporary credentials add `x-amz-security-token` as a **signed** header
  (`sigv4::sign_request`); `Credentials` `Debug` redacts both secret and
  token. Provider errors never embed a token or raw response body.
- STS is called **unsigned** (the JWT is the authenticator); a test asserts
  no `Authorization` header is sent. `FakeCredentialService` (fake.rs) is
  the STS/ECS/IMDS double; `FakeS3::register_session_credential` makes S3
  demand the token and answer `400 ExpiredToken` by the request's own
  signed timestamp (the caller-supplied `now`).

## Multipart and ranged GET (S-08 M2)

- `S3Client::{create_multipart_upload, upload_part, complete_multipart_upload,
  abort_multipart_upload, get_object_range}` all go through the same
  `execute` path as every other call (path-style and virtual-hosted, session
  tokens, ExpiredToken refresh-once). `upload_part` returns the `ETag`
  header verbatim (quoted); `complete` sends parts in the order given.
- **`CompleteMultipartUpload` can answer HTTP 200 with an `<Error>` body**
  (S3 flushes headers, then fails assembly). `xml::parse_complete_multipart`
  treats that — and any 200 that is not a `CompleteMultipartUploadResult` —
  as a failure; `InternalError`/`SlowDown` inside a 200 surface as
  `Service { status: 500 }` so a caller's 5xx retry applies.
- `abort_multipart_upload` is idempotent (404 `NoSuchUpload` is `Ok`).
- `get_object_range(key, start, len)` signs `range: bytes=a-b`, requires
  `206` (a `200` means the server ignored the range and is refused), and a
  range past the end is truncated by S3 (short final slice); `start` past
  the end is `Service { status: 416, code: "InvalidRange" }`.
- **Abandoned uploads**: a failed abort or a killed process leaves an
  incomplete upload that is billed. Operators should set a bucket lifecycle
  rule `AbortIncompleteMultipartUpload` (e.g. `DaysAfterInitiation: 1`).
- `FakeS3` knobs: `with_min_part_size` (default `S3_MIN_PART_SIZE` = 5 MiB,
  non-last parts below it fail `EntityTooSmall` at complete),
  `open_upload_count()`, `request_log()` (`"METHOD uri"` of every
  authenticated object request), `fail_upload_part(n, times)`,
  `set_complete_error_in_200(code)`. It validates ascending part order
  (`InvalidPartOrder`), part existence/ETag (`InvalidPart`), and answers
  `206` + `Content-Range` / `416`. ETags are truncated SHA-256, not MD5.
- Tests: `tests/it/multipart.rs`; the real-endpoint test has a multipart
  (2x5 MiB + 1 MiB) and ranged-GET leg.

## Testing

- **`cargo test -p animus-s3`** — the pure signer's own unit tests
  (`sigv4.rs`, including the S3 doc-example request shape, `UNSIGNED-PAYLOAD`
  support, a sign/verify round trip, `percent_encode`/`percent_decode`
  round-tripping, and the redacting `Debug` for `Credentials`), `xml.rs`'s
  tag-extraction tests, and `client.rs`/`fake.rs` round-trip tests (via
  `tests/client_fake.rs` — the crate's own `[dev-dependencies]` self-
  reference with `features = ["fake"]` makes `fake` available to every test
  target of this crate without needing `--all-features` on the command
  line, see `Cargo.toml`'s own comment on that entry): put/get/head/delete
  round trips, a 404 on a missing key, a key containing special characters
  (space, `+`, parens — the exact shape that would break under the
  double-percent-encoding bug this PR found and fixed, see "Encode exactly
  once" below), a `list_objects_v2` prefix containing a literal `&` (issue
  #855 — the query-string analogue of the same double-encoding bug class,
  see "Encode exactly once" below), `list_objects_v2` pagination across more
  than one page (`FakeS3::with_page_size`), a wrong-secret/unknown-access-key
  request rejected, and both `tests/sigv4_known_answers.rs`/`tests/
  sigv4_chain_matches_dynamo.rs` below (dev-dependencies only, no feature
  needed).
- **`tests/query_encoding.rs`** — issue #855's own end-to-end regression:
  drives `S3Client::list_objects_v2` over a small recording `Transport`
  (not `fake::FakeS3`, so the test inspects exactly what `S3Client` built
  rather than having a second double re-interpret it) and asserts the wire
  query for a prefix containing `&` is the exact canonical SigV4 encoding,
  and that the request's own signature verifies against those same raw
  pairs (and does *not* verify against the truncated prefix a join-then-
  split bug would have produced).
- **`tests/sigv4_known_answers.rs`** — AWS's own published SigV4
  test-vector suite (the same `aws-sig-v4-test-suite` `animus-dynamo`
  vendors, transcribed here as literal test cases rather than a second
  vendored file tree, since only four are needed): `get-vanilla`,
  `get-vanilla-query-order-key-case`, `post-x-www-form-urlencoded`, and an
  `UNSIGNED-PAYLOAD` case, asserting `sigv4::canonical_request`/
  `sigv4::string_to_sign`/the final `Authorization` signature against the
  suite's own precomputed values.
- **`tests/sigv4_chain_matches_dynamo.rs`** (dev-dependency on
  `animus-dynamo`) — the signing-key-chain equivalence proof: the same
  request, credentials, and timestamp signed through both
  `animus_dynamo::sigv4::sign` and `animus_s3::sigv4::sign_request` produce
  the identical `Signature`. **Known limitation, recorded rather than
  papered over**: this crate could not independently verify AWS's own
  published SigV4 documentation example's exact literal signature value
  from this sandbox (no network access to re-derive the precise
  byte-for-byte canonical request from an authoritative source; several
  plausible reconstructions from memory did not reproduce it) — so neither
  this test nor `sigv4.rs`'s own unit test asserts that literal constant.
  Both instead prove what's independently self-checkable: the two crates'
  chains agree with each other, and `sign_request`'s own output round-trips
  through `verify_signature`. Re-deriving and pinning the exact documented
  value is a good follow-up for a session with network/AWS-SDK access.
- **`cargo test -p animus-s3 --all-features`** — everything above, plus
  `prod.rs` compiles and its own unit tests (`server_name_for`'s IP-vs-DNS
  derivation) run, plus `tests/minio_real_endpoint.rs` (below).
- **`tests/minio_real_endpoint.rs`** (`#[cfg(feature = "prod")]`) — an
  opt-in **real** round trip (`put`/`get`/`list`/`delete`) against a real
  S3-compatible endpoint, driven entirely by environment variables so the
  workspace gates stay green with no infrastructure:
  - `ANIMUS_S3_TEST_ENDPOINT` — e.g. `http://127.0.0.1:9000`. **Unset ⇒ the
    test prints a skip line and returns immediately** (never `#[ignore]`d —
    `cargo test -p animus-s3 --all-features` always runs it, it just does
    nothing without this variable) — unless `ANIMUS_S3_REQUIRE_ENDPOINT=1`,
    which makes the skip a panic (CI's `s3-real-endpoint` job sets it, so the
    test can never pass vacuously there).
  - `ANIMUS_S3_TEST_BUCKET` — bucket name (must already exist).
  - `ANIMUS_S3_TEST_ACCESS_KEY_ID` / `ANIMUS_S3_TEST_SECRET_ACCESS_KEY` —
    credentials for that endpoint. **Never printed, never included in a
    panic/assert message** — read once into a `sigv4::Credentials` (whose
    `Debug` already redacts the secret) and nowhere else.
  - A `127.0.0.1`/`localhost` endpoint uses `HyperRustlsTransport::
    new_allow_insecure_http()` when `ANIMUS_S3_TEST_ENDPOINT` starts with
    `http://` (a real MinIO dev instance is commonly plaintext); anything
    else requires TLS.
  - To actually run it: start a local S3-compatible store (CI uses RustFS:
    `docker run -p 9000:9000 -e RUSTFS_VOLUMES=/data -e RUSTFS_ADDRESS=0.0.0.0:9000
    -e RUSTFS_ACCESS_KEY=... -e RUSTFS_SECRET_KEY=... rustfs/rustfs:1.0.0-rc.6`,
    data dir writable by uid 10001; the `minio/minio` image no longer
    resolves, #863), create a bucket, then `ANIMUS_S3_TEST_ENDPOINT=
    http://127.0.0.1:9000 ANIMUS_S3_TEST_BUCKET=test-bucket
    ANIMUS_S3_TEST_ACCESS_KEY_ID=<access key>
    ANIMUS_S3_TEST_SECRET_ACCESS_KEY=<secret> cargo test -p animus-s3
    --features prod --test minio_real_endpoint -- --nocapture`.

## What's non-obvious

- **`Credentials`'s `Debug` never renders the secret** (mirrors
  `animus_control::meta::SecretKey`'s redaction discipline exactly — always
  `"REDACTED"` regardless of the actual value). Never add a `Display` impl,
  never `.secret_access_key()` a value into a log/panic/assert message.
- **Every `S3Client` method takes `now_epoch_ms: u64`** rather than reading
  a clock — see "Determinism posture" above. A future caller (PR 2's
  `SegmentStore` impl) passes `env.wall_now()`.
- **`fake::FakeS3` really verifies signatures**, including rejecting a
  claimed `x-amz-content-sha256` that doesn't match the actual body bytes
  (`XAmzContentSHA256Mismatch`, matching real S3's own behavior) — it is
  not a bare in-memory map that happens to also check for an
  `Authorization` header's presence. This is what makes the fake-backed
  tests a real exercise of the signer, not just the client's HTTP-shape
  logic.
- **`prod::HyperRustlsTransport` builds a fresh `hyper::client::conn::
  http1` connection per call** rather than using `hyper-util`'s pooled
  legacy `Client` — see `prod.rs`'s own doc comment on `send_over` for why
  (the legacy client's connector trait shape doesn't fit a one-shot
  per-request connection cleanly, and this transport deliberately doesn't
  pool anyway; `docs/engineering-lessons.md`'s "Adding TLS to a
  `hyper-util` legacy `Client`..." entry has the fuller account of the
  trait-shape mismatch from `animus-operator`'s own admin client, which hit
  the identical wall).
- **`rustls-native-certs`/`hyper`/`hyper-util`/`http-body-util`/`bytes`
  are all already resolved in the workspace `Cargo.lock`** (transitively,
  via `animus-operator`'s `kube` dependency) at the exact versions this
  crate's `Cargo.toml` pins — adding them as this crate's own direct,
  `prod`-gated dependencies adds no new package version to the lockfile.
