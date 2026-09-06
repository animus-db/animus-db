# CLAUDE.md — animus-s3

This file provides guidance to Claude Code (claude.ai/code) when working in
this crate.

## Purpose

An S3 client for AnimusDB's own use — the client half of S-04's three-PR
plan (`docs/roadmap.md` §2; design amendment in
`docs/adr/0059-backup-restore.md`, "S-04: S3 `SegmentStore` backend —
design," 2026-09-06). **This PR ships no `SegmentStore` and no `s3:` URI on
any CLI flag** — nothing in `animusd` depends on this crate yet. It is a
self-contained, independently testable building block: a pure AWS
Signature Version 4 request signer, and a minimal `put`/`get`/`delete`/
`head`/`list_objects_v2` client generic over an explicit transport seam. PR
2 (not yet started) wraps `client::S3Client` in an `animus_env::
SegmentStore` implementation and wires `s3:` onto `--segment-store`/
`--backup-store`; PR 3 does the Kubernetes-operator egress/credential-secret
side.

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
  `AccessDenied`/`Service{code,message,status}`/`Transport(..)`. **No
  retries in this PR** — deliberately: a future `SegmentStore` layer over
  this client owns retry policy, exactly like `animus_env::SegmentStore`'s
  own doc frames the split between "a store's own consistency contract" and
  "what a caller does about a transient failure."
- `xml` — a small, tolerant tag-scanning extractor for the four things this
  crate's responses need (`Contents/Key`, `Contents/Size`, `IsTruncated`,
  `NextContinuationToken`, and an `Error/Code`+`Message` body). **No XML
  crate was added** — see "Why no XML dependency" below.
- `fake` (`#[cfg(any(test, feature = "fake"))]`) — [`fake::FakeS3`], an
  in-memory `Transport` implementor with real SigV4 signature verification
  (see its own doc). Available to a downstream crate's tests too, via the
  `fake` feature.
- `prod` (`#[cfg(feature = "prod")]`) — [`prod::HyperRustlsTransport`], the
  one real-socket/TLS `Transport`. One connection per request, no pooling.

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
  `#[cfg(test)]` (e.g. a downstream crate's own contract test, once PR 2's
  `SegmentStore` impl exists). Adds no dependency: everything `fake.rs`
  needs (`async-trait`, `std::sync::Mutex`, `BTreeMap`) is already
  unconditional.
- Neither feature is required to use `sigv4`/`client`/`xml` — those three
  modules, and this crate's own default build, need nothing beyond
  `async-trait`/`thiserror`/`sha2`/`hmac`.

## Determinism posture

This crate is **not** `animus-env`-seamed (no `Env`, no `SimEnv` story) —
and that is deliberate for this PR, not an oversight to fix later. `sigv4`
and `client::S3Client` are pure functions of their inputs (including
`now_epoch_ms`, always a parameter); the only place real time, real
sockets, or real randomness could enter is `prod::HyperRustlsTransport`,
which does nothing this crate's own tests exercise. When PR 2 wraps this
client in a `SegmentStore` impl, *that* wrapper is where `env.wall_now()`
and `env.spawn_task`/`ProdEnv`'s real transport get threaded in — this
crate stays exactly as pure as it is today. Don't add an `animus-env`
dependency here "to save a parameter" in a future PR; that would reopen the
seam violation ADR 0003 exists to prevent, one layer earlier than it needs
to.

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
chain the two.** Both the request path and the query string are threaded
through this crate as **raw** (unescaped) strings from construction
(`client::S3Client`'s own methods) all the way to [`sigv4::RequestToSign`];
the wire URI and the signed canonical form are each derived by calling
[`sigv4::canonical_uri_s3`]/[`sigv4::canonical_query_string`] **once**,
independently, on that same raw string — never by re-encoding the other's
already-encoded output (which would double-percent-encode, e.g. turning a
key containing a space into `%20` for one purpose and `%2520` for the
other, breaking the signature). `crate::fake`'s verification path is the
mirror image: it receives an already-canonical wire URI/query and must
[`sigv4::percent_decode`] it back to raw **once** before re-deriving the
canonical form for comparison — see `client.rs`'s and `fake.rs`'s own doc
comments at the exact call sites for the worked-through reasoning; this was
a real bug caught and fixed while building this PR (an earlier draft
percent-encoded an object key at both `client::S3Client`'s call site *and*
inside `canonical_uri_s3`, silently breaking any request for a key
containing a character needing escaping).

## Path-style addressing only (this PR)

`client::S3Config` builds `scheme://host/{bucket}/{key}` — path-style,
which is what makes an explicit `--endpoint`-style config work against
MinIO/localstack (the ADR amendment's own stated goal). Virtual-hosted
style (`bucket.host/key`) is a documented, unimplemented option for a later
PR — not a real-AWS correctness gap (path-style still works against real
S3 for a non-`us-east-1`-created bucket).

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
  once" below), `list_objects_v2` pagination across more than one page
  (`FakeS3::with_page_size`), a wrong-secret/unknown-access-key request
  rejected, and both `tests/sigv4_known_answers.rs`/`tests/
  sigv4_chain_matches_dynamo.rs` below (dev-dependencies only, no feature
  needed).
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
    nothing without this variable).
  - `ANIMUS_S3_TEST_BUCKET` — bucket name (must already exist).
  - `ANIMUS_S3_TEST_ACCESS_KEY_ID` / `ANIMUS_S3_TEST_SECRET_ACCESS_KEY` —
    credentials for that endpoint. **Never printed, never included in a
    panic/assert message** — read once into a `sigv4::Credentials` (whose
    `Debug` already redacts the secret) and nowhere else.
  - A `127.0.0.1`/`localhost` endpoint uses `HyperRustlsTransport::
    new_allow_insecure_http()` when `ANIMUS_S3_TEST_ENDPOINT` starts with
    `http://` (a real MinIO dev instance is commonly plaintext); anything
    else requires TLS.
  - To actually run it: start a local MinIO (`docker run -p 9000:9000
    minio/minio server /data`), create a bucket, then `ANIMUS_S3_TEST_ENDPOINT=
    http://127.0.0.1:9000 ANIMUS_S3_TEST_BUCKET=test-bucket
    ANIMUS_S3_TEST_ACCESS_KEY_ID=minioadmin
    ANIMUS_S3_TEST_SECRET_ACCESS_KEY=minioadmin cargo test -p animus-s3
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
