# Before writing code against an unfamiliar crate's exact API, read its real source from the local registry cache — don't guess from memory (ADR 0064, S-01 `rustls`/`rcgen` integration)

Adding `animus-env`'s TLS support meant writing real code against
`rustls` 0.23's typestate builder API (`ServerConfig::builder_with_provider
(..).with_safe_default_protocol_versions()?.with_client_cert_verifier(..)
.with_single_cert(..)?`), `rustls-pemfile` 2.x's `certs`/`private_key`
free functions, `rustls-pki-types`'s `ServerName`/`CertificateDer`/
`PrivateKeyDer`, and `rcgen` 0.13's `CertificateParams`/`KeyPair`/
`signed_by`/`self_signed` — none memorized precisely enough to write
correct call sites from recollection alone, and the rustls 0.20→0.23
builder API in particular has changed shape across major versions in ways
a stale memory would get subtly wrong (e.g. `with_safe_defaults()` no
longer exists; the provider is now chosen explicitly).

**What worked**: before writing a line of `tls.rs`, `cargo metadata` (in a
disposable scratch crate) fetched the exact candidate versions and their
license fields for the cargo-deny check, then a real `cargo check -p
animus-env --features prod` on a *minimal* module (just the types, no
`prod.rs` wiring yet) pulled the actual crate sources into
`~/.cargo/registry/src/index.crates.io-*/<crate>-<version>/` — at which
point `grep`/`sed` over that real source (not docs.rs, which this sandbox
cannot fetch reliably; not memory) answered every "what's the exact
method signature / builder state name / return type" question in seconds,
including the parts that were *not* how a prior-version API would have
shaped them (`ServerConfig::builder_with_provider` returns
`ConfigBuilder<Self, WantsVersions>`, not `WantsVerifier` directly; the
client-cert verifier is `Arc<dyn ClientCertVerifier>` built through
`WebPkiClientVerifier::builder_with_provider(..).build()`, not a bare
`RootCertStore`). Writing the full module against that ground truth, then
compiling, produced **zero API-mismatch errors** on the first `cargo
check` of the complete file — every fix cycle a guess-first approach would
have spent was avoided entirely.

**General form**: when a task requires calling into a crate already
resolved somewhere in the workspace's dependency graph (directly or
transitively — here, `rustls`/`tokio-rustls` via `kube`'s `rustls-tls`
feature) or about to be added fresh, don't write call sites from memory of
"roughly how this kind of API usually looks." `find /root/.cargo/registry/
src -iname '<crate>-<version>*'` (populated by any prior `cargo check`/
`metadata` touching that crate) or a throwaway `cargo check` to populate it
first, then `grep`/`sed -n` the real `src/*.rs` for the exact struct/trait/
method signature, costs one extra tool call and eliminates an entire class
of compile-fix-recompile cycles — especially valuable for crates (rustls
chief among them) whose typestate-builder APIs are easy to get plausibly,
confidently wrong.
