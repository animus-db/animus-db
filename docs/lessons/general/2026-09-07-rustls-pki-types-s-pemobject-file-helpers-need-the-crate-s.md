# `rustls-pki-types`'s `PemObject::*_file` helpers need the crate's `std` feature, which this workspace's dependency graph does not enable — read the file yourself and parse with `*_slice_iter`/`from_pem_slice` (S-07e, `animus-operator::webhook`)

Loading a PEM cert/key pair from disk reads, at first glance, like it
should be `CertificateDer::pem_file_iter(path)`/`PrivateKeyDer::
from_pem_file(path)` — `rustls-pki-types`'s own convenience methods for
exactly this. Both are gated `#[cfg(feature = "std")]` inside the crate,
and this workspace's `rustls`/`tokio-rustls`/`rustls-pki-types` dependency
set only ever requests `rustls-pki-types`'s `alloc` feature (`rustls`
itself, `Cargo.toml` shows, depends on `pki-types` with `features =
["alloc"]` only) — Cargo feature unification means `std` is simply never
turned on anywhere in this graph, so the `*_file` methods do not exist to
call. `crates/animus-operator/src/admin_client.rs::build_tls_connector`
had already worked around this for a client-side CA load
(`CertificateDer::pem_slice_iter` over bytes read with `std::fs::read`),
but that precedent is easy to miss when writing a *new* loader from
scratch and reaching for the method that reads best (`*_file`) rather than
the one this workspace's feature set actually supports (`*_slice_iter`/
`from_pem_slice`, which need only `alloc` and take a `&[u8]` you read
yourself). `crate::webhook::load_tls_acceptor`'s first draft used the
`_file` variants and only failed at `cargo build` — a clean, unambiguous
"method not found" from a private-to-the-crate `#[cfg]`, not a subtle
runtime gap, but still a wasted round trip. **When adding any new
`rustls-pki-types` PEM consumer in this workspace, grep for an existing
one first** (`admin_client.rs` is the reference shape) rather than trusting
the crate's own public API surface to all be reachable — a dependency's
Cargo features are a property of the whole workspace's unified graph, not
of what any one crate's docs show as available.
