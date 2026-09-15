# Nothing in the five gates builds the container images, so a runtime-image dependency can only be caught by reading — and a comment saying "not yet required by anything we ship" expires silently when the feature ships (2026-09-12, issue #854).

**Nothing in the five gates builds the container images, so a runtime-image
dependency can only be caught by reading — and a comment saying "not yet
required by anything we ship" expires silently when the feature ships
(2026-09-12, issue #854).** The `animusd` runtime stage in the root
`Dockerfile` installed no `ca-certificates`, with a comment reasoning that
an outbound-TLS backup target "would need ca-certificates added here — not
yet required by anything v1 ships". True when written; false once S-04/S-05
landed S3 segment and backup stores (ADR 0059, ADR 0068) that
`animus-operator` generates from `spec.s3`. `animus-s3`'s transport builds
its root store from `rustls_native_certs::load_native_certs`, which on
Debian only that package populates, so every HTTPS object-store operation
failed the TLS handshake in any image-based deployment. Three layers of
testing all structurally missed it: `cargo {fmt,clippy,build,test,deny}`
never build an image; `image.yml`'s smoke test runs only `animusd --help`;
and the `e2e-kind-s3` leg deliberately uses plaintext `http://` MinIO with
`allowInsecureHttp: true`, so it never exercises the TLS path at all. Two
generalizable rules. **(1) A comment asserting a *negative* about the rest
of the repo ("nothing we ship needs X yet") is a dated claim, not an
invariant** — it has no compiler or test holding it true, so a feature
landing elsewhere invalidates it with no signal. Prefer wiring the
dependency when the feature lands, or making the claim testable. **(2) When
a deliberate dev-only escape hatch (`allowInsecureHttp`) is what the only
e2e leg uses, that leg proves the mechanism but not the production
configuration** — the plaintext path and the TLS path share almost no code
below the transport, so green e2e said nothing about the shape every real
deployment uses. A second leg over the non-escape-hatch configuration is
the coverage that would have caught this.
