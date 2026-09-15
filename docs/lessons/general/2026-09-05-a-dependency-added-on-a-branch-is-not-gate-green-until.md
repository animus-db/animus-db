# A dependency added on a branch is not gate-green until `cargo deny check` has actually run against it

PR #630 (S-01, "TLS on every port") landed `rustls-pemfile = "2"` as a new
workspace dependency and merged with every other gate green — but nobody
had run `cargo deny check` against the branch before merge (the sandbox
this work happened in didn't have `cargo-deny` installed, and that was
treated as a reason to skip the gate rather than a reason to install it).
`rustls-pemfile` had in fact been unmaintained since before the PR merged
(`RUSTSEC` advisory, upstream issue rustls/pemfile#61) — `deny.toml`'s
`[advisories]` section has an empty `ignore = []`, so `cargo deny check`
would have failed on `main` the moment CI's `gates` job actually ran it,
which is exactly what happened. The fix was a migration, not an ignore
entry: the advisory's own recommendation is to depend on
`rustls_pki_types::pem::PemObject` (`CertificateDer::pem_slice_iter`/
`PrivateKeyDer::from_pem_slice`, in the `rustls-pki-types` crate already in
the graph at a recent-enough version) instead of `rustls-pemfile` — a
straight drop-in at every call site, since `rustls-pki-types` was already
a transitive dependency doing the DER-typing half of the same job.
**General form, two lessons**: (1) a new external dependency introduced on
a branch is not proven gate-green by fmt/clippy/build/test alone — `cargo
deny check` (advisories/bans/licenses/sources) is a fifth, equally
mandatory gate per the root `CLAUDE.md`'s Commands section, and skipping
it because the tool happens not to be installed in the current sandbox is
exactly the "bypass a gate because it's inconvenient right now" move
Session operating mode item 4 calls out — the correct response is to
install `cargo-deny` (`cargo install cargo-deny`, or find it already on
`PATH`), not to defer the check. (2) when an advisory recommends a
specific migration target, check first whether that target is already in
the dependency graph (here, `rustls-pki-types` was — pulled in for the DER
types `rustls`/`tokio-rustls` themselves consume) — the fix can be a pure
subtraction (delete the flagged dependency, add no new one) rather than a
net-new dependency trading one advisory for a different future one.
