# A trust store proven present is not proven sufficient — prove TLS trust with a positive AND a negative check (2026-09-20, #991)

PR #862 (closing #854) fixed a real bug: the `animusd` runtime image had no
`ca-certificates` package, so `animus-s3`'s outbound TLS path
(`rustls_native_certs::load_native_certs`) built an empty root store and
every `https://` S3 call failed the handshake. The fix — install the
package — was verified the way this repo verifies most things: a test
proves the fix's *precondition* holds (the package is installed, the store
is non-empty). Nothing proved the *consequence* — that a real TLS handshake
against a real endpoint actually succeeds with that store. The existing
`e2e-kind-s3` leg talks to its RustFS target over plain `http://` by
design, and `e2e-kind-tls` covers `animusd`'s own *inbound* listeners, not
this *outbound* client path. So the fix shipped with its precondition
tested and its actual claim untested, and stayed that way for two weeks
(#854 → #862 → this gap noticed only in a 2026-09-17 triage sweep, filed as
#991) before anything would have caught a regression — the package being
dropped from the `Dockerfile` again, a `rustls`/`rustls-native-certs`
version bump changing how the store is built, or a hostname/SNI mismatch in
how the client dials.

**The general lesson: "the mechanism that would make this work is present"
is not the same claim as "this works," and a test suite that only checks
the former is missing the one failure mode the fix was actually for.** A
positive check alone compounds this — a positive-only test for "the trust
store accepts my cert" can pass for the wrong reason (a handshake that
never happened, a check that silently no-ops, a fixture too permissive to
tell a real cert from a broken one). Pairing it with a negative check — an
endpoint whose cert is deliberately *not* trusted must fail, and fail with
the specific error the trust mechanism would produce — is what rules out
"passes vacuously" from "passes because the mechanism works": the two
checks bound the claim from both sides. `e2e-kind-s3-tls`'s own negative
check (an untrusted base image's pod, run directly against the same
RustFS-over-TLS target, must reach phase `Failed` with both `TLS handshake
with rustfs.<ns>.svc:9443` and `UnknownIssuer` in its logs) exists for
exactly this reason: a misconfigured sidecar or a wrong CA would make the
*positive* case fail too, so the positive case succeeding is not, by
itself, proof that the trust chain — rather than some other coincidence —
is what made it succeed. Running the negative check *before* the positive
one (this leg does it before the `AnimusCluster` is even applied) also
turns a wrong cert/trust setup into an early, unambiguous failure instead
of a late, confusing one downstream.

**A second, narrower lesson from the same fix: a non-root runtime image
turns "just add a CA at runtime" into a build-time decision, and that
build-time shape is also the honest production one.** The obvious way to
extend trust to a pod at deploy time — `kubectl exec` in and run
`update-ca-certificates` — needs root to write `/etc/ssl/certs`, and
`animusd`'s image deliberately runs as `USER animus:animus`. There is also
no product-level hook to reach for instead: `spec.tls`'s own CA feeds
rustls's mTLS config for the cluster's *internal* ports directly and is
never merged into the OS trust store the S3 client reads — a different
trust anchor for a different purpose, not an oversight to wire through.
The only correct shape is a **derived image** built at CI/deploy time
(`FROM` the base image, `USER root`, `COPY` the CA in, `RUN
update-ca-certificates`, back to the non-root user) — which is not a
test-only workaround, it is the real answer for anyone pointing this
product at a private-CA S3 endpoint in production too (now documented in
`deploy/operator/README.md`'s S3 section). When a sandboxed test needs a
privileged step a running container structurally can't take, check whether
the *product's* real users would hit the identical wall — if so, the
test's workaround and the product's documented answer should be the same
mechanism, not two different ones.

See `crates/animus-operator/CLAUDE.md`'s e2e section (`E2E_S3_TLS=1`) and
`scripts/e2e-kind.sh`'s own header doc for the concrete instance.
