# A SAN list (or any per-replica identity list) recomputed from live scale is a reissuance trigger nothing can safely absorb — issue #913

Found investigating issue #864's own `e2e-kind-tls` leg: after a
`spec.nodes` 3→4 scale-up (immediately followed by a `spec.controlNodes`
3→4 growth, which recreates every control pod's own `StatefulSet` pod via
its whole-`StatefulSet` config-hash roll), the recreated pod logged
persistent `AlertReceived(BadCertificate)` mTLS handshake failures against
its own peers and never became `Ready`.

## The bug

`desired::certificate::build` (`crates/animus-operator/src/desired/
certificate.rs`) recomputed the cert-manager `Certificate`'s `dnsNames`
list from live `spec.nodes` on every reconcile — one FQDN per pod ordinal.
cert-manager treats any change to a `Certificate.spec` as a reissuance
request, and this operator mounts the **one resulting `Secret`
identically on every pod** (ADR 0064 commit 3's own "one shared cert, not
per-pod" design — a deliberate simplification, not a bug in itself). So a
scale-up reissues the cluster's one shared leaf certificate, in place, in
the same `Secret` every pod reads its TLS material from.

`animusd` reads that material once, at listener-bind time, and never
reloads it (ADR 0064 Decision 6 names this explicitly as out of scope). A
pod that was already running when the reissue landed keeps the *old*
cert/CA in memory for its whole lifetime; a pod created *after* the
reissue mounts the *new* one. Two pods, both alive, both correctly
running the code they were given, now disagree about the cluster's trust
anchor — permanently, since nothing ever re-reads the file.

The e2e's own self-signed `ClusterIssuer` (`selfSigned: {}`) made this
worse in a way worth calling out on its own: for that issuer type,
cert-manager's output `Secret` sets `ca.crt` **equal to the leaf itself**
— there is no separate, stable signing key. So a reissue there doesn't
just rotate a leaf under a stable root; it mints a brand-new,
mutually-untrusted root every time.

## The generalizable lesson

**Two independent principles, and this bug needed both fixed together:**

1. **A value that feeds a resource's declared identity (a cert's SAN list,
   a peer-set fingerprint, anything a reissuance/rebuild is keyed off)
   must not be derived from a live count that a normal, expected operation
   (scaling) changes routinely** — *unless* every consumer of that
   resource can safely absorb the resulting churn (a hot-reload, an
   at-most-once cutover, a versioned handoff). Query first: **can anything
   downstream actually survive this value changing while the system is
   live?** Here, the answer was no (`animusd` boots once, reads once), so
   the SAN list had no business depending on `spec.nodes` at all. The fix
   was not "make the reissue safer" — it was "make the value stop being
   recomputed from something that scales," using a wildcard SAN that
   covers every future ordinal without ever changing. Prefer a
   scale-invariant identity over a scale-derived one whenever the
   consumer can't reload; don't reach for a reload/rotation mechanism
   first just because the recompute trigger is "obviously correct" in
   isolation (a wider SAN list *is* the correct thing to want — the bug
   was in *how* live it was allowed to be, not in wanting it).
2. **A self-signed issuer used directly as the trust anchor for more than
   one long-lived peer is a structural hazard, independent of anything
   above.** Its "CA" is inseparable from its one leaf, so every
   reissuance is a new root, not a rotated leaf under a stable root. Any
   multi-peer mTLS deployment backed by a self-signed root needs a real
   two-step hierarchy — a CA `Certificate` minted once, then a `ca`-typed
   issuer signing every actual leaf off it — even for a throwaway/dev
   setup, because "throwaway" only excuses the *root's provenance* (no
   real CA, no ACME account), not the *shape* the rest of the system
   depends on. A one-off single-service cert (this same repo's own
   webhook `Certificate`, `deploy/operator/webhook.yaml`) can still use a
   bare self-signed issuer safely, because it never has a second peer
   that needs to keep trusting it across a reissue — the hazard is
   specifically about *shared* trust material with peers that can't
   resync.

Neither fix alone would have been enough on its own for every future case:
fix 1 removes the trigger a routine scale-up hits, but fix 2 is what keeps
the *next* trigger (a time-based cert renewal) from reintroducing the same
failure mode.

## Where this is fixed

`crates/animus-operator/src/desired/certificate.rs`'s `dns_names` (wildcard
SANs, no node-count parameter); `scripts/e2e-kind.sh`'s TLS setup phase (a
bootstrap self-signed `ClusterIssuer` mints one CA `Certificate`, a second
`ca`-typed `ClusterIssuer` signs the actual leaf); `docs/adr/
0064-tls-on-every-port.md`'s issue #913 amendment has the full mechanism
and fix; `deploy/operator/README.md`'s TLS section now says explicitly
never to name a bare `selfSigned` issuer as the cluster's own `issuerRef`.
