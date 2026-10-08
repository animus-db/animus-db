# A CA bundle that admits a handshake is not an authorization decision

**Issue #1253.** The cross-region MREC peer CA was merged into the single
`tls.ca_path` the intra listener (and the internal Raft wire) verify client
certificates against. That made a peer region's certificate indistinguishable
from an own-cluster one, so it could send `Forwarded`, bare `Get`/`Put` and
Raft traffic, not only the `MrecApply` frame it was admitted for.

**Why it happened.** The TLS handshake answers "is this certificate valid under
some trusted root", and the first design reused that single answer as the
authorization for every request on the port. A second, lesser trust root cannot
be expressed as "one more CA in the bundle".

**Rule.** When two principals share a listener but not a privilege level, keep
a separate trust root per level (`ca_path` own, `peer_ca_path` peers), admit
both in the handshake, and classify the *connection* afterwards
(`TlsMaterial::classify_peer`). Then gate by an allowlist that is an
exhaustive `match` with no wildcard arm (`peer_region_may_send`): a new request
variant is a compile error until classified, and the default is deny. Test the
refusal with a real-socket client that presents the lesser certificate, and
prove it fails with the gate forced open.
