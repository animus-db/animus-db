# A typed `SocketAddr` field on an admin/wire request silently forces IP dialing, which fails every handshake once certificates carry only DNS SANs — issue #913

Three separate rounds of investigating issue #913's `AlertReceived
(BadCertificate)` failures ruled out a Certificate-reissuance mechanism,
a wildcard-SAN/hostname mismatch, and a per-role certificate-selection
bug — each with direct evidence — before the actual cause turned up in a
place none of those rounds were looking: a Rust field's *type*, not its
value.

## The bug

`POST /admin/control/member/add`'s request body carries an `addr` field
for the new voter's internal dial address. Server-side, that field was
typed `std::net::SocketAddr` (`admin::AddControlMemberReq`) — which can
only ever deserialize a literal numeric `ip:port`, never a DNS name, no
matter what string arrives on the wire (JSON itself has no such
restriction; the restriction was purely `SocketAddr`'s own `FromStr`).
The Kubernetes operator's own caller of this route *knew* it wanted to
address a pod by its stable per-ordinal DNS hostname (every other address
surface in that codebase already did exactly that), but couldn't: the
server would reject a hostname outright. So it worked around the
mismatch by reading the pod's *current* IP from the Kubernetes API
instead and dialing that — a documented, deliberate, reviewed workaround,
reasoning that the pod's own imminent self-registration would replace the
pinned IP with its real hostname "within moments," so any staleness was
transient.

That reasoning held under plaintext. It silently stopped holding the
moment mutual TLS with DNS-only certificate SANs entered the picture:
dialing the resolved IP means the TLS handshake's `ServerName` is
`ServerName::IpAddress`, which a certificate carrying only DNS SANs
rejects outright — and the "self-heals in moments" mechanism itself
*needs a working dial to the same node* to run, so under TLS it can never
fire. A workaround that was merely eventually-consistent under one set of
conditions became permanently broken under another, with no change to
the workaround's own code at all — only the introduction of a new
constraint one layer down (ADR 0064's TLS milestone) that nothing
re-examined this call site against.

## The generalizable lesson

**When a wire-format field's Rust type is narrower than what the domain
actually needs (a hostname-capable address forced into `SocketAddr`,
`FromStr`-only), the type itself becomes a design decision with
consequences far from its declaration** — every caller is silently
constrained to whatever that type's parser accepts, and a caller that
needs more (a DNS name) is forced into a workaround at the call site
instead of a fix at the boundary. That workaround can look completely
reasonable in isolation (reading a live API for a fresher value beats a
stale one) while still being wrong for a reason the workaround's own
author had no way to anticipate (a security layer added later that
cares about *what kind* of address was dialed, not just whether dialing
it currently works). Prefer the type the domain actually needs at the
boundary (a plain string, uninterpreted, the way `ProdEnv::merge_peer`/
`NodeAddrs` already model every other address in this codebase) over a
narrower type plus a workaround — the workaround is exactly where a
future, unrelated change is least likely to be checked against it.

**Corollary for debugging a rejected-certificate failure specifically**:
once the *direction* of a TLS alert is established (which side rejected
which certificate — see the accept/connect-call-site method, a separate
lesson from this same investigation), the next question is not "is the
certificate wrong" but "what name was this connection dialed under" —
and tracing that name back to its source can lead through several layers
of address resolution before reaching the one that actually chose IP
over hostname.

## Where this showed up

`crates/animusd/src/admin.rs`'s `AddControlMemberReq.addr` (now `String`,
was `std::net::SocketAddr`); the workaround it forced,
`crates/animus-operator/src/controller.rs`'s `resolve_control_dial_addr`
(deleted) and `ClusterApi::get_pod_ip` (deleted); the real fix,
`add_control_voter` addressing the promoted ordinal by `desired::
pod_fqdn(name, ns, ordinal)` directly. Full account: `docs/adr/
0037-control-plane-membership-change.md`'s issue #913 amendment and
`docs/adr/0060-kubernetes-operator.md`'s matching "The `SocketAddr` gap,
closed" section.
