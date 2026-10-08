# Cross-cluster mTLS needs the peer CA in the listener's own trust file

**Context.** ADR 0075 G-e (operator federation). animusd's PeerCluster `tls_ca`
only extends how a node verifies a peer's *server* certificate when dialing.
The intra listener verifies the peer's *client* certificate against the node's
single `tls.ca_path`.

**Lesson.** Mounting each peer's CA and setting per-peer `tls_ca` is only half of
trust: without the peer CA in `ca_path` the peer can be dialed but cannot dial
back. Check both directions of an mTLS pair. The cheap fix with no animusd
change is a start-up concatenation (the loader reads every cert in a PEM) into a
scratch file that `ca_path` names; the cost is that CA rotation needs a restart.

**Also.** A NetworkPolicy cannot select DNS names, so peer rules are port-scoped
and authentication must come from the TLS layer; say so rather than imply an
address allowlist.

**Update (issue #1253, 2026-10-08).** Merging the peer CA into `ca_path` over-trusted it on the whole intra port. The peer CA now goes in `tls.peer_ca_path`, which admits the handshake but trusts the connection for `MrecApply` only; see `2026-10-08-admission-is-not-authorization-split-the-trust-root.md`.
