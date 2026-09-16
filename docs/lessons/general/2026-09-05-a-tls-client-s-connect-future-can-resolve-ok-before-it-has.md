# A TLS client's `connect()` future can resolve `Ok` before it has read the server's rejection of the client's own certificate

Testing "a client cert signed by an unrelated CA is refused" by asserting
`TlsConnector::connect(..).await.is_err()` failed — the connect future
resolved `Ok(stream)` even though the server's `WebPkiClientVerifier` (via
`with_client_cert_verifier`) does reject the cert. The reason is TLS 1.3's
handshake shape: the client's own handshake state machine considers itself
"done" once it has *sent* its last flight (`Certificate`,
`CertificateVerify`, `Finished`) — it does not have to *read* anything
further back to consider the handshake complete from its own side, since
in the success case the server also has nothing more to send beyond
optional session tickets. The server only learns the client's cert is
untrusted *after* receiving that flight, at which point it sends a fatal
alert and closes the connection — but the client only observes that alert
on its *next* read (or a failed write once the socket is torn down), never
retroactively failing the already-resolved `connect()` future.
`animus-env`'s own `tls_peer_from_different_ca_is_refused` test (ADR 0064
commit 1) already sidesteps this correctly by asserting on a higher-level
effect (no frame ever delivered to the peer) rather than on the dial call
itself; the commit 2 e2e test hit it fresh at the raw-`rustls` layer and
was fixed the same way — assert on a write+read after the handshake, not
on the handshake future's own `Result`.

**General form**: when writing a TLS negative test around **client**-side
certificate rejection specifically (as opposed to a client rejecting a bad
*server* cert, which normally does fail the connect future — the client
validates the server's cert before sending its own final flight), don't
trust `connect()`'s own `Result`. Attempt a real read or write immediately
after and assert *that* fails; if the handshake already failed outright,
the same assertion still passes (there's nothing to write to).
