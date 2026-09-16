# A TLS alert's direction comes from which call site logged it, not from reading the error name — issue #913

Investigating issue #913's persistent `AlertReceived(BadCertificate)`
failures needed to answer one load-bearing question first: when pod 3
logs this error with a peer's address, did pod 3 reject the peer's
certificate, or did the peer reject pod 3's? Getting this backwards would
have pointed the whole investigation at the wrong side's trust material.

## The method, not just the answer

`rustls::Error::AlertReceived(AlertDescription)` name alone is
ambiguous — it says an alert was *received*, but not by which logical
role (client or server), and a casual reading could go either way. The
only way to answer it correctly was to find the exact call site that logs
it and read what operation it is inside:

`crates/animus-env/src/prod.rs`'s `spawn_accept` (the **inbound**-accept
loop — `listener.accept().await` — a fact establishable purely from the
function's own name and its call to `TcpListener::accept`) is the *only*
place in this crate that logs `"TLS handshake failed (dropping
connection)"` with a `peer_addr`. So a log line naming this message is
always about a connection this node *accepted*, and `peer_addr` is always
the *dialer's* address, never this node's own.

From there, `AlertReceived` means *this side* (the accepting server)
received the alert, i.e. the alert was *sent by the peer* (the dialing
client). Combined with TLS 1.3's own message order — the server sends its
certificate before the client sends its own, so a client that will not
proceed past validating the server's certificate never gets to the point
of sending anything the server could reject — the only consistent reading
is: **the dialing client rejected the accepting server's presented
certificate.** Every other reading (the server rejecting the client's
cert, which would surface as a *sent*, not received, alert on the
server's own side) is inconsistent with which function actually logged
the line.

## The generalizable lesson

**A protocol error's "direction" (who rejected whom) is determined by
which call site (accept vs. connect, client-role vs. server-role code)
produced the log line, cross-referenced against the protocol's own
message order — never by pattern-matching the error type's name in
isolation.** `Received` vs. `Sent` in an error name tells you which local
operation observed the event, not which peer was at fault; conflating
"my code received X" with "I am the one who did X" inverts the finding.
When a distributed-systems bug hinges on which side of a handshake failed
first, grep for the exact log statement, read the enclosing function's
role (does it call `.accept()` or `.connect()`?), and only then reason
about protocol ordering to determine fault direction. Skipping straight to
"received an alert about a bad certificate, so my own certificate is
probably fine and the peer's is bad" would have been backwards here.

## Where this showed up

`crates/animus-env/src/prod.rs:749-753` (`spawn_accept`, the only
`AlertReceived`-adjacent log site) vs. `crates/animus-env/src/prod.rs:838-851`
(`connect_maybe_tls`, the outbound path, which surfaces a handshake
failure as a plain `io::Error` from `connect_maybe_tls`, never through
this specific log line) — `docs/adr/0064-tls-on-every-port.md`'s issue
#913 round-2 amendment has the full directional analysis this method
produced.
