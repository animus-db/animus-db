# Merging a TLS-wrapping branch onto a #596-era generic-stream refactor: `TcpStream::peek` has no substitute on a generic/TLS stream — a zero-length read doesn't work either (merging S-01 onto main)

Merging `claude/s01-tls` (ADR 0064, TLS on every port) onto a `main` that
had, in the meantime, generalized the client-protocol connection handler
to race the request handler against peer abandonment (issue #596,
`peer_closed`/`handle_connection`) found a real integration gap between
two independently-reasonable designs: #596's `peer_closed` used
`OwnedReadHalf::peek` (a raw-socket, non-consuming primitive) to detect a
dropped peer without stealing bytes a pipelined next frame might have
already sent; S-01 wraps that same connection in `MaybeTlsStream`, whose
`Tls` variant is `tokio_rustls::TlsStream` — no such peek exists once the
bytes on the wire are TLS records, and there is no way to "peek behind"
the encryption at the raw socket without duplicating rustls's own framing
logic. A tempting shortcut — read into a zero-length buffer instead — is a
dead end: most `AsyncRead` implementations, `TcpStream` included, special
case an empty destination buffer and return immediately without ever
polling the underlying transport, so it can never actually observe EOF or
wait for anything; it would busy-loop, not detect a close.

**Fixed** with a small purpose-built wrapper (`Rewindable<R>`, `lib.rs`):
an `AsyncRead` adapter holding one optional stashed byte. `peer_closed`
does a real one-byte read (which genuinely waits on the transport and
distinguishes "closed" from "nothing yet", exactly like `peek` did); if a
byte comes back, `Rewindable` stores it and the very next real read
(`read_frame`, the following loop iteration) drains the stash first — from
every caller's point of view this is byte-for-byte identical to a
non-consuming peek, whether the underlying transport is a plain
`TcpStream` or a `MaybeTlsStream::Tls`. `handle_connection` moved from a
generic `<S: AsyncRead + AsyncWrite + Unpin>` (S-01's own shape, which
never needed to split the stream) to a concrete `MaybeTlsStream` parameter
using `tokio::io::split` (the generic splitter, not `TcpStream::
into_split`) — the two designs turned out not to compose by simple
substitution; reconciling them needed a new primitive, not just picking a
side. **General form**: when two branches each generalize the same
function along a different axis (one over the stream *type*, ADR 0064; one
over the stream *usage pattern* — split + race, issue #596), check whether
a primitive one side depends on (`peek`) survives the other side's
generalization before assuming the two diffs will merge cleanly — a
socket-specific capability is exactly the kind of thing a "make it generic"
refactor silently drops.
