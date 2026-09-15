# `rustls` treats a peer's abrupt TCP close as an error, not clean EOF — even when every byte you wanted was already delivered

A `Connection: close` HTTP/1.x server (this crate's hand-rolled admin/
dynamo/console edges) closes the raw socket once its response is fully
written, without sending a TLS `close_notify` alert first. A plain
`TcpStream` client sees this as ordinary EOF; a `tokio_rustls`-wrapped
client's `read_to_end` instead resolves to
`Err(UnexpectedEof("peer closed connection without sending TLS
close_notify"))` — `rustls` treats a missing `close_notify` as a
truncation attack signal by design (RFC 8446 §6.1), regardless of whether
the peer's *application data* was in fact complete. The bytes that did
arrive are still fully present in the caller's buffer (tokio's
`read_to_end` mutates the buffer in place as it reads, independent of the
final `Result`); only the `Result` itself reports failure.

**General form**: a TLS test client reading a `Connection: close`-style
response until EOF should ignore `read_to_end`'s `Result` and trust the
buffer's contents instead (`let _ = stream.read_to_end(&mut buf).await;`)
— treating that specific error as fatal would make every otherwise-correct
response look like a failure. This is specific to a peer that closes
without `close_notify`; a peer that shuts its TLS session down properly
gives a real `Ok` and needs no such workaround.
