# A refused connection's next read can be a reset, not a clean EOF

**Context**: ADR 0073 Phase 0 workstream D, layer 3 (the client/intra port
handshake). The accept-side test for "a pre-baseline peer whose first bytes
are a raw, unversioned frame is refused as bad magic" sent a whole
length-prefixed `ClientRequest` frame (a `u32` length prefix plus its JSON
body) as the client's first bytes, then asserted the next `read` on that
socket returned a clean `Ok(0)` (EOF).

**What actually happened**: the server's own `read_preamble` reads exactly
`HEADER_LEN` (7) bytes, decodes them, finds a bad magic, and closes the
connection immediately — without ever reading the rest of the frame the
client already wrote. The client's frame was longer than 7 bytes, so at the
moment the server's socket closes, its own kernel receive buffer still held
unread bytes from the client. A POSIX/Linux TCP stack that closes a socket
with unread data in its receive buffer sends a **RST**, not a graceful FIN —
so the client's next `read` surfaced `ECONNRESET`, not `Ok(0)`.

The sibling test in the same file (a version-mismatched but otherwise
well-formed preamble, exactly `HEADER_LEN` bytes with no extension) *did*
get a clean `Ok(0)`: the server's `read_exact` consumed every byte the
client had sent, so there was nothing left in the buffer when it closed.

**The lesson**: a test asserting "the peer refused and closed the
connection" must not assume the close is always a graceful FIN/`Ok(0)` —
whether it is depends on whether the *refusing* side happened to read every
byte the *refused* side already sent before deciding to close. When the
refusal fires on a short, fixed-size prefix (a header) and the client's own
first write is longer than that prefix, the close is a reset. Assert on
"the peer never got a real reply" (`Ok(0)` **or** a reset-class error),
not on the literal byte value of the read result — matching the io-error
classification `animus_env::prod`'s own handshake tests already use
(`ProdEnv`'s "closed connection" / `Err` cases treat any I/O error as
"gone", never insisting on a specific `ErrorKind`).
