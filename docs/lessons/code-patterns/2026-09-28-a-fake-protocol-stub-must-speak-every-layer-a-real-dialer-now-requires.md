# A fake protocol stub must speak every layer a real dialer now requires

**Context**: ADR 0073 Phase 0 workstream D, layer 3. Adding a mandatory
per-connection handshake preamble to `animusd`'s client/intra dial helper
(`connect_client`, and the two TLS-aware relay dialers,
`join_request`/`relay_request_with_timeout`) broke four existing,
previously-green real-socket tests
(`forward_hop_timeout_tests`/`forward_transport_failure_tests` and their
neighbors) that stand a **fake** peer in for a real node — a raw
`TcpListener` stub that reads a `ClientRequest` frame and writes back a
canned `ClientResponse`, simulating a slow, hinting, or unresponsive
cluster member.

**What broke and why**: the stub never wrote its own handshake preamble
and never read the dialer's. Once the dialer started performing the
handshake first, its own preamble bytes sat in the stub's receive buffer
where the stub's `read_frame::<ClientRequest>` call expected a
length-prefixed JSON frame — decoding `"CHS1"`'s four ASCII bytes as a
`u32` length prefix either failed outright or, worse, "succeeded" into
nonsense. Symmetrically, whatever the stub wrote back (a `ClientResponse`
frame) landed in the dialer's own `read_preamble` call, which correctly
refused it as bad magic. The failure mode was not a hang or a crash — it
was a **clean, correctly-functioning refusal on both sides**, which is
exactly why it is easy to miss: nothing here is broken, both the real
dialer and the real accept path are refusing precisely the malformed peer
they're supposed to.

**The lesson**: whenever a real dialer or a real accept path gains a new
mandatory pre-frame step (a handshake, a length-prefixed envelope, an
auth exchange), every test double that plays "the peer" on the other end
of that connection needs the identical step added to it — a fake server
speaking only the *old* wire shape is no longer a stand-in for a real
node, it's a stand-in for a *pre-upgrade* node, which the real code now
correctly refuses to talk to. Grep for every hand-rolled `TcpListener`/
`TcpStream` stub that impersonates the protocol's own listener (not just
every dialer) before shipping a mandatory wire-level addition — a stub
is easy to miss because it usually lives inside the very test module
whose *assertions* are unrelated to the wire shape (here: hop-timeout
budgeting, not handshakes), so a change to the dial path's own
requirements doesn't visually touch anything the test author would
think to re-check. The fix is small and symmetric: have the stub perform
the identical exchange (write its own preamble, read and check the
dialer's) before it does anything else — reusing the same shared
exchange function the real accept path calls, not a hand-rolled copy.
