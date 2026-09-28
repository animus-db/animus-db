# An unpooled per-request dial must pipeline its handshake, not fuse it

**Context**: ADR 0073 Phase 0 workstream D, layer 3's follow-up. The
client/intra wire's mandatory per-connection handshake preamble
(`animus_env::exchange_preamble`: write our own preamble, then **wait**
to read and check the peer's, before either side touches a real frame)
is the right shape for a **pooled** connection — `ProdEnv`'s internal
`Network` transport pays this once per pooled TCP connection, amortized
over every frame/heartbeat that connection ever carries. `animusd`'s own
dial-side wrapper, `perform_client_handshake`, copied that fused
write-then-wait shape verbatim for the client/intra wire too.

**What was wrong**: `relay_request_with_timeout` and `join_request` —
the two functions behind every intra forward, `propose_schema`'s
broadcast fallback, `AnimusdRelayClient::relay`, and a joining node's own
seed discovery — do not pool anything. Each opens a **fresh** connection
per call, by design (a forward is a one-shot relay to whichever node the
caller currently believes is the leader). Fusing the handshake meant
every single one of those calls paid a full extra network round trip
(write preamble, wait for the peer's reply, *then* write the request and
wait again) on top of the request's own round trip — a latency
regression that scales with request volume on exactly the hot path
(forwarding) this repo is most sensitive to, not a one-time connection-
setup cost.

**The fix, and why it's safe**: write our own preamble and the request
frame **back to back**, with no wait in between, then read and check the
peer's preamble, then read the response frame — one round trip total,
not two. This is safe specifically because the *accept* side is
asymmetric, not merely cooperative: the real accept-side handler
(`serve_requests`) runs the fused handshake to completion — write ours,
then read-and-check theirs — **before** it ever enters the frame-reading
loop. So a mismatched peer refuses and closes the connection without
ever looking at the frame a pipelined dialer already sent; the frame is
simply dropped along with the rest of the stream. There is no deadlock
risk either, because both sides only ever write a small, bounded amount
before reading anything (one preamble, plus the dialer's one frame) —
neither write can block on the other side's unread output for long
enough to matter.

**The general lesson**: when a per-connection handshake precedes every
message, check whether the connection is pooled/long-lived or opened
fresh per call before choosing "write ours, wait for theirs, then send"
as the shape. For a pooled connection that's the right (and cheap, since
amortized) choice. For an unpooled, one-shot-per-request dial it silently
doubles that call's round-trip cost, and the fix isn't to skip or weaken
the handshake — it's to pipeline it with the first frame, leaning on the
accept side's own "handshake fully checked before any frame is
processed" invariant to make that safe. Verify the accept side actually
has that invariant before pipelining the dial side to match it; if the
accept side ever interleaves handshake-checking with frame-reading, this
technique stops being safe.
