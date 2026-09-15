# A request/reply relay's receive loop is not optional for the client half either — only for the server half (ADR 0061 rung C3d, `SimRelayClient`)

Building `animus_node::sim_relay::SimRelayClient` (a `Network`-backed
`RelayClient` implementor, modeled on `animus_cp_data::
cluster_segment_store`'s own `req_id`/`Pending`-slot correlation) first
shipped with `serve(handler)` as the thing that spawns the one
`RELAY_STREAM` receive loop, documented as "a node that never calls this
can still make outbound `relay()` calls; it just never answers one." That
sentence is only half right, and the wrong half hid a real bug: **the
identical receive loop is what demultiplexes an inbound *request* from an
inbound *reply* on that same stream** (single-consumer, ADR 0026, so both
directions share one inbox). A node that never calls `serve` never starts
that loop at all — so a reply to its *own* outbound `relay()` call, sitting
in its inbox, is never read out and stashed into its `Pending` map either.
The first four-test suite (round trip, timeout, late-reply-not-matched,
concurrent requests) had three of four fail with `None` — not a wrong
answer, no answer ever arrived — because only the *server* side (`relay_a`)
called `serve`; the *client* side (`relay_b`) never did, on the (false)
assumption that "client-only" needs no receive loop.

Root cause, stated generally: whenever one wire carries both a call's
outbound leg and its own inbound reply (true of any request/reply protocol
riding a single-consumer channel, not just this one), the receive loop that
demultiplexes them is not a "server-only" concern — every participant that
ever *originates* a call needs it running too, regardless of whether it
ever *answers* one. Naming the loop-starter the same thing as "start
answering" (this code's first-draft `serve`) actively invites the wrong
mental model. Fixed by separating the two lifecycles: the constructor
(`SimRelayClient::new`) now spawns the receive loop unconditionally (every
node that exists needs it, the same way every node needs a socket bound
before it can either send or receive on a real one), and `serve` was
narrowed to *only* install a handler into an already-running loop's shared
`Arc<Mutex<Option<Handler>>>` — a node that never calls `serve` still
receives its own replies correctly; it just answers an inbound request with
a fixed "no handler installed" error instead of ever hanging.

**General form**: when designing (or reviewing) a request/reply layer over
a shared duplex channel, ask "does the client half need the same receive
loop the server half needs?" before assuming the answer is obviously yes —
it's easy to reason about the server's own inbox and forget that a client
awaiting a reply is *also* a reader of that same inbox, just for a
different message shape. A test that drives a fixture built the "server
only starts the loop" way will show every symptom of a missing reply
(`None`, or a timeout that only ever fires) with no compiler error and no
panic — the loop-driven poll below simply times out silently — so this
class of bug is functional-tests-only to catch, never a build-time one.
