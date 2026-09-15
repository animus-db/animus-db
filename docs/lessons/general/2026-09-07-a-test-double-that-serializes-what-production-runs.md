# A test double that serializes what production runs concurrently hides deadlocks that only nested calls reveal — fix by mirroring production's concurrency shape, keep determinism through the seeded spawner (issue #731, closing the finding above)

The finding immediately above (`SimRelayClient`'s single receive loop
deadlocking on a nested outbound relay call) is now fixed, not merely
characterized. `animus_node::sim_relay::SimRelayClient::serve_loop`
dispatches each inbound `RelayWire::Request` onto its own task
(`env.spawn_task`) instead of `.await`-ing the installed handler inline
before looping back to `recv_stream` — the identical "one task per inbound
unit of work" shape production's real `AnimusdRelayClient` already has
(one `tokio::spawn`ed task per accepted TCP connection). No new
correlation bookkeeping was needed: `RelayWire`'s `req_id` + the existing
`Pending`-slot `BTreeMap` already handle an arbitrary number of concurrent
in-flight requests/replies (proven by the pre-existing
`concurrent_outstanding_requests_resolve_to_the_right_callers` unit test,
which covers the *client* side of exactly this concurrency) — the deadlock
was purely a *server*-side sequencing bug, dispatch never actually needed
a new correlation mechanism, only to stop serializing itself out of the
`Pending` map's own reach.

**The general lesson, stated once for the next test double this shape
bites**: a simulator-side stand-in for a real, concurrent transport
(`SimRelayClient` for `AnimusdRelayClient`; more generally, any
single-task inline-dispatch receive loop standing in for
"one task per connection/request" production code) that *serializes*
what production genuinely runs *concurrently* is not merely a performance
simplification — it changes the double's own reachable state space.
Ordinary request/reply traffic never notices, because a request handler
that only ever *answers* (never itself calls back out through the same
channel) has no way to observe the difference. The gap only opens once
some handler is **also a client of the identical channel** — a nested
outbound call whose own reply can only arrive through the very loop
currently blocked awaiting that handler. This is exactly the shape ADR
0018 transaction recovery's foreign-intent path has (a forwarded read's
own server-side handling can itself need to forward again), and the first
scenario to combine a crash (forcing the recursion-triggering re-election)
with a cross-tablet transactional intent (forcing the second hop) is what
finally exercised it — nine earlier crash-driven scenarios across five
other `sim_cluster_*` modules never happened to combine both, so the bug
sat latent through every one of them.

**The fix pattern, worth reusing verbatim for the next such double**: keep
the receive loop as the sole, ordered *reader* of the shared inbox (ADR
0026's single-consumer invariant is not what needed relaxing — only one
task ever calls `recv_stream`), but hand off the *handling* of whatever it
receives to a freshly spawned task through the environment's own seeded
spawner (`env.spawn_task`, never a raw `tokio::spawn` — this crate has no
`tokio` dependency at all, and even where one exists the `Env` seam is the
one sanctioned nondeterminism boundary, root `CLAUDE.md`). Concurrency
introduced this way stays fully deterministic and seed-reproducible: the
simulator's own single-threaded cooperative executor still decides,
seeded, which of several ready tasks runs next and in what order their
`send`/`recv` calls interleave — spawning more tasks does not reach
outside that scheduler, it only gives the scheduler more tasks to
interleave among. Verify a fix built this way the way this one was:
confirm an ordinary (non-nested) request/reply round trip is unaffected
(a concurrency fix to a shared receive loop is exactly the kind of change
that can silently reorder or drop an unrelated message if the correlation
story it relies on — `req_id` here — isn't already sound for concurrent
use), and confirm the specific symptom that diagnosed the deadlock is
actually gone at the scenario that found it — but **don't assume "the
symptom is gone" means "the scenario now converges."** It doesn't, here:
see the follow-up entry immediately below.
