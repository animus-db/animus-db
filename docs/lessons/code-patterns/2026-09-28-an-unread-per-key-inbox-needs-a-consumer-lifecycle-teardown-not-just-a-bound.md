# An unread per-key inbox needs a consumer-lifecycle teardown, not just a bound — and "closed" must stay distinct from "not yet opened" when buffering-before-first-consumer is load-bearing

Follow-up to `2026-09-28-an-unbounded-per-stream-queue-is-invisible-until-
it-has-its-own-observability.md` (ADR 0026's measure-first PR): once that
PR's per-stream observability confirmed real, unbounded growth in
`ProdEnv`'s `Demux`/`SimEnv`'s inbox, the design question for the fix was
"is a size/count cap enough?" The answer, for the *retired-tablet* half of
that PR's two causes, is no — a cap alone would just make the symptom
"drop the oldest frames once full" instead of "queue forever," but the
frames still queue *at all* for a consumer that will never come back. The
actual fix needed a **teardown** signal: something that knows, positively,
"this key's consumer is gone, stop queuing for it" — a cap is a mitigation
for a queue that's still fundamentally open-ended, not a substitute for
closing it.

**The generalizable shape**: any per-key (per-stream, per-connection,
per-session, ...) inbox keyed by something narrower than "the whole node"
has two independent failure modes, and fixing one does not fix the other:

1. **Unbounded growth from ordinary traffic to a live-but-slow consumer**
   — the textbook backpressure/cap problem. A size or count limit,
   possibly with a drop-oldest or reject-new policy, is the right tool.
2. **Unbounded growth from traffic to a consumer that is *never coming
   back*** — a released resource, a torn-down session, a tablet moved off
   a node. No cap sizes this correctly, because the right answer isn't
   "keep the newest K frames," it's "keep zero, forever, until (if ever)
   the resource comes back." This needs an explicit **lifecycle** event —
   "this key is retired" — fired by whatever component actually knows the
   consumer is gone, not inferred from a timeout or a liveness heuristic
   at the queue itself (the queue has no way to distinguish "consumer is
   slow" from "consumer is never returning" without being told).

**"Single-consumer, drained on demand" is not a memory bound.** It is
tempting to reason that a design where a stream is drained by exactly one
async consumer, on demand, whenever it gets around to it, is inherently
safe from unbounded growth — after all, *something* will eventually drain
it. This reasoning silently assumes a consumer always eventually shows up
at all. The moment a key can outlive its consumer (a released tablet, a
torn-down connection) that assumption breaks, and the queue accumulates
every message ever sent to that key for the rest of the process's life.
The fix here (`Network::close_stream`) is exactly "stop assuming a
consumer is merely slow, and give the code that actually knows better a
way to say so."

**A closed key must stay distinct from a key that was never opened, when
buffer-before-first-consumer is load-bearing elsewhere.** This codebase's
own split-fork "Deterministic first leader" optimization
(`animus-cp-data/CLAUDE.md`) depends on a freshly-materialized child's
very first message queuing in its inbox *before* its own driver's first
`recv` call — i.e., "nobody has read this key yet" must keep meaning
"buffer for whenever they do," not "reject/drop, since nobody's
listening." Once a lifecycle-teardown mechanism is added to the same
queue, it is easy to accidentally conflate "not yet opened" (buffer) with
"deliberately closed" (drop) if the implementation represents both as "no
consumer registered" — they must be two distinct states (here: an absent
tombstone-set entry vs. a present one), with the tombstone set only ever
written by an explicit close call, never inferred from the absence of a
consumer. A design that reuses one flag/state for both will either break
the load-bearing buffering (treating "not yet opened" as closed) or
silently reopen the leak (treating "closed" as "not yet opened, keep
buffering").

**A key can be legitimately reused after being closed, and the design
must say what that means up front.** Here, a tablet id is a stream id
that can be closed (the tablet is dropped from a node's replica set) and
later reopened (the same node is re-added as a replica later) — a
one-way "closed forever" flag would have been wrong. The chosen rule —
the next call to the consumer-side receive primitive implicitly clears
the closed mark — keeps the fix a pure function of "is anyone genuinely
trying to consume this key right now," matching the load-bearing
buffer-before-first-consumer behavior a never-closed key already had,
rather than inventing a second, separate "reopen" API surface.

**The close call's own correctness depends on being sequenced after the
consumer is confirmed gone, not merely requested to leave.** Closing a
key while its consumer might still be concurrently polling races that
consumer's own "start consuming" step (which, by the rule above,
implicitly reopens) — whichever happens last wins, nondeterministically.
The fix's caller (a reconciler tearing down a tablet's driver) only calls
close once the driver has confirmed it stopped polling at all, never
right after *requesting* a stop. Any teardown-driven cleanup of a
per-key resource should ask the same question: does "stop" mean "asked
to stop" or "confirmed stopped," and does the cleanup step require the
latter?
