# A uniform per-key resource cap needs a per-consumer-class review, not just one measurement — and only `cargo test --workspace` caught it

**What happened.** Building the per-stream inbox byte/frame cap (ADR
0026's inbox-cap amendment — bounding a `Demux`/`SimEnv` stream that
nobody ever consumes), the cap's default was derived carefully from real
measurements: the largest legitimate *single* frame this codebase sends
(an `InstallSnapshot` chunk, a maxed `AppendEntries` batch) and the
largest legitimate *backlog* observed live on a lagging-but-live tablet
stream (~0.6–1.9 MB). The resulting default (8 MiB / 4096 frames) was
sized correctly for that one measured class — an ordinary per-tablet Raft
stream — and applied **uniformly to every stream**, including several
reserved, non-tablet streams (a cross-process request-forwarding relay
channel, a replicated-segment-store request/reply channel) that were
never part of the measurement at all. Per-crate gates (`cargo test -p
animus-env`, `-p animus-sim`, `-p animus-cp-data`) all passed. Only the
full `cargo test --workspace` run — which also builds and exercises
`animusd`, the crate that actually *uses* the request-forwarding relay
channel for cross-node client routing — caught the regression: four
`sim_cluster_dynamo_page_size_cap` tests failed with a real, client-
visible `ServiceUnavailable`, because a multi-hop leader-chase re-sends
its whole request (up to a DynamoDB `BatchWriteItem`'s own 16 MB limit)
on every hop, and a still-relevant reply from an earlier hop was being
evicted by the cap before the chase's own bounded retry budget ran out.

**The general shape.** A resource cap keyed by something narrower than
"the whole system" (a stream id, a connection id, a queue name) is
tempting to size once, from whatever traffic shape happens to be easiest
to measure live, and apply uniformly to every key sharing that keyspace.
This is unsound whenever the keyspace is shared by **structurally
different consumer classes**, not just different traffic *volumes* of the
same class:

- **Different legitimate frame sizes.** The class the measurement came
  from (small, individually-bounded protocol messages) is not necessarily
  representative of every class sharing the same cap (a class that
  legitimately carries a whole large payload per message).
- **Different retry-budget shapes.** "This class tolerates a dropped
  message" is not one fact — a protocol with an *indefinite* retry
  cadence (Raft's own heartbeat-driven resend, effectively unlimited
  attempts) tolerates a dropped frame completely differently than a
  protocol with a **finite, shared-across-hops** budget (a client
  operation's overall timeout, spent hop by hop): losing one round costs
  the first class "one more of infinitely many retries" and can cost the
  second class its *entire remaining budget* if it happens at the wrong
  moment.
- **Different liveness properties of the consumer itself.** The specific
  gap the cap was built to bound (a consumer that might never start
  polling at all) does not apply equally to every key — a consumer bound
  to the *whole process's* lifetime (a serve loop started once at bind
  time) never has that gap; a consumer bound to a *narrower, conditional*
  lifetime (started only once some specific resource is locally hosted)
  does. Capping the first class buys nothing (it was never at risk) while
  still paying the second class's cost (evicting its legitimately larger
  traffic).

**What to do.** Before sizing a shared cap from one measured class, ask
explicitly which *other* classes share the same keyspace, and check each
one's own frame-size ceiling, retry-budget shape, and consumer-liveness
story — the same "classify every consumer, don't assume the one you
measured is representative" discipline this same PR's task description
already asked for the *loss-tolerance* question, which also applies to
the *sizing* question. Where a class's shape genuinely differs enough
that no single cap fits both safely, give it a separate policy (exempt it
entirely, or a distinctly-sized cap) rather than forcing one number to
cover every case — and say why in the same place the cap itself is
documented, so a future reader sizing a *different* shared cap the same
way sees the precedent.

**Always run the full `cargo test --workspace` gate before treating a
change as validated, even when every touched crate's own tests pass.** A
regression that only manifests through a *downstream* consumer's real
usage pattern (here, `animusd`'s forwarding layer, several crates away
from where the cap itself lives) is invisible to every gate scoped to the
crate that introduced the change. This is exactly why the root `CLAUDE.md`
gate list ends with a full-workspace run "once at the end," not as an
optional extra.
