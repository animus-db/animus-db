# ADR 0001 — Masterless AP data plane + Raft control plane

- **Status:** Accepted — **amended for v1 by [ADR 0019](0019-cp-only-v1-defer-ap.md):**
  v1 ships the **CP** per-tablet-Raft data plane (ADR 0016/0017) only; the
  leaderless AP data plane decided here is **deferred** (a long-shot future
  improvement). The control-plane decision below is unchanged.
- **Date:** 2026-08-01

## Context

A Dynamo-lineage database must stay available for reads and writes under network
partitions and node failures (the AP point of the CAP triangle). But *some*
state — who is in the cluster, which nodes own which key ranges — must be
linearizable, or replicas will disagree about ownership and corrupt data. These
two requirements pull in opposite directions: one wants availability, the other
wants consistency.

## Decision

We will split the system into two planes:

- A **data plane** that is leaderless and AP, serving reads and writes with
  tunable quorum consistency (R + W > N for read-your-writes).
- A **control plane** that is strongly consistent, backed by Raft, and owns
  cluster metadata: membership and the tablet map (key range → replica set +
  epoch). Metadata mutations are compare-and-swap transactions keyed by epoch.

The planes are never blurred. A control-plane outage must **not** take down the
data plane: data nodes keep serving reads and writes from cached metadata; only
operations that require a *topology change* (membership, tablet moves) block
until the control plane recovers.

## Consequences

- The data plane can survive the loss of the entire control plane for the
  duration of cached-metadata validity, which is the central availability
  property we are buying.
- Routing correctness now depends on **epoch fencing**: data-plane operations
  carry the epoch they believe is current, and replicas reject operations
  bearing a stale epoch (see ADR 0002).
- Two consistency models coexist in one codebase; contributors must always know
  which plane they are working in. We enforce this split in review.
- Clients and nodes can act on slightly stale metadata; the design must tolerate
  that window rather than assume a globally fresh view.
