# ADR 0002 — Tablets as the unit of placement and migration

- **Status:** Accepted
- **Date:** 2026-08-01

## Context

Data must be distributed across nodes and rebalanced as the cluster grows,
shrinks, or develops hot spots. Consistent hashing with virtual nodes (the
classic Dynamo approach) spreads load well but makes *contiguous range scans*
and *targeted migration* awkward, and it couples the placement granularity to
the hash ring rather than to data volume.

## Decision

We will shard data into **tablets**: contiguous, sorted key ranges
`[start, end)` that are the atomic unit of placement and migration. Each tablet
maps to a replica set and carries a monotonically increasing **epoch**. The
control plane owns the tablet map; the epoch is bumped on every change to a
tablet's replica set or range.

The epoch is the fencing token for the data plane (ADR 0001): a data-plane
operation carries the epoch the client/router believed current, and a replica
rejects it if the operation's epoch is older than the replica's (fencing is
tracked **per tablet**). Tablets **split and merge** via control-plane
commands (`SplitTablet`/`MergeTablets`), each bumping the affected tablet's
epoch; the data-plane `Router` resolves a key to its owning tablet from a cached
tablet map. Choosing split points automatically, and rebalancing replica sets on
split/merge, remain future work — splits here keep the parent's replica set.

## Consequences

- Range scans map naturally onto contiguous tablets.
- Migration and rebalancing move whole tablets, decoupled from any hash ring.
- Every replica-set or range change is observable as an epoch bump, giving the
  data plane a clean fencing token and a clean cache-invalidation signal.
- Splitting and merging tablets (and choosing split points) is real future work;
  the epoch and range model is designed up front to accommodate it.
