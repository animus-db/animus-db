# ADR 0002 — Tablets as the unit of placement and migration

- **Status:** Accepted (partition function **amended by [ADR 0022](0022-hash-ring-partitioning.md) + [ADR 0023](0023-table-scoped-tablets.md)**)
- **Date:** 2026-08-01

> **Amendment (ADR 0022 + 0023):** the tablet/epoch/split-merge model below is
> retained, but tablets now partition a **hashed token space**, scoped **per
> table**, not the raw keyspace. Every key is `escape(table) ||
> partition_token(pk) || escape(pk) || rk`, and each tablet is scoped to one table
> (ADR 0023). The "consistent hashing is awkward for range scans" trade-off
> recorded here is resolved by hashing the *partition key only*, so range scans
> still hold *within a partition* (just not across partitions). See ADR 0022
> (the Murmur3 token) and ADR 0023 (per-table scoping).

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
tablet map. **Split, merge, automatic split-point selection, and replica
rebalancing have since all shipped** (originally future work at this ADR's
writing): manual/triggered split is a single control-plane command (ADR 0028);
merge is its data-plane dual (ADR 0033); split points are chosen automatically
via a byte-weighted median over the tablet's live data, not a plain positional
midpoint (ADR 0034); and replica-set rebalancing — including after a
split/merge, since a new tablet starts from the parent's replica set — runs
continuously via `Metadata::rebalance` (ADR 0029), independent of repair.

## Consequences

- Range scans map naturally onto contiguous tablets.
- Migration and rebalancing move whole tablets, decoupled from any hash ring.
- Every replica-set or range change is observable as an epoch bump, giving the
  data plane a clean fencing token and a clean cache-invalidation signal.
- Splitting, merging, automatic split-point selection, and rebalancing — all
  future work at this ADR's writing — are now built (ADR 0028, 0033, 0034,
  0029); the epoch and range model designed up front here is what let each
  land without changing this ADR's fencing/cache-invalidation contract.
