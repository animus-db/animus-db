# ADR 0002 — Tablets as the unit of placement and migration

- **Status:** Accepted (partition function **amended by [ADR 0022](0022-hash-ring-partitioning.md) + [ADR 0023](0023-table-scoped-tablets.md)**; tablet lifecycle **amended by [ADR 0044](0044-split-only-tablets.md)** — split-only, tablet merge removed)
- **Date:** 2026-08-01

> **Amendment (ADR 0022 + 0023):** the tablet/epoch/split-merge model below is
> retained, but tablets now partition a **hashed token space**, scoped **per
> table**, not the raw keyspace. Every key is `escape(table) ||
> partition_token(pk) || escape(pk) || rk`, and each tablet is scoped to one table
> (ADR 0023). The "consistent hashing is awkward for range scans" trade-off
> recorded here is resolved by hashing the *partition key only*, so range scans
> still hold *within a partition* (just not across partitions). See ADR 0022
> (the Murmur3 token) and ADR 0023 (per-table scoping).
>
> **Amendment (ADR 0044, 2026-08-14):** tablets are **split-only** — the
> "split-merge" model this ADR's title and body describe is now just
> "split." Tablet merge shipped (ADR 0033) and was later removed entirely;
> a tablet's range only ever narrows, its count only ever grows, and no
> revival of merge is planned. See ADR 0044 for the full rationale.

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
tracked **per tablet**). Tablets **split** via a control-plane command
(`SplitTablet`), bumping the affected tablet's epoch; the data-plane `Router`
resolves a key to its owning tablet from a cached tablet map. **Split,
automatic split-point selection, and replica rebalancing have since all
shipped** (originally future work at this ADR's writing): manual/triggered
split is a single control-plane command (ADR 0028); split points are chosen
automatically via a byte-weighted median over the tablet's live data, not a
plain positional midpoint (ADR 0034); and replica-set rebalancing — including
after a split, since a new tablet starts from the parent's replica set — runs
continuously via `Metadata::rebalance` (ADR 0029), independent of repair.
**Tablet merge (`MergeTablets`) briefly shipped as split's dual (ADR 0033,
2026-08-07) and was then removed entirely** (ADR 0044, tablets are
split-only, 2026-08-14): a tablet's range only ever narrows now, never
widens, and its count only ever grows.

## Consequences

- Range scans map naturally onto contiguous tablets.
- Migration and rebalancing move whole tablets, decoupled from any hash ring.
- Every replica-set or range change is observable as an epoch bump, giving the
  data plane a clean fencing token and a clean cache-invalidation signal.
- Splitting, merging, automatic split-point selection, and rebalancing — all
  future work at this ADR's writing — are now built (ADR 0028, 0033, 0034,
  0029); the epoch and range model designed up front here is what let each
  land without changing this ADR's fencing/cache-invalidation contract.
