# ADR 0004 — Dynamo-lineage storage primitive (partitioned sorted map-of-maps)

- **Status:** Accepted
- **Date:** 2026-08-01

## Context

The data model shapes every layer above it. We want to support both the
Cassandra (CQL) and DynamoDB models on a common core eventually (ADR 0006), so
the core primitive must be expressive enough for both without committing to
either's surface syntax.

## Decision

We will adopt the Dynamo-lineage primitive shared by Cassandra and DynamoDB: a
**partitioned, sorted map-of-maps**.

- A **partition key** selects a partition (the unit of distribution / hashing
  into tablets).
- Within a partition, rows are held in a **sorted map** keyed by a **clustering
  key**, enabling ordered range scans.
- Each row is itself a map of column → value.
- Cells carry **MVCC timestamps/versions** for conflict resolution and snapshot
  reads.

The `StorageEngine` trait (ADR 0008) exposes exactly the operations the
distributed layer needs over this primitive: point `put`/`get`, ordered range
scan, atomic batch write, consistent snapshot, MVCC versions, and range delete.
It has since grown the replicated-apply surface the CP data plane (ADR 0017)
drives: version-gated LWW `merge`/`merge_tombstone`, the single-fsync
`merge_batch`, and the full-image digests `entries`/`entries_with_tombstones`
(the `InstallSnapshot` source). Originally added for the AP plane's
repair/anti-entropy (ADR 0010, deferred with ADR 0019), these are now consumed
by the per-tablet Raft apply loop.

## Consequences

- Both CQL and DynamoDB map onto this core, so the adapters (ADR 0006) become
  translation layers rather than separate engines.
- Sorted clustering keys give us range scans for free at the storage layer.
- MVCC timestamps are foundational, not bolted on, so snapshot isolation and
  last-write-wins conflict resolution share one mechanism.
- We inherit Dynamo-lineage trade-offs (e.g. large partitions are a known hazard)
  and must design tablets (ADR 0002) and later split logic with that in mind.
