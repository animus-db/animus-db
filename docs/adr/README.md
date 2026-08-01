# Architecture Decision Records

Short documents capturing significant architectural decisions: their context,
the decision, and the consequences we knowingly accept. New records use
[`0000-template.md`](0000-template.md). When a decision changes, update the
relevant ADR (or supersede it) in the same PR as the code change.

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-two-plane-architecture.md) | Masterless AP data plane + Raft control plane | Accepted |
| [0002](0002-tablets-unit-of-placement.md) | Tablets as the unit of placement and migration | Accepted |
| [0003](0003-deterministic-simulation.md) | Deterministic simulation testing and the `Env` seam | Accepted |
| [0004](0004-dynamo-storage-primitive.md) | Dynamo-lineage storage primitive (partitioned sorted map-of-maps) | Accepted |
| [0005](0005-placement-residency.md) | Placement groups + topology-aware data residency | Accepted |
| [0006](0006-dual-cql-dynamo-adapters.md) | Dual CQL + DynamoDB adapters over a common core | Accepted |
| [0007](0007-agpl-cla.md) | AGPL-3.0 + CLA | Accepted |
| [0008](0008-borrowed-storage-first.md) | Borrowed storage engine first, custom LSM deferred | Accepted |
| [0009](0009-in-house-raft-over-env.md) | In-house Raft over the `Env` seam (deviation from openraft) | Accepted |
| [0010](0010-ap-repair-anti-entropy.md) | AP repair: read-repair + background anti-entropy | Accepted |
