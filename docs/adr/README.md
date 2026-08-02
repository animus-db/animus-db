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
| [0011](0011-accord-consensus.md) | Accord-style leaderless transaction consensus (first minimal slice) | Accepted |
| [0012](0012-failure-detection.md) | Heartbeat-based failure detection in the control plane | Accepted |
| [0013](0013-replicated-schemas.md) | Replicated table-schema catalog in the control plane | Accepted |
| [0014](0014-elle-accord-scenario-corpus.md) | Elle consistency testing against Accord + a frozen scenario corpus | Accepted |
| [0015](0015-observability.md) | Deterministic-safe observability seam (metrics) | Accepted |
