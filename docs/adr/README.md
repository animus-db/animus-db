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
| [0016](0016-pluggable-replication-per-tablet-raft.md) | Pluggable replication: per-tablet Raft (CP) alongside the AP data plane | Accepted |
| [0017](0017-per-tablet-raft-data-plane.md) | Per-tablet Raft data plane (leaderful, linearizable KV) | Accepted |
| [0018](0018-cross-tablet-transactions.md) | Cross-tablet transactions on the CP plane (2PC over per-tablet Raft + HLC + MVCC) | Proposed |
| [0019](0019-cp-only-v1-defer-ap.md) | v1 ships the CP plane only; the leaderless AP data plane is deferred | Accepted |
| [0020](0020-admin-interface.md) | Admin / debug interface on a dedicated port (config, status, Raft + storage introspection, operator actions) | Accepted |
| [0021](0021-web-dashboard.md) | Web dashboard over the admin JSON surface (observe + operator actions, static + self-contained) | Accepted |
| [0022](0022-hash-ring-partitioning.md) | Murmur3 partition token: hash the partition key (amends 0002) | Accepted |
| [0023](0023-table-scoped-tablets.md) | Table-scoped tablets on a per-table hash ring | Accepted |
| [0024](0024-drop-table-data-gc.md) | Drop-table data GC: `DropTableTablets` + a per-node reclaim loop (group stop, engine/WAL file deletion, marker prune) | Accepted |
