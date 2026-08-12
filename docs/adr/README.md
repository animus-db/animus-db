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
| [0017](0017-per-tablet-raft-data-plane.md) | Per-tablet Raft data plane (leaderful, linearizable KV) | Accepted (§4/split superseded by 0028) |
| [0018](0018-cross-tablet-transactions.md) | Cross-tablet transactions on the CP plane (2PC over per-tablet Raft + HLC + MVCC) | Accepted — implemented (PR1: HLC + sim clock skew; PR2: HLC commit timestamps as the MVCC version + the range-seal design, replacing `version_floor`; PR2b: MVCC snapshot reads at a timestamp + the read-timestamp cache/logged read ceiling; PR3: single-participant transactions — the value envelope, the txn record/intent/resolve machinery through one Raft group; PR4: multi-participant 2PC across tablet Raft groups, the wire-level coordinator, foreign-intent resolution, and uncertainty-interval read restarts; PR5: in-doubt transaction recovery off a crashed coordinator + the per-node intent-resolver background task; PR6: the multi-tablet Elle serializability corpus + the protocol hardening fixes it found; PR7: atomic Dynamo `TransactWriteItems`, the new `TransactGetItems`, and `/admin/txns` observability — CQL transactional surface, idempotency tokens, `CancellationReasons` fidelity, and manual txn-resolution admin actions deferred) |
| [0019](0019-cp-only-v1-defer-ap.md) | v1 ships the CP plane only; the leaderless AP data plane is deferred | Accepted |
| [0020](0020-admin-interface.md) | Admin / debug interface on a dedicated port (config, status, Raft + storage introspection, operator actions) | Accepted |
| [0021](0021-web-dashboard.md) | Web dashboard over the admin JSON surface (observe + operator actions, static + self-contained) | Accepted |
| [0022](0022-hash-ring-partitioning.md) | Murmur3 partition token: hash the partition key (amends 0002) | Accepted |
| [0023](0023-table-scoped-tablets.md) | Table-scoped tablets on a per-table hash ring | Accepted |
| [0024](0024-drop-table-data-gc.md) | Drop-table data GC: `DropTableTablets` + a per-node reclaim loop (group stop, engine range erase, WAL file deletion) | Accepted |
| [0026](0026-multiplexed-node-stream-addressing.md) | Multiplexed `(node, stream)` addressing on the `Network` seam (retires the `Coresident` sibling-pool liveness cliff) | Accepted |
| [0027](0027-tracing-observability.md) | OpenTelemetry-compatible distributed tracing: `animusd`-only OTLP export, W3C trace-context propagation across a forwarded hop | Accepted |
| [0028](0028-shared-storage-single-command-split.md) | Shared per-node storage, control-plane-only tablet split (supersedes 0017 §4/split) | Accepted |
| [0029](0029-replica-rebalancing.md) | Automatic tablet-replica rebalancing (amends 0005's reconciler, 0017's membership-change primitive) | Accepted |
| [0030](0030-online-cluster-growth.md) | Online cluster growth: admin add-member + heartbeat-driven activation (data-plane only; the control group stays static) | Accepted |
| [0031](0031-tablet-host-reconciler.md) | Per-node tablet-host reconciler + a metadata-applied watch primitive (consolidates 4 polling loops into one event-driven reconciler; amends 0028, 0029) | Accepted — implemented incrementally across PRs 1–6 |
| [0032](0032-seed-join-membership.md) | Seed/join membership: a replicated node address book, `animusd join`, and decommission (closes 0030's `client_route` gap; amends 0024, 0030) | Accepted — implemented (all three PRs: address book, `animusd join`, decommission) |
| [0033](0033-tablet-merge.md) | Tablet merge: an operator-driven, control-plane-only dual of split (amends 0029, extends 0031) | Accepted |
| [0034](0034-byte-based-auto-split.md) | Byte-based auto-split trigger: scoped byte estimate + byte-weighted median (amends 0002, 0028's auto-split loop) | Accepted |
| [0035](0035-control-plane-separate-deployment.md) | Control plane as a separate deployment: `animusd control` / `animusd data` over a `ControlHandle` seam (amends 0030, 0032) | Implemented — all 6 PRs shipped |
| [0036](0036-cluster-allocated-member-ids.md) | Cluster-allocated member ids: a monotonic `MetaCommand::AllocateNodeId` allocator, closing 0032's residual join-index race (amends 0032) | Superseded by [0040](0040-self-minted-string-node-ids.md) |
| [0037](0037-control-plane-membership-change.md) | Control-plane membership change: runtime grow/shrink/replace of control voters via `RaftNode::change_membership` + a new admin API/CLI (amends 0030, 0032) | Accepted — implemented |
| [0038](0038-control-metadata-system-keyspace.md) | Control-plane metadata backed by a per-node system-keyspace storage engine: `Metadata` is `DRIVER_APPLIED`, an async apply task replaces in-core apply (amends 0009, 0013, 0028, 0031, 0035) | Accepted — implemented across six PRs (key encoding, shadow mirror, the cutover, deployment-shape wiring, incremental `WatchMetadata` deltas, the system-table browse surface) |
| [0039](0039-control-metadata-system-tablet.md) | Control-plane metadata as a genuine data-plane tablet (ADR 0038's Option B, revisited): bootstrap circularity, a non-reconciled meta group, migration from the system keyspace, and why the scaling payoff is gated on ADR 0018 (amends 0038; depends on 0018) | Proposed — design-only, not scheduled |
| [0040](0040-self-minted-string-node-ids.md) | Self-minted string node identities + registration-CAS membership: one identity per node (Option B), an opaque validated `NodeId` string, `RegisterNode` CAS superseding ADR 0036's allocator, clean-break config/CLI, and an orphan-member auto-reclaim sweep (amends 0012, 0026, 0030, 0032, 0035, 0037, 0038; supersedes 0036) | Accepted — implemented (PR1–PR6, complete) |
