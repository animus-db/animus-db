# ADR 0074 — Global tables (multi-region): MRSC as a stretch cluster, MREC as async per-item LWW between clusters

- **Status:** Proposed
- **Date:** 2026-10-04
- **Origin:** roadmap G-01, stage G-b ("the ADR"). G-01 reversed the global-tables
  clause of roadmap section 6; this ADR does the design work the other stages
  (G-a topology labels, G-c MRSC, G-d MREC, G-e federation) hang from.
- **Amends:** ADR 0019 (its 2026-08-23 "long shot is closed" premise; a dated
  amendment is added there), and, by short pointer amendments, ADR 0005, 0060
  and 0072.
- **Depends on:** ADR 0005 (placement), 0016/0017 (CP data plane, ReadIndex),
  0018 (2PC/HLC), 0019, 0041-0043 (change log, Streams), 0051 (TTL), 0053 (CQL
  dropped), 0055 (`ConsistentRead: false`), 0059 (backup/PITR change-log
  consumer), 0060 (operator), 0064 (TLS), 0072 (limits), 0073 (upgrade
  compatibility, Phase 2 design).
- **Implementation status:** none. Nothing in this ADR is built. G-c, G-d and
  G-e are **blocked on ADR 0073 Phase 2 P2-B and P2-C** (see section 8).

## 0. Verification note (read first)

The AWS facts below were checked against the AWS documentation **through web
search result extracts only**. Direct page fetches of `docs.aws.amazon.com`
were blocked by the sandbox's egress proxy while this was written, so no page was
read end to end. Each fact carries the URL it was found under. Anything the
extracts did not state is listed under "Not verified" and is treated as an open
question (section 10), never asserted. Before G-c/G-d implementation starts, a
person with docs access should re-read the pages in the table and tick off the
"Not verified" list; the ADR's decisions do not depend on the unverified items
except where flagged.

### Verified (search extracts of the cited pages)

| # | Fact | Source |
|---|---|---|
| V1 | Two versions exist: **2019.11.21 (Current)** and **2017.11.29 (Legacy)**. Legacy is created by making empty regional tables and calling `CreateGlobalTable`; Current is created by `UpdateTable` adding a replica to an existing regional table. Legacy control plane: `CreateGlobalTable`, `DescribeGlobalTable`, `DescribeGlobalTableSettings`, `ListGlobalTables`, `UpdateGlobalTable`, `UpdateGlobalTableSettings`. Current uses `DescribeTable` + `UpdateTable`. A table is a Current replica when `DescribeTable` carries `GlobalTableVersion: "2019.11.21"`. AWS recommends Current. | <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/V2globaltables_versions.html> |
| V2 | `UpdateTable.ReplicaUpdates` is a list of replica update actions (create, update, delete); each can carry region name, KMS key, throughput override (provisioned or on-demand), GSI configuration and table-class override. | <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html> |
| V3 | `MultiRegionConsistency` is `EVENTUAL` (default) or `STRONG`, and "is only valid when you create a global table by specifying one or more Create actions in the ReplicaUpdates action list" - i.e. it is an `UpdateTable` request parameter that fixes the mode at global-table creation. | same (UpdateTable) |
| V4 | `GlobalTableWitnessUpdates` exists on `UpdateTable`: per witness, `Create` or `Delete`. `DescribeTable` returns `GlobalTableWitnesses` (witness region + status) and a `MultiRegionConsistency` value; only one witness region per MRSC table. | UpdateTable page above; <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ReplicaDescription.html> and the `DescribeTable` references |
| V5 | `ReplicaDescription.ReplicaStatus` values: `CREATING`, `CREATION_FAILED`, `UPDATING`, `DELETING`, `ACTIVE`, `REGION_DISABLED`, `INACCESSIBLE_ENCRYPTION_CREDENTIALS`, `ARCHIVING`, `ARCHIVED`, `REPLICATION_NOT_AUTHORIZED`. | <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ReplicaDescription.html> |
| V6 | **MRSC** must be deployed in **exactly three Regions**: three replicas, or two replicas plus one witness. A witness holds data written to the replicas, serves no reads or writes, and lives in a different Region from the two replicas. | <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/V2globaltables_HowItWorks.html> |
| V7 | MRSC semantics: a strongly consistent read on **any** MRSC replica returns the latest version of the item; conditional writes evaluate their condition against the latest version. | same (HowItWorks) |
| V8 | MRSC restrictions: **no TTL**; **no LSIs**; **no transaction APIs** (they error on an MRSC replica); the mode cannot be changed after creation (no MREC<->MRSC conversion); a single-Region table converted to MRSC **must be empty**; additional replicas cannot be added to an existing MRSC table. | HowItWorks page; <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-global-table-design.html> |
| V9 | MRSC Streams: not used for replication, but Streams can be enabled on an MRSC replica; the stream records are identical on every replica and ordered per item (cross-item order may differ between replicas). | HowItWorks page |
| V10 | MRSC concurrency: a write to an item that is concurrently being modified from another Region fails with `ReplicatedWriteConflictException` and may be retried. (The extract did not say whether this is MRSC-only; see Not verified.) | <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/V2globaltables_HowItWorks.html> |
| V11 | MRSC region sets: available in a fixed list of Regions, organized into **Region sets (US, EU, AP)** and "can't span Region sets". Caveat: an AWS announcement dated 2026-09 (<https://aws.amazon.com/about-aws/whats-new/2026/09/dynamodb-mrsc-additional-regions/>) is titled "additional AWS Regions and cross-continent configurations", which appears to relax this; the extract did not state the new rule. | HowItWorks page; the announcement |
| V12 | **MREC**: conflicts resolve **last writer wins, per item**, by the item's private last-write timestamp, implemented as a conditional write requiring the incoming timestamp to be greater than the stored one. A replica may be in any Region where DynamoDB is available (as many replicas as Regions in the partition). | <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/V2globaltables_HowItWorks.html>; <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/globaltables_HowItWorks.html> |
| V13 | MREC replicates by **reading a DynamoDB Stream on a replica and applying to the others**; Streams are therefore enabled by default on every MREC replica and cannot be disabled. (This holds for 2019.11.21.) | HowItWorks page; the `AWS::DynamoDB::GlobalTable` StreamSpecification reference |
| V14 | MREC **transactions** (`TransactWriteItems`/`TransactGetItems`) are atomic **only in the Region where invoked**; the writes are not replicated as a unit, so another replica may briefly show only some of them. | HowItWorks page |
| V15 | MREC **TTL**: supported; settings sync to all replicas; a TTL delete on one replica is **replicated** to the others. TTL deletes are identifiable in Streams (as TTL deletes) **only in the Region where the deletion occurred**; the replicated deletes are not identifiable as TTL deletes in the other Regions' streams. | <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ttl-expired-items.html> and HowItWorks page |

### Not verified (open; see section 10)

- N1: the exact error codes/messages AWS returns for each rejected `UpdateTable`
  global-table request (e.g. creating MRSC with two regions, adding a replica to
  an MRSC table, `STRONG` with a non-empty table). Section 5 uses
  `ValidationException` for those and says so.
- N2: the exact `CreateTable` surface: whether `CreateTable` itself accepts
  `MultiRegionConsistency`/replicas (the extracts only showed `UpdateTable`).
  Decision below: accept it **only** on `UpdateTable`, which is what V1/V3 show.
- N3: the MRSC cross-Region-set rule after the 2026-09 announcement (V11).
- N4: whether `ReplicatedWriteConflictException` is MRSC-only or also an MREC
  occurrence; whether other documented MRSC restrictions exist beyond V8 (the
  extracts did not mention GSIs, PITR, Kinesis as restricted).
- N5: AWS's quota numbers for global tables (replica count caps etc.). V12
  says MREC is bounded by Regions in the partition; no separate numeric quota
  was seen. The catalogue entries in section 6 therefore derive from V6/V12
  rather than a quotas page.
- N6: that the legacy 2017.11.29 operations error specifically on a table that
  is a 2019.11.21 replica, and with which code.
- N7: replication latency, throughput-unit (rWRU) accounting details.

## 1. Context

### 1.1 What ADR 0019 said, and why it needs revisiting

ADR 0019's 2026-08-23 amendment closed the AP "long shot", deleted the
`ReplicationMode` seam and the Accord crate, on this argument: AP is selectable
only through a *per-table* replication mode; a per-table property must be
expressible on a wire the cluster serves; after ADR 0053 dropped CQL the only
wire is DynamoDB, whose `CreateTable` "has no replication-mode field".

That premise is **false as stated**. `UpdateTable` with
`ReplicaUpdates` + `MultiRegionConsistency` (V2, V3) is a wire-level, per-table
replication mode with two values: `EVENTUAL` (async, multi-active, LWW; V12/V13)
and `STRONG` (synchronous, linearizable across Regions; V6/V7). It is
selectable at the point a table becomes global and immutable afterwards (V8).
The amendment's *conclusion* (do not revive leaderless AP as a general
per-table tunable) still stands; what it missed is that DynamoDB's own
multi-Region feature is exactly a per-table mode, with a precise, small
semantics that AnimusDB can implement without the deleted machinery.

### 1.2 What this changes and what it does not

- **Each region stays CP locally.** Inside a cluster, every tablet is a
  leaderful Raft group (ADR 0016/0017); `ConsistentRead: true` is ReadIndex;
  `ConsistentRead: false` is the replica-local read of ADR 0055. Unchanged.
- **MRSC is a CP cross-region mode.** One logical cluster whose tablet Raft
  groups have voters in three regions. It is the existing data plane with a
  topology constraint, not a new replication protocol.
- **MREC is the one AP-shaped place.** Async, multi-active, per-item LWW, between
  *independent* clusters. It is a **new cross-region layer on top of CP
  tablets**: each cluster remains linearizable per tablet; the layer only
  *ships committed change-log records* and applies them as ordinary kind-writes
  with a version comparison. It is **not** a revival of `ReplicationMode`,
  `animus-data`, hinted handoff/read repair, quorum-tunable consistency levels or
  Accord. There is no per-table AP mode inside a cluster; a table is either
  non-global, MRSC, or MREC, and MREC's replicas are whole tables in separate
  clusters.
- ADR 0053 is untouched: there is still no wire that can express a
  per-table AP mode **other than** the two DynamoDB global-table modes.

### 1.3 What the code gives us today (verified by grep on `origin/main` e8c037d)

- `UpdateTable` with `ReplicaUpdates` is rejected by name
  (`crates/animus-dynamo/src/wire.rs`, `UNSUPPORTED_UPDATE_TABLE_KEYS =
  ["SSESpecification", "ReplicaUpdates"]`, message "UpdateTable:
  ReplicaUpdates is not supported"); there are no legacy global-table handlers
  and no `MultiRegionConsistency`/`GlobalTableWitnessUpdates` handling.
- Placement has residency `required_labels` and `SpreadPolicy`
  (`animus-placement`, ADR 0005); `Member.labels` is in replicated `Metadata`
  (`animus-control/src/meta.rs`).
- `HlcTimestamp` exists (`animus-cp-data/src/hlc.rs`) and every row version
  carries one; the change log carries old/new images
  (`animus_item::index::ChangeRecord`) and feeds Streams, GSIs, TTL, backup and
  PITR (ADR 0041-0043, 0059).
- Leadership transfer exists (`RaftNode::transfer_leadership` in
  `animus-control`, `animus-cp-data/src/lib.rs`). **There is no preferred-leader
  or leader-locality mechanism**: a grep of `crates/` for
  `preferred_leader|leader_prefer|leader_affinity` returns nothing; leadership
  lands wherever an election timer fires first.
- **Nothing sets labels or placement policy in production** (roadmap G-01 item
  3): self-registration passes empty labels, table creation installs
  `PlacementPolicy::simple` with no residency/spread, `ClusterConfig` has no
  labels. That is stage G-a, specified separately.

## 2. Decision summary

1. **Two modes, two mechanisms, staged separately.** MRSC (G-c) = stretch cluster
   over the existing CP data plane. MREC (G-d) = per-region clusters plus a
   change-log replication agent with LWW. Federation (G-e) is operator work
   only needed for MREC and for multi-Kubernetes-cluster MRSC.
2. **Support global tables version 2019.11.21 only.** Legacy 2017.11.29
   operations are rejected (section 5.3).
3. **"Region" is a label value**, not a new entity: `topology.kubernetes.io/region`
   on a member (MRSC), and a configured *peer cluster* identified by a region name
   (MREC).
4. **The mode is fixed at global-table creation** (V8) and stored in replicated
   `Metadata`'s table schema.
5. **Everything new is gated** behind named ADR 0073 Phase 2 `Gate`s and rejected
   AWS-faithfully until the cluster is finalized at a version that enables it.
6. **MRSC v1 supports the three-replica form and the two-replicas-plus-witness
   form**, the witness mapped to a vote-bearing replica group member that serves
   no client traffic (section 3.6).
7. **MREC conflict rule: LWW on `(HLC, region id)`** with the origin region id as
   the deterministic tiebreak; CRDTs are rejected (section 9).

## 3. MRSC as a stretch cluster (stage G-c)

### 3.1 Topology

One logical cluster (one control Raft group, one `Metadata`) whose nodes carry a
region label (`topology.kubernetes.io/region`, populated by G-a, section 7).
A MRSC table's tablets are placed so that **each tablet has voters in exactly
the table's three regions**: RF 3, one replica per region, via a
`PlacementPolicy` with `required_labels`/`SpreadPolicy` over the region key
(ADR 0005). Placement is by existing `replan`/`rebalance_step`; `rebalance_step`'s
domain guard (ADR 0029) already preserves a spread constraint.

The **control plane** needs a quorum that survives the loss of one region:
control voters in at least three regions (one per region, 3 or 5 voters;
never a majority in one region). The existing runtime `change_membership`
(ADR 0037) places them; a region-aware control-voter placement check is added
to the admin path and `animusd gen-config`.

### 3.2 Cost, quantified (design constraint; to be measured by B-01)

Let `RTT(a,b)` be inter-region round trip, three regions A,B,C, leader in A.
Raft needs a majority (2 of 3) to commit.

- **Write latency** = local fsync + `min(RTT(A,B), RTT(A,C))` - the nearest-peer
  round trip, plus the follower's fsync. Same-continent neighbours: tens of ms;
  transatlantic 70-100 ms (order of magnitude, from public inter-region
  latencies, **not measured here**). A write issued in a region whose leader is
  elsewhere adds that region-to-leader RTT (forwarding, ADR 0017).
- **`ConsistentRead: true`** (ReadIndex, ADR 0017): the leader must confirm
  leadership with a majority = the same nearest-peer RTT; add the client
  region-to-leader RTT if forwarded. Lease reads would remove the round trip but
  are **not** in the tree and are not proposed here (clock-assumption change; see
  open question Q3).
- **`ConsistentRead: false`** (ADR 0055): replica-local, no WAN hop. V7 says
  AWS MRSC lets a *strongly consistent* read on any replica return the latest
  item; AnimusDB satisfies that with ReadIndex through the leader, which costs
  a WAN hop when the client's region does not hold the leader. This is a
  latency, not a correctness, difference from AWS.
- **Availability:** survives loss of any one region (2 of 3 voters remain);
  a partition isolating one region makes that region read-only for
  `ConsistentRead: false` and unable to commit; the two-region side keeps going.
- **Throughput**: per-tablet Raft is already pipelined; WAN RTT bounds
  per-item serial latency, not aggregate throughput, if batching holds.
  Whether ADR 0044's cross-group heartbeat batching and the shared WAL group
  commit keep their wins over WAN is for B-01's cross-region variant to show.

### 3.3 Leadership placement (new mechanism)

Because writes pay the nearest-majority RTT *from the leader*, and clients are
spread over regions, leader location is the dominant latency lever and there is
no mechanism today (1.3). Decision:

- **Preferred region per tablet**, stored in the table's replicated placement
  policy (`preferred_leader_region: Option<String>`, additive, gated, section 8);
  default: the table's first-listed region, per `ReplicaUpdates` order.
- **Enforcement by transfer, not by election bias.** A per-node,
  event-driven reconciler (same family as the tablet-host reconciler, ADR 0031)
  on a group's current leader, when the leader is outside the preferred region
  and a caught-up replica inside it exists, calls the **existing**
  `transfer_leadership` (hysteresis: only after the preferred replica has been
  caught up and stable for `T`, and rate-limited per group to avoid flapping
  after a partition heals). A failed region simply loses preference until it
  returns; no manual action.
- Election timeouts are **not** biased (biasing randomised timeouts is fragile and
  weakens liveness proofs, ADR 0016); the transfer path is already
  sim-tested (`leadership_transfer.rs`).

### 3.4 WAN-tuned timeouts

Today election/heartbeat timeouts are a `RaftCore` default
(`animus-control/src/raft.rs`), one value for LAN. A WAN group needs election
timeout >> max inter-region RTT + fsync jitter (rule of thumb: >= 10x one-way
RTT, floor at the LAN default) and heartbeat ~ election/10. Decision: a
**per-group timeout profile** (`lan` | `wan`, with the WAN values derived from a
cluster-level `max_region_rtt` setting, default 150 ms) chosen when a group's
replica set spans more than one region label, applied through the same setter
path that sets `heartbeat_interval` today. The control group, spanning regions,
uses the WAN profile as well. Profile is a node-local function of replicated
`Metadata` (replica regions), so it needs no new wire field. Under `SimEnv` a
region-level latency model (section 4.8's WAN `Network` extension) exercises it.

### 3.5 `ReplicaUpdates` -> replica changes

- `Create{RegionName}` on a table: record the region in the table's replicated
  global-table spec (state `Creating`), set the placement policy's region
  requirement, and let `reconfigure_step`/`CasTabletReplicas` add a learner then
  voter in that region for every tablet of the table (the existing learner
  promotion path, ADR 0058/0062 directed Placing). `ReplicaStatus` is `CREATING`
  until every tablet has a promoted voter there, then `ACTIVE`.
- `Delete{RegionName}`: reverse; `DELETING` until the last voter in that region is
  removed.
- Because V8 says a MRSC table has exactly three regions fixed at creation and
  AWS forbids adding replicas, **a MRSC table is created with all three
  regions in one `UpdateTable`** (replicas+witness together), requires an empty
  table, and later `Create`/`Delete` on it is rejected. This also means no
  data movement across regions is ever needed for MRSC creation, only an empty
  table's tablets placed three-way.
- Region failure while `Creating`: `CREATION_FAILED` (V5) and the table stays
  non-global.

### 3.6 Witness

V6: two replicas plus one witness, the witness serving no client traffic.
AnimusDB has learners (non-voting) but no log-only voter. Decision for v1: **a
witness region hosts a full voting replica group member** for each tablet and
for control purposes, **hidden from the data path**: it is never a read target
(ADR 0055 selection skips it), never forwarded to, never a preferred leader
region; `DescribeTable` shows it under `GlobalTableWitnesses`, not `Replicas`.
This over-stores relative to AWS's witness (full copy, not log-only) but needs
no new Raft role and keeps the Raft proofs intact. A log-only witness (votes and
stores the log but no applied state machine) is a later optimization (Q4).

### 3.7 Feature restrictions on MRSC tables (mirror V8/V9)

Rejected on an MRSC table with `ValidationException` (N1): TTL
(`UpdateTimeToLive` enabling), LSIs (at table creation, so a table with LSIs
cannot become MRSC), `TransactWriteItems`/`TransactGetItems`/`ExecuteTransaction`
(AWS errors on MRSC replicas; AnimusDB's 2PC would in any case be
cross-region-latency heavy), converting mode, adding replicas. Streams may be
enabled and are identical per replica by construction (one Raft log, V9).
GSIs/PITR/backup: not documented as restricted (N4); allowed, GSI rows replicate
as ordinary kind-writes in the same Raft entry (ADR 0049). `ConsistentRead`
and conditional writes are linearizable by construction (V7).
`ReplicatedWriteConflictException` is **not** needed for MRSC: one leader
serializes writes per item, so there is no cross-region write conflict to
report. We never emit it for MRSC (N4: if AWS documents a case where
clients must see it for MRSC, revisit).

### 3.8 One `AnimusCluster` over a mesh, or federation? (decision)

**A stretch cluster is one `AnimusCluster` only if the Kubernetes cluster
itself spans regions** (one control plane over a flat pod network, e.g. a
single multi-region cluster or a cluster-mesh with flat pod IPs and stable
`StatefulSet` DNS). The operator (ADR 0060) renders one `StatefulSet` and one
headless Service per `AnimusCluster`; it cannot create pods in another
Kubernetes cluster. Therefore: **the operator-supported MRSC shape in G-c is
"one `AnimusCluster` on a Kubernetes cluster that spans the regions"**, with
the operator's topology spread (G-a) doing the region/zone spread. A deployment
of one Kubernetes cluster per region needs G-e's federation for MRSC as well,
because the control plane's voters and the replicas must still form *one*
`Metadata`. G-e is therefore designed once (section 5.4) to serve both: a
cluster that is a *segment* of a stretch cluster (members join a remote seed
set, ADR 0032) and a cluster that is a *peer* of an MREC set.

## 4. MREC as async replication with LWW (stage G-d)

### 4.1 Shape

Each region is an independent, fully CP animus cluster holding a complete copy of
the table. A table is global (MREC) when its replicated schema carries a
`GlobalTableSpec { consistency: Eventual, replicas: [{region, status}], origin_ids }`.
Per node, per tablet leader, a **replication agent** consumes that tablet's
change log and ships to every peer region, once per peer.

### 4.2 Agent: same consumer shape as the backup/PITR sealer

ADR 0059's PITR sealer is a leader-side change-log consumer with a durable
cursor, at-least-once, resumable. The agent reuses that shape and the same
leader gating: a durable per-`(tablet, peer region)` cursor in replicated state,
batches of `ChangeRecord`s from the cursor, ship, advance the cursor **only
after the peer acks the batch applied**; leader loss resumes from the committed
cursor; the log is not truncated past the slowest consumer (the change-log
retention accounting that already carries the Streams/backup consumers gains
one consumer per peer, with a backlog cap and an alarm; a peer lagging past the
cap forces a **resync** from a snapshot scan, never silent loss).
A fifth/sixth change-log consumer is the cheapest path because GSI maintenance,
Streams and TTL are already derived from the same records (ADR 0049).

### 4.3 Transport and security

Records ship as a new, versioned request over the intra/internal wire (not the
public DynamoDB port, because a replicated write carries origin metadata a
client must not be able to forge) between the *peer region's* data nodes, with
ADR 0064 mutual TLS (peer CA trusted per region pair). Peer endpoints are a
cluster-level config set (a list of `{region, seed addresses}`) discovered via
the operator in G-e. The apply side routes each record to the destination
tablet leader (hinted retry/forwarding as for any client write) by the same
hash-ring token (ADR 0022; both clusters must use the same table key schema,
checked at replica creation).

### 4.4 Conflict resolution: LWW on `(HLC, region id)`

Every replicated write has a version `(hlc, region_id)` where `region_id` is a
small integer assigned to the region at `Create` and never reused. The apply
side compares against the stored item's version: greater wins, equal is a
duplicate (idempotent no-op). Total order = lexicographic `(hlc, region_id)`,
so every replica converges to the same winner regardless of delivery order,
duplication or timing - that is the convergence oracle's definition.
Deletes are replicated as tombstones carrying the same version (kept for a
tombstone retention at least as long as the longest tolerated peer outage; a peer
down past it needs a resync, 4.2). This matches V12 (latest timestamp wins per
item) with an explicit deterministic tiebreak; the extracts did not say how AWS breaks exact timestamp ties (unverified).

**HLC skew bounds fairness, not safety.** LWW picks the winner by timestamp, so a
region whose HLC runs ahead wins ties of "concurrent" writes unfairly; it cannot
break convergence. Mitigations: HLC is already a hybrid logical clock that
ratchets forward on every received timestamp (so skew *propagates* rather than
silently diverging); the apply path **rejects and alarms** a remote HLC more
than `max_clock_skew` (default 500 ms, configurable, wall-clock-derived via
`env.wall_now`, ADR 0051) ahead of local wall time rather than adopting it, and
the replication lag/skew metric is exposed. Documented limitation: within the
skew bound, which of two near-simultaneous cross-region writes wins is arbitrary
but identical everywhere.

### 4.5 No re-replication

Each record carries `origin_region_id`. A record applied by the replication
apply path is written with a flag so the change-log entry it produces is **not**
re-shipped to the origin or to any other peer *unless* the topology is not full
mesh (v1: full mesh only; every region ships its own originated writes to every
peer, so forwarding is never needed). The agent ships only records whose origin
is the local region.

### 4.6 TTL

Per V15, TTL is supported and the delete is replicated. Decision: the reaper
runs in every region (settings synced from the replicated `TtlSpec`) and the
resulting deletes are ordinary local deletes that are shipped. To avoid
N regions all deleting and shipping the same item, an expiry delete carries the
item's own version-derived tombstone `(hlc_of_expiry, region_id)`; duplicates
are idempotent under 4.4. Stream parity: the TTL delete is flagged as TTL in the
*originating* region's stream only; in other regions the replicated delete is an
ordinary REMOVE without the TTL `userIdentity` (V15, exactly as AWS).

### 4.7 Streams parity

V13: MREC needs Streams on every replica, and replication reads the stream.
AnimusDB's agent reads the change log directly rather than the stream (same
records, no extra decode), but **Streams must be enabled with `NEW_AND_OLD_IMAGES`
on every replica when the table becomes MREC**, because the DynamoDB-wire
contract is that Streams are on and cannot be disabled (V13). `UpdateTable` of
`StreamSpecification` to disable on a global replica is rejected. Replicated
writes appear in the destination region's stream like any write (a replica
change-log record flowing through the same ADR 0042/0043 path); lineage
(shard/sequence) is region-local.

### 4.8 Transactions

V14: atomic only in the issuing region. A `TransactWriteItems` commits per ADR
0018 locally; each constituent item write is shipped independently with its own
version (not as a unit), so a remote region can transiently see a subset.
`TransactGetItems` is region-local. This is documented behavior, not a bug; the
corpus asserts convergence of items, not atomic visibility across regions.

### 4.9 Simulation corpus (required deliverable of G-d)

A multi-cluster `SimCluster`: N independent clusters on one deterministic
`SimEnv` with a **WAN `Network` model** (per-region-pair latency, jitter, loss,
partition/heal events, duplicate and reorder delivery), seeded. Cells: steady
two/three regions; asymmetric partition and heal; region crash and rejoin
(including a rejoin past retention, forcing resync); concurrent conflicting
writes to the same item in different regions; delete/put races; TTL expiry in
two regions; HLC skew injection (+/- bounded and beyond-bound); leader churn
mid-batch (cursor resume). Oracles: **convergence** (after heal and quiescence
all regions hold identical item versions), **LWW determinism** (the winner is
the max `(hlc, region_id)`, independent of delivery order, checked by running a
seed twice with permuted delivery), **no loss of a surviving acked write**,
**no re-replication** (shipped-record count bound), and stream-parity
assertions. `ANIMUS_MREC_SEEDS=K` joins the knob table; nightly depth in
`corpus-deep.yml`.

## 5. Wire surface and federation

### 5.1 Accepted operations and fields (2019.11.21 only)

| Operation / field | Behaviour |
|---|---|
| `UpdateTable.ReplicaUpdates[].Create{RegionName}` | Accepted when the gate is open (section 8). `RegionName` must be a configured region (a region label value present on members for MRSC; a configured peer for MREC). Optional per-replica fields: `KMSMasterKeyId`, `ProvisionedThroughputOverride`, `GlobalSecondaryIndexes` overrides, `TableClassOverride` are **rejected** in v1 with `ValidationException` naming the field (AnimusDB has no KMS/table classes, ADR 0072/0069 scope; throughput overrides need per-region capacity accounting that does not exist), to be revisited. |
| `ReplicaUpdates[].Delete{RegionName}` | MREC: accepted (the replica leaves the set; the peer cluster's table is *not* touched, it becomes a standalone table, as in AWS). MRSC: rejected (V8). |
| `ReplicaUpdates[].Update` | Rejected in v1 (no overridable per-replica setting is supported). |
| `UpdateTable.MultiRegionConsistency` (`EVENTUAL`/`STRONG`) | Accepted only together with at least one `Create` (V3). Default `EVENTUAL`. `STRONG` requires: exactly three regions (V6) = this region + two more, or this region + one more + one witness; an **empty** table (V8); no TTL, LSIs (V8). |
| `UpdateTable.GlobalTableWitnessUpdates` | `Create` only, only with `STRONG`, at most one (V4); otherwise rejected. |
| `DescribeTable` | Adds `GlobalTableVersion: "2019.11.21"`, `Replicas[]` (`RegionName`, `ReplicaStatus` from the V5 set; v1 emits `CREATING`, `CREATION_FAILED`, `UPDATING`, `DELETING`, `ACTIVE`), `MultiRegionConsistency`, and `GlobalTableWitnesses[]` (MRSC). The local region is *included* in `Replicas` as AWS does (N1: unverified, matches common SDK output). |
| `CreateTable` | **Does not accept** replica/consistency fields (N2); global-ness begins with `UpdateTable`. |
| `ExecuteTransaction`, `Transact*` | Rejected on MRSC (V8); region-local on MREC (V14). |

### 5.2 Mapping of "region"

MRSC: a `topology.kubernetes.io/region` label value on members of **this** cluster.
`RegionName` in a request must equal such a value; the *local* region is the
receiving node's label. MREC: a named peer in the cluster's peer list (G-e),
whose name is the `RegionName`; a node's own region name comes from the same
label (or a cluster-level `region` setting when the cluster is single-region).
AnimusDB does not validate names against AWS's region list (it is not AWS);
names are operator-defined.

### 5.3 Legacy 2017.11.29 operations

`CreateGlobalTable`, `UpdateGlobalTable`, `DescribeGlobalTable`,
`DescribeGlobalTableSettings`, `ListGlobalTables`, `UpdateGlobalTableSettings`
(V1): **rejected** with a `ValidationException` whose message says the
operation is the legacy global-tables version and that version 2019.11.21
(`UpdateTable` with `ReplicaUpdates`) is the supported one. (N6: AWS's exact
error for legacy operations on a Current table is unverified; for an
*unsupported operation* the existing dispatcher's behaviour for unknown targets
applies.) Reason: the legacy API is a different control plane, strictly worse
(V1: more rWRU, fewer Regions, empty-tables-first creation) and AWS itself
recommends Current. Neither `UpdateTable` nor legacy ops are supported before the
gate opens; until then `ReplicaUpdates` keeps today's
"ReplicaUpdates is not supported" rejection.

### 5.4 Operator federation (stage G-e)

- **MREC peer set:** one `AnimusCluster` per Kubernetes cluster/region; a new
  field `spec.region` (this cluster's region name) and `spec.peers[]`
  (`{region, endpoints (host:port list), tlsSecretRef/CA}`), additive and
  `schemaVersion`-compatible (ADR 0060's golden-fixture rule; ADR 0073 operator CRD
  is Phase 3-scoped, so it follows the CRD's own `schemaVersion` discipline).
  No separate federating CRD in v1: peers are symmetric, each side lists the
  others. A separate `AnimusFederation` resource is rejected for v1 because
  there is no cross-cluster state for it to own.
- **Endpoint discovery:** explicit peer endpoint lists (LoadBalancer/
  multi-cluster-service DNS names supplied by the user), not automatic; the
  operator renders them into `ClusterConfig` (a `peers` section) and a
  `NetworkPolicy` egress rule for the peer ports. Only the intra/replication
  port is exposed to peers (ADR 0047's split), never the client port.
- **Peer TLS trust:** ADR 0064 mutual TLS; each cluster's CA bundle includes the
  peer clusters' CAs (referenced Secrets or cert-manager issuers the operator
  only references, as in 0064).
- **Ordered replica-add:** the operator does not drive replica creation (the
  database does, via `ReplicaUpdates`); it only ensures peers are reachable and
  trusted, and surfaces peer-health as `AnimusCluster` status conditions
  (`PeerReachable`).
- **Stretch segment (G-c over several Kubernetes clusters):** the same config
  lists remote *seed addresses* a node joins (ADR 0032) instead of peers; node
  labels (region) come from G-a; one `Metadata`. Deferred until MREC federation
  works because it needs flat cross-cluster pod networking, the hard part
  (see 3.8).

## 6. ADR 0072 limits catalogue entries

To add to `animus_dynamo::limits` when G-c/G-d land (compiled-in, AWS-faithful,
no knob), each citing the doc it came from:

| Constant | Value | Basis |
|---|---|---|
| `MRSC_REQUIRED_REGIONS` | 3 | V6: exactly three Regions (replicas + witness count). |
| `MRSC_MAX_WITNESSES` | 1 | V4/V6: one witness, in a Region different from the two replicas. |
| `MRSC_MIN_FULL_REPLICAS` | 2 | V6: three replicas or two plus a witness. |
| `GLOBAL_TABLE_VERSION` | `"2019.11.21"` | V1. |
| `MREC_MAX_REPLICAS` | bounded by configured peers (no separate AWS numeric limit found, N5); catalogue entry records "no numeric cap" and the implementation caps at the configured peer count (and 16 as a defensive compiled-in ceiling, an AnimusDB resource choice, not an AWS claim) | V12, N5. |

MRSC region-set restriction (V11) is **not** adopted: AnimusDB regions are
operator-defined, not AWS's US/EU/AP sets.

## 7. Topology labels (G-a: the foundation)

All of 3.1 and 4.x presuppose nodes that carry real region/zone labels, which
nothing populates today (1.3). Stage **G-a** (roadmap G-01, a separate PR by
another agent, **not specified here**) is the foundation: node labels via
`ClusterConfig` and `--label`/`--labels-file`, the operator resolving a pod's
Node topology labels onto pod annotations projected through the downward API,
`topologySpreadConstraints`/anti-affinity on the `StatefulSet`, and table creation
using a `SpreadPolicy` over the zone key when the cluster has >= RF zones. This
ADR consumes its output: the `topology.kubernetes.io/region` label on
`Member.labels` is the definition of "region" for MRSC, and the zone key remains
the intra-region spread key. If G-a changes the label key names, this ADR's
references follow G-a.

## 8. Gating (ADR 0073 Phase 2)

Every new replicated or wire-visible surface is behind a named `Gate` in the
Phase 2 cluster-version mechanism (`animus_control::version`, `Gate`,
`ClusterFeatures`), each assigned the cluster version in which it first ships:

| Gate | Guards |
|---|---|
| `Gate::GlobalTablesSpec` | the `GlobalTableSpec` field on the table schema in `Metadata` and the `MetaCommand`s that create/advance/remove replicas and witnesses |
| `Gate::PreferredLeader` | `preferred_leader_region` in the placement policy |
| `Gate::MrecReplication` | the replication-agent request/response frames on the intra wire and the per-peer cursor entity |
| `Gate::GlobalTablesWire` | acceptance of `ReplicaUpdates`/`MultiRegionConsistency`/`GlobalTableWitnessUpdates`, and the new `DescribeTable` fields |

(Names are provisional until P2-B's registry exists.) Until the cluster has been
**finalized at a version that enables the gate**, the request is rejected exactly
as today: `ReplicaUpdates` returns the existing `ValidationException`
"ReplicaUpdates is not supported", and `DescribeTable` omits the new fields.
Decoders accept every version always (ADR 0073 decision 4: apply never branches on
a gate); only *emission* and *acceptance of new requests* are gated. New
persisted shapes follow ADR 0073's "gated additive field" rule or its Phase 1
checklist (new golden fixture, never an edit to an existing one).

**Blocking, stated plainly:** G-c, G-d and G-e implementation is **blocked on
ADR 0073 P2-B (gate enforcement: `required_gate` tables, send-site asserts,
`is_relayable_command`) and P2-C (node wiring: `ClusterFeatures` fed into every
emitter, boot self-report, the Finalize admin path)**. The mixed-version corpus
(P2-D) is needed before any of these gates can be proven safe to open in a
rolling-upgrade world. P2-A has merged (version core, bootstrap); P2-B/C/D have
not. G-a and this ADR are not blocked. Writing Metadata changes ungated in the
interim is **not** permitted.

## 9. Alternatives considered

- **MREC only.** Cheaper to operate (no WAN in the write path) and what most
  users mean by "global tables", but gives no strongly-consistent
  multi-region mode and is the XL stage. Rejected as the *only* mode: MRSC is
  the cheaper stage (L, reuses CP) and ships first.
- **MRSC only.** Delivers the CP story with no new consistency model, but leaves
  the dominant AWS use (active-active eventual with local writes) unserved and
  cannot give single-digit-ms local writes across regions. Rejected as the
  end state; kept as the first stage.
- **CRDTs instead of LWW.** Per-attribute/mergeable types would lose fewer
  concurrent updates, but AWS's contract is LWW per item (V12) and a client
  cannot observe merge semantics a DynamoDB item does not have; CRDTs would also
  force item-model changes (ADR 0054). Rejected: wire fidelity wins.
- **Revive Accord/leaderless AP** (ADR 0001/0011). Rejected: it solves a
  different problem (intra-cluster leaderless quorum), was deleted for good
  reasons (ADR 0019 amendment), and neither global-tables mode needs it.
- **Cross-region Raft learners (one-way async via learners)** instead of an agent:
  a learner follows a *single* group's log, so it only gives one-directional
  DR, not multi-active. Considered as a future DR-only mode, not global
  tables.
- **A separate federating CRD** (see 5.4). Deferred.
- **Support legacy 2017.11.29.** Rejected (5.3).
- **Lease reads for cheaper MRSC strong reads.** Out of scope (Q3).

## 10. Consequences and open questions

### Consequences

- AnimusDB gains two wire-visible multi-region modes with AWS-faithful
  semantics; ADR 0019's AP closure is narrowed, not reversed.
- A table has a one-way mode (non-global -> MRSC or MREC) with the restrictions
  of V8/V14/V15 enforced on the wire.
- MRSC makes write latency a function of inter-region RTT and leader location;
  the preferred-leader transfer mechanism is new code with its own corpus
  (region partition/heal, leader flapping).
- MREC introduces the project's first intentionally non-linearizable
  cross-cluster behaviour; its correctness story is the WAN corpus and the
  `(hlc, region_id)` total order, not Raft.
- The change log gains per-peer consumers, with backlog/retention policy
  implications; resync-by-scan is a required feature, not an option.
- The website keeps saying "Planned" (it is not implemented); it now may point
  at this ADR.

### Open questions

- Q1: Re-verify V1-V15 and fill N1-N7 against full AWS pages (section 0).
- Q2: Peer discovery beyond explicit lists (DNS SRV, multi-cluster services).
- Q3: Lease-based ReadIndex avoidance for MRSC strongly consistent reads: a
  clock-assumption change needing its own ADR and Jepsen-style corpus cells.
- Q4: Log-only MRSC witness to cut storage (3.6).
- Q5: MREC `max_clock_skew` default and whether to refuse (alarm only vs reject)
  remote HLCs beyond it.
- Q6: Per-replica overrides (`ProvisionedThroughputOverride`,
  `GlobalSecondaryIndexes`, `TableClassOverride`, KMS) and per-region
  capacity accounting.
- Q7: Adding a region to an existing MREC table with data: initial full copy
  (snapshot scan + tail) uses the resync path of 4.2; its interaction with PITR
  and backup catalogs needs a follow-up section when G-d starts.
- Q8: How MRSC and PITR/backup interact (a backup of a stretch table is taken
  from the leader region; the restore target must be non-global).
