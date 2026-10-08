# ADR 0075 — Global tables (multi-region): MRSC as a stretch cluster, MREC as async per-item LWW between clusters

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
- **Implementation status:** **stage G-c (MRSC, the stretch cluster) is built**
  behind `Gate::GlobalTables` (cluster version 2); see the 2026-10-05
  amendments at the end ("the MRSC wire surface as built" and "G-c as built").
  **Stage G-d (MREC, async per-item LWW between clusters) is built** behind
  `Gate::MrecReplication` (cluster version 3), simulation-proven plus one
  real-process two-cluster test over mutual TLS; see the 2026-10-06 amendments at
  the end ("G-d M1/M2/M3 as built" and "G-d as built (M0-M6)"). **Stage G-e (operator
  federation for MREC peers) is built**, unit-tested but with its `kind` e2e
  unrun; see the 2026-10-06 amendment "G-e as built" at the end. Stretch-segment
  federation (MRSC over several Kubernetes clusters) remains deferred. ADR 0073
  Phase 2 (P2-B/C/D) is done, so the "blocked on P2" note in section 8 is historical.

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

## Amendment (2026-10-04): section 3.4 and the control-voter check as built (G-c groundwork)

Branch `g01-c-wan-groundwork`. This is the **ungated** groundwork of stage G-c:
everything here is node-local behaviour derived from the existing
`Member.labels`, plus additive config. Per section 8 it adds **no replicated
`Metadata` field, no `MetaCommand`/`KvCommand` variant and no wire or durable
format change**, so it needs no cluster-version gate and is not blocked on
P2-B/P2-C. What stays gated is listed at the end.

### Timing profile (section 3.4)

- **Formula** (`animus_control::timing::TimingProfile::durations`, pure,
  saturating): `Lan = (150 ms election base, 50 ms heartbeat)` (the historical
  constants); `Wan{max_region_rtt} = (election, heartbeat)` with
  `election = max(150 ms, 5 x max_region_rtt)` and
  `heartbeat = max(50 ms, election / 10)`. At the 150 ms default this is
  750 ms / 75 ms. The design text above says ">= 10x one-way RTT"; `5 x` the
  *round trip* is the same quantity (one RTT is two one-way trips) with the
  randomised `[base, 2 x base)` window supplying the rest of the margin.
- **A group is WAN iff its replicas (voters and learners) carry more than one
  distinct `topology.kubernetes.io/region` label value** (`REGION_LABEL`,
  defined locally with a note that it must equal G-a's constant). An
  unlabelled or single-region cluster is always LAN: **no behaviour change**.
- **Setter:** `RaftCore::set_timing(election_base, heartbeat_interval, now,
  entropy)` (the deleted `set_election_timeout` of issue #313 came back only
  with a caller). It refuses zero, is a no-op when unchanged, and on a change
  re-arms a follower's election deadline from the new base or pulls a leader's
  heartbeat deadline in (never pushes one out). `RaftNode::set_timing_profile`
  / `RaftKvNode::set_timing_profile` compare first, so the idempotent
  no-change path **draws no entropy** (extra RNG draws desync fixed seeds), and
  `RaftKvNode` wakes its driver on a real change so `next_deadline` is
  re-read.
- **Wiring:** the cp-data tablet-host reconciler (`host::Reconciler`) derives
  the profile from `MetadataView::regions` (the member id -> region projection
  `animusd` builds from `Metadata.members`) for every hosted tablet, on `host`,
  on `materialize_split_child`, and re-applies it every tick (a label or
  replica-set change converges). The control group runs an opt-in spawned
  loop, `RaftNode::enable_region_timing`, woken by `metadata_watch` with a 5 s
  fallback. Nothing in `RaftNode::start*` changed, so no existing seeded
  timeline moved.
- **Derived deadlines** all read the installed pair: `transfer_leadership`'s
  one-election-timeout budget, the next cluster-check resend, the
  departing-peer backoff gap, snapshot-resend backoff (heartbeat ticks),
  `election_timeout()` and so animusd's `3 x election_timeout()` health grace.
  The **ADR 0044 heartbeat batcher** cadence is unchanged: its 50 ms tick is
  `<=` every profile's heartbeat, so batching adds at most one 50 ms tick to a
  heartbeat's latency, which is small against the WAN election base (at the
  default, 75 ms + 50 ms against 750 ms).
- **Config:** additive `cluster_settings.max_region_rtt_ms` (default 150,
  `skip_serializing_if` unset, so the frozen cluster-config v1 fixture is
  untouched) and `--max-region-rtt-ms` (needs `--config`). Plumbed via
  `Bound*Node::with_max_region_rtt`.

### Region-aware control-voter check (sections 3.1/3.4)

`animus_control::timing::control_voter_change_check` runs in
`admin_add_control_member` (before any registration side effect, over the
candidate's supplied labels) and `admin_remove_control_member` (`--force`
bypasses it, like the liveness guard). It refuses a voter set that puts a
strict majority of control voters in one region **when the cluster's members
carry labels from more than one region** and the change does not strictly
reduce that region's share relative to the current voters. The second clause
is deliberate: a one-region bootstrap must stay growable voter by voter, so a
step that dilutes an existing concentration is allowed; one that adds to it is
refused.

**Known gaps, stated plainly:**

- Control-only nodes have no `Member` row (`RegisterNode` with role `control`
  never claims `members`), so their labels are not in `Metadata`. Only
  combined-role voters contribute to the control profile and to this check;
  control-only voters count as unlabelled (they dilute, never create, a
  majority). A config-borne label source (G-a's `RoleAddrs.labels`) closes
  this when G-a lands.
- `animusd gen-config` cannot warn about a region-concentrated control set,
  because it cannot know labels yet (they arrive with G-a). The warning is
  added with G-a.
- `ControlHandle::Remote::election_timeout` still reports the hard-coded
  150 ms (a data-only node has no local control `RaftCore`); only the health
  grace reads it, and a too-tight grace is a spurious "no leader" report, not
  a safety issue.

### Evidence

`wan_timing_corpus` (`ANIMUS_WAN_TIMING_SEEDS`): three regions, 60/75/90 ms
one-way, a noisy tail. Under the WAN profile steady state shows zero term
growth and the leader-node kill and leader-region partition+heal cells grow
the term by 1 (at most 2 over 100 seeds) with writes committing and every
acked write durable on all replicas. The LAN-forced negative control on the
same links shows term growth up to 30 and writes stalling on the fault cells.
**Measured finding worth keeping:** with only a few ms of jitter the LAN
profile survives 60-90 ms links too (pre-vote and its lease absorb late
heartbeats, and pipelined heartbeats keep arriving every 50 ms), so the
profile's value is in *re-election over a noisy WAN*, which is what the
fault cells exercise.

### Still gated (not done here)

Preferred-leader placement (3.3), `ReplicaUpdates` mapping (3.5), the MRSC
table mode and its wire surface, and any replicated stretch-cluster state:
blocked on ADR 0073 Phase 2 P2-B/P2-C per section 8.

## Amendment (2026-10-05): the MRSC wire surface as built (G-c, M3)

`UpdateTable` with `ReplicaUpdates` routes to a new typed
`Operation::UpdateTableGlobal` (a separate variant, so no existing
`UpdateTable` literal changed). The decoder is pure and never rejects on the
gate: `animusd` (`global_tables::update_table_global`) checks
`Gate::GlobalTables` first and, while it is closed, returns the pre-G-c text
byte for byte (`UpdateTable: ReplicaUpdates is not supported`; `...:
GlobalTableWitnessUpdates is not supported` / `MultiRegionConsistency is not
supported` for the other two keys). Then, in order: the table exists; the
request shape (`animus_dynamo::global`); not already global; the receiving
node carries a `REGION_LABEL` (D11) and is the table's own Region (the
preferred-leader Region, D3); every named Region is carried by an `Active`
member; the table is `ACTIVE`, has no TTL and no LSI; the table is empty (a
quorum scan, D6: AWS fidelity, not safety). It proposes one
`ConvertTableToGlobal` and waits for `schema.global` to show.

Wire shapes: request `ReplicaUpdates[].Create.RegionName`,
`MultiRegionConsistency` (`STRONG`), `GlobalTableWitnessUpdates[].Create.
RegionName`; response `TableDescription` (and `DescribeTable`'s `Table`)
gains, for a global table only, `GlobalTableVersion` ("2019.11.21"),
`MultiRegionConsistency`, `Replicas[{RegionName, ReplicaStatus}]` (the local
Region included, status derived per D4) and `GlobalTableWitnesses[{RegionName,
WitnessStatus}]`. A regional table's output is byte-identical to before.
Rejected by name, all `ValidationException`: absent/`EVENTUAL` consistency
(stage G-d), a Region count other than three, a repeated or unknown Region,
the own Region named, `Update`/`Delete` actions and per-replica overrides
(`KMSMasterKeyId`, `ProvisionedThroughputOverride`, `OnDemandThroughputOverride`,
`GlobalSecondaryIndexes`, `TableClassOverride`), a witness without a replica
Create, more than one witness, a second change in the same call, a non-empty
table, TTL or LSI present, an already-global table; on a global table
`UpdateTimeToLive` enabling TTL, `TransactWriteItems` and `TransactGetItems`
(hence `ExecuteTransaction`). The six legacy 2017.11.29 operations are
rejected by name, ungated. The `limits` catalogue gained `MRSC_REQUIRED_REGIONS`,
`MRSC_MAX_WITNESSES`, `MRSC_MIN_FULL_REPLICAS` and `GLOBAL_TABLE_VERSION`.

**Unverified against AWS:** docs.aws.amazon.com was unreachable from the
build environment, so the field names above (`ReplicaUpdates`,
`GlobalTableWitnessUpdates`, `MultiRegionConsistency`, `GlobalTableVersion`,
`GlobalTableWitnesses`, `ReplicaStatus`/`WitnessStatus`) come from the plan's
earlier search extracts (section 0), and every error *text* is AnimusDB's own
(N1). Re-read the API reference for `UpdateTable`/`ReplicationGroupUpdate`/
`TableDescription` before G-c ships.

(The M4 items listed here as open were completed; see the next amendment.)

## Amendment (2026-10-05): G-c as built (MRSC stretch cluster)

Stage G-c is implemented in one workstream. What shipped and the decisions the
sections above left open:

- **One gate, `Gate::GlobalTables` (cluster version 2).** Section 8's four
  provisional names collapse into one because everything ships in one release:
  `MetaCommand::ConvertTableToGlobal` and `SetGlobalPreferredLeader`
  (`required_gate` rows, relay allowlist, mirror, golden fixtures), the
  `TableSchema.global` field, the placement-policy region pin, and the client
  acceptance of `ReplicaUpdates`. `MrecReplication` stays for G-d. This is the
  first real gate, so it is also the first bump: `MAX_SUPPORTED = 2`, and the B2
  sim profile is pinned to the literal `[1, 1]` (ADR 0073 amendment). An
  operator unlocks MRSC with `animus cluster finalize` after the last node is
  upgraded; until then `ReplicaUpdates` is refused with the pre-G-c text.
- **Preferred Region lives in the spec, not the placement policy (D2).**
  `GlobalTableSpec.preferred_leader_region`, one source of truth that splits
  follow for free (a child carries `Tablet.table`); the policy only gains the
  region pin (`allowed_values`, ADR 0005 pointer below). The default is the
  Region of the node that received the converting `UpdateTable` (D3, AWS has no
  wire field to choose it); `POST /admin/table/preferred-leader` /
  `animus table preferred-leader` re-points it (`SetGlobalPreferredLeader`).
- **Placement (D5, D8).** The conversion is one command whose apply sets
  `schema.global` and replaces every tablet's policy in the same apply. The
  pin is strict: repair never moves a lost Region's replica to another Region
  (`InsufficientDomains`, no command), so a lost Region's replicas wait for it,
  while repair *within* a Region (a dead node replaced by its Region-mate)
  works. Consequence to state plainly: with a Region down the table is
  under-replicated for as long as it is down, by design.
- **Preferred-leader reconciler step (3.3).** Acts only on a real violation,
  after a stability window, with a per-group minimum interval, targeting the
  best caught-up voter of the preferred Region with the same threshold the
  actuator arms at. A witness-Region leader is always transferred away (D7:
  the witness is a full voter that may lead transiently during a failover; it
  never serves eventual reads, 3.6). Not implemented: campaign suppression for
  the witness (a follow-up if review wants "never leads").
- **Replica status is derived, not stored (D4):** `ACTIVE` iff every routable
  tablet has a desired replica in that Region, else `CREATING`. It reflects the
  desired set, not Raft voter promotion.
- **Strong reads keep ReadIndex through the leader (D9);** no lease reads.
- **Decommission guard (D10).** `admin_drain` refuses the last `Active` member
  of a Region a global table pins, by Region and table name; `force` (`animus
  admin drain ... --force`) overrides it. With the override the replica stays
  in its Region (the strict pin), nothing crosses Regions.
- **Operator.** `spec.maxRegionRttMs` (additive, optional, schema version
  unchanged) is rendered into `cluster_settings.max_region_rtt_ms`. The
  supported stretch shape is one `AnimusCluster` on a Kubernetes cluster that
  spans the Regions (3.8); federation is G-e.
- **Visibility.** `GET /admin/global-tables` (per global table: Regions,
  witness, preferred Region, derived replica status, per-tablet replica
  placement by Region, the node-local leader and whether it is off the
  preferred Region, Active members per Region, and warnings: a pinned Region
  with no Active member, a control quorum a single Region's loss would break);
  the dashboard Placement tab shows the preferred Region per tablet and a
  "leader off preferred" badge, derived from `/admin/status`.

**Evidence.** `animusd` `sim_cluster_mrsc` (`ANIMUS_MRSC_SEEDS`,
`ANIMUS_MRSC_CELL`, `ANIMUS_SEED`): six combined nodes over the LSM engine,
three Regions at 60/75/90 ms with jitter and a heavy tail, cluster finalized
to version 2 through the real admin path, a table converted over the real
DynamoDB wire. Cells: `steady` (preferred Region moved twice through the admin
action, from a client on every node), `region_loss_leader_region`,
`region_loss_follower_region`, `region_partition_heal`, `split_under_mrsc`,
`in_region_node_replacement`, `witness_form_region_loss`,
`drain_last_node_of_region_refused`; oracles are durability of every acked
write, a `ConsistentRead: true` register that never goes backwards, the
one-replica-per-Region placement invariant at every phase boundary, and
convergence of the leader to the preferred Region. Negative controls (each
must fail the oracle it names): preferred-leader disabled, an unpinned policy,
and a register checker that must reject a stale read. The pure tier
(`animus-cp-data` `preferred_leader_corpus`, same knob) covers the reconciler
step alone. Both run per push at depth 1 and nightly in `corpus-deep.yml`
(`mrsc_leader`, `mrsc_cluster`). Mutation checked in M4: removing the
decommission guard fails `drain_last_node_of_region_refused`; the negative
controls above are the checks for the preferred-leader step, the pin and the
register checker.

**Known limitations found while building it (not fixed here):**
- Issue #1229 (pre-existing, reproduced on `main` with a plain table): a split
  child whose replicas all move to other nodes lost its pre-split rows. Fixed
  separately by #1231 (ADR 0058's 2026-10-05 amendment); `split_under_mrsc`
  covers writes acked both before and after the split.
- Issue #1226 (pre-existing): quiescence never settles on links whose RTT
  exceeds the heartbeat interval, so stretch groups get no quiescence benefit.
- The control quorum across Regions is only checked at admin time; a cluster
  whose control majority sits in one Region loses DDL when that Region fails.
  `/admin/global-tables` warns; placement of control voters is the operator's.
- The AWS field names and error texts are unverified against the live API
  (see the M3 amendment); a docs re-read is still owed before release.
- Cost numbers of section 3.2 are unmeasured; an `animus-bench` cross-region
  variant (ADR 0076) is a follow-up.


## Amendment (2026-10-05): G-d M1 as built (formats and gate, no behaviour change)

The first milestone of stage G-d lands the shapes and the gate; nothing emits them
yet. Decisions that differ from, or pin down, sections 4 and 8 above (the plan
`g01/g-d-plan.md` D1/D3 are adopted):

- **The LWW stamp is a calendar `MrecVersion`, not the node HLC (D1; deviates from
  4.4's `(hlc, region id)`).** The item HLC is relative to the `Env` clock's epoch
  (process start under `ProdEnv`), so it is not comparable between clusters.
  `MrecVersion { wall_ms: u64, logical: u32, region_id: u32 }` (derived `Ord` = tuple
  order, total across Regions) lives **inside the base-row value**: stored-item gains
  the additive variants `VersionedItem { item, ver }` and `VersionedTombstone { ver }`
  inside v1. An unversioned row compares as `MrecVersion::ZERO`. A delete already
  writes a real tombstone *value* (nothing GCs it), so carrying the stamp on the
  tombstone is all "no resurrection" needs. `region_id` is the 32-bit FNV-1a of the
  Region name (`animus_control::mrec_region_id`, vectors pinned in a test): ADR 4.4
  asked for "a small integer", but only a total order is needed and a name-derived id
  needs no cross-cluster allocation protocol.
- **Gate: a new `Gate::MrecReplication` at cluster version 3 (D3), not version 2.** G-c
  is on `main` at 2; a binary at 2 cannot decode the new shapes. The gate guards the
  commands, the `Eventual` mode with its replica set, and the data-plane shapes
  (`WriteSchema.mrec`, `KindEvalOp::Replicate`). `MIN_SUPPORTED` stays 1 (ADR 0073's
  2026-10-05 amendment: the era starts at version 1, so a floor of 2 would stop fresh
  clusters from starting an era).
- **Spec shape.** `GlobalTableSpec.consistency` gains `Eventual`; for an MREC table
  `regions`/`witness`/`preferred_leader_region` are empty and a new
  `replicas: Vec<MrecReplica { region, region_id, status, local }>` (additive, skipped
  when empty) holds every replica *including this cluster's own* (exactly one
  `local`), capped at `MREC_MAX_REPLICAS = 16`. MREC Regions are **peer names** (the
  `cluster_settings.region` namespace of ADR 0075 D6), not member labels. The status
  (`Creating`/`Active`/`Deleting`/`CreationFailed`) is stored, not derived, because
  it depends on a remote cluster. `validate` is mode-aware and rejects mixed shapes.
- **Commands.** Four, not three: `ConvertTableToMrec { table, local_region,
  region_id }` (the table need not be empty; placement untouched; the local replica
  starts `Active`), `AddMrecReplica` (starts `Creating`), `RemoveMrecReplica` (the
  local replica cannot be removed) and `SetMrecReplicaStatus`. Their apply is real
  but inert until M4 (state-based rejections only, never gate-based). The MRSC-only
  restrictions (no TTL, no LSI) now test `is_mrsc()`; an MREC table keeps both (4.6).
  Consumers that treat `TableSchema.global.is_some()` as MRSC (`animusd`'s read
  path, `global_tables`, the preferred-leader view) must test `is_mrsc()` before M4
  emits an MREC spec; they are untouched in M1 because nothing can produce one.
- **Data plane.** `WriteSchema.mrec: Option<MrecWriteStamp { region_id, wall_ms }>` and
  `KindEvalOp::Replicate { item: Option<Item>, ver }` ride as JSON blobs inside
  `KindEval`/`KindEvalBatch`/`TxnStage`, so there is **no binary `KvCommand` codec
  bump and no new `KvCommand` variant**. `KvCommand::required_gate` is therefore
  content-dependent (an exhaustive match whose three carriers fold their entries'
  content), enforced at the single `gated_propose` choke point. Apply of a
  `Replicate` that arrives anyway is a deterministic rejection until M2 gives it
  last-writer-wins semantics.
- **Section 8's provisional gate table is superseded** by one gate per release
  surface (as in G-c): `GlobalTablesSpec`/`PreferredLeader`/`GlobalTablesWire` shipped
  as `GlobalTables`, and `MrecReplication` is the MREC gate (it will also cover the
  intra replication frames M3 adds, which are cross-node variants and get their own
  `required_gate` row then).

## Amendment (2026-10-05): G-d M2 as built (last-writer-wins apply)

M2 gives the M1 shapes their semantics; nothing emits `WriteSchema.mrec` or
`KindEvalOp::Replicate` from the wire yet (M4), so every existing table is
byte-identical and the work is driven by tests.

- **The stamp rule (local writes).** A write to a table whose entry carries
  `WriteSchema.mrec { region_id, wall_ms }` stamps its base row at apply:
  `MrecVersion::next_local(stored, wall_ms, region_id)` = `wall = max(wall_ms,
  stored.wall_ms)`, `logical = 0` when `wall_ms` is strictly ahead of the stored
  stamp and `stored.logical + 1` otherwise (a `u32` overflow bumps the wall part), the
  local `region_id`. It is **strictly greater than the stored stamp whatever its
  region id**, so a local write made after observing a remote one beats it
  (causality per item, also under a slow or skewed local clock), and it is a pure
  function of `(entry, stored row)`, so every replica writes identical bytes. A delete
  writes a *versioned tombstone*. An unversioned row (a pre-conversion row, or a
  restored/imported one) compares as `MrecVersion::ZERO`.
- **The LWW rule (replicated writes).** `KindEvalOp::Replicate { item, ver }` is
  unconditional (a condition that rides anyway is ignored). It applies **iff
  `ver > stored`** (strict: equal is an idempotent re-delivery), through the ordinary
  `derive_kind_writes` path, so LSI rows, the change record with images, Streams and
  PITR see it like any local write, and `ver` is written verbatim. Otherwise the
  entry applies as a no-op that writes **nothing, not even a change record**, and the
  leader-local result says `superseded` (`KindEvalResult::superseded`,
  `KindEvalItemResult::Superseded`, animusd's `KindEvalApplied::Superseded`; the
  replicated outcome stays `Applied`, so nothing about the replicated slot maps
  changed). A `Replicate` whose entry has `mrec: None` is a deterministic rejection
  (never a versioned row on a non-MREC table).
- **Intents.** A key holding a foreign transaction intent already yields the entry's
  `ConditionFailed` outcome for every op; for a `Replicate`, which carries no
  condition, that outcome can only mean "intent on the key", and is the shipper's
  `Retry` (no separate outcome variant was added). Transactions are region-local:
  every Dynamo transaction write is a `pending` write evaluated by `evaluate_kind_eval`
  at `TxnStage` apply, so it is **stamped at stage** from the stored stamp and the
  stage entry's `mrec.wall_ms`; because an intent blocks both local and replicated
  writes to the key until resolve, nothing can change the stored stamp between stage
  and resolve, so stage-time stamping is exact (no re-stamping at `TxnResolve`). A
  `Replicate` staged in a transaction is a validation rejection.
- **TTL.** The reaper's delete is an ordinary local `Delete`; its proposer (M4) puts
  the **expiry instant** in `mrec.wall_ms`, and `max(wall_ms, stored.wall_ms)` keeps it
  causal. No apply special case.
- **Writer audit and structural guards.** The base-row writers of an MREC table are:
  `KindEval`/`KindEvalBatch`/`TxnStage` (stamped), the TTL reaper (a `KindEval`), and
  the edge-valued writers that cannot stamp: the Dynamo fast arms and the raw client
  protocol. Those are closed structurally: `table_change_records_carry_images` is
  `true` for an MREC table (so the fast arms are never taken), `marker_batch_write_raw`
  refuses an MREC table, and `cp_txn` refuses a non-`pending` write to one. Restore
  and import write into a freshly created table (unversioned rows, `ZERO`); the M4
  convert must refuse a table that is still restoring/importing. Backfill/seed copies
  write derived GSI rows and markers, never base rows.
- **Evidence.** `animus-cp-data/src/mrec_props.rs`: a pure convergence proptest
  (random interleavings of local writes and deliveries, then every record to every
  region in random order with duplication; all regions byte-identical, equal to the
  max-version record per key, no resurrection, causality, idempotence) with two
  negative controls that must be caught (arrival-order LWW, a dropped region
  tiebreak); `tests/it/mrec_apply.rs`: the same rule through the real apply arms over
  a 3-replica Raft group; `ANIMUS_MREC_PROP_CASES=K` (default 256).
- **For later milestones.** M3's receiver handler maps the leader-local results to
  the per-record `Applied`/`Superseded`/`Retry`/`Rejected` answers; `ProbeIdentity::
  ValueProves` confirm fallbacks decode a versioned row correctly but are weak for a
  replicate (value equality does not prove *this* entry won), so the receiver must use
  `RequiresOwnEntry`.


## Amendment (2026-10-06): G-d M3 as built (peer transport, config, receiver)

M3 lets one cluster hand a batch of stamped records to another and have the
receiver apply them with M2's last-writer-wins rule. Nothing ships yet (the
shipper is M4), so every existing table is unchanged and the work is driven by
tests (`SimWorld` for semantics, real loopback sockets for the transport).

- **Config** (`cluster_settings`, all additive and skipped when unset; `--region`,
  repeatable `--peer REGION=host:port[,host:port...]`, `--allow-insecure-peers`,
  `--mrec-max-clock-skew-ms MS` on `animusd` and `animusd gen-config`):
  `region` (this cluster's name), `peers: [{region, endpoints: ["host:intra-port"],
  tls_ca?}]`, `allow_insecure_peers`, `mrec_max_clock_skew_ms` (default 500).
  Static, node-local, identical on every node, like `peer_book`. Startup validation
  (`ClusterSettings::validate_mrec`): peers need a region, no peer is the own region
  or repeats, every peer has a well-formed endpoint, skew is positive; a flag
  is rejected for a mode with no config.
- **Wire** (`animus-node`): `ClientRequest::MrecApply(MrecApplyRequest {proto,
  from_region, table, records})` / `ClientResponse::MrecApply(MrecApplyResponse)`,
  intra-only (`Surface::Intra`), **class G, `Gate::MrecReplication`**; `KindWriteOp::Replicate`
  is accepted on the leader write RPCs (`KindWriteItem`/`KindWriteBatch`, whose
  `required_gate` is content-dependent) and `KindWriteItemReply::Superseded` is the
  lost-LWW slot. The frame has its own `MREC_PROTO = 1` because two clusters roll
  independently. **Correction found by the real-socket test:** a whole-batch
  `MrecApplyResponse::Refused` is gate **Base**, not `MrecReplication`: it is how a node
  whose own gate is still closed says "not yet", and a class-G reply could not be
  emitted by exactly the node that needed to (a debug build panics on a closed-gate
  emit). Per-record `Answers` stay class G. Fixture `client-frame/v1-mrec.bin`.
- **Auth**: ADR 0064 mutual TLS on the peer's intra port is the cross-cluster
  authentication (each side's `ca_path` bundles both CAs; a peer's optional `tls_ca` is
  trusted in addition when verifying that peer's server cert). A node with no TLS
  neither dials (`PeerError::Refused`) nor accepts (`Refused{retryable:false}`, checked
  before the gate) unless `allow_insecure_peers`.
- **Client**: `PeerClient::call(to, payload, timeout) -> Result<Vec<u8>, PeerError>`
  (bytes in/out, so `SimWorld`'s `PeerBridge` and `ProdPeerClient` are interchangeable);
  `ProdPeerClient` tries a peer's endpoints in order through the gated intra relay.
- **Receiver** (`animusd::mrec_receiver::handle_mrec_apply`, `E: Env`-generic): groups
  records by the receiver's *own* tablet layout and proposes one `KindEvalBatch` of
  `Replicate` per tablet through the normal leader path (follower-connected nodes forward);
  per-record `Applied | Superseded | Retry | Rejected`. A replicate is
  `ProbeIdentity::RequiresOwnEntry` (value equality never proves it won), so a lost confirm
  is `Retry`, which is safe because a replicate is idempotent as state; `ConditionFailed`
  on a replicate can only be a foreign txn intent, also `Retry`. A stamp beyond
  `wall_now + mrec_max_clock_skew_ms` is `Retry` and counted
  (`mrec_skew_rejected_total`), decided once at the accepting node and carried as a value.
  More than 64 in-flight batches per node answers `Retry` for all records. Whole-batch
  refusals (`Refused`): unknown `proto`, gate closed (retryable), no TLS, no region, not a
  peer, table not MREC here or not replicated with the sender (retryable: the replica may
  not have reached this node's metadata yet).
- **Bound-node gap closed**: `Node::bind` has no config, so `run_bound_node*` now installs
  the MREC settings in its start half like the other entry points do. (The same path still
  does not install `max_region_rtt`; filed separately, not changed here.)
- **Tests**: `sim_world_mrec_tests.rs` (A to B over `SimWorld`, `ANIMUS_MREC_WORLD_SEEDS`,
  default 4, run at 24: newer wins / older and equal `Superseded` / tombstones never
  resurrect / skew rejected then accepted plus a negative control / partition then heal /
  duplicated, shuffled and lossy delivery converge to the max stamp per key / a
  follower-connected entry / grouping by the receiver's own layout (2 tablets) / refusals
  / gate-closed receiver / seed determinism / the bridge is a `PeerClient`); `mrec_writer_guard_tests`
  plus `raw_and_edge_valued_writers_refuse_an_mrec_table` (`marker_batch_write_raw`,
  `cp_txn`); `tests/mrec_peer_transport.rs` over real sockets (two clusters with different
  CAs, mutual TLS both directions; a stranger CA refused; plaintext refused, refused at
  the receiver, and allowed with `allow_insecure_peers` on both sides).
  **Not covered over real sockets:** applying data; that needs a converted table, i.e. the
  replica-create saga (M4), so the receiver handler is reached and answers a retryable
  refusal. The full real-process two-cluster apply is M6.
- **Moved to later milestones**: `/admin/global-tables` peer-health fields need shipper
  state, so they land with M4 (the receiver counts are in metrics now).

## Amendment (2026-10-06): G-d as built (M0-M6, MREC global tables)

Stage G-d is complete: a table can be made eventually consistent across independent
clusters with `UpdateTable ReplicaUpdates` (no `MultiRegionConsistency`, or
`EVENTUAL`), and writes on every replica converge by last-writer-wins. This
amendment records what the milestones built and the deviations from sections 4, 5
and 8 above. Plan: `g01/g-d-plan.md`; per-milestone detail is in the M1-M3
amendments above and in the module docs named below.

**Milestones.** M0 a multi-cluster `SimWorld` (N independent clusters on one
virtual clock and a lossy WAN, `sim_world.rs`); M1 formats and gate; M2 the apply
rule (`apply_mrec`: the incoming stamp wins iff it is greater than the stored one,
a loser answers `Superseded`, deletes are stamped tombstones so nothing resurrects);
M3 config, `MrecApply` frame, receiver, `ProdPeerClient` over the intra port;
M4 the shipper (`mrec_shipper.rs`), replica saga (`mrec_saga.rs`), stamping at
every write site, `DescribeTable Replicas`; M5 the fault corpus
(`sim_world_mrec_corpus.rs`, 17 cells, 4 negative controls, `ANIMUS_MREC_SEEDS`,
nightly depth 15); M6 the operator surface and the real-process test.

**F2 amends section 4.2: current-state shipping, not literal change-record
shipping (decided 2026-10-06, see below).** Section 4.2
describes a consumer of the change log shipping each `ChangeRecord`. As built, the
change log only *names dirty keys*: per `(led tablet, peer)` the shipper reads the
keys changed above the peer's cursor (`mrec:<region>`) and ships each key's
**current row** (value or tombstone, with its `MrecVersion`). The same path serves
steady state, the initial copy (a scan under `mrecscan:<region>`), a resync after
the retention cap and a split child. Consequences: re-delivery is idempotent for
free (LWW), no per-record version had to be added to `ChangeRecord`, and there is
one code path instead of a log path plus a separate scan. The cost: **a key written
several times between two ticks arrives once**, so the receiver's DynamoDB Stream
sees the coalesced final state, not every intermediate write (the stream keeps
per-key ordering and old/new image parity, which the corpus's stream oracle
checks). If the maintainer rules for literal record shipping, `ship_one`'s read step
and the stream oracle are the places that change; the wire, stamps and apply rule do
not.

*Decision (maintainer, 2026-10-06): keep current-state shipping.* Literal
record shipping was considered as a way for every region to process the same
history, and rejected: it cannot give that under concurrent multi-region writes
(arrival order differs per region and last-writer-wins drops the loser; a resync
past the retention cap or a new replica still starts from a snapshot), while it
would cost a new per-record version in the change log and traffic proportional to
write rate. AWS documents the same behaviour for its own MREC tables: "The MREC
replication process might combine multiple changes in a short period of time into a
single replicated write, resulting in each replica's Stream containing slightly
different records", and "Streams records on MREC replicas are always ordered on a
per-item basis, but ordering between items might differ between replicas"; identical
per-replica streams ("including Stream record ordering") are an MRSC property
([global tables: how it works](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/V2globaltables_HowItWorks.html),
read 2026-10-06). An application that needs one history in every region uses an
MRSC table (G-c).

**F1: the stamp is a calendar `MrecVersion`, not the cluster HLC.** The item HLC is
relative to each process's `Env` clock epoch and is not comparable between clusters
(and re-basing the cluster HLC onto wall time would change uncertainty, ceilings and
PITR/seal ages for every table, ADR 0018). `MrecVersion { wall_ms, logical,
region_id }` lives inside the row value (M1 amendment), stamped on the origin
leader from `env.wall_now()` at write time (a transaction stamps at stage time; the
TTL reaper stamps `min(expiry, now)`; the apply takes the max with the stored
stamp). A receiver refuses a record whose wall time is more than
`mrec_max_clock_skew_ms` ahead of its own clock (default 500 ms). This is where
`wall_now` (ADR 0051) earns its second use, still inside the `Env` seam.

**F3: gate = cluster version 3, `MIN_SUPPORTED` stays 1.** `Gate::MrecReplication`
guards the four `MetaCommand`s plus `MarkMrecCopied`, `GlobalTableSpec.replicas`, the
`WriteSchema.mrec` / `KindEvalOp::Replicate` data-plane shapes and the `MrecApply`
frame (rows in ADR 0073's inventory). A cluster finalized to version 2 only
(G-c open) keeps the old `ReplicaUpdates` rejection text and ships nothing. The
cross-cluster frame carries its own `MREC_PROTO` since two clusters roll
independently.

**Replica lifecycle (D7/D8).** The saga driver is the leader of the table's lowest
active tablet. Create: `Creating`, then a `MrecControl::CreateReplica` to the peer
(which adopts an identical-shape table or refuses a different shape for good, giving
`CreationFailed`), `AddPeer` to the other replicas (full mesh), then a shipper scan
per tablet whose completion marks `MarkMrecCopied`, then `Active` on both sides.
Delete: `Deleting`, `Leave` to the peer (its table survives standalone), a grace
period, then `RemoveMrecReplica`. Streams are forced to `NEW_AND_OLD_IMAGES` and
cannot be disabled on an MREC table.

**Split lineage: an unfiltered scan, not an inherited floor.** A split child's CHANGE
and CURSOR scopes are dropped by `trim_split_child`, so the child has no cursor and
scans its own rows in full, then resumes the log; the peer answers `Superseded` for
rows it already holds. The plan's inherited-floor filter would need a cursor row
exempt from that trim (an ADR 0073 format change) and a skew argument for a modest
saving; it is not built. Proven by `a_split_of_the_source_tablet_keeps_shipping_every_row`.

**TTL: `MrecControl::SetTtl`.** The replicated `TtlSpec` is copied at create and
re-sent on `UpdateTimeToLive` by the saga driver (class G with the frame; fixture
`client-frame/v1-mrec-control-ttl.bin`). Every region runs its own reaper; an expiry
is a stamped delete, so an update in another region before the expiry reaches it can
lose to the expiry tombstone (AWS-faithful, D9).

**M6 operator surface.** `GET /admin/global-tables` now lists MREC tables beside
MRSC ones (`consistency: "EVENTUAL"`): the replica set with status and copy
progress (`tablets_copied`/`tablets_total`) and **this node's** per-(tablet, peer)
shipper health: `backlog`, `lag_ms` (age of the oldest unshipped change at the last
tick), `last_ack_age_ms`, `scanning`, `needs_resync`, `caught_up`, `failures`,
`last_error`, `operator_error`, `shipped_rows`. Health is in memory and node-local
(reset on restart; a fleet view fans out over every node, as for MRSC leaders), and
a refusal that retrying cannot fix (a shape mismatch) and a `CreationFailed` replica
raise `warnings`. `animus admin global-tables` prints that JSON; the dashboard's
Placement tab shows an MREC table's replica status in the node detail and no longer
counts an MREC table's leaders as "off preferred" (it has no preferred Region).
Metrics for the shipper and receiver landed in M4.

**Evidence.** The semantics are proven over `SimWorld` (the M4 saga/e2e/edge tests
at 20+ seeds each, and the M5 corpus: convergence, floor/LWW, loss, provenance, echo,
amplification, TTL, stream-parity oracles, with negative controls that must trip).
The real-socket evidence is `animusd/tests/mrec_peer_transport.rs`: the mutual-TLS
handshake between two clusters with different CAs, a stranger CA refused, plaintext
refused without `allow_insecure_peers`, and
`two_real_clusters_replicate_a_table_both_ways_over_mutual_tls` (two single-node
`ProdEnv` clusters, finalized to version 3, a table created over the TLS DynamoDB
wire, `ReplicaUpdates Create`, a pre-existing row copied, a write on each side
readable on the other, the admin view naming the table). That test runs both
clusters on loopback in one process: it shows the real TLS client, loops and
framing work, **not** WAN behaviour.

**Known gaps and residuals.**
- No WAN latency, bandwidth or cost measurement; the two real clusters share a host.
  `kind`/operator e2e cannot run in this sandbox, so the operator rendering peers
  into `cluster.json` (G-e, built afterwards, see "G-e as built") is unverified on a real cluster.
- The AWS wire field and error names MREC uses (`ReplicaUpdates`, `Replicas`,
  `ReplicaStatus`, the validation texts) remain unverified against the AWS docs
  (section 0); they are the shapes AWS's public API reference extracts showed.
- F2 (above): the receiver's stream coalesces writes made within one shipping round, as AWS MREC does; decided 2026-10-06.
- Shipper health is per node and in memory; no cluster-wide lag aggregate or alert.
- A TTL change made while the saga driver role moves is not re-sent until the next
  change, and two regions setting different TTL attributes concurrently end with the
  last push winning.
- The shipper's own gate check has no discriminating test (a spec below the gate
  cannot be forced; stamping asserts the gate); the wire check is discriminated.
- A split child re-sends its rows once (above); a cluster with very large MREC
  tablets pays WAN volume per split.
- `SchemaRegistry::sync_indexes` (`animus-dynamo`, pre-existing, not fixed here)
  does not refresh the key schema of an already-registered table, so a table dropped
  and recreated with a *different* key schema stays unreadable on that node until a
  restart; MREC's re-create path hits it only for a different shape.
- Only the full-mesh topology exists (D8); `ReplicaUpdates[].Update` and the
  per-replica overrides (KMS, throughput) are rejected, as section 5 says.

## Amendment (2026-10-06): G-e as built (operator federation for MREC peers)

Section 5.4's MREC half is built in `animus-operator`; no `animusd` change was
needed.

**CRD (additive, `schemaVersion` stays 1).** `spec.region` (this cluster's MREC
region name), `spec.peers[]` = `{region, endpoints[host:port, intra port],
caSecretRef{name, key (default ca.crt)}?}`, `spec.allowInsecurePeers` (dev
only) and `spec.mrecMaxClockSkewMs`. Every field is skipped when unset, so an
existing spec serializes and hashes unchanged (no upgrade-triggered pod roll).
A new golden fixture `v1-peers.json` pins the shape (the existing three are
untouched). No federation CRD, as decided. The CRD offers a Secret reference
only, **not** a cert-manager `issuerRef` as 0064 does for the cluster's own
certificate: an `Issuer` carries no CA bytes the operator could mount, so a
cert-manager user references the Secret holding the CA certificate (its
`ca.crt` or `tls.crt` key, hence the `key` field).

**Config mapping.** The operator renders `cluster.json`'s `cluster_settings`
with the exact animusd shape G-d defined: `region`, `peers[{region, endpoints,
tls_ca}]`, `allow_insecure_peers` (only when true) and
`mrec_max_clock_skew_ms`, on every node of every role. The field names are
pinned against animusd's own pinned-JSON test
(`mrec_settings_are_additive_and_pinned_json`) by literal comparison; the
operator crate does not link animusd, so no test runs animusd's parser over the
operator's output.

**Validation.** `AnimusClusterSpec::validate_peers_spec` (shared by the webhook
and the reconciler): peers need `spec.region`; no peer repeats or equals the own
region; each endpoint is `host:port`; **peers without `spec.tls` are refused
unless `spec.allowInsecurePeers: true`** (a CA reference also needs `spec.tls`).
On a violation the reconciler sets `PeersSpecInvalid` and applies nothing
(refuse, not strip: dropping `peers` would stop replication, and dropping the
TLS rule would open an unauthenticated link).

**TLS trust.** Each referenced CA Secret is mounted read-only at
`/etc/animus/peer-ca/<peer index>/ca.crt` (index, not region name, so any region
string is a valid path). Two uses: the peer's `tls_ca` (animusd verifies that
peer's server certificate against it in addition to the own CA), and the
**inbound** side, which is the subtle one: the intra listener verifies client
certificates against the node's single `tls.ca_path`, so a peer's client
certificate verifies only if its CA is in that file. animusd's loader accepts a
PEM with several certificates, so when any peer names a CA the generated
`entrypoint.sh` concatenates the own `ca.crt` and every peer CA into
`/tmp/animus-tls-ca-bundle.pem` before `exec` (both role branches) and
`tls.ca_path` points there. No animusd change; the bundle is rebuilt on every
container start, so a CA rotation in a peer Secret needs a pod restart (animusd
does not reload TLS material either).

**NetworkPolicy.** A `NetworkPolicy` cannot match a DNS name, and peer endpoints
are user-supplied names, so the rules are port-scoped, not address-scoped, and
only exist when `spec.peers` is set: egress to the peer endpoints' ports (the
distinct set, any destination, `0.0.0.0/0` and `::/0`), and ingress on the
**intra port only** from any source. Authentication is the mutual TLS handshake,
not network location; this is weaker than a source allowlist and is stated
rather than hidden (an operator who knows the peer's CIDRs can tighten it by
hand, as `spec.s3.egressCidrs` allows for S3). The client (dynamo), admin,
console, internal and client ports are never opened to peers; a test pins that
the only any-source ingress ports are dynamo (pre-existing) and intra. With
`allowInsecurePeers` the intra port is open and unauthenticated by construction.

**`PeerReachable`.** A positive-polarity condition, set on every reconcile that
has peers (removed when it has none), from `GET /admin/global-tables` on every
pod through the existing admin client (the TLS-aware path the drain sequence
uses). Shipper health is node-local (a node reports the tablets it leads), so
the pure function `peers::evaluate` aggregates all pods. Per peer: no shipper
entry yet gives `Unknown` (no MREC table replicates with it, so there is no
reachability signal without a table); at least one shipper with no `last_error`
that is `caught_up` or acknowledged within 5 minutes gives `True`; otherwise
`False`, naming the error or the staleness. Overall: `False` if any peer is
`False`, else `Unknown` if any is `Unknown` or no pod answered, else `True`.
The operator does not dial peers itself and does not drive replica creation.

**Not done.** Stretch-segment federation (MRSC over several Kubernetes clusters,
remote seed addresses, flat pod networking) stays deferred as section 5.4 says.
No automatic endpoint discovery. No source-address NetworkPolicy for peers. The
two-cluster `kind` e2e was not written (it needs two clusters or two namespaces
with a routable, TLS-trusting path and a global table, and `kind` cannot run in
the sandbox this was built in); **none of the rendering, mounts, NetworkPolicy
or condition has been exercised against a real Kubernetes API server**, only
unit tests of the builders and the reconciler over fakes.

**Security residual (issue #1253): resolved** by the "Peer trust class" amendment
below.

## Amendment (2026-10-08): Peer trust class (issue #1253)

Before this change the bundled `tls.ca_path` trusted a peer region's CA on the
whole intra port, which also serves `Forwarded` and bare `Get`/`Put` requests, so
a node holding a peer-CA certificate could read and write any table, bypassing
the DynamoDB port's SigV4 and table policies. The same acceptor also guarded the
internal Raft wire.

**Decision (per-certificate trust, no new port).** `TlsSection` gains an optional
`peer_ca_path` (additive, default absent). `ca_path` is the **own** cluster CA;
`peer_ca_path` is a bundle of peer-region CAs. The mutual-TLS acceptor still
admits a client certificate chaining to either (so a peer can complete the
handshake), but `TlsMaterial::classify_peer` (animus-env) reports a certificate
that does not verify against the own-CA-only verifier as `PeerTrust::PeerRegionOnly`.

- **Intra port.** `animusd` classifies each connection after the TLS accept and
  threads the result into `handle_connection`. A `PeerRegionOnly` connection may
  send only `MrecApply`, decided by `animus_node::peer_region_may_send`, an
  exhaustive match with no wildcard arm (a new `ClientRequest` variant is a
  compile error until classified; the safe default is `false`). Anything else gets
  `ClientResponse::Error`, a warn log and the `peer_region_request_refused`
  metric; the connection stays open.
- **Internal Raft wire.** A `PeerRegionOnly` connection is dropped after the
  handshake (peers never speak Raft). This closes a wider hole than the issue
  text described.
- **Back-compat.** With no `peer_ca_path`, every admitted certificate is `Own`,
  exactly today's behaviour, so an existing merged-`ca_path` config keeps working
  (and stays as permissive as before until it moves to `peer_ca_path`).
- **Operator.** `ca_path` is always the own CA. When a peer names a CA `Secret`,
  `peer_ca_path` is the peers-only bundle the entrypoint concatenates.
- **Plaintext (`allow_insecure_peers`).** Unchanged: there is no certificate, so
  the intra port cannot tell peers apart. It is unauthenticated and for tests and
  trusted networks only.

ADR 0073 classification: **L** (node-local enforcement). No wire or durable
format change: the refusal reuses `ClientResponse::Error`, and the config field is
optional. In a mixed-version cluster an older node simply does not enforce.

Tests: `classify_peer` and `peer_region_may_send` unit tests; real-process
`animusd/tests/mrec_peer_transport.rs` (a peer-region certificate's `Forwarded`
and `Get` are refused, `MrecApply` still reaches the handler, an own-CA
certificate is not gated); operator tests pin `ca_path` and `peer_ca_path`.
