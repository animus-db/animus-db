# ADR 0019 — v1 ships the CP plane only; the leaderless AP data plane is deferred

- **Status:** Accepted (amends ADR 0001 for the v1 scope)
- **Date:** 2026-08-03

## Context

ADR 0001 set AnimusDB's founding shape: a **leaderless AP data plane** (Dynamo
lineage — tunable-quorum reads/writes, available under partition, convergent via
repair/anti-entropy, ADR 0010) paired with a small **strongly-consistent Raft
control plane** owning cluster metadata. ADR 0016/0017 then added a *second*,
**leaderful CP data plane** (per-tablet Raft, linearizable single-tablet KV) as a
modular alternative, and ADR 0018 sketched cross-tablet transactions over it.

Both data planes are substantially built:

- **AP** (`animus-data`): durable, self-healing (failure → `Down` → re-place →
  converge), wired into `animusd` as the default plane behind both wire adapters.
- **CP** (`animus-cp-data`): linearizable single-tablet KV, ReadIndex reads,
  membership change, tablet split, an Elle linearizability corpus, and — after v1
  Phase 1 — runnable + cross-process-routable in `animusd` (per-table mode
  selection, ADR 0017 #3a/#3b).

The v1 plan scoped **AP + CP both** as a "linearly-scalable" first release. But the
remaining v1 work — horizontal sharding (live tablet split + dynamic node-join +
rebalancing), plus each plane's transaction story — must be built **per plane**,
roughly doubling the hardest remaining effort, and means maintaining **two
consistency models** and **two transaction stories** (Accord for AP, 2PC/HLC for
CP). The maintainer's priority is **strong consistency (CP)**. Shipping one plane
*well* beats shipping two half-finished.

## Decision

**AnimusDB v1 ships the leaderful CP data plane only.** The leaderless AP data
plane is **deferred** — recorded here as a *long-shot future improvement*, not part
of v1.

This **amends ADR 0001** for the v1 scope: the Raft **control plane** is unchanged
(it remains the metadata authority), but the **data plane** is CP-only. AnimusDB v1
is therefore a **strongly-consistent, linearly-scalable, per-tablet-Raft store**
(closer to CockroachDB / TiKV) rather than a Dynamo-lineage AP store. ADR 0001 is
not superseded — its control-plane decision stands and its AP decision is the
foundation the deferred long-shot would revive.

## Consequences

**v1 scope shrinks to one well-defined target:**

- **CP becomes the default and only data plane.** The per-table `ReplicationMode`
  seam (ADR 0017 #3a) is **retained but forced to `Cp`** in v1 (an `Ap` selection
  is unsupported) — kept as the forward-compatibility hook for AP's eventual
  return, not removed.
- **`animusd`'s serving path drops the AP roles** (the migration below): the data
  replica (`serve_replica`), the `DataClient` quorum coordinator, anti-entropy, and
  hinted handoff leave the v1 node. The bootstrap tablet becomes a CP Raft group;
  client / DynamoDB / CQL reads and writes route to CP unconditionally.
- **Phase 2 sharding targets one plane.** Split *execution* is only the CP
  live-group division (ADR 0017 Stage D + `Coresident`); there is no AP
  metadata-split path to build. The shared substrate (shard map, key-range routing,
  node-join, address distribution) is unchanged — so dropping AP **halves the
  plane-specific work** without losing the "linearly scalable" headline.
- **Cross-tablet transactions** (ADR 0018, 2PC/HLC over the CP groups) remain the
  designated post-single-tablet step — now the *only* transaction story to build
  (Accord-over-AP is deferred with AP).

**Knowingly given up (the CP trade, already documented in ADR 0017):**

- **Availability under partition.** v1 has no sloppy-quorum AP path: a tablet is
  write-unavailable during election / quorum loss. v1 chooses linearizability over
  the AP availability ADR 0001 prized.
- The Dynamo-lineage identity for v1. (Reclaimable via the long shot.)

**Code disposition — retain dormant, do not delete:**

- `animus-data` (AP plane), `animus-consensus`'s AP-frontier paths, and the AP /
  frontier Elle corpora (ADR 0010, 0014 frontier) are **kept compiling and tested**
  but removed from the v1 *serving path* and the v1 *acceptance gate*. They are a
  sunk, working asset and the foundation the long-shot revival builds on; deleting
  them buys a leaner tree at the cost of that option. (The maintainer may elect
  physical removal later; this ADR's intent is *defer*, not *destroy* — "for now".)

## The long shot (reviving AP, post-v1)

Re-wire `animus-data` into the node, restore per-table `Ap`/`Cp` selection (the
`ReplicationMode` seam is preserved for exactly this), and adopt Accord (ADR 0011,
already AP-aligned) as AP's transaction story. The dual-plane vision of ADR 0001 +
the pluggable-replication frame of ADR 0016 stay on record so this remains feasible
rather than a rewrite. It is a stretch goal, not a roadmap commitment.

## Migration (execution follow-on, not this ADR)

1. Make CP the unconditional plane in `animusd`: route the client / DynamoDB / CQL
   read/write paths to the CP group; bootstrap the initial tablet as a CP Raft
   group; force `ReplicationMode::Cp`.
2. Remove the AP roles from node assembly (`serve_replica`, `DataClient`,
   anti-entropy, hinted handoff, the `data`/`coord` `ProdEnv` roles and their
   `ClusterConfig` ports) from the v1 path.
3. Trim the v1 acceptance gate to the CP + control-plane suites; leave the AP /
   frontier suites compiling but out of the gate.
4. Proceed with Phase 2 (sharding) against CP only.

This ADR builds on ADR 0017/0018 (the CP stack it makes v1's whole data plane),
ADR 0016 (pluggable replication — the frame that made one-plane-at-a-time viable),
and amends ADR 0001 (whose AP data-plane decision becomes the deferred long shot;
its control-plane decision stands).
