# ADR 0079 — Per-node density and the background-work budget: placement capacity, awake-group admission, background permit pools

- **Status:** Proposed (2026-10-12). Docs-only; no code changes with this ADR ("ADR first, code after").
- **Date:** 2026-10-12
- **Origin:** issue #1295 (follow-up to #1199, closed by ADR 0078); the ADR 0078 maintainer decision 5 (2026-10-11) that a per-node cap on awake groups gets its own ADR once the stable ceiling is measured. Measurements: `docs/density-measurements.md` (branch `claude/per-node-density`, commit `795465c`).
- **Amends:** none. **Depends on:** ADR 0003 (determinism, the `Env` seam), ADR 0005 (placement), ADR 0029 (rebalancing), ADR 0031 (host reconciler), ADR 0044/0048 (quiescence), ADR 0058 (learners, snapshots), ADR 0073 (upgrade compatibility), ADR 0075 (region-pinned placement, MREC receiver), ADR 0078 (campaign governor, `LoadMonitor`, staggered hosting).
- **Not in scope:** client-path and transport admission (issue #1296: per-group propose bounds, the node-wide request gate, unbounded transport queues). This ADR only says where its permit pools and `LoadMonitor` input could be shared with that work.
- **Motivating evidence:** issue #1297 (at G=3000 awake, 2651 of 3000 groups led after 600 s with `max_term` flat at 3), diagnosed as accepted overload; see "Resolved dependency".

## Context

ADR 0078 bounds the *election* feedback loop. It says in "What this ADR is not" that it does not make an overloaded node faster, and it deferred a cap on awake groups to a separate ADR (decision 5). Nothing bounds how many tablets a node carries, how many of them are awake, or how much background work (snapshots, splits, compactions) runs beside the apply, fsync and heartbeat paths. Under load that work competes with exactly the paths whose lateness starts the #1199 loop.

### What was measured, and what was inferred

All from `docs/density-measurements.md`: one 4-core, 15 GB sandbox, release build, **single ProdEnv runs** (not seed-reproducible, so order of magnitude only). The rig is three `ProdEnv` nodes in **one process with `worker_threads = 2`**, `MemoryEngine`, no client traffic, ADR 0078 mechanisms at their defaults.

Measured:

- Awake groups, `ANIMUS_BP_LIVE_QUIESCE=0`: G=1000 and 1250 pass (1.55 and 1.80 cores, `max_term` 2 and 5). G=1500 fails the 120 s hold (led count dips by 4, `max_term` 3 to 7, 1.93 cores). G=2000 fails (dip of 1, all-led in 29-57 s across two runs, `max_term` 16-45). G=3000 never converges (2651/3000 after 600 s, `max_term` 3, 1.92 cores, RSS 604 MB). Because the runtime has 2 worker threads, 1.9 cores means saturated, not "using 1.9 cores of need".
- Cost slope between the two passing rows: about 0.48-0.52 core per 1000 awake replicas, process-wide.
- Quiesced (production default): G=1000/2000/3000 (3000/6000/9000 replicas) all led in 3-15 s and held, 0.04/0.07/0.10 cores, RSS 79/146/216 MB, so about 23 KB per hosted quiesced replica (base about 10 MB) and about 0.011 core per 1000 quiesced replicas.
- fds were flat at 24 (peak 143-147 during hosting), because `MemoryEngine` opens no files.
- Node-loss repair burst (sim): 60 and 300 tables on 6 nodes gave 6 and 12 tablets on the dead node, converged in 7 virtual s, peak 7 and 25 proposals/s, about 2.3 accepted reconfigurations per affected tablet, 0 snapshots.
- Split storm (sim): 8 and 17 splits completed under node restart and control-leader transfer.

Inferred, and labelled so wherever used below:

- Per-node figures are one third of the process totals. This gives about 0.5 core per 1000 of a node's own awake replicas, i.e. **about 1900-2000 awake replicas per node-core with no client load**. Not measured per node.
- The true awake ceiling per core. The rig cannot exceed about 2 cores, so the ceiling lies between 1250 and 1500 groups (3750-4500 replicas) for 2 worker threads, and a larger `worker_threads` run is needed for a per-core figure.
- Everything about `LsmEngine` (fds, memtable memory, compaction), client write cost, snapshot cost and real split cost. **Not measured.** The node-loss burst was tiny (4-10 % of tablets on the dead node, empty tablets, no CPU model), so it shows no capacity number.

### What exists today (grepped at `795465c`, not assumed)

- **Placement counts replicas, not capacity.** `animus-placement`'s `Candidate` is `{node, labels}` only (`crates/animus-placement/src/lib.rs:37`); there is no weight or limit anywhere in the crate. `select_replicas_balanced` takes a per-node replica-count map `load` and prefers least-loaded eligible nodes (line 262). `rebalance_step`/`rebalance_plan` (lines 458, 479) move one replica at a time from a most-loaded to a least-loaded node and converge to max - min <= 1 when the policy has no `SpreadPolicy` (ADR 0029; with a spread constraint only monotonic non-worsening and termination hold). A small node therefore gets as many tablets as a large one.
- **Registration carries addresses and labels, not capacity.** `MetaCommand::RegisterNode` is the sole path that inserts a new member (`animus-control/src/meta.rs`, ADR 0040 Decision C); a narrow label write was added for region/zone (ADR 0075, G-01 stage G-a, `meta.rs:611`). `Gate` today runs `Base`, `Era`, `GlobalTables` (2), `MrecReplication` (3), `TxnSealChecked` (4) (`animus-control/src/version.rs:172`).
- **Repair is proposed in one tick.** `reconcile_loop` (`animus-control/src/node.rs`, about line 2938) calls `view.reconcile(&recently_done, &recently_down)` and proposes every returned command in a loop. `REPAIR_DWELL` (5 s, line 282) delays repair after a node goes down; once it elapses, every violating tablet's fix is proposed in the same tick. Rebalancing has a cadence (`REBALANCE_EVERY_N_TICKS` 8, about 4 s at `RECONCILE_INTERVAL` 500 ms, line 170) and runs only when repair proposed nothing; repair has no pacing. Each fix starts a learner and a snapshot stream with no node-level cap on concurrent sends or receives. ADR 0078's `host_unsettled_max` gates only fresh `Host` actions.
- **Splits have a per-tablet interval and cooldown only.** `AUTO_SPLIT_INTERVAL` 2 s and `AUTO_SPLIT_COOLDOWN` 15 s (`crates/animusd/src/lib.rs:15086`, `:15091`). No cap on concurrent splits per node or per cluster.
- **LSM compaction is trigger-only.** `compaction_trigger` (`animus-storage/src/lsm.rs:259`, default `DEFAULT_COMPACTION_TRIGGER`) only starts an L0 compaction (line 3038); there is no L0-count write stall and no compaction IO rate limit. Maintenance is one task per engine, and each tablet has its own private engine (ADR 0050), so N tablets can run N compactions at once. The memtable gate `BACKPRESSURE_OVERSHOOT_FACTOR` 4 (line 243) makes writers wait and then error, and client retries amplify the load.
- **A retryable-refusal precedent exists.** The MREC receiver admits at most `MREC_MAX_INFLIGHT_APPLIES` = 64 batches per node (`animusd/src/mrec_peer.rs:79`) and answers the rest with a retry.
- **ADR 0078's `LoadMonitor` and `CampaignGovernor`** exist per node, `K=32`, `K_eff` = `K`, `K/2` at lag >= 30 ms, `K/4` at lag >= 100 ms, dead band 10 ms. ADR 0078 deliberately does not auto-size `K` from `available_parallelism` (a nondeterministic input outside the `Env` seam, and a fixed number reads better in `/admin`); decisions 2, 4, 5 and 6 of its maintainer decisions are constraints here: fixed knobs not scaled by an autodetected core count (2), lag stays advisory in `/admin/health` and never fails the health check or the roll gate (4), density admission is this separate ADR (5), and `H` stays fixed (6).
- **Quiescence** (ADR 0048): a quiesced group has no timers; a write, a peer message, or the reconciler's `down`-driven `wake()` un-quiesces it. The wake is spread over one election window by `on_local_wake` but not capped.

### Three layers, three different resources

| Layer | Bounds | Resource protected | Scope |
|---|---|---|---|
| 1. Placement capacity | hosted replicas per node | memory, files, disk (held even by a quiesced replica) | control plane, replicated |
| 2. Awake-group admission | awake (non-quiesced) replicas per node | CPU (heartbeats, timers) | node-local |
| 3. Background-work budget | concurrent snapshots, splits, compactions per node; repair proposals per tick | CPU, disk IO, fsync | node-local (plus one control-leader pacing rule) |

They are one ADR because they share one signal (`LoadMonitor` lag), one reduction rule (`K_eff`-style), one refusal style and one knob style, and because the operator derives all of them from the same pod resources.

## Decision

### 1. Placement capacity (control plane)

**Declared capacity.** Each node declares `capacity = { weight: u32, max_replicas: Option<u32> }`.

- `weight` is relative capacity, default **100** (a node that declares nothing, including every existing node, is weight 100 with no max). Only ratios matter.
- `max_replicas` is an absolute ceiling on hosted replicas (voters and learners) on that node, default none.

It is advertised at registration through a new optional field on `RegisterNode`, and may be changed afterwards through a new `MetaCommand::SetNodeCapacity { node, capacity }` (idempotent, and rejected for an unknown node, like `RegisterNodeAddrs`). It is stored in `Metadata` beside the node's labels and shown in `/admin` and `animus node list`.

**Rules in `animus-placement`.** `Candidate` gains `capacity: Capacity` (default = weight 100, no max). Everything stays pure and deterministic; ties break by `NodeId` as today.

1. *Load is normalised.* The unit compared is `fill(n) = replicas(n) / weight(n)` as an exact rational (cross-multiplied integers, no floating point, so every replica computes the same answer).
2. *`select_replicas_balanced`* (new mint) orders eligible candidates by `(fill after adding one, node)` rather than `(count, node)`. With equal weights this is the same order as today (count + 1 over a constant), so existing behaviour and tests are unchanged for clusters that declare nothing.
3. *`replan` and `replan_repair`* exclude from the candidate list any node whose `replicas(n) >= max_replicas(n)`, except that a node already holding a replica of that tablet is never excluded from keeping it (never shrink a set because of capacity). `replan_repair` stays growth-only and best effort.
4. *`rebalance_step`/`rebalance_plan`* move a replica from the node with the highest `fill` to the lowest `fill` when that strictly reduces `max fill - min fill` after the move, never onto a node at `max_replicas`. The convergence statement becomes: with no `SpreadPolicy` and no `max_replicas` binding, the result is within one replica's worth of weight of proportional (`|replicas(n)/weight(n) - replicas(m)/weight(m)|` is at most `1/min_weight` for all pairs). With equal weights this is exactly the existing max - min <= 1. With a spread constraint or a binding `max_replicas`, only monotonic non-worsening and termination hold, as today.
5. *Rebalancing never fills a node past `max_replicas`, and a node already above it (the cap was lowered) is a **source only**: rebalance drains it toward the others at the usual cadence, and nothing forces the move.*

**Total capacity below demand.** Three cases.

- *A new tablet cannot be placed because every eligible node is at `max_replicas`.* **Refuse.** `CreateTable` and split-child placing return a retryable `ResourceExhausted`-style error (on the DynamoDB wire the existing capacity-class error, the exact mapping to be chosen in implementation) naming the cluster-wide shortage. Refusing at creation is the only moment the cluster can say no cleanly; best-effort over-placement would push the overload onto a node the operator told us was full.
- *A repair cannot reach the RF because the surviving nodes are at `max_replicas`.* **Best effort, bounded by the cap.** `replan_repair` grows as far as the caps allow (the existing growth-only, best-effort contract). The tablet stays under-replicated, which `/admin/health` already reports; an alarm gauge `placement_capacity_blocked_tablets` goes non-zero. Over-placing past the cap to restore RF would trade durability for density, but the cap is a placement limit, not a safety limit: the operator can raise it. This is Open question 3.
- *An in-place split.* The fork is placement-blind (ADR 0062): both children inherit the parent's replicas, so the split itself cannot be refused by capacity. The follow-up directed Placing respects the caps. A split that would push a node over `max_replicas` is still allowed (it keeps writes served), the node is over its cap, and rebalance drains it afterwards. This is the one place the cap is soft, and the metric shows it.

`weight` alone never refuses anything: it only shapes where replicas go.

**Interaction with region pinning (ADR 0075).** Capacity is applied inside the pinned candidate set. `replan_pinned` selects one replica per Region from nodes in that Region, so the per-Region choice uses `fill` among that Region's nodes. A Region whose nodes are all at `max_replicas` cannot take a pinned replica: creating or converting a global table is refused (the conversion is all-or-nothing today), and a lost Region's replica still waits rather than being placed elsewhere. Weights are compared only within a Region for pinned tablets; across Regions there is no balancing to preserve (the policy fixes one per Region).

**Operator derivation.** The operator derives `weight` from the pod's CPU request (`weight = max(1, millicores / 100)`, so a 4-core pod is 40 and a 16-core pod is 160) and `max_replicas` from the memory request divided by a per-replica memory budget. The per-replica figure is provisional: about 25 KB for the Raft/host layer (measured with `MemoryEngine`) plus an engine cost that is **not measured**; until an `LsmEngine` run exists the operator's default sets `max_replicas` unset and exposes `spec.nodeCapacity.{weight,maxReplicas}` as explicit overrides only. The derived values are rendered into `cluster.json` (`cluster_settings`-style, additive) and the node advertises them at registration; the operator never edits `Metadata` itself. This keeps the value config, so it stays deterministic and outside `available_parallelism`.

**Default (provisional):** weight 100 for every node, `max_replicas` unset. A homogeneous cluster behaves exactly as today. **Knobs:** `--node-weight` / `node_weight`, `--node-max-replicas` / `node_max_replicas`; operator `spec.nodeCapacity`.

### 2. Awake-group admission (node-local)

**The cap.** `awake_max = A`: the maximum number of awake (non-quiesced) data-plane replicas this node keeps awake. The control-plane group never counts. Learners do count (they receive heartbeats and apply).

**Default (provisional).** `A = 1500 x cores` where `cores` is a **configured** number (flag, or the operator's pod CPU limit rendered into config), never read from `available_parallelism` (ADR 0078, decision 2). Basis: the rig measured about 1900-2000 awake replicas per node-core with no client load (inferred from a one-third split of a saturated 2-thread process); 1500 is a conservative start, and the density measurements doc suggests halving again for a node with client writes. For a 4-core pod this is 6000 awake replicas. The calibration run (implementation plan step 1) replaces this number. `0` disables the cap.

**The cap is soft.** A write, a read through the ReadIndex path, or a peer message to a quiesced group must be served; the cap cannot refuse them forever, or a quiesced tablet becomes unwritable on a busy node. So the cap controls who is *deferred*, not who is refused.

**Past the cap: defer un-quiesce, do not refuse hosting.** Two options were considered.

- *Refuse new hosting past the cap.* Rejected. Hosting is a quiesced-cost action (about 23 KB and about 0.011 core per 1000, measured), the awake cap protects CPU, and a node refusing to host its assigned replicas breaks the replica-set invariants the reconciler and placement rely on (an RF-3 tablet with one replica that will not start is under-replicated for as long as the node is hot). It would also couple layer 2 to layer 1 and make the awake working set a placement input.
- *Defer un-quiesce through a wake queue.* Chosen. A group that is quiesced and would wake is admitted if `awake < A_eff`; otherwise it is queued FIFO on first deferral (the same `(first_deferred_at, stream id)` shape as the campaign queue) and woken when a slot frees (another group quiesces, which it does after `--quiesce-after` of no activity). What deferral means depends on the trigger:
  - *Reconciler wake (a replica marked `Down`, the mass un-quiesce).* Fully deferred and paced by the queue. The group is not leaderless-and-awake while waiting; it stays quiesced, and it still has its leader belief and lease state, so reads and writes route as before until the queue reaches it.
  - *Local client write or peer message.* **Admitted over the cap** and counted as `overcap_wakes`. These are one group per request, naturally bounded by the client admission gate (and by #1296 when that lands). The cap sheds *bulk* wakes, not individual writes.
  - *ADR 0078 exempt campaigns (transfer, `campaign_now`).* Wake over the cap for the same reason.
  A hard-bounded variant is rejected because it needs a refusal path on the write path, which is #1296's problem and would block this ADR on it.

**Lag feedback.** `A_eff = A` normally and is reduced by `LoadMonitor` lag like `K_eff`: `A/2` at lag >= 30 ms, `A/4` at lag >= 100 ms (minimum `A/8`), dead band 10 ms, same decay. This is the safety net for a mis-sized `A`: if the node is late with fewer than `A` groups awake (client load, a noisy neighbour, a slow disk), no new bulk wakes are admitted until lag falls. It does not put anyone to sleep; groups already awake stay awake and quiesce on their own timer. Reducing `A_eff` below the current awake count simply stops admission.

**Interaction with ADR 0078.** The governor caps *concurrent campaigns*; this caps the *awake population*. They compose without a new coupling: the wake queue sits upstream of `on_local_wake`'s election-timer jitter, so fewer groups arrive at the governor at once. Both read the same `LoadMonitor`. A deferred (still quiesced) group holds no campaign permit and no election deadline, so it cannot interfere with the responder logic that makes candidate-side gating deadlock-free. Staggered hosting (`H`) is unchanged: a newly hosted group starts awake, counts toward `awake` and may push the node past `A`; hosting is never refused (above). On a restart this means the first wave can exceed `A` briefly, and the quiesce timer brings it back down. This is intended; Open question 5 asks whether hosting should also respect `A`.

**Interaction with quiescence (ADR 0048).** `--quiesce-after` is untouched. An optional follow-up, not in this ADR, would shorten `quiesce-after` when `awake > A_eff` (shed awake groups faster). It is left out because quiescing a group that is about to receive a write just creates a wake.

**Observability.** `/admin/health` gains `awake`, `awake_max`, `awake_max_effective`, `wake_queue_depth`, `overcap_wakes` (advisory only; per ADR 0078 decision 4, sustained high lag or a full queue never fails the health check or the roll gate).

**Knob:** `--awake-max-per-core` / `awake_max_per_core` (default 1500), and `--cores` / `cores` (configured; required to enable the cap, else the cap is off). `0` disables.

### 3. Background-work budget (node-local)

**Pools.** A `BackgroundBudget` per node, created beside the `CampaignGovernor`, shared by `Arc`, synchronous and `Env`-seam clean (no tasks, time passed in, `BTreeMap`/`BTreeSet`). Permit pools:

| Pool | What takes a permit | Default (provisional) | Basis |
|---|---|---|---|
| `snapshot_streams` | each outgoing `InstallSnapshot` stream and each incoming one (separate counters, same size) | 2 send, 2 receive | **inferred**: 0 snapshots were exercised by the repair-burst measurement |
| `catchup_learners` | each learner being caught up on this node as the receiver | 4 | inferred |
| `splits` | each in-place split this node is the parent leader for, from `CutoverSplit`-equivalent start to children's Placing done | 1 | inferred (no real-load data) |
| `compactions` | each LSM compaction run across all engines on the node | `max(1, cores / 2)` | **not measured** (MemoryEngine rig) |

The defaults are deliberately low and are expected to change after the ProdEnv storm and `LsmEngine` runs in the plan.

**Effective size.** Each pool's `size_eff` is reduced by the same lag rule as `K_eff`: full below 30 ms, half at >= 30 ms, a quarter at >= 100 ms, minimum 1 (a pool never reaches 0, or background work would stall forever and L0 would grow without bound).

**What happens past the cap.**

- *Snapshot and catch-up streams:* a **retryable refusal**, in the style of `MREC_MAX_INFLIGHT_APPLIES`. The sender asks for a permit before starting; the receiver refuses an `InstallSnapshot`/learner start with a retry-after hint when its receive pool is full. The leader's existing retry logic treats a refusal as "not now" (the learner stays a learner; replication to it resumes later). No Raft safety rule changes: a refused snapshot is indistinguishable from a slow follower. The retry interval is the existing replication retry cadence plus jitter drawn from `Env`.
- *Splits:* the auto-split sweep skips a tablet when the pool is full and tries again on its next sweep (`AUTO_SPLIT_INTERVAL`); a manual split request gets the retryable refusal. `AUTO_SPLIT_COOLDOWN` stays per-tablet.
- *Compactions:* the compaction scheduler takes a permit before starting a run and **defers** (re-checks at the maintenance tick) when none is free. Deferral is bounded by an L0 safety valve: when a tablet's L0 table count reaches `2 x compaction_trigger` its compaction **bypasses the pool** (counted `compaction_overcap`), because an unbounded L0 makes reads and eventually writes worse than an extra compaction. This does not add the L0 write stall that is missing today (see Open question 6); it only stops the pool from making L0 growth worse.

**Repair-proposal pacing (control leader).** `reconcile()` currently returns every violating tablet's fix and `reconcile_loop` proposes all of them in one tick. New rule: at most `repair_proposals_per_tick = R` *new* `CasTabletReplicas` repairs are proposed per `RECONCILE_INTERVAL` tick, selected deterministically (ascending `TabletId`, as ADR 0078 orders hosting) from the violating set; the rest are returned to the next tick, where `recently_done` already excludes tablets in flight. Pacing also counts repairs already in flight (a learner being caught up whose promotion is pending): the leader does not start a new repair while `in_flight >= R_inflight`. Defaults (provisional): `R = 8`, `R_inflight = 16`. The leader's `LoadMonitor` lag reduces both by the same rule. Basis: the measured burst (25 proposals/s peak at 12 tablets) shows the shape but not a capacity, so these are **inferred** and set so a 1000-tablet loss takes minutes of ticks, not one burst. `REPAIR_DWELL` is unchanged. Rebalancing keeps its own cadence and still yields to repair.
  - *Liveness bound:* with `V` violating tablets and no failures, all are proposed within `ceil(V / R)` ticks, so a loss of `V = 1000` tablets is fully proposed in `125 x 500 ms` = about 63 s, and the actual restore time is then set by the receive pools. This trades repair latency for not saturating the surviving nodes; durability exposure during that window is the cost and is Open question 4.
  - Because repairs share the leader's node with the data plane, the control leader's own lag counts: a leader that is late proposes fewer.

**Knobs:** `--bg-snapshot-streams`, `--bg-catchup-learners`, `--bg-splits`, `--bg-compactions`, `--repair-proposals-per-tick`, `--repair-inflight-max` and their `cluster_settings` mirrors; `0` disables the matching pool (unbounded, today's behaviour). The operator derives `bg-compactions` from the pod CPU request and passes the others unchanged.

### Resolved dependency: the awake-overload stall (#1297)

At G=3000 awake (about 2x the measured ceiling) the rig left 349 groups leaderless for 10 minutes with `max_term` flat at 3, which looked like a liveness defect. The investigation (ADR 0078's 2026-10-12 amendment) found **accepted overload, not a defect**:
- The governor never starved or leaked: inflight held at `K_eff` and admissions kept rising.
- Responders granted the pre-votes (30,043 grants, 9 refusals).
- 29,099 of those grants arrived after the round's election window had lapsed and were dropped by design (`handle_pre_vote_resp` while `campaign_hold` is set). A dropped grant was 1.7 to 3.4 s old on average, against a round window of 0.77 to 0.88 s: `S_max` caps the stretch at 600 ms on purpose, to bound failover.

Once a saturated node's round trip exceeds the capped window, a cold start cannot elect, and only staying below capacity prevents it. Layer 2 is that guard: `A` keeps the node out of this region. The 1250-1500 ceiling measured on this code stands, and calibration needs no re-measurement for #1297.

### Knobs (summary)

| Knob (flag / `cluster_settings`) | Default (provisional) | Layer | Meaning |
|---|---|---|---|
| `--node-weight` / `node_weight` | 100 | 1 | relative capacity, advertised at registration |
| `--node-max-replicas` / `node_max_replicas` | unset | 1 | absolute hosted-replica ceiling |
| `--awake-max-per-core` | 1500 | 2 | `A` per configured core; `0` disables |
| `--cores` | unset (cap off) | 2, 3 | configured core count; never autodetected |
| `--bg-snapshot-streams` | 2 send / 2 recv | 3 | snapshot permit pools |
| `--bg-catchup-learners` | 4 | 3 | learner catch-up pool |
| `--bg-splits` | 1 | 3 | concurrent splits |
| `--bg-compactions` | `max(1, cores/2)` | 3 | concurrent LSM compactions |
| `--repair-proposals-per-tick` | 8 | 3 | `R` |
| `--repair-inflight-max` | 16 | 3 | `R_inflight` |
| (constants) | lag thresholds 30/100 ms, dead band 10 ms, minimum pool size 1, L0 bypass at `2 x compaction_trigger` | 2, 3 | ADR/code change to alter, like ADR 0078's |

All fixed numbers; none derived from `available_parallelism`. The operator derives `weight`, `maxReplicas`, `cores`, and `bg-compactions` from pod requests when it renders `cluster.json`.

### Metrics

Additive `Metric` variants (no-op under sim): `CpAwakeReplicas` (gauge, high-water), `CpAwakeMaxEffective`, `CpWakeQueueDepth`, `CpWakeDeferred`, `CpWakeOvercap`, `BgPermitInflight{Pool}` (gauge, hwm), `BgPermitDeferred{Pool}`, `BgCompactionOvercap`, `CpRepairDeferred`, `CpRepairInflight`, `PlacementCapacityBlockedTablets`. Today the in-flight learner and snapshot-stream counts are not gauges (only cumulative `CpSnapshot*`/`CpReconfigure*` counters), which is why the measurement could not sample them; the pool gauges fix that as a side effect. C-17 prints matching `C17 metric=` lines.

## ADR 0073 compatibility classification

| Item | Class | Reason |
|---|---|---|
| Node capacity in `Metadata` (`Capacity`, `RegisterNode` field, `SetNodeCapacity` command, `Candidate.capacity` input) | **G (gated)** | A replicated shape: a new field on an existing command and a new `MetaCommand` variant. An older replica that cannot decode it would wedge applying it. New `Gate::NodeCapacity` opening at the next cluster version (5), an exhaustive `required_gate` row for `SetNodeCapacity` (no `_` arm), emit sites check the gate (a node only advertises capacity, and the operator only sets non-default values, once the gate is open; before that a node registers with the old shape), a per-gate test and a mixed-version corpus cell. The `RegisterNode` field is `Option` + `skip_serializing_if`, so the golden fixtures are unchanged. A new `meta` snapshot field gets a version tag and a new fixture, never an edit of an old one. |
| Placement arithmetic with weights / max replicas | L given the gated input | `replan`, `replan_repair` and `rebalance_step` are pure functions evaluated by the control leader; an older leader with no capacity data computes count-balance. But every replica must compute the same answer for `rebalance_step` determinism in the property tests, so the pure functions only read capacity after the gate opens (before then every node is weight 100, which reduces to today's output). |
| Awake cap and wake queue | L | Decides when *this* node un-quiesces a group. No wire or replicated change. |
| Snapshot/learner permit pools | L, with a wire reading | The refusal reuses an existing reply path (retry); if the existing snapshot reply has no "busy, retry" form, a new reply variant would be **G** (name its gate in the implementation; an older sender must treat an unknown refusal as a failure it retries, not a wedge). Verify against the `InstallSnapshot` response shape in implementation step 3; do not assume. |
| Split, compaction pools | L | In-memory, per node. |
| Repair-proposal pacing | L | The control leader proposes fewer `CasTabletReplicas` per tick; every proposal is an existing command. |
| New `cluster_settings` fields, operator spec fields | F-adjacent, additive | Optional, `skip_serializing_if`; the frozen `ClusterConfig` v1 fixture is untouched (ADR 0078 / `max_region_rtt_ms` precedent). |
| `Metric` variants, admin JSON | not a format | Additive. |

Rolling-upgrade note: a mixed cluster before the gate opens behaves as today (weight 100 everywhere); the first node to run the new binary can enforce layers 2 and 3 immediately because they are node-local.

## Test plan

Every distributed behaviour gets a seeded fault-injecting simulation. Three tiers, reusing ADR 0078's `Simulator::set_cpu_cost` (no new sim primitive is proposed, but see Open question 7 about a disk-cost model for compaction).

### Tier 0: pure and property tests

- `animus-placement`: with equal weights every function returns the existing output byte for byte (the full existing property suite unchanged is the guard); with unequal weights `rebalance_plan` converges to within `1/min_weight` of proportional on random clusters, never exceeds `max_replicas` by rebalancing, and terminates; `replan_repair` never shrinks a set; `replan_pinned` with a full Region fails cleanly; `rebalance_step` equals the looped `rebalance_plan` (the existing equivalence test) for random weights.
- Wake queue: FIFO, no starvation while any slot turns over, local-write wake always admitted, `A_eff` as a function of lag.
- `BackgroundBudget`: permit RAII return, `size_eff` as a function of lag and never below 1, lazy expiry backstop (as the campaign permit), L0 bypass.
- Repair pacing: output ascending `TabletId`, never more than `R` per tick, every violating tablet eventually proposed, in-flight limit honoured.
- Gate: `required_gate` exhaustiveness, `SetNodeCapacity` refused before the gate opens, golden fixture for the `meta` shape.

### Tier 1: `SimEnv` corpus, new `animus-cp-data` `tests/it/density_admission_corpus.rs` and an `animus-control` repair-pacing cell set

Knobs `ANIMUS_DENSITY_ADMISSION_SEEDS=K` (default 1), `ANIMUS_DENSITY_ADMISSION_CELL=<substring>`, `ANIMUS_SEED=<seed>`. 3 nodes, `G` RF3 groups, CPU model set so awake capacity is `A`-scaled. Cells:

1. **mass wake**: all groups quiesced, a node marked `Down`, every group's wake requested at once; awake high-water stays at `A_eff` plus over-cap local-write wakes, all groups eventually served.
2. **write to a deferred group**: a quiesced group queued behind a full cap receives a client write; the write is served (soft cap) and counted.
3. **awake overload with the cap vs without**: demand above capacity with `A` set to the measured ceiling vs unlimited; with the cap led count monotone, without it the #1199/#1297 shape (control (a)).
4. **cap and lag**: lag injected on a node with few groups awake; `A_eff` falls, bulk wakes stop, recovers when lag clears.
5. **cap does not break elections**: governor at `K`, wake queue full, leader loss; ADR 0078 cell 10's lost-leader bound still holds.
6. **snapshot storm**: many learners need snapshots at once; in-flight sends/receives high-water at most the pool size; all converge; a refused snapshot is retried, no learner is lost.
7. **split storm**: more eligible tablets than the pool; concurrent splits at most the pool; all eventually split.
8. **compaction under write load** (`LsmEngine<SimEnv>`): concurrent compactions at most the pool until the L0 bypass, never more than the bypass allows; no acked write lost.
9. **node loss repair burst** with data above the snapshot threshold and a placement spread across enough nodes: proposals per tick at most `R`, in-flight at most `R_inflight`, all tablets reach RF within the stated bound, no acked write lost.
10. **heterogeneous nodes**: weights 100/100/200, create tables and add a node; final counts proportional within the bound; a lowered `max_replicas` drains the node without losing a replica.
11. **capacity exhausted**: all nodes at `max_replicas`; table creation is refused with the retryable error; repair is best-effort and the blocked gauge is non-zero; raising the cap resolves it.
12. **region-pinned capacity** (ADR 0075): a Region full; global conversion refused; a lost Region's replica waits.
13. **disabled == baseline**: all knobs `0`/default: the trace of a fixed-seed run equals the pre-change trace.

Asserted on every cell: the Raft safety oracle (no two leaders in a term, no committed write lost), no group permanently leaderless once the fault clears, every permit pool's high-water at most its size (plus counted bypasses), and no pool stuck at zero.

**Negative controls** (each must fail its cell):

- **(a)** all three layers off on cell 3 and 9: reproduces the collapse / the repair burst (the regression proof).
- **(b)** a hard cap that refuses wakes: cell 2 must fail (a write is refused or stalls), proving the soft cap is needed.
- **(c)** no lag reduction: cell 4 must fail (bulk wakes continue under lag).
- **(d)** pool permit leak (no RAII release): cell 6 must show a stuck pool and a learner that never catches up.
- **(e)** pool minimum 0 under lag: cell 8 must fail (L0 grows without bound).
- **(f)** repair pacing without the in-flight limit: cell 9's in-flight bound must fail.
- **(g)** count-balance ignoring weights: cell 10 must fail the proportionality bound.
- **(h)** best-effort over-placement past `max_replicas` on creation: cell 11 must fail (a node over its cap).

### Tier 2: cluster tier, `animusd` `sim_cluster_scale` (C-17)

New `C17 metric=` lines: `awake_hwm`, `wake_deferred_total`, `bg_snapshot_hwm`, `bg_split_hwm`, `bg_compaction_hwm`, `repair_deferred_total`, `repair_inflight_hwm`, `capacity_blocked_tablets`. Structural assertions only. The `sim_cluster_scale_node_loss_repair_burst` cell from `795465c` gains pre-loaded tablets (data above the snapshot threshold) and a spread placement, as `docs/density-measurements.md` section C says it needs, and graduates from `#[ignore]`.

### Tier 3: `ProdEnv` calibration and liveness

- Reuse `density_repro_g3000_holds_led_count` with `ANIMUS_BP_LIVE_QUIESCE=0` and the cap on: G=3000 must hold the led count (the #1297 shape must not appear below `A`).
- The calibration the doc lists as follow-ups: a `worker_threads` sweep for the true per-core figure; an `LsmEngine` variant (fds, memory per hosted tablet, compaction); a loaded variant with client writes; a data-bearing node-loss run with real snapshots; a ProdEnv split storm. Their results set the final defaults (in particular `A`, the pool sizes, `R`, and the operator's `max_replicas` derivation).

## Alternatives considered

- **Hosted-replica cap as a hard refusal of hosting.** Rejected for layer 2 (above); for layer 1 it exists, but only as a *placement* limit, so no node is ever told to host what it cannot.
- **Auto-size `A` and the pools from `available_parallelism`.** Rejected: nondeterministic input outside the `Env` seam, harder to read in `/admin`, and ADR 0078 decision 2 rules it out. The operator derives the number from pod requests and renders it as config.
- **Lag feedback only, no fixed cap.** A feedback-only limit reacts after the node is already late and needs the lag to appear first (the measurements show led-count dips and `max_term` growth right at saturation). The fixed value is the readable bound; the feedback is the safety net. ADR 0078's reasoning, repeated.
- **A hard cap that refuses client writes to deferred groups.** Needs the client-path refusal machinery of #1296 and makes a quiesced tablet unwritable on a busy node. Rejected; the cap is soft.
- **Shorten `--quiesce-after` instead of an admission cap.** Cannot cap a burst (the wake happens before any timer), and quiescing a soon-to-be-written group creates a wake. Complementary, not a substitute.
- **Replicated, cluster-wide background-work tokens through the control plane.** Puts consensus state on the data plane's failure path and needs a gate; the resource is one node's CPU and disk, so a node-local pool is right. (The one control-plane pacing rule, repair proposals, lives where the proposals are made.)
- **A token bucket for repairs (rate) rather than a per-tick count plus in-flight limit.** A fixed rate is wrong in both directions, as in ADR 0078; the in-flight limit self-scales with how long repairs take.
- **Weighted placement by observed load (CPU, bytes) instead of declared weight.** Reactive and unstable (a rebalance changes the load it measures), and needs per-node load in `Metadata`. Declared capacity is stable, deterministic and readable. Observed-load-aware rebalancing can build on it later.
- **A write stall on L0 count and a compaction IO rate limit now.** Both are storage-engine features of their own; they are listed as a gap (Open question 6) and kept out of this ADR so the permit pool does not become a storage redesign.

## Implementation plan

One implementation PR on one branch (per the repo's operating mode), ordered commits, each green on all gates. This ADR is its own docs-only PR first.

1. **Calibration harness first** (no product change except test knobs): worker-thread sweep, `LsmEngine` variant, loaded variant, data-bearing node-loss cell, split-storm run. Record results in `docs/density-measurements.md` and set the defaults. Negative-control (a) repros on cells 3 and 9.
2. **Pure functions**: `animus-placement` `Capacity` and weighted `select_replicas_balanced`/`replan`/`replan_repair`/`rebalance_*`; the equal-weights-equals-today guard; property tests.
3. **Gated metadata shape**: `Gate::NodeCapacity`, `RegisterNode` field, `SetNodeCapacity`, `required_gate` row, golden fixture, mixed-version cell, admin/CLI display, operator derivation. Resolve the snapshot-refusal wire question here.
4. **Awake-group admission**: wake queue, `A_eff`, wiring beside the reconciler's `wake()` and `on_local_wake`; cells 1-5, controls (b), (c).
5. **Background budget**: `BackgroundBudget`, snapshot/catch-up refusal, split and compaction pools, L0 bypass; cells 6-8, controls (d), (e).
6. **Repair pacing** in `reconcile_loop`; cell 9, control (f).
7. **Capacity cells** 10-12, controls (g), (h); Tier 2 metrics and assertions.
8. **Docs**: this ADR to Accepted/implemented with an as-built note; ADR 0044 amendment (the awake-cap line); `crates/animus-placement/CLAUDE.md`, `animus-control`, `animus-cp-data`, `animus-operator` guides; `docs/roadmap.md` (C-17, #1295); `website/` if it states placement or density claims; `docs/lessons/` entries.

## Open questions for the maintainer

1. **Is a declared per-node `weight` plus an optional `max_replicas` the right capacity shape?** Recommended: yes, both. Weight only shapes placement and never refuses; `max_replicas` is the explicit ceiling, default unset so nothing changes for a homogeneous cluster.
2. **Should the operator set `max_replicas` by default from the memory request?** Recommended: no, not until an `LsmEngine` run measures per-tablet memory and fds. Expose `spec.nodeCapacity` overrides now and leave the derived default for weight only (from the CPU request).
3. **Past `max_replicas`, should repair over-place to restore RF?** Recommended: no. Table creation is refused, repair is best-effort within the caps, and `placement_capacity_blocked_tablets` pages the operator, who can raise the cap. The cap is a placement limit, so an operator who prefers durability over density raises it. Alternative: a per-cluster `capacity_overflow: allow_for_repair` setting that lets repair (only) exceed the cap.
4. **Repair pacing: is a longer time-to-full-RF after a large node loss acceptable** (a 1000-tablet loss takes about 63 s to propose at `R` = 8, longer to finish)? Recommended: yes, with `R` raised by the operator for clusters that want the opposite trade. Alternative: pace only when the leader's lag is high (feedback only).
5. **Should hosting also respect `A`** (the first wave after a restart can exceed it)? Recommended: no; ADR 0078's stagger already bounds the ramp, and refusing to host breaks replica-set invariants.
6. **Is an L0 write stall and a compaction IO rate limit in scope for a later ADR?** Recommended: yes, as a storage-engine ADR; this one only adds the permit pool and the L0 bypass.
7. **Does the test plan need a disk-cost model in `animus-sim`** (analogous to `set_cpu_cost`) to test compaction pacing under SimEnv? Recommended: no for this ADR (cell 8 asserts the pool bound and no data loss, not latency); revisit if the ProdEnv compaction run shows IO is the first saturated resource.
8. **#1297 (resolved):** diagnosed as accepted overload (see "Resolved dependency"). No gate on this ADR remains; it is kept here only so the numbering of the other questions does not change.
9. **Should the permit pools and `LoadMonitor` input be shared with #1296's client-path admission** (one lag-reduction rule for the request gate as well)? Recommended: yes, a single `lag -> factor` helper used by both, with the client-gate wiring left to #1296's ADR.
10. **Cluster version.** Recommended: a new `Gate::NodeCapacity` at cluster version 5, finalised with the rest of the release, rather than folding capacity into an existing gate.
