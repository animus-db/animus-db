# ADR 0079 — Per-node density and the background-work budget: placement capacity, awake-group admission, background permit pools

- **Status:** Accepted (2026-10-12), implemented (2026-10-14). The maintainer settled the ten open questions on 2026-10-12, taking the recommendation on each (see "Maintainer decisions"). The defaults were calibrated in step 1 (`docs/density-measurements.md`); see "As built (consolidated)".
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
- *Splits:* the auto-split sweep skips a tablet when the pool is full and tries again on its next sweep (`AUTO_SPLIT_INTERVAL`); an explicit split (admin endpoint, `SplitTablet`, stream grow) **queues for the permit** (FIFO, cancellation-safe, polled on `Env` time) for up to `SPLIT_WAIT_BOUND` (3 x `CLIENT_TIMEOUT`, 30 s) and only then gets the retryable `SPLIT_BUSY`. Refusing an operator who asked for N splits in a row would make every back-to-back caller (stream grow, scripts, tests) write its own retry loop. `AUTO_SPLIT_COOLDOWN` stays per-tablet.
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

## Maintainer decisions (2026-10-12)

The maintainer accepted the recommendation on every open question below:

1. **Capacity shape.** Each node declares a `weight` (default 100) plus an optional `max_replicas` (default unset). Weight only shapes placement and never refuses. `max_replicas` is the explicit hard ceiling.
2. **No derived `max_replicas` yet.** The operator derives `weight` from the CPU request only. `max_replicas` is set by hand through `spec.nodeCapacity` until an `LsmEngine` run measures the per-tablet memory and file-descriptor cost.
3. **The cap is strict under repair.** Repair never over-places past `max_replicas`. Table creation is refused, repair is best-effort within the caps, and `placement_capacity_blocked_tablets` alerts the operator, who raises the cap. There is no `capacity_overflow` setting.
4. **Fixed repair pacing.** `R = 8` new repairs per tick and `R_inflight = 16`, both operator-tunable. Pacing does not depend on lag, because a node loss produces its burst before the lag rises.
5. **Hosting does not respect `A`.** ADR 0078's stagger bounds the post-restart ramp. Hosting is never refused.
6. **The L0 write stall and the compaction IO rate limit go to a separate storage-engine ADR** (#1300). This ADR keeps only the compaction permit pool and its L0 bypass.
7. **No `SimEnv` disk-cost model for now.** Revisit only if the `ProdEnv` `LsmEngine` run shows IO saturates first.
8. **#1297** was resolved as accepted overload. It does not gate this ADR.
9. **One shared `lag -> factor` helper** for `K_eff`, `A_eff` and the permit pools. #1296's client-path admission reuses it; that wiring belongs to #1296's own ADR.
10. **A dedicated `Gate::NodeCapacity` at cluster version 5**, finalised with the release.

## Open questions for the maintainer (as originally posed)

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

## As built (consolidated, 2026-10-14)

All eight implementation steps landed on one branch. Layer 1, declared per-node
placement capacity: `animus-placement` `Capacity` and weighted `replan`,
`replan_repair` and `rebalance_step` (equal weights are byte-identical to the
old behaviour); `Gate::NodeCapacity` (cluster version 5, `MAX_SUPPORTED = 5`)
gates `Capacity` in `Metadata`, the `RegisterNode` field and `SetNodeCapacity`;
`--node-weight` / `--node-max-replicas`, `GET /admin/capacity`, `animus cluster
capacity`, a creation refusal and a blocked gauge when no node fits, and the
operator derives capacity from pod resources. Layer 2, awake-group admission
(node-local, L): a soft cap of `--awake-max-per-core` (default 1500) times
`--cores` on awake data-plane groups, a wake queue and `LoadMonitor` lag
reduction, which defers un-quiesce rather than refusing hosting; the operator
derives `cores`. Layer 3, the background-work budget (L): `BackgroundBudget`
permit pools for snapshot and catch-up streams, splits and compactions with a
retryable refusal and an L0 bypass, plus repair-proposal pacing in the control
leader's `reconcile_loop`. Proofs: `capacity_corpus` and `repair_pacing`
(`animus-control`) and `density_admission_corpus` (`animus-cp-data`), each with
its negative controls; Tier 2 `C17` density lines. Deferred: #1300 (L0 stall),
#1303, #1304, #1305, #1306, and the operator `nofile` term. The per-step notes
below are the detail.

## Implementation step 3 as built (2026-10-13)

Step 3 shipped `Gate::NodeCapacity` (cluster version 5), `Capacity` in
`Metadata` (`node_capacity`, mirror kind `node_capacity`), the `RegisterNode`
`capacity` field and `SetNodeCapacity`; the control leader's repair, rebalance
and directed-Placing passes feed declared capacity and per-node replica counts
into the step 2 `*_with_load` functions only once the gate is open (gate closed
or nothing declared: the original functions, byte for byte); `--node-weight` /
`--node-max-replicas` (`cluster_settings.node_weight` / `node_max_replicas`);
`GET /admin/capacity` and `animus cluster capacity`; and the operator
derivation. The compatibility record is ADR 0073's 2026-10-13 amendment.

- **A node advertises through a sync loop, not at registration.**
  `RegisterNode.capacity` exists, is gated by content and is applied
  fill-if-empty, but `animusd` registers with `None` (the old shape, so a join
  into a cluster below version 5 stays valid) and `ClientCtx::sync_node_capacity`
  proposes `SetNodeCapacity` once the node is a member and the gate is open.
  The process configuration is the source of truth: a restart with a changed
  flag overwrites, and a node that declares nothing clears a leftover entry.
- **`CutoverSplit` target.** The apply-time directed-Placing target uses
  `select_replicas_balanced` over the pre-fork replica counts when capacity is
  in effect (still a pure function of replicated state), so a child's target
  respects weights and ceilings; the fork itself remains placement-blind.
- **Table creation refusal (section 1, "Total capacity below demand") is not
  in this step.** `CreateTable` placement still goes through
  `select_replicas`; refusing a new tablet when every eligible node is at
  `max_replicas` and the `placement_capacity_blocked_tablets` gauge are step 7
  (cells 11-12), together with the retryable wire error mapping.
- **Operator derivation (maintainer instruction of 2026-10-13, superseding open
  question 2's "weight only"):** `weight = max(1, cpu_millicores / 100)` and
  `max_replicas = min(0.5 * memory_request / 64 KiB, 1000)` from
  `spec.resources.requests` (`docs/density-measurements.md`, 2026-10-13), each
  overridden field by field by `spec.nodeCapacity.{weight,maxReplicas}`; absent
  requests render neither key. The `nofile / 2` term is not applied (the
  operator cannot see a container's `nofile`, a runtime default far above the
  cap), and the constant 1000 is the temporary hosting-time cap pending #1303.

### The snapshot-refusal wire question (resolved)

The classification table left open whether a full receive pool can refuse an
`InstallSnapshot` through an existing reply. Read against the code:
`InstallSnapshotResp { term, last_index, next_offset }` has **no busy form**.
The one refusal-shaped reply, `(last_index 0, next_offset 0)` to an offset-0
chunk, means "I already hold state through your offer's base": the leader
forgets the transfer, marks that base as *served* and probes with
`AppendEntries` (`handle_install_snapshot_resp`), which is exactly wrong for
"not now" (a refused base would never be re-offered until the base moved).
**Decision (the ADR's recommended direction: reuse the existing path, add no
wire shape): the receiver refuses by not acknowledging the first chunk of an
offer** (no reply at all, and it buffers nothing). The sender already treats
a silent peer as slow: the offset-0 chunk is resent by the heartbeat `Backoff`
schedule and the ack-driven capped resend, "a stuck transfer is never
permanently silenced". This is class **L** (receiver-local, no new variant, no
gate, an older sender is unaffected because it already tolerates silence). The
cost is that there is no retry-after hint: the retry cadence is the existing
backoff, and step 5 must release the *sender's* permit when its transfer sits
unacknowledged past a bound, rather than rely on a refusal to do it. If step 5's
measurements show the backoff too slow, a `Busy` reply variant would then be
class **G** under its own gate, with an older sender treating an unknown
variant as a failure it retries; that is a deliberate later amendment, not
part of this step.

## Implementation step 4 as built (2026-10-13): awake-group admission

Layer 2 shipped as `animus-cp-data` `awake_admission.rs` (`AwakeAdmission`,
`ElectionBackpressure.awake`, attached to the `LoadMonitor`), the
`RaftKvNode::wake_bulk()` split of the wake signal, `--cores` /
`--awake-max-per-core` (`cluster_settings.cores` / `awake_max_per_core`,
`--config`/`--node` only, same merge rule as `--node-weight`), five `Metric`
variants (`CpAwakeReplicas`, `CpAwakeMaxEffective`, `CpWakeQueueDepth`,
`CpWakeDeferred`, `CpWakeOvercap`), the `/admin/health` fields (`awake`,
`awake_max_effective`, `wake_queue_depth`, `wake_deferred`, `overcap_wakes`;
advisory), and the Tier 1 corpus `density_admission_corpus.rs` (knob
`ANIMUS_DENSITY_SEEDS`, not the `ANIMUS_DENSITY_ADMISSION_SEEDS` the Tier 1
section named). Class **L** as classified: no wire or replicated change, no gate.

- **Cap off unless `--cores` is set** (never autodetected); `--awake-max-per-core 0`
  also disables it. A node without it runs the pre-step driver (`awake` is `None`).
  `animusd`'s `SimCluster` assembly passes `None`, so every cluster-tier test is
  unchanged.
- **Shared lag rule (decision Q9).** `load_monitor::lag_divisor` / `scale_by_lag`
  is the one lag-to-factor function; `k_eff` and `a_eff` both call it. The ADR's
  "minimum `A/8`" parenthetical is superseded by the shared rule (`A/2` at 30 ms,
  `A/4` at 100 ms, never below 1): the governor's `K_eff` has no `K/8` tier and
  one function was the point. Equivalence of the refactor is a test
  (`k_eff_is_behaviour_identical_to_the_pre_shared_helper_body`): the old body,
  verbatim, against `k_eff` over 4000 lags x 75 `k` values.
- **What is deferred.** Only the host reconciler's proactive `Down` wake, now
  `wake_bulk()`, and only for a quiesced **follower** (`on_local_wake` is a no-op
  for a leader). A deferred group stays quiesced, queued FIFO, and is woken by
  the queue callback; the reconciler re-wakes each tick, which is the backstop
  for a lost callback. The client-facing `wake()` carries its own `direct` flag
  that wins over a coalesced bulk wake (found by cell 1: without it a client
  wake of a queued group was deferred, see
  `docs/lessons/testing/2026-10-13-a-deferral-keyed-on-a-coalesced-flag-loses-the-direct-wake.md`).
- **Soft cap.** A client write, a peer message (pre-vote, `AppendEntries`,
  `WakeRequest`) and a campaign un-quiesce inside the core and are never refused:
  the driver reports each quiescence change, and one that was not an admitted bulk
  wake and leaves the node past `A_eff` is counted (`CpWakeOvercap`). Hosting
  registers awake and exempt (decision Q5), with an RAII membership that returns
  the slot on any exit of the driver.
- **Measured behaviour worth knowing (Tier 1, `G` = 60, `A` = 6).** Peer-message
  wakes make the cap leaky by design, but mildly: after a node loss the bulk-wake
  awake high-water was 10 against 60 uncapped (`admit_awake_hwm` is exactly
  `A_eff`; the rest is counted overcap). In the CPU-model cell the capped run's
  worst scheduling lag was 115-630 ms against 4.8-5.7 s uncapped.
- **A woken group does not always give its slot back, so a lost leader bypasses
  the cap (follow-up, built).** A leader cannot quiesce while any voter is behind
  (`quiesce_entry_ok`), so with a node *killed* the groups a bulk wake admits stay
  awake until the dead replica is replaced and the slots do not turn over. The
  first version therefore left every queued group quiesced, and for a group whose
  *leader* was on the dead node that meant no election and no repair until a client
  touched it (a breach of ADR 0078's lost-leader bound). Now the reconciler calls
  `RaftKvNode::wake_bulk_leader_lost()` instead of `wake_bulk()` when the replica's
  own `leader()` belief (the raw `leader_id` a quiesced follower keeps; a Raft member
  id, which is the node id in `view.down`; no new wire) is in `view.down`. The
  driver then skips the admission ask: the group wakes **over the cap**, is counted
  in `CpWakeOvercap` when it lands past `A_eff`, and its election stays paced by the
  campaign governor `K`. A group whose Down replica is only a follower keeps its
  leader, stays deferrable, and its re-replication waits for the queue. The cost is
  that the over-cap population after a node loss is the number of groups the dead
  node led (about `G/N`), not `A`. **Cell 3's old assertion ("capped lag several times
  calmer than uncapped") was root-caused and replaced, not weakened blindly**
  (seed `0x5b12e826afff8b4a`, node 1, 60 s overload): the capped run's lag is higher
  (8-21 s against 4.7 s) because it *elects* the lost-leader groups (53 of 60 led after
  60 s, 41 uncapped, whose lost groups starve and never elect); admission churn is
  not the cause (3 admits, 20 queued, no re-queues, campaign admits about 6/s in both
  runs). Node 1's message backlog grew about 150/s capped against about 22/s uncapped,
  i.e. arrivals of about 447/s against 317/s, which is about 8.4 and 7.7 per led
  group: the extra load is the heartbeat and vote traffic of 12 more live leaders. The
  cell now asserts the capped run leads no fewer groups under load than the uncapped
  one and reports lag and backlog; cell 1's high-water bound is `4/5` of the uncapped
  run's (the bypassed third is awake by design). Corpus: cells 1 and 3
  lost their post-bulk client wake and assert that every group led by the killed node
  gets a live leader with no client touch; negative control (d)
  (`AwakeFault::NoLostLeaderBypass`) fails that oracle. Open: once repair replaces
  the dead voter the awake groups can quiesce and the queue drain; cell 1 keeps node
  0 dead, so that drain is only covered by cell 4 (a live node marked `Down`).
- **Cell 5's bound is per campaign attempt.** ADR 0078's lost-leader bound is
  missed on reseeded variants by one `2 * base` window when a pre-vote round
  fails; the same happens with no awake cap (#1306). Cell 5 asserts the literal
  bound times the number of campaign attempts the survivors were admitted for.
- **Tier 3.** `ANIMUS_BP_LIVE_AWAKE_MAX=A` turns the cap on in the ProdEnv
  liveness rig. Because hosting is exempt (decision Q5) and
  `ANIMUS_BP_LIVE_QUIESCE=0` never quiesces, the cap has nothing to defer in
  `density_repro_g3000_holds_led_count`: the #1297 shape at G=3000 awake is
  guarded only by ADR 0078's stagger, which is what Open question 5 recommended.
- **Not in this step:** operator rendering of `cores` from the pod CPU limit
  (with the rest of the operator derivation), the Tier 1 cell 3 "control (a)"
  collapse arm (belongs with steps 5-7), and a `SimCluster` hook for the C-17
  `awake_hwm` line.

## Implementation step 5 as built (2026-10-13): background budget

Classification: **L** throughout (no wire shape, no replicated state, no durable
format; a refused snapshot chunk is simply not acknowledged, as step 3 decided).
No `Gate`.

- **Mechanism.** `animus_cp_data::background_budget` (`BackgroundBudget`,
  `BgPermit`, `BgFlow`, `SplitPermits`, `BudgetSizes`, `BudgetFault`): one
  synchronous, `Env`-seam-clean budget per physical node, shared by `Arc` with
  the hosted groups (a per-group `BgFlow` filters inbound `InstallSnapshot`
  chunks / learner traffic and outbound snapshot chunks in the raftkv driver),
  with the split path (`ClientCtx::trigger_split`, auto-split sweeps skip a
  tablet whose pool is full and ask again next sweep; an explicit split waits FIFO for
  the permit, up to `SPLIT_WAIT_BOUND`, then gets the retryable `SPLIT_BUSY`) and with the tablet engines (`animus-storage`
  `CompactionGate`, installed by `LsmTabletFactory`). Permits are RAII (error,
  cancel, drop all return them) and may carry an idle TTL that a full pool reaps
  lazily at the next acquire (no timer). Effective sizes follow node scheduling
  lag through the shared `scale_by_lag` and are never below 1.
- **Defaults.** Snapshot send 2 and receive 2 (`--bg-snapshot-streams`), learner
  catch-up 4 (`--bg-catchup-learners`) and splits 1 (`--bg-splits`) are **on by
  default**; compactions (`--bg-compactions`) default to `max(1, cores/2)` when
  `--cores` is set and to **off** (today's behaviour) when it is not. `0` turns
  any pool off. Each flag has a `cluster_settings.bg_*` mirror, node-local, like
  step 4's `cores`.
- **L0 bypass.** An engine whose L0 holds `>= 2 x compaction_trigger` tables is
  granted a compaction slot over the cap (counted in `BgCompactionOvercap`); a
  deferred engine re-asks every `COMPACTION_DEFER_RETRY` (100 ms, one pending
  retry per engine). Issue #1300 (a storage-level L0 write stall / compaction IO
  limit) is untouched.
- **The unacknowledged-transfer bound.** A receiver with a full pool refuses by
  not acknowledging the first chunk (step 3). The sender releases its send permit
  when a transfer sits unacknowledged for `snapshot_unacked_bound(heartbeat) =
  heartbeat * 2 * SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS` (64 ticks): by then the
  existing backoff (gaps 1, 2, 4 .. 32, then every 32nd tick) has offered the
  first chunk about seven times, so silence means a dead, partitioned or refusing
  receiver. The released peer sits out one further bound before it may ask again,
  so a never-ready receiver cannot starve the other sends. The receiver's own slot
  (and a learner's catch-up slot) is reaped after twice that bound of leader
  silence. Both bounds derive from the existing timing constants; none is new
  wall-clock state.
- **Observability.** Metrics `BgPermitInflight{Pool}` (gauge + hwm),
  `BgPermitDeferred{Pool}`, `BgCompactionOvercap`; `/admin/health` gains a
  `bg_permits` object (per pool `inflight`/`hwm`/`deferred`, plus
  `compaction_overcap`) beside step 4's awake fields. `SimCluster` runs with every
  pool off.
- **Corpus** (`density_admission_corpus.rs`, `ANIMUS_DENSITY_SEEDS`): cell 6
  (snapshot storm: send/receive high-water within the pools, every group
  converges), **6b** (an unacknowledged transfer releases its permit within the
  bound and the next group's learner is served), cell 7 (split storm: at most one
  split in progress, all complete), cell 8 (compaction under write load: pool
  cap held, L0 bounded, bypass counted). Negative controls are named
  `control_bg_*` so they do not clash with the step 4 control (d) (lost-leader
  bypass): **bg_d** (`LeakPermits`, ADR control (d): the catch-up pool fills with
  never-returned permits and learners starve), **bg_d2** (`NoIdleBound`: a dead
  receiver keeps the only send slot, the next group starves), **bg_e**
  (`PoolMinZero`, ADR control (e): a pool allowed to reach 0 under lag stalls
  compaction). Each asserts the oracle FAILS with its mechanism off.

## Implementation step 6 as built (2026-10-14): repair pacing

Classification: **L** (a control-leader-local computation over replicated
state; every proposal is an existing `CasTabletReplicas`). No `Gate`, no wire
or durable shape.

- **Mechanism.** `animus_control::repair_pacing` (`RepairPacer`, `RepairPacing`,
  `RepairPacingHandle`) sits between the pure `PlacementView::reconcile` and the
  propose loop in `reconcile_loop` (`node.rs`). `RaftNode::set_repair_pacing`
  sets it after `start`; defaults are on (`R` = 8, `R_inflight` = 16), so every
  sim and `SimCluster` is paced. `0` turns the matching limit off (both `0` =
  the pre-ADR behaviour). `animusd`: `--repair-proposals-per-tick`,
  `--repair-inflight-max` and `cluster_settings.repair_proposals_per_tick` /
  `repair_inflight_max` (`--config`/`--node` only, like the other 0079 knobs),
  applied to the combined and control-only assemblies (the data-only node has no
  control leader).
- **Ordering.** Ascending `TabletId` (as ADR 0078 orders hosting), re-sorted in
  the pacer; the first `R` that fit under the in-flight cap are proposed, the
  rest are deferred and simply come back from `reconcile` on the next tick
  (their tablets still violate). Q4 (decision): a fixed cadence, not adaptive;
  the leader-lag reduction of `R` the section above mentions is **not**
  built (the section 4 `LoadMonitor` factor is data-plane local and the
  decision is the fixed cadence). Q3 (decision): `max_replicas` stays strict
  under repair (step 3's `replan_repair_with_load`; nothing here relaxes it).
- **In flight, defined (decision 2026-10-14, "observable end": a repair is
  done when its replica set is realised).** A repair is in flight from the
  moment its `CasTabletReplicas` is *accepted* by the leader's core until the
  first of: (1) **settled**: the tablet's `settled_epoch` equals its current
  epoch and that epoch is past the tracked `expected_epoch` (the CAS applied and
  the data plane reported the resulting replica set realised, below); (2)
  `REPAIR_INFLIGHT_TTL` (= `REPAIR_DWELL`, 5 s, an existing constant) elapsed,
  the safety net for a repair that never settles and the only bound while the
  gate is closed; (3) its tablet leaves the tablet map; (4) the leader loses
  leadership (the tracker is dropped; a new leader starts empty, as before). A
  proposal whose CAS has not applied yet (the view's epoch still equals the
  tracked `expected_epoch`) is a *retry* of the same repair: it passes through
  free and keeps its original start time. A repair superseded by another CAS is
  freed by the same rule: the tablet's current epoch is settled, or the TTL
  runs out.
- **The completion report (`MetaCommand::RepairSettled { tablet, epoch }`).**
  The control plane cannot see a learner caught up: `Metadata` holds only the
  *desired* replica set (the CAS writes it at once) and the learner/voter
  phase lives in the tablet's own Raft group. So the tablet's data-plane leader
  reports it: `animusd::repair_settled` ticks every 200 ms, and for each led
  tablet whose epoch is past `INITIAL`, whose group's voter set equals the
  `Metadata` replica set and which has no learner left (every target promoted,
  caught up by the promotion criterion), proposes `RepairSettled` through the
  relay path (at most 16 per tick; an unconfirmed report is retried after 2 s).
  Apply: epoch-CAS (a stale epoch is `Rejected`, an unknown tablet too),
  idempotent (`NoOp` when already recorded), sets the tablet's new
  `settled_epoch: Option<Epoch>` (`skip_serializing_if` none, so every
  existing encoding and fixture is byte-identical). Rebalance moves and split
  placings also report (one extra small command per placement change); the
  pacer ignores them.
- **Classification (ADR 0073): G, behind `Gate::NodeCapacity`, no new cluster
  version.** `required_gate` has an exhaustive row naming `NodeCapacity`; the
  loop emits nothing while `Metadata::capacity_gate_open()` is false (so the
  pacer keeps the TTL-only behaviour exactly), and `RaftNode::propose` refuses
  it while closed. v5 is unreleased, so **the v5 era covers both the capacity
  metadata and the repair-settled report**: a binary that can decode one can
  decode the other, so a second gate would only add a version no deployment can
  sit at. Fixture `metadata/v1-repair-settled.json` (new), shape pinned by
  `repair_settled_wire_shapes_are_pinned`.
- **Throughput.** Sustained repair throughput is bounded by `R` per tick and
  by real completion (a slot is held for the learner's actual catch-up, not 5
  s): `min(R / tick, R_inflight / completion time)`. Measured in the
  `animus-control` cell (30 violating tablets, simulated promotion 300 ms after
  the CAS): all repairs commit 7.6 s after the kill (5 s dwell + 2.6 s) against
  11.3 s with the TTL alone, in-flight high-water 8 against 16. At `R` = 8 per
  500 ms tick a 1000-tablet loss proposes in about 63 s (it was about 5 minutes
  under the TTL-only definition, `R_inflight / TTL` = 3.2/s); slow real
  catch-ups lengthen it, bounded by the TTL, and the data plane's own pools
  (step 5) remain the real limiter on bytes moved. While the gate is closed
  (clusters below version 5) the TTL-only bound still applies; raise
  `--repair-inflight-max` for a faster burst there.
- **Metrics.** `CpRepairDeferred` (counter, per deferred tablet per tick),
  `CpRepairInflight` (level), `CpRepairInflightHwm`; the C-17
  `sim_cluster_scale_node_loss_*` cells print `repair_deferred_total` and
  `repair_inflight_hwm`, and the per-push cell asserts the high-water is within
  `1..=16`. Rebalancing still yields while repairs are deferred.
- **Corpus** (`animus-control` `tests/it/repair_pacing.rs`, knob
  `ANIMUS_REPAIR_PACING_SEEDS`; cells `cell9_settled_report_frees_slots_before_the_ttl`, `cell9_unsettled_repairs_are_freed_by_the_ttl`, negative control (g) completion detection off, plus `animusd` `sim_cluster_repair_settled`; 60 RF3 tablets over 6 nodes, one node killed,
  the real `reconcile_loop`): cell 9 (default pacing: at most `R` repairs
  committed per tick window, in-flight high-water at most `R_inflight`, all
  tablets RF3 within `REPAIR_DWELL + ceil(V/slots) * (TTL + tick) + 5 s`, the
  in-flight gauge returns to 0 after the repairs age out) and 9b (`R_inflight` 4
  binds, still converges). Negative controls: **(a)** pacing off, the paced
  oracle fails (30 repairs in one window); **(f)** `R` on and the in-flight limit
  off, the `R_inflight` = 4 oracle fails (high-water 30). The cell's "no acked
  write lost, data above the snapshot threshold" half is the animusd
  `sim_cluster_scale_node_loss_with_data_installs_snapshots` cell (no client
  writes during the repair), since the pure control-plane rig has no data plane.


## Implementation step 7 as built (2026-10-14): capacity cells, creation refusal, Tier 2 lines, operator `cores`

Classification: **L** throughout (control-leader-local gauge, wire-edge refusal
and test code). No `Gate`, no wire or durable shape; `Metric::
PlacementCapacityBlockedTablets` is an additive appended variant.

- **Creation refusal (section 1, "Total capacity below demand").**
  `Metadata::capacity_initial_replicas(policy)` (`animus-control`) is the one
  capacity-aware first placement: `None` when capacity is not in effect (gate
  closed or nothing declared, so today's pick is byte-for-byte unchanged),
  otherwise `select_replicas_balanced` over the `Active` members (weighted
  fill, full nodes dropped). **Every `Active` node at `max_replicas` is
  `CapacityExhausted`; some but fewer than RF nodes with room mints on those**
  (best effort, the bootstrap rule, and the blocked gauge reports it).
  `animusd` `zone_aware_initial_replicas` calls it first. Wire mapping:
  **`ServiceUnavailable` (HTTP 503)**, the documented retryable code every SDK
  backs off and retries, with a message naming the shortage ("no capacity for a
  new tablet: all N active node(s) are at their max_replicas; raise a node's
  max_replicas or add a node; retry"). `dynamo::internal` maps the
  `schema::CAPACITY_EXHAUSTED_REFUSAL` prefix, so `CreateTable` and the lazy
  first-write provisioning share it. **`CreateTable` checks before it commits
  the schema**, so a refusal leaves no schema-without-tablet behind (a retry would
  otherwise answer `ResourceInUseException`); `provision_tablet` re-checks and
  returns at once instead of waiting out `SCHEMA_COMMIT_TIMEOUT`. The
  region-pinned conversion (`UpdateTable ReplicaUpdates`) is refused the same
  way when `Metadata::regions_without_capacity` names a Region whose `Active`
  nodes are all at their ceiling. Not covered: `ImportTable`/`RestoreTable`
  destination tablets still use the first-N pick (they mint through
  `active_replicas_for_new_tablet`); the split path stays soft by design.
- **`PlacementCapacityBlockedTablets`** (level, control leader, recomputed each
  reconcile tick; `0` on a non-leader and whenever no node declares a ceiling): an
  `Active` tablet with fewer live replicas than its policy RF, no admissible node
  outside its set with room, and at least one admissible node full. The in-place
  split's soft over-cap case (section 1, third bullet) is **not** in this gauge
  (that tablet is fully replicated; the node is over, not the tablet blocked);
  a node above its ceiling is visible in `/admin/capacity`.
- **Cells** (`animus-control` `tests/it/capacity_corpus.rs`, knob
  `ANIMUS_CAPACITY_SEEDS`; placed beside the cell 9 repair-pacing rig because they
  need the control plane and no data plane): **10** (weights 100/100/200 and
  seeded variants, tablets minted through the creation path, a node added,
  rebalance to a fixed point within `1/min_weight`, then a lowered ceiling drains
  the heaviest node losing no replica), **11** (the real `reconcile_loop`: four
  RF3 tablets over four nodes at `max_replicas` 3, creation refused, a node dies,
  repair stays within every ceiling and the gauge equals the blocked count,
  raising the ceilings restores RF3 and the gauge returns to 0), **12** (a full
  Region is reported, the conversion proceeds once it has room, and a wholly lost
  Region's replica waits rather than moving to another Region). Controls **(g)**
  (count balance ignoring weights) and **(h)** (over-placing creation) each fail
  the cell's oracle. The wire half of cell 11 is `animusd`
  `sim_cluster_cluster_version::create_table_is_refused_retryably_when_every_node_is_at_max_replicas`
  (503, no half-created table, success after the ceilings rise).
- **Tier 2 lines.** `sim_cluster_scale`'s density, split-storm and node-loss cells
  now print `awake_hwm`, `wake_deferred_total`, `bg_snapshot_hwm`, `bg_split_hwm`,
  `bg_compaction_hwm` and `capacity_blocked_tablets` and assert structurally (each
  pool's high-water within its size, nothing capacity-blocked with no ceiling, the
  snapshot and split pools held a permit when the cell exercised them). **The
  layers are on in those cells**: `SimCluster` has nine constructors and six
  reconciler build sites (restarts included), so a cell installs a
  `sim_cluster::SimDensity` for its thread (`SimDensity::install`, an RAII guard)
  and `build_reconciler` and the four `DataRole` sites read it. Density and storm
  run the production default (2 cores, `A` = 3000, pools 2/2/4/1/1); the node-loss
  cell runs 1 core and `A` = 4 so the loss exercises the queue. `awake_hwm` is a
  sampled high-water of the `CpAwakeReplicas` level. Gaps: `bg_compaction_hwm`
  prints 0 (the cells run `MemoryEngine`, which has no compaction pool), and the
  `DataRole` split pool is its own `BackgroundBudget` (only the split pool records
  there, with no lag attachment), a fixture simplification.
- **A defect the cluster tier found (fixed here).** Turning the split pool on in
  the split storm left one over-threshold table unsplit forever. The bytes sweep
  deferred it on a full pool, it quiesced before the pool freed, and the sweep's
  "a quiesced group was already checked by its last awake tick" skip (ADR 0044
  PR6) is false for a tablet that tick *deferred*. The sweep now remembers
  pool-deferred tablets and evaluates them even if quiesced, forcing its estimate
  gate. Lesson: `docs/lessons/code-patterns/2026-10-14-a-deferral-must-defeat-the-skip-that-assumes-the-last-tick-decided.md`.
  The storm also had to wait for every table to split (`>= tablets_before + g`)
  rather than for "nothing mid-split", which a deferred split satisfies early.
- **Operator `cores` (layers 2 and 3).** `derive_cores`/`cores_from_millicores`
  (`animus-operator` `desired/cluster_config.rs`, beside `derive_node_capacity`)
  render `cluster_settings.cores` = `spec.nodeCapacity.cores` if set, else
  **`max(1, floor(cpu request millicores / 1000))`** from
  `spec.resources.requests.cpu` (500m is 1, 2500m is 2, 4 cores is 4), else unset
  (the cap and pools stay off). Rounded down on purpose: the safe error for a cap
  and a compaction pool is too small, not one that over-admits a pod promised less
  than it assumed. `bg_compactions` is not rendered separately: the node derives
  `max(1, cores / 2)` from `cores` (`BudgetSizes::resolve`). The CRD
  (`deploy/operator/crd.yaml`) is regenerated; zero is refused at admission.
- **Cell 3b** (`density_admission_corpus`, "absorbable overload", step 4's cell 3
  discussion): the CPU model gives each survivor `1.5 x (G/N + A)` awake groups of
  capacity, so the capped population (`A_eff` plus the bypassed lost-leader
  elections) fits and the uncapped one (every group awake) does not. Asserted: the
  capped run's worst scheduling lag is no worse than uncapped **and** it leads no
  fewer groups under load. First result (seed `0x24ef92c11692b620`): capped lag
  12 ms against 13557 ms uncapped, 60 against 59 led, terms 2 against 19; held
  across `ANIMUS_DENSITY_SEEDS=10`.
- **Cell 9's window oracle was a flake (step 6 test, fixed here).** `repair_pacing`
  asserted at most `R` commits in a `RECONCILE_INTERVAL` window anchored at the
  first commit; seed 2584926886 (4th of `ANIMUS_REPAIR_PACING_SEEDS=5`) put two
  ticks' batches (16) in one window because the commit latency of the first batch
  exceeded the second's. The oracle is now cumulative, `R x (ticks since the first
  commit + 2)`; both negative controls still fail it; green at K=30. Lesson:
  `docs/lessons/testing/2026-10-14-bound-a-rate-by-cumulative-commits-not-by-a-window-anchored-at-an-observation.md`.

**2026-10-17 note:** the refusal path this ADR said it lacked now exists: ADR 0080 refuses a client write that would wake a quiesced group when lag >= 100 ms and the awake set is at `A_eff`, and the node gate shrinks with the same lag. The ADR 0079 awake cap and the ADR 0080 `QueuedBytes` budget are independent bounds that compose; size the pod for the floor plus the budget plus the working set.

**2026-10-18 note (ADR 0081):** decision 6 is answered. The L0 write stall and the compaction IO rate limit are ADR 0081 (issue #1300), implemented: per-engine L0 slowdown/stop levels before the WAL append (delay only, never `Err`), and a node-wide compaction write-bytes token bucket beside this ADR's permit pool (permit first, then bucket; the L0 bypass is also the bucket's urgent level, so the two limiters cannot deadlock a stall). The permit pool and its bypass are unchanged.

## Amendment (2026-10-27, issue #1389): the node memtable budget sizes the flush

A fixed per-tablet flush threshold fits neither end of the density range: 64 KiB makes a hot tablet
flush a swarm of tiny L0 tables whose compaction dominates its writes (an experiment with a 2 MiB flush
cut compaction bytes per commit about 10x), while 2 MiB times thousands of hosted tablets is the memory
blow-up this ADR exists to prevent. So the threshold is a share of a per-node budget:

    flush threshold = clamp(node_memtable_budget / active engines, floor, cap)

- **Defaults:** budget 256 MiB (`--memtable-budget-bytes N`, `cluster_settings.memtable_budget_bytes`,
  node-local class L, additive; `0` turns it off and engines keep the fixed `LsmOptions` threshold),
  floor 64 KiB (the old fixed value), cap 4 MiB. A lone hot tablet flushes 4 MiB tables; at 4096 active
  tablets each is back at 64 KiB.
- **Active** = a hosted engine whose memtable holds bytes. It is counted by each engine, not derived
  from the quiescence state, so it needs no clock and no entropy: an engine that flushes, is released or
  is dropped stops counting, and every other engine's next threshold read sees the new share.
- **Shape:** the same node-shared object pattern as the compaction pool and IO bucket. The budget lives
  in `BackgroundBudget` (`flush_budget()`), `LsmTabletFactory` joins every tablet engine to it, the
  engine consults it on each write and flush. The control keyspace engine does not join (it is tiny).
- **Bound:** while `budget / floor` or fewer engines are active the sum of memtables stays within the
  budget (plus one write each, plus a transient where an idle engine holds bytes sized for an older,
  smaller share until its next write). Past that the floor binds and the total can exceed the budget; the
  floor is what keeps flushes from degenerating. The ADR 0081 memtable hard cap is 4x the live threshold.
- **WAL:** `LsmOptions::production()` raises the segment size to 1 MiB so a capped flush covers a few whole
  segments. No durable format changes.
- **Tests:** `animus-storage` `lsm_adaptive_flush` (compaction bytes per ingested byte 7.1 fixed vs 1.35
  adaptive, 7.1 again when crowded to the floor), `FlushBudget` unit tests, and `animusd`
  `sim_cluster_flush_budget` (8 hosted tablets per node on a 64 KiB scaled budget stay within it; the
  control without a budget peaks at 478 KB).

