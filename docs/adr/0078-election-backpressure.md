# ADR 0078 — Election backpressure: per-node campaign admission, lag-stretched election timeouts, staggered hosting

- **Status:** Accepted (2026-10-11), implemented (2026-10-11). The maintainer settled the six open questions on 2026-10-11 (see "Maintainer decisions" below); see "As built (2026-10-11)" for where the implementation differs from the design.
- **Date:** 2026-10-11
- **Origin:** issue #1199 (found by C-17 Tier 2, `crates/animus-cp-data/tests/group_density_cost.rs`); maintainer decision "ADR first, code after".
- **Amends:** none. **Depends on:** ADR 0003 (determinism, the `Env` seam), ADR 0009 (control-plane Raft, pre-vote), ADR 0016/0017 (CP data plane, per-tablet Raft), ADR 0031 (host reconciler), ADR 0044/0048 (quiescence, heartbeat batching), ADR 0058 (learners), ADR 0073 (upgrade compatibility), ADR 0075 section 3.4 (per-group timing profile).

## Context

### The problem, as measured

A node hosts one Raft group per tablet replica, so a node with thousands of replicas runs thousands of independent election timers. Each group decides alone, from its own clock, that its leader is gone. Nothing couples those decisions to what the node can actually do. Issue #1199 measured the consequence (release build, 4 cores, three `ProdEnv` RF3 replicas sharing one process over loopback, heartbeat batcher on, `MemoryEngine`):

| Setup | Result |
|---|---|
| 1000 RF3 groups hosted at once | For 5 to 32 s no group has a leader. Every group passes term 1 and the max term reaches 7 to 32. They eventually converge. |
| Same 1000 groups, hosted 100 at a time (waiting for leaders between batches) | All 1000 led within 1.3 s. |
| G=3000, batch 250 | Past about 2000 hosted groups the led count **falls**, from 1769 to 1306 over about 65 s, while the max term climbs from 10 to 48. Process CPU is flat at about 1.9 cores. It never recovers. G=5000 fails the same way. |

The two ends of that table are the whole argument. The same groups, the same code and the same CPU either converge in 1.3 s or never, depending only on **how many campaigns are in flight at once** and **whether anything slows down when the node is late**.

The mechanism (issue, and ADR 0044's 2026-10-04 and 2026-10-09 amendments):

1. Awake (non-quiesced) RF3 groups cost about 1.3 cores per 1000 groups in that one-process setup (1325 to 1451 ms/s after #1207). By my arithmetic from the issue, 1.9 cores delivered against roughly 2000 hosted groups needing about 2.6 cores is a deficit of about a quarter. That is enough.
2. Heartbeats arrive late because the runtime is behind, not because the leader is dead.
3. A follower whose timer lapses starts a pre-vote (`start_pre_vote`, `animus-control/src/raft.rs`), and a successful pre-vote majority starts a real election (`start_election`) that bumps the term and needs a durable hard-state write. A campaign costs several messages and an fsync round per group, more than the heartbeat it replaces.
4. The extra work makes the node later, which lapses more timers. Nothing in the loop pushes back. That is a positive feedback loop with no governor, hence a cliff instead of graceful degradation.

### What exists today (grepped, not assumed)

- **Election timing is per core, fixed and local.** `RaftCore` holds `election_base` (150 ms LAN). `reset_election_timer` draws a deadline uniformly in `[base, 2*base)` from the caller's entropy. `RaftCore::set_timing` (ADR 0075 section 3.4) is the only thing that changes the base, and it is fed by the topology-derived LAN/WAN profile, never by load. There is no backoff after a failed round: a `PreCandidate` or `Candidate` whose round lapses falls back to a fresh `start_pre_vote` immediately.
- **Pre-vote exists** (ADR 0009) and stops a node with a stalled driver from bumping terms against a healthy leader. It does not help here, because the nodes are not partitioned: the leader really is late, and the followers that pre-vote genuinely cannot hear it, so they win.
- **There is no node-level campaign state.** `start_pre_vote` / `start_election` / `campaign_now` / `handle_timeout_now` consult only the core's own role and flags (`cluster_check_*`, `is_voter`, `state_machine_behind`, `storage_full`). `grep` finds no counter of in-flight campaigns across groups.
- **The responder side keys off the election deadline, not off having campaigned.** `handle_pre_vote` computes `has_live_leader` from `leader_id.is_some() && now < election_deadline` (and the vote lease). A follower whose deadline has lapsed therefore grants pre-votes even if it has not itself started campaigning. This is load-bearing for the design below.
- **The per-node batcher** (ADR 0044 phase 2, `heartbeat_batch.rs`, on by default) is the only per-node object the data plane shares today: one flush task per node, 50 ms tick, groups register into it. It coalesces bare heartbeats on the wire. It does not look at lag and does not touch campaigns.
- **Quiescence** (ADR 0048) stops a quiet group's timers entirely, so a quiesced group cannot campaign. The reconciler's `down`-driven `RaftKvNode::wake()` (host `tick`) wakes every hosted group whose replica set intersects the failure detector's down set in one pass, which is the "mass un-quiesce" the issue names. `on_local_wake` then re-arms each woken group's election timer, so the wake is already spread over one `[base, 2*base)` window, but with no cap on how many campaign inside it.
- **The host reconciler hosts everything `plan` says in one pass.** `Reconciler::tick` runs `plan(...)`, then loops over every `HostAction::Host` and awaits `self.host(...)` for each. It is event-driven off `metadata_watch` with a 500 ms fallback (`RECONCILE_FALLBACK_INTERVAL`). On a restart with N tablet replicas all N groups start together and all N are leaderless at once. `group_density_cost`'s `ANIMUS_DENSITY_BATCH` is a test-side stagger only; the product has none.
- **Leadership transfer and the preferred-leader step go through `TimeoutNow` → `start_election`** (`handle_timeout_now`), skipping the timer and pre-vote. Split children's materializing replica calls `campaign_now` (`start_hosted_campaigning`) so a child gets a leader in about one round trip.
- **`SimEnv` can freeze a node but not starve it.** `Simulator::pause(node, dur)` defers a node's timers and deliveries to a resume instant (all-or-nothing). There is no model in which a node is slow in proportion to its load, which is why #1199 has only a `ProdEnv` repro. The issue's fourth bullet ("a SimEnv repro with CPU-starved scheduling") is part of this design.

### What this ADR is not

It does not make an overloaded node faster. If steady-state heartbeat demand alone exceeds the CPU, the node is still overloaded; C-17's density thresholds (1000 groups per node) remain the capacity statement. What this ADR buys is **graceful degradation**: past the cliff, leadership is retained and latency rises, instead of leadership being lost and never regained. The cliff in the issue is also reached only by awake groups; quiescence still keeps idle ones free. And the three mechanisms address three different parts of the loop (the burst at start, the false trigger, the amplification), so they are specified separately and each carries its own negative control.

## Decision

Three node-local mechanisms, each independently switchable, all **default on** (the failure they prevent is a silent cliff, the same argument that put quiescence on by default). All three live outside the Raft safety argument: they only **delay** when a node may start a campaign or host a group. None changes who may win an election, what a vote means, or what a term is.

### 1. Per-node campaign admission (`CampaignGovernor`)

A new per-node object in `animus-cp-data`, `CampaignGovernor`, created once per node next to the `HeartbeatBatcher` and shared (`Arc`) by every data-plane `RaftKvNode` the node hosts. Synchronous and `Env`-seam clean: no tasks of its own, time passed in as `Nanos`, no hashing containers (`BTreeMap` throughout).

**What is gated.** A group's entry into `start_pre_vote` from the timer path (`tick` when `now >= election_deadline` and the role is `Follower`, `PreCandidate` or `Candidate`). Gated means the group must hold a **campaign permit** before it broadcasts `PreVote`.

**What is not gated** (it is directed, bounded, or must not be late):

- `TimeoutNow` (leadership transfer, and so the preferred-leader step of ADR 0075) and `campaign_now` (split child, one designated replica). They take a permit if one is free and **proceed over the cap if not**, counted in the in-flight gauge. They are already one-per-group and driven by a bounded decision, so they cannot form a storm.
- Single-voter groups (they win without a message).
- The control-plane group (`animus-control`'s `RaftNode`), which is one group per node and whose liveness every data group depends on. It never takes a permit.
- Granting votes and pre-votes (the responder side). See "Why candidate-side only" below.

**The rule.**

- `K = campaign_max_inflight` (default **32**; `0` disables the governor). At most `K_eff` permits are outstanding at once; `K_eff = K` normally and is reduced by observed lag (mechanism 2): `K/2` at lag >= 30 ms, `K/4` (minimum 1) at lag >= 100 ms.
- A permit is held for the **life of one campaign attempt**: from the moment the group is admitted until the first of (a) it becomes leader, (b) it returns to `Follower` because it heard a current leader or adopted a higher term, (c) the attempt's own election deadline lapses without either (the core is about to start a fresh round), (d) the group is shut down or released. The driver holds it as an RAII `CampaignPermit`, so any exit path drops it; the governor also expires a permit after `2 * election_base_max` of its holder's profile (a leak backstop, checked lazily on `try_admit`), so a lost drop cannot shrink the pool forever.
- One permit covers the whole pre-vote → vote sequence. `start_election` reached via `handle_pre_vote_resp` does not ask again.
- A group that is refused is **queued in FIFO order of first refusal**, keyed `(first_deferred_at, stream id)` in a `BTreeMap`. A permit returned to the governor admits the head of the queue and wakes it through its existing `WakeSignal`. A group that fails an attempt (rule (c)) re-enters at the **back**, with a fresh ticket, so a group that cannot win does not pin a permit ahead of groups that haven't had a turn.
- Because waking is by notification, deferral costs no polling. As a backstop against a lost wake the deferred group also re-checks at its own `election_base` after being deferred.

**Seam in `RaftCore`** (sync, I/O-free, kept that way): the core gains a deferral state, not a callback into the node.

- `campaign_due(now) -> bool`: pure, true iff a `tick` at `now` would call `start_pre_vote`'s broadcast path. The driver calls it before `tick`.
- `tick_gated(now, entropy, admit: bool)`: when `admit` is false and a campaign is due, the core does **not** reset the election timer and does **not** change role or term. It records `campaign_hold = Some(retry_at)` and returns nothing; `next_deadline` includes `retry_at` so the driver wakes. The default `tick` is `tick_gated(.., true)`, so a caller with no governor (and every existing test) is byte-identical.
- `campaign_active() -> bool`: true while role is `PreCandidate` or `Candidate`, so the driver can release the permit on the transition to `Follower` or `Leader` without the core knowing about the governor.

**Why candidate-side only (the deadlock this avoids).** `handle_pre_vote` grants unless `now < election_deadline` and a leader is believed. A deferred follower deliberately leaves `election_deadline` lapsed (it neither re-arms nor clears it), so it keeps **granting** pre-votes and votes to the campaigners that were admitted, even though it is itself waiting its turn. If deferral re-armed the timer, a deferred voter would suddenly refuse everyone and the admitted campaigners could not reach a majority, so permits would be held to timeout and the cap would make things slower, not faster. With candidate-side gating, K admitted campaigns are answered by every other voter on the node at wire speed.

**Lost-leader liveness bound.** Let `Q` be the number of groups of this node that are queued when a leader is lost, `T_hold` the permit hold (typically a few round trips; at most one election timeout, `2*base_eff`, by rule (c)). A group that lost its leader and campaigns is admitted after at most `ceil(Q / K_eff)` hold periods, so its re-election delay is bounded by

`delay <= 2*base_eff + ceil(Q / K_eff) * T_hold_max`, with `T_hold_max = 2*base_eff`.

The worst case needs every earlier campaign to time out; in the common case holds are a few milliseconds, so the term is small (at the issue's scale, 1000 queued groups, K=32, a 5 ms hold: about 160 ms on top of the usual timeout). The proof obligations: the queue is FIFO and finite (every group is queued at most once), a permit is always returned (RAII plus lazy expiry), a failing attempt re-queues at the back, so no group is starved while any permit turns over. Property tests assert exactly these (see Test plan).

**Interaction with the neighbours.**

- *Pre-vote.* Unchanged on the wire and in the responder. Pre-vote still decides whether a campaign may bump the term; the governor only decides when the pre-vote may start. A pre-vote that fails to win costs a permit for one round and no term bump, so the governor bounds the pre-vote traffic too, which is what the self-feeding loop was made of.
- *Leadership transfer.* Exempt from the cap, so a transfer is never queued behind a storm. Its permit still counts, so a burst of transfers shows in the gauge. `transfer_leadership`'s one-election-timeout budget is untouched (it uses the unstretched `election_base`, see mechanism 2).
- *Quiescence.* A quiesced group has no election deadline and no tick, so it never asks. When the reconciler wakes a set of them, `on_local_wake` jitters their deadlines over `[base, 2*base)` as today and the governor sizes the resulting burst. The reconciler's wake loop is unchanged.
- *Preferred-leader step* (ADR 0075). It acts via transfer, exempt. The step's convergence under a saturated governor is a corpus cell.
- *Learners and non-voters.* They never reach the gated path (`start_pre_vote` returns early for `!is_voter()` before the point where the permit is taken). The core takes the permit check **after** those early returns, so a learner never occupies a slot.

### 2. Lag-stretched election timeouts

**Signal.** A per-node `LoadMonitor` (same module family, one `Arc` per node) turns scheduling lateness into a number. One task per node (spawned through `env.spawn_task`) loops `env.sleep(PROBE)`; after each wake, `sample = (env.now() - slept_from) - PROBE`, floored at zero. This measures exactly what hurts a Raft timer: how late the runtime runs a timer that was due. Under `SimEnv` it is zero unless a fault makes it otherwise, so an unloaded sim is untouched.

- `PROBE = 20 ms`.
- Estimate: `lag = max(sample, lag - lag / 32)`: instant attack, slow integer decay (half-life about 450 ms), so a late stretch cannot flap between probes. No floating point, no entropy.
- Dead band: `lag < 10 ms` reads as **zero**. A healthy node (a ProdEnv scheduling jitter of a few ms) is bit-for-bit the same as today.

**Rule.** The `LoadMonitor` publishes a stretch `S = min(c * lag, S_max)` when lag is above the dead band, else zero, with `c = 4` and `S_max = 4 * election_base` (so at most 5x the LAN base, 600 ms of base, randomized to 1.2 s at the top of the `[b, 2b)` window). The driver installs it with `RaftCore::set_lag_stretch(S)`, which, like `set_timing`, is a no-op when unchanged and draws no entropy. `reset_election_timer` and `next_cluster_check_resend` use `election_base + lag_stretch`. **Not stretched:** `transfer_leadership`'s budget, the health grace (`3 * election_timeout()`, `SUSTAINED_HEALTH_ELECTION_TIMEOUTS`), the leader's heartbeat interval, and the vote lease: those read `election_base`, the configured base, on purpose. A leader's heartbeat is not slowed (that would make followers later), only the followers' patience grows.

Why `c = 4`: a follower's wait for a heartbeat is a sum of the leader's lateness to send and the follower's lateness to observe, and the lag on this node is a proxy for both when the node hosts both (the one-process test). Four probe-lags of slack is the margin that makes one late heartbeat not a campaign trigger. This is a **provisional** value, calibrated against the corpus and the C-17 sweep in step 6 of the plan.

**Composition with the WAN profile.** The WAN base (`max(150ms, 5*rtt)`) is the configured base; the stretch is added on top and capped relative to it, so a WAN group stretches in proportion (`S_max = 4 * its own base`). The ADR 0075 formulae are not edited.

**Liveness.** A real leader failure on a loaded node is detected later, never not at all: `delay <= 2*(election_base + S_max) = 10*election_base` in the worst case (1.5 s on the LAN defaults), plus the admission term above. The cap is what makes this a bounded trade: unbounded stretch would turn overload into unavailability. Beyond that cap the node is in the "just overloaded" regime of the non-goals paragraph.

**What it does and does not do.** It removes the false trigger: the heartbeat that is late because the node is late no longer expires the follower. It cannot replace admission: when many leaders really are dead at once (a node loss), lag is low and stretch is zero, and only the governor limits the burst. Conversely when lag is high, admission alone would still let each admitted campaign run against a congested peer. The two are complementary, and `K_eff` reducing with lag is the one place they meet.

### 3. Staggered hosting

**Rule.** The reconciler hosts new groups in **waves** rather than all at once.

- `host_unsettled_max = H` (default **100**; `0` disables staggering).
- A hosted group is **settled** when it has a leader it has heard from (it is leader, or `leader_id` is set and fresh), or when `host_settle_timeout` (default `4 * election_base`, 600 ms LAN) has elapsed since it was hosted. The timeout is the liveness backstop: a group that cannot elect (a minority is up) must not hold the ramp forever.
- Per `Reconciler::tick`, `plan` may emit at most `H - unsettled` `HostAction::Host` actions, in **ascending `TabletId` order**. The cap is applied inside `plan` (it is pure; the deferred tablets must stay "to host" in the planner's next state, not be recorded as hosted), so `plan` takes the remaining budget as an input and the pure property tests cover it.
- When `plan` deferred any Host action, the reconciler arms its own short wake (`min(settle poll, 50 ms)`, reusing the `INPLACE_SPLIT_RECONCILE_INTERVAL`-style cadence) instead of waiting for the 500 ms fallback, and re-ticks as groups settle.
- **Exempt:** `MaterializeSplitChild` (bounded by the fork that caused it, and the split path already designates one campaigner), `Reconfigure`, `Release`, `Reclaim`. Only fresh hosting is staggered.
- **Why ascending `TabletId`.** Every node derives its order from replicated metadata, so on a whole-cluster restart all three replicas of the early tablets are in the first waves on their three nodes together and their groups can elect. A random or per-node order would spread each tablet's replicas across different waves and delay every quorum.

**Liveness.** Ramp time is at most `ceil(G / H) * T_settle`, with `T_settle` the settle timeout at worst (a group that never settles still releases its slot after the timeout) and typically one election round (the measured 1.3 s for 1000 groups at batch 100 is about 130 ms per wave). Groups not yet hosted on this node are served by their other replicas if a quorum exists elsewhere, which is the RF3 case for one restarting node. For a whole-cluster restart the late waves are late by the same order as the ramp itself, and the roadmap can measure it (step 7). The ramp is surfaced in `/admin/health` as a boolean `hosting_ramp` and counts (hosted, deferred, unsettled), because "node is up but still hosting" is an operational fact, not an error.

**Interaction.** Staggering reduces the number of leaderless groups in existence, so it lowers the pressure on mechanism 1 at start; mechanism 1 handles the bursts staggering cannot, which are not caused by hosting (a node loss, a mass un-quiesce, a network heal). Quiesced groups hosted in a wave are quiesced after `--quiesce-after` as today; a restart brings them back awake, which is why the stagger sits at hosting and not at wake. Staggering does not change which replica leads: the preferred-leader step (ADR 0075) runs per tick over hosted groups and converges as waves land.

### Knobs

| Knob (flag / `cluster_settings`) | Default | Meaning |
|---|---|---|
| `--campaign-max-inflight` / `campaign_max_inflight` | 32 | `K`. `0` disables admission. |
| `--lag-stretch-max` / `lag_stretch_max_x` | 4 | `S_max` as a multiple of the election base. `0` disables stretch. |
| `--host-unsettled-max` / `host_unsettled_max` | 100 | `H`. `0` disables staggering. |
| `--host-settle-timeout-ms` / `host_settle_timeout_ms` | `4 * base` (600) | Settle backstop. |
| (constant) | `PROBE` 20 ms, dead band 10 ms, `c` 4, decay 1/32 | Not operator knobs. Changing them is a code and ADR change. |

Same shape as the existing `--quiesce-after` / `--max-region-rtt-ms` (a flag with the `--config`-driven `cluster_settings` mirror, additive and `skip_serializing_if`). Defaults are provisional until calibrated (plan step 6). The governor is deliberately not auto-sized from `available_parallelism`: that is nondeterministic input outside the `Env` seam, and a number the operator reads in `/admin` is easier to reason about than a derived one.

### Metrics and observability

Additive `Metric` variants (the seam is additive, no-op under sim): `CpCampaignAdmitted`, `CpCampaignDeferred`, `CpCampaignInflight` (gauge, with a high-water mark), `CpCampaignQueueDepth`, `CpElectionLagNanos` (gauge), `CpElectionStretchNanos`, `CpHostWaveDeferred`. `/admin/health` and the dashboard gain the node-level readings next to the existing election-timeout figure. C-17's Tier 2 harness reads the same numbers.

### ADR 0073 compatibility classification

Everything here is **L (node-local)**. There is no new message, no change to any message's meaning, no replicated `Metadata` or `MetaCommand` field, no `KvCommand`, no WAL or snapshot record, no key layout.

| Item | Class | Reason |
|---|---|---|
| Campaign admission | L | Decides when *this* node starts a pre-vote. Peers see the same `PreVote` / `RequestVote` messages, later. A node running an older binary answers them as before. |
| Lag-stretched timeouts | L | Changes when *this* follower lapses. The leader's heartbeat cadence and every wire field are unchanged. |
| Staggered hosting | L | Changes when *this* node starts a group, in an order derived from replicated metadata but acted on locally. |
| `campaign_hold`, `lag_stretch`, permit state | L | In-memory only, not persisted. |
| New `cluster_settings` fields | F-adjacent, additive | Optional, `skip_serializing_if`, so the frozen `ClusterConfig` v1 fixture is untouched (the `max_region_rtt_ms` precedent, ADR 0075). No version bump, no new fixture. If the maintainer prefers a fixture proving old readers ignore the new keys, that is one `serde` test. |
| `Metric` variants, admin JSON fields | not a format | Observability; additive. |

No `Gate`, no cluster-version bump, no `required_gate` row. Mixed-version clusters are fine: a mixed pair of nodes differ only in how long they wait. **One behavioural note for rolling upgrades:** a node on the new binary hosting a wave-by-wave ramp after its restart is exactly the "restart wakes N groups" scenario this fixes, so the roll's per-node health wait (ADR 0073 Phase 3, `roll-health`) should see fewer leaderless groups, not more; the `hosting_ramp` flag is advisory there and is not added to the roll gate by this ADR.

## Test plan

Every distributed behavior needs a seed-reproducible fault-injecting simulation (root `CLAUDE.md`), and `SimEnv` proves ordering, not real-thread liveness, so there are three tiers.

### Step zero: a SimEnv that can be starved

New in `animus-sim`: `Simulator::set_cpu_cost(node, per_event: Duration)`. Each timer fire and message delivery for that node costs `per_event` of virtual node-CPU: the node's events are serialized, an event runs at `max(scheduled, node_busy_until)`, then `node_busy_until += per_event`. It reuses the defer-at-delivery hook `pause` already has (a deferred timer or delivery re-checks `paused_until`); this adds `busy_until` beside it. Unset (the default) is a no-op, so no existing seed's trace moves. It is deterministic (a pure function of the schedule), traced, and works with `ANIMUS_SEED` replay. With it, the issue's collapse is reproducible without wall time: choose `per_event` so steady heartbeat demand is 1.1x to 1.3x capacity.

### Tier 1: pure `SimEnv` corpus, `animus-cp-data` `tests/it/election_backpressure_corpus.rs`

Knob `ANIMUS_ELECTION_BP_SEEDS=K` (default 1), `ANIMUS_ELECTION_BP_CELL=<substring>`, `ANIMUS_SEED=<seed>`. 3 nodes, `G` RF3 groups (G=300 per-push; G=2000 under the nightly knob), batcher on, `MemoryEngine`, cpu cost set so that capacity is about 1.0x of steady demand at the top of the sweep. Cells:

1. **cold start, all at once** (hosting stagger off, so the governor and stretch alone are exercised).
2. **cold start, staggered** (all three on).
3. **node restart**: crash and restart one node; its groups re-host.
4. **leader node loss**: kill the node leading about a third of the groups, with every group quiesced beforehand (mass un-quiesce through the reconciler's `down` wake).
5. **sustained overload**: demand 1.15x capacity for 60 s of virtual time after convergence.
6. **overload then relief**: overload for 30 s, then cost lowered; must recover to all-led without a restart.
7. **one-node partition and heal**: the isolated node's groups cannot win; healthy nodes' groups must not be slowed; after heal all converge.
8. **leadership transfer under saturation**: governor full of failing campaigns, then `transfer_leadership` on one group; must finish within the election-timeout budget (the exemption works).
9. **preferred-leader step under saturation** (3 regions, WAN profile): leaders reach the preferred region, `wan_timing_corpus` assertions unchanged.
10. **single group lost-leader bound**: kill one group's leader while the governor has `Q` queued groups; new leader within `2*base_eff + ceil(Q/K_eff)*T_hold_max` (the formula above, asserted literally).
11. **learners / quiesced groups take no permits** (gauge stays 0).
12. **disabled == baseline**: with all three knobs off the trace of a fixed-seed run equals the pre-change trace byte for byte (guards the "default tick is `tick_gated(.., true)`" claim); with them on and no load (lag zero, `<= K` campaigns) the same.

Asserted on every cell: the Raft safety oracle already used by the raftkv corpus (no two leaders in a term, no committed write lost); no group permanently leaderless once the fault clears; the governor's high-water mark `<= K + (exempt campaigns in flight)`; and for cells 1, 3, 4, 5 bounded term churn (`max_term - 1` per group below a stated ceiling) and a monotone led-count after the first full convergence (the shape the issue shows failing: led count decreasing over time).

**Negative controls** (each must fail its cell, proving the oracle bites; same convention as the MREC and MRSC corpora):

- **(a) admission off, stretch off, stagger off** on cell 5: must reproduce the collapse (led count falls, term churn above the ceiling). This is the regression proof for #1199 itself; it runs first and is expected to fail the assertion without the mechanisms.
- **(b) stretch off only** on cell 5: churn above the ceiling although admission is on (shows stretch is load-bearing independently).
- **(c) admission only gating the responder** (a deliberate bug, `#[cfg(test)]` switch): cell 1 must show permits held to timeout and time-to-all-led above bound (the deadlock described above).
- **(d) LIFO queue / permit leak** (no RAII release): cell 10's bound must be violated and a group must starve.
- **(e) unbounded stretch** (`S_max = inf`): cell 4's failover-time bound must fail.
- **(f) per-node random hosting order** instead of ascending `TabletId` on a whole-cluster restart: time-to-quorum must exceed the ascending-order run.

Pure unit and property tests beside the code: governor FIFO fairness and no-starvation over random permit hold times; permit drop returns the slot; lazy expiry; `K_eff` as a function of lag; `LoadMonitor` estimate monotone, bounded, dead band; the stretch function; `plan` with a budget: output sorted ascending, every deferred tablet eventually emitted, never more than the budget, split-child materialization never deferred.

### Tier 2: cluster tier, `animusd` `sim_cluster_scale` (C-17)

Extend the density cells with the `C17 metric=` lines `sim_cluster_scale.rs` already prints (format: `C17 metric=<name> tablets=<n> nodes=<n> value=<v>`):

- `election_time_to_all_led_ms`, `election_max_term`, `election_term_churn_per_group_x1000`, `campaign_inflight_hwm`, `campaign_deferred_total`, `leaderless_group_ms`.

Structural assertions only (the file's convention): `campaign_inflight_hwm <= K + exempt`; every group led by the end; `max_term` below a stated bound at the density row (the issue's G=3000 row). `ANIMUS_SCALE_SEEDS` / `ANIMUS_SCALE_MAX_TABLETS` select size; nightly `corpus-deep.yml` runs the 50k setting. The metric the maintainer should watch is **`election_term_churn_per_group`**: in the issue it is about 7 to 32 at 1000 groups and 48 at 3000; with the mechanisms it should be near 1.

*As built (2026-10-11):* the six lines print at the end of every density cell (`emit_election`), plus `campaign_admitted_total` (a wiring sanity check) and `preferred_leader_transfers_total` (the exempt-permit allowance in the hwm assertion, `hwm <= K + transfers`); `max_term` is asserted at most 8. `SimCluster`'s `build_reconciler` did not install the governor and monitor until this step (production's node assembly did), so every `SimCluster` test now runs them at their defaults (the full `animusd --lib` tier stays green); the counters are read from the `SimEnv::metrics()` sink (`SimCluster::env_metric`). The density cells create their tables one at a time and each create waits for its group's first leader, so the per-push rows show `campaign_inflight_hwm` 1 and no deferral: they pin the wiring and the churn, not the governor's bound (the Tier 1 corpus and Tier 3 do that).

### Tier 3: `ProdEnv` liveness, `animus-cp-data` `tests/election_backpressure_liveness.rs`

Its own `tests/*.rs` target (`ProdEnv`, real threads, `prod-heavy`), a timeout-guarded `#[tokio::test(multi_thread)]`, per `docs/lessons/testing/`:

- **Per-push (`ProdEnv`, light):** 3 `ProdEnv` nodes, RF3, G=300, the runtime built with `worker_threads(2)` so scheduling lateness is real and reproducible. Host all at once; assert all led within a bound, `max_term` below a ceiling, `campaign_inflight_hwm <= K`. Then kill one replica's node and assert every group it led is re-led within the lost-leader bound. Converged-or-timeout polling, not a one-shot assert.
- **Ignored, nightly or manual:** the issue's repro, `ANIMUS_DENSITY_RF=3 ANIMUS_DENSITY_GROUPS=3000 ANIMUS_DENSITY_BATCH=0 ... group_density_cost`, must reach "all led" and hold the led count for a 120 s window (the shape that today decays), both with and without a harness-side batch. The acceptance for closing #1199.

SimEnv does not replace the `ProdEnv` test: it proves the ordering and the bound, not that a real runtime's timers behave the way the `LoadMonitor` assumes.

*As built (2026-10-11):* `tests/election_backpressure_liveness.rs` (`prod-heavy`, run per push in `prod-liveness-scattered`). The groups are hosted through each node's real `host::Reconciler` in the production configuration (batching, quiescence, governor, monitor, stagger), the node loss happens once every replica has quiesced, and the killed node is marked `Down` after 500 ms so the reconciler wakes the quiesced followers (ADR 0048 fork H); the lost-leader bound is the formula above at its worst case (`K_eff = K/4`, `base_eff = 5 * base`) plus that detection path. About 8 s in a debug build. Under CPU contention (three copies at once) it found that a deferred `PreCandidate` finished its lapsed pre-vote round on a late grant and campaigned on an exempt permit over the cap; `handle_pre_vote_resp` now ignores grants while `campaign_hold` is set (`docs/lessons/code-patterns/2026-10-11-a-deferral-must-gate-the-message-path-not-only-the-timer.md`). The G=3000 acceptance is the `#[ignore]`d `density_repro_g3000_holds_led_count` in the same file (same rig at G=3000; the issue's `group_density_cost` rig starts bare `RaftKvNode`s, so it never had the mechanisms).

## Alternatives considered

- **A larger static election timeout.** Slows every failover for every group, all the time, to protect against a condition that only happens under load; it does nothing for the start-up burst (all groups are leaderless regardless of the timeout). Rejected; lag-stretch is the adaptive version, capped.
- **Per-group exponential backoff after a failed round** (stateless, the usual Raft answer). Smooths a single group's retries but cannot bound the number of concurrent campaigns across groups, which is the quantity that matters here; and it delays a legitimate retry after a transient partition heals. Rejected as the primary mechanism. (Cheap to add later behind the governor if the corpus shows retry churn.)
- **A node-wide campaigns-per-second token bucket.** A fixed rate is wrong in both directions: too slow when the node is healthy and campaigns resolve in milliseconds, too fast when it is not. A concurrency cap self-scales with how long campaigns actually take (Little's law), which is exactly the signal missing today.
- **Gating the responder as well as the candidate.** Deadlocks, as shown in mechanism 1. Rejected.
- **Replicated/cluster-wide election tokens through the control plane.** Puts consensus state on the data plane's failure path, adds a replicated shape that needs an ADR 0073 gate, and the control plane would be the thing under strain. The problem is node-local (one node's CPU); a node-local fix is right.
- **Scale quiescence harder instead.** Quiescence (ADR 0048) already makes idle groups free; this ADR is for groups that are awake, which is what restart, mass wake and real load produce. Complementary, not a substitute.
- **Fix it in the runtime** (dedicated Raft-timer threads or priorities). Plausible later, and orthogonal, but it is `ProdEnv`-only: it cannot be tested under `SimEnv` and gives the determinism seam nothing to prove.
- **A hard cap on hosted groups per node.** Refusing work is a different (admission-control) feature with product consequences; flagged as an open question, not part of this ADR.
- **A fixed sleep between hosted groups** (say 10 ms each). Not adaptive: too slow for a fast node, still too fast for a slow one. Settle-gated waves adapt to the actual election latency.
- **Stretch driven by CPU utilization.** Utilization needs `/proc` or `getrusage`, which is real I/O outside the seam; scheduling lateness is measured entirely through `env.now()` / `env.sleep()` and is what Raft timers are actually exposed to.

## Implementation plan

Per the repo's session operating mode (one bigger PR per workstream), this is one implementation PR on one branch, built as the ordered commits below; each commit keeps the five gates green on its own so a reviewer can bisect and the maintainer can split at any boundary if wanted. This ADR is its own docs-only PR first.

1. **`animus-sim` starvation model** (`set_cpu_cost`), unit tests, plus the baseline Tier 1 harness with cells 1 and 5 and **negative control (a)**, asserting that the collapse reproduces with all mechanisms absent. This is the "repro first" commit. No product code changes; no existing seed moves.
2. **`RaftCore` seam**: `campaign_due`, `tick_gated`, `campaign_hold` in `next_deadline`, `campaign_active`, `set_lag_stretch`. Core unit tests. Default paths unchanged (corpus cell 12 and the existing pure corpora byte-identical).
3. **`CampaignGovernor`** and the driver wiring (RAII permit, FIFO queue with `WakeSignal` wakeups, lazy expiry), one per node in `animusd` next to the batcher; exemptions; metrics; flags and their cluster-settings mirror. Governor property tests; corpus cells 1, 4, 7, 8, 10, 11 and controls (c), (d).
4. **`LoadMonitor` and lag stretch**, `K_eff(lag)`, wiring; unit tests; cells 5, 6 and controls (b), (e).
5. **Staggered hosting** in `host::plan` and `Reconciler::tick`, the settle tracker, the self-wake, `hosting_ramp` in `/admin/health`; `plan` property tests; cells 2, 3 and control (f).
6. **Calibration and Tier 2**: sweep `K ∈ {8, 16, 32, 64, unbounded}`, `c`, `S_max`, `H` over the corpus and C-17; set the final defaults; the `sim_cluster_scale` `C17 metric=` lines and assertions; cell 9 with `wan_timing_corpus`. Re-baseline any existing corpus whose trace moved with a note in the PR (expected only where more than `K` campaigns were simultaneous).
7. **`ProdEnv` liveness test** and the density acceptance run recorded in ADR 0044's C-17 amendment; **docs**: this ADR to Accepted/implemented with an "as built" note, `crates/animus-control/CLAUDE.md` and `crates/animus-cp-data/CLAUDE.md` (the seam, the governor, the exemptions), `docs/roadmap.md` (C-17 and #1199), `website/` if it states election or restart behavior, and a `docs/lessons/` entry for anything learned. Close #1199 on this PR.

## Maintainer decisions (2026-10-11)

The maintainer accepted the recommendation on each of the six questions this ADR raised:

1. **On by default.** All three mechanisms ship enabled, since the failure is a silent cliff. Corpora with more than `K` simultaneous campaigns are re-baselined in the same change that turns them on.
2. **Defaults.** `K=32`, `H=100`, `c=4`, `S_max=4x` are the starting values, to be confirmed by measurement in implementation step 6. `K` stays a fixed knob; it does not scale with a core count.
3. **Exemptions confirmed.** The control group and leadership transfers are exempt from the campaign governor.
4. **Advisory only.** `hosting_ramp` and the lag gauge stay advisory in `/admin/health`. Sustained high lag does not fail the health check or the roll gate (ADR 0073 Phase 3).
5. **Density admission is a separate ADR.** A per-node cap on awake groups is out of scope here. It gets its own ADR once Tier 2 shows where the stable ceiling is.
6. **Restart tail accepted.** `H` stays fixed. A ramp-time-dependent `H` can follow later if measurement justifies it.

## As built (2026-10-11)

Implemented on one branch as the seven planned steps. The defaults are unchanged (`K=32`, `H=100`, `c=4`, `S_max=4x`, settle timeout 600 ms) and all three mechanisms are on by default. Deviations from the text above, and results:

- **CPU model ordering.** `Simulator::set_cpu_cost` keeps a per-node queue that serves due **timers ahead of waiting deliveries, in seeded random order** (`CpuOrder::Random`, which the corpus uses). Plain FIFO turned out to model a uniformly slow clock: every event is late by the same amount, heartbeats keep their relative order, and nothing collapses, so the #1199 shape never appeared.
- **Overload level.** The overload cells and control (a) run at **1.25x** capacity, not the 1.15x the test plan states.
- **Load monitor in the corpus.** Because the CPU model serves timers ahead of deliveries, the monitor's own timer-lateness probe reads about 0 under the sim. The corpus therefore feeds a **detached `LoadMonitor`** from the simulator's queue (`Simulator::cpu_backlog`). The real probe task is covered separately (`load_monitor_task_measures_a_paused_node`) and in Tier 3.
- **Cell 10** adds a 50 ms `OWN_ELECTION_SLACK` to the literal lost-leader bound. The formula stops at admission, but the admitted group still needs its own pre-vote, vote and commit round trips, and the probe resolution is 10 ms. A permit leak (control (d)) misses the bound by seconds.
  *Amendment (2026-10-14, issue #1306):* two corrections to what cell 10 asserts. (1) The formula holds **per campaign attempt**, not per leader loss. A follower's attempt can be lost without a term bump (a pre-vote round that fails when both followers pre-campaign together, or a plain split vote); the failed attempt re-queues at the back and costs another full window (reseeded variants took up to 7 attempts, each about 0.9 s at Q=64). Cell 10 asserts `delay <= attempts * round + slack`, `attempts` being the largest per-group `campaign_stats().rounds_started` delta on the survivors since the kill (floor 1; a deferred campaign starts no round). The node-wide `CpCampaignAdmitted` metric counts every hosted group and the ghosts, so it cannot be the count. (2) The queue ahead of the lost group in this rig is the ghost campaigners, which are not woken by a grant: they re-ask on a `base` sleep, and the governor protects the queue head, so each pass costs `2*base + base`, not `2*base`. A real group is woken by its callback at once, so this is a harness term, not a production one; `round = 2*base + ceil(Q/K)*(2*base + base)`. The canonical seed met the literal bound by phase luck (0.92 s against 0.95 s; seed `s01` took 1.21 s with a single attempt). A leaked permit admits nothing, so control (d) still gets the single-attempt bound and misses it by seconds. Because the bound scales with `attempts`, a livelock would only widen it, so cell 10 also caps `attempts` independently: over 47 reseeded runs the count was 1:20 2:8 3:8 4:4 5:1 6:4 7:2 (mean 2.5), so an attempt succeeds with probability about 0.4 and the count is roughly geometric; a per-run false-failure budget of 1e-6 gives `ceil(ln 1e-6 / ln 0.6) = 28` (`attempt_cap()`; re-derive if K, Q or the election timing change). New negative control (g) partitions the two survivors from each other for 60 s after the kill (38 attempts) and must fail the cap.
- **Control (e)** (`S_max = inf`) runs on **cell 4b**, a node loss as the load ends, sharing 4b's seed. On cell 4 the reconciler's `down` wake goes through `on_local_wake`, which draws an unstretched jitter, so the stretch cap is not what bounds failover there.
- **Control (c)** (the governor also gating the responder) only bites on the cell-4 shape (mass un-quiesce after a leader-node loss). On a cold start (cell 1) the deadlock does not form, so (c) is asserted on cell 4.
- **Two governor bugs found by the corpus and Tier 3, fixed with tests.** (1) A `PreCandidate`/`Candidate` whose retry was deferred is `campaign_active` by role, so the driver's post-step check gave it an exempt permit, removing it from the queue and leaking an over-cap slot on every retry. A whole-cluster restart with per-node hosting orders then stayed leaderless. `RaftCore::campaign_deferred()` now exposes the hold, and the driver skips the exempt acquire while it is set. (2) A deferred `PreCandidate` finished its lapsed pre-vote round on a late grant and campaigned over the cap. `handle_pre_vote_resp` now ignores grants while `campaign_hold` is set (`docs/lessons/code-patterns/2026-10-11-a-deferral-must-gate-the-message-path-not-only-the-timer.md`).
- **Calibration sweep** (`#[ignore]`d, release, about 6.5 min; `K` in {8, 16, 32, 64, unbounded}, `c` in {2, 4, 8}, `S_max` in {2x, 4x, 8x}, `H` in {50, 100, 200} over cells 2, 2b, 4b, 5, 10). The only violations are `K=8` and `K=16` on cell 10's lost-leader bound, so **`K=32` is the smallest swept value that passes**. Nothing contradicts the other defaults.

  | Variant | Cell 10 recovery | Cell 10 hwm | Cell 4b recovery | Cell 2 all led | Violations |
  |---|---|---|---|---|---|
  | default (`K=32`) | 0.92 s | 32 | 2.80 s | 1.0 s | none |
  | `K=8` | 3.62 s | 8 | 1.80 s | 1.0 s | cell 10 bound |
  | `K=16` | 1.82 s | 16 | 2.60 s | 1.0 s | cell 10 bound |
  | `K=64` | 0.46 s | 64 | 2.50 s | 1.0 s | none |
  | unbounded | 0.33 s | 97 | 2.60 s | 1.0 s | none |
  | `c=2` / `c=8` | 0.92 s | 32 | 1.50 / 3.00 s | 1.0 / 1.5 s | none |
  | `H=50` / `H=200` | n/a | n/a | n/a | 1.5 / 1.0 s | none |

  `K` trades lost-leader latency against burst width roughly linearly. `c` and `S_max` barely move any cell.
- **Tier 2 and Tier 3** are as described in their own as-built notes above. `SimCluster` now runs the governor and the load monitor at their defaults, so every `animusd` `SimCluster` test exercises them.
- **Density acceptance (#1199).** `density_repro_g3000_holds_led_count` (G=3000 RF3 on three `ProdEnv` nodes, mechanisms at defaults, release) passed: all 3000 groups led in 11 s and the led count held for the 120 s window, with `max_term` 3 and governor high-water mark 32. It uses the Tier 3 rig, where groups are hosted through each node's real `host::Reconciler` in the production configuration. It does not use `group_density_cost`'s bare-`RaftKvNode` rig, which never had the mechanisms. `ANIMUS_BP_LIVE_GROUPS=N` changes its group count.
- **Operator.** `AnimusClusterSpec` gains `campaignMaxInflight`, `lagStretchMaxX`, `hostUnsettledMax` and `hostSettleTimeoutMs`, rendered into `cluster.json`'s `cluster_settings` like `maxRegionRttMs`. `hostSettleTimeoutMs: 0` is refused by the webhook and dropped by the renderer, because animusd refuses it at startup.

## Open questions for the maintainer (as originally posed)

1. **Default on or opt-in?** This ADR proposes all three on by default, since the failure is a silent cliff; the cost is that corpora with more than `K` simultaneous campaigns will re-baseline. Opt-in for one release is the conservative alternative.
2. **Defaults.** `K=32`, `H=100`, `c=4`, `S_max=4x` are reasoned but not measured; step 6 sets them. Is a fixed `K` acceptable, or should it scale with a configured core count (an explicit flag, never auto-detected)?
3. **Exempting the control group and transfers.** Proposed exempt (the first is one group per node and everything depends on it; the second is directed and bounded). Confirm.
4. **Should an overloaded node say so?** `hosting_ramp` and the lag gauge are in `/admin/health` as advisory. Should sustained high lag also make a node *fail* its health check (and so the roll gate, ADR 0073 Phase 3), or stay advisory?
5. **A per-node cap on awake groups** (density admission) is the logical next step and is out of scope here. Worth a separate ADR once Tier 2 shows where the stable ceiling is?
6. **Whole-cluster restart tail.** With waves, the last tablets of a very large cluster come up after `ceil(G/H) * T_settle`. Acceptable, or should `H` grow with elapsed ramp time?

## Amendment (2026-10-12): a cold start past the cliff does not elect (#1297)

**Finding.** With quiescence off, G=3000 awake RF3 groups on three `ProdEnv`
nodes (2 worker threads, about 1.9 cores in use) leave about 12% of groups
leaderless for the whole 10 minutes, with `max_term` flat at 3. Reproduced
(`ANIMUS_BP_LIVE_QUIESCE=0 ANIMUS_BP_LIVE_GROUPS=3000`, release): 2623/3000 led
at 600 s, 377 leaderless, `max_term` 3 from 40 s on. This is overload, not a
liveness defect in the governor or pre-vote path. ADR 0078 keeps leadership and
bounds failover while the node is past its cliff; it does not make an election
*complete* once a round trip outlasts the stretch cap, and a cold start is such
a case.

**Evidence** (the rig now prints `BP-DIAG`, `BP-STATS`, `BP-AGE`, `BP-LAG`
lines from `RaftCore::campaign_stats`, `RaftKvNode::campaign_deferred` and a
10 ms scheduler probe):

- *No starvation, no permit leak.* Governor `inflight` sits at `K_eff` (8 to 16,
  halved by lag) on every node for the whole run, `admitted` keeps rising
  (about 12 per second per node at the end), `expired_permits` is 5 to 7 over
  600 s, and the queue depth tracks the number of leaderless replicas.
- *Pre-vote is granted, but too late.* The leaderless groups started 15,495
  pre-vote rounds and the responders granted 30,043 pre-votes (refusals: 9, all
  a vote lease). Only 27 grants were counted toward a majority; 29,099 arrived
  after the round's election timer had lapsed, with `campaign_hold` set, and
  were dropped by the `handle_pre_vote_resp` rule. So 97% of grants exist but
  arrive late. Every pre-vote grant reaches the candidate; none reaches it in
  time.
- *How late.* A dropped grant is on average 1.7 s old at 20 s into the run and
  3.4 s old by 170 s (largest 8.2 s), against a round window of 0.77 to 0.88 s
  (`[b, 2b)` with the lag stretch at its `S_max` cap of 600 ms, `b` = 750 ms).
  The rounds that succeed (counted grants) average 0.3 to 0.5 s.
- *Where the delay is.* A 10 ms tokio sleeper on the same runtime oversleeps by
  10 to 20 ms on average and 130 to 390 ms at most, so the lag monitor reads
  roughly 100 to 400 ms and the stretch saturates at its cap. The message
  pipeline (one connection per destination, one unbatched response per group
  per heartbeat) is far behind the task scheduler: a request-response pair
  takes seconds. Process RSS grows (425 MB at 160 s, 618 MB at 600 s at G=3000;
  flat at G=1250 and 1500), i.e. an unbounded backlog.
- *The demand really exceeds the box.* CPU is saturated at about 1.9 cores from
  G=1250 on (962 ticks per 5 s at G=1250, 1500, 2000 and 3000 alike). ADR 0044's
  own measurement is 1.2 to 1.8 ms/s per awake RF3 group, so 3000 groups need
  3.6 to 5.4 cores. The convergence time degrades monotonically with G (5 s at
  1250, 7 s at 1500, 29 to 57 s at 2000, not done by 250 s at 2500, not at 3000 in 600 s).
  At 2500 the late phase shows term churn (max_term 14, 23 leaderless groups
  with replicas at terms 9 to 11): real elections that start and lose because
  the vote grant round trip also outlasts its window.

**Consequence.** Nothing in the governor or the load monitor can fix this:
the bound `S_max` is deliberate (it caps failover), and a longer window would
only slow the recovery of every group that does fail over. The remedy is not to
host more awake groups than the node can heartbeat (density admission, #1295),
or to quiesce them. The "What this ADR is not" statement stands for groups that
already have a leader; it should be read as not covering an election that has
to *start* past the cliff.

**Not changed.** The `campaign_hold` rule that drops a late grant is correct
(accepting it would start an election without a permit); changing it would not
help while the round trip exceeds the window of the *next* round as well.
`RaftCore::campaign_stats` (pre-vote rounds, grants counted and dropped, the
responder's refusal reasons, ages) stays as a diagnostic.

**2026-10-17 note:** ADR 0080 adds the other half of overload control: this ADR keeps elections alive under CPU starvation; ADR 0080 keeps the data path from queueing without bound (`ProposeResult::Busy`, edge admission, bounded transport queues). The `LoadMonitor` lag feeds both.
