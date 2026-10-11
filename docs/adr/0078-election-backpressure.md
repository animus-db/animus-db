# ADR 0078 — Election backpressure: per-node campaign admission, lag-stretched election timeouts, staggered hosting

- **Status:** Proposed (2026-10-11) — design only; nothing is implemented
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

### Tier 3: `ProdEnv` liveness, `animus-cp-data` `tests/election_backpressure_liveness.rs`

Its own `tests/*.rs` target (`ProdEnv`, real threads, `prod-heavy`), a timeout-guarded `#[tokio::test(multi_thread)]`, per `docs/lessons/testing/`:

- **Per-push (`ProdEnv`, light):** 3 `ProdEnv` nodes, RF3, G=300, the runtime built with `worker_threads(2)` so scheduling lateness is real and reproducible. Host all at once; assert all led within a bound, `max_term` below a ceiling, `campaign_inflight_hwm <= K`. Then kill one replica's node and assert every group it led is re-led within the lost-leader bound. Converged-or-timeout polling, not a one-shot assert.
- **Ignored, nightly or manual:** the issue's repro, `ANIMUS_DENSITY_RF=3 ANIMUS_DENSITY_GROUPS=3000 ANIMUS_DENSITY_BATCH=0 ... group_density_cost`, must reach "all led" and hold the led count for a 120 s window (the shape that today decays), both with and without a harness-side batch. The acceptance for closing #1199.

SimEnv does not replace the `ProdEnv` test: it proves the ordering and the bound, not that a real runtime's timers behave the way the `LoadMonitor` assumes.

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

## Open questions for the maintainer

1. **Default on or opt-in?** This ADR proposes all three on by default, since the failure is a silent cliff; the cost is that corpora with more than `K` simultaneous campaigns will re-baseline. Opt-in for one release is the conservative alternative.
2. **Defaults.** `K=32`, `H=100`, `c=4`, `S_max=4x` are reasoned but not measured; step 6 sets them. Is a fixed `K` acceptable, or should it scale with a configured core count (an explicit flag, never auto-detected)?
3. **Exempting the control group and transfers.** Proposed exempt (the first is one group per node and everything depends on it; the second is directed and bounded). Confirm.
4. **Should an overloaded node say so?** `hosting_ramp` and the lag gauge are in `/admin/health` as advisory. Should sustained high lag also make a node *fail* its health check (and so the roll gate, ADR 0073 Phase 3), or stay advisory?
5. **A per-node cap on awake groups** (density admission) is the logical next step and is out of scope here. Worth a separate ADR once Tier 2 shows where the stable ceiling is?
6. **Whole-cluster restart tail.** With waves, the last tablets of a very large cluster come up after `ceil(G/H) * T_settle`. Acceptable, or should `H` grow with elapsed ramp time?
