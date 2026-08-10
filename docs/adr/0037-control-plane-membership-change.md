# ADR 0037 — Control-plane membership change

- **Status:** Accepted — implemented (all five PRs landed: #116 core plumbing,
  #120 replicated voter bookkeeping + wire, #122 admin API + CLI, #124
  decommission integration + live-config audit, this PR's end-to-end
  sim/`ProdEnv` tests + docs). Amends ADR 0030 (online cluster growth: "the
  control group stays static") and ADR 0032 (seed/join membership: the
  decommission control-core-id refusal) — see the pointer notes added to
  both. Coordinates with ADR 0036 (cluster-allocated member ids): see
  "Coordination with ADR 0036" below.
- **Date:** 2026-08-10

## Context

ADR 0030 shipped online cluster growth deliberately scoped to the **data**
plane only: "the control group stays static," an accepted limitation at the
time because nothing had yet proven a control-voter membership change safe.
Two things changed since:

1. **`animus-cp-data` already proved single-server `RaftCore::change_membership`
   safe in production** — every per-tablet CP group grows/shrinks/replaces a
   replica through exactly this primitive (`reconfigure_step`, ADR 0017 #3,
   ADR 0029). The control plane's own `RaftCore` is the *same* generic core
   (ADR 0016) — `change_membership`/`transfer_leadership` already existed at
   the core level; only `animus-control`'s own `RaftNode<E>` driver had never
   exposed a thin wrapper over them (cp-data's `node.rs` had one; this crate's
   `node.rs` only ever exposed `propose`).
2. **ADR 0035 already proved a `ControlHandle::Remote`-style mirror/discovery
   pattern works** for "a node with no local control `RaftCore` learns the
   live cluster state over the wire" — the same shape a genuinely elastic
   control group needs for a `Remote` caller (a data-only node, the CLI, the
   dashboard) to discover "who are the current voters."

So the static-group limitation was a *scope* decision, not a *safety* one —
this ADR lifts it by generalizing two things the codebase had already built
for the data plane onto the control plane: (a) cp-data's single-server
reconfiguration primitive, and (b) ADR 0030/0032's "static seed ∪ replicated
overlay" address/membership discovery pattern. No new consensus mechanism, no
joint config — this stays single-server-at-a-time, exactly like cp-data.

## Decision

### Two sources of truth, deliberately kept separate

Exactly the split the tablet model already uses: `Metadata.tablets[t]
.replicas` (the control-plane *decision*) vs. a tablet's own `RaftCore
.config()` (the *live* Raft config), bridged by `reconfigure_step`. Here:

- **Authoritative, transactional**: `RaftCore.config` — the control group's
  own Raft log/snapshot. Changing it is *only* ever done via
  `change_membership` on the current control leader's in-process `RaftNode`
  handle — unchanged by this ADR, no new membership authority invented.
- **Discovery/bookkeeping, eventually-consistent**: `Metadata.node_addrs[*]
  .control: Option<SocketAddr>` (PR2/PR4), replicated through the same
  `Metadata` Raft log the control voters themselves run. Not what quorum math
  reads — what a joining node, a `Remote` data node, `animus admin`, and the
  dashboard read to discover "who to even try talking to." An admin action is
  the bridge between the two (not an automatic reconciler — control-voter
  changes are rare and operator-driven, unlike tablet replica repair).

### `RaftNode` wrappers (PR1)

`RaftNode<E>::change_membership`/`transfer_leadership` (`animus-control`'s
`src/node.rs`) are thin wrappers over the identical `RaftCore` calls
`animus-cp-data` already drives — same shape, plus a `record_reconfigure`
metrics event. Nothing in the sync core changed. `tests/control_membership.rs`
covers this at the driver level: add/remove/catch-up, reject a multi-server
delta / leader self-removal / a second change while one is in flight,
transfer-then-remove the leader, and crash-mid-change (before commit, after
commit) converging to one of exactly two well-defined outcomes — proven both
on a fixed seed and swept across 200 seeds.

### Replicated voter bookkeeping + wire (PR2)

`ControlHandle::config() -> Option<BTreeSet<NodeId>>` (not a bare set): `Local`
is always `Some(raft.config())`; `Remote` answers the last voter set it has
*observed on the wire* (`RemoteControlClient::control_voters`, fed by the same
`Status`/`WatchMetadata` round trip `metadata_fresh()` already makes) — `None`
until the first reply lands, so "never fetched yet" and "the control group
genuinely has zero voters" stay distinguishable. `ClientResponse::Status`
gained `control_voters: BTreeSet<NodeId>` (`#[serde(default)]`) — the
answering node's own live config at reply time, distinct from
`Metadata.node_addrs`'s `role: "control"` bookkeeping (a discovery *hint*: a
node can be registered with the control role and not currently be a live
voter, either before its membership change lands or after removal).

### Admin API + CLI (PR3)

Two new admin actions, deliberately kept **local-control-leader-only and not
relayed** — symmetric with `admin_drain`/`admin_remove_member`'s existing
"destructive, rare, must not silently reach the real leader through a relay
chain" discipline, and structurally necessary here regardless: `change_membership`
is not a `MetaCommand` at all, so there is no "relay" shape for it — only a
genuine control-group voter's own in-process `RaftNode` handle can call it.

| Endpoint | Semantics |
|---|---|
| `POST /admin/control/member/add {node, addr}` | Proposes a bookkeeping `MetaCommand::RegisterNodeAddrs` (merging `addr` into whatever address-book entry already exists for `node`) and then calls `change_membership(current ∪ {node})` on the local leader. Idempotent re-add of an existing voter refreshes the address (no-op success). Refuses an id colliding with an existing member, or at/above `ALLOC_ID_BASE`. |
| `POST /admin/control/member/remove {node}` | Idempotent no-op if `node` isn't currently a voter. Refuses outright if removal would leave **zero** voters. If `node == current leader`, arms a `transfer_leadership` to another live voter, polls (bounded) for the step-down, then reports the same "retry on the leader" refusal every other not-leader case uses — it does not try to complete the removal itself once it has stepped down. Otherwise calls `change_membership` directly and prunes the replicated `.control` address (best-effort). |
| `GET /admin/control/members` | Read-only, serves on any node: `{voters: [...], addrs: {...}}` — the poll target for "has my add committed / caught up." |

**Quorum-loss policy, as shipped (this is the concrete answer to plan §2/§9's
open question, not left ambiguous):**

- **Refuse outright** if the removal would leave `< 1` voters (there is no
  admin action that can recover a control group with zero voters).
- **Warn but proceed** if the removal would leave exactly **1** voter (Raft
  itself tolerates it; the caller — `admin.rs`, the CLI — must surface the
  `warning` field, never silently drop it).
- **No other trigger.** The plan's originally-sketched second trigger —
  "warn if every *other* remaining voter is currently believed `Down`" — was
  assessed during PR3 and **deliberately not implemented**:
  `ControlHandle::believes_alive` is keyed to **raftkv** ids (the failure
  detector's `heartbeat_loop` runs only on the data role, ADR 0012), so
  calling it with a control id is always `false` for reasons that have
  nothing to do with actual liveness — it would fire unconditionally rather
  than only when informative. A combined-mode voter's paired raftkv id
  (`RAFTKV_ID_BASE + control_id`) is only a *naming convention*, not a
  structural guarantee for an operator-chosen `control-add` id, so bridging
  the two id spaces here would be guessing, not reading a real signal.
- **The accepted consequence** (proven directly, not just asserted in prose,
  by `tests/control_membership.rs::
  removing_a_live_voter_while_a_third_is_already_dead_can_strand_the_group`
  and its admin-level counterpart in `control_membership_admin.rs`): because
  the guard only ever counts the *resulting* voter set, a removal that goes
  from an odd-sized group (majority tolerates one failure) to an even-sized
  one (majority tolerates none) **while a survivor is already genuinely
  dead** carries **no warning at all** if the resulting count is `2` or more
  — even though the group is now permanently wedged (the removal's own
  config-change entry can never itself commit, so `config_change_in_flight`
  never clears, and every further membership change fails with "already in
  flight" forever). This is the main new operational risk this ADR
  knowingly accepts, not a bug the tests are hiding — see Consequences.

`animus-cli`'s `animus admin control-add`/`control-remove`/`control-grow`
(`crates/animus-cli/src/main.rs`) are thin wrappers, plus `control-grow`'s own
client-side loop: growing N voters at once is **always** a sequence of
single-server deltas (the core's own single-server-delta rule), so
`control-grow` adds one id, polls `GET /admin/control/members` on the
newly-added node's own admin port until it reports itself a voter, then
proceeds to the next — never one multi-server change.

### Decommission integration + live-config audit (PR4)

`admin_remove_member`'s control-core-id refusal (`lib.rs`) changed from
checking the **static** `ClusterConfig`-derived `control_ids` snapshot to the
**live** `ctx.control.config()` — a node that *used to be* a control-core
voter but has since been `control-remove`d now decommissions its data role
normally; a node that is still a *live* voter (even one added at runtime, an
id the static list never knew about) is still correctly refused. `animus
admin decommission --force-control-remove` runs `control-remove` first (with
its own leadership-transfer handling) and only then the ordinary
drain→drain-status→remove flow, unlocking the "decommission a combined node
that is also a control voter" runbook below.

This PR also did the "grep every `control_ids` read" audit the plan called
out as the single biggest risk in this whole stack (the same class of bug as
the ADR 0029 ReadIndex live-vs-static-config trap already in
`docs/engineering-lessons.md`): every read that decides a *correctness*
question now reads the live config; every read that's a *seed/bootstrap*
value (peer books, heartbeat targets) was deliberately left static, with one
exception knowingly deferred — see the two deferrals below.

### End-to-end tests + this doc (PR5)

The full `docs/engineering-lessons.md`-linked §9 failure-case list, closed out
as follows (see the PR description for the explicit "already covered vs.
newly written" mapping): the crash-mid-change cases, leader-removal
happy/timeout paths, and the core-level in-flight rejection were already
covered by PR1–PR4's own tests; this PR adds the quorum-loss-with-a-genuinely-
dead-voter risk (both at the core level and through the real admin HTTP
path), the in-flight rejection surfacing as a clean retryable `409` through
the admin path (not just at the core), a freshly-added voter's process
restarting mid-catch-up and resuming, a real `ProdEnv` growth-liveness check
(3→5 under real threads/time, bounded leadership churn), and a genuine
multi-process split-deployment scenario (`control_membership_split.rs`) that
grows the control quorum and replaces a voter while data-plane traffic keeps
flowing.

## Operator runbooks (§7)

**Grow 3→5:** start two new `animusd control` (or combined) processes whose
own `--config` lists only the *current* voters as their control peer book
(they sit as quiet non-voters — no traffic addressed to them yet, since
nobody else's config includes them). `animus admin control-grow
<leader-admin-addr> 3 <addr3> 4 <addr4>` adds id 3, polls it to convergence,
then adds id 4. Data nodes' peer books need no manual step — the existing
"static seed ∪ replicated `node_addrs` overlay" (`control_peer_sync_loop`)
picks up every new control address on its next tick.

**Replace a dead voter:** the dead voter is unreachable but still occupies a
config slot. `animus admin control-remove <leader> <dead-id>` (refuses only if
`dead-id` is the current leader — impossible for a genuinely dead node, since
a dead leader triggers an election first) → `animus admin control-add
<leader> <new-id> <new-addr>` for its replacement. **No automatic
replace-on-failure** — deliberately operator-driven (see Non-goals). Operators
should check `GET /admin/control/members` reachability before removing a
*second* voter if a *third* is suspected dead — the shipped guard, as
documented above, will not catch that combination for you.

**Decommission a combined node that is also a control voter:** two-phase —
(1) `animus admin decommission <addr> <id> --force-control-remove` runs
`control-remove` first (auto-transferring leadership away if the target is
currently leader); (2) once it's no longer a control voter, the ordinary
drain→drain-status→remove data-plane decommission proceeds unchanged (it was
refused before phase 1 could happen, since `admin_remove_member`'s live-config
check — PR4 — still correctly refuses a live control voter without the flag).

## Coordination with ADR 0036

ADR 0036 (cluster-allocated member ids) shipped in parallel and is **not**
wired into this stack's `control-add`: an id is still **operator-supplied**,
below `animus_control::meta::ALLOC_ID_BASE` (refused at/above it, so the two
ranges stay structurally disjoint even though nothing forces an operator to
pick from below it today) — a deliberate stopgap, exactly as ADR 0036's own
"Follow-on work this sets up" section anticipated: "control-plane membership
change (a future ADR) would let an allocated id's node become a *real*
control voter instead of a permanent non-voter." Wiring `control-add` to mint
its id from the same allocator (rather than requiring an operator-chosen one)
is future work, not solved twice here.

## Non-goals

- **Joint/multi-server config changes.** Single-server-at-a-time only, same
  as cp-data — growing by more than one voter is a client-side loop
  (`control-grow`) over sequential single-server deltas, not a server-side
  joint-consensus mechanism.
- **Automatic replace-on-failure for control voters.** Unlike cp-data's
  `reconfigure_step` (which repairs a dead tablet replica automatically), a
  dead control voter is *not* automatically replaced — deliberately kept
  operator-driven, since a control-quorum change is rare and high-stakes.
  Automating it is future work if the manual-replacement operational cost
  proves too high in practice.
- **A liveness-aware quorum-loss guard.** As documented above: the shipped
  guard counts the resulting voter set only. A real per-control-voter
  liveness signal (closing the id-space mismatch `believes_alive` hits)
  is left as follow-up work, not built here. **Update: implemented by the
  ADR 0037 hardening trio's PR 2 (PR #136, the quorum-guard liveness fix)** — not by
  bridging `believes_alive`'s raftkv-keyed signal, but by growing a new,
  genuinely control-id-native one: `RaftCore::peer_last_contact`
  (stamped from the leader's own `AppendEntriesResp` traffic, success or
  reject) backs `RaftNode::control_peer_believed_alive`
  (`CONTROL_PEER_LIVENESS_TIMEOUT = 500ms`). `admin_remove_control_member`
  now refuses a removal that would leave fewer than a majority of the
  *resulting* voters reachable, with a `force: bool` escape hatch
  (`animus admin control-remove ... [--force]`) deliberately **independent**
  of `decommission --force-control-remove` — see `animus-control/CLAUDE.md`'s
  `node.rs` entry, `animusd/CLAUDE.md`'s "Control-plane membership change"
  gotcha, and `docs/engineering-lessons.md`'s two closed entries for the
  mechanism, the consumer, and the general lesson.
- **Wiring ADR 0036's allocator into `control-add`.** See "Coordination with
  ADR 0036" above.
- **Fixing `heartbeat_loop`'s static destination list.** See "Known
  deferrals" below. **Update: closed by PR #134** — see that deferral's own
  "Update" paragraph for the fix (which turned out to be two parts, not one).

## Consequences

- **Closes ADR 0030's static-control-group limitation** and **ADR 0032's
  static control-core-id decommission refusal** — the control deployment is
  now operationally elastic: grow, shrink, and replace a voter at runtime,
  with a tested end-to-end operator flow for each of the three runbooks
  above.
- **The main new risk this ADR knowingly accepts**: a *bad* `control-remove`
  call — one that removes a live voter while a different voter is already
  dead, without the operator having checked reachability first — can
  permanently wedge the control group for any further membership change (not
  data-plane traffic, which is unaffected; only *further control-plane
  membership changes* are blocked, since the stuck config-change entry never
  commits). This was previously impossible by construction (the group
  couldn't change at all); it is now guarded only by the count-only
  refuse/warn policy documented above, which this ADR treats as a deliberate,
  documented trade-off rather than a defect — the alternative (a
  liveness-aware guard) needs the id-space unification flagged as future
  work, not a quick fix.

  **Update: the ADR 0037 hardening trio's PR 2 (PR #136) closes this risk by default**
  — the count-only guard above is superseded by the liveness-aware one (see
  the Non-goals update above): the exact scenario this bullet describes (a
  removal that strands the group because a *different* survivor is already
  dead) is now refused outright unless the operator passes `--force`. The
  risk is not eliminated — `--force` still reaches it, deliberately, since
  an operator sometimes genuinely needs to push through a removal despite a
  known-dead peer (e.g. mid-replacement) — but it is no longer the
  *unconditional default*; using it is now informed consent to a named,
  explained risk rather than a silent gap the count-only guard couldn't see.
- **`admin_remove_member`'s control-core-id check is now dynamic, not
  static** (PR4) — every other `control_ids`/`admin.control_ids` read in
  `animusd` was audited and left static on purpose (legitimate seed/bootstrap
  uses), with the two exceptions below.

### Known deferrals (carried forward from PR4, recorded here so they don't
silently vanish between PRs)

1. **`heartbeat_loop`'s `control_ids` (heartbeat destination list) is a
   bring-up-time snapshot with no live-overlay refresh.** A raftkv node
   started before a control voter is added at runtime never heartbeats that
   voter directly, so if it later becomes leader, this specific
   already-running raftkv node's heartbeats keep missing it — bounded in
   practice (every *other* raftkv node's heartbeats still reach it, and a
   restart re-reads current `control_ids`), but a real, if narrow, gap.
   Flagged and deliberately left as a follow-up, not fixed in this stack —
   see `crates/animusd/CLAUDE.md`'s "cluster's members are the raftkv ids"
   gotcha and `docs/engineering-lessons.md`'s ADR 0037 PR4 audit entry for
   the full reasoning on why this one was scoped out.

   **Update: closed by PR #134** (the ADR 0037 hardening trio's PR 1). The
   fix is two parts, not one — this deferral's own text above named only the
   destination-list half; the investigation that closed it surfaced a
   second, previously-undocumented half the original text missed entirely:
   even a fully live destination list is inert if the *sending* node's
   `raftkv` env peer book never learns the runtime-added voter's *address*.
   `peer_sync_loop` merged `Metadata.cp_member_addrs`/`node_addrs[*].raftkv`
   into that book, but never `node_addrs[*].control` — so `ProdEnv::send`
   silently dropped every heartbeat aimed at a runtime-added voter even once
   it was correctly named as a destination (`ProdEnv::send`'s own doc: an
   address-less peer is a fire-and-forget drop, no error surfaced anywhere).
   Both halves shipped together, since fixing only one leaves the other's
   silent drop in place: (a) `peer_sync_loop` now also merges
   `node_addrs[*].control` into the raftkv env's own peer book, alongside
   its existing `.raftkv`/`cp_member_addrs` merges; (b) a new animusd-local
   `heartbeat_loop_live` re-derives the destination list every tick from
   `ctx.control.config()` (falling back to the static list until a
   `ControlHandle::Remote` data-only node's first live read lands), replacing
   the two animusd call sites that previously pinned
   `animus_control::node::heartbeat_loop`'s static `control_ids` argument
   forever — that function itself (and its sim call sites) is untouched, by
   design: the static-list contract is still correct and load-bearing for
   `SimEnv`. Regression: `tests/heartbeat_live_destinations.rs::
   heartbeat_reaches_a_runtime_added_voter_after_it_becomes_leader` — grows a
   control voter at runtime, forces a deterministic 2-voter leadership
   transfer onto it (not a race: with exactly two voters, a self-removal's
   armed transfer has only one possible target), and proves a pre-existing
   combined node's heartbeats sustain `believes_alive: true` on the new
   leader's own `/admin/raft` view across several `DETECT_TIMEOUT` windows —
   which would have failed on either half of the fix alone. See
   `docs/engineering-lessons.md`'s entry on this PR for the mini-lesson
   (a static-destination-list audit must also check the transport address
   book) and `crates/animusd/CLAUDE.md`'s updated gotcha.
2. **`control-grow`'s CLI-side orchestration loop is tested server-side, not
   at the CLI-binary level.** `animus-cli` has no integration-test harness
   that spawns the actual `animus` binary; `control-grow`'s sequential-add
   behavior is proven by directly exercising the same admin actions it calls
   (`control_membership_admin.rs`, `control_membership_split.rs`) in the
   same sequence the CLI loop would drive, not by invoking the CLI process
   itself. A CLI-level test harness is out of scope for this stack.

## Engineering lesson

Recorded in `docs/engineering-lessons.md`: a count-only quorum guard (refuse
`< 1`, warn `== 1`) looks complete because it correctly handles every case a
test that only ever kills *the node being removed* will exercise — the real
gap only shows up when a *different*, already-dead survivor is left
untouched by the removal itself, so the resulting count looks safe (2 or
more) while the resulting *fault tolerance* is not (an even-sized group with
a dead member can be worse than the smaller odd-sized group it replaced).
Any "how many are left" quorum check needs an explicit test for "one of the
*other* survivors is already gone," not just "how many remain after this
one action," or it will pass every test that only ever removes/kills one
node at a time and still ship a silent stranding hazard.

**Update: closed by the ADR 0037 hardening trio's PR 2 (PR #136)** — see the Non-goals
and Consequences "Update" paragraphs above, and `docs/engineering-lessons.md`'s
matching closure notes on its "id-space mismatch" and "resulting count only"
entries.
