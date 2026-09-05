# Engineering-lessons archive

This file holds engineering-lessons entries (from
[`engineering-lessons.md`](engineering-lessons.md), formerly the root
`CLAUDE.md`'s "Engineering practices" section) that have
been **superseded**: the specific mechanism, function, or field each entry
describes has since been deleted or replaced by a later redesign, so the
entry no longer describes any code in this repository. They are moved here
**verbatim** (unedited) rather than deleted, because the *bug* each one
documents, and the general lesson drawn from it, remain part of this
project's institutional memory — grep this file when a change rhymes with
one of these stories, even though the specific names/functions no longer
exist.

Where a superseded entry's lesson is still generally applicable,
`engineering-lessons.md` keeps a one-line pointer back here instead of the
full entry.
Entries are grouped by the ADR/PR that superseded them and otherwise appear
in their original order.

## Superseded by ADR 0028 (single-command, control-plane-only tablet split)

ADR 0028 replaced the original two-phase tablet split (a control-plane
metadata write *plus* a separate data-plane `KvCommand::Split`/
`propose_split` command that could fail, race, or orphan independently) with
a single, epoch-CAS-gated `MetaCommand::SplitTablet` that is the *entire*
operation — the new sibling tablet's `StorageScope` covers already-present
data on the same node-shared storage engine (ADR 0026/0028), so there is no
handoff, no new-group bootstrap message, and no data-plane half left to fail
independently. Every mechanism below (the data-plane `Split` command,
`DropOrphanTablet`, the `pending` retry map, `current_split_bound`/
`SPLIT_BOUND_KEY`, the split hook + derived member ids, the `cp-hosted`
marker, `Coresident`/`sibling()` minting, `cp_member_id`/`cp_base_id`
translation, `propose_split_data`/`applied_split_key`, and
`claim_auto_split`/`release_auto_split`) no longer exists.

- **Superseded by ADR 0028** (single-command, control-plane-only split — the
  data-plane `KvCommand::Split`/`propose_split` this entry describes is deleted;
  a metadata-level `SplitTablet` is now the *entire* operation). Retained for
  historical record. **Metadata-level dedup of a proposal only picks one *winner* — it does not stop
  other legitimate callers from still invoking a side-effecting state-machine
  command, which must therefore be idempotent at APPLY time, not just deduped at
  the propose layer.** In `--cluster N`, every node's auto-split loop shares one
  `ClusterEdgeState`, so multiple nodes could independently observe the same
  over-threshold tablet and each call `propose_split`; the control plane's
  `SplitTablet` metadata command dedups which proposal wins the *metadata*
  race, but nothing stopped a second `Split` command from also landing in the
  committed CP-group Raft log. Re-applying it recomputed the handoff from
  storage — now empty, since the first application had already tombstoned the
  range — and re-fired the split hook with an empty handoff, which could win
  the mint race and silently seed the new tablet with **no data** (a silent
  flake with zero logged errors, `tablet_auto_splits_when_it_grows`, ~1-in-3 to
  1-in-10 standalone). Fix: make `Split` apply idempotent (a persistent
  `already_split` flag; every application after the first is a no-op) —
  replay-safe and failover-safe by construction, not a patch for one race. Any
  command carrying a hook/side-effect (not a plain value write) that more than
  one caller can legitimately propose needs this. (PR #30.)
- **Superseded by ADR 0028**: `DropOrphanTablet` (and the orphan it exists to
  clean up) no longer exist — a split's single atomic command makes an orphan
  structurally impossible. Retained for historical record. **A CAS guard closes the *concurrent* instance of a race; a *sequential*
  instance of the same race needs its own answer — usually cleanup, not
  another precondition.** The `SplitTablet` epoch CAS above stops two
  proposers from *both* committing at the same epoch, but does nothing when
  the first mint's own commit has already advanced the epoch by the time a
  second, later trigger reads it: that second `SplitTablet` is now proposed
  against the *current* epoch, so the CAS sees no conflict and lets it through
  — even though the underlying CP-data group can still only ever apply one
  real `Split`, so one of the two mints is doomed regardless. Live-observed as
  permanent orphans continuing to accumulate (one per abandonment, unbounded)
  under `--cluster 3 --auto-split 2000` + leader churn, well after the CAS fix
  above landed — the "did I catch the bug" question was scoped to concurrency,
  and a sequential path around the same guard was still open. **A CAS answers
  "is this still the state I read"; it does not answer "did someone already
  reserve the *outcome* my proposal is racing toward" when a valid,
  non-conflicting intermediate state change is enough to let a doomed proposal
  through.** The fix here isn't a stronger CAS (there's no cheap read at
  propose time that would tell the loser it's doomed) — it's giving the
  *already-existing* "abandon" detection (comparing the group's real
  `applied_split_key()` against the proposer's own key) a real cleanup action
  (`MetaCommand::DropOrphanTablet`) instead of only suppressing further
  retries. When a race has both a concurrent and a sequential shape, expect to
  need two different mechanisms — a precondition for the first, a
  detect-and-reclaim for the second — not one guard stretched to cover both.
  (`animus-control` `meta.rs::drop_orphan_tablet_removes_a_never_seeded_split_child`;
  `animusd` `ClientCtx::drop_orphan_tablet`, `auto_split_loop`'s abandon branch,
  `trigger_split`'s step-2 failure path;
  `animusd/tests/cp_plane.rs::a_lost_split_race_does_not_leave_a_permanent_orphan`
  reproduces the *sequential* shape deterministically via two manual splits —
  no timing race needed, since the precondition is just "propose against an
  already-narrowed range.")
- **Superseded by ADR 0028**: `auto_split_loop`'s `pending` retry map (and step 2
  it was retrying) no longer exist — split is a single-step command. Retained
  for historical record. **A retry loop keyed on a resource id must recheck the resource still
  exists — a precondition that only checks its own transient state ("did *my*
  attempt fail in a way I recognize") silently assumes the resource itself is
  immortal.** `auto_split_loop`'s pending-retry map has one such gap left even
  after the fix above: dropping the whole table out from under a still-pending
  split (`DropTableTablets` removes the source tablet, and any child it had
  minted, in one apply) leaves a `pending` entry retrying a tablet id that can
  never have a leader again — the existing "did a different key win"
  abandon-check doesn't fire (`local_cp` on an unregistered id returns `None`),
  so the loop reads this as "still committing" and retries forever, one wasted
  routing round trip per tick. Fixed with the obvious missing check: does the
  target still exist in `Metadata` at all, before retrying. **Not every fix
  needs (or can usefully get) a regression test** — this one has no black-box
  behavioral difference between "gave up" and "quietly retries forever" other
  than resource waste and log noise, so it's verified by review + the existing
  suite staying green rather than by a new assertion; don't force a test where
  there's nothing external to assert against. (`animusd` `auto_split_loop`'s
  retry phase, the tablet-existence guard added right before the confirm
  call.)
- **Superseded by ADR 0028**: `current_split_bound`/`SPLIT_BOUND_KEY` and the
  "a group can be split more than once" data-plane mechanism this entry
  discusses are deleted — split no longer has a data-plane half at all.
  Retained for historical record. **Before reaching for "remember everything" to disambiguate an edge case,
  check whether a cheap, independent check at the point of irreversible action
  can bound the state to O(1) instead.** Lifting the CP-data "a group can only
  ever apply one `Split`" limit (letting a tablet reshard repeatedly as it
  regrows) initially seemed to need a full per-split history: a caller
  confirming "did my key `K` ever apply" needs a real answer even after a
  *later* split narrows past `K`, and a single "current boundary" value can't
  tell "K applied, then something narrowed further" apart from "K never
  applied, something else did instead." A full history answers that
  unambiguously — but it's unbounded (grows with every split a lineage ever
  does, for the life of the process) and, worse, quadratic to maintain (each
  split re-persists the *whole* history, so split N pays to rewrite N-1 prior
  entries) — the same shape of hazard as the control plane's old
  "re-serialize-per-chunk" election-storm bug. The actual fix: keep only the
  current boundary (O(1), forever), accept that the confirm signal is
  sometimes ambiguous, and add a **second, independent, cheap check at the one
  place that does something irreversible** (deleting a tablet) — verify the
  tablet isn't still locally, genuinely hosted before ever touching it. A
  wrong call into the *heuristic* signal now just skips a cleanup opportunity
  (the original, already-tolerated "orphan lingers" outcome); only a wrong
  call into the *hard gate* would cause real harm, and that check is simple
  enough to get right by inspection. Generalizes: when the cost of
  "remembering enough to always be certain" is unbounded growth, ask whether
  the actual danger is concentrated at one action (here, deletion) — if so,
  guard *that action* directly instead of trying to make the upstream signal
  perfectly precise. (`animus-cp-data` `current_split_bound`/`SPLIT_BOUND_KEY`;
  `animusd` `drop_orphan_tablet`'s local-hosting gate.)
- **Superseded by ADR 0028**: the split hook + `base + tablet*STRIDE` member-id
  derivation this entry discusses are deleted (ADR 0026 Stage B made a
  tablet's CP group member id simply the base `raftkv` id, at any split
  depth, so there is no derivation left to get wrong). Retained for
  historical record. **A recursive operation that "works" once may be relying on a depth-1 coincidence —
  prove it at depth ≥ 2.** Tablet *split* worked the first time for two accidental
  reasons that both break at depth 2: (a) only the *bootstrap* group was started with
  a split hook, so a split-created child had no machinery to split *itself*; and (b)
  the member-id derivation `base + tablet*STRIDE` (flat, from the node's base id)
  equals the compounding `parent_member + tablet*STRIDE` *only* because the bootstrap
  parent's member id == its base id — for a grandchild they diverge, and the
  reconfigure loop (which translates the replicated base-id replica set flatly) then
  churns forever on the mismatch. Fix recursive invariants to hold at any depth: give
  **every** spawned instance the same machinery (a hook), and derive ids from a
  **fixed root** (the base id), never the immediate parent. (ADR 0017 deep splits.)
- **Superseded by ADR 0028**: a fresh split child no longer needs handoff
  seeding at all (it's a `StorageScope` over already-present shared-engine
  data), so it forms exactly like a fresh whole-keyspace tablet — there is no
  more "fresh split child vs. join" distinction to make. Retained for
  historical record. **Distinguish "seed a fresh child" from "join an existing group empty" by a durable
  monotonic signal, not a race.** A node *added* to a tablet's replica set by the
  reconciler must host an **empty** group and catch up via `InstallSnapshot`; an
  *original* replica of a fresh split must **seed** from its local handed-off data —
  starting empty there loses data. Don't let a polling host-loop race the split hook
  to decide which; gate on the tablet **epoch** (`INITIAL` = fresh split → leave it to
  the hook; bumped by a reconfigure → a join → host empty). A deterministic signal
  turns a data-loss race into a clean branch. (ADR 0017 D1 join-hosting.)
- **Superseded by ADR 0028**: the `cp-hosted` durable marker this entry
  describes is deleted — every tablet on a node now shares one `LsmEngine`,
  opened once at node start, so there is no per-tablet "which engines exist
  here" question left to answer; a restart just re-discovers every tablet to
  host from replicated `Metadata`. Retained for historical record. **Which physical engines a node hosts is *local* durable state — a marker file,
  not derivable from replicated `Metadata`.** Re-hosting a node's per-tablet CP
  groups after a restart (ADR 0017 #2) can't be driven purely off the replicated
  tablet map: that map records placement in **stable base node ids**, not which
  co-resident `sib-<id>/db-t{id}-` engines actually exist on *this* node. So
  `animusd` writes a small durable `cp-hosted` marker (per `raftkv` env) when it
  stands up a split tablet's group, and reads it at start to re-host (recover the
  engine + WAL). Bonus: pre-populating the per-node mint-guard (`minted`) from that
  marker *before* starting the parent group gives **split crash-idempotency** — the
  parent re-applying its committed `Split` on WAL recovery finds the tablet already
  hosted and won't mint the sibling twice. (A genuinely-local durable record is fine;
  the "prefer a live read of the durable layer" caution is about *stale derived
  caches*, which this is not.)
- **Superseded by ADR 0026 Stage B / ADR 0028**: a tablet's CP group member id
  is now simply its base `raftkv` id (stream-addressed, not a derived
  `NodeId`), so the base↔member translation this entry describes no longer
  exists. Retained for historical record. **Keep the replicated tablet map in stable base node ids; translate to per-tablet
  group member ids at the edge.** A tablet's Raft *group member ids* differ from the
  node's base id (a split tablet uses `base + tablet*STRIDE` so co-resident groups
  get distinct inboxes), but failure-detection and placement speak **base ids**. So
  `Metadata.tablets[t].replicas` stays base ids, and the data-plane reconfigure loop
  translates with one function (`cp_members_for`) so its `desired` set matches the
  running group's `config()` exactly — no spurious reconfigure churn, and no need to
  reconcile the map to derived ids. The bootstrap tablet is the identity case
  (member == base); only split tablets derive.
- **Superseded by ADR 0026/0028 for this use**: `animus-cp-data` no longer uses
  `Coresident`/`sibling()` at all — every tablet a node hosts shares one env,
  addressed by `stream` (ADR 0026 Stage B). The `Coresident` trait itself still
  exists in `animus-env`/`animus-sim` (unused by cp-data now); the pattern
  below (sub-trait, not supertrait) is still the right one if a future
  capability needs it. Retained for historical record. **Extend the `Env` seam with a *sub-trait* bound only where used, not by widening
  the supertrait — capabilities not every env has stay opt-in.** In-band tablet
  split (ADR 0017 D) needs a node to mint a second inbox at runtime
  (`sibling(id) -> Self`). Adding that to the `Env` supertrait would force *every*
  env — `ProdEnv` included — to implement runtime inbox-minting (an unsolved
  production-network problem) just to compile. Instead it's a separate
  `Coresident: Env` trait that only the split path bounds on (`impl<E: Coresident,
  S> RaftKvNode<E, S>`), so `SimEnv` implements it, `ProdEnv` doesn't yet, and
  nothing else changes. Same shape as the metrics seam (additive, default-off) but
  via a trait bound rather than a defaulted method, because it returns `Self`. Keep
  the consumer generic over `Env` and inject the capability where needed (here, a
  `SplitHook` closure built with a `Coresident` env), so the driver stays
  `<E: Env>` and existing call paths (`split.rs`, hook = `None`) are byte-identical.
- **Superseded by ADR 0026 Stage B / ADR 0028**: `cp_member_id`/`cp_base_id`
  are deleted; `cp_forward_target` no longer translates at all, since a
  tablet's member id is always its base id. Retained for historical record. **An id-translation seam must be applied in *both* directions — and the identity
  case masks the missing one.** The tablet map speaks stable **base** node ids and a
  tablet group speaks **derived member** ids (`cp_member_id`); `cp_forward_target`
  consumed a group's leader *hint* (a member id) as a `client_route` key (base ids) —
  and worked anyway for the bootstrap tablet, where member == base. It also worked
  for the **first** provisioned table, which wins the tablet-id race with bootstrap
  and rides the bootstrap group; only a **second** table (or split child) gets
  derived ids, so the miss surfaced as a bimodal flake ("no CP group leader
  reachable" on a *healthy, led* group — the follower had the hint but couldn't map
  it, and having a local replica suppressed the forward-anywhere fallback, so it
  waited out `CLIENT_TIMEOUT`). Fixes and morals: add the inverse (`cp_base_id`) at
  the same seam as the forward map; when debugging "no leader", first dump the
  group state (`/admin/raftkv`) — *formed-but-unroutable* looks identical to
  *never-formed* from the client; and regression-test derived-id paths with a
  **second** provisioned table, per-process, reading via **every** node (≥2 forced
  forwards, deterministic teeth wherever the leader lands).
  (`animusd` `cp_forward_target`; `cp_cross_process.rs::second_table_…`.)
- **Superseded by ADR 0028**: split has no step 2 anymore — `SplitTablet` is
  the whole operation, so this entry's specific scenario (a step-2
  `propose_split` failure stranding step 1's mint) can no longer occur.
  Retained for historical record; the *general* two-step-operation lesson
  still applies elsewhere. **A two-step operation where step 1 is a cheap, always-visible metadata write and
  step 2 is the expensive, failure-prone "make it real" step must never let a
  background loop discard a step-2 failure — that silently strands step 1's
  effect forever.** `animusd`'s tablet auto-split (`auto_split_loop`) commits
  `MetaCommand::SplitTablet` (step 1 — instantly makes the new tablet visible in
  `Metadata.tablets` with a real range/replica set) then calls `propose_split` on
  the tablet's CP-group leader (step 2 — actually mints the Raft group). The loop
  used to do `let _ = ctx.trigger_split(..).await`; under real leader churn (bulk
  writes causing elections) step 2 fails with `NotLeader`/no-route often enough
  that discarding it left permanently orphaned tablets — `leader: unknown` forever,
  any read/write to that range hangs — and *worse*, since the underlying data never
  physically moved on a step-2 failure, the source tablet kept re-triggering on
  later ticks and minting *more* orphans from the same unshrunk dataset (observed:
  9 of 13 splits orphaned in one bulk-seed run). Fix: track step-1-committed/
  step-2-pending tablets and retry step 2 with the *same* split key every tick
  until it succeeds (safe — `propose_split` is idempotent per group), and skip a
  tablet for a *fresh* split while one is already pending (stops the cascade). The
  general rule: when step 1 of a two-phase operation is itself durable/replicated
  and only step 2 completes the effect, a caller that abandons step 2 must not also
  let step 1's artifact linger unreconciled — either retry step 2 forever (this
  case) or roll back step 1. Regression-tested as a pure decision function
  (`is_fresh_split_candidate`, mirroring `topology::decide_cp_route`'s split), since
  the real leader-churn race isn't reproducible on demand over real network/time —
  when an integration-level repro of a race is impractical, extract the invariant
  it depends on into a pure function and unit-test *that*.
- **Superseded by ADR 0028**: `propose_split_data`/`applied_split_key`/the
  `pending`-retry map this entry discusses are all deleted along with the
  data-plane half of split. Retained for historical record; the general
  "`Accepted` isn't `committed`" lesson still applies (see `cp_put_local`'s
  own confirm-by-index). **`ProposeResult::Accepted` means "appended to the leader's local log," never
  "committed" — every proposer must confirm, and a bare boolean flag isn't always
  enough to confirm the caller's *specific* request.** Fixing the auto-split
  orphan bug (the entry above) closed the case where step 2 (`propose_split`)
  returns a hard error, but tracing (ADR 0027 instrumentation added specifically
  to diagnose this) showed a second, worse failure mode: `propose_split_data`
  trusted `Accepted` as final success, like every CP-data write path *except*
  this one (`cp_put_local`/`cp_delete_local` already poll for local confirmation
  first — see `engine_applied_index`'s doc). Under the leader churn this
  workload causes, an accepted-but-uncommitted `Split` entry is silently
  truncated, and since the control-plane `SplitTablet` metadata was *already*
  committed by that point, an unconfirmed "success" permanently stranded the
  tablet with nothing left to retry it (my own `pending`-retry fix never
  engaged, because nothing looked like a failure). Fix: added
  `RaftKvNode::applied_split_key()` (mirrors `engine_applied_index`'s
  confirm-by-index shape) and made `propose_split_data`/`cp_split_here` poll it
  before reporting success. **The key, not just a flag, mattered**: an initial
  version exposed a bare `is_split() -> bool`, but live verification (rebuild +
  reproduce + re-check `/admin/raftkv`, not just re-reading the fix) showed
  tablets *still* going unminted — under `--cluster N`'s shared edge state, two
  different nodes' auto-split loops can independently read a tablet's live pairs
  and compute a *different* median in the same tick (the pairs can shift
  between two racing reads), each proposing a `Split` at their own key; the
  group splits once, so the loser's key never applies even though *a* split
  did — a bare "has it split" boolean can't tell a caller its own key lost.
  Comparing the *exact* applied key closed it. **Corollary**: a `pending`-retry
  map keyed on "keep replaying the same request" needs an exit for "this exact
  request can now never succeed, but that's fine" (a losing key retried forever
  is a live-forever no-op that also wrongly excludes the tablet from future
  triggers) — not just "retry until it succeeds." **Method note**: when a live
  reproduction is available, verify a fix by rebuilding and re-observing state,
  not just re-reading the diff — the first fix compiled, passed all tests, and
  still didn't fully close the bug in practice.
- **Superseded by ADR 0028**: `claim_auto_split`/`release_auto_split` and the
  cluster-wide contention guard this entry describes are deleted — a
  same-tick redundant `SplitTablet` from multiple nodes is now just a normal
  epoch-CAS race with one clean winner, no orphan risk to guard against.
  Retained for historical record. **`auto_split_loop`'s "only the leader's host triggers" gate
  (`ctx.edge.cp_leader(tablet)`) is *not* actually node-scoped — it is scoped
  to the shared registry, and under `--cluster N` that registry is shared by
  every node.** `ClusterEdgeState::cp_leader` scans **every** registered CP
  group handle for a tablet (across the whole cluster's shared `raftkv` map in
  `--cluster N` dev mode) and returns whichever one `is_leader()` — it has no
  concept of "which node is asking." So in a 3-node `--cluster N` run, all 3
  nodes' independent `auto_split_loop` tasks see `Some(leader)` simultaneously
  whenever *any* replica leads (always true), and all 3 can independently
  compute a (possibly different) median and propose a fresh `SplitTablet` for
  the *same* source tablet in the same tick — live-observed as 3 near-
  simultaneous `step 1 (split metadata) did not commit` warnings for the same
  tablet, and as a tablet's split id churning upward forever
  (8→10→12…, each new metadata-only tablet permanently orphaned once a
  different key wins the source group's one-time data-plane split). Same root
  cause as the documented "per-node decision must dedup on per-node state,
  never the shared `ClusterEdgeState`" gotcha (`cp_join_host_loop`,
  `/admin/raftkv`'s node-local caveat) — just not yet swept into this loop.
  Unlike those, `local_cp`-style per-node scoping isn't available here without
  threading this node's own id through the loop, so the fix instead adds a
  cluster-wide (not per-node) claim set,
  `ClusterEdgeState::claim_auto_split`/`release_auto_split`, that a loop must
  win before proposing a fresh split for a tablet — held for the whole
  attempt (step 1, step 2, any pending retry) and released only on a
  terminal outcome (success, step-1 failure, or abandonment). **General
  check when adding a new "only the owning node acts" gate near
  `ClusterEdgeState`: does the underlying registry actually distinguish
  callers by node, or does it just answer "does *anyone* in the cluster
  satisfy this" — those are silently identical in `--cluster N` and only
  diverge (bimodally) in a real one-process-per-node deployment.**
- **Superseded by ADR 0028**: the abandon path + `pending` map this entry
  describes are deleted along with the rest of the two-phase split retry
  machinery. Retained for historical record. **An "abandon and forget" exit from a retry loop must still leave the
  cooldown state a *fresh* attempt would have set — otherwise the tablet is
  eligible again on the very next tick, not after backing off.**
  `auto_split_loop`'s pending-retry loop drops a tablet from `pending` when it
  detects its own key lost the group's one-time split to a different proposer
  (`abandoned`, see the entry above on the confirm-by-key check) — correct,
  since retrying a losing key can never succeed. But it only cleared `pending`
  and didn't touch `last_triggered`, so `is_fresh_split_candidate` (which
  excludes a tablet only while it's `pending` *or* within
  `AUTO_SPLIT_COOLDOWN` of `last_triggered`) saw a clean slate immediately —
  this node's *next* tick could propose a brand-new fresh split for the same
  source tablet with a new median, right away. Combined with any repeated
  contention on that tablet (this is exactly what the cross-node
  `auto_split_loop` contention entry above produces), this manifested as a
  tablet's split id climbing every tick forever (8→10→12…), each attempt
  abandoned in turn, never actually converging. Fixed by inserting into
  `last_triggered` on the abandon path too, identical to what a fresh trigger
  already does — so an abandoned tablet backs off for the normal cooldown
  before being reconsidered, giving the *winning* split's data move a chance
  to actually shrink it below `threshold`. **General check for any "give up
  and exit the retry path" branch: does it leave the surrounding loop's
  own rate-limit/cooldown state as if this had been a normal, successful
  cycle — not just as if this attempt had never happened?**

## Superseded by ADR 0031 (per-node `ClusterEdgeState`, tablet-host reconciler)

ADR 0031 made `--cluster N`'s in-process `ClusterEdgeState` genuinely
per-node (PR2), then replaced four independent `ProdEnv` polling loops
(`cp_join_host_loop`, `cp_gc_loop`, `cp_reconfigure_loop`, plus their
per-node bookkeeping — `minted`, `pending_release`) with one pure planner
(`animus_cp_data::host::plan`, PR3) and one event-driven per-node executor
(`animus_cp_data::host::Reconciler`, driven by `animusd::
tablet_host_reconciler_loop`, PR4). The loops, the `minted`/`pending_release`
fields, and the fixed 500ms/150ms polling cadences these entries describe no
longer exist in `animusd`; see `crates/animus-cp-data/CLAUDE.md`'s "Per-node
tablet-host reconciler" section for the replacement.

- **Superseded by ADR 0031 PR4 for this specific pair**: `cp_reconfigure_loop`
  and its `jitter`/150ms-cadence mitigation are deleted — the tablet-host
  reconciler reacts to a `metadata_watch` wake (event-driven), so it observes a
  replica-set change on the commit that made it and there is no cadence ratio
  left to tune against `reconcile_loop`. Retained for historical record; the
  *general* lesson (two independent fixed-period pollers racing a one-shot
  outcome) still applies to any future pair of loops. **Two independent, un-jittered fixed-period polling loops that can each "win"
  a one-shot outcome are a real, silent flake source — not just theoretical.**
  Rewiring tablet split to a single control-plane command surfaced this in
  `animusd::cp_reconfigure_loop` (steps a CP group's Raft voters toward a
  tablet's replicated replica set) racing `animus-control`'s `reconcile_loop`
  (re-CASes a replica set back to satisfy its placement *policy*): a manual
  replica-set drop is a **one-shot** race — whichever loop observes it first
  decides the outcome, because the loser's own next tick sees an
  already-equal-to-desired state and never retries. Both loops polled a fixed
  500ms, so once one loop happened to start ticking with an unlucky phase
  offset relative to the other (here: an eager, synchronous, awaited
  `LsmEngine::open` inserted before the loop's spawn point, versus the
  other side's non-blocking, self-spawning `RaftNode::start`), it lost *every*
  time, deterministically, for the life of the process — reproduced as
  `animusd::tests::cp_reconfigure::cp_group_follows_tablet_replica_set` timing
  out 100% of runs in isolation. Re-rolling a random jitter each tick only
  turned "always loses" into "wins about half the time across separate test
  runs" (a one-shot race stays a coin flip no matter how much you jitter one
  side — jitter decorrelates *repeated* ticks, but there's only one tick that
  matters here). What actually fixed it: making the loop that must react to an
  operator-driven change poll **meaningfully faster** than the policy
  loop it's racing (`CP_RECONFIGURE_INTERVAL` cut from 500ms to 150ms, a third
  of `RECONCILE_INTERVAL`), so it overwhelmingly observes the change first.
  When two independent pollers can produce a stable-but-wrong equilibrium by
  one of them winning a single race, check not just "is each one individually
  correct" but "does either one systematically have first-mover advantage,
  and does that matter" — and if a manual/operator action must reliably beat
  an automatic policy-enforcement loop, poll for it faster, don't just add
  jitter. (`animusd` `cp_reconfigure_loop`/`jitter`.)

- **A *per-node* decision must dedup on *per-node* state, never on the shared
  `ClusterEdgeState` — in `--cluster N` that edge is shared across nodes and silently
  reports another node's state.** The CP join-host loop (ADR 0023 provisioning) gated
  "already hosting this tablet?" on `edge.local_cp(tablet)`. In one-process-per-node
  that is this node's view; in an in-process `--cluster N` run the edge is **shared**,
  so as soon as *one* replica hosted a freshly provisioned tablet and registered it,
  every other replica's loop saw it via `edge.local_cp` and **skipped** — leaving the
  tablet hosted on a single replica, no majority, no election, "no CP group leader
  reachable". The signature was **bimodal flakiness** (race: all replicas host iff
  they poll before the first registers, ≈1.5 s; else one hosts and it stalls to the
  timeout). Dedup on the genuinely per-node `minted` claim set instead. This is the
  *hosting-path* instance of the documented "shared `--cluster` edge masks per-node"
  gotcha — assume any `edge.*` read is cluster-wide in `--cluster N`. (`animusd`
  `cp_join_host_loop`. Both halves of this entry are historical now: ADR 0031
  PR2 made the edge genuinely per-node, and PR4 replaced
  `cp_join_host_loop`/`minted` with the tablet-host reconciler's own
  `LocalState::hosted` — which is per-node *by construction*, since the
  reconciler owns the hosted map outright.)

- **Mechanism superseded by ADR 0031 PR4**: `cp_join_host_loop` (and its
  per-tick unconditional re-narrow) is deleted — the tablet-host reconciler's
  planner emits an explicit `NarrowScope` action whenever a hosted tablet's
  metadata range shrank, so the re-sync is now a planned, ordered action
  rather than a per-tick patch-up. The *lesson* (a cached per-node handle
  derived from replicated state needs an explicit re-sync for every way that
  state can change) is exactly what the reconciler design institutionalizes;
  retained for the record. **A cached per-node handle derived from replicated state (here, a
  `StorageScope`'s range) needs an explicit re-sync step for every way that
  state can change in place — "it was correct when constructed" is not "it
  stays correct."** The single-command split redesign (ADR 0028) gave a split
  child's `RaftKvNode` a freshly-constructed, correctly-narrowed `StorageScope`
  (via the normal join-host path, since a new tablet id is unseen by the
  per-node `minted` claim set) — but the **source** tablet's `RaftKvNode`
  predates the split, and nothing ever called `RaftKvNode::narrow_scope` on it
  afterward, even though the primitive existed and was unit-tested in
  isolation (`animus-cp-data/tests/narrow_scope.rs`) — its only call site in
  the whole workspace. `animus-cp-data/CLAUDE.md`'s own doc for the redesign
  even asserted `MetaCommand::SplitTablet` "narrows the source tablet's
  `StorageScope` range," describing the *intent* as if the wiring existed,
  when only the *replicated metadata* range narrowed — the per-node scope
  object was silently stale forever. Symptom: `/admin/raftkv`'s `key_count`
  for the parent tablet after a split kept reporting its pre-split (larger)
  count — caught by a regression test asserting the parent + child counts sum
  to the total instead of double-counting. Normal client reads never observed
  it (routing resolves the target tablet from the narrowed *metadata* range
  before ever reaching the stale scope), but any debug/admin surface that
  reads a tablet's `StorageScope` directly by tablet id would. Fixed by having
  the per-node `cp_join_host_loop` (`animusd`) — the loop that already polls
  `Metadata.tablets` every tick — call `narrow_scope` unconditionally on an
  **already-hosted** tablet too (previously it only acted on a tablet new to
  its `minted` set), not just on first hosting; `narrow_scope` is a cheap,
  idempotent mutex set, so doing it every tick regardless of whether the range
  actually changed is safe. **When a redesign introduces a new "narrow/update
  this cached thing" primitive, grep for its actual call sites, not just its
  unit test — a primitive that exists and is tested in isolation but is never
  wired into the production reaction loop is functionally dead code with a
  green test suite.** (`animusd` `cp_join_host_loop`, `CpGroup::narrow_scope`,
  `ClusterEdgeState::local_cp_member`;
  `tests/admin_endpoint.rs::admin_raftkv_key_count_is_scoped_per_tablet_after_split`.)

- **Mechanism superseded by ADR 0031 PR4**: `cp_gc_tablet`'s `current_range`
  parameter (the by-convention fix this entry describes) is deleted — the
  planner's `HostAction::Release` now *carries* the erase bound
  (`erase_bound`, always the tablet's current replicated range, never a
  scope fact), and the executor narrows to it immediately before erasing, so
  the narrow-before-erase ordering is a structural property of one function's
  output rather than a convention between two unrelated loops. Lesson
  retained for the record. **A teardown that erases "my own scope" must re-derive the scope from
  replicated state at the point of irreversible action — not trust an
  in-memory cache that a *different* code path is responsible for keeping
  current.** The fix directly above (`cp_join_host_loop` re-narrowing an
  already-hosted tablet's `StorageScope` every tick) has a gap the very next
  feature exposed: that re-narrow is **permanently skipped** the instant this
  node leaves the tablet's replica set (`plan_join_host` returns `None` →
  the loop's `continue` — the pure join-host decision, correctly, never touches
  a tablet this node no longer replicates). So a node dropped from a
  just-split tablet's replica set *within one join-host tick* (~250ms) of the
  split — exactly the shape ADR 0029's rebalancer produces, since a split
  raises the hosting nodes' replica counts, which is what makes the
  rebalancer target that tablet next — is left with a **permanently
  stale-wide** `StorageScope` for the tablet it's being removed from. The
  removed-replica release GC (`cp_gc_tablet`) used to call `erase_scope()`
  straight off that group's own (possibly stale-wide) scope; since ADR
  0026/0028 put every tablet a node hosts on **one shared engine**, an
  unbounded erase there doesn't just fail to reclaim less than it should — it
  actively **tombstones a co-hosted sibling's live keys** (the split's new
  child, which the departing node is typically still a replica of, since the
  replica-set change only touched the *parent*), at a version high enough to
  beat the child's own fresh writes under per-key LWW: silent, permanent
  corruption of a tablet this node was never even asked to release. The fix:
  the release phase now passes the tablet's **current** `Metadata`-replicated
  range into `cp_gc_tablet`, which calls `narrow_scope` on it immediately
  before `erase_scope` — bounding the erase to what the tablet's replicated
  state says right now, not to whatever the group's own cache happened to
  freeze at. **General check when a fix makes some cached/derived value
  "usually current": ask what happens at the one moment that value is about
  to be used for something irreversible (here, an engine-wide tombstone
  erase) if the normal refresh path was never given a chance to run — refresh
  it again, from the authoritative source, right at that point, rather than
  trusting the ambient cache is fresh enough.** Same family as the
  `RaftKvNode` read-barrier `all_nodes`-vs-`config()` entry below, and as the
  "prefer a live read of the durable layer over observation-built in-memory
  state" entry. Caught by design review before it ever shipped, not by a live
  incident. (`animusd` `cp_gc_tablet`'s `current_range` parameter,
  `cp_gc_release_phase`; `animus-cp-data/tests/
  narrow_scope.rs::narrow_then_erase_scope_spares_a_co_hosted_siblings_data`
  deterministically proves the mechanism at the primitive level;
  `animusd/tests/cp_rebalance_gc.rs::
  split_then_immediate_release_spares_the_new_siblings_data` is the end-to-end
  regression — worth noting **how** it forces the race: proposing the split
  and the follow-up replica-set CAS back-to-back on the control leader's own
  Raft log, rather than round-tripping the split through the client wire
  protocol first, is what makes the race reproducible at all — the
  wire-protocol version gave the ~250ms join-host tick enough real time to
  win and self-heal the scope before the drop landed, catching the pre-fix
  bug 0 times in 5 runs; the back-to-back-propose version caught it in ~3 of
  5. Even so this remains a genuine timing race, not a deterministic repro —
  the primitive-level test is what actually proves the fix, the E2E test is
  corroborating evidence.)

## Superseded by the "health ≈ is the data at risk" dashboard ladder (PR feat/console-health-data-risk)

The dashboard's `computeHealth()` originally treated any tablet without an
elected leader (`leaderlessCount`) or with fewer hosting groups than
configured (`underReplicatedCount`) as "degraded" — collapsing every kind of
"not fully converged" tablet (including a split-child mid-formation, whose
data was never at risk per ADR 0028) into the same red status as a genuine
node-failure-driven redundancy loss. Replaced by a four-rung ladder
(`quorum-lost`/`under-replicated`/`healthy`/`forming`) keyed on whether each
assigned replica's *node* is actually live, so routine transitions render
neutral and only genuine data-risk states degrade health (ADR 0021 §7).

- **A health/status rollup that gates on a *proxy* signal (a member's `Down`
  status) rather than the actual risk that signal stands in for (a tablet
  under-replicated/leaderless) can diverge from reality forever, because the
  two clear on different triggers.** The dashboard's `computeHealth()`
  (ADR 0021) treated any `Down` member as itself "degraded" — but a `Down`
  member only clears on manual decommission (ADR 0032 PR3) or the node
  rejoining, while the actual data-loss risk it represents is cleared much
  sooner, automatically, once the placement reconciler repairs every tablet
  the dead node used to replicate onto a spare (`failure_auto_replaces_
  replica_onto_spare`). So a cluster whose data was fully re-replicated
  within seconds could show "Degraded" indefinitely, until someone
  remembered to decommission the long-dead node. Fixed by keying "degraded"
  on the tablets' own derived status (`leaderlessCount`/`underReplicatedCount`,
  already computed per-tablet for the "Under-replicated" stat tile) instead
  of the member roster; `downCount` is kept as informational context in the
  banner/tiles, not a health-gating input. **General check for any rollup
  built from "X is down/unhealthy ⇒ overall is unhealthy": does the thing
  being protected (data replication, request-serving capacity) actually
  recover on a faster/different path than the raw signal does — and if so,
  gate on the protected property, not the signal.**

## Superseded by ADR 0044 (split-only tablets: tablet merge removed)

ADR 0044 (2026-08-14) removed tablet merge entirely — `MetaCommand::
MergeTablets`, `Metadata::merged_tablets`/`absorbed_by`, and
`animus-cp-data`'s `HostAction::WidenScope`/`Absorb` reconciler reaction no
longer exist anywhere in this repository. Every entry below was written
against that now-deleted mechanism. Two of the three lessons drawn from them
still generalize beyond merge specifically — a pointer back to each remains
in `engineering-lessons.md` at the point each entry used to live.

- **When two different root causes produce the identical observable absence,
  don't try to reconstruct which one happened from the remaining state —
  record an explicit signal at the moment the distinction is still known.**
  Wiring tablet merge (ADR 0033, the data-plane dual of ADR 0028's split), a
  per-node reconciler observing "a tablet I used to host vanished from the
  replicated tablet map" must react completely differently depending on
  *why*: merged into a sibling (tear the group down, but the data is still
  live — a survivor now serves it on the same shared engine, so **never
  erase**) vs. the whole table dropped (tear down **and erase** — nothing is
  left to serve that range). Both produce the exact same absence from
  `Metadata.tablets`, and the tempting inference — "does some other tablet's
  range now cover mine, so it must be a merge survivor" — is unsound: two
  different tables' still-unsplit tablets can have byte-identical default
  ranges (`KeyRange::whole()`), and by the time the reconciler is deciding
  what to do, the vanished tablet's own table identity is gone from view too
  (it's not in the map anymore), so there's no way to disambiguate a
  same-table survivor from an unrelated table's coincidentally-matching
  tablet. The fix was a tiny, explicit, **permanently-retained** replicated
  marker (`Metadata::merged_tablets: BTreeSet<TabletId>`, ADR 0033) set at
  the one moment the distinction is unambiguous (the `MergeTablets` apply
  itself, which knows exactly which tablet it just absorbed) — cheap because
  tablet ids are never reused (so the marker never needs pruning and can
  never resurrect a wrong decision for a later id), and correct by
  construction instead of by inference. **General check when a planner reacts
  to "X disappeared" from a coarser view: are there multiple legitimate
  reasons X can disappear that demand different actions, and if so, is there
  actually enough information left in the coarser view at decision time to
  tell them apart — or does the distinguishing fact need to be captured
  explicitly, closer to where it was still known, even at the cost of a
  small permanent marker?** (`animus-control::Metadata::merged_tablets`;
  `animus-cp-data::host::{HostAction::Absorb, MetadataView::merged}`.)
- **Tearing down a Raft group whose data will keep being SERVED (not erased)
  must drain the group's committed log into the engine first — `shutdown()`
  halts the async apply task at its next loop-top check WITHOUT draining, and
  deleting the group's WAL then destroys the only local copy of the
  committed-but-unapplied tail.** Found via ADR 0033's own 3-node merge
  integration test flaking ~1-in-5 *in isolation* (per the standing rule, a
  flaky `ProdEnv` test is a real bug): a write acked by the absorbed group's
  leader right before the merge was applied to *that leader's* engine (ack
  requires leader-local apply) but not yet to a follower's — commit-index
  propagation runs up to one heartbeat behind, while the reconciler's
  event-driven `metadata_watch` fires the `Absorb` teardown on the very
  commit that made the merge visible, i.e. *designed* to race that window.
  The follower's engine then permanently lacked the acked key, and if that
  node hosted the merge survivor's leader, linearizable reads answered a
  definitive "key absent" forever — indistinguishable from data loss. The
  same non-draining shutdown is **harmless for `Release`/`Reclaim`** (their
  teardowns erase the data anyway; other replicas serve) — which is exactly
  why it was never noticed: the invariant "a torn-down group's unapplied
  tail doesn't matter" was true for every teardown that existed before merge
  added one whose data lives on. Three-part fix, each load-bearing: the
  `Absorb` teardown drains (commit covers the local log, engine-applied
  covers commit) while the driver is still live; `plan` defers the
  survivor's `WidenScope` until the absorb confirms (drain-before-widen —
  the planner's fixed emission order alone would have widened *first*); and
  the read path stopped conflating two "None"s — a ReadIndex barrier
  failure and a genuinely-served absent — plus gained the read-side dual of
  ADR 0028's pre-propose range check (a get/scan whose group's live
  `scope_range()` doesn't contain the request errors retryably; for scans
  the un-widened scope was otherwise a *silent truncation*, since
  `linearizable_scan` filters rows through the live scope). **Two general
  checks: (1) when a new feature makes a previously-universal teardown
  invariant ("this group's data dies with it") false for one new path, audit
  the teardown's every step against the new path — the WAL delete that was
  cleanup before is data loss now; (2) grep read paths for `Option`-collapse
  points where "couldn't serve" and "served: absent" merge into one value —
  the Get/Scan arm asymmetry (Get mapped `None` to absent, Scan mapped it to
  an error) was the tell.** The deterministic regression drives the write →
  merge-view tick with zero intervening sim time, so the apply task provably
  hasn't run — no wall-clock race needed.
  (`animus-cp-data::host::Reconciler::teardown`'s Absorb drain + `plan`'s
  `absorbing` gate; `RaftKvNode::linearizable_get_served`; `animusd`
  `cp_get_local`/`cp_scan_local`; regressions:
  `reconciler_corpus.rs::scenario_merge_widens_and_absorbs`,
  `host::tests::widen_is_deferred_while_the_absorbed_sibling_is_still_hosted`,
  `animusd` `split_fence_tests`' read/scan duals.)
- **CONFIRMED and fixed: a suspected latent cross-group LWW version hazard on
  split/merge (flagged in a PR #90 review comment) was real** — every tablet
  a node hosts shares one physical `StorageEngine` (ADR 0026/0028), and
  `animus-cp-data` stamps each write's MVCC version as its **own** group's
  local Raft log index, which restarts low/independent for a fresh group. A
  split's new sibling could carry a version no higher than what the *source*
  group already stamped for a key now in the sibling's range; a merge
  survivor's group keeps running but starts serving keys the absorbed
  sibling's group versioned under a different, unrelated sequence. Either
  way `StorageEngine::merge`'s per-key LWW silently no-ops the write (loud,
  not silent corruption — the confirm loop's poll-for-exact-value-equality
  times out — but the write never lands). Reproduced directly at the
  `RaftKvNode` level with no control-plane machinery needed: write a key
  through a whole-keyspace group at a high index, narrow it away, start a
  **fresh** sibling group over the *same* shared engine scoped to that key's
  range, write the key again — silently dropped
  (`animus-cp-data/tests/cross_group_lww.rs`).
  **Design space explored, and why the obvious-looking alternatives don't
  work**: (1) seeding a fresh/widened group's floor from a **live,
  per-replica** read (`storage.latest_version()`, or "whichever
  `next_tablet_id` counter value happens to be current when this replica's
  own tick fires") looks tempting since it needs no schema change, but two
  *different replicas of the same group* can observe different values at
  slightly different real-world moments — and since the group's `RaftCore`
  log-index numbering (Host) or an already-running group's live floor
  (merge's widen) must be **byte-identical across every replica** applying
  the same command, a per-replica-timing-dependent floor either breaks Raft
  log-matching outright (Host: divergent `snapshot_index` bases before any
  election) or makes two replicas stamp *different* versions for the
  identical committed write (merge: a bare local read has no cross-replica
  agreement at all). (2) Using the **tablet's own id** as the floor works
  cleanly for split (a fresh sibling's id is always allocated *after*, hence
  numerically greater than, the source's) but not for merge in general: `left`
  and `right` are chosen by **key-range adjacency**, not id order — a tablet
  re-split from the *middle* of an existing chain mints a new id that can be
  *numerically larger* than an unrelated tablet further right in key-range
  order, so a later merge of that pair can have `right.id > left.id`, and
  "bump past `right`'s id" would then either be a no-op or, worse, could
  someday design itself into `left` permanently unable to out-version
  `right`'s history. **The fix that actually holds**: a `version_floor: u64`
  field on `animus_tablet::Tablet` itself (shared by both planes' `Tablet`
  type, so no projection duplication needed) — `0` by default (byte-identical
  to today, `#[serde(default)]` for back-compat), bumped **once, by the
  control plane's own deterministic `apply`** at exactly the two moments a
  cross-group version collision can occur: `SplitTablet` sets the new
  sibling's floor to `source.version_floor + 1` (always exceeds anything the
  source could have stamped, since a group's own local index realistically
  never approaches the scale factor between rescopes — auto-split already
  caps a tablet's key/byte count long before that); `MergeTablets` bumps the
  surviving `left`'s floor to `max(left, right) + 1` (exceeds *both* sides,
  closing the "which id is bigger" trap the id-based scheme fell into). Every
  data replica reads this **already-agreed, replicated** value from
  `Metadata`/`MetadataView` at `Host`/`WidenScope` time — never computes it
  locally — so it is identical across replicas by construction, the same
  discipline as every other epoch-CAS'd placement fact in this codebase.
  `RaftKvNode`'s actual stamped version is `floor * SCALE + local_index`
  (`effective_version`, `SCALE = 2^40`) — a group's own log index is
  completely untouched (no Raft log-matching risk at all; `engine_applied`
  still tracks the raw index), only the *storage-layer version number it
  stamps* changes, and only for a tablet that has actually been through a
  split/merge. **General lesson: when a per-group monotonic counter (a Raft
  log index, a local sequence number) is reused as a version/ordering token
  that must compare correctly *across* groups whose identities can change
  over time (a split/merge/rebalance lineage), the floor that keeps groups
  from colliding must be a value every replica reads identically from
  already-replicated state — never derived from a live per-replica read (even
  a "conservative always-safe upper bound" one), and the exact arithmetic
  direction (which side's id/floor can legitimately end up numerically larger)
  needs checking against the *actual* pairing rule (adjacency, not allocation
  order) before trusting an id-based shortcut.** (`animus_tablet::Tablet::
  version_floor`; `animus-control::meta.rs`'s `SplitTablet`/`MergeTablets`
  apply; `animus-cp-data::RaftKvNode::start_hosted_with_floor`/
  `bump_version_floor`/`effective_version`; regressions in both crates —
  `animus-cp-data/tests/cross_group_lww.rs`,
  `animus-control::meta::tests::{split_tablet_seeds_the_new_siblings_version_
  floor_past_the_sources, merge_tablets_bumps_the_survivors_version_floor_
  past_both_sides}`.) **Superseded twice over: `version_floor` itself was
  retired by ADR 0018 PR2's range-seal design** (an ordering-based fence
  replaced the version-space separation this entry's whole fix relied on),
  **independently of this stack's merge removal** — this entry was already
  describing dead code before ADR 0044 shipped.
- **`animusd/tests/split_cluster.rs`'s two failures (`split_and_merge_over_a_
  split_deployment`, `decommission_racing_a_tablet_split_converges_with_no_
  data_loss`) were both 100%-deterministic in a genuine multi-process
  control-only + data-only deployment, tracing to one design gap in the ADR
  0018 §2 amendment's range-seal handoff (`animus-cp-data/src/host.rs`).**
  The seal proposal (`propose_seal`) used to be a **one-shot side effect
  bundled into the same tick as the local, irreversible action it was
  supposed to precede** — `NarrowScope`'s local scope mutation (leader-gated
  propose inline) or `Absorb`'s teardown (leader-gated propose, then a
  drain-wait gated only on "nothing pending locally"). Two related bugs
  followed from that one shape:
  1. **A one-shot side effect hung off a self-erasing trigger never
     retries — make the trigger a persistent, re-derived condition
     instead.** `NarrowScope`'s local `narrow_scope()` call ran
     unconditionally, regardless of leadership; the paired seal-propose call
     only fired if this same replica *also* happened to be leader at that
     exact tick. Since narrowing immediately makes the triggering mismatch
     (`t.range != current`) vanish on that replica, a replica that narrowed
     while a *follower* and was only *later* promoted to leader had no
     second chance — the condition that would have re-triggered the attempt
     was already gone, permanently, even though the actual precondition
     ("does a covering seal exist yet") hadn't changed at all. Fixed by
     computing the seal-pending condition fresh every tick
     (`TabletFacts::pending_seals`, an async engine scan independent of
     local scope/teardown state) and turning it into its own action
     (`HostAction::ProposeSeal`), so whichever replica eventually holds
     leadership gets its chance regardless of when leadership shuffles
     relative to the local mutation.
  2. **An irreversible local action ordered after a distributed action must
     gate on committed evidence of that action, not on local progress.**
     `Absorb`'s teardown considered itself free to tear down (delete the
     only local copy of the group's Raft WAL) once "nothing pending
     locally" — a check a quiescent follower satisfies trivially *before
     the leader has even proposed the seal*. A fast follower could
     therefore destroy its own voter before the seal ever committed,
     dropping the group below quorum and permanently stranding the
     leader's own, now-orphaned proposal (accepted locally, never able to
     commit again). Fixed by additionally requiring a **locally-observed
     committed** seal (the same `seal_covers` engine scan) before a replica
     may tear down — not "distributed progress inferred from local state,"
     the actual distributed fact itself. This gate is self-supporting, not
     a deadlock: requiring every absorbed replica to stay up until it
     observes the seal is exactly what keeps the quorum needed to commit
     that seal alive in the first place; a genuinely quorum-dead group (an
     unrelated double failure) correctly stalls loudly instead of tearing
     down early — the same correctness-over-liveness call this system makes
     everywhere else a durability/visibility gate is at stake.
  **Diagnostic method**: reproduced deterministically (3/3 identical
  failures, not contention-gated) in isolation first, then added temporary
  `eprintln!`s directly at the two decision points (`Reconciler::teardown`'s
  Absorb `fully_drained` check and propose-seal call; `gather_facts`'s
  `parent_seal_observed` computation and `NarrowScope`'s propose-seal call)
  and re-ran each failing test once — the trace immediately showed a
  follower reaching "fully drained" with `commit == log_end` (nothing to
  wait for) *before* any "leader proposing seal" line ever printed for test
  1, and zero "leader proposing narrow-seal" lines across the entire ~150-
  tick run for test 2 — the same per-call-site `eprintln!` idiom the prior
  HLC witnessing-chain bug's entry above used, applied to a completely
  different subsystem. Confirmed the fix by reverting just the source file
  (keeping the new tests) and re-running: both new `reconciler_corpus.rs`
  scenarios fail against the unfixed code, pass against the fixed code.
  Regression: `animusd/tests/split_cluster.rs`'s original pair (the
  real-world acceptance) plus two new deterministic `SimEnv` scenarios in
  `animus-cp-data/tests/reconciler_corpus.rs` that force the exact
  interleaving by hand (`absorb_follower_waits_for_committed_seal_before_
  tearing_down`, `narrow_seal_survives_a_late_promotion_after_narrowing_
  as_a_follower`) — see `animus-cp-data/CLAUDE.md`'s range-seal and
  Absorb-drain invariant entries, and ADR 0018's PR2 amendment corrective
  note #2, for the full mechanism.

## Zero-copy split defense stack (mechanisms deleted by ADR 0050 Train B rung 7)

Moved verbatim from `docs/engineering-lessons.md` when the copy-based split
pivot deleted the machinery these entries defended (frozen basis, declared-
range fences, the seal range-CAS). The generalized forms keep pointers in
the live log.

- **A value a child inherits from a parent that keeps mutating must be
  frozen at the inheritance event, never derived live from the parent's
  current state.** `Metadata::effective_stream_shard_watermark`/
  `stream_shard_parent_id` (ADR 0042 §8/ADR 0043 §A4/§A6) used to walk
  `split_parents` to the parent tablet's *current* seal chain on every
  call — correct only so long as the parent never sealed again before the
  child did. The moment it did, the parent's later (necessarily higher)
  end-HLC retroactively became the child's own effective watermark too,
  making a pre-split backlog the child had physically inherited in place
  (ADR 0043 §A4's shared-storage split design) look already-sealed before
  the child ever sealed it itself — a silent, permanent loss, invisible
  unless the child happened to seal first (the race that let this ship
  undetected through round 3). The fix (PR1) captures the parent's stream
  state **once**, at the instant `MetaCommand::SplitTablet` applies, into
  a frozen `Metadata::stream_split_basis` entry — a single-hop lookup
  thereafter, not a live walk. **Corollary: a test comment that
  acknowledges a derivation's time-dependency (e.g. "this assertion must
  run before the parent seals again, since X is derived live") is a
  signal to fix the derivation, not to order the test around it** — the
  pre-fix `stream_lineage_corpus.rs::scenario_split_mid_stream` had
  exactly such a comment, naming its own ordering constraint, for months
  before this bug was found and its literal inverse
  (`split_then_parent_seals_first`) written as the regression.
- **A corpus convention that always narrows a split's scope synchronously
  with (or before) the `SplitTablet` apply can only ever test the FIXED
  ordering — it structurally cannot express the real race where the local,
  un-replicated scope-narrow lags the control-plane commit.** Found
  2026-08-15 investigating the D8 duplication flake above (the mirror-image
  DUPLICATION direction of #216's own loss bug — same watermark machinery,
  opposite symptom): `stream_lineage_corpus.rs`'s `scenario_split_mid_
  stream`/`scenario_split_then_parent_seals_first` both call
  `n.narrow_scope(..)` on the parent's nodes in the test script itself,
  strictly before (or as part of the same step as) applying
  `MetaCommand::SplitTablet` — modelling `animus_cp_data::host::HostAction::
  NarrowScope` as if it always lands atomically with the control commit
  that triggers it. In production it does not: `SplitTablet` commits only
  to the control Raft; `RaftKvNode::narrow_scope` is a separate, local,
  per-node action the tablet-host reconciler applies only once it next
  notices the metadata change (event-driven watch or a 500ms fallback) —
  and nothing synchronizes that against a *different* background loop
  (`animusd::index_drain::change_consumer_loop`'s 200ms seal tick) reading
  the same tablet's still-wide `pending_changes()` in the meantime. The
  parent's seal in that window physically captures records that, per the
  metadata just committed, already belong to the split-off child; the
  child's own first seal — whose watermark is `Metadata::stream_split_
  basis`, deliberately frozen *before* that racing parent seal (the exact
  mechanism #216 added to fix the loss direction) — has no way to learn the
  parent already covered them, and re-seals the same physical records
  (never deleted by a seal, only by a later trim) into its own epoch 0:
  the same packed HLC delivered twice, caught by `verify_lineage`'s
  `seen_hlcs` set once a scenario actually drives the two seals in this
  order (`scenario_split_then_parent_reseals_before_scope_narrows`, a new
  cell proving the mechanism). General form: when a corpus's own helper
  always performs two steps of a protocol in the same call/in a fixed
  order because "that's how the test drives it," check whether production
  ever lets them land in the *other* order or with a delay between them —
  a convention baked into every existing scenario can hide an entire bug
  class from hundreds of seeds, exactly as it did here (and as the sibling
  `dueling_seals_orphan_hot_range` cell's own two-snapshot scripting had to
  be added by hand for the *other* seal-store race the ordinary corpus
  couldn't reach either). **Fixed 2026-08-15**: `seal_now`, the hot-trim
  arm, and the open-shard hot-read path (`animusd::index_drain`) now fence
  their `pending_changes()` candidate set to the tablet's current
  metadata-declared range (`in_declared_range`, ADR 0028's write-fence
  idiom applied to the seal arm's read side) — see ADR 0043 §A4's own
  "split-seal range-fence amendment" for the full as-built writeup.
- **A cache-fed fence cannot protect against the cache itself being
  stale — only the state machine's own `apply` sees the true, current
  state, and only it can arbitrate.** The direct follow-up to the lesson
  above, found while empirically verifying that fix in production: the
  range fence just described reads `ctx.effective_metadata()`, which for
  every real deployment shape resolves to `RaftNode::metadata()` — a clone
  of a cache an *async apply task* (ADR 0038) populates, decoupled from the
  Raft log's own commit index. That cache can lag the true, already-
  committed `SplitTablet` on the SAME node running the seal arm,
  independent of whether the physical scope has narrowed — and since the
  tablet-host reconciler that drives `narrow_scope` reads the identical
  cache to decide when to act, the fence and the physical scope it exists
  to backstop can BOTH be consulting the same stale snapshot at once. In
  that specific sub-window the fence provides zero protection, because
  it's checking against the exact source that let the physical scope stay
  wide in the first place — confirmed empirically, not just by reasoning:
  the real D8 e2e test still showed the duplication after the fence alone
  shipped, at a rate not clearly distinguishable from noise across two
  matched before/after samples (do the comparison, don't eyeball "seems
  better"). **Fixed 2026-08-15**: `MetaCommand::SealStreamShard` gained an
  `expected_range` stamp, checked by `Metadata::apply` itself (mirroring
  the existing epoch-CAS idiom on `SplitTablet`/`CasTabletReplicas`) —
  since `apply` runs strictly sequentially in Raft commit order, a
  proposal fenced against a range a racing, earlier-committed `SplitTablet`
  has already superseded is rejected regardless of any node's own cache
  freshness; no read, however fresh, can substitute for checking against
  the state actually being mutated. General form: **when a proposal-side
  check reads any replicated/cached state to decide "is this still
  valid," ask whether that same state can lag the fact the check exists to
  catch — if so, the check is a useful first-line filter (cheaper, catches
  the common case, avoids wasted round trips) but never the authoritative
  backstop; that has to live at the point of actual, sequential commit
  (a state-machine `apply` arm, a CAS), not a read anywhere upstream of
  it.** A residual can remain even after this: `hot_read`'s own open-tail
  serve has no apply-time backstop available at all (reads don't go
  through `apply`), so it stays a first-line-only, permanently incomplete
  fence — accepted, deferred to the ADR 0044 quiescence work, rather than
  chased with more read-side cleverness that would just move the same
  staleness window somewhere else.

## The copy-based split-build driver (deleted by the copy-split-deletion stack, 2026-09-01)

Moved verbatim from `docs/engineering-lessons.md` when the copy-based
split's own background driver (ADR 0050 Train B — `SplitBuild`/
`split_driver_tick`/`ship`/`ship_all`/`tail_pass`/`SeedRows`/
`MetaCommand::BeginSplit` itself, and the `--split-mode {copy,inplace}`
selector that used to choose it) was deleted whole (Layers A/B1/B2 of that
stack; see `docs/adr/0058-*.md`'s 2026-09-01 as-built note and
`docs/adr/0050-*.md`'s matching amendment). The in-place split (ADR 0058
Train 2, directed by ADR 0062) is the sole surviving split mechanism. The
generalized forms keep pointers in the live log.

- **A change-log consumer's resume cursor must be a commit-order (HLC)
  watermark, never a key-position cursor** (ADR 0050 rung 4, the
  split-build tail). `pending_changes`' key order is prefix-then-HLC, NOT
  commit order — a later write to a *lower* prefix inserts *below* any
  key-position cursor and is skipped forever. The sealer learned this once
  (its load-bearing re-sort in `seal_now`, recorded only as a code
  comment); the split-build driver re-made the identical mistake with a
  "resume after the last key I saw" cursor, caught red by its own e2e
  (`split_build.rs`, 4 of 16 racing writes silently missing while the
  build reported converged). Within one tablet, HLC order IS commit order
  (`assert_ts_monotonic`), so filtering the scan by a packed-HLC watermark
  (the key's own trailing 8 bytes) is complete where any key cursor is
  not. Advance the watermark only after the tick's work fully succeeds, or
  a failed ship loses its dirty set. General form: before giving any
  key-ordered scan a positional resume cursor, ask what order NEW entries
  arrive in — if insertion order ≠ scan order, a positional cursor is a
  silent-loss bug.
- **A batched fast path plus an unbatched incremental path over the same
  data is a performance bug waiting for its first big input — the
  incremental one silently costs one round trip per ITEM where its sibling
  costs one per MEGABYTE** (ADR 0050's split build, 2026-08-19). The bulk
  copy pass batched rows into 256 KB `SeedBatch` chunks; the tail pass that
  chases writes arriving during the copy called the very same `ship()`
  helper *inside its per-dirty-unit loop*, so every partition key bought a
  full consensus round + apply-confirm (plus a forwarded hop for an
  off-node child). Both paths looked correct and shared the same primitive
  — the batching lived in the caller, and only one caller did it. Made
  vastly worse by a second, independent conservatism: the tail's watermark
  started at 0, so its FIRST pass classified every change record in the log
  as dirty and re-shipped the whole table one key at a time, every merge an
  idempotent no-op. Together: on a 20,000-row split, ~6,000 no-op Raft
  entries per child and ~85% of the build's wall clock spent re-copying
  data it already had — while the children's key counts sat visibly flat.
  **The generalizable rules.** (1) When one loop batches and a sibling loop
  over the same rows doesn't, that asymmetry is the bug — an accumulate-
  and-flush-on-budget shape is usually a few lines and needs no semantic
  argument, because an idempotent, versioned batch doesn't care where the
  chunk boundaries fall. (2) A "safe" zero/empty starting watermark is not
  free when a cheap, *sound* starting value is available from a pass the
  code already makes: this one was recoverable from the same pre-bulk
  read that already computed the version floor, under the identical
  monotonicity argument. Ask what the conservative default actually costs
  on the first large input, not whether it's correct. **The diagnostic
  that made it obvious in minutes**: one consensus entry == one Raft log
  index, so a receiver's own `commit_index` growth divided by rows
  received IS the effective batch size — visible from `/admin/raftkv`
  with no instrumentation, and it turned "the split feels slow" into
  "6,000 entries moved 0 rows." That ratio is now the regression's
  assertion, too: an entry-count budget catches a re-introduced per-row
  ship where a wall-clock assertion would just go flaky.
  (`crates/animusd/src/index_drain.rs::tail_pass`, `split_driver_tick`.)
- **A crate's own gotcha bullet can itself go stale — verify a "the log
  index is the version" premise against the primary source before trusting
  it enough to build on (2026-08-19).** Tasked with dropping the split
  driver's version-floor pre-pass scan (`index_drain.rs`) in favor of an
  O(1) `group.engine_applied_index()` read, on the stated premise "CP
  writes need no client-assigned version — the Raft log index *is* the
  MVCC version" (verbatim, then-current text of this crate's own
  `CLAUDE.md`). That premise was true once but had been superseded over a
  year earlier: ADR 0018 §2/PR2 (2026-08-11) retired the Raft-index MVCC
  encoding and replaced it with a packed HLC commit timestamp
  (`hlc::pack(ts) = wall_ms << 20 | logical`) — a completely different
  value space from a Raft log index (wall-clock milliseconds vs. an entry
  count), so the proposed substitution was unsound in both directions:
  under real workloads it would under-filter back to the exact unfiltered-
  final-image regression a prior rung of the same ADR had fixed, buying
  nothing for a known cost. (A first draft of this entry also claimed it
  could *over-filter* under `SimEnv`; that was withdrawn on review —
  `animusd` has no `animus-sim` dependency, so this driver never runs
  under a simulated clock. Reviewing your own supporting arguments as
  hard as the conclusion is part of the lesson: the conclusion held, one
  of its three legs did not.) The tell was
  in the *type* the target field held (`ts: HlcTimestamp` on every
  `KvCommand` variant, `KvCommand`'s own doc comment naming `hlc::pack` as
  "the engine's MVCC version at apply") — one grep away from the code the
  premise was supposedly about, and a mismatch the crate's own summary
  bullet had quietly drifted away from. **Rule:** before implementing an
  optimization whose soundness rests on an invariant stated only in a
  summary doc (a `CLAUDE.md` gotcha bullet, a one-line ADR recap), grep the
  actual type/field the invariant is about and read its own doc comment —
  the summary is a pointer, not the source, and it can lag the code by
  exactly as long as nobody happened to need that bullet to be right. This
  generalizes the existing "before implementing a 'close this documented
  gap' task, grep the code" rule (root `CLAUDE.md`) to invariants, not just
  missing-feature claims. Found and corrected in the same change that
  fixed the stale bullet (`crates/animusd/CLAUDE.md`'s "CP writes need no
  client-assigned version" entry) and recorded the rejected optimization
  (ADR 0050's 2026-08-19 "investigated and rejected" amendment) so it
  isn't re-attempted on the same false premise.
- **A convergence veto that guards a correctness property must be
  accelerated, never bounded or bypassed — the bound belongs on the *load*
  that's slow to drain, not on the gate itself (issue #288).** The
  split-cutover GSI-drain veto (`index_drain.rs::split_driver_tick`
  stage 3a) blocks `CutoverSplit` until the parent's `"gsi"` cursor reaches
  the highest pending change record — because cutover retires the parent and
  the reconciler reclaims its engine outright (no drain-before-halt exists
  post-ADR-0044, see `animus-cp-data/CLAUDE.md`'s "Superseded by ADR 0044"
  entry), so firing
  cutover past an un-drained cursor would silently lose GSI updates forever
  (children are born with empty change logs by design). An unthrottled write
  flood racing the split made this veto converge too slowly (several
  10s-of-seconds retries under load, see the "unthrottled continuous write
  flood" entry above) — but the correct fix was never to loosen the veto
  (e.g. force cutover after N stalled ticks, mirroring `SPLIT_MAX_TAIL_
  PASSES`'s bounded chase for the *build* phase). `SPLIT_MAX_TAIL_PASSES`
  is safe to bound because its own correctness never depended on the lag
  being zero (the post-freeze final drain + final image still transfer
  everything regardless of the bound); the GSI-drain veto's correctness
  *is* exactly "the lag is zero" — there is no compensating post-cutover
  mechanism, so a bound here would be a straightforward data-loss bug, not
  a liveness relaxation. The sound fix exploits a fact the *build* phase
  doesn't have: once the parent is frozen (Freeze rejects every later user
  write), the backlog this veto watches is fixed, not growing — so driving
  the drain to exhaustion in a tight loop, right there in the frozen
  endgame, has zero fairness cost and only removes the artificial one-tick
  (`INDEX_DRAIN_INTERVAL`, 200ms) lag between "a drain pass makes progress"
  and "the veto notices," including surviving a transient propose failure
  under load without waiting a full extra tick to retry it. **General rule**:
  before touching a gate that's "too slow to satisfy," classify it — is the
  gate a correctness invariant (something bad happens if you proceed before
  it holds) or a liveness heuristic (nothing unsafe happens, it's just an
  imperfect proxy for "caught up")? Only the second kind may ever grow a
  bounded-chase escape hatch; the first kind's only legal fix is making the
  thing it's waiting on happen faster, exploiting whatever makes the wait
  bounded now (here: the parent going static at freeze) rather than relaxing
  what "caught up" means.
  (`crates/animusd/src/index_drain.rs::split_driver_tick`,
  `FROZEN_ENDGAME_GSI_DRAIN_MAX_PASSES`.)
- **A retry loop whose "retry" recomputes from unchanged inputs is a spin,
  not a retry — and only a cluster LARGER than the replication factor can
  prove a tablet-id-addressed forward** (ADR 0050 fork F5 fallout,
  2026-08-17). Every tablet-id-addressed internal RPC (`SeedRows`,
  `ForceSeal`, `TriggerAutoSplit`, `ClearBackfillCursor`, `StreamHotRead`)
  used a resolve → relay-once → on-"not the leader here"-refusal
  re-resolve-from-scratch loop, and one even documented that shape as
  "correct (converged-or-timeout)". It converges only when the calling
  node hosts a replica of the target tablet: the local replica's own
  leader hint is what changes between iterations. With **no** local
  replica, `resolve_cp_route`'s fallback deterministically returns the
  tablet's *first* metadata replica every time, that follower refuses
  with the real leader's address embedded in the refusal every time, and
  the loop threw that hint away every time — an infinite spin dressed as
  a retry. The split driver hit it the first time anyone ran a split on a
  cluster with more nodes than RF: fork F5 places children at fresh
  balance-chosen homes, so the parent's leader routinely hosts no replica
  of one child, and seeding that child spun forever — the parent parked
  `Splitting` holding every key with an empty `Building` child beside it,
  indefinitely (the "auto-split made 2 new tablets but never rebalanced
  the keys" field report). Every split e2e ran 3-node clusters at RF 3,
  where every node hosts every tablet and the no-local-replica branch is
  structurally unreachable. Two general forms: (1) for any retry loop,
  name the input that CHANGES on a failed attempt — if the answer is
  "none", it is a spin, and the fix is feeding the failure's own payload
  (here: the refusal's leader hint) back into the next attempt, done once
  at a shared choke point (`forward_to_tablet_leader`, now backing
  `cp_forward` and every tablet-addressed RPC alike); (2) the existing
  "test through a follower-connected node" rule is not enough for
  tablet-addressed forwards — the caller must host *no replica at all* of
  the target, which requires a cluster larger than RF
  (`split_build.rs::split_completes_when_a_child_lives_off_the_parent_leader_node`
  is the 5-node teeth).

## Superseded by ADR 0054 step 4b (a leader-side "seatbelt double-check" kept alongside an apply-evaluated write must predict the client's own decision, not replicate the byte-level mechanism it replaces)

Moved verbatim from `docs/engineering-lessons.md` when ADR 0054 step 4b
deleted the whole mechanism this entry describes (`predict_kind_eval_
decision`/`report_kind_eval_seatbelt_mismatch`, `Metric::
KindEvalSeatbeltMismatch`, and the `rmw285_confirm_gate` test hook it
names) — apply's own decision is the only one computed at all now, so
there is nothing left to compare it against. The generalized form (below)
keeps a pointer in the live log.

Cutting `kind_write_item_at_leader` over to `KvCommand::KindEval` (ADR 0054
step 3), the task brief for the kept seatbelt double-check assumed the
classic "two concurrent `ADD`s, one refused" scenario would be the thing the
mismatch metric catches. Tracing the actual code paths found this is not so:
the *old* seatbelt was a byte-level OCC check (`KindBatch.conditions =
vec![(base_key, raw_old)]`, comparing the leader's exact read bytes against
whatever is committed at apply) with no relationship to the client's own
`ConditionExpression` — a plain, unconditional `ADD` has no condition to
evaluate at all, so a leader-side prediction built from `condition.evaluate`
can *only* ever answer `Applied` for it, never `ConditionFailed`. Reproducing
the old byte-level seatbelt's staleness signal was not what was asked for,
and would have required inspecting engine state at apply time from the
leader side, which nothing exposes. The seatbelt double-check that actually
matters — and that the metric's own doc had to be worded around — is
narrower: it predicts what the SAME evaluation logic (`condition.evaluate` +
`apply_update`) would decide from the leader's own resolved read, and
compares that prediction against apply's confirmed decision. That only ever
disagrees for a **conditioned** write, and only because the value legitimately
changed between the leader's read and apply's own fresher one — which is
symmetric in principle (a leader's stale read can go stale in either
direction) but the task's intended semantics single out one direction as
"expected" (leader too pessimistic, apply succeeds) and the other as
"worth investigating" (leader too optimistic, apply rejects). Both directions
are mechanically ordinary races, not distinguishable by looking at a single
disagreement in isolation; the asymmetry is a policy call about which
direction, if it dominated the counter's rate over time, would suggest a
real evaluator divergence between the leader-side and apply-side code paths
(which *are* meant to agree) rather than ordinary timing — worth recording
in a comment precisely because nothing in the mechanism itself enforces it.

**Testing implication**: a regression that manufactures a specific
disagreement direction needs a *conditioned* write and a genuine timing race
between two writes to the same key, not just concurrent unconditional ones —
the existing `rmw285_confirm_gate` test hook (issue #285, already `#[cfg(test)]`
in `dynamo.rs`) that delays a write's own post-rmw_lock phase is exactly the
tool for this: arm it, let the gated write's leader-side read observe a
before-image its own `ConditionExpression` would reject, land a second,
ungated write in the gap that makes the condition become true, then let the
first write's entry apply against the now-favorable state. This is a cheap,
deterministic way to prove a "kept old evaluator vs. new evaluator" double
check both fires correctly and never fails the request — worth reusing for
any future ADR 0054-style migration that keeps a comparison-only legacy
evaluator alongside a cut-over one.

