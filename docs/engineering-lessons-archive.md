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
