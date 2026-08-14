# ADR 0033 — Tablet merge: an operator-driven, control-plane-only dual of split

- **Status:** Accepted — implemented in `animus-control`, `animus-cp-data`,
  `animusd`. Amends ADR 0029 (which flagged merge as "deferred/operator-driven"
  and left `MetaCommand::MergeTablets` unwired) and extends ADR 0031's
  per-node tablet-host reconciler with two new planned actions.
- **Date:** 2026-08-07

## Context

`MetaCommand::MergeTablets { left, right }` has existed in
`animus-control::meta` since before ADR 0028, unit-tested
(`animus-control/tests/tablet_split_merge.rs`), and does the metadata half of
a merge correctly: given two tablets with adjacent ranges and identical
replica sets, it widens `left` to cover both and removes `right`. But nothing
in the system ever *called* it outside a unit test — no admin endpoint, no
CLI command, and critically **no data-plane reaction**: nothing told a
hosting node's per-tablet Raft groups that a merge had happened, so `right`'s
group would keep running forever (a zombie, still voting, still occupying a
Raft WAL file) and `left`'s `StorageScope` would never widen to actually
serve the range `right` used to own. ADR 0029 named this directly: *"merge
is deferred/operator-driven."*

Since ADR 0028 (shared per-node storage, control-plane-only split), the
natural shape for merge is obvious: it is split's **dual**. Split narrows a
source tablet's range and mints a sibling covering the vacated upper range —
both already servable from the same node-shared `StorageEngine`, confined by
`StorageScope`, because no data ever moves. Merge should widen a surviving
tablet's range to reabsorb a sibling's, and tear the sibling's now-redundant
Raft group down — again with no data movement, because `MergeTablets`
already requires the two tablets to share an identical replica set (the same
physical nodes, hence the same shared engine on each).

### The one real design question: distinguishing "merged away" from "table dropped"

The straightforward per-node reaction to a merge — "the tablet I used to host
vanished from `Metadata.tablets`; tear its group down" — is **already** the
predicate `HostAction::Reclaim` uses (ADR 0024/0031's drop-table GC). But
`Reclaim`'s teardown *erases* the tablet's data (`RaftKvNode::erase_scope`),
because a table drop means every replica of every one of the table's tablets
is being torn down at once — there's nothing left to serve that range. A
merged-away tablet's data must **never** be erased: a sibling tablet (`left`)
is about to serve — or, by the time a node's reconciler observes it, already
serving — exactly that range on the very same physical engine. Erasing it
there would silently corrupt live, currently-being-served data.

The two cases are structurally indistinguishable from `Metadata.tablets`
alone: a hosted-but-now-absent tablet id looks identical whether its whole
table was just dropped or it was just merged into a neighbor. The tempting
shortcut — "check whether some other tablet's range now covers mine" — is
unsound: two different tables' still-unsplit tablets can have byte-identical
default ranges (`KeyRange::whole()`, i.e. `[∅, ∞)`), and by the time a
reconciler is deciding what to do about the vanished tablet, there is no
table identity left in `view.tablets` to disambiguate against (the entry is
gone). Getting this wrong either corrupts a merge survivor's data (erasing
when it should absorb) or permanently leaks a dropped table's storage
(absorbing — skipping the erase — when it should reclaim).

## Decision

We will:

1. **Harden `MetaCommand::MergeTablets` with the same epoch-CAS discipline
   `SplitTablet`/`CasTabletReplicas` already have**, applied to *both*
   tablets: `MergeTablets { left, expected_left_epoch, right,
   expected_right_epoch }`. Rejected if either tablet's epoch has moved since
   the caller read it (a concurrent rebalance/repair CAS, or another
   split/merge touching either side), instead of applying against state the
   proposer never actually observed. Also reject a merge across two
   different tables' tablets (a check the original implementation was
   missing) — the physical keys on each side live under a different table's
   `StorageScope` prefix, so merging them would silently conflate two
   unrelated tables' data.
2. **Add a tiny, permanent, replicated marker: `Metadata::merged_tablets:
   BTreeSet<TabletId>`.** `MergeTablets`'s apply inserts `right` here. This is
   the explicit signal a per-node reconciler needs to tell "vanished because
   merged" apart from "vanished because dropped" — deliberately **never
   pruned**, because tablet ids are never reused (the existing monotonic
   allocator invariant), so an entry can never resurrect a wrong decision for
   some later, unrelated tablet reusing the id. Its size is bounded by the
   total number of merges ever performed, which is itself bounded by the
   total number of splits ever performed (a tablet cannot be merged unless it
   was first split off from something) — a permanently-small footprint, the
   same shape as `Metadata::next_tablet_id`.
3. **Extend `animus_cp_data::host`'s pure planner (ADR 0031) with two new
   actions, the dual of the existing split-narrow/drop-reclaim pair:**
   - `HostAction::WidenScope { tablet, range }` — the dual of `NarrowScope`:
     when an already-hosted tablet's metadata range has *grown* (rather than
     shrunk), and the new range is a proper superset of the tablet's current
     live `StorageScope` range, widen to match. (A metadata range that is
     neither a subset nor a superset of the current live scope — which
     should never happen in practice — is a defensive no-op either way,
     never guessed.)
   - `HostAction::Absorb { tablet }` — planned instead of `Reclaim` when a
     hosted-but-now-absent tablet appears in `MetadataView::merged`. Its
     teardown (`Reconciler::teardown` with `TeardownKind::Absorb`) stops the
     group and deletes its own WAL file exactly like `Reclaim`, but skips
     *both* the `narrow_scope` call and the `erase_scope()` call — the
     tablet's physical keys are left exactly where they are, now served
     through the survivor's widened scope.

   Both slot into the planner's existing fixed emission order (`NarrowScope`/
   `WidenScope` → `Host` → `Reconfigure` → `Release`/`Reclaim`/`Absorb`), so
   "widen the scope before anything else notices the tablet" and "absorb
   without erasing" are structural properties of one planner's output order,
   not a convention two independently-timed loops would have to agree on.

   **Post-merge hardening (2026-08-07), found by the very `ProdEnv`
   integration test this ADR shipped with flaking ~1-in-5 in isolation (a
   flaky `ProdEnv` test is a real bug — root `CLAUDE.md`): a linearizable
   read through the merged survivor returned a definitive "key absent" for
   an acked pre-merge write.** Root cause: the `Absorb` teardown raced the
   absorbed group's **async apply task**. A write acked by the absorbed
   group's leader is applied to *that leader's* engine before the ack, but a
   follower's engine apply lags (commit-index propagation is up to one
   heartbeat behind, and the apply task is a separate task) — and the
   reconciler's event-driven watch fires the `Absorb` on the very commit
   that made the merge visible. `RaftKvNode::shutdown()` halts the apply
   task at its next loop-top check *without draining committed-but-unapplied
   entries*, and the teardown then deletes the group's Raft WAL — the only
   local copy — so the acked write silently never reached that node's
   engine, permanently. If that node hosts the survivor's leader, reads of
   the key answer "absent" forever — indistinguishable from data loss. Three
   fixes, each load-bearing:
   - **`Absorb` drains before halting** (`ABSORB_DRAIN_TIMEOUT`): the
     teardown waits, while the driver is still live, for the replica's own
     commit index to cover its full local log and for the engine-applied
     watermark to cover that commit — only then shutdown + WAL delete.
     Unlike `Release`/`Reclaim` (whose teardowns erase the data anyway, so a
     lost tail is moot), an absorbed tablet's data is about to be *served*
     from this very engine. On timeout with the engine at least caught up to
     the locally-known commit, it proceeds with a loud warning (the residual:
     entries committed elsewhere whose commit-index propagation lost the
     race to the leader's own teardown — retained by the replicas that did
     drain, and re-winnable while the dissolving group can still elect); an
     engine that can't even reach the local commit re-registers and retries
     next tick. Deterministic regression: the
     `merge_widens_survivor_and_absorbs_sibling_unerased` corpus scenario
     proposes a write through the absorbed group and ticks the merge view
     with **zero intervening sim time** — pre-fix the write is provably lost,
     post-fix drained.
   - **Drain-before-widen ordering**: `plan` defers every `WidenScope` while
     this node still hosts a merged-away tablet (`state.hosted ∩
     view.merged` non-empty — an `Absorb` not yet confirmed), so the
     survivor's scope never covers a range whose local drain hasn't
     completed. Coarse (any pending absorb defers every widen — the planner
     has no absorbed→survivor association once the absorbed tablet left the
     map) but sound; costs one reconcile tick.
   - **A read-side scope pre-check + served/absent disambiguation**
     (`animusd::cp_get_local`/`cp_scan_local`, the read dual of ADR 0028's
     pre-propose range check): a get/scan resolving to a group whose live
     `scope_range()` does not contain the requested key/window (routing
     raced a merge's widen or a split's narrow) surfaces as a **retryable
     error** — never served; and a ReadIndex barrier failure (deposed /
     mid-election leader) is likewise a retryable error, never collapsed
     into `Value(None)` = "absent" (the forwarded `Get` arm used to do
     exactly that, while the `Scan` arm already errored — the asymmetry was
     the tell). `cp_read`/`cp_scan_one` retry these `"; retry"`-class
     errors internally with re-resolved routing, bounded by
     `CLIENT_TIMEOUT`, so the client-visible contract is unchanged. For
     scans the pre-check also closes a *silent truncation*:
     `linearizable_scan` filters rows through the live scope, so an
     un-widened survivor would return partial results with no error at all.
4. **Add an operator trigger**: `ClientCtx::trigger_merge` (mirroring
   `trigger_split` exactly) resolves both tablets' current epochs from one
   `Metadata` snapshot, proposes `MergeTablets`, and confirms by polling for
   the specific pair of effects only this exact merge produces (`left`'s
   epoch advanced past what was read, **and** `right` is gone from the map)
   — robust against `right` vanishing for an unrelated reason mid-poll.
   Exposed as `POST /admin/tablet/merge {left, right}` (mirroring
   `/admin/tablet/split`), `ClientRequest::MergeTablets` (mirroring
   `ClientRequest::SplitTablet`, so a follower-connected client relays via the
   existing `is_relayable_command` allowlist — `MergeTablets` was added to
   it), and `animus admin merge <admin-addr> <left> <right>` on the CLI.

## Consequences

- Merge is no longer a unit-tested-but-unreachable metadata command — it is
  now a real, atomic, operator-driven operation with a full data-plane
  reaction, following exactly the same "commit of one command is the whole
  operation" shape ADR 0028 established for split: no second, independently-
  failable data-plane step, so there is nothing left to leave half-done.
- A cluster operator (or a future automatic size-based trigger) can now
  actually reverse an over-eager split, reclaiming the per-tablet Raft
  overhead (one WAL file, one voter-set-tracking group) of a tablet that
  shrank back below a useful size, without losing any data or requiring a
  second data-copy step.
- **Automatic (size-based) merge triggering — the inverse of
  `auto_split_loop` — is explicitly out of scope for this increment.**
  `trigger_merge` is manual/operator-driven only, matching ADR 0029's
  original framing. A future automatic trigger would need its own signal
  (e.g. "this tablet's key count, plus its neighbor's, comfortably fits under
  the split threshold") and its own cooldown/contention discipline, mirroring
  `auto_split_loop`'s shape — a natural, separately-landable follow-up.
- ADR 0029's rebalancer can still diverge two adjacent siblings' replica sets
  (its own, still-current caveat), which blocks a later merge (the CAS
  requires identical replica sets) until they re-converge — this ADR does
  not change that; an operator (or a future auto-merge trigger) simply
  retries once placement has re-converged.
- `Metadata::merged_tablets` is one more small, permanently-retained field on
  the replicated state machine — bounded, and consistent with how this
  codebase already tolerates a couple of permanently-growing counters
  (`next_tablet_id`) rather than building a bespoke pruning scheme for a
  marker that is cheap to keep forever and dangerous to infer.

This ADR amends ADR 0029 (closes the "merge is deferred" note) and extends
ADR 0031 (two new planner actions, same fixed-order contract) and ADR 0028
(the shared-storage/no-data-movement premise merge relies on, symmetrically
to split).

## Amendment (2026-08-11, ADR 0018 PR2)

The `Absorb` drain (above) guarantees the absorbed group's own committed
writes reach the engine before its WAL is deleted — a data-loss fix. It does
**not**, on its own, stop a *very* stale absorbed-group leader from
continuing to accept new writes for its own range after the merge has
already committed and a survivor may already be serving it. ADR 0018 PR2's
**range seal** closes that residual, the merge dual of ADR 0028's fence
gap: the absorbed leader proposes `KvCommand::Seal` for its own full range
from inside the same drain-wait this ADR's `Absorb` teardown already
performs (so the seal is durably observable by the time the drain
completes), and the survivor's `WidenScope` additionally waits for that
seal (`Metadata::absorbed_by` provenance) before considering the widen
safe — layered on top of, never a substitute for, the drain-before-widen
sequencing above. See ADR 0018's PR2 amendment for the full design.

## Amendment (2026-08-14, ADR 0042/0043)

A stream's hidden per-stream table (fixed, equal token-range tablets,
provisioned once by `CreateStreamShards`) is exempt from this ADR's merge
entirely: `Metadata::apply`'s `MergeTablets` arm rejects a stream table's
tablets outright, the same guard ADR 0034's amendment adds for auto-split.
A shard's range identity is load-bearing for ADR 0042's shard-routing
contract (`token(pk)` maps to exactly one shard by construction) in a way
an ordinary table's tablet range is not, so this ADR's whole
absorb-and-drain mechanism — designed for tablets whose range boundaries
are incidental, operator- or auto-split-driven — simply never applies to
one. Growing a stream's shard count is instead ADR 0042's own committed
roadmap item: a generation-cut resharding that closes an entire shard
generation and opens a fresh one, never a merge of two existing shards.
