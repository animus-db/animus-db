# ADR 0031 — Per-node tablet-host reconciler + a metadata-applied watch primitive

- **Status:** Accepted — implemented incrementally across PRs 1–6. PR1
  delivered the metadata-watch primitive (§trigger); PR2 made
  `ClusterEdgeState` genuinely per-node; PR3 delivered the pure planner
  (`animus_cp_data::host::plan`); PR4 delivered the executor
  (`animus_cp_data::host::Reconciler`) and wired it into `animusd`, retiring
  `cp_join_host_loop`, `cp_gc_loop`/`cp_gc_release_phase`, and
  `cp_reconfigure_loop`. **PR5 (this PR) delivers a dedicated `SimEnv`
  lifecycle fault-injection corpus for the reconciler**
  (`animus-cp-data/tests/reconciler_corpus.rs` — 18 frozen, name-seeded
  scenarios spanning host/split-narrow/reconfigure/release/reclaim, crash+
  restart, a network partition blocking a release, a control-plane replay
  epoch flicker, and the split-then-immediate-release sibling-corruption
  regression driven at zero ticks; depth knob `ANIMUS_RECONCILER_SEEDS`, held
  green at ×300 / 5,400 scenario runs) beyond PR4's own focused
  `reconciler_hosts_narrows_releases_and_confirms_sparing_a_sibling` test —
  see `animus-cp-data/CLAUDE.md`'s "Reconciler lifecycle corpus" section.
  PR6 (further docs cleanup) remains.
- **2026-08-10 note:** the `metadata_watch()` primitive this ADR introduced is
  unchanged in its public contract (fires once a `Metadata` change is durable
  *and* visible), but under ADR 0038 the bump now comes from the control
  plane's async apply task after it publishes its cache — not from the
  consensus driver loop directly — since `Metadata` is no longer applied
  in-core. Every consumer here (the reconciler's trigger) is unaffected.
- **Date:** 2026-08-07

## Context

A node's tablet *lifecycle* — deciding to host a tablet's `RaftKvNode`,
keeping its Raft voter set in step with the replicated replica set, narrowing
its `StorageScope` when it is the source of a split, releasing/erasing a
tablet it no longer replicates, and reclaiming a dropped table's tablets — is
currently smeared across **four independent, fixed-period polling loops** in
`animusd`:

| Loop | Interval | Owns |
|------|----------|------|
| `cp_join_host_loop` | 250ms | start a tablet's `RaftKvNode`; per-tick `narrow_scope` patch-up |
| `cp_reconfigure_loop` | 150ms | step a hosted group's Raft voters toward the replicated replica set |
| `cp_gc_loop` (reclaim phase) | 500ms | tear down + erase a dropped table's tablets |
| `cp_gc_loop` (release phase) | 500ms + `pending_release` dampener | tear down + erase a tablet this node was rebalanced/drained off |

These loops share mutable state — the per-node `minted` claim set, the
`pending_release` epoch-stability dampener, the `ClusterEdgeState` group
registry, and each hosted group's live `StorageScope` — with invariants held
only by *convention and cadence tuning*, not by construction. Three
independent sources of truth exist for one question, "does this node
replicate tablet T": the replicated `Metadata.tablets[t].replicas`, the
group's own live Raft `config()`, and the local `ClusterEdgeState` handle
registry — and every bug fixed in this area for the last dozen PRs has been
some pair of those three disagreeing at the wrong moment.

ADR 0028 already names this directly: *"The `cp_reconfigure_loop`/
`reconcile_loop` race (above) is mitigated, not eliminated. … An event-driven
reconfiguration trigger (react to a `Metadata` change directly, rather than
polling) would close this properly and is a candidate follow-up if it is ever
observed to matter beyond test flakiness."* The root `CLAUDE.md` records the
concrete failure mode that motivated the 500ms→150ms mitigation: two
un-jittered fixed-period pollers racing a **one-shot** outcome (a manual
replica-set change), where cadence *ratio*, not correctness, decides who wins.

Beyond that specific race, the wider pattern recurring across the last several
engineering-practices entries has one signature: **a per-node cache of
replicated state going stale** between the tick that last refreshed it and the
tick that acts on it —

- a stale `StorageScope` (parent tablet never re-narrowed after a split);
- GC's release-phase erase using a stale-wide scope and tombstoning a
  co-hosted sibling's live data;
- write fences that existed and were unit-tested but had zero production
  callers, because nothing re-derived the "current scope" input they needed
  at the one call site that mattered;
- a read-barrier quorum keyed on a hosting-time peer snapshot instead of the
  live Raft config.

Every one of these was eventually fixed point-by-point (see the root
`CLAUDE.md`'s "cached per-node handle" entries). But point fixes over four
independently-timed loops do not add up to one coherent lifecycle — each fix
closes the specific race that was found, not the *shape* that keeps producing
new ones: **the per-node reaction to a replicated-state change is decided
piecemeal, in whatever order each loop's own tick happens to observe things,
instead of once, in one place, in a fixed order.**

## Decision

We will consolidate the four loops into **one per-node tablet-host `Reconciler` (`animus_cp_data::host::Reconciler`)**
living in `animus-cp-data` (generic over `E: Env`, so it is `SimEnv`-testable
like everything else in this codebase), delivered across six PRs:

1. **PR1 (this PR):** an ADR, plus the primitive the reconciler is triggered
   by — `RaftNode::metadata_watch()` on the control-plane driver: an
   executor-agnostic "applied index advanced" notification, so a reaction to a
   `Metadata` change no longer has to wait out a polling interval.
2. **PR2:** per-node `ClusterEdgeState` in `--cluster N` (today's dev-mode
   in-process bring-up shares one `ClusterEdgeState` across all nodes, which
   is exactly the "shared edge masks per-node state" gotcha documented
   repeatedly in the root `CLAUDE.md` — the reconciler needs a genuinely
   per-node registry to reason about correctly, in-process or not).
3. **PR3:** the pure planner — `plan(view, facts, local_state) -> Vec<HostAction>`,
   where `HostAction` is one of `Host` / `NarrowScope` / `Reconfigure` /
   `Release { erase_bound }` / `Reclaim`, **emitted in that fixed order** so
   "narrow the scope before erasing anything" and "reconfigure only a tablet
   already hosted" are structural properties of the planner's output, not
   properties some ordering of loop ticks happens to have provided.
4. **PR4 (delivered):** the reconciler itself —
   `animus_cp_data::host::Reconciler<E: Env, S: StorageEngine>`. Each tick
   takes **one** `Metadata` snapshot (as a `MetadataView`), gathers the
   impure facts the plan needs from its *own* hosted `RaftKvNode` map and
   engine (`is_leader()`, `config()`, `scope_range()`, an async
   `StorageScope::has_data` check for join candidates), calls `plan` once,
   then executes the returned actions in the fixed order `plan` emits them.
   The reconciler owns the hosted-group map directly (the single writer of
   "does this node host tablet T"); `animusd` mirrors every change into its
   own `ClusterEdgeState` (routing) via `on_host`/`on_teardown` hooks passed
   at construction, making that registry a read-only mirror with exactly one
   writer. `animusd::BoundNode::start_with` replaced its three separate
   loops (`cp_reconfigure_loop`, `cp_join_host_loop`, `cp_gc_loop`) with one
   `tablet_host_reconciler_loop` task that races
   `RaftNode::metadata_watch().changed(last_seen)` against a 500ms fallback
   sleep (load-bearing for an ADR 0030 growth node, whose own control raft
   never advances) and coalesces to the freshest observed index before each
   tick. The `last_applied() == 0` pre-recovery guard stays in `animusd`
   (a live `RaftNode` read the pure planner has no business taking), gated on
   *both* the local raft and the growth-node remote-metadata mirror being
   unavailable, so it never permanently blocks a growth node's reconciler.
5. **PR5 (delivered):** a `SimEnv` lifecycle corpus exercising the reconciler
   across the full host → reconfigure → split-narrow → release → reclaim
   sequence under fault injection, beyond PR4's own focused
   `reconciler_hosts_narrows_releases_and_confirms_sparing_a_sibling` test
   (`animus-cp-data/tests/reconciler.rs`) —
   `animus-cp-data/tests/reconciler_corpus.rs`, 18 frozen name-seeded
   scenarios (host/elect/serve; split-narrow-sibling; rebalance-off release;
   drop-table reclaim; spare-join promotion; a growth node's late first view;
   reconfigure repairing a `Down` replica and transferring leadership off
   itself; crash+restart of a sole replica and of a follower, each relying on
   the `has_data` restart-upgrade; a control-plane replay epoch flicker
   around the release dampener; the documented Reclaim-has-no-dampener
   contract boundary; a network partition blocking a release until healed;
   the split-then-immediate-release sibling-corruption regression driven
   deterministically at zero ticks; a re-add cancelling a pending release;
   narrow-never-widens; and multi-tablet idempotence) plus depth
   (`ANIMUS_RECONCILER_SEEDS`) and coverage-guard tests, held green at ×300 /
   5,400 scenario runs. See `animus-cp-data/CLAUDE.md`'s "Reconciler
   lifecycle corpus" section for the full list and how to run/extend it.
6. **PR6:** retire remaining doc references to the old loop names; further
   docs cleanup.

### The trigger (this PR)

`RaftNode::metadata_watch() -> MetadataWatch` hands out a cloneable handle
wrapping an `AtomicU64` (the latest index the driver has observed becoming
client-visible) plus a `futures::task::AtomicWaker`. `MetadataWatch::changed(last_seen)`
returns a future that resolves once the watermark exceeds `last_seen`.

This follows the same template as `animus-cp-data`'s `ProposeSignal` (the
primitive that already cut single-write CP-data latency by racing a
`select` arm off an `AtomicWaker` instead of waiting for the next heartbeat
tick): no tokio-only primitive (`Notify`/`watch`, which `SimEnv` cannot
drive), register-the-waker-before-checking-the-flag, and — the one
difference from `ProposeSignal`'s one-shot flag — `changed()` re-checks a
*monotonic watermark* fresh on every poll rather than consuming a flag, so
there is no wake-before-park race to reason about: if the watermark already
advanced past `last_seen` before the future is even polled the first time,
that first poll resolves immediately, no wake required.

The driver bumps the watermark, and wakes any parked waiter, at exactly the
points client-visible state can move — the same **durable-before-visible**
frontier `metadata()` itself is gated on (`min(commit_index, durable_index)`
on the leader, `commit_index` on a follower; see `raft.rs::apply`'s doc and
the root `CLAUDE.md`'s "durable-before-visible" entry). Concretely: once after
WAL recovery (a restart can recover already-applied state), and after each of
the driver loop's two `flush_and_maybe_compact` calls per iteration — which
between them cover a follower's apply-on-commit (already done inside
`handle()`, before the second flush) and a leader's `mark_durable_through`-gated
apply (inside `flush_wal`, called from either flush). `MetadataWatch::bump`
is a `fetch_max` plus a wake **only when the watermark actually advanced**, so
calling it defensively at multiple points in the loop costs nothing extra on
the (common) "nothing changed this iteration" case.

`MetadataWatch` is deliberately single-waiter, mirroring `ProposeSignal`: an
`AtomicWaker` only remembers the most recently registered waker, so two
concurrent callers of `changed()` would starve one of them. This matches the
intended consumer exactly — one `Reconciler` task per node — and is
documented on the type rather than hidden.

## Consequences

**What becomes structurally impossible** (once PRs 2–4 land on top of this
primitive):

- A "narrow before erase" ordering violation — today `narrow_scope` is a
  ceteris-paribus per-tick patch-up in `cp_join_host_loop`, entirely
  independent of `cp_gc_loop`'s release-phase erase; nothing *enforces* that
  the narrow the release needs has actually happened before the erase runs
  (the fix that shipped instead re-passes the current range into the erase
  call site directly, which works but leaves the ordering as a convention
  between two unrelated loops rather than a property of one planner's output
  order).
- Three independent, differently-timed views of "does this node replicate
  tablet T" disagreeing — the reconciler takes exactly **one** `Metadata`
  snapshot per tick and derives every decision from it plus the impure facts
  gathered *for that same tick*.
- A cadence-ratio race between two loops each capable of "winning" a one-shot
  outcome — there is only one loop left to race against `reconcile_loop`
  (the control plane's own policy reconciler, which stays separate — see
  below), and the event-driven trigger this PR adds means the reaction fires
  on the change itself, not on the next arbitrarily-phased tick.

**Costs and risks knowingly accepted:**

- **One loop is now a bigger single point of review and failure.** Four
  loops, misbehaving independently, each had a narrow blast radius (a bug in
  `cp_gc_loop`'s release phase could not, by construction, break
  `cp_join_host_loop`'s hosting decision). A single reconciler that owns the
  whole lifecycle raises the stakes of a bug in the shared planner —
  mitigated by keeping the planner **pure** (PR3) so it is unit-testable
  exhaustively without any `Env`, and by the PR5 fault-injection corpus.
- **`MetadataWatch` is single-waiter by design**, not a general-purpose
  broadcast/pub-sub primitive — a future consumer needing more than one
  independent watcher on one `RaftNode` would need its own instance (cheap:
  `RaftNode::metadata_watch()` returns a fresh clone sharing the same
  underlying cursor, so multiple *clones* are fine as long as only one task at
  a time calls `changed()` on any given clone's lineage) or a genuine
  broadcast primitive, which this PR does not build.
- **A metadata-watch wake is bounded by the driver's own flush cadence, same
  as today.** The control plane does not have `animus-cp-data`'s
  wake-on-propose optimization (`ProposeSignal` there wakes the *consensus*
  loop itself to replicate immediately; here we are only notifying an
  *external* watcher of a change the driver was going to make visible on its
  own schedule regardless). This is correct, not a shortfall: `metadata_watch`
  promises "notified as soon as `metadata()` could reflect the change," and
  that visibility is itself bound by the same driver cadence a caller polling
  `metadata()` directly would see.

**What deliberately does NOT change:**

- `RaftCore` stays sync and `Env`-free; the watch primitive lives entirely in
  the driver (`RaftNode`), exactly where `ProposeSignal` lives in
  `animus-cp-data`'s driver rather than its core.
- The `StorageEngine` trait, write fences (`RaftKvNode::scope_range`, the
  `*_fenced` proposers), and the wire adapters are untouched.
- `animus-control`'s own `reconcile_loop` (placement policy + rebalancing,
  ADR 0005/0029) and `detect_loop` (failure detection, ADR 0012) remain
  separate loops — they decide the *replicated* replica set, which is a
  cluster-wide policy question the control-plane leader alone can answer.
  The `Reconciler` this ADR introduces is downstream of that
  decision: it reacts, per node, to whatever replica set `Metadata` already
  says. `animusd`'s `auto_split_loop`, `bootstrap`, `peer_sync`, and the
  heartbeat loop also stay separate — none of them owns "does this node's
  local state match what `Metadata` says about this node," which is the one
  question the reconciler exists to answer.
- `SharedWal`/`TaggedRecord` (the multiplexed-WAL-file machinery ADR 0028
  built but deliberately left unwired) stays parked. It solves a different
  problem (one physical WAL file per node instead of one per tablet) that is
  orthogonal to *which* tablets a node hosts and how it reacts to that
  changing — nothing in this consolidation depends on it, and wiring it in
  remains its own follow-up with its own fault-injection story.

This ADR amends ADR 0028 (names the "mitigated, not eliminated" race this
closes) and ADR 0029 (the release-GC and reconfigure mechanics the reconciler
subsumes); it builds on ADR 0026 Stage B (stream addressing) and ADR 0009 (the
durable-before-visible frontier `metadata_watch` mirrors) without changing
either.
