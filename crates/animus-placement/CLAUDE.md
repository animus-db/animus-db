# CLAUDE.md — animus-placement

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

Topology-aware placement and data residency (ADR 0005): given cluster
membership and a placement policy, decide which nodes replicate a tablet.

## Entry points

- `lib.rs` — the whole crate:
  - `PlacementPolicy` (replication factor + residency `required_labels` +
    optional `SpreadPolicy` failure-domain), with `simple`/`require_label`/
    `spread_across` builders and `admits` (residency check).
  - `Candidate` (a node id + its topology labels).
  - `select_replicas(candidates, policy)` — fresh placement.
  - `replan(current, candidates, policy)` — recompute after a membership
    change, **keeping eligible survivors** so only failed/ineligible replicas
    move (minimal data churn).
  - `rebalance_step(tablets, candidates)` — one **load-rebalancing** move (ADR
    0029): the balance-driven counterpart of `replan`. Where `replan` only moves
    a replica *off* a failed/ineligible node, this moves a *healthy* replica from
    a most-loaded node to a least-loaded one so a cluster grown N→M spreads its
    existing tablets onto the new members. At most **one** move per call
    (`Some((tablet, new_set))` / `None` when balanced or no legal move) — a
    deliberate one-CAS-per-evaluation churn bound; repeated application converges
    to max−min ≤ 1 (moving src→dst with count diff ≥ 2 strictly reduces the
    sum-of-squares, so it never oscillates). Skips tablets whose *current* set
    violates their policy (that's `replan`/reconcile's job), preserves residency,
    and never worsens spread (strict: keeps distinct domains; best-effort: never
    raises max-per-domain).
  - `PlacementError` (`InsufficientCandidates`, `InsufficientDomains`).

## What's non-obvious

- This crate is a **pure, deterministic library** — no `Env`, no I/O, no
  randomness. It must return the same set on every Raft replica and on replay,
  so it leans on `BTreeMap`/`BTreeSet` ordering and sorts its output. Don't
  introduce a clock, RNG, or `HashMap` here.
- It depends only on `animus-env` (`NodeId`), **not** on `animus-control` — the
  control plane depends on placement (now a *normal* dependency: `Metadata`
  stores `PlacementPolicy` and `Metadata::reconcile` calls `replan`), so a
  reverse dep would be a cycle. The control plane builds `Candidate`s from
  `Active` `Metadata` members and turns the result into a `CasTabletReplicas`.
  Sim integration tests: `animus-control/tests/placement_reconcile.rs`
  (caller-driven) and `placement_auto_reconcile.rs` (leader-driven, automatic).
- Selection is greedy **least-loaded-domain-first**, which gives even spread for
  fresh placement and, seeded with the survivors, prefers fresh domains on a
  `replan` — so a single replica death is replaced like-for-like in its own
  failure domain. Strict spread needs ≥ RF distinct domains or it errors;
  best-effort doubles up only after every domain holds one.
- Liveness is the **caller's** job: pass only the candidates you'd place on
  (e.g. `Active` members). This crate enforces *policy* (residency + spread),
  not health.
- `rebalance_step` is generic over the tablet key (`<K: Ord + Copy>`) — the only
  generic in the crate; callers pass whatever id type indexes their tablet map.
- No seeds/replay apply here: this is a pure library with no sim timers — the
  `ANIMUS_SEED` convention belongs to the through-Raft integration tests in
  `animus-control`, not to this crate's own suite.
- Historical note: per-shard Accord placement (`animus-consensus`'s
  `ShardedOwner`/`ShardRouter`, one Accord group per tablet whose replica set was
  the tablet's) used to be a consumer of the tablet map this crate feeds; that
  driver was trimmed with Accord's testbed-only scope (ADR 0018/0019).

## Tests

`cargo test -p animus-placement` — residency, strict/best-effort spread, error
cases, determinism, churn-minimizing `replan` (`tests/placement.rs`), and the
`rebalance_step` planner: noop-when-balanced, single most→least move, residency +
strict/best-effort spread guards, at-most-one-move + repeated-application
convergence, and input-permutation determinism (`tests/rebalance.rs`). The
through-Raft, fault-injecting integration tests are
`animus-control/tests/placement_reconcile.rs` (repair) and
`placement_rebalance.rs` (rebalancing).

`tests/placement_props.rs` (ADR 0061 rung A1, `proptest` dev-dep) generalizes
the fixed-scenario suites above over randomized topologies (random node
counts, RF, residency labels, failure domains): `rebalance_step` moves at
most one replica per call, never worsens spread/residency compliance on any
move, and terminates in a provably bounded number of steps (the
sum-of-squares argument in `rebalance_step`'s own doc); `replan` under random
node failures never places a dropped node, keeps every surviving replica,
and returns an RF-sized, residency-admitted set whenever `Ok`. **A precise
finding, not a bug**: `rebalance_step`'s "converges to max−min ≤ 1" claim is
proven here only for a policy with **no** `SpreadPolicy` — a strict or
best-effort spread constraint can legally block every improving move on
every eligible tablet, so the module doc for this test file explains why the
final-balance assertion is conditioned on `policy.spread.is_none()` rather
than asserted unconditionally. Case counts are kept modest (64/`ProptestConfig`)
for the per-push gate; the properties hold up to several thousand cases
locally (see the file's generation-strategy comments for how infeasible
cases are avoided rather than filtered by a high-discard `prop_assume!`).

## Deferred (ADR 0005)

Residency across read-repair / anti-entropy / hinted handoff / backup. (Policy
replication in `Metadata` and the in-node automatic reconciler now exist — see
`animus-control`'s `SetTabletPolicy` + `Metadata::reconcile` + `reconcile_loop`.)
A cluster-default policy and operator-facing policy management are future work.
