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

## Tests

`cargo test -p animus-placement` — residency, strict/best-effort spread, error
cases, determinism, churn-minimizing `replan` (`tests/placement.rs`). The
through-Raft, fault-injecting integration test is
`animus-control/tests/placement_reconcile.rs`.

## Deferred (ADR 0005)

Residency across read-repair / anti-entropy / hinted handoff / backup. (Policy
replication in `Metadata` and the in-node automatic reconciler now exist — see
`animus-control`'s `SetTabletPolicy` + `Metadata::reconcile` + `reconcile_loop`.)
A cluster-default policy and operator-facing policy management are future work.
