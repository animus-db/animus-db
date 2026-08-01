# CLAUDE.md — custos-placement

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

Placement groups + topology-aware data residency (ADR 0005). **Skeleton only** —
not started.

## When you build this

- Membership already carries topology labels (`custos-control`'s `Member`); this
  crate adds named placement policies constraining which topology domains a
  tablet's replicas may occupy, and a reconciler that picks satisfying replica
  sets.
- The hard, deliberate part is residency across *every* path — hinted handoff,
  read-repair, anti-entropy, backup — not just initial placement. Residency is
  only as strong as its weakest path.
- Stay deterministic: build it over the `Env` seam and test under `custos-sim`.
