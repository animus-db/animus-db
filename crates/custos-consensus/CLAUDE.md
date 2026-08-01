# CLAUDE.md — custos-consensus

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

Accord-style leaderless transactions layered over the AP data plane. **Skeleton
only** — out of scope until the data plane is more mature.

## When you build this

- This is where the Elle cycle checker in `custos-test` earns its keep: the
  transactional path is exactly what `check_cycles` validates.
- Like the control plane, keep the protocol logic in a synchronous, I/O-free
  core driven over the `Env` seam, so it stays deterministic and replayable.
