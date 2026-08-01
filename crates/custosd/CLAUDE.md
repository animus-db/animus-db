# CLAUDE.md — custosd

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The node server daemon. **Placeholder `main` only** — no node wiring yet.

## When you build this

- This is where `ProdEnv` is finally instantiated: bind the node's address +
  peers + data dir, then assemble a control-plane `RaftNode` and a data-plane
  `serve_replica` on it (give them distinct node ids — one inbox per node is
  single-consumer).
- Keep all logic in the library crates; this binary should be thin wiring +
  config + `tracing` setup, with nothing the deterministic path needs to share.
