# CLAUDE.md — custos-data

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The leaderless AP data plane: quorum reads/writes for a tablet, routing via the
tablet map, and per-tablet epoch fencing (ADR 0001, 0002).

## Entry points

- `lib.rs` — `DataMsg` wire protocol (each op names its `tablet` + `epoch`;
  `Sync` carries a `(key, value, version)` batch for repair).
- `replica.rs` — `serve_replica(env, storage, floor_epoch) -> ReplicaHandle`:
  the per-node server over a `StorageEngine`; plus `serve_anti_entropy(...)`,
  the background convergence loop.
- `client.rs` — `DataClient` (quorum coordinator, incl. read-repair),
  `TabletView` (replicas + epoch + R/W for one tablet), `Router` (key → owning
  tablet), `ReadResult`.

## What's non-obvious

- Choose `R + W > N` so a read intersects every acknowledged write. The
  coordinator returns as soon as a quorum responds, so a down replica never
  blocks; ops it can't quorum on fail (the caller records them `info`, never
  lost-write).
- **Routing reads from a cached `TabletView`/`Router`**, not from a live control
  query — that's what keeps the data plane serving during a control-plane outage
  (only topology changes, which bump epochs, need the control plane).
- **Epoch fencing is per tablet** (`ReplicaHandle::set_epoch(tablet, epoch)`): a
  replica rejects an op whose epoch is older than its known epoch for *that*
  tablet, and advances on a newer one. A topology change to one tablet must not
  fence another.
- **Repair / anti-entropy (ADR 0010)** is what makes *raw replica state*
  converge — `R + W > N` only makes quorum *reads* intersect, so a replica that
  missed a write stays stale until repaired. Replica writes apply via
  `StorageEngine::merge` (per-key LWW, idempotent/commutative — not `put`'s
  engine-wide monotonic version, which would reject re-applying a value at its
  original version). **Read-repair**: a quorum read that sees responders disagree
  pushes the winner back as a fire-and-forget `Sync` (repairs the read's
  participants only). **Anti-entropy**: `serve_anti_entropy` full-pushes a
  replica's `entries()` digest to peers on a timer, converging even keys nobody
  reads. Both are epoch-fenced. Deferred: Merkle digests (vs. full-push) and
  tombstone propagation (no data-plane delete yet).
- A replica serves over any `StorageEngine`; values are opaque bytes. Higher
  layers (e.g. the dynamo adapter, or list-append test workloads) define their
  own value encoding.
- Give the coordinator and replicas **distinct node ids** — one inbox per node
  is single-consumer (don't co-locate a replica and a control `RaftNode` on the
  same id).

## Tests

`cargo test -p custos-data` — quorum + node-kill + fencing (`quorum.rs`),
two-plane integration (`two_plane.rs`), multi-tablet routing (`routing.rs`),
read-repair + background anti-entropy convergence (`repair.rs`).
