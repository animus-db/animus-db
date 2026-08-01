# CLAUDE.md — custos-data

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The leaderless AP data plane: quorum reads/writes for a tablet, routing via the
tablet map, and per-tablet epoch fencing (ADR 0001, 0002).

## Entry points

- `lib.rs` — `DataMsg` wire protocol (each op names its `tablet` + `epoch`).
- `replica.rs` — `serve_replica(env, storage, floor_epoch) -> ReplicaHandle`:
  the per-node server over a `StorageEngine`.
- `client.rs` — `DataClient` (quorum coordinator), `TabletView`
  (replicas + epoch + R/W for one tablet), `Router` (key → owning tablet),
  `ReadResult`.

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
- A replica serves over any `StorageEngine`; values are opaque bytes. Higher
  layers (e.g. the dynamo adapter, or list-append test workloads) define their
  own value encoding.
- Give the coordinator and replicas **distinct node ids** — one inbox per node
  is single-consumer (don't co-locate a replica and a control `RaftNode` on the
  same id).

## Tests

`cargo test -p custos-data` — quorum + node-kill + fencing (`quorum.rs`),
two-plane integration (`two_plane.rs`), multi-tablet routing (`routing.rs`).
