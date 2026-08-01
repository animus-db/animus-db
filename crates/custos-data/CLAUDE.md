# CLAUDE.md — custos-data

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The leaderless AP data plane: quorum reads/writes for a tablet, routing via the
tablet map, and per-tablet epoch fencing (ADR 0001, 0002).

## Entry points

- `lib.rs` — `DataMsg` wire protocol (each op names its `tablet` + `epoch`;
  quorum `Write`/`Delete` + repair traffic: `Sync` carries a `SyncEntry =
  (key, Option<value>, version)` batch — `None` is a tombstone — and the
  segment-digest pair `SyncDigest`/`SyncPull` drives range-based anti-entropy).
- `digest.rs` — pure, deterministic segment digests: `digest(entries)` →
  `Vec<SegmentDigest>`, `divergent(mine, theirs)` → the segments to pull,
  `entries_in_segments(entries, segments)` → the entries to push back.
- `replica.rs` — `serve_replica(env, storage, floor_epoch) -> ReplicaHandle`:
  the per-node server over a `StorageEngine`; `serve_replica_with_residency(...,
  allowed)` is the same but rejects repair traffic from peers outside `allowed`
  (residency, ADR 0005); plus `serve_anti_entropy(...)`, the background
  convergence loop (now a digest exchange, not a full push).
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
  `StorageEngine::merge` and deletes via `merge_tombstone` (per-key LWW,
  idempotent/commutative — not `put`'s engine-wide monotonic version, which
  would reject re-applying at the original version). **Read-repair**: a quorum
  read that sees responders disagree pushes the winner back as a fire-and-forget
  `Sync` (repairs the read's participants only). **Anti-entropy**:
  `serve_anti_entropy` runs a timer loop that exchanges a **segment digest**
  (`SyncDigest`) with peers; a peer pulls (`SyncPull`) only the segments whose
  hash differs, answered by a `Sync` of just those entries (tombstones included,
  so a delete reaches a replica that still holds the value). A converged pair
  moves no entry data; a one-key difference moves only that key's segment —
  `digest.rs` holds the pure logic. Both paths are epoch-fenced.
- **Residency on repair (ADR 0005)**: `serve_replica_with_residency(allowed)`
  drops any `Sync`/`SyncDigest`/`SyncPull` from a node outside `allowed`, so
  repair cannot leak across a residency boundary even to a reachable node. The
  send side is already bounded (read-repair → `view.replicas`, anti-entropy →
  the caller's `peers` list — both the tablet placement). Derive `allowed` from
  `PlacementPolicy::admits`, the same check the control plane places with.
  Deferred: tombstone GC (a grace period before reclaiming tombstones), and
  residency on hinted handoff / backup.
- A replica serves over any `StorageEngine`; values are opaque bytes. Higher
  layers (e.g. the dynamo adapter, or list-append test workloads) define their
  own value encoding.
- Give the coordinator and replicas **distinct node ids** — one inbox per node
  is single-consumer (don't co-locate a replica and a control `RaftNode` on the
  same id).

## Tests

`cargo test -p custos-data` — quorum + node-kill + fencing (`quorum.rs`),
two-plane integration (`two_plane.rs`), multi-tablet routing (`routing.rs`),
read-repair + background anti-entropy convergence, incl. tombstone propagation
(`repair.rs`), segment-digest anti-entropy converging only divergent ranges
(`digest_anti_entropy.rs`, asserted at the wire level via the sim `Send` trace),
and residency on the repair paths — a reachable but ineligible peer never
receives repaired data (`residency_repair.rs`). `digest.rs` has inline unit
tests for the digest itself.
