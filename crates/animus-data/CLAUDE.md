# CLAUDE.md — animus-data

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
  (residency, ADR 0005); plus `serve_anti_entropy(env, handle, tablet, peers,
  interval)`, the background convergence loop (a digest exchange, not a full
  push). It takes the `ReplicaHandle` — not a `(storage, epoch)` pair — and reads
  the tablet's **live** epoch from it each round, so it is not fenced after a
  reconcile bumps the epoch (see the anti-entropy bullet below).
- `client.rs` — `DataClient` (quorum coordinator, incl. read-repair),
  `TabletView` (replicas + epoch + R/W for one tablet), `Router` (key → owning
  tablet), `ReadResult`. `DataClient::with_hints(env, store, allowed)` enables
  **hinted handoff** (a write/delete that misses a replica buffers a hint);
  `DataClient::new` keeps the no-hint behavior. The `write`/`read`/`delete`
  signatures are unchanged. `DataClient::scan(view, start, end, limit, timeout)`
  is the **native quorum range scan** (`DataMsg::ScanRange`/`ScanResp`): it
  broadcasts a half-open `[start, end)` range read, and once `r` replicas respond
  **merges their per-replica records by per-key newest MVCC version** (LWW, exactly
  like a point read — tombstones ride the merge so a newer delete shadows a stale
  value on another replica, then are excluded), returning the sorted live
  `(key, value)` set (`None` if a read quorum is unreachable; optional `limit`
  caps the first N keys in key order). Epoch-fenced like a point read. The wire
  adapters (`animusd`'s DynamoDB base `Query`/`Scan`) use it instead of tracking
  written keys in memory.
- `hint.rs` — hinted handoff (ADR 0010 + 0005): a `HintStore` (per-coordinator,
  in-memory, `BTreeMap`-keyed by `(target, tablet, key)`, per-key LWW) plus two
  replay drivers — `serve_hint_handoff` (probe-based, for a holder with a
  dedicated node id; `DataMsg::Probe`/`ProbeAck` confirm reachability, then a
  `Sync` replay clears the hint) and `serve_hint_replay` (send-only, for a holder
  sharing its inbox with a co-located `recv` consumer; re-sends each round). Both
  are residency-bounded (`AllowedTargets`).

## What's non-obvious

- Choose `R + W > N` so a read intersects every acknowledged write. The
  coordinator returns as soon as a quorum responds, so a down replica never
  blocks; ops it can't quorum on fail (the caller records them `info`, never
  lost-write).
- **An ack means the write durably applied.** A replica replies
  `WriteAck { ok: true }` / `DeleteAck { ok: true }` only when its
  `StorageEngine::merge` / `merge_tombstone` returned `Ok` — a superseded no-op
  (`Ok(false)`) still counts (durable state already reflects a newer write), but
  a storage `Err` is `ok: false`. The coordinator counts only `ok` acks toward W,
  so a write/delete that fewer than W replicas could persist *fails* instead of
  being falsely reported committed (it never silently swallows the storage
  result). Matters now that the replica can be backed by the durable on-disk LSM
  (`animusd`), where a persist can genuinely fail.
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
- **Anti-entropy follows the LIVE tablet epoch.** `serve_anti_entropy` takes the
  `ReplicaHandle` and stamps each round's `SyncDigest` with
  `handle.epoch(tablet)` read **live that round** — *not* a constant captured at
  start. After a placement reconcile bumps the tablet epoch (the control plane
  advances each replica via `ReplicaHandle::set_epoch`), a loop still stamping the
  old epoch would be fenced by up-to-date peers, leaving a re-placed spare reliant
  on read-repair on its first read; reading the epoch live keeps **background**
  convergence working across the reconcile. Fencing is *not* weakened: a
  genuinely older-epoch peer's repair is still rejected. The epoch is read under a
  brief lock released before any `.await` (no guard held across an await). Proven
  in `repair.rs` (`anti_entropy_tracks_the_live_epoch_after_a_reconcile` and
  `anti_entropy_still_fences_a_genuinely_stale_epoch_peer`).
- **Residency on repair (ADR 0005)**: `serve_replica_with_residency(allowed)`
  drops any `Sync`/`SyncDigest`/`SyncPull` from a node outside `allowed`, so
  repair cannot leak across a residency boundary even to a reachable node. The
  send side is already bounded (read-repair → `view.replicas`, anti-entropy →
  the caller's `peers` list — both the tablet placement). Derive `allowed` from
  `PlacementPolicy::admits`, the same check the control plane places with.
- **Hinted handoff (ADR 0010 + 0005)**: a third convergence path, on the *write*
  side. When a quorum `write`/`delete` commits at `W` but a tablet replica did
  not ack it (down/partitioned), a hinting `DataClient` (built with
  `with_hints`) buffers a hint `(target, tablet, epoch, key, value/tombstone,
  version)` in a `HintStore` — but **only for a target the placement admits**
  (`AllowedTargets`, derived from `PlacementPolicy::admits`). A replay driver
  delivers the hint via the ordinary `Sync` path (epoch-fenced, per-key LWW,
  idempotent) when the target returns: `serve_hint_handoff` probes first
  (dedicated holder env), `serve_hint_replay` re-sends each round (shared inbox,
  as in `animusd`). It is an **accelerator** layered on anti-entropy — a lost
  hint costs only promptness, never a write (the `W` durable replicas converge
  the laggard via anti-entropy regardless). The store is keyed per
  `(target, tablet, key)` so a superseding write clears a stale hint. **No lock is
  held across an `.await`** in `hint.rs` (the store `Mutex` is taken and dropped
  inside each synchronous helper). Residency backstop: the replica's
  `serve_replica_with_residency` must admit the holder/coordinator node (a
  trusted in-region participant), exactly as it must for coordinator read-repair.
  Deferred: tombstone GC (a grace period before reclaiming tombstones); a
  capped/TTL'd + durable hint store; and residency on backup.
- **Observability (ADR 0015)** is *observe-only* — recording changes no quorum
  semantics and every public signature stays additive. The `DataClient`
  coordinator records `data_quorum_writes_*`/`data_quorum_reads_*`
  (attempted/succeeded/failed; a delete is a quorum mutation counted under the
  *write* counters), `data_read_repair_triggered` + `_keys_repaired` (a divergent
  read pushes the winner back), and `data_hints_stored` (per residency-admitted
  hint buffered for an unreached replica). The background loops record
  `data_hints_delivered` (a hint batch re-sent to a returning target) and
  `data_anti_entropy_rounds` (each round emitting a non-empty digest). The
  coordinator's handle defaults to `env.metrics()`; thread a recording handle in
  with `DataClient::with_metrics` / `serve_anti_entropy_with_metrics` /
  `serve_hint_{handoff,replay}_with_metrics` (the originals forward
  `env.metrics()`) — that's how a sim test reads counters back without touching
  `SimEnv` (`SimEnv::metrics()` is the no-op default). Recording is a relaxed
  atomic add (no wall clock, no `HashMap`, no I/O), so it never perturbs
  determinism.
- A replica serves over any `StorageEngine`; values are opaque bytes. Higher
  layers (e.g. the dynamo adapter, or list-append test workloads) define their
  own value encoding.
- Give the coordinator and replicas **distinct node ids** — one inbox per node
  is single-consumer (don't co-locate a replica and a control `RaftNode` on the
  same id).

## Tests

`cargo test -p animus-data` — quorum + node-kill + fencing (`quorum.rs`),
the native range scan (`scan.rs`): merging divergent replicas newest-per-key in
key order, excluding a tombstoned key even when one replica holds a stale value,
honoring `limit`, failing (`None`) below a read quorum, fencing a stale-epoch
scan, and byte-reproducibility from a seed,
two-plane integration (`two_plane.rs`), multi-tablet routing (`routing.rs`),
read-repair + background anti-entropy convergence, incl. tombstone propagation
(`repair.rs`), segment-digest anti-entropy converging only divergent ranges
(`digest_anti_entropy.rs`, asserted at the wire level via the sim `Send` trace),
and residency on the repair paths — a reachable but ineligible peer never
receives repaired data (`residency_repair.rs`), ack-durability — a replica
whose storage `merge`/`merge_tombstone` errors replies `ok: false` so the quorum
write/delete fails rather than falsely succeeding (`ack_durability.rs`, with a
failing-engine test double), and hinted handoff (`hinted_handoff.rs`): a write
(then a delete) with one replica crashed buffers a hint, the replica recovers,
and the hint is replayed so it converges with **no read and no anti-entropy**
(via both the probe-based `serve_hint_handoff` and the send-only
`serve_hint_replay`); plus the residency bound — a placement-ineligible replica
is never hinted nor replayed to, while an eligible one is. `digest.rs` has inline
unit tests for the digest itself.

`metrics.rs` (ADR 0015) drives a known workload and asserts the `data_*`
counters move by the expected amounts — a quorum write+read bumps the
attempted/succeeded counters; a two-replica crash makes a write fail sub-quorum;
an `R=3` read against a deliberately-stale replica triggers exactly one
read-repair / one repaired key; a crash-then-return replica buffers a hint that
the replay loop delivers; a seeded replica's anti-entropy loop counts its rounds
— and that the recorded snapshot is byte-identical across two runs of one seed
(determinism). It threads a recording `MetricsHandle` into the coordinator/loops
since `SimEnv::metrics()` is the no-op default.

`concurrent_multithread.rs` is a **real multi-threaded** liveness regression
(`#[tokio::test(flavor = "multi_thread", worker_threads = 4)]` over `ProdEnv`,
timeout-guarded): the deterministic single-threaded `SimEnv` proves logic/order
but **not** real-thread liveness, so a `std::sync::Mutex` guard stranded across
an `.await`, or the serve loop / anti-entropy loop / a coordinator wedging on a
lock or waker handoff under contention, would pass the sim and only deadlock
here. It stands up a 3-replica tablet (each running `serve_replica` +
`serve_anti_entropy` on a tight interval over its own `MemoryEngine`) and drives
several concurrent `DataClient`s hammering the same keys with interleaved
write/read/delete, then probes that a read quorum still answers. **Audit
result:** the data plane holds **no** lock across an await — the epoch `Mutex` is
taken and released inside the synchronous `fenced()` helper *before* any storage/
network await, and the compiler's `Send` bound on `spawn_task` makes the
cross-await `MutexGuard` pattern a **build error**, not merely a test failure. So
the regression is clean; the passing test is the liveness *confidence*, not a bug
fix. **Lesson** (mirrors `animus-storage/tests/lsm_concurrent.rs`): concurrency
primitives need a `ProdEnv` multi-thread test; the sim proves order, not races.
