# CLAUDE.md — animus-env

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The `Env` seam: the single boundary through which all AnimusDB code reaches
time, randomness, the network, disk, and task spawning. Defines the traits and
the production implementation; the deterministic implementation lives in
`animus-sim`.

## Entry points

- `lib.rs` — the traits `Clock`, `Rng`, `Network`, `Disk`, `Spawner`, combined
  into the **`Env` supertrait** (scoped to one `NodeId`), plus `Nanos`,
  `Envelope`, `BoxFuture`, `PRIMARY_STREAM` (the default stream id the
  `send`/`recv` defaults ride on, ADR 0026), and the `EnvExt::spawn_task`
  convenience. Also the vestigial `Coresident` sub-trait (see below).
- `prod.rs` — `ProdEnv`: real monotonic clock, `OsRng`, `tokio::spawn`,
  length-prefixed TCP, `tokio::fs` + `fsync`. Owns a real recording metrics sink
  and exposes `metrics_text()` (ADR 0015).
- `metrics.rs` — the **observability seam** (ADR 0015): a closed `Metric` enum
  (`control_*` Raft + `storage_*` LSM-engine counters, plus legacy `data_*`
  leaderless-AP counters that are **dormant** — the AP plane was deleted, ADR
  0019, but the enum is append-only so the variants stay), a fixed-array
  lock-free `MetricSink`, the cheap-to-clone `MetricsHandle`, and the
  `MetricSnapshot` text export. The enum is **append-only**: add new variants
  *after* the existing ones (and a matching row in `Metric::ALL`) so slots and
  the export order stay stable and the snapshot remains byte-reproducible.

## What's non-obvious

- `Env` is a *supertrait*, not a bag of accessors: a handle **is** a `Clock` +
  `Rng` + `Network` + `Disk` + `Spawner`. Callers write `env.now()`,
  `env.send(..)`, `env.recv()` directly. Because components are `<E: Env>`, the
  supertrait's methods are in scope from the bound — you do **not** need to
  `use animus_env::Clock` etc. in generic code (doing so trips an unused-import
  warning).
- `Network::send` is fire-and-forget (no delivery result); `recv` is
  **single-consumer per node** — never run two receive loops on one `NodeId`.
- **`ProdEnv` pools one outbound TCP connection per destination *address***
  (`TCP_NODELAY` set) instead of dialing per message — a Raft heartbeat no
  longer pays a handshake. Frames (`[from: u64][len: u32][payload]`) are
  unchanged and carry `from` per message, so one stream per addr is correct
  even with co-resident ids; the per-address `tokio::sync::Mutex` is held
  across the whole frame write (frame integrity) without head-of-line blocking
  across peers. On a write error (peer restarted) the stale stream is dropped
  and the send reconnects **once**, then surfaces the error — still
  fire-and-forget, and the frame in flight when a peer dies can be lost
  (higher layers retry, as before). The cache is shared with siblings like the
  peer book.
- **`Coresident` (ADR 0017 D) is vestigial — superseded by multiplexed streams
  (ADR 0026, below).** The sub-trait (`sibling(&self, id) -> Self`: a fresh
  handle on the same physical node bound to a different `NodeId` with its own
  inbox) still exists and `SimEnv`/`ProdEnv` still implement it, but it has
  **zero live call sites**: co-hosting a second protocol instance (a tablet's
  CP Raft group) now rides a distinct *stream* on the node's own id, and
  `animusd`'s `CP_SIBLING_POOL`/listener-pool plumbing is gone. Being a
  *sub-trait* (not part of the `Env` supertrait) was the right shape — nothing
  else ever had to care — and remains the pattern for any future opt-in env
  capability. Don't build new features on `sibling`; if a use appears, prefer
  a stream.
- `Disk` is append + explicit `sync`; bytes are not durable until `sync`
  returns. This models real crash semantics and is what `animus-sim` exploits.
  `Disk::replace` atomically swaps a file's whole contents (temp-file + rename
  in `ProdEnv`) — used for WAL compaction. In `ProdEnv`, the file-creating paths
  (`append`/`replace`) create missing parent directories, so a filename
  carrying a subdirectory prefix (e.g. `"db/wal"`) works instead of silently
  failing on a missing parent (`append` does it lazily — retry-on-`NotFound`,
  not a `create_dir_all` per call). **Namespace changes are fsynced**: `sync`
  and `replace` fsync the containing directory chain after the file
  `sync_all`/rename (POSIX requires it — without it a just-created WAL segment
  or completed manifest swap can vanish on power loss); `remove` deliberately
  does not (a resurrected orphan is harmless and cleanup handles it). The dir
  fsync runs only on the **first** `sync` of a file — creation is a one-time
  namespace change; a per-env `dir_synced` memo (invalidated by `remove`,
  refreshed by `replace`) keeps the WAL group-commit hot path at one fsync per
  commit. **`append` must `flush` before returning**: `tokio::fs::File`
  buffers, `write_all` can return with the write still in flight on the
  blocking pool, and dropping the handle completes it in the *background* — so
  without the flush two sequential `append`s (separate handles) can land in
  the file in **inverted order** (observed: an SSTable whose index preceded
  its data block, the long-standing `lsm_concurrent` flake), and a following
  `sync()` (a different fd) can fsync before the buffered write reaches the
  page cache. `read_at(file, offset, len)` /
  `size(file)` / `remove(file)` are the random-access + delete primitives an
  on-disk LSM needs (SSTable block reads, file sizing, compaction cleanup); they
  view the same durable + buffered bytes as `read`, so a crash drops an un-synced
  tail consistently across all of them.
- `ProdEnv` is *not* covered by the simulation tests (it's the nondeterministic
  side). Don't add logic here that the deterministic path needs to share.
- `ProdEnv::shutdown()` aborts every task the env owns — its inbound-connection
  accept loop plus everything spawned through `Spawner::spawn` (so the env tracks
  spawned `AbortHandle`s). `animusd`'s `Node::shutdown` calls it on each of the
  node's three role envs to tear the node down and free its listener ports for a
  restart in the same runtime. Production-edge only; determinism is unaffected.
  **`abort()` only requests cancellation — it does not wait for the task (and
  the `TcpListener` it may own) to actually be dropped**, so a bare `shutdown`
  followed immediately by a same-address rebind can race this same process's
  own not-yet-unwound accept-loop task for the port; under `cargo test
  --workspace`-level CPU contention that race can lag long enough to flake a
  restart test even behind a generous retry bound (see the "abort() is a
  request, not a guarantee" entry in `docs/engineering-lessons.md`). Use
  **`ProdEnv::shutdown_and_wait()`** (async) instead when the caller needs the
  listener genuinely gone before proceeding — it aborts the same way, then
  polls `AbortHandle::is_finished()` (bounded) before returning.
  `animusd::Node::shutdown_graceful` uses the `Node`-level dual
  (`shutdown_and_wait`) for exactly this reason.
- **Metrics are additive and determinism-safe (ADR 0015).** `Env::metrics()` has
  a **default** returning a shared no-op `MetricsHandle`, so the supertrait is
  unchanged and every `E: Env` impl (`SimEnv` included) compiles untouched.
  Recording is a relaxed atomic add — no wall clock (a timestamped metric takes
  `Clock::now`), no I/O, no `HashMap` (a snapshot uses `BTreeMap`). `ProdEnv`
  overrides `metrics()` with a recording sink; a sim test that wants to *read*
  counters threads a recording handle into the component (e.g.
  `RaftNode::start_with_metrics`) rather than relying on the no-op default — so
  no change to `animus-sim` is needed to observe metrics. The storage engine
  follows the same pattern: `LsmEngine::open`/`open_with` forward
  `env.metrics()`, and the additive `LsmEngine::open_with_metrics` threads a
  recording handle in for a sim test. (The deleted AP data plane's
  `DataClient::with_metrics`/`serve_*_with_metrics` variants followed it too —
  gone with `animus-data`, ADR 0019.)
- **`Disk::list` is per-env and non-recursive** (ADR 0024): it enumerates only the
  files this handle's own `Disk` methods could open — production reads the env's
  data dir without descending into a sibling's `sib-<id>/` (that is the sibling's
  disk). It exists so a teardown path (drop-table GC) can find every file of a
  prefix-named component; deletion stays on the seam (`remove`), so teardown is
  sim-testable.
- `ProdEnv::shutdown_tasks()` (abort only the env's own tasks, leave shared
  resources alone) is **vestigial with zero callers** — it existed to tear down
  a single sibling without draining the shared listener pool. Prefer
  `shutdown()`; remove `shutdown_tasks` if you're cleaning up.

- **Multiplexed `(node, stream)` addressing (ADR 0026).** `Network` gained a
  second addressing axis so a node can host more than one protocol instance
  without minting a whole new `NodeId`: `send_stream`/`recv_stream` are the
  primitive methods every implementor provides; `send`/`recv` are **default**
  methods over `PRIMARY_STREAM` (`= 0`), so every call site that predates this
  axis — which is nearly everything — needs no change and behaves identically.
  `SimEnv` re-keys its inbox `BTreeMap` from `NodeId` to `(NodeId, u64)` (no new
  RNG draw or timeline event, so determinism is unaffected). `ProdEnv` demuxes
  by a `Demux` (`BTreeMap<u64, VecDeque<Envelope>>` + per-stream `Waker`s)
  behind one `Arc<StdMutex<_>>` per env, fed by a background pump task that
  drains the accept loop's raw frames (now `[from][stream][len][payload]`,
  the `stream` field ADR 0026 added) and routes each into its stream's queue,
  waking a parked `recv_stream(stream)`. The staged plan in the ADR completed:
  the per-tablet CP Raft groups (`animus-cp-data`) migrated onto streams
  (Stage B), which is what made `Coresident`/the sibling pool vestigial.
  This is the same "additive default over a well-known constant" shape the
  metrics seam (`Env::metrics()`) uses — extend the trait so nothing existing
  has to change, not by widening every implementor's required surface.

## Tests

The seam is exercised end-to-end through `animus-sim` (`cargo test -p
animus-sim`). One `ProdEnv` unit test (`prod::tests`, real temp dir) asserts a
nested `"sub/dir/file"` `append`+`sync`+`read` round-trips — i.e. the disk
creates parent directories. `cargo test -p animus-env`. `metrics.rs::tests`
cover incr/snapshot round-trips, that clones share one sink, and that the text
export is stable + ordered.
