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
  `Envelope`, `BoxFuture`, and the `EnvExt::spawn_task` convenience.
- `prod.rs` — `ProdEnv`: real monotonic clock, `OsRng`, `tokio::spawn`,
  length-prefixed TCP, `tokio::fs` + `fsync`. Owns a real recording metrics sink
  and exposes `metrics_text()` (ADR 0015).
- `metrics.rs` — the **observability seam** (ADR 0015): a closed `Metric` enum
  (`control_*` Raft + `data_*` leaderless-AP + `storage_*` LSM-engine counters), a fixed-array lock-free
  `MetricSink`, the cheap-to-clone `MetricsHandle`, and the `MetricSnapshot` text
  export. The enum is **append-only**: add new variants *after* the existing ones
  (and a matching row in `Metric::ALL`) so slots and the export order stay stable
  and the snapshot remains byte-reproducible.

## What's non-obvious

- `Env` is a *supertrait*, not a bag of accessors: a handle **is** a `Clock` +
  `Rng` + `Network` + `Disk` + `Spawner`. Callers write `env.now()`,
  `env.send(..)`, `env.recv()` directly. Because components are `<E: Env>`, the
  supertrait's methods are in scope from the bound — you do **not** need to
  `use animus_env::Clock` etc. in generic code (doing so trips an unused-import
  warning).
- `Network::send` is fire-and-forget (no delivery result); `recv` is
  **single-consumer per node** — never run two receive loops on one `NodeId`.
- **`Coresident` (ADR 0017 D) is a *sub-trait*, not part of `Env`.** It adds one
  method — `sibling(&self, id) -> Self`, a fresh handle on the same physical node
  bound to a different `NodeId` (its own inbox) — so a node can host a *second*
  protocol instance (the new tablet's Raft group after a split) by minting its id
  **in band**, instead of the harness/bootstrap pre-allocating every id. It is
  deliberately separate from the `Env` supertrait: only the co-residency-aware
  split path bounds on it, so every other `E: Env` is unaffected and an env that
  can't multiplex inboxes (a transport keyed by one address) simply isn't
  `Coresident`. `SimEnv` implements it (trivially — `Simulator::env` already mints
  inboxes lazily); **`ProdEnv` implements it too now** (ADR 0017 #3b) via a
  **pre-bound listener pool**: `bind_with_pool(node_id, listen, pool_listens, dir)`
  binds the main listener plus one spare listener per `pool_listens` addr (each its
  own accept loop + inbox), and `sibling(id)` hands one out **synchronously** —
  binding a socket is `async`/fallible but the trait method is sync/infallible, so
  the listeners are pre-bound. A sibling shares the parent's **peer book** (`Arc`,
  so a later `set_peers` reaches it) and the pool, but gets its own inbox, id, and
  data dir (`<dir>/sib-<id>`); the pool size **bounds** co-resident groups —
  exhausting it **panics** the (background) split-hook task, so the over-cap
  tablet's group is never minted and that tablet ends up **leaderless** (writes to
  its range then hang). Size the pool generously for the workload (`animusd` uses
  `CP_SIBLING_POOL = 64`); a truly unbounded fix needs an `async`/fallible
  `sibling` that binds on demand. The caller publishes the sibling's `local_addr()` for
  address distribution — which (carrying group-replica addrs in replicated
  `Metadata` + a per-node `set_peers` sync loop) is the remaining 3b plumbing.
- `Disk` is append + explicit `sync`; bytes are not durable until `sync`
  returns. This models real crash semantics and is what `animus-sim` exploits.
  `Disk::replace` atomically swaps a file's whole contents (temp-file + rename
  in `ProdEnv`) — used for WAL compaction. In `ProdEnv`, the file-creating paths
  (`append`/`replace`) `create_dir_all` the file's parent first, so a filename
  carrying a subdirectory prefix (e.g. `"db/wal"`) works instead of silently
  failing on a missing parent. `read_at(file, offset, len)` /
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
- **Metrics are additive and determinism-safe (ADR 0015).** `Env::metrics()` has
  a **default** returning a shared no-op `MetricsHandle`, so the supertrait is
  unchanged and every `E: Env` impl (`SimEnv` included) compiles untouched.
  Recording is a relaxed atomic add — no wall clock (a timestamped metric takes
  `Clock::now`), no I/O, no `HashMap` (a snapshot uses `BTreeMap`). `ProdEnv`
  overrides `metrics()` with a recording sink; a sim test that wants to *read*
  counters threads a recording handle into the component (e.g.
  `RaftNode::start_with_metrics`) rather than relying on the no-op default — so
  no change to `animus-sim` is needed to observe metrics. The data plane follows
  the same pattern: the `DataClient` coordinator defaults to `env.metrics()` and
  takes an explicit handle via `DataClient::with_metrics`; the background loops
  have additive `serve_anti_entropy_with_metrics` / `serve_hint_*_with_metrics`
  variants (the originals forward `env.metrics()`). The storage engine follows it
  too: `LsmEngine::open`/`open_with` forward `env.metrics()`, and the additive
  `LsmEngine::open_with_metrics` threads a recording handle in for a sim test.
- **`Disk::list` is per-env and non-recursive** (ADR 0024): it enumerates only the
  files this handle's own `Disk` methods could open — production reads the env's
  data dir without descending into a sibling's `sib-<id>/` (that is the sibling's
  disk). It exists so a teardown path (drop-table GC) can find every file of a
  prefix-named component; deletion stays on the seam (`remove`), so teardown is
  sim-testable.
- **Tearing down a single sibling: `ProdEnv::shutdown_tasks()`, never
  `shutdown()`.** A sibling shares its parent's listener **pool** (`Arc`), and
  `shutdown()` drains the pool and aborts the unclaimed slots' accept loops —
  killing the spare inboxes every *future* split on that node needs.
  `shutdown_tasks()` aborts only the env's own tasks (its accept loop + everything
  it spawned) and leaves the pool alone. The claimed slot is not returned to the
  pool (slots are single-use by design); `CP_SIBLING_POOL` sizing accounts for it.

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
  waking a parked `recv_stream(stream)`. A `Coresident::sibling` gets its own
  `Demux` + pump (its inbox is genuinely separate); this is orthogonal to (and
  does not yet replace) `Coresident` — see the ADR for the staged plan to
  eventually retire the sibling pool once a real consumer (the per-tablet CP
  Raft group after a split) migrates onto a stream instead of a minted `NodeId`.
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
