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
  length-prefixed TCP, `tokio::fs` + `fsync`.

## What's non-obvious

- `Env` is a *supertrait*, not a bag of accessors: a handle **is** a `Clock` +
  `Rng` + `Network` + `Disk` + `Spawner`. Callers write `env.now()`,
  `env.send(..)`, `env.recv()` directly. Because components are `<E: Env>`, the
  supertrait's methods are in scope from the bound — you do **not** need to
  `use animus_env::Clock` etc. in generic code (doing so trips an unused-import
  warning).
- `Network::send` is fire-and-forget (no delivery result); `recv` is
  **single-consumer per node** — never run two receive loops on one `NodeId`.
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

## Tests

The seam is exercised end-to-end through `animus-sim` (`cargo test -p
animus-sim`). One `ProdEnv` unit test (`prod::tests`, real temp dir) asserts a
nested `"sub/dir/file"` `append`+`sync`+`read` round-trips — i.e. the disk
creates parent directories. `cargo test -p animus-env`.
