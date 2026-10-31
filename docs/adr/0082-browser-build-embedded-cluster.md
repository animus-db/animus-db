# ADR 0082 — Browser build: an `embedded` cluster on a virtual clock driven from JavaScript, not a real-time `WebEnv`

- **Status:** Accepted (2026-10-30), implemented the same day on branch `feat/wasm-browser-build`.
- **Date:** 2026-10-30
- **Origin:** the maintainer goal "run AnimusDB in a web browser from the project website".
- **Amends:** none. **Cross-references:** ADR 0003 (determinism, the `Env` seam), ADR 0061 (the `E: Env`-generic node brain; its 2026-10-30 amendment points here).
- **Not in scope:** persistence in the browser (IndexedDB), a real-time `WebEnv`, networking between tabs, any change to a wire or on-disk format (none is touched).

## Context

The website is documentation, and the most direct way to document a database is to let a reader use one. AnimusDB's request path is already generic over the `Env` seam (`ClientCtx<E, R>`, `dispatch_item_op`, `execute_item_op_as`; ADR 0061), and `SimCluster` (ADR 0061 rung D1) is a whole multi-node cluster on one deterministic `animus_sim::Simulator`: a real multi-voter control-plane Raft quorum, a real Raft group per tablet replica, a per-node tablet-host reconciler, and the same generic request path the production DynamoDB edge uses. Nothing in that stack needs a socket, a thread, a disk or a wall clock. What kept it from `wasm32-unknown-unknown` was only that `animusd` was one crate that also contained the real process (sockets, TLS, `ProdEnv`, OpenTelemetry, signals) and that `SimCluster` was `cfg(test)`.

## Decision

1. **Two cargo features on `animusd`.** `server` (default on) holds everything that is the real process; `embedded` compiles the `SimCluster` fixture outside `cfg(test)` and exposes it as `animusd::embedded::EmbeddedCluster`. `cargo build -p animusd` is unchanged. The split gates whole items by `#[cfg(feature = "server")]`; the generic brain is not rewritten (`docs/lessons/code-patterns/2026-10-30-gate-a-process-boundary-by-feature-not-by-rewriting-the-generic-core.md`).
2. **Embed `SimCluster`; the page drives its virtual clock.** `EmbeddedCluster` exposes `submit`/`poll`/`advance`/`crash`/`restart`/`partition`/`heal_all`/`grow`/`status`. A new crate `animus-web` (`cdylib` + `rlib`) wraps it in a plain-Rust core (the request pump, native-tested) and a thin `wasm-bindgen` shell, the JS class `AnimusCluster`. JavaScript measures real elapsed milliseconds in a `requestAnimationFrame` loop and calls `tick(realMs, speed)`; the cluster advances `realMs * speed` of **virtual** time. Real time never enters Rust.
3. **Single-threaded, in memory.** The wasm build has no threads, sockets or disk. Reloading the tab discards the cluster. Requests complete through promises settled by the pump; a request unanswered after 30 virtual seconds resolves with 504, and a request to a crashed node is refused with 503.
4. **Shipping.** `scripts/build-web-demo.sh` builds the `wasm-release` profile (`opt-level = "s"`, fat LTO), runs a `wasm-bindgen-cli` pinned to the `wasm-bindgen` version in `Cargo.lock`, and writes `website/demo/pkg/` (gitignored). `pages.yml` runs it before uploading `website/`. The wasm is loaded lazily when the visitor clicks "Start cluster". Measured 2026-10-30: 5.7 MB raw, 1.68 MB gzipped.

## Why not a real-time `WebEnv`

A `WebEnv` (an `Env` over `setTimeout`/`fetch`/`WebSocket`, so a node could be a real peer of a server cluster) is possible in principle: the generic brain is the same code. It was rejected for this goal for three reasons.

- **The `Env` contract is `Send + Sync`; JS futures are not.** `wasm-bindgen-futures` `JsFuture` is `!Send`, so an `Env` whose `sleep`/`spawn` are built on browser primitives cannot satisfy the trait bounds without unsafe `Send` wrappers (and this workspace forbids `unsafe`). Loosening the trait for all implementors would be a workspace-wide change for one consumer.
- **Browsers throttle background timers.** A tab in the background gets `setTimeout` clamped to one second or worse. Raft's election and heartbeat timers on a real clock would then flap: a leader that cannot heartbeat for seconds is deposed, and the cluster churns while nobody is looking. Virtual time simply pauses with the tab.
- **Determinism.** A virtual clock makes a run a pure function of its seed and the calls made on it (ADR 0003): bugs seen in the browser replay natively, and the demo exercises the code the project's tests exercise rather than a second environment implementation with its own timing behaviour.

## What is gated and what is not served

Gated off the browser build (`server`): `Node`/`BoundNode` and the `run_node*`/`start_cluster*` entry points, the TCP serve loops and TLS of the DynamoDB/admin/console edges, `ProdEnv`, `FsSegmentStore`, `socket2`, `tokio-rustls`, OpenTelemetry/tracing-subscriber, tokio `net`/`fs`/`signal`/`rt-multi-thread`/`time`, the `animusd` binary, and the real-store variants of the segment/backup store handles. CI asserts `socket2`, `tokio-rustls` and `opentelemetry` are absent from `animus-web`'s wasm dependency graph.

Served in the browser: item operations (including `Batch*`, `Transact*`, PartiQL), `CreateTable`/`DeleteTable`/`ListTables`/`DescribeTable`/`UpdateTable` throughput, the four DynamoDB Streams reads, and global secondary indexes (the embedded cluster runs the per-node change-consumer loop). Not served, answering the same "unsupported by generic dispatch" error `SimCluster` returns: on-demand backup, export, import, restore, tagging, and `UpdateTable` that adds or removes an index or a stream.

## Limits and risks

- **Size.** About 1.7 MB gzipped is a real download, hence the lazy load. `wasm-opt -Oz` was measured and rejected: 13% smaller raw, about 10% larger gzipped.
- **Embedded API is panic-sensitive.** A panic in the wasm instance aborts it. `EmbeddedCluster::grow` refuses (an error, not a panic) while a node is down or a partition is in place, because the fixture waits for every node to see the new member. The shell installs a panic hook that writes to `console.error`, and the page asks for a reload after one.
- **A hand-driven session is not byte-reproducible**, because real frame timing feeds `tick`. The same seed builds the same cluster, and the same sequence of calls and steps gives the same bytes (native tests assert this).
- **`--features embedded` tests are not part of the default `gates` nextest run**; a CI job (`wasm`) runs them, with the wasm checks.

## Future: a `WebEnv`

If a browser node that joins a real cluster is ever wanted, the path is a `WebEnv` as another instantiation of the same generic `ClientCtx<E, R>` (as `SimEnv` is), plus a relaxation or adapter for the `Send + Sync` bound, a clock policy for hidden tabs (for example, stepping a virtual clock from the page and treating the tab as a non-voting learner), and a WebSocket or fetch transport. None of that blocks or is blocked by this decision.
