# ADR 0015 — Deterministic-safe observability seam (metrics)

- **Status:** Accepted
- **Date:** 2026-08-02

## Context

AnimusDB has rich internal behavior — leader elections, log replication,
chunked snapshot installs, heartbeat-driven failure detection (ADR 0012) — but
no way to *observe* it at runtime. An operator running `animusd` cannot tell how
often elections happen, whether a follower is rejecting `AppendEntries`, whether
snapshots are being shipped, or how often the failure detector flips a member
`Down`/`Active`.

The hard constraint is determinism (ADR 0003): **all** nondeterminism flows
through the `Env` seam, and the deterministic simulator is the source of truth
for correctness. A naive metrics layer breaks this in three classic ways — it
reads the wall clock to timestamp samples, it keys counters by string in a
`HashMap` (nondeterministic iteration order), and it does its own I/O (a
`/metrics` HTTP handler, a background flush). Any of those would make a sim run
no longer a pure function of its seed, or leak `HashMap` ordering into logic.

We also must not disturb the carefully-built `Env` supertrait
(`Clock + Rng + Network + Disk + Spawner`) or any of the many components generic
over `E: Env`: adding a required method would force a change to every `Env` impl
(`SimEnv`, `ProdEnv`) and ripple through call sites.

## Decision

Add a **minimal, deterministic-safe metrics seam** in `animus-env`
(`metrics.rs`), wired into the control-plane Raft driver, and exposed from
`ProdEnv` as structured text. The data-plane live endpoint is wired at
integration; the seam itself does **no** HTTP.

- **Closed `Metric` enum + fixed atomic array.** The recording sink
  (`MetricSink`) is a fixed-size array of `AtomicU64` counters (one slot per
  `Metric` variant) plus one `AtomicI64` leadership gauge. Recording is a single
  relaxed atomic add — lock-free, allocation-free, no map lookup. A closed enum
  (vs. arbitrary string keys) keeps recording O(1) and makes the exported metric
  names a small, reviewable, stable surface. **No `HashMap`/`HashSet`**; a
  snapshot collects into a `BTreeMap`, so export order is deterministic.

- **No wall clock, no I/O, no randomness in the seam.** Recording touches only
  atomics; it never reads time. Any metric that needs a timestamp takes one
  derived from `Clock::now` (the `Env` clock), never `std::time`. Export
  (`MetricSnapshot::to_text`) is a pure read rendered as stable `name value`
  lines with **no embedded timestamp** (a scrape adds its own), so equal
  snapshots are byte-identical — which makes "same seed ⇒ same metrics" testable.

- **Additive `Env::metrics`, with a no-op default.** `Env` gains one method,
  `fn metrics(&self) -> MetricsHandle`, **with a default** that returns a shared
  no-op handle. Every existing `Env` impl keeps compiling and behaving
  identically with no change — `SimEnv` included (it does not override it). The
  supertrait set is untouched. Returning a handle (not `Option`) means recording
  sites need no `if let Some(..)` guard: the no-op handle is a real sink that is
  recorded into and simply never read.

- **`ProdEnv` records for real; `start_with_metrics` for sim observability.**
  `ProdEnv` overrides `metrics()` to return its own recording handle, so an
  assembled production node accumulates counters with no extra wiring, and
  `ProdEnv::metrics_text()` renders the current snapshot. Because `SimEnv` uses
  the no-op default, a *test* that wants to read counters constructs a recording
  `MetricsHandle` and threads it into the component via the additive
  `RaftNode::start_with_metrics(env, nodes, handle)` — so the deterministic suite
  observes metrics **without changing `animus-sim` at all**. `RaftNode::start`
  forwards `env.metrics()`, so production keeps recording into the env's sink.

- **What the control plane records.** The `RaftNode` driver loops record, all
  from `Env`-supplied or core-derived inputs (so recording is a deterministic
  function of the run): `elections_started`/`elections_won` (from role/term
  transitions: candidate at a higher term, then entering `Leader`); a leadership
  **gauge** (a *level*, not an event); `append_entries_sent` and
  `append_entries_rejected` (read off the messages the core emits — a rejection
  surfaces as an outbound `AppendEntriesResp { success: false }`);
  `snapshot_installs` (a completed follower-side `InstallSnapshotResp`); and
  `failure_detector_down`/`failure_detector_up` (the `Active`↔`Down` edges the
  `detect_loop` proposes, ADR 0012). Names are `control_*`-prefixed.

## Consequences

- The control plane is **observable** at runtime with zero determinism risk:
  every counter is exercised under the deterministic simulator and asserted to
  move under known events (`animus-control/tests/metrics.rs` — a forced election
  bumps `elections_started`/`elections_won` and the gauge; a crashed
  heartbeating member bumps `failure_detector_down`, its recovery bumps
  `failure_detector_up`), and the recorded snapshot is asserted byte-identical
  across two runs of the same seed.
- The seam is **additive and supertrait-preserving**: no `Env` impl or `E: Env`
  call site needed changing; `SimEnv` is untouched.
- The export is **timeless structured text** (`name value` lines). It is not
  Prometheus exposition format, but is trivially adaptable to it; choosing a
  timeless, dependency-free format keeps the seam pure and avoids pulling a
  metrics crate into the determinism-critical core.
- **Integration is the only remaining wiring.** `animusd` exposes the data
  surface by serving `ProdEnv::metrics_text()` (or a per-control-node
  `RaftNode::metrics().snapshot().to_text()`) from an HTTP `/metrics` handler.
  The exact one-line hook is documented in `docs/getting-started.md`; no HTTP
  lives in `animus-env`.
- **Deferred:** data-plane and storage-engine counters (read/write quorums,
  read-repair, anti-entropy, LSM compactions); histograms/latency (would need a
  deterministic bucketing scheme); and the live `/metrics` HTTP endpoint in
  `animusd` (the seam and the text export are ready for it).
