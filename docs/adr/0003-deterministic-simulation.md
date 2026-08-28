# ADR 0003 — Deterministic simulation testing and the `Env` seam

- **Status:** Accepted
- **2026-08-19 note:** [ADR 0051](0051-dynamodb-ttl.md) (accepted, implemented)
  adds `Clock::wall_now()` — the first **calendar-time** reading in the
  codebase, admitted for DynamoDB TTL, whose timestamps are absolute epoch
  seconds chosen by the client and so cannot be interpreted by any monotonic
  reading. This does not weaken the guarantee below: under `SimEnv` the wall
  clock is a fixed epoch base plus elapsed *virtual* time (plus the existing
  per-node `set_clock_skew_for` offset), so it stays a pure function of the
  run's seed. It is a derived reading **inside** the seam, not a hole in it.
  `Nanos` remains the only clock for timing — deadlines, timeouts, elections,
  backoff — and the two types are deliberately not interconvertible; see ADR
  0051 §1.
- **2026-08-28 note:** [ADR 0061](0061-testability-node-crate-simulator.md)
  rung B2/B3 extends the `SimEnv` fault vocabulary the "Known fidelity
  limits" section below used to list as gaps: per-node/per-link `NetConfig`
  overrides, message duplication, wire-payload corruption, heavy-tailed
  delay, an ENOSPC-distinguishable disk `ErrorKind`, clock drift (on top of
  ADR 0018 §2's static skew), fsync-acked-but-lost, and a `pause` (alive-but-
  frozen) primitive. Every new knob is opt-in and default-off, so this does
  not weaken the guarantee below — see `crates/animus-sim/CLAUDE.md` for the
  full contract of each.
- **2026-08-28 note (2):** [ADR 0061](0061-testability-node-crate-simulator.md)
  rung B4 builds the failure-minimization facility this ADR's own
  Consequences promised and the "Known fidelity limits" section below used to
  list as still-missing: `animus_test::shrink`, opt-in via `ANIMUS_SHRINK=1`,
  default off. It minimizes a failing scenario's own **parameters**, seed
  held fixed — not the seed itself, which is opaque by design and has no
  "smaller" — see the amended limits entry below and
  `crates/animus-test/src/shrink.rs`'s module doc for why.
- **2026-08-28 note (3):** [ADR 0061](0061-testability-node-crate-simulator.md)
  rung B5 closes the enforcement gap the "This is enforced in review and by
  `clippy.toml`" line below used to describe accurately only for
  `HashMap`/`HashSet`: `clippy.toml` now also carries `disallowed-methods`
  for `Instant::now`/`SystemTime::now`/`tokio::spawn`/
  `tokio::time::{sleep,timeout}`/`thread_rng`/`OsRng`, workspace-wide. The
  crates this ADR's seam actually targets (`animus-control`, `animus-cp-data`,
  `animus-storage`, `animus-tablet`, `animus-placement`, `animus-dynamo`,
  `animus-sim`, `animus-test`) were already clean in their non-test code —
  the discipline had held by review — so the lint mostly formalizes an
  existing fact rather than surfacing violations; the handful of genuine
  exceptions (real-thread `ProdEnv` liveness tests, `animus-env`'s own
  `ProdEnv`, `animus-cli`/`animus-operator`'s real-socket process boundaries)
  carry individually-justified `#[allow(clippy::disallowed_methods, reason =
  "...")]`s. **`animusd` is the one deliberate exception**: it is the
  pre-Phase-C process boundary this whole crate-split ADR exists to carve
  down, with ~600 real call sites across 84 files — the lint is disabled at
  the package level there (`crates/animusd/Cargo.toml`'s own `[lints.clippy]`
  comment explains why), tracked as intentional debt until Phase C's
  `animus-node` extraction gives that Env-generic core the workspace default
  back. See rung B5's own delivery note for the full per-crate count.
- **Date:** 2026-08-01

## Context

Distributed-systems bugs hide in rare interleavings of message reordering,
partitions, crashes, and clock skew. Such interleavings are nearly impossible to
reproduce with real networks and wall clocks, so a bug seen once in CI may never
be seen again. The state of the art (FoundationDB, TigerBeetle) is to make the
*entire* system run on top of a single deterministic substrate so that any run
is byte-for-byte reproducible from a seed.

## Decision

All nondeterminism flows through a single `Env` seam — a set of traits
(`Clock`, `Rng`, `Network`, `Disk`, `Spawner`) combined into an `Env`
supertrait. Components are **generic over `E: Env`** (monomorphized, not `dyn`),
so the same code runs in production and under simulation with no branches.

- `ProdEnv` provides real time, `tokio` task spawning, TCP, real `fsync`, and
  OS randomness.
- `SimEnv` (crate `animus-sim`) provides a virtual clock, a seeded ChaCha RNG, an
  in-memory network with controllable delay/drop/reorder/partition, a fake disk
  that distinguishes synced from un-synced bytes (a "crash" drops un-synced
  bytes), and a cooperative single-threaded run-queue.

### Known fidelity limits (audit, 2026-08-06; refreshed 2026-08-28, ADR 0061 rungs B2/B3/B4)

The seam is honoured throughout, but both envs are weaker than the paragraph
above reads, and the gaps are exactly where prod-only bugs have already hidden:

- **`ProdEnv` durability:** `sync` is a file-fd `sync_all` only — neither
  `append`'s file *creation* nor `replace`'s *rename* fsyncs the parent
  **directory**, so a just-created WAL segment or a completed manifest swap can
  be lost by a power crash even after `sync`/`replace` returned. **Fixed in
  PR #27** (directory-fsync chain on first sync / after rename), which also
  root-caused a worse latent bug the audit missed: `append` dropped its
  `tokio::fs::File` without `flush().await`, so a write could still sit in
  tokio's user-space buffer when a later `sync` (a different fd) fsynced —
  and two sequential appends via separate handles could land **inverted** on
  disk (the long-standing `lsm_concurrent` flake; independently found and
  fixed in PRs #26 and #27).
- **`SimEnv` disk faults — closed, then extended.** The original gap (the sim
  disk never returned an error, never left a *partial* (torn) tail on crash,
  and could not corrupt a byte) was **closed in PR #24**: opt-in, seed-driven
  `DiskConfig` (error injection, torn-tail-on-crash, corruption; default-off,
  byte-identical traces) — whose first run found two real WAL data-loss bugs
  (see ADR 0008), proving the gap was load-bearing. **ADR 0061 rung B3
  extends it further**: `DiskConfig::set_enospc_prob` gives the injected
  error an `ErrorKind::StorageFull` so a caller that branches on `ErrorKind`
  (as production code must, to tell disk-full apart from a generic failure)
  is exercisable under simulation — sharing one roll with the pre-existing
  generic `error_prob` (see `animus-sim/CLAUDE.md`'s "one roll, two buckets"
  note), and `DiskConfig::set_fsync_lie_prob` models a **fsync that acks
  and still loses the write on power loss** (`sync` returns `Ok` but the
  bytes are left exposed to a following crash exactly like an un-synced
  tail) — a real filesystem behavior `sync`'s prior all-or-nothing contract
  couldn't represent at all. Both default off, byte-identical traces.
- **Network — closed, ADR 0061 rung B2/B3.** The delay/drop model was a
  single global `NetConfig` with no per-node or per-link override (disk had
  one, network didn't); reordering was only emergent from per-message
  jitter, with no explicit duplication or corruption knob, and every delay
  draw came from one uniform distribution (no way to model an occasional
  much-slower message without raising the delay for every message). All
  closed: `Simulator::set_net_config_for`/`set_link_net_config` (mirroring
  `set_disk_config_for`'s shape, link beats per-node beats global — see
  `NetConfig`'s own doc for the exact resolution order and the node a link
  override keys on), `NetConfig::set_duplicate_prob` (a surviving message is
  re-delivered with its own independently-drawn delay — see the type's doc
  for why independent rather than simultaneous), `NetConfig::set_corrupt_prob`
  (the network analogue of the disk's at-rest corruption: one payload byte
  bit-flipped in transit), and `NetConfig::set_heavy_tail_prob` +
  `heavy_tail_max_jitter` (an occasional message drawn from a much wider
  jitter ceiling). All default off, and — same discipline as `DiskConfig` —
  every extra roll is gated on its own threshold being non-zero, so an
  unconfigured `NetConfig` draws exactly the pre-existing two-draw sequence
  (drop, then jitter) in the same order.
- **Per-node clock skew and drift — closed, ADR 0018 §2 / ADR 0061 rung
  B3.** All nodes sharing one virtual clock, with no per-node divergence
  modeled at all, was listed here as a target bug class; it is now closed
  two ways. `Simulator::set_clock_skew_for(node, skew_nanos)` (ADR 0018 §2)
  gives a node a static signed-nanosecond offset on its own `Clock::now()`/
  `wall_now()` reads. `Simulator::set_clock_drift_for(node, drift_ppm)`
  (ADR 0061 rung B3) layers a *progressively widening* component on top,
  proportional to elapsed virtual time since the call. Both are **read-side
  only, by design, not by omission**: `Clock::sleep`'s timers still fire
  against the single shared `(time, seq)` timeline — a per-node skewed or
  drifting *timeline* would let nodes' timers interleave in a skew-dependent
  order, reordering the shared event loop and breaking the single-timeline
  determinism story this crate provides. Skew/drift model a node's clock
  *reading* wrong (exactly what an HLC's physical component has to be
  robust to, ADR 0018), never a different flow of time for that node — see
  `animus-sim/CLAUDE.md` for the full contract and `hlc_skew.rs` for the
  causality property this exists to support.
- **Alive-but-frozen nodes — closed, ADR 0061 rung B3.** `crash` (drops
  volatile state) and `stop` (removes tasks) were the only ways to model a
  misbehaving node; there was no way to model one that is neither — alive,
  with its state fully intact, just not making progress for a while (a GC
  pause, a cgroup throttle, a VM stall). `Simulator::pause(node, dur)`
  closes this: no timer the node owns fires and no message it sends leaves
  before the resume instant, and a message addressed to it queues (deferred,
  not dropped) until then — see `animus-sim/CLAUDE.md` for the defer
  mechanism (it reuses the existing timeline rather than adding a second
  queue) and `tests/pause.rs`.
- **Threading:** the sim is single-threaded and cooperative, so it proves logic
  and ordering, never real-thread liveness — any concurrency primitive needs a
  timeout-guarded `multi_thread` test over `ProdEnv` (see the root `CLAUDE.md`
  practice entry; found via the WAL group-commit deadlock). Still open —
  nothing in ADR 0061 changes this boundary; it is deliberate (see the ADR's
  own Consequences).
- **Shrinking/minimization — closed for scenario parameters, ADR 0061 rung
  B4.** This ADR's own Consequences promised "shrinking a failure to a
  minimal seed" from the start; that phrase was never literally buildable —
  a `SimEnv` run is a pure function of an *opaque* seed (ADR 0003's whole
  point), so no seed is "smaller" than another and there is nothing to
  shrink *to*. What rung B4 built instead, `animus_test::shrink`, is
  **scenario-parameter minimization**: given a failing named scenario, hold
  its seed fixed and delta-debug its own explicit parameters (fault
  schedule, round/client/keyspace counts, outage windows — whatever the
  corpus's own `Scenario` type exposes) down to a locally-minimal
  reproducing case, opt-in via `ANIMUS_SHRINK=1`, default off, iteration-
  budget-bounded, and itself deterministic (same failing input always
  reduces to the same minimized output). **Deliberately not built**: fault-
  *schedule* minimization (suppressing one specific injected fault — one
  dropped message, say — out of an ambient `NetConfig`/`DiskConfig`
  probability, as opposed to one whole scheduled `Scenario` fault entry)
  needs a recorded-schedule replay mode so that suppressing one fault
  decision doesn't perturb every RNG draw after it, which touches the same
  RNG-draw-order machinery this file's byte-identical-trace guarantee
  depends on — deferred rather than risking that guarantee for a half-
  working version. See `crates/animus-test/CLAUDE.md`'s shrink section and
  `crates/animus-test/src/shrink.rs`'s module doc for the full account.

System code must never call `std::time::*`, spawn raw tasks, touch real
sockets/disk, use unseeded RNG, or iterate a `HashMap`/`HashSet` (use
`BTreeMap`/`BTreeSet`). **Lint-enforced (`clippy.toml`) as of ADR 0061 rung
B5**, not review-only: `disallowed-types` catches `HashMap`/`HashSet`/`OsRng`;
`disallowed-methods` catches `Instant::now`/`SystemTime::now`/`tokio::spawn`/
`tokio::time::{sleep,timeout}`/`thread_rng`. Raw socket/disk I/O
(`std::fs`/`std::net`/`tokio::fs`/`tokio::net`) is **not** lint-enforced —
rung B5 judged it impractical (no single small replacement to name in a
`reason` string, and `animusd`'s listener binding alone would need dozens of
individually-meaningless allows) and left it review-only; see the ADR's own
Decision 4 delivery note. Every exception to the two enforced lints carries
an individually-justified `#[allow(clippy::disallowed_{methods,types}, reason
= "...")]`, except `animusd`, exempted at the package level as documented
debt (see the 2026-08-28 note (3) above). A failing simulation run prints
its seed for one-command replay.

## Consequences

- Every distributed behavior gets a reproducible, fault-injecting test, and
  (ADR 0061 rung B4; see the amendment above for why "minimal seed" was never
  literally the right framing) a failing scenario's own parameters can be
  automatically minimized to a small reproducing case, holding its seed fixed.
- There is an upfront cost: the `Env` seam must be designed carefully and all
  subsystems must be written against it from day one. Retrofitting is expensive,
  so we pay this cost first (milestone M1).
- We forgo some convenient APIs (wall clock, `tokio::spawn`, `HashMap` iteration)
  in system code.
- The seam is designed so a future move to `madsim` is a drop-in replacement of
  the simulation backend, not a rewrite of the system code.
