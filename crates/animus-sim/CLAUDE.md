# CLAUDE.md — animus-sim

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The deterministic simulator: a seeded `SimEnv` implementing the `animus-env`
seam, plus a `Simulator` that drives an entire distributed run as a pure
function of one seed. This is the substrate every distributed test runs on.

## Entry points

- `Simulator::new(seed)`, `sim.env(node_id) -> SimEnv`.
- Driving: `run()` (to quiescence), `run_for(dur)` / `run_until(deadline)`
  (bounded virtual time), `run_until_quiescent(max_steps)`.
- Faults: `partition`/`partition_pair`/`heal`, `crash`/`restart`, `stop`
  (process exit), `pause(node, dur)` (alive but frozen — see below),
  `set_net_config(NetConfig)` / `set_net_config_for(node, ..)` (per-node,
  keyed on the **sender**) / `set_link_net_config(from, to, ..)` (per
  directed link, most specific — ADR 0061 rung B2, mirrors
  `set_disk_config_for`'s per-node shape one level further); `NetConfig`
  itself now carries delay/jitter, drop, **duplicate**
  (`set_duplicate_prob`: a surviving message is re-delivered with its own
  independent delay draw), **corrupt** (`set_corrupt_prob`: one payload byte
  bit-flipped), and a **heavy-tailed delay** option
  (`set_heavy_tail_prob` + `heavy_tail_max_jitter`: an occasional much
  slower message without raising the common-case delay) —
  `set_disk_config(DiskConfig)` / `set_disk_config_for(node, ..)` (disk faults:
  per-op injected `io::Error`s on `append`/`sync`/`read`/`read_at`/`replace`/
  `link` (ADR 0058 rung 2 — modelled as a snapshot copy into an independent
  map slot, no inode/directory concept, since that's indistinguishable from
  a real hard link for the trait's sanctioned use: sharing an already-fully-
  synced, never-mutated-in-place file), torn un-synced tails on crash, byte
  corruption of the torn region,
  `DiskConfig::set_sync_delay` — a fixed extra virtual-time latency on every
  `append`/`sync`, issue #279's slow-disk livelock repro; unlike the other
  knobs it draws no RNG, so it's a plain fixed cost, not a seed-sampled
  fault — `DiskConfig::set_enospc_prob` (ADR 0061 rung B3: an
  `ErrorKind::StorageFull` fault, distinguishable by a caller that branches
  on `ErrorKind`, sharing one roll with `set_error_prob`'s generic
  `ErrorKind::Other` — see `inject_disk_fault`'s doc for the bucket split),
  and `DiskConfig::set_fsync_lie_prob` (fsync-acked-but-lost: `sync` returns
  `Ok` but silently skips the buffered→durable move, so the bytes stay
  exposed to a following `crash` exactly like any other un-synced tail)),
  and `corrupt_durable(node, file, offset)` (flip one durable byte —
  at-rest corruption of synced data, e.g. to hit an SSTable's per-block CRC).
- Observability: `trace()` / `trace_lines()`, `now()`, `seed()`.
- Clock skew (ADR 0018 §2 sim support): `set_clock_skew_for(node, skew_nanos)`
  — a per-node signed-nanosecond offset applied only to that node's own
  `Clock::now()` reads (mirrors the `set_disk_config_for` per-node-override
  shape). **Opt-in and default-zero**: with no call, every node's `now()` is
  byte-identical to the global clock, so this changes nothing for any
  existing test. Clamped so a reading never underflows below 0 or overflows
  `u64::MAX`. Deliberately **read-side only** — `sleep`'s timers still fire
  against the single global timeline, since a per-node skewed *timeline*
  would reorder the shared event loop and break determinism; skew models a
  node's clock *reading* wrong (exactly what an HLC has to tolerate), not a
  different flow of time for that node. **Clock drift** (ADR 0061 rung B3):
  `set_clock_drift_for(node, drift_ppm)` layers a progressively widening
  component on top of any static skew — `drift_ppm * elapsed_nanos_since_
  the_call / 1_000_000`, added to `now()`/`wall_now()` reads. Same
  read-side-only limit as static skew (never the shared timer timeline), same
  default-empty/no-RNG/no-timeline-event contract.
- **Process pause** (`pause(node, dur)`, ADR 0061 rung B3): alive but frozen
  for `dur` of virtual time, then resumes on its own with full state intact —
  a GC pause / cgroup throttle / VM stall, distinct from `crash` (drops
  volatile state) and `stop` (removes tasks). While paused: no timer the node
  owns fires, no message it sends leaves, and no message addressed to it is
  delivered — all three are **deferred** (re-timelined to the resume instant,
  not dropped or cancelled), so the node "catches up" the instant it
  unfreezes. Deterministic: the resume deadline is computed from the current
  virtual clock, never drawn from the RNG; traced at the call site
  (`TraceEvent::Pause`).

## What's non-obvious

- **`SimSegmentStore` (`segment_store.rs`, ADR 0043 §A7) is `animus-env`'s
  `SegmentStore` seam's deterministic corpus impl** — a seeded,
  fault-injectable in-memory `BTreeMap<String, Vec<u8>>` store, built from a
  `SimEnv` handle (`SimSegmentStore::new(env)`) rather than pulled off the
  `Simulator` the way `sim.env(node)` hands out an `Env` handle, because
  `SegmentStore` is deliberately **not** part of the `Env` supertrait (F5) —
  a consumer threads the store explicitly, same as `StorageEngine`.
  Constructing it from *any* node's `SimEnv` handle is fine for determinism:
  every node's `Rng` draws off the same single `SimState`-shared stream (see
  `next_u64`'s `self.shared.lock().rng.next_u64()` above), so "which node's
  handle" never changes the draw sequence — only *whether* a draw happens at
  all changes it.
  - **Fault knobs mirror `DiskConfig`'s own discipline exactly**: `roll`
    draws RNG only when its threshold is non-zero, so a store with no
    configured `SegmentFaultConfig` perturbs neither the RNG stream nor any
    other test's determinism — a fault schedule is opt-in per test, just
    like `NetConfig`/`DiskConfig`.
  - **Ack-lost is the deliberate *opposite* of `DiskConfig`'s "no state
    change on a fault" rule.** `inject_disk_fault` guarantees a failed op
    changed nothing; `SimSegmentStore`'s `put_ack_lost`/`delete_ack_lost`
    faults do the op for real (the object lands / is removed) and *then*
    return an injected error — modeling the exact ambiguity ADR 0043 §A3's
    seal step must tolerate (a crash or network error between the store
    acking and the caller's proposal), not a clean failure. Don't reuse
    `inject_disk_fault`'s "no state change" shape for a fault whose whole
    point is that the state *did* change.
  - **Unavailability windows are a plain `Option<Nanos>` deadline compared
    against `Clock::now()`**, not a new timeline/RNG mechanism — no new
    `Simulator` machinery needed since `SegmentStore` isn't part of `Env`'s
    own event loop. `set_unavailable_until`/`clear_unavailable` are ordinary
    setters a test calls directly (no RNG draw: the deadline is
    caller-chosen, not sampled), and healing happens for free once virtual
    time (driven by `sim.run_for`/`run_until`, as always) passes it.
  - **Tests spawn the async workload via `env.spawn_task` and drive it with
    `Simulator::run_until_quiescent`/`run_for`, not `#[tokio::test]`** — this
    crate has no `tokio` dependency at all (async here is driven by the
    simulator's own cooperative executor). None of `SegmentStore`'s methods
    genuinely suspend, so a spawned task completes within the first drain
    under `run_until_quiescent`; a test that calls `Clock::sleep` (to check
    an unavailability window healing) needs `run_for(dur)` instead, bounded
    past the sleep. A panicking assertion inside the spawned block
    propagates out of the polling call exactly like any other panic (the
    simulator polls synchronously on the test's own thread), so no
    completion flag is needed to catch a task that silently never ran its
    assertions.

- **`Simulator` is `Clone`** (added for ADR 0031 PR5's reconciler corpus): it
  hands out another handle to the SAME shared world (clones the inner `Arc`),
  exactly like `SimEnv`'s own `Clone`, not a fork. This is what lets a test's
  spawned "driver" task carry its own `Simulator` handle to call the `&self`
  fault-injection methods (`stop`/`crash`/`partition_pair`/`heal`/`env`) from
  *inside* an async scenario script, while the outer synchronous test code
  keeps its own handle to drive `run_for`/`run_until` (the only `&mut self`
  methods — no field either handle touches is exclusive to one of them).
- **`run()` never returns for protocols with perpetual timers** (Raft
  heartbeats). Use `run_for`/`run_until` whenever the control plane is involved.
- The core is a single shared `SimState` behind a `Mutex`, plus a custom
  waker-driven executor: the loop drains ready tasks, then fires the earliest
  event on a unified `(time, seq)` timeline (timers + message deliveries). The
  `seq` tiebreaker is what makes ordering total and reproducible.
- A future is *checked out* of the task map while polled so it can re-enter the
  state lock (e.g. via `env.send`) without deadlocking. Wakers hold a `Weak` to
  avoid a reference cycle.
- `crash(node)` drops un-synced disk + the inbox **and mutes the node's
  outbound sends** (a dead node emits nothing); deliveries to a crashed node are
  dropped until `restart`.
- `restart(node)` **re-arms** the node's tasks: it clears `crashed` *and* marks
  every task the node owns ready so the run loop re-polls them. This is load-
  bearing — crashing drops the waker of any task parked on `recv()` (the inbox is
  volatile), so without the re-poll a later delivery would find no registered
  recv waker and the task would never wake again. Re-polling a parked `Recv` on
  an empty inbox is side-effect-free (no RNG, no timeline event), and tasks are
  re-armed in ascending id order, so determinism holds. Regression test:
  `determinism.rs::restart_resumes_a_parked_recv`.
- `stop(node)` models a **process exit**: it removes the node's tasks (each
  spawned task is tagged with its owner node id) and volatile state (inbox,
  un-synced disk), keeping durable disk. Start a fresh node on the same id
  afterward and it recovers from disk — the real restart-and-rejoin path (see
  `animus-control/tests/restart.rs`). `crash` keeps the tasks running but mute;
  `stop` ends them.
- Determinism invariants to preserve when editing: only `BTreeMap`/`BTreeSet`,
  RNG drawn only in deterministic order, no wall clock. Disk ops add no timeline
  events and — under the **default** `DiskConfig` — draw no RNG, so they don't
  perturb traces; every existing test is byte-identical with the fault model
  present. With a non-default `DiskConfig` the disk *does* draw RNG (error
  sampling per op; tear point + corrupted byte on crash, files in `BTreeMap`
  name order), which is deterministic but seed-shifting — so a fault config is
  strictly **opt-in per test**. `error_rate` never fires on `size`/`remove`/
  `list` (metadata ops stay reliable so discovery/GC paths don't need fault
  handling to be testable). A `crash` tear keeps a seed-chosen **strict**
  prefix of each file's buffered bytes and makes it durable (it reached the
  platter); `corrupt_on_crash` flips one byte only inside that retained
  region, never in previously-durable bytes. Tests: `tests/disk_faults.rs`
  (default-off byte-identity is asserted against a run with no config at all).

- **`pause`'s defer mechanism reuses the existing `(time, seq)` timeline
  rather than adding a second queue.** A `Timer`/`Deliver` event due while its
  owner/destination is paused is popped from the timeline as usual (the
  global clock still advances to its scheduled time — pause freezes a
  *node's* perception, not the shared timeline) and then, instead of firing
  (waking a task / pushing into an inbox), is **re-inserted** at
  `(paused_until[node], fresh_seq)` and the loop moves on. When that new key
  is eventually reached, the node is by definition no longer paused
  (`clock < until` is false at `clock == until`), so it fires normally —
  possibly deferred again if a second, later-ending `pause` call landed in
  the meantime, which composes correctly for free. This is why `pause`
  needed one new piece of bookkeeping `Sleep` didn't carry before:
  `timer_owner: BTreeMap<TimerId, NodeId>`, populated the first time a
  `Sleep` future actually schedules a timeline entry (a `sleep` that
  resolves immediately on first poll never registers one, and never needs
  to — there's no timer to ever defer). A paused **sender's** send is
  handled differently — not via the timeline defer (the send call already
  computed a delivery time before there's anything to intercept) but by
  clamping `deliver_at` up to `paused_until[from]` inline in `send_stream`,
  which also covers the edge case of a task already ready-queued (about to
  run) at the exact moment `pause` was called: the executor still polls it
  synchronously (pause never blocks *execution*, only future timer/delivery
  *events*), but whatever it sends is held back regardless.
- **Multiplexed `(node, stream)` addressing (ADR 0026).** The inbox/waker maps
  are keyed `(NodeId, u64)` instead of `NodeId`, so a node can be addressed on
  more than one stream; `crash`/`stop` now node-prefix-scan both maps (the same
  pattern `Disk::list`'s node-prefix scan already used) to clear *every* stream
  of a crashed/stopped node, not just its primary one. `Simulator::env` still
  only pre-registers `PRIMARY_STREAM`'s inbox entry — any other stream is
  created lazily on first send/recv. No new RNG draw or timeline event shape,
  so the determinism argument (trace = pure function of the seed) is unchanged;
  `tests/determinism.rs::multiplexed_streams_are_isolated_and_deterministic`
  proves it directly (two streams to one node don't cross-talk, and the run —
  trace included — reproduces byte-for-byte from the seed).

- **`NetConfig`'s new knobs share one fixed draw order per send, all gated on
  their own threshold being non-zero** (ADR 0061 rung B2/B3): drop roll →
  corrupt roll (+ byte offset, only if the payload is non-empty — `gen_below`
  itself already draws nothing for `n == 0`, so an empty payload naturally
  costs one fewer draw, not a special case) → the primary delivery's jitter
  draw (its own heavy-tail roll, then the jitter) → duplicate roll → (if it
  fires) the duplicate's **own, independently-drawn** jitter (its own
  heavy-tail roll too). With every new threshold at its default 0 this
  reduces to exactly the original two-draw sequence (drop, then jitter) —
  the same "extra rolls are gated on non-zero, so off costs nothing"
  discipline `DiskConfig` already established, now shared via a
  `net_cfg_for(from, to)` resolution helper (link → per-node-on-sender →
  global) that mirrors `disk_cfg_for`. **The duplicate is independently
  delayed, not delivered at the same instant** — a deliberate choice (see
  `NetConfig::set_duplicate_prob`'s doc): it models a real duplicated packet
  taking its own path through the network, and it means a duplicate can
  arrive *before* the original.
- **`DiskConfig::enospc_threshold` and `error_threshold` share one roll**,
  not two independent ones (`inject_disk_fault`): `roll < enospc_threshold`
  fires ENOSPC, else `roll < enospc_threshold + error_threshold` fires
  generic, else no fault. This is what keeps a pre-existing `error_prob`-only
  config's draw *and* comparison byte-identical (`enospc_threshold` defaults
  to 0, so the combined check degenerates to exactly the old single
  comparison) rather than merely drawing the same number of values — two
  independent rolls would have needed to fire in a fixed order too, but
  would have drawn an *extra* value even for an enospc-only config, which
  one-roll-two-buckets avoids.

## Tests

`cargo test -p animus-sim` — `tests/determinism.rs` asserts byte-identical
traces across runs, reproducible partitions, and the crash/disk model;
`tests/disk_faults.rs` asserts the opt-in disk fault model is default-off
byte-identical and seed-reproducible when enabled, including `link`
(ADR 0058 rung 2): its own basic hard-link-semantics test (`dst` reads
`src`'s bytes, survives a `remove` of `src`, an already-linked `dst` is
safely overwritten on relink, a missing `src` is a clean `NotFound`) and a
reproducibility test proving it participates in the same seeded
error-injection schedule as every other disk op. The storage-facing fault
corpus (LSM torn-tail recovery, injected-error write paths, CRC on corrupted
blocks) lives in `animus-storage/tests/lsm_disk_faults.rs`; the SSTable-clone
crash-safety corpus (`LsmEngine::clone_to`) lives in
`animus-storage/tests/lsm_clone.rs`. `tests/clock_skew.rs`
(ADR 0018 §2 sim support) proves the clock-skew knob: per-node `now()` offsets
by exactly its configured skew while an unskewed node tracks the global clock;
a large negative skew clamps at 0 near time zero instead of underflowing; and
the same seed + skew script reproduces an identical observed `now()` sequence
(the determinism guarantee holds with skew configured, not just by default).
The HLC-specific causality-under-skew property (a behind-clock node's mint
still exceeds an ahead-clock node's) is tested in
`animus-cp-data/tests/hlc_skew.rs`, since it needs both this crate and
`animus_cp_data::hlc`.

`tests/net_faults.rs` (ADR 0061 rung B2/B3) covers per-node/per-link
`NetConfig` resolution (link beats per-node-on-sender beats global — and a
per-node override scopes to the node as *sender*, leaving another node's
sends on the same destination unaffected), duplication (probability 1.0
delivers every message exactly twice, with independent — usually differing —
delays, reproducibly from the seed; probability 0 delivers once, matching
before), wire-payload corruption (probability 1.0 flips exactly one payload
byte reproducibly; probability 0 never touches the payload), heavy-tailed
jitter (with the ordinary ceiling at 0 and the heavy tail always selected,
delivery delay reaches well past what the ordinary ceiling would ever allow),
and a default-byte-identical test proving an explicitly-default `NetConfig`
set globally/per-node/per-link changes nothing. `tests/pause.rs` covers
`pause`: a timer due mid-pause is deferred to the resume instant, not fired
early or lost; a message addressed to a paused node queues (delivered only
after resume, never dropped); a paused node's own send is held back until
resume (including the ready-queued-at-pause-time edge case); the pause
script is deterministic and traced (`PAUSE node=... until=...`); and another
node's schedule is unaffected by a peer's pause. `tests/disk_faults.rs`
additionally covers ENOSPC (`ErrorKind::StorageFull` is distinguishable from
a generic `ErrorKind::Other` fault, the two compose on one shared roll and
both occur reproducibly, and the default `enospc_prob == 0` leaves a
generic-error-only config's outcome sequence pinned to what it was before
ENOSPC existed) and fsync-acked-but-lost (`sync` returns `Ok` but a following
crash still loses the "acked" bytes at probability 1.0, reproducibly; at
probability 0 — the default — a genuinely synced write survives a crash as
before). `tests/clock_skew.rs` additionally covers clock drift
(`set_clock_drift_for`): a node's observed skew widens linearly with elapsed
virtual time at exactly its configured ppm rate, composes additively with a
static skew rather than replacing it, is reproducible from the seed, and
defaults to no divergence with no call.

`segment_store.rs`'s own `#[cfg(test)] mod tests` (unit tests, not a
`tests/*.rs` integration file — small enough to sit beside the impl) covers
`SimSegmentStore`: the shared `animus_env::test_support::
assert_segment_store_contract`; an ack-lost `put`/`delete` leaves the actual
state change intact while the caller sees an error (checked from a second
clone of the store, not just the original handle); an unavailability window
fails every op with no state change until virtual time (advanced via
`Clock::sleep` + driving the simulator) passes the deadline, then heals on
its own; `clear_unavailable` heals early regardless of virtual time; and a
determinism regression comparing two runs of the same seed + fault schedule
for byte-identical outcome sequences (plus a different seed diverging), with
a sanity check that the configured fault probability actually produces both
outcomes over the run.
