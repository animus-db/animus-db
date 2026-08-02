# ADR 0009 — In-house Raft over the `Env` seam (deviation from openraft)

- **Status:** Accepted
- **Date:** 2026-08-01

## Context

The bootstrap brief suggests `openraft` (or `raft-rs`) for control-plane
consensus. Independently, ADR 0003 makes determinism non-negotiable: *all*
nondeterminism — time, task scheduling, network, randomness — must flow through
the `Env` seam so a run is byte-reproducible from a seed, and the M3 acceptance
criteria require exactly that (leader election and leader-kill survival,
replayable from a seed, under `SimEnv`).

`openraft` drives its own time (timers), spawns its own `tokio` tasks, and owns
its own RPC scheduling. None of that goes through our `Env`, so it cannot be
driven by the single-threaded, virtual-clock `SimEnv`; its election timeouts and
task interleavings would be real-time and nondeterministic. Making it
deterministic would mean forking it or adopting `madsim` wholesale now — a much
larger commitment than M3 warrants.

## Decision

For the M3 control-plane skeleton we will implement a **small, self-contained
Raft** (leader election + log replication + commit + apply) as a *synchronous*
`RaftCore` state machine that runs entirely over `Env`: a thin per-node driver
owns the `Env` and feeds the core timer ticks and decoded messages, and the core
returns outbound messages and applies committed entries. All randomness
(election-timeout jitter) and time come from `Env`. This keeps the control plane
fully deterministic and replayable.

The core implements the safety-critical Raft rules (term/vote handling, log
up-to-dateness for votes, `AppendEntries` consistency check with conflict
truncation, commit only of current-term entries via majority `matchIndex`).

## Consequences

- The control plane is deterministic and testable under simulation today, which
  is the whole point of the project.
- We own and must maintain a Raft implementation. It is deliberately minimal.
  Durability is implemented (follow-up to M3): the core emits a write-ahead log
  of hard-state/log/snapshot records that the driver `fsync`s before acting, and
  recovers from on startup (see `persist.rs`). The log is offset by a
  state-machine **snapshot**; on a threshold the node snapshots its applied state
  and **truncates** the covered log prefix, and the WAL is rewritten to its live
  image (snapshot + hard state + log tail) via an atomic `Disk::replace`
  (temp-file + rename in production) — so both the log and the WAL are bounded by
  the live tail. A follower that has fallen behind the leader's compacted prefix
  is caught up by an `InstallSnapshot` RPC. Recovery restores the snapshot and
  re-applies the tail, so each committed command lands exactly once relative to
  the snapshot base (no double-applied CAS). The full WAL write/compact/recover
  flow is diagrammed in [`docs/wal.md`](../wal.md). Restart-and-rejoin is now
  tested end-to-end in the simulator (`Simulator::stop` drops a node's tasks +
  volatile state; a fresh node started on the same disk recovers and rejoins —
  see `tests/restart.rs`). The `InstallSnapshot` RPC is **chunked**: the leader
  splits the serialized `Metadata` into offset-addressed chunks of at most
  `SNAPSHOT_CHUNK_BYTES` and ships them one per round trip (tracking each
  follower's byte offset in `snapshot_offset`); the follower reassembles them in
  a contiguous buffer and installs the snapshot atomically only once every byte
  has arrived (`InstallSnapshotResp.next_offset` drives the next chunk, and
  `last_index` is echoed non-zero only on completion). Chunking lives entirely in
  the sync `RaftCore` (chunk production + follower reassembly), so it stays
  I/O-free and deterministic. A multi-chunk transfer is tested in
  `tests/install_snapshot.rs::follower_catches_up_via_multi_chunk_snapshot`.
  **Still deferred:** a transfer interrupted by a leader change restarts from
  offset 0 (no cross-leader resumption), and there is no flow-control on the
  chunk stream.
- If we later need the maturity of `openraft`, the `Env`-driven boundary (a sync
  core + an I/O driver) is a clean place to swap implementations, and a `madsim`
  backend behind `Env` (ADR 0003) would let a third-party Raft run
  deterministically.
- This ADR supersedes the brief's dependency suggestion for the control plane;
  ADR 0001 (two-plane architecture) is otherwise unchanged.

## Durable-before-visible: closing the apply-before-fsync window (resolved)

**The bug.** `RaftCore::propose` advanced `commit_index` and **applied the command
to `Metadata` synchronously**, then returned `Accepted` — while the WAL `append +
fsync` ran **asynchronously** in the driver loop (`flush_wal`), normally parked in
its `select` between ticks. So an applied command was **client-visible (and acked)
before it was durable on disk**: the DynamoDB edge's `CreateTable` waits on
`has_table_schema`, so it returned `200` while the entry might still be only in
memory. A crash in that window lost an acknowledged command (recovery restores the
last fsynced snapshot + WAL tail, without it) — the intermittent failure of
`animusd`'s `tests/dynamo_schema.rs::create_table_survives_node_restart`.

**The fix (shipped): a durable watermark, gated on the *leader*.** `RaftCore` now
carries a `durable_index`, and on the **leader** `apply` advances `last_applied`
only up to `min(commit_index, durable_index)` — never past what is fsynced. The
driver advances the watermark via `RaftCore::mark_durable_through` **immediately
after `env.sync(WAL)`** in `flush_wal` (passing the log high-water captured at
drain), so a committed entry becomes applied/visible on the leader **only once it
is on disk**. A proposer that observes applied state (`has_table_schema`,
`metadata()`) therefore waits for durability for free — no caller change needed.

This is the same "ack-means-synced" rule the data plane already enforces
(`animus-data` `ack_durability`) and mirrors `animus-consensus`'s `persist_then_ship`
ordering (WAL fsync *before* the apply effect). Multi-node safety was already in
place — the driver flushes **before** sending outbound (`drive`'s
"durability before action"), so a follower fsyncs before its `AppendEntriesResp`
and the leader before its `AppendEntries`; commit therefore already rested on
durable logs. This change closes the remaining gap: the **leader applying/exposing
its own entry** before its local fsync (acute in a single-node group, where commit
is self-only).

`recovered()` sets `durable_index` to the recovered `last_log_index` (everything
from the WAL/snapshot is durable). Regression coverage:
`persistence.rs::a_command_is_visible_only_after_it_is_durable` (a committed-but-
unsynced command is invisible and does not survive a crash; after the fsync it is
both visible and crash-durable). A core driven by hand must simulate the driver's
fsync — drain, then `mark_durable_through(last_log_index())` — or its `metadata()`
never reflects proposals (see the `persist` helper in `persistence.rs`).

**Follower reads apply on commit (the gate is leader-only — done).** A follower
never acks a control-plane write to a client (writes are proposed to the leader);
it only serves *reads* of its local `Metadata`. A committed entry already rests on
a quorum of durable logs — the driver flushes **before** sending outbound, so a
follower fsyncs before its `AppendEntriesResp` and the leader before its
`AppendEntries`. So a follower may safely expose a committed entry on **commit**,
without waiting on its **own** local fsync. `apply` is therefore **role-aware**:
the leader's frontier is `min(commit_index, durable_index)` (ack-path gated), a
non-leader's is `commit_index` (apply-on-commit). This avoids needlessly widening
cross-node read-visibility lag. `last_applied` only moves forward, so a follower
that applied to commit then wins an election keeps those (committed / quorum-
durable) entries, while its *own future* proposals stay durability-gated (their
index exceeds `durable_index` until it fsyncs). Coverage:
`follower_visibility.rs` (a hand-driven follower applies a committed entry with
`durable_index == 0`; a leader stays gated on its own proposal; a follower→leader
transition keeps the applied entry and gates new proposals; and end-to-end, both
followers in a `SimEnv` cluster reflect the leader's committed command).

A pre-existing *cross-node* race remains independent of this: a query/read issued
on a follower immediately after a `CreateTable` on the leader can still outrun
replication to that follower (the entry has to *arrive* and commit there first).
The cure is the same everywhere: wait for the replicated definition on the target
node before reading it (`await_table_schema`/`await_table_index` in the `animusd`
tests), exactly as the restart tests already do.
