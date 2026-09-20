# ADR 0009 — In-house Raft over the `Env` seam (deviation from openraft)

- **Status:** Accepted
- **Date:** 2026-08-01
- **2026-08-10 note:** the control plane's own state machine (`Metadata`) is
  now `DRIVER_APPLIED` too (ADR 0038) — `RaftCore`'s sync/async split (this
  ADR's core contribution) is unchanged, but `RaftCore` no longer applies
  `MetaCommand`s in-core itself; see ADR 0038.
- **2026-08-24 note:** `RaftCore` gains a **learner** (non-voting) membership
  class alongside its existing voter config (ADR 0058 Train 1) — a per-member
  `learners: BTreeSet<NodeId>`, kept in the same config-in-log discipline as
  the voter `config` this ADR already documents (a membership-change log
  entry carries both sets together). A learner receives `AppendEntries`/
  `InstallSnapshot` exactly like a voter (its `match_index` is tracked the
  same way) but is excluded from `cluster_size`/`majority()` entirely and
  never campaigns or pre-votes (`start_election`/`start_pre_vote` gate on
  `is_voter`, the same check that already protected a not-yet-added node —
  see "Test gotcha (membership)" in `animus-cp-data/CLAUDE.md`, now a
  *durable* instance of that same transient state rather than only a
  bring-up race). Applies to both planes, since both instantiate the same
  generic `RaftCore<C, S>`. See ADR 0058 for the full design and rationale;
  this note only records that the primitive lives here.

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

## Pre-vote + a configurable election timeout (spurious-election hardening — done)

**The problem.** Under write load a per-tablet CP Raft group (which reuses this
same `RaftCore`, ADR 0016) suffered a **leader-election storm**: the term climbed
continuously (1 → 37 in a few seconds) because a replica whose async driver was
briefly busy (real disk I/O) missed a heartbeat window, timed out, and **campaigned
— incrementing the term** — disrupting a perfectly healthy leader and truncating
in-flight writes. A single stalled/partitioned node repeatedly bumping the cluster
term is the exact failure mode standard Raft's **pre-vote** extension exists to
prevent.

**The fix (shipped): pre-vote.** Before a node increments its term to start a real
election it runs a **pre-vote round** as a new `Role::PreCandidate`. It solicits
`RaftMsg::PreVote { term = current_term + 1, .. }` from its peers **without**
bumping its term or casting a real vote. A peer grants a pre-vote only if it would
actually vote: it has **no live leader** (not a leader itself, and not a follower
still within its election timeout of the last heartbeat — the leader lease), the
candidate's prospective term is not behind, and the candidate's log is at least as
up to date. Only on a **pre-vote majority** does the node call the existing
`start_election` (which increments the term, becomes `Candidate`, and sends real
`RequestVote`s). Key invariants that make it safe and deterministic:

- **A pre-vote never changes any node's term.** Both `PreVote` and `PreVoteResp`
  bypass the "step down on a higher term" rule in `handle`; the *only* place a
  pre-candidate adopts a newer term is a **rejecting** `PreVoteResp` carrying a
  higher real term (it learns it is behind and reverts to a plain follower at that
  term — never beyond it). So a partitioned node loops through harmless pre-vote
  rounds and can neither inflate its own term nor a healthy peer's.
- **The leader lease is `leader_id.is_some() && now < election_deadline`** (plus
  `role == Leader` for the leader itself) — data the core already tracks, evaluated
  at the injected `now`, so the whole decision stays a pure function of
  `(state, message, now, entropy)`. No clock, no `HashMap`, no I/O. **Amended
  2026-09-19 (issue #930, below)**: also covers a role-gated `voted_for.is_some()`
  — `leader_id` alone left a granted real vote with no lease of its own.
- **Single-node / trivial-majority groups still elect immediately:** `start_pre_vote`
  short-circuits to `start_election` when self alone is already a pre-vote majority.

Pre-vote rides the shared `RaftMsg` enum additively, so **both** planes (control +
`animus-cp-data`) keep their wire formats; the cp-data driver forwards the new
variants through `KvWire::Raft` unchanged.

**Configurable election timeout — removed (issue #313, 2026-09-01 amendment).**
`RaftCore::set_election_timeout(base, now, entropy)` originally set the
election-timeout base (still randomized in `[base, 2*base)`, default 150ms)
and re-armed the timer, with the stated intent that an assembly layer would
widen it for a node doing real disk I/O — cutting the rate of spurious
timeouts at the source, complementary to pre-vote (which makes any timeout
that does slip through non-disruptive). That assembly layer was never
built: the setter had zero call sites (grep-verified) beyond its own
definition and a doc cross-reference, for the entire time between this
ADR's authoring and its removal. Rather than leave a documented-but-unwired
knob in limbo, it was deleted; `RaftCore::election_timeout()` (the
read-only accessor) stays — `transfer_leadership` arms its deadline from
it, and `animus-control::node`'s driver now also logs it as the budget an
aborted leadership transfer had to fit in (issue #313's own fix, see the
"Leadership transfer" entries in `animus-control/CLAUDE.md`). If a real
need to widen the timeout for a slow-disk node resurfaces, re-add the
setter alongside its actual caller in the same change, not speculatively
ahead of one.

Coverage: `tests/pre_vote.rs` — core-level (a live-leader lease rejects a pre-vote
and the term is untouched; an expired lease grants; a timeout makes a pre-candidate
without bumping the term; a voter that just granted a real vote rejects a
competing pre-vote until its own election deadline, issue #930 below; a
`Candidate`'s own self-vote protects it the same way) and end-to-end under
`SimEnv` (an isolated follower's pre-vote rounds do not move the stable
leader's term, and it rejoins on heal with no election; a genuine leader
crash still elects a new leader at a higher term). The pre-existing
hand-driven election tests (`follower_visibility`, `install_snapshot`,
`driver_applied_sm`) now drive the pre-vote round explicitly. See the
2026-09-19 amendment below for the `voted_for`-lease gap and its fix.

## Amendment (2026-09-01): a lagging peer under sustained write load could
never catch up — issues #532/#537, two cooperating fixes

**The problem.** A per-tablet CP-data Raft group (this core, reused
unchanged by `animus-cp-data`) under a **sustained per-item writer** could
leave a freshly-added learner — or any peer that fell behind — permanently
stalled: `match_index` pinned at a fixed value for an entire run while the
leader's own log raced ahead (confirmed live via instrumented state; the
idle-cluster equivalent of the identical scenario promotes in seconds — the
stall tracks write load, not topology). Two independent, cooperating
mechanisms, both in this shared core, were found and fixed:

**1. Unbounded `AppendEntries` batches.** `replicate_to` shipped a lagging
peer the ENTIRE outstanding tail (`next_index..=last_log_index`) in a
single message, cloned fresh
(`self.log.iter().filter(|e| e.index >= next).cloned().collect()`) on
every call — and `replicate_now`'s wake-on-propose (ADR 0017's
single-write-latency fix, above) re-invokes `broadcast_append`/
`replicate_to` on **every single propose**, with no coalescing beyond the
boolean `ProposeSignal`. Under a sustained writer, a lagging peer therefore
received an unbounded sequence of ever-larger, overlapping `AppendEntries`
messages, each re-cloning a growing tail on the leader's own consensus
loop regardless of whether the peer had acked the previous one — real,
unbounded per-propose CPU cost, confirmed to hang a `SimEnv` test process
for minutes of real wall-clock time at a large enough log/propose count,
despite `SimEnv` charging **zero** virtual time for it (a purely
wall-clock pathology, matching this issue's own "real-time race" framing).

**The fix**: `MAX_APPEND_ENTRIES_BATCH` caps the entries a single
`AppendEntries` may carry — **512**. Derivation: the cap only has to stop
unbounded growth, not minimize batch size — a real replication round (WAL
append + `fsync` on the receiving peer) costs roughly the same wall-clock
time whether it carries a dozen entries or a few hundred, so shrinking the
cap much below "a real catch-up distance" only adds round trips (each
still paying that fixed cost) without shrinking per-round work
meaningfully — a net loss once round latency, not per-entry cloning, is
the bottleneck (confirmed empirically: a small cap and no cap converged
equally poorly against a disk-latency-throttled peer before this value was
widened). `COMPACT_THRESHOLD`/`SNAPSHOT_THRESHOLD` (both 64, the
CP-data-plane and control-plane compaction windows respectively) already
bound how far behind this path is ever exercised before a peer falls back
to the chunked `InstallSnapshot` path instead — 512 is comfortably above
that window (one round trip in the common case) while orders of magnitude
below the unbounded growth observed in the field (a leader's log racing
past 25,000 entries while a stuck peer's own message kept growing to
match). `handle_append_resp`'s success arm already re-invokes
`replicate_to` immediately when more remains, so a peer needing several
batches clears the backlog in back-to-back acked round trips, not one per
external propose — this cap only bounds a *lagging* peer's traffic; an
up-to-date peer's steady-state traffic is far under it and unaffected.

**Seeding was investigated and found already sound — no change made.** A
freshly-added learner's `next_index`/`match_index` are not explicitly
seeded at `apply_config`/`log_append` time; `replicate_to` falls through to
`next_index.get(&peer).copied().unwrap_or(1).max(1)`, i.e. `next = 1`. This
is the same conservative default classic Raft uses for a peer with no
known state, and is exactly correct for a genuinely fresh learner (which
needs the whole log, or a snapshot if the log has already compacted past
index 1): `replicate_to`'s own `next <= self.snapshot_index` check routes
it to the (already-bounded, chunked) `InstallSnapshot` path automatically
whenever the log has compacted past that default. A narrower, real gap
exists — `next_index`/`match_index` entries are never cleared on
`remove_learner`/`RemoveMember`, so re-adding the *same* `NodeId` later
(uncommon in production; ids are not deliberately reused) could inherit a
stale, too-high `next_index` and fall back to a slow one-at-a-time
`handle_append_resp` decrement instead of starting fresh — flagged as a
narrow, out-of-scope follow-up, not exercised by this issue's own
reproduction.

**2. Snapshot-transfer invalidation under repeated compaction (found
investigating this same issue; the residual finding beyond the batch cap
alone).** Once a peer falls far enough behind to need the chunked
`InstallSnapshot` path, `snapshot_upto` — called by a `DRIVER_APPLIED`
driver's own apply task whenever `COMPACT_THRESHOLD`/`SNAPSHOT_THRESHOLD`
is crossed — unconditionally drops **every** in-flight transfer's own
progress the instant the base moves again (`snapshot_blob = None`,
`snapshot_offset.clear()`): correct and necessary, since the in-flight
bytes were captured at the OLD base and shipping them mislabeled with a
new `snapshot_index` would corrupt the receiver. Under sustained writes,
ordinary threshold-triggered compaction can re-cross faster than a lagging
peer's own multi-chunk transfer (network round trips, `SNAPSHOT_CHUNK_
BYTES`-sized chunks) can complete — restarting it from chunk 0 against a
newer, larger image, forever. A `SimEnv` reproduction confirmed this
directly: with the batch cap alone, a learner made real initial progress
via ordinary `AppendEntries`, then plateaued permanently the moment it fell
back to the snapshot path (see `animus-cp-data/tests/
learner_catchup_under_load.rs`).

**The fix**: `RaftCore::snapshot_transfer_in_flight()` is a new pure
accessor (`!self.snapshot_offset.is_empty()`) a `DRIVER_APPLIED` driver's
own compaction gate now consults. `animus-cp-data`'s `apply_and_compact`
defers a **threshold**-triggered base advance (never an `image_needed` one
— a peer is actively waiting on that image, so it must always proceed)
while some peer's transfer is genuinely in flight, up to
`COMPACT_DEFER_CEILING` (`COMPACT_THRESHOLD * 8`) — a bounded emergency
ceiling past which compaction proceeds regardless, so the WAL still bounds
even against a dead/partitioned/hopelessly-outpaced peer's transfer that
will never complete. This is a **policy** change in the driver only — the
core's own `snapshot_upto` correctness argument (an advance always
invalidates every in-flight transfer) is completely unchanged; the driver
now simply calls it less often under one specific, bounded condition.

**Both fixes are additive to the shared `RaftCore`** (a new capped-length
slice in `replicate_to`, a new read-only accessor) and change no wire
format, no persisted state, and no existing safety invariant (log
matching, commit safety, the `InstallSnapshot` chunking/O(chunk) property
above). Regression: `animus-cp-data/tests/learner_catchup_under_load.rs`
(the `SimEnv` centerpiece — a sustained per-item writer against a
disk-latency-throttled fresh learner, proven red with either fix reverted,
green with both in place) plus the existing `ANIMUS_LEARNER_SEEDS`/
`ANIMUS_INPLACE_SPLIT_SEEDS` corpora and the `ANIMUS_RAFTKV_SEEDS`
linearizability corpus, all held green over the modified replication path.

**Honest residual, not closed by this amendment**: the real `ProdEnv`
end-to-end proof (`animusd/tests/cluster_gt_rf_split_bench.rs`, unmodified)
converged fully in 1 of 3 runs on the validating host post-fix (5.75s to
full convergence) — a clear, substantial improvement over the pre-fix
mechanism (confirmed via the same `SimEnv` scenario: real progress that
used to plateau at a fixed point now clears 10x+ more of the backlog
before the run's own write window ends), but the other 2 of 3 runs still
did not reach `done` within the bench's 240s budget, an unchanged ratio
from this same bench's own pre-fix baseline. A third, not-yet-identified
contributing factor on this specific host/workload shape is suspected;
flagged here rather than smoothed over, per this ADR's own "not confirmed"
discipline elsewhere in this file, and recommended as a dedicated
follow-up investigation.

## Amendment (2026-09-01): the third mechanism behind the residual —
unbounded `InstallSnapshot` chunk resend FREQUENCY — issues #532/#537

**The problem, confirmed the third contributing factor the previous
amendment's residual flagged.** Once a peer's transfer falls onto the
chunked `InstallSnapshot` path (above), `replicate_now`'s wake-on-propose
calls `broadcast_append` for every peer on every propose, and for that peer
`snapshot_chunk_for` re-sliced and re-sent whatever chunk was still
outstanding **unconditionally**, on every one of those calls — resending
the identical unacked chunk at write rate, long before the peer could
possibly have acked the last one (confirmed live on an instrumented bench
run: 96,451 `InstallSnapshot` chunk sends for only 196 real offset
transitions, the tracked offset parked at a fixed value for the whole run).
Compounding it, the ack-handler's own resend
(`handle_install_snapshot_resp`) was equally unconditional: every response
the flood provoked — including a duplicate, no-progress ack from a chunk
the follower had already superseded — fed straight back into another
resend, so the flood was self-sustaining once started, bounded only by
round-trip time rather than by anything either caller controlled. Together
this congested the peer's own single-consumer inbox badly enough that its
transfer could not complete inside the previous amendment's own
`COMPACT_DEFER_CEILING` window, so ordinary threshold-triggered compaction
eventually invalidated it anyway and it restarted from chunk 0 — repeating
forever under sustained write load, exactly the residual the previous
amendment left open.

**A second, independent defect surfaced building this fix, not previously
suspected**: under the pre-fix flood's own overlapping in-flight sends,
acks can reach the leader in an order that does not track real progress —
an ack generated for an EARLIER, already-superseded request can be
processed by the leader AFTER a LATER one that already advanced things
further (both are genuine, freshly-generated acks; nothing here is
stale/reordered network delivery, only overlapping *requests* completing
out of sequence). `handle_install_snapshot_resp`'s bare
`self.snapshot_offset.insert(from, next_offset)` let such an ack regress
the leader's own tracked offset backward — confirmed directly by
instrumenting the pre-fix code: 217 such regressions in a single run of
`animus-cp-data/tests/learner_catchup_under_load.rs`, each stepping
backward by exactly one chunk. The pre-fix flood's own sheer resend volume
papered over this (enough brute-force duplicate sends eventually
re-advanced past any transient regression anyway, at the cost of the flood
itself) — which is precisely why a naive throttle regressed convergence
before this second defect was found and fixed structurally (see the two
rejected prototypes below).

**The fix, two parts.** (1) `RaftCore::snapshot_offset`'s update in
`handle_install_snapshot_resp` is now `entry(from).max(next_offset)` —
monotonic regardless of ack arrival order, closing the regression above
independent of any resend policy. (2) A new `SnapshotResend` gate bounds a
resend of an **unchanged** offset — never a genuinely new one, which always
ships immediately at every call site — per caller: `replicate_now`
(wake-on-propose) gets `Capped(0)` (send once, then wait for real progress
or a different trigger); `handle_install_snapshot_resp`'s own ack-driven
resend gets `Capped(SNAPSHOT_ACK_RESEND_CAP = 8)`; every other trigger
(heartbeat tick, a peer's own `AppendEntries` response, an explicit
`WakeRequest`, a fresh leadership term) keeps `Always`, since each is
already bounded by something other than write rate. `RaftCore::
snapshot_chunk_sent` (the per-peer `(offset, resend-count)` marker this
gates against) and `RaftCore::snapshot_chunk_advances` (a lifetime,
test-observability-only counter of genuine advances, never resends) are
both additive core state, cleared/removed at exactly the points
`snapshot_offset` itself already is (per-peer on transfer completion,
wholesale on `snapshot_upto` invalidation, on a fresh leadership term) —
same discipline the `MAX_APPEND_ENTRIES_BATCH`/`snapshot_transfer_in_flight`
fixes above used, no wire format or persisted-state change.

**Two narrower prototypes were tried first and rejected** against
`animus-cp-data/tests/learner_catchup_under_load.rs` (the learner never
caught up): skipping a mid-snapshot peer from wake-on-propose entirely, and
throttling wake-on-propose by propose *count* (1-in-2, 1-in-20). Building
the fix that actually converges surfaced why both failed, and it is
**not** what it first looked like: `replicate_now`'s own wake is a single
coalesced `AtomicBool` (`ProposeSignal`), not a per-propose counter, so
under that test's own tight synchronous burst-of-ten-proposes shape it
already fires at most once per burst regardless of either throttle — the
convergence-breaking mechanism in THAT test is entirely the ack-handler's
own self-sustaining cascade above, which neither prototype's throttle ever
touched, compounded by the monotonic-regression defect neither prototype
was designed to catch. A genuinely stuck transfer needs *some* bounded
number of ack-driven retries to escape before the next heartbeat — under
sustained write load `heartbeat_deadline` is perpetually deferred by
`replicate_now`'s own reset on every propose, so that backstop rarely fires
in time on its own — which is why `Capped(0)` on the ack-handler's own call
site (tried too) also regressed this test, and why `SNAPSHOT_ACK_RESEND_CAP`
is a small nonzero bound rather than either extreme.

**Message volume is measurable under `SimEnv` even though it costs nothing
there** — a genuinely new finding for this repo's testing doctrine, not
just this fix: a resend flood advances no virtual time and (with `SimEnv`'s
default zero network/disk delay) can cost near-zero real time too, so a
test that only watches convergence timing (`learner_catchup_under_load.rs`)
cannot by itself prove a flood is bounded — it could regress back to
thousands of redundant sends per real chunk without ever going red. The
fix for that: `animus-cp-data/tests/snapshot_resend_bound.rs`, threading a
recording `MetricsHandle` (ADR 0015, `Metric::CpSnapshotShips`, already
existing) as the numerator and the new `snapshot_chunk_advances` accessor
as the exact denominator — deliberately not periodic external polling of
`snapshot_offset`, which an earlier draft of this test tried and found
silently undercounts (a genuine advance can happen well inside a single
millisecond once a transfer is flowing, and a coarser poll just misses
it, inflating the measured ratio regardless of how effective the fix
actually is). That test also found that the two workload shapes matter for
which mechanism dominates: driving one propose per scheduler turn (`SimEnv`
never coalescing `replicate_now`'s wake the way a synchronous burst does)
reproduces the field's own per-write flood and shows `Capped(0)` on
wake-on-propose alone already cutting sends-per-genuine-advance from
several hundred (matching the field's own ~492-per-transition order of
magnitude) down to roughly 90; adding the ack-handler's own
`SNAPSHOT_ACK_RESEND_CAP` trims that further to a smaller but still
comparable figure at this one seed — the real win of the ack-side cap is
not this particular measurement but that it gives the mechanism a genuine
STRUCTURAL worst case, where `Always` there has none of its own at all.

Regression: `animus-cp-data/tests/learner_catchup_under_load.rs` (unchanged,
stays green — the guard both rejected prototypes failed) and the new
`animus-cp-data/tests/snapshot_resend_bound.rs` (red on the unfixed
mechanism, several hundred sends per genuine chunk advance; green with
this fix, comfortably under a small bound). Full suites
(`animus-control`, `animus-cp-data`, `ANIMUS_LEARNER_SEEDS=25`,
`ANIMUS_RAFTKV_SEEDS=5`) held green over the modified path. The real
`ProdEnv` end-to-end bench (`cluster_gt_rf_split_bench.rs`, unmodified)
converged fully in 3 of 3 runs on the validating host with this fix on top
of the previous amendment's — closing the residual that amendment left
open, on this host and workload shape.

### Addendum (2026-09-02): `state_machine_behind` and `AppendEntriesResp::needs_snapshot` (issue #554)

The core protocol type gains two small, purely additive pieces so a
`DRIVER_APPLIED` plane can tell its leader "my state machine is behind its
own log's compacted start, regardless of what my log tail says" — see ADR
0017's own 2026-09-02 addendum for the full mechanism and the bug it
closes (an engine wiped and reopened fresh behind an already-compacted
log, silently reporting itself caught up). This lives here rather than
purely in `animus-cp-data` because `RaftCore` is the shared sync core (ADR
0016/0017), so the change is:

- A new `RaftCore` field, `state_machine_behind: bool` — default `false`,
  set only via the new `set_state_machine_behind` setter. **Never called
  by the in-core control plane** (`animus-control::node`'s driver), which
  makes every dependent behavior below permanently inert for that plane:
  `Metadata`'s own async apply task (ADR 0038) already seeds its
  `engine_applied` from the system-keyspace engine's own durable watermark
  key (`node.rs`'s `meta_apply_loop`, not `core.last_applied()`) — the
  identical fix ADR 0017's addendum describes for the data plane — so the
  control plane does not have this bug's *active* trigger today. It is
  not immune to the same *class* of gap in principle (nothing currently
  destroys-and-reopens a control node's system-keyspace engine the way
  `animus-cp-data::host`'s reconciler does for a tablet's engine), but
  building that detection/request path for a mechanism with no live caller
  would be speculative, untestable machinery — flagged here, not built.
- `start_pre_vote`/`start_election` both gain `|| self.state_machine_behind`
  alongside their existing `!self.is_voter()` campaign gate — mirroring
  exactly how a learner is already kept from campaigning, for the same
  underlying reason (a `state_machine_behind` node's log looks perfectly
  eligible; only its engine is untrustworthy, so it must not become
  leader and risk shipping a corrupt `InstallSnapshot` image to a healthy
  peer). Two independent gate sites, not one, since `handle_pre_vote_resp`
  can reach `start_election` directly on a pre-vote majority without going
  back through `start_pre_vote`.
- `RaftMsg::AppendEntriesResp` gains `needs_snapshot: bool`
  (`#[serde(default)]`, so an older wire peer that never sets it decodes
  to `false` — harmless: a replica that never learns about a gap simply
  doesn't proactively close it early, and still refuses reads/campaigning
  on its own regardless). `handle_append_resp` threads it through and, on
  `true`, resets the peer's `next_index` to 1 (letting the pre-existing
  `replicate_to`/`snapshot_chunk_for` `next <= snapshot_index` check do
  the rest) **unless** a new leader-side map, `snapshot_served_through:
  BTreeMap<NodeId, u64>`, already shows this peer fully served at or past
  the leader's current `snapshot_index` — without that guard, a leader
  that resets on *every* still-true ack (not just the first) restarts a
  fresh chunked transfer before the peer ever finishes digesting the last
  one, a genuine, reproducible livelock (confirmed live building this:
  `next_index` oscillating between 1 and past-`snapshot_index` forever,
  the peer's `engine_applied` never advancing past 0) — see ADR 0017's
  addendum for the full account and the fix's other half (making
  `state_machine_behind` a live, per-loop-iteration recomputation on the
  `DRIVER_APPLIED` side rather than a driver-latched one-shot flag, which
  turned out to be necessary but not sufficient on its own).
- `handle_install_snapshot`'s "already at least this far along" fast path
  (`last_index <= self.snapshot_index` ⇒ drop the transfer, just ack —
  the *correct* behavior for an ordinary caught-up replica) is additionally
  gated `&& !self.state_machine_behind`: for a behind replica this is
  precisely the wrong call, since its own `snapshot_index` is exactly the
  fact it cannot trust, and `last_index == self.snapshot_index` is the
  overwhelmingly common #554 shape (the log matched before the engine was
  lost) — without this the offer meant to fix the gap was being silently
  discarded, every time, with the engine never touched.

Wire codec: `animus-cp-data::codec`'s hand-rolled binary framing needed its
own explicit encode/decode arm for the new field (version bump), per this
crate's standing rule that `#[serde(default)]` only protects the
`serde_json` WAL path, never a hand-rolled one — see `animus-cp-data/
CLAUDE.md`'s `codec.rs` entry.

Regression: `animus-control`'s existing suite (unchanged, stays green —
every dependent behavior above is inert when `state_machine_behind` is
never set, which no control-plane call site does);
`animus-cp-data/tests/engine_wipe_needs_snapshot.rs` is the live proof
over the `DRIVER_APPLIED` plane that actually exercises this.

## Amendment (2026-09-15): boot-time genesis-vs-wiped-restart check (issue #667, P0 Raft safety)

**The hazard.** Every existing recovery path (`node.rs`'s `drive`: `if
!state.is_empty() { RaftCore::recovered(..) } else { /* keep the fresh
RaftCore::new built at start time */ }`) treated a control-plane voter's
persisted WAL replaying to `PersistedState::is_empty()` as an unqualified
genesis bootstrap — safe for a voter's very first-ever run, but identical,
byte for byte, to a control voter's disk being wiped (ephemeral storage,
`storage.ephemeral: true`, a real Kubernetes `EmptyDir`) and restarting
*into an already-established cluster*. `current_term`/`voted_for` are
exactly what a real vote grant needs to remember to avoid granting a
second, contradicting vote in a term it already voted in — and both are
gone. A wiped voter that restarts, is asked to vote for a different
candidate than the one it already (durably, before the wipe) voted for in
the same term, and grants it, can elect two leaders in one term — a direct
violation of Election Safety. Liveness of the *rejoin* was never the
problem (pre-vote's log check already stops a fresh node's empty log from
winning a real election against an established group); this is purely a
safety hole. Root-caused via the S-07d growth roll incident and
`crates/animus-control/tests/wiped_voter_rejoin.rs`'s own repro (see issue
#667).

**The decision.** A node whose persisted Raft state is empty must refuse
to act as a **voter** — grant no real vote, start no campaign — until it
has resolved, via peers reachable through the `Env` seam, whether this is
a genuine first-ever bootstrap or an already-established voter's disk
wiped clean. A refused node must be re-admitted through the existing
**learner/rejoin path** (ADR 0032/0058: removed from the voter set, added
back as a non-voting learner, promoted once caught up) as a deliberate new
membership event — never simply restarted as a static voter again. This
was considered against two alternatives named in the issue and rejected:
treating `storage.ephemeral: true` as unsupported/invalid for control
voters (pushes the whole problem to the operator with no in-process
safety net at all — a config mistake still silently double-votes) and
doing nothing beyond documentation (leaves a real, demonstrated P0 hazard
live). The mechanism:

- `RaftCore` gains `cluster_check_pending: Option<BTreeSet<NodeId>>`
  (`Some(peers still owed an answer)` while resolving, `None` once
  resolved either way or never applicable — every pre-existing
  construction path, `new` and `recovered`, leaves it `None`) and a sticky
  `cluster_check_refused: bool` with no path back to `false` short of a
  fresh `RaftCore` from a real restart through the rejoin path.
- `node.rs`'s `drive`, in the branch where `state.is_empty()` (the ONLY
  place this whole mechanism is entered — never for a `recovered` core,
  whose term/`voted_for` are already trustworthy), calls
  `RaftCore::begin_cluster_check` instead of doing nothing. That method
  broadcasts a new, term-and-vote-inert message pair,
  `RaftMsg::ClusterProbe`/`ClusterProbeResp { term, committed_index,
  config }`, to every configured peer, and parks until either (a) any one
  peer answers with real history (`term > 0 || committed_index > 0`), or
  (b) every configured peer has answered genuinely empty. A never-
  answering peer is never assumed fresh — the election-timeout tick keeps
  resending the probe for as long as resolution is pending, so "peers
  unreachable" means keep probing, never vote, indefinitely.
- **`handle_request_vote`'s real grant condition** gains `&&
  cluster_checked` (`!cluster_check_refused && cluster_check_pending.is_none()`)
  alongside its pre-existing `is_voter() && can_vote && log_ok` — this is
  the one and only place a real, persisted vote is ever granted, so it is
  the one and only gate this fix needs on the safety side.
  **`handle_pre_vote` is deliberately NOT gated** the same way: granting a
  pre-vote touches no persisted state and cannot itself cause a double
  vote, and gating it too would (as built and reverted during this fix —
  see below) needlessly slow down every legitimate case pre-vote already
  makes safe. `start_pre_vote`/`start_election` both also refuse to
  *campaign* while pending or refused, mirroring the existing
  `state_machine_behind`/learner campaign gates above.

**The hard part: telling a wiped restart apart from an ADR 0060 growth
join, without requiring every configured peer to be simultaneously
online for anything but genuine multi-node genesis.** Both a wiped,
already-established voter and a brand-new voter joining an established
cluster via `change_membership` (ADR 0037/0060 — an everyday, frequent
operation, not a rare disaster-recovery event) present the **identical**
local symptom: empty persisted state. An early build of this fix treated
*any* peer showing real history as proof of a wiped restart and refused
unconditionally — verified, via
`wiped_voter_rejoin.rs`'s own `growth_then_wiped_leader_rejoin_reestablishes_leader`,
to livelock an entire 4-voter group: the newly-grown 4th voter refused
itself the instant its own `change_membership` commit landed on its
peers, permanently dropping the live quorum below majority. **The fix:
`ClusterProbeResp` additionally carries the responder's own current
*committed voter config*.** A responder showing real history is a
wiped-voter-restart signal only when its own config already names the
asker as a voter — proof this exact identity was already an established
voter somewhere. If the asker is not (yet) in that config, it has never
had voting rights under this identity before and cannot possibly have a
forgotten prior vote to double — safe to proceed immediately as an
ordinary fresh voter, exactly as it always could (pre-vote's log check,
ADR 0060's own existing safety argument, unchanged). This resolves in a
single round trip to any one reachable peer with real history, regardless
of how many other peers are up — growth is not slowed by this mechanism
at all in the common case. Only a genuine **multi-node genesis** (nobody
has any history yet) still needs every configured peer to confirm empty,
which is inherent: nobody can yet authoritatively answer "is the asker
already one of my voters" before the cluster's very first commit exists.

**Boot-path entropy/scheduling sensitivity (a real, generalizable
lesson).** The initial `ClusterProbe` round is sent from a **separate
spawned task** at boot, not inline before `drive`'s first `env.recv()`
(risks a mutual multi-node-genesis stall) and not deferred to the first
election-timeout tick either (the deferred version is what let the ADR
0060 growth race above happen at all under `SimEnv`'s near-zero latency).
`begin_cluster_check`'s own entropy draw is **reused from the same value
`RaftCore::new`'s own construction already drew**, never a fresh
`env.next_u64()` call, specifically because this branch runs on every
genesis node's boot and an extra draw there reshuffles the entropy
sequence for the rest of a `SimEnv` run — confirmed to desync two
unrelated fixed-seed corpus cells
(`chunked_snapshot_receiver_stop_restart_3`,
`transfer_third_voter_wins_the_election_a_transfer_armed_a_different_target_for`)
even though total draw counts ended up unchanged for the resolved-quickly
case (the extra spawned task and wire traffic alone can still perturb
`SimEnv`'s own deterministic tie-breaking between ready tasks). Both were
root-caused as pre-existing/orthogonal (one a genuine latent bug in
`InstallSnapshot` resume — fixed alongside, see below — the other a
seed re-pin, no bug) rather than papered over; see `docs/lessons/` for
the general "any boot-path behavior change can desync a hand-tuned fixed
seed even with zero logic bugs" lesson.

**An unrelated, pre-existing bug found and fixed in the same change**
(issue #899, discovered by the corpus desync above):
`handle_install_snapshot_resp`'s monotonic `max` guard on `snapshot_offset`
(added earlier to stop out-of-order acks regressing an in-flight
transfer) also swallowed a genuinely restarted follower's own honest
`next_offset: 0` reset, permanently deadlocking that follower's catch-up
— the leader kept re-sending chunks at its own stale, pre-restart offset
forever. Fixed by treating `next_offset == 0` as an authoritative reset
rather than folding it into the max. See `crates/animus-control/tests/
install_snapshot.rs`'s `leader_resumes_from_offset_zero_after_a_restarted_
follower_resets` for the regression.

**Scope: control plane only.** The CP data plane (`animus-cp-data`) reuses
this same `RaftCore`, and its own per-tablet driver has the identical
`state.is_empty()`-branches-on-recovery shape (`lib.rs`'s `fresh_group`) —
the same hazard exists there for a wiped data-plane voter. Not fixed in
this change: a tablet's peer set is dynamic and reconstituted constantly
(every `CreateTablet`, every split minting two fresh children, every
`reconfigure_step` replica move) in a way the control plane's
one-time-genesis config is not, so the same "wait for peers" mechanism
would add real per-tablet-genesis latency cluster-wide rather than a
one-time cluster-genesis cost — a materially different liveness tradeoff
needing its own design decision. Tracked as issue #900. **Operational
mitigation in the meantime, for both planes**: a data-directory volume
backing any control OR data-plane voter must be **persistent**, never
`storage.ephemeral: true`/an `EmptyDir` — see `crates/animus-operator/
CLAUDE.md`'s matching note.

Regression: `crates/animus-control/tests/wiped_voter_double_vote_safety.rs`
(the safety cell — hand-driven, seed-free, proves the double-grant is
refused and, in a chained scenario, that it would otherwise elect a second
leader in the same term); the three existing `wiped_voter_rejoin.rs`
scenarios (liveness after a legitimate rejoin, unaffected); a real
`ProdEnv` 3-node scenario in `tests/prod_liveness.rs` wiping a voter's data
directory and confirming both the refusal and that the other two nodes
keep serving.

### Follow-up amendment (2026-09-15, same day): single-peer evidence is not quorum evidence — a real bootstrap-race regression, found and fixed

The version above decided a refusal verdict the instant **any one**
configured peer answered `handle_cluster_probe` with real history that
named this node as an established voter. This is unsound for an ordinary
multi-node genesis bootstrap under real `ProdEnv` threading, not just an
adversarial edge case — found via `crates/animusd`'s own real-socket
`forward_to_tablet_leader_survives_a_dead_first_guess` flaking (~1/3 of
runs) after this fix landed, reproduced deterministically with a temporary
`tracing` subscriber capturing the actual decision. The captured trace
showed three of a genuine 4-node genesis founding committee **cascading**
into a permanent, wrong refusal: a real bring-up starts nodes
sequentially, and even a simultaneous start races real OS thread
scheduling, so a majority of founders can complete a real election among
themselves (term > 0, committed_index advancing) before every founding
peer's own probe round has finished exchanging with all three of its
peers. The slower founder's own honest evidence — a peer answering with
real history whose committed config names it — is then indistinguishable
from a genuine wiped-voter restart, since every genesis founder's config
contains every other founder from construction, whether or not it has
ever voted. The original design's own justification ("network latency <<
election_base, so every founding peer's probe round resolves before any
of them could legitimately start a real election") holds under `SimEnv`
(every node's clock starts at the same virtual instant) but not under
real threading, where founders do not start their own election clocks in
lockstep.

**Fixed** by requiring evidence from **every** configured peer (not the
first respondent) before ever committing to a refusal verdict, and adding
a structural veto: if any peer answers genuinely empty
(`term == 0 && committed_index == 0`), that peer has itself never
participated in anything, which disproves "this is a genuinely established
cluster" regardless of what any other peer reported (a truly established
cluster's surviving voters — the audience a wiped voter's vote would
actually contradict — would all already show real history). Only once
every peer has answered, none showed fresh state, and at least one named
this node as an already-established voter does the mechanism refuse
(`RaftCore::handle_cluster_probe_resp`'s own doc has the full decision
table; `cluster_check_saw_established_with_me`/`cluster_check_saw_fresh_
peer` are the two new tracked signals). The peer-not-recognizing-this-node
resolution (the ADR 0060 growth case) is unchanged — it was, and remains,
unambiguous on a single reply and does not need to wait for the rest of
the peer set.

**Residual, stated plainly**: this does not close every conceivable
timing — a scenario where two or more voters of the same cluster are
simultaneously wiped, one of them racing a still-genuinely-fresh peer,
could still resolve the wiped voter(s) as safe-fresh rather than refused.
This is a materially narrower and less likely scenario than "an ordinary
multi-node cluster occasionally bootstraps for the first time under real
scheduling," which the original design broke outright; a fully airtight
answer would need a durable, cross-node "has this exact identity ever
actually participated" record (e.g. in the replicated control-plane
`Metadata` itself, surviving the wiped node's own local disk loss) —
out of scope for this fix, and a candidate follow-up if the residual is
ever judged worth closing.

Also fixed in the same pass: `crates/animusd/src/sim_cluster_control_
growth.rs`'s two scenarios asserted `/admin/control/member/remove`
succeeded on the very next call after a real leadership transfer/election,
with no retry — `RaftCore::change_membership`'s own pre-existing erratum
guard (Raft §4/Ongaro, unrelated to this amendment) rejects a config
change until the new leader has committed a no-op in its own current
term, a genuine one-round-trip-after-election transient the transfer poll
immediately above it already retried on but the remove call did not; both
scenarios now retry on `409` exactly like the transfer poll does. A
`SimCluster`-driven fixed-seed test
(`sim_cluster_data_only.rs::c_crash_of_a_data_only_replica_holder_the_
rest_keep_serving_then_it_catches_up_over_seeds`) also needed its whole
seed list re-pinned — this amendment's own added network activity on the
genesis boot path reshuffles `SimEnv`'s entropy stream for every later
draw in the same run, the same collateral class documented in
`docs/lessons/testing/2026-09-15-boot-path-entropy-desyncs-fixed-seeds.
md`.

See `docs/lessons/code-patterns/2026-09-15-single-peer-evidence-is-not-
quorum-evidence.md` for the full incident and the generalizable lesson.

### Second follow-up amendment (2026-09-15, same day): the wait-for-every-peer fix above was still not enough — two further, real regressions found and fixed

PR #902 (carrying the amendment above) still failed the exact same shape
of CI check under real staggered bring-up
(`dynamo_txn_idempotency::same_token_same_fingerprint_retry_after_commit_
is_cached` and `dynamo_execute_transaction::
execute_transaction_over_a_follower_connected_node`, both "cluster did not
bootstrap in 20s"). Two independent, compounding bugs, found by tracing
the actual driver wake path and by a direct, cargo-bypassing repro loop
(the shared build tree's own concurrent-session contamination — see
`docs/lessons/testing/2026-09-15-shared-cargo-target-dir-phantom-method-
not-found.md` — was masking the real signal until isolated):

1. **`RaftCore::next_deadline()` never accounted for the cluster-check
   resend deadline at all.** The wait-for-every-peer fix above added a
   resend mechanism inside `tick()`, correctly decoupled from
   `election_deadline`'s own legitimate resets (`handle_append_entries`
   pushes it out on every valid leader contact) — but `node.rs`'s driver
   loop computes how long to sleep from `next_deadline()` alone, and that
   function returned only `election_deadline` for a non-leader. A still-
   checking founder that starts receiving ordinary heartbeats from an
   already-elected sibling could have its own driver oversleep past its
   own resend deadline for as long as `election_deadline` kept getting
   reset — the resend logic was correct, but `tick()` was never invoked
   at the right time to run it. **Fixed**: `next_deadline()` now returns
   `min(election_deadline, cluster_check_resend_deadline)` whenever a
   cluster check is pending. See `docs/lessons/code-patterns/2026-09-15-a-
   timer-driven-loop-must-wake-for-every-deadline-it-owns.md`.

2. **`config.contains(&self.id)` cannot distinguish a genesis race from a
   genuinely established restart, even with the wait-for-every-peer fix in
   place — this was the actual, dominant root cause.** A genesis config
   lists every founder from the very first committed entry onward, so
   `config.contains` is unconditionally `true` for every founder the
   instant ANY majority elects, regardless of whether that founder has
   ever voted. Waiting for every peer to answer (amendment above) only
   delays the identical wrong conclusion until every peer has
   independently raced ahead — which a real, CPU-starved, staggered
   bring-up does routinely, not rarely. **Fixed** by adding a genuinely
   new signal, `ever_heard_from_prober`, to `RaftMsg::ClusterProbeResp`: a
   peer now also reports whether it has ever itself received real,
   durably-forgettable-vote evidence from the asker — a self-vote
   (`handle_request_vote`), a vote we GRANTED it (`handle_vote_resp`,
   `granted: true` only, never a rejection), or proof it won a real
   election (`AppendEntries`/`InstallSnapshot` as leader) — tracked in a
   new per-core `heard_from: BTreeSet<NodeId>`. `handle_cluster_probe_resp`
   now resolves immediately, safely, the instant any single peer answers
   `ever_heard_from_prober: false`, alongside the pre-existing fresh-peer
   and config-membership decisive signals. A genuinely established voter's
   surviving peers keep answering `true` for as long as they keep
   running (they really did exchange real votes/appends with it before
   the wipe), so this never weakens the original safety property — it
   only adds a THIRD way to resolve "safe" faster and more precisely,
   closing exactly the gap the first amendment's own "Residual, stated
   plainly" paragraph anticipated (a durable, or in this case in-process,
   "has this identity ever actually participated" signal). See
   `docs/lessons/code-patterns/2026-09-15-config-membership-cannot-
   disambiguate-a-genesis-race-from-an-established-restart.md` for the
   full incident, including a real bug found and fixed in the fix itself
   (an earlier, broader version of `heard_from` wrongly counted this
   node's own outbound vote REJECTIONS as evidence of the rejected
   candidate's participation, reproducing the exact false refusal this
   amendment exists to close).

**New coverage**: `animus-control/tests/next_deadline.rs::
next_deadline_wakes_for_a_cluster_check_resend_even_after_election_
deadline_is_pushed_out` (fix 1, confirmed red-before-green) and a new
file, `animus-control/tests/staggered_genesis_boot.rs` — a deterministic,
hand-driven 3-node genesis bootstrap with staggered starts and lost/
delayed cluster-check probes, asserting convergence to a single leader
with no false refusal within a bounded step budget (fix 2, confirmed
red-before-green against `ever_heard_from_prober` specifically). Both
fixes were additionally verified directly against the two real,
previously-flaking `ProdEnv` binaries named above, run standalone outside
`cargo test` to avoid the shared-target-dir contamination that was
masking the signal: 30/30 and 40/40 clean runs respectively.

**Residual, updated**: the two-or-more-simultaneous-wipes edge case named
in the first amendment's own residual paragraph is narrowed further by
this fix (a wipe racing a still-genuinely-fresh peer now also needs the
surviving peers to have never witnessed the wiped identity vote, which is
a strictly rarer combination than before) but not eliminated — the
`ever_heard_from_prober` signal is deliberately in-memory, not
WAL-persisted (a responder that itself restarts, recovered rather than
wiped, forgets it until the asker sends another real message), so a
coordinated whole-cluster restart racing a single voter's disk wipe is
still not fully covered. This is unchanged from before either fix in this
amendment — no prior mechanism covered it either — and remains a
candidate follow-up, not a regression introduced here.

## Amendment (2026-09-19): a just-granted real vote had no pre-vote lease of its own — issue #930

**The problem.** The pre-vote leader lease (above) was `leader_id.is_some()
&& now < election_deadline` — but `leader_id` is set only by
`handle_append_entries`/`InstallSnapshot`/`become_leader`, never by
`handle_request_vote` on a granted real vote. A voter that had just granted
a real vote to the term's eventual winner therefore had `leader_id == None`
— no lease at all — for the whole window between casting that vote and the
winner's first `AppendEntries` actually arriving. If a *different* voter's
own election timeout fired inside that window (plausible under real
scheduling jitter or network degradation, never mind a deliberate
partition), it could win a **pre-vote** round against these unprotected
voters, then a **real** election (real votes have no live-leader gate at
all — only pre-vote does), deposing the just-elected leader in a "return
bout" that could occasionally chain. Not a safety violation — every step
still followed Raft's term/log rules — but a liveness/stability defect.
Found and recorded (not yet fixed) while authoring
`animusd/src/sim_cluster_control_membership_admin.rs`'s issue #923
regression; see `docs/lessons/code-patterns/2026-09-16-a-voter-that-just-
granted-a-real-vote-has-no-pre-vote.md` for the original incident.

**The fix.** `handle_pre_vote`'s lease now also covers a granted real vote,
gated by role:

```rust
let voted_lease = matches!(self.role, Role::Follower | Role::Candidate)
    && self.voted_for.is_some()
    && now.0 < self.election_deadline.0;
let has_live_leader = self.role == Role::Leader
    || (self.leader_id.is_some() && now.0 < self.election_deadline.0)
    || voted_lease;
```

A granted real vote is a per-term commitment to this term's likely winner
(Raft's own vote-splitting safety already relies on it), and granting it
already reset the granter's own `election_deadline`
(`handle_request_vote`) — so `voted_for.is_some()` is exactly as
trustworthy a lease signal as `leader_id.is_some()`, for that same window.

**The role gate is load-bearing, not incidental — a naive, role-less
`voted_for.is_some() && now < election_deadline` was tried first and
deadlocked the most ordinary recovery case there is.** After a leader
crashes, every surviving follower already has `voted_for = Some(<the dead
leader>)` for the still-current term (that vote is how the leader got
elected), and `voted_for` is never cleared by a mere timeout — only a
higher term clears it, being a durable per-term commitment. Meanwhile
`start_pre_vote` (the handler for a node's own election timeout) resets
`election_deadline` on every pre-vote round it starts, forever, for as
long as no majority is reached — a purely local retry cadence, unrelated
to any vote. With no role gate, that stale vote for the now-dead leader
combined with that perpetually-refreshed deadline to make every survivor
believe it still had a live leader for as long as it kept timing out into
fresh rounds — i.e. forever — so no survivor would ever grant another's
pre-vote and the cluster could never re-elect. Caught immediately by the
pre-existing `election_still_succeeds_when_leader_is_gone` test going from
green to red under the naive draft. `RaftCore::tick`'s `Follower |
PreCandidate | Candidate` arm moves a timed-out `Follower`/`Candidate` to
`PreCandidate` in the same step its own `election_deadline` lapses, so
gating the lease on role makes it expire at exactly that transition — a
`PreCandidate` gets no protection from a `voted_for` it can no longer
vouch for (only `leader_id`, already cleared the moment it starts
campaigning itself).

**Two consequences, not bugs.** (1) A `Candidate` sets `voted_for =
Some(self)` atomically with a fresh `election_deadline` in
`start_election`, so it is never stale — it now rejects a competing
pre-vote for the rest of its own election deadline instead of granting it
(the pre-fix behavior, since `leader_id` stays `None` for a whole
candidacy). This is standard pre-vote behavior and reduces dueling
candidacies rather than causing any. (2) `voted_for` is persisted and a
recovered `RaftCore` always restarts as `Follower`
(`RaftCore::recovered`), so a node that restarts with a vote already
recorded for its current term refuses pre-votes for one full,
freshly-randomized election-timeout window after restart, even though it
has no live-leader belief of its own yet — a one-time, bounded startup
cost (at most one election timeout, ~150–300ms), not a recurring one.

**Coverage**: `animus-control/tests/pre_vote.rs`'s
`prevote_rejected_after_granting_a_real_vote_until_deadline` (core-level,
confirmed red before this fix and green after) and
`prevote_rejected_by_a_candidate_within_its_own_election_deadline`
(consequence 1, also confirmed red-before/green-after). A cluster-level
`SimEnv` reproduction of the full "return bout" race (a directed transfer
plus a brief total freeze of the new leader, swept over seeds 0..1000 at
a freeze duration inside the election-timeout band) was attempted but
abandoned: at freeze durations long enough to reliably reproduce a
disruption, deposals turned out to occur at statistically indistinguishable
rates before and after this fix (~8% either way), because in a 5-voter
group multiple followers routinely cross their *own* natural timeout
independently and duel each other directly — a scenario this fix
correctly leaves alone (once a follower's own deadline lapses it is a
`PreCandidate`, unprotected in both the old and new code, by design).
Isolating the fix's specific effect at the cluster level — one lone
early-timing-out voter needing exactly one still-protected voter's grant
to reach majority — needs a scenario at least as tightly engineered as
`transfer_third_voter_wins.rs`'s own (an exhaustive seed scan against a
hand-picked topology), which was judged out of scope for this change; the
core-level tests above pin the mechanism directly and exercise the exact
code path the cluster race depends on.
