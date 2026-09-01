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
  `(state, message, now, entropy)`. No clock, no `HashMap`, no I/O.
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
without bumping the term) and end-to-end under `SimEnv` (an isolated follower's
pre-vote rounds do not move the stable leader's term, and it rejoins on heal with
no election; a genuine leader crash still elects a new leader at a higher term).
The pre-existing hand-driven election tests (`follower_visibility`,
`install_snapshot`, `driver_applied_sm`) now drive the pre-vote round explicitly.

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
