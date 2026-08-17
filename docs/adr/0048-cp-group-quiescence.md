# ADR 0048 — CP-group quiescence (phase 1)

- **Status:** Accepted — implemented (PR1–PR7 of this stack).
- **2026-08-16 note (later the same day):**
  [ADR 0050](0050-per-tablet-storage-copy-based-splits.md) (accepted, in
  delivery) leaves quiescence itself untouched — a retired split parent's
  group is *removed*, strictly cheaper than quiesced — but retires this
  ADR's `hot_read` scope-transition latch together with the residual it
  narrowed (immutable tablet ranges leave no scope transition to latch),
  and adds the storage-side sibling of this ADR's concern: the idle cost of
  a per-tablet engine, measured as a gating item in 0050's first rung.
- **Date:** 2026-08-16
- **Amends:** [ADR 0017](0017-per-tablet-raft-data-plane.md) (the per-tablet
  Raft data plane); closes [ADR 0044](0044-split-only-tablets.md)'s
  cheap-groups-roadmap follow-up 1 ("Quiescence"); narrows the
  [ADR 0043](0043-stream-shard-subsystem.md) §A7/A7b `hot_read`
  scope-transition residual to a strictly smaller, accepted sub-window
  (see "The `hot_read` scope-transition latch," below).
- **Depends on:** ADR 0009 (in-house Raft over `Env`), ADR 0012
  (heartbeat-based failure detection), ADR 0031 (the per-node tablet-host
  reconciler).

## Context

ADR 0044 made tablets split-only: every split permanently grows a table's
group count, and nothing ever shrinks it back down. That ADR's own
"cheap-groups roadmap" named quiescence as the first, largest win (an
estimated ~80% of the per-group idle cost) and left it as an unscheduled
follow-up: *"today every hosted tablet group ticks forever regardless of
load."* A verification pass ahead of this stack (session `4a4368f2`,
2026-08-15) confirmed the shape of the problem and corrected its scope:

- **The largest per-group idle timer is not the Raft heartbeat — it is the
  apply task's 5ms poll.** `apply_loop`'s old `APPLY_IDLE_POLL` woke every
  hosted group ~200 times/second regardless of activity, versus ~20-40/s
  from the leader's heartbeat and inbound-message wakes. ADR 0044's
  follow-up named only "election/heartbeat timers"; the apply poll was the
  larger, cheaper-to-fix half (no consensus semantics involved) — this ADR
  corrects that scope on ADR 0044 itself (see its own text, amended above).
- **Animus already has the precondition that makes quiescence sound.**
  Unlike CockroachDB (which needs node-level epoch leases to detect a dead
  leaseholder without per-range traffic) and TiKV (whose hibernate-regions
  shipped without that precondition and needed store-level failure-driven
  wakeups retrofitted afterward), Animus's ADR 0012 failure detector is
  already a **node-level** liveness layer, wholly independent of any
  tablet group's own Raft stream (`heartbeat_loop`/`heartbeat_loop_live`
  ride the control group's `PRIMARY_STREAM`, a tablet group rides its own
  `stream = tablet_id`). So quiescing a tablet group cannot be mistaken for
  node failure, and the tablet-host reconciler is a ready-made proactive
  wake trigger for "the leader died while we were dormant" — TiKV's bug,
  closed by construction here rather than bolted on later (fork H, below).
- **Animus has no lease reads, only ReadIndex.** Every linearizable read
  goes through `read_barrier`, which probes the *current* voter config and
  needs a majority of same-term acks — a quiesced leader serves this with
  one probe round and no un-quiescing at all, and a usurped leader's probe
  simply fails. The classic quiescence-vs-stale-lease-read hazard both
  CockroachDB and TiKV have to reason carefully about does not exist here.

## Decision

Quiescence lives in the **shared sync core** (`animus-control::RaftCore`,
generic over the command type) as an opt-in mode, so the decision stays
pure, clock-free, and `SimEnv`-drivable, and the control plane is
untouched by construction (fork G). `RaftCore` gains `quiesce_after:
Option<Duration>` (default `None` — today's behavior exactly, byte-for-byte),
`quiesced: bool`, `last_activity: Nanos`, and one new `RaftMsg::Quiesce {
term, commit_index }`. `next_deadline()` returns `Option<Nanos>` — `None`
while quiesced — so both the control-plane and CP-data drivers drop the
timer arm from their `select` entirely when it's `None`: a quiesced group
posts **zero** timeline events under `SimEnv`, genuine, observable
quiescence, not a degenerate busy-loop.

### The core state machine (PR3)

**Leader-side entry predicate** (`RaftCore::quiesce_entry_ok`, pure,
re-evaluated at every heartbeat deadline): no local activity for
`quiesce_after`; `commit_index == last_log_index` and `commit_index >=
first_term_index()`; every voter's `match_index == last_log_index`; no
transfer/departing-peer/config-change in flight; no snapshot machinery
pending in either direction; and two external inputs a `DRIVER_APPLIED`
driver feeds in once per loop iteration — `quiesce_engine_caught_up` (the
async apply task has actually merged everything the core thinks is
applied) and `quiesce_veto` (fork D, PR5). On satisfying it the leader
broadcasts `Quiesce` once and sets its own flag. **The leader stays
leader** (fork A) — every background sweeper gates on `is_leader()`, so a
step-down design would silently starve the seal arm, GSI drain,
`txn_resolver_loop`, and `auto_split_loop`.

**Follower-side**: accept `Quiesce` only if `term == current_term`, `from
== leader_id`, and the follower's own `last_log_index == commit_index ==
msg.commit_index` — proof this follower is caught up to *exactly* the
state the leader broadcast from. Otherwise ignored outright; the follower
keeps ticking, and its own ordinary election timeout is what notices if
the leader really is gone.

**Un-quiesce triggers**, exhaustively: any inbound Raft message; any local
propose (`put`/`change_membership`/`transfer_leadership`/`propose_seal`/
`narrow_scope`); `shutdown()`; an explicit `RaftKvNode::wake()`; a
`read_barrier` on the leader. The wake plumbing reuses `ProposeSignal`'s
exact shape (`AtomicBool` + `futures::task::AtomicWaker`) — executor-
agnostic, so no tokio-only primitive sneaks into a `SimEnv`-tested path.

**Fork B — locally-woken follower disambiguation**: a quiesced follower
that is locally touched (`RaftKvNode::wake()`) doesn't blindly wait out a
stale election timer. `on_local_wake` re-arms a **full fresh** election
timeout and, if it has a recorded leader, sends `RaftMsg::WakeRequest`
directly; the leader answers with an ordinary replicate (heartbeat or
catch-up) if still leader, otherwise nothing, and the follower campaigns
only if the fresh timeout actually expires unanswered. This avoids the
naive "pure timeout" alternative's cost: deposing a healthy quiesced
leader (a term bump + churn) on the very first touch of every cold
tablet — exactly the workload this feature exists for.

**Fork C — where the decision lives, and the pre-vote interaction**: pure
`RaftCore`, opt-in `quiesce_after: Option<Duration>`. `election_deadline`
is left **stale** while quiesced rather than set to infinity —
`handle_pre_vote`'s existing lease check (`has_live_leader = role ==
Leader || (leader_id.is_some() && now < election_deadline)`) then does the
right thing (grant a pre-vote to a locally-woken candidate) with zero
change to that safety-relevant path, confining the whole feature to
`next_deadline()`/`tick`/`handle`.

Quiescence state is **volatile, never persisted** — same doctrine as
`last_contact`/`match_index`. A restarted node starts as a ticking
follower and wakes its group naturally.

### Wake-on-demand (PR4)

`ClientCtx::resolve_cp_route` (the edge's routing-resolution entry point)
calls `wake()` on a local group handle before deciding anything — cheap,
unconditional, and a no-op on every state that isn't "quiesced follower
checking in." This is what turns "the group has quiesced" into "the group
resumes on first touch" for an ordinary client request.

**Fork H**: the per-node tablet-host reconciler (`Reconciler::tick`)
proactively wakes any hosted group whose replica set intersects the
failure detector's `down` set. Without this, a quiesced follower whose
leader genuinely died while both were dormant has **nothing** that will
ever wake it — no timer at all, no client traffic (nobody is touching a
cold tablet), a strictly worse availability story than before quiescence
existed. This is precisely the bug TiKV had to retrofit a fix for;
Animus's node-level failure detector (ADR 0012) already gives the
reconciler everything it needs to close it by construction, ~10 lines.

### The `hot_read` scope-transition latch (PR4, narrowing the ADR 0043
residual)

**The decided scope, restated:** ADR 0043 identified a genuine,
narrow-but-real fabrication class — a live `GetRecords` consumer polling
a tablet's open hot tail during the exact window between a `SplitTablet`
commit and this node's own reconciler locally executing `narrow_scope`
could observe a record that later re-appears, under a **different**
`eventID`, once the split-off sibling correctly seals it. `hot_read` has
no `ReadIndex` barrier by design (F8) and no apply-time backstop is
possible on a pure read path, so ADR 0043 deferred closure to this phase's
work rather than attempting it inline.

**As shipped**: `hot_read_scope_ok` (`animusd`) refuses retryably (the
crate's established `"...; retry"` shape) whenever a group's **live**
`RaftKvNode::scope_range()` is strictly wider than the tablet's range per
a **freshly fetched** `metadata_fresh()` snapshot. Both call sites
(`ClientRequest::StreamHotRead`'s handler and `ClientCtx::
read_stream_hot_records`'s local branch) now source their `Metadata` from
`metadata_fresh()` instead of `effective_metadata()`/`metadata_cached()`
— exactly the "possibly-stale mirror" this crate's own documented
discipline already names as the wrong tool for a permanent decision — and
gate on it before ever calling `index_drain::hot_read`.

**Why this is sound where a naive reading of "reconciler-maintained
latch" would not have been.** The literal plan language asked for "a
reconciler-maintained per-group scope-transition latch." A latch computed
purely from the reconciler's own tick cadence — set when a tick's
`gather_facts` first notices a scope mismatch, cleared once `narrow_scope`
executes in that same tick — cannot close the window that matters most:
the interval **before** the reconciler's next tick even runs (bounded by
`metadata_watch` wake latency plus, in the worst case, the 500ms fallback
poll), during which the reconciler has not yet observed the split at all
and so cannot raise any flag. `RaftKvNode::scope_range()` itself has no
such lag — it is a live read of exactly the value the reconciler mutates,
current the instant `narrow_scope` runs and unchanged otherwise — so
cross-checking it against the **freshest obtainable** declared range (not
the reconciler's last-observed one, and never a cached/mirrored one)
narrows the residual to a strictly smaller sub-window than a purely
tick-cadence-bound flag would leave open, with no new shared mutable
state to reason about.

**What this narrows, but the accepted sub-window that remains — the
identical layer-2 structure the #220 write-side investigation found.**
`metadata_fresh()` is only as fresh as its own source. For a
`ControlHandle::Local` node (every combined node — the common deployment
shape), `metadata_fresh()` resolves to `raft.metadata()`: the ADR 0038
published cache a **local, asynchronous control apply task** maintains,
decoupled from the control Raft's own commit index. In the sub-window
between a `SplitTablet` actually committing and this node's own apply
task catching its published cache up to it, the declared range in `meta`
and the live scope are stale **together** — `hot_read_scope_ok` sees no
mismatch (both still reflect the pre-split width) and passes, so a
hot-read can still observe the fabrication class in this narrower window.
This closes: (a) an ADR 0030 mirror's own (much longer) refresh-interval
staleness on a data-only/growth (`Remote`) node, where `metadata_fresh()`
performs a genuine round trip instead of trusting the mirror; and (b) the
window where this node's cache has already observed the split but its
reconciler has not yet ticked `narrow_scope`. It does **not** close: (c)
the commit-to-local-cache-apply lag itself, which exists on every node
regardless of `ControlHandle` variant. Full closure of (c) would need a
per-read control-leader round trip on every `hot_read` call — the read-
side analogue of the per-write round trip the #220 analysis already
rejected as disproportionate for the identical reason (this is a
leader-local, no-`ReadIndex`-barrier hot path by design, F8; adding a
control-plane round trip to every call defeats the point). The accepted
remaining exposure is bounded to the control apply task's own catch-up
latency (milliseconds under normal load), a categorically smaller window
than the reconciler's own tick cadence this latch already closes.

**Evidence**: the D8 historical adjudicator
(`auto_split_mid_stream_with_live_consumer_across_every_node`) — which had
previously shown the fabrication signature (distinct `eventID`s, same
item, same packed-HLC suffix) at a rate not clearly distinguishable from
noise even after the write-side range-fence CAS fix (5/15 → 4/15 across
one measurement) — is green across 10 consecutive iterations after this
latch, modulo the separately-documented cascading-third-split harness
limitation this test's own doc already names. D8 remains the live
adjudicator for the accepted remaining sub-window above, not just a
historical regression check: a future distinct-`eventID` failure there
should be read against the apply-lag window first, before assuming the
pre-latch bug has recurred wholesale.

### Quiesce vetoes (PR5, fork D)

A subsystem-held flag, not a core-internal scan (scanning an async engine
from inside the pure sync core would violate the sync-core/async-driver
split ADR 0009 established). Two holders, ORed together once per
consensus-loop iteration:

- **In-crate**: a non-empty `TxnTracker` (`pending`/`unresolved_decided`)
  always vetoes on its own — a group with a live 2PC intent or an
  undelivered resolve must never go dormant out from under
  `txn_resolver_loop`.
- **External**: `RaftKvNode::set_quiesce_veto(bool)`, held by `animusd`'s
  `change_consumer_loop` for a led tablet whose change log
  (`pending_changes()`) was non-empty on its last sweep, released the
  instant a sweep finds it empty.

This is the invariant PR6 depends on: "quiesced ⇒ this group's `TxnTracker`
and change log are both empty" is true by construction, not by
observation.

### Sweeper skip (PR6, the fleet-scale CPU win)

With PR5's invariant in hand, `change_consumer_loop`, `txn_resolver_loop`,
and `auto_split_loop` (`animusd`) skip a led, quiesced tablet outright
rather than merely finding nothing to do:

- `change_consumer_loop`/`txn_resolver_loop`: sound directly from PR5's
  invariant.
- `auto_split_loop`: a quiesced group's bytes/key-count are static (no
  activity for `quiesce_after` means no new writes since it last
  quiesced), so whatever its last pre-quiescence tick already checked
  still holds; skipping introduces no missed threshold crossing.

**This is where the actual fleet-scale CPU win lands.** PR5 alone only
stopped pointless Raft timer/heartbeat/apply-poll activity (the core
state machine's own win); the per-tablet LSM scans and materializations
these three loops perform are what actually costs CPU at scale, and only
PR6 removes them for an idle tablet. The skip is a strict, reversible
short-circuit: any write un-quiesces the group via the pre-existing
propose-wake plumbing, so the very next tick resumes normal sweeping.

### Observability + production wiring (PR7)

- `Metric::CpQuiesces`/`CpUnquiesces` (counters, incremented on every
  genuine transition the consensus loop observes) and
  `Metric::CpGroupsQuiesced` (a level, sampled once per `metrics_sample_
  loop` tick across a node's currently-hosted groups — the identical
  "counter slot re-purposed as a last-write-wins level" shape
  `StreamHotBytes`/`StreamSegmentsLive` already use).
- `quiesced: bool` on `/admin/raftkv`'s `CpRaftView` and the Console
  Tablets view (a neutral "quiesced" pill, the `.forming` style — an
  informational diagnostic, never a health/data-risk signal per ADR
  0021 §7's own rule). **Fork F**: admin/dashboard reads never wake a
  group — `CpGroup::is_quiesced()`/`RaftKvNode::is_quiesced()` are pure
  frozen-accessor reads, so an open browser tab polling the dashboard
  every few seconds cannot un-quiesce a fleet. The flag exists
  specifically to let an operator confirm the feature is working.
- `--quiesce-after SECS` (`animusd`, threaded through `--config`/`--node`
  and `--cluster N`): **defaults ON at 5 seconds.** See the flag's own doc
  (`main.rs::DEFAULT_QUIESCE_AFTER_SECS`) for the full evidence and
  caveats behind this default — restated in "Consequences," below, since
  it is the one call in this ADR that is explicitly a maintainer-
  reviewable judgment, not a settled fact.

## Fork decisions (summary table)

All eight forks were decided in the maintainer's 2026-08-16 fork review
session, as recommended by the verification pass; this ADR records the
as-built outcome of each.

| Fork | Question | Decision |
|------|----------|----------|
| A | Does a quiesced leader remain leader? | Remain leader — a step-down design breaks every `is_leader()`-gated background loop. |
| B | How does a locally-woken follower tell "quiesced leader" from "dead leader"? | Explicit `WakeRequest`, not a bare timeout — avoids deposing a healthy quiesced leader on every cold tablet's first touch. |
| C | Where does the decision live? | The pure `RaftCore`, opt-in `quiesce_after`; `election_deadline` stays stale (not infinite) so `handle_pre_vote` needs no edit. |
| D | Who can veto quiescence? | An explicit subsystem-held flag (in-crate `TxnTracker`, external `change_consumer_loop`), never a core-internal engine scan. |
| E | Ship the apply-loop fix (PR1) separately/first? | Yes — most of the measurable win, no consensus semantics, independently defensible. |
| F | Do admin/dashboard reads wake groups? | No — frozen accessors only; surface `quiesced` as the diagnostic instead. |
| G | Does the control-plane group quiesce in phase 1? | No — scoped to data-plane groups; the control group's own loops tick regardless and there is one group per node, nothing to amortize. |
| H | Proactive wake from the node-level detector? | Yes, in PR4 — ~10 lines in the reconciler, closing the one genuine regression this feature could otherwise introduce (TiKV's own retrofitted bug). |

## Phase-2 handoff constraints

Phase 2 (heartbeat coalescing across co-hosted groups) is **not** part of
this stack. Phase 1 was built to hand it a clean seam:

- **Decision in the core, transport in the driver.** Phase 2 replaces
  per-group `AppendEntries`/`Quiesce` carriage with one batched per-node-
  pair message; that must stay a driver/codec change only, with the core's
  per-group predicate (`quiesce_entry_ok`) untouched.
- **Don't let the wake protocol assume a per-group stream.** Today
  `stream = tablet_id`; a coalesced heartbeat needs a node-level stream
  (precedent: `SEGMENT_STREAM = u64::MAX` in `cluster_segment_store.rs`).
  `Quiesce`/`WakeRequest` stay ordinary `RaftMsg`/`KvWire` variants a
  future batcher can *wrap*, adding no liveness fact derivable only from a
  group's own stream.
- **Phase 1 hands phase 2 a per-node group registry with a uniform wake
  handle.** `ClusterEdgeState::hosted_groups()` is that registry; PR4's
  `wake()` is that handle. A coalesced inbound message must demux into `N`
  groups by tablet id, which is exactly this map.
- **"Quiesced" must not mean "unreachable."** Phase 2's coalesced
  heartbeat will carry commit indices for co-hosted groups including
  quiesced ones; a quiesced group must always process inbound Raft
  traffic — quiescence removes only *timers*, a rule this phase's own
  `handle`/`witness_append_entries` already honor unconditionally (every
  inbound message un-quiesces, full stop, never a "quiesced groups refuse
  messages" special case).

## Consequences

- ADR 0044's follow-up 1 ("Quiescence") is closed; its own text is amended
  to record the apply-poll scope correction and point here.
- ADR 0043's `hot_read` residual is **narrowed, not fully closed**: this
  latch closes the reconciler-tick-cadence window and the ADR 0030 mirror
  window, but a strictly smaller sub-window (this node's own control apply
  task's commit-to-cache-apply lag, milliseconds under normal load)
  remains accepted — full closure would need a per-read control-leader
  round trip, rejected as disproportionate for the same reason the #220
  write-side analysis rejected the equivalent per-write trade. That ADR's
  own text now cross-references this latch design and the D8 evidence
  above, including the accepted remaining window.
- ADR 0044's "one shared engine and WAL per node" claim (~line 155) is
  corrected: the engine is shared (ADR 0028); each group still holds its
  own Raft WAL file, `SharedWal` remains built-but-unwired.
- **The `--quiesce-after` default-ON call is explicitly flagged for
  maintainer review, not presented as settled**: the mechanism and every
  wake/veto/skip path built on it are exercised by a seed-reproducible
  `SimEnv` corpus at depth, the real-thread `ProdEnv` leader-kill liveness
  regression (the one property `SimEnv` structurally cannot prove), and
  this crate's full existing `ProdEnv` integration suite (auto-split, 2PC
  transactions, DynamoDB Streams end to end, the D8 adjudicator) all
  passing unmodified with quiescence wired in — no destabilization was
  found. What was **not** separately validated: a large (dozens-to-
  hundreds of tablets) fleet under sustained mixed load with real
  inter-process network latency, the one instrument the pre-work
  verification pass's own "open risk" section named (a `--cluster 3
  --auto-split-bytes`-manufactured ~50-tablet fleet, diffing `GET
  /metrics` over a 60s idle window before/after). If a future deployment's
  own soak testing finds 5s too aggressive, the fix is lowering the
  constant or defaulting to `0` — never the mechanism itself, which is
  correct at any threshold `> 0`.
- Phase 2 (heartbeat amortization, roadmap item 2) remains unscheduled;
  the constraints above are what this phase leaves it to build against.
  Roadmap items 3 (asymmetric replicas) and 4 (fleet-scale amortization)
  are unaffected by this stack.
- Updated alongside this ADR: the ADR index, `animus-cp-data/CLAUDE.md`
  and `animusd/CLAUDE.md` (the new mechanisms/knobs), root `CLAUDE.md`'s
  knob table (no new env var — `--quiesce-after` is a CLI flag, not a
  `ANIMUS_*` test knob), and `docs/engineering-lessons.md`.
