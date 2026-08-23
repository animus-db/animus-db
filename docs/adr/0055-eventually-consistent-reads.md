# ADR 0055 — `ConsistentRead` becomes real: eventually-consistent reads by default, served from any replica

- **Status:** Proposed
- **Date:** 2026-08-23
- **Amends:** [0041](0041-materialized-secondary-indexes.md) §5 (the
  accept-and-ignore decision), [0017](0017-per-tablet-raft-data-plane.md) §3
  (adds a second read mode beside ReadIndex), [0048](0048-cp-group-quiescence.md)
  (a read that must not wake a group)
- **Depends on:** [0016](0016-pluggable-replication-per-tablet-raft.md),
  [0018](0018-cross-tablet-transactions.md) (the read-timestamp cache and
  committed ceiling this mode deliberately does not touch),
  [0050](0050-per-tablet-storage-copy-based-splits.md) (`Building` children)

## Context

Every client read AnimusDB serves today is linearizable, whatever the client
asked for. ADR 0041 §5 settled `ConsistentRead` as **accept-and-ignore** on
exactly that ground — "accepted-and-ignored against an LSI `Query`, a base
`Query`/`Scan`, and a base `GetItem` alike, since every one of those reads is
already linearizable here regardless of what the client asked for" — with the
sole rejection being `ConsistentRead=true` against a GSI, which DynamoDB also
rejects.

That was a defensible place to stop while the CP data plane was being built. It
is the wrong place to stay, for three reasons.

**1. The default read is the expensive one, and DynamoDB's is not.** A
DynamoDB client that does not ask for `ConsistentRead` is asking for the cheap,
eventually-consistent read, and is billed half as much for it. AnimusDB charges
the halved capacity (`read_units(bytes, consistent)`) while doing the full
linearizable work — the one place the wire adapter's fidelity is a promise the
engine does not keep.

**2. The cost is larger than "one round trip."** `linearizable_get_served` does
three things, only the first of which is the ReadIndex barrier:

- `read_barrier()` — the quorum probe (ADR 0017 §3);
- `ensure_ceiling_above(ts)` — which, when the committed ceiling does not
  already cover the read's mint, **proposes a `ReadCeiling` Raft entry and waits
  for it to commit**. A read can become a write. It is amortized over a margin,
  but it is on the read path;
- `ts_cache.bump(..)` — recording the read's timestamp so a later write to the
  key is pushed above it.

The second and third are ADR 0018 transaction machinery, not linearizability.
An eventually-consistent read has no position in the serialization order, so it
correctly skips both — and that, rather than the heartbeat round, is where most
of the saving is.

**3. `Scan` pays per tablet for a guarantee it does not deliver.**
`cp_scan` fans out across the tablets overlapping the scan window and
`cp_scan_local` takes a *separate* ReadIndex barrier on each
(`linearizable_scan`). An N-tablet `Scan` therefore pays N quorum round trips
and still is not a cross-tablet snapshot — each tablet barriers at its own
instant. The guarantee is per-tablet either way; only the bill is per-tablet
too.

**What the ADR record does and does not already say.** ADR 0016 (§, the
two-plane design) proposed a per-table `consistency: eventual | linearizable`,
but "eventual" there meant *the leaderless AP data plane*, which ADR 0019
deferred and deleted. No surviving ADR designs an eventually-consistent read.
ADR 0017 §3 rules **leader leases** off the table until `SimEnv` grows per-node
clock skew and pause injection, and names linearizable **follower reads** (a
follower asking the leader for `readIndex`) as a natural later extension. This
ADR is neither of those; see "This is not a lease" below.

**The oracle came first, deliberately.** Until PR #357 this codebase had no
test that could tell a linearizable read from a stale one: `raftkv_linearizable`
asserted `check_cycles`, whose data-dependency edges structurally cannot place a
read-only transaction on a cycle in a one-mop-per-transaction workload.
`check_strict_cycles` (real-time edges) closed that, and the frozen corpus is
green on it. Introducing a second, deliberately weaker read mode before that
oracle existed would have made every future failure unattributable.

## Decision

### 1. Two read modes, and which reads may choose

`ConsistentRead` becomes load-bearing on the base-table read path:

| Read | Mode |
|------|------|
| `GetItem`, `BatchGetItem` (`ConsistentRead` absent/`false`) | **Eventual** |
| `Query`, `Scan` on a base table or LSI (`ConsistentRead` absent/`false`) | **Eventual** |
| the same with `ConsistentRead: true` | **Linearizable** (today's path, unchanged) |
| `Query`/`Scan` against a **GSI** | **Eventual**; `ConsistentRead: true` still rejected (`ValidationException`), per ADR 0041 §5 |

**Not client-selectable — these stay linearizable regardless of any flag, and
the flag must never be threaded into them:**

- the **conditional-write / RMW read** (`raw_quorum_read` behind the write
  path's condition gate). Its own comment records the bug that made it
  linearizable: two racing `attribute_not_exists` puts both "succeeding". A
  write's correctness is not a client's latency preference. (Under ADR 0054,
  should it land, condition evaluation moves into apply and this read's role
  changes shape — but not its consistency requirement.)
- `TransactGetItems` and `TransactWriteItems` condition checks — ADR 0018
  snapshot semantics.
- every **internal** read: the ADR 0041 index drain, the ADR 0045 backfill
  seeder, the ADR 0051 TTL reaper, the ADR 0050 split copy driver, the ADR 0043
  segment janitor. These have their own consistency requirements, none of which
  is "whatever the client asked for."

### 2. What "eventual" promises, precisely

Matching DynamoDB, and stated as what a client may **not** assume:

- **No read-your-own-writes.** An acked write may not be visible to the client's
  own next eventual read.
- **No monotonic reads.** Two successive eventual reads may go *backwards* —
  see §6, which is the real cost of serving from any replica.
- **No cross-key or cross-tablet atomicity** (already true of `Scan`).
- Staleness is bounded only by replication + apply lag; there is no promised
  bound and none should be documented, because a partitioned replica's lag is
  unbounded until the freshness gate (§4) removes it from service.

What it **does** promise, and what the implementation must not weaken:

- **Committed-and-applied only.** A read observes `min(commit, durable)` state,
  never an uncommitted or unapplied value. `applied ⊆ committed` is ADR 0017's
  own invariant and this mode inherits it, so the worst outcome is a **stale
  value**, never a fabricated, torn, or rolled-back one.
- **Never a phantom absence for a live key.** See §5.

This is the same argument shape ADR 0042 §7 already accepted for barrier-free
`GetRecords`, and it is worth being precise about where it stops transferring.
That section leaned on three properties: the change log is **append-only**,
**positional** (an iterator names an exact HLC, not "the latest state of key
K"), and committed-and-applied. A base-table row has only the third. It is
mutable and addressed by key, so an eventual base read can return a value that a
later read supersedes *and* one that a later read contradicts. §7's reasoning is
precedent for skipping the barrier; it is not a proof that base reads are as
benign as stream reads. They are not, and §6 records the difference rather than
inheriting a guarantee that was never argued for this case.

### 3. Routing: any replica, this node first

Eventual reads are **not** routed to the leader. `decide_cp_route`'s inputs
already include `has_local_replica`; the eventual path gets a sibling decision
that returns `Local` whenever this node hosts *any* serviceable replica of the
target tablet, and otherwise forwards to one.

This — not the skipped barrier — is the largest win. A data node that hosts the
tablet answers with **zero network hops**, where today it must forward to the
leader and wait for a quorum probe on the far side. It also removes the read
load concentration on tablet leaders that the leaderful design otherwise implies,
which is the read-scaling property ADR 0016 gave up when the AP plane was
deferred.

Preference order, when this node does not host a replica: the ADR 0047 intra
port to any serviceable replica, leader included (a leader is a replica; it is
simply not privileged here). Replica selection among serviceable candidates is
deliberately left unspecified in this ADR beyond "prefer local" — a
locality/load policy is a follow-up, and picking one now would bake a guess into
the wire path.

### 4. The freshness gate: what may serve

Serving from any replica means the set of things that must **refuse** grows.
A replica may serve an eventual read only when all hold:

- the tablet is `Active` — a **`Building` split child (ADR 0050) must refuse**.
  It is mid-copy by construction and its keyspace is legitimately incomplete;
  `cp_scan` and the split summary already filter `Building`, and this is the
  same filter for the same reason;
- the group is not being reclaimed or released by the ADR 0031 reconciler;
- the replica's live `scope_range()` contains the requested key or window.
  This check exists on the leader path already (`cp_scan_local` refuses a window
  outside its scope, "stale routing, likely a split crossover"); off-leader it
  matters *more*, because a follower's scope can lag its leader's across a
  cutover;
- the replica is not mid-`InstallSnapshot` catch-up.

A refusal is **retryable routing**, never an absent key — the ADR 0033 read-path
distinction (`linearizable_get_served`'s outer `Option`) applies unchanged and
with more force, since there are now more ways to be the wrong node to ask.

### 5. Intents, off-leader

`local_get`'s current contract reads a key covered by a `Pending` intent as
**absent**. That is correct for its stated purpose — a raw test-facing peek —
and wrong for a client read: it would report a phantom deletion of a live key
whenever a transaction happens to be in flight over it.

An eventual read therefore resolves to the **last committed value**, ignoring
any pending intent, rather than reporting absence or waiting. It does not chase
the intent: a follower cannot run `cp_get_local_resolving`'s cross-tablet
`TxnStatus` query, and an eventual read has no reason to — "the value before the
in-flight transaction" is a legitimate eventually-consistent answer, and waiting
would reintroduce the latency this mode exists to avoid.

### 6. No ceiling, no timestamp cache — and the monotonicity cost

An eventual read **must not** call `ensure_ceiling_above` and **must not** bump
the ADR 0018 read-timestamp cache. It takes no position in the serialization
order, so nothing needs to be pushed above it. This is the correctness crux of
the whole change: an eventual read that bumped the cache would let a *stale*
observation push a concurrent write's timestamp forward, corrupting the very
order it is not part of.

The honest cost of §3 follows directly. Serving from any replica means two
successive eventual reads by the same client can land on replicas at different
applied watermarks, so the second may observe **less** than the first. Under
leader-local eventual reads this could not happen while leadership held; under
any-replica it can happen routinely. We accept it: it is DynamoDB's contract,
it is what the read-scaling win costs, and §2 declines to promise monotonic
reads precisely so no caller can build on the stronger accident.

Concretely, this is a real-time anomaly of exactly the shape
`check_strict_cycles` was built to catch — which is why the eventual path must
be checked by `check_cycles` and the linearizable path by `check_strict_cycles`,
never one oracle for both (§8).

### 7. Quiescence

An eventual read **must not wake a quiesced group** (ADR 0048). A quiesced
replica has stopped its timers, not lost its applied state, and answering from
that state is exactly what this mode is for. This is a strict improvement on the
linearizable path, which must wake a group to take the barrier at all, and it
extends ADR 0048's existing rule that admin/dashboard reads never wake a group —
`quiesced` stays a pure diagnostic.

### 8. Testing: two modes, two oracles

The `raftkv_linearizable` corpus gains an eventual read mode, and the two modes
are checked by **different oracles**:

- linearizable reads → `check_strict_cycles` (real-time edges), with the
  existing per-scenario `realtime_edges > 0` vacuity guard;
- eventual reads → `check_cycles` plus durability and convergence. Real-time
  edges are *expected* to be violable here, so asserting the strict check on an
  eventual workload would be a test bug, not a finding.

A corpus cell that mixes both modes asserts the strict check over the
linearizable subset only. The negative controls in `strict_negative_control.rs`
already pin the boundary from both sides.

Additionally, the freshness gate (§4) needs its own fault-injecting cells: an
eventual read routed at a `Building` child, at a replica mid-snapshot-catchup,
and across a split cutover where the follower's scope lags — each asserting a
retryable refusal rather than a wrong answer or a phantom absence.

## Alternatives considered

**Leader-local eventual reads (barrier skipped, routing unchanged).** Much the
smaller change: no routing work, no freshness gate beyond what the leader path
already does, and it still removes the barrier, the ceiling proposal, and the
per-tablet `Scan` round trips. Rejected as the *destination* because it leaves
read load concentrated on tablet leaders and gives up the scaling property that
is most of the point. It remains a valid **implementation staging post** if the
routing work proves larger than expected — the read-mode seam in
`animus-cp-data` is identical either way, and only §3/§4 differ.

**A per-table `consistency:` mode (ADR 0016's original shape).** Rejected: the
DynamoDB wire contract puts this on the *request*, not the table, and a
per-table mode cannot express the one thing clients actually want, which is a
strong read of a table they mostly read weakly.

**Linearizable follower reads (ADR 0017 §3's "natural later extension").** Not
this ADR, and not a substitute: a follower that asks the leader for `readIndex`
still pays the round trip and still cannot serve while partitioned from the
leader. It is a latency/locality optimization of the *strong* path and remains
open on its own merits.

## This is not a lease

ADR 0017 §3 rules leader leases off the table because they turn a timing
property into a **safety** property: a lease read that fires after the lease has
actually expired is a *silent stale read presented as linearizable*, and the
determinism story makes it invisible to the deterministic suite.

Nothing here relies on elapsed real time, and nothing here is presented as
linearizable. An eventual read makes a weaker promise **explicitly**, at the
client's request, and its staleness is a documented contract rather than a bet
that a clock behaved. The `ConsistentRead: true` path is untouched and remains
ReadIndex — ADR 0017 §3's "ReadIndex is the baseline indefinitely" stands
exactly as written for the mode that claims linearizability.

## Consequences

- The halved eventual-read capacity charge becomes truthful.
- Tablet leaders stop being the funnel for all read traffic; a hosting node
  answers locally.
- `Scan` stops paying N quorum round trips for a per-tablet guarantee.
- Quiesced groups stay quiesced under read load.
- **A new class of correct-but-surprising client observation** (non-monotonic
  reads, §6) that did not exist before, mitigated only by documentation and by
  `ConsistentRead: true` being available and honest.
- More ways for a read to be routed to the wrong node, all of which must surface
  as retryable routing errors and none of which may surface as absence (§4).
- The wire adapter's read path grows a mode dimension that every future read
  surface must consciously answer for.
- Three decode-site doc comments in `animus-dynamo::wire` currently state the
  accept-and-ignore rule as settled fact (`Operation::GetItem`'s and
  `Query`/`Scan`'s `consistent_read` fields, and `BatchGet::consistent_read`:
  "Accept-and-ignore for the base table, which is linearizable here already
  (ADR 0041 §5)"). They are the precise places the §1 matrix has to land, and
  are load-bearing documentation rather than incidental prose — a reader who
  trusts them after this ADR ships would draw exactly the wrong conclusion.

## Open

- Replica selection policy among serviceable candidates (§3) — locality, load,
  or lag-aware. Deliberately unspecified here.
- Nothing on the wire decode: `BatchGetItem` already carries
  `consistent_read` **per table-spec** (`BatchGet::consistent_read`,
  `decode_consistent_read(spec)`), which is DynamoDB's own shape, so a batch
  may legitimately mix modes across its tables and the eventual path must
  honour the existing per-spec flag rather than collapsing it to one mode per
  request. Recorded here because it is the one place the plumbing is already
  ahead of this ADR.
