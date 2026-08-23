# ADR 0055 — Cheap eventually-consistent reads: `ConsistentRead: false` served from any replica

- **Status:** Accepted — implemented.
- **Date:** 2026-08-23
- **Amends:** [ADR 0017](0017-per-tablet-raft-data-plane.md) (the data plane's
  read path is no longer leader-only: ReadIndex remains the *linearizable* read
  and gains a replica-local sibling for the weaker one);
  [ADR 0041](0041-materialized-secondary-indexes.md) §5 (`ConsistentRead` stops
  being accept-and-ignore for correctness and becomes the flag that actually
  selects a read path); [ADR 0048](0048-cp-group-quiescence.md) (a second class
  of read that deliberately does not wake a quiesced group, after ADR 0051's
  reaper).
- **Depends on:** ADR 0018 §2 (the value envelope and the MVCC version chain an
  eventual read falls back through), ADR 0035 (a control-only node hosts no
  replica and so never serves one), ADR 0047 (the intra port a forwarded
  eventual read rides), ADR 0050 (per-tablet engines — the thing a replica reads
  locally).
- **Closes:** the forward reference [ADR 0019](0019-cp-only-v1-defer-ap.md)
  makes when it argues that `ConsistentRead` is a read-path option over a
  strongly-consistent store rather than a residual use for an AP plane.

## Context

DynamoDB gives a client exactly one consistency knob, and it is per-read:
`ConsistentRead`. `true` asks for a strongly-consistent read; `false` — **the
wire default**, and therefore the overwhelming majority of real traffic — asks
for an eventually-consistent one, which AWS prices at **half** an RCU precisely
because it is served by one replica instead of coordinated across them.

AnimusDB decoded that flag from day one and then ignored it. Every read —
`GetItem`, `BatchGetItem`, base `Query`/`Scan`, LSI and GSI reads alike — went
down one path: **ReadIndex on the tablet's Raft group leader** (ADR 0017 B.2).
The flag survived only in two places, both real but both narrow: `read_capacity`
halved the *reported* charge for an eventual read, and a GSI `Query`/`Scan`
rejected `ConsistentRead: true` outright (ADR 0041 §5), since a GSI is
maintained asynchronously and cannot honor it.

Being stronger than asked is not a correctness problem. It is a **cost** problem,
and the costs are not small:

1. **A quorum probe per read.** `read_barrier()` records `read_index =
   commit_index` and confirms leadership by a round trip to a quorum of peers.
   That is a network round trip on the critical path of every single read.
2. **A Raft proposal per read, sometimes.** ADR 0018 §2/PR2b's
   `ensure_ceiling_above(ts)` drives the group's committed read ceiling above the
   read's own timestamp, *proposing and awaiting a `ReadCeiling` entry* when the
   ceiling is behind. A read that appends to the log is an expensive read.
3. **A read-timestamp-cache bump per read**, pushing later writes to the same
   span above the read's timestamp — write amplification bought entirely for a
   linearizability guarantee the client did not ask for.
4. **A forwarding hop per read**, whenever the node the client reached is not the
   tablet's leader. On a table with one tablet, *every* client on *every* other
   node pays it.
5. **No read scaling at all.** Every read of a tablet lands on its leader.
   Followers hold complete, durable, up-to-date copies of the data and serve no
   reads whatsoever. For a database whose one-line description is "masterless,
   linearly-scalable," a read path that funnels a tablet's entire read volume
   through one node is the single most conspicuous scaling gap in v1.

Cost 5 is the important one. The other four are latency; this one is a ceiling.

There is also a **quiescence** interaction worth naming (ADR 0048): an idle group
stops its Raft timers entirely, and `resolve_cp_route` wakes any local replica
before routing so a first touch does not wait out idle-detection latency. A read
that requires a leader and a quorum probe genuinely needs the group awake. A read
that only inspects a replica's engine does not — and on a large fleet, waking
cold groups to serve reads that never needed them woken is exactly the cost ADR
0044's cheap-groups roadmap exists to avoid.

## Decision

**`ConsistentRead: false` selects a genuinely different read: served from *any*
replica of the key's tablet, from that replica's own applied engine state, with
no ReadIndex barrier, no ceiling proposal, no read-timestamp-cache bump, no
leadership requirement, and no wake of a quiesced group.**

`ConsistentRead: true` keeps today's path unchanged, byte for byte.

### 1. What an eventual read may and may not return

The contract, stated precisely, because the difference between the two halves is
where every bug in this area lives:

- **It may return an older state of the tablet.** A follower's engine is the
  result of applying a *prefix* of the group's committed log, so whatever it
  answers is a state the tablet genuinely had. Staleness is unbounded in
  principle (a partitioned replica can be arbitrarily behind) and small in
  practice (one AppendEntries round). This is exactly DynamoDB's promise, and
  exactly all of it.
- **It may never return a state the tablet never had.** No fabricated absence,
  no fabricated deletion, no torn read, no value assembled from two different
  points in the log. This is the ADR 0033 read-path lesson in its
  read-your-replica form, and it is what the freshness gate and the intent rule
  below exist to enforce.

**A multi-tablet read mixes freshness, and that is fine.** `Query`/`Scan` fan
out per tablet (`cp_scan`) and each tablet decides independently whether it can
be served cheaply, so one page can merge a replica-local slice with a
leader-served one. Every slice is still a genuine committed state of *its own*
tablet, which is all a hash-ring scan ever promised — DynamoDB's `Scan` gives no
cross-partition snapshot even at `ConsistentRead: true`. Pagination is unaffected
for the same reason it always was: the cursor is a **key**, not a position, so a
stale window can under-fill a page but can never skip an item into the gap
between two pages.

The practical consequence, and the one users will actually notice:
**read-your-writes no longer holds for the default read.** A `PutItem` followed
immediately by a `GetItem` with no `ConsistentRead` may not see the write. That
is not a regression against DynamoDB — it is fidelity to it — but it is a
behavior change against every previous revision of this database, and it is the
main thing this ADR trades away. See Consequences.

### 2. The freshness gate: `RaftKvNode::stale_read_ready()`

A replica may serve an eventual read only when both hold, both checked locally
and cheaply:

- **It knows a current leader** (its Raft `leader_id` is set). A replica that has
  never received an `AppendEntries` — a freshly added voter under an ADR 0029
  rebalance, a node between start and its first heartbeat — has an engine that is
  not yet *any* state this tablet ever had. Serving it would report every item as
  absent, which is not staleness; it is a fabricated deletion of the whole
  tablet.
- **Its engine holds everything it knows to be committed**
  (`engine_applied_index() >= commit_index()`). This excludes a replica carrying
  a committed-but-unmerged log tail, and — the case that actually matters — one
  **mid-`InstallSnapshot`**: `RaftCore` sets `commit_index = snapshot.last_index`
  the moment the last chunk arrives, while `engine_applied` only advances *after*
  the driver has merged the whole image (`install_engine_image` →
  `fetch_max(last_index)`). The gate is therefore closed for exactly the window
  in which the engine is a half-written image, which is the one state a replica
  can be in that is not a prefix of the log at all.

Note what this is deliberately *not*: a **staleness bound**. A replica partitioned
from its leader passes the gate and answers from an arbitrarily old state. Adding
a bound (a closed timestamp, a lease, a max-lag threshold) is a real design with
real machinery behind it — Cockroach's closed timestamps are the reference — and
none of it is needed to honor DynamoDB's contract, which promises no bound
either. A caller that needs one asks for `ConsistentRead: true`. Recording this
explicitly so a future reader does not mistake the gate for a guarantee it never
made.

Quiescence falls out for free: a quiesced group is idle by construction, so it is
fully applied, so it passes the gate — and nothing on this path calls `wake()`.

### 3. Transaction intents: fall back a version, never report absence

A base-scope value may be an `Envelope::Intent` (ADR 0018 §2) — a transaction has
staged a write over the key and the covering record has not been decided here
yet. The linearizable path resolves it, paying a round trip to the anchor tablet
when the record is foreign. An eventual read must not: that round trip is the
entire cost it exists to avoid.

The two obvious cheap answers are both wrong. Returning the *staged* value would
expose an uncommitted write. Returning *absent* — which is what `local_get`'s raw
peek does, correctly, for its admin/debug callers — would fabricate a deletion of
an item that exists.

**An eventual read returns the key's last committed value: the MVCC version one
below the intent's own** (`storage.get_at(physical, version - 1)`, the same "one
hop back" `resolve_decided` already performs on the aborted/too-late branch, now
factored into a shared `prior_committed`). That value is a state the tablet
genuinely had, and the covering transaction has not committed as far as this
replica knows — so returning it is staleness and nothing more. If the transaction
did commit, the read is stale, which it is allowed to be. If it aborted, the read
is simply correct.

The same rule applies row-by-row inside an eventual **scan** (`stale_scan_rows`):
a row under an intent contributes its last committed value rather than being
dropped, because a client-visible page that silently omits an item that exists is
the same fabricated deletion in scan clothing.

Non-base row kinds (LSI rows, the change log, footprints, cursors) hold only
committed values by construction — only `KindBatch` writes them and it always
commits outright — so their eventual read is plain `local_scan_kind`, differing
from the linearizable form purely by the missing barrier.

### 4. Routing: nearest replica, one hop, always a fallback

`ClientCtx` gains a parallel, deliberately small routing path (`cp_stale_local` /
`cp_stale_forward_target`), used only for eventual reads:

- **This node holds a serveable replica** — it is a voter in the group's own
  durable Raft config (the same check `resolve_cp_route` makes, so a node
  mid-release from a rebalance does not serve), the key is inside the handle's
  live `scope_range()`, and the gate passes — then the read is answered **with
  zero network hops and zero consensus work**, whether or not this node is the
  leader.
- **Otherwise**, one forwarded hop to *any* replica (intra port, ADR 0047), which
  serves it under the identical rules or refuses.

Two properties make this safe to add without widening the failure surface:

- **Every step is best-effort, and the fallback is the strong path.** Each helper
  returns `None` for "not cheaply", and every caller falls straight through to
  the untouched linearizable loop. There is no eventual-read-specific failure a
  client can observe — only an eventual read that quietly cost what a strong one
  costs. This is also why the diff leaves the ReadIndex path literally unchanged:
  a `Strong` read compiles down to exactly what it always did.
- **A forwarded eventual read never chases a leader.** It does not go through
  `forward_to_tablet_leader` — there is no leader to chase, a refusal means "not
  cheaply, then", and waiting out an election to serve a stale read is
  incoherent. One connection, one reply, a short `STALE_READ_FORWARD_TIMEOUT`
  (2s, deliberately far below `CLIENT_TIMEOUT`: a cheap read that sits ten
  seconds on an unresponsive replica has already lost every property it was
  chosen for).

Read *spreading* therefore comes from clients reaching different nodes, each
answering locally — not from a coordinator fanning out. The forwarding target is
the first **other** replica with a known intra address, in `NodeId` order:
deterministic and stable, deliberately not load-aware, and excluding this node
(which has already declined, so relaying to ourselves would only re-derive the
same refusal a round trip later). A replica-picking policy is a larger question
than this ADR, and a stable answer is the right placeholder until it is asked.

### 5. Wire plumbing

`ClientRequest::Get`/`Scan`/`KindScan` gain a `#[serde(default)] stale: bool`
rather than gaining three new variants. Two reasons, both practical: adding a
variant means updating ADR 0047's exhaustive `surface_of` table and every gating
allowlist (the standing hazard the root `CLAUDE.md` warns about — a missed
allowlist is a bimodal per-process flake the compiler cannot catch), while adding
a field is caught at every construction site by `error[E0063]`. And `Get`/`Scan`
are already client-facing, so the plain protocol gets the same choice for free
(`animus get-eventual`, added to the CLI as the hand-driven way to observe
replica lag on a live cluster).

### 6. What stays strongly consistent, deliberately

Not every read is a client's to weaken:

- **`TransactGetItems`** — DynamoDB's own API has no `ConsistentRead` on it; it
  is always strongly consistent, and ADR 0018's quiescent-round argument depends
  on it.
- **Transaction preconditions and `ConditionCheck` reads** — the observed value
  decides whether a transaction commits.
- **`await_table_serveable`** — the `CreateTable` readiness probe exists to prove
  the group has *elected and can serve linearizably* before the 200 (ADR 0023's
  2026-08-17 amendment). An eventual read would pass against a replica that has
  merely applied something, handing the client exactly the formation window the
  probe was added to hide.
- **The evaluate-at-leader write path** (ADR 0046 U3 / ADR 0049) — a write's own
  old-image read happens at the leader, under `rmw_lock`, and is not a client
  read at all.
- **The GSI drain, the backfill seeder, the split driver, the TTL reaper** — all
  read their own led tablet's engine locally already, and none of them route
  through `cp_read`/`cp_scan`.

**GSI reads become eventual unconditionally**, which is not a special case: a GSI
`Query`/`Scan` rejects `ConsistentRead: true` (ADR 0041 §5), so the flag is
always `false` there and the ordinary derivation produces `Eventual`. The
consistency is derived from the flag rather than hard-coded, so the two facts
stay tied together in one place if that rejection ever moves.

### 7. Observability

Three counters (ADR 0015, data-role sink, `/metrics`): `cp_eventual_reads_local`
(served from this node's own replica — the outcome the feature exists to
produce), `cp_eventual_reads_forwarded` (one hop to another replica), and
`cp_eventual_reads_fell_back` (took the strong path after all). The last is the
one worth alerting on: a high fallback rate means the cheap path is not being
taken, and **no client-visible symptom would ever reveal that** — the reads
still return correct answers, just at the price the feature was meant to
remove.

What these deliberately do *not* measure is **staleness**. See Consequences.

### 8. Capacity

Unchanged, and now honest: `read_capacity` already halved the reported charge for
an eventual read. Before this ADR that was a charge for work the database did
anyway. It now reflects what actually happened.

## Alternatives considered

**Leader-local, barrier-free reads (serve on the leader, skip only the ReadIndex
probe).** This removes costs 1–3 and — because a write is durable-before-ack and
therefore applied at the leader before the client hears about it — *preserves
read-your-writes*, which would have meant essentially no test churn and no
behavior change to explain. Rejected because it leaves costs 4 and 5 untouched,
and cost 5 is the reason to do this at all: reads would still all land on the
leader, and the scaling ceiling would still be exactly where it was. It is also
strictly subsumed — when the node a client reaches happens to host the leader,
the chosen design *is* the leader-local barrier-free read.

**A bounded-staleness read (closed timestamps / follower leases).** A strictly
better read than either option here, and a genuinely large piece of machinery: a
per-group closed-timestamp protocol, its propagation, and its interaction with
the HLC and the read ceiling. DynamoDB's contract does not ask for it and its
wire cannot express it. Deferred, not rejected — §2's gate is deliberately shaped
so a bound could be added *to* it later rather than replacing it.

**Resolving intents on the eventual path.** Correct and occasionally very
expensive: a foreign intent costs a cross-tablet status query, which is precisely
the round trip the path exists to avoid, and intents are exactly the situation in
which that round trip is slowest. §3's version fallback is cheaper, always local,
and returns a genuinely committed value.

**Serving eventual reads from a replica with no freshness gate.** Simpler, and
wrong in the one way that matters: a freshly added voter's empty engine would
report a populated tablet as entirely absent. §2 exists for that single case.

## Consequences

**Read-your-writes is gone from the default read, and this is the real cost.**
Existing tests that write and then immediately read without `ConsistentRead:
true` observe the write missing, non-deterministically. Eleven such tests across
eight files (`admin_endpoint`, `dynamo_batch_get`, `dynamo_documents`,
`dynamo_index_writes`, `dynamo_indexes`, `dynamo_schema`, `dynamo_txn`,
`dynamo_wire`) are updated to ask for the consistency they actually depend on —
which is what a DynamoDB client must do, and which makes each such test say out
loud what it needs. Application code that silently relied on the old
over-strong behavior needs the same change, and (no back-compat until further
notice) that is a deliberate break, not an accident.

**Because those failures are races, a green run is weak evidence.** The audit
that matters is "which reads exist to verify a write", not "which reads failed
this time" — see `docs/engineering-lessons.md`'s entry on this. Some latent
flakes may remain in reads that happened to win the race; each is fixed the
same way when it surfaces.

**A second, less obvious class: multi-endpoint read loops.** Several suites
round-robin a paginated `Query`/`Scan` walk across all three nodes on purpose,
to exercise the forwarded read path. That rotation was implicitly safe while
every node forwarded to the same leader — one state, one convergence check.
Replica-local reads make consecutive pages sample different, independently
lagging replicas, so a walk can terminate a page early and drop an item into
the gap. The rotation is worth keeping (it tests something real), so the fix is
to stabilize the data across endpoints instead: ask for the strong read where
the API allows it, and where it does not — a GSI `Query` rejects
`ConsistentRead: true` — converge on *every* endpoint before walking, not just
one. Anything that collects from several nodes and compares (parallel-scan
segment fleets included) needs the same audit.

**Followers now serve client traffic.** Read volume on a tablet is no longer
bounded by its leader's capacity. That is the payoff; it also means follower
latency and follower engine health are now on a client-visible path, where before
they were purely internal.

**The public consistency claim had to change.** `website/index.html` and
`website/docs.html` said reads were "linearizable by default — no quorum tuning,
no read repair, no 'eventually'". Writes still are; reads are linearizable *on
request*. Both pages now say so, because a consistency claim a user reads before
choosing a database is not marketing copy that can lag the implementation.

**Two read paths to keep honest.** The strong path is untouched by this change,
which keeps the regression risk low, but "which reads are strong" is now a real
question every future feature has to answer. §6 is the current list; anything
added to it should be added there too.

**Staleness is unbounded and unmeasured.** A partitioned replica will serve old
data indefinitely and report no error. §7's counters say which *path* a read
took, which is not the same question: a read served locally from a replica an
hour behind counts as the best outcome. There is no operator surface that says
"this replica is behind and serving reads" — a named gap, and the obvious next
piece of work if this path proves to need one. The natural shape is the same
`commit_index - engine_applied_index` the freshness gate already computes, plus
a leader-contact age, surfaced per hosted group on `/admin/raftkv`.

**The gate can false-negative.** `engine_applied_index()` is read after the core
lock is released, so a concurrent apply can make a caught-up replica look behind
and fall back to the strong path. Always correct, occasionally not as cheap as it
could be — the right direction for this comparison to be wrong in.

## Testing

- `animus-cp-data`'s `tests/stale_read.rs` proves the primitives
  deterministically over `SimEnv`: a follower serves from applied state with no
  barrier, an intent-covered key reads its last committed value rather than
  absent (point and scan), and the gate refuses a replica that has heard from no
  leader.
- The **linearizability corpora are deliberately untouched.**
  `animus-test`'s `raftkv_linearizable.rs` and `txn_serializable.rs` read
  through `linearizable_get`/the transaction path, and must keep doing so: an
  eventual read is not linearizable, and feeding one to `check_cycles` would
  either report a false violation or — worse, if the checker were relaxed to
  accommodate it — quietly stop testing the property it exists for. The
  eventual path's own correctness argument is "a genuine committed prefix of
  one tablet's log," which is a different claim and is proven by the two suites
  below, not by a consistency checker.
- `animusd`'s `tests/dynamo_eventual_read.rs` proves the wire contract end to end
  over real HTTP: `ConsistentRead: false` converges on every node of a cluster
  (converged-or-timeout, never a fixed-deadline assert — the eventual-property
  rule), `ConsistentRead: true` is immediately correct on every node including
  the ones that host only followers, and both agree once the cluster is quiet.
