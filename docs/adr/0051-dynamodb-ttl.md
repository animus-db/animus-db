# ADR 0051 — DynamoDB-style TTL: a wall-clock seam and a reaper on the kind-write path

- **Status:** Accepted — implemented.
- **Date:** 2026-08-19
- **Amends:** [ADR 0003](0003-deterministic-simulation.md) (the `Env` seam gains a
  wall-clock method — the first calendar-time reading in the codebase, and the
  reason this ADR spends most of its length on determinism);
  [ADR 0013](0013-replicated-schemas.md) (a table's schema entry gains a
  `TtlSpec`); [ADR 0048](0048-cp-group-quiescence.md) (the reaper is the first
  background loop that deliberately does *not* wake a quiesced group to do its
  reading).
- **Depends on:** ADR 0049 (the universal kind-write path — the reason the
  reaper needs no index/stream maintenance code of its own), ADR 0046 U3
  (evaluate-at-leader), ADR 0042/0043 (Streams — a TTL deletion is a stream
  record with a distinguishing `userIdentity`).

## Context

DynamoDB's TTL lets a client name one item attribute holding an **absolute Unix
epoch second**; items whose timestamp is in the past are deleted by a background
process "typically within 48 hours," free of write-capacity charge. It is one of
the most-used DynamoDB features — session stores, caches, event buffers,
regulatory retention — and its absence is conspicuous in an adapter that already
covers Streams, GSIs, and transactions.

The feature is deceptively small at the wire (two API calls, one catalog field)
and genuinely interesting underneath, for one reason: **it is the first thing in
this database that depends on calendar time.**

Everything Animus does today is measured in *intervals* — election timeouts,
heartbeat periods, retention windows, backoff. Those are monotonic by
construction and flow through `Clock::now()`, whose doc has said since ADR 0003:

> It is deliberately *not* a wall-clock time: system code must never depend on
> calendar time.

That rule is what makes ADR 0003's determinism guarantee possible, and it is
correct for every consumer that existed before this one. But a TTL timestamp is
chosen by the *client*, in *its* calendar, and written into an item that may
outlive the process, the node, and the cluster generation that stored it. No
monotonic reading can interpret `1735689600`. The choice is not whether to admit
calendar time, but where to admit it so the determinism guarantee survives.

A second, quieter design pressure: TTL deletion is a **write** — and in this
codebase a write to a table with a GSI, an LSI, or a stream is not a single key
mutation. It must maintain index rows, emit a change-log record, and produce the
old image a stream consumer expects. ADR 0049 made *every* table route *every*
mutation through `KindBatch`; this ADR is the first new feature built after that
landed, and it is the test of whether that universality actually pays.

## Decision

### 1. `Clock::wall_now()` — one seam, still a seam

`animus-env` gains a `UnixMillis` type and a second clock method:

```rust
fn wall_now(&self) -> UnixMillis;
```

`UnixMillis` is deliberately **not** convertible to or from `Nanos`. The two
answer different questions ("how long since this env started" vs. "what time is
it"), and keeping them incomparable at the type level is what stops a future
change from quietly measuring a timeout against a clock that can step backwards.

- **`ProdEnv`** reads `SystemTime::now()` fresh on every call, so an NTP
  correction is picked up rather than baked in at bind time. A pre-epoch system
  clock reads as 0 rather than panicking.
- **`SimEnv`** returns `SIM_WALL_EPOCH_MS` (a fixed constant —
  2020-01-01T00:00:00Z) plus elapsed **virtual** time, with the same per-node
  `set_clock_skew_for` offset `Clock::now()` already applies.

The `SimEnv` half is the whole determinism argument, and it is worth stating
plainly: **the wall clock under simulation is a pure function of the run's
seed.** Two runs of the same seed on different calendar days read the same
timestamps, so a TTL sweep replays exactly like anything else
(`ANIMUS_SEED=<seed>`). Admitting calendar time did not carve a hole in ADR 0003;
it added a derived reading *inside* the seam.

Reusing the per-node clock skew is not incidental either. A TTL reaper on a node
whose clock reads three minutes fast will delete items three minutes early, and
that is a real property of any distributed TTL implementation — including the
real DynamoDB. Because `set_clock_skew_for` now skews the wall clock too, that
scenario is directly testable rather than hypothetical.

**The rule for every future caller**, recorded on the method itself: `wall_now`
is for interpreting externally-supplied calendar values and nothing else. Every
deadline, timeout, election, and backoff keeps using `now()`. A backwards
wall-clock step must never be able to stall the system.

### 2. TTL configuration lives in the replicated catalog

A table's schema entry gains `ttl: Option<TtlSpec>`, where `TtlSpec` carries just
the attribute name, mutated only through a new replicated
`MetaCommand::SetTableTtl`. This mirrors ADR 0042's `StreamSpec` in every
respect — durable, cluster-agreed, recovered from Raft, mirrored into the system
keyspace by re-serializing the whole schema entry (ADR 0038).

One deliberate divergence from `SetTableStream`: **TTL is idempotent and
in-place mutable.** A stream has a minted `label` that makes its identity
`(table, label)`, so re-enabling one without an intervening disable is rejected.
TTL has no such identity — so re-enabling with the same attribute name is a
`NoOp`, and *changing* the attribute name in place is `Applied`. Both are legal
DynamoDB operations and neither needs a disable first.

The wire surface is `UpdateTimeToLive` and `DescribeTimeToLive`, decoded in
`animus-dynamo`'s pure wire layer. `DescribeTimeToLive` omits `AttributeName`
entirely when the status is `DISABLED`, matching AWS. AWS's `ENABLING`/
`DISABLING` statuses are **not** modelled: our enable is a single replicated
command with no asynchronous phase to report, so only `ENABLED`/`DISABLED` can
ever be observed.

### 3. Reads stay AWS-faithful: an expired item is visible until it is deleted

Real DynamoDB keeps returning expired-but-not-yet-reaped items from `GetItem`,
`Query`, and `Scan`, and documents that clients who care should filter
client-side. We match that exactly, and **no read path in this change acquired a
TTL check.**

This was a genuine fork, and the tempting answer is the other one: filtering at
read is stronger, hides reaper lag, and makes end-to-end tests assert
immediately instead of polling. We rejected it for three reasons.

- **It is a different feature wearing TTL's name.** An application ported from
  DynamoDB that reads an expired item today would silently start missing it.
  Being *quietly stricter* than the system we emulate is a worse failure mode
  than being faithfully weaker, because nothing surfaces the difference.
- **The cost is paid on the hot path, forever, by every table.** Every read of
  every item on a TTL-enabled table would decode and range-check an attribute,
  to hide a window that the reaper closes anyway.
- **It would make expiry depend on the reader's clock, not the reaper's.** Two
  nodes with skewed clocks would disagree about whether the same item exists —
  a far nastier contract than "it disappears shortly after its timestamp."

The consequence is honest and documented: between a TTL timestamp passing and
the reaper's sweep, the item is still there. That window is bounded by the sweep
interval, which is minutes here rather than DynamoDB's 48 hours.

### 4. The reaper deletes through the universal kind-write path

A per-node background loop (`animusd::ttl_reaper`), spawned on data-capable
nodes, self-gating on leadership per tablet — the same "run everywhere, gate on
`group.is_leader()`" shape `index_drain`, `auto_split_loop`, and the tablet-host
reconciler already use. Per tick, for each locally-led tablet of a TTL-enabled
table, it scans for expired items and deletes them.

The deletion is **not** a raw key removal. It goes through
`kind_write_item_at_leader` with `KindWriteOp::Delete` — the identical primitive
`DeleteItem` uses. This is the ADR 0049 payoff, and it is the single most
important structural decision here: because every mutation already routes
through `KindBatch`, the reaper needs **no** index-maintenance code, **no**
stream-emission code, and **no** change-log code of its own. GSI rows, LSI rows,
the change-log record, and the old image a stream consumer reads all fall out by
construction, and stay correct automatically as those subsystems evolve. A
reaper that wrote keys directly would have been a second, permanently-drifting
implementation of the write path.

**Every delete is conditional on the expiry value the sweep actually observed**
(`ConditionExpression`'s `attr = :v`, against the TTL attribute). If a client
extends or removes an item's TTL between the scan and the delete, the condition
fails and the item survives. Without this, a sweep would race every
`UpdateItem` that refreshes a session — the exact workload TTL exists for. The
guard costs nothing: the condition is evaluated at the leader under the same
`rmw_lock` the ordinary write path takes, and the apply-time OCC seatbelt backs
it.

### 5. The five-year sanity window

An expiry more than **five years** in the past is treated as *not expired*.

This mirrors AWS, and it defends against one specific, catastrophic, and
extremely common client mistake: writing **milliseconds** into an attribute the
table interprets as **seconds**. `Date.now()` in JavaScript yields ~1.7×10¹²,
which read as seconds is the year 55000 — harmless, far future. The dangerous
direction is the inverse: any timestamp derived by dividing when it should not
have been, or a unit-confused constant, lands deep in the past and marks *the
entire table* for immediate deletion. The window converts a data-loss event into
a no-op.

It is a genuinely load-bearing safety property rather than a nicety, and it is
tested on both sides of its boundary.

### 6. Quiescence: read without waking, wake only to delete

ADR 0048 stops an idle group's timers entirely, and establishes that reads never
wake a group. A TTL reaper that woke every quiesced group on every tick to look
for expired items would defeat quiescence for every TTL-enabled table — while
almost always finding nothing.

So the reaper **scans locally without waking**, and wakes a group only when it
has actually found an expired item to delete (a delete is a Raft proposal, which
requires an awake group regardless). An idle tablet with nothing to expire stays
asleep and costs one local read per sweep interval.

### 7. A TTL deletion is distinguishable in the stream

DynamoDB tags stream records produced by TTL expiry with a `userIdentity` of
`{"PrincipalId": "dynamodb.amazonaws.com", "Type": "Service"}`, which is how
consumers tell a system expiry from a user delete — the documented basis for the
common "archive expired items to cold storage" pattern. Records the reaper
produces carry it; records from a client `DeleteItem` do not.

## Consequences

- **Calendar time is now reachable**, and the guard against its misuse is a
  documented contract plus an unconvertible type, not a lint. This is the one
  genuinely new risk the change introduces: a future author reaching for
  `wall_now()` to measure an interval. The method's own doc says not to, and the
  type system makes mixing the two clocks an explicit conversion rather than an
  accident.
- **Determinism holds.** The wall clock under `SimEnv` is a pure function of
  virtual time and seed, so TTL sweeps replay from a seed like everything else,
  and clock skew between nodes is a testable fault rather than a thought
  experiment.
- **The reaper inherits index, stream, and change-log correctness** from the
  kind-write path instead of reimplementing it. This is the strongest evidence
  so far for ADR 0049's universality bet: a whole new mutation source landed
  without touching the write path at all.
- **Expired items are briefly visible**, by choice (§3), bounded by the sweep
  interval rather than AWS's 48 hours.
- **A skewed node reaps early or late.** Deletion is a wall-clock decision made
  by whichever node leads the tablet, so a node three minutes fast expires items
  three minutes early. This is inherent to distributed TTL (AWS makes no
  tighter promise), it is now directly simulable, and the conditional delete
  ensures the *only* consequence is timing — never a lost update.
- **Sweep cost scales with table size, not with expiry count.** A large
  TTL-enabled table is scanned per interval whether or not anything expired.
  A cursor bounds each tick's work; making the sweep cheaper (a secondary index
  ordered by expiry, or a per-tablet "earliest expiry" watermark to skip whole
  tablets) is a real follow-up, deliberately not attempted here.

## Alternatives considered

- **Derive a monotonic deadline at write time.** Convert the client's absolute
  timestamp into `now() + delta` when the item is written, avoiding calendar
  time entirely. Rejected: it breaks across restarts (a monotonic base resets),
  across nodes (each has its own base), and silently changes semantics for an
  item written on one node and read on another. It also cannot answer
  `DescribeTimeToLive` or round-trip the client's own attribute.
- **Read `SystemTime` directly at the wire edge**, passing an epoch second down
  as an argument, with no `Env` change. Rejected: it punches a hole straight
  through the ADR 0003 seam at exactly the layer that is hardest to test, and
  makes the reaper unsimulable — no seed replay, no clock-skew fault injection,
  no deterministic assertion about what a sweep deleted.
- **Filter expired items at read time** (§3). Rejected on faithfulness, hot-path
  cost, and reader-clock-dependence.
- **Reap by writing tombstone keys directly**, bypassing the kind-write path for
  speed. Rejected: a second implementation of the write path that must
  independently maintain GSIs, LSIs, change-log records, and stream images —
  guaranteed to drift from the real one (§4).
- **Delete unconditionally**, without re-checking the observed expiry. Rejected:
  it races every TTL refresh, which is the single most common TTL workload (§4).
