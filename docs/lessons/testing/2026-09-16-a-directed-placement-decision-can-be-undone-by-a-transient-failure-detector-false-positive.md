# A directed placement decision can be undone by a transient failure-detector false positive — and the mechanism that protects it stops working the moment it's marked `done`

**Mechanism**: `Metadata::split_placing_reconcile` (ADR 0062 §2) drives a
split child's replicas toward a directed-Placing `target`, pausing (never
retargeting) while a target member is merely `Down`, and only recomputing a
fresh target via `replan` once `retarget_ready_this_tick`'s dwell
(`SPLIT_PLACING_RETARGET_DWELL`, 5s) says a member has been continuously
non-`Active` long enough to treat as genuinely gone. That 5s dwell applies
identically whether the target has already been achieved (`t.replicas ==
target`) or is still mid-convergence — but retargeting an *already-achieved*
target is strictly more disruptive than retargeting one still converging:
`replan`'s only remaining eligible candidates for a split child are typically
its own pre-split siblings, so discarding an achieved decision can converge
the tablet right back toward the set the split was moving it away from.

Reproduced directly, more than once, over a real multi-node `ProdEnv`
cluster (`crates/animusd/tests/split_placing_two_replica_diff_e2e.rs`) with
no synthetic fault injection at all — just ordinary background CPU/disk
contention on the host running the tests: a target member's control-plane
liveness status flips `Down` for long enough to cross the dwell (a false
positive — the process itself never died, its heartbeat was merely
delayed), and the tablet's `voter_history` shows it reach the correct
directed-Placing target and then, seconds later, get retargeted away from
it.

**The reproduction recipe, precisely, for anyone re-running this**: a
4-core host with two cores pinned (`taskset -c 0,1`) running the target
test, and two more concurrent `cargo test -p animusd --test dynamo_txn`
integration-test loops pinned to the SAME two cores as extra contention —
this test's own 6-worker-thread runtime plus two more full 6-worker-thread
test binaries all sharing two cores is deliberately far more thread/core
oversubscription than any real deployment or CI runner would see. `uptime`
1-minute load averages during these batches ranged from under 1 (early in
a batch) up to about 5 (once all three processes were fully warmed up and
contending) on the 4-core host; batches were never run while independent,
unrelated load was also present, so this range reflects the three
processes' own contention alone.

**Two layers to the fix — both now implemented (2026-09-16, issue #928's own
follow-up landed in the same change that closed #921 for good)**:

1. `retarget_ready_this_tick` now uses a *longer*, separate dwell
   (`SPLIT_PLACING_RETARGET_DWELL_ACHIEVED`, 30s) once `t.replicas` already
   equals the stored `target` — a deliberate asymmetry, not an oversight:
   ADR 0062 §2's own stated goal ("the target is never recomputed while
   it is healthy, which is what makes it stable") is extended to the point
   right after the target is realized, where undoing it costs the most. A
   genuinely (not falsely) dead member still self-heals, just more slowly.
   See `crates/animus-control/src/node.rs`'s `SPLIT_PLACING_RETARGET_DWELL_
   ACHIEVED` doc and the regression,
   `split_placing_phase_holds_an_already_achieved_target_past_the_base_
   dwell` (`crates/animus-control/tests/placement_split_placing.rs`).

2. **This only protects the narrow window between achieving the target and
   `MarkSplitPlacingDone` firing — `animusd::split_placing_completion`'s own
   settle window (`SPLIT_PLACING_DONE_SETTLE`) is just 1.5s.** The instant
   `done` is set, the tablet falls under *ordinary* `Metadata::reconcile()`/
   `rebalance()` (ADR 0005's violation/balance repair), which had **no
   dwell or hysteresis at all** — reacting to any observed non-`Active`
   replica immediately. A false positive arriving after `done` (the
   overwhelmingly likely case, given the 1.5s window) reproduced the
   identical divergence via this path (filed as issue #928, then closed in
   the same change as #921): `reconcile_placement`/`rebalance_placement`
   gained a `recently_done: &BTreeSet<TabletId>` parameter (threaded from a
   new driver-local `node::recently_done_this_tick`, the same
   `env.now()`-keyed pattern as `retarget_ready_this_tick`, tracking the
   FIRST tick each `split_placing` entry was observed `done`) — a tablet
   named in it is excluded from ordinary repair/rebalance for the same
   `SPLIT_PLACING_RETARGET_DWELL_ACHIEVED` window, counted from when `done`
   was first observed. **This closes the concrete, directed-Placing-shaped
   instance of #928, not #928's fully general form** — an ordinary tablet
   with no `split_placing` history at all still has no repair dwell against
   a false positive; that remains a real, wider-blast-radius question
   (every tablet in the cluster, not just split children) deliberately left
   open. See `crates/animus-control/CLAUDE.md`'s matching entry, ADR 0062's
   2026-09-16 amendment, and the regressions `meta::tests::
   reconcile_does_not_repair_away_an_achieved_recently_done_target` (pure)
   and `ordinary_reconcile_holds_a_recently_done_target_past_the_grace_
   window` (real `reconcile_loop`/`SimEnv`, `tests/
   placement_split_placing.rs`).

**A related, but SUPERSEDED, finding from the same investigation — read
this before trusting the test's own older comments**: `reconfigure_step`'s
step 1 (remove a `Down` extra voter) USED TO have documented, unconditional
priority over its own learner-add sequencing (ADR 0058 Train 1), so a
false-positive `Down` on an original (non-target) replica could legitimately
skip the over-replicated N+2 intermediate a test asserting on it expected —
not a bug at the time, a test invariant that assumed the common path was the
only legal one (fixed then by softening the hard assertion to a diagnostic).
**Issue #920/PR #932 (a separate, later fix, landed independently) reordered
that priority**: a `Down` extra voter is now removed only once every
`desired` member is already a voter, closing a DIFFERENT hazard (a rolling
restart shrinking live quorum mid-swap) as a side effect also making the
over-replicated intermediate structurally reliable again whenever a
replacement is pending — 60 further contended runs against the combined
fix never once hit the diagnostic-fallback path this superseded finding
describes. The diagnostic softening in the test stays as a defensive
belt-and-suspenders check (real `ProdEnv` timing can still make one
replica's own sampling miss a genuinely-reached transient), not because the
skip-path is expected again.

**General rule**: when a control-plane mechanism reacts to a liveness
signal (a failure detector's `Down`/`Active` transition) to make or unmake a
placement decision, ask two separate questions, not one: (1) is there a
dwell/hysteresis at all before reacting, and (2) does that same protection
apply *after* the decision has been realized, or does the mechanism hand
off to a different, less-protected code path (here: ordinary `reconcile()`)
the moment it's satisfied? A dwell that only covers the "still converging"
half of a mechanism's lifetime can be strictly narrower than it looks — most
of a real production window is spent in the "already done" state, which is
exactly where this gap lived. And when a SEPARATE fix later changes a
mechanism your own test comments describe as "documented, unconditional" —
re-verify, don't just trust the prose; it can go stale silently (this file's
own #670-era comments about `reconfigure_step`'s step ordering were exactly
this kind of staleness once #932 landed).

**Also fixed in the same investigation**: `crates/animusd/tests/
split_placing_two_replica_diff_e2e.rs`'s background writer used
`let _ = writer.await;`, discarding the writer task's own `JoinError` — see
issue #619's own lesson entry for the general form of that bug. A second,
independent test-harness gap found in the same investigation: `admin()`'s
raw TCP I/O had no retry at all, panicking immediately on a transient
`ConnectionReset` under the same contention — given a retry with a measured
budget (30s), mirroring `put`'s own treatment below.

**A third, DIFFERENT failure shape, correctly left un-retried**: a
sustained `ConnectionRefused` (no retry budget recovers it) traced back to
a genuinely halted node — `RaftKvNode`'s apply loop hit its own
`assert!(halted, "raftkv wal {append,sync} failed while running")` on a
REAL WAL fsync failure, i.e. actual disk I/O exhaustion on the host from
three full multi-node `LsmEngine`-backed clusters (this test plus two
`dynamo_txn` contention binaries) all issuing real fsync traffic while
pinned to two shared cores — the identical "wal group-commit sync
failed... under disk pressure" confound issue #670's own original report
already named as separate from the protocol-level question. Corroborated,
not newly introduced, by this investigation, and correctly NOT treated as
something to retry around: a real storage failure fail-fasting when not
intentionally halted is by-design crash-safety behavior
(durable-before-visible), not a defect.

**`put`'s own retry budget — a measured ceiling over a KNOWN, still-open
defect, not harness noise (issue #950)**: the budget went through several
revisions (20s → 45s → 100s → 150s) as reversion-bug contributions to the
apparent timeouts were fixed above and the residual tail was re-measured
each time (see `put`'s own doc comment in the test file for the full
sequence). The first two rounds' framing — "a heavy-tailed real `ProdEnv`
liveness property of extreme contention, not a masked correctness bug" —
was itself incomplete. Instrumenting `put`'s own per-attempt timing against
a SHARED clock origin with the test's existing `/admin/raftkv` leader poll
(so a stalled `put`'s exact wall-clock window can be checked against
whether the target tablet group had a leader at that moment) showed, across
three reproductions (51.9s, 47.0s, 102.7s totals):

```
PUTDIAG t=13836ms key=[119,56,48] attempt 1 failed after 10.002857478s: relay to peer node failed
PUTDIAG t=24028ms key=[119,56,48] attempt 2 failed after 10.04037591s: no CP group leader reachable
PUTDIAG t=34221ms key=[119,56,48] attempt 3 failed after 10.042607227s: no CP group leader reachable
PUTDIAG t=44685ms key=[119,56,48] attempt 4 failed after 10.311502806s: relay to peer node failed
...
PUTDIAG t=106509ms key=[119,56,48] succeeded after 10 attempt(s), total 102.676585368s
```

— every failing attempt taking almost exactly `CLIENT_TIMEOUT` (10s) or
`HINTED_FORWARD_HOP_TIMEOUT` (6s, compounding with `cp_route`'s own wait
when both fire in one attempt), stacking attempt after attempt — **while
the independently-polled admin leader trace for the SAME window showed a
continuously known, stable leader the entire time** (and, in the clearest
reproduction, an already-converged, UNCHANGING voter set — not even
mid-reconfigure). This rules out leader election: `cp_route`
(`crates/animusd/src/forwarding.rs`) and the forward-hop chase
(`crates/animusd/src/write_path.rs`'s `cp_kind_write_raw_bounded`) are
failing to resolve or reach a route to a leader that demonstrably exists
and is reachable from at least one other replica throughout. Filed as its
own issue, #950, with the full breakdown and proposed fix directions (fan
out the route/hint lookup across known replicas instead of one sequential
per-node wait; shorter first-probe timeouts with backoff instead of
committing a full `CLIENT_TIMEOUT`/`HINTED_FORWARD_HOP_TIMEOUT` per
attempt; a metric/log line when this stall class occurs in production).
**The general lesson**: when a client-visible timeout keeps needing to
widen even after every mechanism bug you can find has been fixed, don't
stop at "measured, so it's fine" — cross-reference the client's own
per-attempt timing against an independent, concurrently-collected signal
(here: an admin poll the test already had, sharing one clock origin) before
concluding the tail is unexplainable harness noise. It may be a real,
separately-fileable product gap hiding behind what looks like ordinary
contention-induced slowness.
