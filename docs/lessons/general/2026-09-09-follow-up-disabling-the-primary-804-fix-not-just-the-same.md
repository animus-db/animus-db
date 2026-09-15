# Follow-up: disabling the PRIMARY #804 fix (not just the same-day-correction hunks) still didn't fire the raftkv corpus, and the reason generalizes past this one bug (2026-09-09)

A maintainer follow-up asked for a stronger teeth-proof than the entry
above: disable the *primary* fix (`engine_image` emits `None` for the
header unconditionally, matching pre-#804 `main` exactly — the receiver
witnesses only `storage.latest_version()`) rather than only the two
same-day-correction hunks, and iterate on the corpus until it fires within
50 seeds. `cargo test -p animus-cp-data --test hlc_differential_skew` went
red in that state as expected, including the *original* reproduction
(`lagging_replica_mints_below_a_committed_non_row_writing_entry`) hitting
`assert_ts_monotonic` directly — confirming the disable was the real
thing. `ANIMUS_RAFTKV_SEEDS=50 cargo test -p animus-test --test
raftkv_linearizable` (run twice, cleanly, 249s and 272s) stayed green both
times.

**Three organic ingredients were added and instrumented, one at a time,
each confirmed working on its own terms, and the corpus still never
fired:**

1. A `poison_cas` burst fired immediately before `StopRestart`, on the
   about-to-be-stopped leader. **Diagnosed as structurally unable to
   matter**: those entries land in the victim's own durable WAL before it
   stops, and `RaftCore::recovered`'s WAL replay on restart is a
   log-*scanning* witness (one of the two "strong" ones) — it witnesses
   them correctly regardless of anything else. Moving the burst to fire
   *after* the restart, once the survivors elect a fresh leader (confirmed
   via `engine_latest_version()` instrumentation and a fine-grained
   5ms-step poll to land the burst before the victim's own snapshot round
   trip could complete), fixed that specific gap — verified the victim
   never received these specific entries via WAL replay.
2. An explicit, seeded `transfer_leadership` onto the restart victim,
   retried until `RaftCore::transfer_leadership`'s own gate
   (`peer_match(target) >= commit_index`) opened. **Confirmed working by
   instrumentation on every attempt**: `now_leader == victim` every single
   time, well within budget — Raft election dynamics alone (the two live
   survivors campaign immediately; the just-restarted node is still busy
   recovering) never once let the victim win on its own, exactly as
   predicted, and this ingredient reliably corrected for it.
3. A genuinely fresh write ("poke") proposed on the new leader after the
   transfer, since `compaction_crossing_*`'s own fault fires only once the
   workload has *already fully drained* (confirmed directly:
   `ok_ops=90/90` in the recorder at the moment the fault fires) — so
   without a deliberate new write, no replica ever mints anything after
   the restart for `assert_ts_monotonic` to have a chance to run against.

**Why it still didn't fire, run to ground with `engine_latest_version()`
instrumentation and an A/B (burst present vs. `ANIMUS_SKIP_BURST=1`)
comparison**: the burst itself is a guaranteed CAS miss and provably never
writes a row (verified: with the burst skipped, `engine_latest_version()`
stays flat and identical across all three replicas straight through the
transfer) — but issuing it is exactly what triggers `mint_pushed`'s own
per-term write-conflict machinery on the *first* propose in the new
leader's term, and this raises `engine_latest_version()` on the survivors
by several real seconds' worth of wall_ms (confirmed: skipping the burst
also removes this jump). The victim, once its own `InstallSnapshot` for
the pre-burst state lands, is back to **ordinarily following** — so it
witnesses the burst's own entries (and this fold-in write) via
`witness_append_entries`, a strong, log-scanning witness, the instant they
replicate, `engine_latest_version()`'s own catch-up lag notwithstanding
(that accessor reads *rows*, not the in-memory `Hlc`/`max_applied_ts` the
witnessing chain actually maintains — a real trap in this specific
diagnostic, worth naming on its own: `engine_latest_version()` is the
wrong probe for "has this replica witnessed X," since it can legitimately
lag behind correct witnessing that already happened via log receipt,
pre-apply). For the fix's absence to matter at all, an entry must be
compacted away *before* the receiving replica's own `InstallSnapshot` is
built — which means, for a burst fired **after** a restart to ever be
invisible to the victim, the sender must **compact again**, a second time,
crossing `COMPACT_THRESHOLD` (or forcing an on-demand image early) between
the burst and whatever moment the victim's own snapshot request is
served — the exact two-compaction choreography (real writes hidden by the
*first* compaction, before the restart; the failed-CAS burst hidden by a
*second* one, after it) `hlc_differential_skew.rs`'s own hand-scripted
scenario builds deliberately, and that a `StopRestart`-shaped corpus cell
with only ONE scheduled fault has no way to reach without adding a second,
explicit forced-compaction step of its own — at which point the "organic"
corpus cell is, structurally, the scripted regression with extra
indirection, not a broader net cast over more of the state space.

**General lesson, worth the session it cost**: when a witnessing/ordering
bug's fix works by comparing "what got compacted/discarded" against "what
a specific replica has independently confirmed," the reachability
condition is almost always a race between two *independent* trigger
events (here: the sender's own second compaction, and the receiver's own
snapshot request) that a single scheduled fault cannot pin relative to
each other — proving this rigorously (not just suspecting it) took
building the ingredient, confirming each piece works via targeted
instrumentation and A/B toggles, and then tracing the SPECIFIC accessor
(`engine_latest_version()`) being used to judge "did it work" back to
which witness point it actually reads. All three organic ingredients
above were removed from the shipped corpus rather than landed half-working
— per this repo's own convention, unproven complexity that doesn't
demonstrably do its job doesn't ship; the differential-skew mechanism and
the per-write `poison_cas` ingredient from the entry above remain, since
those are independently useful (broad coverage; the necessary-but-not-
sufficient log ingredient) even though neither alone reproduces this
specific narrow gap.
- **A poll with exponential back-off has an average overshoot of half a
  step — when the producer already has a natural wake point, a
  multi-waiter watch removes that overshoot for free, and reaching for a
  single-`AtomicWaker` shortcut to build it is the wrong call the moment a
  second waiter can exist** (cluster-performance follow-up to ADR 0049 §5
  and issue #276). Every CP write confirm loop in `animusd::write_path`
  (`cp_put_local`/`cp_delete_local`/`cp_kind_raw_local`/
  `cp_kind_eval_local`/`poll_probe`) waited for its own accepted Raft entry
  to apply via `sleep(poll); poll = (poll * 2).min(CP_CONFIRM_POLL_MAX)`,
  `CP_CONFIRM_POLL_INIT` 200µs doubling to a 5ms cap. Real apply latency is
  a few ms, so the doubling schedule's own checkpoints (0.2/0.6/1.4/3.0/
  6.2ms) rounded every write up to whichever one came next — an average
  half-a-step overshoot on top of the real latency, the generic cost of
  *any* poll-based wait regardless of how well-tuned its interval is.
  The fix is not a shorter initial interval or a gentler back-off curve —
  both are still guessing at a cadence — but recognizing that the apply
  task **already knows the instant it makes progress** (it is the thing
  advancing `engine_applied`), so the wait belongs on a wake, not a timer.
  `animus-cp-data` grew `AppliedWatch`, a multi-waiter watch bumped at
  every site `engine_applied` advances, and every confirm loop's poll tail
  became one shared `wait_applied_past` helper parked on it — a single
  seed-driven measurement (`write_path::kind_eval_confirm_wake_tests`)
  showed 20 sequential single-item writes drop from 4ms of virtual time
  (the old scheme's `WRITE_COUNT * CP_CONFIRM_POLL_INIT` floor) to
  **0ns**, because under `SimEnv`'s zero-latency network the apply task
  simply gets its turn before the confirm task is polled again — no timer
  needed at all. **The single-`AtomicWaker` shortcut (issue #276) was
  wrong here for the identical reason it was wrong for `MetadataWatch`**:
  a single-tablet leader routinely has many concurrent single-item writes
  each confirming a distinct index at once, so `AppliedWatch` was built
  multi-waiter (a `Mutex<BTreeMap<slot, Waker>>` registry, register-
  before-check, `bump` drains and wakes every registered slot) from the
  very first line, rather than starting single-waiter "because today there
  is only one caller" and waiting for a second consumer to silently break
  it the way `MetadataWatch` did. The one thing a pure wake-on-progress
  wait cannot provide on its own is a **forced re-check**: if the awaited
  index never applies at all (a lost leadership before commit), nothing
  ever bumps the watch, so a bare `watch.changed(seen).await` with no
  timeout would hang forever even though the surrounding loop's own
  `confirm_wait_is_futile`/deadline logic is sitting right there ready to
  catch it — the wait just never returns control to let it run. Proven
  directly (`write_path::wait_applied_past_futility_tests`): a genuine
  three-voter leadership change, engineered with `Simulator::partition`/
  `heal` rather than `RaftKvNode::transfer_leadership` — an armed transfer
  only freezes *new* proposes, it does not stop an already-accepted entry
  from replicating and committing on a healthy zero-latency `SimEnv` link,
  so a first draft using it always observed `Confirmed`, never the
  `Superseded` the test needs; only fully isolating the leader before it
  ever proposes guarantees the entry can never commit at all. So
  `wait_applied_past` races its watch against a plain bounded sleep
  (reusing the old scheme's own cap, `CP_CONFIRM_POLL_MAX`, repurposed
  from an interval into a forced-recheck ceiling) — a wake-on-progress
  primitive that removes a *poll's* timer still needs its own timeout the
  moment "progress never happens" is a real, reachable state, not merely
  "progress happens late." (`crates/animus-cp-data/src/lib.rs`,
  `crates/animusd/src/write_path.rs`, `crates/animusd/src/lib.rs`.)
