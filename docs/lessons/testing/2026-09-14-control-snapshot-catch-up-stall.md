# Control-plane `InstallSnapshot` catch-up stall (issue #898)

## What happened

`crates/animus-control/tests/prod_liveness.rs::large_metadata_catch_up_stays_live`
flaked intermittently on CI (`prod-liveness-scattered`), the second recurrence
of the shape #741 fixed (that fix made the *compaction-convergence poll*
progress-gated; this failure was a *different* poll in the same test — the
follower's own 12s catch-up budget — genuinely, permanently stalling with
`control term Δ0`, i.e. no election, a stable leader/follower relationship
that simply stopped making progress). A freshly-joined follower (`node2`)
completed exactly one `InstallSnapshot` install to an intermediate index and
then made zero further progress for the rest of the run, while the leader's
own `snapshot_index()` had already moved far past it.

## Root causes (two, compounding)

1. **`animus-control`'s `meta_apply_and_compact` never adopted
   `animus-cp-data`'s own `COMPACT_DEFER_CEILING` gate**, despite its own doc
   comment claiming it "mirrors `animus-cp-data::apply_and_compact`'s
   shape/ordering precisely." `RaftCore::snapshot_upto` unconditionally
   invalidates every peer's in-flight chunked transfer the moment the
   snapshot base moves again (required for correctness for a `DRIVER_APPLIED`
   lazily-built blob). Without a defer, ordinary sustained metadata churn
   (even a handful of trailing commits, not perpetual load) can re-cross
   `SNAPSHOT_THRESHOLD` while a lagging follower's transfer is still on the
   wire, restarting it from chunk 0 against a newer image — repeatedly,
   forever, if churn keeps winning the race. `animus-cp-data` had already
   found and fixed this exact class for the data plane (issues #532/#537);
   the control plane's own `raft.rs` doc comments even *referenced*
   `COMPACT_DEFER_CEILING` as an established concept, but the driver-side gate
   that actually uses `RaftCore::snapshot_transfer_in_flight()` was never
   written for `meta_apply_and_compact`.

2. **`RaftCore::snapshot_transfer_in_flight()` had a real gap even for a
   driver that DOES defer**: it was defined purely as "`snapshot_offset` is
   non-empty," and `snapshot_offset` is populated only once a peer's FIRST
   ack is *processed* — not the moment a chunk is *sent* (that's tracked
   separately, in `snapshot_chunk_sent`, precisely for the unrelated
   resend-cap mechanism). That leaves a real window, from "leader ships chunk
   0" to "leader processes the first ack" — stretchable arbitrarily far by a
   slow/contended peer or link (exactly `ProdEnv`'s real-thread CI shape) —
   during which a transfer is genuinely on the wire but the accessor reports
   `false`, so a threshold crossing landing inside that window still
   invalidates it. The defer gate from fix 1 never engaged during exactly the
   window it mattered most. Fixed by widening the accessor to also check
   `snapshot_chunk_sent` — both maps are already cleared together at every
   existing invalidation/completion point, so this is a strictly more
   accurate reading of the same underlying fact, and it benefits
   `animus-cp-data` for free since both planes share the same `RaftCore`.

## Two more, found validating this fix under real `ProdEnv` contention (not covered by fixes 1-3 above)

Fixes 1-3 were written and never gate-validated before landing — resuming
this work from that checkpoint, running the full crate suite plus 20x
`prod_liveness.rs` stress reps (per CLAUDE.md's flaky-test rule) surfaced two
further, independent gaps in the SAME mechanism:

4. **A leader's own sender-side snapshot bookkeeping (`snapshot_offset`/
   `snapshot_chunk_sent`) survived losing leadership.** `RaftCore`'s generic
   higher-term step-down (`handle`, the one place that already cleared a
   stale `transfer_target` "so a future `is_leader`-independent inspection
   never reports a transfer in flight for a node that isn't leading") never
   cleared these two maps the identical way. Unlike `transfer_target`,
   `snapshot_transfer_in_flight()` (what the fix-1 defer gate reads) has no
   `role == Leader` guard of its own — so a sender-side entry left over from
   a brief, real leadership stint (ordinary `ProdEnv` election churn under
   contention, not a bug in itself) persisted into this node's NEXT life as
   a follower and made its own local compaction defer forever on behalf of a
   transfer that no longer has a sender. Confirmed live: `large_metadata_
   catch_up_stays_live` failing ~20-40% of stress runs with one 2-node
   majority replica pinned at a small `snapshot_index` while its
   `engine_applied_index` had fully caught up — the classic "stuck defer"
   signature, not a slow apply task. Fixed by clearing all three snapshot
   maps at that same step-down site, mirroring `transfer_target`'s own
   precedent exactly.

5. **Even with fix 4, a peer that never acks at all can still wedge
   compaction forever** — the scenario that matters in production, not just
   this test: a down/partitioned replica, or (what this test deliberately
   does) a configured cluster member that simply hasn't started yet.
   `SNAPSHOT_COMPACT_DEFER_CEILING`'s escape hatch is sized in `behind`
   (bytes/commands applied past the snapshot base), which requires **more
   writes** to ever cross — once a burst of writes stops, `behind` stops
   growing, and a peer that will never ack holds the defer open
   indefinitely for a transfer that no longer has anyone to protect.
   **Three designs were tried here, in order, before landing one that
   actually holds**:

   1. A resend-COUNT-based attempt (treat `SNAPSHOT_TRANSFER_STALE_RESENDS`
      consecutive un-acked heartbeat resends to one peer as "give up
      deferring for it") — **rejected**: `snapshot_compaction_race.rs`'s
      own deliberately-slow link needed ~40 heartbeat-driven resends before
      its peer's first-EVER ack — a real, correct, merely-slow transfer —
      landing right at a plausible-looking count threshold and reopening
      the original invalidation race for a transfer that was actually
      fine. The count is a proxy for elapsed time, and it's a bad one: it
      scales with `heartbeat_interval`, not with anything about how slow a
      genuinely live peer might legitimately be.
   2. A flat **time-since-streak-started** ceiling
      (`SNAPSHOT_COMPACT_DEFER_TIME_CEILING`, 10s), plus a proposed faster
      secondary escape gated on `RaftCore::peer_last_contact` going stale
      — **also rejected**, for two independent reasons found later, not up
      front: (a) `become_leader` OPTIMISTICALLY seeds every peer's
      `last_contact` to the moment leadership begins (see that method's
      own doc — the ADR 0037 liveness mechanism this reuses), so a peer
      that was never really contacted at all and a peer that stopped
      responding mid-transfer are BOTH "stale relative to leadership
      start" by that field alone — the exact distinction this design
      needed to make, and couldn't; (b) a flat ceiling counting from
      streak start (rather than idling) conflates "this transfer has run a
      while" (fine — a large multi-chunk snapshot legitimately needs many
      round trips) with "this transfer has made no progress in a while"
      (the actual question) — `animus-cp-data/tests/
      hlc_differential_skew.rs`'s own crashed-peer scenario (see gap 6,
      below) needed an answer within a few virtual seconds, far tighter
      than any margin safe for a large real transfer's total duration; no
      single constant honestly answers both questions.
   3. **Shipped**: an **idle-progress-gated** ceiling —
      `SNAPSHOT_COMPACT_DEFER_IDLE_CEILING` (2s) — bounding idle time
      since the LAST observed forward progress, never total transfer
      duration. `RaftCore::snapshot_transfer_peers()` (reinstated, then
      repurposed, after design 2's rejected peer-recency use of it) names
      the outstanding peer(s); the PRE-EXISTING `RaftCore::
      snapshot_chunk_advances(peer)` — already the exact metric
      `snapshot_resend_bound.rs` uses for the identical "genuine progress,
      not resends" reason — gives a real forward-progress signal.
      `meta_apply_and_compact` sums `snapshot_chunk_advances` across every
      outstanding peer each pass and resets its own `compact_defer_since:
      Option<Nanos>` (paired with a `compact_defer_progress: Option<u64>`
      tracking the last-seen sum) to `now` every time that sum changes —
      so the idle clock measures only time since the last genuine chunk
      advance, correctly giving a large-but-progressing transfer
      unlimited patience while still catching a genuinely stuck one within
      2 seconds. `RaftCore::snapshot_transfer_in_flight()` needed **no
      change** across any of the three designs — it stays a pure,
      `now`-unaware fact exactly as its own doc argues it should; the
      time-awareness lives entirely in the driver that already has
      `env.now()` in hand.

6. **Found only by running `cargo test -p animus-cp-data` in FULL (not
   `--lib`, which the task's own gate list initially specified and which
   skips every `tests/*.rs` integration binary) — design 2 above, while it
   still stood, regressed a wholly separate, PRE-EXISTING test in the
   plane that shares this same `RaftCore`.** `animus-cp-data/tests/
   hlc_differential_skew.rs::receiver_installs_the_durable_high_water_
   mark_not_just_the_rows` (ADR 0018 §2's issue #804 amendment) crashes
   and partitions one replica, then asserts the SENDER's own compaction
   durably advances its watermark past a failed-CAS burst that writes no
   row. The crashed replica's own phantom, never-to-be-acked
   `InstallSnapshot` chunk (queued the moment the leader's first
   compaction made it eligible — the identical `snapshot_transfer_in_
   flight` widening from fix 2, read by THIS plane's own pre-existing
   `apply_and_compact`/`COMPACT_DEFER_CEILING` gate too) held the sender's
   own compaction deferred for the whole test, since design 2's flat 10s
   ceiling never got anywhere near firing inside this test's own
   few-virtual-second budget. Fixed by mirroring design 3 (the idle-
   progress-gated ceiling) into `animus-cp-data`'s own `apply_and_compact`/
   `apply_loop` — `COMPACT_DEFER_IDLE_CEILING`, `compact_defer_since`/
   `compact_defer_progress`, byte-for-byte the same shape as
   `animus-control`'s own.

**The generalizable lesson underneath fixes 1, 4, 5, and 6**: this whole
class of bug is the same shape wearing different clothes — some piece of
leader-only bookkeeping outlives the condition that justified creating it
(an active send, an active leadership stint, an active peer, genuine
recent progress), and nothing yet reads *how long* it's been stale before
treating it as still meaningful. Any time new bookkeeping is added to feed
a defer/backoff gate, ask explicitly: what retires this entry, and is
retirement guaranteed to happen on every path that could make it
meaningless (not just the "happy path" completion)? A pure, `now`-unaware
core is the right place for the *fact* (is a chunk outstanding); it is the
*wrong* place to decide how long is too long — that answer needs
`env.now()`, which only the driver layer has, and belongs there even when
it takes an extra parameter threaded through a driver-owned loop to get it
there. **And a second, sharper lesson from gap 6 specifically: "how long is
too long" must be measured as idle time since the last real progress, not
total elapsed time since the condition started** — the two questions look
interchangeable until a test with a tight time budget and a test that
legitimately needs a long time both exist, at which point no single flat
constant can answer both honestly; only an idle-since-last-progress design
can. **And a third, from HOW gap 6 was found: a fix to `RaftCore` (shared
by two planes) is not validated by one plane's own test suite alone — the
gate for any `animus-control`'s `RaftCore`/driver change is `cargo test -p
animus-cp-data` run in FULL, never `--lib`, which silently skips every
integration test in that crate.**

## The lesson that generalizes: fixing a race can unmask a second, previously-masked one

Landing fixes 1+2 above made a **pre-existing, previously-green** fixed-seed
test — `chunked_snapshot_receiver_stop_restart_3` — start failing
deterministically. Not a new bug introduced by the fix in the naive sense:
the underlying gap already existed (a leader's per-peer `snapshot_offset`/
`snapshot_chunk_sent` bookkeeping is never reset when that peer's *process*
restarts — same `NodeId`, but a brand-new, empty `RaftCore` — and a
restarted follower reporting `next_offset == 0` forever can never be
reconciled against a leader that keeps resending from its own stale, now-
unreachable non-zero offset, since `handle_install_snapshot`'s reassembly
gate requires `offset == 0` on a fresh buffer). It was **masked** before
fix 1 by the very *absence* of a compaction defer: ordinary
threshold-triggered recompaction happened often enough, independent of any
particular peer's transfer state, to incidentally wipe this stale
bookkeeping clean before a restarted peer's next request ever hit it. Making
compaction *less* eager (the whole point of the defer) removed that
incidental safety net and let the latent gap actually deadlock.

**The general form**: a fix that makes some background process fire *less
often* (a defer, a backoff, a coalescing change) can remove an *accidental*
cleanup/reset side effect that a completely different code path was quietly
relying on for correctness. The reliable way to catch this is exactly what
caught it here — **re-run the full existing test suite for the crate/plane
being touched, not just the new regression test**, before calling a fix
done. A green new test proves the fix closes the reported hole; it says
nothing about whether the fix opened a different one. The follow-up fix here
(rebase a peer's tracked snapshot offset down to its own reported truth once
it reports a regression `SNAPSHOT_OFFSET_REGRESSION_REBASE` times in a row,
rather than blindly taking the max forever) had to distinguish "a stale,
reordered ack for the ongoing transfer" (self-heals within a round trip —
tolerate it, per the existing monotonic guard) from "the peer's buffer
genuinely reset" (persists indefinitely — must be honored). A bounded
consecutive-count threshold, not a one-shot check, is what makes that
distinction safely: seeing a regression once is not evidence of a restart,
seeing it many times in a row with zero intervening forward progress is.

## Where the mechanism lives

- `crates/animus-control/src/node.rs`: `SNAPSHOT_COMPACT_DEFER_CEILING` +
  the defer gate in `meta_apply_and_compact` (fix 1);
  `SNAPSHOT_COMPACT_DEFER_IDLE_CEILING` + the `compact_defer_since:
  Option<Nanos>`/`compact_defer_progress: Option<u64>` locals (owned by
  `meta_apply_loop`, threaded through `meta_apply_and_compact`'s own
  signature) that track idle time since the last observed forward
  progress (fix 5, design 3 — the shipped one).
- `crates/animus-control/src/raft.rs`: `RaftCore::snapshot_transfer_in_flight`
  widened to check `snapshot_chunk_sent` too (fix 2); `become_leader` now
  clears `snapshot_chunk_sent` alongside `snapshot_offset` for the same
  reason it always cleared the latter; `snapshot_offset_regressions` +
  `SNAPSHOT_OFFSET_REGRESSION_REBASE` in `handle_install_snapshot_resp` (fix
  3, the unmasked-bug fix); the generic higher-term step-down in `handle`
  now clears `snapshot_offset`/`snapshot_offset_regressions`/
  `snapshot_chunk_sent` alongside `transfer_target` (fix 4);
  `RaftCore::snapshot_transfer_peers()` (the outstanding-peer set) plus the
  pre-existing `RaftCore::snapshot_chunk_advances(peer)` are what fix 5's
  design 3 reads for its progress signal — no other change to this file for
  fix 5's own three design iterations.
- `crates/animus-cp-data/src/lib.rs`: `COMPACT_DEFER_IDLE_CEILING` +
  matching `compact_defer_since`/`compact_defer_progress` locals in
  `apply_and_compact`/`apply_loop` — the identical fix 5/design-3 shape,
  needed here too since this plane shares the same `RaftCore` and reads the
  same `snapshot_transfer_in_flight()` (fix 6).
- `crates/animus-control/tests/snapshot_compaction_race.rs`: the new
  deterministic `SimEnv` regression for fixes 1+2 (an artificially slow
  leader<->follower link stands in for `ProdEnv`'s real contention, since
  `SimEnv`'s own near-zero default latency lets a small synthetic transfer
  outrun even a bursty churn schedule and never exposes the race). This same
  test is also what caught fix 5's own design-1 (rejected) resend-count
  attempt reopening the original race — see its own module doc.
- `crates/animus-control/tests/control_corpus.rs`'s pre-existing
  `chunked_snapshot_receiver_stop_restart_3` is what caught fix 3's own
  necessity — no new test was needed for it beyond making that one pass
  again.
- `crates/animus-cp-data/tests/hlc_differential_skew.rs`'s pre-existing
  `receiver_installs_the_durable_high_water_mark_not_just_the_rows` is what
  caught fix 5's own design-2 (rejected) flat-time-ceiling attempt as a
  regression in this OTHER plane, and is what fix 6 makes pass again — see
  gap 6 above for the full mechanism.
- None of fixes 4, 5, or 6 has a dedicated `SimEnv` regression (all are
  `ProdEnv` real-thread-contention/real-leadership-churn/cross-plane
  shapes — see `prod_liveness.rs`'s own module doc for why this class of
  property needs a real-thread integration guard rather than a
  virtual-clock one): validated by 10-20x stress reps of `prod_liveness.
  rs::large_metadata_catch_up_stays_live` per round (0 failures with the
  shipped design, versus a 20-40% failure rate with fixes 1-3 alone) plus
  the full `cargo test -p animus-cp-data`/`cargo test -p animus-control`
  suites for fix 6.
