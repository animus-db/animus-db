# A live check that does not move the metric you built the fix for is measuring a different bug — attribute before you tune

**Context**: issue #1061's removal notice was built to stop a departing peer's
snapshot transfers from restarting on every compaction
(`cp_snapshot_transfer_restarts`). The first live check (bulk seed + random
writes, then **five minutes with no writes**) still showed the counter
climbing at ~27/min on two nodes and ~105/min on a third — at a **zero write
rate**, which no compaction-driven mechanism can explain, and the same
counter on `main` looked no better. Two readings were on the table: "the fix
is incomplete" and "the fix is fine and the counter is measuring something
else". Guessing between them would have produced a plausible-sounding change
either way.

**What settled it**: a throwaway instrument at the one line that increments the
counter, printing *tablet, peer, its `match`/`next_index`, the transfer
bookkeeping and the leader's compaction floor* — then read the lines for the
idle window only. Every increment was one tablet, one **voter** peer (never a
departing or removed one — `departing = {}` in every line), `next_index = 1`,
`snapshot_chunk_sent = (0, 6683)`: the same offer re-sent thousands of times to
a peer already past its base. A different bug, older than the branch,
untouched by it — found only because the instrument named the (tablet, peer,
role) instead of a total.

**Lessons**:
- A counter's increment site can be *reachable without the thing it is named
  after*: `CpSnapshotTransferRestarts` is bumped when the idle ceiling overrides
  an in-flight transfer, but the compaction that follows may no-op, so nothing
  actually restarts. Before treating a metric as ground truth, read what
  guarantees the event it names.
- The hold-with-no-writes phase is the cheapest way to separate a
  write-driven mechanism from a self-sustaining one. Keep it in every live
  check of a "this flood is bounded" claim.
- Attribute first (which group, which peer, which role, does that peer still
  host the replica), fix second. The instrument is throwaway — remove it before
  committing — but the observability it motivated (`departing`/
  `snapshot_transfer_peers` on `/admin/raftkv`, the removal counters) is worth
  keeping, and a first version of a new counter should be sanity-checked
  against the others live (here `ignored == sent` on some nodes exposed that
  acks answering a stranger notice were being filed as stale).
- Prove the *pre-existing* half on the base branch: build `main` in a scratch
  worktree with its own `CARGO_TARGET_DIR` and run the same script, rather than
  asserting "it reproduces on main" from reading the code.

**A metric can also *under*-report the same bug.** The identical livelock, in a
run where two voters were stuck below the leader's base, froze that tablet's
commit index outright (the leader commits the third-highest `match_index` of
four voters) — the workload stalled, a reconfigure logged
`transfer_leadership rejected` ~1,650 times — while
`cp_snapshot_transfer_restarts` read 2, because a stalled tablet's `behind`
never crosses the compaction threshold the counter is gated on. A flat counter
is not evidence of health; a flat *throughput* number (items seeded, commits)
next to it is. Record both in a live check, and note run-to-run variance:
across the five runs of unfixed builds made while chasing this (`main` and the
notice branch), the busiest node's restart count for the same workload ranged
from 2 to 355, and one run with no growth at all in the idle hold still had
stuck groups.
