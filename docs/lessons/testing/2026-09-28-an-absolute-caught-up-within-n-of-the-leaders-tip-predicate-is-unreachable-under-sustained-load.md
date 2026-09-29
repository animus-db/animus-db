# An absolute "caught up within N entries of the leader's tip" predicate is unreachable under sustained load, and a bounded-burst-then-drain test hides this

**Context**: issue #1064 — under a directed-Placing reconfigure (ADR 0062: a
desired replica set differing from the current one by more than one member,
the post-split-cutover retarget shape) driven alongside a CONTINUOUS
(never-stopping) writer, `RaftCore::learner_caught_up`'s promotion
predicate (ADR 0058 Train 1) never returned `true` for an otherwise
perfectly healthy learner. The second desired replica was never even added,
and neither stale voter was ever removed — the group stayed wedged at its
old voter set indefinitely.

**What went wrong**: the predicate compared a learner's own tracked
`match_index` to the LEADER'S OWN `last_log_index()`, within a fixed
absolute threshold (`RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD = 4`). Under a
continuous writer, `last_log_index()` is not a stable "how far the group
has gotten" fact — it is the leader's own freshest LOCAL append, advancing
every time `reconfigure_step`'s caller (a production reconciler ticking
independently of the write stream) happens to sample it, including entries
nobody — not even another voter — has been sent yet. A caller that samples
this predicate anywhere near a live write stream sees a gap of "however
many entries the leader just appended for itself," which a single write
batch bigger than the threshold makes permanently unsatisfiable: the gap
that has to close by the next sample re-opens by the same amount (or more)
on every sample, forever. This is a *rate* mismatch dressed up as a
capacity problem, and no amount of real, genuine replication progress can
close a gap defined against a baseline that always moves at least as fast
as the thing racing to catch up to it.

**Why the existing test suite didn't catch it**: this exact promotion
predicate already had a dedicated regression
(`animus-cp-data/tests/learner_catchup_under_load.rs`, issues #532/#537).
That test drives a *bounded* write window, stops the writer, and only then
polls `learner_caught_up` to convergence — the "converged-or-timeout" idiom
this repo's own `CLAUDE.md` prescribes for eventual properties. That idiom
is correct for proving the learner *eventually* catches up once load stops,
but it structurally cannot exercise "is the promotion criterion satisfiable
while the writer keeps going" — the one condition `reconfigure_step`'s real
production caller (a reconciler on its own tick, a client write stream with
no natural stopping point) actually has to work under. A bounded-burst-
then-drain test and a permanently-wedged-while-load-continues bug are
invisible to each other by construction: the former never samples mid-load,
and the latter never manifests once load stops. The fix's own regression
(`tests/directed_placing_under_sustained_load.rs`) had to be built the
opposite way on purpose: keep the writer running for the entire test,
sample the predicate in the worst-case order (propose, then check,
*then* let time pass), and never stop until convergence or a timeout.

**A second trap found building that regression**: the first draft of the
new test used a write cadence (`BURST_GAP` equal to the default heartbeat
interval, with a fairly large burst size) that also happened to push the
group's total log length past `COMPACT_THRESHOLD` almost immediately. That
version failed even AFTER the real fix (comparing against `commit_index()`
instead of `last_log_index()`) — not because the fix was wrong, but because
it had accidentally started exercising a second, genuinely separate,
pre-existing defect: repeated `InstallSnapshot` transfer restarts under
compaction churn (the same class of mechanism suspected for issue #1061).
Two real bugs entangled in one test produce a confusing "the fix didn't
work" signal that sends the investigation toward the wrong one. The
practical rule: when a fix's own regression test still fails after the fix
lands, don't assume the fix is wrong — check whether the test's *load
profile* is incidentally tripping a different mechanism than the one under
test, by instrumenting the relevant counters (here, `Metric::
CpSnapshotTransferRestarts`/`CpSnapshotImageBuilds` via
`RaftKvNode::start_with_metrics`) before touching anything else. Giving the
test a deliberately generous settle time relative to the slow peer's own
round-trip cost (well below anything that could trigger compaction) isolated
the baseline-metric defect cleanly, and is now called out explicitly in the
test's own module doc so a future reader doesn't accidentally "fix" it back
into a load profile that re-entangles the two.

**The general lesson**: an eventual-convergence promotion/readiness
criterion should be defined against a quantity that is itself bounded by
real quorum progress (here, `commit_index()`, which cannot advance without
a majority of *voters* — never the candidate being measured — actually
acking), not against a value that a single caller can advance unilaterally
and arbitrarily fast relative to the write stream. And any test built to
prove a *liveness* property under load must actually keep the load running
while checking the property — a "run load, stop, then check" test proves a
different, weaker claim.
