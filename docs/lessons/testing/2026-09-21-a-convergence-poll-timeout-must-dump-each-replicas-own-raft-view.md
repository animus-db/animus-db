# A "leaderless or under-replicated" convergence timeout has two unrelated causes with one symptom — dump every survivor's own Raft view (role/term/voters/learners) at the timeout, and expect a kill issued right after the *control-plane* map converges to land on the data plane mid-reconfigure

**A "leaderless or under-replicated" convergence timeout has two unrelated
causes with one symptom — dump every survivor's own Raft view
(role/term/voters/learners) at the timeout, and expect a kill issued right
after the *control-plane* map converges to land on the data plane
mid-reconfigure** (issue #1019, 2026-09-21, `animusd/tests/cluster_growth.rs::
dashboard_health_recovers_after_grown_cluster_loses_an_original_node`, one
CI failure in ~306 tests). The test's health poll fans `/admin/raftkv` out
over every survivor and needs, per tablet, a leader plus a full replica
set; its 30s timeout panicked with no state at all. The issue's own
reading (and this repo's prior lesson on the same test) pointed at a
*known* hazard — a grown node's `remote_metadata` mirror long-poll-parked
on the killed node, which replies ~8s late with stale data — and that
hazard is real: forcing the killed node to be the control leader
(`POST /admin/control/transfer`) and tracing `remote_metadata_watch_loop`
showed exactly the 8.0s zombie park. But it self-healed in ~50ms once the
loop fell through to a live seed, and the group that stayed leaderless
for 27s+ was one the mirror had nothing to do with. What the per-survivor
dump showed instead: every survivor of that tablet a *follower*, for the
rest of the run. The cause was a Raft election deadlock
(`docs/lessons/code-patterns/2026-09-21-never-gate-vote-granting-on-the-
responders-own-membership-view.md`): the killed node led that group and
had just promoted a learner, the promotion entry had reached the other
voters but not the learner, and the learner refused to vote because it
still believed it was a learner — a *permanent* leaderless state that no
timeout budget fixes. Three things generalize. (1) **A poll that waits
for the control-plane `tablet_map` to converge and then injects a fault
is injecting it into the data plane's still-running reconfigure**
(learner add → catch-up → promote → leadership transfer → old-voter
removal take seconds after the map already says "moved") — that is the
window this bug needed, and it is the normal shape of every growth-then-
kill test, so the timeout dump must show the data-plane config the kill
landed on (the test now prints every node's `/admin/raftkv` view right
before the kill, too). (2) **"Leaderless" and "a grown node never hosted
its replica" are indistinguishable from leader/replica *counts*; only
each node's own `role`/`term`/`voters`/`learners` separates them** — the
counts-only panic message cost this bug a whole reproduction round, and
the state was already one admin GET away. (3) **Forcing a suspected
precondition is how to make a 1-in-300 failure reproducible enough to
trace, and the rate it produces is itself evidence**: forcing "killed
node = control leader" moved the rate from 0/16 to 1/29 — enough to get
one fully-traced failure, and low enough to say that condition was
necessary-adjacent, not sufficient, which is what sent the investigation
from the mirror to the Raft config views. Corollary for this file's
older `cluster_growth.rs` entry (2026-08-12): its "only when all three
tests run concurrently in one binary" trigger no longer describes CI —
`prod-liveness-animusd` runs under nextest with one process per test and
`test-threads=1` (`.config/nextest.toml`), so a flake there is reproduced
by one test alone under CPU pressure, never by the binary's siblings.
