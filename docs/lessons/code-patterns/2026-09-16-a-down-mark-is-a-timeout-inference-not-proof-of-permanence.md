# A `Down` mark is a timeout inference, not proof of permanence — a reconfigure must add the replacement before evicting it

A failure detector's `Down` transition is, by construction, "no heartbeat
within the detection window" — a probabilistic inference tuned for fast
reaction, not a proof that the marked node is gone forever. A rolling
restart with durable storage (a Kubernetes pod recreation, a process
respawn under a supervisor) routinely takes longer than a tight detection
window and looks, from the detector's point of view, exactly like a real
failure. Any automatic repair path that reacts to `Down` has to be correct
for *both* cases — the node never comes back, and the node comes back in a
few seconds — without knowing in advance which one it is in.

The mistake this generalizes from (issue #920): a single-server Raft
reconfiguration step removed an extra `Down` voter *immediately*, ahead of
adding its replacement through the group's own add-then-catch-up-then-
promote learner phase. That ordering is sound under one assumption —
"the removed node is never coming back, so there is nothing to wait for" —
which is true for a genuine permanent loss and false for a transient one.
Removing the old voter before the replacement is safely a voter shrinks
the live quorum requirement for the whole in-flight window (3 voters → 2,
instead of 3 → 4 → 3 via the learner phase every other reconfigure path
already used). If the same rolling operation that triggered the `Down`
mark then goes on to touch a *second* voter — an entirely ordinary next
step of an ordinary rolling restart — the group can permanently lose
majority, because the node evicted early has nowhere to rejoin: placement
no longer names it a replica, so its own host/reconciler will not bring it
back, and nothing else can recreate the vote it would have cast.

**The general pattern**: when a repair/reconfigure decision is driven by a
liveness signal that is itself a timeout inference (a failure detector, a
health check, a missed-heartbeat threshold), any step that *reduces*
redundancy (removing a voter, dropping a replica, shrinking a quorum) must
wait until any step that *replaces* it has already restored the pre-move
safety margin — never the other way around, and never "fire immediately,
there's nothing to lose" reasoning that is only true in the permanent-loss
case. This is the same ADD-before-REMOVE discipline single-server Raft
membership changes already use for an ordinary healthy rebalance; the bug
was applying a different, unsafe ordering specifically to the down-node
path, on the reasoning that urgency justified skipping the safety margin.
Urgency is exactly the wrong reason to skip it: the down node might come
back on its own in the time the safety margin would have bought, and if it
doesn't, removing it one tick later than "immediately" costs nothing.

**How this surfaces in practice**: a symptom that looks like "the whole
cluster is stuck for tens of seconds with no leader reachable" long after
a routine, successful rolling restart has finished, with every node
individually healthy. The stuck state is not a timing/timeout tuning
problem (raising the detection window only shrinks the reproducing window,
it doesn't close it) and not a consensus-safety bug in the classic
split-brain sense (the group never elects two leaders — it just can no
longer elect any leader at all, because the majority denominator now
requires a vote from a node that will never rejoin). Reproduce it by
racing a genuine crash-and-restart against the real failure detector (not
a hand-injected placement command) at a depth that actually trips the
detector's own timeout, then continuing the rolling operation into a
second voter while the first repair is still in flight — a repair
triggered by a hand-crafted membership command alone will not show this,
since it skips the "was this actually urgent, or just slow to come back"
ambiguity that only a real timeout-driven `Down` transition creates.
