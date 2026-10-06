# A chaos eventual-read violation must name the serving node, time and writer class

`[eventual-prefix]` (ADR 0055) failures from the chaos harness were
undiagnosable from CI annotations: the message gave neither the replica that
served the stale read, when, nor whether each missing value came from a txn or
a single-key append. Those three facts separate "oracle false positive" from
"one replica permanently applied entries as no-ops" (a divergence only eventual
reads can see, since the harness's final and convergence reads are consistent
and hit the leader).

The harness now records node + time per eventual read, classifies the writer
of every missing/extra value (txn vs single, ok/info/fail, invoke..done), and
at the end does a per-node eventual read of every key (`[replica-convergence]`)
plus a per-node counters dump (`counters.txt`) so permanent divergence is
distinguishable from a transient anomaly. When an oracle can only be satisfied
by a leader-routed check, add a per-replica check beside it.
