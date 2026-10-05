# An Elle workload cannot witness "writable during the fault"; use a probe writer

**Context:** issue #1219 (disk-full leader step-down). The first version of the
new assertion counted Elle-recorded acked writes inside the fault window and
failed on a seed where the step-down had demonstrably worked (the new leader was
committing, `commit_index` advancing).

**Why:** the corpus clients read keys they may never have written, and a read of
an absent key returns `None`, which the harness treats as ambiguous and polls
until `OP_BUDGET` (9 s). A few such reads park every client for longer than the
whole fault window, so "no acked workload write in the window" says nothing
about availability. It would also have been seed-luck-dependent in the other
direction (a pass without the fix).

**Do:** for an availability-during-fault property, add a dedicated probe writer
on its own key, outside the Elle history, that writes through the current leader
and confirms with a linearizable read, recording ack times; assert acks past a
grace period after the fault. Stop it before the convergence/finalize checks so
it cannot race them. Prove the assertion has teeth by disabling the fix and
watching it fail (here: `if false && c.is_leader()` on the step-down).

**Related:** a leader-only fault can let the workload drain entirely inside the
window, so "acked writes after the heal" is only a valid assertion for faults
that take out the whole quorum.
