# A driver that fixes its own order must tell the decision machine which node is next

**Found by**: ADR 0073 Phase 3, P3-D (operator partition driver); a unit test over `animus-roll`
let a roll proceed while an *old* node's roll-health was not ok.

`animus_roll::decide` picks "the next node" by its own order (data first, control voters, leader
last) and excludes **that node's** verdict from the gate (its answer is about to be stale). The
Kubernetes StatefulSet replaces pods highest ordinal first, so the pod actually about to be
restarted was a different node: the machine ignored the unhealthy verdict of the wrong node and
approved the restart of a node it had never judged.

Rules that generalize:

- When a state machine's "target" is also an input to its safety gate, any driver whose platform
  imposes a different order must pass the real target in (`decide_with_target(.., Some(next))`),
  never map the machine's answer onto a different node afterwards.
- Leadership-transfer logic belongs with the target too: the leader being the platform's next pod
  (not the machine's) is exactly when the transfer is needed.
- Test the driver with the verdict problem on a node that is *not* the machine's pick; a test
  where the machine's pick and the platform's pick coincide proves nothing.
