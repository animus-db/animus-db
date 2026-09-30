# Never promote a voter that cannot vote yet: a vote-refusing boot gate plus a stale wait-for-all set can deadlock forever

**Context**: issue #1131. A learner hosted fresh for a tablet still runs the
#667 boot-time cluster check, which refuses all votes until it resolves. The
check's wait-for-every-peer set is snapshotted from the learner's stale
bootstrap peers. The leader promoted the learner (catch-up was the only gate)
before the check resolved; the next probe reply named the learner as a voter,
took the ambiguous wait-for-all path, and one dead peer in the set made it
wait forever. With one original voter killed, the 4-voter group could not elect
(~3/46 under CPU load, invisible to idle runs).

**Lesson**: "cannot vote yet" is pending OR refused — a wiped voter whose check
resolved to refused is just as mute, so the reported flag must cover both
(a first draft reported only the pending half).

**Also**: the moment a node becomes a voter it counts in the quorum
denominator, so promotion must require that it can actually vote, not only that
its log is caught up. Gate the decision at the source (the learner reports its
own "cannot vote yet" state on the ack the leader already reads) rather than
trying to make the gated node's own wait safe. A wait-for-all aggregation over a
set captured at boot is only as live as its least-live member.

**Why**: the gate only delays promotion, so it cannot weaken the safety property
the boot gate protects. Test it by making the learner's check unresolvable
(dead bootstrap peer, links cut) while it is otherwise caught up, then kill a
voter; a virtual-time "elects within budget" assertion fails without the gate.
