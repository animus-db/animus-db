# A test of one of two redundant channels must silence the other, or it passes vacuously

**Context**: issue #1061 gave a removed peer two independent ways to learn of
its removal: the leader's own notice schedule (leader-initiated), and the
leader's reply to the peer's own pre-vote (receiver-initiated). The first
draft of the "leader sends a notice, never a snapshot" test passed — and
**also passed with the leader-initiated branch deleted**, because after the
partition healed the peer's election timer (150–300ms) always fired before
the leader's backoff gate (up to 1.6s) reopened, so the receiver-initiated
reply taught the peer first and the branch under test never ran.

**Lesson**: when a mechanism has redundancy on purpose, "the outcome
happened" proves nothing about which path produced it. For each channel:
- silence the *other* one in the harness (here: `mute(v)` drops everything the
  peer sends, so its campaign cannot reach the leader; `no_campaign(v)` drops
  only its `PreVote`/`RequestVote` while acks still flow);
- then run the **mutation check**: disable the channel under test and confirm
  the test goes red. A test that survives deleting its own subject is decoration.

Two related harness traps from the same work:
- A bare-`RaftCore` harness for a `DRIVER_APPLIED` state machine must emulate
  the driver's side effects (`take_snapshot_needed` -> `set_snapshot_blob`,
  `drain_pending_install`, `mark_durable_through`). Without them a snapshot
  path silently sends *nothing* (the image is never built), so a "never ships a
  snapshot" assertion passes vacuously — and a fresh-replica install test
  never delivers anything.
- An existing test that waited for the *old* channel
  (`reconciler_corpus`'s `partition_blocks_release` waiting on the removal
  entry landing in the peer's log) needed widening, not a retry: the new
  channel legitimately usually wins that race. Adapt the predicate to what the
  test actually means ("the replica learned it was removed"), and say why.
