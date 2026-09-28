# Removing a peer needs a positive notification channel, not a timeout (and not a snapshot)

**Context**: issue #1061. A leader that removed a voter tracked it in a
leader-local `departing` map and kept replicating to it — through
`AppendEntries`, or, once its own log had been compacted past the peer, a
full chunked `InstallSnapshot` — until the peer acked. Two problems fell out
of one root: the duty was discharged by *state transfer* (replicate the
log/snapshot until it lands), and its bookkeeping lived only in the memory of
whichever node happened to be leader.

**The obvious fix, and why it was wrong.** The visible symptom was a
never-ending snapshot flood to a peer that could never ack (crashed, or its
host already released it). The first fix bounded it with a give-up timeout on
silence. That closed the flood and *opened a worse hole*: the peer that gave
up on was, in the partitioned-not-dead case, never told it had been removed,
and the host reconciler releases a replica only when its own log-derived
config excludes it — so it stayed hosted forever as a zombie. "Stop trying
after a while" is the wrong shape for a duty whose completion is the only
thing that ever tells the other side to stop: you have abandoned it, not
finished it. (The same hole already existed for any leadership change,
because `become_leader` cleared the map and re-derived nothing.)

**The fix**: give the duty a *positive, tiny, idempotent channel* that
carries the fact, not the state (`RaftMsg::Removed { stamp, membership }` +
an ack), and make it survive the failure modes that lost the old one:
1. **Never ship state to a peer that is leaving.** A snapshot is wasted bytes
   for a node that is no longer a member; the notice is a few dozen.
2. **Re-derive the obligation from durable state** (the config entries still
   in the new leader's log) instead of holding it in a leader-local map.
3. **Make the other side able to ask.** The removed peer's own election timer
   fires, and the leader answers a non-member's pre-vote with the notice: the
   obligation is then discharged by a message the *victim* sends, which no
   leader-side give-up, compaction or leadership change can lose. Put the
   fallback on the party that cannot be prevented from noticing the problem.
4. **Bound the volume, not the duty.** The give-up timeout survives, but only
   as a cap on traffic to a genuinely dead peer (a capped-backoff trickle of
   notices, minutes long), justified precisely because it is no longer the
   only channel.

**Corollaries that bit while building it**:
- A stale or delayed notice must be *unable* to un-member a node that was
  re-added since. Guard on term **and** on the notice's `(term, index)` stamp
  vs the latest config entry that includes the recipient (lexicographic, so a
  diverged uncommitted suffix orders correctly), clear the flag when a later
  self-including entry/snapshot lands, and keep the "I was told" bit
  *separate* from the log-derived config — adopting an out-of-log config into
  state that `apply_config`/`recompute_config` derive from the log is silently
  undone by the next truncation, or worse, survives it.
- Only notify for a **committed** removal: an entry the log can truncate is
  repaired by the log; a notice cannot be un-said.
- A per-peer backoff schedule for a *sustained writer* must be time-based:
  `replicate_now` (wake-on-propose) keeps pushing the heartbeat deadline out,
  so a schedule keyed off heartbeat ticks either never advances or fires per
  write.
- A vote-safety guard added to make "we gave up on a peer" safe (responder
  refuses a candidate it does not recognise) was unnecessary — the zombie's log
  lacks the removal entry, so `log_ok` already refuses it — and it had a
  liveness cost. Check what the *existing* check already proves before adding
  a second, staler copy of the same decision (see the #1019 lessons).
