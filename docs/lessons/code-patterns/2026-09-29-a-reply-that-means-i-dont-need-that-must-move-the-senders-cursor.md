# A reply that means "I don't need that" must move the sender's cursor, or the sender offers it forever

**Context**: an `InstallSnapshot` receiver that is already past the offered
base declines with `InstallSnapshotResp { last_index: 0, next_offset: 0 }` —
deliberately *not* the shape of a completed install (PR #1048 removed the old
reply because it was indistinguishable from one and inflated install counters
and regressed `next_index`). The leader's handler for that reply treated it as
"mid-transfer, nothing buffered": it inserted a zero offset (a phantom
in-flight entry), re-sent chunk 0, and never touched `next_index`, which a
`needs_snapshot` echo had reset to 1. The follower was past the base, so it
declined again, forever — a self-sustaining loop at zero write rate that
pinned the leader's compaction, inflated `cp_snapshot_transfer_restarts`
every 2s and starved the follower of log entries.

**Lesson**: when a protocol grows a "no thanks" reply, the sender's state
machine needs a transition for it that *advances or clears whatever made it
make the offer* — the cursor (`next_index`), the "already served" memory, the
in-flight bookkeeping. A reply the sender ignores is a livelock waiting for the
one trigger that re-arms the offer. Check every reply shape has a handler that
makes progress, not just one that does not corrupt.

Related traps hit while fixing it:
- Discriminate the reply by *which request it answers*, not by its bare
  fields: `(0, 0)` to a chunk at offset 0 can only be a refusal (an accepting
  receiver buffers it), while the same `(0, 0)` to a chunk at a nonzero offset
  means "I lost my buffer". Use the request's own offset, held in the sender's
  bookkeeping.
- An "offer" is often resent several times before the first ack, so the
  refusal arrives more than once: the second must be a no-op, not re-create
  the bookkeeping the first cleared.
- A bare-core harness that delivers messages to a fixpoint turns the
  legitimate `needs_snapshot` ack/`AppendEntries` ping-pong (which is
  RTT-paced in production) into an infinite recursion; deliver one hop per
  tick.
- A voter that never answers (an absent third node in a 2-of-3 harness) is
  legitimately re-offered a snapshot forever — count only what the live peer
  is sent, or the assertion measures the wrong thing.
