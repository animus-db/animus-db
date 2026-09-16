# Move a timer's trigger, not its mechanism — and check what the seam already gives you for free (ADR 0058 rung 4)

The in-place split's write-blip regression (measured ~726ms vs. the copy
path's ~300ms, rung 3's own bench) traced to one cause: a freshly-forked
child Raft group has no leader until *some* replica's cold, randomized
election timeout eventually fires. The fix was **not** a new consensus
primitive — it was making the replica that already knows it should lead
(the parent's own current leader, a fact every replica already computes
locally) call the *existing* pre-vote round immediately instead of waiting
for `tick`'s timer to expire. `RaftCore::campaign_now` is three lines: a
role guard, then `self.start_pre_vote(now, entropy)` — the identical
function a real timeout calls.

Two things worth generalizing from this:

1. **When only *when* something fires needs to change, not *what* it does
   when it fires, reuse the existing mechanism at the new trigger point
   rather than building a parallel one.** A hand-rolled "immediate leader"
   path would have needed its own safety argument for every interaction
   pre-vote already has one for (a peer that hasn't started yet, two
   replicas racing to self-nominate, a lease check against a live leader).
   Calling `start_pre_vote` early inherits all of them for free — including
   the fallback: a round that wins no majority in time re-arms the ordinary
   election timer via the same `reset_election_timer` call the timeout path
   uses, so "campaign failed" and "never campaigned at all" converge on the
   identical retry, with no second code path to keep in sync.
2. **Before assuming an early message needs new machinery to reach a
   not-yet-started peer, check whether the transport already queues by
   destination regardless of consumer readiness.** ADR 0026's multiplexed
   `(node, stream)` addressing already queues an inbound message whether or
   not anything is currently polling `recv_stream` for that stream — true of
   both `ProdEnv`'s per-stream `Demux` and `SimEnv`'s inbox map. That made
   "send a `PreVote` to a peer whose own `RaftKvNode` for this child hasn't
   started yet" a non-problem: the message simply waits in that peer's inbox
   until its own bootstrap reaches its first receive, at which point it's
   just this group's first inbound message. No new buffering, no "is the
   peer up yet" probe, no retry-until-reachable loop.

The general shape: a live-timeout problem is not automatically a
"needs-new-protocol" problem — check first whether calling the existing
timeout handler early, at a caller-computed trigger, already has every
safety property the new call site needs, and whether the seam underneath it
already tolerates the ordering you're about to introduce.
