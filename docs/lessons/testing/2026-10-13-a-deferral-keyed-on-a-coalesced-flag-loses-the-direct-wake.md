# A deferral keyed on a coalesced wake flag loses the direct wake that arrives with it

**Context.** ADR 0079's awake-group admission defers *bulk* wakes (the host
reconciler's `Down` wake) and must admit every other wake over the cap (a client
request's `RaftKvNode::wake()`). Both ride one `WakeSignal`: a raised flag plus a
waker, consumed by the consensus loop, so two wakes in one loop iteration become
one.

**What went wrong.** The first version added a `bulk` flag and treated "bulk
flag set" as "this wake may be deferred". The reconciler re-wakes every hosted
group each tick, so a client `wake()` that landed in the same iteration found
`bulk` already set and was deferred: a queued group could not be rescued by the
very client request the soft cap promises to serve. The corpus cell caught it
(13 groups stayed leaderless after the clients arrived); no unit test of the
admission alone could, because the loss is in how two wakes coalesce.

**Rule.** When a deferrable and a non-deferrable trigger share one coalescing
signal, give the non-deferrable one its own flag (`direct`) that *wins*, and
decide "deferrable" as `bulk && !direct`. A flag that says "this wake is allowed
to be refused" must be impossible to set on behalf of a wake that is not.

**Also.** A leader cannot quiesce while any voter is behind
(`quiesce_entry_ok`), so after a node is killed, groups woken for it stay awake
and the awake slots never turn over. Any admission scheme that frees slots "when
another group quiesces" must be tested with the node dead (slots stuck) as well
as merely marked `Down` (slots turn over); the two have different liveness.
