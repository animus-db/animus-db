# `RaftKvNode::linearizable_get`/`linearizable_scan` only ever serve on the confirmed leader — calling them on a follower returns `None` unconditionally (the ReadIndex ban unconditionally fails for a non-leader), not a slow or stale read.

**`RaftKvNode::linearizable_get`/`linearizable_scan` only ever serve on the
confirmed leader — calling them on a follower returns `None` unconditionally
(the ReadIndex ban unconditionally fails for a non-leader), not a slow or
stale read.** A test that wants to confirm a write *replicated* to a
follower (as opposed to confirming linearizability) must read that
follower with `local_get` (a raw, non-linearizable engine read), not
`linearizable_get` — calling the latter on whichever handle isn't currently
leading is not "testing the follower," it's testing a guaranteed `None`,
and asserting `Some(value)` against it fails deterministically regardless
of how long you wait. Caught immediately (first run) by the ADR 0031 PR5
reconciler corpus's 2-replica scenarios asserting both replicas' handles
via `linearizable_get` — fixed by reading the leader linearizably and
polling the follower with `local_get`.
