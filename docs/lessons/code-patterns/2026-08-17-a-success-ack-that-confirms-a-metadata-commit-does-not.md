# A success ack that confirms a metadata commit does not confirm the asynchronous machinery that commit triggers — a client-facing "created" reply must wait for *serveability*, not hand the client the formation window.

**A success ack that confirms a metadata commit does not confirm the
asynchronous machinery that commit triggers — a client-facing "created"
reply must wait for *serveability*, not hand the client the formation
window.** `CreateTable`'s 200 (DynamoDB edge; CQL's `CREATED` result had
the identical shape) waited only for the `CreateTableSchema`/
`CreateTablet` commits; the tablet's Raft group then forms and elects
asynchronously (each replica's tablet-host reconciler → election, ≥ one
election timeout), so a client's immediately-following first write landed
inside that window and only succeeded via the election-wait machinery
(`cp_forward`'s backoff pass / the local `RouteDecision::Wait`) — burning
much of `CLIENT_TIMEOUT`, or failing outright, under unlucky timing. The
root fix is a converged-or-timeout wait on the *served* property itself
(`ClientCtx::await_table_serveable`: a linearizable probe read through
the ordinary routing machinery — ReadIndex success implies an elected
leader with quorum contact, so "readable" covers "can commit a write
promptly" too), never a longer client timeout, and never a wait on a
*proxy* (leader-hint gossip, reconciler state) that can diverge from what
a real routed request experiences. The regression test's load-bearing
assertion is deliberately **one-shot at ack time**
(`tests/create_table_ready.rs`): the property under test is "already
true when the reply arrives", so a poll there would mask exactly the
race being pinned — the inverse of the usual converged-or-timeout rule,
which governs *eventual* properties, not ack-implied ones. General form:
when an API reply means "you can now use X", the reply path must itself
exercise X the way a client would; auto-provision paths whose own next
op already rides the waiting machinery (`cp_route`) need no such gate.
