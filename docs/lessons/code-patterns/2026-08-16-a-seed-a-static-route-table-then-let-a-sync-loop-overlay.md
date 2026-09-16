# A "seed a static route table, then let a sync loop overlay `Metadata` on top" pattern needs a real, non-empty static seed if any consumer resolves through it *synchronously*, before the loop's first tick.

**A "seed a static route table, then let a sync loop overlay
`Metadata` on top" pattern needs a real, non-empty static seed if any
consumer resolves through it *synchronously*, before the loop's first
tick.** Adding `intra_route` (ADR 0047, mirroring the pre-existing
`client_route`/`route_sync_loop` shape) first tried an empty static seed
on the theory that the sync loop's 200ms cadence would converge it from
`Metadata` quickly enough — reasonable for most consumers, which
tolerate "not yet known, retry." It broke the growth-node/join-node
mirror's own seed-building (`start_with_streams`'s `ctx.intra_addr(id)`
call, feeding `remote_metadata_sync_loop`), which runs **synchronously**
at ctx-construction time and captures its `seeds` argument once, by
value, at spawn time — an empty seed there is permanent, not
transient, since the loop it feeds never re-reads the route table itself.
Fix: thread the real seed (`intra_route: BTreeMap<NodeId, SocketAddr>`)
as a full sibling parameter everywhere `client_route` already is,
including through `ClientResponse::JoinInfo`. **General form**: when
copying an existing "static seed ∪ replicated overlay" pattern for a new
address axis, check whether *every* consumer of the new table reads it
lazily (tolerates emptiness) or captures a value from it once,
synchronously, at construction time — the latter needs the seed
populated for real, not deferred to the first tick.
