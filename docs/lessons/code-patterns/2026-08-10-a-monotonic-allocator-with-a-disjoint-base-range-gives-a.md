# A monotonic allocator with a disjoint base range gives a *hard* uniqueness guarantee and needs no pre-check — prefer it over a best-effort collision guard whenever the state machine itself can enforce uniqueness.

**A monotonic allocator with a disjoint base range gives a *hard*
uniqueness guarantee and needs no pre-check — prefer it over a best-effort
collision guard whenever the state machine itself can enforce uniqueness.**
`animusd join --node I` (ADR 0032) protects against two operators picking
the same index with a pre-bind `Status` read compared for exact
address-book equality — the ADR's own doc names the *real* backstop as
`RegisterNodeAddrs`'s idempotent apply, i.e. the pre-check narrows but does
not close the race. Adding `MetaCommand::AllocateNodeId` (ADR 0036) —
mirroring the existing tablet-id allocator
(`Metadata::next_tablet_id`/`next_free_tablet_id`, ADR 0023) instead of
inventing a new mechanism — makes two racing proposals land on two
distinct ids *by construction*: the monotonic floor plus an apply-time
presence check is evaluated identically on every replica, so no epoch-CAS
and no pre-bind guess is needed at all. General shape to reach for: when a
cluster hands out an id/slot/index anywhere, ask whether a client-side
guess-then-verify is standing in for a server-side monotonic allocator that
could instead make the race structurally impossible.
