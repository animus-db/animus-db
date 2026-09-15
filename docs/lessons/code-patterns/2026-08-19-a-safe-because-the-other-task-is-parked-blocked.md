# A "safe because the other task is parked/blocked" precondition is a liability with a name — go find the ones written down (issue #279, 2026-08-19).

**A "safe because the other task is parked/blocked" precondition is a
liability with a name — go find the ones written down (issue #279,
2026-08-19).** Porting the WAL-persist decoupling to the control-plane
driver, the third drainer turned out to be a *public* method,
`RaftNode::flush`, whose own doc comment stated the hazard outright: "because
the driver is parked at that point, this is the sole WAL writer." That
sentence was accurate when written and false the moment persistence moved off
the loop. The generalisable move: when making a synchronous step concurrent,
grep the touched subsystem for prose asserting *why* something is currently
safe — "parked", "blocked", "the only writer", "cannot interleave", "under
this one lock hold" — and treat each hit as a precondition to re-derive, not
as documentation to preserve. They are cheaper to find than the races they
turn into, and unlike the invariants living only in control flow, someone
already did the work of writing them down.
