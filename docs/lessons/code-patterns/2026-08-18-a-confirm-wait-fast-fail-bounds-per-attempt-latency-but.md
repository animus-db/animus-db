# A confirm-wait fast-fail bounds per-attempt latency but doesn't make it safe to hold a serialization lock across the wait — when a lock is provably redundant with an apply-time check, scope it to read+eval only

**A confirm-wait fast-fail bounds per-attempt latency but doesn't make it
safe to hold a serialization lock across the wait — when a lock is
provably redundant with an apply-time check, scope it to read+eval only**
(issue #285). `dynamo::kind_write_item_at_leader` held `ctx.data().
rmw_lock` (one lock per node, shared by every table/tablet this node
leads) across its own read *and* the full `cp_kind_local` propose+
confirm-poll — so one item's slow confirm (apply backlog stretches this
even with the #268 `confirm_wait_is_futile` fast-fail, which only bounds
*this* attempt, not how long that attempt takes to even resolve under
load) stalled every *other* evaluated write on the node behind it, not
just racing writes of the *same* item. The lock was never the thing
making concurrent writes of one item safe in the first place — the
apply-time OCC seatbelt (`KindBatch.conditions`, checked byte-for-byte
against the actual committed value on every replica) already had to work
lock-free, since `txn_resolver_loop`'s recovery pushes never take this
lock at all. Once a lock is provably redundant with an apply-time check
like this, its only remaining job is a same-node collision-rate
optimization, so it only ever needs to span the read+evaluate that
produces the value the check is *based on* — never the propose/confirm
that verifies it. **The scoping pattern already existed one function
away**: `ClientCtx::txn_stage_local` takes the identical `rmw_lock` only
around its own read+evaluate loop, dropping it before staging — grep
sibling functions touching the same lock/primitive for an already-
established narrower scoping before assuming a wider one is the
house convention just because it's what you found first.
(`crates/animusd/src/dynamo.rs::kind_write_item_at_leader`.)
