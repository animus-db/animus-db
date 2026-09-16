# When a registration/claim CAS is meant to be the "sole claim path," audit every *existing* call site that currently establishes the same identity through a wholly different, older command before tightening the CAS's companion update-only command — a design that only reasons about "the new join flow" misses the others and ships a bimodal per-process hang.

**When a registration/claim CAS is meant to be the "sole claim path," audit
every *existing* call site that currently establishes the same identity
through a wholly different, older command before tightening the CAS's
companion update-only command — a design that only reasons about "the new
join flow" misses the others and ships a bimodal per-process hang.**
Retiring the ADR 0036 allocator for ADR 0040's `MetaCommand::RegisterNode`
CAS, the obvious design compares the *proposed* `NodeAddrs` **and**
`labels` against whatever's on file, rejecting a mismatch as a genuine
collision — and it looks right in isolation (a fresh unit test proves the
fresh-claim and reject-on-mismatch cases cleanly). It broke
`animusd`'s `runtime_added_voter_survives_leadership_change_to_a_different_
original_voter` integration test (a real, pre-existing scenario, not a new
one this PR added) with a 15-second timeout, not a compile error: a
permanently-non-voter *control-only* growth node
(`BoundControlNode::start_control_with`) has **no other command that ever
claims its membership** — no `bootstrap()` insert (it's outside the
pre-growth set), no `admin_add_member` call (that branch only exists in
`BoundNode::start_with`'s combined-mode growth path) — so its *own*
self-registration is the *only* thing that ever creates its `members` row,
and a labels-strict CAS run against a `Metadata` where that row doesn't
exist yet works fine there. The failure mode that unit tests alone don't
reach: a **combined-mode** growth node's `admin_add_member(node, real_labels)`
(via `UpsertMember`) and its own `spawn_common_tail` self-registration race
*independently* (two unrelated `tokio::spawn` tasks with no ordering
between them) — whichever wins first "claims" membership with its own
labels, and the *other* command's differing labels then permanently fail
a labels-inclusive CAS comparison against a `Metadata` that already has a
`members` row but not yet a `node_addrs` one, since there is nothing there
to ever become "identical" (the losing command retries forever, always
rejected, since the winner's row never changes). **Fix**: key the CAS on
the *one field only this command ever writes* (`node_addrs` alone, not
`members`/`labels`), so "member already claimed by some other, decoupled
command with no address yet" is treated as an unclaimed *address* slot,
never a collision — the actual identity/address collision this CAS exists
to prevent is always visible in that one field regardless of which
membership-establishing command got there first. **General rule**: before
shipping a CAS meant to be the sole path for claiming X, grep for every
*pre-existing* command that can independently create a partial version of
X (here: three different call sites all capable of inserting a `members`
row with no matching `node_addrs` entry) and design the collision key
around the field the CAS actually owns, not the union of every field a
fully-formed claim eventually has — then add a regression unit test for
exactly the "claimed via a different command, no address yet" shape, not
just the two obvious cases a fresh design starts with. (`animus-control`'s
`meta.rs::register_node_claims_an_address_for_a_member_already_claimed_
without_one`; ADR 0040 PR4.)
