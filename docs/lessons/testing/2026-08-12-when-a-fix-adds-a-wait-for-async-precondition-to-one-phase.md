# When a fix adds a wait-for-async-precondition to ONE phase of a multi-phase test, audit every other phase for the same shape — the race doesn't know which phase it's in.

**When a fix adds a wait-for-async-precondition to ONE phase of a
multi-phase test, audit every other phase for the same shape — the race
doesn't know which phase it's in.** ADR 0040 PR4 fixed
`control_membership_split.rs`'s GROW phase to poll for a joining node's
background self-registration (`spawn_common_tail`'s fire-and-forget
`register_node` task) landing before calling `admin/member/add` — two
legitimate `RegisterNode` proposals for the same id otherwise race with
different `NodeAddrs` payloads, and the registration CAS *correctly*
rejects the loser ("already claimed by a different registration"). The
REPLACEMENT phase of the same test performed the identical
join-then-admin-add sequence with no such wait and inherited the identical
race, surfacing (rarely under load at first, then ~1-in-2 in isolation as
timing shifted with unrelated merges) at
`control_membership_split.rs:454`. Not a CAS bug — the CAS did its job;
the test raced two registrars it was responsible for sequencing. Fix:
the same poll, mirrored onto the replacement phase. The general audit
question: "this test just gained a wait — where else does it (or its
siblings) perform the same async-then-act sequence without one?"
