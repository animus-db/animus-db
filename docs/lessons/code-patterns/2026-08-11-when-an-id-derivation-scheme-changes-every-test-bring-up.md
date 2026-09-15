# When an id-derivation scheme changes, every test bring-up helper's hardcoded "pick a known-free id" literal is a landmine — grep for bare integer id literals in test files, don't just fix the production derivation.

**When an id-derivation scheme changes, every test bring-up helper's
hardcoded "pick a known-free id" literal is a landmine — grep for bare
integer id literals in test files, don't just fix the production
derivation.** ADR 0040 PR1 collapsed the pre-existing `control_id(i) = i` /
`raftkv_id(i) = 300 + i` split into one id per node (`node_id(i) = i`).
Several `animusd` integration tests picked a "obviously free" id for a
growth/control-add scenario by reasoning about the *old* two-space scheme
(e.g. "control ids in this split config are `{0,1,2}`, raftkv ids start at
300, so `3` is free" or "so `300` already exists as a data-plane member,
useful as a collision target") — every one of those literals silently
became wrong once ids unified: `3` collided with the first data-only
node's own new id, `300` no longer named anything at all. These are
exactly the kind of bug the compiler cannot catch (a valid `u64`, a valid
HTTP call, a differently-shaped but still-well-formed JSON error body) —
they only surface as a runtime test assertion failure, one test at a time,
each with a different symptom. **General check: after any change to how
node/member ids are derived or allocated, grep every test file for bare
integer literals passed where an id is expected (`add_control_member`,
`RoleAddrs`/`ClusterConfig` construction, expected-voter-set assertions)
and re-derive each one from the new scheme's actual id space — don't trust
a comment that describes the *old* scheme's reasoning, even one sitting
right next to the literal it once justified.** (`animusd/tests/
control_membership_admin.rs::{add_control_member_collision_shapes,
grow_control_group_converges_everywhere}`, `control_membership_split.rs::
grow_then_replace_a_voter_over_a_split_deployment_with_live_data_traffic`,
ADR 0040 PR1.)
