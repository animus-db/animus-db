# A "resulting count" quorum-loss guard only catches the failure mode where the node being removed is the *only* thing that changed — it is blind to "a different survivor was already dead before this call."

**A "resulting count" quorum-loss guard only catches the failure mode where
the node being removed is the *only* thing that changed — it is blind to
"a different survivor was already dead before this call."** Writing ADR
0037 PR5's end-to-end failure-case sweep, the shipped `admin_remove_control_
member` guard (refuse `< 1` remaining voters, warn at exactly `1`) looks
complete because every test up to that point only ever killed *the node
being removed itself* — so "how many voters remain" and "how much fault
tolerance remains" always agreed. The gap only appears once a *different*
voter is already dead when the removal happens: going from an odd-sized
group (majority tolerates one failure) to an even-sized one (majority
tolerates none) while that other voter stays down can leave 2-or-more
*counted* voters with only 1 *functioning* one — a resulting count of 2
looks safe to a count-only guard and gets no warning at all, yet the group
is now permanently wedged (the removal's own config-change entry can never
itself commit, so no further membership change can ever succeed either).
Proven directly, not just argued in prose, by
`animus-control/tests/control_membership.rs::
removing_a_live_voter_while_a_third_is_already_dead_can_strand_the_group`
(core level: a stranded 2-voter config with one dead never commits
anything again) and `animusd/tests/control_membership_admin.rs::
removing_a_live_voter_while_another_is_already_dead_can_silently_strand_
the_group` (through the real admin path: the removal succeeds with no
warning, then every subsequent membership-change attempt fails with
"already in flight," forever). **Any "how many are left" quorum check
needs its own explicit test for "one of the OTHER survivors is already
gone," not just "how many remain after this one action"** — a test suite
that only ever removes/kills one node at a time will pass a guard that
still ships a silent stranding hazard. Recorded in ADR 0037's Consequences
section as a knowingly-accepted risk rather than a defect, since fixing it
for real needs the raftkv/control id-space unification the previous entry
above already explains why PR3 didn't attempt.

**Update: closed by the ADR 0037 hardening trio's quorum-guard liveness
fix (PR 2, PR #136)**, via the previous entry's `control_peer_believed_alive`
signal — `admin_remove_control_member` now computes `live` = how many of
the *resulting* voters are actually reachable, refusing when that's below
a majority (naming the apparently-dead voter(s), pointing at a new
`--force` escape hatch that is deliberately **independent** of
`decommission --force-control-remove` — the two flags are separate,
neither implies the other). The core-level primitive
(`RaftCore::change_membership`) still has no survivor-liveness guard by
design — that stays a pure single-server-delta mechanism, unchanged; the
guard lives one layer up, in `animusd`'s admin action, the only layer with
a `RaftNode` handle to ask. The regression test this entry named
(`animusd/tests/control_membership_admin.rs::
removing_a_live_voter_while_another_is_already_dead_can_silently_strand_
the_group`) is renamed+flipped to `..._is_refused_without_force` (proving
the refusal) with a `..._succeeds_with_force` sibling (proving the escape
hatch still reaches the exact same stranding consequence this entry
documents, now as informed consent rather than an unconditional default);
the core-level test
(`removing_a_live_voter_while_a_third_is_already_dead_can_strand_the_group`)
is unchanged, since the core itself was never in scope for this fix.
