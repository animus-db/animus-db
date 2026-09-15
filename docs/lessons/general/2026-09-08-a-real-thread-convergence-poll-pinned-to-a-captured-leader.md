# A real-thread convergence poll pinned to a captured leader index is unsound the moment the mechanism it drives can legitimately re-elect (issue #781)

`crates/animusd/tests/cp_reconfigure.rs::cp_group_follows_tablet_replica_set`
captures the CP group's leader index once, at group formation
(`leader_idx`), then drops a follower from the tablet's replica set and
polls for the group to reconfigure down to two voters. The poll used to
read the group's admin view only through `nodes[leader_idx]` — sound only
if the leader that formed the group is still the leader once the drop's
own reconfigure commits. It is not guaranteed to be: the *dropped* node
keeps campaigning once it stops receiving heartbeats from the group it
was just removed from (nothing tells a removed voter it was removed until
the leader's own reconfigure actually lands), and under real scheduling
jitter it can win a term against a momentarily slow original leader
before the drop itself commits — moving leadership to the *other kept*
node. The group still converges correctly; a poll pinned to the stale
`leader_idx` never observes that convergence at all, and spins to a false
60s timeout that reads exactly like a genuinely stuck reconfigure in a CI
log with no way to tell the two apart.

**The fix generalizes past this one test**: any convergence poll over a
real, re-electable consensus group must check *every* node's own view for
"whoever currently leads," never a single node captured once before the
poll's own trigger fires — the same "converged-or-timeout, not a
fixed-target check" family the root `CLAUDE.md`'s engineering-lessons
entry already names, sharpened for the specific case where the *target*
of the poll (not just its timing) can move. The fix also tracks each
node's last-observed `(is_leader, voters)` and prints it in the panic
message on a genuine timeout — turning a bare "60s elapsed" into evidence
that actually answers the question a real production incident needs
answered (did leadership move, and to where, or did nothing happen at
all).

**Characterization (issue #781's own investigation)**: 5 real-thread runs
alone plus 5 more under deliberate CPU contention (six busy-loop processes
started immediately before, on a 4-vCPU sandbox) all passed and converged
on the *original* `leader_idx` every time — no re-election was observed in
this environment even under load, so the fix is prophylactic (closing a
real, reasoned-through race) rather than a reproduction of an observed
failure. A `SimEnv` deterministic sibling exists precisely because a
real-thread test alone can't pin the race by seed: `crates/animus-cp-data/
tests/reconfigure_healthy_drop.rs` drives the identical drop (both a
follower and, separately, the leader itself, forcing the transfer-to-
remove-self path) through the real per-node reconfigure loop under
`SimEnv`'s virtual clock, and additionally asserts that *after*
convergence the leader's term and identity hold for several more virtual
seconds with no further fault injected — a term change there would be a
genuine production finding (a removed voter deposing the leader), not a
test artifact. Both stayed green through this investigation.
