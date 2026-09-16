# A conservative "wait, don't guess" branch sized for a short election silently becomes a full-timeout stall once the same local signal can also mean "this node's own processing lagged its peers"

**A routing decision that correctly waits rather than guesses when a
node's own local view is momentarily unresolved — reasoned about as
"this is a short election, and the only real route might be this very
node" — can silently regress into a full-budget stall once the SAME
local signal (`RaftKvNode::leader()` returning `None`) also has a second,
much more common cause under load: this one node's own heartbeat/apply
processing simply lagging behind the rest of its group, not a genuine
election at all.** Issue #950: `ClientCtx::cp_route`'s "this node hosts a
replica of the tablet but doesn't know its leader" branch
(`topology::decide_cp_route`'s `RouteDecision::Wait`) polled only this
node's own local state for up to the full `CLIENT_TIMEOUT` (10s), with no
fallback — sound reasoning for a genuine, brief election, but every real
reproduction showed a continuously known, stable leader per an
independent poll of the SAME group the whole time. The bug wasn't in the
Wait decision's own logic (which correctly never forwards blindly to a
node that might itself become leader mid-election) — it was in treating
"my own local view is unresolved" as equivalent to "the group's own
state is unresolved," when the two diverge exactly under the conditions
(a loaded, oversubscribed host) most likely to surface the bug in
production.

**The generalizable rule: when a wait/retry branch is justified by "this
condition is normally short-lived" (an election, a formation window, a
lock release), audit whether the LOCAL signal it polls can also go stale
for a reason unrelated to that short-lived condition — and if so, the
branch needs an escape hatch that consults other sources of truth, not
just a longer local wait.** A purely local poll cannot, by construction,
tell "genuinely still settling" apart from "my own view of a settled
fact has lagged" — the fix is not a bigger timeout (which only makes the
stall longer before it fails) but asking a DIFFERENT vantage point (here:
every other known replica, concurrently, since none of them carries a
signal worth preferring over another — see the sibling
code-patterns entry on racing unvouched candidates concurrently rather
than serially) once the branch has had a bounded, short chance to resolve
on its own first. The short first window still protects the original
"don't guess mid-election" property; the escape hatch only fires once
that window has plausibly been exceeded by something other than a normal
election.

**A second, narrower lesson from the same incident**: three distinct
client-visible error strings (`"no CP group leader reachable"`, `"relay
to peer node failed"`, `"forwarded CP op: not the leader here;
leader_hint=none"`) all traced back to this ONE mechanism once the
evidence was read carefully (elapsed time matching `CLIENT_TIMEOUT`/
`HINTED_FORWARD_HOP_TIMEOUT` almost exactly, every time) — resist the
urge to treat multiple distinct error messages as necessarily multiple
distinct bugs; trace each one to the exact branch that produces it before
assuming they need separate fixes.
