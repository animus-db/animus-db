# A correct safety refusal can turn a latent misconfiguration into an outage once a feature starts exercising it more often

Found root-causing issue #864's recurring S-07d (`animus-operator`)
`spec.controlNodes` growth stall: the promoted pod converged, printed
"ready," and the whole control group then went silently leaderless for
the rest of the 120s stall guard, with the operator's own log showing
nothing but its periodic reconcile cadence.

## The trap: two individually-correct, individually-old decisions compose into a new failure only once a third thing changes their frequency

`storage.ephemeral: true` (`animus-operator`'s `StorageSpec`) has existed
since before this feature, and its own doc comment already says plainly
that it is "a real Raft safety hazard for any voter pod, not just a
durability trade-off." Issue #667's boot-time check
(`RaftCore::begin_cluster_check`) is a *separate*, independently-correct
fix: a control voter that restarts with an empty Raft WAL, while its own
peers' committed config still names it with real history, is refused
**permanently** rather than risking an unsafe double vote. Neither of
these was wrong, and neither was new. What was new was S-07d's own
`spec.controlNodes` growth mechanism, which (for reasons specific to a
`StatefulSet` having exactly **one** shared pod template) rolls **every**
pod's own container whenever the raw `controlNodes` threshold number
changes — including pods whose own effective role never changes, purely
because the threshold that produced that role is baked into one hash
shared by the whole set. Combine the three: an `ephemeral: true` cluster
whose `controlNodes` grows now routinely deletes-and-recreates every
pre-existing control voter's Pod (wiping its `emptyDir`), each of which
then correctly, permanently refuses to rejoin as a voter — and once
enough of them do, the group loses quorum for good. Three individually
reasonable, individually old decisions; a new failure mode only once a
fourth feature started exercising the interaction routinely rather than
rarely.

None of the individual pieces show up as wrong under inspection: the
storage flag's hazard was already documented; the safety refusal is
provably necessary (Raft correctness); the growth mechanism's
whole-`StatefulSet` roll was a known, accepted cost written down in this
crate's own `CLAUDE.md`. The bug only exists in the *composition*, and
specifically in the fact that composition got *exercised* for the first
time by a feature that had no reason to know about either of the other
two.

## The generalizable rule

When a new feature adds a **restart** (or any other event a safety
mechanism keys off) to a code path that used to run rarely, audit what
that restart now interacts with — a storage mode, a safety check, a
timeout — not just whether the new feature's own logic is correct in
isolation. A configuration flag whose hazard is already known and
documented (here: `ephemeral: true`) is not "safe because nobody hit it
yet"; it is safe only for as long as nothing routinely triggers the
condition the hazard describes. The audit question to ask explicitly
when reviewing a change that makes something restart more often, or for
the first time in a new circumstance: "what does this restart wipe, and
what does something else do in permanent response to that wipe?" A clean
`SimEnv` reproduction of the new feature's own logic (which this
investigation also built, and which converged on every seed) proves
nothing about this class of bug either — the interaction lives entirely
outside the mechanism being tested, in a deployment-level choice
(`storage.ephemeral`) the simulator has no model of at all.

## The secondary, closely-related lesson: a retry that never happens is indistinguishable from a stall that never gets diagnosed

Separately, the same investigation found `add_control_voter` tried each
already-confirmed voter ordinal exactly once per reconcile and logged
nothing beyond an aggregate `warn!` on total failure — several of its
"no progress this reconcile" branches logged nothing at all. This meant
a real, root-causeable failure (the storage/restart/refusal interaction
above) and a purely transient one (`RaftCore::change_membership`'s
erratum guard, a genuine one-round-trip-after-election 409 that two
*other* call sites in this codebase already needed a bounded retry for,
issues #667/#900) were both invisible from the same silent log line.
**A reconciler that can fail for several distinct reasons must log which
one, every time, not just on the branch someone happened to add
`warn!` to first** — the alternative is re-litigating "which of N
possible causes was it" from log silence on every future occurrence,
exactly as this issue's own history shows happened three times before
the diagnostics existed to settle it.

See ADR 0060's 2026-09-15 amendment and issue #864 for the full
investigation, evidence, and fix.
