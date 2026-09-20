# A quorum-loss hazard's validation rule must be scoped to configurations that can actually lose quorum, not to every configuration that triggers the underlying local hazard

Found implementing issue #989 (`animus-operator`'s `storage.ephemeral`
rejection): the informational hazard this rule tightens
(`CONDITION_EPHEMERAL_VOTER_STORAGE_HAZARD`, issue #864) had always fired
"unconditionally whenever `spec.storage.ephemeral` is `true`", and the
most obvious way to turn a warning into a hard rejection is to keep the
exact same predicate and just change what happens when it fires. That
would have been wrong here, and the mistake is generalizable.

## The trap: the *local* hazard (data loss on restart) is not the *systemic*
## hazard the validation exists to prevent (quorum loss)

`storage.ephemeral: true` really does lose a voter's own Raft WAL on every
pod restart, for *any* number of voters, including exactly one. But the
specific, named danger this rejection exists to close — issue #667's
boot-time check permanently refusing a wiped EXISTING voter, costing the
group its quorum for good — structurally **cannot occur** with a single
voter: there is no quorum to lose beyond the one voter itself, and (traced
into `RaftCore::begin_cluster_check`'s own decision procedure, ADR 0009's
2026-09-15 amendment) with zero *other* configured peers, the "wait for
every configured peer to answer" condition the permanent-refusal path
depends on is vacuously satisfied immediately — a wiped lone voter simply
restarts as a fresh, empty bootstrap, never entering the sticky refused
state a multi-peer wiped restart can reach.

Copying the informational condition's own "any ephemeral storage, full
stop" predicate into the hard-rejection rule would have made the
validator reject a real, common, and genuinely safe use case (a
disposable single-node dev/CI cluster) for a mechanism it cannot actually
suffer from — a stricter rule than the danger it was written to prevent,
which is its own kind of bug (a false positive that erodes trust in the
validator generally, not just an inconvenience).

## The generalizable rule

When turning an "X is risky" warning into an "X is rejected" hard rule,
don't just promote the warning's own predicate — re-derive the predicate
from the **specific failure mode** the rejection is meant to prevent, and
check whether that failure mode's own preconditions narrow the warning's
scope. Concretely: trace the actual mechanism (here, a boot-time check's
own decision procedure) rather than assuming "the informational condition
already identified the risky configurations, so the rejection should
cover the same set." A boundary case (N=1, here) is often exactly where a
"wait for every peer" / "need a majority" / "requires at least two
parties" protection degenerates to vacuous truth or a no-op — which is
usually a sign the surrounding hazard doesn't apply either, not a gap in
the protection. When a warning and a rejection end up covering genuinely
different sets for this reason, keep both: the warning still has value
for the narrower, still-real local hazard (data loss) even where the
rejection no longer applies, and each condition's own doc comment should
say precisely which case it covers so the two don't drift back together
by accident later.
