# Deleting a "recruit an external host" mechanism must also delete its downstream "don't release the recruit" exception — the second one doesn't announce itself as dead (ADR 0062 rung 5, Stage 1/2 deletion)

Implementing ADR 0062 rung 5 (deleting `animus-cp-data::host`'s Stage 1/2
learner-add-and-wait machinery: `HostAction::AddSplitLearner`, its tick
arm, `INPLACE_SPLIT_LEARNER_CATCH_UP_THRESHOLD`, and phase 1's "host a node
recruited only via a child's own `replicas`" branch) found a second,
un-named piece of the same mechanism the task's own scope list hadn't
mentioned: phase 3's release check carried a matching `recruited_for_split`
exclusion (`tablets_to_release_set(..).filter(|t| !recruited_for_split
.contains(t))`), added in the same original rung specifically so a
recruited-but-not-yet-promoted learner wouldn't be immediately released as
a stray "not in `replicas`" tablet. Nothing named it as dead — it wasn't in
the "delete this" list, and it doesn't reference `AddSplitLearner` or any
of the symbols that list did name, so a literal grep for the named symbols
would miss it entirely.

It was provably dead by the exact same invariant chain that made phase 1's
recruiting branch dead (ADR 0062 rung 4: `SplitChild::replicas` IS the
parent's own `replicas` at propose time, and a `Splitting` parent's
`replicas` cannot change for the intent's whole lifetime since
`reconcile_placement`/`rebalance_placement` both skip non-`Active`
tablets) — so a node named in a child's `replicas` is *always* already a
member of the parent's own `replicas` too, meaning `tablets_to_release_set`
(which only ever returns a tablet whose `replicas` do NOT contain
`base_id`) can never produce a candidate `recruited_for_split` would need
to protect. Verified independently before deleting it (traced
`reconcile_placement`/`rebalance_placement`'s own `t.state != Active`
guards in `animus-control::meta`), not assumed from the phase-1 finding
alone — the two branches recruit/protect the same set for the same reason,
but a "the cause is dead" argument for one branch doesn't automatically
transfer to a different function reading different inputs without tracing
it again.

**General lesson**: when a task's scope names specific symbols/branches to
delete because they're dead under some new invariant, that invariant
usually kills more than the named list — grep for *every* place that reads
the same condition the deleted branch produced (here: "is this tablet a
recruited-via-child-replicas one"), not just the symbols explicitly named,
before declaring the cleanup done. A downstream exception whose only job
was to compensate for a mechanism that's gone is exactly as dead as the
mechanism itself, and is easy to miss because it doesn't share a name or an
import with what got deleted.
