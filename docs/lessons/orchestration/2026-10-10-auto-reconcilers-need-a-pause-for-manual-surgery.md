# Auto-reconcilers need a pause switch for manual surgery (issue #1277)

A level-triggered reconciler that restores "desired" state (the operator's
control-voter auto-add) silently reverts any manual corrective step that
temporarily violates it (`control-remove` during quorum-loss recovery). When a
runbook calls for such a step, the runbook must name how to pause the
reconciler first; give every such loop an explicit, visible (status condition)
opt-out rather than relying on operators racing the reconcile period.
