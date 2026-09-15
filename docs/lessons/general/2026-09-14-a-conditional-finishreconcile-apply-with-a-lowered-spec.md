# A conditional `finish_reconcile`/apply-with-a-lowered-spec branch needs the SAME clamp-before-apply guard every sibling failure branch already has — check by pattern, not by branch (issue #853)

`animus-operator`'s scale-down drain-failure branch called
`finish_reconcile(&cluster, ...)` with the caller's own, already-lowered
`cluster.spec.nodes` — even though three sibling failure branches in the
*same function* (`spec.tls`/`spec.s3`/`spec.backup_store` each rejected as
invalid) already established the correct pattern: clone `cluster` into a
`pinned` copy with the unsafe field reset to a safe value, and finish the
reconcile with `&pinned`, never the raw input. The drain-failure branch's
own comment even said the right thing ("don't scale the StatefulSet down
past a pod that never finished draining") while the code one line later
did exactly that, because nothing clamped `spec.nodes` before it reached
`apply_children` → `desired::statefulset::build`, which applies
`spec.nodes` verbatim as `StatefulSet.spec.replicas` with no awareness of
how much of a multi-step operation (here: a highest-ordinal-first drain
sequence) actually completed. The fix is the identical `pinned` idiom,
just computed from the drain loop's own state (the last ordinal that
failed to drain, `+1`, since every ordinal above it already finished)
instead of a constant reset value. **The general rule**: when a function
has several failure branches that all end by finishing/applying a
reconcile early, and *some* of them already pin/clamp the state they pass
down, a newly added or overlooked branch that skips that step is not an
independent bug to reason about from scratch — it is a **pattern
violation** discoverable by literally diffing the branch against its
siblings in the same function. Grepping a function for its own established
idiom (here: `let mut pinned = (*cluster).clone();`) before trusting that
"this branch already does the safe thing" is a cheap, mechanical check
that would have caught this without needing to trace the whole
apply-side of the pipeline. Also: a **partial-progress test case** (some
ordinals drain successfully, a later one fails) is worth adding alongside
the immediate-failure one — a test that only ever fails on the very first
step of a multi-step sequence can't tell "clamp to the pre-operation
count" apart from "clamp to how far we actually got," and only one of
those is the right fix once real partial progress is possible.
