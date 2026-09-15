# Diagnose "my predicate never turns true" by instrumenting the state it READS, not its own bookkeeping (issues #532/#537)

A learner catch-up predicate (`RaftCore::learner_caught_up`, keyed on
`match_index`) never firing under sustained write load looks, from the
caller's side, like a bug in the *caller* — the reconciler loop that
decides when to check it, the settle window around it, the promotion
sequencing. None of that was where the defect lived. The actual root
cause was two layers below the symptom, inside the shared `RaftCore`
(`animus-control`) that both the control plane and the CP data plane
reuse: `replicate_to` sending an unbounded, ever-growing `AppendEntries`
tail on every propose, and (found investigating the same symptom further)
`snapshot_upto` invalidating an in-flight chunked transfer on every
compaction crossing. **The generalizable move**: when a predicate never
turns true, don't start by auditing the code that calls it or the
bookkeeping around when it's checked — instrument the *live state the
predicate itself reads* (here, `match_index`/`snapshot_offset` on the
actual `RaftCore`) and watch it over time. A pinned or oscillating value,
observed directly, points straight at the mechanism that's supposed to
move it — which can live in a completely different crate than the one
whose loop looks broken.
