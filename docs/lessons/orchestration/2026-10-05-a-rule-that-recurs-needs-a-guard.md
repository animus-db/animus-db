# A merge-time rule that recurs needs a CI guard, not another lesson

`corpus-deep.yml` crossed GitHub's 25 `workflow_dispatch` input cap a second
time on 2026-10-05 (G-01's `zone_placement_seeds` landed via #1183 on top of
a file already at 25), one day after
`2026-10-04-workflow-dispatch-inputs-are-capped-at-25.md` recorded the first
time. The file went invalid and every corpus-deep run failed at parse time.
A lesson only helps whoever reads it while resolving that merge; the merge
queue resolves nothing and reads nothing.

**Rule.** When a lesson describes a mechanical invariant that a merge can break
without any per-push gate noticing, add the check to CI in the same change.
Here: `scripts/check-workflow-dispatch-inputs.sh`, run in `ci.yml` right after
the fixtures check.
