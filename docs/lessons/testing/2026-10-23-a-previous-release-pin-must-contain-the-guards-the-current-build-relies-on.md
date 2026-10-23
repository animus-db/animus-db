# A previous-release pin must contain the durable guards the current build relies on (issue #1355)

`upgrade_previous_release` `clean_3_nodes` failed once in ~35 runs with
`[eventual-prefix]` and `[restart-all]`. The restart path, not only the snapshot
path of #1315, reproduces the old-binary hazard: a replica restarted onto the
current build replays a short log tail over the engine the old binary wrote, and
a stale `TxnStage` whose resolve was compacted away is accepted because the old
binary wrote no resolved marker.

- **No apply-side rule can repair it.** The current build cannot tell "never
  resolved" from "resolved by a binary that wrote no marker", and skipping
  replay at or below the engine's max row version is unsafe (`SeedBatch` carries
  source-cluster versions). A hazard that the new side cannot close is a support
  boundary: move the pin past the fix and write the older tree down as
  unsupported (ADR 0073's 2026-10-23 amendment), as was done for `efcaa6cb`.
- **Pick the pin by what the current build's replay depends on**, not by "oldest
  green tree": every durable guard (marker, replay fences) must be in it.
- **A pin with a version era of its own changes the harness.** `roll plan` only
  restarts nodes whose binary range misses the goal (`active + 1`), and an R-1
  with an era covers it until it is finalized to its own maximum. The harness
  now finalizes R-1 first, as an operator would, then rolls, then finalizes
  to this tree's maximum.
- Keep the old-binary model cells as the executable record of the unsupported
  path, with an all-current control as the regression for the supported one.
