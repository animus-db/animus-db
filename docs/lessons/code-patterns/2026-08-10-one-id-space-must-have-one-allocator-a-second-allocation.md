# One id space must have one allocator — a second allocation path silently breaks the invariant the first one carries.

**One id space must have one allocator — a second allocation path silently breaks
the invariant the first one carries.** Tablet ids are never-reused *because*
provisioning allocates via `next_free_tablet_id()` (folds in the monotonic
`next_tablet_id`); `trigger_split` allocated `max(live ids)+1` instead, so
drop-highest-table-then-split re-mints the freed id — and a replica still holding
the dropped tablet's files re-hosts them as the new tablet (ADR 0024 violation;
GC can never reclaim them since the id is live again). The apply-side validation
only rejected collisions with *present* tablets, so nothing self-healed. Route
every mint through the one allocator, and make the replicated apply reject ids
below the monotonic counter so a divergent client can't reintroduce it.
