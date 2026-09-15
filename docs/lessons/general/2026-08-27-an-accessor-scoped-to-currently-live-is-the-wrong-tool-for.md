# An accessor scoped to "currently live" is the wrong tool for "everything that ever happened," even when it's the closest existing primitive (ADR 0059 §10, Train 3 PR②)

`Metadata::pitr_replay_segments`'s first implementation was built on
`live_split_descendants` (ADR 0059 §6's own on-demand-capture
re-planning accessor: "given a pinned tablet, which of its *currently
live* descendants must report for the backup to be considered complete").
It looked like the obvious tool for "given a base snapshot's pinned
tablet, which tablets' segments must be replayed" — both questions start
from a pinned tablet and walk forward through splits — but the two
questions are not the same question: capture-completeness only cares
about tablets that still *exist*, while replay must cover every tablet
that *ever* held relevant data, including one retired by an ordinary
`DropTableTablets` (no split at all). `live_split_descendants` answers
empty for exactly that case (a dropped-not-split tablet has no
`split_lineage` entry), so replay silently produced zero segments for
any deleted table's own un-split tablet — caught by this PR's own first
end-to-end test (a deleted-table PITR restore), not by review, and fixed
by writing a direct forward DFS over `split_lineage` that includes every
*visited* tablet regardless of current liveness.

**The generalizable rule**: when a new need "starts from a pinned entity
and walks forward through the same lineage table" as an existing
accessor, don't reach for that accessor just because the traversal shape
matches — check what its own *filter* condition means, since a
completeness predicate ("must this thing still exist to matter") and a
coverage predicate ("did this thing ever hold matter") are easy to
conflate when they happen to agree on every input a first draft's tests
exercise (a table that's still alive, or was split rather than dropped).
Two nearly-identical unit-test names in this PR's own regression suite —
one proving the accessor is re-planned correctly onto live split
descendants, the other proving it still finds a dropped-never-split
tablet's own segments — is the shape that catches this: write the
"the entity is GONE, not just re-planned" case explicitly rather than
assuming a lineage-walking accessor's existing split coverage implies
drop coverage too.
