# Before implementing a task framed as "close this documented gap," grep the actual code — an ADR/CLAUDE.md's "still deferred"/"future work" language can lag well behind a fix that already shipped.

**Before implementing a task framed as "close this documented gap," grep the
actual code — an ADR/CLAUDE.md's "still deferred"/"future work" language can
lag well behind a fix that already shipped.** Tasked with closing ADR 0013's
"index entry data isn't replicated, so a restarted/uninformed node's GSI
query silently returns incomplete results" gap, the lazy
backfill-from-base-table-scan design the task asked to *evaluate* had
already been implemented and end-to-end tested (`animusd`'s
`backfill_index_if_needed`/`SchemaRegistry::backfilled`, commit `46e25b5`) —
only the ADR's "Still deferred" section and the crate's own `CLAUDE.md` bullet
had never been updated to say so. Grepping for the gap's own likely
mechanism names (`backfill`, `sync_indexes`, the registry struct) before
writing new code turned "implement X" into "harden X's one remaining edge
case and fix the stale docs" — a much smaller, correct-scoped change than a
reimplementation would have been (and a reimplementation risks silently
reverting a previously-fixed bug the existing tests already guard).
