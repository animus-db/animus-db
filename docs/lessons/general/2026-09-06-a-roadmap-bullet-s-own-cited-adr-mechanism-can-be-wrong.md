# A roadmap bullet's own cited ADR/mechanism can be wrong — verify against the code before writing docs that repeat it (docs/roadmap.md U-07, `GET /admin/gc`)

`docs/roadmap.md`'s own U-07 bullet for this route read "orphan-sweep
phase from `segment_janitor_loop` (ADR 0024/0040)" — and the task briefing
built on top of that citation, describing "GC internals" as living in ADR
0024 (drop-table data GC) and ADR 0040 (self-minted node identities).
Neither ADR mentions `segment_janitor.rs`, the segment janitor, or DynamoDB
Streams orphan reaping anywhere — ADR 0040 is about node-id minting and
registration-CAS membership, unrelated in subject entirely, and ADR 0024
covers a genuinely different GC mechanism (the tablet-host reconciler's
`Reclaim` action over dropped tables' *local engine files*, no admin-
surfaced counter anywhere). The segment janitor this route actually
instruments is documented in ADR 0042 §10 (the orphan-reap amendment) and
ADR 0043 §A9 (the janitor loop itself, `crates/animusd/CLAUDE.md`'s own
`segment_janitor.rs` entry, `docs/streams-notes.md`) — confirmed by
grepping the ADR corpus for "segment janitor" / "segment_janitor" before
writing a single line of the as-built note, which is what caught the
mismatch.

Two field-level consequences followed from taking the citation at face
value having been avoided: the as-built notes went into ADR 0020 (route
table) and ADR 0043 (the janitor's own doc), not ADR 0024/0040 where the
roadmap bullet's citation would have pointed; and `dropped_tables_pending`
— a field the task considered adding to this route, reasonably assuming
"GC" meant the ADR-0024 drop-table mechanism the roadmap bullet named —
was correctly recognized as belonging to a *different* subsystem with no
cheap replicated counter to surface, rather than mistakenly wired to
`stream_shards` data that has nothing to do with dropped-table tombstones.
`docs/roadmap.md`'s own bullet was corrected in the same change (see this
PR's U-07 entry) rather than left to keep misleading the next reader.

The generalizable rule: a roadmap/task-brief ADR citation is planning
prose, not verified fact — it can drift from the code the moment the
feature it describes gets implemented under a different ADR than whoever
wrote the roadmap entry assumed, and nothing re-checks a citation once
it's written down. Grep the actual mechanism's own module/doc comments for
which ADR it cites BEFORE trusting a task brief's or roadmap's citation of
the same mechanism, especially when the citation is being asked to anchor
new documentation of your own — propagating a wrong citation into a new
as-built note makes the mistake harder to unwind later, not easier.
