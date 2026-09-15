# A background loop deliberately left out of a lower crate needs no capability trait at all for new admin instrumentation — it already holds the real host (docs/roadmap.md U-07, `GET /admin/gc`)

The first two U-07 routes (`/admin/backup-store`, `/admin/ttl`) each
needed a small capability trait (`BackupJanitorProgressHost`/
`TtlReaperProgressHost`, `animus_node::host`) purely because the loop
publishing the progress had *already* moved to `animus-node` (ADR 0061
rung C2) and therefore no longer held a concrete `animusd::ClientCtx` to
mutate directly — the trait exists solely to let a lower, `E`-generic
crate write into a field on a struct it cannot name. It would have been
easy to assume the third route needed the identical shape, since the
first two both established it and the task briefing described all "leaf
background loops" as roughly interchangeable.

`segment_janitor.rs`, though, is the one loop rung C2 explicitly left in
`animusd` (documented in that crate's own `CLAUDE.md`, and in
`animus-node/CLAUDE.md`'s "segment_janitor did NOT move" entry) — its
replica-repair phase is real placement/membership orchestration over live
cluster membership, not a value one narrow I/O-delegation method can
express, so forcing the move would have meant either dragging real
decision logic into the lower crate or building a capability trait wide
enough to expose it anyway (the exact "contorted trait" failure mode ADR
0061 warns against). Because it never moved, `segment_janitor_loop`/
`segment_janitor_tick` already take a genuine `&ClientCtx`/`ClientCtx` by
value — the *simplest* thing here was not to copy the two-crate
capability-trait pattern at all, but to add the progress type as a plain
`animusd`-local struct and mutate `ClientCtx::segment_janitor_progress`
directly, with the `AdminHost::gc_view` method (still added in
`animus-node`, since the dispatch table itself lives there) as the only
place a lower crate needed to know anything about this route at all.

The generalizable rule: when a task briefing describes several sibling
mechanisms as needing "the same treatment," check each one's own actual
location/scope before assuming the pattern that worked for the first two
also fits the third — a loop that was deliberately, documentedly *not*
moved in an earlier refactor is exactly the kind of exception that a
template-copying pass will otherwise paper over with unnecessary
indirection. The fix that turned out simplest also turned out to be less
code, not more — a sign the extra trait would have been the wrong call
had it been added anyway.
