# A filter enforced on N serve paths needs a regression per path × per record shape, even when every path calls one shared predicate — the shared function proves the paths *agree*, not that any given path is ever actually reached with the shape in hand

**A filter enforced on N serve paths needs a regression per path × per
record shape, even when every path calls one shared predicate — the
shared function proves the paths *agree*, not that any given path is
ever actually reached with the shape in hand** (2026-08-17, issue #267).
The consumer-hidden stream-record filter (`ChangeRecord::
consumer_hidden`, both `GetRecords` serve branches) had its
streamed-mid-backfill `seeded` regression pinned only on the open-tail
path: the test's own seal knobs were deliberately tuned so nothing ever
sealed, which made the sealed segment-decode branch
(`get_records_sealed`) — the very path the sealer's
"seal seed markers into segments by design" decision routes those
records through over time — structurally unreachable from the test. The
`marker` shape meanwhile had sealed-path coverage and the `seeded` shape
none: two shapes × two paths, only half the cells covered, invisible
unless you draw the matrix. When a test tunes knobs so one serve path
never engages, that tuning is the test's own blind spot — add the dual
with the knobs inverted (`animusd`
`tests/stream_backfill_seed_filter.rs`, the paired open/sealed tests).
