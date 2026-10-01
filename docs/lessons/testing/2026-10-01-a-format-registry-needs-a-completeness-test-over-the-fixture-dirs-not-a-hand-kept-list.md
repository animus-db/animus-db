# A harness registry of formats needs a completeness test over the fixture directories, not a hand-kept list.

**A harness registry of formats needs a completeness test over the fixture
directories, not a hand-kept list.** Tier 0 originally iterated a hardcoded
`FORMATS` array that had drifted from `transcode::TABLE` (it silently skipped
`encryption-envelope`), so a format added to the table but not the array, or a
new `tests/fixtures/formats/<dir>` added to a crate and never registered, was
green while testing nothing. The fix is to derive coverage from the filesystem:
iterate `TABLE`, scan every crate's fixture directories, and require each to be
named in `TABLE` (whole-file) or `EMBEDDED` (carried inside another format or
off-disk), with a negative control that feeds the pure check a synthetic
unregistered name. Fixture file names are not uniform (`v1.bin`, `v1.json`,
nested per-kind directories), so the version scan must tolerate any extension
and recurse.
