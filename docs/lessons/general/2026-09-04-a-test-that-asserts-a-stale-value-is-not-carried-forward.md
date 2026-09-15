# A test that asserts "a stale value is not carried forward" via a bare substring search can be defeated by your own commit-message-shaped comment (roadmap U-01, 2026-09-04)

Extending `dashboard_storage.js`'s `SYSTEM_TABLE_KINDS` dropdown to all 16
real `EntityKind` variants (`animus-control::syskv`) also meant dropping a
stray `["keyspace", "keyspace"]` entry that had never matched any real
`EntityKind::from_segment` segment (selecting it always returned zero
rows — a latent, harmless-but-confusing pre-existing bug, fixed in the same
change since it's literally the same list this bullet touches, not a
separate drive-by). The regression test asserted the fix the obvious way:
`!storage_js.contains("\"keyspace\"")`. It failed — not because the entry
survived, but because the doc comment directly above the array, explaining
*why* `"keyspace"` was dropped, contains the literal substring `"keyspace"`
too. A plain `.contains()`/`!.contains()` check against a whole served JS
file (or any other whole-text asset) can't distinguish "the thing exists as
data" from "the thing is merely *mentioned*", and a comment describing a
removal is exactly the shape of text likely to reintroduce the very string
the assertion is checking the absence of. The fix: narrow the checked
substring to the actual syntactic shape being asserted against
(`["keyspace",` — the array-literal-entry shape, not the bare word), which
a normal-prose comment is very unlikely to accidentally reproduce. General
form: an absence assertion over unstructured text should search for the
narrowest fragment that could only appear in the construct being tested
for, not the shortest string that seems to identify it — the same
"structure, not substring" caution `docs/engineering-lessons.md` already
applies to log/metric assertions applies here too.
