# A newly-enforced wire-decode shape rule (e.g. AWS's 3-character table-name minimum) can invalidate an entire crate's own test-fixture convention, not just a handful of tests.

**Before wiring up a previously-unenforced AWS validation rule, grep the
whole repo for fixtures that would now violate it — the violation is often
the *dominant* convention, not a rare edge case.** Adding
`animus_dynamo::limits::is_valid_table_or_index_name` enforcement to
`wire::table_name`/`decode_index_entry`/`Query`+`Scan`'s `IndexName` (ADR
0072 layer 2) turned out to invalidate `"TableName":"t"` — the single most
common table-name literal in this codebase, used **179 times** in
`animus-dynamo/src/wire.rs`'s own decode/encode unit tests alone, plus
dozens more single/double-letter table and index names (`"e"`, `"d"`,
`"pk"`, `"bt"`, `"bk"`, `"mb"`, `"s1"`/`"s2"`/`"s3"`, GSI/LSI names `"a"`,
`"g"`, `"l"`, `"k"`, `"i"`) scattered across nine files in `animus-dynamo`
and `animusd`. A quick manual look at "a couple of tests that might need
fixing" would have badly undercounted the blast radius.

**How to find them all reliably**: don't grep for the specific short names
you happen to notice — extract every `"TableName":"..."`/`"IndexName":"..."`
value in the repo with a regex, dedupe, and filter by the rule itself
(`length < MIN_TABLE_NAME_CHARS`, or invalid characters):

```sh
grep -rhoE '"TableName" *: *"[^"]*"' crates/ --include="*.rs" \
  | sed -E 's/"TableName" *: *"([^"]*)"/\1/' | sort -u \
  | awk '{ if (length($0) < 3) print "SHORT:", $0 }'
```

Then, for each short literal, `grep -c` it in isolation (e.g. `"t"`) before
touching anything — a single letter is very likely **also** used elsewhere
in the same file as an ordinary attribute name or value (e.g. `"t":{"SS":
["a"]}}` is an *attribute* named `t`, not a table), so a blind
find-and-replace across the whole codebase is wrong. Scope the replacement:
if the token appears only in table/index-name position within a file (grep
`"TableName":"X"`/`"RequestItems":{"X"`/`"IndexName":"X"` before trusting a
bare `"X"` count), a whole-file exact-string `replace_all` is safe and far
faster than touching each call site by hand; if it collides with an
attribute-name usage (as `"pk"` did — both a table name *and* every
fixture's partition-key attribute name in `batch_write.rs`), replace only
the anchored `"TableName":"X"` substring, one occurrence at a time.

**A helper that bypasses the decoder needs checking separately.** Not every
table name in a fixture goes through wire validation: `SimCluster::
create_table` provisions a tablet directly (`CreateTableSchema`/
`CreateTablet`), never through `wire::decode_request`, so a short name
passed to it alone never fails — but the very next `PutItem`/`GetItem`
against that same table *does* go through `table_name()`, so the JSON
`"TableName"` value must still be renamed even when the `create_table("t")`
call site technically wouldn't need to be.
