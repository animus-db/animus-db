# A doc comment paragraph whose second line happens to start with `+` (or `-`/`*`) fails `clippy -D warnings`'s `doc_lazy_continuation` lint even though nothing about the prose is a list

**A doc comment paragraph whose second line happens to start with `+` (or
`-`/`*`) fails `clippy -D warnings`'s `doc_lazy_continuation` lint even
though nothing about the prose is a list** — `rustdoc`/clippy parse `///`
blocks as Markdown, and Markdown reads a line beginning `+ ` as a new list
item; every subsequent line of that same paragraph is then flagged as an
unindented "continuation" of a list item that was never intended to
exist. Hit writing the ADR 0045 "E1" phantom-seed-record fix: "...closed
by this PR's `ChangeRecord::seeded` flag\n/// + `animusd::dynamo_streams`'s
filter):" read as "start a list item `+ ...`," and clippy flagged every
following line of the same paragraph (not just that one) as mis-indented.
The fix is prose, not indentation: reword so no line of a doc comment
starts with a bare list-marker character unless a list is actually
intended. General rule: when clippy's `-D warnings` gate rejects a doc
comment you just wrote with "doc list item without indentation," check
the *first character* of every line in that paragraph for `-`/`*`/`+`
before assuming the lint is confused — it almost always isn't. (ADR 0045
follow-up "E1" fix, 2026-08-16.)
