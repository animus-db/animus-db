# Deleting a seam: grep the *verb* as well as the *noun*, then grep the prose.

**Deleting a seam: grep the *verb* as well as the *noun*, then grep the
prose.** Removing the `ReplicationMode` seam (ADR 0019's 2026-08-23
amendment) had an obvious target list — the enum, the `TableSchema::mode`
field, the `with_mode` builder, the `Metadata::table_mode` accessor — and
that list was **incomplete**. A replicated-catalog field's *write* path in
this codebase is a separate `MetaCommand` variant, not the struct field:
`MetaCommand::SetTableMode` only surfaced from grepping the accessor and
builder names, never from grepping the type. The general rule: for anything
that lives in replicated `Metadata`, the removal set is
`{type, field, builder, accessor, the MetaCommand variant that mutates it,
every gating match site that variant appears in}` — and this repo's own
standing lesson about `is_relayable_command`/`cp_serve_forwarded` allowlists
is the reason the last item matters.

The second half is compiler-invisible and therefore easier to miss: deleting
a subsystem strands **documentation** pointing at it, and nothing fails the
build. After this removal, `raftkv_linearizable.rs`'s module doc still opened
with "This is the CP counterpart of the Accord corpus in `corpus.rs`" — a
file deleted in the same change — and `hlc.rs` still told readers to "see
`crates/animus-consensus/src/node.rs`'s `mvcc_version` doc," a path that no
longer exists. Both passed fmt, clippy `-D warnings`, build and the full test
suite. So after the code gate is green, grep the deleted names across `*.rs`
doc comments, `CLAUDE.md` files and ADRs as a separate pass, and decide per
hit: **re-anchor** it (a comparison that stood on its own — "the same poll
every corpus uses"), or **mark it historical** where the lineage is the point
("unlike the `(logical, node)` encoding the deleted `animus-consensus`
used"). What you must not leave is a live-voice pointer to a path or file
that is gone.
