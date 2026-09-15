# A hard-wrapped `+`/`-`/digit-`.` landing at the start of a doc-comment line is a markdown list marker, not prose — clippy's `doc_lazy_continuation` cascades errors onto every following line (ADR 0044 phase 2, C-02 PR 2)

This repo's own long-form doc-comment style hard-wraps prose at roughly
column 80, sometimes splitting a hyphenated or `+`-joined phrase across
the line boundary (e.g. "...a second buffering\n/// + timer layer..."). A
markdown line that starts (after the `///`/`//!` prefix) with `+ `, `- `,
`* `, or `N. ` is a **list item marker** to pulldown_cmark regardless of
authorial intent — the wrap in this case landed `+ timer layer purely for
the...` at the start of its own raw source line, which rustdoc's markdown
parser reads as opening a new bulleted list right there. `clippy::
doc_lazy_continuation` (part of `-D warnings`) then flags **every
subsequent line up to the next blank line** as "doc list item without
indentation" — nine separate errors from one accidental wrap, none of
which point at the actual `+` that caused it (the errors start on the
line *after*). `cargo build`/`cargo test` don't run clippy, so this is
invisible until the actual `-D warnings` gate — a genuinely confusing
first read, since the flagged lines look like ordinary prose with nothing
wrong.

**Fix**: reflow the paragraph so no line begins with a markdown list/
emphasis-adjacent character after word-wrapping — moving the `+`-joined
phrase (or dash, or an ordinal like "1.") off the line start is enough;
no `#[allow]` needed, and none should be reached for here, since the
underlying text isn't actually a list and an allow would just suppress a
real (if minor) rendering defect in the shipped rustdoc output too.

**General form**: when `-D warnings` reports a `doc_lazy_continuation`
error on a line that reads as unremarkable prose, don't inspect that
line — inspect the line(s) *before* it (back to the last blank doc-comment
line) for one that starts with `+`/`-`/`*`/a bare number followed by `.`
or `)` purely as an artifact of hard-wrapping. Any hand-wrapped prose
convention that can split a `word + word`/`word - word` phrase across a
line boundary is exposed to this; it is cheap to avoid by keeping such a
joiner on the same line as at least one of its operands.
