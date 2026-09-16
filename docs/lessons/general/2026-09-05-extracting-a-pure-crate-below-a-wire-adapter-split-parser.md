# Extracting a pure crate below a wire adapter: split parser from evaluator, not by file (ADR 0054 step 1, C-01)

Moving `animus-dynamo`'s item model into a new crate below it (`animus-item`)
so a future protocol-agnostic apply path could depend on it without pulling
in the wire adapter looked at first like a file-level move: `condition.rs`,
`index.rs`, and `numkey.rs` were already fully pure (no `serde_json::Value`
anywhere) and moved wholesale, tests included. `wire.rs`'s
`UpdateExpression` machinery did not split that cleanly, because it is two
things wearing one name: a **tokenizer/parser** that turns request JSON
(`ExpressionAttributeNames`/`Values`, both `serde_json::Value`-shaped) into
`Vec<UpdateAction>`, and an **evaluator** (`apply_update` and everything it
calls) that folds those already-resolved actions against an `Item`. Only the
evaluator half is genuinely part of the pure item-mutation model; the parser
half is JSON/wire-decode work that has no business in a crate meant to sit
below the wire adapter, even though both halves reference the exact same
`UpdateAction`/`PathSegment` types. The fix was to move the **data types**
(needed by both halves) and the **evaluator** together, and leave the
**parser** in `wire.rs`, importing the types back across the new crate
boundary. The general rule: when a "this whole module needs to move below
crate X" instinct meets a module that decodes wire-format input into a
value and also *evaluates* that value, check whether the decode step reaches
for a wire-specific type (`serde_json::Value`, a JSON `Map`, an HTTP header)
that the evaluator itself never touches — if so, the boundary is inside the
file, at the parse/evaluate seam, not at the file's edges.

**A second reusable trick for this kind of move**: a large single-file test
module (this one had 247 `#[test]` functions in one `mod tests` block) can be
split into individually-relocatable chunks *without* a real Rust parser by
exploiting `cargo fmt`'s own indentation invariant — every top-level item
inside `mod tests { .. }` is indented exactly 4 spaces, so its own closing
brace is a line that is *exactly* `"    }"`, regardless of what braces or
JSON-string-literal punctuation appear inside the item's body (those are all
indented deeper). A small script walking line-by-line, treating each run
from one non-blank line up to the next bare `"    }"` line as one item, gave
a correct decomposition of the whole test module into individually
classifiable chunks (by keyword: does this chunk call `decode_request`/
`Operation::` → stays; does it only touch `apply_update`/`UpdateAction`
construction → moves) — verified by reconstructing the total test count
(324 before, 324 after, exactly redistributed) rather than trusting the
classifier's keyword list alone. This is much faster and less error-prone
than hand-copying dozens of test functions, and generalizes to any
similarly-large rustfmt'd test module that needs splitting by content rather
than by file.

**A third pattern, for keeping "no behaviour change" honest across a crate
boundary that also needs a different error type**: when a moved function
used to return the enclosing crate's own error type (here, `WireError`, with
a `code`/`message` shape client-visible error handling depends on) but the
new crate cannot depend on that type, give the new crate its own minimal
error type with the **same field shape** (`UpdateError { code, message }`,
mirroring the existing `ConditionError` precedent in the same codebase)
rather than collapsing to a bare `String`. Every test that used to assert
`err.code == "ValidationException"` then keeps working unchanged after the
move, and the original crate's wrapper becomes a one-line `From` impl
instead of a place where message text has to be retyped and could silently
drift.
