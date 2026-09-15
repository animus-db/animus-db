# A "which statement kind is this" dispatch must run before the full lex, not after, when later grammar uses bytes the current lexer doesn't know (ADR 0071, W-07 PR 2, `animus-dynamo::partiql`)

`parse_select_statement`'s first draft lexed the entire input, then
inspected token 0 to decide "is this a `SELECT`, or an `INSERT`/`UPDATE`/
`DELETE` we should reject with a named 'not supported yet' error?" That
looked right in isolation and every `SELECT`-shaped unit test passed. It
broke on the very first `INSERT` test case: `INSERT INTO t VALUE
{'pk':?}` — PR 3's still-unimplemented `document` grammar uses `{`/`}`,
bytes this PR's lexer has no token for at all, so lexing the whole
statement failed with "unexpected character `{`" before the parser ever
got to look at token 0. The intended, specific "`INSERT` statements are
not supported yet (PR 3)" message never had a chance to fire. The fix was
mechanical once seen: peek the statement's leading bare word directly off
the raw text (skip whitespace, read one identifier-shaped run, match
against the keyword table) *before* calling the full lexer, and only lex
the rest once the statement is known to be a shape this PR's grammar can
even tokenize. **The general lesson: when a parser's grammar grows in
stages (this PR only implements a subset; a later PR's syntax is already
named in the design but not yet lexable), any "which shape is this, and do
we even support it" dispatch must happen on the raw input, not after a
full tokenize** — a full lex implicitly assumes the input already belongs
to a grammar the lexer knows, which a not-yet-supported statement kind by
definition may not. Caught immediately by a unit test
(`rejects_insert_update_delete_with_named_error`) that asserted the error
*message*, not just that parsing failed — a test that only checked
`is_err()` would have passed on the wrong error and hidden this.
