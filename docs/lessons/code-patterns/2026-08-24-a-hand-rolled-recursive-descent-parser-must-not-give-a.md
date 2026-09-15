# A hand-rolled recursive-descent parser must not give a separator token (`,`) lookahead-based "maybe this ends the list" behavior — that reintroduces exactly the spelling-based ambiguity a real tokenizer was built to remove (2026-08-24, issue #372).

**A hand-rolled recursive-descent parser must not give a separator token
(`,`) lookahead-based "maybe this ends the list" behavior — that
reintroduces exactly the spelling-based ambiguity a real tokenizer was
built to remove (2026-08-24, issue #372).** Replacing
`animus-dynamo::wire::decode_update_expression`'s substring keyword scan
with a real clause tokenizer required, per the DynamoDB grammar, that
`SET`/`REMOVE`/`ADD`/`DELETE` only start a new clause at a genuine
clause-start *position* (expression start, or right after a completed
action with no separator) — never merely because a token is spelled that
way, so `SET set = :v` (an unaliased attribute literally named `set`)
parses correctly. The first implementation still broke on `SET add = :v,
remove = :w`: after finishing the `add` action it saw the following `,`
and *peeked past it* — if the next token happened to spell a clause
keyword, it guessed "trailing comma before a new clause" and stopped the
`SET` clause early, misreading `remove` as a `REMOVE` clause instead of the
second `SET` action's attribute name it actually was. The grammar itself
already disambiguates this without any lookahead: a `,` immediately after
a completed action is *always* an action separator within the current
clause (the grammar has no other legal use for it there), full stop — only
the unconditional absence of a following token (a bare trailing comma at
the very end of the expression) is safe to special-case, because there is
nothing left to misinterpret. **General rule: when a parser's separator
token has to decide "continue this production or end it," that decision
must come from the separator's own unambiguous grammar role, never from
inspecting or guessing at what a subsequent token might mean** — peeking
past a separator to pattern-match on the next token's spelling is the same
category of bug as the substring scan the rewrite was fixing in the first
place, just moved one token later. Caught by a test the rewrite's own task
required (`reserved_words_as_attribute_names_in_a_set_action_list`), not
by any pre-existing test — a reminder that a new parser needs the
ambiguous-looking cases in its own test corpus, not just the cases the old
implementation happened to get right.
