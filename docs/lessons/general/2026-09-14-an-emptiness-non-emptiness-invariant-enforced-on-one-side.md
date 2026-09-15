# An emptiness/non-emptiness invariant enforced on one side of a value's lifecycle (write) must be enforced on every side it can enter from, not assumed inherited (issue #848)

`UpdateAction::Delete`'s apply arm already enforced "DynamoDB does not
store empty sets" on its own *output* (`set_is_empty(&remaining)` removes
the attribute rather than writing back `SS([])`) — proving the invariant
was known and deliberately maintained on that one path. Two other
entry points for the identical value shape had no such guard at all:
`decode_string_set`/the inline `"BS"` arm (`animus_dynamo::wire`) accepted
an empty `SS`/`NS`/`BS` array straight from wire JSON with no check, and
`UpdateAction::Add`'s absent-seed arm (`(None, v) => v.clone()`) would
happily seed an attribute with whatever operand it was given, empty set
included — neither path shares any code with `Delete`'s own guard, so
`Delete` enforcing the rule proved nothing about whether `Add`/`PutItem`
did.

**The general form**: when a codebase enforces an invariant on a value at
one point in its lifecycle (here: never *store* this shape), grep every
other point that value can be *written* from — a decode boundary, a
sibling `UpdateAction` variant, a direct-construction call site bypassing
the decoder entirely — before assuming the invariant holds everywhere. A
guard that exists because a comment says "DynamoDB does not store X" is a
statement about the desired end state, not a proof that every path
reaching that end state has been checked; the fix here needed three
independent guards (the wire decoder's own `SS`/`NS`/`BS` arms, `Add`'s
absent-seed arm for a caller that builds an `UpdateAction` directly rather
than through the decoder, and a matching key-attribute empty-string/binary
check at the `animusd` edge, since key-attribute validity depends on the
resolved table schema the wire/decode layer never sees) — a single shared
`empty_set_message`/`reject_empty_key_value` helper reused by all three,
not one check assumed to cover the others.
