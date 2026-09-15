# A document-path update function whose operand must already exist (`list_append`) composes with `if_not_exists` for the absent-key case for free — no separate provisioning write is needed (ADR 0061 rung D2 PR 2)

`UpdateExpression`'s `list_append(a, b)` requires both operands to already
be lists — a missing `a` (the common "first write to this key" case for an
append-workload corpus) is a validation error, not an implicit empty list.
`sim_cluster_dynamo_corpus.rs`'s own write is `SET items =
list_append(if_not_exists(items, :empty), :v)`: `if_not_exists`'s own
default-value branch supplies the empty seed inline, in the same expression,
with no separate `PutItem`/`BatchWriteItem` provisioning step needed before
the first append — verified by this corpus's very first run (which never
provisions a key ahead of time) landing every append correctly from a
key's first touch onward, at every seed. Worth remembering generally: when
building any DynamoDB-wire workload around `list_append` (or any other
function whose grammar requires an existing operand), reach for
`if_not_exists(path, default)` as the composed operand rather than a
separate upsert/provisioning write — it is both fewer requests and fewer
places for a corpus's own bookkeeping to drift from the server's actual
state.
