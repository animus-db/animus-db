# `UpdateTable`'s own `AttributeDefinitions` requirement is scoped to the new index's keys only, because the decoder never sees the table's existing ones (roadmap W-11)

`decode_create_table` has the whole picture in one JSON body — the base
table's `KeySchema` plus every declared GSI/LSI's own — so its
`AttributeDefinitions` check can legitimately be "exactly the union of
every key attribute this request's own key schema names, nothing more,
nothing less." `decode_update_table`'s `GlobalSecondaryIndexUpdates`
`Create` arm cannot use the same rule: this crate is deliberately pure (no
I/O, no replicated catalog, `animus-dynamo/CLAUDE.md`'s own charter), so it
has no way to know the base table's *existing* partition/sort key or any
*other* index's keys — only the one new index's own `KeySchema` is ever in
the request body. So the required set for `UpdateTable` is scoped to
**just the new index's own key attribute(s)** — not because DynamoDB's
real rule is narrower there (it isn't: AWS's own `AttributeDefinitions`
rule spans a table's whole key schema, existing keys included, and
tolerates a caller re-declaring one), but because that is the largest set
this decoder can actually verify without a capability it deliberately
doesn't have. A caller that also re-sends the base table's own key
definitions on an `UpdateTable` call — a real, AWS-legal pattern some
SDKs/IaC tools use — would be rejected here as "unused," a known,
narrower-than-AWS gap; nothing in this repo's own test fixtures does that
today, so it wasn't hit, but it is the direct consequence of the pure-crate
boundary above and worth knowing before "fixing" it by loosening the
unused-side check instead of giving the decoder catalog access (which
`animus-dynamo/CLAUDE.md`'s charter rules out).
