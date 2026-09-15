# A pagination cursor that echoes back a superset of another cursor's attributes needs an *exact*-match validation, not a "the attributes I need are present" check (2026-08-22, DynamoDB `Query` pagination).

**A pagination cursor that echoes back a superset of another cursor's
attributes needs an *exact*-match validation, not a "the attributes I
need are present" check (2026-08-22, DynamoDB `Query` pagination).**
`animusd::dynamo`'s base/GSI/LSI `Query`/`Scan` cursors are all real
`Item`s built by `key_item_of`/`gsi_key_item_of`/`lsi_key_item_of` — and a
GSI or LSI cursor *always* carries the base table's own key attributes
too (real DynamoDB's `LastEvaluatedKey` needs them for uniqueness). That
means a base-table cursor's attribute set is a **subset** of every index
cursor's set on the same table. A resume-key resolver that only checks
"does this `Item` have the attributes I need" (the shape `resolve_key`/
`gsi_resume_key`/`lsi_resume_key` already had, since they only ever read
the keys they need and ignore the rest) will *silently accept* a GSI or
LSI cursor replayed against the base table — it has `pk`/`sk` and then
some, and "then some" is invisible to a presence check. The fix is a
dedicated exact-set-equality check (`validate_query_cursor_shape`, an
`Item`'s key names compared as a `BTreeSet` against the target's expected
set) run *before* the lenient resolver, on every pagination path. The
general form: whenever cursor/token shapes for related-but-distinct
operations nest inside each other (a wider shape strictly containing a
narrower one), presence-checking the narrower shape's fields is not
enough to reject the wider shape — verify the field *set*, not just that
the fields you need happen to be there. (`crates/animusd/src/dynamo.rs`.)
