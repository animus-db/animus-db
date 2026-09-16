# A response builder that reads only the base schema silently drops catalog data a bridge type never carried in the first place (issue #319)

Investigating "`UpdateTable`'s GSI-create decoder silently drops
`AttributeDefinitions`" (`animus-dynamo/src/wire.rs`) found the premise was
half right and half a red herring. The half that's real: real DynamoDB's
`DescribeTable` response echoes an `AttributeDefinitions` entry for *every*
declared key attribute — base table **and** every GSI's hash/sort **and**
every LSI's alternate sort — but this adapter's `attribute_definitions`
helper only ever iterated `schema.partition_key`/`schema.sort_key`, so an
index-only key attribute (one that isn't coincidentally also the base
table's own key) simply never appeared in the response, on either
`CreateTable`- or `UpdateTable`-declared indexes alike. Fixed by having
`attribute_definitions` also walk the `indexes: &[SecondaryIndex]` list
already passed to its two callers (`describe_table_response`/
`delete_table_response`), deduplicated against the base keys.

The half that's a red herring: the type itself. `animus_control::IndexDef`
has no type field at all — an index's key attribute type has never been
recorded anywhere in the catalog, for a `CreateTable`-declared index just
as much as an `UpdateTable`-added one (already traced and documented, twice
independently, in `animusd/CLAUDE.md`'s console PR6 entry and
`animus-node/src/console.rs`'s own `IndexKeySummary`/`CreateTableRequest`
doc comments, both predating this fix). Nothing downstream — the GSI/LSI
row-key encoder (keys by the item's own runtime `AttributeValue`, never by
a declared schema type), a `Query`'s `KeyConditionExpression` type check
(`declared_sort_key_type` already documents "index sort attribute simply
won't be found, treated as unknown, don't reject" as a *known*, benign
gap) — ever consumed that missing type either. So the fix here renders
every index-only attribute's `AttributeType` as the same `"S"` placeholder
every other untyped attribute already defaults to (an *honest* "present,
unknown type" answer, not a fabricated one) rather than inventing a real
per-index type store, which would be a materially bigger change (a new
`IndexDef` field, both bridge directions, an ADR amendment, and — per the
console's own explicit doc comments — reinstating the Add-GSI form's type
picker it deliberately removed pending exactly this).

**The general lesson**: when a bridge/projection type can't carry a piece
of data (here, `IndexDef` has no type field), grep every *reader* of the
richer catalog independently before concluding "nothing consumes it, so
there's no bug" — one specific consumer (`DescribeTable`'s echo) can still
have a real, fixable completeness gap even though the *root* cause (no
type stored) is correctly out of scope. Conflating "the type is never
recorded" with "therefore every surface that touches key attributes is
equally fine" would have missed a real, additive, AWS-fidelity fix that
needed no schema change and broke no existing caller (verified: the
Data Console's Add-GSI wire request deliberately sends no
`AttributeDefinitions` at all — see `animusd::lib`'s `add_gsi` — so any
fix that *rejected* a missing `AttributeDefinitions` entry, rather than
just filling the response in more completely, would have broken that
already-shipped feature; the additive-only shape sidesteps that trap
entirely).
