# A wire adapter's `UpdateTable`-add-a-GSI decode path can silently ignore attribute types even though the equivalent `CreateTable` path requires them — check what a decoder actually reads before assuming its request shape mirrors its sibling operation's (2026-08-20).

**A wire adapter's `UpdateTable`-add-a-GSI decode path can silently
ignore attribute types even though the equivalent `CreateTable` path
requires them — check what a decoder actually reads before assuming its
request shape mirrors its sibling operation's (2026-08-20).** This
adapter's `GlobalSecondaryIndexUpdates` `Create` decoding
(`animus_dynamo::wire::decode_index_updates` → `decode_index_entry`)
reads `KeySchema` straight off the `Create` object itself and never looks
at a top-level `AttributeDefinitions` at all — unlike `CreateTable`,
where `AttributeDefinitions` feeds the base table's own `ColumnDef`
types. A new GSI's hash/sort attribute therefore gets no explicit type
recorded anywhere in the catalog; `IndexDef` stores only the attribute
*name*. Before building a feature on top of an existing wire operation,
read what its decoder actually consumes; a sibling operation's contract is
not evidence the one you're calling shares it. **And when the gap means a
UI control's value cannot survive its own round trip, remove the control —
do not paper over it with a default.** The Config tab's Add-GSI form
originally offered an `S`/`N`/`B` picker per key attribute; the pick was
accepted with a `200 OK` and read straight back as `S`, so the screen
contradicted itself within one interaction. Defaulting the *display* to
`"S"` would have hidden that, which is worse than the bug: an invented
value is indistinguishable from a recorded one. The fix was to delete the
picker, stop sending the `AttributeDefinitions` the decoder ignores, and
make the type explicitly nullable end-to-end (`console::IndexKeySummary`'s
`Option<String>`, rendered as a bare attribute name) so the absence is
visible rather than filled in. The decoder gap itself is issue #319 — an
incidental pre-existing bug, so its own change with its own test, per the
repo convention. General form: a fallback default is only honest when the
fallback is unreachable in practice; where it *is* reachable, model the
absence. (`crates/animus-dynamo/src/wire.rs`, `crates/animusd/src/
console.{rs,js}`.)
**Follow-up (2026-08-20): the ADR text this same PR wrote asserted the
*sibling* `CreateTable` path did not share this gap — that assertion was
wrong, and nobody had traced it to find out.** The Config tab's own ADR
amendment claimed "a GSI declared at `CreateTable` time on the same table
[gets a type]"; the create-table-form PR (PR6) actually traced
`schema::to_control`/`index_to_control` before believing it, and found
the identical gap: `to_control` only builds a `ColumnDef` for the base
table's own partition/sort key, and `index_to_control` never receives
`key_types` for *any* index, `CreateTable`-declared or not. The lesson
generalizes past this one decoder: **an unverified claim about a sibling
code path, once written into an ADR or a doc comment, is exactly as
trustworthy as an unverified assumption — restating it in prose does not
make it checked.** A task that says "verify against the decoder, don't
assume it behaves like its sibling" applies even when the thing you'd be
trusting is this repo's own prior documentation of that sibling. Trace
the actual code for *every* new call site that offers a control backed by
it, even one an earlier PR's ADR text already described with apparent
confidence.
