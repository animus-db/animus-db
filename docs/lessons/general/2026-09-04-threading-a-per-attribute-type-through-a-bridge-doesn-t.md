# Threading a per-attribute type through a bridge doesn't require widening every plain-data struct along the way — a side-channel merge at the edge can be enough (issue #319, W-05)

`IndexDef`'s new `hash_attribute_type`/`sort_attribute_type` (ADR 0013's
catalog) needed to reach `DescribeTable`'s `AttributeDefinitions` response
without inventing a genuine per-index type anywhere it didn't already
belong. The tempting shape — mirror the new fields onto `animus-dynamo`'s
own local `GlobalSecondaryIndex`/`LocalSecondaryIndex` (`registry.rs`), the
type every layer between the wire decoder and the response builder already
passes around — would have meant updating every one of that struct's ~27
existing construction sites across `wire.rs`'s decoders, `animusd::dynamo`,
`console.rs`, and every test file building one by hand, on top of the 24
sites `IndexDef` itself already needed (root `CLAUDE.md`'s own "a future
8th port" entry already names this exact E0063 fan-out pattern as
expected/compiler-enumerated, but expected doesn't mean warranted here).

Tracing the actual data flow found the type never needs to ride
`SecondaryIndex` at all: `DescribeTable`'s response builder
(`wire::attribute_definitions`) already takes a flat `key_types:
&[(String, String)]` name→type map and a *separate* `indexes: &[SecondaryIndex]`
for the name coverage — it was already designed to resolve type by **name
lookup**, not by reading a type off the index struct itself. So the fix is
a pure edge-side merge: `animus_dynamo::schema::index_attribute_types(&[IndexDef])
-> Vec<(String, String)>` extracts `(name, type)` pairs straight from the
catalog's own `IndexDef`s (which DO now carry the real type), and
`animusd::dynamo::describe_table`/`delete_table` `.extend()` it onto the
base table's own `key_types` before calling the unchanged response builder.
Zero changes to `SecondaryIndex`/`GlobalSecondaryIndex`/`LocalSecondaryIndex`
or their ~27 sites; only `IndexDef`'s own 24 sites (the catalog type that
actually needed to durably carry the value) were touched. The general
form: before widening a struct that's constructed in dozens of places to
carry a new value, check whether the value's only consumer already
resolves by a decoupled key (a name, an id) rather than by reading the
struct's own field — if so, a merge at the one edge that has both sources
in hand is strictly less invasive than threading the field through
everything in between, and produces the identical observable behavior.

A related, deliberately-not-done change from the same investigation: the
roadmap item's own wording asked to check whether `CreateTable`/`UpdateTable`
already rejects an index key attribute missing from `AttributeDefinitions`
(real DynamoDB does) and add it if not. It doesn't — and grepping
`crates/animusd/tests/*.rs` for `GlobalSecondaryIndexes`/`AttributeDefinitions`
co-occurrence found the *base table's own* key attributes go undeclared in
`AttributeDefinitions` in the large majority of this repo's own CreateTable
fixtures too (e.g. `dynamo_indexes.rs::gsi_write_then_query` declares
neither `id` nor `email`), not just index-only ones — this adapter has
always treated `AttributeDefinitions` as fully optional, a wider and older
design choice than #319's own scope. Adding the strict AWS rejection would
therefore have broken dozens of pre-existing tests across files this task's
own validation gate never reaches, for a behavior change far larger than
the "M"-sized item described. Left unenforced, documented here and in the
session report, rather than either silently expanding the diff's blast
radius or silently skipping the question asked.
