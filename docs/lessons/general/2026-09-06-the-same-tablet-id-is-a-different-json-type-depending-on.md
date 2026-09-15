# The same tablet id is a different JSON type depending on which side of `GET /admin/system-table` it comes from (docs/roadmap.md U-05, the Tablets tab lineage panel)

Building the split-lineage/directed-placing panel (`dashboard_tablets.js`)
needed to correlate two things read off the same `GET /admin/system-table?
kind=split_lineage` response: a row's own `id` (a tablet id) against
`value.parent` (also a tablet id) embedded inside a *different* row's JSON
body. They look identical on the page — both are decimal tablet ids — but
they cross the wire as two different JSON types, because `admin.rs`
renders them through two different functions with two different
conventions: `system_table_id_display` deliberately renders a numeric
entity id (`TabletId`'s 8 raw big-endian bytes) as a decimal **string**
(`"5"`, never a JSON number — the same file's own doc explains why:
`u64` can exceed `f64`'s exact integer range), while `system_table_
value_display`'s JSON-passthrough convention for `SplitLineage`/
`SplitPlacing` just re-serializes the stored Rust struct as-is, and
`TabletId`'s `#[derive(Serialize)]` on a one-field tuple struct is a serde
*newtype*, which `serde_json` serializes transparently as the bare inner
value — a JSON **number** (`5`), not a string. So the exact same tablet id
shows up as `"5"` when it's a row's own `id` and `5` when it's a `parent`
field inside a row's `value`. A client that builds a `Map` keyed by
`row.id` and later looks it up with a bare `value.parent` (or compares the
two with `===`) gets a silent miss — JavaScript's `"5" === 5` is `false`,
and neither side throws, so a naive implementation just quietly finds no
ancestor/child for every real relationship instead of erroring loudly.

**General form**: whenever a UI cross-references two fields that name the
"same kind of thing" (a foreign-key-shaped relationship) but arrive
through two different serialization paths on the SAME endpoint — one
because a route's own display layer normalizes ids to strings, the other
because the underlying stored value passes straight through a derived
`Serialize` — never assume they share a JSON type. Normalize both sides
explicitly (`String(x)` in JS, or the equivalent) before keying a map or
comparing, and grep the server-side rendering function for each field
independently rather than trusting that "it's a tablet id on both sides"
implies "it's the same wire shape on both sides." This is a live instance
of a mechanical rule worth generalizing: a hand-rolled JSON view layer
(as opposed to one type's own uniform `#[derive(Serialize)]`) is exactly
where two fields carrying the identical domain value can diverge in wire
representation, because each field's rendering was a separate decision.
