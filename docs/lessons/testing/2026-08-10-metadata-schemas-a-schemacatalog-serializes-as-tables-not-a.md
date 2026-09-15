# `Metadata.schemas` (a `SchemaCatalog`) serializes as `{"tables": {...}}`, not a bare map — a test reading `/admin/status` JSON must index through that wrapper.

**`Metadata.schemas` (a `SchemaCatalog`) serializes as `{"tables": {...}}`,
not a bare map — a test reading `/admin/status` JSON must index through
that wrapper.** Building plan-syskv-ui's `system_table.rs`, a first-draft
`await_status` predicate checked `status["schemas"].get("orders")` and
timed out at 15s even though the `ProposeSchema(CreateTableSchema)` call
had already returned `PutOk` and the endpoint under test was working
correctly — the predicate was checking the wrong JSON shape, not waiting
on a slow commit. `SchemaCatalog` is `#[derive(Serialize)]`'d as a
one-field struct (`{ tables: BTreeMap<TableName, TableSchema> }`) so its
iteration order stays deterministic and its accessor surface can grow
without widening `Metadata` — a deliberate wrapper, not an oversight —
but that means it does **not** serialize as a bare `{"orders": {...}}`
map the way every other `BTreeMap`-typed `Metadata` field
(`members`/`tablets`/`policies`/`node_addrs`/`cp_member_addrs`) does. A
15s-timeout failure with no other symptom (no error reply, no 4xx/5xx,
the proposal genuinely committed by the time you check by hand) is the
signature of "polling the wrong JSON path", not "the thing is actually
slow" — check the field's Rust type before writing the predicate, not
just its name.
